//! Three voters (ADR-0008 steps 2 and 5, D-70 A): `cluster::Voter`s over
//! the segmented log on simulated disks, a network that loses, delays and
//! reorders messages, process crashes, power losses with torn writes,
//! partitions — so sequencers die and the next in the schedule takes over —
//! and compaction on every node, during catch-up and during takeover.
//!
//! Checked after every event (unified-stream §5):
//!
//! * **1** — a committed record never changes position and stays on a
//!   majority: durable there, or compacted after it was committed; no cut
//!   ever removes a node's copy of a committed record.
//! * **2** — at most one sequencer per slot, across nodes and incarnations.
//! * **3** — every node's log matches the committed log up to the commit
//!   point that node knows.
//! * **5** — every recovered log is one unbroken chain: lsn monotonic,
//!   `prev_hash` continuous, a torn batch only at the end.
//! * **6** — no sequencer of slot k raises the commit point past what was
//!   durable on a majority when a majority's promises past k took effect.
//! * Storage keeps its word: a restart recovers at least what the node was
//!   told is durable, and the promise it made durable.
//! * D-57 A: a sequencer's compaction floor never passes the commit point,
//!   nor the slowest replica it serves unless the byte bound does; only a
//!   replica the byte bound passed may be lost.
//! * Refcount (§3): a catch-up never finds the segment it is reading gone.
//!
//! At the end the network heals and every node comes back; a sequencer must
//! emerge and every node not behind every log's floor must reach the whole
//! log, committed. One behind every floor needs a state copy (step 7b).
//!
//! 100 seeds by default (pins during compaction need that many);
//! `STRAUM_SIM_SEEDS=<n>` sweeps more; `STRAUM_SIM_SEED=<n>` runs one;
//! `STRAUM_SIM_STEPS=<n>` makes each run longer; `STRAUM_SIM_TRACE=1` prints
//! every node's slot, end and commit point every 20 000 events of a fault
//! cycle and every 2 000 rounds of healing — the trace a stall is diagnosed
//! from (D-85).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines
)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;

use cluster::{Config, Effect, LagBound, Message, Proposal, Voter};
use journal::{Chain, Kind, Verifier, decode};
use storage::simulated::SimDisk;
use storage::{Pin, SegmentLimits, Segments, WalLimits};

const NODES: u8 = 3;
const STEPS: usize = 1500;
/// Bytes one read of the log hands back at most, or one batch if that
/// alone is larger (D-81 A).
const READ_BYTES: usize = 4096;
/// Events a fault cycle is expected to recover within (D-81 A); reported,
/// not the liveness criterion (D-85).
const CYCLE_BOUND: usize = 2000;
/// Events, or healing rounds, without progress after which a scenario has
/// stalled: the eventual-liveness criterion (D-85).
const STALL_EVENTS: usize = 20_000;
const STALL_ROUNDS: usize = 2_000;
/// Events after the last progress within which losing the able majority
/// still counts as a state-copy case, not a stall (D-85).
const LOSS_GRACE: usize = 5_000;
/// However much progress it makes, a cycle that has not committed by then
/// has stalled — no allowance (D-90 B). Derived from what was observed, not
/// fitted to the seeds: with the cap at 50 000 000, every cycle of 10 000
/// seeds commits, the longest after a catch-up of 50 675 records (seed
/// 1 831; 602 635 events from its slot's start). At `CATCH_UP_COST` the cap
/// admits 208 000 records, 4.1 times that; a slower catch-up fails
/// `CATCH_UP_COST` first.
const STALL_CAP_EVENTS: usize = 5_000_000;
/// Events a catch-up may cost per record of the distance it covers, per
/// replica caught up — the sequencer serves its log one read at a time,
/// so two replicas caught up together take twice as long (D-90 B).
/// Measured over the 10 000-seed sweep, from the start of the slot that
/// commits to the commit: 24 catch-ups of 2 000 records or more cost
/// 10.8–13.0 (12 typical); shorter ones scatter up to 21.4, the takeover
/// itself weighing more than the rate, so they are not measured. The bound
/// is twice the typical cost: a catch-up at half speed fails the sweep.
const CATCH_UP_COST: f64 = 24.0;
const CATCH_UP_MEASURED: u64 = 2_000;
/// Heartbeats of silence before a takeover in the slow-replica test. At
/// the default 3, this simulation's random delivery takes the slot over
/// about every 300 events even with no fault — several times in a 3 000-
/// event window — and every new slot starts with all replicas detached
/// (R1), so flow control rarely reaches `T` (D-93 A, D-99).
const STEADY_TAKEOVER_TICKS: u32 = 30;
const STALL_CAP_ROUNDS: usize = 200_000;
/// The gate for seeds without an uncommitted bound, in percent (D-82 A).
const UNBOUNDED_FLOOR: u64 = 85;
/// Cycles a seed group needs before its gate is asserted (D-82 A).
const GATE_CYCLES: u64 = 300;
/// Every node keeps at most this much log behind its commit point for
/// nodes that are behind (D-57 A): about a segment and a half.
const RETAIN_BYTES: u64 = 1000;

/// `RETAIN_BYTES`, or `STRAUM_SIM_RETAIN_BYTES`: a realistic bound shows how
/// many fault cycles need a state copy once it is not tiny (D-87 A).
fn retain_bytes() -> u64 {
    std::env::var("STRAUM_SIM_RETAIN_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(RETAIN_BYTES)
}

fn seeds() -> std::ops::Range<u64> {
    match std::env::var("STRAUM_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(seed) => seed..seed + 1,
        None => {
            0..std::env::var("STRAUM_SIM_SEEDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100)
        }
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn directory() -> PathBuf {
    PathBuf::from("/node")
}

thread_local! {
    /// ADR-0009 (D-170 A): zeros written this far ahead of each segment
    /// (0: none), and power lost sector by sector in any order
    /// (`SimDisk::power_loss_sectors`) instead of a torn tail. Per test
    /// thread: the tests that set it run every seed with it.
    static ZEROS_AHEAD: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn limits() -> SegmentLimits {
    SegmentLimits {
        wal: WalLimits {
            preallocate_bytes: ZEROS_AHEAD.get(),
            ..WalLimits::default()
        },
        segment_bytes: 700,
    }
}

/// The uncommitted bound by seed group (D-82 A): `seed % 3` is 0 — small,
/// 4–16, so a sequencer holds batches back while a majority is away; 1 —
/// effectively none, for coverage; 2 — the default, which also bounds how
/// long a takeover takes.
fn config(id: u8, seed: u64) -> Config {
    Config {
        id,
        nodes: (0..NODES).collect(),
        owner: 0,
        slot: 1,
        window: 24,
        max_queue: 32,
        max_batch: 4,
        recent_batches: 4,
        max_uncommitted: match seed % 3 {
            0 => 4 + seed % 13,
            1 => 1_000_000,
            _ => cluster::DEFAULT_MAX_UNCOMMITTED,
        },
        takeover_ticks: 3,
        lag_bound: lag_bound(seed),
    }
}

/// Flow control on replica lag (D-89 A2) in half of each bounded group:
/// `(seed / 3) % 2 == 1`. It needs an uncommitted bound (ADR-0008, Lemma 2),
/// so never in the group without one. `L` from the floor the sequencer
/// raises it to (`max_uncommitted + max_batch`) up to 7 above — close to it, so
/// that holds happen, `T` 2–8
/// heartbeats.
fn lag_bound(seed: u64) -> Option<LagBound> {
    (seed % 3 != 1 && (seed / 3) % 2 == 1).then(|| LagBound {
        records: seed % 8,
        ticks: 2 + (seed % 7) as u32,
    })
}

struct Node {
    disk: SimDisk,
    log: Option<Segments<SimDisk>>,
    core: Option<Voter>,
    incarnation: u64,
    /// Batches the core handed over that are not written yet.
    unwritten: VecDeque<(u64, Vec<Vec<u8>>)>,
    /// Highest lsn the log was told to sync, and did.
    durable: u64,
    /// The promise as the disk holds it, and one asked and not yet synced.
    promised: u64,
    promising: Option<u64>,
    /// A cut asked and not yet done.
    truncate: Option<u64>,
    /// Records this node holds, by lsn: on disk or handed to the log.
    records: BTreeMap<u64, Vec<u8>>,
    /// Everything at or below is compacted away.
    floor: u64,
    /// The committed slot table through the floor, durable before the
    /// compaction that needs it: what the node's snapshot carries (D-73 A).
    snapshot_slots: Vec<(u64, u64)>,
    reads: VecDeque<(u8, u64, u64)>,
    /// Segments held for replicas this node catches up as sequencer.
    cursors: HashMap<u8, (u64, Pin)>,
}

#[derive(Default)]
struct Stats {
    committed: u64,
    takeovers: u64,
    truncations: u64,
    restarts: u64,
    power_losses: u64,
    torn: u64,
    reads: u64,
    pinned_during_compaction: u64,
    removed_segments: u64,
    compactions_during_takeover: u64,
    /// Flow control (D-89 A2): heartbeats a sequencer held new batches,
    /// and the stretches between batches in which it held any.
    held_ticks: u64,
    holds_ended: u64,
    /// Replicas R4 detached at the `T`-th held heartbeat (D-93 A).
    detached_at_t: u64,
    /// Each of them: (sequencer, replica, step, commit point then).
    detaches: Vec<(u8, u8, usize, u64)>,
    lost: u64,
    behind_every_floor: u64,
    dropped: u64,
    /// The longest snapshot slot table after a compaction pruned it: bounded
    /// by the slot changes in the retained log, not by the run (D-79 A).
    longest_table: usize,
    /// Fault cycles with a majority up throughout, those a later slot's
    /// commit followed within `CYCLE_BOUND` events, and cycles without a
    /// majority (not gated) (D-81 A).
    cycles: [u64; 3],
    cycles_taken: [u64; 3],
    cycles_without_majority: u64,
    /// Events from a majority being up to the next commit, per cycle; and
    /// cycles that did not commit within `STRAUM_SIM_EVENTUAL`: (seed, step).
    recovery: Vec<usize>,
    stalls: Vec<(u64, usize)>,
    /// Cycles that could not commit because no majority could catch up:
    /// a replica needs a state copy (step 7b). Classified, not recovered.
    needs_state_copy: Vec<(u64, usize)>,
    /// Of those, cut off from a node that, back, restored a majority able to
    /// catch up — and then committed (D-87).
    rejoined_recovered: u64,
    /// Cycles whose catch-up covered at least `CATCH_UP_MEASURED` records:
    /// (seed, distance, events, replicas caught up) (D-90 B).
    catch_ups: Vec<(u64, u64, usize, usize)>,
    /// Healing: the seed group, the highest patience when the network
    /// recovered, and the rounds until the first new commit (D-86 A, D-88).
    heal_after_patience: Vec<(u64, u32, usize)>,
}

/// The furthest a cycle or a healing has got: the highest commit point,
/// and each node's furthest log end while not sequencing (D-85).
#[derive(Default)]
struct Progress {
    commit: u64,
    ends: [u64; NODES as usize],
}

struct World {
    /// Bytes each log keeps behind its commit point (`RETAIN_BYTES`, or a
    /// realistic bound in its own test; D-87 A).
    retain: u64,
    seed: u64,
    step: usize,
    rng: Rng,
    nodes: Vec<Node>,
    network: Vec<(u8, u8, Message)>,
    isolated: Option<u8>,
    /// A fault cycle is running (D-81 A).
    cycling: bool,
    /// No faults: the log commits, and compaction gets something to remove
    /// while the nodes that fell behind are still being caught up.
    calm: bool,
    /// No rotation, and `STEADY_TAKEOVER_TICKS` of silence before a takeover:
    /// the slow-replica test watches one slot long enough for flow control
    /// to act (D-93 A).
    steady: bool,
    /// No proposals: the slow-replica test lets a replica catch up and be
    /// counted again (D-93 A).
    paused: bool,
    /// A replica whose disk is slow: 99 of its 100 writes are deferred —
    /// its promises with them, as `node`'s WAL thread syncs the appends
    /// before a promise (D-93 A; only the slow-replica test sets it).
    slow: Option<u8>,
    /// The committed log as it was when committed.
    oracle: BTreeMap<u64, Vec<u8>>,
    commit: u64,
    /// Per node, heartbeats flow control held with the sequencer's slot and
    /// end unchanged, and that slot and end (D-89 A2).
    held: [(u32, u64, u64); NODES as usize],
    /// Slot → the (node, incarnation) that sequenced it.
    leaders: BTreeMap<u64, (u8, u64)>,
    /// Slot k → how far its sequencer's log was durable on a majority when
    /// a majority's promises past k took effect.
    fences: BTreeMap<u64, u64>,
    /// Nodes a sequencer's byte bound compacted past: the only ones that
    /// may be lost (D-57 A).
    forced: Vec<u8>,
    /// Lost by a sequencer: until a state copy (step 7b) they wait.
    lost: Vec<u8>,
    /// The highest floor a compaction reached where the byte bound, not the
    /// commit point or a replica, set it: a node needing records below it
    /// may be lost (D-57 A), even after a cut shrank the log since.
    byte_bound_floor: u64,
    stats: Stats,
    next_body: u64,
}

impl World {
    fn fail(&self, what: &str) -> ! {
        panic!(
            "seed {} step {}: {what}\n(reproduce: STRAUM_SIM_SEED={} cargo test -p sim --test cluster)",
            self.seed, self.step, self.seed
        )
    }

    fn new(seed: u64) -> Self {
        Self::build(seed, false)
    }

    /// `steady`: the slow-replica test's world (D-93 A), set before the
    /// voters start.
    fn build(seed: u64, steady: bool) -> Self {
        let mut world = World {
            retain: retain_bytes(),
            seed,
            step: 0,
            rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1),
            nodes: (0..NODES)
                .map(|_| Node {
                    disk: SimDisk::new(),
                    log: None,
                    core: None,
                    incarnation: 0,
                    unwritten: VecDeque::new(),
                    durable: 0,
                    promised: 0,
                    promising: None,
                    truncate: None,
                    records: BTreeMap::new(),
                    floor: 0,
                    snapshot_slots: Vec::new(),
                    reads: VecDeque::new(),
                    cursors: HashMap::new(),
                })
                .collect(),
            network: Vec::new(),
            isolated: None,
            cycling: false,
            calm: false,
            slow: None,
            steady,
            paused: false,
            oracle: BTreeMap::new(),
            held: [(0, 0, 0); NODES as usize],
            commit: 0,
            leaders: BTreeMap::new(),
            fences: BTreeMap::new(),
            forced: Vec::new(),
            lost: Vec::new(),
            byte_bound_floor: 0,
            stats: Stats::default(),
            next_body: 0,
        };
        for id in 0..NODES {
            world.start(id);
        }
        world
    }

    fn up(&self, id: u8) -> bool {
        self.nodes[usize::from(id)].core.is_some()
    }

    fn leading(&self) -> Option<u8> {
        (0..NODES).find(|&id| {
            self.nodes[usize::from(id)]
                .core
                .as_ref()
                .is_some_and(Voter::leading)
        })
    }

    /// Opens a node's log, checks it, cuts a torn batch and starts a voter.
    fn start(&mut self, id: u8) {
        // Nor what was sent to it while it was down.
        self.network.retain(|(_, to, _)| *to != id);
        let node = &mut self.nodes[usize::from(id)];
        let mut recovered: Vec<Vec<u8>> = Vec::new();
        let opened = Segments::open(node.disk.clone(), &directory(), limits(), &mut |_, r| {
            recovered.push(r.to_vec())
        });
        let Ok(mut log) = opened else {
            self.fail(&format!(
                "node {id}: the log did not open: {:?}",
                opened.err()
            ));
        };
        // Invariant 5: one chain, from wherever compaction left the log.
        let start = match recovered.first().map(|r| decode(r)) {
            None => Chain::genesis(),
            // Slot 0: `prev_hash` binds the first batch; a torn one must not
            // leave its slot on an empty log (seed 4045, D-82; as `node`).
            Some(Ok(first)) => Chain::resume(first.prev_hash, first.lsn - 1, 0),
            Some(Err(e)) => self.fail(&format!("node {id}: undecodable record: {e}")),
        };
        let mut verifier = Verifier::new(start);
        for (at, record) in recovered.iter().enumerate() {
            if let Err(e) = verifier.feed(record) {
                self.fail(&format!("node {id}: invariant 5, record {at}: {e}"));
            }
        }
        let tail = verifier.finish();
        if tail.partial > 0 {
            self.stats.torn += 1;
            log.truncate_tail(tail.partial).unwrap();
        }
        let end = tail.chain.next_lsn() - 1;
        let node = &mut self.nodes[usize::from(id)];
        // Storage keeps its word.
        if end < node.durable {
            let durable = node.durable;
            self.fail(&format!(
                "node {id}: recovered through {end}, told durable through {durable}"
            ));
        }
        node.records = recovered
            .iter()
            .map(|r| (decode(r).unwrap().lsn, r.clone()))
            .filter(|(lsn, _)| *lsn <= end)
            .collect();
        // The snapshot's table, then the slots the log after it begins.
        let slots = cluster::merge_slots(&node.snapshot_slots, &log_slots(&node.records));
        node.floor = recovered
            .first()
            .map_or(end, |r| decode(r).unwrap().lsn - 1);
        // Reopening made what it recovered durable (D-43).
        node.durable = end;
        node.log = Some(log);
        node.incarnation += 1;
        node.promising = None;
        node.truncate = None;
        // What it compacted it had committed: the cut guard knows it from
        // the start, as the node's applied lsn tells its voter (found by
        // this simulation, seed 64: a restarted node blind to it was asked
        // to cut below its floor).
        let mut config = config(id, self.seed);
        if self.steady {
            config.takeover_ticks = STEADY_TAKEOVER_TICKS;
        }
        let voter = Voter::new(config, tail.chain, slots, node.promised)
            .knowing_commit(node.floor)
            .incarnation(node.incarnation);
        // Every fourth seed rotates (ADR-0008 step 6): `SlotEnd` and the
        // segments, compaction and cuts around it.
        node.core = Some(if self.seed % 4 == 1 && !self.steady {
            voter.rotate_every(4)
        } else {
            voter
        });
        self.drain(id);
    }

    /// Carries out what a voter asked for.
    fn drain(&mut self, id: u8) {
        loop {
            let effects = match self.nodes[usize::from(id)].core.as_mut() {
                Some(core) => core.take_effects(),
                None => return,
            };
            if effects.is_empty() {
                break;
            }
            for effect in effects {
                self.apply(id, effect);
            }
        }
        // Invariant 2, and a sequencer that stepped down holds no reader.
        let node = &mut self.nodes[usize::from(id)];
        let Some(core) = node.core.as_ref() else {
            return;
        };
        if core.leading() {
            let me = (id, node.incarnation);
            let slot = core.slot();
            match self.leaders.get(&slot) {
                Some(&other) if other != me => self.fail(&format!(
                    "slot {slot} has two sequencers: {other:?} and {me:?}"
                )),
                Some(_) => {}
                None => {
                    self.leaders.insert(slot, me);
                    self.stats.takeovers += 1;
                }
            }
        } else if let Some(log) = node.log.as_mut() {
            for (_, (_, pin)) in node.cursors.drain() {
                log.unpin(pin);
            }
        }
    }

    fn apply(&mut self, id: u8, effect: Effect) {
        match effect {
            Effect::Append { first, records } => {
                let node = &mut self.nodes[usize::from(id)];
                for record in &records {
                    node.records
                        .insert(decode(record).unwrap().lsn, record.clone());
                }
                node.unwritten.push_back((first, records));
            }
            Effect::Send { to, message } => self.network.push((id, to, message)),
            Effect::Promise { slot } => {
                let node = &mut self.nodes[usize::from(id)];
                node.promising = Some(node.promising.map_or(slot, |p| p.max(slot)));
            }
            Effect::Truncate { after } => {
                // Invariant 1: a cut takes only records the committed log
                // does not hold at their lsn.
                let node = &self.nodes[usize::from(id)];
                if let Some((lsn, _)) = node
                    .records
                    .range(after + 1..)
                    .find(|(lsn, record)| self.oracle.get(lsn) == Some(*record))
                {
                    self.fail(&format!(
                        "node {id}: asked to cut its log to {after}, removing its copy of committed lsn {lsn}"
                    ));
                }
                self.nodes[usize::from(id)].truncate = Some(after);
            }
            Effect::Read { to, from, upto } => {
                self.nodes[usize::from(id)]
                    .reads
                    .push_back((to, from, upto));
            }
            Effect::ReadDone { to } => {
                let node = &mut self.nodes[usize::from(id)];
                if let (Some((_, pin)), Some(log)) = (node.cursors.remove(&to), node.log.as_mut()) {
                    log.unpin(pin);
                }
            }
            Effect::Committed(lsn) => self.committed(id, lsn),
            Effect::Lost { to } => {
                // D-57 A: lost only past what the byte bound keeps — the
                // sequencer holds at least that much log (one segment of
                // slack: removal takes whole segments), or a compaction
                // passed the node by that bound before.
                let bounded = self.agrees(to) < self.byte_bound_floor;
                // No exemption for a replica whose last slot the compacted log
                // no longer holds: the snapshot's slot table places it
                // (D-73 A).
                if !bounded && !self.forced.contains(&to) {
                    let node = &self.nodes[usize::from(id)];
                    self.fail(&format!(
                        "node {to} lost by sequencer {id} within the retention bound: first segment {:?}, its end {:?} durable {}",
                        node.log.as_ref().and_then(|l| l.keys().next()),
                        self.nodes[usize::from(to)].core.as_ref().map(Voter::end),
                        self.nodes[usize::from(to)].durable,
                    ));
                }
                self.stats.lost += 1;
                if !self.lost.contains(&to) {
                    self.lost.push(to);
                }
                let node = &mut self.nodes[usize::from(id)];
                if let (Some((_, pin)), Some(log)) = (node.cursors.remove(&to), node.log.as_mut()) {
                    log.unpin(pin);
                }
            }
        }
    }

    /// Invariants 1, 3 and 6 at a node that learns a commit point.
    fn committed(&mut self, id: u8, lsn: u64) {
        let node = &self.nodes[usize::from(id)];
        let core = node.core.as_ref().unwrap();
        if lsn > self.commit {
            // The sequencer that reached it: this node, leading — or having
            // just stepped down, its last `Committed` handed over with the
            // rest of what it had not given out (`Voter` on a durable promise
            // past its slot). One sync can make both durable at once, so
            // judging by the role at hand-over failed a commit that was the
            // sequencer's (seed 4629). Then the slot is the committed
            // record's, and it must have been this node's, in this life.
            let me = (id, node.incarnation);
            let slot = if core.leading() {
                core.slot()
            } else {
                let slot = node
                    .records
                    .get(&lsn)
                    .map(|r| decode(r).unwrap().slot)
                    .filter(|slot| self.leaders.get(slot) == Some(&me));
                match slot {
                    Some(slot) => slot,
                    None => self.fail(&format!(
                        "node {id}: learned commit {lsn} before any sequencer reached it ({})",
                        self.commit
                    )),
                }
            };
            if let Some(&limit) = self.fences.get(&slot) {
                if lsn > limit {
                    self.fail(&format!(
                        "node {id}: sequencer of slot {slot} raised the commit point to {lsn}, past {limit}, what a majority held durably when a majority promised past it"
                    ));
                }
            }
            for at in self.commit + 1..=lsn {
                match node.records.get(&at) {
                    Some(record) => {
                        self.oracle.insert(at, record.clone());
                    }
                    None if at <= node.floor => {}
                    None => self.fail(&format!("committed {at}, the sequencer does not hold it")),
                }
            }
            self.commit = lsn;
            self.stats.committed = lsn;
        }
        self.check_against_oracle(id);
    }

    /// Invariants 1 (position) and 3: what a node holds up to the commit
    /// point it knows is the committed log. Above it a node may hold a tail
    /// of an older slot, which it cuts before it takes what follows.
    fn check_against_oracle(&self, id: u8) {
        let node = &self.nodes[usize::from(id)];
        let Some(core) = node.core.as_ref() else {
            return;
        };
        for (lsn, record) in node.records.range(..=core.commit()) {
            if self
                .oracle
                .get(lsn)
                .is_some_and(|committed| committed != record)
            {
                self.fail(&format!("node {id}: committed lsn {lsn} has other content"));
            }
        }
    }

    /// Invariant 1 (survival): a committed record is durable on a majority,
    /// counting a node that compacted it after commit.
    fn check_majority(&self) {
        let majority = usize::from(NODES) / 2 + 1;
        for (lsn, committed) in self.oracle.iter().rev().take(64) {
            let holding = self
                .nodes
                .iter()
                .filter(|n| {
                    *lsn <= n.floor || (*lsn <= n.durable && n.records.get(lsn) == Some(committed))
                })
                .count();
            if holding < majority {
                self.fail(&format!(
                    "committed lsn {lsn} is durable on {holding} nodes"
                ));
            }
        }
    }

    /// For every slot being sequenced that a majority has now promised past
    /// (in effect from `on_promised`), how far its log is durable on a
    /// majority.
    fn fence(&mut self) {
        let majority = usize::from(NODES) / 2 + 1;
        for leader in 0..usize::from(NODES) {
            let Some(core) = self.nodes[leader].core.as_ref().filter(|c| c.leading()) else {
                continue;
            };
            let slot = core.slot();
            let past = self
                .nodes
                .iter()
                .filter(|n| n.core.as_ref().map_or(n.promised, Voter::slot) > slot)
                .count();
            if past < majority || self.fences.contains_key(&slot) {
                continue;
            }
            let log = &self.nodes[leader].records;
            // Below its floor the sequencer compacted what was committed.
            let leader_floor = self.nodes[leader].floor;
            let mut held: Vec<u64> = self
                .nodes
                .iter()
                .map(|n| {
                    let mut at = n.floor;
                    if n.durable <= n.floor {
                        return at;
                    }
                    for (lsn, record) in n.records.range(n.floor + 1..=n.durable) {
                        if *lsn > leader_floor && log.get(lsn) != Some(record) {
                            break;
                        }
                        at = *lsn;
                    }
                    at
                })
                .collect();
            held.sort_unstable_by(|a, b| b.cmp(a));
            self.fences.insert(slot, held[majority - 1]);
        }
    }

    /// Writes what the voter handed over, then a cut it asked for; with
    /// `sync`, makes the log and the promise durable and reports both.
    fn write(&mut self, id: u8, sync: bool) {
        let node = &mut self.nodes[usize::from(id)];
        let Some(log) = node.log.as_mut() else { return };
        while let Some((first, records)) = node.unwritten.pop_front() {
            log.begin(first).unwrap();
            for record in &records {
                log.stage(record).unwrap();
            }
            log.commit(false).unwrap();
        }
        if let Some(after) = node.truncate.take() {
            // To the last batch boundary at or below `after`, as the node's
            // WAL thread cuts.
            let asked = after;
            let mut after = after;
            loop {
                let Some(next) = node.records.get(&(after + 1)) else {
                    let (floor, end) = (node.floor, node.records.keys().next_back().copied());
                    let commit = node.core.as_ref().map(Voter::commit);
                    self.fail(&format!(
                        "node {id}: cut asked at {asked}, no record at {} (floor {floor}, end {end:?}, commit {commit:?})",
                        after + 1
                    ));
                };
                if decode(next).unwrap().kind == Kind::BatchHeader {
                    break;
                }
                after -= 1;
            }
            // Invariant 1, on the cut as made.
            let removed = node
                .records
                .range(after + 1..)
                .find(|(lsn, record)| self.oracle.get(lsn) == Some(*record))
                .map(|(lsn, _)| *lsn);
            if let Some(lsn) = removed {
                self.fail(&format!(
                    "node {id}: cut to {after}, removing its copy of committed lsn {lsn}"
                ));
            }
            let node = &mut self.nodes[usize::from(id)];
            // The chain at the cut: the record after it names the hash, the
            // one at it (or, compacted, the committed log) the slot.
            let next = decode(&node.records[&(after + 1)]).unwrap();
            let slot = node
                .records
                .get(&after)
                .or(self.oracle.get(&after))
                .map_or(0, |r| decode(r).unwrap().slot);
            let chain = if after == 0 {
                Chain::genesis()
            } else {
                Chain::resume(next.prev_hash, after, slot)
            };
            let node = &mut self.nodes[usize::from(id)];
            node.log.as_mut().unwrap().truncate_after(after).unwrap();
            node.records.retain(|lsn, _| *lsn <= after);
            node.durable = node.durable.min(after);
            self.stats.truncations += 1;
            node.core.as_mut().unwrap().on_truncated(chain);
            self.drain(id);
        }
        if !sync {
            return;
        }
        let node = &mut self.nodes[usize::from(id)];
        let log = node.log.as_mut().unwrap();
        // No segment yet: nothing was ever written, nothing to sync.
        if log.keys().next().is_some() {
            log.commit(true).unwrap();
        }
        let end = node
            .records
            .keys()
            .next_back()
            .copied()
            .unwrap_or(node.floor);
        node.durable = node.durable.max(end);
        let durable = node.durable;
        let promise = node.promising.take();
        if let Some(slot) = promise {
            node.promised = node.promised.max(slot);
        }
        let core = node.core.as_mut().unwrap();
        core.on_durable(durable);
        if let Some(slot) = promise {
            core.on_promised(slot);
        }
        self.drain(id);
        self.fence();
    }

    fn crash(&mut self, id: u8, power: bool) {
        // Nothing addressed to the old process reaches its successor, as over
        // TCP (the transport requirement in `cluster::Message`). What it sent
        // before it died may still arrive at the others, late and reordered.
        self.network.retain(|(_, to, _)| *to != id);
        let node = &mut self.nodes[usize::from(id)];
        node.core = None;
        node.log = None;
        node.unwritten.clear();
        node.reads.clear();
        node.cursors.clear();
        node.promising = None;
        node.truncate = None;
        if power {
            // The same draw either way, so every seed keeps its scenario.
            let torn = self.rng.below(300);
            let node = &mut self.nodes[usize::from(id)];
            if ZEROS_AHEAD.get() > 0 {
                node.disk
                    .power_loss_sectors(torn.wrapping_mul(0x9e37_79b9_7f4a_7c15));
            } else {
                node.disk.power_loss(torn as usize);
            }
            self.stats.power_losses += 1;
        }
        // Until it restarts, it holds what it made durable.
        let node = &mut self.nodes[usize::from(id)];
        let durable = node.durable;
        node.records.retain(|lsn, _| *lsn <= durable);
    }

    fn deliver(&mut self, index: usize) {
        // `remove`, not `swap_remove`: delivering the first message must not
        // move the last ahead of all the others.
        let (from, to, message) = self.network.remove(index);
        if self.isolated.is_some_and(|i| i == from || i == to) || !self.up(to) {
            self.stats.dropped += 1;
            return;
        }
        if let Some(core) = self.nodes[usize::from(to)].core.as_mut() {
            core.on_message(from, message);
        }
        self.drain(to);
    }

    /// Reads the next batch a node was asked for: a replica it catches up as
    /// sequencer (held by a pin across reads), or a candidate's fetch.
    fn read(&mut self, id: u8) {
        let node = &mut self.nodes[usize::from(id)];
        let Some((to, from, upto)) = node.reads.pop_front() else {
            return;
        };
        let leading = node.core.as_ref().is_some_and(Voter::leading);
        let Some(log) = node.log.as_mut() else { return };
        self.stats.reads += 1;
        let Some(segment) = log.segment_of(from) else {
            if let Some((held, _)) = node.cursors.get(&to) {
                if *held <= from {
                    let held = *held;
                    self.fail(&format!(
                        "refcount: segment {held} was pinned and lsn {from} is gone"
                    ));
                }
            }
            node.core.as_mut().unwrap().on_read_missing(to, from);
            self.drain(id);
            return;
        };
        if leading {
            match node.cursors.get(&to) {
                Some((held, _)) if *held == segment => {}
                _ => {
                    if let Some((_, pin)) = node.cursors.remove(&to) {
                        log.unpin(pin);
                    }
                    let pin = log.pin(segment);
                    node.cursors.insert(to, (segment, pin));
                }
            }
        }
        // Whole batches from `from`, within the segment that holds it.
        let mut batches = cluster::ReadBatches::new(from, upto, READ_BYTES);
        log.read(segment, &mut |record| batches.feed(record))
            .unwrap();
        let core = node.core.as_mut().unwrap();
        match batches.finish() {
            Some(batch) => core.on_read(to, batch),
            // Unwritten yet, or not a batch's start here: as the node's WAL
            // thread, it answers that the log does not have it.
            None => core.on_read_missing(to, from),
        }
        self.drain(id);
    }

    /// Gives back segments below the floor the voter allows; while some
    /// node is taking a slot over, counted.
    fn compact(&mut self, id: u8) {
        let (seed, step) = (self.seed, self.step);
        let taking = self.leading().is_none();
        let node = &mut self.nodes[usize::from(id)];
        let Some(core) = node.core.as_ref() else {
            return;
        };
        let Some(log) = node.log.as_mut() else { return };
        // Pending writes and cuts would land in segments about to be judged.
        if !node.unwritten.is_empty() || node.truncate.is_some() {
            return;
        }
        let mut kept = 0u64;
        let mut bytes_floor = 0;
        for (key, bytes) in log.sizes().rev() {
            kept += bytes;
            if kept > self.retain {
                bytes_floor = key;
                break;
            }
        }
        let floor = core.compaction_floor(bytes_floor);
        if floor > core.commit() {
            panic!(
                "seed {seed} step {step}: node {id}: compaction floor {floor} past its commit point {}",
                core.commit()
            );
        }
        if core.leading() {
            // D-57 A, checked against its own terms rather than the
            // sequencer's answer: never past the slowest replica it serves
            // unless the byte bound is.
            let others: Vec<u8> = (0..NODES).filter(|&r| r != id).collect();
            let slowest = others.iter().filter_map(|&r| core.replica_durable(r)).min();
            let allowed = slowest.map_or(u64::MAX, |d| d.max(bytes_floor));
            assert!(
                floor <= allowed,
                "seed {seed} step {step}: compaction floor {floor} past the retention rule ({allowed}): slowest {slowest:?}, bytes floor {bytes_floor}"
            );
            for &replica in &others {
                if core
                    .replica_durable(replica)
                    .is_some_and(|durable| durable < bytes_floor.min(floor))
                    && !self.forced.contains(&replica)
                {
                    self.forced.push(replica);
                }
            }
        }
        // A follower's floor is its commit point less the byte bound's worth:
        // a node further behind it was passed by the byte bound, and may be
        // lost when this one leads.
        if !core.leading() {
            for other in (0..NODES).filter(|&r| r != id) {
                if self.nodes[usize::from(other)].durable < floor && !self.forced.contains(&other) {
                    self.forced.push(other);
                }
            }
        }
        let node = &mut self.nodes[usize::from(id)];
        let log = node.log.as_mut().unwrap();
        // What removal would take without pins: sealed segments, the last
        // one kept, whose successor starts at or below floor + 1.
        let keys: Vec<u64> = log.keys().collect();
        let covered = keys
            .windows(2)
            .take(keys.len().saturating_sub(2))
            .take_while(|pair| pair[1] <= floor + 1)
            .count();
        // The snapshot, with the slot table through the floor, is durable
        // before any segment goes (D-67 B, D-73 A).
        let known = cluster::merge_slots(&node.snapshot_slots, &log_slots(&node.records));
        node.snapshot_slots = known
            .into_iter()
            .filter(|&(_, first)| first <= floor)
            .collect();
        let removed = log.remove_through(floor).unwrap();
        self.stats.removed_segments += removed as u64;
        if removed < covered {
            self.stats.pinned_during_compaction += 1;
        }
        if removed > 0 && taking {
            self.stats.compactions_during_takeover += 1;
        }
        let first = log.keys().next().unwrap_or(0);
        let new_floor = first.saturating_sub(1).max(node.floor);
        if removed > 0 && bytes_floor > 0 && new_floor <= bytes_floor {
            self.byte_bound_floor = self.byte_bound_floor.max(new_floor);
        }
        node.floor = new_floor;
        node.records.retain(|lsn, _| *lsn > new_floor);
        // Every table forgets what compaction reached (D-79 A): the one the
        // next snapshot carries, and the voter's.
        cluster::prune_slots(&mut node.snapshot_slots, new_floor);
        self.stats.longest_table = self.stats.longest_table.max(node.snapshot_slots.len());
        if let Some(core) = node.core.as_mut() {
            core.on_compacted(new_floor);
        }
    }

    fn propose(&mut self) {
        let Some(id) = self.leading() else {
            return;
        };
        let core = self.nodes[usize::from(id)].core.as_mut().unwrap();
        for _ in 0..1 + self.rng.below(3) {
            self.next_body += 1;
            let body = format!("message {}", self.next_body).into_bytes();
            if core
                .propose(Proposal {
                    kind: Kind::Data,
                    properties: Vec::new(),
                    body,
                })
                .is_err()
            {
                break;
            }
        }
        if self.rng.chance(50) {
            core.flush().unwrap();
        }
        self.drain(id);
    }

    fn tick(&mut self) {
        for id in 0..NODES {
            if let Some(core) = self.nodes[usize::from(id)].core.as_mut() {
                let before = core.flow();
                let counted: Vec<u8> = (0..NODES)
                    .filter(|&p| core.counts(p) == Some(true))
                    .collect();
                core.tick();
                // D-89 (2): between two batches, flow control holds at most
                // `T` heartbeats in all — R4 detaches at the `T`-th, and by
                // Lemma 2 nothing holds again before the next batch. Seen
                // from outside: held heartbeats are counted while the
                // sequencer's slot and end stay where they were, whatever
                // ends a hold in between, and must stay below `T`.
                let at = (core.slot(), core.end());
                let held = &mut self.held[usize::from(id)];
                if let (Some((_, was)), Some((_, now))) = (before, core.flow()) {
                    let limit = lag_bound(self.seed).map_or(0, |b| b.ticks);
                    if now < was && held.0 + 1 == limit && (held.1, held.2) == at {
                        self.stats.detached_at_t += 1;
                        for &peer in &counted {
                            if core.counts(peer) == Some(false) {
                                self.stats.detaches.push((id, peer, self.step, self.commit));
                            }
                        }
                    }
                }
                match core.flow() {
                    Some((holding, _)) => {
                        if (held.1, held.2) != at {
                            if held.0 > 0 {
                                self.stats.holds_ended += 1;
                            }
                            *held = (0, at.0, at.1);
                        }
                        if holding {
                            held.0 += 1;
                            self.stats.held_ticks += 1;
                            let limit = lag_bound(self.seed).map_or(0, |b| b.ticks);
                            assert!(
                                held.0 < limit.max(1),
                                "seed {} step {}: node {id} held new batches {} heartbeats with no batch made, T = {limit}",
                                self.seed,
                                self.step,
                                held.0
                            );
                        }
                    }
                    None => {
                        if held.0 > 0 {
                            self.stats.holds_ended += 1;
                        }
                        *held = (0, 0, 0);
                    }
                }
                self.drain(id);
            }
        }
    }

    fn event(&mut self) {
        let id = self.rng.below(u64::from(NODES)) as u8;
        let mut roll = self.rng.below(100);
        if self.calm && matches!(roll, 70..=72 | 90..=91 | 97..) {
            roll = 40;
        }
        // A fault cycle keeps a majority up: no crash or partition of the
        // others while it runs; loss, reordering and the rest go on.
        if self.cycling && matches!(roll, 90..=91 | 97..) {
            roll = 40;
        }
        match roll {
            0..=24 => {
                if !self.paused {
                    self.propose();
                }
            }
            25..=39 => {
                if self.up(id) {
                    if self.slow == Some(id) && self.rng.chance(99) {
                        return;
                    }
                    let sync = self.rng.chance(60);
                    self.write(id, sync);
                }
            }
            40..=69 => {
                if !self.network.is_empty() {
                    let index = self.rng.below(self.network.len() as u64) as usize;
                    self.deliver(index);
                }
            }
            70..=72 => {
                if !self.network.is_empty() {
                    let index = self.rng.below(self.network.len() as u64) as usize;
                    self.network.swap_remove(index);
                    self.stats.dropped += 1;
                }
            }
            73..=77 => self.tick(),
            78..=85 => {
                if self.up(id) {
                    self.read(id);
                }
            }
            86..=89 => {
                if self.up(id) {
                    self.compact(id);
                }
            }
            90..=91 => {
                if self.up(id) {
                    let power = self.rng.chance(50);
                    self.crash(id, power);
                }
            }
            92..=96 => {
                if !self.up(id) {
                    self.stats.restarts += 1;
                    self.start(id);
                }
            }
            _ => {
                self.isolated = match self.isolated {
                    Some(_) => None,
                    None => Some(id),
                };
            }
        }
    }

    /// A fault cycle (D-80 B, D-81 A): the sequencer crashes or is cut off,
    /// then events run as usual until a later slot commits a record or
    /// `CYCLE_BOUND` events pass — except that nobody else crashes or is
    /// cut off meanwhile, so a majority stays up: without one no takeover
    /// can finish, and that would measure the environment, not the
    /// protocol. Loss, reordering, compaction and the old sequencer's
    /// restart go on.
    fn fault_cycle(&mut self) {
        let Some(leader) = self.leading() else {
            return;
        };
        let slot = self.nodes[usize::from(leader)]
            .core
            .as_ref()
            .map_or(0, Voter::slot);
        if self.rng.chance(50) {
            let power = self.rng.chance(50);
            self.crash(leader, power);
        } else {
            self.isolated = Some(leader);
        }
        // Progress is guaranteed from the first event at which a majority
        // is up and reachable (restarts go on; nothing else fails); events
        // are counted from there. `eventual` stands in for "eventually".
        let mut since: Option<usize> = None;
        // When the live nodes first held no majority able to catch up.
        let mut unable_at: Option<usize> = None;
        let mut taken = None;
        // The newest slot being sequenced: since when, and every node's end
        // then — where a catch-up to its first record begins (D-90 B).
        let mut sequencing = None;
        let mut best = Progress::default();
        self.progressed(self.isolated, &mut best);
        let mut last_progress = 0usize;
        self.cycling = true;
        for event in 0.. {
            self.event();
            self.check_majority();
            self.step += 1;
            self.track_slot(&mut sequencing, event);
            let reachable = (0..NODES)
                .filter(|&id| self.up(id) && self.isolated != Some(id))
                .count();
            if since.is_none() && reachable >= usize::from(NODES / 2 + 1) {
                since = Some(event);
            }
            if self
                .oracle
                .values()
                .next_back()
                .is_some_and(|r| decode(r).unwrap().slot > slot)
            {
                taken = Some(event - since.unwrap_or(event));
                self.catch_up_cost(event, sequencing.as_ref());
                break;
            }
            if event % 20_000 == 0 {
                self.trace(&format!(
                    "cycle event {event} from slot {slot} last {last_progress}"
                ));
            }
            if self.progressed(self.isolated, &mut best) || since.is_none() {
                last_progress = event;
            }
            if unable_at.is_none() && self.able() <= usize::from(NODES) / 2 {
                unable_at = Some(event);
            }
            // Progress that never ends in a commit is a stall too: creeping
            // catch-ups that keep being passed over (D-85, without patience).
            if event - last_progress > STALL_EVENTS {
                break;
            }
            // However much progress it makes, no commit by the cap is a
            // stall (D-90 B: no allowance).
            if event > STALL_CAP_EVENTS {
                break;
            }
        }
        self.cycling = false;
        let group = (self.seed % 3) as usize;
        self.stats.cycles[group] += 1;
        match taken {
            Some(events) => {
                self.stats.cycles_taken[group] += u64::from(events <= CYCLE_BOUND);
                self.stats.recovery.push(events);
            }
            // No majority that can catch up: a replica lost past the byte
            // bound needs a copy of the state (step 7b, not built; D-57 A).
            // Outside what this model guarantees until then, and counted
            // apart — never as recovered. Only when that came during a
            // takeover's ordinary course: within `LOSS_GRACE` events of the
            // last progress (above the 99th percentile of recovery, 2 933).
            // A replica passed by compaction once progress had been absent
            // longer was passed because of a stall, and is counted as one.
            // In the 10 000-seed sweep the latest such loss came 572 events
            // after the last progress.
            None if unable_at.is_some_and(|at| at <= last_progress + LOSS_GRACE) => {
                // Checked, not assumed (D-87): every node keeping the
                // majority from catching up was passed by the retention
                // bound — the byte bound, as D-57 A allows — and not by an
                // early deletion or a catch-up that went wrong. Any other is
                // a stall.
                let unexplained: Vec<u8> = (0..NODES)
                    .filter(|&id| self.up(id) && self.isolated != Some(id))
                    .filter(|&id| self.lost.contains(&id) || self.behind_every_floor(id))
                    .filter(|&id| {
                        !self.forced.contains(&id) && self.agrees(id) >= self.byte_bound_floor
                    })
                    .collect();
                if !unexplained.is_empty() {
                    self.stats.stalls.push((self.seed, self.step));
                    return;
                }
                self.stats.needs_state_copy.push((self.seed, self.step));
                // The grace decides nothing: once the node cut off comes
                // back, a cycle that has a majority able to catch up again
                // must commit like any other (D-87).
                if self.isolated.take().is_some() && self.able() > usize::from(NODES) / 2 {
                    if self.commits_after(slot) {
                        self.stats.rejoined_recovered += 1;
                    } else {
                        self.stats.stalls.push((self.seed, self.step));
                    }
                }
            }
            None => {
                if std::env::var_os("STRAUM_SIM_TRACE").is_some() {
                    self.dump("stall");
                }
                self.stats.stalls.push((self.seed, self.step));
            }
        }
    }

    /// Runs events, faults paused as in a cycle, until a slot after `slot`
    /// commits a record (true) — or progress stops for `STALL_EVENTS`, or
    /// `STALL_CAP_EVENTS` pass (false: a stall, D-90 B).
    fn commits_after(&mut self, slot: u64) -> bool {
        let mut best = Progress::default();
        self.progressed(self.isolated, &mut best);
        let mut last_progress = 0usize;
        self.cycling = true;
        let mut sequencing = None;
        let mut done = false;
        for event in 0.. {
            self.event();
            self.check_majority();
            self.step += 1;
            self.track_slot(&mut sequencing, event);
            if self
                .oracle
                .values()
                .next_back()
                .is_some_and(|r| decode(r).unwrap().slot > slot)
            {
                self.catch_up_cost(event, sequencing.as_ref());
                done = true;
                break;
            }
            if event % 20_000 == 0 {
                self.trace(&format!(
                    "rejoined event {event} from slot {slot} last {last_progress}"
                ));
            }
            if self.progressed(self.isolated, &mut best) {
                last_progress = event;
            }
            if event - last_progress > STALL_EVENTS || event > STALL_CAP_EVENTS {
                break;
            }
        }
        self.cycling = false;
        done
    }

    fn ends(&self) -> Vec<u64> {
        (0..NODES)
            .map(|id| {
                self.nodes[usize::from(id)]
                    .core
                    .as_ref()
                    .map_or(0, Voter::end)
            })
            .collect()
    }

    /// Follows the newest slot being sequenced: when it changes, the event
    /// and every node's end then.
    fn track_slot(&self, sequencing: &mut Option<(u64, usize, Vec<u64>)>, event: usize) {
        let newest = self
            .nodes
            .iter()
            .filter_map(|n| n.core.as_ref().filter(|v| v.leading()).map(Voter::slot))
            .max();
        if let Some(slot) = newest {
            if sequencing.as_ref().map(|s| s.0) != Some(slot) {
                *sequencing = Some((slot, event, self.ends()));
            }
        }
    }

    /// A cycle just committed at `event`. If the slot that committed was the
    /// one followed since it started, its catch-up is measured: the distance
    /// from the second-furthest end when the slot started (a majority of
    /// three needs two logs) to the slot's first lsn, which nothing moves
    /// once the slot exists, and the events from then to the commit. Kept
    /// when long enough to measure a rate (D-90 B).
    fn catch_up_cost(&mut self, event: usize, sequencing: Option<&(u64, usize, Vec<u64>)>) {
        let last = self
            .oracle
            .values()
            .next_back()
            .map_or(0, |r| decode(r).unwrap().slot);
        let Some((_, started, ends)) = sequencing.filter(|s| s.0 == last) else {
            return;
        };
        let mut sorted = ends.clone();
        sorted.sort_unstable();
        let from = sorted
            .get(sorted.len().saturating_sub(2))
            .copied()
            .unwrap_or(0);
        let target = self
            .oracle
            .iter()
            .find(|(_, r)| decode(r).unwrap().slot == last)
            .map_or(0, |(lsn, _)| *lsn);
        let distance = target.saturating_sub(from);
        // The sequencer serves its log one read at a time: replicas caught
        // up together share it. Counted: those that started behind the
        // slot and covered at least half the distance since.
        let now = self.ends();
        let replicas = ends
            .iter()
            .zip(&now)
            .filter(|&(&then, &now)| then < target && now >= then + distance / 2)
            .count()
            .max(1);
        if distance >= CATCH_UP_MEASURED {
            self.stats
                .catch_ups
                .push((self.seed, distance, event - started, replicas));
        }
    }

    /// One line per node: role, slot, end, commit (`STRAUM_SIM_TRACE=1`).
    fn trace(&self, what: &str) {
        if std::env::var_os("STRAUM_SIM_TRACE").is_none() {
            return;
        }
        let st: Vec<String> = self
            .nodes
            .iter()
            .map(|n| {
                n.core.as_ref().map_or("down".into(), |v| {
                    format!(
                        "lead={} slot={} end={} commit={}",
                        v.leading(),
                        v.slot(),
                        v.end(),
                        v.commit()
                    )
                })
            })
            .collect();
        eprintln!(
            "trace: {what} seed {} isolated {:?} network {}: {st:?}",
            self.seed,
            self.isolated,
            self.network.len()
        );
    }

    /// Every node's voter, as `Debug` prints it, with the driver's queues:
    /// what a stall is diagnosed from (`STRAUM_SIM_TRACE=1`).
    fn dump(&self, what: &str) {
        eprintln!(
            "trace: {what} seed {} step {} isolated {:?} lost {:?} commit {} network {}",
            self.seed,
            self.step,
            self.isolated,
            self.lost,
            self.commit,
            self.network.len()
        );
        for (id, n) in self.nodes.iter().enumerate() {
            match n.core.as_ref() {
                None => eprintln!("trace:  n{id} down"),
                Some(v) => {
                    let d: String = format!("{v:?}")
                        .chars()
                        .filter(|c| !c.is_whitespace())
                        .collect();
                    eprintln!(
                        "trace:  n{id} floor {} durable {} reads {:?} unwritten {} truncate {:?} promising {:?}\n        {}",
                        n.floor,
                        n.durable,
                        n.reads,
                        n.unwritten.len(),
                        n.truncate,
                        n.promising,
                        &d[..d.len().min(4000)]
                    );
                }
            }
        }
    }

    /// Nodes up, not cut off, and able to catch up: not lost nor behind
    /// every log's floor (a replica that needs a state copy, step 7b).
    fn able(&self) -> usize {
        (0..NODES)
            .filter(|&id| {
                self.up(id)
                    && self.isolated != Some(id)
                    && !self.lost.contains(&id)
                    && !self.behind_every_floor(id)
            })
            .count()
    }

    /// Whether anything moved toward a commit since `best`, which it then
    /// raises: the highest commit point among nodes up and not cut off
    /// (`except`), or a node not sequencing whose log got past the furthest
    /// it reached while not sequencing — a catch-up. A sequencer's own
    /// appends never count, nor does a change of sequencer, and candidates
    /// that fetch, cut and fetch again never pass their best (D-85). Summed
    /// or lowest ends both mistook a change of sequencer for a stall (seeds
    /// 160, 6376).
    fn progressed(&self, except: Option<u8>, best: &mut Progress) -> bool {
        let mut moved = false;
        for (id, n) in self.nodes.iter().enumerate() {
            let Some(v) = n.core.as_ref() else { continue };
            if except == Some(id as u8) {
                continue;
            }
            if v.commit() > best.commit {
                best.commit = v.commit();
                moved = true;
            }
            if !v.leading() && v.end() > best.ends[id] {
                best.ends[id] = v.end();
                moved = true;
            }
        }
        moved
    }

    /// Where a node's log stops matching the committed log: what it must be
    /// shipped from, once a divergent tail is cut.
    fn agrees(&self, id: u8) -> u64 {
        let node = &self.nodes[usize::from(id)];
        let mut agrees = node.floor;
        for (lsn, record) in node.records.range(node.floor + 1..) {
            if self
                .oracle
                .get(lsn)
                .is_some_and(|committed| committed != record)
            {
                break;
            }
            if !self.oracle.contains_key(lsn) && *lsn > self.commit {
                break;
            }
            agrees = *lsn;
        }
        agrees
    }

    /// A node whose log ends below every other node's compaction floor can
    /// be brought back only by a state copy (step 7b).
    fn behind_every_floor(&self, id: u8) -> bool {
        let end = self.nodes[usize::from(id)]
            .core
            .as_ref()
            .map_or(0, Voter::end);
        (0..NODES)
            .filter(|&other| other != id)
            .all(|other| end < self.nodes[usize::from(other)].floor)
    }

    /// Everything comes back and the network delivers everything: a
    /// sequencer emerges, a new record commits, and every node able to catch
    /// up commits through the commit point at that moment.
    fn heal(&mut self) {
        self.isolated = None;
        for id in 0..NODES {
            if !self.up(id) {
                self.start(id);
            }
        }
        let before = self.commit;
        // D-86 A: how patient each node is when the network recovers, and
        // how many rounds the first new commit then takes.
        let patience = self
            .nodes
            .iter()
            .filter_map(|n| n.core.as_ref().map(Voter::patience))
            .max()
            .unwrap_or(1);
        // Eventual liveness (D-85): healing goes on while it makes progress
        // and fails only once `STALL_ROUNDS` pass without any; the work it
        // may need grows with the uncommitted tail a takeover must copy.
        let mut best = Progress::default();
        self.progressed(None, &mut best);
        let mut last_progress = 0usize;
        let mut goal: Option<u64> = None;
        for round in 0.. {
            if std::env::var_os("STRAUM_SIM_TRACE").is_some() && round % 2_000 == 0 {
                let st: Vec<String> = self
                    .nodes
                    .iter()
                    .map(|n| {
                        n.core.as_ref().map_or("down".into(), |v| {
                            format!(
                                "lead={} slot={} end={} commit={}",
                                v.leading(),
                                v.slot(),
                                v.end(),
                                v.commit()
                            )
                        })
                    })
                    .collect();
                eprintln!("trace: heal seed {} round {round}: {st:?}", self.seed);
            }
            if self.progressed(None, &mut best) {
                last_progress = round;
            }
            if round - last_progress > STALL_ROUNDS || round > STALL_CAP_ROUNDS {
                break;
            }
            // One new commit is all healing needs; past it, proposals would
            // keep the end moving away from a follower still catching up.
            if round % 8 == 0 && self.commit <= before {
                self.propose();
            }
            self.tick();
            for id in 0..NODES {
                self.write(id, true);
            }
            for _ in 0..1000 {
                if self.network.is_empty() {
                    break;
                }
                self.deliver(0);
            }
            for id in 0..NODES {
                while !self.nodes[usize::from(id)].reads.is_empty() {
                    self.read(id);
                }
            }
            let Some(leader) = self.leading() else {
                continue;
            };
            let end = self.nodes[usize::from(leader)].core.as_ref().unwrap().end();
            // Behind every floor, lost, or passed by a byte bound and behind
            // the sequencer's floor (D-57 A): each needs a state copy, 7b.
            let leader_floor = self.nodes[usize::from(leader)].floor;
            let behind: Vec<u8> = (0..NODES)
                .filter(|&id| {
                    self.behind_every_floor(id)
                        || self.lost.contains(&id)
                        || (self.forced.contains(&id) && self.agrees(id) < leader_floor)
                })
                .collect();
            // Once a new commit happened, the goal is fixed: the cluster's
            // commit point then — not a log end, which a sequencer that has
            // not heard it was replaced may still claim (seed 16). Rotation
            // keeps writing `SlotEnd` and `SlotStart`, so a cluster that
            // rotates is never quiet at one end; it has settled once
            // everyone it can reach has committed through the goal.
            if self.commit > before && goal.is_none() {
                goal = Some(self.commit);
                self.stats
                    .heal_after_patience
                    .push((self.seed % 3, patience, round));
            }
            let settled = goal.is_some_and(|goal| {
                (0..NODES).filter(|id| !behind.contains(id)).all(|id| {
                    let core = self.nodes[usize::from(id)].core.as_ref().unwrap();
                    core.commit() >= goal
                })
            });
            // A majority must remain to commit with.
            let enough = usize::from(NODES) - behind.len() > usize::from(NODES) / 2;
            let quiet = (0..NODES).filter(|id| !behind.contains(id)).all(|id| {
                let core = self.nodes[usize::from(id)].core.as_ref().unwrap();
                core.end() == end && core.commit() == end
            }) && self.network.is_empty();
            if settled || (quiet && !enough) {
                self.stats.behind_every_floor += behind.len() as u64;
                return;
            }
        }
        let state: Vec<String> = self
            .nodes
            .iter()
            .map(|n| {
                let core = n.core.as_ref().unwrap();
                format!(
                    "leading {} slot {} end {} commit {} durable {} floor {} promised {} refused {}",
                    core.leading(),
                    core.slot(),
                    core.end(),
                    core.commit(),
                    n.durable,
                    n.floor,
                    n.promised,
                    core.refused(),
                )
            })
            .collect();
        for n in &self.nodes {
            let d: String = format!("{:?}", n.core.as_ref().unwrap())
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            if let Some(i) = d.find("peers") {
                eprintln!("{}", &d[i..d.len().min(i + 520)]);
            }
            if let Some(i) = d.find("slots:[") {
                eprintln!("  {}", &d[i..d.len().min(i + 160)]);
            }
            if let Some(i) = d.find("last_slot") {
                eprintln!("  {}", &d[i..d.len().min(i + 60)]);
            }
        }
        self.fail(&format!(
            "the cluster did not settle after healing: {state:?}, commit {}, forced {:?}, lost {}",
            self.commit, self.forced, self.stats.lost
        ));
    }
}

/// The slot table of `records`, ascending: where each slot begins in it.
fn log_slots(records: &BTreeMap<u64, Vec<u8>>) -> Vec<(u64, u64)> {
    let mut slots: Vec<(u64, u64)> = Vec::new();
    for record in records.values() {
        let r = decode(record).unwrap();
        if slots.last().is_none_or(|&(slot, _)| slot < r.slot) {
            slots.push((r.slot, r.lsn));
        }
    }
    slots
}

fn run(seed: u64) -> Stats {
    run_with_retain(seed, retain_bytes())
}

fn run_with_retain(seed: u64, retain: u64) -> Stats {
    let mut world = World::new(seed);
    world.retain = retain;
    // `STRAUM_SIM_STEPS=<n>`: longer runs, which with rotation cross
    // thousands of slots (D-79 A).
    let steps = std::env::var("STRAUM_SIM_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(STEPS);
    // Every seed group runs fault cycles (D-80 B, D-81 A, D-82 A).
    let cycles = true;
    while world.step < steps {
        if world.step % 100 == 0 {
            world.calm = !world.calm;
            if world.calm {
                world.isolated = None;
            }
        }
        if cycles && !world.calm && world.rng.chance(1) {
            world.fault_cycle();
        } else {
            world.event();
            world.check_majority();
            world.step += 1;
        }
    }
    world.step = steps;
    world.heal();
    world.check_majority();
    for id in 0..NODES {
        world.check_against_oracle(id);
    }
    world.stats
}

#[test]
fn three_voters_keep_one_committed_log_through_crashes_takeovers_and_compaction() {
    let mut total = Stats::default();
    // D-72: takeovers per hundred steps.
    let mut seeds_run = 0u64;
    for seed in seeds() {
        let stats = run(seed);
        seeds_run += 1;
        total.committed += stats.committed;
        total.takeovers += stats.takeovers;
        total.truncations += stats.truncations;
        total.restarts += stats.restarts;
        total.power_losses += stats.power_losses;
        total.torn += stats.torn;
        total.reads += stats.reads;
        total.pinned_during_compaction += stats.pinned_during_compaction;
        total.removed_segments += stats.removed_segments;
        total.compactions_during_takeover += stats.compactions_during_takeover;
        total.held_ticks += stats.held_ticks;
        total.holds_ended += stats.holds_ended;
        total.lost += stats.lost;
        total.behind_every_floor += stats.behind_every_floor;
        total.dropped += stats.dropped;
        total.longest_table = total.longest_table.max(stats.longest_table);
        for group in 0..3 {
            total.cycles[group] += stats.cycles[group];
            total.cycles_taken[group] += stats.cycles_taken[group];
        }
        total.cycles_without_majority += stats.cycles_without_majority;
        total.recovery.extend(stats.recovery);
        total.stalls.extend(stats.stalls);
        total.needs_state_copy.extend(stats.needs_state_copy);
        total.rejoined_recovered += stats.rejoined_recovered;
        total.catch_ups.extend(stats.catch_ups);
        total.heal_after_patience.extend(stats.heal_after_patience);
    }
    println!(
        "committed {} takeovers {} cuts {} restarts {} power losses {} torn {} reads from the log {} \
         compactions held by a pin {} segments removed {} compactions during a takeover {} \
         replicas lost {} behind every floor at the end {} messages dropped {} longest slot table {}",
        total.committed,
        total.takeovers,
        total.truncations,
        total.restarts,
        total.power_losses,
        total.torn,
        total.reads,
        total.pinned_during_compaction,
        total.removed_segments,
        total.compactions_during_takeover,
        total.lost,
        total.behind_every_floor,
        total.dropped,
        total.longest_table
    );
    let steps = std::env::var("STRAUM_SIM_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(STEPS) as f64;
    let rate = total.takeovers as f64 * 100.0 / (seeds_run as f64 * steps);
    total.recovery.sort_unstable();
    let at = |q: f64| {
        total
            .recovery
            .get(((total.recovery.len() as f64 - 1.0) * q) as usize)
            .copied()
            .unwrap_or(0)
    };
    println!(
        "fault cycles: {} recovered, {} stalled, {} need a state copy (7b; {} committed once the node cut off came back) {:?}; events to recover p50 {} p90 {} p99 {} max {}; stalls (seed, step): {:?}",
        total.recovery.len(),
        total.stalls.len(),
        total.needs_state_copy.len(),
        total.rejoined_recovered,
        &total.needs_state_copy[..total.needs_state_copy.len().min(10)],
        at(0.5),
        at(0.9),
        at(0.99),
        at(1.0),
        &total.stalls[..total.stalls.len().min(20)]
    );
    println!(
        "flow control (D-89 A2, half of each bounded group): {} heartbeats held new batches, in {} stretches between batches, none reaching T",
        total.held_ticks, total.holds_ended
    );
    // D-86 A: rounds to the first commit after healing, by seed group and
    // the highest patience at the moment the network recovered. By group:
    // without an uncommitted bound the first commit also waits for a
    // catch-up across the whole tail, which is not patience (D-88).
    let mut by_patience: BTreeMap<(u64, u32), Vec<usize>> = BTreeMap::new();
    for &(group, p, rounds) in &total.heal_after_patience {
        by_patience.entry((group, p)).or_default().push(rounds);
    }
    for ((group, p), rounds) in &mut by_patience {
        rounds.sort_unstable();
        let group = [
            "uncommitted bound 4-16",
            "no uncommitted bound",
            "uncommitted bound 64",
        ][*group as usize];
        println!(
            "healing, {group}, patience {p}x: {} runs, rounds to the first commit p50 {} max {}",
            rounds.len(),
            rounds[rounds.len() / 2],
            rounds[rounds.len() - 1]
        );
    }
    let pct = |g: usize| total.cycles_taken[g] as f64 * 100.0 / total.cycles[g].max(1) as f64;
    println!(
        "takeovers per hundred steps: {rate:.2}; fault cycles with a majority up, a later slot committed within {CYCLE_BOUND} events: uncommitted bound 4-16 {} of {} ({:.1}%), default {} {} of {} ({:.1}%), none {} of {} ({:.1}%); without a majority {}",
        total.cycles_taken[0],
        total.cycles[0],
        pct(0),
        cluster::DEFAULT_MAX_UNCOMMITTED,
        total.cycles_taken[2],
        total.cycles[2],
        pct(2),
        total.cycles_taken[1],
        total.cycles[1],
        pct(1),
        total.cycles_without_majority
    );
    // D-85: eventual liveness. Every fault cycle whose live nodes hold a
    // majority able to catch up commits again — no stall, in any run, a
    // single seed included. The ones without one are classified above.
    assert!(
        total.stalls.is_empty(),
        "fault cycles stalled with a majority able to make progress (seed, step): {:?}",
        total.stalls
    );
    // D-90 B: no cycle is let off for being slow; every catch-up is as fast
    // as measured, within twice.
    // Events a record, per replica caught up.
    let mut costs: Vec<(f64, u64, u64, usize, usize)> = total
        .catch_ups
        .iter()
        .map(|&(seed, distance, events, replicas)| {
            (
                events as f64 / (distance as f64 * replicas as f64),
                seed,
                distance,
                events,
                replicas,
            )
        })
        .collect();
    costs.sort_by(|a, b| b.0.total_cmp(&a.0));
    println!(
        "catch-ups of at least {CATCH_UP_MEASURED} records: {}, costliest (events a record per replica, seed, records, events, replicas): {:?}, cheapest {:?}",
        costs.len(),
        &costs[..costs.len().min(5)],
        costs.last()
    );
    if let Some(&(cost, seed, distance, events, _)) = costs.first() {
        assert!(
            cost <= CATCH_UP_COST,
            "seed {seed}: a catch-up of {distance} records took {events} events, {cost:.1} a record, over {CATCH_UP_COST}"
        );
    }
    // D-81 A, D-82 A: after a sequencer fault, with a majority up, a later
    // slot commits within the bound in at least 9 cycles in 10 — in each
    // group with a bound. Without one (coverage only) the gate is the
    // measured level less its spread (D-82 A, reviving D-81 B for that
    // group alone). Asserted from `GATE_CYCLES` cycles a group: the default
    // run's 30 or so are too few — one cycle moves the rate 3 points — so
    // the gate is the sweep's (`STRAUM_SIM_SEEDS=10000`, docs/testing.md).
    if std::env::var("STRAUM_SIM_SEED").is_err() {
        for (group, percent) in [(0, 90), (2, 90), (1, UNBOUNDED_FLOOR)] {
            if total.cycles[group] < GATE_CYCLES {
                continue;
            }
            assert!(
                total.cycles_taken[group] * 100 >= total.cycles[group] * percent,
                "liveness after a fault, seed group {group}: {} of {} cycles took over within {CYCLE_BOUND} events, below {percent}%",
                total.cycles_taken[group],
                total.cycles[group]
            );
        }
    }
    // Not vacuous: the sweep committed, took slots over, cut tails, crashed,
    // cut torn batches, caught replicas up from the log, and compacted around
    // a reader and during a takeover.
    if std::env::var("STRAUM_SIM_SEED").is_err() {
        assert!(total.committed > 0 && total.restarts > 0 && total.power_losses > 0);
        assert!(total.takeovers > 0 && total.truncations > 0);
        assert!(total.reads > 0 && total.removed_segments > 0 && total.torn > 0);
        // Flow control held, and let go (D-89 A2).
        assert!(total.held_ticks > 0 && total.holds_ended > 0);
        // Only the tiny default bound compacts early enough for these.
        if retain_bytes() == RETAIN_BYTES {
            assert!(total.pinned_during_compaction > 0 && total.compactions_during_takeover > 0);
        }
    }
}

/// Every seed that ever stalled or failed to heal while D-85 was found and
/// fixed, run on every `cargo test` (D-85):
/// 4, 79 — takeovers taking turns for ever under a fixed wait for silence;
/// 1, 15, 16 — healing checks that could not settle with a rotating
/// sequencer or a stale one's tail; 22, 7, 91 — slow or passed-over
/// takeovers while the read and wait faults were open; 160 — a catch-up
/// drowned in reads queued behind stale answers; 1384 — a sequencer's read
/// answered to an older fetch from the same replica;
/// 643 — healing that churned through thousands of slots without the
/// doubling wait; 4629 — a fetch for an older slot served across a promise,
/// which let a sequencer commit past invariant 6's fence (every invariant
/// is checked in `run`); 8956 — the one cycle past the old 500 000-event
/// cap before D-88 A, which now commits in 740 events (D-90 B).
#[test]
fn every_seed_that_stalled_recovers() {
    for seed in [1, 4, 7, 15, 16, 22, 79, 91, 160, 643, 1384, 4629, 8956] {
        let stats = run(seed);
        assert!(
            stats.stalls.is_empty(),
            "seed {seed}: fault cycles stalled: {:?}",
            stats.stalls
        );
    }
}

/// ADR-0009 (D-170 A): the main sweep's seeds and every check in `run`, with
/// zeros written 128 bytes ahead of each segment (segments of 700 bytes, so
/// extensions and seals happen often) and power lost sector by sector, in
/// any order, instead of a torn tail. `STRAUM_SIM_SEEDS` sweeps more.
#[test]
fn three_voters_keep_one_committed_log_with_zeros_ahead_and_sector_torn_power_loss() {
    ZEROS_AHEAD.set(128);
    let (mut committed, mut power_losses, mut seeds_run) = (0, 0, 0u64);
    for seed in seeds() {
        let stats = run(seed);
        assert!(
            stats.stalls.is_empty(),
            "seed {seed}: fault cycles stalled: {:?}",
            stats.stalls
        );
        committed += stats.committed;
        power_losses += stats.power_losses;
        seeds_run += 1;
    }
    // Not vacuous: records committed and power was lost, over the zeros.
    assert!(
        committed > 0 && power_losses > 0,
        "committed {committed}, power losses {power_losses}"
    );
    println!(
        "with zeros ahead: {seeds_run} seeds, committed {committed}, power losses {power_losses}"
    );
}

/// The cycles that outlasted the old 500 000-event cap, run to their end
/// (D-90 B). No uncommitted bound: a sequencer without a majority kept
/// appending, and takeovers during the catch-up moved the slot that
/// commits further on, so the catch-up covered 37 000–51 000 records — at
/// 12 events a record, 450 000–610 000 events from the slot's start. Each
/// must commit, at no more than `CATCH_UP_COST` a record a replica. About 100 s in release:
/// `cargo test --release -p sim --test cluster -- --ignored a_tail_outrunning`.
#[test]
#[ignore = "about 100 s in release"]
fn a_tail_outrunning_its_catch_up_commits_eventually() {
    for seed in [802, 1831, 4714] {
        let stats = run(seed);
        assert!(stats.stalls.is_empty(), "seed {seed}: {:?}", stats.stalls);
        assert!(
            stats
                .catch_ups
                .iter()
                .any(|&(_, distance, _, _)| distance > 30_000),
            "seed {seed}: no long catch-up any more: {:?}",
            stats.catch_ups
        );
        for &(_, distance, events, replicas) in &stats.catch_ups {
            assert!(
                events as f64 <= CATCH_UP_COST * (distance * replicas as u64) as f64,
                "seed {seed}: {distance} records for {replicas} replicas in {events} events"
            );
        }
    }
}

/// With a realistic retention bound no fault cycle needs a state copy and
/// none stalls: the cycles the default sweep classifies as needing one come
/// from its 1 000-byte bound (D-87 A — 0 of 18 524 cycles at 10 000 seeds
/// and 100 000 bytes, against 504 of 18 439 at 1 000).
#[test]
fn a_realistic_retention_bound_needs_no_state_copy() {
    for seed in 0..100 {
        let stats = run_with_retain(seed, 100_000);
        assert!(
            stats.stalls.is_empty() && stats.needs_state_copy.is_empty(),
            "seed {seed}: stalled {:?}, needs a state copy {:?}",
            stats.stalls,
            stats.needs_state_copy
        );
    }
}

/// What one slow-replica window did (D-93 A).
#[derive(Debug)]
struct SlowWindow {
    #[expect(dead_code, reason = "read through Debug, in the failure messages")]
    seed: u64,
    /// A takeover happened during it; none should, with `steady`.
    taken_over: bool,
    /// The commit point moved while the replica was slow: the majority of
    /// the sequencer and the other replica kept committing.
    progressed: bool,
    /// R4 detached the slow replica, by the settled sequencer, at the
    /// `T`-th held heartbeat during it; and the commit point moved after
    /// that moment, within the window — the majority of the sequencer and
    /// the other replica committing without it.
    detached_at_t: bool,
    progressed_after: bool,
    /// Takeovers started by silence during the slow window, by any node and
    /// by the slow replica (D-98).
    window_takeovers: u64,
    started_by_slow: u32,
    /// Of those, the ones it won: it led the slot it was taking (D-98).
    won_by_slow: u32,
    /// Takeovers by silence won during the window, by any node.
    won: u32,
    /// The largest lag behind the commit point during it, against `L`.
    #[expect(dead_code, reason = "read through Debug, in the failure messages")]
    lag_in_window: (u64, u64),
    /// After the disk was fast again, under the same load: the largest lag
    /// behind the commit point, the lag at the end, and whether the
    /// replica was still served (not lost).
    lag_under_load: (u64, u64),
    kept: bool,
    /// Then with proposals paused: events until the replica held the
    /// commit point and the sequencer counted it again (attached).
    rejoined: Option<usize>,
}

/// Four times: proposals paused until one sequencer counts both replicas
/// (attached); a replica's disk turns slow for `SLOW_EVENTS` events — 99
/// of its 100 writes are deferred, its promises with them, so it is
/// certainly more than `L` behind for `T` heartbeats — under the
/// usual load; then fast again, `LOAD_EVENTS` more under load; then
/// proposals paused until it is counted again. No crash, partition or
/// loss: the mechanism alone. A realistic retention bound (100 000 bytes).
fn run_slow(seed: u64, steady: bool, slowed: bool) -> (Stats, Vec<SlowWindow>) {
    const SLOW_EVENTS: usize = 3_000;
    const LOAD_EVENTS: usize = 10_000;
    const REJOIN_EVENTS: usize = 30_000;
    let mut world = World::build(seed, steady);
    world.retain = 100_000;
    world.calm = true;
    let mut windows = Vec::new();
    let steps = |world: &mut World, n: usize| {
        for _ in 0..n {
            world.event();
            world.check_majority();
            world.step += 1;
        }
    };
    // The sequencer, if one counts both replicas.
    let counting = |world: &World, replicas: usize| {
        let leaders: Vec<u8> = (0..NODES)
            .filter(|&id| {
                world.nodes[usize::from(id)]
                    .core
                    .as_ref()
                    .is_some_and(Voter::leading)
            })
            .collect();
        match leaders[..] {
            [leader] => world.nodes[usize::from(leader)]
                .core
                .as_ref()
                .and_then(Voter::flow)
                .is_some_and(|(_, attached)| attached == replicas)
                .then_some(leader),
            _ => None,
        }
    };
    for _ in 0..4 {
        world.paused = true;
        let mut settled = None;
        for _ in 0..REJOIN_EVENTS {
            steps(&mut world, 1);
            settled = counting(&world, 2);
            if settled.is_some() {
                break;
            }
        }
        world.paused = false;
        let Some(leader) = settled else {
            panic!("seed {seed}: never settled with proposals paused");
        };
        let slow = (leader + 1) % NODES;
        let (commit, detached, takeovers) = (
            world.commit,
            world.stats.detaches.len(),
            world.stats.takeovers,
        );
        let lag = |world: &World| {
            world
                .commit
                .saturating_sub(world.nodes[usize::from(slow)].durable)
        };
        let config = config(0, seed);
        let bound = lag_bound(seed).map_or(0, |b| {
            b.records
                .max(config.max_uncommitted + config.max_batch as u64)
        });
        world.slow = slowed.then_some(slow);
        let mut in_window = 0;
        // Takeovers started by silence (not handoffs), by any node and by
        // the slowed replica.
        let mut window_takeovers = 0;
        let mut started_by_slow = 0;
        let mut won_by_slow = 0;
        let mut won = 0;
        let mut taking = [None; NODES as usize];
        for _ in 0..SLOW_EVENTS {
            steps(&mut world, 1);
            in_window = in_window.max(lag(&world));
            for id in 0..NODES {
                let now = world.nodes[usize::from(id)]
                    .core
                    .as_ref()
                    .and_then(Voter::taking);
                if now.is_some() && now != taking[usize::from(id)] {
                    window_takeovers += 1;
                    started_by_slow += u32::from(id == slow);
                }
                // A takeover by silence won: the node leads the slot it was
                // taking — a sequencer deposed, or a dead one replaced.
                if taking[usize::from(id)].is_some()
                    && world.nodes[usize::from(id)]
                        .core
                        .as_ref()
                        .is_some_and(Voter::leading)
                {
                    won += 1;
                    won_by_slow += u32::from(id == slow);
                }
                taking[usize::from(id)] = now;
            }
        }
        world.slow = None;
        let progressed = world.commit > commit;
        // The slow replica, detached at `T` by the sequencer the window
        // settled on — and the commit point after that moment.
        let detach = world.stats.detaches[detached..]
            .iter()
            .find(|&&(by, peer, ..)| by == leader && peer == slow)
            .map(|&(.., commit)| commit);
        let detached_at_t = detach.is_some();
        let progressed_after = detach.is_some_and(|at| world.commit > at);
        let mut most = 0;
        for _ in 0..LOAD_EVENTS {
            steps(&mut world, 1);
            most = most.max(lag(&world));
        }
        let lag_under_load = (most, lag(&world));
        let kept = !world.lost.contains(&slow);
        world.paused = true;
        let mut rejoined = None;
        for event in 0..REJOIN_EVENTS {
            steps(&mut world, 1);
            if lag(&world) == 0 && counting(&world, 2).is_some() {
                rejoined = Some(event);
                break;
            }
        }
        world.paused = false;
        let taken_over = world.stats.takeovers > takeovers;
        windows.push(SlowWindow {
            seed,
            taken_over,
            progressed,
            detached_at_t,
            progressed_after,
            window_takeovers,
            started_by_slow,
            won_by_slow,
            won,
            lag_in_window: (in_window, bound),
            lag_under_load,
            kept,
            rejoined,
        });
    }
    world.heal();
    world.check_majority();
    for id in 0..NODES {
        world.check_against_oracle(id);
    }
    (world.stats, windows)
}

/// D-93 A: flow control where it must act. A replica is slowed until it
/// falls more than `L` behind: the sequencer holds new batches, at the
/// `T`-th held heartbeat R4 detaches it, and the majority of the sequencer
/// and the other replica keeps committing. Fast again under the same load,
/// it is caught up from the log but may not close the gap — the catch-up
/// races the appends, which flow control does not hold for a detached
/// replica (L2) — so what is checked there is that it stays served and its
/// lag is reported; with the load paused it catches up and is counted
/// again. Every safety invariant is checked after every event, as in every
/// run. Seeds with flow control on (half of each bounded group), 5 by
/// default (about 40 s in a debug build), `STRAUM_SIM_SLOW_SEEDS=200` for the sweep; `STRAUM_SIM_SEED`
/// runs one.
#[test]
fn a_slow_replica_is_detached_at_t_and_rejoins_while_the_majority_commits() {
    let count: usize = std::env::var("STRAUM_SIM_SLOW_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let one: Option<u64> = std::env::var("STRAUM_SIM_SEED")
        .ok()
        .and_then(|v| v.parse().ok());
    let mut windows = Vec::new();
    let mut held = 0;
    for seed in (0..)
        .filter(|&s| lag_bound(s).is_some() && one.is_none_or(|one| one == s))
        .take(if one.is_some() { 1 } else { count })
    {
        let (stats, w) = run_slow(seed, true, true);
        held += stats.held_ticks;
        windows.extend(w);
    }
    let detached = windows.iter().filter(|w| w.detached_at_t).count();
    let after = windows.iter().filter(|w| w.progressed_after).count();
    let mut most: Vec<u64> = windows.iter().map(|w| w.lag_under_load.0).collect();
    most.sort_unstable();
    let closed = windows.iter().filter(|w| w.lag_under_load.1 == 0).count();
    let mut rejoin: Vec<usize> = windows.iter().filter_map(|w| w.rejoined).collect();
    rejoin.sort_unstable();
    println!(
        "slow replica: {} windows, {detached} detached at T (the slow replica, by the settled sequencer), commit moved after the detach in {after}, {held} heartbeats held; under load after it: largest lag p50 {:?} max {:?} records, gap closed in {closed}; paused, rejoined after p50 {:?} max {:?} events",
        windows.len(),
        most.get(most.len() / 2),
        most.last(),
        rejoin.get(rejoin.len() / 2),
        rejoin.last()
    );
    let bad: Vec<&SlowWindow> = windows
        .iter()
        .filter(|w| {
            w.taken_over
                || !w.progressed
                || !w.detached_at_t
                || !w.progressed_after
                || !w.kept
                || w.rejoined.is_none()
        })
        .collect();
    assert!(bad.is_empty(), "{} windows: {bad:?}", bad.len());
}

/// D-98 A, at the standard timing (3 heartbeats, rotation where the seed
/// rotates): the slow-replica scenario against the same runs without a
/// slow disk. Takeovers during the windows, those the slow replica itself
/// started, and the commit point after each detach. 20 seeds by default,
/// `STRAUM_SIM_SLOW_SEEDS` for the sweep. The gate of D-98 A and D-101 A;
/// about 10 s in release and four minutes in a debug build, so run as
/// `cargo test --release -p sim --test cluster -- --ignored a_slow_disk`.
#[test]
#[ignore = "about 10 s in release: cargo test --release -p sim --test cluster -- --ignored a_slow_disk"]
fn a_slow_disk_adds_no_takeovers_at_the_standard_timing() {
    let count: usize = std::env::var("STRAUM_SIM_SLOW_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let mut totals = [(0u64, 0u32, 0usize, 0usize, 0usize, 0u32, 0u32); 2];
    for seed in (0..).filter(|&s| lag_bound(s).is_some()).take(count) {
        for (i, slowed) in [false, true].into_iter().enumerate() {
            let (_, windows) = run_slow(seed, false, slowed);
            for w in &windows {
                let t = &mut totals[i];
                t.0 += w.window_takeovers;
                t.1 += w.started_by_slow;
                t.2 += 1;
                t.3 += usize::from(w.detached_at_t);
                t.4 += usize::from(w.detached_at_t && w.progressed_after);
                t.5 += w.won_by_slow;
                t.6 += w.won;
            }
        }
    }
    for (label, t) in ["without a slow disk", "with one"].iter().zip(totals) {
        println!(
            "standard timing, {label}: {} windows, {} takeovers started by silence in them, {} won; {} started by the slowed replica, which won {}; {} detached at T, commit moved after {} of those",
            t.2, t.0, t.6, t.1, t.5, t.3, t.4
        );
    }
    let (base, slow) = (totals[0], totals[1]);
    // Back to the level without a slow disk: within 10%, the spread seen
    // between 20 and 100 seeds (+3%, −6%); before D-98 A it was +47%.
    assert!(
        slow.0 * 10 <= base.0 * 11,
        "a slow disk raised takeovers by silence from {} to {}",
        base.0,
        slow.0
    );
    // The slowed replica deposes no sequencer it would not have deposed
    // anyway.
    assert!(
        slow.5 <= base.5,
        "the slowed replica won {} takeovers, against {} without a slow disk",
        slow.5,
        base.5
    );
    // The majority committed after every detach.
    assert_eq!(slow.4, slow.3, "commit stopped after a detach");
}
