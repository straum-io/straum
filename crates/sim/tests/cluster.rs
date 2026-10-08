//! Three nodes, one sequencer, two replicas (ADR-0008 step 2): the `cluster`
//! cores over the segmented log on simulated disks, a network that loses,
//! delays and reorders messages, process crashes, power losses with torn
//! writes, partitions, and compaction while a replica is being caught up.
//!
//! Checked after every event (unified-stream §5):
//!
//! * **1** — a committed record never changes position, and stays on a
//!   majority: durable there, or compacted after it was committed.
//! * **3** — every node's records match the committed log wherever they
//!   overlap it; a replica's known commit point only covers its own log.
//! * **5** — every recovered log is one unbroken chain: lsn monotonic,
//!   `prev_hash` continuous, a torn batch only at the end.
//! * Storage keeps its word: a restart recovers at least what the node was
//!   told is durable.
//! * Refcount (§3): a catch-up never finds the segment it is reading gone.
//!
//! At the end the network heals and every node comes back; every replica
//! that was not lost must then hold the whole log, committed.
//!
//! `STRAUM_SIM_SEEDS=<n>` sweeps more seeds; `STRAUM_SIM_SEED=<n>` runs one.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    clippy::large_enum_variant
)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;

use cluster::{Config, Effect, Message, Proposal, Replica, Sequencer};
use journal::{Chain, Kind, Verifier, decode};
use storage::simulated::SimDisk;
use storage::{Pin, SegmentLimits, Segments, WalLimits};

const NODES: u8 = 3;
const SEQUENCER: u8 = 0;
const STEPS: usize = 1500;
/// The sequencer keeps at most this much log for replicas that are behind
/// (D-57 A): about seven segments.
const RETAIN_BYTES: u64 = 5000;

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
                .unwrap_or(30)
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

fn limits() -> SegmentLimits {
    SegmentLimits {
        wal: WalLimits::default(),
        segment_bytes: 700,
    }
}

/// One seed in three runs with a small uncommitted bound, so the sequencer
/// holds batches back while a majority is away; the others effectively
/// without one.
fn config(id: u8, seed: u64) -> Config {
    Config {
        id,
        nodes: (0..NODES).collect(),
        owner: SEQUENCER,
        slot: 1,
        window: 24,
        max_queue: 32,
        max_batch: 4,
        recent_batches: 4,
        max_uncommitted: if seed % 3 == 0 {
            4 + seed % 13
        } else {
            1_000_000
        },
        takeover_ticks: 3,
    }
}

enum Core {
    Sequencer(Sequencer),
    Replica(Replica),
}

impl Core {
    fn take_effects(&mut self) -> Vec<Effect> {
        match self {
            Core::Sequencer(s) => s.take_effects(),
            Core::Replica(r) => r.take_effects(),
        }
    }
    fn on_message(&mut self, from: u8, message: Message) {
        match self {
            Core::Sequencer(s) => s.on_message(from, message),
            Core::Replica(r) => r.on_message(from, message),
        }
    }
    fn on_durable(&mut self, lsn: u64) {
        match self {
            Core::Sequencer(s) => s.on_durable(lsn),
            Core::Replica(r) => r.on_durable(lsn),
        }
    }
    fn commit(&self) -> u64 {
        match self {
            Core::Sequencer(s) => s.commit(),
            Core::Replica(r) => r.commit(),
        }
    }
    fn end(&self) -> u64 {
        match self {
            Core::Sequencer(s) => s.end(),
            Core::Replica(r) => r.end(),
        }
    }
}

struct Node {
    disk: SimDisk,
    log: Option<Segments<SimDisk>>,
    core: Option<Core>,
    /// Batches the core handed over that are not written yet.
    unwritten: VecDeque<(u64, Vec<Vec<u8>>)>,
    /// Highest lsn the log was told to sync, and did.
    durable: u64,
    /// Records this node holds, by lsn: on disk or handed to the log.
    records: BTreeMap<u64, Vec<u8>>,
    /// Everything at or below is compacted away.
    floor: u64,
    reads: VecDeque<(u8, u64)>,
    cursors: HashMap<u8, (u64, Pin)>,
}

#[derive(Default)]
struct Stats {
    committed: u64,
    restarts: u64,
    power_losses: u64,
    torn: u64,
    reads: u64,
    pinned_during_compaction: u64,
    removed_segments: u64,
    lost: u64,
    dropped: u64,
}

struct World {
    seed: u64,
    step: usize,
    rng: Rng,
    nodes: Vec<Node>,
    network: Vec<(u8, u8, Message)>,
    isolated: Option<u8>,
    /// No faults: the log commits, and compaction gets something to remove
    /// while the replicas that fell behind are still being caught up.
    calm: bool,
    /// The committed log as it was when committed.
    oracle: BTreeMap<u64, Vec<u8>>,
    commit: u64,
    lost: Vec<u8>,
    /// Replicas the byte bound forced the sequencer's floor past: the only
    /// ones that may be lost (D-57 A).
    forced: Vec<u8>,
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
        let mut world = World {
            seed,
            step: 0,
            rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1),
            nodes: (0..NODES)
                .map(|_| Node {
                    disk: SimDisk::new(),
                    log: None,
                    core: None,
                    unwritten: VecDeque::new(),
                    durable: 0,
                    records: BTreeMap::new(),
                    floor: 0,
                    reads: VecDeque::new(),
                    cursors: HashMap::new(),
                })
                .collect(),
            network: Vec::new(),
            isolated: None,
            calm: false,
            oracle: BTreeMap::new(),
            commit: 0,
            lost: Vec::new(),
            forced: Vec::new(),
            stats: Stats::default(),
            next_body: 0,
        };
        for id in 0..NODES {
            world.start(id);
        }
        world
    }

    /// Opens a node's log, checks it, cuts a torn batch and starts its core.
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
            Some(Ok(first)) => Chain::resume(first.prev_hash, first.lsn - 1, first.slot),
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
        node.floor = recovered
            .first()
            .map_or(end, |r| decode(r).unwrap().lsn - 1);
        // Reopening made what it recovered durable (D-43).
        node.durable = end;
        node.log = Some(log);
        node.core = Some(if id == SEQUENCER {
            Core::Sequencer(Sequencer::new(config(id, self.seed), tail.chain))
        } else {
            Core::Replica(Replica::new(config(id, self.seed), tail.chain))
        });
        self.check_against_oracle(id);
        self.drain(id);
    }

    /// Carries out what a core asked for.
    fn drain(&mut self, id: u8) {
        loop {
            let effects = match self.nodes[usize::from(id)].core.as_mut() {
                Some(core) => core.take_effects(),
                None => return,
            };
            if effects.is_empty() {
                return;
            }
            for effect in effects {
                self.apply(id, effect);
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
                self.check_against_oracle(id);
            }
            Effect::Send { to, message } => self.network.push((id, to, message)),
            Effect::Promise { .. } | Effect::Truncate { .. } => {
                self.fail(&format!("node {id}: a fixed-owner core took a slot over"));
            }
            Effect::Read { to, from } => self.nodes[usize::from(id)].reads.push_back((to, from)),
            Effect::ReadDone { to } => {
                let node = &mut self.nodes[usize::from(id)];
                if let (Some((_, pin)), Some(log)) = (node.cursors.remove(&to), node.log.as_mut()) {
                    log.unpin(pin);
                }
            }
            Effect::Committed(lsn) => {
                if id == SEQUENCER {
                    self.freeze(lsn);
                } else if lsn > self.commit {
                    self.fail(&format!(
                        "node {id}: knows commit {lsn}, the sequencer committed {}",
                        self.commit
                    ));
                }
            }
            Effect::Lost { to } => {
                if !self.forced.contains(&to) {
                    let seq = &self.nodes[usize::from(SEQUENCER)];
                    let replica = &self.nodes[usize::from(to)];
                    self.fail(&format!(
                        "replica {to} lost within the retention bound: sequencer {:?}, first segment {:?}, replica end {:?} durable {}",
                        match seq.core.as_ref() { Some(Core::Sequencer(s)) => format!("{s:?}").chars().take(700).collect::<String>(), _ => String::new() },
                        seq.log.as_ref().and_then(|l| l.keys().next()),
                        replica.core.as_ref().map(Core::end),
                        replica.durable,
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

    /// The sequencer committed through `lsn`: the records are fixed now.
    fn freeze(&mut self, lsn: u64) {
        if lsn <= self.commit {
            return;
        }
        let sequencer = &self.nodes[usize::from(SEQUENCER)];
        for at in self.commit + 1..=lsn {
            match sequencer.records.get(&at) {
                Some(record) => {
                    self.oracle.insert(at, record.clone());
                }
                None if at <= sequencer.floor => {}
                None => self.fail(&format!("committed {at}, the sequencer does not hold it")),
            }
        }
        self.commit = lsn;
        self.stats.committed = lsn;
        for id in 0..NODES {
            self.check_against_oracle(id);
        }
    }

    /// Invariants 1 (position) and 3: whatever a node holds of the
    /// committed log is the committed log.
    fn check_against_oracle(&self, id: u8) {
        let node = &self.nodes[usize::from(id)];
        for (lsn, record) in node.records.range(..=self.commit) {
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
        for lsn in self.oracle.keys().rev().take(64) {
            let holding = self
                .nodes
                .iter()
                .filter(|n| *lsn <= n.floor || (*lsn <= n.durable && n.records.contains_key(lsn)))
                .count();
            if holding < majority {
                self.fail(&format!(
                    "committed lsn {lsn} is durable on {holding} nodes"
                ));
            }
        }
    }

    fn up(&self, id: u8) -> bool {
        self.nodes[usize::from(id)].core.is_some()
    }

    /// Writes what the core handed over; with `sync`, makes it durable.
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
        if sync {
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
            if let Some(core) = node.core.as_mut() {
                core.on_durable(durable);
            }
            self.drain(id);
        }
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
        if power {
            let torn = self.rng.below(300) as usize;
            let node = &mut self.nodes[usize::from(id)];
            node.disk.power_loss(torn);
            self.stats.power_losses += 1;
        }
        // Until it restarts, it holds what it made durable.
        let node = &mut self.nodes[usize::from(id)];
        let durable = node.durable;
        node.records.retain(|lsn, _| *lsn <= durable);
    }

    fn deliver(&mut self, index: usize) {
        let (from, to, message) = self.network.swap_remove(index);
        if self.isolated.is_some_and(|i| i == from || i == to) || !self.up(to) {
            self.stats.dropped += 1;
            return;
        }
        if let Some(core) = self.nodes[usize::from(to)].core.as_mut() {
            core.on_message(from, message);
        }
        self.drain(to);
    }

    /// Reads the next batch for a replica being caught up.
    fn read(&mut self) {
        let node = &mut self.nodes[usize::from(SEQUENCER)];
        let Some((to, from)) = node.reads.pop_front() else {
            return;
        };
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
            if let Some(Core::Sequencer(s)) = node.core.as_mut() {
                s.on_read_missing(to, from);
            }
            self.drain(SEQUENCER);
            return;
        };
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
        let mut batch = Vec::new();
        let mut remaining = None::<u32>;
        log.read(segment, &mut |record| {
            let Ok(decoded) = decode(record) else { return };
            match remaining {
                None if decoded.lsn == from && decoded.kind == Kind::BatchHeader => {
                    remaining = Some(journal::BatchHeader::decode(decoded.body).unwrap().records);
                    batch.push(record.to_vec());
                }
                Some(n) if n > 0 && decoded.lsn == from + batch.len() as u64 => {
                    remaining = Some(n - 1);
                    batch.push(record.to_vec());
                }
                _ => {}
            }
        })
        .unwrap();
        if remaining != Some(0) {
            self.fail(&format!(
                "lsn {from} is in segment {segment} but no whole batch starts there"
            ));
        }
        if let Some(Core::Sequencer(s)) = node.core.as_mut() {
            s.on_read(to, batch);
        }
        self.drain(SEQUENCER);
    }

    /// Gives back segments below a floor no higher than what the node knows
    /// is committed. Sometimes far ahead of the slowest replica.
    fn compact(&mut self, id: u8) {
        let (seed, step) = (self.seed, self.step);
        let node = &mut self.nodes[usize::from(id)];
        let Some(core) = node.core.as_ref() else {
            return;
        };
        let commit = core.commit();
        let Some(log) = node.log.as_mut() else { return };
        let floor = match core {
            // The sequencer's floor is its own decision (D-57 A); the driver
            // only knows the segment sizes the byte bound is measured on.
            Core::Sequencer(s) => {
                let mut kept = 0u64;
                let mut bytes_floor = 0;
                for (key, bytes) in log.sizes().collect::<Vec<_>>().into_iter().rev() {
                    kept += bytes;
                    if kept > RETAIN_BYTES {
                        bytes_floor = key;
                        break;
                    }
                }
                let floor = s.compaction_floor(bytes_floor);
                // D-57 A, checked against its own terms rather than the
                // sequencer's answer: never past the commit point, never past
                // the slowest replica it serves unless the byte bound is.
                let slowest = (1..NODES).filter_map(|r| s.replica_durable(r)).min();
                let allowed = s
                    .commit()
                    .min(slowest.map_or(u64::MAX, |d| d.max(bytes_floor)));
                assert!(
                    floor <= allowed,
                    "seed {seed} step {step}: compaction floor {floor} past the retention rule ({allowed}): commit {}, slowest {slowest:?}, bytes floor {bytes_floor}",
                    s.commit()
                );
                // Only the byte bound may take a replica's next record.
                for replica in 1..NODES {
                    if s.replica_durable(replica)
                        .is_some_and(|durable| durable < bytes_floor.min(floor))
                        && !self.forced.contains(&replica)
                    {
                        self.forced.push(replica);
                    }
                }
                floor
            }
            // Nobody reads from a replica's log yet: its own commit point.
            Core::Replica(_) => commit / 2 + self.rng.below(commit / 2 + 1),
        };
        // Pending writes would land in a segment we are about to judge.
        if !node.unwritten.is_empty() {
            return;
        }
        // What removal would take without pins: sealed segments, the last
        // one kept, whose successor starts at or below floor + 1.
        let keys: Vec<u64> = log.keys().collect();
        let covered = keys
            .windows(2)
            .take(keys.len().saturating_sub(2))
            .take_while(|pair| pair[1] <= floor + 1)
            .count();
        let removed = log.remove_through(floor).unwrap();
        self.stats.removed_segments += removed as u64;
        if removed < covered {
            self.stats.pinned_during_compaction += 1;
        }
        let first = log.keys().next().unwrap_or(0);
        let new_floor = first.saturating_sub(1).max(node.floor);
        node.floor = new_floor;
        node.records.retain(|lsn, _| *lsn > new_floor);
    }

    fn propose(&mut self) {
        let Some(Core::Sequencer(s)) = self.nodes[usize::from(SEQUENCER)].core.as_mut() else {
            return;
        };
        for _ in 0..1 + self.rng.below(3) {
            self.next_body += 1;
            let body = format!("message {}", self.next_body).into_bytes();
            if s.propose(Proposal {
                kind: Kind::Publish,
                properties: Vec::new(),
                body,
            })
            .is_err()
            {
                break;
            }
        }
        if self.rng.chance(50) {
            s.flush().unwrap();
        }
        self.drain(SEQUENCER);
    }

    fn event(&mut self) {
        let id = self.rng.below(u64::from(NODES)) as u8;
        let mut roll = self.rng.below(100);
        if self.calm && matches!(roll, 70..=72 | 90..=91 | 97..) {
            roll = 40;
        }
        match roll {
            0..=24 => self.propose(),
            25..=39 => {
                if self.up(id) {
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
            73..=77 => {
                if let Some(Core::Sequencer(s)) = self.nodes[usize::from(SEQUENCER)].core.as_mut() {
                    s.tick();
                    self.drain(SEQUENCER);
                }
            }
            78..=85 => self.read(),
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

    /// Everything comes back and the network delivers everything: every
    /// replica that was not lost ends with the whole log, committed.
    fn heal(&mut self) {
        self.isolated = None;
        for id in 0..NODES {
            if !self.up(id) {
                self.start(id);
            }
        }
        if let Some(Core::Sequencer(s)) = self.nodes[usize::from(SEQUENCER)].core.as_mut() {
            s.flush().unwrap();
        }
        // With replicas lost below the floor there may be no majority left to
        // commit with: then the commit point must simply stay where it is.
        let live = usize::from(NODES) - self.lost.len();
        let can_commit = live > usize::from(NODES) / 2;
        let before = self.commit;
        self.drain(SEQUENCER);
        for _ in 0..10_000 {
            if let Some(Core::Sequencer(s)) = self.nodes[usize::from(SEQUENCER)].core.as_mut() {
                s.tick();
            }
            self.drain(SEQUENCER);
            for id in 0..NODES {
                self.write(id, true);
            }
            for _ in 0..1000 {
                if self.network.is_empty() {
                    break;
                }
                self.deliver(0);
            }
            while !self.nodes[usize::from(SEQUENCER)].reads.is_empty() {
                self.read();
            }
            let end = self.nodes[usize::from(SEQUENCER)]
                .core
                .as_ref()
                .unwrap()
                .end();
            let settled = (0..NODES).filter(|id| !self.lost.contains(id)).all(|id| {
                let core = self.nodes[usize::from(id)].core.as_ref().unwrap();
                core.end() == end && core.commit() == end
            });
            if settled && self.network.is_empty() {
                return;
            }
        }
        if !can_commit {
            if self.commit != before {
                self.fail("committed without a majority of live voters");
            }
            return;
        }
        let state: Vec<String> = self
            .nodes
            .iter()
            .map(|n| {
                let core = n.core.as_ref().unwrap();
                format!(
                    "end {} commit {} durable {} floor {} unwritten {} refused {}",
                    core.end(),
                    core.commit(),
                    n.durable,
                    n.floor,
                    n.unwritten.len(),
                    match core {
                        Core::Replica(r) => r.refused(),
                        Core::Sequencer(_) => 0,
                    }
                )
            })
            .collect();
        self.fail(&format!(
            "the cluster did not settle after healing: {state:?}, lost {:?}, reads {:?}, sequencer {:?}",
            self.lost,
            self.nodes[0].reads,
            match self.nodes[0].core.as_ref() { Some(Core::Sequencer(s)) => format!("{s:?}").chars().take(1500).collect::<String>(), _ => String::new() }
        ));
    }
}

fn run(seed: u64) -> Stats {
    let mut world = World::new(seed);
    for step in 0..STEPS {
        world.step = step;
        if step % 100 == 0 {
            world.calm = !world.calm;
            if world.calm {
                world.isolated = None;
            }
        }
        world.event();
        world.check_majority();
    }
    world.step = STEPS;
    world.heal();
    world.check_majority();
    for id in 0..NODES {
        world.check_against_oracle(id);
    }
    world.stats
}

#[test]
fn three_nodes_keep_one_committed_log_through_crashes_partitions_and_compaction() {
    let mut total = Stats::default();
    for seed in seeds() {
        let stats = run(seed);
        total.committed += stats.committed;
        total.restarts += stats.restarts;
        total.power_losses += stats.power_losses;
        total.torn += stats.torn;
        total.reads += stats.reads;
        total.pinned_during_compaction += stats.pinned_during_compaction;
        total.removed_segments += stats.removed_segments;
        total.lost += stats.lost;
        total.dropped += stats.dropped;
    }
    println!(
        "committed {} restarts {} power losses {} torn {} reads from the log {} \
         compactions held by a pin {} segments removed {} replicas lost {} messages dropped {}",
        total.committed,
        total.restarts,
        total.power_losses,
        total.torn,
        total.reads,
        total.pinned_during_compaction,
        total.removed_segments,
        total.lost,
        total.dropped
    );
    // Not vacuous: the sweep committed, crashed, cut torn batches, caught
    // replicas up from the log, compacted around a reader, and lost a
    // replica that fell below the floor.
    if std::env::var("STRAUM_SIM_SEED").is_err() {
        assert!(total.committed > 0 && total.restarts > 0 && total.power_losses > 0);
        assert!(total.reads > 0 && total.removed_segments > 0 && total.torn > 0);
        assert!(total.pinned_during_compaction > 0 && total.lost > 0);
    }
}
