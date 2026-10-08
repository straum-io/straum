//! Ungraceful takeover (ADR-0008 step 5; unified-stream §2.4, §5): three
//! `cluster::Voter`s on in-memory logs, with crashes, power losses, a
//! promise made durable by a node that dies before it answers, partitions
//! that leave a sequencer writing alone, lost and reordered messages, and
//! log cuts and reads that a crash can fall between.
//!
//! Checked after every step:
//!
//! 1. a committed record never changes or moves: every node that learns a
//!    commit point holds the oracle's records up to it, and no cut goes
//!    below the commit point the cluster has reached;
//! 2. at most one sequencer per slot, across nodes and incarnations;
//! 3. committed prefixes agree (the oracle is one log for all nodes);
//! 5. every log a node restarts from is one chain;
//! 6. no sequencer of slot k raises the commit point once a majority has
//!    durably promised a slot above k.
//!
//! Each run ends healed: every node up, nothing lost, and a sequencer must
//! emerge, commit new records, and every node must reach the same log.
//!
//! `STRAUM_SIM_SEEDS=<n>` sweeps more seeds; `STRAUM_SIM_SEED=<n>` runs one;
//! `STRAUM_SIM_TRACE=1` prints the protocol's events.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::collections::{BTreeMap, VecDeque};

use cluster::{Config, Effect, Message, Proposal, Voter};
use journal::{BatchHeader, Chain, Kind, Verifier, decode};

const NODES: u8 = 3;
const STEPS: u64 = 3000;

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

fn config(id: u8) -> Config {
    Config {
        id,
        nodes: (0..NODES).collect(),
        owner: 0,
        slot: 1,
        window: 16,
        max_queue: 32,
        max_batch: 4,
        recent_batches: 4,
        max_uncommitted: 64,
        takeover_ticks: 3,
    }
}

struct Node {
    voter: Option<Voter>,
    incarnation: u64,
    /// Lsn i + 1 at index i: everything handed to the log, written.
    records: Vec<Vec<u8>>,
    durable: usize,
    /// The promise as the disk holds it, and one asked and not yet synced.
    promised: u64,
    promising: Option<u64>,
    /// Made durable and not yet reported: a crash here loses the report,
    /// not the durability.
    report_durable: bool,
    report_promise: Option<u64>,
    truncate: Option<u64>,
    reads: VecDeque<(u8, u64)>,
    /// The commit point up to which this node's log was checked.
    checked: u64,
}

#[derive(Default)]
struct Stats {
    committed: u64,
    takeovers: u64,
    truncations: u64,
    restarts: u64,
    power_losses: u64,
    promises_unanswered: u64,
    cut_below_commit: u64,
}

struct World {
    seed: u64,
    step: u64,
    rng: Rng,
    nodes: Vec<Node>,
    network: Vec<(u8, u8, Message)>,
    isolated: Option<u8>,
    calm: bool,
    /// The committed log: lsn → record.
    oracle: BTreeMap<u64, Vec<u8>>,
    commit: u64,
    /// Slot → the (node, incarnation) that sequenced it.
    leaders: BTreeMap<u64, (u8, u64)>,
    /// Slot k → how far its sequencer's log was durable on a majority when
    /// a majority's promises past k became durable: what slot k may still
    /// commit (invariant 6).
    fences: BTreeMap<u64, u64>,
    next_body: u64,
    stats: Stats,
}

impl World {
    fn fail(&self, what: &str) -> ! {
        panic!(
            "seed {} step {}: {what}\n(reproduce: STRAUM_SIM_SEED={} cargo test -p sim --test takeover)",
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
                    voter: None,
                    incarnation: 0,
                    records: Vec::new(),
                    durable: 0,
                    promised: 0,
                    promising: None,
                    report_durable: false,
                    report_promise: None,
                    truncate: None,
                    reads: VecDeque::new(),
                    checked: 0,
                })
                .collect(),
            network: Vec::new(),
            isolated: None,
            calm: false,
            oracle: BTreeMap::new(),
            commit: 0,
            leaders: BTreeMap::new(),
            fences: BTreeMap::new(),
            next_body: 0,
            stats: Stats::default(),
        };
        for id in 0..NODES {
            world.start(id);
        }
        world
    }

    fn up(&self, id: u8) -> bool {
        self.nodes[usize::from(id)].voter.is_some()
    }

    /// Recovery: the log verified as one chain (invariant 5), a torn batch
    /// cut, the slot table rebuilt, the durable promise read back.
    fn start(&mut self, id: u8) {
        self.network.retain(|(_, to, _)| *to != id);
        let node = &mut self.nodes[usize::from(id)];
        node.records.truncate(node.durable);
        let mut verifier = Verifier::new(Chain::genesis());
        let mut slots: Vec<(u64, u64)> = Vec::new();
        let mut broken = None;
        for record in &node.records {
            if let Err(e) = verifier.feed(record) {
                broken = Some(e);
                break;
            }
            let r = decode(record).unwrap();
            if slots.last().is_none_or(|&(slot, _)| slot < r.slot) {
                slots.push((r.slot, r.lsn));
            }
        }
        if let Some(e) = broken {
            self.fail(&format!("node {id}: its log is not one chain: {e}"));
        }
        let node = &mut self.nodes[usize::from(id)];
        let tail = verifier.finish();
        let end = usize::try_from(tail.chain.next_lsn() - 1).unwrap();
        node.records.truncate(end);
        node.durable = end;
        slots.retain(|&(_, first)| first <= end as u64);
        node.incarnation += 1;
        node.promising = None;
        node.report_durable = false;
        node.report_promise = None;
        node.truncate = None;
        node.reads.clear();
        node.checked = 0;
        node.voter = Some(Voter::new(config(id), tail.chain, slots, node.promised));
    }

    fn crash(&mut self, id: u8, power: bool) {
        self.network.retain(|(_, to, _)| *to != id);
        let node = &mut self.nodes[usize::from(id)];
        if node.report_promise.is_some() {
            self.stats.promises_unanswered += 1;
        }
        node.voter = None;
        if power {
            node.records.truncate(node.durable);
            self.stats.power_losses += 1;
        }
    }

    fn drain(&mut self, id: u8) {
        loop {
            let Some(voter) = self.nodes[usize::from(id)].voter.as_mut() else {
                return;
            };
            let effects = voter.take_effects();
            if effects.is_empty() {
                break;
            }
            for effect in effects {
                self.effect(id, effect);
            }
        }
        // Invariant 2: one sequencer per slot.
        let node = &self.nodes[usize::from(id)];
        if let Some(voter) = node.voter.as_ref().filter(|v| v.leading()) {
            let me = (id, node.incarnation);
            let slot = voter.slot();
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
        }
    }

    fn effect(&mut self, id: u8, effect: Effect) {
        // `STRAUM_SIM_TRACE=1`: the protocol's events, without the steady
        // stream of appends and acks between them.
        if std::env::var_os("STRAUM_SIM_TRACE").is_some() {
            match &effect {
                Effect::Send {
                    message: Message::Append { .. } | Message::Ack { .. },
                    ..
                } => {}
                Effect::Send { to, message } => eprintln!("{} n{id}->{to} {message:?}", self.step),
                Effect::Read { .. } | Effect::ReadDone { .. } => {}
                Effect::Append { first, records } => {
                    let slots: Vec<u64> = records.iter().map(|r| decode(r).unwrap().slot).collect();
                    eprintln!("{} n{id} append {first} slots {slots:?}", self.step);
                }
                other => eprintln!("{} n{id} {other:?}", self.step),
            }
        }
        match effect {
            Effect::Append { first, records } => {
                let node = &mut self.nodes[usize::from(id)];
                if first != node.records.len() as u64 + 1 {
                    let have = node.records.len();
                    self.fail(&format!(
                        "node {id}: append at {first}, its log ends at {have}"
                    ));
                }
                node.records.extend(records);
            }
            Effect::Send { to, message } => self.network.push((id, to, message)),
            Effect::Read { to, from } => self.nodes[usize::from(id)].reads.push_back((to, from)),
            Effect::Committed(lsn) => self.committed(id, lsn),
            Effect::Promise { slot } => {
                let node = &mut self.nodes[usize::from(id)];
                node.promising = Some(node.promising.map_or(slot, |p| p.max(slot)));
            }
            Effect::Truncate { after } => {
                // Invariant 1: a cut removes only records the committed log
                // does not hold at their lsn — a tail from an older slot —
                // never a node's copy of a committed record.
                let node = &self.nodes[usize::from(id)];
                let cut = node
                    .records
                    .iter()
                    .enumerate()
                    .skip(usize::try_from(after).unwrap())
                    .find(|&(index, record)| self.oracle.get(&(index as u64 + 1)) == Some(record));
                if let Some((index, _)) = cut {
                    for (n, node) in self.nodes.iter().enumerate() {
                        let log: Vec<(u64, u64, Kind)> = node
                            .records
                            .iter()
                            .take(12)
                            .map(|r| {
                                let r = decode(r).unwrap();
                                (r.lsn, r.slot, r.kind)
                            })
                            .collect();
                        let v = node
                            .voter
                            .as_ref()
                            .map(|v| (v.leading(), v.slot(), v.commit(), v.end()));
                        eprintln!("node {n} {v:?} promised {}: {log:?}", node.promised);
                    }
                    self.fail(&format!(
                        "node {id}: asked to cut its log to {after}, removing its copy of committed lsn {}",
                        index + 1
                    ));
                }
                if after < self.commit {
                    self.stats.cut_below_commit += 1;
                }
                self.nodes[usize::from(id)].truncate = Some(after);
            }
            Effect::ReadDone { .. } | Effect::Lost { .. } => {}
        }
    }

    /// Invariants 1, 3 and 6 at a node that learns a commit point.
    fn committed(&mut self, id: u8, lsn: u64) {
        let node = &self.nodes[usize::from(id)];
        if lsn as usize > node.records.len() {
            self.fail(&format!(
                "node {id}: commit {lsn} beyond its own log ({})",
                node.records.len()
            ));
        }
        if lsn > self.commit {
            let voter = node.voter.as_ref().unwrap();
            if !voter.leading() {
                self.fail(&format!(
                    "node {id}: learned commit {lsn} before any sequencer reached it ({})",
                    self.commit
                ));
            }
            // Invariant 6: nothing becomes durable on a majority in slot k
            // once a majority has promised past it. An ack sent before the
            // promises may still arrive, and reports durability from then.
            let slot = voter.slot();
            if let Some(&limit) = self.fences.get(&slot) {
                if lsn > limit {
                    self.fail(&format!(
                        "node {id}: sequencer of slot {slot} raised the commit point to {lsn}, past {limit}, what a majority held durably when a majority promised past it"
                    ));
                }
            }
            for at in self.commit + 1..=lsn {
                let record = node.records[usize::try_from(at - 1).unwrap()].clone();
                self.oracle.insert(at, record);
            }
            self.commit = lsn;
            self.stats.committed = lsn;
        }
        let node = &self.nodes[usize::from(id)];
        for at in node.checked + 1..=lsn {
            if node.records[usize::try_from(at - 1).unwrap()] != self.oracle[&at] {
                self.fail(&format!(
                    "node {id}: committed lsn {at} differs from the committed log"
                ));
            }
        }
        let node = &mut self.nodes[usize::from(id)];
        node.checked = node.checked.max(lsn);
    }

    fn deliver(&mut self, index: usize) {
        let (from, to, message) = self.network.remove(index);
        if self.isolated.is_some_and(|i| i == from || i == to) {
            return;
        }
        if let Some(voter) = self.nodes[usize::from(to)].voter.as_mut() {
            voter.on_message(from, message);
            self.drain(to);
        }
    }

    /// The disk syncs: the log and any promise asked become durable; their
    /// reports follow as a separate event.
    fn sync(&mut self, id: u8) {
        let node = &mut self.nodes[usize::from(id)];
        if node.durable < node.records.len() {
            node.durable = node.records.len();
            node.report_durable = true;
        }
        if let Some(slot) = node.promising.take() {
            node.promised = node.promised.max(slot);
            node.report_promise = Some(slot);
        }
    }

    /// For every slot being sequenced that a majority has now promised
    /// past, how far its log is durable on a majority. A promise is in
    /// effect from `on_promised` — durable, and acted on: until then the
    /// node still follows the older slot, and the answer it then sends
    /// carries what it took meanwhile.
    fn fence(&mut self) {
        let majority = usize::from(NODES) / 2 + 1;
        for leader in 0..usize::from(NODES) {
            let Some(voter) = self.nodes[leader].voter.as_ref().filter(|v| v.leading()) else {
                continue;
            };
            let slot = voter.slot();
            let past = self
                .nodes
                .iter()
                .filter(|n| n.voter.as_ref().map_or(n.promised, Voter::slot) > slot)
                .count();
            if past < majority || self.fences.contains_key(&slot) {
                continue;
            }
            let log = &self.nodes[leader].records;
            let mut held: Vec<usize> = self
                .nodes
                .iter()
                .map(|n| {
                    n.records[..n.durable]
                        .iter()
                        .zip(log)
                        .take_while(|(a, b)| a == b)
                        .count()
                })
                .collect();
            held.sort_unstable_by(|a, b| b.cmp(a));
            self.fences.insert(slot, held[majority - 1] as u64);
        }
    }

    fn report(&mut self, id: u8) {
        let node = &mut self.nodes[usize::from(id)];
        let durable = std::mem::take(&mut node.report_durable).then_some(node.durable as u64);
        let promise = node.report_promise.take();
        let voter = node.voter.as_mut().unwrap();
        if let Some(lsn) = durable {
            voter.on_durable(lsn);
        }
        if let Some(slot) = promise {
            voter.on_promised(slot);
        }
        self.drain(id);
        self.fence();
    }

    fn truncate(&mut self, id: u8) {
        let node = &mut self.nodes[usize::from(id)];
        let Some(after) = node.truncate.take() else {
            return;
        };
        let after_index = usize::try_from(after).unwrap();
        let chain = if after == 0 {
            Chain::genesis()
        } else {
            let next = decode(&node.records[after_index]).unwrap();
            let last = decode(&node.records[after_index - 1]).unwrap();
            Chain::resume(next.prev_hash, after, last.slot)
        };
        node.records.truncate(after_index);
        node.durable = node.durable.min(after_index);
        self.stats.truncations += 1;
        node.voter.as_mut().unwrap().on_truncated(chain);
        self.drain(id);
    }

    fn read(&mut self, id: u8) {
        let node = &mut self.nodes[usize::from(id)];
        let Some((to, from)) = node.reads.pop_front() else {
            return;
        };
        let start = usize::try_from(from - 1).unwrap();
        let batch = node.records.get(start).and_then(|header| {
            let header = decode(header).ok()?;
            if header.kind != Kind::BatchHeader {
                return None;
            }
            let count = usize::try_from(BatchHeader::decode(header.body).ok()?.records).ok()?;
            node.records.get(start..=start + count).map(<[_]>::to_vec)
        });
        let voter = node.voter.as_mut().unwrap();
        match batch {
            Some(batch) => voter.on_read(to, batch),
            None => voter.on_read_missing(to, from),
        }
        self.drain(id);
    }

    fn propose(&mut self) {
        for id in 0..NODES {
            let Some(voter) = self.nodes[usize::from(id)].voter.as_mut() else {
                continue;
            };
            if !voter.leading() {
                continue;
            }
            self.next_body += 1;
            let proposal = Proposal {
                kind: Kind::Publish,
                properties: Vec::new(),
                body: self.next_body.to_be_bytes().to_vec(),
            };
            let _ = voter.propose(proposal);
            let _ = voter.flush();
            self.drain(id);
        }
    }

    fn tick(&mut self) {
        for id in 0..NODES {
            if let Some(voter) = self.nodes[usize::from(id)].voter.as_mut() {
                voter.tick();
                self.drain(id);
            }
        }
    }

    fn event(&mut self) {
        let id = self.rng.below(u64::from(NODES)) as u8;
        let mut roll = self.rng.below(100);
        if self.calm && matches!(roll, 60..=64 | 70..=73 | 74..=76) {
            roll = 30;
        }
        match roll {
            0..=9 => self.propose(),
            10..=19 => self.tick(),
            20..=49 => {
                if !self.network.is_empty() {
                    let index = self.rng.below(self.network.len() as u64) as usize;
                    self.deliver(index);
                }
            }
            50..=59 => {
                if self.up(id) {
                    self.sync(id);
                }
            }
            60..=64 => {
                if !self.network.is_empty() {
                    let index = self.rng.below(self.network.len() as u64) as usize;
                    self.network.swap_remove(index);
                }
            }
            65..=69 => {
                if self.up(id) {
                    self.report(id);
                }
            }
            70..=71 => {
                if self.up(id) {
                    let power = self.rng.chance(50);
                    self.crash(id, power);
                }
            }
            72..=73 => {
                self.isolated = if self.isolated.is_some() {
                    None
                } else {
                    Some(id)
                };
            }
            74..=76 => {
                if !self.up(id) {
                    self.stats.restarts += 1;
                    self.start(id);
                }
            }
            77..=86 => {
                if self.up(id) {
                    self.truncate(id);
                }
            }
            _ => {
                if self.up(id) {
                    self.read(id);
                }
            }
        }
    }

    /// Every node up, nothing lost or reordered: a sequencer must emerge and
    /// commit, and every node must reach the same log.
    fn heal(&mut self) {
        self.isolated = None;
        for id in 0..NODES {
            if !self.up(id) {
                self.start(id);
            }
        }
        let goal = self.commit + 1;
        for round in 0..400 {
            if round % 4 == 0 {
                self.propose();
            }
            self.tick();
            for _ in 0..4 {
                // Bounded: a protocol that answers every message with another
                // must still make progress, and a stall shows as a failure.
                let mut budget = 2000;
                while !self.network.is_empty() && budget > 0 {
                    self.deliver(0);
                    budget -= 1;
                }
                for id in 0..NODES {
                    self.sync(id);
                    self.report(id);
                    self.truncate(id);
                    while !self.nodes[usize::from(id)].reads.is_empty() {
                        self.read(id);
                    }
                }
            }
            let commits: Vec<u64> = self
                .nodes
                .iter()
                .map(|n| n.voter.as_ref().unwrap().commit())
                .collect();
            if self.commit > goal && commits.iter().all(|&c| c == self.commit) {
                return;
            }
        }
        let state: Vec<(bool, u64, u64, u64)> = self
            .nodes
            .iter()
            .map(|n| {
                let v = n.voter.as_ref().unwrap();
                (v.leading(), v.slot(), v.end(), v.commit())
            })
            .collect();
        for (id, n) in self.nodes.iter().enumerate() {
            let tail: Vec<(u64, u64, Kind)> = n
                .records
                .iter()
                .rev()
                .take(4)
                .map(|r| {
                    let r = decode(r).unwrap();
                    (r.lsn, r.slot, r.kind)
                })
                .collect();
            let head: Vec<(u64, u64, Kind)> = n
                .records
                .iter()
                .take(8)
                .map(|r| {
                    let r = decode(r).unwrap();
                    (r.lsn, r.slot, r.kind)
                })
                .collect();
            let v = n.voter.as_ref().unwrap();
            eprintln!(
                "node {id}: durable {} promised {} refused {} truncate {:?} tail {tail:?} head {head:?}",
                n.durable,
                n.promised,
                v.refused(),
                n.truncate
            );
            let dbg = format!("{v:?}");
            let cut: String = dbg.chars().filter(|c| !c.is_whitespace()).collect();
            let start = cut.find("slot:").unwrap_or(0);
            eprintln!("  voter: {}", &cut[start..cut.len().min(start + 700)]);
        }
        self.fail(&format!(
            "healed cluster did not converge past commit {goal}: (leading, slot, end, commit) {state:?}"
        ));
    }
}

fn run(seed: u64) -> Stats {
    let mut world = World::new(seed);
    for step in 0..STEPS {
        world.step = step;
        if step % 200 == 0 {
            world.calm = !world.calm;
            if world.calm {
                world.isolated = None;
            }
        }
        world.event();
    }
    world.step = STEPS;
    world.heal();
    world.stats
}

#[test]
fn takeovers_keep_one_committed_log() {
    let mut total = Stats::default();
    for seed in seeds() {
        let s = run(seed);
        total.committed += s.committed;
        total.takeovers += s.takeovers;
        total.truncations += s.truncations;
        total.restarts += s.restarts;
        total.power_losses += s.power_losses;
        total.promises_unanswered += s.promises_unanswered;
        total.cut_below_commit += s.cut_below_commit;
    }
    println!(
        "committed {} takeovers {} truncations {} restarts {} power losses {} durable promises never answered {} divergent tails cut below the commit point {}",
        total.committed,
        total.takeovers,
        total.truncations,
        total.restarts,
        total.power_losses,
        total.promises_unanswered,
        total.cut_below_commit
    );
    if std::env::var("STRAUM_SIM_SEED").is_err() {
        assert!(total.committed > 0 && total.takeovers > 0 && total.truncations > 0);
        assert!(
            total.cut_below_commit > 0,
            "no divergent tail reached below a commit point"
        );
        assert!(total.power_losses > 0 && total.promises_unanswered > 0);
    }
}
