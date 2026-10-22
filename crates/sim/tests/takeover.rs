//! Ungraceful takeover and planned handoff (ADR-0008 steps 5 and 6;
//! unified-stream §2.4, §5): three `cluster::Voter`s on in-memory logs, with
//! sequencers handing their slots over at random — a crash can fall before,
//! during and after `SlotEnd`, and a `SlotEnd` can be written and never
//! committed — crashes, power losses, a
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
//! And 2a, a `SlotEnd` closes its slot: nothing of slot k follows slot k's
//! `SlotEnd` in the committed log.
//!
//! And 7, every node's own proposals (D-109 B, D-117 B): each node submits
//! them to its `Voter`, whose outbox forwards them over the same faulty
//! network — lost, cut off by a partition, sent to an owner that is fenced
//! and still running, or crashed with — in order per link, as TCP delivers.
//! A proposal commits at most once; one node incarnation's proposals commit
//! in the order it made them; one handed back as given up never commits
//! later. At the end, with no new proposals, each is committed, given up,
//! refused at a full outbox, or orphaned by its node's crash — none waits.
//!
//! Each run ends healed: every node up, nothing lost, and a sequencer must
//! emerge, commit new records, and every node must reach the same log.
//!
//! `STRAUM_SIM_SEEDS=<n>` sweeps more seeds; `STRAUM_SIM_SEED=<n>` runs one;
//! `STRAUM_SIM_TRACE=1` prints the protocol's events.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::collections::{BTreeMap, VecDeque};

use cluster::{Config, Effect, Message, Proposal, Voter};
use journal::{Chain, Kind, Verifier, decode};

const NODES: u8 = 3;
const STEPS: u64 = 3000;
/// Bytes one read of the log hands back at most, or one batch if that
/// alone is larger (D-81 A).
const READ_BYTES: usize = 4096;

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
        lag_bound: None,
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
    reads: VecDeque<(u8, u64, u64)>,
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
    /// `hand_off` accepted, and slots led without anyone asking for a
    /// promise of them: taken over by a committed `SlotEnd`.
    handoffs: u64,
    slot_ends: u64,
    /// Unified-stream §5: a `SlotEnd` written and never committed, and a
    /// node crashing with one in its log not yet committed.
    slot_ends_lost: u64,
    crashes_with_slot_end: u64,
    handed_over: u64,
    /// Invariant 7: proposals submitted; committed in a later slot than
    /// the one their node had promised when it submitted them (carried
    /// across a slot change); given up; refused; orphaned by a crash.
    submitted: u64,
    carried: u64,
    given_up: u64,
    refused: u64,
    orphaned: u64,
    /// Sent to a slot that stayed open, the forward lost (D-120).
    stranded: u64,
    /// Forwards a sequencer's full or sealed queue refused (D-120 A).
    refused_at_sequencer: u64,
}

/// What became of one node's own proposal (invariant 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    /// Submitted while its node had promised this slot.
    Pending(u64),
    Committed,
    GivenUp,
    Refused,
    /// Its node crashed with it uncommitted: it may still commit once.
    Orphaned,
}

/// A proposal's identity, from its body: (node, incarnation, number).
fn ident(body: &[u8]) -> Option<(u8, u64, u64)> {
    if body.len() != 17 {
        return None;
    }
    let incarnation = u64::from_be_bytes(body[1..9].try_into().ok()?);
    let number = u64::from_be_bytes(body[9..17].try_into().ok()?);
    Some((body[0], incarnation, number))
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
    /// Slots someone asked promises of, and slots whose `SlotEnd` committed.
    asked: std::collections::BTreeSet<u64>,
    ended: std::collections::BTreeSet<u64>,
    /// Slots whose `SlotEnd` some node appended.
    written: std::collections::BTreeSet<u64>,
    /// Invariant 7: every own proposal's fate, and the last number each
    /// (node, incarnation) committed.
    proposals: BTreeMap<(u8, u64, u64), Fate>,
    next_proposal: u64,
    last_committed: BTreeMap<(u8, u64), u64>,
    /// Proposals one of whose forwards was lost on the way (invariant 7).
    lost_forwards: std::collections::BTreeSet<(u8, u64, u64)>,
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
            asked: Default::default(),
            ended: Default::default(),
            written: Default::default(),
            proposals: BTreeMap::new(),
            next_proposal: 0,
            last_committed: BTreeMap::new(),
            lost_forwards: Default::default(),
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
        self.drop_to(id);
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
        node.voter = Some(
            Voter::new(config(id), tail.chain, slots, node.promised).incarnation(node.incarnation),
        );
    }

    fn crash(&mut self, id: u8, power: bool) {
        self.drop_to(id);
        let node = &mut self.nodes[usize::from(id)];
        if node.report_promise.is_some() {
            self.stats.promises_unanswered += 1;
        }
        let pending_end = node.records.iter().any(|record| {
            let r = decode(record).unwrap();
            r.kind == Kind::SlotEnd && !self.ended.contains(&r.slot)
        });
        if pending_end {
            self.stats.crashes_with_slot_end += 1;
        }
        // Its outbox goes with it: what it held may still commit, once.
        let incarnation = self.nodes[usize::from(id)].incarnation;
        for (&(node, life, _), fate) in &mut self.proposals {
            if node == id && life == incarnation && matches!(fate, Fate::Pending(_)) {
                *fate = Fate::Orphaned;
                self.stats.orphaned += 1;
            }
        }
        let node = &mut self.nodes[usize::from(id)];
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
                    if !self.asked.contains(&slot) {
                        self.stats.handed_over += 1;
                    }
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
                for record in &records {
                    let r = decode(record).unwrap();
                    if r.kind == Kind::SlotEnd {
                        self.written.insert(r.slot);
                    }
                }
                let node = &mut self.nodes[usize::from(id)];
                node.records.extend(records);
            }
            Effect::Send { to, message } => {
                if let Message::Promise { slot } = message {
                    self.asked.insert(slot);
                }
                self.network.push((id, to, message));
            }
            Effect::Read { to, from, upto } => {
                self.nodes[usize::from(id)]
                    .reads
                    .push_back((to, from, upto));
            }
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
                // Invariant 2a: a `SlotEnd` closes its slot.
                let r = decode(&record).unwrap();
                if self.ended.contains(&r.slot) {
                    self.fail(&format!(
                        "committed lsn {at} is in slot {}, after that slot's SlotEnd",
                        r.slot
                    ));
                }
                // Invariant 7: once, in its node's order, never after it
                // was given up.
                if let Some(key @ (node, life, number)) = ident(r.body) {
                    let fate = self.proposals.get(&key).copied();
                    match fate {
                        Some(Fate::Pending(submitted)) => {
                            if r.slot > submitted {
                                self.stats.carried += 1;
                            }
                        }
                        Some(Fate::Orphaned) => {}
                        other => self.fail(&format!(
                            "committed lsn {at}: proposal {key:?} commits, its fate was {other:?}"
                        )),
                    }
                    self.proposals.insert(key, Fate::Committed);
                    let last = self.last_committed.insert((node, life), number);
                    if let Some(last) = last.filter(|&last| number <= last) {
                        self.fail(&format!(
                            "committed lsn {at}: proposal {number} of node {node} (life {life}) after its {last}"
                        ));
                    }
                }
                if r.kind == Kind::SlotEnd {
                    self.ended.insert(r.slot);
                    self.stats.slot_ends += 1;
                    if std::env::var_os("STRAUM_SIM_TRACE").is_some() {
                        eprintln!("{} SlotEnd of slot {} committed at {at}", self.step, r.slot);
                    }
                }
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
        // The node applies what it learned is committed, in order: its
        // outbox confirms its proposals and hands back the ones it shows
        // lost.
        let from = node.checked + 1;
        let node = &mut self.nodes[usize::from(id)];
        node.checked = node.checked.max(lsn);
        let mut lost = Vec::new();
        for at in from..=lsn {
            let record = &node.records[usize::try_from(at - 1).unwrap()];
            let voter = node.voter.as_mut().unwrap();
            lost.extend(voter.applied(&decode(record).unwrap()));
        }
        for proposal in lost {
            let key = ident(&proposal.body).unwrap();
            match self.proposals.get(&key) {
                Some(Fate::Pending(_)) => {
                    self.proposals.insert(key, Fate::GivenUp);
                    self.stats.given_up += 1;
                }
                other => self.fail(&format!(
                    "node {id}: gave up proposal {key:?}, its fate was {other:?}"
                )),
            }
        }
    }

    fn deliver(&mut self, index: usize) {
        // Forwards keep their order on a link, as TCP does: the earliest of
        // that link goes first. They are lost only whole (the drop event, a
        // partition, a crash), never reordered.
        let index = match &self.network[index] {
            (from, to, Message::Forward { .. }) => {
                let (from, to) = (*from, *to);
                self.network
                    .iter()
                    .position(|(f, t, m)| {
                        *f == from && *t == to && matches!(m, Message::Forward { .. })
                    })
                    .unwrap_or(index)
            }
            _ => index,
        };
        let (from, to, message) = self.network.remove(index);
        let cut_off = self.isolated.is_some_and(|i| i == from || i == to);
        // A forward its receiver will not take — cut off, down, or not
        // sequencing the slot it was sent for — is lost here (invariant 7).
        if let Message::Forward { slot, proposal } = &message {
            let taken = !cut_off
                && self.nodes[usize::from(to)]
                    .voter
                    .as_ref()
                    .is_some_and(|v| v.leading() && v.slot() == *slot);
            if !taken {
                self.forward_lost(proposal);
            }
        }
        if cut_off {
            return;
        }
        if let Some(voter) = self.nodes[usize::from(to)].voter.as_mut() {
            // A forward the sequencer's full or sealed queue refuses is lost
            // too.
            let forward = match &message {
                Message::Forward { proposal, .. } => Some(proposal.clone()),
                _ => None,
            };
            let refused = voter.forwards_refused();
            voter.on_message(from, message);
            if let Some(proposal) = forward.filter(|_| voter.forwards_refused() > refused) {
                self.stats.refused_at_sequencer += 1;
                self.forward_lost(&proposal);
            }
            self.drain(to);
        }
    }

    /// Invariant 7: a forward that never reached a sequencer of its slot.
    fn forward_lost(&mut self, proposal: &Proposal) {
        if let Some(key) = ident(&proposal.body) {
            self.lost_forwards.insert(key);
        }
    }

    /// Messages to `id` go with it; its forwards among them are lost.
    fn drop_to(&mut self, id: u8) {
        let (gone, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.network)
            .into_iter()
            .partition(|(_, to, _)| *to == id);
        self.network = kept;
        for (_, _, message) in gone {
            if let Message::Forward { proposal, .. } = message {
                self.forward_lost(&proposal);
            }
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
        // To the last batch boundary at or below `after`, as the node's
        // WAL thread cuts.
        let mut after = after;
        while after > 0
            && decode(&node.records[usize::try_from(after).unwrap()])
                .unwrap()
                .kind
                != Kind::BatchHeader
        {
            after -= 1;
        }
        let after_index = usize::try_from(after).unwrap();
        // Invariant 1, on the cut as made.
        if let Some(index) = (after_index..node.records.len())
            .find(|&index| self.oracle.get(&(index as u64 + 1)) == Some(&node.records[index]))
        {
            self.fail(&format!(
                "node {id}: cut to {after}, removing its copy of committed lsn {}",
                index + 1
            ));
        }
        let node = &mut self.nodes[usize::from(id)];
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
        let Some((to, from, upto)) = node.reads.pop_front() else {
            return;
        };
        let start = usize::try_from(from - 1).unwrap();
        let mut batches = cluster::ReadBatches::new(from, upto, READ_BYTES);
        for record in node.records.iter().skip(start) {
            if batches.done() {
                break;
            }
            batches.feed(record);
        }
        let batch = batches.finish();
        let voter = node.voter.as_mut().unwrap();
        match batch {
            Some(batch) => voter.on_read(to, batch),
            None => voter.on_read_missing(to, from),
        }
        self.drain(id);
    }

    /// Every node makes one of its own proposals, through its outbox as a
    /// node does (invariant 7); a sequencer sometimes hands its slot over
    /// instead. A sequencer then makes a batch.
    fn propose(&mut self) {
        for id in 0..NODES {
            let incarnation = self.nodes[usize::from(id)].incarnation;
            let Some(voter) = self.nodes[usize::from(id)].voter.as_mut() else {
                continue;
            };
            if voter.leading() && self.rng.chance(5) {
                if voter.hand_off().is_ok() {
                    if std::env::var_os("STRAUM_SIM_TRACE").is_some() {
                        eprintln!("{} n{id} hands slot {} over", self.step, voter.slot());
                    }
                    self.stats.handoffs += 1;
                }
                self.drain(id);
                continue;
            }
            self.next_proposal += 1;
            let key = (id, incarnation, self.next_proposal);
            let mut body = vec![id];
            body.extend_from_slice(&incarnation.to_be_bytes());
            body.extend_from_slice(&self.next_proposal.to_be_bytes());
            let slot = voter.slot();
            let fate = match voter.submit(Proposal {
                kind: Kind::Data,
                properties: Vec::new(),
                body,
            }) {
                Ok(()) => Fate::Pending(slot),
                Err(_) => {
                    self.stats.refused += 1;
                    Fate::Refused
                }
            };
            let _ = voter.flush();
            self.stats.submitted += 1;
            self.proposals.insert(key, fate);
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
                    // `remove`, not `swap_remove`: forwards keep their order on
                    // a link, and the position in this list is that order.
                    let (_, _, message) = self.network.remove(index);
                    if let Message::Forward { proposal, .. } = &message {
                        self.forward_lost(proposal);
                    }
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
    /// commit, and every node must reach the same log. Then a planned
    /// handoff must pass the next slot to its owner without a promise round.
    fn heal(&mut self) {
        self.isolated = None;
        for id in 0..NODES {
            if !self.up(id) {
                self.start(id);
            }
        }
        self.converge(self.commit + 1);
        // A handoff from before the healing can have just committed its
        // `SlotEnd`: its sequencer stepped down and the successor has not
        // started yet (seed 6055). Wait for one.
        for _ in 0..40 {
            if (0..NODES).any(|id| {
                self.nodes[usize::from(id)]
                    .voter
                    .as_ref()
                    .unwrap()
                    .leading()
            }) {
                break;
            }
            self.converge(self.commit + 1);
        }
        let (id, slot) = (0..NODES)
            .find_map(|id| {
                let v = self.nodes[usize::from(id)].voter.as_ref().unwrap();
                v.leading().then(|| (id, v.slot()))
            })
            .unwrap_or_else(|| self.fail("healed cluster has no sequencer"));
        let voter = self.nodes[usize::from(id)].voter.as_mut().unwrap();
        if let Err(e) = voter.hand_off() {
            self.fail(&format!(
                "healed sequencer of slot {slot} refused to hand off: {e}"
            ));
        }
        self.stats.handoffs += 1;
        self.drain(id);
        // What was queued before the seal commits first, then `SlotEnd`,
        // then the next slot's records.
        let next = slot + 1;
        for _ in 0..40 {
            if self.leaders.contains_key(&next) {
                break;
            }
            self.converge(self.commit + 1);
        }
        if !self.leaders.contains_key(&next) || self.asked.contains(&next) {
            self.fail(&format!(
                "slot {slot} handed off in a healed cluster, and slot {next} was not taken over by its SlotEnd (led: {:?}, promises asked: {})",
                self.leaders.get(&next),
                self.asked.contains(&next)
            ));
        }
    }

    /// Invariant 7 at the end: with no new proposals, every node's own
    /// proposal is committed, given up, refused or orphaned — or stranded:
    /// sent to the sequencer of a slot that stays open, with its forward
    /// lost on the way. Nothing then shows it lost or lets it be sent again
    /// (D-120); only that is allowed to wait. A proposal held unsent, or
    /// waiting with every forward of it delivered, fails.
    fn settle(&mut self) {
        for _ in 0..200 {
            if !self
                .proposals
                .values()
                .any(|f| matches!(f, Fate::Pending(_)))
            {
                return;
            }
            self.tick();
            for id in 0..NODES {
                if let Some(voter) = self.nodes[usize::from(id)].voter.as_mut() {
                    let _ = voter.flush();
                }
                self.drain(id);
            }
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
        for id in 0..NODES {
            let (held, sent) = self.nodes[usize::from(id)].voter.as_ref().unwrap().held();
            if held != sent {
                self.fail(&format!(
                    "node {id}: {} of its {held} proposals still unsent after the cluster settled",
                    held - sent
                ));
            }
        }
        let waiting: Vec<(u8, u64, u64)> = self
            .proposals
            .iter()
            .filter(|(_, f)| matches!(f, Fate::Pending(_)))
            .map(|(&key, _)| key)
            .collect();
        if let Some(key) = waiting.iter().find(|k| !self.lost_forwards.contains(k)) {
            self.fail(&format!(
                "proposal {key:?} still waits after the cluster settled, and no forward of it was lost"
            ));
        }
        self.stats.stranded += waiting.len() as u64;
    }

    fn converge(&mut self, goal: u64) {
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
    world.settle();
    world.stats.slot_ends_lost = world.written.difference(&world.ended).count() as u64;
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
        total.handoffs += s.handoffs;
        total.slot_ends += s.slot_ends;
        total.slot_ends_lost += s.slot_ends_lost;
        total.crashes_with_slot_end += s.crashes_with_slot_end;
        total.handed_over += s.handed_over;
        total.submitted += s.submitted;
        total.carried += s.carried;
        total.given_up += s.given_up;
        total.refused += s.refused;
        total.orphaned += s.orphaned;
        total.stranded += s.stranded;
        total.refused_at_sequencer += s.refused_at_sequencer;
    }
    println!(
        "committed {} takeovers {} truncations {} restarts {} power losses {} durable promises never answered {} divergent tails cut below the commit point {} handoffs {} committed {} never committed {} crashes with one pending {} slots handed over without a promise round {}",
        total.committed,
        total.takeovers,
        total.truncations,
        total.restarts,
        total.power_losses,
        total.promises_unanswered,
        total.cut_below_commit,
        total.handoffs,
        total.slot_ends,
        total.slot_ends_lost,
        total.crashes_with_slot_end,
        total.handed_over
    );
    println!(
        "own proposals {} carried across a slot change {} given up {} refused {} orphaned by a crash {} stranded {} forwards refused by a sequencer {}",
        total.submitted,
        total.carried,
        total.given_up,
        total.refused,
        total.orphaned,
        total.stranded,
        total.refused_at_sequencer
    );
    if std::env::var("STRAUM_SIM_SEED").is_err() {
        assert!(total.committed > 0 && total.takeovers > 0 && total.truncations > 0);
        assert!(
            total.cut_below_commit > 0,
            "no divergent tail reached below a commit point"
        );
        assert!(total.power_losses > 0 && total.promises_unanswered > 0);
        assert!(total.handed_over > 0, "no slot was handed over by SlotEnd");
        assert!(
            total.carried > 0 && total.orphaned > 0,
            "no proposal was carried across a slot change, or orphaned"
        );
        assert!(
            total.slot_ends_lost > 0 && total.crashes_with_slot_end > 0,
            "no SlotEnd was lost, or no crash fell on a pending one"
        );
    }
}

/// A planned handoff whose successor had just cut its log: the slot is
/// handed over once the successor holds `SlotEnd`, not taken by silence
/// (D-85, seed 4905).
#[test]
fn a_handoff_waits_for_a_successor_catching_up() {
    run(4905);
}

/// A forward still in flight when its sender gave it up reaches its old
/// owner leading a later slot: taken there, it would commit after its node
/// gave it up. A sequencer takes a forward only into the slot it was sent
/// for (D-117 B; caught by mutation at seed 14165, rare in the sweep).
#[test]
fn a_forward_for_an_older_slot_is_not_taken_into_a_later_one() {
    run(14165);
}

/// A proposal shown lost — its forward refused or dropped, a later one of
/// its node committed in the same slot — is given up and handed back, and
/// never commits afterwards (D-115 A). Rare since each node holds only its
/// share of the sequencer's queue (D-120 A): 7 in 2 000 seeds; this seed
/// has 3.
#[test]
fn a_proposal_shown_lost_is_given_up_and_never_commits() {
    assert!(run(1676).given_up > 0);
}
