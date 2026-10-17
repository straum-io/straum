//! Three nodes, each the `cluster` core plus the apply function, with
//! sessions at every gateway (ADR-0008 steps 3b-1, 3b-2). No MQTT: sessions
//! propose `apply::Proposal`s — publish, subscribe, unsubscribe, ack, end —
//! forwarded to the sequencer over a network that drops, delays and
//! reorders; nodes crash and lose power, and a restarted gateway resets its
//! old sessions through the log.
//!
//! Checked at every effect a node produces for one of its live sessions:
//!
//! * **4** — PUBACK and delivery never precede commit: the record behind the
//!   effect is durable on a majority, with the committed content.
//! * The effects are what an independent reference model of the committed
//!   log says, in the same order: deliveries once per matching session at
//!   the lesser guarantee, subscription options, retained messages, inflight
//!   and backlog with their bounds, slow-consumer closes, acknowledgements.
//! * Determinism (unified-stream §1): every node that applied lsn L holds the
//!   same state there.
//!
//! At the end every session ends and every node applies the whole log: no
//! session may be left in the state ("closing a session
//! removes all of its subscriptions and inflight state").
//!
//! The log here is in memory and never compacted: a node that restarts
//! rebuilds its state from lsn 1. Compaction needs the state copy of step 7b.
//! Disk faults are the business of `cluster.rs` and `journal.rs`.
//!
//! `STRAUM_SIM_SEEDS=<n>` sweeps more seeds; `STRAUM_SIM_SEED=<n>` runs one.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    clippy::large_enum_variant
)]

use std::collections::{BTreeMap, BTreeSet};

use apply::{Effect, Limits, Options, Origin, Proposal, RetainHandling, State};
use bytes::Bytes;
use cluster::{Config, Effect as Step, Message, Voter};
use domain::{DeliveryGuarantee as G, Topic, TopicFilter};
use journal::{Chain, Verifier, decode};

const NODES: u8 = 3;
/// Bytes one read of the log hands back at most, or one batch if that
/// alone is larger (D-81 A).
const READ_BYTES: usize = 4096;
const STEPS: usize = 1500;
const TOPICS: [&str; 4] = ["a", "a/b", "a/c", "z"];
const FILTERS: [&str; 5] = ["a", "a/+", "a/#", "#", "z"];
/// Small, so the simulation reaches them.
const LIMITS: Limits = Limits {
    // Low enough to be reached when takeovers drop many proposals: the
    // refusal path stays in every default run (D-70 A).
    max_subscriptions_per_session: 2,
    max_subscriptions: 5,
    max_retained: 2,
    max_retained_bytes: 16,
    max_inflight: 2,
    max_backlog: 3,
    max_backlog_bytes: 24,
    max_total_queued_bytes: 40,
};

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
    fn pick<T: Copy>(&mut self, from: &[T]) -> T {
        from[self.below(from.len() as u64) as usize]
    }
}

fn config(id: u8) -> Config {
    Config {
        id,
        nodes: (0..NODES).collect(),
        owner: 0,
        slot: 1,
        window: 24,
        max_queue: 32,
        max_batch: 4,
        recent_batches: 4,
        max_uncommitted: 1_000_000,
        // Twice the node's: faults here are frequent enough that three
        // heartbeats of silence took most of the run up with takeovers.
        takeover_ticks: 6,
        lag_bound: None,
    }
}

enum Wire {
    Cluster(Message),
    /// A gateway's proposal on its way to the sequencer: one hop, to the
    /// owner of the slot its gateway promised.
    Forward(Proposal),
}

struct Node {
    core: Option<Voter>,
    /// The promise as the disk holds it, and one asked and not yet synced.
    promised: u64,
    promising: Option<u64>,
    /// A cut asked and not yet done.
    truncate: Option<u64>,
    /// Every record handed to this node's log, lsn i + 1 at index i.
    records: Vec<Vec<u8>>,
    written: usize,
    durable: usize,
    state: State,
    /// The last snapshot of `state`, taken only at a durable lsn; survives
    /// crashes and power losses, as the node's snapshot file does (D-67 B).
    snapshot: Option<Vec<u8>>,
    /// The committed slot table the snapshot was taken with, durable with
    /// it; and the one kept as records apply, pruned by compaction — what
    /// the node keeps beside its snapshot (D-79 A).
    snapshot_slots: Vec<(u64, u64)>,
    committed_slots: Vec<(u64, u64)>,
    /// Records below this index are compacted away: emptied, so any read of
    /// one fails the run, and a restart can only rebuild from the snapshot
    /// and the log after it.
    floor: usize,
    /// Survives crashes, as a gateway's durable counter would: sessions of
    /// incarnation k are numbered from k * 1000.
    incarnation: u64,
    next_session: u64,
    /// Sessions open at this gateway, with the at-least-once deliveries they
    /// have been handed and not yet acknowledged.
    live: BTreeMap<u64, BTreeSet<u64>>,
}

/// The reference: what the committed log means, written without the apply
/// crate's data structures.
#[derive(Default)]
struct RefSession {
    subscriptions: Vec<(TopicFilter, G, Options)>,
    inflight: Vec<(u64, RefMessage)>,
    backlog: Vec<(Option<u64>, RefMessage)>,
    next: u64,
}

#[derive(Clone)]
struct RefMessage {
    topic: Topic,
    payload: Bytes,
    properties: Bytes,
    retain: bool,
}

impl RefMessage {
    fn size(&self) -> usize {
        self.payload.len() + self.properties.len()
    }
}

#[derive(Default)]
struct Reference {
    applied: u64,
    sessions: Vec<(Origin, RefSession)>,
    retained: Vec<(Topic, Bytes, Bytes, G)>,
    effects: BTreeMap<u64, Vec<Effect>>,
}

impl Reference {
    fn session(&mut self, origin: Origin) -> Option<&mut RefSession> {
        self.sessions
            .iter_mut()
            .find(|(o, _)| *o == origin)
            .map(|(_, s)| s)
    }

    fn total_subscriptions(&self) -> usize {
        self.sessions
            .iter()
            .map(|(_, s)| s.subscriptions.len())
            .sum()
    }

    /// Inflight and backlog payload bytes of every session.
    fn total_backlog(&self) -> usize {
        self.sessions
            .iter()
            .map(|(_, s)| {
                s.backlog.iter().map(|(_, m)| m.size()).sum::<usize>()
                    + s.inflight.iter().map(|(_, m)| m.size()).sum::<usize>()
            })
            .sum()
    }

    fn tidy(&mut self) {
        self.sessions.retain(|(_, s)| {
            !(s.subscriptions.is_empty() && s.inflight.is_empty() && s.backlog.is_empty())
        });
        self.sessions.sort_by_key(|(o, _)| *o);
    }

    fn give(&mut self, to: Origin, message: RefMessage, g: G, lsn: u64, out: &mut Vec<Effect>) {
        let total = self.total_backlog();
        let Some(s) = self.session(to) else { return };
        let id = (g == G::AtLeastOnce).then(|| {
            s.next += 1;
            s.next - 1
        });
        // In order: nothing goes past what already waits.
        let budget = total + message.size() <= LIMITS.max_total_queued_bytes;
        if s.backlog.is_empty()
            && (id.is_none() || (s.inflight.len() < LIMITS.max_inflight && budget))
        {
            if let Some(id) = id {
                s.inflight.push((id, message.clone()));
            }
            out.push(Effect::Deliver {
                to,
                id,
                topic: message.topic,
                payload: message.payload,
                properties: message.properties,
                guarantee: g,
                retain: message.retain,
                lsn,
            });
            return;
        }
        let mine: usize = s.backlog.iter().map(|(_, m)| m.size()).sum();
        let size = message.size();
        if s.backlog.len() < LIMITS.max_backlog
            && mine + size <= LIMITS.max_backlog_bytes
            && total + size <= LIMITS.max_total_queued_bytes
        {
            s.backlog.push((id, message));
        } else if id.is_some() {
            self.sessions.retain(|(o, _)| *o != to);
            out.push(Effect::Close { to, lsn });
        }
    }

    fn apply(&mut self, lsn: u64, record: &[u8]) {
        assert_eq!(lsn, self.applied + 1);
        self.applied = lsn;
        let record = decode(record).unwrap();
        if matches!(
            record.kind,
            journal::Kind::BatchHeader | journal::Kind::SlotStart | journal::Kind::SlotEnd
        ) {
            return;
        }
        let mut out = Vec::new();
        match Proposal::decode(&record).unwrap() {
            Proposal::Publish {
                origin,
                request,
                topic,
                payload,
                properties,
                guarantee,
                retain,
            } => {
                if retain {
                    self.retained.retain(|(t, _, _, _)| *t != topic);
                    let bytes: usize = self
                        .retained
                        .iter()
                        .map(|(_, p, q, _)| p.len() + q.len())
                        .sum();
                    if !payload.is_empty()
                        && self.retained.len() < LIMITS.max_retained
                        && bytes + payload.len() + properties.len() <= LIMITS.max_retained_bytes
                    {
                        self.retained.push((
                            topic.clone(),
                            payload.clone(),
                            properties.clone(),
                            guarantee,
                        ));
                    }
                }
                let mut targets = Vec::new();
                for (o, s) in &self.sessions {
                    let matching: Vec<&(TopicFilter, G, Options)> = s
                        .subscriptions
                        .iter()
                        .filter(|(f, _, opt)| f.matches(&topic) && !(opt.no_local && *o == origin))
                        .collect();
                    if let Some(best) = matching.iter().map(|(_, g, _)| *g).max() {
                        let keep =
                            retain && matching.iter().any(|(_, _, opt)| opt.retain_as_published);
                        targets.push((*o, best.min(guarantee), keep));
                    }
                }
                for (to, g, keep) in targets {
                    let message = RefMessage {
                        topic: topic.clone(),
                        payload: payload.clone(),
                        properties: properties.clone(),
                        retain: keep,
                    };
                    self.give(to, message, g, lsn, &mut out);
                }
                out.push(Effect::Accepted {
                    to: origin,
                    request,
                    lsn,
                });
            }
            Proposal::Subscribe {
                origin,
                request,
                filter,
                guarantee,
                options,
            } => {
                if self.session(origin).is_none() {
                    self.sessions.push((origin, RefSession::default()));
                    self.sessions.sort_by_key(|(o, _)| *o);
                }
                let total = self.total_subscriptions();
                let s = self.session(origin).unwrap();
                let existed = s.subscriptions.iter().any(|(f, _, _)| *f == filter);
                if !existed
                    && (s.subscriptions.len() >= LIMITS.max_subscriptions_per_session
                        || total >= LIMITS.max_subscriptions)
                {
                    out.push(Effect::Subscribed {
                        to: origin,
                        request,
                        granted: None,
                        lsn,
                    });
                } else {
                    s.subscriptions.retain(|(f, _, _)| *f != filter);
                    s.subscriptions.push((filter.clone(), guarantee, options));
                    out.push(Effect::Subscribed {
                        to: origin,
                        request,
                        granted: Some(guarantee),
                        lsn,
                    });
                    let send = match options.retain_handling {
                        RetainHandling::OnSubscribe => true,
                        RetainHandling::OnNewSubscription => !existed,
                        RetainHandling::Never => false,
                    };
                    if send {
                        let mut retained = self.retained.clone();
                        retained.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                        for (topic, payload, properties, g) in retained {
                            if filter.matches(&topic) {
                                let message = RefMessage {
                                    topic,
                                    payload,
                                    properties,
                                    retain: true,
                                };
                                self.give(origin, message, guarantee.min(g), lsn, &mut out);
                            }
                        }
                    }
                }
            }
            Proposal::Unsubscribe {
                origin,
                request,
                filter,
            } => {
                if let Some(s) = self.session(origin) {
                    s.subscriptions.retain(|(f, _, _)| *f != filter);
                }
                out.push(Effect::Unsubscribed {
                    to: origin,
                    request,
                    lsn,
                });
            }
            Proposal::Ack { origin, delivery } => {
                // Answered whatever it finds (D-81, seed 1189).
                out.push(Effect::Acked {
                    to: origin,
                    delivery,
                    lsn,
                });
                if let Some(s) = self.session(origin) {
                    if let Some(at) = s.inflight.iter().position(|(id, _)| *id == delivery) {
                        s.inflight.remove(at);
                        while !s.backlog.is_empty() {
                            let waiting_qos1 = s.backlog[0].0.is_some();
                            if waiting_qos1 && s.inflight.len() >= LIMITS.max_inflight {
                                break;
                            }
                            let (id, m) = s.backlog.remove(0);
                            if let Some(id) = id {
                                s.inflight.push((id, m.clone()));
                            }
                            out.push(Effect::Deliver {
                                to: origin,
                                id,
                                topic: m.topic,
                                payload: m.payload,
                                properties: m.properties,
                                guarantee: if id.is_some() {
                                    G::AtLeastOnce
                                } else {
                                    G::AtMostOnce
                                },
                                retain: m.retain,
                                lsn,
                            });
                        }
                    }
                }
            }
            Proposal::SessionGone { origin } => {
                self.sessions.retain(|(o, _)| *o != origin);
                out.push(Effect::Gone { to: origin, lsn });
            }
            Proposal::NodeReset { node, below } => {
                self.sessions
                    .retain(|(o, _)| !(o.node == node && o.session < below));
                out.push(Effect::Reset { node, below, lsn });
            }
        }
        self.tidy();
        self.effects.insert(lsn, out);
    }
}

#[derive(Default)]
struct Stats {
    committed: u64,
    effects: u64,
    deliveries: u64,
    acked: u64,
    closes: u64,
    refused: u64,
    restarts: u64,
    power_losses: u64,
    forwards_dropped: u64,
    snapshots: u64,
    snapshot_restarts: u64,
    compactions: u64,
    compacted_restarts: u64,
    cuts: u64,
    takeovers: u64,
    compactions_during_takeover: u64,
}

struct World {
    seed: u64,
    step: usize,
    rng: Rng,
    nodes: Vec<Node>,
    network: Vec<(u8, u8, Wire)>,
    isolated: Option<u8>,
    /// No faults: the log commits, sessions live long enough to be
    /// delivered to and to acknowledge.
    calm: bool,
    oracle: BTreeMap<u64, Vec<u8>>,
    commit: u64,
    reference: Reference,
    digests: BTreeMap<u64, u64>,
    /// Slot → the (node, incarnation) that sequenced it (invariant 2).
    leaders: BTreeMap<u64, (u8, u64)>,
    request: u64,
    stats: Stats,
}

impl World {
    fn fail(&self, what: &str) -> ! {
        panic!(
            "seed {} step {}: {what}\n(reproduce: STRAUM_SIM_SEED={} cargo test -p sim --test apply)",
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
                    core: None,
                    promised: 0,
                    promising: None,
                    truncate: None,
                    records: Vec::new(),
                    written: 0,
                    durable: 0,
                    state: State::new(LIMITS),
                    snapshot: None,
                    snapshot_slots: Vec::new(),
                    committed_slots: Vec::new(),
                    floor: 0,
                    incarnation: 0,
                    next_session: 0,
                    live: BTreeMap::new(),
                })
                .collect(),
            network: Vec::new(),
            isolated: None,
            calm: false,
            oracle: BTreeMap::new(),
            commit: 0,
            reference: Reference::default(),
            digests: BTreeMap::new(),
            leaders: BTreeMap::new(),
            request: 0,
            stats: Stats::default(),
        };
        for id in 0..NODES {
            world.start(id);
        }
        world
    }

    fn start(&mut self, id: u8) {
        self.network.retain(|(_, to, _)| *to != id);
        let node = &mut self.nodes[usize::from(id)];
        // A compacted log resumes the chain at its first record left.
        let start = match node.records.get(node.floor) {
            Some(first) if node.floor > 0 => {
                let first = decode(first).unwrap();
                // Slot 0, as `node` (D-82).
                Chain::resume(first.prev_hash, node.floor as u64, 0)
            }
            _ => Chain::genesis(),
        };
        let mut verifier = Verifier::new(start);
        for record in &node.records[node.floor..] {
            verifier.feed(record).unwrap();
        }
        let tail = verifier.finish();
        node.records
            .truncate(usize::try_from(tail.chain.next_lsn() - 1).unwrap());
        node.written = node.records.len();
        // Reopening makes what it recovered durable (D-43).
        node.durable = node.records.len();
        // From the snapshot, then the log after it: every lsn applied from
        // here is checked against the other nodes' digests as before.
        node.state = match &node.snapshot {
            Some(bytes) => State::decode(LIMITS, bytes).unwrap(),
            None => State::new(LIMITS),
        };
        let from = node.state.applied();
        let end = node.records.len() as u64;
        if from > end {
            self.fail(&format!(
                "node {id}: snapshot at {from} is ahead of its log, which ends at {end}"
            ));
        }
        let node = &mut self.nodes[usize::from(id)];
        if from > 0 {
            self.stats.snapshot_restarts += 1;
        }
        if node.floor > 0 {
            self.stats.compacted_restarts += 1;
        }
        node.incarnation += 1;
        node.next_session = node.incarnation * 1000;
        node.live.clear();
        let below = node.next_session;
        let mut slots: Vec<(u64, u64)> = Vec::new();
        for record in &node.records[node.floor..] {
            let r = decode(record).unwrap();
            if slots.last().is_none_or(|&(slot, _)| slot < r.slot) {
                slots.push((r.slot, r.lsn));
            }
        }
        node.promising = None;
        node.truncate = None;
        // A follower whatever it was; it knows as committed what it applied.
        // The snapshot's slot table, then the slots the log after it begins
        // (D-73 A, D-79 A), as `node` does.
        node.committed_slots = if node.snapshot.is_some() {
            node.snapshot_slots.clone()
        } else {
            Vec::new()
        };
        let slots = cluster::merge_slots(&node.committed_slots, &slots);
        node.core = Some(
            Voter::new(config(id), tail.chain, slots, node.promised)
                .knowing_commit(node.state.applied())
                .incarnation(node.incarnation),
        );
        // Whatever the previous incarnation's sessions left goes.
        self.send(id, Proposal::NodeReset { node: id, below });
        self.drain(id);
    }

    /// What the node does every `snapshot_every` records: encode the
    /// applied state, if it is no further than the durable log.
    fn snapshot(&mut self, id: u8) {
        let node = &mut self.nodes[usize::from(id)];
        if node.state.applied() > node.durable as u64 {
            return;
        }
        let mut bytes = Vec::new();
        node.state.encode(&mut bytes);
        node.snapshot = Some(bytes);
        node.snapshot_slots = node.committed_slots.clone();
        self.stats.snapshots += 1;
        // Then what the node gives back: below the snapshot, and below what
        // the sequencer still ships to a replica or a replica has committed;
        // at a batch's end, where a segment would start, and never the last
        // durable record, which says where the log stands.
        let Some(core) = node.core.as_ref() else {
            return;
        };
        // No byte bound here: a sequencer keeps what any replica still
        // needs, a follower keeps everything a next sequencer may serve.
        let floor = core.compaction_floor(0);
        let mut floor = usize::try_from(floor.min(node.state.applied()))
            .unwrap()
            .min(node.durable.saturating_sub(1));
        while floor > node.floor
            && decode(&node.records[floor]).unwrap().kind != journal::Kind::BatchHeader
        {
            floor -= 1;
        }
        if floor > node.floor {
            for record in &mut node.records[node.floor..floor] {
                *record = Vec::new();
            }
            node.floor = floor;
            cluster::prune_slots(&mut node.committed_slots, floor as u64);
            if let Some(core) = node.core.as_mut() {
                core.on_compacted(floor as u64);
            }
            self.stats.compactions += 1;
            if !self
                .nodes
                .iter()
                .any(|n| n.core.as_ref().is_some_and(Voter::leading))
            {
                self.stats.compactions_during_takeover += 1;
            }
        }
    }

    fn crash(&mut self, id: u8, power: bool) {
        self.network.retain(|(_, to, _)| *to != id);
        let node = &mut self.nodes[usize::from(id)];
        node.core = None;
        node.live.clear();
        let kept = if power { node.durable } else { node.written };
        node.records.truncate(kept);
        node.written = kept;
        node.durable = node.durable.min(kept);
        if power {
            self.stats.power_losses += 1;
        }
    }

    fn drain(&mut self, id: u8) {
        loop {
            let steps = match self.nodes[usize::from(id)].core.as_mut() {
                Some(core) => core.take_effects(),
                None => return,
            };
            if steps.is_empty() {
                break;
            }
            for step in steps {
                match step {
                    Step::Append { first, records } => {
                        let node = &mut self.nodes[usize::from(id)];
                        assert_eq!(first, node.records.len() as u64 + 1);
                        node.records.extend(records);
                    }
                    Step::Send { to, message } => {
                        self.network.push((id, to, Wire::Cluster(message)));
                    }
                    Step::Read { to, from, upto } => {
                        let node = &self.nodes[usize::from(id)];
                        let start = usize::try_from(from - 1).unwrap();
                        let mut batches = cluster::ReadBatches::new(from, upto, READ_BYTES);
                        if start >= node.floor {
                            for record in node.records.iter().skip(start) {
                                if batches.done() {
                                    break;
                                }
                                batches.feed(record);
                            }
                        }
                        let batch = batches.finish();
                        let core = self.nodes[usize::from(id)].core.as_mut().unwrap();
                        match batch {
                            Some(batch) => core.on_read(to, batch),
                            None => core.on_read_missing(to, from),
                        }
                    }
                    Step::Committed(lsn) => {
                        let leading = self.nodes[usize::from(id)]
                            .core
                            .as_ref()
                            .is_some_and(Voter::leading);
                        if leading {
                            self.freeze(id, lsn);
                        } else if lsn > self.commit {
                            self.fail(&format!(
                                "node {id}: learned commit {lsn} before a sequencer reached it"
                            ));
                        }
                    }
                    Step::ReadDone { .. } | Step::Lost { .. } => {}
                    Step::Promise { slot } => {
                        let node = &mut self.nodes[usize::from(id)];
                        node.promising = Some(node.promising.map_or(slot, |p| p.max(slot)));
                    }
                    Step::Truncate { after } => self.nodes[usize::from(id)].truncate = Some(after),
                }
            }
        }
        if let Some(core) = self.nodes[usize::from(id)].core.as_ref() {
            let n = &self.nodes[usize::from(id)];
            if core.end() != n.records.len() as u64 && n.truncate.is_none() {
                let tail: Vec<(u64, u64)> = n
                    .records
                    .iter()
                    .rev()
                    .take(4)
                    .filter(|r| !r.is_empty())
                    .map(|r| {
                        let d = decode(r).unwrap();
                        (d.lsn, d.slot)
                    })
                    .collect();
                self.fail(&format!(
                    "node {id}: voter end {} but log holds {} ({tail:?}), leading {}",
                    core.end(),
                    n.records.len(),
                    core.leading()
                ));
            }
        }
        // Invariant 2: one sequencer per slot.
        if let Some(core) = self.nodes[usize::from(id)]
            .core
            .as_ref()
            .filter(|c| c.leading())
        {
            let me = (id, self.nodes[usize::from(id)].incarnation);
            match self.leaders.get(&core.slot()) {
                Some(&other) if other != me => {
                    let slot = core.slot();
                    self.fail(&format!(
                        "slot {slot} has two sequencers: {other:?} and {me:?}"
                    ));
                }
                Some(_) => {}
                None => {
                    self.leaders.insert(core.slot(), me);
                    self.stats.takeovers += 1;
                }
            }
        }
        self.apply(id);
    }

    fn freeze(&mut self, leader: u8, lsn: u64) {
        if lsn <= self.commit {
            return;
        }
        for at in self.commit + 1..=lsn {
            let record =
                self.nodes[usize::from(leader)].records[usize::try_from(at - 1).unwrap()].clone();
            if record.is_empty() {
                self.fail(&format!("committed {at}, which the sequencer compacted"));
            }
            self.reference.apply(at, &record);
            self.oracle.insert(at, record);
        }
        self.commit = lsn;
        self.stats.committed = lsn;
    }

    fn apply(&mut self, id: u8) {
        loop {
            let node = &self.nodes[usize::from(id)];
            let Some(core) = node.core.as_ref() else {
                return;
            };
            let next = node.state.applied() + 1;
            if next > core.commit() || next > node.records.len() as u64 {
                return;
            }
            let bytes = node.records[usize::try_from(next - 1).unwrap()].clone();
            let mut effects = Vec::new();
            let node = &mut self.nodes[usize::from(id)];
            let record = decode(&bytes).unwrap();
            if node
                .committed_slots
                .last()
                .is_none_or(|&(slot, _)| slot < record.slot)
            {
                node.committed_slots.push((record.slot, record.lsn));
            }
            if let Err(e) = node.state.apply(&record, &mut effects) {
                self.fail(&format!("node {id}: lsn {next} did not apply: {e}"));
            }
            let digest = node.state.digest();
            match self.digests.get(&next) {
                Some(&first) if first != digest => {
                    self.fail(&format!(
                        "node {id}: state at lsn {next} differs from another node's"
                    ));
                }
                Some(_) => {}
                None => {
                    self.digests.insert(next, digest);
                }
            }
            self.check(id, next, &bytes, &effects);
            self.gateway(id, &effects);
        }
    }

    fn mine(&self, id: u8, effect: &Effect) -> bool {
        effect.to().is_some_and(|to| {
            to.node == id && self.nodes[usize::from(id)].live.contains_key(&to.session)
        })
    }

    fn check(&mut self, id: u8, lsn: u64, bytes: &[u8], effects: &[Effect]) {
        let observed: Vec<&Effect> = effects.iter().filter(|e| self.mine(id, e)).collect();
        if observed.is_empty() {
            return;
        }
        if self
            .oracle
            .get(&lsn)
            .is_none_or(|committed| committed != bytes)
        {
            self.fail(&format!(
                "node {id}: effect of lsn {lsn}, which is not the committed record"
            ));
        }
        let index = usize::try_from(lsn - 1).unwrap();
        let holding = self
            .nodes
            .iter()
            // A record compacted after commit counts as held (D-57 A).
            .filter(|n| {
                n.floor > index
                    || (n.durable > index && n.records.get(index).is_some_and(|r| r == bytes))
            })
            .count();
        if holding < usize::from(NODES) / 2 + 1 {
            self.fail(&format!(
                "node {id}: effect of lsn {lsn}, durable on {holding} nodes"
            ));
        }
        let expected: Vec<&Effect> = self.reference.effects[&lsn]
            .iter()
            .filter(|e| self.mine(id, e))
            .collect();
        if observed != expected {
            self.fail(&format!(
                "node {id}: lsn {lsn} made {observed:?}, the committed log means {expected:?}"
            ));
        }
        self.stats.effects += observed.len() as u64;
        for effect in observed {
            match effect {
                Effect::Deliver { .. } => self.stats.deliveries += 1,
                Effect::Acked { .. } => self.stats.acked += 1,
                Effect::Close { .. } => self.stats.closes += 1,
                Effect::Subscribed { granted: None, .. } => self.stats.refused += 1,
                _ => {}
            }
        }
    }

    /// What the gateway does with its sessions' effects: remembers
    /// at-least-once deliveries to acknowledge, and ends a closed session.
    fn gateway(&mut self, id: u8, effects: &[Effect]) {
        for effect in effects {
            if !self.mine(id, effect) {
                continue;
            }
            let live = &mut self.nodes[usize::from(id)].live;
            match effect {
                Effect::Deliver {
                    to, id: Some(d), ..
                } => {
                    live.get_mut(&to.session).unwrap().insert(*d);
                }
                Effect::Close { to, .. } => {
                    live.remove(&to.session);
                }
                _ => {}
            }
        }
    }

    fn up(&self, id: u8) -> bool {
        self.nodes[usize::from(id)].core.is_some()
    }

    fn write(&mut self, id: u8, sync: bool) {
        let node = &mut self.nodes[usize::from(id)];
        if node.core.is_none() {
            return;
        }
        node.written = node.records.len();
        if let Some(after) = node.truncate.take() {
            // To the last batch boundary at or below `after`, as the node's
            // WAL thread cuts; never a copy of a committed record.
            let asked = after;
            let mut after = usize::try_from(after).unwrap();
            while after > 0
                && decode(&node.records[after]).unwrap().kind != journal::Kind::BatchHeader
            {
                after -= 1;
            }
            let around: Vec<(u64, u64, journal::Kind, bool)> = node.records
                [after.saturating_sub(2)..node.records.len().min(after + 6)]
                .iter()
                .filter(|r| !r.is_empty())
                .map(|r| {
                    let d = decode(r).unwrap();
                    (d.lsn, d.slot, d.kind, self.oracle.get(&d.lsn) == Some(r))
                })
                .collect();
            if let Some(index) = (after..node.records.len())
                .find(|&i| self.oracle.get(&(i as u64 + 1)) == Some(&node.records[i]))
            {
                let commit = node.core.as_ref().map(Voter::commit);
                self.fail(&format!(
                    "node {id}: cut to {after} (asked {asked}, its commit {commit:?}, cluster {}), removing its copy of committed lsn {}: around {around:?}",
                    self.commit,
                    index + 1
                ));
            }
            let node = &mut self.nodes[usize::from(id)];
            let chain = if after == 0 {
                Chain::genesis()
            } else {
                let next = decode(&node.records[after]).unwrap();
                let last = decode(&node.records[after - 1]).unwrap();
                Chain::resume(next.prev_hash, after as u64, last.slot)
            };
            node.records.truncate(after);
            node.written = after;
            node.durable = node.durable.min(after);
            self.stats.cuts += 1;
            node.core.as_mut().unwrap().on_truncated(chain);
            self.drain(id);
        }
        let node = &mut self.nodes[usize::from(id)];
        let core = node.core.as_mut().unwrap();
        if sync {
            node.durable = node.written;
            let durable = node.durable as u64;
            core.on_durable(durable);
            if let Some(slot) = node.promising.take() {
                node.promised = node.promised.max(slot);
                core.on_promised(slot);
            }
        }
        self.drain(id);
    }

    fn deliver(&mut self, index: usize) {
        // `remove`, not `swap_remove`: delivering one message must not move
        // the newest ahead of all the others.
        let (from, to, wire) = self.network.remove(index);
        if self.isolated.is_some_and(|i| i == from || i == to) || !self.up(to) {
            if matches!(wire, Wire::Forward(_)) {
                self.stats.forwards_dropped += 1;
            }
            return;
        }
        match wire {
            Wire::Cluster(message) => {
                if let Some(core) = self.nodes[usize::from(to)].core.as_mut() {
                    core.on_message(from, message);
                }
            }
            // One hop: a node that no longer sequences drops it.
            Wire::Forward(proposal) => self.propose(to, proposal),
        }
        self.drain(to);
    }

    fn propose(&mut self, at: u8, proposal: Proposal) {
        let Some(s) = self.nodes[usize::from(at)]
            .core
            .as_mut()
            .filter(|c| c.leading())
        else {
            self.stats.forwards_dropped += 1;
            return;
        };
        let mut body = Vec::new();
        proposal.encode(&mut body).unwrap();
        let kind = proposal.kind();
        // A full queue refuses; that is a loss before commit, which is
        // allowed.
        let properties = proposal.properties().to_vec();
        let _ = s.propose(cluster::Proposal {
            kind,
            properties,
            body,
        });
        if self.rng.chance(50) {
            s.flush().unwrap();
        }
        self.drain(at);
    }

    /// A gateway's proposal: to its own log if it sequences, else to the
    /// owner of the slot it promised.
    fn send(&mut self, id: u8, proposal: Proposal) {
        let Some(core) = self.nodes[usize::from(id)].core.as_ref() else {
            return;
        };
        if core.leading() {
            self.propose(id, proposal);
            return;
        }
        match core.owner() {
            Some(owner) if owner != id => self.network.push((id, owner, Wire::Forward(proposal))),
            _ => self.stats.forwards_dropped += 1,
        }
    }

    /// A session at a gateway does something.
    fn client(&mut self, id: u8) {
        if !self.up(id) {
            return;
        }
        let node = &mut self.nodes[usize::from(id)];
        if node.live.len() < 2 || self.rng.chance(2) {
            let session = node.next_session;
            node.next_session += 1;
            node.live.insert(session, BTreeSet::new());
        }
        let sessions: Vec<u64> = self.nodes[usize::from(id)].live.keys().copied().collect();
        let session = self.rng.pick(&sessions);
        let origin = Origin { node: id, session };
        self.request += 1;
        let request = self.request;
        let guarantee = if self.rng.chance(60) {
            G::AtLeastOnce
        } else {
            G::AtMostOnce
        };
        let proposal = match self.rng.below(40) {
            0..=15 => {
                let retain = self.rng.chance(20);
                let payload = if retain && self.rng.chance(20) {
                    Bytes::new()
                } else {
                    Bytes::from(format!("m{request}"))
                };
                Proposal::Publish {
                    origin,
                    request,
                    topic: Topic::new(Bytes::from_static(self.rng.pick(&TOPICS).as_bytes()))
                        .unwrap(),
                    payload,
                    // Opaque to `apply`: any bytes must come back as they went.
                    properties: Bytes::from(format!("p{request}")),
                    guarantee,
                    retain,
                }
            }
            16..=23 => Proposal::Subscribe {
                origin,
                request,
                filter: TopicFilter::new(Bytes::from_static(self.rng.pick(&FILTERS).as_bytes()))
                    .unwrap(),
                guarantee,
                options: Options {
                    no_local: self.rng.chance(20),
                    retain_as_published: self.rng.chance(30),
                    retain_handling: self.rng.pick(&[
                        RetainHandling::OnSubscribe,
                        RetainHandling::OnNewSubscription,
                        RetainHandling::Never,
                    ]),
                },
            },
            24..=25 => Proposal::Unsubscribe {
                origin,
                request,
                filter: TopicFilter::new(Bytes::from_static(self.rng.pick(&FILTERS).as_bytes()))
                    .unwrap(),
            },
            26..=38 => {
                let pending: Vec<u64> = self.nodes[usize::from(id)].live[&session]
                    .iter()
                    .copied()
                    .collect();
                if pending.is_empty() {
                    return;
                }
                let delivery = self.rng.pick(&pending);
                // Forgotten here once sent; the gateway retries until it
                // sees the effect, which an applied ack makes harmless.
                self.nodes[usize::from(id)]
                    .live
                    .get_mut(&session)
                    .unwrap()
                    .remove(&delivery);
                Proposal::Ack { origin, delivery }
            }
            _ => {
                self.nodes[usize::from(id)].live.remove(&session);
                Proposal::SessionGone { origin }
            }
        };
        self.send(id, proposal);
    }

    fn tick(&mut self) {
        for id in 0..NODES {
            if let Some(core) = self.nodes[usize::from(id)].core.as_mut() {
                core.flush().unwrap();
                core.tick();
                self.drain(id);
            }
        }
    }

    fn event(&mut self) {
        let id = self.rng.below(u64::from(NODES)) as u8;
        let mut roll = self.rng.below(100);
        if self.calm && matches!(roll, 75..=77 | 85..=86 | 94..) {
            roll = 50;
        }
        match roll {
            0..=29 => self.client(id),
            30..=42 => {
                if self.up(id) {
                    let sync = self.rng.chance(70);
                    self.write(id, sync);
                    if self.rng.chance(15) {
                        self.snapshot(id);
                    }
                }
            }
            43..=74 => {
                if !self.network.is_empty() {
                    let index = self.rng.below(self.network.len() as u64) as usize;
                    self.deliver(index);
                }
            }
            75..=77 => {
                if !self.network.is_empty() {
                    let index = self.rng.below(self.network.len() as u64) as usize;
                    if matches!(self.network.swap_remove(index).2, Wire::Forward(_)) {
                        self.stats.forwards_dropped += 1;
                    }
                }
            }
            78..=84 => self.tick(),
            85..=86 => {
                if self.up(id) {
                    let power = self.rng.chance(50);
                    self.crash(id, power);
                }
            }
            87..=93 => {
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

    fn settle(&mut self) {
        for _ in 0..10_000 {
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
            let Some(leader) = (0..NODES).find(|&id| {
                self.nodes[usize::from(id)]
                    .core
                    .as_ref()
                    .is_some_and(Voter::leading)
            }) else {
                continue;
            };
            let end = self.nodes[usize::from(leader)].core.as_ref().unwrap().end();
            let settled = self.nodes.iter().all(|n| {
                let core = n.core.as_ref().unwrap();
                core.commit() == end && n.state.applied() == end
            });
            if settled && self.network.is_empty() {
                return;
            }
        }
        let state: Vec<String> = self
            .nodes
            .iter()
            .map(|n| {
                let c = n.core.as_ref().unwrap();
                format!(
                    "leading {} slot {} end {} commit {} applied {} floor {} refused {}",
                    c.leading(),
                    c.slot(),
                    c.end(),
                    c.commit(),
                    n.state.applied(),
                    n.floor,
                    c.refused()
                )
            })
            .collect();
        self.fail(&format!(
            "the cluster did not settle after healing: {state:?}"
        ));
    }

    /// Everything comes back; then every session ends. Every node applies
    /// the whole log, agrees on the state, and holds no session.
    fn heal(&mut self) {
        self.isolated = None;
        for id in 0..NODES {
            if !self.up(id) {
                self.start(id);
            }
        }
        self.settle();
        // Clients acknowledge what they hold; each ack can let a waiting
        // message out, which is acknowledged in turn, until nothing waits.
        for _ in 0..100 {
            let mut sent = false;
            for id in 0..NODES {
                let pending: Vec<(u64, u64)> = self.nodes[usize::from(id)]
                    .live
                    .iter()
                    .flat_map(|(s, ds)| ds.iter().map(move |d| (*s, *d)))
                    .collect();
                for (session, delivery) in pending {
                    self.nodes[usize::from(id)]
                        .live
                        .get_mut(&session)
                        .unwrap()
                        .remove(&delivery);
                    self.send(
                        id,
                        Proposal::Ack {
                            origin: Origin { node: id, session },
                            delivery,
                        },
                    );
                    sent = true;
                }
            }
            if !sent {
                break;
            }
            self.settle();
        }
        for id in 0..NODES {
            let sessions: Vec<u64> = self.nodes[usize::from(id)].live.keys().copied().collect();
            for session in sessions {
                self.nodes[usize::from(id)].live.remove(&session);
                self.send(
                    id,
                    Proposal::SessionGone {
                        origin: Origin { node: id, session },
                    },
                );
            }
            // What a lost SessionGone left behind, the next reset takes: the
            // gateway retries until it sees the effect. Here, once more.
            let below = self.nodes[usize::from(id)].next_session;
            self.send(id, Proposal::NodeReset { node: id, below });
        }
        self.settle();
        let digests: Vec<u64> = self.nodes.iter().map(|n| n.state.digest()).collect();
        if digests.windows(2).any(|w| w[0] != w[1]) {
            self.fail("nodes disagree on the state of the whole log");
        }
        let left = self.nodes[0].state.sessions();
        if left != 0 {
            self.fail(&format!("{left} sessions left after every session ended"));
        }
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
    }
    world.step = STEPS;
    world.heal();
    world.stats
}

#[test]
fn effects_follow_commit_and_mean_what_the_committed_log_means() {
    let mut total = Stats::default();
    for seed in seeds() {
        let s = run(seed);
        total.committed += s.committed;
        total.effects += s.effects;
        total.deliveries += s.deliveries;
        total.acked += s.acked;
        total.closes += s.closes;
        total.refused += s.refused;
        total.restarts += s.restarts;
        total.power_losses += s.power_losses;
        total.forwards_dropped += s.forwards_dropped;
        total.snapshots += s.snapshots;
        total.snapshot_restarts += s.snapshot_restarts;
        total.compactions += s.compactions;
        total.compacted_restarts += s.compacted_restarts;
        total.takeovers += s.takeovers;
        total.cuts += s.cuts;
        total.compactions_during_takeover += s.compactions_during_takeover;
    }
    println!(
        "committed {} effects checked {} deliveries {} acked {} slow-consumer closes {} subscriptions refused {} restarts {} power losses {} forwards dropped {} snapshots {} restarts from a snapshot {} compactions {} restarts of a compacted log {} takeovers {} cuts {} compactions during a takeover {}",
        total.committed,
        total.effects,
        total.deliveries,
        total.acked,
        total.closes,
        total.refused,
        total.restarts,
        total.power_losses,
        total.forwards_dropped,
        total.snapshots,
        total.snapshot_restarts,
        total.compactions,
        total.compacted_restarts,
        total.takeovers,
        total.cuts,
        total.compactions_during_takeover
    );
    if std::env::var("STRAUM_SIM_SEED").is_err() {
        assert!(total.committed > 0 && total.deliveries > 0 && total.acked > 0);
        assert!(total.closes > 0 && total.refused > 0 && total.restarts > 0);
        assert!(total.power_losses > 0 && total.forwards_dropped > 0);
        assert!(total.snapshot_restarts > 0 && total.compacted_restarts > 0);
        assert!(total.takeovers > 0 && total.cuts > 0);
    }
}
