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
//! session may be left in the state (CLAUDE.md §7, "closing a session
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
use cluster::{Config, Effect as Step, Message, Replica, Sequencer};
use domain::{DeliveryGuarantee as G, Topic, TopicFilter};
use journal::{Chain, Verifier, decode};

const NODES: u8 = 3;
const SEQUENCER: u8 = 0;
const STEPS: usize = 1500;
const TOPICS: [&str; 4] = ["a", "a/b", "a/c", "z"];
const FILTERS: [&str; 5] = ["a", "a/+", "a/#", "#", "z"];
/// Small, so the simulation reaches them.
const LIMITS: Limits = Limits {
    max_subscriptions_per_session: 3,
    max_subscriptions: 12,
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
        owner: SEQUENCER,
        slot: 1,
        window: 24,
        max_queue: 32,
        max_batch: 4,
        recent_batches: 4,
        max_uncommitted: 1_000_000,
    }
}

enum Core {
    Sequencer(Sequencer),
    Replica(Replica),
}

impl Core {
    fn take(&mut self) -> Vec<Step> {
        match self {
            Core::Sequencer(s) => s.take_effects(),
            Core::Replica(r) => r.take_effects(),
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

enum Wire {
    Cluster(Message),
    /// A gateway's proposal on its way to the sequencer.
    Forward(Proposal),
}

struct Node {
    core: Option<Core>,
    /// Every record handed to this node's log, lsn i + 1 at index i.
    records: Vec<Vec<u8>>,
    written: usize,
    durable: usize,
    state: State,
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
        if record.kind == journal::Kind::BatchHeader {
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
                if let Some(s) = self.session(origin) {
                    if let Some(at) = s.inflight.iter().position(|(id, _)| *id == delivery) {
                        s.inflight.remove(at);
                        out.push(Effect::Acked {
                            to: origin,
                            delivery,
                            lsn,
                        });
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
                    records: Vec::new(),
                    written: 0,
                    durable: 0,
                    state: State::new(LIMITS),
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
        let mut verifier = Verifier::new(Chain::genesis());
        for record in &node.records {
            verifier.feed(record).unwrap();
        }
        let tail = verifier.finish();
        node.records
            .truncate(usize::try_from(tail.chain.next_lsn() - 1).unwrap());
        node.written = node.records.len();
        // Reopening makes what it recovered durable (D-43).
        node.durable = node.records.len();
        node.state = State::new(LIMITS);
        node.incarnation += 1;
        node.next_session = node.incarnation * 1000;
        node.live.clear();
        let below = node.next_session;
        node.core = Some(if id == SEQUENCER {
            Core::Sequencer(Sequencer::new(config(id), tail.chain))
        } else {
            Core::Replica(Replica::new(config(id), tail.chain))
        });
        // Whatever the previous incarnation's sessions left goes.
        self.send(id, Proposal::NodeReset { node: id, below });
        self.drain(id);
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
                Some(core) => core.take(),
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
                    Step::Read { to, from } => {
                        let node = &self.nodes[usize::from(id)];
                        let start = usize::try_from(from - 1).unwrap();
                        let header = decode(&node.records[start]).unwrap();
                        let count = journal::BatchHeader::decode(header.body).unwrap().records;
                        let batch =
                            node.records[start..=start + usize::try_from(count).unwrap()].to_vec();
                        if let Some(Core::Sequencer(s)) = self.nodes[usize::from(id)].core.as_mut()
                        {
                            s.on_read(to, batch);
                        }
                    }
                    Step::Committed(lsn) if id == SEQUENCER => self.freeze(lsn),
                    Step::Committed(_) | Step::ReadDone { .. } | Step::Lost { .. } => {}
                }
            }
        }
        self.apply(id);
    }

    fn freeze(&mut self, lsn: u64) {
        if lsn <= self.commit {
            return;
        }
        for at in self.commit + 1..=lsn {
            let record = self.nodes[usize::from(SEQUENCER)].records
                [usize::try_from(at - 1).unwrap()]
            .clone();
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
            if let Err(e) = node.state.apply(&decode(&bytes).unwrap(), &mut effects) {
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
            .filter(|n| n.durable > index && n.records.get(index).is_some_and(|r| r == bytes))
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
        let Some(core) = node.core.as_mut() else {
            return;
        };
        node.written = node.records.len();
        if sync {
            node.durable = node.written;
            let durable = node.durable as u64;
            match core {
                Core::Sequencer(s) => s.on_durable(durable),
                Core::Replica(r) => r.on_durable(durable),
            }
        }
        self.drain(id);
    }

    fn deliver(&mut self, index: usize) {
        let (from, to, wire) = self.network.swap_remove(index);
        if self.isolated.is_some_and(|i| i == from || i == to) || !self.up(to) {
            if matches!(wire, Wire::Forward(_)) {
                self.stats.forwards_dropped += 1;
            }
            return;
        }
        match wire {
            Wire::Cluster(message) => match self.nodes[usize::from(to)].core.as_mut() {
                Some(Core::Sequencer(s)) => s.on_message(from, message),
                Some(Core::Replica(r)) => r.on_message(from, message),
                None => {}
            },
            Wire::Forward(proposal) => self.propose(proposal),
        }
        self.drain(to);
    }

    fn propose(&mut self, proposal: Proposal) {
        let Some(Core::Sequencer(s)) = self.nodes[usize::from(SEQUENCER)].core.as_mut() else {
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
        self.drain(SEQUENCER);
    }

    fn send(&mut self, id: u8, proposal: Proposal) {
        if id == SEQUENCER {
            self.propose(proposal);
        } else {
            self.network.push((id, SEQUENCER, Wire::Forward(proposal)));
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
        if let Some(Core::Sequencer(s)) = self.nodes[usize::from(SEQUENCER)].core.as_mut() {
            s.flush().unwrap();
            s.tick();
            self.drain(SEQUENCER);
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
            let end = self.nodes[usize::from(SEQUENCER)]
                .core
                .as_ref()
                .unwrap()
                .end();
            let settled = self.nodes.iter().all(|n| {
                let core = n.core.as_ref().unwrap();
                core.commit() == end && n.state.applied() == end
            });
            if settled && self.network.is_empty() {
                return;
            }
        }
        self.fail("the cluster did not settle after healing");
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
    }
    println!(
        "committed {} effects checked {} deliveries {} acked {} slow-consumer closes {} subscriptions refused {} restarts {} power losses {} forwards dropped {}",
        total.committed,
        total.effects,
        total.deliveries,
        total.acked,
        total.closes,
        total.refused,
        total.restarts,
        total.power_losses,
        total.forwards_dropped
    );
    if std::env::var("STRAUM_SIM_SEED").is_err() {
        assert!(total.committed > 0 && total.deliveries > 0 && total.acked > 0);
        assert!(total.closes > 0 && total.refused > 0 && total.restarts > 0);
        assert!(total.power_losses > 0 && total.forwards_dropped > 0);
    }
}
