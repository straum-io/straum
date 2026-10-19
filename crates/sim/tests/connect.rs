//! Step 7c, stage 2 (ADR-0008 "Step 7c design", D-146 A): client ids over
//! the committed log, with gateways that lag it. No MQTT and no cluster
//! faults: `apply` sees only the committed order, which `cluster.rs` and
//! `takeover.rs` check on their own. What matters here is what that order
//! can hold:
//!
//! * three gateways, each applying the committed log some positions behind
//!   it, propose with `seen` at their own applied position;
//! * proposals commit after a delay, one origin's in the order proposed;
//!   some are given up and never commit, and some commit late, as a
//!   proposal held across a slot change does (D-109 B);
//! * clients connect through any gateway, reconnect elsewhere while their
//!   old connection is still open, disconnect cleanly or not, and their
//!   gateways restart;
//! * a node restarts from a snapshot taken mid-run.
//!
//! Checked after every record: invariant A (the attachment is the origin of
//! the last `Connect` that passed, unless it was since detached or its
//! session deleted; `origins` its exact inverse), invariant B (no `Connect`
//! changes a client id's state if one of its fence events committed after
//! its `seen`), and the bounds: tombstones, persisted entries, detached
//! bytes. Each of the ADR's nine scenarios is counted, and every one must
//! occur in the default seeds.
//!
//! Measured: how often a client's newest CONNECT is refused as stale — the
//! fence's cost, which sizes `connect_window` and `max_connect_tombstones`
//! (`connect_refusal_rates`, ignored by default: `cargo test -p sim --release
//! --test connect -- --ignored --nocapture`).
//!
//! `STRAUM_SIM_SEEDS=<n>` sweeps more seeds; `STRAUM_SIM_SEED=<n>` runs one.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines
)]

use std::collections::BTreeMap;

use apply::{ClientEffect, ConnectLimits, Effect, Limits, Origin, Proposal, Refusal, State};
use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee as G, Topic, TopicFilter};
use journal::{Chain, Entry, decode};

const GATEWAYS: u8 = 3;
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
                .unwrap_or(200)
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

/// What a run is made of.
#[derive(Debug, Clone, Copy)]
struct Params {
    limits: Limits,
    connect: ConnectLimits,
    /// A gateway applies up to this many positions behind the commit.
    lag: u64,
    /// A proposal commits up to this many steps after it is proposed; one
    /// step commits at most one record.
    delay: u64,
    /// Percent of proposals held across a slot change: they commit
    /// `late` steps later.
    held: u64,
    late: u64,
    /// Percent of connects that persist.
    persist: u64,
    /// Client ids: with one action a step, each client changes about
    /// once every `clients` steps.
    clients: usize,
}

fn params() -> Params {
    Params {
        limits: Limits {
            max_inflight: 2,
            max_backlog: 3,
            max_backlog_bytes: 6,
            max_total_queued_bytes: 40,
            ..Limits::default()
        },
        connect: ConnectLimits {
            connect_window: 64,
            max_connect_tombstones: 4,
            max_persisted_sessions: 4,
            max_detached_queued_bytes: 8,
        },
        lag: 12,
        delay: 8,
        held: 3,
        late: 200,
        persist: 50,
        clients: 8,
    }
}

struct Pending {
    origin: Origin,
    proposal: Proposal,
    due: u64,
}

/// One connection a client made: the client, and which of its attempts.
#[derive(Clone, Copy)]
struct Conn {
    client: usize,
    attempt: u64,
    open: bool,
    closed: bool,
}

#[derive(Default, Debug)]
struct Stats {
    records: u64,
    connects: u64,
    connected: u64,
    /// Stale refusals of a client's newest CONNECT: the fence's cost.
    newest_refused: u64,
    /// Stale refusals of a CONNECT the same client had already superseded.
    older_refused: u64,
    newest_committed: u64,
    quota: u64,
    // The nine scenarios.
    racing: u64,
    late_after_slot_change: u64,
    late_after_removal: u64,
    floor_rises: u64,
    disconnect_racing: u64,
    from_taken_over: u64,
    restarted_detached: u64,
    quota_then_resumed: u64,
    deleted_then_connected: u64,
    max_tombstones: usize,
}

/// Invariants A and B, from effects and from which entries exist, never
/// from the fence's own bookkeeping.
#[derive(Default)]
struct Oracle {
    expected: BTreeMap<usize, Origin>,
    fenced: BTreeMap<usize, u64>,
    /// Per client, the lsn its entry was last removed at.
    removed: BTreeMap<usize, u64>,
}

fn name(client: usize) -> Bytes {
    Bytes::from(format!("client-{client}"))
}

struct World {
    rng: Rng,
    p: Params,
    state: State,
    chain: Chain,
    log: Vec<Vec<u8>>,
    pending: Vec<Pending>,
    applied: [u64; GATEWAYS as usize],
    next_session: [u64; GATEWAYS as usize],
    conns: BTreeMap<Origin, Conn>,
    attempts: Vec<u64>,
    /// Per client, its newest connection.
    current: Vec<Option<Origin>>,
    oracle: Oracle,
    stats: Stats,
    /// A new persisted session was refused at the limit.
    quota_seen: bool,
    /// Clients whose detached session was deleted.
    deleted: Vec<bool>,
    seed: u64,
}

impl World {
    fn new(seed: u64, p: Params) -> Self {
        Self {
            rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1),
            p,
            state: State::with_connect(p.limits, p.connect),
            chain: Chain::genesis(),
            log: Vec::new(),
            pending: Vec::new(),
            applied: [0; GATEWAYS as usize],
            next_session: [1; GATEWAYS as usize],
            conns: BTreeMap::new(),
            attempts: vec![0; p.clients],
            current: vec![None; p.clients],
            oracle: Oracle::default(),
            stats: Stats::default(),
            quota_seen: false,
            deleted: vec![false; p.clients],
            seed,
        }
    }

    fn fail(&self, what: &str) -> ! {
        panic!(
            "seed {}: {what}\n(reproduce: STRAUM_SIM_SEED={} cargo test -p sim --test connect)",
            self.seed, self.seed
        )
    }

    fn propose(&mut self, step: u64, origin: Origin, proposal: Proposal) {
        let mut delay = self.rng.below(self.p.delay + 1);
        if self.rng.chance(self.p.held) {
            delay += self.p.late;
        }
        self.pending.push(Pending {
            origin,
            proposal,
            due: step + delay,
        });
    }

    fn connect(&mut self, step: u64) {
        let client = self.rng.below(self.p.clients as u64) as usize;
        let gateway = self.rng.below(u64::from(GATEWAYS)) as usize;
        let origin = Origin {
            node: gateway as u8 + 1,
            session: self.next_session[gateway],
        };
        self.next_session[gateway] += 1;
        // Scenario 1: a newer CONNECT while an older one is still pending.
        if self.current[client].is_some_and(|o| self.pending.iter().any(|p| p.origin == o)) {
            self.stats.racing += 1;
        }
        self.attempts[client] += 1;
        self.current[client] = Some(origin);
        self.conns.insert(
            origin,
            Conn {
                client,
                attempt: self.attempts[client],
                open: false,
                closed: false,
            },
        );
        let persist = self.rng.chance(self.p.persist);
        let clean_start = self.rng.chance(30);
        let proposal = Proposal::Connect {
            origin,
            client_id: ClientId::new(name(client)).unwrap(),
            seen: self.applied[gateway],
            clean_start,
            persist,
        };
        self.stats.connects += 1;
        self.propose(step, origin, proposal);
    }

    fn some_conn(&mut self, open: bool) -> Option<Origin> {
        let candidates: Vec<Origin> = self
            .conns
            .iter()
            .filter(|(_, c)| !c.closed && (!open || c.open))
            .map(|(o, _)| *o)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        Some(candidates[self.rng.below(candidates.len() as u64) as usize])
    }

    fn act(&mut self, step: u64) {
        match self.rng.below(100) {
            0..=29 => self.connect(step),
            30..=44 => {
                if let Some(origin) = self.some_conn(false) {
                    // Scenario 5: a Disconnect while this client's Connect
                    // through another gateway is pending.
                    let client = self.conns[&origin].client;
                    if self.current[client]
                        .is_some_and(|o| o != origin && self.pending.iter().any(|p| p.origin == o))
                    {
                        self.stats.disconnect_racing += 1;
                    }
                    self.conns.get_mut(&origin).unwrap().closed = true;
                    self.propose(step, origin, Proposal::Disconnect { origin });
                }
            }
            45..=49 => {
                if let Some(origin) = self.some_conn(false) {
                    self.conns.get_mut(&origin).unwrap().closed = true;
                    self.propose(step, origin, Proposal::SessionGone { origin });
                }
            }
            50..=69 => {
                // Any origin, taken over or not: scenario 6's records.
                if let Some(origin) = self.some_conn(false) {
                    let proposal = Proposal::Subscribe {
                        origin,
                        request: step,
                        filter: TopicFilter::new(Bytes::from_static(b"t")).unwrap(),
                        guarantee: if self.rng.chance(50) {
                            G::AtLeastOnce
                        } else {
                            G::AtMostOnce
                        },
                        options: apply::Options::default(),
                    };
                    self.propose(step, origin, proposal);
                }
            }
            70..=97 => {
                if let Some(origin) = self.some_conn(true) {
                    let proposal = Proposal::Publish {
                        origin,
                        request: step,
                        topic: Topic::new(Bytes::from_static(b"t")).unwrap(),
                        payload: Bytes::from_static(b"m"),
                        properties: Bytes::new(),
                        guarantee: if self.rng.chance(50) {
                            G::AtLeastOnce
                        } else {
                            G::AtMostOnce
                        },
                        retain: false,
                    };
                    self.propose(step, origin, proposal);
                }
            }
            _ => {
                // A gateway restarts: its sessions reset through the log.
                let gateway = self.rng.below(u64::from(GATEWAYS)) as usize;
                let node = gateway as u8 + 1;
                let below = self.next_session[gateway];
                for (o, c) in &mut self.conns {
                    if o.node == node {
                        c.closed = true;
                    }
                }
                let origin = Origin { node, session: 0 };
                self.propose(step, origin, Proposal::NodeReset { node, below });
            }
        }
    }

    /// Commits the first pending proposal that is due and has no earlier
    /// one of its origin before it; a few are given up instead.
    fn commit(&mut self, step: u64) {
        let Some(index) = (0..self.pending.len()).find(|&i| {
            self.pending[i].due <= step
                && !self.pending[..i]
                    .iter()
                    .any(|p| p.origin == self.pending[i].origin)
        }) else {
            return;
        };
        let pending = self.pending.remove(index);
        if self.rng.chance(2) {
            return;
        }
        let mut body = Vec::new();
        pending.proposal.encode(&mut body).unwrap();
        let mut records = Vec::new();
        self.chain
            .batch(
                1,
                &[Entry {
                    kind: pending.proposal.kind(),
                    properties: pending.proposal.properties(),
                    body: &body,
                }],
                &mut records,
            )
            .unwrap();
        let (mut effects, mut clients) = (Vec::new(), Vec::new());
        let mut lsn = 0;
        let before = self.present();
        let (floor, _) = self.state.connect_fence();
        for record in &records {
            let decoded = decode(record).unwrap();
            lsn = decoded.lsn;
            if let Err(e) = self
                .state
                .apply_connect(&decoded, &mut effects, &mut clients)
            {
                self.fail(&format!("lsn {lsn} does not apply: {e}"));
            }
        }
        self.log.extend(records);
        self.stats.records += 1;
        if self.state.connect_fence().0 > floor {
            self.stats.floor_rises += 1;
        }
        self.observe(&pending, lsn, &effects, &clients);
        self.check(before, &pending.proposal, lsn, &effects, &clients);
    }

    fn present(&self) -> Vec<bool> {
        (0..self.p.clients)
            .map(|c| self.state.client(&name(c)).is_some())
            .collect()
    }

    /// What the effects tell the clients and the counters.
    fn observe(
        &mut self,
        pending: &Pending,
        lsn: u64,
        effects: &[Effect],
        clients: &[ClientEffect],
    ) {
        for effect in effects {
            if let Effect::Close { to, .. } = effect {
                if let Some(c) = self.conns.get_mut(to) {
                    c.closed = true;
                }
            }
        }
        for effect in clients {
            match effect {
                ClientEffect::Connected {
                    to,
                    session_present,
                    ..
                } => {
                    let conn = self.conns.get_mut(to).unwrap();
                    conn.open = true;
                    let client = conn.client;
                    self.stats.connected += 1;
                    if conn.attempt == self.attempts[client] {
                        self.stats.newest_committed += 1;
                    }
                    // Scenario 8: at the limit, a resumption still passes.
                    let at_limit =
                        self.state.persisted().0 >= self.p.connect.max_persisted_sessions;
                    if *session_present && self.quota_seen && at_limit {
                        self.stats.quota_then_resumed += 1;
                    }
                    if !*session_present && self.deleted[client] {
                        self.stats.deleted_then_connected += 1;
                        self.deleted[client] = false;
                    }
                }
                ClientEffect::ConnectRefused { to, reason, .. } => {
                    let conn = self.conns.get_mut(to).unwrap();
                    conn.closed = true;
                    let client = conn.client;
                    let newest = conn.attempt == self.attempts[client];
                    match reason {
                        Refusal::Stale => {
                            let Proposal::Connect { seen, .. } = pending.proposal else {
                                unreachable!()
                            };
                            if lsn.saturating_sub(seen) > self.p.connect.connect_window {
                                self.stats.late_after_slot_change += 1;
                            } else if self.oracle.removed_after(client, seen) {
                                self.stats.late_after_removal += 1;
                            }
                            if newest {
                                self.stats.newest_committed += 1;
                                self.stats.newest_refused += 1;
                            } else {
                                self.stats.older_refused += 1;
                            }
                        }
                        Refusal::Quota => {
                            self.stats.quota += 1;
                            self.quota_seen = true;
                        }
                    }
                }
                ClientEffect::TakenOver { to, .. } => {
                    if let Some(c) = self.conns.get_mut(to) {
                        c.closed = true;
                    }
                }
                ClientEffect::Refused { to, .. } => {
                    self.stats.from_taken_over += 1;
                    if let Some(c) = self.conns.get_mut(to) {
                        c.closed = true;
                    }
                }
                _ => {}
            }
        }
    }

    fn check(
        &mut self,
        before: Vec<bool>,
        proposal: &Proposal,
        lsn: u64,
        effects: &[Effect],
        clients: &[ClientEffect],
    ) {
        for effect in clients {
            if let ClientEffect::Connected { to, .. } = effect {
                let Proposal::Connect { seen, .. } = proposal else {
                    self.fail(&format!("Connected from {proposal:?}"));
                };
                let client = self.conns[to].client;
                let last = self.oracle.fenced.get(&client).copied().unwrap_or(0);
                if last > *seen {
                    self.fail(&format!(
                        "invariant B: client {client} changed at lsn {last}, after its Connect's seen {seen} (lsn {lsn})"
                    ));
                }
                self.oracle.fenced.insert(client, lsn);
                self.oracle.expected.insert(client, *to);
            }
        }
        for effect in effects {
            if let Effect::Close { to, .. } = effect {
                self.oracle.expected.retain(|_, o| o != to);
            }
        }
        let detached = |o: &Origin| match proposal {
            Proposal::Disconnect { origin } | Proposal::SessionGone { origin } => origin == o,
            Proposal::NodeReset { node, below } => o.node == *node && o.session < *below,
            _ => false,
        };
        self.oracle.expected.retain(|_, o| !detached(o));
        for (c, was) in before.iter().enumerate() {
            if self.state.client(&name(c)).is_none() {
                self.oracle.expected.remove(&c);
                if *was {
                    self.oracle.fenced.insert(c, lsn);
                    self.oracle.removed.insert(c, lsn);
                    // Scenario 9: this record deleted a detached session.
                    if clients
                        .iter()
                        .any(|e| matches!(e, ClientEffect::SessionDeleted { .. }))
                    {
                        self.deleted[c] = true;
                    }
                }
            }
        }
        for c in 0..self.p.clients {
            let actual = self.state.client(&name(c)).and_then(|v| v.attached);
            if actual != self.oracle.expected.get(&c).copied() {
                self.fail(&format!(
                    "invariant A for client {c} at lsn {lsn}: attached {actual:?}, expected {:?}",
                    self.oracle.expected.get(&c)
                ));
            }
            if let Some(o) = actual {
                if self.state.attached_to(o) != Some(&name(c)[..]) {
                    self.fail(&format!("origins is not the inverse at {o:?}"));
                }
            }
        }
        let limits = self.p.connect;
        let (_, tombstones) = self.state.connect_fence();
        let (persisted, detached_bytes) = self.state.persisted();
        if tombstones > limits.max_connect_tombstones
            || persisted > limits.max_persisted_sessions
            || detached_bytes > limits.max_detached_queued_bytes
        {
            self.fail(&format!(
                "a bound exceeded at lsn {lsn}: tombstones {tombstones}, persisted {persisted}, detached bytes {detached_bytes}"
            ));
        }
        self.stats.max_tombstones = self.stats.max_tombstones.max(tombstones);
    }

    /// Scenario 7: a node restarts from a snapshot and goes on; it must
    /// hold exactly the state of one that applied the whole log.
    fn restart(&mut self) {
        let detached = (0..self.p.clients).any(|c| {
            self.state
                .client(&name(c))
                .is_some_and(|v| v.attached.is_none())
        });
        let mut bytes = Vec::new();
        self.state.encode(&mut bytes);
        let restarted = State::decode_connect(self.p.limits, self.p.connect, &bytes)
            .unwrap_or_else(|e| self.fail(&format!("the snapshot does not decode: {e}")));
        if restarted != self.state || restarted.digest() != self.state.digest() {
            self.fail("a restarted node holds another state");
        }
        if detached {
            self.stats.restarted_detached += 1;
        }
        self.state = restarted;
    }

    fn step(&mut self, step: u64) {
        let commit = self.state.applied();
        for gateway in 0..GATEWAYS as usize {
            let behind = self.rng.below(self.p.lag + 1);
            self.applied[gateway] = self.applied[gateway].max(commit.saturating_sub(behind));
        }
        self.act(step);
        self.commit(step);
        if self.rng.chance(1) {
            self.restart();
        }
    }
}

impl Oracle {
    /// Whether `client`'s entry was removed after `seen`: a fence event
    /// that is not a `Connect`.
    fn removed_after(&self, client: usize, seen: u64) -> bool {
        self.removed.get(&client).is_some_and(|&r| r > seen)
    }
}

fn run(seed: u64, p: Params) -> Stats {
    let mut world = World::new(seed, p);
    for step in 0..STEPS {
        world.step(step);
    }
    // Everything still pending commits, or is given up.
    let mut step = STEPS;
    while !world.pending.is_empty() {
        world.commit(step);
        step += 1;
        if step > STEPS + 100_000 {
            world.fail("pending proposals never committed");
        }
    }
    // The whole log replayed by a fresh node gives the same state.
    let mut replayed = State::with_connect(p.limits, p.connect);
    let (mut effects, mut clients) = (Vec::new(), Vec::new());
    for record in &world.log {
        replayed
            .apply_connect(&decode(record).unwrap(), &mut effects, &mut clients)
            .unwrap();
    }
    if replayed != world.state {
        world.fail("a node replaying the log holds another state");
    }
    world.stats
}

#[test]
fn client_ids_keep_invariants_a_and_b_and_every_scenario_occurs() {
    let mut total = Stats::default();
    for seed in seeds() {
        let s = run(seed, params());
        total.records += s.records;
        total.connects += s.connects;
        total.connected += s.connected;
        total.newest_refused += s.newest_refused;
        total.older_refused += s.older_refused;
        total.newest_committed += s.newest_committed;
        total.quota += s.quota;
        total.racing += s.racing;
        total.late_after_slot_change += s.late_after_slot_change;
        total.late_after_removal += s.late_after_removal;
        total.floor_rises += s.floor_rises;
        total.disconnect_racing += s.disconnect_racing;
        total.from_taken_over += s.from_taken_over;
        total.restarted_detached += s.restarted_detached;
        total.quota_then_resumed += s.quota_then_resumed;
        total.deleted_then_connected += s.deleted_then_connected;
        total.max_tombstones = total.max_tombstones.max(s.max_tombstones);
    }
    eprintln!("{total:#?}");
    let scenarios = [
        ("two Connects racing for one client id", total.racing),
        (
            "a late Connect after a slot change",
            total.late_after_slot_change,
        ),
        (
            "a late Connect after the entry was removed",
            total.late_after_removal,
        ),
        ("tombstone overflow raising the floor", total.floor_rises),
        ("Disconnect racing Connect", total.disconnect_racing),
        ("records from a taken-over origin", total.from_taken_over),
        ("restart while detached", total.restarted_detached),
        (
            "the limit, refusing and then resuming",
            total.quota_then_resumed,
        ),
        (
            "detached overflow deleting a session, then the next CONNACK",
            total.deleted_then_connected,
        ),
    ];
    for (scenario, count) in scenarios {
        assert!(count > 0, "scenario never occurred: {scenario}");
    }
    assert!(total.max_tombstones <= params().connect.max_connect_tombstones);
}

/// The fence's cost: the share of clients' newest CONNECTs refused as
/// stale, by window, tombstone queue and gateway lag. Printed, not
/// asserted; it sizes `connect_window` and `max_connect_tombstones`.
#[test]
#[ignore = "a measurement: run with --ignored --nocapture"]
fn connect_refusal_rates() {
    eprintln!(
        "clients window tombstones lag delay held | newest refused / newest committed | older refused | floor rises"
    );
    for &clients in &[8usize, 64, 512] {
        for &(window, tombstones) in &[(16u64, 4usize), (64, 4), (64, 64), (256, 64), (1024, 1024)]
        {
            for &lag in &[4u64, 16, 64] {
                let mut p = params();
                p.clients = clients;
                p.connect.connect_window = window;
                p.connect.max_connect_tombstones = tombstones;
                p.lag = lag;
                let (mut newest, mut refused, mut older, mut floor) = (0, 0, 0, 0);
                for seed in seeds() {
                    let s = run(seed, p);
                    newest += s.newest_committed;
                    refused += s.newest_refused;
                    older += s.older_refused;
                    floor += s.floor_rises;
                }
                eprintln!(
                    "{clients:>7} {window:>6} {tombstones:>10} {lag:>3} {:>5} {:>4} | {refused:>6} / {newest:<7} = {:.4} | {older:>6} | {floor}",
                    p.delay,
                    p.held,
                    refused as f64 / newest.max(1) as f64,
                );
            }
        }
    }
}
