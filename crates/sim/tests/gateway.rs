//! MQTT clients against three nodes (ADR-0008 step 3b-2): each node is the
//! `cluster` core, the apply function and a `gateway`; clients speak MQTT
//! 3.1.1 or 5 through `codec`, over connections that arrive in order (TCP)
//! and can drop. Proposals travel to the sequencer over a network that drops
//! and reorders; nodes crash and lose power.
//!
//! Checked at every packet a gateway sends:
//!
//! * **4** — a PUBACK, SUBACK, UNSUBACK or delivered PUBLISH leaves a
//!   gateway only while it handles a committed effect, and that effect's
//!   record is durable on a majority with the committed content. Nothing of
//!   the sort leaves while the gateway reads client bytes.
//! * **7** within a gateway: never two open sessions with one client id.
//! * Order: a subscriber receives one publisher's messages on one topic in
//!   the order they were published.
//!
//! At the end the network heals, clients acknowledge everything, every
//! QoS 1 publish of a connected client is acknowledged, and once every
//! client has gone no node's state holds a session and no gateway is
//! retrying anything.
//!
//! The log is in memory and never compacted (state copy: step 7b).
//!
//! `STRAUM_SIM_SEEDS=<n>` sweeps more seeds; `STRAUM_SIM_SEED=<n>` runs one.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    clippy::large_enum_variant
)]

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU16;
use std::time::Duration;

use apply::{Limits, Proposal, State};
use bytes::{Bytes, BytesMut};
use cluster::{Config, Effect as Step, Message, Replica, Sequencer};
use codec::{
    Connect, ConnectProperties, Packet, PacketId, PubAck, Publish, Qos, ReasonCode, Sender,
    Subscribe, SubscriptionOptions, Unsubscribe, Version,
};
use domain::{ClientId, Topic, TopicFilter};
use gateway::{Action, Gateway};
use journal::{Chain, Verifier, decode};

const NODES: u8 = 3;
const SEQUENCER: u8 = 0;
const STEPS: usize = 2000;
const CLIENTS: usize = 6;
const TOPICS: [&str; 3] = ["a", "a/b", "z"];
const FILTERS: [&str; 4] = ["a", "a/+", "#", "z"];
const TICK: Duration = Duration::from_millis(50);
const LIMITS: Limits = Limits {
    max_subscriptions_per_session: 4,
    max_subscriptions: 64,
    max_retained: 8,
    max_retained_bytes: 1024,
    max_inflight: 4,
    max_backlog: 8,
    max_backlog_bytes: 512,
    max_total_queued_bytes: 2048,
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

fn cluster_config(id: u8) -> Config {
    Config {
        id,
        nodes: (0..NODES).collect(),
        owner: SEQUENCER,
        slot: 1,
        window: 24,
        max_queue: 64,
        max_batch: 8,
        recent_batches: 8,
        max_uncommitted: 1_000_000,
        takeover_ticks: 3,
    }
}

fn gateway_config(node: u8, first_session: u64) -> gateway::Config {
    gateway::Config {
        node,
        first_session,
        codec: codec::Limits::default(),
        max_connections: 16,
        connect_timeout: Duration::from_secs(5),
        max_pending: 8,
        max_pending_bytes: 4096,
        max_total_pending_bytes: 16_384,
        commit_timeout: Duration::from_secs(30),
        retry_interval: Duration::from_millis(500),
        receive_maximum: NonZeroU16::new(8).unwrap(),
        topic_alias_maximum: 4,
        max_waiting: 32,
        max_waiting_bytes: 4096,
        max_total_waiting_bytes: 16_384,
        max_retries: 64,
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
    Forward(Proposal),
}

struct Node {
    core: Option<Core>,
    gateway: Option<Gateway>,
    records: Vec<Vec<u8>>,
    written: usize,
    durable: usize,
    state: State,
    incarnation: u64,
}

struct Client {
    client_id: &'static str,
    /// (node, connection) while connected.
    conn: Option<(u8, u64)>,
    version: Version,
    connected: bool,
    /// Bytes from the gateway, in order.
    inbox: BytesMut,
    next_packet: u16,
    seq: u64,
    /// The last sequence seen per (publisher, topic) on this connection.
    seen: HashMap<(String, String), u64>,
    /// QoS 1 deliveries to acknowledge.
    to_ack: Vec<PacketId>,
    /// QoS 1 publishes not yet acknowledged to it.
    unacked: Vec<u16>,
}

#[derive(Default)]
struct Stats {
    committed: u64,
    packets_checked: u64,
    deliveries: u64,
    pubacks: u64,
    properties_checked: u64,
    takeovers: u64,
    restarts: u64,
    power_losses: u64,
    v5: u64,
    v311: u64,
}

struct World {
    seed: u64,
    step: usize,
    now: Duration,
    rng: Rng,
    nodes: Vec<Node>,
    network: Vec<(u8, u8, Wire)>,
    isolated: Option<u8>,
    calm: bool,
    /// Proposals the sequencer node's own gateway made, handled after the
    /// step that made them: handling them at once applied later records
    /// while the gateway was still being handed an earlier record's
    /// effects, which reordered them (a harness bug the ordering check
    /// found). A driver handles effects one after another.
    loopback: std::collections::VecDeque<Proposal>,
    oracle: BTreeMap<u64, Vec<u8>>,
    commit: u64,
    clients: Vec<Client>,
    /// Connection → client.
    owners: HashMap<u64, usize>,
    next_conn: u64,
    stats: Stats,
}

/// Packets a gateway may only send because of a committed effect.
/// The properties an MQTT 5 publisher attaches to `payload`: derived from it,
/// so a subscriber can tell what it should have received.
fn properties_for(version: Version, payload: &str) -> Option<Box<codec::PublishProperties>> {
    (version == Version::V5).then(|| {
        Box::new(codec::PublishProperties {
            user: vec![codec::UserProperty {
                name: Bytes::from_static(b"payload"),
                value: Bytes::from(payload.to_string()),
            }],
            correlation_data: Some(Bytes::from(payload.to_string())),
            ..Default::default()
        })
    })
}

fn committed_only(packet: &Packet) -> bool {
    matches!(
        packet,
        Packet::PubAck(_) | Packet::SubAck(_) | Packet::UnsubAck(_) | Packet::Publish(_)
    )
}

impl World {
    fn fail(&self, what: &str) -> ! {
        panic!(
            "seed {} step {}: {what}\n(reproduce: STRAUM_SIM_SEED={} cargo test -p sim --test gateway)",
            self.seed, self.step, self.seed
        )
    }

    fn new(seed: u64) -> Self {
        let mut world = World {
            seed,
            step: 0,
            now: Duration::ZERO,
            rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1),
            nodes: (0..NODES)
                .map(|_| Node {
                    core: None,
                    gateway: None,
                    records: Vec::new(),
                    written: 0,
                    durable: 0,
                    state: State::new(LIMITS),
                    incarnation: 0,
                })
                .collect(),
            network: Vec::new(),
            isolated: None,
            calm: false,
            loopback: std::collections::VecDeque::new(),
            oracle: BTreeMap::new(),
            commit: 0,
            clients: (0..CLIENTS)
                .map(|i| Client {
                    // Two clients share an id: takeover within a gateway.
                    client_id: ["c0", "c1", "c2", "c3", "dup", "dup"][i],
                    conn: None,
                    version: Version::V5,
                    connected: false,
                    inbox: BytesMut::new(),
                    next_packet: 1,
                    seq: 0,
                    seen: HashMap::new(),
                    to_ack: Vec::new(),
                    unacked: Vec::new(),
                })
                .collect(),
            owners: HashMap::new(),
            next_conn: 0,
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
        node.durable = node.records.len();
        node.state = State::new(LIMITS);
        node.incarnation += 1;
        node.core = Some(if id == SEQUENCER {
            Core::Sequencer(Sequencer::new(cluster_config(id), tail.chain))
        } else {
            Core::Replica(Replica::new(cluster_config(id), tail.chain))
        });
        node.gateway = Some(Gateway::new(
            gateway_config(id, node.incarnation * 1_000_000),
            self.now,
        ));
        self.gateway_actions(id, None);
        self.drain(id);
    }

    fn crash(&mut self, id: u8, power: bool) {
        self.network.retain(|(_, to, _)| *to != id);
        let node = &mut self.nodes[usize::from(id)];
        node.core = None;
        node.gateway = None;
        let kept = if power { node.durable } else { node.written };
        node.records.truncate(kept);
        node.written = kept;
        node.durable = node.durable.min(kept);
        if power {
            self.stats.power_losses += 1;
        }
        // Its connections drop.
        for client in &mut self.clients {
            if client.conn.is_some_and(|(n, _)| n == id) {
                client.conn = None;
                client.connected = false;
            }
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
                    // A fixed sequencer and its replicas never take a slot over.
                    Step::Promise { .. } | Step::Truncate { .. } => {
                        unreachable!("no takeover in a fixed-owner cluster")
                    }
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
            for effect in effects {
                let now = self.now;
                if let Some(g) = self.nodes[usize::from(id)].gateway.as_mut() {
                    g.effect(&effect, now);
                }
                self.gateway_actions(id, Some((next, bytes.clone())));
            }
        }
    }

    /// Carries out what gateway `id` asked for. `cause` is the committed
    /// record whose effect it was handling, if any.
    fn gateway_actions(&mut self, id: u8, cause: Option<(u64, Vec<u8>)>) {
        let actions = match self.nodes[usize::from(id)].gateway.as_mut() {
            Some(g) => g.take_actions(),
            None => return,
        };
        for action in actions {
            match action {
                Action::Propose(proposal) => self.send(id, proposal),
                Action::Close { conn } => {
                    if let Some(c) = self.owners.remove(&conn) {
                        self.clients[c].conn = None;
                        self.clients[c].connected = false;
                    }
                }
                Action::Send { conn, bytes } => {
                    let Some(&c) = self.owners.get(&conn) else {
                        continue;
                    };
                    let version = self.clients[c].version;
                    let mut buf = BytesMut::from(&bytes[..]);
                    while let Some(packet) = codec::decode(
                        &mut buf,
                        Some(version),
                        Sender::Server,
                        &codec::Limits::default(),
                    )
                    .unwrap()
                    {
                        self.stats.packets_checked += 1;
                        if committed_only(&packet) {
                            self.check_committed(id, &packet, cause.as_ref());
                        }
                    }
                    self.clients[c].inbox.extend_from_slice(&bytes);
                }
            }
        }
        self.check_one_session_per_client_id(id);
    }

    /// Invariant 4 at the wire.
    fn check_committed(&self, id: u8, packet: &Packet, cause: Option<&(u64, Vec<u8>)>) {
        let Some((lsn, bytes)) = cause else {
            self.fail(&format!(
                "gateway {id} sent {packet:?} while reading a client, not from a committed effect"
            ));
        };
        if self
            .oracle
            .get(lsn)
            .is_none_or(|committed| committed != bytes)
        {
            self.fail(&format!(
                "gateway {id} sent {packet:?} for lsn {lsn}, not committed"
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
                "gateway {id} sent {packet:?} for lsn {lsn}, durable on {holding} nodes"
            ));
        }
    }

    fn check_one_session_per_client_id(&self, id: u8) {
        if let Some(g) = &self.nodes[usize::from(id)].gateway {
            let mut ids: Vec<&[u8]> = g.client_ids().collect();
            let all = ids.len();
            ids.sort_unstable();
            ids.dedup();
            if ids.len() != all {
                self.fail(&format!(
                    "gateway {id} holds two sessions for one client id"
                ));
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
        // A gateway's forwards to the sequencer travel one link, in order
        // (the transport requirement in `cluster::Message`): reordering two
        // publishes of one session would commit them out of publish order
        // (found by this simulation). Cluster messages may still reorder.
        let index = match &self.network[index] {
            (from, _, Wire::Forward(_)) => {
                let from = *from;
                self.network
                    .iter()
                    .position(|(f, _, w)| *f == from && matches!(w, Wire::Forward(_)))
                    .unwrap_or(index)
            }
            _ => index,
        };
        // `remove`, not `swap_remove`: the queue keeps its order.
        let (from, to, wire) = self.network.remove(index);
        if self.isolated.is_some_and(|i| i == from || i == to) || !self.up(to) {
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

    fn loopback(&mut self) {
        while let Some(proposal) = self.loopback.pop_front() {
            self.propose(proposal);
        }
    }

    fn send(&mut self, id: u8, proposal: Proposal) {
        if id == SEQUENCER {
            self.loopback.push_back(proposal);
        } else {
            self.network.push((id, SEQUENCER, Wire::Forward(proposal)));
        }
    }

    /// Bytes from client `c` to its gateway, as one write.
    fn client_send(&mut self, c: usize, packets: &[Packet]) {
        let Some((node, conn)) = self.clients[c].conn else {
            return;
        };
        let version = self.clients[c].version;
        let mut out = BytesMut::new();
        for packet in packets {
            codec::encode(packet, version, Sender::Client, &mut out).unwrap();
        }
        let now = self.now;
        if let Some(g) = self.nodes[usize::from(node)].gateway.as_mut() {
            g.received(conn, &out, now);
        }
        self.gateway_actions(node, None);
    }

    /// Client `c` reads what its gateway sent.
    fn client_read(&mut self, c: usize) {
        if self.clients[c].conn.is_none() {
            self.clients[c].inbox.clear();
            return;
        }
        let version = self.clients[c].version;
        let mut packets = Vec::new();
        while let Some(packet) = codec::decode(
            &mut self.clients[c].inbox,
            Some(version),
            Sender::Server,
            &codec::Limits::default(),
        )
        .unwrap()
        {
            packets.push(packet);
        }
        for packet in packets {
            let client = &mut self.clients[c];
            match packet {
                Packet::ConnAck(ack) => {
                    client.connected = ack.reason == ReasonCode::Success;
                }
                Packet::PubAck(ack) => {
                    client.unacked.retain(|id| *id != ack.id.get());
                    self.stats.pubacks += 1;
                }
                Packet::Publish(p) => {
                    self.stats.deliveries += 1;
                    if let Some(id) = p.id {
                        client.to_ack.push(id);
                    }
                    // D-65: what an MQTT 5 publisher attached arrives at an MQTT 5
                    // subscriber unchanged; a 3.1.1 publisher attached none.
                    if client.version == Version::V5 {
                        let text = String::from_utf8_lossy(&p.payload).to_string();
                        let from_v5 = text.contains("v5:");
                        let expected = properties_for(
                            if from_v5 { Version::V5 } else { Version::V311 },
                            &text,
                        );
                        if p.properties != expected {
                            let got = p.properties.clone();
                            self.fail(&format!("client {c} received {text} with properties {got:?}, sent {expected:?}"));
                        }
                        self.stats.properties_checked += 1;
                    }
                    if p.retain {
                        continue;
                    }
                    let text = String::from_utf8_lossy(&p.payload).to_string();
                    let (publisher, seq) = text.rsplit_once(':').unwrap();
                    let seq: u64 = seq.parse().unwrap();
                    let key = (publisher.to_string(), p.topic.unwrap().as_str().to_string());
                    let last = client.seen.insert(key.clone(), seq);
                    if let Some(last) = last.filter(|&last| seq <= last) {
                        self.fail(&format!(
                            "client {c} received {key:?} seq {seq} after {last}: out of order"
                        ));
                    }
                }
                _ => {}
            }
        }
    }

    fn connect(&mut self, c: usize) {
        let up: Vec<u8> = (0..NODES).filter(|&n| self.up(n)).collect();
        if up.is_empty() {
            return;
        }
        let node = self.rng.pick(&up);
        let conn = self.next_conn;
        self.next_conn += 1;
        let version = if self.rng.chance(50) {
            Version::V5
        } else {
            Version::V311
        };
        let now = self.now;
        let accepted = self.nodes[usize::from(node)]
            .gateway
            .as_mut()
            .unwrap()
            .accept(conn, now);
        if !accepted {
            return;
        }
        match version {
            Version::V5 => self.stats.v5 += 1,
            Version::V311 => self.stats.v311 += 1,
        }
        let client = &mut self.clients[c];
        client.conn = Some((node, conn));
        client.version = version;
        client.connected = false;
        client.inbox.clear();
        client.seen.clear();
        client.to_ack.clear();
        client.unacked.clear();
        self.owners.insert(conn, c);
        let connect = Packet::Connect(Box::new(Connect {
            version,
            client_id: ClientId::new(Bytes::from_static(client.client_id.as_bytes())).unwrap(),
            clean_start: true,
            keep_alive_secs: 0,
            will: None,
            username: None,
            password: None,
            properties: ConnectProperties::default(),
        }));
        // Another client with this id, open at this gateway: the CONNECT
        // must take its session over (invariant 7 within one gateway).
        let rival = (0..CLIENTS).find(|&o| {
            o != c
                && self.clients[o].client_id == self.clients[c].client_id
                && self.clients[o].connected
                && self.clients[o].conn.is_some_and(|(n, _)| n == node)
        });
        let ready = self.nodes[usize::from(node)]
            .gateway
            .as_ref()
            .is_some_and(Gateway::ready);
        self.client_send(c, &[connect]);
        if let (Some(rival), true) = (rival, ready) {
            if self.clients[rival].conn.is_some() {
                self.fail(&format!(
                    "client {c} connected as {} at gateway {node}, client {rival}'s session was not taken over",
                    self.clients[c].client_id
                ));
            }
            self.stats.takeovers += 1;
        }
    }

    fn packet_id(&mut self, c: usize) -> PacketId {
        let client = &mut self.clients[c];
        let id = client.next_packet;
        client.next_packet = client.next_packet.wrapping_add(1).max(1);
        PacketId::new(id).unwrap()
    }

    fn client(&mut self, c: usize) {
        self.client_read(c);
        if self.clients[c].conn.is_none() {
            if self.rng.chance(30) {
                self.connect(c);
            }
            return;
        }
        if !self.clients[c].connected {
            return;
        }
        match self.rng.below(50) {
            0..=19 => {
                let qos = if self.rng.chance(60) {
                    Qos::AtLeastOnce
                } else {
                    Qos::AtMostOnce
                };
                let topic = self.rng.pick(&TOPICS);
                self.clients[c].seq += 1;
                let payload = format!(
                    "{}#{c}@{}v{}:{}",
                    self.clients[c].client_id,
                    self.clients[c].conn.map_or(0, |(_, conn)| conn),
                    if self.clients[c].version == Version::V5 {
                        5
                    } else {
                        3
                    },
                    self.clients[c].seq
                );
                let properties = properties_for(self.clients[c].version, &payload);
                let id = (qos == Qos::AtLeastOnce).then(|| self.packet_id(c));
                if let Some(id) = id {
                    self.clients[c].unacked.push(id.get());
                }
                let publish = Packet::Publish(Publish {
                    topic: Some(Topic::new(Bytes::from_static(topic.as_bytes())).unwrap()),
                    id,
                    qos,
                    duplicate: false,
                    retain: self.rng.chance(10),
                    payload: Bytes::from(payload),
                    properties: properties.clone(),
                });
                self.client_send(c, &[publish]);
            }
            20..=27 => {
                let qos = if self.rng.chance(60) {
                    Qos::AtLeastOnce
                } else {
                    Qos::AtMostOnce
                };
                let id = self.packet_id(c);
                let filter =
                    TopicFilter::new(Bytes::from_static(self.rng.pick(&FILTERS).as_bytes()))
                        .unwrap();
                self.client_send(
                    c,
                    &[Packet::Subscribe(Subscribe {
                        id,
                        subscription_identifier: None,
                        user: Vec::new(),
                        filters: vec![(filter, SubscriptionOptions::qos(qos))],
                    })],
                );
            }
            28..=29 => {
                let id = self.packet_id(c);
                let filter =
                    TopicFilter::new(Bytes::from_static(self.rng.pick(&FILTERS).as_bytes()))
                        .unwrap();
                self.client_send(
                    c,
                    &[Packet::Unsubscribe(Unsubscribe {
                        id,
                        user: Vec::new(),
                        filters: vec![filter],
                    })],
                );
            }
            30..=45 => {
                let acks: Vec<Packet> = std::mem::take(&mut self.clients[c].to_ack)
                    .into_iter()
                    .map(|id| {
                        Packet::PubAck(PubAck {
                            id,
                            reason: ReasonCode::Success,
                            properties: Default::default(),
                        })
                    })
                    .collect();
                self.client_send(c, &acks);
            }
            46..=47 => self.client_send(c, &[Packet::PingReq]),
            48 => {
                // Clean goodbye.
                let goodbye = Packet::Disconnect(codec::Disconnect {
                    reason: ReasonCode::Success,
                    properties: Default::default(),
                });
                self.client_send(c, &[goodbye]);
            }
            _ => {
                // The socket drops.
                if let Some((node, conn)) = self.clients[c].conn.take() {
                    self.clients[c].connected = false;
                    self.owners.remove(&conn);
                    if let Some(g) = self.nodes[usize::from(node)].gateway.as_mut() {
                        g.closed(conn);
                    }
                    self.gateway_actions(node, None);
                }
            }
        }
    }

    fn tick(&mut self) {
        if let Some(Core::Sequencer(s)) = self.nodes[usize::from(SEQUENCER)].core.as_mut() {
            s.flush().unwrap();
            s.tick();
            self.drain(SEQUENCER);
        }
        for id in 0..NODES {
            let now = self.now;
            if let Some(g) = self.nodes[usize::from(id)].gateway.as_mut() {
                g.tick(now);
            }
            self.gateway_actions(id, None);
        }
    }

    fn event(&mut self) {
        self.loopback();
        self.now += TICK;
        let id = self.rng.below(u64::from(NODES)) as u8;
        let mut roll = self.rng.below(100);
        if self.calm && matches!(roll, 75..=77 | 85..=86 | 94..) {
            roll = 50;
        }
        match roll {
            0..=29 => {
                let c = self.rng.below(CLIENTS as u64) as usize;
                self.client(c);
            }
            30..=42 => {
                if self.up(id) {
                    let sync = self.rng.chance(70);
                    self.write(id, sync);
                }
            }
            43..=74 => {
                if !self.network.is_empty() {
                    // Mostly in order, as links are; now and then any message,
                    // so reordering stays in the fault model. Uniformly random
                    // delivery from one queue made latency grow with the queue
                    // until commit crawled.
                    let index = if self.rng.chance(80) {
                        0
                    } else {
                        self.rng.below(self.network.len() as u64) as usize
                    };
                    self.deliver(index);
                }
            }
            75..=77 => {
                if !self.network.is_empty() {
                    let index = self.rng.below(self.network.len() as u64) as usize;
                    // `remove`: dropping one message must not reorder the rest.
                    self.network.remove(index);
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
        for _ in 0..2000 {
            self.now += TICK;
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
            self.loopback();
            for c in 0..CLIENTS {
                self.client_read(c);
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
            let quiet = self.nodes.iter().all(|n| {
                let load = n.gateway.as_ref().unwrap().load();
                load.pending == 0 && load.retries == 0
            });
            if settled && quiet && self.network.is_empty() {
                return;
            }
        }
        self.fail("the cluster did not settle");
    }

    fn heal(&mut self) {
        self.isolated = None;
        for id in 0..NODES {
            if !self.up(id) {
                self.start(id);
            }
        }
        self.settle();
        // Every connected client acknowledges what it holds, and publishes
        // once more at QoS 1: that must be acknowledged.
        for _ in 0..20 {
            for c in 0..CLIENTS {
                self.client_read(c);
                let acks: Vec<Packet> = std::mem::take(&mut self.clients[c].to_ack)
                    .into_iter()
                    .map(|id| {
                        Packet::PubAck(PubAck {
                            id,
                            reason: ReasonCode::Success,
                            properties: Default::default(),
                        })
                    })
                    .collect();
                self.client_send(c, &acks);
            }
            self.settle();
        }
        let mut waiting = Vec::new();
        for c in 0..CLIENTS {
            if self.clients[c].connected {
                let id = self.packet_id(c);
                self.clients[c].unacked = vec![id.get()];
                self.clients[c].seq += 1;
                let payload = format!(
                    "{}#{c}@{}v{}:{}",
                    self.clients[c].client_id,
                    self.clients[c].conn.map_or(0, |(_, conn)| conn),
                    if self.clients[c].version == Version::V5 {
                        5
                    } else {
                        3
                    },
                    self.clients[c].seq
                );
                let properties = properties_for(self.clients[c].version, &payload);
                let publish = Packet::Publish(Publish {
                    topic: Some(Topic::new(Bytes::from_static(b"z")).unwrap()),
                    id: Some(id),
                    qos: Qos::AtLeastOnce,
                    duplicate: false,
                    retain: false,
                    payload: Bytes::from(payload),
                    properties: properties.clone(),
                });
                self.client_send(c, &[publish]);
                waiting.push(c);
            }
        }
        self.settle();
        for c in waiting {
            self.client_read(c);
            if self.clients[c].connected && !self.clients[c].unacked.is_empty() {
                self.fail(&format!(
                    "client {c}'s QoS 1 publish was not acknowledged after healing"
                ));
            }
        }
        // Everyone leaves.
        for c in 0..CLIENTS {
            if self.clients[c].conn.is_some() {
                let goodbye = Packet::Disconnect(codec::Disconnect {
                    reason: ReasonCode::Success,
                    properties: Default::default(),
                });
                self.client_send(c, &[goodbye]);
            }
        }
        self.settle();
        for (id, node) in self.nodes.iter().enumerate() {
            let left = node.state.sessions();
            if left != 0 {
                self.fail(&format!(
                    "node {id} holds {left} sessions after every client left"
                ));
            }
        }
    }
}

fn run(seed: u64) -> Stats {
    let mut world = World::new(seed);
    for step in 0..STEPS {
        world.step = step;
        // Mostly calm: 50 steps of chaos in every 200. The disk and
        // cluster simulations take the heavy chaos; this one is about
        // what MQTT clients observe, which needs the log to commit.
        let calm = step % 200 >= 50;
        if calm != world.calm {
            world.calm = calm;
            if calm {
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
fn mqtt_clients_see_only_what_is_committed() {
    let mut total = Stats::default();
    for seed in seeds() {
        let s = run(seed);
        total.committed += s.committed;
        total.packets_checked += s.packets_checked;
        total.deliveries += s.deliveries;
        total.pubacks += s.pubacks;
        total.properties_checked += s.properties_checked;
        total.takeovers += s.takeovers;
        total.restarts += s.restarts;
        total.power_losses += s.power_losses;
        total.v5 += s.v5;
        total.v311 += s.v311;
    }
    println!(
        "committed {} packets checked {} deliveries {} properties checked {} pubacks {} takeovers {} restarts {} power losses {} connections v5 {} v3.1.1 {}",
        total.committed,
        total.packets_checked,
        total.deliveries,
        total.properties_checked,
        total.pubacks,
        total.takeovers,
        total.restarts,
        total.power_losses,
        total.v5,
        total.v311
    );
    if std::env::var("STRAUM_SIM_SEED").is_err() {
        assert!(total.deliveries > 0 && total.pubacks > 0 && total.takeovers > 0);
        assert!(total.properties_checked > 0);
        assert!(total.restarts > 0 && total.v5 > 0 && total.v311 > 0);
    }
}
