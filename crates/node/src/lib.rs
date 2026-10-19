//! The runtime driver of the new path (ADR-0008 step 4): one node of a
//! cluster, running the sans-IO cores against sockets, a disk and a clock.
//!
//! * The **node loop** — one tokio task — owns the sequencer or replica
//!   core, the applied state and the gateway. It carries out the core's
//!   effects, applies committed records in order, hands their effects to
//!   the gateway, and carries out the gateway's actions. It never touches a
//!   file and never awaits one connection.
//! * The **WAL thread** owns the segmented log on disk: recovery and its
//!   chain check, appends with group commit, and reads for catch-up and for
//!   applying records older than this process (`wal`).
//! * **Links** carry `cluster::wire` frames, one TCP connection per
//!   direction, opened by a HELLO naming the sender and the digest of its
//!   `apply` limits; a mismatch refuses the link (`link`).
//! * **MQTT connections** (`mqtt`): a reader that reads only on a credit
//!   from the node loop, so the gateway's backpressure reaches TCP, and a
//!   writer behind a bounded queue; a client that lets it fill is closed.
//!
//! Cluster mode stays refused by the server's configuration until step 7
//! (ADR-0008): this crate is reached from tests.

mod link;
pub mod metrics;
mod mqtt;
mod wal;

pub mod experimental;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub use experimental::{ConfigError, Experimental};

use apply::{Effect, Limits, State};
use cluster::wire::Frame;
use cluster::{Config as CoreConfig, Effect as Step, Proposal, Voter};
use gateway::{Action, Gateway};
use metrics::Stage;
use storage::SegmentLimits;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use transport::{BufferBudget, Listener, TransportConfig};

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("storage: {0}")]
    Storage(#[from] storage::StorageError),
    #[error("the log does not recover as one chain: {0}")]
    Recovery(#[from] journal::ChainError),
    #[error("the snapshot is refused: {0}")]
    Snapshot(#[from] apply::SnapshotError),
    #[error("the snapshot file is torn or damaged")]
    SnapshotDamaged,
    /// The log was compacted past a snapshot, or the snapshot is ahead of
    /// what the log holds: neither is left by this node's own order of
    /// writes, so the data directory is not one node's.
    #[error("the snapshot at lsn {snapshot} does not meet the log, which holds {first}..{end}")]
    SnapshotGap { snapshot: u64, first: u64, end: u64 },
    /// The data directory was first used by a cluster with another
    /// configuration digest (`cluster_digest`), or its `cluster` file is
    /// damaged (`pinned` is `None`).
    #[error(
        "the data directory belongs to another cluster: pinned {pinned:x?}, this configuration {digest:x}"
    )]
    OtherCluster { pinned: Option<u64>, digest: u64 },
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("transport: {0}")]
    Transport(#[from] transport::TransportError),
    #[error("configuration: {0}")]
    Config(#[from] ConfigError),
}

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub listen: SocketAddr,
    /// `node` and `first_session` are the node's to set and are overwritten.
    pub gateway: gateway::Config,
    /// Packets queued per connection; a client that lets the queue fill is
    /// closed (a writer that makes no progress).
    pub write_queue: usize,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This node's core configuration: its id, the voters, the slot owner.
    pub core: CoreConfig,
    /// Where other nodes connect to this one.
    pub listen: SocketAddr,
    /// The other voters and where to reach them.
    pub peers: Vec<(u8, SocketAddr)>,
    pub data: PathBuf,
    pub segments: SegmentLimits,
    /// The same on every node: refused otherwise.
    pub limits: Limits,
    pub heartbeat: Duration,
    pub max_frame: usize,
    /// Frames queued per peer link; beyond, dropped (the protocol resends).
    pub link_queue: usize,
    pub transport: TransportConfig,
    pub mqtt: Option<MqttConfig>,
    /// The retention bound of D-57 A: the log keeps no more than this many
    /// bytes behind a replica that is only slow; a replica further behind
    /// is lost (it needs a state copy, step 7b).
    pub retain_bytes: u64,
    /// Applied records between two snapshots. The log below the last
    /// durable snapshot may go (D-67 B); a snapshot encodes the whole
    /// state on the node loop, so this trades disk for that pause.
    pub snapshot_every: u64,
    /// Leading, hand the slot over to the next owner every this many
    /// heartbeats (§2.4 planned rotation, ADR-0008 step 6). `None`, the
    /// default: a sequencer is stable.
    pub rotate_every: Option<u32>,
    /// Bytes one read of the log hands back at most — or one batch, if that
    /// alone is larger: a catch-up or a candidate's fetch moves this much
    /// per round trip (D-81 A). At most half of `max_frame`, which carries it;
    /// more is refused at startup (D-83 A).
    pub read_bytes: usize,
    /// Where the Prometheus text of `Node::metrics` is served (D-112):
    /// `None`, the default, serves none. Plain HTTP, no credentials:
    /// loopback unless the operator says otherwise (D-107).
    pub metrics: Option<SocketAddr>,
}

/// The starting `retain_bytes` (D-91 A): 64 MiB, the old path's
/// `STRAUM_WAL_RETAIN_BYTES`. Compatible, not shown optimal: what share of
/// failovers it sends to a state copy on a real workload is not measured.
/// In the cluster simulation, 100 000 bytes already left no fault cycle
/// needing one (10 000 seeds; D-87 A). Used once step 7a reads
/// configuration.
pub const DEFAULT_RETAIN_BYTES: u64 = 64 * 1024 * 1024;

/// What a node reports about itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Status {
    pub end: u64,
    pub commit: u64,
    pub applied: u64,
    /// `apply::State::digest` at `applied`.
    pub digest: u64,
    /// The highest slot this node has promised, and whether it sequences it.
    pub slot: u64,
    pub leading: bool,
    /// This node's proposals refused at a full outbox, and given up as
    /// lost before a later one of its committed (D-109 B, D-115 A).
    pub refused: u64,
    pub given_up: u64,
}

/// A running node. `shutdown` stops it and waits for every part. Dropping
/// it only ends the node loop (its stop channel closes); its link and MQTT
/// tasks run until the runtime ends, and the log thread until its queue
/// closes — nothing waits for them.
pub struct Node {
    pub listen: SocketAddr,
    pub mqtt: Option<SocketAddr>,
    /// Where the metrics are served, if they are (`NodeConfig::metrics`).
    pub metrics_listen: Option<SocketAddr>,
    proposals: mpsc::Sender<Proposal>,
    effects: broadcast::Sender<(u64, u64, Vec<Effect>)>,
    /// Operator handoffs, each with where its answer goes.
    hand_offs: mpsc::Sender<oneshot::Sender<Result<(), cluster::ClusterError>>>,
    metrics: mpsc::Sender<oneshot::Sender<metrics::Snapshot>>,
    trace: mpsc::Sender<TraceRequest>,
    status: watch::Receiver<Status>,
    stop: Option<oneshot::Sender<()>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    wal: Option<std::thread::JoinHandle<()>>,
}

/// The snapshot file's body, this node's format (D-79 A):
/// `[version=2][slots u32] { [slot u64][first lsn u64] } [apply::State]`.
/// The slot table is the committed log's, through the snapshot's lsn and
/// pruned by compaction; the state is `apply`'s own encoding. Version 2:
/// before D-79 the body was `apply`'s alone, which starts with its own
/// version 1, so an old body is refused here rather than misread. The
/// table goes first because `apply`'s body carries no length of its own.
const SNAPSHOT_VERSION: u8 = 2;

fn encode_snapshot(slots: &[(u64, u64)], state: &State) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + slots.len() * 16);
    bytes.push(SNAPSHOT_VERSION);
    bytes.extend_from_slice(&u32::try_from(slots.len()).unwrap_or(u32::MAX).to_be_bytes());
    for &(slot, first) in slots {
        bytes.extend_from_slice(&slot.to_be_bytes());
        bytes.extend_from_slice(&first.to_be_bytes());
    }
    state.encode(&mut bytes);
    bytes
}

/// The slot table and the state's bytes. A table the log could not have
/// produced — not ascending in both — is refused, as is a count past the
/// bytes that are there.
/// A slot table: (slot, first lsn), ascending.
type Slots = Vec<(u64, u64)>;

fn decode_snapshot(bytes: &[u8]) -> Result<(Slots, &[u8]), NodeError> {
    let u64_at = |at: usize| -> Option<u64> {
        Some(u64::from_be_bytes(
            bytes.get(at..at.checked_add(8)?)?.try_into().ok()?,
        ))
    };
    if bytes.first() != Some(&SNAPSHOT_VERSION) {
        return Err(NodeError::SnapshotDamaged);
    }
    let count = bytes
        .get(1..5)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_be_bytes)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or(NodeError::SnapshotDamaged)?;
    let end = count
        .checked_mul(16)
        .and_then(|n| n.checked_add(5))
        .filter(|&end| end <= bytes.len())
        .ok_or(NodeError::SnapshotDamaged)?;
    let mut slots: Slots = Vec::with_capacity(count);
    for index in 0..count {
        let at = 5 + index * 16;
        let entry = u64_at(at)
            .zip(u64_at(at + 8))
            .ok_or(NodeError::SnapshotDamaged)?;
        if slots
            .last()
            .is_some_and(|&(slot, first)| entry.0 <= slot || entry.1 <= first)
        {
            return Err(NodeError::SnapshotDamaged);
        }
        slots.push(entry);
    }
    let state = bytes.get(end..).ok_or(NodeError::SnapshotDamaged)?;
    Ok((slots, state))
}

/// A digest of the limits, compared at every link: a fixed function of the
/// values (FNV-1a), not of the build, so nodes on different builds agree.
pub fn limits_digest(limits: &Limits) -> u64 {
    let fields = [
        limits.max_subscriptions_per_session,
        limits.max_subscriptions,
        limits.max_retained,
        limits.max_retained_bytes,
        limits.max_inflight,
        limits.max_backlog,
        limits.max_backlog_bytes,
        limits.max_total_queued_bytes,
    ];
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for field in fields {
        for byte in (field as u64).to_be_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

/// What every node of one cluster must share, as one number (ADR-0008
/// step 7a, D-104 A): the voters, the first slot and its owner — they fix
/// the schedule — the timing and the bounds that decide liveness and what
/// a batch may hold, and the `apply` limits, part of what the log means.
/// Sent in every HELLO; a link with another digest is refused, and a data
/// directory keeps the digest it was first used with. Addresses are not in
/// it: a node may listen on `0.0.0.0` while its peers name it by host.
pub fn cluster_digest(config: &NodeConfig) -> u64 {
    let core = &config.core;
    let mut voters = core.nodes.clone();
    voters.sort_unstable();
    let (lag_records, lag_ticks) = core
        .lag_bound
        .map_or((0, 0), |b| (b.records, u64::from(b.ticks)));
    let fields = voters.iter().map(|&v| u64::from(v)).chain([
        u64::MAX,
        u64::from(core.owner),
        core.slot,
        u64::try_from(config.heartbeat.as_millis()).unwrap_or(u64::MAX),
        u64::from(core.takeover_ticks),
        core.max_uncommitted,
        core.window,
        core.max_batch as u64,
        config.max_frame as u64,
        lag_records,
        lag_ticks,
        limits_digest(&config.limits),
    ]);
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for field in fields {
        for byte in field.to_be_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

/// A data directory belongs to the cluster it was first used with: its
/// `cluster` file keeps that cluster's digest, written aside, synced and
/// renamed before anything else is written there. A node started over
/// another cluster's data — another schedule, other limits — would read
/// its promises and log as its own; it is refused instead (D-104 A).
fn pin_cluster(data: &Path, digest: u64) -> Result<(), NodeError> {
    std::fs::create_dir_all(data)?;
    let path = data.join("cluster");
    match std::fs::read(&path) {
        Ok(bytes) => {
            let pinned = <[u8; 8]>::try_from(bytes.as_slice())
                .map(u64::from_be_bytes)
                .map_err(|_| NodeError::OtherCluster {
                    pinned: None,
                    digest,
                })?;
            if pinned != digest {
                return Err(NodeError::OtherCluster {
                    pinned: Some(pinned),
                    digest,
                });
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let next = data.join("cluster.next");
            let mut file = std::fs::File::create(&next)?;
            file.write_all(&digest.to_be_bytes())?;
            file.sync_all()?;
            std::fs::rename(&next, &path)?;
            std::fs::File::open(data)?.sync_all()?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// The next incarnation of this node, durable before it is used: the
/// gateway numbers its sessions from it, and no number may serve two
/// incarnations (`gateway::Config::first_session`).
fn incarnation(data: &Path) -> Result<u64, NodeError> {
    let path = data.join("incarnation");
    let mut previous = [0u8; 8];
    match std::fs::File::open(&path) {
        Ok(mut file) => file.read_exact(&mut previous)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let next = u64::from_be_bytes(previous).saturating_add(1);
    let temporary = data.join("incarnation.next");
    let store = || -> std::io::Result<()> {
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&next.to_be_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temporary, &path)?;
        std::fs::File::open(data)?.sync_all()
    };
    store()?;
    Ok(next)
}

/// What the node loop owns besides the applied state.
struct Loop {
    core: Voter,
    links: BTreeMap<u8, mpsc::Sender<Frame>>,
    gateway: Option<Gateway>,
    conns: HashMap<u64, mqtt::Conn>,
    /// Connections whose reader waits for the gateway to want more.
    paused: BTreeSet<u64>,
    started: tokio::time::Instant,
    /// What this node measures about itself (D-112); refusals and give-ups
    /// among it (D-115 A: counted, never silent).
    measured: metrics::Measured,
    /// The stages' stamps, while a run asks for them (D-136 A).
    trace: metrics::Trace,
}

/// A run's request for the stages' stamps (`Node::trace_start`,
/// `Node::trace_take`).
enum TraceRequest {
    Start(usize),
    Take(oneshot::Sender<metrics::Trace>),
}

impl Loop {
    fn now(&self) -> Duration {
        self.started.elapsed()
    }

    /// What `Node::metrics` hands out: the measurements, the cores' counts
    /// and the status, read together in one pass of the loop.
    fn snapshot(&self, id: u8, applied: u64) -> metrics::Snapshot {
        let m = &self.measured;
        metrics::Snapshot {
            node: id,
            status: Status {
                end: self.core.end(),
                commit: self.core.commit(),
                applied,
                digest: 0,
                slot: self.core.slot(),
                leading: self.core.leading(),
                refused: m.refused,
                given_up: m.given_up,
            },
            durable: self.core.durable(),
            voter: self.core.stats(),
            replicas: self.core.replicas(),
            gateway: self.gateway.as_ref().map(Gateway::load),
            accepted: m.accepted,
            refused: m.refused,
            committed: m.committed,
            given_up: m.given_up,
            link_drops: m.link_drops,
            forwards_dropped: m.forwards_dropped,
            proposal_commit: m.proposal_commit,
            append_durable: m.append_durable,
            append_commit: m.append_commit,
            takeovers_completed: m.takeovers_completed,
            takeovers_abandoned: m.takeovers_abandoned,
            takeover: m.takeover,
            spells: m.spells(tokio::time::Instant::now()),
            heard_commit: self.core.heard_commit(),
            own: m.own,
            replica_progress: m.replicas.clone(),
            flights: m.flights.clone(),
            serving: self.gateway.as_ref().is_some_and(Gateway::ready),
        }
    }

    /// This node's own proposal: the core holds it until its record
    /// commits and sends it when a sequencer can take it (D-109 B).
    fn propose(&mut self, proposal: Proposal) {
        if self.core.submit(proposal).is_ok() {
            self.measured.accepted(tokio::time::Instant::now());
        } else {
            self.measured.refused();
            tracing::warn!(
                "proposal refused: this node holds a full queue of uncommitted proposals"
            );
        }
    }

    /// Carries out what the gateway asked for, until it asks nothing.
    fn gateway_actions(&mut self) {
        loop {
            let Some(gateway) = self.gateway.as_mut() else {
                return;
            };
            let actions = gateway.take_actions();
            if actions.is_empty() {
                return;
            }
            for action in actions {
                match action {
                    Action::Propose(proposal) => {
                        let mut body = Vec::new();
                        if proposal.encode(&mut body).is_err() {
                            // The gateway checks lengths before it proposes.
                            tracing::error!("a gateway proposal does not encode");
                            continue;
                        }
                        self.propose(Proposal {
                            kind: proposal.kind(),
                            properties: proposal.properties().to_vec(),
                            body,
                        });
                    }
                    Action::Send { conn, bytes } => {
                        let full = self
                            .conns
                            .get(&conn)
                            .is_some_and(|c| c.writer.try_send(bytes).is_err());
                        if full {
                            // The client does not read: closed, not buffered
                            // without bound.
                            self.drop_conn(conn);
                            if let Some(g) = self.gateway.as_mut() {
                                g.closed(conn);
                            }
                        }
                    }
                    Action::Close { conn } => self.drop_conn(conn),
                }
            }
        }
    }

    /// Dropping the handle closes the socket once its queue is written.
    fn drop_conn(&mut self, conn: u64) {
        self.conns.remove(&conn);
        self.paused.remove(&conn);
    }

    fn credit(&mut self, conn: u64) {
        // Backpressure from the outbox too (D-120 A): while it is full, no
        // client is read, so a publish waits in TCP instead of being refused.
        let room = !self.core.outbox_full();
        let wants = room && self.gateway.as_ref().is_some_and(|g| g.wants_read(conn));
        match self.conns.get(&conn) {
            Some(c) if wants => {
                let _ = c.credit.try_send(());
            }
            Some(_) => {
                self.paused.insert(conn);
            }
            None => {}
        }
    }

    /// Gives a credit to every paused connection the gateway would now read.
    fn resume(&mut self) {
        if self.core.outbox_full() {
            return;
        }
        let Some(gateway) = self.gateway.as_ref() else {
            return;
        };
        let ready: Vec<u64> = self
            .paused
            .iter()
            .copied()
            .filter(|&c| gateway.wants_read(c))
            .collect();
        for conn in ready {
            self.paused.remove(&conn);
            if let Some(c) = self.conns.get(&conn) {
                let _ = c.credit.try_send(());
            }
        }
    }

    fn mqtt(&mut self, event: mqtt::Event) {
        let now = self.now();
        let Some(gateway) = self.gateway.as_mut() else {
            return;
        };
        match event {
            mqtt::Event::Opened { conn, handle } => {
                // Refused: the handle is dropped and the socket closes.
                if gateway.accept(conn, now) {
                    self.conns.insert(conn, handle);
                    self.credit(conn);
                }
            }
            mqtt::Event::Bytes { conn, bytes } => {
                gateway.received(conn, &bytes, now);
                self.gateway_actions();
                self.credit(conn);
            }
            mqtt::Event::Closed { conn } => {
                gateway.closed(conn);
                self.drop_conn(conn);
            }
        }
        self.gateway_actions();
    }
}

impl Node {
    pub async fn start(config: NodeConfig) -> Result<Self, NodeError> {
        // Before the log is opened or anything listens: a configuration
        // that breaks a rule is refused, not corrected (D-83 A, D-104 A).
        config.check()?;
        pin_cluster(&config.data, cluster_digest(&config))?;
        let (wal_events, mut wal_rx) = mpsc::channel(1024);
        let (recovered, wal, wal_thread) = wal::start(
            config.data.clone(),
            config.segments,
            config.retain_bytes,
            wal_events,
        )?;
        let chain = recovered.chain;
        // The snapshot, then the log after it, with nothing between them.
        let end = chain.next_lsn().saturating_sub(1);
        let (snapshot_slots, state) = match &recovered.snapshot {
            Some(bytes) => {
                let (slots, state) = decode_snapshot(bytes)?;
                (slots, State::decode(config.limits, state)?)
            }
            None => (Vec::new(), State::new(config.limits)),
        };
        let first = recovered.first.unwrap_or(end.saturating_add(1));
        if state.applied().saturating_add(1) < first || state.applied() > end {
            return Err(NodeError::SnapshotGap {
                snapshot: state.applied(),
                first,
                end,
            });
        }

        let budget = Arc::new(BufferBudget::new(
            config.transport.total_connection_buffer_bytes,
        ));
        let listener = Listener::bind(
            TransportConfig {
                listen: config.listen,
                ..config.transport
            },
            Arc::clone(&budget),
            None,
        )?;
        let listen = listener.local_addr()?;
        let digest = cluster_digest(&config);
        let slot = config.core.slot;
        let id = config.core.id;

        // This process's life, durable before use: numbers the gateway's
        // sessions, orders this node's reports past its restarts, and tells
        // peers in HELLO that this is a new process (D-151 A).
        let life = incarnation(&config.data)?;

        let (inbound, mut inbound_rx) = mpsc::channel::<link::Inbound>(1024);
        let (seen, incarnations): (BTreeMap<_, _>, Vec<_>) = config
            .peers
            .iter()
            .map(|&(peer, _)| {
                let (tx, rx) = watch::channel(0u64);
                ((peer, tx), rx)
            })
            .unzip();
        let mut tasks = vec![tokio::spawn(link::listen(
            listener,
            config.core.nodes.clone(),
            slot,
            digest,
            config.max_frame,
            inbound,
            Arc::new(seen),
        ))];
        let mut links = BTreeMap::new();
        for (&(peer, address), incarnation) in config.peers.iter().zip(incarnations) {
            let (tx, rx) = mpsc::channel(config.link_queue);
            links.insert(peer, tx);
            tasks.push(tokio::spawn(link::dial(
                address,
                Frame::Hello {
                    version: cluster::wire::WIRE_VERSION,
                    node: id,
                    slot,
                    config: digest,
                    incarnation: life,
                },
                rx,
                config.transport,
                Arc::clone(&budget),
                config.heartbeat,
                incarnation,
            )));
        }
        let started = tokio::time::Instant::now();
        let (mqtt_tx, mut mqtt_rx) = mpsc::channel::<mqtt::Event>(1024);
        let mut mqtt_listen = None;
        let mut metrics_listen = None;
        let gateway = match &config.mqtt {
            Some(m) => {
                let first_session = life << 32;
                let listener = Listener::bind(
                    TransportConfig {
                        listen: m.listen,
                        ..config.transport
                    },
                    Arc::clone(&budget),
                    None,
                )?;
                mqtt_listen = Some(listener.local_addr()?);
                tasks.push(tokio::spawn(mqtt::listen(listener, m.write_queue, mqtt_tx)));
                Some(Gateway::new(
                    gateway::Config {
                        node: id,
                        first_session,
                        ..m.gateway
                    },
                    started.elapsed(),
                ))
            }
            None => None,
        };

        let (proposals, mut proposals_rx) = mpsc::channel::<Proposal>(1024);
        let (effects, _) = broadcast::channel(4096);
        let (status_tx, status) = watch::channel(Status::default());
        let (stop, mut stop_rx) = oneshot::channel::<()>();
        let (hand_offs, mut hand_offs_rx) =
            mpsc::channel::<oneshot::Sender<Result<(), cluster::ClusterError>>>(4);
        // Snapshots on demand (D-112): nothing is built while nobody asks.
        let (metrics, mut metrics_rx) = mpsc::channel::<oneshot::Sender<metrics::Snapshot>>(4);
        let (trace, mut trace_rx) = mpsc::channel::<TraceRequest>(4);
        if let Some(address) = config.metrics {
            let listener = tokio::net::TcpListener::bind(address).await?;
            metrics_listen = Some(listener.local_addr()?);
            tasks.push(tokio::spawn(metrics::serve(listener, metrics.clone())));
        }

        // A follower, whatever it was before: a sequencer that restarts
        // does not lead its old slot again; the schedule moves on (step 5).
        let core = Voter::new(
            config.core.clone(),
            chain,
            cluster::merge_slots(&snapshot_slots, &recovered.slots),
            recovered.promised,
        )
        .knowing_commit(state.applied())
        // What forwarding waits for after a restart: the slot of the last
        // committed record, from the snapshot until the replay passes it.
        .knowing_applied_slot(snapshot_slots.last().map(|&(slot, _)| slot))
        .incarnation(life);
        let core = match config.rotate_every {
            Some(ticks) => core.rotate_every(ticks),
            None => core,
        };
        let mut node = Loop {
            core,
            links,
            gateway,
            conns: HashMap::new(),
            paused: BTreeSet::new(),
            started,
            measured: metrics::Measured::default(),
            trace: metrics::Trace::default(),
        };
        // The gateway's NodeReset goes to the log before anything else.
        node.gateway_actions();
        let effects_tx = effects.clone();
        let heartbeat = config.heartbeat;
        let snapshot_every = config.snapshot_every.max(1);
        // At most half a frame (`NodeConfig::check`): the batches read go
        // out in one.
        let read_bytes = config.read_bytes;
        let mut state = state;
        // The committed log's slot table, from committed records only, kept
        // to go into the next snapshot and pruned by each compaction
        // (D-73 A, D-79 A).
        let mut committed_slots = snapshot_slots;
        tasks.push(tokio::spawn(async move {
            let mut snapshot = state.applied();
            let mut snapshotting = false;
            let mut retained_floor = 0u64;
            // Appended by this process and not yet applied: bounded by the
            // sequencer's uncommitted bound and the replication window.
            let mut recent: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
            let mut reading = false;
            let mut last = Status::default();
            let mut tick = tokio::time::interval(heartbeat);
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = tick.tick() => {
                        node.core.tick();
                        // D-132: progress, once a heartbeat — not per event.
                        let replicas = node.core.replicas();
                        for (&peer, link) in &node.links {
                            node.measured.queued(peer, link.max_capacity() - link.capacity());
                        }
                        node.measured.progress(
                            node.core.end(),
                            node.core.heard_commit(),
                            &replicas,
                            node.core.commit(),
                            tokio::time::Instant::now(),
                        );
                        let now = node.now();
                        if let Some(g) = node.gateway.as_mut() {
                            g.tick(now);
                        }
                        node.gateway_actions();
                    }
                    Some(event) = mqtt_rx.recv() => node.mqtt(event),
                    // A HELLO after the first is ignored; forwards are messages.
                    Some((from, frame)) = inbound_rx.recv() => {
                        if let Frame::Message(message) = frame {
                            if node.trace.on() {
                                match &message {
                                    cluster::Message::Append { prev, records, .. } if !records.is_empty() => {
                                        let last = prev.saturating_add(records.len() as u64);
                                        node.trace.stamp_now(Stage::Received, from, prev.saturating_add(1), last);
                                    }
                                    cluster::Message::Ack { durable, .. } => {
                                        node.trace.stamp_now(Stage::ReportReceived, from, *durable, *durable);
                                    }
                                    _ => {}
                                }
                            }
                            node.core.on_message(from, message);
                        }
                    }
                    Some(proposal) = proposals_rx.recv() => node.propose(proposal),
                    Some(reply) = hand_offs_rx.recv() => {
                        let _ = reply.send(node.core.hand_off());
                    }
                    Some(reply) = metrics_rx.recv() => {
                        let _ = reply.send(node.snapshot(id, state.applied()));
                    }
                    Some(request) = trace_rx.recv() => match request {
                        TraceRequest::Start(capacity) => node.trace.start(capacity),
                        TraceRequest::Take(reply) => {
                            let _ = reply.send(node.trace.take());
                        }
                    },
                    Some(event) = wal_rx.recv() => match event {
                        wal::Event::Durable { lsn, bytes_floor, started, written, synced } => {
                            if node.trace.on() {
                                node.trace.stamp(Stage::WalStarted, id, lsn, lsn, started);
                                node.trace.stamp(Stage::WalWritten, id, lsn, lsn, written);
                                node.trace.stamp(Stage::Synced, id, lsn, lsn, synced);
                                node.trace.stamp_now(Stage::DurableHandled, id, lsn, lsn);
                            }
                            retained_floor = bytes_floor;
                            node.measured.durable(lsn, tokio::time::Instant::now());
                            node.core.on_durable(lsn);
                        }
                        wal::Event::Promised(slot) => node.core.on_promised(slot),
                        wal::Event::Truncated(chain) => {
                            // What was appended past the cut is not the log.
                            drop(recent.split_off(&chain.next_lsn()));
                            node.measured.truncated(chain.next_lsn().saturating_sub(1));
                            node.core.on_truncated(chain);
                        }
                        wal::Event::Snapshot(lsn) => {
                            snapshotting = false;
                            snapshot = lsn;
                            // Never below what a restart needs: the log after
                            // the durable snapshot. A sequencer also keeps what
                            // its replicas still read, up to the byte bound
                            // (D-57 A). A replica keeps the byte bound's worth
                            // too: when it takes the slot over (step 5) it is
                            // the one others catch up from.
                            let floor = node.core.compaction_floor(retained_floor);
                            let _ = wal.send(wal::Command::Compact { floor: floor.min(lsn) });
                        }
                        wal::Event::Compacted(floor) => {
                            cluster::prune_slots(&mut committed_slots, floor);
                            node.core.on_compacted(floor);
                        }
                        wal::Event::Read { to, from, batch, started, done } => {
                            if node.trace.on() {
                                let last = batch
                                    .as_ref()
                                    .map_or(0, |b| from.saturating_add(b.len() as u64).saturating_sub(1));
                                node.trace.stamp(Stage::ReadStarted, to, from, last, started);
                                node.trace.stamp(Stage::ReadDone, to, from, last, done);
                                node.trace.stamp_now(Stage::ReadHandled, to, from, last);
                            }
                            match batch {
                                Some(batch) => node.core.on_read(to, batch),
                                None => node.core.on_read_missing(to, from),
                            }
                        }
                        wal::Event::Range { records, started, done } => {
                            if node.trace.on() {
                                node.trace.stamp(Stage::RangeStarted, id, 0, 0, started);
                                node.trace.stamp(Stage::RangeDone, id, records.len() as u64, 0, done);
                            }
                            reading = false;
                            for record in records {
                                if let Ok(r) = journal::decode(&record) {
                                    recent.entry(r.lsn).or_insert(record);
                                }
                            }
                        }
                        wal::Event::Failed(error) => {
                            tracing::error!(%error, "the log failed; the node stops");
                            break;
                        }
                    },
                }
                let _ = node.core.flush();
                // Carry out what the core asked for, until it asks nothing.
                loop {
                    let steps = node.core.take_effects();
                    if steps.is_empty() {
                        break;
                    }
                    for step in steps {
                        match step {
                            Step::Append { first, records } => {
                                for (offset, record) in records.iter().enumerate() {
                                    recent.insert(first.saturating_add(offset as u64), record.clone());
                                }
                                let last = first.saturating_add(records.len() as u64).saturating_sub(1);
                                node.measured.appended(last, tokio::time::Instant::now());
                                node.trace.stamp_now(Stage::Appended, id, first, last);
                                let _ = wal.send(wal::Command::Append { first, records });
                            }
                            Step::Send { to, message } => {
                                if let Some(link) = node.links.get(&to) {
                                    // Full: dropped; the heartbeat resends — except
                                    // a forward, lost: counted by name (D-115 A).
                                    let forward =
                                        matches!(message, cluster::Message::Forward { .. });
                                    // D-135 A: an append's records, for the bytes
                                    // in flight to this peer.
                                    let shipped = match &message {
                                        cluster::Message::Append { prev, records, .. }
                                            if !records.is_empty() =>
                                        {
                                            Some((
                                                prev.saturating_add(1),
                                                prev.saturating_add(records.len() as u64),
                                                records.iter().map(|r| r.len() as u64).sum::<u64>(),
                                            ))
                                        }
                                        _ => None,
                                    };
                                    let report = match &message {
                                        cluster::Message::Ack { durable, .. } => Some(*durable),
                                        _ => None,
                                    };
                                    if link.try_send(Frame::Message(message)).is_err() {
                                        node.measured.link_dropped(forward);
                                    } else {
                                        if let Some((first, last, bytes)) = shipped {
                                            node.measured.shipped(to, last, bytes);
                                            node.trace.stamp_now(Stage::Shipped, to, first, last);
                                        }
                                        if let Some(durable) = report {
                                            node.trace.stamp_now(Stage::ReportSent, to, durable, durable);
                                        }
                                    }
                                }
                            }
                            Step::Read { to, from, upto } => {
                                node.trace.stamp_now(Stage::ReadAsked, to, from, upto);
                                let _ = wal.send(wal::Command::Read {
                                    to,
                                    from,
                                    upto,
                                    bytes: read_bytes,
                                });
                            }
                            Step::Lost { to } => {
                                tracing::error!(replica = to, "replica below the log's floor; it needs a state copy");
                            }
                            Step::Promise { slot } => {
                                let _ = wal.send(wal::Command::Promise { slot });
                            }
                            Step::Truncate { after } => {
                                let _ = wal.send(wal::Command::Truncate { after });
                            }
                            Step::Committed(_) | Step::ReadDone { .. } => {}
                        }
                    }
                }
                // Apply what is committed, in order; each record's effects
                // reach the gateway before the next record is applied.
                let commit = node.core.commit();
                while state.applied() < commit {
                    let next = state.applied().saturating_add(1);
                    let Some(record) = recent.remove(&next) else {
                        // Older than this process: read it back.
                        if !reading {
                            reading = true;
                            let _ = wal.send(wal::Command::Range { from: next, upto: commit });
                        }
                        break;
                    };
                    let Ok(decoded) = journal::decode(&record) else {
                        tracing::error!(lsn = next, "a committed record does not decode; the node stops");
                        return;
                    };
                    if committed_slots.last().is_none_or(|&(slot, _)| slot < decoded.slot) {
                        committed_slots.push((decoded.slot, decoded.lsn));
                    }
                    // The outbox removes only from its front: the given-up
                    // entries, then the one this record confirms, if any.
                    let held = node.core.held().0;
                    let lost = node.core.applied(&decoded).len();
                    let confirmed = held.saturating_sub(node.core.held().0) > lost;
                    node.measured
                        .left_outbox(lost, confirmed, tokio::time::Instant::now());
                    if lost > 0 {
                        tracing::warn!(lost, "proposals lost before a later one of this node committed; their publishers time out");
                    }
                    let mut out = Vec::new();
                    if let Err(error) = state.apply(&decoded, &mut out) {
                        tracing::error!(%error, "a committed record does not apply; the node stops");
                        return;
                    }
                    if out.is_empty() {
                        continue;
                    }
                    // Observers see an effect before any client does: a test
                    // can then check a PUBACK came after its commit.
                    // Cloned only for a test that listens.
                    if effects_tx.receiver_count() > 0 {
                        let _ = effects_tx.send((next, decoded.slot, out.clone()));
                    }
                    let now = node.now();
                    if let Some(g) = node.gateway.as_mut() {
                        for effect in &out {
                            g.effect(effect, now);
                        }
                    }
                    node.gateway_actions();
                }
                let now = tokio::time::Instant::now();
                node.measured.committed_through(commit, now);
                node.measured
                    .roles(node.core.taking(), node.core.leading(), node.core.slot(), now);
                node.measured.blocked(node.core.outbox_full(), now);
                node.resume();
                // Only what is durable here: a restart must find the log up
                // to the snapshot, or it could not check the two meet.
                if !snapshotting
                    && state.applied().saturating_sub(snapshot) >= snapshot_every
                    && state.applied() <= node.core.durable()
                {
                    let bytes = encode_snapshot(&committed_slots, &state);
                    let _ = wal.send(wal::Command::Snapshot {
                        lsn: state.applied(),
                        bytes,
                    });
                    snapshotting = true;
                }
                // The digest hashes the whole state: only when it changed.
                let digest = if state.applied() == last.applied {
                    last.digest
                } else {
                    state.digest()
                };
                last = Status {
                    end: node.core.end(),
                    commit,
                    applied: state.applied(),
                    digest,
                    slot: node.core.slot(),
                    leading: node.core.leading(),
                    refused: node.measured.refused,
                    given_up: node.measured.given_up,
                };
                status_tx.send_if_modified(|current| {
                    let changed = *current != last;
                    *current = last;
                    changed
                });
            }
        }));

        Ok(Self {
            listen,
            mqtt: mqtt_listen,
            proposals,
            hand_offs,
            metrics,
            metrics_listen,
            trace,
            effects,
            status,
            stop: Some(stop),
            tasks,
            wal: Some(wal_thread),
        })
    }

    /// Proposes a record: to the log if this node is the sequencer, over
    /// the link to it otherwise.
    pub async fn propose(&self, proposal: Proposal) {
        let _ = self.proposals.send(proposal).await;
    }

    /// Asks this node to hand its slot over to the next owner in the
    /// schedule (ADR-0008 step 6; the operator's command, D-105 A): a
    /// protocol handoff — the queue is written, then `SlotEnd`, and the
    /// sequencer steps down once it has committed and the successor holds
    /// it, or after `takeover_ticks` heartbeats; fencing, the majority and
    /// the committed prefix rule as in any slot change. It neither stops
    /// the node nor forces a leader: the node goes on serving, and if the
    /// successor fails the next one takes over by silence.
    ///
    /// # Errors
    /// `NotLeader` when this node is not sequencing (or has stopped),
    /// `NoSuccessor` when the next slot is its own again.
    pub async fn hand_off(&self) -> Result<(), cluster::ClusterError> {
        let (reply, answer) = oneshot::channel();
        if self.hand_offs.send(reply).await.is_err() {
            return Err(cluster::ClusterError::NotLeader);
        }
        answer
            .await
            .unwrap_or(Err(cluster::ClusterError::NotLeader))
    }

    /// A snapshot of what this node measures (D-112), as it is now; `None`
    /// once the node stopped.
    pub async fn metrics(&self) -> Option<metrics::Snapshot> {
        let (reply, answer) = oneshot::channel();
        self.metrics.send(reply).await.ok()?;
        answer.await.ok()
    }

    /// Starts recording the stages' stamps (D-136 A), at most `capacity`,
    /// discarding any recorded before. Diagnostic: the cap benchmark's
    /// catch-up mode; nothing reads them back into a decision.
    pub async fn trace_start(&self, capacity: usize) {
        let _ = self.trace.send(TraceRequest::Start(capacity)).await;
    }

    /// The stamps recorded since `trace_start`; recording stops. `None`
    /// once the node stopped.
    pub async fn trace_take(&self) -> Option<metrics::Trace> {
        let (reply, answer) = oneshot::channel();
        self.trace.send(TraceRequest::Take(reply)).await.ok()?;
        answer.await.ok()
    }

    /// Where proposals go, as [`Node::propose`], for a task that outlives
    /// a borrow of the node (the cap benchmark's load, D-84 A).
    pub fn proposer(&self) -> mpsc::Sender<Proposal> {
        self.proposals.clone()
    }

    pub fn status(&self) -> watch::Receiver<Status> {
        self.status.clone()
    }

    /// Every committed record's effects, as this node applies them, with
    /// its lsn and slot: `(lsn, slot, effects)`.
    pub fn effects(&self) -> broadcast::Receiver<(u64, u64, Vec<Effect>)> {
        self.effects.subscribe()
    }

    /// Stops the node: its links and connections close, the loop ends, the log thread
    /// finishes what it was given and exits.
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
        if let Some(wal) = self.wal.take() {
            let _ = tokio::task::spawn_blocking(move || wal.join()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_body_carries_the_slot_table_and_refuses_a_damaged_one() {
        let state = State::new(Limits::default());
        let slots = vec![(3, 20), (4, 30)];
        let bytes = encode_snapshot(&slots, &state);
        let (decoded, rest) = decode_snapshot(&bytes).unwrap();
        assert_eq!(decoded, slots);
        assert_eq!(State::decode(Limits::default(), rest).unwrap(), state);

        // Another version, a count past the bytes, a table not ascending.
        let mut other = bytes.clone();
        other[0] = 1;
        assert!(decode_snapshot(&other).is_err());
        let mut long = bytes.clone();
        long[1..5].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_snapshot(&long).is_err());
        let backwards = encode_snapshot(&[(4, 30), (3, 20)], &state);
        assert!(decode_snapshot(&backwards).is_err());
        for cut in 0..5 {
            assert!(decode_snapshot(&bytes[..cut]).is_err());
        }
    }
}
