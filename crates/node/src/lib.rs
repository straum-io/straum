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
mod mqtt;
mod wal;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use apply::{Effect, Limits, State};
use cluster::wire::Frame;
use cluster::{Config as CoreConfig, Effect as Step, Proposal, Replica, Sequencer};
use gateway::{Action, Gateway};
use storage::SegmentLimits;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use transport::{BufferBudget, Listener, TransportConfig};

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("storage: {0}")]
    Storage(#[from] storage::StorageError),
    #[error("the log does not recover as one chain: {0}")]
    Recovery(#[from] journal::ChainError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("transport: {0}")]
    Transport(#[from] transport::TransportError),
}

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub listen: SocketAddr,
    /// `node` and `first_session` are the node's to set and are overwritten.
    pub gateway: gateway::Config,
    /// Packets queued per connection; a client that lets the queue fill is
    /// closed (CLAUDE.md §8: a writer that makes no progress).
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
}

/// What a node reports about itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Status {
    pub end: u64,
    pub commit: u64,
    pub applied: u64,
    /// `apply::State::digest` at `applied`.
    pub digest: u64,
}

/// A running node. Dropping it does not stop it; `shutdown` does.
pub struct Node {
    pub listen: SocketAddr,
    pub mqtt: Option<SocketAddr>,
    proposals: mpsc::Sender<Proposal>,
    effects: broadcast::Sender<(u64, Vec<Effect>)>,
    status: watch::Receiver<Status>,
    stop: Option<oneshot::Sender<()>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    wal: Option<std::thread::JoinHandle<()>>,
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

#[expect(
    clippy::large_enum_variant,
    reason = "one per node: its size is never multiplied"
)]
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

/// What the node loop owns besides the applied state.
struct Loop {
    core: Core,
    owner: u8,
    links: BTreeMap<u8, mpsc::Sender<Frame>>,
    gateway: Option<Gateway>,
    conns: HashMap<u64, mqtt::Conn>,
    /// Connections whose reader waits for the gateway to want more.
    paused: BTreeSet<u64>,
    started: tokio::time::Instant,
}

impl Loop {
    fn now(&self) -> Duration {
        self.started.elapsed()
    }

    fn propose(&mut self, proposal: Proposal) {
        match &mut self.core {
            Core::Sequencer(s) => {
                // ponytail: dropped, not held. Nothing is acknowledged that
                // was not committed: the gateway closes a publisher whose
                // proposal does not commit within `commit_timeout`, and
                // resends its lifecycle proposals. Holding them back is a
                // card (D-69).
                if s.propose(proposal).is_err() {
                    tracing::warn!("proposal refused: the sequencer's queue is full");
                }
            }
            Core::Replica(_) => {
                // Forwarded over a link that keeps order
                // (gateway::Action::Propose).
                if let Some(link) = self.links.get(&self.owner) {
                    let _ = link.try_send(Frame::Forward(proposal));
                }
            }
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
        let wants = self.gateway.as_ref().is_some_and(|g| g.wants_read(conn));
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
        let (wal_events, mut wal_rx) = mpsc::channel(1024);
        let (chain, wal, wal_thread) =
            wal::start(config.data.clone(), config.segments, wal_events)?;

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
        let limits = limits_digest(&config.limits);
        let slot = config.core.slot;
        let id = config.core.id;
        let owner = config.core.owner;

        let (inbound, mut inbound_rx) = mpsc::channel::<link::Inbound>(1024);
        let mut tasks = vec![tokio::spawn(link::listen(
            listener,
            config.core.nodes.clone(),
            slot,
            limits,
            config.max_frame,
            inbound,
        ))];
        let mut links = BTreeMap::new();
        for &(peer, address) in &config.peers {
            let (tx, rx) = mpsc::channel(config.link_queue);
            links.insert(peer, tx);
            tasks.push(tokio::spawn(link::dial(
                address,
                Frame::Hello {
                    node: id,
                    slot,
                    limits,
                },
                rx,
                config.transport,
                Arc::clone(&budget),
            )));
        }

        let started = tokio::time::Instant::now();
        let (mqtt_tx, mut mqtt_rx) = mpsc::channel::<mqtt::Event>(1024);
        let mut mqtt_listen = None;
        let gateway = match &config.mqtt {
            Some(m) => {
                let first_session = incarnation(&config.data)? << 32;
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

        let core = if id == owner {
            Core::Sequencer(Sequencer::new(config.core.clone(), chain))
        } else {
            Core::Replica(Replica::new(config.core.clone(), chain))
        };
        let mut node = Loop {
            core,
            owner,
            links,
            gateway,
            conns: HashMap::new(),
            paused: BTreeSet::new(),
            started,
        };
        // The gateway's NodeReset goes to the log before anything else.
        node.gateway_actions();
        let effects_tx = effects.clone();
        let heartbeat = config.heartbeat;
        let limits_config = config.limits;
        tasks.push(tokio::spawn(async move {
            let mut state = State::new(limits_config);
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
                        if let Core::Sequencer(s) = &mut node.core {
                            s.tick();
                        }
                        let now = node.now();
                        if let Some(g) = node.gateway.as_mut() {
                            g.tick(now);
                        }
                        node.gateway_actions();
                    }
                    Some(event) = mqtt_rx.recv() => node.mqtt(event),
                    Some((from, frame)) = inbound_rx.recv() => match (frame, &mut node.core) {
                        (Frame::Message(message), Core::Sequencer(s)) => s.on_message(from, message),
                        (Frame::Message(message), Core::Replica(r)) => r.on_message(from, message),
                        (Frame::Forward(proposal), Core::Sequencer(_)) => node.propose(proposal),
                        _ => {}
                    },
                    Some(proposal) = proposals_rx.recv() => node.propose(proposal),
                    Some(event) = wal_rx.recv() => match (event, &mut node.core) {
                        (wal::Event::Durable(lsn), Core::Sequencer(s)) => s.on_durable(lsn),
                        (wal::Event::Durable(lsn), Core::Replica(r)) => r.on_durable(lsn),
                        (wal::Event::Read { to, from, batch }, Core::Sequencer(s)) => match batch {
                            Some(batch) => s.on_read(to, batch),
                            None => s.on_read_missing(to, from),
                        },
                        (wal::Event::Range(records), _) => {
                            reading = false;
                            for record in records {
                                if let Ok(r) = journal::decode(&record) {
                                    recent.entry(r.lsn).or_insert(record);
                                }
                            }
                        }
                        (wal::Event::Failed(error), _) => {
                            tracing::error!(%error, "the log failed; the node stops");
                            break;
                        }
                        _ => {}
                    },
                }
                if let Core::Sequencer(s) = &mut node.core {
                    let _ = s.flush();
                }
                // Carry out what the core asked for, until it asks nothing.
                loop {
                    let steps = node.core.take();
                    if steps.is_empty() {
                        break;
                    }
                    for step in steps {
                        match step {
                            Step::Append { first, records } => {
                                for (offset, record) in records.iter().enumerate() {
                                    recent.insert(first.saturating_add(offset as u64), record.clone());
                                }
                                let _ = wal.send(wal::Command::Append { first, records });
                            }
                            Step::Send { to, message } => {
                                if let Some(link) = node.links.get(&to) {
                                    // Full: dropped; the heartbeat resends.
                                    let _ = link.try_send(Frame::Message(message));
                                }
                            }
                            Step::Read { to, from } => {
                                let _ = wal.send(wal::Command::Read { to, from });
                            }
                            Step::Lost { to } => {
                                tracing::error!(replica = to, "replica below the log's floor; it needs a state copy");
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
                        let _ = effects_tx.send((next, out.clone()));
                    }
                    let now = node.now();
                    if let Some(g) = node.gateway.as_mut() {
                        for effect in &out {
                            g.effect(effect, now);
                        }
                    }
                    node.gateway_actions();
                }
                node.resume();
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

    pub fn status(&self) -> watch::Receiver<Status> {
        self.status.clone()
    }

    /// Every committed record's effects, as this node applies them.
    pub fn effects(&self) -> broadcast::Receiver<(u64, Vec<Effect>)> {
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
