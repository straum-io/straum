//! One shard on one core: one compio runtime thread owns the store and
//! runs every connection, the group commit and every waiting fetch as
//! tasks. No other thread touches broker state.
//!
//! ```text
//! accept ─► connection: reader ──► admission ──► produce queue ──► commit task ──► store.put_batch
//!                  │   ▲ stops reading at a budget              (one group at a time)       │
//!                  │   └── per-connection in-flight bytes, global pending bytes             │
//!                  └─► request tasks (fetch, admin) ─► response queue ─► writer ◄── acks ◄──┘
//! ```

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use compio::BufResult;
use compio::buf::{IntoInner, IoBuf};
use compio::io::{AsyncReadExt, AsyncWriteExt};
use compio::net::{TcpListener, TcpStream};
use straum_proto::{
    self as proto, BrokerStatsOk, DescribeTopicOk, ErrorBody, ErrorCode, FetchOk, FetchedMessage,
    FrameHeader, HelloOk, MetadataOk, Node, PartitionDescription, PartitionMetadata, ProduceOk,
    Request, Response, TopicMetadata, TopicSummary, ack, types,
};
use straum_store::{Store, StoreError};

use crate::catalog::{self, Catalog, Partition, TopicRecord};
use crate::config::Config;
use crate::identity::{self, Identity};
use crate::sync::{Event, LocalMutex, Oneshot};

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error(transparent)]
    Identity(#[from] identity::IdentityError),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("{0}: a record of m/topics does not decode")]
    Catalog(u64),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A produce request admitted to the queue.
struct Job {
    partition: Rc<Partition>,
    leader_epoch: u32,
    messages: Vec<proto::Message>,
    /// Frame size, counted in `pending_bytes` until the reply.
    bytes: u64,
    reply: Oneshot<Result<ProduceOk, ErrorBody>>,
}

#[derive(Default)]
struct Counters {
    produce_requests: Cell<u64>,
    groups_committed: Cell<u64>,
}

struct Shared {
    config: Config,
    identity: Identity,
    store: LocalMutex<Store>,
    catalog: RefCell<Catalog>,
    queue: RefCell<VecDeque<Job>>,
    /// Notified when a job is queued, and at shutdown.
    queued: Event,
    pending_bytes: Cell<u64>,
    pending_peak: Cell<u64>,
    /// Notified when pending bytes are released.
    pending_released: Event,
    connections: Cell<u32>,
    fetch_waiters: Cell<u32>,
    /// Requests read and not yet answered, on all connections.
    outstanding: Cell<u64>,
    answered: Event,
    closing: Cell<bool>,
    stop: Arc<AtomicBool>,
    /// A write to the store failed: it refuses writes until restarted.
    storage_failed: Cell<bool>,
    /// The broker assigns timestamps; never below the last one assigned or
    /// stored, so a clock step backwards does not reach the WAL.
    clock_floor: Cell<u64>,
    counters: Counters,
}

impl Shared {
    fn add_pending(&self, n: u64) {
        let v = self.pending_bytes.get() + n;
        self.pending_bytes.set(v);
        self.pending_peak.set(self.pending_peak.get().max(v));
    }

    fn release_pending(&self, n: u64) {
        self.pending_bytes
            .set(self.pending_bytes.get().saturating_sub(n));
        self.pending_released.notify();
    }

    fn timestamp(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        let ts = now.max(self.clock_floor.get());
        self.clock_floor.set(ts);
        ts
    }
}

/// Stops a running broker from another thread: `serve` returns after its
/// shutdown sequence.
#[derive(Clone)]
pub struct StopHandle {
    flag: Arc<AtomicBool>,
    addr: SocketAddr,
}

impl StopHandle {
    pub fn stop(&self) {
        self.flag.store(true, Ordering::SeqCst);
        // Wakes the accept loop, which checks the flag after every accept.
        let _ = std::net::TcpStream::connect_timeout(&self.addr, Duration::from_secs(1));
    }
}

/// Opens the data directory and the store, then serves until stopped.
/// `ready` gets the bound address and a handle that stops the broker.
pub async fn serve(
    config: Config,
    ready: impl FnOnce(SocketAddr, StopHandle),
) -> Result<(), BrokerError> {
    std::fs::create_dir_all(&config.data_dir)?;
    let identity = Identity {
        cluster_id: config.cluster_id.clone(),
        node_id: config.node_id,
    };
    let created = identity::check_or_create(&config.data_dir, &identity)?;
    let store_config = straum_store::Config {
        segment_max_bytes: config.store_segment_max_bytes,
        max_frame_bytes: config.store_max_frame_bytes,
        write_mode: config.store_write_mode,
        rebuild_index: config.store_rebuild_index,
        ..straum_store::Config::default()
    };
    let store = Store::open(config.data_dir.join("store"), store_config).await?;
    let report = store.recovery_report();
    eprintln!(
        "straum-broker: node {} of cluster {:?}{}; store recovery: {} action(s)",
        identity.node_id,
        identity.cluster_id,
        if created { " (new data directory)" } else { "" },
        report.actions.len()
    );
    if config.store_rebuild_index {
        eprintln!(
            "straum-broker: recovery: topic index rebuilt from the WAL (--store-rebuild-index, \
             this start only)"
        );
    }
    for action in &report.actions {
        eprintln!("straum-broker: recovery: {action:?}");
    }
    let (catalog, clock_floor) = load_catalog(&store, config.node_id).await?;

    let listener = TcpListener::bind(config.listen).await?;
    let addr = listener.local_addr()?;
    let stop = Arc::new(AtomicBool::new(false));
    let shared = Rc::new(Shared {
        identity,
        store: LocalMutex::new(store),
        catalog: RefCell::new(catalog),
        queue: RefCell::new(VecDeque::new()),
        queued: Event::default(),
        pending_bytes: Cell::new(0),
        pending_peak: Cell::new(0),
        pending_released: Event::default(),
        connections: Cell::new(0),
        fetch_waiters: Cell::new(0),
        outstanding: Cell::new(0),
        answered: Event::default(),
        closing: Cell::new(false),
        stop: Arc::clone(&stop),
        storage_failed: Cell::new(false),
        clock_floor: Cell::new(clock_floor),
        counters: Counters::default(),
        config,
    });
    let commit = compio::runtime::spawn(commit_loop(Rc::clone(&shared)));
    let driver = compio::runtime::Runtime::with_current(|rt| rt.driver_type());
    eprintln!("straum-broker: compio driver {driver:?}");
    eprintln!("straum-broker: listening on {addr}");
    ready(addr, StopHandle { flag: stop, addr });

    loop {
        let accepted = listener.accept().await;
        if shared.stop.load(Ordering::SeqCst) {
            break;
        }
        let Ok((stream, _)) = accepted else {
            continue;
        };
        if shared.connections.get() >= shared.config.max_connections {
            drop(stream);
            continue;
        }
        let _ = stream.set_nodelay(true);
        // Counted here, not in the task, so a burst of accepts cannot pass
        // `max_connections` before the tasks run.
        shared.connections.set(shared.connections.get() + 1);
        compio::runtime::spawn(connection(Rc::clone(&shared), stream)).detach();
    }

    // Shutdown: no new connections or requests; finish the queued groups,
    // answer waiting fetches, wait for the answers to be written.
    eprintln!("straum-broker: stopping");
    drop(listener);
    shared.closing.set(true);
    shared.queued.notify();
    for p in shared.catalog.borrow().partitions() {
        p.committed.notify();
    }
    let _ = commit.await;
    let deadline =
        Instant::now() + Duration::from_millis(u64::from(shared.config.shutdown_timeout_ms));
    while shared.outstanding.get() > 0 {
        let g = shared.answered.generation();
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero()
            || compio::time::timeout(left, shared.answered.wait_change(g))
                .await
                .is_err()
        {
            break;
        }
    }
    eprintln!("straum-broker: stopped");
    Ok(())
}

/// Topics from `m/topics`, and the highest timestamp stored.
async fn load_catalog(store: &Store, node_id: u32) -> Result<(Catalog, u64), BrokerError> {
    let mut catalog = Catalog::new(node_id);
    let mut floor = 0;
    if let Some((first, next)) = store.topic_range(catalog::META_TOPIC) {
        for seq in first..next {
            let Some(e) = store.read_entry(catalog::META_TOPIC, seq).await? else {
                continue;
            };
            floor = floor.max(e.timestamp);
            let record = TopicRecord::decode(&e.payload).ok_or(BrokerError::Catalog(seq))?;
            catalog.add(record, |t| store.topic_range(t).unwrap_or((0, 0)));
        }
    }
    let partitions: Vec<(Vec<u8>, u64)> = catalog
        .partitions()
        .map(|p| (p.store_topic.clone(), p.next_seq.get()))
        .collect();
    for (topic, next) in partitions {
        if next > 0
            && let Some(e) = store.read_entry(&topic, next - 1).await?
        {
            floor = floor.max(e.timestamp);
        }
    }
    Ok((catalog, floor))
}

// --- Connections --------------------------------------------------------------

struct Conn {
    out: RefCell<VecDeque<(Vec<u8>, u64)>>,
    queued: Event,
    /// Bytes of requests read and not yet answered and written, plus the
    /// reserved size of fetch responses.
    inflight: Cell<u64>,
    released: Event,
    /// Requests read and not yet answered.
    outstanding: Cell<u64>,
    reading: Cell<bool>,
    broken: Cell<bool>,
    hello: Cell<bool>,
}

impl Conn {
    /// Queues a response; `charge` is released once it is written.
    fn respond(&self, sh: &Shared, correlation_id: u64, resp: &Response, charge: u64) {
        let mut frame = Vec::new();
        proto::encode_response(&mut frame, correlation_id, resp);
        self.outstanding
            .set(self.outstanding.get().saturating_sub(1));
        sh.outstanding.set(sh.outstanding.get().saturating_sub(1));
        sh.answered.notify();
        if self.broken.get() {
            self.release(charge);
            return;
        }
        self.out.borrow_mut().push_back((frame, charge));
        self.queued.notify();
    }

    fn error(&self, sh: &Shared, correlation_id: u64, code: ErrorCode, message: &str, charge: u64) {
        self.respond(sh, correlation_id, &Response::error(code, message), charge);
    }

    fn release(&self, charge: u64) {
        self.inflight
            .set(self.inflight.get().saturating_sub(charge));
        self.released.notify();
    }
}

async fn connection(sh: Rc<Shared>, stream: TcpStream) {
    let conn = Rc::new(Conn {
        out: RefCell::new(VecDeque::new()),
        queued: Event::default(),
        inflight: Cell::new(0),
        released: Event::default(),
        outstanding: Cell::new(0),
        reading: Cell::new(true),
        broken: Cell::new(false),
        hello: Cell::new(false),
    });
    let (mut rd, wr) = stream.into_split();
    compio::runtime::spawn(writer(Rc::clone(&sh), Rc::clone(&conn), wr)).detach();
    let _ = read_frames(&sh, &conn, &mut rd).await;
    conn.reading.set(false);
    conn.queued.notify();
}

/// Reads frames until EOF, an error, a fatal frame or shutdown.
async fn read_frames(sh: &Rc<Shared>, conn: &Rc<Conn>, rd: &mut TcpStream) -> std::io::Result<()> {
    let cfg = &sh.config;
    while !sh.closing.get() && !conn.broken.get() {
        let BufResult(res, head) = rd
            .read_exact(Vec::with_capacity(proto::FRAME_HEADER).slice(..proto::FRAME_HEADER))
            .await;
        res?;
        let head = head.into_inner();
        let Ok(head) = <&[u8; proto::FRAME_HEADER]>::try_from(&head[..]) else {
            return Ok(());
        };
        let h = FrameHeader::parse(head);
        let Some(body_len) = h.body_len() else {
            begin(sh, conn);
            conn.error(
                sh,
                0,
                ErrorCode::MalformedRequest,
                "frame length below 10",
                0,
            );
            return Ok(());
        };
        let size = h.frame_size();
        if size > u64::from(cfg.max_frame_bytes) {
            begin(sh, conn);
            conn.error(
                sh,
                h.correlation_id,
                ErrorCode::FrameTooLarge,
                &format!(
                    "frame of {size} bytes; the limit is {}",
                    cfg.max_frame_bytes
                ),
                0,
            );
            return Ok(());
        }
        // Backpressure: wait for room in this connection's budget (a fetch
        // reserves its largest response too) and, for a produce, in the
        // global one. Nothing more is read from this socket meanwhile.
        let charge = size
            + if h.msg_type == types::FETCH {
                u64::from(cfg.max_fetch_bytes)
            } else {
                0
            };
        loop {
            let g = conn.released.generation();
            if conn.inflight.get() + charge <= cfg.max_inflight_bytes {
                break;
            }
            conn.released.wait_change(g).await;
        }
        // The global budget is reserved now, before the body is read:
        // other connections pass their check while this one reads.
        let pending = if h.msg_type == types::PRODUCE {
            loop {
                let g = sh.pending_released.generation();
                if sh.pending_bytes.get() + size <= cfg.max_pending_bytes {
                    break;
                }
                sh.pending_released.wait_change(g).await;
            }
            sh.add_pending(size);
            size
        } else {
            0
        };
        let BufResult(res, body) = rd
            .read_exact(Vec::with_capacity(body_len).slice(..body_len))
            .await;
        if res.is_err() || sh.closing.get() {
            // A request read once shutdown began is not handled: the commit
            // task may have finished, and it would never be answered.
            sh.release_pending(pending);
            return res.map(|_| ());
        }
        let body = body.into_inner();
        conn.inflight.set(conn.inflight.get() + charge);
        begin(sh, conn);
        handle(sh, conn, h, &body, charge, pending);
    }
    Ok(())
}

/// Counts a request as outstanding until `respond`.
fn begin(sh: &Shared, conn: &Conn) {
    conn.outstanding.set(conn.outstanding.get() + 1);
    sh.outstanding.set(sh.outstanding.get() + 1);
}

async fn writer(sh: Rc<Shared>, conn: Rc<Conn>, mut wr: TcpStream) {
    loop {
        let g = conn.queued.generation();
        let next = conn.out.borrow_mut().pop_front();
        match next {
            Some((frame, charge)) => {
                let BufResult(res, _) = wr.write_all(frame).await;
                conn.release(charge);
                if res.is_err() {
                    conn.broken.set(true);
                    let dropped: Vec<_> = conn.out.borrow_mut().drain(..).collect();
                    for (_, c) in dropped {
                        conn.release(c);
                    }
                    break;
                }
            }
            None if !conn.reading.get() && conn.outstanding.get() == 0 => break,
            None => conn.queued.wait_change(g).await,
        }
    }
    sh.connections.set(sh.connections.get().saturating_sub(1));
}

/// `pending` is the global budget reserved for a Produce frame; every path
/// that does not queue the request as a job releases it.
fn handle(
    sh: &Rc<Shared>,
    conn: &Rc<Conn>,
    h: FrameHeader,
    body: &[u8],
    charge: u64,
    pending: u64,
) {
    let corr = h.correlation_id;
    let req = Request::decode(h.msg_type, body);
    if !matches!(req, Ok(Request::Produce(_))) {
        sh.release_pending(pending);
    }
    let req = match req {
        Ok(r) => r,
        Err(proto::DecodeError::UnknownType(t)) => {
            return conn.error(
                sh,
                corr,
                ErrorCode::Unsupported,
                &format!("unknown message type {t:#06x}"),
                charge,
            );
        }
        Err(e) => {
            return conn.error(
                sh,
                corr,
                ErrorCode::MalformedRequest,
                &e.to_string(),
                charge,
            );
        }
    };
    if !conn.hello.get() && !matches!(req, Request::Hello(_) | Request::Ping { .. }) {
        sh.release_pending(pending);
        return conn.error(sh, corr, ErrorCode::Unauthenticated, "Hello first", charge);
    }
    match req {
        Request::Hello(hello) => {
            let resp = if conn.hello.get() {
                Response::error(ErrorCode::Unsupported, "a second Hello on one connection")
            } else if hello.min_version > hello.max_version {
                Response::error(ErrorCode::MalformedRequest, "min_version above max_version")
            } else if !(hello.min_version..=hello.max_version).contains(&proto::VERSION) {
                Response::Error(ErrorBody {
                    min_version: proto::VERSION,
                    max_version: proto::VERSION,
                    ..ErrorBody::new(ErrorCode::UnsupportedVersion, "no common protocol version")
                })
            } else if hello.auth_mechanism != "none" {
                Response::error(
                    ErrorCode::Unauthenticated,
                    "this broker supports only the auth mechanism none",
                )
            } else {
                conn.hello.set(true);
                Response::HelloOk(HelloOk {
                    version: proto::VERSION,
                    cluster_id: sh.identity.cluster_id.clone(),
                    node_id: sh.identity.node_id,
                    max_frame_bytes: sh.config.max_frame_bytes,
                    max_message_bytes: sh.config.max_message_bytes,
                })
            };
            conn.respond(sh, corr, &resp, charge);
        }
        Request::Ping { nonce } => conn.respond(sh, corr, &Response::Pong { nonce }, charge),
        Request::Metadata { topics } => {
            let resp = if topics.len() > proto::MAX_REQUEST_TOPICS as usize {
                Response::error(
                    ErrorCode::MalformedRequest,
                    format!(
                        "Metadata names more than {} topics",
                        proto::MAX_REQUEST_TOPICS
                    ),
                )
            } else {
                metadata(sh, topics)
            };
            conn.respond(sh, corr, &resp, charge);
        }
        Request::ListTopics { cursor, limit } => {
            let resp = if limit == 0 {
                Response::error(ErrorCode::MalformedRequest, "ListTopics limit 0")
            } else {
                list_topics(sh, &cursor, limit.min(proto::MAX_REQUEST_TOPICS))
            };
            conn.respond(sh, corr, &resp, charge);
        }
        Request::DeleteTopic { .. } => conn.error(
            sh,
            corr,
            ErrorCode::Unsupported,
            "DeleteTopic is not supported in version 0",
            charge,
        ),
        Request::Produce(p) => produce(sh, conn, corr, p, charge, pending),
        req => {
            let (sh, conn) = (Rc::clone(sh), Rc::clone(conn));
            compio::runtime::spawn(async move {
                let resp = match req {
                    Request::CreateTopic(c) => create_topic(&sh, c).await,
                    Request::DescribeTopic { name } => describe_topic(&sh, &name).await,
                    Request::BrokerStats => broker_stats(&sh).await,
                    Request::Fetch(f) => fetch(&sh, f).await,
                    _ => Response::error(ErrorCode::Unsupported, "unexpected request"),
                };
                conn.respond(&sh, corr, &resp, charge);
            })
            .detach();
        }
    }
}

/// One page of topics after `cursor`; `next_cursor` is the page's last
/// name if more topics follow it, else empty.
fn list_topics(sh: &Shared, cursor: &str, limit: u32) -> Response {
    let catalog = sh.catalog.borrow();
    let mut rest = catalog.topics_after(cursor);
    let topics: Vec<TopicSummary> = rest
        .by_ref()
        .take(limit as usize)
        .map(|t| TopicSummary {
            name: t.name.clone(),
            partitions: t.partitions,
        })
        .collect();
    let next_cursor = match (rest.next(), topics.last()) {
        (Some(_), Some(last)) => last.name.clone(),
        _ => String::new(),
    };
    Response::ListTopicsOk {
        topics,
        next_cursor,
    }
}

fn metadata(sh: &Shared, requested: Vec<String>) -> Response {
    let catalog = sh.catalog.borrow();
    let topics = requested
        .into_iter()
        .map(|name| match catalog.topic(&name) {
            None => TopicMetadata {
                name,
                error: ErrorCode::UnknownTopic as u16,
                partitions: vec![],
            },
            Some(t) => {
                let partitions = (0..t.partitions)
                    .filter_map(|p| catalog.lookup(&name, p).ok().map(|e| (p, e)))
                    .map(|(p, e)| PartitionMetadata {
                        partition: p,
                        leader_node_id: e.placement.leader,
                        leader_epoch: e.placement.leader_epoch,
                        replicas: e.placement.replicas.clone(),
                        in_sync_replicas: e.placement.replicas.clone(),
                        start_seq: e.partition.start_seq.get(),
                        next_seq: e.partition.next_seq.get(),
                    })
                    .collect();
                TopicMetadata {
                    name,
                    error: 0,
                    partitions,
                }
            }
        })
        .collect();
    let (host, port) = sh.config.advertised();
    Response::MetadataOk(MetadataOk {
        cluster_id: sh.identity.cluster_id.clone(),
        controller_node_id: sh.identity.node_id,
        nodes: vec![Node {
            node_id: sh.identity.node_id,
            host,
            port,
            rack: String::new(),
        }],
        topics,
    })
}

/// The partition of a request, through the placement table, with the
/// epoch check of `expected_leader_epoch`.
fn locate(
    sh: &Shared,
    topic: &str,
    partition: u32,
    expected_epoch: u32,
) -> Result<(Rc<Partition>, u32), ErrorBody> {
    catalog::check_name(topic).map_err(|c| ErrorBody::new(c, "topic name"))?;
    let catalog = sh.catalog.borrow();
    let e = catalog
        .lookup(topic, partition)
        .map_err(|c| ErrorBody::new(c, format!("{topic}/{partition}")))?;
    let epoch = e.placement.leader_epoch;
    if expected_epoch != 0 && expected_epoch != epoch {
        return Err(ErrorBody {
            leader_node_id: e.placement.leader,
            leader_epoch: epoch,
            ..ErrorBody::new(
                ErrorCode::FencedLeaderEpoch,
                format!("expected leader epoch {expected_epoch}, the partition is at {epoch}"),
            )
        });
    }
    Ok((Rc::clone(&e.partition), epoch))
}

fn check_limits(cfg: &Config, m: &proto::Message) -> Result<(), String> {
    if m.body.len() as u64 > u64::from(cfg.max_message_bytes) {
        return Err(format!(
            "body of {} bytes; the limit is {}",
            m.body.len(),
            cfg.max_message_bytes
        ));
    }
    if let Some(k) = &m.key
        && k.len() as u64 > u64::from(cfg.max_key_bytes)
    {
        return Err(format!(
            "key of {} bytes; the limit is {}",
            k.len(),
            cfg.max_key_bytes
        ));
    }
    if m.headers.len() as u64 > u64::from(cfg.max_headers) {
        return Err(format!(
            "{} headers; the limit is {}",
            m.headers.len(),
            cfg.max_headers
        ));
    }
    if let Some(h) = m
        .headers
        .iter()
        .find(|h| h.name.len() as u64 > u64::from(cfg.max_header_name_bytes))
    {
        return Err(format!(
            "header name of {} bytes; the limit is {}",
            h.name.len(),
            cfg.max_header_name_bytes
        ));
    }
    if m.header_bytes() as u64 > u64::from(cfg.max_header_bytes) {
        return Err(format!(
            "{} header bytes; the limit is {}",
            m.header_bytes(),
            cfg.max_header_bytes
        ));
    }
    Ok(())
}

fn produce(sh: &Rc<Shared>, conn: &Rc<Conn>, corr: u64, p: proto::Produce, charge: u64, size: u64) {
    sh.counters
        .produce_requests
        .set(sh.counters.produce_requests.get() + 1);
    let refuse = |code, msg: &str| {
        sh.release_pending(size);
        conn.error(sh, corr, code, msg, charge)
    };
    let (partition, epoch) = match locate(sh, &p.topic, p.partition, p.expected_leader_epoch) {
        Ok(x) => x,
        Err(e) => {
            sh.release_pending(size);
            return conn.respond(sh, corr, &Response::Error(e), charge);
        }
    };
    match p.ack_level {
        ack::DURABLE => {}
        ack::WRITTEN | ack::REPLICATED => {
            return refuse(
                ErrorCode::UnsupportedAckLevel,
                "version 0 supports only ack level durable",
            );
        }
        _ => return refuse(ErrorCode::MalformedRequest, "unknown ack level"),
    }
    if p.messages.is_empty() {
        return refuse(
            ErrorCode::MalformedRequest,
            "a Produce needs at least one message",
        );
    }
    if let Some(why) = p
        .messages
        .iter()
        .find_map(|m| check_limits(&sh.config, m).err())
    {
        return refuse(ErrorCode::MessageTooLarge, &why);
    }
    if sh.storage_failed.get() {
        return refuse(
            ErrorCode::StorageError,
            "a store write failed; restart the broker",
        );
    }
    let reply = Oneshot::new();
    sh.queue.borrow_mut().push_back(Job {
        partition,
        leader_epoch: epoch,
        messages: p.messages,
        bytes: size,
        reply: reply.clone(),
    });
    sh.queued.notify();
    let (sh, conn) = (Rc::clone(sh), Rc::clone(conn));
    compio::runtime::spawn(async move {
        let resp = match reply.recv().await {
            Ok(ok) => Response::ProduceOk(ok),
            Err(e) => Response::Error(e),
        };
        conn.respond(&sh, corr, &resp, charge);
    })
    .detach();
}

/// Commits queued produce requests in groups: everything queued while the
/// previous group was committing, up to the group caps and the store's
/// frame limit. Each request is answered after its group's `put_batch`.
async fn commit_loop(sh: Rc<Shared>) {
    loop {
        let g = sh.queued.generation();
        if sh.queue.borrow().is_empty() {
            if sh.closing.get() {
                return;
            }
            sh.queued.wait_change(g).await;
            continue;
        }
        let mut store = sh.store.lock().await;
        let group = take_group(&sh, &store);
        if group.is_empty() {
            continue;
        }
        let headers: Vec<Vec<(&[u8], &[u8])>> = group
            .iter()
            .flat_map(|j| &j.messages)
            .map(|m| {
                m.headers
                    .iter()
                    .map(|h| (&h.name[..], &h.value[..]))
                    .collect()
            })
            .collect();
        let mut batch = Vec::with_capacity(headers.len());
        let mut i = 0;
        for job in &group {
            for m in &job.messages {
                batch.push(straum_store::Message {
                    topic: &job.partition.store_topic,
                    payload: &m.body,
                    key: m.key.as_deref(),
                    headers: &headers[i],
                });
                i += 1;
            }
        }
        let now = sh.timestamp();
        let result = store.put_batch(&batch, now).await;
        drop(store);
        sh.counters
            .groups_committed
            .set(sh.counters.groups_committed.get() + 1);
        let mut at = 0;
        for job in group {
            let count = job.messages.len();
            let reply = match &result {
                Ok(seqs) => {
                    let base = seqs[at];
                    job.partition.next_seq.set(base + count as u64);
                    job.partition.committed.notify();
                    Ok(ProduceOk {
                        base_seq: base,
                        count: count as u32,
                        leader_epoch: job.leader_epoch,
                        timestamp_ms: now,
                    })
                }
                Err(e) => Err(ErrorBody::new(ErrorCode::StorageError, e.to_string())),
            };
            at += count;
            sh.release_pending(job.bytes);
            job.reply.send(reply);
        }
        if result.is_err() {
            sh.storage_failed.set(true);
        }
    }
}

/// Takes queued jobs for one group. A job that alone exceeds the store's
/// frame limit is answered `MessageTooLarge` here, before any I/O.
fn take_group(sh: &Shared, store: &Store) -> Vec<Job> {
    let cfg = &sh.config;
    let empty = store.batch_frame_bytes(&[]);
    let (mut bytes, mut messages, mut frame) = (0u64, 0u64, empty);
    let mut group = Vec::new();
    let mut queue = sh.queue.borrow_mut();
    while let Some(job) = queue.front() {
        let job_frame: usize = job
            .messages
            .iter()
            .map(|m| {
                let headers: Vec<(&[u8], &[u8])> = m
                    .headers
                    .iter()
                    .map(|h| (&h.name[..], &h.value[..]))
                    .collect();
                store.batch_frame_bytes(&[straum_store::Message {
                    topic: &job.partition.store_topic,
                    payload: &m.body,
                    key: m.key.as_deref(),
                    headers: &headers,
                }]) - empty
            })
            .sum();
        let n = job.messages.len() as u64;
        if group.is_empty() && empty + job_frame > store.max_frame_bytes() as usize {
            let Some(job) = queue.pop_front() else { break };
            sh.release_pending(job.bytes);
            job.reply.send(Err(ErrorBody::new(
                ErrorCode::MessageTooLarge,
                "the request's messages exceed what one store commit can hold",
            )));
            continue;
        }
        if !group.is_empty()
            && (bytes + job.bytes > cfg.max_group_bytes
                || messages + n > u64::from(cfg.max_group_messages)
                || frame + job_frame > store.max_frame_bytes() as usize)
        {
            break;
        }
        bytes += job.bytes;
        messages += n;
        frame += job_frame;
        let Some(job) = queue.pop_front() else { break };
        group.push(job);
    }
    group
}

async fn create_topic(sh: &Shared, c: proto::CreateTopic) -> Response {
    if let Err(code) = catalog::check_name(&c.name) {
        return Response::error(code, "topic name");
    }
    if c.partitions == 0 || c.replication_factor == 0 {
        return Response::error(
            ErrorCode::MalformedRequest,
            "partitions and replication_factor are at least 1",
        );
    }
    if c.partitions > 1 || c.replication_factor > 1 {
        return Response::error(
            ErrorCode::Unsupported,
            "version 0 supports one partition and one replica",
        );
    }
    if sh.storage_failed.get() {
        return Response::error(
            ErrorCode::StorageError,
            "a store write failed; restart the broker",
        );
    }
    let mut store = sh.store.lock().await;
    // Checked under the store lock: two CreateTopic of one name serialize.
    if sh.catalog.borrow().topic(&c.name).is_some() {
        return Response::error(ErrorCode::TopicExists, &c.name);
    }
    let record = TopicRecord {
        name: c.name,
        partitions: c.partitions,
        replication_factor: c.replication_factor,
        retention_ms: c.retention_ms,
    };
    let bytes = record.encode();
    let now = sh.timestamp();
    let msg = straum_store::Message {
        topic: catalog::META_TOPIC,
        payload: &bytes,
        ..Default::default()
    };
    if let Err(e) = store.put_batch(&[msg], now).await {
        sh.storage_failed.set(true);
        return Response::error(ErrorCode::StorageError, e.to_string());
    }
    let seqs = |t: &[u8]| store.topic_range(t).unwrap_or((0, 0));
    sh.catalog.borrow_mut().add(record, seqs);
    Response::CreateTopicOk
}

async fn describe_topic(sh: &Shared, name: &str) -> Response {
    if let Err(code) = catalog::check_name(name) {
        return Response::error(code, "topic name");
    }
    let Some(record) = sh.catalog.borrow().topic(name).cloned() else {
        return Response::error(ErrorCode::UnknownTopic, name);
    };
    let store = sh.store.lock().await;
    let catalog = sh.catalog.borrow();
    let partitions = (0..record.partitions)
        .filter_map(|p| catalog.lookup(name, p).ok().map(|e| (p, e)))
        .map(|(p, e)| {
            let (start, next) = (e.partition.start_seq.get(), e.partition.next_seq.get());
            PartitionDescription {
                partition: p,
                start_seq: start,
                next_seq: next,
                message_count: next - start,
                index_bytes: store
                    .topic_stats(&e.partition.store_topic)
                    .map_or(0, |s| s.index_bytes),
            }
        })
        .collect();
    Response::DescribeTopicOk(DescribeTopicOk {
        name: record.name,
        replication_factor: record.replication_factor,
        retention_ms: record.retention_ms,
        partitions,
    })
}

async fn broker_stats(sh: &Shared) -> Response {
    let s = sh.store.lock().await.stats();
    Response::BrokerStatsOk(BrokerStatsOk {
        node_id: sh.identity.node_id,
        open_connections: sh.connections.get(),
        pending_bytes: sh.pending_bytes.get(),
        pending_bytes_peak: sh.pending_peak.get(),
        max_pending_bytes: sh.config.max_pending_bytes,
        fetch_waiters: sh.fetch_waiters.get(),
        produce_requests: sh.counters.produce_requests.get(),
        groups_committed: sh.counters.groups_committed.get(),
        fsyncs: s.fsyncs,
        messages_accepted: s.messages_accepted,
        messages_deduplicated: s.messages_deduplicated,
        body_bytes_received: s.payload_bytes_received,
        body_bytes_new: s.payload_bytes_new,
        wal_bytes: s.wal_bytes,
        segment_bytes: s.segment_bytes,
        index_bytes: s.topic_bytes,
    })
}

async fn fetch(sh: &Shared, f: proto::Fetch) -> Response {
    let (partition, epoch) = match locate(sh, &f.topic, f.partition, f.expected_leader_epoch) {
        Ok(x) => x,
        Err(e) => return Response::Error(e),
    };
    let max_bytes = f.max_bytes.min(sh.config.max_fetch_bytes) as usize;
    let max_messages = sh.config.max_fetch_messages as usize;
    let wait = Duration::from_millis(u64::from(f.max_wait_ms.min(sh.config.max_fetch_wait_ms)));
    let deadline = Instant::now() + wait;
    loop {
        let g = partition.committed.generation();
        let messages =
            match read_messages(sh, &partition, f.from_seq, max_bytes, max_messages).await {
                Ok(m) => m,
                Err(e) => return Response::error(ErrorCode::StorageError, e.to_string()),
            };
        let left = deadline.saturating_duration_since(Instant::now());
        if !messages.is_empty() || left.is_zero() || sh.closing.get() {
            return Response::FetchOk(FetchOk {
                leader_epoch: epoch,
                start_seq: partition.start_seq.get(),
                next_seq: partition.next_seq.get(),
                messages,
            });
        }
        if sh.fetch_waiters.get() >= sh.config.max_fetch_waiters {
            return Response::Error(ErrorBody {
                retry_after_ms: 100,
                ..ErrorBody::new(ErrorCode::Overloaded, "too many waiting fetches")
            });
        }
        sh.fetch_waiters.set(sh.fetch_waiters.get() + 1);
        let _ = compio::time::timeout(left, partition.committed.wait_change(g)).await;
        sh.fetch_waiters.set(sh.fetch_waiters.get() - 1);
    }
}

/// Messages from `max(from, start_seq)` while their encoded size fits
/// `max_bytes`, at most `max_messages` of them; the first one always.
async fn read_messages(
    sh: &Shared,
    partition: &Partition,
    from: u64,
    max_bytes: usize,
    max_messages: usize,
) -> Result<Vec<FetchedMessage>, StoreError> {
    let store = sh.store.lock().await;
    let mut out = Vec::new();
    let mut bytes = 0;
    let start = from.max(partition.start_seq.get());
    for seq in start..partition.next_seq.get() {
        let Some(e) = store.read_entry(&partition.store_topic, seq).await? else {
            break;
        };
        let message = proto::Message {
            key: e.key,
            headers: e
                .headers
                .into_iter()
                .map(|(name, value)| proto::Header { name, value })
                .collect(),
            body: e.payload,
        };
        let n = fetched_bytes(&message);
        if !out.is_empty() && bytes + n > max_bytes {
            break;
        }
        bytes += n;
        out.push(FetchedMessage {
            seq,
            timestamp_ms: e.timestamp,
            message,
        });
        if bytes >= max_bytes || out.len() >= max_messages {
            break;
        }
    }
    Ok(out)
}

/// A `FetchedMessage`'s encoded size (docs/protocol.md 4.6, 4.8): seq and
/// timestamp, flags, the key with its length, the header count and each
/// header with its lengths, the body with its length. Counting only the
/// content let a Fetch of empty bodies return every message up to
/// `next_seq`.
fn fetched_bytes(m: &proto::Message) -> usize {
    let key = m.key.as_ref().map_or(0, |k| 2 + k.len());
    let headers = if m.headers.is_empty() {
        0
    } else {
        2 + m.headers.len() * 6 + m.header_bytes()
    };
    8 + 8 + 1 + key + headers + 4 + m.body.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `fetched_bytes` is exactly what one more message adds to an encoded
    /// FetchOk.
    #[test]
    fn fetched_bytes_matches_the_encoding() {
        let fetch_ok = |messages: Vec<FetchedMessage>| {
            let mut out = Vec::new();
            proto::encode_response(
                &mut out,
                1,
                &Response::FetchOk(FetchOk {
                    leader_epoch: 1,
                    start_seq: 0,
                    next_seq: 1,
                    messages,
                }),
            );
            out.len()
        };
        let empty = fetch_ok(Vec::new());
        let header = |n: &[u8], v: &[u8]| proto::Header {
            name: n.to_vec(),
            value: v.to_vec(),
        };
        for message in [
            proto::Message {
                key: None,
                headers: Vec::new(),
                body: Vec::new(),
            },
            proto::Message {
                key: Some(Vec::new()),
                headers: Vec::new(),
                body: b"body".to_vec(),
            },
            proto::Message {
                key: Some(b"key".to_vec()),
                headers: vec![header(b"h", b"v"), header(b"name", b"")],
                body: b"x".to_vec(),
            },
        ] {
            let n = fetched_bytes(&message);
            let one = fetch_ok(vec![FetchedMessage {
                seq: 7,
                timestamp_ms: 9,
                message,
            }]);
            assert_eq!(one - empty, n);
        }
    }
}
