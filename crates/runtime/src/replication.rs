//! Leader and follower over TCP.
//!
//! An adapter and nothing more. What a position means, when a frame may be
//! taken and what a follower may acknowledge are decided in `replication`,
//! which knows nothing about sockets; this module moves bytes between that
//! state machine and a `transport::Connection` (docs/replication.md §119).
//!
//! The socket decides nothing. In particular it never decides that something
//! is durable: only a completed durability operation does that, and on the
//! leader only a report from the follower.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use replication::{Accepted, Follower, Limits, Link, Live, Message, Reconciled, Redial};
use storage::{Committer, RecordSource, Wal, WalRecord};
use tokio::sync::{mpsc, watch};
use transport::{BufferBudget, Connection};

use crate::config::{ReplicationSettings, WalSettings};
use crate::metrics::Metrics;

/// One committed batch on its way to a follower.
///
/// The bytes are copied once, out of the buffer the WAL wrote them from and
/// into something that can outlive the next commit. One copy per batch, not
/// per record; whether a rotation of owned buffers beats it is the
/// measurement docs/replication.md §112 has been holding open.
pub(crate) struct Shipment {
    previous: u64,
    last: u64,
    batch: Bytes,
}

/// Hands a freshly committed batch to the replication task, if there is one.
///
/// Never blocks and never fails the commit. A follower that cannot keep up
/// loses its place in this queue and is caught up from the log instead — the
/// bytes are on the disk, and §130 keeps them addressable: under `quorum`
/// everything past the follower, otherwise the last `STRAUM_WAL_RETAIN_BYTES`.
/// The
/// alternative, blocking the WAL thread on a socket, would make local
/// durability wait for a remote node, which this phase explicitly does not
/// do.
///
/// A closed queue is counted the same way as a full one. The replication
/// task keeps its receiver for the life of the process — it retries a
/// refused follower rather than returning (D-23) — so the queue closes only
/// at shutdown, after the writer has stopped (docs/replication.md §120).
pub(crate) fn ship(
    to: &mpsc::Sender<Shipment>,
    previous: u64,
    last: u64,
    batch: &[u8],
    metrics: &Metrics,
) {
    let Ok(permit) = to.try_reserve() else {
        // Checked before the copy, so a full queue costs nothing.
        Metrics::increment(&metrics.replication_shipments_dropped, 1);
        metrics.replication_queued.store(
            u64::try_from(to.max_capacity()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        return;
    };
    permit.send(Shipment {
        previous,
        last,
        batch: Bytes::copy_from_slice(batch),
    });
    // One store per batch, never per record.
    metrics.replication_queued.store(
        u64::try_from(to.max_capacity().saturating_sub(to.capacity())).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}

/// Everything the leader task needs that is not its own state.
pub(crate) struct Leading {
    pub(crate) peer: SocketAddr,
    pub(crate) settings: ReplicationSettings,
    pub(crate) wal: WalSettings,
    pub(crate) budget: Arc<BufferBudget>,
    /// The leader's own durable sequence, so a reconnect can be reconciled
    /// against what this node actually holds.
    pub(crate) local: Arc<AtomicU64>,
    /// How far the follower is known to be durable. Read by compaction:
    /// under `quorum` the log stays addressable past it, otherwise it only
    /// tells whether a rewrite left the follower behind (§130, D-18, D-20).
    pub(crate) replicated: Arc<AtomicU64>,
    pub(crate) metrics: Arc<Metrics>,
    /// Where the quorum watermark goes. The coordinator is the only thing
    /// that writes it, and the broker loop the only thing that reads it
    /// (CONSTITUTION.md §30).
    pub(crate) quorum: watch::Sender<u64>,
    /// Durable copies required, this node counted (docs/replication.md §127).
    pub(crate) required_copies: usize,
    /// Whether a PUBACK on this node waits for the quorum this task builds.
    /// When it does, a refused follower means nothing is acknowledged until
    /// it is served again, and that has to be said (D-8, D-23).
    pub(crate) acknowledges_on_quorum: bool,
    /// How long one frame may take to leave. The same limit, and the same
    /// meaning, as a client connection's writer (CLAUDE.md §8): a follower
    /// that holds the connection and stops reading is cut off, counted and
    /// reconnected, rather than holding this task for ever (D-27).
    pub(crate) write_stall_timeout: std::time::Duration,
}

/// Ships committed batches to one follower, reconnecting as needed.
pub(crate) async fn lead(
    held: Leading,
    mut shipments: mpsc::Receiver<Shipment>,
    mut shutdown: watch::Receiver<bool>,
) {
    // When to dial again, and whether a refusal is news, are decided in
    // `replication` (D-28): a refusal is retried, not final (D-23).
    let mut redial = Redial::new();
    loop {
        if *shutdown.borrow() {
            return;
        }

        let attempt = tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            attempt = transport::connect(
                held.peer,
                held.settings.read_buffer_bytes,
                held.settings.write_buffer_bytes,
                Arc::clone(&held.budget),
            ) => attempt,
        };

        match attempt {
            Ok(connection) => {
                redial.connected();
                Metrics::increment(&held.metrics.replication_connections, 1);
                let outcome = serve(&held, connection, &mut shipments, &mut shutdown).await;
                held.metrics
                    .replication_connected
                    .store(0, Ordering::Relaxed);
                match outcome {
                    Err(LinkError::TooFarBehind { from, available }) => {
                        Metrics::increment(&held.metrics.replication_errors, 1);
                        // Under `quorum` the log stays addressable past this
                        // follower. It cannot be served from this log at all,
                        // so keeping records for it means nothing and would
                        // grow the log for as long as it stays like this.
                        // Back to "no position matters".
                        held.replicated.store(u64::MAX, Ordering::Relaxed);
                        if held.acknowledges_on_quorum
                            && held.metrics.quorum_unavailable.swap(1, Ordering::Relaxed) == 0
                        {
                            // Said once, at the transition. The log is not
                            // poisoned and nothing is closed: acknowledgements
                            // are withheld, as `quorum` promises (D-8).
                            tracing::error!(
                                peer = %held.peer,
                                "acknowledgements stopped: a quorum can no longer form until the \
                                 follower is given a fresh copy of the log"
                            );
                        }
                        if redial.refused(from) {
                            tracing::error!(
                                peer = %held.peer, from, available,
                                "the follower is below the retention floor and needs a fresh \
                                 copy of the log; it will be served once it has one"
                            );
                        } else {
                            tracing::debug!(peer = %held.peer, from, available, "follower still below the retention floor");
                        }
                    }
                    Err(error) => {
                        Metrics::increment(&held.metrics.replication_errors, 1);
                        tracing::warn!(peer = %held.peer, %error, "replication link ended");
                        redial.ended();
                    }
                    Ok(()) => redial.ended(),
                }
            }
            Err(error) => {
                tracing::debug!(peer = %held.peer, %error, "cannot reach the follower");
            }
        }

        // Bounded backoff: an accept loop that spins on a refused connection
        // is the same mistake as one that spins on EMFILE (CLAUDE.md §3).
        tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            () = tokio::time::sleep(redial.wait()) => {}
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LinkError {
    #[error("{0}")]
    Transport(#[from] transport::TransportError),
    #[error("{0}")]
    Protocol(#[from] replication::ReplicationError),
    #[error("{0}")]
    Storage(#[from] storage::StorageError),
    #[error("{0}")]
    Refused(#[from] replication::Incompatible),
    #[error("the peer closed the connection")]
    Closed,
    /// A frame could not be written within the write-stall limit (D-27).
    #[error("the follower stopped reading: a frame did not leave within the write-stall limit")]
    Stalled,
    /// The follower asked for a sequence below this log's retention floor.
    /// The bytes may still be in the file; the names are not, so there is
    /// no answer to give. It is not discarded and not reset: an operator has
    /// to decide, because the only answers are a fresh copy of the log or
    /// accepting the loss (docs/replication.md §118, §130).
    #[error("follower needs sequence {from}, this log can be read from {available}")]
    TooFarBehind { from: u64, available: u64 },
}

/// One connection's life: handshake, catch up, then stream.
async fn serve(
    held: &Leading,
    mut connection: Connection,
    shipments: &mut mpsc::Receiver<Shipment>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), LinkError> {
    // The follower speaks first, and what it says about itself is what
    // counts — not anything this leader remembers from a previous connection
    // (docs/replication.md §117).
    //
    // Until it has, nothing is owed to it, so a shutdown does not wait: a
    // peer whose kernel accepted the connection and that never speaks would
    // otherwise hold the shutdown for ever (D-21). After the handshake the
    // stream ends when the queue closes, and not before (§120).
    let reported = tokio::select! {
        biased;
        _ = shutdown.changed() => return Ok(()),
        reported = read_durable(&mut connection, held.settings.limits) => reported?,
    };

    let local = held.local.load(Ordering::Relaxed);
    let (mut link, outcome) = Link::connect(reported, local, held.required_copies)?;
    held.replicated.store(link.durable(), Ordering::Relaxed);
    held.metrics
        .replication_connected
        .store(1, Ordering::Relaxed);
    tracing::info!(peer = %held.peer, reported, local, ?outcome, "replication link up");

    if let Reconciled::Resend { .. } = outcome {
        if let Some(range) = link.behind(local) {
            catch_up(held, &mut connection, &mut link, range).await?;
        }
    }

    stream(held, connection, &mut link, shipments).await
}

/// Steady state: live batches out, durability reports in.
/// Deliberately not watching the shutdown signal. The queue closing is the
/// stop signal, and the runtime closes it only after the WAL writer has
/// finished — so a shutdown ships what was committed rather than dropping it
/// on the floor (docs/replication.md §120).
async fn stream(
    held: &Leading,
    mut connection: Connection,
    link: &mut Link,
    shipments: &mut mpsc::Receiver<Shipment>,
) -> Result<(), LinkError> {
    let mut header = Vec::with_capacity(replication::RECORDS_HEADER_BYTES);
    // Caught up: a quorum can form again, whatever refused this follower
    // before (D-23).
    if held.metrics.quorum_unavailable.swap(0, Ordering::Relaxed) == 1 {
        tracing::info!(peer = %held.peer, "the follower is served again; acknowledgements can resume");
    }
    announce(held, link.begin(held.local.load(Ordering::Relaxed)));
    loop {
        // Behind is a state, not an event. Waiting for the next commit to
        // reveal a gap leaves a follower behind for as long as the leader is
        // idle, and after the live queue has overflowed everything still in
        // it is stale — skipped, revealing nothing (docs/replication.md §123).
        if let Some(range) = link.behind(held.local.load(Ordering::Relaxed)) {
            catch_up(held, &mut connection, link, range).await?;
        }

        tokio::select! {
            biased;
            filled = connection.reader.fill() => {
                if filled? == 0 {
                    return Err(LinkError::Closed);
                }
                drain_reports(held, &mut connection, link)?;
            }
            shipment = shipments.recv() => {
                let Some(shipment) = shipment else {
                    return Ok(());
                };
                // A leftover from before a catch-up, or past a gap the queue
                // left: the top of the loop reads the gap back out of the log.
                if link.live(shipment.previous, shipment.last) == Live::Skip {
                    continue;
                }
                send_frame(held, &mut connection, &mut header, link, shipment.previous, shipment.last, &shipment.batch).await?;
            }
        }
    }
}

/// Writes one frame and records that it went out. Sent is not durable.
async fn send_frame(
    held: &Leading,
    connection: &mut Connection,
    header: &mut Vec<u8>,
    link: &mut Link,
    previous: u64,
    last: u64,
    batch: &[u8],
) -> Result<(), LinkError> {
    header.clear();
    replication::encode_records_header(previous, last, batch, header);
    // The whole frame under one limit, as a client writer's flush is: a
    // frame that cannot leave in that long is a follower that stopped
    // reading, and under `quorum` one that silently stops every
    // acknowledgement (D-27).
    let write = async {
        connection.writer.write_all(header).await?;
        connection.writer.write_all(batch).await
    };
    match tokio::time::timeout(held.write_stall_timeout, write).await {
        Ok(written) => written?,
        Err(_) => return Err(LinkError::Stalled),
    }
    link.on_sent(last);
    held.metrics
        .replication_in_flight
        .store(link.in_flight(), Ordering::Relaxed);
    Ok(())
}

/// Takes every complete report sitting in the read buffer.
/// Publishes a watermark that moved. Monotonic by construction: the
/// coordinator only hands back an advance.
fn announce(held: &Leading, reached: Option<u64>) {
    if let Some(reached) = reached {
        let _ = held.quorum.send(reached);
    }
}

fn drain_reports(
    held: &Leading,
    connection: &mut Connection,
    link: &mut Link,
) -> Result<(), LinkError> {
    loop {
        let consumed = {
            let buffered = connection.reader.buf_mut();
            match replication::decode(buffered, held.settings.limits)? {
                None => return Ok(()),
                Some((Message::Records(_), _)) => return Err(LinkError::Closed),
                Some((Message::Durable(sequence), consumed)) => {
                    // The only thing that advances remote durability. A write
                    // that succeeded and a frame that was delivered are not
                    // this.
                    let local = held.local.load(Ordering::Relaxed);
                    if let Some(advance) = link
                        .on_report(sequence, local)
                        .map_err(|_| LinkError::Closed)?
                    {
                        held.replicated.store(advance.durable, Ordering::Relaxed);
                        held.metrics
                            .replication_follower_sequence
                            .store(advance.durable, Ordering::Relaxed);
                        announce(held, advance.watermark);
                    }
                    consumed
                }
            }
        };
        let _ = connection.reader.buf_mut().split_to(consumed);
    }
}

/// Reads until one durability report is complete. Used for the handshake,
/// where nothing else is expected.
async fn read_durable(connection: &mut Connection, limits: Limits) -> Result<u64, LinkError> {
    loop {
        let decoded = match replication::decode(connection.reader.buf_mut(), limits)? {
            Some((Message::Durable(sequence), consumed)) => Some((sequence, consumed)),
            Some((Message::Records(_), _)) => return Err(LinkError::Closed),
            None => None,
        };
        if let Some((sequence, consumed)) = decoded {
            let _ = connection.reader.buf_mut().split_to(consumed);
            return Ok(sequence);
        }
        // One read is not one frame: the handshake may arrive in pieces like
        // anything else on a byte stream.
        if connection.reader.fill().await? == 0 {
            return Err(LinkError::Closed);
        }
    }
}

/// Sends the records the follower is missing, read back out of the log.
///
/// The log is the source of truth and the live queue is only a shortcut, so
/// this is also what recovers from a shipment that was dropped because the
/// follower was slow. Records are read on a blocking thread and handed over a
/// bounded channel: the log is not held in memory and a slow socket stops the
/// reader rather than growing a queue.
async fn catch_up(
    held: &Leading,
    connection: &mut Connection,
    link: &mut Link,
    // From `Link::behind`: never past this node's durable sequence (D-22).
    (from, until): (u64, u64),
) -> Result<(), LinkError> {
    // Counted here rather than at the call sites: reading the log back is
    // what the counter is about, and a handshake that resends is the same
    // event as a gap noticed mid-stream.
    Metrics::increment(&held.metrics.replication_catchups, 1);

    let (frames, mut ready) = mpsc::channel::<Shipment>(held.settings.catch_up_queue);
    let path = held.wal.path.clone();
    let limits = held.wal.limits;
    let bound = held.settings.catch_up_batch_bytes;

    let reader = tokio::task::spawn_blocking(move || -> Result<(), LinkError> {
        let mut batch: Vec<u8> = Vec::with_capacity(bound);
        let mut start = from.saturating_sub(1);
        let mut last = from.saturating_sub(1);
        let send = |start: u64, last: u64, batch: &mut Vec<u8>| {
            let shipment = Shipment {
                previous: start,
                last,
                batch: Bytes::copy_from_slice(batch),
            };
            batch.clear();
            frames.blocking_send(shipment).is_ok()
        };

        let mut stopped = false;
        let mut refused = None;
        // The floor and the records come out of one read of one file. A
        // separate call to check the range first would be checking a log that
        // the next call is no longer reading (docs/replication.md §130).
        storage::replay_addressable(
            &path,
            limits,
            &mut |floor| {
                if !Link::serves(from, floor) {
                    // Compacted away. Under `quorum` the log keeps what a
                    // follower it has heard from needs, so arriving here means
                    // one it never heard from or one that lost its own log;
                    // otherwise the follower fell further behind than the
                    // retained bytes (§130). Either way an operator question,
                    // not one this code may answer by resetting anybody.
                    refused = Some(floor);
                    return false;
                }
                true
            },
            &mut |sequence, framed| {
                if stopped || sequence < from || sequence > until {
                    return;
                }
                if batch.is_empty() {
                    start = sequence.saturating_sub(1);
                }
                last = sequence;
                batch.extend_from_slice(framed);
                if batch.len() >= bound && !send(start, last, &mut batch) {
                    stopped = true;
                }
            },
        )?;
        if let Some(available) = refused {
            return Err(LinkError::TooFarBehind { from, available });
        }
        if !stopped && !batch.is_empty() {
            send(start, last, &mut batch);
        }
        Ok(())
    });

    let mut header = Vec::with_capacity(replication::RECORDS_HEADER_BYTES);
    let mut outcome = Ok(());
    while let Some(shipment) = ready.recv().await {
        if outcome.is_err() {
            continue;
        }
        outcome = send_frame(
            held,
            connection,
            &mut header,
            link,
            shipment.previous,
            shipment.last,
            &shipment.batch,
        )
        .await;
    }
    // The reader stops on its own once the channel closes, so joining it
    // cannot hang on a socket that died.
    match reader.await {
        Ok(Ok(())) => outcome,
        Ok(Err(error)) => outcome.and(Err(error)),
        Err(_) => outcome.and(Err(LinkError::Closed)),
    }
}

/// Everything the follower task needs.
pub(crate) struct Following {
    pub(crate) settings: ReplicationSettings,
    pub(crate) wal: WalSettings,
    pub(crate) metrics: Arc<Metrics>,
}

/// Accepts one leader at a time and applies what it sends.
pub(crate) async fn follow(
    held: Following,
    listener: transport::Listener,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        // Checked every time round: `changed()` fires once, and a loop that
        // waited for a second change after shutdown would wait forever.
        if *shutdown.borrow() {
            return;
        }
        let accepted = tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            accepted = listener.accept() => accepted,
        };
        let Ok(accepted) = accepted else {
            continue;
        };
        let Ok(connection) = accepted.establish().await else {
            continue;
        };
        Metrics::increment(&held.metrics.replication_connections, 1);

        if let Err(error) = apply(&held, connection, &mut shutdown).await {
            Metrics::increment(&held.metrics.replication_errors, 1);
            tracing::warn!(%error, "replication link from the leader ended");
        }
    }
}

/// What the follower's own log has made durable, and the bytes on the way to
/// it. One thread, because `sync_data` blocks.
struct Durability {
    /// An `Option` so it can be dropped before the thread is joined: the
    /// thread ends when its channel closes, and joining while still holding
    /// the sender is waiting for something this side is preventing.
    records: Option<mpsc::Sender<WalRecord>>,
    reports: mpsc::Receiver<u64>,
    writer: Option<std::thread::JoinHandle<()>>,
}

impl Durability {
    fn start(
        wal: &WalSettings,
        queue: usize,
        metrics: Arc<Metrics>,
    ) -> Result<(Self, u64), storage::StorageError> {
        let (log, recovery) = Wal::open(&wal.path, wal.limits)?;
        let (records, queued) = mpsc::channel(queue);
        let (reports, ready) = mpsc::channel(queue);
        // Correlations belong to the leader's log and are inside the record
        // bodies; this node measures itself in sequences.
        let mut committer = Committer::new(log, wal.limits, true, 0);

        let writer = std::thread::Builder::new()
            .name("straum-follower-wal".to_owned())
            .spawn(move || {
                let mut source = Queued(queued);
                loop {
                    match committer.commit_available(&mut source) {
                        Ok(None) => break,
                        // Sent only now: written was not durable, and the
                        // durability operation is what just completed.
                        Ok(Some(commit)) => {
                            // The same counters the leader's own log keeps. A
                            // follower whose commits are invisible is a
                            // follower nobody can tell is keeping up.
                            Metrics::increment(&metrics.wal_commits, 1);
                            Metrics::increment(
                                &metrics.wal_records_committed,
                                commit.records as u64,
                            );
                            Metrics::increment(&metrics.wal_bytes_committed, commit.bytes as u64);
                            metrics
                                .wal_durable_sequence
                                .store(commit.durable_sequence, Ordering::Relaxed);
                            metrics.wal_sync_nanos.store(
                                u64::try_from(commit.sync.as_nanos()).unwrap_or(u64::MAX),
                                Ordering::Relaxed,
                            );
                            if reports.blocking_send(commit.durable_sequence).is_err() {
                                break;
                            }
                        }
                        Err(error) => {
                            Metrics::increment(&metrics.wal_commit_failures, 1);
                            tracing::error!(%error, "the follower's log failed");
                            break;
                        }
                    }
                }
            })
            .map_err(storage::StorageError::Io)?;

        Ok((
            Self {
                records: Some(records),
                reports: ready,
                writer: Some(writer),
            },
            recovery.sequence,
        ))
    }
}

impl Drop for Durability {
    fn drop(&mut self) {
        // The sender first. It is what the thread is waiting on, and what
        // tells it to commit the rest and stop.
        self.records = None;
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

struct Queued(mpsc::Receiver<WalRecord>);

impl RecordSource for Queued {
    fn recv(&mut self) -> Option<WalRecord> {
        self.0.blocking_recv()
    }

    fn try_recv(&mut self) -> Option<WalRecord> {
        self.0.try_recv().ok()
    }
}

/// One leader connection: report where this node is, then take what it sends.
async fn apply(
    held: &Following,
    mut connection: Connection,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), LinkError> {
    let (mut durability, recovered) =
        Durability::start(&held.wal, held.settings.queue, Arc::clone(&held.metrics))?;
    let mut follower = Follower::resuming_from(recovered);

    // The follower speaks first, and says what it actually holds. A batch it
    // synced before the last connection died is durable, and saying so is
    // what stops the leader sending it again (docs/replication.md §117).
    let mut out = Vec::with_capacity(replication::DURABLE_BYTES);
    replication::encode_durable(follower.durable(), &mut out);
    connection.writer.write_all(&out).await?;
    held.metrics
        .replication_connected
        .store(1, Ordering::Relaxed);

    let outcome = loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break Ok(()),
            reported = durability.reports.recv() => {
                let Some(sequence) = reported else {
                    break Err(LinkError::Closed);
                };
                // sync_data finished. Only now.
                if let Some(durable) = follower.on_durable(sequence) {
                    out.clear();
                    replication::encode_durable(durable, &mut out);
                    connection.writer.write_all(&out).await?;
                    held.metrics
                        .replication_follower_sequence
                        .store(durable, Ordering::Relaxed);
                }
            }
            filled = connection.reader.fill() => {
                if filled? == 0 {
                    break Err(LinkError::Closed);
                }
                if let Err(error) = take(held, &mut connection, &mut follower, &mut durability).await {
                    break Err(error);
                }
            }
        }
    };

    held.metrics
        .replication_connected
        .store(0, Ordering::Relaxed);
    outcome
}

/// Takes every complete frame sitting in the read buffer.
///
/// One read is not one frame: it can hold a piece of one, exactly one,
/// several, or the tail of one and the head of the next. The decoder asks
/// for more until a frame is whole, so a read boundary is never a protocol
/// boundary.
async fn take(
    held: &Following,
    connection: &mut Connection,
    follower: &mut Follower,
    durability: &mut Durability,
) -> Result<(), LinkError> {
    loop {
        let taken = {
            let buffered = connection.reader.buf_mut();
            match replication::decode(buffered, held.settings.limits)? {
                None => return Ok(()),
                Some((Message::Durable(_), _)) => return Err(LinkError::Closed),
                Some((Message::Records(frame), consumed)) => {
                    // The state machine decides; this function only moves
                    // bytes. A gap or a straddle ends the connection rather
                    // than being papered over.
                    match follower.accept(frame) {
                        Ok(Accepted::Append { through }) => {
                            (Some(Bytes::copy_from_slice(frame.batch)), through, consumed)
                        }
                        Ok(Accepted::Acknowledge { durable }) => (None, durable, consumed),
                        Err(refused) => {
                            tracing::warn!(%refused, "refused a replication frame");
                            return Err(LinkError::Closed);
                        }
                    }
                }
            }
        };
        let _ = connection.reader.buf_mut().split_to(taken.2);

        match taken.0 {
            // Into the log as it arrived. Nothing is acknowledged here.
            Some(batch) => {
                let Some(records) = &durability.records else {
                    return Err(LinkError::Closed);
                };
                if records.send(WalRecord::replicated(batch)).await.is_err() {
                    return Err(LinkError::Closed);
                }
            }
            // Already held. Say where this node is; write nothing.
            None => {
                let mut out = Vec::with_capacity(replication::DURABLE_BYTES);
                replication::encode_durable(taken.1, &mut out);
                connection.writer.write_all(&out).await?;
            }
        }
    }
}
