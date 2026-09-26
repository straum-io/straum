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
use replication::{Accepted, Follower, Limits, Message, Peer, Reconciled};
use storage::{Committer, RecordSource, Wal, WalRecord};
use tokio::sync::{mpsc, watch};
use transport::{Connection, ReadBudget};

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
/// bytes are not lost, they are on the disk, and §118 keeps them there. The
/// alternative, blocking the WAL thread on a socket, would make local
/// durability wait for a remote node, which this phase explicitly does not
/// do.
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
    pub(crate) budget: Arc<ReadBudget>,
    /// The leader's own durable sequence, so a reconnect can be reconciled
    /// against what this node actually holds.
    pub(crate) local: Arc<AtomicU64>,
    /// How far the follower is known to be durable. Read by compaction,
    /// which will not rewrite the log past it (§118).
    pub(crate) replicated: Arc<AtomicU64>,
    pub(crate) metrics: Arc<Metrics>,
}

/// Ships committed batches to one follower, reconnecting as needed.
pub(crate) async fn lead(
    held: Leading,
    mut shipments: mpsc::Receiver<Shipment>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = std::time::Duration::from_millis(50);
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
                Arc::clone(&held.budget),
            ) => attempt,
        };

        match attempt {
            Ok(connection) => {
                backoff = std::time::Duration::from_millis(50);
                Metrics::increment(&held.metrics.replication_connections, 1);
                if let Err(error) = serve(&held, connection, &mut shipments).await {
                    Metrics::increment(&held.metrics.replication_errors, 1);
                    tracing::warn!(peer = %held.peer, %error, "replication link ended");
                }
                held.metrics
                    .replication_connected
                    .store(0, Ordering::Relaxed);
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
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(std::time::Duration::from_secs(5));
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
    /// The follower needs records this log no longer holds. It is not
    /// discarded and not reset: an operator has to decide, because the only
    /// answers are a fresh copy of the log or accepting the loss
    /// (docs/replication.md §118).
    #[error("follower needs sequence {from}, this log starts at {available}")]
    TooFarBehind { from: u64, available: u64 },
}

/// One connection's life: handshake, catch up, then stream.
async fn serve(
    held: &Leading,
    mut connection: Connection,
    shipments: &mut mpsc::Receiver<Shipment>,
) -> Result<(), LinkError> {
    // The follower speaks first, and what it says about itself is what
    // counts — not anything this leader remembers from a previous connection
    // (docs/replication.md §117).
    let reported = read_durable(&mut connection, held.settings.limits).await?;

    let local = held.local.load(Ordering::Relaxed);
    let (mut peer, outcome) = Peer::connected(reported, local)?;
    held.replicated.store(peer.durable(), Ordering::Relaxed);
    held.metrics
        .replication_connected
        .store(1, Ordering::Relaxed);
    tracing::info!(peer = %held.peer, reported, local, ?outcome, "replication link up");

    if let Reconciled::Resend { from } = outcome {
        catch_up(held, &mut connection, &mut peer, from).await?;
    }

    stream(held, connection, &mut peer, shipments).await
}

/// Steady state: live batches out, durability reports in.
/// Deliberately not watching the shutdown signal. The queue closing is the
/// stop signal, and the runtime closes it only after the WAL writer has
/// finished — so a shutdown ships what was committed rather than dropping it
/// on the floor (docs/replication.md §120).
async fn stream(
    held: &Leading,
    mut connection: Connection,
    peer: &mut Peer,
    shipments: &mut mpsc::Receiver<Shipment>,
) -> Result<(), LinkError> {
    let mut header = Vec::with_capacity(replication::RECORDS_HEADER_BYTES);
    loop {
        // Behind is a state, not an event. Waiting for the next commit to
        // reveal a gap leaves a follower behind for as long as the leader is
        // idle, and after the live queue has overflowed everything still in
        // it is stale — skipped, revealing nothing (docs/replication.md §123).
        let local = held.local.load(Ordering::Relaxed);
        if peer.sent() < local {
            Metrics::increment(&held.metrics.replication_catchups, 1);
            catch_up(held, &mut connection, peer, peer.sent().saturating_add(1)).await?;
        }

        tokio::select! {
            biased;
            filled = connection.reader.fill() => {
                if filled? == 0 {
                    return Err(LinkError::Closed);
                }
                drain_reports(held, &mut connection, peer)?;
            }
            shipment = shipments.recv() => {
                let Some(shipment) = shipment else {
                    return Ok(());
                };
                if shipment.last <= peer.sent() {
                    // Already sent: a batch left over from before a catch-up.
                    continue;
                }
                if shipment.previous != peer.sent() {
                    // A gap the queue left behind. The top of the loop reads
                    // it back out of the log, so this frame is simply not
                    // the one to send now.
                    continue;
                }
                send_frame(&mut connection, &mut header, peer, shipment.previous, shipment.last, &shipment.batch, &held.metrics).await?;
            }
        }
    }
}

/// Writes one frame and records that it went out. Sent is not durable.
async fn send_frame(
    connection: &mut Connection,
    header: &mut Vec<u8>,
    peer: &mut Peer,
    previous: u64,
    last: u64,
    batch: &[u8],
    metrics: &Metrics,
) -> Result<(), LinkError> {
    header.clear();
    replication::encode_records_header(previous, last, batch, header);
    connection.writer.write_all(header).await?;
    connection.writer.write_all(batch).await?;
    peer.on_sent(last);
    metrics
        .replication_in_flight
        .store(peer.in_flight(), Ordering::Relaxed);
    Ok(())
}

/// Takes every complete report sitting in the read buffer.
fn drain_reports(
    held: &Leading,
    connection: &mut Connection,
    peer: &mut Peer,
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
                    if peer.on_durable(sequence).map_err(|_| LinkError::Closed)? {
                        held.replicated.store(sequence, Ordering::Relaxed);
                        held.metrics
                            .replication_follower_sequence
                            .store(sequence, Ordering::Relaxed);
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
    peer: &mut Peer,
    from: u64,
) -> Result<(), LinkError> {
    let allocated = storage::allocated(&held.wal.path)?;
    let base = allocated.base();
    if from <= base {
        // The records were compacted away. §118 keeps them while a replica
        // is known to be behind, so arriving here means this follower was
        // away long enough for the leader to forget it existed — an operator
        // question, not one this code may answer by resetting anybody.
        return Err(LinkError::TooFarBehind {
            from,
            available: base.saturating_add(1),
        });
    }

    let (frames, mut ready) = mpsc::channel::<Shipment>(held.settings.catch_up_queue);
    let path = held.wal.path.clone();
    let limits = held.wal.limits;
    let bound = held.settings.catch_up_batch_bytes;

    let reader = tokio::task::spawn_blocking(move || -> Result<(), storage::StorageError> {
        let mut sequence = base;
        let mut batch: Vec<u8> = Vec::with_capacity(bound);
        let mut start = base;
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
        storage::replay_frames(&path, limits, &mut |framed| {
            sequence = sequence.saturating_add(1);
            if stopped || sequence < from {
                return;
            }
            if batch.is_empty() {
                start = sequence.saturating_sub(1);
            }
            batch.extend_from_slice(framed);
            if batch.len() >= bound && !send(start, sequence, &mut batch) {
                stopped = true;
            }
        })?;
        if !stopped && !batch.is_empty() {
            send(start, sequence, &mut batch);
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
            connection,
            &mut header,
            peer,
            shipment.previous,
            shipment.last,
            &shipment.batch,
            &held.metrics,
        )
        .await;
    }
    // The reader stops on its own once the channel closes, so joining it
    // cannot hang on a socket that died.
    match reader.await {
        Ok(Ok(())) => outcome,
        Ok(Err(error)) => outcome.and(Err(error.into())),
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
