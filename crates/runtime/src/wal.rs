//! The durable write path, wired to the broker loop.
//!
//! The WAL runs on its own thread because `sync_data` blocks for
//! milliseconds: on a worker thread it would stall everything else the
//! executor is holding. Nothing about it is visible to the broker except the
//! token it echoes back (docs/storage.md §79, §83).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{BufMut, Bytes, BytesMut};
use domain::Topic;
use protocol::DurableToken;
use storage::{Committer, RecordSource, StorageError, Wal, WalRecord};
use tokio::sync::mpsc;

use crate::broker_loop::Event;
use crate::config::WalSettings;
use crate::metrics::Metrics;

/// Submits records and hands out the tokens the broker waits on.
#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    records: mpsc::Sender<WalRecord>,
    next: Arc<AtomicU64>,
}

impl Submitter {
    /// Queues one message for the log.
    ///
    /// Awaits when the queue is full: backpressure reaches the publisher
    /// instead of a publish being acknowledged early to make room (§8).
    pub(crate) async fn submit(&self, topic: &Topic, payload: &Bytes) -> Option<DurableToken> {
        let correlation = self.next.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        let record = WalRecord {
            correlation,
            payload: encode(topic, payload),
        };
        self.records.send(record).await.ok()?;
        Some(DurableToken(correlation))
    }
}

/// One record: the topic, then the payload.
///
/// ```text
/// [ topic length: u16 BE ][ topic ][ payload ]
/// ```
fn encode(topic: &Topic, payload: &Bytes) -> Bytes {
    let topic = topic.as_bytes();
    let mut record = BytesMut::with_capacity(2 + topic.len() + payload.len());
    record.put_u16(u16::try_from(topic.len()).unwrap_or(u16::MAX));
    record.put_slice(topic);
    record.put_slice(payload);
    record.freeze()
}

/// Opens the log, recovers its durable prefix and starts the writer thread.
pub(crate) fn start(
    settings: &WalSettings,
    events: mpsc::Sender<Event>,
    metrics: Arc<Metrics>,
) -> Result<(Submitter, std::thread::JoinHandle<()>), StorageError> {
    let (wal, recovery) = Wal::open(&settings.path, settings.limits)?;
    tracing::info!(
        path = %settings.path.display(),
        records = recovery.records,
        durable_offset = recovery.durable_offset,
        discarded_bytes = recovery.discarded_bytes,
        "recovered the write-ahead log"
    );
    if recovery.records > 0 {
        // Saying so is cheaper than letting somebody assume otherwise.
        tracing::warn!(
            records = recovery.records,
            "recovered records are not replayed: there are no persistent sessions yet"
        );
    }

    let (records, queue) = mpsc::channel(settings.queue);
    let committer = Committer::new(wal, settings.limits);

    let writer = std::thread::Builder::new()
        .name("straum-wal".to_owned())
        .spawn(move || write(committer, Queue(queue), events, metrics))
        .map_err(StorageError::Io)?;

    Ok((
        Submitter {
            records,
            next: Arc::new(AtomicU64::new(0)),
        },
        writer,
    ))
}

fn write(
    mut committer: Committer,
    mut queue: Queue,
    events: mpsc::Sender<Event>,
    metrics: Arc<Metrics>,
) {
    let outcome = committer.run(&mut queue, |commit| {
        Metrics::increment(&metrics.wal_commits, 1);
        Metrics::increment(&metrics.wal_records_committed, commit.records as u64);
        Metrics::increment(&metrics.wal_bytes_committed, commit.bytes as u64);
        metrics
            .wal_sync_nanos
            .store(commit.sync.as_nanos() as u64, Ordering::Relaxed);
        metrics
            .wal_durable_sequence
            .store(commit.durable_sequence, Ordering::Relaxed);

        // The broker may acknowledge everything up to here, and not one
        // record more (docs/storage.md §99, invariant 1).
        let _ = events.blocking_send(Event::Durable(DurableToken(commit.last_correlation)));
    });

    if let Err(error) = outcome {
        Metrics::increment(&metrics.wal_commit_failures, 1);
        // Nothing is reported as durable after this, so nothing is
        // acknowledged as durable either (invariant 7).
        tracing::error!(%error, "the write-ahead log failed; no further publish will be acknowledged");
    }
    tracing::debug!("wal writer stopped");
}

/// The runtime's channel, seen the way the writer thread needs it.
struct Queue(mpsc::Receiver<WalRecord>);

impl RecordSource for Queue {
    fn recv(&mut self) -> Option<WalRecord> {
        self.0.blocking_recv()
    }

    fn try_recv(&mut self) -> Option<WalRecord> {
        self.0.try_recv().ok()
    }
}
