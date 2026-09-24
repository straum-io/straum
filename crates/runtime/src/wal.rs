//! The durable write path, wired to the broker loop.
//!
//! The WAL runs on its own thread because `sync_data` blocks for
//! milliseconds: on a worker thread it would stall everything else the
//! executor is holding. Nothing about it is visible to the broker except the
//! token it echoes back (docs/storage.md §79, §83).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use std::collections::HashMap;

use broker::BrokerLimits;
use bytes::{BufMut, Bytes, BytesMut};
use domain::{DeliveryGuarantee, Topic};
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
    pub(crate) async fn submit(
        &self,
        topic: &Topic,
        payload: &Bytes,
        guarantee: DeliveryGuarantee,
        retain: bool,
    ) -> Option<DurableToken> {
        let correlation = self.next.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        let record = WalRecord {
            correlation,
            payload: encode(topic, payload, guarantee, retain),
        };
        self.records.send(record).await.ok()?;
        Some(DurableToken(correlation))
    }
}

/// One record: what it is, what it was published as, then the message.
///
/// ```text
/// [ kind: u8 ][ flags: u8 ][ topic length: u16 BE ][ topic ][ payload ]
/// ```
///
/// The kind is there so the log can carry more than publishes later without
/// another format change; a change to what these bytes mean is a change to
/// the file magic, so an older log is refused rather than misread.
const KIND_PUBLISH: u8 = 1;
const FLAG_RETAIN: u8 = 0b0000_0001;
const FLAG_AT_LEAST_ONCE: u8 = 0b0000_0010;

fn encode(topic: &Topic, payload: &Bytes, guarantee: DeliveryGuarantee, retain: bool) -> Bytes {
    let topic = topic.as_bytes();
    let mut flags = 0u8;
    if retain {
        flags |= FLAG_RETAIN;
    }
    if guarantee == DeliveryGuarantee::AtLeastOnce {
        flags |= FLAG_AT_LEAST_ONCE;
    }

    let mut record = BytesMut::with_capacity(4 + topic.len() + payload.len());
    record.put_u8(KIND_PUBLISH);
    record.put_u8(flags);
    record.put_u16(u16::try_from(topic.len()).unwrap_or(u16::MAX));
    record.put_slice(topic);
    record.put_slice(payload);
    record.freeze()
}

/// What one record said, for replay.
pub(crate) struct Recorded {
    pub(crate) topic: Topic,
    pub(crate) payload: Bytes,
    pub(crate) guarantee: DeliveryGuarantee,
    pub(crate) retain: bool,
}

/// `None` for anything this version does not understand: a record it cannot
/// read is not a record it may guess at.
fn decode(record: &[u8]) -> Option<Recorded> {
    let kind = *record.first()?;
    if kind != KIND_PUBLISH {
        return None;
    }
    let flags = *record.get(1)?;
    let length = usize::from(u16::from_be_bytes([*record.get(2)?, *record.get(3)?]));
    let end = 4usize.checked_add(length)?;
    let topic = Topic::new(Bytes::copy_from_slice(record.get(4..end)?)).ok()?;
    let payload = Bytes::copy_from_slice(record.get(end..)?);

    Some(Recorded {
        topic,
        payload,
        guarantee: if flags & FLAG_AT_LEAST_ONCE != 0 {
            DeliveryGuarantee::AtLeastOnce
        } else {
            DeliveryGuarantee::AtMostOnce
        },
        retain: flags & FLAG_RETAIN != 0,
    })
}

/// Rebuilds what survived a restart: the last retained message per topic.
///
/// Bounded by the same limits the live store has, so a long log cannot use
/// more memory on the way back in than it was allowed to on the way out.
pub(crate) fn restore(
    settings: &WalSettings,
    limits: BrokerLimits,
) -> Result<Vec<Recorded>, StorageError> {
    let mut retained: HashMap<Topic, Recorded> = HashMap::new();
    let mut bytes = 0usize;
    let mut refused = 0u64;

    let recovery = storage::replay(&settings.path, settings.limits, &mut |record| {
        let Some(recorded) = decode(record) else {
            return;
        };
        if !recorded.retain {
            return;
        }
        // The log is replayed in commit order, so the last word wins — an
        // empty payload is how a retained message was deleted.
        if recorded.payload.is_empty() {
            if let Some(gone) = retained.remove(&recorded.topic) {
                bytes = bytes.saturating_sub(gone.payload.len());
            }
            return;
        }
        let previous = retained
            .get(&recorded.topic)
            .map(|held| held.payload.len())
            .unwrap_or(0);
        let would_hold = bytes
            .saturating_sub(previous)
            .saturating_add(recorded.payload.len());
        if would_hold > limits.max_retained_bytes
            || (previous == 0 && retained.len() >= limits.max_retained_messages)
        {
            refused = refused.saturating_add(1);
            return;
        }
        bytes = would_hold;
        retained.insert(recorded.topic.clone(), recorded);
    })?;

    tracing::info!(
        path = %settings.path.display(),
        records = recovery.records,
        retained = retained.len(),
        refused,
        discarded_bytes = recovery.discarded_bytes,
        "replayed the write-ahead log"
    );

    Ok(retained.into_values().collect())
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
