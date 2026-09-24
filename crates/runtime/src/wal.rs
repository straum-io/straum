//! The durable write path, wired to the broker loop.
//!
//! The WAL runs on its own thread because `sync_data` blocks for
//! milliseconds: on a worker thread it would stall everything else the
//! executor is holding. Nothing about it is visible to the broker except the
//! token it echoes back (docs/storage.md §79, §83).
//!
//! Record bodies read back from a file are as untrusted as bytes from a
//! socket — a crash leaves whatever it leaves — so this module refuses
//! indexing and unchecked arithmetic the same way the packet decoder does.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use std::collections::HashMap;

use broker::{Broker, BrokerLimits, SessionEvent, SessionRecord};
use bytes::{BufMut, Bytes, BytesMut};
use domain::{ClientId, DeliveryGuarantee, Topic, TopicFilter};
use protocol::{DurableToken, Outbox, Publish};
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
            payload: encode(correlation, topic, payload, guarantee, retain),
        };
        self.records.send(record).await.ok()?;
        Some(DurableToken(correlation))
    }

    /// Queues what is kept for one session.
    ///
    /// Never awaits: the broker loop calls this, and the WAL thread reports
    /// back through the same loop's channel. A loop that waited on a full
    /// log queue would be waiting on itself.
    ///
    /// `false` means the queue was full and the caller should try again.
    pub(crate) fn try_submit_session(&self, event: &SessionEvent) -> bool {
        let correlation = self.next.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        let record = WalRecord {
            correlation,
            payload: encode_session(correlation, event),
        };
        self.records.try_send(record).is_ok()
    }
}

/// One record. Two kinds so far; the kind byte is what lets a third be added
/// without a format change. A change to what these bytes mean is a change to
/// the file magic, so an older log is refused rather than misread.
///
/// ```text
/// publish: [ 1 ][ flags: u8 ][ correlation: u64 BE ][ topic len: u16 BE ][ topic ][ payload ]
/// session: [ 2 ][ flags: u8 ][ correlation: u64 BE ][ client len: u16 BE ][ client ]
///          [ owes from: u64 BE ][ (filter len: u16 BE)(filter)(guarantee: u8) ... ]
/// ```
///
/// The correlation is written down rather than derived from the record's
/// position, because publishes are submitted from many connection tasks at
/// once: numbering them in step with the queue would need a lock on the
/// publish path, and eight bytes a record is the cheaper of the two.
/// `FLAG_GONE` on a session record means the identity keeps nothing.
const KIND_PUBLISH: u8 = 1;
const KIND_SESSION: u8 = 2;
const FLAG_RETAIN: u8 = 0b0000_0001;
const FLAG_AT_LEAST_ONCE: u8 = 0b0000_0010;
const FLAG_GONE: u8 = 0b0000_0100;
const PREFIX_BYTES: usize = 12;

fn put_prefix(record: &mut BytesMut, kind: u8, flags: u8, correlation: u64, length: usize) {
    record.put_u8(kind);
    record.put_u8(flags);
    record.put_u64(correlation);
    record.put_u16(u16::try_from(length).unwrap_or(u16::MAX));
}

fn encode(
    correlation: u64,
    topic: &Topic,
    payload: &Bytes,
    guarantee: DeliveryGuarantee,
    retain: bool,
) -> Bytes {
    let topic = topic.as_bytes();
    let mut flags = 0u8;
    if retain {
        flags |= FLAG_RETAIN;
    }
    if guarantee == DeliveryGuarantee::AtLeastOnce {
        flags |= FLAG_AT_LEAST_ONCE;
    }

    let mut record = BytesMut::with_capacity(
        PREFIX_BYTES
            .saturating_add(topic.len())
            .saturating_add(payload.len()),
    );
    put_prefix(&mut record, KIND_PUBLISH, flags, correlation, topic.len());
    record.put_slice(topic);
    record.put_slice(payload);
    record.freeze()
}

fn encode_session(correlation: u64, event: &SessionEvent) -> Bytes {
    let (client, kept) = match event {
        SessionEvent::Kept(record) => (record.client.as_bytes(), Some(record)),
        SessionEvent::Gone(client) => (client.as_bytes(), None),
    };
    let filters = kept.map(|record| record.filters.as_slice()).unwrap_or(&[]);
    let room = filters
        .iter()
        .map(|(filter, _)| filter.as_bytes().len().saturating_add(3))
        .sum::<usize>();

    let mut record = BytesMut::with_capacity(
        PREFIX_BYTES
            .saturating_add(client.len())
            .saturating_add(8)
            .saturating_add(room),
    );
    let flags = if kept.is_some() { 0 } else { FLAG_GONE };
    put_prefix(&mut record, KIND_SESSION, flags, correlation, client.len());
    record.put_slice(client);
    record.put_u64(kept.map_or(0, |held| held.owes_from));
    for (filter, guarantee) in filters {
        let bytes = filter.as_bytes();
        record.put_u16(u16::try_from(bytes.len()).unwrap_or(u16::MAX));
        record.put_slice(bytes);
        record.put_u8(u8::from(*guarantee == DeliveryGuarantee::AtLeastOnce));
    }
    record.freeze()
}

/// What one record said, for replay.
pub(crate) enum Recorded {
    Publish {
        correlation: u64,
        topic: Topic,
        payload: Bytes,
        guarantee: DeliveryGuarantee,
        retain: bool,
    },
    Session {
        correlation: u64,
        record: SessionRecord,
    },
    /// This identity keeps nothing any more.
    SessionGone { correlation: u64, client: ClientId },
}

impl Recorded {
    fn correlation(&self) -> u64 {
        match self {
            Self::Publish { correlation, .. }
            | Self::Session { correlation, .. }
            | Self::SessionGone { correlation, .. } => *correlation,
        }
    }
}

/// `None` for anything this version does not understand: a record it cannot
/// read is not a record it may guess at.
fn decode(record: &[u8]) -> Option<Recorded> {
    let kind = *record.first()?;
    let flags = *record.get(1)?;
    let correlation = u64::from_be_bytes(<[u8; 8]>::try_from(record.get(2..10)?).ok()?);
    let length = usize::from(u16::from_be_bytes([*record.get(10)?, *record.get(11)?]));
    let end = PREFIX_BYTES.checked_add(length)?;
    let name = record.get(PREFIX_BYTES..end)?;
    let rest = record.get(end..)?;

    match kind {
        KIND_PUBLISH => Some(Recorded::Publish {
            correlation,
            topic: Topic::new(Bytes::copy_from_slice(name)).ok()?,
            payload: Bytes::copy_from_slice(rest),
            guarantee: if flags & FLAG_AT_LEAST_ONCE != 0 {
                DeliveryGuarantee::AtLeastOnce
            } else {
                DeliveryGuarantee::AtMostOnce
            },
            retain: flags & FLAG_RETAIN != 0,
        }),
        KIND_SESSION => {
            let client = ClientId::new(Bytes::copy_from_slice(name)).ok()?;
            if flags & FLAG_GONE != 0 {
                return Some(Recorded::SessionGone {
                    correlation,
                    client,
                });
            }
            let owes_from = u64::from_be_bytes(<[u8; 8]>::try_from(rest.get(..8)?).ok()?);
            let mut filters = Vec::new();
            let mut at = 8usize;
            while at < rest.len() {
                let head = at.checked_add(2)?;
                let length = usize::from(u16::from_be_bytes(
                    <[u8; 2]>::try_from(rest.get(at..head)?).ok()?,
                ));
                let body = head.checked_add(length)?;
                let filter =
                    TopicFilter::new(Bytes::copy_from_slice(rest.get(head..body)?)).ok()?;
                let guarantee = if *rest.get(body)? == 0 {
                    DeliveryGuarantee::AtMostOnce
                } else {
                    DeliveryGuarantee::AtLeastOnce
                };
                filters.push((filter, guarantee));
                at = body.checked_add(1)?;
            }
            Some(Recorded::Session {
                correlation,
                record: SessionRecord {
                    client,
                    filters,
                    owes_from,
                },
            })
        }
        _ => None,
    }
}

/// What the first pass over the log found.
struct Found {
    /// The last retained message per topic, and which record said so.
    retained: HashMap<Topic, (u64, Bytes, DeliveryGuarantee)>,
    /// What is kept for each identity; the last word in the log wins.
    sessions: HashMap<ClientId, (u64, SessionRecord)>,
    /// The highest correlation any record carried, so the next one written
    /// continues rather than repeats.
    correlation: u64,
    refused: u64,
}

/// Reads the log once, keeping only what is bounded: retained messages and
/// session state. Payloads of ordinary publishes are not held here — the
/// second pass streams them.
fn scan(settings: &WalSettings, limits: BrokerLimits) -> Result<Found, StorageError> {
    let mut found = Found {
        retained: HashMap::new(),
        sessions: HashMap::new(),
        correlation: 0,
        refused: 0,
    };
    let mut bytes = 0usize;

    let recovery = storage::replay(&settings.path, settings.limits, &mut |record| {
        let Some(recorded) = decode(record) else {
            return;
        };
        found.correlation = found.correlation.max(recorded.correlation());
        match recorded {
            Recorded::Session {
                correlation,
                record,
            } => {
                // Bounded by the same limit the live store has: a log full of
                // session records must not use more memory on the way back in
                // than it was allowed to on the way out (CLAUDE.md §8).
                if found.sessions.contains_key(&record.client)
                    || found.sessions.len() < limits.max_offline_sessions
                {
                    found
                        .sessions
                        .insert(record.client.clone(), (correlation, record));
                } else {
                    found.refused = found.refused.saturating_add(1);
                }
            }
            Recorded::SessionGone { client, .. } => {
                found.sessions.remove(&client);
            }
            Recorded::Publish {
                correlation,
                topic,
                payload,
                guarantee,
                retain: true,
            } => {
                // The log is replayed in commit order, so the last word wins —
                // an empty payload is how a retained message was deleted.
                if payload.is_empty() {
                    if let Some((_, gone, _)) = found.retained.remove(&topic) {
                        bytes = bytes.saturating_sub(gone.len());
                    }
                    return;
                }
                let previous = found
                    .retained
                    .get(&topic)
                    .map(|(_, held, _)| held.len())
                    .unwrap_or(0);
                let would_hold = bytes.saturating_sub(previous).saturating_add(payload.len());
                if would_hold > limits.max_retained_bytes
                    || (previous == 0 && found.retained.len() >= limits.max_retained_messages)
                {
                    found.refused = found.refused.saturating_add(1);
                    return;
                }
                bytes = would_hold;
                found
                    .retained
                    .insert(topic, (correlation, payload, guarantee));
            }
            Recorded::Publish { .. } => {}
        }
    })?;

    tracing::info!(
        path = %settings.path.display(),
        records = recovery.records,
        retained = found.retained.len(),
        sessions = found.sessions.len(),
        refused = found.refused,
        discarded_bytes = recovery.discarded_bytes,
        "replayed the write-ahead log"
    );
    Ok(found)
}

/// Puts back everything the log held: retained messages, the sessions that
/// were kept, and the messages those sessions had not finished with.
///
/// Returns the highest correlation seen, so the next record written
/// continues the numbering instead of repeating it.
pub(crate) fn restore(
    settings: &WalSettings,
    limits: BrokerLimits,
    broker: &mut Broker,
) -> Result<u64, StorageError> {
    let found = scan(settings, limits)?;
    let correlation = found.correlation;

    // Which single record is the current word on each topic and each
    // identity. Everything else the log says about them is history, and
    // compaction below is what makes that a size rather than a comment.
    let mut current: std::collections::HashSet<u64> = std::collections::HashSet::with_capacity(
        found.retained.len().saturating_add(found.sessions.len()),
    );
    current.extend(found.retained.values().map(|(at, _, _)| *at));
    current.extend(found.sessions.values().map(|(at, _)| *at));

    for (topic, (_, payload, guarantee)) in found.retained {
        broker.restore_retained(topic, payload, guarantee);
    }

    // Everything a session still owes sits after its mark, so the oldest mark
    // is where the second pass has to start paying attention.
    let mut from = u64::MAX;
    let mut sessions = 0usize;
    for (_, (_, record)) in found.sessions {
        from = from.min(record.owes_from);
        if broker.restore_session(record) {
            sessions = sessions.saturating_add(1);
        }
    }
    if sessions == 0 || from >= correlation {
        compact(settings, &current, from)?;
        return Ok(correlation);
    }

    // Second pass: the publishes themselves, streamed rather than held. They
    // route into the restored sessions' backlogs exactly as they did live.
    let mut out = Outbox::new();
    let mut replayed = 0u64;
    storage::replay(&settings.path, settings.limits, &mut |record| {
        let Some(Recorded::Publish {
            correlation,
            topic,
            payload,
            guarantee,
            ..
        }) = decode(record)
        else {
            return;
        };
        // At-most-once was never owed to anyone who was not listening.
        if correlation <= from || guarantee != DeliveryGuarantee::AtLeastOnce {
            return;
        }
        replayed = replayed.saturating_add(1);
        broker.restore_publish(
            correlation,
            &Publish {
                topic,
                payload,
                guarantee,
                retain: false,
            },
            &mut out,
        );
    })?;

    tracing::info!(sessions, replayed, from, "restored kept sessions");
    compact(settings, &current, from)?;
    Ok(correlation)
}

/// Drops everything the log no longer needs: superseded session records,
/// tombstoned identities, overwritten retained messages, and publishes every
/// session has finished with.
///
/// Runs at startup, where nothing is appending and the broker's state came
/// from this same log a moment ago. `current` names the one record that is
/// the present word on each topic and identity; `floor` is the oldest mark
/// any session still owes from, so publishes at or below it are owed to
/// nobody (docs/storage.md §104).
fn compact(
    settings: &WalSettings,
    current: &std::collections::HashSet<u64>,
    floor: u64,
) -> Result<(), StorageError> {
    if settings.compact_bytes == 0 {
        return Ok(());
    }
    let size = std::fs::metadata(&settings.path)
        .map(|held| held.len())
        .unwrap_or(0);
    if size < settings.compact_bytes {
        return Ok(());
    }

    let outcome = storage::compact(&settings.path, settings.limits, &mut |payload| {
        let Some(recorded) = decode(payload) else {
            // A record this version cannot read is a record it cannot keep
            // a promise about either.
            return false;
        };
        match recorded {
            // The tombstone did its work in the pass above; carrying it
            // forward would only ask the next startup to do it again.
            Recorded::SessionGone { .. } => false,
            Recorded::Session { correlation, .. } => current.contains(&correlation),
            Recorded::Publish {
                correlation,
                guarantee,
                ..
            } => {
                current.contains(&correlation)
                    || (guarantee == DeliveryGuarantee::AtLeastOnce && correlation > floor)
            }
        }
    })?;

    tracing::info!(
        path = %settings.path.display(),
        records_before = outcome.records_before,
        records_after = outcome.records_after,
        bytes_before = outcome.bytes_before,
        bytes_after = outcome.bytes_after,
        "compacted the write-ahead log"
    );
    Ok(())
}

/// Opens the log, recovers its durable prefix and starts the writer thread.
pub(crate) fn start(
    settings: &WalSettings,
    restored: u64,
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

    // A weak sender on purpose, and downgraded here rather than inside the
    // closure so the strong one is not carried along by the move. The broker
    // loop holds a submitter of its own for session records, so a strong
    // sender here would close a cycle: two channels, each kept open only by
    // the thread waiting on the other.
    let weak = events.downgrade();
    drop(events);

    let writer = std::thread::Builder::new()
        .name("straum-wal".to_owned())
        .spawn(move || write(committer, Queue(queue), weak, metrics))
        .map_err(StorageError::Io)?;

    Ok((
        Submitter {
            records,
            // Continues where the recovered log left off: a repeated
            // correlation would make a session's mark mean two records.
            next: Arc::new(AtomicU64::new(restored)),
        },
        writer,
    ))
}

fn write(
    mut committer: Committer,
    mut queue: Queue,
    events: mpsc::WeakSender<Event>,
    metrics: Arc<Metrics>,
) {
    let outcome = committer.run(&mut queue, |commit| {
        Metrics::increment(&metrics.wal_commits, 1);
        Metrics::increment(&metrics.wal_records_committed, commit.records as u64);
        Metrics::increment(&metrics.wal_bytes_committed, commit.bytes as u64);
        metrics.wal_sync_nanos.store(
            u64::try_from(commit.sync.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        metrics
            .wal_durable_sequence
            .store(commit.durable_sequence, Ordering::Relaxed);

        // The broker may acknowledge everything up to here, and not one
        // record more (docs/storage.md §99, invariant 1).
        if let Some(events) = events.upgrade() {
            let _ = events.blocking_send(Event::Durable(DurableToken(commit.last_correlation)));
        }
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
