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
        // The queue slot first, the number second. A correlation taken before
        // the record is certain to be queued leaves a hole the durable
        // watermark can never close, and the watermark is what an
        // acknowledgement rests on (docs/replication.md §110).
        let permit = self.records.reserve().await.ok()?;
        let correlation = self.next.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        permit.send(WalRecord::local(
            correlation,
            encode(correlation, topic, payload, guarantee, retain),
        ));
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
        // Same order as `submit`, and here it is the whole point: this call
        // is expected to fail and be retried, and a correlation burned on
        // every refusal would stall the watermark permanently.
        let Ok(permit) = self.records.try_reserve() else {
            return false;
        };
        let correlation = self.next.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        permit.send(WalRecord::local(
            correlation,
            encode_session(correlation, event),
        ));
        true
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
/// position: compaction rewrites the log, so a position is where a record
/// happens to sit today and a correlation is what it is. Eight bytes a
/// record is what that costs.
///
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

    // The header's high-water is a floor under whatever the surviving records
    // carry. Compaction removes records — an at-most-once publish and a
    // session tombstone go whatever their age — so the largest correlation
    // still in the file is not the largest ever handed out
    // (docs/replication.md §116).
    found.correlation = found.correlation.max(recovery.correlation);

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
        compact(settings, &current, from, correlation)?;
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
    compact(settings, &current, from, correlation)?;
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
/// Whether one record is still the present word, or still owed to somebody.
///
/// `current` names the single record that is current for each topic and each
/// identity; `floor` is the oldest mark any session still owes from.
fn should_keep(payload: &[u8], current: &std::collections::HashSet<u64>, floor: u64) -> bool {
    let Some(recorded) = decode(payload) else {
        // A record this version cannot read is a record it cannot keep a
        // promise about either.
        return false;
    };
    match recorded {
        // The tombstone did its work when the live set was computed;
        // carrying it forward only asks the next pass to do it again.
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
}

/// What the log still needs, read from the log itself.
///
/// The writer thread can work this out alone: session records carry their
/// own mark, and a stale one is conservative — an older mark keeps more,
/// never less. That is what lets compaction run beside the writer without
/// asking the broker anything.
fn live_set(
    settings: &WalSettings,
    limits: BrokerLimits,
) -> Result<(std::collections::HashSet<u64>, u64), StorageError> {
    let found = scan(settings, limits)?;
    let mut current: std::collections::HashSet<u64> = std::collections::HashSet::with_capacity(
        found.retained.len().saturating_add(found.sessions.len()),
    );
    current.extend(found.retained.values().map(|(at, _, _)| *at));
    current.extend(found.sessions.values().map(|(at, _)| *at));

    let floor = found
        .sessions
        .values()
        .map(|(_, record)| record.owes_from)
        .min()
        .unwrap_or(u64::MAX);
    Ok((current, floor))
}

fn compact(
    settings: &WalSettings,
    current: &std::collections::HashSet<u64>,
    floor: u64,
    correlation: u64,
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

    let outcome = storage::compact(
        &settings.path,
        settings.limits,
        correlation,
        &mut |payload| should_keep(payload, current, floor),
    )?;

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

/// Positions the writer thread shares with the rest of the process. Both are
/// read once per rewrite and never on the append path.
struct Marks {
    /// The correlation allocator, so compaction can write its value into the
    /// header rather than guess it from what survived (§116).
    allocated: Arc<AtomicU64>,
    /// How far a replica has been given the log. `u64::MAX` means nobody is
    /// waiting on these bytes, which is what a single node means (§118).
    replicated: Arc<AtomicU64>,
    /// The live fan-out to a follower, if there is one.
    shipments: Option<mpsc::Sender<crate::replication::Shipment>>,
    /// This node's own durable sequence, published for the replication task.
    local: Arc<AtomicU64>,
}

/// Opens the log, recovers its durable prefix and starts the writer thread.
/// Where a committed batch goes besides the disk, if anywhere
/// (docs/replication.md §119).
pub(crate) struct FanOut {
    /// The live queue to a follower.
    pub(crate) shipments: Option<mpsc::Sender<crate::replication::Shipment>>,
    /// This node's own durable sequence, published so a reconnecting
    /// follower can be reconciled against what this log actually holds.
    pub(crate) local: Arc<AtomicU64>,
}

pub(crate) fn start(
    settings: &WalSettings,
    limits: BrokerLimits,
    restored: u64,
    fan_out: FanOut,
    // Whether a commit waits for the storage device: `fsynced` does,
    // `written` does not (docs/storage.md §80).
    sync: bool,
    events: mpsc::Sender<Event>,
    metrics: Arc<Metrics>,
) -> Result<(Submitter, Arc<AtomicU64>, std::thread::JoinHandle<()>), StorageError> {
    let (wal, recovery) = Wal::open(&settings.path, settings.limits)?;
    tracing::info!(
        path = %settings.path.display(),
        records = recovery.records,
        durable_offset = recovery.durable_offset,
        discarded_bytes = recovery.discarded_bytes,
        "recovered the write-ahead log"
    );
    let (records, queue) = mpsc::channel(settings.queue);
    let committer = Committer::new(wal, settings.limits, sync, restored);

    let owned = settings.clone();
    // A weak sender on purpose, and downgraded here rather than inside the
    // closure so the strong one is not carried along by the move. The broker
    // loop holds a submitter of its own for session records, so a strong
    // sender here would close a cycle: two channels, each kept open only by
    // the thread waiting on the other.
    let weak = events.downgrade();
    drop(events);

    // The allocator itself, shared with the writer thread so compaction can
    // write down the correlation high-water. Read once per rewrite, never on
    // the append path, and not a channel — so no cycle to close.
    let next = Arc::new(AtomicU64::new(restored));
    // The replication task lowers `replicated` once there is a follower;
    // until then nothing is waiting on the bytes.
    let replicated = Arc::new(AtomicU64::new(u64::MAX));
    let marks = Marks {
        allocated: Arc::clone(&next),
        replicated: Arc::clone(&replicated),
        shipments: fan_out.shipments,
        local: fan_out.local,
    };

    let writer = std::thread::Builder::new()
        .name("straum-wal".to_owned())
        .spawn(move || write(committer, Queue(queue), owned, limits, weak, metrics, marks))
        .map_err(StorageError::Io)?;

    // `next` continues where the recovered log left off: a repeated
    // correlation would make a session's mark mean two records.
    Ok((Submitter { records, next }, replicated, writer))
}

fn write(
    mut committer: Committer,
    mut queue: Queue,
    settings: WalSettings,
    limits: BrokerLimits,
    events: mpsc::WeakSender<Event>,
    metrics: Arc<Metrics>,
    marks: Marks,
) {
    // The loop is here rather than `Committer::run` because compaction needs
    // a point between two commits, and `run` does not offer one: the log is
    // rewritten while nothing is staged and nothing else holds the file.
    let outcome = loop {
        let commit = match committer.commit_available(&mut queue) {
            // The source is gone; every submitter has dropped.
            Ok(None) => break Ok(()),
            Ok(Some(commit)) => commit,
            Err(error) => break Err(error),
        };
        report(&commit, &events, &metrics);
        marks
            .local
            .store(commit.durable_sequence, Ordering::Relaxed);
        if let Some(shipments) = &marks.shipments {
            // The fork to a replica, after the local durability operation.
            // Beside it would halve what a quorum waits for and is a
            // measurement, not a refactor (docs/replication.md §112).
            crate::replication::ship(
                shipments,
                commit
                    .durable_sequence
                    .saturating_sub(u64::try_from(commit.records).unwrap_or(0)),
                commit.durable_sequence,
                committer.committed_batch(),
                &metrics,
            );
        }

        if let Err(error) = shrink(&mut committer, &settings, limits, &metrics, &marks) {
            // A log that cannot be rewritten is still a log that works; it
            // only grows. Nothing is acknowledged differently because of it.
            tracing::warn!(%error, "could not compact the log; it keeps growing");
        }
    };

    if let Err(error) = outcome {
        Metrics::increment(&metrics.wal_commit_failures, 1);
        // Nothing is reported as durable after this, so nothing is
        // acknowledged as durable either (invariant 7).
        tracing::error!(%error, "the write-ahead log failed; no further publish will be acknowledged");
    }
    tracing::debug!("wal writer stopped");
}

/// Rewrites the log if it has grown past the threshold.
///
/// Called between commits, where nothing is staged: the alternative is
/// writing a batch into a file that is about to be replaced.
fn shrink(
    committer: &mut Committer,
    settings: &WalSettings,
    limits: BrokerLimits,
    metrics: &Metrics,
    marks: &Marks,
) -> Result<(), StorageError> {
    if settings.compact_bytes == 0 || committer.log_bytes() < settings.compact_bytes {
        return Ok(());
    }

    // A replica that has not received these records still needs the bytes,
    // and STRAUMW3 only kept the *numbers* meaningful — a sequence that
    // exists logically is no use to a follower asking to be sent it
    // (docs/replication.md §118, C4). Holding the whole log is the blunt
    // answer and the only correct one until there is a retention policy: a
    // follower that stays away keeps the log growing, which an operator can
    // see and act on, where dropping the records it needs is silent.
    let behind = marks.replicated.load(Ordering::Relaxed);
    let durable = committer.durable_sequence();
    if behind < durable {
        tracing::debug!(
            replicated = behind,
            durable,
            "not compacting: a replica is still behind the log"
        );
        return Ok(());
    }

    let (current, floor) = live_set(settings, limits)?;
    // The allocator, not the log: the highest correlation handed out is not
    // the highest one still on disk (docs/replication.md §116).
    let correlation = marks.allocated.load(Ordering::Relaxed);
    let outcome = committer.compact(correlation, &mut |payload| {
        should_keep(payload, &current, floor)
    })?;
    Metrics::increment(&metrics.wal_compactions, 1);

    tracing::info!(
        records_before = outcome.records_before,
        records_after = outcome.records_after,
        bytes_before = outcome.bytes_before,
        bytes_after = outcome.bytes_after,
        "compacted the write-ahead log beside the writer"
    );
    Ok(())
}

fn report(commit: &storage::Commit, events: &mpsc::WeakSender<Event>, metrics: &Metrics) {
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
        let _ = events.blocking_send(Event::Durable {
            sequence: commit.durable_sequence,
            correlation: DurableToken(commit.durable_correlation),
        });
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use storage::Wal;

    fn scratch(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "straum-runtime-wal-{name}-{}-{:?}.wal",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn filter(pattern: &str) -> TopicFilter {
        TopicFilter::new(Bytes::copy_from_slice(pattern.as_bytes())).unwrap()
    }

    fn topic(name: &str) -> Topic {
        Topic::new(Bytes::copy_from_slice(name.as_bytes())).unwrap()
    }

    fn client(name: &str) -> ClientId {
        ClientId::new(Bytes::copy_from_slice(name.as_bytes())).unwrap()
    }

    #[test]
    fn a_refused_session_record_does_not_consume_a_correlation() {
        let (records, mut queue) = mpsc::channel(1);
        let submitter = Submitter {
            records,
            next: Arc::new(AtomicU64::new(0)),
        };

        assert!(submitter.try_submit_session(&SessionEvent::Gone(client("a"))));

        // The queue is full. This refusal is an ordinary, expected outcome —
        // the broker loop retries — and it must cost nothing: a correlation
        // burned here is a hole no commit can ever fill, and the durable
        // watermark would stop advancing for good (docs/replication.md §110).
        assert!(!submitter.try_submit_session(&SessionEvent::Gone(client("b"))));

        assert_eq!(queue.try_recv().unwrap().correlation, 1);
        assert!(submitter.try_submit_session(&SessionEvent::Gone(client("b"))));
        assert_eq!(queue.try_recv().unwrap().correlation, 2);
    }
    /// A log whose last two records are exactly the kinds compaction throws
    /// away: an at-most-once publish, which is owed to nobody, and a session
    /// tombstone, which has already done its work. Both carry the highest
    /// correlations in the log.
    fn log_whose_newest_records_are_disposable(path: &std::path::Path) {
        let (mut wal, _) = Wal::open(path, storage::WalLimits::default()).unwrap();

        // 1: a retained message. Compaction keeps it — it is the current word
        // on its topic.
        wal.stage(&encode(
            1,
            &topic("sensors/eu"),
            &Bytes::from_static(b"21"),
            DeliveryGuarantee::AtMostOnce,
            true,
        ))
        .unwrap();
        // 2: a kept session. Also current.
        wal.stage(&encode_session(
            2,
            &SessionEvent::Kept(SessionRecord {
                client: client("keeper"),
                filters: vec![(filter("sensors/#"), DeliveryGuarantee::AtLeastOnce)],
                owes_from: 2,
            }),
        ))
        .unwrap();
        // 3: an at-most-once publish. Nobody is owed it, so it goes.
        wal.stage(&encode(
            3,
            &topic("chatter"),
            &Bytes::from_static(b"noise"),
            DeliveryGuarantee::AtMostOnce,
            false,
        ))
        .unwrap();
        // 4: a tombstone. It did its work when the live set was computed.
        wal.stage(&encode_session(4, &SessionEvent::Gone(client("gone"))))
            .unwrap();
        wal.commit().unwrap();
    }

    #[test]
    fn compaction_cannot_rewind_the_correlation_high_water() {
        let path = scratch("highwater");
        log_whose_newest_records_are_disposable(&path);

        let settings = WalSettings {
            path: path.clone(),
            queue: 8,
            // Any size at all: this log is to be rewritten.
            compact_bytes: 1,
            limits: storage::WalLimits::default(),
        };
        let limits = BrokerLimits::default();

        let found = scan(&settings, limits).unwrap();
        assert_eq!(found.correlation, 4, "four records were written");

        let (current, floor) = live_set(&settings, limits).unwrap();
        compact(&settings, &current, floor, found.correlation).unwrap();

        // The two newest records really are gone — otherwise this proves
        // nothing.
        let left = storage::replay(&path, settings.limits, &mut |_| {}).unwrap();
        assert_eq!(
            left.records, 2,
            "compaction kept records it should not have"
        );

        // And the high-water did not come back down with them. Before
        // STRAUMW3 this read 2, and the next publish would have been given a
        // correlation two other records already had
        // (docs/replication.md §116).
        let after = scan(&settings, limits).unwrap();
        assert_eq!(after.correlation, 4);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_restored_broker_continues_numbering_above_everything_compacted_away() {
        let path = scratch("restore");
        log_whose_newest_records_are_disposable(&path);

        let settings = WalSettings {
            path: path.clone(),
            queue: 8,
            compact_bytes: 1,
            limits: storage::WalLimits::default(),
        };
        let limits = BrokerLimits::default();

        // The whole startup path, which compacts on its way through.
        let mut broker = Broker::new(limits);
        let restored = restore(&settings, limits, &mut broker).unwrap();
        assert_eq!(restored, 4);

        // Restarting again on the compacted log gives the same answer, which
        // is the property that makes it an identity at all.
        let mut broker = Broker::new(limits);
        assert_eq!(restore(&settings, limits, &mut broker).unwrap(), 4);
        let _ = std::fs::remove_file(&path);
    }
    #[test]
    fn a_replica_that_is_behind_holds_the_log_open() {
        let path = scratch("c4");
        let settings = WalSettings {
            path: path.clone(),
            queue: 8,
            // Anything at all is large enough to rewrite.
            compact_bytes: 1,
            limits: storage::WalLimits::default(),
        };
        let limits = BrokerLimits::default();
        let metrics = Metrics::default();

        let build = || {
            let (mut wal, _) = storage::Wal::open(&settings.path, settings.limits).unwrap();
            // Four records nothing needs: at-most-once, owed to no session.
            for correlation in 1..=4u64 {
                wal.stage(&encode(
                    correlation,
                    &topic("chatter"),
                    &Bytes::from_static(b"noise"),
                    DeliveryGuarantee::AtMostOnce,
                    false,
                ))
                .unwrap();
            }
            wal.commit().unwrap();
            Committer::new(wal, settings.limits, true, 0)
        };

        // A replica that has only had the first two records still needs the
        // bytes of the other two, even though STRAUMW3 keeps their numbers
        // meaningful (docs/replication.md §118).
        let mut committer = build();
        let behind = Marks {
            allocated: Arc::new(AtomicU64::new(0)),
            replicated: Arc::new(AtomicU64::new(2)),
            shipments: None,
            local: Arc::new(AtomicU64::new(0)),
        };
        shrink(&mut committer, &settings, limits, &metrics, &behind).unwrap();
        drop(committer);
        assert_eq!(
            storage::replay(&path, settings.limits, &mut |_| {})
                .unwrap()
                .records,
            4,
            "the log was rewritten out from under a replica that is behind"
        );

        // Caught up, and the same log is compacted as it always was.
        let mut committer = build();
        let caught_up = Marks {
            allocated: Arc::new(AtomicU64::new(0)),
            replicated: Arc::new(AtomicU64::new(u64::MAX)),
            shipments: None,
            local: Arc::new(AtomicU64::new(0)),
        };
        shrink(&mut committer, &settings, limits, &metrics, &caught_up).unwrap();
        drop(committer);
        assert_eq!(
            storage::replay(&path, settings.limits, &mut |_| {})
                .unwrap()
                .records,
            0,
            "nothing needed these records and they should have gone"
        );
        let _ = std::fs::remove_file(&path);
    }
}
