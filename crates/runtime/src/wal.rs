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
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::broker_loop::Durable;
use crate::config::WalSettings;
use crate::metrics::Metrics;

/// Submits records and hands out the tokens the broker waits on.
#[derive(Debug, Clone)]
pub(crate) struct Submitter {
    records: mpsc::Sender<WalRecord>,
    next: Arc<AtomicU64>,
    /// One permit per acknowledgement the broker may hold back for
    /// durability, `max_pending_durable` in all (C8, D-7).
    admission: Arc<Semaphore>,
}

impl Submitter {
    /// Waits until the broker may hold one more acknowledgement back.
    ///
    /// Taken before a publish that will be acknowledged goes to the log, so
    /// the wait lands on that publisher alone, at its own reader: every other
    /// command from every other client keeps moving (D-7). The permit is the
    /// pending acknowledgement's for as long as there is one; the broker loop
    /// hands it back with [`Submitter::release`].
    pub(crate) async fn admit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.admission).acquire_owned().await.ok()
    }

    /// A submitter with no writer behind it, for tests that drive the broker
    /// loop directly. The receiver is what the writer would have read.
    #[cfg(test)]
    pub(crate) fn detached(admission: Arc<Semaphore>) -> (Self, mpsc::Receiver<WalRecord>) {
        let (records, queue) = mpsc::channel(16);
        let next = Arc::new(AtomicU64::new(0));
        (
            Self {
                records,
                next,
                admission,
            },
            queue,
        )
    }

    /// Returns permits whose acknowledgement is no longer held, or never
    /// was: the record was durable before the broker saw it.
    pub(crate) fn release(&self, permits: usize) {
        if permits > 0 {
            self.admission.add_permits(permits);
        }
    }

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
    /// The highest WAL sequence the log has ever allocated. A different
    /// space from `correlation`: this one numbers records the log accepted,
    /// and it is what a replication position is expressed in.
    sequence: u64,
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
        sequence: 0,
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
    // Read straight off the header, never derived from the correlations in
    // the bodies: the log allocates sequences for records that carry no
    // correlation at all.
    found.sequence = recovery.sequence;

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

/// Where a recovered log leaves the two identity spaces.
///
/// They are independent and neither is derived from the other
/// (docs/storage.md, location-vs-identity). `sequence` numbers what the WAL
/// accepted and is what a replication position means; `correlation` is the
/// broker's own publication identity, opaque to the log. They coincide
/// numerically in an ordinary single-node run, which is an accident of that
/// run and not a contract — one record type that carries no correlation, or
/// one compaction that moves them by different amounts, separates them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Restored {
    /// The highest WAL sequence the log has ever allocated.
    pub(crate) sequence: u64,
    /// The highest correlation any surviving record carried, or the header's
    /// floor if that is larger.
    pub(crate) correlation: u64,
}

/// Puts back everything the log held: retained messages, the sessions that
/// were kept, and the messages those sessions had not finished with.
///
/// Returns where both identity spaces stand, so the next record written
/// continues each of them instead of repeating it.
pub(crate) fn restore(
    settings: &WalSettings,
    // Nothing has reported a position yet: under `quorum` everything still
    // addressable stays so, and the first rewrite beside the writer, after
    // the follower has spoken, is the one that may move the floor (D-20).
    retention: Retention,
    limits: BrokerLimits,
    broker: &mut Broker,
) -> Result<Restored, StorageError> {
    let found = scan(settings, limits)?;
    let correlation = found.correlation;
    let restored = Restored {
        sequence: found.sequence,
        correlation,
    };

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
        compact(settings, retention.retain(0), &current, from, correlation)?;
        return Ok(restored);
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
    compact(settings, retention.retain(0), &current, from, correlation)?;
    Ok(restored)
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
    retain: storage::Retain,
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
        retain,
        &mut |payload| should_keep(payload, current, floor),
    )?;

    tracing::info!(
        path = %settings.path.display(),
        records_before = outcome.records_before,
        records_after = outcome.records_after,
        bytes_before = outcome.bytes_before,
        floor = outcome.floor,
        bytes_after = outcome.bytes_after,
        "compacted the write-ahead log"
    );
    Ok(())
}

/// What a rewrite keeps readable by sequence, so a follower can be caught
/// up from it (docs/replication.md §130).
///
/// Decided once, from the configuration: whether there is a follower and
/// whether acknowledgements wait for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Retention {
    /// No follower: nothing reads the log by sequence, and a rewrite keeps
    /// only what the broker still needs.
    Nothing,
    /// A follower that acknowledgements do not wait for: the last this many
    /// bytes stay addressable, whatever the follower has reported. One that
    /// falls further behind than that is lost, and said to be (D-18).
    Bytes(u64),
    /// A follower that acknowledgements wait for: everything past its own
    /// durable reports stays addressable, so a quorum resumes on its own
    /// after an outage of any length. The quorum watermark plays no part —
    /// it is a durability position, not a catch-up one (D-20).
    Follower,
}

impl Retention {
    /// What to keep, given the follower's reported durable position.
    pub(crate) fn retain(self, replicated: u64) -> storage::Retain {
        match self {
            Self::Nothing => storage::Retain::NONE,
            Self::Bytes(bytes) => storage::Retain {
                from: u64::MAX,
                bytes,
            },
            Self::Follower => storage::Retain {
                from: replicated.saturating_add(1),
                bytes: 0,
            },
        }
    }
}

/// Positions the writer thread shares with the rest of the process. All are
/// read once per rewrite and never on the append path.
struct Marks {
    /// The correlation allocator, so compaction can write its value into the
    /// header rather than guess it from what survived (§116).
    allocated: Arc<AtomicU64>,
    /// How far the follower has reported itself durable. `u64::MAX` means no
    /// follower position matters: a single node, or a follower this log has
    /// refused for good.
    replicated: Arc<AtomicU64>,
    retention: Retention,
    /// The live fan-out to a follower, if there is one.
    shipments: Option<mpsc::Sender<crate::replication::Shipment>>,
    /// This node's own durable sequence, published for the replication task.
    local: Arc<AtomicU64>,
    /// Set by a test to make the next commit fail the way a real one does.
    #[cfg(test)]
    fail: Arc<std::sync::atomic::AtomicBool>,
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
    /// What a rewrite keeps readable for the follower.
    pub(crate) retention: Retention,
    /// Test-only: makes the next commit fail (see `Runtime::fail_log`).
    #[cfg(test)]
    pub(crate) fail: Arc<std::sync::atomic::AtomicBool>,
}

#[expect(
    clippy::too_many_arguments,
    reason = "the log is wired once; the alternative is a struct that exists only to be destructured"
)]
pub(crate) fn start(
    settings: &WalSettings,
    limits: BrokerLimits,
    restored: u64,
    fan_out: FanOut,
    // Whether a commit waits for the storage device: `fsynced` does,
    // `written` does not (docs/storage.md §80).
    sync: bool,
    // Where each commit is reported. Its own channel, not the one commands
    // arrive on, so the broker loop can stop reading reports when a quorum
    // is behind without stopping every client's commands (D-7).
    durable: mpsc::Sender<Durable>,
    admission: Arc<Semaphore>,
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
    // The allocator itself, shared with the writer thread so compaction can
    // write down the correlation high-water. Read once per rewrite, never on
    // the append path, and not a channel — so no cycle to close.
    let next = Arc::new(AtomicU64::new(restored));
    // Under `quorum` nothing is known about the follower until it reports,
    // and what it needs must not be rewritten away before then: from zero,
    // everything still addressable stays so. Otherwise the position decides
    // only whether a follower was lost, and there is none to lose yet.
    let replicated = Arc::new(AtomicU64::new(match fan_out.retention {
        Retention::Follower => 0,
        Retention::Nothing | Retention::Bytes(_) => u64::MAX,
    }));
    metrics.wal_retention_floor.store(
        storage::allocated(&settings.path)?.retention_floor(),
        Ordering::Relaxed,
    );
    let rewrites = Rewrites {
        at: recovery.durable_offset,
        addressable_from: recovery.addressable_offset,
        lost: false,
    };
    let marks = Marks {
        allocated: Arc::clone(&next),
        replicated: Arc::clone(&replicated),
        retention: fan_out.retention,
        shipments: fan_out.shipments,
        local: fan_out.local,
        #[cfg(test)]
        fail: fan_out.fail,
    };

    let writer = std::thread::Builder::new()
        .name("straum-wal".to_owned())
        .spawn(move || {
            write(
                committer,
                Queue(queue),
                owned,
                limits,
                durable,
                metrics,
                marks,
                rewrites,
            )
        })
        .map_err(StorageError::Io)?;

    // `next` continues where the recovered log left off: a repeated
    // correlation would make a session's mark mean two records.
    Ok((
        Submitter {
            records,
            next,
            admission,
        },
        replicated,
        writer,
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the writer thread's whole state, moved in once; a struct would exist only to be destructured"
)]
fn write(
    mut committer: Committer,
    mut queue: Queue,
    settings: WalSettings,
    limits: BrokerLimits,
    durable: mpsc::Sender<Durable>,
    metrics: Arc<Metrics>,
    marks: Marks,
    mut rewrites: Rewrites,
) {
    // The loop is here rather than `Committer::run` because compaction needs
    // a point between two commits, and `run` does not offer one: the log is
    // rewritten while nothing is staged and nothing else holds the file.
    let outcome = loop {
        let commit = match committer.commit_available(&mut queue) {
            // The source is gone; every submitter has dropped.
            Ok(None) => break Ok(()),
            Ok(Some(commit)) => {
                // A test standing in for a failed write or sync: the same
                // exit a real failure takes, so what follows is the real
                // failure path, not a model of it.
                #[cfg(test)]
                if marks.fail.swap(false, Ordering::Relaxed) {
                    break Err(StorageError::Io(std::io::Error::other(
                        "a failure injected by a test",
                    )));
                }
                commit
            }
            Err(error) => break Err(error),
        };
        report(&commit, &durable, &metrics);
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

        if let Err(error) = shrink(
            &mut committer,
            &settings,
            limits,
            &metrics,
            &marks,
            &mut rewrites,
        ) {
            // A log that cannot be rewritten is still a log that works; it
            // only grows. Nothing is acknowledged differently because of it.
            tracing::warn!(%error, "could not compact the log; it keeps growing");
        }
    };

    // Closed before the failure is counted or logged: from the moment it is
    // visible, a submitter must already find the queue gone and get no
    // token, so no publish can be queued into a log that will never write it
    // (D-9).
    drop(queue);
    if let Err(error) = outcome {
        Metrics::increment(&metrics.wal_commit_failures, 1);
        // Nothing is reported as durable after this, so nothing is
        // acknowledged as durable either (invariant 7).
        tracing::error!(%error, "the write-ahead log failed; no further publish will be acknowledged");
    }
    tracing::debug!("wal writer stopped");
}

/// The writer's own record of the last rewrite.
struct Rewrites {
    /// Where the log ended when it was last rewritten, or when the writer
    /// started.
    at: u64,
    /// Where the addressable region starts in the file as it is now.
    addressable_from: u64,
    /// Whether the floor has already been reported as past the follower,
    /// so one fall is said once rather than at every rewrite (D-18).
    lost: bool,
}

/// Rewrites the log once enough has been appended since the last rewrite.
///
/// Called between commits, where nothing is staged: the alternative is
/// writing a batch into a file that is about to be replaced.
///
/// Measured from the last rewrite, not from the size of the file. A file
/// that has to stay large — a retained suffix, a live set bigger than the
/// threshold — would otherwise be over it after every commit and rewritten
/// after every commit (D-18).
fn shrink(
    committer: &mut Committer,
    settings: &WalSettings,
    limits: BrokerLimits,
    metrics: &Metrics,
    marks: &Marks,
    rewrites: &mut Rewrites,
) -> Result<(), StorageError> {
    // Once per commit, a store: how much of the log a follower can still be
    // served from, as it is now rather than as of the last rewrite.
    metrics.wal_retained_bytes.store(
        committer
            .log_bytes()
            .saturating_sub(rewrites.addressable_from),
        Ordering::Relaxed,
    );
    if settings.compact_bytes == 0
        || committer.log_bytes().saturating_sub(rewrites.at) < settings.compact_bytes
    {
        return Ok(());
    }

    // Commits wait from here to the end of the rewrite, so this is the stall
    // a rewrite costs them, scan included (D-24).
    let started = std::time::Instant::now();
    let (current, floor) = live_set(settings, limits)?;
    // The allocator, not the log: the highest correlation handed out is not
    // the highest one still on disk (docs/replication.md §116).
    let correlation = marks.allocated.load(Ordering::Relaxed);
    let follower = marks.replicated.load(Ordering::Relaxed);
    let outcome = committer.compact(
        correlation,
        marks.retention.retain(follower),
        &mut |payload| should_keep(payload, &current, floor),
    )?;
    rewrites.at = committer.log_bytes();
    rewrites.addressable_from = outcome.bytes_after.saturating_sub(outcome.retained_bytes);
    Metrics::increment(&metrics.wal_compactions, 1);
    metrics.wal_rewrite_nanos.store(
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    metrics
        .wal_retention_floor
        .store(outcome.floor, Ordering::Relaxed);
    metrics
        .wal_retained_bytes
        .store(outcome.retained_bytes, Ordering::Relaxed);

    // A follower that needs a record below the floor can no longer be
    // served from this log. While it is streaming live it may never ask;
    // once it has to catch up, it is refused (§130). Said at the moment the
    // floor passes it, because that is the moment it became true.
    let behind = follower != u64::MAX && follower.saturating_add(1) < outcome.floor;
    if behind && !rewrites.lost {
        Metrics::increment(&metrics.replication_followers_lost, 1);
        tracing::error!(
            follower,
            floor = outcome.floor,
            "the log no longer holds what the follower needs: if it has to catch up it \
             will need a fresh copy of the log"
        );
    }
    rewrites.lost = behind;

    tracing::info!(
        records_before = outcome.records_before,
        records_after = outcome.records_after,
        bytes_before = outcome.bytes_before,
        bytes_after = outcome.bytes_after,
        floor = outcome.floor,
        retained_bytes = outcome.retained_bytes,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "compacted the write-ahead log beside the writer"
    );
    Ok(())
}

fn report(commit: &storage::Commit, durable: &mpsc::Sender<Durable>, metrics: &Metrics) {
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
    //
    // A strong sender, where it used to be a weak one. The weak one existed
    // because these reports shared the command channel, and the broker loop
    // ended only when that channel had no senders left: a strong one here
    // would have kept it open for as long as this thread waited for the loop
    // to let go of its submitter. The loop now ends on the command channel
    // alone, so this sender keeps nothing alive. Once the loop is gone the
    // send fails, and a report nobody can act on is dropped.
    let _ = durable.blocking_send(Durable {
        sequence: commit.durable_sequence,
        correlation: DurableToken(commit.durable_correlation),
    });
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
            admission: Arc::new(Semaphore::new(1)),
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
            retain_bytes: 0,
            limits: storage::WalLimits::default(),
        };
        let limits = BrokerLimits::default();

        let found = scan(&settings, limits).unwrap();
        assert_eq!(found.correlation, 4, "four records were written");

        let (current, floor) = live_set(&settings, limits).unwrap();
        compact(
            &settings,
            storage::Retain::NONE,
            &current,
            floor,
            found.correlation,
        )
        .unwrap();

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
            retain_bytes: 0,
            limits: storage::WalLimits::default(),
        };
        let limits = BrokerLimits::default();

        // The whole startup path, which compacts on its way through.
        let mut broker = Broker::new(limits);
        let restored = restore(&settings, Retention::Nothing, limits, &mut broker).unwrap();
        assert_eq!(restored.correlation, 4);

        // Restarting again on the compacted log gives the same answer, which
        // is the property that makes it an identity at all.
        let mut broker = Broker::new(limits);
        assert_eq!(
            restore(&settings, Retention::Nothing, limits, &mut broker)
                .unwrap()
                .correlation,
            4
        );
        let _ = std::fs::remove_file(&path);
    }

    /// The two identity spaces are independent, and a restart continues each
    /// of them on its own.
    ///
    /// Not a test that two numbers survive: they are made to differ by 600
    /// first, so a restart that continued the wrong one is off by 600 rather
    /// than off by nothing. `local_sequence` used to be seeded from the
    /// correlation high-water, which is only ever right when a log has one
    /// record per publish and has never been rewritten.
    #[test]
    fn a_restart_continues_each_identity_space_on_its_own() {
        let path = scratch("spaces");
        let settings = WalSettings {
            path: path.clone(),
            queue: 8,
            // Large: this test is about the two numbers, and a rewrite in the
            // middle of it would be a second variable.
            compact_bytes: u64::MAX,
            retain_bytes: 0,
            limits: storage::WalLimits::default(),
        };
        let limits = BrokerLimits::default();

        // 100 records, carrying correlations 601..=700. The log allocates a
        // sequence per record and reads no bodies, so it ends at sequence 100
        // while the broker's own numbering ends at 700.
        {
            let mut wal = Wal::open(&path, settings.limits).unwrap().0;
            for index in 1..=100u64 {
                let correlation = index.saturating_add(600);
                wal.stage(&encode(
                    correlation,
                    &topic("sensors/one"),
                    &Bytes::from_static(b"x"),
                    DeliveryGuarantee::AtMostOnce,
                    false,
                ))
                .unwrap();
            }
            assert_eq!(wal.commit().unwrap().0.durable_sequence, 100);
        }

        let mut broker = Broker::new(limits);
        let restored = restore(&settings, Retention::Nothing, limits, &mut broker).unwrap();
        assert_eq!(restored.sequence, 100, "the WAL sequence");
        assert_eq!(restored.correlation, 700, "the broker's correlation");

        // And each one continues from its own value, not from the other's.
        let mut wal = Wal::open(&path, settings.limits).unwrap().0;
        wal.stage(&encode(
            restored.correlation.saturating_add(1),
            &topic("sensors/one"),
            &Bytes::from_static(b"y"),
            DeliveryGuarantee::AtMostOnce,
            false,
        ))
        .unwrap();
        assert_eq!(
            wal.commit().unwrap().0.durable_sequence,
            101,
            "the next sequence followed the correlation instead of the sequence"
        );
        drop(wal);

        let mut last = 0u64;
        storage::replay(&path, settings.limits, &mut |record| {
            if let Some(recorded) = decode(record) {
                last = recorded.correlation();
            }
        })
        .unwrap();
        assert_eq!(last, 701, "the next correlation did not continue at 701");

        let _ = std::fs::remove_file(&path);
    }

    /// Four at-most-once records nothing needs, committed, and the writer's
    /// side of the log around them.
    fn disposable(settings: &WalSettings) -> Committer {
        let (mut wal, _) = storage::Wal::open(&settings.path, settings.limits).unwrap();
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
    }

    /// One more at-most-once record, appended to the log as it now is.
    fn append(settings: &WalSettings, correlation: u64) -> Committer {
        let (mut wal, _) = storage::Wal::open(&settings.path, settings.limits).unwrap();
        wal.stage(&encode(
            correlation,
            &topic("chatter"),
            &Bytes::from_static(b"noise"),
            DeliveryGuarantee::AtMostOnce,
            false,
        ))
        .unwrap();
        wal.commit().unwrap();
        Committer::new(wal, settings.limits, true, 0)
    }

    fn marks(retention: Retention, replicated: u64) -> Marks {
        Marks {
            allocated: Arc::new(AtomicU64::new(0)),
            replicated: Arc::new(AtomicU64::new(replicated)),
            retention,
            shipments: None,
            local: Arc::new(AtomicU64::new(0)),
            fail: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn fresh() -> Rewrites {
        Rewrites {
            at: 0,
            addressable_from: 0,
            lost: false,
        }
    }

    fn addressable(path: &std::path::Path) -> Vec<u64> {
        let mut seen = Vec::new();
        storage::replay_addressable(
            path,
            storage::WalLimits::default(),
            &mut |_| true,
            &mut |sequence, _| seen.push(sequence),
        )
        .unwrap();
        seen
    }

    fn settings(path: &std::path::Path) -> WalSettings {
        WalSettings {
            path: path.to_path_buf(),
            queue: 8,
            // Anything at all is large enough to rewrite.
            compact_bytes: 1,
            retain_bytes: 0,
            limits: storage::WalLimits::default(),
        }
    }

    /// Under `quorum` a follower that has had only the first two records
    /// keeps the other two readable by the numbers it will ask for, and the
    /// rewrite still runs (D-20). It used to stop altogether.
    #[test]
    fn under_quorum_the_follower_position_is_the_floor() {
        let path = scratch("quorum-floor");
        let settings = settings(&path);
        let metrics = Metrics::default();
        let mut committer = disposable(&settings);
        shrink(
            &mut committer,
            &settings,
            BrokerLimits::default(),
            &metrics,
            &marks(Retention::Follower, 2),
            &mut fresh(),
        )
        .unwrap();
        drop(committer);

        assert_eq!(addressable(&path), vec![3, 4]);
        let seen = metrics.snapshot();
        assert_eq!(seen.wal_compactions, 1);
        assert_eq!(seen.wal_retention_floor, 3);
        assert!(seen.wal_rewrite_nanos > 0, "the rewrite was not timed");
        assert_eq!(seen.replication_followers_lost, 0);
        let _ = std::fs::remove_file(&path);
    }

    /// Without a follower nothing reads the log by sequence, and the
    /// rewrite keeps only what the broker needs — nothing, here.
    #[test]
    fn without_a_follower_nothing_stays_addressable() {
        let path = scratch("no-follower");
        let settings = settings(&path);
        let metrics = Metrics::default();
        let mut committer = disposable(&settings);
        shrink(
            &mut committer,
            &settings,
            BrokerLimits::default(),
            &metrics,
            &marks(Retention::Nothing, u64::MAX),
            &mut fresh(),
        )
        .unwrap();
        drop(committer);

        assert!(addressable(&path).is_empty());
        assert_eq!(
            storage::replay(&path, settings.limits, &mut |_| {})
                .unwrap()
                .records,
            0
        );
        assert_eq!(metrics.snapshot().wal_retention_floor, 5);
        let _ = std::fs::remove_file(&path);
    }

    /// An asynchronous follower behind a byte budget it no longer fits in is
    /// lost, and that is said once — not again at the next rewrite that
    /// finds it still behind (D-18).
    #[test]
    fn a_follower_the_budget_no_longer_covers_is_reported_once() {
        let path = scratch("lost");
        let settings = settings(&path);
        let metrics = Metrics::default();
        let limits = BrokerLimits::default();
        let mut committer = disposable(&settings);
        // Room for one record: the floor lands on 4, and a follower at 1
        // needs 2.
        let one = std::fs::metadata(&path).unwrap().len() - storage::HEADER_BYTES as u64;
        let behind = marks(Retention::Bytes(one / 4), 1);
        let mut rewrites = fresh();
        shrink(
            &mut committer,
            &settings,
            limits,
            &metrics,
            &behind,
            &mut rewrites,
        )
        .unwrap();
        assert_eq!(addressable(&path), vec![4]);
        assert_eq!(metrics.snapshot().replication_followers_lost, 1);

        // More is appended and rewritten again; the follower has not moved.
        // Still lost, and not news.
        drop(committer);
        let mut committer = append(&settings, 5);
        shrink(
            &mut committer,
            &settings,
            limits,
            &metrics,
            &behind,
            &mut rewrites,
        )
        .unwrap();
        drop(committer);
        assert_eq!(addressable(&path), vec![5]);
        assert_eq!(metrics.snapshot().replication_followers_lost, 1);
        let _ = std::fs::remove_file(&path);
    }

    /// The trigger counts what was appended since the last rewrite. A log
    /// that stays above the threshold after a rewrite — here, because the
    /// whole of it is retained — is not rewritten again at the next commit.
    #[test]
    fn a_log_that_stays_large_is_not_rewritten_after_every_commit() {
        let path = scratch("trigger");
        let mut settings = settings(&path);
        let metrics = Metrics::default();
        let limits = BrokerLimits::default();
        let mut committer = disposable(&settings);
        let four = committer.log_bytes();
        settings.compact_bytes = four;
        let keep_all = marks(Retention::Follower, 0);
        let mut rewrites = fresh();
        shrink(
            &mut committer,
            &settings,
            limits,
            &metrics,
            &keep_all,
            &mut rewrites,
        )
        .unwrap();
        assert_eq!(metrics.snapshot().wal_compactions, 1);
        assert!(committer.log_bytes() >= settings.compact_bytes);

        // One more record: the file is over the threshold, the appended
        // bytes are not.
        drop(committer);
        let mut committer = append(&settings, 5);
        shrink(
            &mut committer,
            &settings,
            limits,
            &metrics,
            &keep_all,
            &mut rewrites,
        )
        .unwrap();
        assert_eq!(metrics.snapshot().wal_compactions, 1);
        drop(committer);
        let _ = std::fs::remove_file(&path);
    }
}
