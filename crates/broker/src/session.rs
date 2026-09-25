//! One connected session and what it owes its subscriber.

use std::collections::VecDeque;

use auth::Permissions;
use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee, DeliveryId, SessionId, Topic, TopicFilter};

/// At-least-once deliveries that have to wait for room in the inflight
/// window.
///
/// Deliberately one concrete type with a narrow surface — push, pop, len.
/// When this becomes a persistent queue reading from the WAL, the change is
/// this file, not the broker.
#[derive(Debug)]
pub struct Backlog {
    waiting: VecDeque<Waiting>,
    limit: usize,
    /// Payload bytes waiting here, and the ceiling on them. A message count
    /// alone is not a memory bound: a thousand messages is a megabyte or a
    /// gigabyte depending on what was published (CLAUDE.md §7).
    bytes: usize,
    byte_limit: usize,
}

#[derive(Debug, Clone)]
pub struct Waiting {
    pub topic: Topic,
    pub payload: Bytes,
    /// Which log record this came from, `0` when it came from no log.
    pub correlation: u64,
}

impl Backlog {
    pub fn new(limit: usize, byte_limit: usize) -> Self {
        Self {
            waiting: VecDeque::new(),
            limit,
            bytes: 0,
            byte_limit,
        }
    }

    /// `false` means the session is too far behind to keep it — either in
    /// messages or in bytes, whichever it reaches first.
    #[must_use]
    pub fn push(&mut self, topic: Topic, payload: Bytes, correlation: u64) -> bool {
        let weight = payload.len();
        if self.waiting.len() >= self.limit || self.bytes.saturating_add(weight) > self.byte_limit {
            return false;
        }
        self.bytes = self.bytes.saturating_add(weight);
        self.waiting.push_back(Waiting {
            topic,
            payload,
            correlation,
        });
        true
    }

    pub fn pop(&mut self) -> Option<Waiting> {
        let waiting = self.waiting.pop_front()?;
        self.bytes = self.bytes.saturating_sub(waiting.payload.len());
        Some(waiting)
    }

    /// Payload bytes waiting here.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// How far behind this session is; the runtime reports it as a gauge.
    pub fn len(&self) -> usize {
        self.waiting.len()
    }

    pub fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }

    /// The oldest log record still waiting here, ignoring messages that were
    /// never logged.
    pub fn oldest_correlation(&self) -> Option<u64> {
        self.waiting
            .iter()
            .filter(|waiting| waiting.correlation != 0)
            .map(|waiting| waiting.correlation)
            .min()
    }
}

#[derive(Debug)]
pub struct Session {
    pub id: SessionId,
    pub client: ClientId,
    /// What this session was authorised to do when it connected.
    pub permissions: Permissions,
    /// What this session subscribed to and what was granted, so closing it
    /// can undo all of it in one call (CLAUDE.md §7) and a restart can put
    /// all of it back.
    pub filters: Vec<(TopicFilter, DeliveryGuarantee)>,
    /// At-least-once deliveries handed out and not yet acknowledged.
    /// Kept whole, so a session that comes back gets them again.
    pub inflight: Vec<Inflight>,
    pub backlog: Backlog,
    /// Whether a connection is attached right now. A session that asked to be
    /// resumed stays here without one, collecting what it is owed.
    pub online: bool,
    /// Whether to keep this session when the connection goes away.
    pub resume: bool,
    /// Published if this connection ends any way but a DISCONNECT. Taken
    /// when it is published or when the client says goodbye, so it can
    /// never be published twice (3.1.1 §3.1.2.5).
    pub will: Option<protocol::Will>,
    /// Deduplication of overlapping subscriptions without allocating per
    /// publish: the publish stamps its generation, a second match on the same
    /// generation only raises the guarantee (CLAUDE.md §6).
    pub seen: u64,
    pub best: DeliveryGuarantee,
    /// Non-zero only for a session read back from the log and not yet
    /// resumed: everything up to this record was already accounted for
    /// before the restart, so replay starts after it.
    pub owes_from: u64,
}

/// A message handed to a session and not yet acknowledged. The payload is
/// kept because an unacknowledged message has to be sent again when the
/// session comes back (3.1.1 §4.4).
#[derive(Debug, Clone)]
pub struct Inflight {
    pub id: DeliveryId,
    pub topic: Topic,
    pub payload: Bytes,
    /// Which log record this came from, `0` when it came from no log.
    pub correlation: u64,
}

impl Session {
    pub fn new(
        id: SessionId,
        client: ClientId,
        permissions: Permissions,
        resume: bool,
        backlog: usize,
        backlog_bytes: usize,
    ) -> Self {
        Self {
            id,
            client,
            permissions,
            filters: Vec::new(),
            inflight: Vec::new(),
            backlog: Backlog::new(backlog, backlog_bytes),
            online: true,
            resume,
            will: None,
            seen: 0,
            best: DeliveryGuarantee::AtMostOnce,
            owes_from: 0,
        }
    }

    /// The last log record this session has finished with, or `fallback`
    /// when it owes nothing.
    ///
    /// This one number is the whole of what has to be written down for a
    /// session: replaying the log from after it rebuilds everything the
    /// session was owed, so no payload is ever written twice
    /// (docs/storage.md §103).
    pub fn watermark(&self, fallback: u64) -> u64 {
        let inflight = self
            .inflight
            .iter()
            .filter(|held| held.correlation != 0)
            .map(|held| held.correlation)
            .min();
        let oldest = match (inflight, self.backlog.oldest_correlation()) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) | (None, Some(a)) => a,
            (None, None) => return fallback,
        };
        // The oldest record it still owes has to come back, so the mark sits
        // one before it.
        oldest.saturating_sub(1)
    }

    pub fn forget(&mut self, delivery: DeliveryId) -> bool {
        match self.inflight.iter().position(|held| held.id == delivery) {
            Some(index) => {
                self.inflight.swap_remove(index);
                true
            }
            None => false,
        }
    }
}
