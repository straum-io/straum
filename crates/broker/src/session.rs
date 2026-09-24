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
}

#[derive(Debug, Clone)]
pub struct Waiting {
    pub topic: Topic,
    pub payload: Bytes,
}

impl Backlog {
    pub fn new(limit: usize) -> Self {
        Self {
            waiting: VecDeque::new(),
            limit,
        }
    }

    /// `false` means the session is too far behind to keep it.
    #[must_use]
    pub fn push(&mut self, topic: Topic, payload: Bytes) -> bool {
        if self.waiting.len() >= self.limit {
            return false;
        }
        self.waiting.push_back(Waiting { topic, payload });
        true
    }

    pub fn pop(&mut self) -> Option<Waiting> {
        self.waiting.pop_front()
    }

    /// How far behind this session is; the runtime reports it as a gauge.
    pub fn len(&self) -> usize {
        self.waiting.len()
    }

    pub fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }
}

#[derive(Debug)]
pub struct Session {
    pub id: SessionId,
    pub client: ClientId,
    /// What this session was authorised to do when it connected.
    pub permissions: Permissions,
    /// What this session subscribed to, so closing it can undo all of it in
    /// one call (CLAUDE.md §7).
    pub filters: Vec<TopicFilter>,
    /// At-least-once deliveries handed out and not yet acknowledged.
    pub inflight: Vec<DeliveryId>,
    pub backlog: Backlog,
    /// Deduplication of overlapping subscriptions without allocating per
    /// publish: the publish stamps its generation, a second match on the same
    /// generation only raises the guarantee (CLAUDE.md §6).
    pub seen: u64,
    pub best: DeliveryGuarantee,
}

impl Session {
    pub fn new(id: SessionId, client: ClientId, permissions: Permissions, backlog: usize) -> Self {
        Self {
            id,
            client,
            permissions,
            filters: Vec::new(),
            inflight: Vec::new(),
            backlog: Backlog::new(backlog),
            seen: 0,
            best: DeliveryGuarantee::AtMostOnce,
        }
    }

    pub fn forget(&mut self, delivery: DeliveryId) -> bool {
        match self.inflight.iter().position(|held| *held == delivery) {
            Some(index) => {
                self.inflight.swap_remove(index);
                true
            }
            None => false,
        }
    }
}
