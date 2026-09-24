//! The broker: sessions, subscriptions, routing, inflight tracking, overflow
//! policy.
//!
//! A synchronous state machine. It owns no tasks, no channels and no clock:
//! commands come in, effects go into the caller's [`Outbox`]. Everything that
//! blocks — sockets, timers, `sync_data` — belongs to the runtime.

mod index;
pub use session::Backlog;
mod session;

use std::collections::HashMap;
use std::collections::VecDeque;

use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee, DeliveryId, SessionId, Topic, TopicFilter};
use protocol::{
    CloseReason, Command, Connect, Delivery, DurableToken, Outbound, Outbox, Publish, RequestId,
    Response, SubscribeResult,
};

use index::{Index, Subscriber};
use session::Session;

/// Limits owned by this crate. Defaults are the ones in
/// `docs/configuration.md`; changing one is a behavior change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerLimits {
    pub max_subscriptions_per_session: usize,
    /// Unacknowledged at-least-once deliveries per session.
    pub max_inflight: usize,
    /// At-least-once deliveries waiting for room in the inflight window.
    pub session_backlog_messages: usize,
    /// Topics that may hold a retained message. A map keyed by remote
    /// data is only bounded if somebody bounds it (CLAUDE.md §8).
    pub max_retained_messages: usize,
    /// Payload bytes held across all retained messages.
    pub max_retained_bytes: usize,
}

impl Default for BrokerLimits {
    fn default() -> Self {
        Self {
            max_subscriptions_per_session: 1024,
            max_inflight: 32,
            session_backlog_messages: 1024,
            max_retained_messages: 10_000,
            max_retained_bytes: 67_108_864,
        }
    }
}

/// Plain counters; the runtime publishes them (CLAUDE.md §14).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub published_messages: u64,
    pub delivered_messages: u64,
    pub dropped_messages: u64,
    pub slow_consumer_closes: u64,
    /// Publishes and subscriptions refused by the access rules.
    pub unauthorized_publishes: u64,
    pub unauthorized_subscribes: u64,
    /// Retained messages the store refused to take, because taking them
    /// would have meant growing without a bound.
    pub retained_rejected: u64,
    pub retained_messages: u64,
    pub active_sessions: u64,
}

/// The last message published to a topic with the retain flag.
#[derive(Debug, Clone)]
struct Retained {
    payload: Bytes,
    guarantee: DeliveryGuarantee,
}

pub struct Broker {
    limits: BrokerLimits,
    /// Dense slots so routing never hashes; the maps are only touched when a
    /// session starts or ends.
    sessions: Vec<Option<Session>>,
    free: Vec<usize>,
    by_id: HashMap<SessionId, usize>,
    by_client: HashMap<ClientId, usize>,
    index: Index,
    /// Reused across publishes: routing allocates nothing.
    matched: Vec<usize>,
    generation: u64,
    next_delivery: u64,
    next_client: u64,
    /// Replies withheld until the WAL says their record is durable
    /// (docs/storage.md §79). Tokens are monotonic, so this stays in order.
    pending_durable: VecDeque<(DurableToken, SessionId, RequestId)>,
    /// The highest token the WAL has reported. A publish whose record was
    /// already durable before the broker saw the command is acknowledged
    /// straight away, so the two paths may arrive in any order.
    durable_high_water: u64,
    /// The last retained message per topic, and what they weigh.
    retained: HashMap<Topic, Retained>,
    retained_bytes: usize,
    /// Reused when a new subscription replays what is stored.
    replay: Vec<(Topic, Bytes, DeliveryGuarantee)>,
    stats: Stats,
}

impl Broker {
    pub fn new(limits: BrokerLimits) -> Self {
        Self {
            limits,
            sessions: Vec::new(),
            free: Vec::new(),
            by_id: HashMap::new(),
            by_client: HashMap::new(),
            index: Index::new(),
            matched: Vec::new(),
            generation: 0,
            next_delivery: 0,
            next_client: 0,
            pending_durable: VecDeque::new(),
            durable_high_water: 0,
            retained: HashMap::new(),
            retained_bytes: 0,
            replay: Vec::new(),
            stats: Stats::default(),
        }
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// Handles one command from one session.
    pub fn handle(&mut self, session: SessionId, command: Command, out: &mut Outbox) {
        self.dispatch(session, command, None, out);
    }

    /// Like [`Broker::handle`], for a publish the runtime is persisting: the
    /// acknowledgement waits for [`Broker::on_durable`] (docs/storage.md §79).
    pub fn handle_durable(
        &mut self,
        session: SessionId,
        command: Command,
        durable: DurableToken,
        out: &mut Outbox,
    ) {
        self.dispatch(session, command, Some(durable), out);
    }

    /// Everything submitted up to and including `token` is durable.
    pub fn on_durable(&mut self, token: DurableToken, out: &mut Outbox) {
        self.durable_high_water = self.durable_high_water.max(token.0);
        while let Some((pending, session, request)) = self.pending_durable.front().copied() {
            if pending > token {
                break;
            }
            self.pending_durable.pop_front();
            // A session that left in the meantime has nothing to hear.
            if self.by_id.contains_key(&session) {
                out.push(session, Outbound::Reply(Response::PublishAccepted(request)));
            }
        }
    }

    /// The runtime could not enqueue something for a session. What happens now
    /// is the broker's decision (CLAUDE.md §8).
    pub fn on_overflow(&mut self, session: SessionId, rejected: Outbound, out: &mut Outbox) {
        match rejected {
            // At-most-once is allowed to be lost; that is what it means.
            Outbound::Deliver(delivery) if delivery.id.is_none() => {
                self.stats.dropped_messages = self.stats.dropped_messages.saturating_add(1);
            }
            Outbound::Deliver(delivery) => {
                if let Some(id) = delivery.id {
                    if let Some(slot) = self.slot_of(session) {
                        if let Some(held) = self.session_mut(slot) {
                            held.forget(id);
                        }
                    }
                }
                self.close(session, Some(CloseReason::SlowConsumer), out);
            }
            Outbound::Reply(_) => self.close(session, Some(CloseReason::SlowConsumer), out),
            // Already on its way out.
            Outbound::Close(_) => {}
        }
    }

    fn dispatch(
        &mut self,
        session: SessionId,
        command: Command,
        durable: Option<DurableToken>,
        out: &mut Outbox,
    ) {
        match command {
            Command::Connect(connect) => self.connect(session, connect, out),
            Command::Publish { ack, publish } => self.publish(session, ack, publish, durable, out),
            Command::Subscribe { request, filters } => {
                self.subscribe(session, request, filters, out);
            }
            Command::Unsubscribe { request, filters } => {
                self.unsubscribe(session, request, filters, out);
            }
            Command::Ack(delivery) => self.acknowledge(session, delivery, out),
            Command::Disconnect { .. } => self.close(session, None, out),
        }
    }

    fn connect(&mut self, session: SessionId, connect: Connect, out: &mut Outbox) {
        let client = if connect.client_id.is_empty() {
            self.assign_client_id()
        } else {
            connect.client_id
        };

        // One identity, one connection: the older one loses (CLAUDE.md §6).
        if let Some(slot) = self.by_client.get(&client).copied() {
            if let Some(previous) = self.session_mut(slot).map(|held| held.id) {
                self.close(previous, Some(CloseReason::SessionTakenOver), out);
            }
        }

        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                self.sessions.push(None);
                self.sessions.len().saturating_sub(1)
            }
        };
        if let Some(entry) = self.sessions.get_mut(slot) {
            *entry = Some(Session::new(
                session,
                client.clone(),
                connect.permissions.clone(),
                self.limits.session_backlog_messages,
            ));
        }
        self.by_id.insert(session, slot);
        self.by_client.insert(client, slot);
        self.stats.active_sessions = self.stats.active_sessions.saturating_add(1);

        // No persistent sessions, so a session is never present (§6).
        out.push(
            session,
            Outbound::Reply(Response::Connected {
                session_present: false,
            }),
        );
    }

    fn publish(
        &mut self,
        session: SessionId,
        ack: Option<RequestId>,
        publish: Publish,
        durable: Option<DurableToken>,
        out: &mut Outbox,
    ) {
        self.stats.published_messages = self.stats.published_messages.saturating_add(1);

        // Authorisation is a filter match, not a password check: cheap
        // enough for the broker loop (docs/configuration.md).
        let permitted = self
            .slot_of(session)
            .and_then(|slot| self.sessions.get(slot).and_then(Option::as_ref))
            .is_some_and(|held| held.permissions.may_publish(&publish.topic));

        if permitted {
            if publish.retain {
                self.retain(&publish);
            }
            self.route(&publish, out);
        } else {
            // 3.1.1 has no way to tell a publisher it was refused, and
            // withholding the acknowledgement would only stall it (§6).
            self.stats.unauthorized_publishes = self.stats.unauthorized_publishes.saturating_add(1);
        }

        // After routing completed for every matched session (§5).
        if let Some(request) = ack {
            let durable = durable.filter(|token| token.0 > self.durable_high_water);
            match durable {
                Some(token) => self.pending_durable.push_back((token, session, request)),
                None => out.push(session, Outbound::Reply(Response::PublishAccepted(request))),
            }
        }
    }

    fn route(&mut self, publish: &Publish, out: &mut Outbox) {
        self.generation = self.generation.saturating_add(1);
        let generation = self.generation;

        // Split borrows: the index is read while sessions are stamped.
        let Self {
            index,
            sessions,
            matched,
            ..
        } = self;
        matched.clear();

        index.matches(&publish.topic, &mut |subscriber: Subscriber| {
            let Some(Some(session)) = sessions.get_mut(subscriber.slot) else {
                return;
            };
            if session.seen == generation {
                // Overlapping filters deliver once, at the strongest grant.
                session.best = session.best.max(subscriber.guarantee);
                return;
            }
            session.seen = generation;
            session.best = subscriber.guarantee;
            matched.push(subscriber.slot);
        });

        for position in 0..self.matched.len() {
            let Some(slot) = self.matched.get(position).copied() else {
                break;
            };
            // Delivered guarantee is the weaker of asked-for and granted (§6).
            let guarantee = match self.sessions.get(slot).and_then(Option::as_ref) {
                Some(session) => publish.guarantee.min(session.best),
                None => continue,
            };
            self.deliver(
                slot,
                &publish.topic,
                &publish.payload,
                guarantee,
                false,
                out,
            );
        }
    }

    /// Hands one message to one session, or parks it, or gives up on the
    /// session. `retain` marks a stored message being replayed to a new
    /// subscriber rather than live traffic.
    fn deliver(
        &mut self,
        slot: usize,
        topic: &Topic,
        payload: &Bytes,
        guarantee: DeliveryGuarantee,
        retain: bool,
        out: &mut Outbox,
    ) {
        let limits = self.limits;
        let delivery_id = self.next_delivery;

        let Some(session) = self.session_mut(slot) else {
            return;
        };
        let id = session.id;

        let (effect, used_id, dropped) = match guarantee {
            DeliveryGuarantee::AtMostOnce => (
                Some(Delivery {
                    id: None,
                    topic: topic.clone(),
                    payload: payload.clone(),
                    retain,
                }),
                false,
                false,
            ),
            DeliveryGuarantee::AtLeastOnce if session.inflight.len() < limits.max_inflight => {
                let delivery = DeliveryId(delivery_id);
                session.inflight.push(delivery);
                (
                    Some(Delivery {
                        id: Some(delivery),
                        topic: topic.clone(),
                        payload: payload.clone(),
                        retain,
                    }),
                    true,
                    false,
                )
            }
            DeliveryGuarantee::AtLeastOnce => {
                // The window is full: wait in the backlog, or give up on a
                // session that is too far behind to catch up.
                let accepted = session.backlog.push(topic.clone(), payload.clone());
                (None, false, !accepted)
            }
        };

        if used_id {
            self.next_delivery = self.next_delivery.saturating_add(1);
        }
        if let Some(delivery) = effect {
            self.stats.delivered_messages = self.stats.delivered_messages.saturating_add(1);
            out.push(id, Outbound::Deliver(delivery));
        }
        if dropped {
            self.close(id, Some(CloseReason::SlowConsumer), out);
        }
    }

    /// Keeps the last retained message for a topic, or forgets it.
    ///
    /// An empty payload is how MQTT deletes one (3.1.1 §3.3.1.3). The store
    /// is bounded in both directions: a client that publishes retained
    /// messages to endless topics fills it and then gets refused, rather
    /// than growing it until the broker dies.
    fn retain(&mut self, publish: &Publish) {
        if publish.payload.is_empty() {
            if let Some(gone) = self.retained.remove(&publish.topic) {
                self.retained_bytes = self.retained_bytes.saturating_sub(gone.payload.len());
            }
            self.stats.retained_messages = self.retained.len() as u64;
            return;
        }

        let previous = self
            .retained
            .get(&publish.topic)
            .map(|held| held.payload.len())
            .unwrap_or(0);
        let would_hold = self
            .retained_bytes
            .saturating_sub(previous)
            .saturating_add(publish.payload.len());
        let new_topic = previous == 0 && !self.retained.contains_key(&publish.topic);

        if would_hold > self.limits.max_retained_bytes
            || (new_topic && self.retained.len() >= self.limits.max_retained_messages)
        {
            self.stats.retained_rejected = self.stats.retained_rejected.saturating_add(1);
            return;
        }

        self.retained.insert(
            publish.topic.clone(),
            Retained {
                payload: publish.payload.clone(),
                guarantee: publish.guarantee,
            },
        );
        self.retained_bytes = would_hold;
        self.stats.retained_messages = self.retained.len() as u64;
    }

    /// Hands a new subscriber whatever is stored for its filter.
    fn replay_retained(
        &mut self,
        slot: usize,
        filter: &TopicFilter,
        granted: DeliveryGuarantee,
        out: &mut Outbox,
    ) {
        // Collected first: the delivery path needs the sessions mutably.
        let mut replay = std::mem::take(&mut self.replay);
        replay.clear();
        replay.extend(
            self.retained
                .iter()
                .filter(|(topic, _)| filter.matches(topic))
                .map(|(topic, held)| (topic.clone(), held.payload.clone(), held.guarantee)),
        );

        for (topic, payload, stored) in replay.drain(..) {
            self.deliver(slot, &topic, &payload, stored.min(granted), true, out);
        }
        self.replay = replay;
    }

    fn subscribe(
        &mut self,
        session: SessionId,
        request: RequestId,
        filters: Vec<(TopicFilter, DeliveryGuarantee)>,
        out: &mut Outbox,
    ) {
        let Some(slot) = self.slot_of(session) else {
            return;
        };
        let limit = self.limits.max_subscriptions_per_session;
        let mut granted = Vec::with_capacity(filters.len());

        for (filter, guarantee) in filters {
            let Some(held) = self.sessions.get_mut(slot).and_then(Option::as_mut) else {
                return;
            };
            if !held.permissions.may_subscribe(&filter) {
                // Per filter, which is exactly what SUBACK can express.
                granted.push(SubscribeResult::Failure);
                self.stats.unauthorized_subscribes =
                    self.stats.unauthorized_subscribes.saturating_add(1);
                continue;
            }
            let known = held.filters.contains(&filter);
            if !known && held.filters.len() >= limit {
                granted.push(SubscribeResult::Failure);
                continue;
            }
            if !known {
                held.filters.push(filter.clone());
            }
            self.index
                .subscribe(&filter, Subscriber { slot, guarantee });
            granted.push(SubscribeResult::Granted(guarantee));

            // A new subscriber hears the stored state of its topics before it
            // hears anything live (3.1.1 §3.8.4).
            self.replay_retained(slot, &filter, guarantee, out);
        }

        out.push(
            session,
            Outbound::Reply(Response::Subscribed { request, granted }),
        );
    }

    fn unsubscribe(
        &mut self,
        session: SessionId,
        request: RequestId,
        filters: Vec<TopicFilter>,
        out: &mut Outbox,
    ) {
        let Some(slot) = self.slot_of(session) else {
            return;
        };
        for filter in filters {
            self.index.unsubscribe(&filter, slot);
            if let Some(held) = self.sessions.get_mut(slot).and_then(Option::as_mut) {
                held.filters.retain(|existing| *existing != filter);
            }
        }
        out.push(session, Outbound::Reply(Response::Unsubscribed(request)));
    }

    fn acknowledge(&mut self, session: SessionId, delivery: DeliveryId, out: &mut Outbox) {
        let Some(slot) = self.slot_of(session) else {
            return;
        };
        let limits = self.limits;
        let mut next_delivery = self.next_delivery;
        let mut delivered = 0u64;

        let Some(held) = self.sessions.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        if !held.forget(delivery) {
            return;
        }

        // The window has room: let the backlog through in order.
        while held.inflight.len() < limits.max_inflight {
            let Some(waiting) = held.backlog.pop() else {
                break;
            };
            let id = DeliveryId(next_delivery);
            next_delivery = next_delivery.saturating_add(1);
            held.inflight.push(id);
            delivered = delivered.saturating_add(1);
            out.push(
                session,
                Outbound::Deliver(Delivery {
                    id: Some(id),
                    topic: waiting.topic,
                    payload: waiting.payload,
                    // Whatever this was when it arrived, it is live traffic
                    // for this session now.
                    retain: false,
                }),
            );
        }

        self.next_delivery = next_delivery;
        self.stats.delivered_messages = self.stats.delivered_messages.saturating_add(delivered);
    }

    /// Removes a session and everything it owned, in this call (§7).
    fn close(&mut self, session: SessionId, reason: Option<CloseReason>, out: &mut Outbox) {
        let Some(slot) = self.by_id.remove(&session) else {
            return;
        };
        let Some(entry) = self.sessions.get_mut(slot) else {
            return;
        };
        let Some(held) = entry.take() else {
            return;
        };

        for filter in &held.filters {
            self.index.unsubscribe(filter, slot);
        }
        self.by_client.remove(&held.client);
        self.free.push(slot);
        self.stats.active_sessions = self.stats.active_sessions.saturating_sub(1);

        if let Some(reason) = reason {
            if matches!(reason, CloseReason::SlowConsumer) {
                self.stats.slow_consumer_closes = self.stats.slow_consumer_closes.saturating_add(1);
            }
            out.push(session, Outbound::Close(reason));
        }
    }

    fn slot_of(&self, session: SessionId) -> Option<usize> {
        self.by_id.get(&session).copied()
    }

    fn session_mut(&mut self, slot: usize) -> Option<&mut Session> {
        self.sessions.get_mut(slot).and_then(Option::as_mut)
    }

    #[expect(
        clippy::expect_used,
        reason = "the generated identifier is ASCII by construction"
    )]
    fn assign_client_id(&mut self) -> ClientId {
        self.next_client = self.next_client.saturating_add(1);
        let text = format!("straum-{}", self.next_client);
        ClientId::new(Bytes::from(text)).expect("generated client ids are valid UTF-8")
    }
}
