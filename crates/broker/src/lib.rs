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

use auth::Permissions;
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
    /// Payload bytes those deliveries may hold. A message count is not a
    /// memory bound on its own — the messages can be a megabyte each (§7).
    pub session_backlog_bytes: usize,
    /// Payload bytes every session backlog may hold together. A per-session
    /// limit times the session count is a product, not a bound (§8), so the
    /// backlogs get a budget the way the writer queues do.
    pub total_backlog_bytes: usize,
    /// Sessions kept for a client that asked to be resumed but is not
    /// connected. A map keyed by client identity needs a bound (§8).
    pub max_offline_sessions: usize,
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
            max_inflight: 256,
            session_backlog_messages: 16_384,
            session_backlog_bytes: 8_388_608,
            total_backlog_bytes: 1_073_741_824,
            max_offline_sessions: 4096,
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
    /// Sessions kept for clients that are not connected, and sessions
    /// that could not be kept because the limit was reached.
    pub offline_sessions: u64,
    pub offline_refused: u64,
    /// At-least-once messages a session backlog had no room for. Each one
    /// is a message the session will never receive.
    pub backlog_refused: u64,
    /// At-most-once messages dropped because their session was offline.
    pub offline_dropped: u64,
    /// Payload bytes waiting across every session backlog.
    pub backlog_bytes: u64,
    pub active_sessions: u64,
}

/// What has to be written down so a session survives a restart.
///
/// Not the messages it is owed: those are already in the log as publishes,
/// and `owes_from` says where to start reading them again
/// (docs/storage.md §103).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub client: ClientId,
    pub filters: Vec<(TopicFilter, DeliveryGuarantee)>,
    /// The last log record this session had finished with.
    pub owes_from: u64,
}

/// A change to what is kept for a client across restarts. The runtime writes
/// these to the log; the broker never does I/O itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    Kept(SessionRecord),
    /// This identity keeps nothing any more.
    Gone(ClientId),
}

/// One message on its way to one session, and the log record it came from.
/// `correlation` is `0` for anything that was never logged.
struct Outgoing<'a> {
    topic: &'a Topic,
    payload: &'a Bytes,
    correlation: u64,
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
    /// Payload bytes waiting in every session backlog. A plain counter, not
    /// an atomic: the broker is one task and owns all of them.
    backlog_bytes: usize,
    /// Reused when a new subscription replays what is stored.
    replay: Vec<(Topic, Bytes, DeliveryGuarantee)>,
    /// What the runtime still has to write to the log. Drained every batch;
    /// only a session that asked to be resumed ever puts anything here.
    session_events: Vec<SessionEvent>,
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
            backlog_bytes: 0,
            replay: Vec::new(),
            session_events: Vec::new(),
            stats: Stats::default(),
        }
    }

    /// Hands over what has to reach the log, and clears it.
    pub fn drain_session_events(&mut self, into: &mut Vec<SessionEvent>) {
        into.append(&mut self.session_events);
    }

    pub fn stats(&self) -> Stats {
        Stats {
            backlog_bytes: self.backlog_bytes as u64,
            ..self.stats
        }
    }

    /// Puts back a retained message that survived a restart.
    ///
    /// Only the runtime calls this, once, before the loop starts: the log is
    /// the recovery boundary, and this is how what it held becomes state
    /// again (docs/storage.md §81).
    pub fn restore_retained(&mut self, topic: Topic, payload: Bytes, guarantee: DeliveryGuarantee) {
        self.retain(&Publish {
            topic,
            payload,
            guarantee,
            retain: true,
        });
    }

    /// Puts back a session that survived a restart, offline and with its
    /// subscriptions in place.
    ///
    /// Only the runtime calls this, before the loop starts. Nothing is
    /// delivered here: the messages this session is owed come back from the
    /// log afterwards, through [`Broker::restore_publish`].
    ///
    /// `false` when the offline limit is already full — the same bound that
    /// applies while running applies on the way back in (CLAUDE.md §8).
    pub fn restore_session(&mut self, record: SessionRecord) -> bool {
        if self.stats.offline_sessions >= self.limits.max_offline_sessions as u64
            || self.by_client.contains_key(&record.client)
        {
            return false;
        }
        let slot = self.free_slot();
        // Nothing is allowed until the next CONNECT re-authorises it: the
        // rules may have changed while this session was gone.
        let mut held = Session::new(
            SessionId(0),
            record.client.clone(),
            Permissions::from_rules(Vec::new()),
            true,
            self.limits.session_backlog_messages,
            self.limits.session_backlog_bytes,
        );
        held.online = false;
        held.owes_from = record.owes_from;
        for (filter, guarantee) in record.filters {
            if held.filters.len() >= self.limits.max_subscriptions_per_session {
                break;
            }
            self.index
                .subscribe(&filter, Subscriber { slot, guarantee });
            held.filters.push((filter, guarantee));
        }
        if let Some(entry) = self.sessions.get_mut(slot) {
            *entry = Some(held);
        }
        self.by_client.insert(record.client, slot);
        self.stats.offline_sessions = self.stats.offline_sessions.saturating_add(1);
        true
    }

    /// Routes one logged publish back to the restored sessions that had not
    /// finished with it.
    ///
    /// Only the runtime calls this, after [`Broker::restore_session`] and
    /// before the loop starts. No session is connected yet, so this only
    /// fills backlogs.
    pub fn restore_publish(&mut self, correlation: u64, publish: &Publish, out: &mut Outbox) {
        self.route(publish, correlation, out);
        out.take().for_each(drop);
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

    fn connect(&mut self, session: SessionId, mut connect: Connect, out: &mut Outbox) {
        let client = if connect.client_id.is_empty() {
            // An assigned identity cannot be asked for again: keeping the
            // session would park it under a name only the broker knows, until
            // the offline limit filled up with ghosts.
            connect.resume = false;
            self.assign_client_id()
        } else {
            connect.client_id
        };

        // What this identity left behind, if anything.
        let existing = self.by_client.get(&client).copied();
        let resumable = existing.is_some_and(|slot| {
            self.sessions
                .get(slot)
                .and_then(Option::as_ref)
                .is_some_and(|held| held.resume)
        });

        if let Some(slot) = existing {
            match (connect.resume, resumable) {
                // One identity, one connection: whoever was here loses the
                // socket either way (CLAUDE.md §6).
                (true, true) => {
                    if let Some(previous) = self.session_mut(slot).map(|held| held.id) {
                        if self.by_id.contains_key(&previous) {
                            out.push(previous, Outbound::Close(CloseReason::SessionTakenOver));
                            self.by_id.remove(&previous);
                        }
                    }
                    self.resume(slot, session, client, connect.permissions, out);
                    return;
                }
                // A clean start throws away whatever was kept, and a session
                // that was never meant to be resumed cannot be. Either way
                // this one goes, parked or not — `close` would park it again.
                _ => {
                    let online = self
                        .sessions
                        .get(slot)
                        .and_then(Option::as_ref)
                        .map(|held| (held.id, held.online));
                    if let Some((previous, true)) = online {
                        out.push(previous, Outbound::Close(CloseReason::SessionTakenOver));
                        self.by_id.remove(&previous);
                        self.stats.active_sessions = self.stats.active_sessions.saturating_sub(1);
                    }
                    self.discard(slot);
                }
            }
        }

        let slot = self.free_slot();
        if let Some(entry) = self.sessions.get_mut(slot) {
            *entry = Some(Session::new(
                session,
                client.clone(),
                connect.permissions.clone(),
                connect.resume,
                self.limits.session_backlog_messages,
                self.limits.session_backlog_bytes,
            ));
        }
        self.by_id.insert(session, slot);
        self.by_client.insert(client, slot);
        self.stats.active_sessions = self.stats.active_sessions.saturating_add(1);

        out.push(
            session,
            Outbound::Reply(Response::Connected {
                session_present: false,
            }),
        );
    }

    /// Hands a session that was kept back to a new connection: its
    /// subscriptions never left the index, so what it was owed is waiting.
    fn resume(
        &mut self,
        slot: usize,
        session: SessionId,
        client: ClientId,
        permissions: Permissions,
        out: &mut Outbox,
    ) {
        let was_offline;
        {
            let Some(held) = self.sessions.get_mut(slot).and_then(Option::as_mut) else {
                return;
            };
            was_offline = !held.online;
            held.id = session;
            held.online = true;
            held.resume = true;
            // Everything the log had to replay has been replayed by now.
            held.owes_from = 0;
            // Re-authorised on every connect: the rules may have changed
            // since this session was last here.
            held.permissions = permissions;
        }

        // And so are its subscriptions. A filter the rules no longer allow
        // goes, rather than keeping a grant nobody would be given today.
        let mut revoked = Vec::new();
        if let Some(held) = self.sessions.get_mut(slot).and_then(Option::as_mut) {
            // Split borrows: the rules are read while the list is rewritten.
            let Session {
                filters,
                permissions,
                ..
            } = held;
            filters.retain(|(filter, _)| {
                let allowed = permissions.may_subscribe(filter);
                if !allowed {
                    revoked.push(filter.clone());
                }
                allowed
            });
        }
        for filter in &revoked {
            self.index.unsubscribe(filter, slot);
        }

        self.by_id.insert(session, slot);
        self.by_client.insert(client, slot);
        if was_offline {
            self.stats.offline_sessions = self.stats.offline_sessions.saturating_sub(1);
            self.stats.active_sessions = self.stats.active_sessions.saturating_add(1);
        }

        out.push(
            session,
            Outbound::Reply(Response::Connected {
                session_present: true,
            }),
        );

        self.redeliver(slot, out);
    }

    /// Sends again what the session was given and never acknowledged, then
    /// lets the backlog through.
    fn redeliver(&mut self, slot: usize, out: &mut Outbox) {
        let Some(held) = self.sessions.get(slot).and_then(Option::as_ref) else {
            return;
        };
        let session = held.id;
        let pending: Vec<Delivery> = held
            .inflight
            .iter()
            .map(|held| Delivery {
                id: Some(held.id),
                topic: held.topic.clone(),
                payload: held.payload.clone(),
                // The client may already have it; that is what DUP says.
                duplicate: true,
                retain: false,
            })
            .collect();

        for delivery in pending {
            self.stats.delivered_messages = self.stats.delivered_messages.saturating_add(1);
            out.push(session, Outbound::Deliver(delivery));
        }
        self.drain_backlog(slot, out);
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
            // The log record this came from, so a session that never
            // acknowledges it can be told where to resume from.
            self.route(&publish, durable.map_or(0, |token| token.0), out);
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

    fn route(&mut self, publish: &Publish, correlation: u64, out: &mut Outbox) {
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
                Outgoing {
                    topic: &publish.topic,
                    payload: &publish.payload,
                    correlation,
                },
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
        message: Outgoing<'_>,
        guarantee: DeliveryGuarantee,
        retain: bool,
        out: &mut Outbox,
    ) {
        let Outgoing {
            topic,
            payload,
            correlation,
        } = message;
        let limits = self.limits;
        let delivery_id = self.next_delivery;
        // Read before the session is borrowed. A per-session limit times the
        // session count is a product, not a bound (§8), so the backlogs get
        // a budget of their own the way the writer queues do — a plain
        // counter, because the broker is one task and owns all of them.
        let room = limits
            .total_backlog_bytes
            .saturating_sub(self.backlog_bytes);

        let Some(session) = self.session_mut(slot) else {
            return;
        };
        let id = session.id;

        // A session read back from the log already accounted for everything
        // up to its mark before the restart; replaying it again would be a
        // duplicate nobody asked for.
        if session.owes_from != 0 && correlation <= session.owes_from {
            return;
        }

        let online = session.online;

        let (effect, used_id, dropped, backlogged) = match guarantee {
            // A session that is not connected cannot be told; at-most-once
            // means exactly that this is allowed to be lost (3.1.1 §3.3.1.3).
            DeliveryGuarantee::AtMostOnce if !online => (None, false, false, 0),
            DeliveryGuarantee::AtMostOnce => (
                Some(Delivery {
                    id: None,
                    topic: topic.clone(),
                    payload: payload.clone(),
                    duplicate: false,
                    retain,
                }),
                false,
                false,
                0,
            ),
            DeliveryGuarantee::AtLeastOnce
                if online && session.inflight.len() < limits.max_inflight =>
            {
                let delivery = DeliveryId(delivery_id);
                session.inflight.push(session::Inflight {
                    id: delivery,
                    topic: topic.clone(),
                    payload: payload.clone(),
                    correlation,
                });
                (
                    Some(Delivery {
                        id: Some(delivery),
                        topic: topic.clone(),
                        payload: payload.clone(),
                        duplicate: false,
                        retain,
                    }),
                    true,
                    false,
                    0,
                )
            }
            DeliveryGuarantee::AtLeastOnce => {
                // The window is full: wait in the backlog, or give up on a
                // session that is too far behind to catch up.
                let weight = payload.len();
                let accepted = weight <= room
                    && session
                        .backlog
                        .push(topic.clone(), payload.clone(), correlation);
                (None, false, !accepted, if accepted { weight } else { 0 })
            }
        };

        if used_id {
            self.next_delivery = self.next_delivery.saturating_add(1);
        }
        self.backlog_bytes = self.backlog_bytes.saturating_add(backlogged);
        match effect {
            Some(delivery) => {
                self.stats.delivered_messages = self.stats.delivered_messages.saturating_add(1);
                out.push(id, Outbound::Deliver(delivery));
            }
            // Nothing to send and nothing parked: the session is away and this
            // message was allowed to be lost.
            None if !online && guarantee == DeliveryGuarantee::AtMostOnce => {
                self.stats.offline_dropped = self.stats.offline_dropped.saturating_add(1);
            }
            None => {}
        }
        if dropped {
            // The backlog is a bound, and a bound that is reached loses
            // messages: that is what bounded memory costs (§8). Counted
            // rather than silent, because an operator who sees this needs
            // to raise STRAUM_SESSION_BACKLOG_MESSAGES.
            self.stats.backlog_refused = self.stats.backlog_refused.saturating_add(1);
            // Only a session with a connection can be closed. An offline
            // one has nothing to close, and `close` would count it out of
            // the active sessions it was never in.
            if online {
                self.close(id, Some(CloseReason::SlowConsumer), out);
            }
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
            // A retained message is state, not a log record: it already sits
            // in the log under its own publish.
            self.deliver(
                slot,
                Outgoing {
                    topic: &topic,
                    payload: &payload,
                    correlation: 0,
                },
                stored.min(granted),
                true,
                out,
            );
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
            let known = held
                .filters
                .iter()
                .position(|(existing, _)| *existing == filter);
            match known {
                // Re-subscribing replaces the grant (3.1.1 §3.8.4).
                Some(position) => {
                    if let Some((_, held_guarantee)) = held.filters.get_mut(position) {
                        *held_guarantee = guarantee;
                    }
                }
                None if held.filters.len() >= limit => {
                    granted.push(SubscribeResult::Failure);
                    continue;
                }
                None => held.filters.push((filter.clone(), guarantee)),
            }
            self.index
                .subscribe(&filter, Subscriber { slot, guarantee });
            granted.push(SubscribeResult::Granted(guarantee));

            // A new subscriber hears the stored state of its topics before it
            // hears anything live (3.1.1 §3.8.4).
            self.replay_retained(slot, &filter, guarantee, out);
        }

        // What this session is subscribed to changed, so what a restart
        // would put back changed with it.
        self.note_session(slot);

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
                held.filters.retain(|(existing, _)| *existing != filter);
            }
        }
        self.note_session(slot);
        out.push(session, Outbound::Reply(Response::Unsubscribed(request)));
    }

    fn acknowledge(&mut self, session: SessionId, delivery: DeliveryId, out: &mut Outbox) {
        let Some(slot) = self.slot_of(session) else {
            return;
        };
        let forgotten = self
            .sessions
            .get_mut(slot)
            .and_then(Option::as_mut)
            .is_some_and(|held| held.forget(delivery));
        if !forgotten {
            return;
        }
        // One acknowledgement, one slot free in the window.
        self.drain_backlog(slot, out);
    }

    /// Lets waiting at-least-once messages through while the inflight window
    /// has room. An offline session keeps them where they are.
    fn drain_backlog(&mut self, slot: usize, out: &mut Outbox) {
        let limits = self.limits;
        let mut next_delivery = self.next_delivery;
        let mut delivered = 0u64;
        // What leaves the backlog stops counting against the budget.
        let mut released = 0usize;

        let Some(held) = self.sessions.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        if !held.online {
            return;
        }
        let session = held.id;

        while held.inflight.len() < limits.max_inflight {
            let Some(waiting) = held.backlog.pop() else {
                break;
            };
            let id = DeliveryId(next_delivery);
            next_delivery = next_delivery.saturating_add(1);
            released = released.saturating_add(waiting.payload.len());
            held.inflight.push(session::Inflight {
                id,
                topic: waiting.topic.clone(),
                payload: waiting.payload.clone(),
                correlation: waiting.correlation,
            });
            delivered = delivered.saturating_add(1);
            out.push(
                session,
                Outbound::Deliver(Delivery {
                    id: Some(id),
                    topic: waiting.topic,
                    payload: waiting.payload,
                    duplicate: false,
                    // Whatever this was when it arrived, it is live traffic
                    // for this session now.
                    retain: false,
                }),
            );
        }

        self.next_delivery = next_delivery;
        self.backlog_bytes = self.backlog_bytes.saturating_sub(released);
        self.stats.delivered_messages = self.stats.delivered_messages.saturating_add(delivered);
    }

    /// Ends a connection. A session that asked to be resumed stays behind
    /// without one; anything else is removed here and now (§7).
    fn close(&mut self, session: SessionId, reason: Option<CloseReason>, out: &mut Outbox) {
        let Some(slot) = self.by_id.remove(&session) else {
            return;
        };

        let keep = self
            .sessions
            .get(slot)
            .and_then(Option::as_ref)
            .is_some_and(|held| held.resume)
            && self.stats.offline_sessions < self.limits.max_offline_sessions as u64;

        if keep {
            if let Some(held) = self.sessions.get_mut(slot).and_then(Option::as_mut) {
                held.online = false;
            }
            self.stats.active_sessions = self.stats.active_sessions.saturating_sub(1);
            self.stats.offline_sessions = self.stats.offline_sessions.saturating_add(1);
            // The one moment the mark is exactly right: nothing is being
            // handed out any more, so what it still owes is what it owes.
            self.note_session(slot);
        } else {
            if self
                .sessions
                .get(slot)
                .and_then(Option::as_ref)
                .is_some_and(|held| held.resume)
            {
                // Keeping it would mean growing without a bound.
                self.stats.offline_refused = self.stats.offline_refused.saturating_add(1);
            }
            self.discard(slot);
            self.stats.active_sessions = self.stats.active_sessions.saturating_sub(1);
        }

        if let Some(reason) = reason {
            if matches!(reason, CloseReason::SlowConsumer) {
                self.stats.slow_consumer_closes = self.stats.slow_consumer_closes.saturating_add(1);
            }
            out.push(session, Outbound::Close(reason));
        }
    }

    /// Removes everything a session owned: subscriptions, inflight, backlog.
    fn discard(&mut self, slot: usize) {
        let Some(entry) = self.sessions.get_mut(slot) else {
            return;
        };
        let Some(held) = entry.take() else {
            return;
        };
        // Whatever it was still owed stops counting against the budget with
        // it; a session that is gone holds nothing.
        self.backlog_bytes = self.backlog_bytes.saturating_sub(held.backlog.bytes());
        for (filter, _) in &held.filters {
            self.index.unsubscribe(filter, slot);
        }
        // Nothing is kept for this identity any more, and a restart has to
        // agree with that.
        if held.resume {
            self.session_events
                .push(SessionEvent::Gone(held.client.clone()));
        }
        self.by_client.remove(&held.client);
        self.by_id.remove(&held.id);
        self.free.push(slot);
        if !held.online {
            self.stats.offline_sessions = self.stats.offline_sessions.saturating_sub(1);
        }
    }

    fn free_slot(&mut self) -> usize {
        match self.free.pop() {
            Some(slot) => slot,
            None => {
                self.sessions.push(None);
                self.sessions.len().saturating_sub(1)
            }
        }
    }

    /// Notes that what is kept for this session changed, so the runtime can
    /// write it down. Only a session that asked to be resumed is kept.
    fn note_session(&mut self, slot: usize) {
        let Some(held) = self.sessions.get(slot).and_then(Option::as_ref) else {
            return;
        };
        if !held.resume {
            return;
        }
        self.session_events.push(SessionEvent::Kept(SessionRecord {
            client: held.client.clone(),
            filters: held.filters.clone(),
            owes_from: held.watermark(self.durable_high_water),
        }));
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
