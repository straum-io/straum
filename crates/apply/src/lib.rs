//! The apply function (ADR-0008 steps 3b-1 and 3b-2; unified-stream §1,
//! §2.3).
//!
//! Every node runs the same function over the same committed log, so every
//! node holds the same state: subscriptions, retained messages and
//! at-least-once inflight state are derived from committed records only,
//! and every observable effect — a publisher's acknowledgement, a delivery,
//! a slow consumer's close — is the application of a committed record
//! (ADR-0005). Nothing here knows MQTT: the gateway turns packets into
//! [`Proposal`]s and [`Effect`]s back into packets.
//!
//! Every bound is decided here, from the log, so that every node refuses the
//! same thing at the same lsn: subscriptions per session
//! and in total, retained messages in count and bytes, inflight per session,
//! and the backlog behind it per session and in total. A bound that one
//! gateway checked alone would not bound the shared state.
//!
//! Not yet here: sessions that outlive their connection, expiry and wills go
//! through the log at step 7. Until then a session is an [`Origin`] — one
//! connection at one gateway — and ends with its `SessionGone` record, or
//! with its gateway's `NodeReset`.
//!
//! A record that does not decode stops the node rather than being skipped:
//! skipping it would make this node's state differ from the others'.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

mod snapshot;

pub use snapshot::SnapshotError;

use std::collections::{BTreeMap, VecDeque};
use std::hash::{Hash, Hasher};

use bytes::{BufMut, Bytes};
use domain::{DeliveryGuarantee, Topic, TopicError, TopicFilter};
use journal::{Kind, Record};

/// Where a proposal came from and where its effects go: a session at one
/// gateway node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Origin {
    pub node: u8,
    pub session: u64,
}

/// When a subscription is handed the retained messages that match it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RetainHandling {
    OnSubscribe,
    /// Only if the session did not already have this subscription.
    OnNewSubscription,
    Never,
}

/// How a subscription receives messages, beyond its guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Options {
    /// Not the session's own publishes.
    pub no_local: bool,
    /// Deliver the publisher's retain flag as it was.
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::OnSubscribe,
        }
    }
}

/// What a gateway proposes. Each becomes the body of one `journal` record of
/// the kind [`Proposal::kind`] names; `request` is the gateway's own
/// correlation, echoed in the effect that answers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Proposal {
    Publish {
        origin: Origin,
        request: u64,
        topic: Topic,
        payload: Bytes,
        /// The record's properties section, carried and never read here: the
        /// gateway writes a publish's message properties into it and reads
        /// them back for MQTT 5 subscribers (D-65, ADR-0003).
        properties: Bytes,
        guarantee: DeliveryGuarantee,
        /// Kept as the topic's last message and handed to the next
        /// subscriber; an empty payload removes it.
        retain: bool,
    },
    Subscribe {
        origin: Origin,
        request: u64,
        filter: TopicFilter,
        guarantee: DeliveryGuarantee,
        options: Options,
    },
    Unsubscribe {
        origin: Origin,
        request: u64,
        filter: TopicFilter,
    },
    /// The session received at-least-once delivery `delivery`.
    Ack { origin: Origin, delivery: u64 },
    /// The session's connection ended: its subscriptions, inflight and
    /// backlog go.
    SessionGone { origin: Origin },
    /// A gateway restarted: every session it had, numbered below `below`, is
    /// gone. Its new sessions are numbered from `below` up.
    NodeReset { node: u8, below: u64 },
}

/// What applying a committed record makes observable. Every node computes
/// all of them; each gateway acts on those addressed to its own sessions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// The publish at `lsn` is committed: its publisher may be acknowledged.
    Accepted {
        to: Origin,
        request: u64,
        lsn: u64,
    },
    /// `granted` is `None` when a bound refused the subscription.
    Subscribed {
        to: Origin,
        request: u64,
        granted: Option<DeliveryGuarantee>,
        lsn: u64,
    },
    Unsubscribed {
        to: Origin,
        request: u64,
        lsn: u64,
    },
    /// A message for a subscriber. `lsn` is the record that caused it: the
    /// publish, the subscribe for a retained message, or the acknowledgement
    /// that made room for one that waited. `id` is set for at-least-once:
    /// the session acknowledges it with [`Proposal::Ack`].
    Deliver {
        to: Origin,
        id: Option<u64>,
        topic: Topic,
        payload: Bytes,
        properties: Bytes,
        guarantee: DeliveryGuarantee,
        retain: bool,
        lsn: u64,
    },
    Acked {
        to: Origin,
        delivery: u64,
        lsn: u64,
    },
    Gone {
        to: Origin,
        lsn: u64,
    },
    Reset {
        node: u8,
        below: u64,
        lsn: u64,
    },
    /// A slow consumer: its backlog was full when an at-least-once message
    /// arrived for it. Its state is already gone; the gateway closes the
    /// connection.
    Close {
        to: Origin,
        lsn: u64,
    },
}

impl Effect {
    /// The session the effect is for, if it is for one.
    pub fn to(&self) -> Option<Origin> {
        match self {
            Self::Accepted { to, .. }
            | Self::Subscribed { to, .. }
            | Self::Unsubscribed { to, .. }
            | Self::Deliver { to, .. }
            | Self::Acked { to, .. }
            | Self::Gone { to, .. }
            | Self::Close { to, .. } => Some(*to),
            Self::Reset { .. } => None,
        }
    }

    pub fn lsn(&self) -> u64 {
        match self {
            Self::Accepted { lsn, .. }
            | Self::Subscribed { lsn, .. }
            | Self::Unsubscribed { lsn, .. }
            | Self::Deliver { lsn, .. }
            | Self::Acked { lsn, .. }
            | Self::Gone { lsn, .. }
            | Self::Reset { lsn, .. }
            | Self::Close { lsn, .. } => *lsn,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ApplyError {
    #[error("record at lsn {lsn} is not the next one, {expected}")]
    OutOfOrder { lsn: u64, expected: u64 },
    #[error("record at lsn {0} does not decode")]
    Malformed(u64),
    #[error("record at lsn {0} has a topic or filter that is not one: {1}")]
    Topic(u64, TopicError),
    #[error("record kind {0:?} is not applied before a later step")]
    NotYet(Kind),
}

/// A topic or filter longer than 65 535 bytes: MQTT cannot carry one, so a
/// gateway proposing it has a bug. Refused rather than truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a topic or filter longer than 65535 bytes")]
pub struct TooLong;

fn put_short(bytes: &[u8], out: &mut Vec<u8>) -> Result<(), TooLong> {
    let length = u16::try_from(bytes.len()).map_err(|_| TooLong)?;
    out.put_u16(length);
    out.put_slice(bytes);
    Ok(())
}

fn guarantee_byte(guarantee: DeliveryGuarantee) -> u8 {
    match guarantee {
        DeliveryGuarantee::AtMostOnce => 0,
        DeliveryGuarantee::AtLeastOnce => 1,
    }
}

fn options_byte(options: Options) -> u8 {
    let handling = match options.retain_handling {
        RetainHandling::OnSubscribe => 0,
        RetainHandling::OnNewSubscription => 1,
        RetainHandling::Never => 2,
    };
    u8::from(options.no_local) | (u8::from(options.retain_as_published) << 1) | (handling << 2)
}

fn put_origin(origin: Origin, out: &mut Vec<u8>) {
    out.put_u8(origin.node);
    out.put_u64(origin.session);
}

impl Proposal {
    /// Session lifecycle uses the kinds reserved for it: `SessionGone`, and
    /// `NodeReset` its own kind; `Disconnect` stays the per-session event
    /// step 7 brings.
    /// The properties section of the record this proposal becomes: a
    /// publish's, empty for everything else.
    pub fn properties(&self) -> &[u8] {
        match self {
            Self::Publish { properties, .. } => properties,
            _ => &[],
        }
    }

    pub fn kind(&self) -> Kind {
        match self {
            Self::Publish { .. } => Kind::Publish,
            Self::Subscribe { .. } => Kind::Subscribe,
            Self::Unsubscribe { .. } => Kind::Unsubscribe,
            Self::Ack { .. } => Kind::Ack,
            Self::SessionGone { .. } => Kind::SessionGone,
            Self::NodeReset { .. } => Kind::NodeReset,
        }
    }

    /// The record body. Publish, subscribe, unsubscribe:
    /// `[node u8][session u64][request u64]`, then for a publish
    /// `[guarantee u8][retain u8][topic_len u16][topic][payload]`, a
    /// subscribe `[guarantee u8][options u8][filter_len u16][filter]`, an
    /// unsubscribe `[filter_len u16][filter]`. Ack: `[node][session]
    /// [delivery u64]`. SessionGone: `[node][session]`. NodeReset:
    /// `[node u8][below u64]`.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), TooLong> {
        match self {
            Self::Publish {
                origin,
                request,
                topic,
                payload,
                guarantee,
                retain,
                ..
            } => {
                put_origin(*origin, out);
                out.put_u64(*request);
                out.put_u8(guarantee_byte(*guarantee));
                out.put_u8(u8::from(*retain));
                put_short(topic.as_bytes(), out)?;
                out.put_slice(payload);
            }
            Self::Subscribe {
                origin,
                request,
                filter,
                guarantee,
                options,
            } => {
                put_origin(*origin, out);
                out.put_u64(*request);
                out.put_u8(guarantee_byte(*guarantee));
                out.put_u8(options_byte(*options));
                put_short(filter.as_bytes(), out)?;
            }
            Self::Unsubscribe {
                origin,
                request,
                filter,
            } => {
                put_origin(*origin, out);
                out.put_u64(*request);
                put_short(filter.as_bytes(), out)?;
            }
            Self::Ack { origin, delivery } => {
                put_origin(*origin, out);
                out.put_u64(*delivery);
            }
            Self::SessionGone { origin } => put_origin(*origin, out),
            Self::NodeReset { node, below } => {
                out.put_u8(*node);
                out.put_u64(*below);
            }
        }
        Ok(())
    }

    pub fn decode(record: &Record<'_>) -> Result<Self, ApplyError> {
        let lsn = record.lsn;
        let mut body = Body(record.body, lsn);
        let proposal = match record.kind {
            Kind::Publish => {
                let origin = body.origin()?;
                let request = body.u64()?;
                let guarantee = body.guarantee()?;
                let retain = body.flag()?;
                let topic = Topic::new(body.short()?).map_err(|e| ApplyError::Topic(lsn, e))?;
                Self::Publish {
                    origin,
                    request,
                    topic,
                    payload: Bytes::copy_from_slice(body.0),
                    properties: Bytes::copy_from_slice(record.properties.bytes()),
                    guarantee,
                    retain,
                }
            }
            Kind::Subscribe => {
                let origin = body.origin()?;
                let request = body.u64()?;
                let guarantee = body.guarantee()?;
                let options = body.options()?;
                let filter =
                    TopicFilter::new(body.short()?).map_err(|e| ApplyError::Topic(lsn, e))?;
                body.finish()?;
                Self::Subscribe {
                    origin,
                    request,
                    filter,
                    guarantee,
                    options,
                }
            }
            Kind::Unsubscribe => {
                let origin = body.origin()?;
                let request = body.u64()?;
                let filter =
                    TopicFilter::new(body.short()?).map_err(|e| ApplyError::Topic(lsn, e))?;
                body.finish()?;
                Self::Unsubscribe {
                    origin,
                    request,
                    filter,
                }
            }
            Kind::Ack => {
                let origin = body.origin()?;
                let delivery = body.u64()?;
                body.finish()?;
                Self::Ack { origin, delivery }
            }
            Kind::SessionGone => {
                let origin = body.origin()?;
                body.finish()?;
                Self::SessionGone { origin }
            }
            Kind::NodeReset => {
                let node = body.byte()?;
                let below = body.u64()?;
                body.finish()?;
                Self::NodeReset { node, below }
            }
            other => return Err(ApplyError::NotYet(other)),
        };
        Ok(proposal)
    }
}

/// Reads a record body; every length in it is checked.
struct Body<'a>(&'a [u8], u64);

impl<'a> Body<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], ApplyError> {
        if self.0.len() < count {
            return Err(ApplyError::Malformed(self.1));
        }
        let (head, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(head)
    }

    fn byte(&mut self) -> Result<u8, ApplyError> {
        self.take(1)?
            .first()
            .copied()
            .ok_or(ApplyError::Malformed(self.1))
    }

    fn flag(&mut self) -> Result<bool, ApplyError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ApplyError::Malformed(self.1)),
        }
    }

    fn u64(&mut self) -> Result<u64, ApplyError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| ApplyError::Malformed(self.1))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn origin(&mut self) -> Result<Origin, ApplyError> {
        Ok(Origin {
            node: self.byte()?,
            session: self.u64()?,
        })
    }

    fn guarantee(&mut self) -> Result<DeliveryGuarantee, ApplyError> {
        match self.byte()? {
            0 => Ok(DeliveryGuarantee::AtMostOnce),
            1 => Ok(DeliveryGuarantee::AtLeastOnce),
            _ => Err(ApplyError::Malformed(self.1)),
        }
    }

    fn options(&mut self) -> Result<Options, ApplyError> {
        let byte = self.byte()?;
        let retain_handling = match (byte >> 2) & 0b11 {
            0 => RetainHandling::OnSubscribe,
            1 => RetainHandling::OnNewSubscription,
            2 => RetainHandling::Never,
            _ => return Err(ApplyError::Malformed(self.1)),
        };
        if byte & 0b1111_0000 != 0 {
            return Err(ApplyError::Malformed(self.1));
        }
        Ok(Options {
            no_local: byte & 1 != 0,
            retain_as_published: byte & 0b10 != 0,
            retain_handling,
        })
    }

    fn short(&mut self) -> Result<Bytes, ApplyError> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| ApplyError::Malformed(self.1))?;
        let length = usize::from(u16::from_be_bytes(bytes));
        Ok(Bytes::copy_from_slice(self.take(length)?))
    }

    fn finish(self) -> Result<(), ApplyError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(ApplyError::Malformed(self.1))
        }
    }
}

/// The bounds. Every node must be given the same ones: they are part of what
/// the log means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_subscriptions_per_session: usize,
    pub max_subscriptions: usize,
    pub max_retained: usize,
    pub max_retained_bytes: usize,
    /// At-least-once deliveries unacknowledged per session.
    pub max_inflight: usize,
    /// At-least-once deliveries waiting behind a full inflight window, per
    /// session, in messages and in payload bytes.
    pub max_backlog: usize,
    pub max_backlog_bytes: usize,
    /// All sessions' inflight and backlog payload bytes together: a
    /// per-session bound times the session count is a product, not a
    /// bound.
    pub max_total_queued_bytes: usize,
}

impl Default for Limits {
    /// The old path's defaults where it has one (docs/configuration.md).
    fn default() -> Self {
        Self {
            max_subscriptions_per_session: 1024,
            max_subscriptions: 1_000_000,
            max_retained: 10_000,
            max_retained_bytes: 64 * 1024 * 1024,
            max_inflight: 256,
            max_backlog: 16_384,
            max_backlog_bytes: 8 * 1024 * 1024,
            max_total_queued_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Message {
    topic: Topic,
    payload: Bytes,
    /// The publish's properties section, opaque here (D-65).
    properties: Bytes,
    retain: bool,
}

impl Message {
    /// What it counts against the byte bounds: payload and properties.
    fn size(&self) -> usize {
        self.payload.len().saturating_add(self.properties.len())
    }

    fn deliver(
        self,
        to: Origin,
        id: Option<u64>,
        guarantee: DeliveryGuarantee,
        lsn: u64,
    ) -> Effect {
        Effect::Deliver {
            to,
            id,
            topic: self.topic,
            payload: self.payload,
            properties: self.properties,
            guarantee,
            retain: self.retain,
            lsn,
        }
    }
}

/// One session's subscriptions and at-least-once state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
struct Session {
    subscriptions: BTreeMap<Bytes, (TopicFilter, DeliveryGuarantee, Options)>,
    inflight: BTreeMap<u64, Message>,
    inflight_bytes: usize,
    /// What waits, in order: an id for at-least-once, `None` for
    /// at-most-once queued behind it.
    backlog: VecDeque<(Option<u64>, Message)>,
    backlog_bytes: usize,
    next_delivery: u64,
}

/// The state every node derives from the committed log. Ordered maps: the
/// order effects are produced in must be the same on every node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct State {
    limits: Limits,
    /// The last lsn applied.
    applied: u64,
    sessions: BTreeMap<Origin, Session>,
    subscriptions: usize,
    /// The last retained message per topic, with the publisher's guarantee.
    retained: BTreeMap<Bytes, (Message, DeliveryGuarantee)>,
    retained_bytes: usize,
    /// Inflight and backlog bytes of every session.
    queued_bytes: usize,
}

impl State {
    /// Empty: nothing applied, lsn 0.
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            applied: 0,
            sessions: BTreeMap::new(),
            subscriptions: 0,
            retained: BTreeMap::new(),
            retained_bytes: 0,
            queued_bytes: 0,
        }
    }

    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// Sessions with any state: subscriptions, inflight or backlog.
    pub fn sessions(&self) -> usize {
        self.sessions.len()
    }

    /// At-least-once deliveries unacknowledged by `origin`.
    pub fn inflight(&self, origin: Origin) -> usize {
        self.sessions.get(&origin).map_or(0, |s| s.inflight.len())
    }

    /// Applies the next committed record and appends what it makes
    /// observable to `out`. Batch headers, `SlotStart` and `SlotEnd` advance the
    /// position and change nothing else. Records must come in lsn order, each exactly once.
    pub fn apply(&mut self, record: &Record<'_>, out: &mut Vec<Effect>) -> Result<(), ApplyError> {
        let expected = self.applied.checked_add(1).ok_or(ApplyError::OutOfOrder {
            lsn: record.lsn,
            expected: u64::MAX,
        })?;
        if record.lsn != expected {
            return Err(ApplyError::OutOfOrder {
                lsn: record.lsn,
                expected,
            });
        }
        // A slot's first and last records mark a change of sequencer: they
        // are part of the log's order and change no state (steps 5, 6).
        if !matches!(
            record.kind,
            Kind::BatchHeader | Kind::SlotStart | Kind::SlotEnd
        ) {
            let proposal = Proposal::decode(record)?;
            self.effect(proposal, record.lsn, out);
        }
        self.applied = record.lsn;
        Ok(())
    }

    fn drop_session(&mut self, origin: Origin) {
        if let Some(session) = self.sessions.remove(&origin) {
            self.subscriptions = self
                .subscriptions
                .saturating_sub(session.subscriptions.len());
            self.queued_bytes = self
                .queued_bytes
                .saturating_sub(session.backlog_bytes.saturating_add(session.inflight_bytes));
        }
    }

    fn effect(&mut self, proposal: Proposal, lsn: u64, out: &mut Vec<Effect>) {
        match proposal {
            Proposal::Publish {
                origin,
                request,
                topic,
                payload,
                properties,
                guarantee,
                retain,
            } => {
                let message = Message {
                    topic,
                    payload,
                    properties,
                    retain,
                };
                if retain {
                    self.retain(&message, guarantee);
                }
                self.route(origin, &message, guarantee, lsn, out);
                out.push(Effect::Accepted {
                    to: origin,
                    request,
                    lsn,
                });
            }
            Proposal::Subscribe {
                origin,
                request,
                filter,
                guarantee,
                options,
            } => self.subscribe(origin, request, filter, guarantee, options, lsn, out),
            Proposal::Unsubscribe {
                origin,
                request,
                filter,
            } => {
                if let Some(session) = self.sessions.get_mut(&origin) {
                    if session.subscriptions.remove(filter.as_bytes()).is_some() {
                        self.subscriptions = self.subscriptions.saturating_sub(1);
                    }
                }
                self.forget_if_empty(origin);
                out.push(Effect::Unsubscribed {
                    to: origin,
                    request,
                    lsn,
                });
            }
            Proposal::Ack { origin, delivery } => self.ack(origin, delivery, lsn, out),
            Proposal::SessionGone { origin } => {
                self.drop_session(origin);
                out.push(Effect::Gone { to: origin, lsn });
            }
            Proposal::NodeReset { node, below } => {
                let gone: Vec<Origin> = self
                    .sessions
                    .keys()
                    .filter(|o| o.node == node && o.session < below)
                    .copied()
                    .collect();
                for origin in gone {
                    self.drop_session(origin);
                }
                out.push(Effect::Reset { node, below, lsn });
            }
        }
    }

    fn forget_if_empty(&mut self, origin: Origin) {
        let empty = self.sessions.get(&origin).is_some_and(|s| {
            s.subscriptions.is_empty() && s.inflight.is_empty() && s.backlog.is_empty()
        });
        if empty {
            self.sessions.remove(&origin);
        }
    }

    fn retain(&mut self, message: &Message, guarantee: DeliveryGuarantee) {
        if let Some((old, _)) = self.retained.remove(message.topic.as_bytes()) {
            self.retained_bytes = self.retained_bytes.saturating_sub(old.size());
        }
        if message.payload.is_empty() {
            return;
        }
        let bytes = self.retained_bytes.saturating_add(message.size());
        // Beyond a bound the message is still delivered, only not kept
        // (the old path's rule).
        if self.retained.len() >= self.limits.max_retained || bytes > self.limits.max_retained_bytes
        {
            return;
        }
        self.retained_bytes = bytes;
        self.retained.insert(
            Bytes::copy_from_slice(message.topic.as_bytes()),
            (message.clone(), guarantee),
        );
    }

    fn route(
        &mut self,
        publisher: Origin,
        published: &Message,
        guarantee: DeliveryGuarantee,
        lsn: u64,
        out: &mut Vec<Effect>,
    ) {
        // ponytail: every subscription is tried; a routing index (the old
        // broker's trie) when a benchmark asks, phase 5.
        let mut matched = Vec::new();
        for (origin, session) in &self.sessions {
            // One delivery per session, at the strongest guarantee among its
            // matching filters, never above the publisher's.
            let mut best: Option<(DeliveryGuarantee, bool)> = None;
            for (filter, granted, options) in session.subscriptions.values() {
                if !filter.matches(&published.topic) || (options.no_local && *origin == publisher) {
                    continue;
                }
                let keep_retain = published.retain && options.retain_as_published;
                best = Some(match best {
                    Some((g, r)) => (g.max(*granted), r || keep_retain),
                    None => (*granted, keep_retain),
                });
            }
            if let Some((granted, retain)) = best {
                matched.push((*origin, granted.min(guarantee), retain));
            }
        }
        for (origin, guarantee, retain) in matched {
            // Refcounts, not copies: fan-out shares one payload and one
            // properties section.
            let message = Message {
                retain,
                ..published.clone()
            };
            self.deliver(origin, message, guarantee, lsn, out);
        }
    }

    /// Hands `message` to `origin`, in order. With nothing waiting it goes at
    /// once if at-most-once, or if at-least-once and both the session's
    /// inflight window and the global queued budget have room; otherwise,
    /// and whenever anything already waits, it joins the backlog behind it:
    /// an at-most-once message must not overtake an earlier one that waits
    /// (ordering — found by the gateway simulation). A backlog
    /// without room drops an at-most-once message and closes the session
    /// for an at-least-once one.
    fn deliver(
        &mut self,
        origin: Origin,
        message: Message,
        guarantee: DeliveryGuarantee,
        lsn: u64,
        out: &mut Vec<Effect>,
    ) {
        let limits = self.limits;
        let total = self.queued_bytes;
        let Some(session) = self.sessions.get_mut(&origin) else {
            return;
        };
        let at_least_once = guarantee == DeliveryGuarantee::AtLeastOnce;
        let id = at_least_once.then(|| {
            let id = session.next_delivery;
            session.next_delivery = session.next_delivery.saturating_add(1);
            id
        });
        let size = message.size();
        let budget = total.saturating_add(size) <= limits.max_total_queued_bytes;
        let now = session.backlog.is_empty()
            && (!at_least_once || (session.inflight.len() < limits.max_inflight && budget));
        if now {
            if let Some(id) = id {
                session.inflight.insert(id, message.clone());
                session.inflight_bytes = session.inflight_bytes.saturating_add(size);
                self.queued_bytes = total.saturating_add(size);
            }
            out.push(message.deliver(origin, id, guarantee, lsn));
            return;
        }
        let fits = session.backlog.len() < limits.max_backlog
            && session.backlog_bytes.saturating_add(size) <= limits.max_backlog_bytes
            && budget;
        if fits {
            session.backlog.push_back((id, message));
            session.backlog_bytes = session.backlog_bytes.saturating_add(size);
            self.queued_bytes = total.saturating_add(size);
        } else if at_least_once {
            self.drop_session(origin);
            out.push(Effect::Close { to: origin, lsn });
        }
    }

    fn ack(&mut self, origin: Origin, delivery: u64, lsn: u64, out: &mut Vec<Effect>) {
        let max_inflight = self.limits.max_inflight;
        // Every committed acknowledgement is answered, also one retried
        // after it was applied or one for a session that is gone: the
        // gateway retries an acknowledgement until it sees `Acked`, and
        // with no answer it retried for ever, holding a retry slot (found by
        // the MQTT simulation, seed 1189, D-81).
        out.push(Effect::Acked {
            to: origin,
            delivery,
            lsn,
        });
        let Some(session) = self.sessions.get_mut(&origin) else {
            return;
        };
        // An acknowledgement retried after it was applied changes nothing.
        let Some(acked) = session.inflight.remove(&delivery) else {
            return;
        };
        let mut released = acked.size();
        session.inflight_bytes = session.inflight_bytes.saturating_sub(released);
        // The backlog drains in order until an at-least-once message finds
        // the window full again. A message moving into the window stays in
        // the queued budget; one going out at most once leaves it.
        while let Some((id, _)) = session.backlog.front() {
            if id.is_some() && session.inflight.len() >= max_inflight {
                break;
            }
            let Some((id, message)) = session.backlog.pop_front() else {
                break;
            };
            let size = message.size();
            session.backlog_bytes = session.backlog_bytes.saturating_sub(size);
            let guarantee = match id {
                Some(id) => {
                    session.inflight.insert(id, message.clone());
                    session.inflight_bytes = session.inflight_bytes.saturating_add(size);
                    DeliveryGuarantee::AtLeastOnce
                }
                None => {
                    released = released.saturating_add(size);
                    DeliveryGuarantee::AtMostOnce
                }
            };
            out.push(message.deliver(origin, id, guarantee, lsn));
        }
        self.queued_bytes = self.queued_bytes.saturating_sub(released);
        self.forget_if_empty(origin);
    }

    #[expect(clippy::too_many_arguments, reason = "one subscribe, unpacked once")]
    fn subscribe(
        &mut self,
        origin: Origin,
        request: u64,
        filter: TopicFilter,
        guarantee: DeliveryGuarantee,
        options: Options,
        lsn: u64,
        out: &mut Vec<Effect>,
    ) {
        let session = self.sessions.entry(origin).or_default();
        let existed = session.subscriptions.contains_key(filter.as_bytes());
        let room = existed
            || (session.subscriptions.len() < self.limits.max_subscriptions_per_session
                && self.subscriptions < self.limits.max_subscriptions);
        if !room {
            self.forget_if_empty(origin);
            out.push(Effect::Subscribed {
                to: origin,
                request,
                granted: None,
                lsn,
            });
            return;
        }
        session.subscriptions.insert(
            Bytes::copy_from_slice(filter.as_bytes()),
            (filter.clone(), guarantee, options),
        );
        if !existed {
            self.subscriptions = self.subscriptions.saturating_add(1);
        }
        out.push(Effect::Subscribed {
            to: origin,
            request,
            granted: Some(guarantee),
            lsn,
        });
        let send = match options.retain_handling {
            RetainHandling::OnSubscribe => true,
            RetainHandling::OnNewSubscription => !existed,
            RetainHandling::Never => false,
        };
        if !send {
            return;
        }
        let matching: Vec<(Message, DeliveryGuarantee)> = self
            .retained
            .values()
            .filter(|(message, _)| filter.matches(&message.topic))
            .cloned()
            .collect();
        for (message, published) in matching {
            // A retained message is delivered as one (MQTT 5 §3.3.1.3).
            let message = Message {
                retain: true,
                ..message
            };
            self.deliver(origin, message, guarantee.min(published), lsn, out);
        }
    }

    /// A hash of the whole state, for comparing nodes in one build: two
    /// nodes that applied the same log must agree on it.
    pub fn digest(&self) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        self.applied.hash(&mut hasher);
        self.sessions.hash(&mut hasher);
        self.retained.hash(&mut hasher);
        hasher.finish()
    }
}
