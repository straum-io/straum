//! The contract between protocol implementations and the broker.
//!
//! No protocol vocabulary lives here: a protocol implementation translates its
//! wire format into [`Command`] and translates [`Outbound`] back. The broker
//! never sees a packet, a connection or a socket.

use auth::Permissions;
use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee, DeliveryId, SessionId, Topic, TopicFilter};

/// Issued by the runtime for a publish it is persisting, and echoed back by
/// the WAL once that publish is durable. Monotonic: `on_durable(token)`
/// means everything up to and including `token` is durable
/// (`docs/storage.md` §79, §81).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DurableToken(pub u64);

/// Opaque correlation chosen by the protocol layer and echoed back.
/// The broker never interprets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestId(pub u32);

/// What a session asks the broker to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Connect(Connect),
    /// `ack: Some` => the sender wants confirmation that the broker accepted it.
    Publish {
        ack: Option<RequestId>,
        publish: Publish,
    },
    Subscribe {
        request: RequestId,
        filters: Vec<(TopicFilter, DeliveryGuarantee)>,
    },
    Unsubscribe {
        request: RequestId,
        filters: Vec<TopicFilter>,
    },
    /// The client confirmed an at-least-once delivery.
    Ack(DeliveryId),
    Disconnect {
        reason: DisconnectReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    /// Empty when the client did not present one; the broker assigns an
    /// identity in that case.
    pub client_id: ClientId,
    /// Filled in by whoever checked the credentials. A CONNECT does not
    /// become a command until somebody has (docs/configuration.md).
    pub permissions: Permissions,
    /// Take over a session this identity left behind, if there is one.
    /// A protocol that has no such notion always sends `false`.
    pub resume: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub topic: Topic,
    pub payload: Bytes,
    pub guarantee: DeliveryGuarantee,
    /// Keep this as the last message for its topic, and hand it to
    /// whoever subscribes next. An empty payload forgets it instead.
    pub retain: bool,
}
/// Why a session ended. `Client` is a clean goodbye, the other two are not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason {
    Client,
    ConnectionLost,
    ProtocolViolation,
}

/// What the broker produces for one session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outbound {
    /// To the session that issued the command.
    Reply(Response),
    /// A message for a subscriber session.
    Deliver(Delivery),
    /// The broker decided to end this session.
    Close(CloseReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Connected {
        session_present: bool,
    },
    ConnectRejected(ConnectRejectReason),
    /// Emitted after routing completed for every matched session.
    PublishAccepted(RequestId),
    Subscribed {
        request: RequestId,
        granted: Vec<SubscribeResult>,
    },
    Unsubscribed(RequestId),
}

/// Rejections the broker itself makes. A protocol-level rejection (an
/// unsupported version, a malformed identifier) never reaches the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectRejectReason {
    /// The user name is unknown or the password does not match.
    BadCredentials,
    /// Authenticated, or anonymous, but not allowed to connect.
    NotAuthorized,
    /// The broker is at capacity.
    ServerUnavailable,
}

/// One entry per filter of a [`Command::Subscribe`], in the same order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeResult {
    Granted(DeliveryGuarantee),
    Failure,
}

/// Why the broker closed a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// The session could not keep up and an at-least-once message or a reply
    /// could not be queued for it.
    SlowConsumer,
    /// Another connection claimed the same client identity.
    SessionTakenOver,
    /// The broker is shutting down.
    Shutdown,
}

/// One message on its way to one subscriber.
///
/// The payload is shared, not copied: fan-out to N sessions clones a refcount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// `Some` exactly for at-least-once deliveries, which stay in the
    /// session's inflight set until [`Command::Ack`].
    pub id: Option<DeliveryId>,
    pub topic: Topic,
    pub payload: Bytes,
    /// This message was handed to the session before and not
    /// acknowledged; the client may already have it.
    pub duplicate: bool,
    /// Set when this is a stored message being handed to a new
    /// subscriber, so the client can tell it from live traffic.
    pub retain: bool,
}

impl Delivery {
    pub fn guarantee(&self) -> DeliveryGuarantee {
        match self.id {
            Some(_) => DeliveryGuarantee::AtLeastOnce,
            None => DeliveryGuarantee::AtMostOnce,
        }
    }
}

/// Where the broker writes what it produced while handling one command.
///
/// Owned by the caller and reused: [`Outbox::take`] empties it without giving
/// up its capacity, so a steady-state broker loop does not allocate here.
#[derive(Debug, Default)]
pub struct Outbox {
    items: Vec<(SessionId, Outbound)>,
}

impl Outbox {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, session: SessionId, outbound: Outbound) {
        self.items.push((session, outbound));
    }

    /// Yields everything produced so far and leaves the outbox empty.
    pub fn take(&mut self) -> std::vec::Drain<'_, (SessionId, Outbound)> {
        self.items.drain(..)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delivery(id: Option<u64>) -> Delivery {
        Delivery {
            id: id.map(DeliveryId),
            topic: Topic::new(Bytes::from_static(b"a/b")).unwrap(),
            payload: Bytes::from_static(b"payload"),
            duplicate: false,
            retain: false,
        }
    }

    #[test]
    fn delivery_guarantee_follows_the_identifier() {
        assert_eq!(delivery(None).guarantee(), DeliveryGuarantee::AtMostOnce);
        assert_eq!(
            delivery(Some(1)).guarantee(),
            DeliveryGuarantee::AtLeastOnce
        );
    }

    #[test]
    fn fan_out_shares_one_payload() {
        let source = delivery(None);
        let copies: Vec<Delivery> = (0..8).map(|_| source.clone()).collect();

        for copy in &copies {
            assert_eq!(copy.payload.as_ptr(), source.payload.as_ptr());
        }
    }

    #[test]
    fn outbox_is_emptied_without_giving_up_capacity() {
        let mut outbox = Outbox::new();
        for session in 0..16 {
            outbox.push(SessionId(session), Outbound::Close(CloseReason::Shutdown));
        }
        assert_eq!(outbox.len(), 16);

        let taken: Vec<_> = outbox.take().collect();
        assert_eq!(taken.len(), 16);
        assert!(outbox.is_empty());

        // The broker loop reuses one outbox; a second round must not allocate.
        let capacity = outbox.items.capacity();
        outbox.push(SessionId(0), Outbound::Close(CloseReason::SlowConsumer));
        let _ = outbox.take();
        assert_eq!(outbox.items.capacity(), capacity);
    }
}
