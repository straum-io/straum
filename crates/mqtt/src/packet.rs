//! What the broker accepts from a client. Packets the broker only ever sends
//! are not decoded.

use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee, Topic, TopicFilter};
use std::num::NonZeroU16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    Publish(Publish),
    PubAck(PacketId),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    PingReq,
    Disconnect,
}

/// Packet identifier. Zero is invalid (3.1.1 §2.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PacketId(NonZeroU16);

impl PacketId {
    pub fn new(value: u16) -> Option<Self> {
        NonZeroU16::new(value).map(Self)
    }

    pub fn get(self) -> u16 {
        self.0.get()
    }
}

/// The will is validated during decoding and dropped: no will publishing
/// yet (CLAUDE.md §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    pub client_id: ClientId,
    pub clean_session: bool,
    pub keep_alive_secs: u16,
    /// Carried through to the runtime, which is where credentials are
    /// checked; the broker never sees them.
    pub username: Option<Bytes>,
    pub password: Option<Bytes>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub topic: Topic,
    /// `Some` for QoS 1: the identifier a PUBACK has to carry.
    pub id: Option<PacketId>,
    pub payload: Bytes,
    /// The broker keeps the last retained message per topic and hands
    /// it to whoever subscribes next (§6).
    pub retain: bool,
}

impl Publish {
    pub fn guarantee(&self) -> DeliveryGuarantee {
        match self.id {
            Some(_) => DeliveryGuarantee::AtLeastOnce,
            None => DeliveryGuarantee::AtMostOnce,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    pub id: PacketId,
    /// Never empty; at most `max_filters_per_subscribe` entries.
    pub filters: Vec<(TopicFilter, DeliveryGuarantee)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    pub id: PacketId,
    /// Never empty; at most `max_filters_per_subscribe` entries.
    pub filters: Vec<TopicFilter>,
}

/// The CONNACK the broker sends back. `session_present` may only be set on an
/// accepted connection (3.1.1 §3.2.2.2), so the two cases are separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnAck {
    Accepted { session_present: bool },
    Rejected(ConnectRejected),
}

/// CONNACK return codes other than 0x00.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectRejected {
    UnacceptableProtocolVersion,
    IdentifierRejected,
    ServerUnavailable,
    BadCredentials,
    NotAuthorized,
}

impl ConnectRejected {
    pub(crate) fn code(self) -> u8 {
        match self {
            Self::UnacceptableProtocolVersion => 0x01,
            Self::IdentifierRejected => 0x02,
            Self::ServerUnavailable => 0x03,
            Self::BadCredentials => 0x04,
            Self::NotAuthorized => 0x05,
        }
    }
}
