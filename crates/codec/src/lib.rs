//! MQTT 3.1.1 and MQTT 5 over one packet model (ADR-0003; ADR-0008 step 3a).
//!
//! Sans-IO: bytes in, packets out, and back. Properties, reason codes and
//! reason strings are part of the model; a 3.1.1 packet is the same value
//! with empty properties. Both directions decode and encode, so a simulated
//! client speaks through the same code the gateway does.
//!
//! This crate is new beside `mqtt`, which the running server still uses
//! (D-49 A); nothing here changes it. The contract the codec hands the
//! gateway is this packet model: the gateway (step 3b) turns packets into
//! proposals for the log and committed records into packets.

mod decode;
mod encode;
mod packet;
mod properties;
mod reason;
mod wire;

pub use decode::decode;
pub use encode::encode;
pub use packet::{
    AckProperties, Auth, AuthProperties, ConnAck, ConnAckProperties, Connect, ConnectProperties,
    Disconnect, DisconnectProperties, Packet, PacketId, PubAck, Publish, PublishProperties, Qos,
    RetainHandling, SubAck, Subscribe, SubscriptionOptions, UnsubAck, Unsubscribe, UserProperty,
    Will, WillProperties,
};
pub use reason::ReasonCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Version {
    /// Protocol level 4.
    V311,
    /// Protocol level 5.
    V5,
}

/// Which side sent the bytes: the packets each may send differ, and so do
/// some properties (a Subscription Identifier on a PUBLISH, a Session Expiry
/// on a DISCONNECT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sender {
    Client,
    Server,
}

/// Limits on what a peer may make the decoder hold. Every one bounds a
/// remote-controlled length or count before it is allocated (CLAUDE.md §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Whole packet, fixed header included.
    pub max_packet_bytes: usize,
    pub max_topic_bytes: usize,
    pub max_topic_levels: usize,
    pub max_client_id_bytes: usize,
    pub max_filters_per_subscribe: usize,
    /// User Properties in one property section.
    pub max_user_properties: usize,
    /// Payloads up to this size are copied at ingress instead of sliced out
    /// of the read buffer (CLAUDE.md §9).
    pub payload_copy_threshold_bytes: usize,
}

impl Default for Limits {
    /// The old codec's defaults (docs/configuration.md), and 32 User
    /// Properties per section.
    fn default() -> Self {
        Self {
            max_packet_bytes: 1_048_576,
            max_topic_bytes: 1024,
            max_topic_levels: 32,
            max_client_id_bytes: 256,
            max_filters_per_subscribe: 64,
            max_user_properties: 32,
            payload_copy_threshold_bytes: 512,
        }
    }
}

/// Every way a packet is refused. The offending bytes are not retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("packet exceeds the configured maximum size")]
    PacketTooLarge,
    #[error("malformed variable byte integer")]
    MalformedVarint,
    #[error("unknown packet type {0}")]
    UnknownPacketType(u8),
    #[error("packet type {0} is not accepted from this side, or not in this version")]
    UnexpectedPacketType(u8),
    #[error("invalid fixed header or acknowledge flags for packet type {0}")]
    InvalidFlags(u8),
    #[error("a packet other than CONNECT arrived first")]
    NotConnected,
    #[error("packet ends before its fields do")]
    Truncated,
    #[error("trailing bytes after the last field")]
    TrailingBytes,
    #[error("U+0000 in a string")]
    NulInString,
    #[error("a string is not valid UTF-8")]
    InvalidUtf8,
    #[error("protocol name is not MQTT")]
    InvalidProtocolName,
    #[error("unsupported protocol level {0}")]
    UnsupportedProtocolLevel(u8),
    #[error("reserved or contradictory connect flags")]
    InvalidConnectFlags,
    #[error("invalid QoS {0}")]
    InvalidQos(u8),
    #[error("packet identifier is zero")]
    ZeroPacketId,
    #[error("client id is longer than the configured maximum")]
    ClientIdTooLong,
    #[error("topic name invalid")]
    TopicNameInvalid,
    #[error("topic filter invalid")]
    TopicFilterInvalid,
    #[error("more filters than the configured maximum")]
    TooManyFilters,
    #[error("SUBSCRIBE or UNSUBSCRIBE without a filter")]
    EmptyFilterList,
    #[error("reserved or contradictory subscription options")]
    InvalidSubscriptionOptions,
    #[error("unknown property identifier")]
    UnknownProperty,
    #[error("a property this packet may not carry")]
    PropertyNotAllowed,
    #[error("a property that may appear once appeared twice")]
    DuplicateProperty,
    #[error("a property value out of range")]
    InvalidProperty,
    #[error("topic alias zero")]
    TopicAliasInvalid,
    #[error("more user properties than the configured maximum")]
    TooManyUserProperties,
    #[error("unknown reason code {0:#04x}")]
    UnknownReasonCode(u8),
    #[error("reason code {0:#04x} is not one this packet may carry")]
    ReasonNotAllowed(u8),
}

impl DecodeError {
    /// What an MQTT 5 peer is told: the reason of the CONNACK or DISCONNECT
    /// that ends the connection (§4.13). A 3.1.1 peer is told nothing; its
    /// connection is closed.
    pub fn reason(self) -> ReasonCode {
        use DecodeError::*;
        match self {
            PacketTooLarge => ReasonCode::PacketTooLarge,
            InvalidProtocolName | UnsupportedProtocolLevel(_) => {
                ReasonCode::UnsupportedProtocolVersion
            }
            ClientIdTooLong => ReasonCode::ClientIdentifierNotValid,
            TopicNameInvalid => ReasonCode::TopicNameInvalid,
            TopicFilterInvalid => ReasonCode::TopicFilterInvalid,
            TopicAliasInvalid => ReasonCode::TopicAliasInvalid,
            TooManyFilters | TooManyUserProperties => ReasonCode::ImplementationSpecificError,
            NotConnected
            | UnexpectedPacketType(_)
            | EmptyFilterList
            | PropertyNotAllowed
            | DuplicateProperty
            | InvalidProperty
            | ReasonNotAllowed(_)
            | InvalidSubscriptionOptions => ReasonCode::ProtocolError,
            MalformedVarint | UnknownPacketType(_) | InvalidFlags(_) | Truncated
            | TrailingBytes | NulInString | InvalidUtf8 | InvalidConnectFlags | InvalidQos(_)
            | ZeroPacketId | UnknownProperty | UnknownReasonCode(_) => ReasonCode::MalformedPacket,
        }
    }
}

/// Why a packet could not be encoded. Every case is the caller's mistake or
/// a value the version cannot express; none comes from a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    #[error("a string or binary field is longer than 65535 bytes")]
    FieldTooLong,
    #[error("the packet is longer than a Remaining Length can say")]
    TooLarge,
    #[error("this version has no form for the packet")]
    NotInVersion,
    #[error("a reason code this packet may not carry")]
    ReasonNotAllowed,
    #[error("fields that contradict each other")]
    Invalid,
}
