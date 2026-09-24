//! Sans-IO MQTT 3.1.1: codec, validation, per-connection protocol state.
//!
//! Nothing here reads the clock, touches a socket or spawns. Limits arrive as
//! a typed [`MqttLimits`]; the environment is parsed in `server`.

use std::time::Duration;

mod connection;
mod deadline;
mod decode;
mod encode;
mod packet;

pub use connection::{Connection, Disposition, PacketIds, Reaction, encode_outbound, encode_pong};

pub use decode::decode;
pub use encode::{connack, pingresp, puback, publish, suback, unsuback};
pub use packet::{
    ConnAck, Connect, ConnectRejected, Packet, PacketId, Publish, Subscribe, Unsubscribe,
};

/// Protocol limits owned by this crate. Defaults are the ones documented in
/// `docs/configuration.md`; changing one is a behavior change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MqttLimits {
    /// From accept to a valid CONNECT.
    pub connect_timeout: Duration,
    /// Whole packet, fixed header included.
    pub max_packet_bytes: usize,
    pub max_topic_bytes: usize,
    pub max_topic_levels: usize,
    pub max_client_id_bytes: usize,
    pub max_filters_per_subscribe: usize,
    /// Payloads up to this size are copied at ingress instead of sliced out of
    /// the read buffer (CLAUDE.md §9).
    pub payload_copy_threshold_bytes: usize,
}

impl Default for MqttLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(10_000),
            max_packet_bytes: 1_048_576,
            max_topic_bytes: 1024,
            max_topic_levels: 32,
            max_client_id_bytes: 256,
            max_filters_per_subscribe: 64,
            payload_copy_threshold_bytes: 512,
        }
    }
}

/// Every way a packet can be rejected. The offending bytes are not retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MqttError {
    #[error("packet exceeds the configured maximum size")]
    PacketTooLarge,
    #[error("malformed remaining length")]
    MalformedRemainingLength,
    #[error("unknown packet type {0}")]
    UnknownPacketType(u8),
    #[error("packet type {0} is not accepted from a client")]
    UnexpectedPacketType(u8),
    #[error("invalid fixed header flags for packet type {0}")]
    InvalidFlags(u8),
    #[error("a packet other than CONNECT arrived first")]
    NotConnected,
    #[error("a second CONNECT on one connection")]
    AlreadyConnected,
    #[error("a packet after DISCONNECT")]
    AfterDisconnect,
    #[error("acknowledgement of an identifier that was never sent")]
    UnknownPacketId,
    #[error("more inflight deliveries than the session allows")]
    InflightFull,
    #[error("request id was not issued by this connection")]
    ForeignRequestId,
    #[error("U+0000 in a string")]
    NulInString,
    #[error("packet ends before its fields do")]
    Truncated,
    #[error("trailing bytes after the last field")]
    TrailingBytes,
    #[error("protocol name is not MQTT")]
    InvalidProtocolName,
    #[error("unsupported protocol level {0}")]
    UnsupportedProtocolLevel(u8),
    #[error("reserved or contradictory connect flags")]
    InvalidConnectFlags,
    #[error("QoS 2 is not supported")]
    Qos2Unsupported,
    #[error("invalid QoS {0}")]
    InvalidQos(u8),
    #[error("packet identifier is zero")]
    ZeroPacketId,
    #[error("client id is longer than the configured maximum")]
    ClientIdTooLong,
    #[error("topic is longer than the configured maximum")]
    TopicTooLong,
    #[error("topic has more levels than the configured maximum")]
    TooManyTopicLevels,
    #[error("more filters than the configured maximum")]
    TooManyFilters,
    #[error("no filter in the packet")]
    EmptyFilterList,
    #[error("{0}")]
    Topic(#[from] domain::TopicError),
    #[error("{0}")]
    ClientId(#[from] domain::InvalidClientId),
}
