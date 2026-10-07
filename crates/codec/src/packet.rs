//! One packet model for MQTT 3.1.1 and MQTT 5 (ADR-0003).
//!
//! A 3.1.1 packet is the same value with empty properties and the reason
//! codes 3.1.1 implies; encoding for 3.1.1 drops what it cannot carry and maps
//! what it can (`encode`). Properties are typed per packet: a property a
//! packet may not carry has no field to land in, and the decoder refuses it.

use std::num::{NonZeroU16, NonZeroU32};

use bytes::Bytes;
use domain::{ClientId, Topic, TopicFilter};

use crate::ReasonCode;

/// Packet identifier. Zero is invalid (§2.2.1).
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Qos {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    /// Decoded so that it can be refused with the right reason; not
    /// supported before phase 6 (ADR-0003).
    ExactlyOnce = 2,
}

impl Qos {
    pub(crate) fn from_bits(bits: u8) -> Option<Self> {
        match bits {
            0 => Some(Self::AtMostOnce),
            1 => Some(Self::AtLeastOnce),
            2 => Some(Self::ExactlyOnce),
            _ => None,
        }
    }
}

/// A User Property: a name and a value, both UTF-8 (§3.1.2.11.8). Order and
/// repetition are meaningful and kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserProperty {
    pub name: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    /// Boxed: by far the largest variant, and a packet is moved once per
    /// packet; one allocation per connection, not per message.
    Connect(Box<Connect>),
    ConnAck(Box<ConnAck>),
    Publish(Publish),
    PubAck(PubAck),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
    /// MQTT 5 only.
    Auth(Auth),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    pub version: crate::Version,
    pub client_id: ClientId,
    /// Clean Session in 3.1.1, Clean Start in 5.
    pub clean_start: bool,
    pub keep_alive_secs: u16,
    pub will: Option<Will>,
    pub username: Option<Bytes>,
    pub password: Option<Bytes>,
    pub properties: ConnectProperties,
}

/// CONNECT properties (§3.1.2.11). Absent ones take their defaults, which
/// the spec gives and these fields hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectProperties {
    pub session_expiry_secs: u32,
    pub receive_maximum: NonZeroU16,
    pub maximum_packet_size: Option<NonZeroU32>,
    pub topic_alias_maximum: u16,
    pub request_response_information: bool,
    pub request_problem_information: bool,
    pub authentication_method: Option<Bytes>,
    pub authentication_data: Option<Bytes>,
    pub user: Vec<UserProperty>,
}

impl Default for ConnectProperties {
    fn default() -> Self {
        Self {
            session_expiry_secs: 0,
            receive_maximum: NonZeroU16::MAX,
            maximum_packet_size: None,
            topic_alias_maximum: 0,
            request_response_information: false,
            request_problem_information: true,
            authentication_method: None,
            authentication_data: None,
            user: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Will {
    pub topic: Topic,
    pub payload: Bytes,
    pub qos: Qos,
    pub retain: bool,
    pub properties: WillProperties,
}

/// Will properties (§3.1.3.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WillProperties {
    pub delay_secs: u32,
    pub payload_format_utf8: bool,
    pub message_expiry_secs: Option<u32>,
    pub content_type: Option<Bytes>,
    pub response_topic: Option<Topic>,
    pub correlation_data: Option<Bytes>,
    pub user: Vec<UserProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    pub session_present: bool,
    pub reason: ReasonCode,
    pub properties: ConnAckProperties,
}

/// CONNACK properties (§3.2.2.3).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnAckProperties {
    pub session_expiry_secs: Option<u32>,
    pub receive_maximum: Option<NonZeroU16>,
    pub maximum_qos: Option<Qos>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<NonZeroU32>,
    pub assigned_client_id: Option<ClientId>,
    pub topic_alias_maximum: Option<u16>,
    pub reason_string: Option<Bytes>,
    pub user: Vec<UserProperty>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifiers_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<Bytes>,
    pub server_reference: Option<Bytes>,
    pub authentication_method: Option<Bytes>,
    pub authentication_data: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    /// `None` only in MQTT 5, when a Topic Alias stands for it; resolving the
    /// alias is the connection's job, not the codec's.
    pub topic: Option<Topic>,
    /// `Some` exactly when `qos` is not `AtMostOnce`.
    pub id: Option<PacketId>,
    pub qos: Qos,
    pub duplicate: bool,
    pub retain: bool,
    pub payload: Bytes,
    /// `None` when there are none, so a publish without properties — every
    /// 3.1.1 publish — carries no allocation for them.
    pub properties: Option<Box<PublishProperties>>,
}

/// PUBLISH properties (§3.3.2.3).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublishProperties {
    pub payload_format_utf8: bool,
    pub message_expiry_secs: Option<u32>,
    pub topic_alias: Option<NonZeroU16>,
    pub response_topic: Option<Topic>,
    pub correlation_data: Option<Bytes>,
    pub user: Vec<UserProperty>,
    /// Server to client only: the subscriptions that matched.
    pub subscription_identifiers: Vec<NonZeroU32>,
    pub content_type: Option<Bytes>,
}

/// The properties of an acknowledgement (PUBACK, SUBACK, UNSUBACK).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AckProperties {
    pub reason_string: Option<Bytes>,
    pub user: Vec<UserProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubAck {
    pub id: PacketId,
    pub reason: ReasonCode,
    pub properties: AckProperties,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    pub id: PacketId,
    pub subscription_identifier: Option<NonZeroU32>,
    pub user: Vec<UserProperty>,
    /// Never empty; at most `max_filters_per_subscribe` entries.
    pub filters: Vec<(TopicFilter, SubscriptionOptions)>,
}

/// Subscription Options (§3.8.3.1). In 3.1.1 only `qos` exists; the rest
/// keep the values that mean "as 3.1.1 does".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionOptions {
    pub qos: Qos,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

impl SubscriptionOptions {
    pub fn qos(qos: Qos) -> Self {
        Self {
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::OnSubscribe,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainHandling {
    /// Send retained messages at the time of the subscribe.
    OnSubscribe = 0,
    /// Only if the subscription did not exist.
    OnNewSubscription = 1,
    Never = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    pub id: PacketId,
    /// One per filter, in order.
    pub reasons: Vec<ReasonCode>,
    pub properties: AckProperties,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    pub id: PacketId,
    pub user: Vec<UserProperty>,
    /// Never empty; at most `max_filters_per_subscribe` entries.
    pub filters: Vec<TopicFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    pub id: PacketId,
    /// One per filter in MQTT 5; 3.1.1 carries none.
    pub reasons: Vec<ReasonCode>,
    pub properties: AckProperties,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disconnect {
    pub reason: ReasonCode,
    pub properties: DisconnectProperties,
}

/// DISCONNECT properties (§3.14.2.2). The session expiry interval is the
/// client's to send, the server reference the server's.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DisconnectProperties {
    pub session_expiry_secs: Option<u32>,
    pub reason_string: Option<Bytes>,
    pub user: Vec<UserProperty>,
    pub server_reference: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    pub reason: ReasonCode,
    pub properties: AuthProperties,
}

/// AUTH properties (§3.15.2.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthProperties {
    pub authentication_method: Option<Bytes>,
    pub authentication_data: Option<Bytes>,
    pub reason_string: Option<Bytes>,
    pub user: Vec<UserProperty>,
}
