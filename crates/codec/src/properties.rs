//! MQTT 5 properties (§2.2.2): a length-prefixed section of
//! `[identifier][value]`, read into the typed property struct of the packet
//! that carries it.
//!
//! A property is refused if its identifier is unknown, if this packet may
//! not carry it, if it appears twice where it may appear once, or if its
//! value is out of range (a zero Receive Maximum, a Payload Format Indicator
//! of 2). User Properties are bounded in number (`Limits`).
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use std::num::{NonZeroU16, NonZeroU32};

use bytes::{BufMut, Bytes, BytesMut};
use domain::{ClientId, Topic};

use crate::packet::{
    AckProperties, AuthProperties, ConnAckProperties, ConnectProperties, DisconnectProperties,
    PublishProperties, Qos, UserProperty, WillProperties,
};
use crate::wire::{Reader, put_binary, put_varint};
use crate::{DecodeError, EncodeError, Limits};

const PAYLOAD_FORMAT: u8 = 0x01;
const MESSAGE_EXPIRY: u8 = 0x02;
const CONTENT_TYPE: u8 = 0x03;
const RESPONSE_TOPIC: u8 = 0x08;
const CORRELATION_DATA: u8 = 0x09;
const SUBSCRIPTION_ID: u8 = 0x0B;
const SESSION_EXPIRY: u8 = 0x11;
const ASSIGNED_CLIENT_ID: u8 = 0x12;
const SERVER_KEEP_ALIVE: u8 = 0x13;
const AUTH_METHOD: u8 = 0x15;
const AUTH_DATA: u8 = 0x16;
const REQUEST_PROBLEM: u8 = 0x17;
const WILL_DELAY: u8 = 0x18;
const REQUEST_RESPONSE: u8 = 0x19;
const RESPONSE_INFO: u8 = 0x1A;
const SERVER_REFERENCE: u8 = 0x1C;
const REASON_STRING: u8 = 0x1F;
const RECEIVE_MAXIMUM: u8 = 0x21;
const TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
const TOPIC_ALIAS: u8 = 0x23;
const MAXIMUM_QOS: u8 = 0x24;
const RETAIN_AVAILABLE: u8 = 0x25;
const USER: u8 = 0x26;
const MAXIMUM_PACKET_SIZE: u8 = 0x27;
const WILDCARD_AVAILABLE: u8 = 0x28;
const SUBSCRIPTION_ID_AVAILABLE: u8 = 0x29;
const SHARED_AVAILABLE: u8 = 0x2A;

/// Which identifiers a section may hold, and which of them more than once,
/// as a bit per identifier: they are all below 64. A User Property may
/// always repeat (§3.1.2.11.8).
pub(crate) struct Allowed {
    ids: u64,
    repeatable: u64,
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "const evaluation over the constant identifiers above; out of range fails the build"
)]
const fn set(ids: &[u8]) -> Allowed {
    let mut bits = 0u64;
    let mut i = 0;
    while i < ids.len() {
        bits |= 1 << ids[i];
        i += 1;
    }
    Allowed {
        ids: bits,
        repeatable: 1 << USER,
    }
}

/// A PUBLISH from the server carries one Subscription Identifier per
/// matching subscription (§3.3.4).
const fn repeating(allowed: Allowed, id: u8) -> Allowed {
    Allowed {
        ids: allowed.ids,
        repeatable: allowed.repeatable | (1 << id),
    }
}

pub(crate) const CONNECT: Allowed = set(&[
    SESSION_EXPIRY,
    RECEIVE_MAXIMUM,
    MAXIMUM_PACKET_SIZE,
    TOPIC_ALIAS_MAXIMUM,
    REQUEST_RESPONSE,
    REQUEST_PROBLEM,
    USER,
    AUTH_METHOD,
    AUTH_DATA,
]);
pub(crate) const WILL: Allowed = set(&[
    WILL_DELAY,
    PAYLOAD_FORMAT,
    MESSAGE_EXPIRY,
    CONTENT_TYPE,
    RESPONSE_TOPIC,
    CORRELATION_DATA,
    USER,
]);
pub(crate) const CONNACK: Allowed = set(&[
    SESSION_EXPIRY,
    RECEIVE_MAXIMUM,
    MAXIMUM_QOS,
    RETAIN_AVAILABLE,
    MAXIMUM_PACKET_SIZE,
    ASSIGNED_CLIENT_ID,
    TOPIC_ALIAS_MAXIMUM,
    REASON_STRING,
    USER,
    WILDCARD_AVAILABLE,
    SUBSCRIPTION_ID_AVAILABLE,
    SHARED_AVAILABLE,
    SERVER_KEEP_ALIVE,
    RESPONSE_INFO,
    SERVER_REFERENCE,
    AUTH_METHOD,
    AUTH_DATA,
]);
/// A client may not send a Subscription Identifier (§3.3.4); a server may.
pub(crate) const PUBLISH_FROM_CLIENT: Allowed = set(&[
    PAYLOAD_FORMAT,
    MESSAGE_EXPIRY,
    TOPIC_ALIAS,
    RESPONSE_TOPIC,
    CORRELATION_DATA,
    USER,
    CONTENT_TYPE,
]);
pub(crate) const PUBLISH_FROM_SERVER: Allowed = repeating(
    set(&[
        PAYLOAD_FORMAT,
        MESSAGE_EXPIRY,
        TOPIC_ALIAS,
        RESPONSE_TOPIC,
        CORRELATION_DATA,
        USER,
        SUBSCRIPTION_ID,
        CONTENT_TYPE,
    ]),
    SUBSCRIPTION_ID,
);
pub(crate) const ACK: Allowed = set(&[REASON_STRING, USER]);
pub(crate) const SUBSCRIBE: Allowed = set(&[SUBSCRIPTION_ID, USER]);
pub(crate) const UNSUBSCRIBE: Allowed = set(&[USER]);
/// Session Expiry is the client's to send, Server Reference the server's.
pub(crate) const DISCONNECT_FROM_CLIENT: Allowed = set(&[SESSION_EXPIRY, REASON_STRING, USER]);
pub(crate) const DISCONNECT_FROM_SERVER: Allowed = set(&[REASON_STRING, USER, SERVER_REFERENCE]);
pub(crate) const AUTH: Allowed = set(&[AUTH_METHOD, AUTH_DATA, REASON_STRING, USER]);

/// One property, decoded and range-checked.
enum Property {
    PayloadFormatUtf8(bool),
    MessageExpiry(u32),
    ContentType(Bytes),
    ResponseTopic(Topic),
    CorrelationData(Bytes),
    SubscriptionId(NonZeroU32),
    SessionExpiry(u32),
    AssignedClientId(ClientId),
    ServerKeepAlive(u16),
    AuthMethod(Bytes),
    AuthData(Bytes),
    RequestProblem(bool),
    WillDelay(u32),
    RequestResponse(bool),
    ResponseInfo(Bytes),
    ServerReference(Bytes),
    ReasonString(Bytes),
    ReceiveMaximum(NonZeroU16),
    TopicAliasMaximum(u16),
    TopicAlias(NonZeroU16),
    MaximumQos(Qos),
    RetainAvailable(bool),
    User(UserProperty),
    MaximumPacketSize(NonZeroU32),
    WildcardAvailable(bool),
    SubscriptionIdAvailable(bool),
    SharedAvailable(bool),
}

fn flag(reader: &mut Reader) -> Result<bool, DecodeError> {
    match reader.byte()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DecodeError::InvalidProperty),
    }
}

fn read_one(id: u8, reader: &mut Reader) -> Result<Property, DecodeError> {
    use Property::*;
    let invalid = DecodeError::InvalidProperty;
    Ok(match id {
        PAYLOAD_FORMAT => PayloadFormatUtf8(flag(reader)?),
        MESSAGE_EXPIRY => MessageExpiry(reader.u32()?),
        CONTENT_TYPE => ContentType(reader.string()?),
        RESPONSE_TOPIC => ResponseTopic(Topic::new(reader.string()?).map_err(|_| invalid)?),
        CORRELATION_DATA => CorrelationData(reader.binary()?),
        SUBSCRIPTION_ID => SubscriptionId(NonZeroU32::new(reader.varint()?).ok_or(invalid)?),
        SESSION_EXPIRY => SessionExpiry(reader.u32()?),
        ASSIGNED_CLIENT_ID => {
            AssignedClientId(ClientId::new(reader.string()?).map_err(|_| invalid)?)
        }
        SERVER_KEEP_ALIVE => ServerKeepAlive(reader.u16()?),
        AUTH_METHOD => AuthMethod(reader.string()?),
        AUTH_DATA => AuthData(reader.binary()?),
        REQUEST_PROBLEM => RequestProblem(flag(reader)?),
        WILL_DELAY => WillDelay(reader.u32()?),
        REQUEST_RESPONSE => RequestResponse(flag(reader)?),
        RESPONSE_INFO => ResponseInfo(reader.string()?),
        SERVER_REFERENCE => ServerReference(reader.string()?),
        REASON_STRING => ReasonString(reader.string()?),
        RECEIVE_MAXIMUM => ReceiveMaximum(NonZeroU16::new(reader.u16()?).ok_or(invalid)?),
        TOPIC_ALIAS_MAXIMUM => TopicAliasMaximum(reader.u16()?),
        TOPIC_ALIAS => {
            TopicAlias(NonZeroU16::new(reader.u16()?).ok_or(DecodeError::TopicAliasInvalid)?)
        }
        MAXIMUM_QOS => MaximumQos(match reader.byte()? {
            0 => Qos::AtMostOnce,
            1 => Qos::AtLeastOnce,
            _ => return Err(invalid),
        }),
        RETAIN_AVAILABLE => RetainAvailable(flag(reader)?),
        USER => User(UserProperty {
            name: reader.string()?,
            value: reader.string()?,
        }),
        MAXIMUM_PACKET_SIZE => MaximumPacketSize(NonZeroU32::new(reader.u32()?).ok_or(invalid)?),
        WILDCARD_AVAILABLE => WildcardAvailable(flag(reader)?),
        SUBSCRIPTION_ID_AVAILABLE => SubscriptionIdAvailable(flag(reader)?),
        SHARED_AVAILABLE => SharedAvailable(flag(reader)?),
        _ => return Err(DecodeError::UnknownProperty),
    })
}

/// Reads a whole section and hands each property to `apply`.
fn read_section(
    reader: &mut Reader,
    allowed: &Allowed,
    limits: &Limits,
    apply: &mut dyn FnMut(Property) -> Result<(), DecodeError>,
) -> Result<(), DecodeError> {
    let length = usize::try_from(reader.varint()?).map_err(|_| DecodeError::Truncated)?;
    let mut section = Reader(reader.take(length)?);
    let mut seen = 0u64;
    let mut users = 0usize;
    while !section.is_empty() {
        let id = u8::try_from(section.varint()?).map_err(|_| DecodeError::UnknownProperty)?;
        let bit = 1u64
            .checked_shl(u32::from(id))
            .ok_or(DecodeError::UnknownProperty)?;
        // Unknown first: an identifier no packet may carry is malformed, one
        // this packet may not carry is a protocol error.
        let property = read_one(id, &mut section)?;
        if allowed.ids & bit == 0 {
            return Err(DecodeError::PropertyNotAllowed);
        }
        if allowed.repeatable & bit == 0 && seen & bit != 0 {
            return Err(DecodeError::DuplicateProperty);
        }
        seen |= bit;
        if id == USER {
            users = users
                .checked_add(1)
                .ok_or(DecodeError::TooManyUserProperties)?;
            if users > limits.max_user_properties {
                return Err(DecodeError::TooManyUserProperties);
            }
        }
        apply(property)?;
    }
    Ok(())
}

pub(crate) fn connect(
    reader: &mut Reader,
    limits: &Limits,
) -> Result<ConnectProperties, DecodeError> {
    let mut out = ConnectProperties::default();
    read_section(reader, &CONNECT, limits, &mut |property| {
        match property {
            Property::SessionExpiry(v) => out.session_expiry_secs = v,
            Property::ReceiveMaximum(v) => out.receive_maximum = v,
            Property::MaximumPacketSize(v) => out.maximum_packet_size = Some(v),
            Property::TopicAliasMaximum(v) => out.topic_alias_maximum = v,
            Property::RequestResponse(v) => out.request_response_information = v,
            Property::RequestProblem(v) => out.request_problem_information = v,
            Property::User(v) => out.user.push(v),
            Property::AuthMethod(v) => out.authentication_method = Some(v),
            Property::AuthData(v) => out.authentication_data = Some(v),
            _ => return Err(DecodeError::PropertyNotAllowed),
        }
        Ok(())
    })?;
    // Authentication Data without a method means nothing (§3.1.2.11.10).
    if out.authentication_data.is_some() && out.authentication_method.is_none() {
        return Err(DecodeError::InvalidProperty);
    }
    Ok(out)
}

pub(crate) fn will(reader: &mut Reader, limits: &Limits) -> Result<WillProperties, DecodeError> {
    let mut out = WillProperties::default();
    read_section(reader, &WILL, limits, &mut |property| {
        match property {
            Property::WillDelay(v) => out.delay_secs = v,
            Property::PayloadFormatUtf8(v) => out.payload_format_utf8 = v,
            Property::MessageExpiry(v) => out.message_expiry_secs = Some(v),
            Property::ContentType(v) => out.content_type = Some(v),
            Property::ResponseTopic(v) => out.response_topic = Some(v),
            Property::CorrelationData(v) => out.correlation_data = Some(v),
            Property::User(v) => out.user.push(v),
            _ => return Err(DecodeError::PropertyNotAllowed),
        }
        Ok(())
    })?;
    Ok(out)
}

pub(crate) fn connack(
    reader: &mut Reader,
    limits: &Limits,
) -> Result<ConnAckProperties, DecodeError> {
    let mut out = ConnAckProperties::default();
    read_section(reader, &CONNACK, limits, &mut |property| {
        match property {
            Property::SessionExpiry(v) => out.session_expiry_secs = Some(v),
            Property::ReceiveMaximum(v) => out.receive_maximum = Some(v),
            Property::MaximumQos(v) => out.maximum_qos = Some(v),
            Property::RetainAvailable(v) => out.retain_available = Some(v),
            Property::MaximumPacketSize(v) => out.maximum_packet_size = Some(v),
            Property::AssignedClientId(v) => out.assigned_client_id = Some(v),
            Property::TopicAliasMaximum(v) => out.topic_alias_maximum = Some(v),
            Property::ReasonString(v) => out.reason_string = Some(v),
            Property::User(v) => out.user.push(v),
            Property::WildcardAvailable(v) => out.wildcard_subscription_available = Some(v),
            Property::SubscriptionIdAvailable(v) => {
                out.subscription_identifiers_available = Some(v)
            }
            Property::SharedAvailable(v) => out.shared_subscription_available = Some(v),
            Property::ServerKeepAlive(v) => out.server_keep_alive = Some(v),
            Property::ResponseInfo(v) => out.response_information = Some(v),
            Property::ServerReference(v) => out.server_reference = Some(v),
            Property::AuthMethod(v) => out.authentication_method = Some(v),
            Property::AuthData(v) => out.authentication_data = Some(v),
            _ => return Err(DecodeError::PropertyNotAllowed),
        }
        Ok(())
    })?;
    Ok(out)
}

pub(crate) fn publish(
    reader: &mut Reader,
    from_server: bool,
    limits: &Limits,
) -> Result<Option<Box<PublishProperties>>, DecodeError> {
    let allowed = if from_server {
        &PUBLISH_FROM_SERVER
    } else {
        &PUBLISH_FROM_CLIENT
    };
    let mut out = PublishProperties::default();
    let mut any = false;
    read_section(reader, allowed, limits, &mut |property| {
        any = true;
        match property {
            Property::PayloadFormatUtf8(v) => out.payload_format_utf8 = v,
            Property::MessageExpiry(v) => out.message_expiry_secs = Some(v),
            Property::TopicAlias(v) => out.topic_alias = Some(v),
            Property::ResponseTopic(v) => out.response_topic = Some(v),
            Property::CorrelationData(v) => out.correlation_data = Some(v),
            Property::User(v) => out.user.push(v),
            Property::SubscriptionId(v) => out.subscription_identifiers.push(v),
            Property::ContentType(v) => out.content_type = Some(v),
            _ => return Err(DecodeError::PropertyNotAllowed),
        }
        Ok(())
    })?;
    // A section that only restated defaults is no section.
    Ok((any && out != PublishProperties::default()).then(|| Box::new(out)))
}

pub(crate) fn ack(reader: &mut Reader, limits: &Limits) -> Result<AckProperties, DecodeError> {
    let mut out = AckProperties::default();
    read_section(reader, &ACK, limits, &mut |property| {
        match property {
            Property::ReasonString(v) => out.reason_string = Some(v),
            Property::User(v) => out.user.push(v),
            _ => return Err(DecodeError::PropertyNotAllowed),
        }
        Ok(())
    })?;
    Ok(out)
}

pub(crate) fn subscribe(
    reader: &mut Reader,
    limits: &Limits,
) -> Result<(Option<NonZeroU32>, Vec<UserProperty>), DecodeError> {
    let mut id = None;
    let mut user = Vec::new();
    read_section(reader, &SUBSCRIBE, limits, &mut |property| {
        match property {
            Property::SubscriptionId(v) => id = Some(v),
            Property::User(v) => user.push(v),
            _ => return Err(DecodeError::PropertyNotAllowed),
        }
        Ok(())
    })?;
    Ok((id, user))
}

pub(crate) fn unsubscribe(
    reader: &mut Reader,
    limits: &Limits,
) -> Result<Vec<UserProperty>, DecodeError> {
    let mut user = Vec::new();
    read_section(reader, &UNSUBSCRIBE, limits, &mut |property| {
        match property {
            Property::User(v) => user.push(v),
            _ => return Err(DecodeError::PropertyNotAllowed),
        }
        Ok(())
    })?;
    Ok(user)
}

pub(crate) fn disconnect(
    reader: &mut Reader,
    from_server: bool,
    limits: &Limits,
) -> Result<DisconnectProperties, DecodeError> {
    let allowed = if from_server {
        &DISCONNECT_FROM_SERVER
    } else {
        &DISCONNECT_FROM_CLIENT
    };
    let mut out = DisconnectProperties::default();
    read_section(reader, allowed, limits, &mut |property| {
        match property {
            Property::SessionExpiry(v) => out.session_expiry_secs = Some(v),
            Property::ReasonString(v) => out.reason_string = Some(v),
            Property::User(v) => out.user.push(v),
            Property::ServerReference(v) => out.server_reference = Some(v),
            _ => return Err(DecodeError::PropertyNotAllowed),
        }
        Ok(())
    })?;
    Ok(out)
}

pub(crate) fn auth(reader: &mut Reader, limits: &Limits) -> Result<AuthProperties, DecodeError> {
    let mut out = AuthProperties::default();
    read_section(reader, &AUTH, limits, &mut |property| {
        match property {
            Property::AuthMethod(v) => out.authentication_method = Some(v),
            Property::AuthData(v) => out.authentication_data = Some(v),
            Property::ReasonString(v) => out.reason_string = Some(v),
            Property::User(v) => out.user.push(v),
            _ => return Err(DecodeError::PropertyNotAllowed),
        }
        Ok(())
    })?;
    Ok(out)
}

/// Writes one section: properties into a scratch buffer, then its length and
/// the bytes. The scratch buffer is one allocation per encoded MQTT 5 packet
/// that has properties.
// ponytail: scratch buffer per section; write in place behind a reserved
// length if an encode benchmark shows it (phase 5).
pub(crate) struct Writer(BytesMut);

impl Writer {
    pub fn new() -> Self {
        Self(BytesMut::new())
    }

    pub fn flag(&mut self, id: u8, value: Option<bool>) {
        if let Some(value) = value {
            self.0.put_u8(id);
            self.0.put_u8(u8::from(value));
        }
    }

    pub fn u16(&mut self, id: u8, value: Option<u16>) {
        if let Some(value) = value {
            self.0.put_u8(id);
            self.0.put_u16(value);
        }
    }

    pub fn u32(&mut self, id: u8, value: Option<u32>) {
        if let Some(value) = value {
            self.0.put_u8(id);
            self.0.put_u32(value);
        }
    }

    pub fn varint(&mut self, id: u8, value: u32) -> Result<(), EncodeError> {
        self.0.put_u8(id);
        put_varint(value, &mut self.0)
    }

    pub fn bytes(&mut self, id: u8, value: Option<&[u8]>) -> Result<(), EncodeError> {
        if let Some(value) = value {
            self.0.put_u8(id);
            put_binary(value, &mut self.0)?;
        }
        Ok(())
    }

    pub fn user(&mut self, user: &[UserProperty]) -> Result<(), EncodeError> {
        for property in user {
            self.0.put_u8(USER);
            put_binary(&property.name, &mut self.0)?;
            put_binary(&property.value, &mut self.0)?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn finish(self, out: &mut BytesMut) -> Result<(), EncodeError> {
        let length = u32::try_from(self.0.len()).map_err(|_| EncodeError::TooLarge)?;
        put_varint(length, out)?;
        out.put_slice(&self.0);
        Ok(())
    }
}

pub(crate) fn write_connect(p: &ConnectProperties) -> Result<Writer, EncodeError> {
    let default = ConnectProperties::default();
    let mut w = Writer::new();
    w.u32(
        SESSION_EXPIRY,
        (p.session_expiry_secs != 0).then_some(p.session_expiry_secs),
    );
    w.u16(
        RECEIVE_MAXIMUM,
        (p.receive_maximum != default.receive_maximum).then_some(p.receive_maximum.get()),
    );
    w.u32(
        MAXIMUM_PACKET_SIZE,
        p.maximum_packet_size.map(NonZeroU32::get),
    );
    w.u16(
        TOPIC_ALIAS_MAXIMUM,
        (p.topic_alias_maximum != 0).then_some(p.topic_alias_maximum),
    );
    w.flag(
        REQUEST_RESPONSE,
        p.request_response_information.then_some(true),
    );
    w.flag(
        REQUEST_PROBLEM,
        (!p.request_problem_information).then_some(false),
    );
    w.user(&p.user)?;
    w.bytes(AUTH_METHOD, p.authentication_method.as_deref())?;
    w.bytes(AUTH_DATA, p.authentication_data.as_deref())?;
    Ok(w)
}

pub(crate) fn write_will(p: &WillProperties) -> Result<Writer, EncodeError> {
    let mut w = Writer::new();
    w.u32(WILL_DELAY, (p.delay_secs != 0).then_some(p.delay_secs));
    w.flag(PAYLOAD_FORMAT, p.payload_format_utf8.then_some(true));
    w.u32(MESSAGE_EXPIRY, p.message_expiry_secs);
    w.bytes(CONTENT_TYPE, p.content_type.as_deref())?;
    w.bytes(
        RESPONSE_TOPIC,
        p.response_topic.as_ref().map(Topic::as_bytes),
    )?;
    w.bytes(CORRELATION_DATA, p.correlation_data.as_deref())?;
    w.user(&p.user)?;
    Ok(w)
}

pub(crate) fn write_connack(p: &ConnAckProperties) -> Result<Writer, EncodeError> {
    let mut w = Writer::new();
    w.u32(SESSION_EXPIRY, p.session_expiry_secs);
    w.u16(RECEIVE_MAXIMUM, p.receive_maximum.map(NonZeroU16::get));
    if let Some(qos) = p.maximum_qos {
        w.0.put_u8(MAXIMUM_QOS);
        w.0.put_u8(qos as u8);
    }
    w.flag(RETAIN_AVAILABLE, p.retain_available);
    w.u32(
        MAXIMUM_PACKET_SIZE,
        p.maximum_packet_size.map(NonZeroU32::get),
    );
    w.bytes(
        ASSIGNED_CLIENT_ID,
        p.assigned_client_id.as_ref().map(ClientId::as_bytes),
    )?;
    w.u16(TOPIC_ALIAS_MAXIMUM, p.topic_alias_maximum);
    w.bytes(REASON_STRING, p.reason_string.as_deref())?;
    w.user(&p.user)?;
    w.flag(WILDCARD_AVAILABLE, p.wildcard_subscription_available);
    w.flag(
        SUBSCRIPTION_ID_AVAILABLE,
        p.subscription_identifiers_available,
    );
    w.flag(SHARED_AVAILABLE, p.shared_subscription_available);
    w.u16(SERVER_KEEP_ALIVE, p.server_keep_alive);
    w.bytes(RESPONSE_INFO, p.response_information.as_deref())?;
    w.bytes(SERVER_REFERENCE, p.server_reference.as_deref())?;
    w.bytes(AUTH_METHOD, p.authentication_method.as_deref())?;
    w.bytes(AUTH_DATA, p.authentication_data.as_deref())?;
    Ok(w)
}

pub(crate) fn write_publish(p: Option<&PublishProperties>) -> Result<Writer, EncodeError> {
    let mut w = Writer::new();
    let Some(p) = p else { return Ok(w) };
    w.flag(PAYLOAD_FORMAT, p.payload_format_utf8.then_some(true));
    w.u32(MESSAGE_EXPIRY, p.message_expiry_secs);
    w.u16(TOPIC_ALIAS, p.topic_alias.map(NonZeroU16::get));
    w.bytes(
        RESPONSE_TOPIC,
        p.response_topic.as_ref().map(Topic::as_bytes),
    )?;
    w.bytes(CORRELATION_DATA, p.correlation_data.as_deref())?;
    w.user(&p.user)?;
    for id in &p.subscription_identifiers {
        w.varint(SUBSCRIPTION_ID, id.get())?;
    }
    w.bytes(CONTENT_TYPE, p.content_type.as_deref())?;
    Ok(w)
}

pub(crate) fn write_ack(p: &AckProperties) -> Result<Writer, EncodeError> {
    let mut w = Writer::new();
    w.bytes(REASON_STRING, p.reason_string.as_deref())?;
    w.user(&p.user)?;
    Ok(w)
}

pub(crate) fn write_subscribe(
    id: Option<NonZeroU32>,
    user: &[UserProperty],
) -> Result<Writer, EncodeError> {
    let mut w = Writer::new();
    if let Some(id) = id {
        w.varint(SUBSCRIPTION_ID, id.get())?;
    }
    w.user(user)?;
    Ok(w)
}

pub(crate) fn write_disconnect(p: &DisconnectProperties) -> Result<Writer, EncodeError> {
    let mut w = Writer::new();
    w.u32(SESSION_EXPIRY, p.session_expiry_secs);
    w.bytes(REASON_STRING, p.reason_string.as_deref())?;
    w.user(&p.user)?;
    w.bytes(SERVER_REFERENCE, p.server_reference.as_deref())?;
    Ok(w)
}

pub(crate) fn write_auth(p: &AuthProperties) -> Result<Writer, EncodeError> {
    let mut w = Writer::new();
    w.bytes(AUTH_METHOD, p.authentication_method.as_deref())?;
    w.bytes(AUTH_DATA, p.authentication_data.as_deref())?;
    w.bytes(REASON_STRING, p.reason_string.as_deref())?;
    w.user(&p.user)?;
    Ok(w)
}
