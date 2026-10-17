//! Incremental decoding of MQTT 3.1.1 and 5 packets, in either direction.
//!
//! Every length in a packet is remote-controlled, so this module refuses
//! indexing, unchecked arithmetic and truncating casts.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use bytes::{Bytes, BytesMut};
use domain::{ClientId, Topic, TopicFilter};

use crate::packet::{
    Auth, AuthProperties, ConnAck, Connect, ConnectProperties, Disconnect, DisconnectProperties,
    Packet, PubAck, Publish, Qos, RetainHandling, SubAck, Subscribe, SubscriptionOptions, UnsubAck,
    Unsubscribe, Will,
};
use crate::reason::{ReasonCode, allowed};
use crate::wire::Reader;
use crate::{DecodeError, Limits, Sender, Version, properties};

/// Remaining Length is at most four bytes (§2.1.4).
const MAX_LENGTH_BYTES: usize = 4;

/// Decodes one packet `from` the given side, consuming exactly its bytes.
///
/// `version` is the connection's, `None` before its CONNECT: then only a
/// CONNECT is accepted, and it says which version the rest will be.
///
/// Returns `Ok(None)` while the packet is incomplete; `buf` is then
/// untouched. An error means the connection is unusable: an MQTT stream has
/// no point to resynchronize at. `DecodeError::reason` says which reason
/// code an MQTT 5 peer is told.
pub fn decode(
    buf: &mut BytesMut,
    version: Option<Version>,
    from: Sender,
    limits: &Limits,
) -> Result<Option<Packet>, DecodeError> {
    let Some(header) = Header::peek(buf, limits)? else {
        return Ok(None);
    };
    if buf.len() < header.total {
        return Ok(None);
    }
    let mut reader = Reader(buf.split_to(header.total).freeze());
    reader.take(header.length)?;

    let version = match (version, header.kind, from) {
        (None, 1, Sender::Client) => None,
        (None, _, _) => return Err(DecodeError::NotConnected),
        (Some(_), 1, _) => return Err(DecodeError::UnexpectedPacketType(1)),
        (Some(version), _, _) => Some(version),
    };
    let Some(version) = version else {
        require_flags(header, 0)?;
        return connect(reader, limits).map(Some);
    };
    let v5 = version == Version::V5;
    let server = from == Sender::Server;

    let packet = match (header.kind, from) {
        (2, Sender::Server) => {
            require_flags(header, 0)?;
            connack(reader, v5, limits)?
        }
        (3, _) => publish(header.flags, reader, v5, server, limits)?,
        (4, _) => {
            require_flags(header, 0)?;
            puback(reader, v5, limits)?
        }
        (8, Sender::Client) => {
            require_flags(header, 0b0010)?;
            subscribe(reader, v5, limits)?
        }
        (9, Sender::Server) => {
            require_flags(header, 0)?;
            suback(reader, v5, limits)?
        }
        (10, Sender::Client) => {
            require_flags(header, 0b0010)?;
            unsubscribe(reader, v5, limits)?
        }
        (11, Sender::Server) => {
            require_flags(header, 0)?;
            unsuback(reader, v5, limits)?
        }
        (12, Sender::Client) => {
            require_flags(header, 0)?;
            reader.finish()?;
            Packet::PingReq
        }
        (13, Sender::Server) => {
            require_flags(header, 0)?;
            reader.finish()?;
            Packet::PingResp
        }
        // 3.1.1 has no DISCONNECT from the server (§3.14).
        (14, Sender::Server) if !v5 => return Err(DecodeError::UnexpectedPacketType(14)),
        (14, _) => {
            require_flags(header, 0)?;
            disconnect(reader, v5, server, limits)?
        }
        // AUTH is reserved in 3.1.1.
        (15, _) if !v5 => return Err(DecodeError::UnknownPacketType(15)),
        (15, _) => {
            require_flags(header, 0)?;
            auth(reader, limits)?
        }
        // PUBREC, PUBREL, PUBCOMP: QoS 2, phase 6 (ADR-0003). Packets the
        // other side never sends.
        (kind @ 1..=14, _) => return Err(DecodeError::UnexpectedPacketType(kind)),
        (kind, _) => return Err(DecodeError::UnknownPacketType(kind)),
    };
    Ok(Some(packet))
}

#[derive(Clone, Copy)]
struct Header {
    kind: u8,
    flags: u8,
    /// Bytes of fixed header: type byte plus Remaining Length.
    length: usize,
    /// Fixed header plus Remaining Length.
    total: usize,
}

impl Header {
    fn peek(buf: &[u8], limits: &Limits) -> Result<Option<Self>, DecodeError> {
        let Some(&first) = buf.first() else {
            return Ok(None);
        };
        let Some((remaining, length)) = remaining_length(buf)? else {
            return Ok(None);
        };
        let total = length
            .checked_add(remaining)
            .ok_or(DecodeError::MalformedVarint)?;
        // Checked before waiting for the rest: an oversized packet must not
        // pin a read buffer until it arrives.
        if total > limits.max_packet_bytes {
            return Err(DecodeError::PacketTooLarge);
        }
        Ok(Some(Self {
            kind: first >> 4,
            flags: first & 0x0f,
            length,
            total,
        }))
    }
}

/// The Remaining Length and the length of the whole fixed header.
fn remaining_length(buf: &[u8]) -> Result<Option<(usize, usize)>, DecodeError> {
    let mut value: usize = 0;
    let mut multiplier: usize = 1;
    for (index, &byte) in buf.iter().enumerate().skip(1).take(MAX_LENGTH_BYTES) {
        let part = usize::from(byte & 0x7f)
            .checked_mul(multiplier)
            .ok_or(DecodeError::MalformedVarint)?;
        value = value
            .checked_add(part)
            .ok_or(DecodeError::MalformedVarint)?;
        if byte & 0x80 == 0 {
            let length = index.checked_add(1).ok_or(DecodeError::MalformedVarint)?;
            return Ok(Some((value, length)));
        }
        multiplier = multiplier
            .checked_mul(128)
            .ok_or(DecodeError::MalformedVarint)?;
    }
    // A fifth continuation byte cannot be part of a valid length.
    if buf.len() > MAX_LENGTH_BYTES {
        return Err(DecodeError::MalformedVarint);
    }
    Ok(None)
}

fn require_flags(header: Header, expected: u8) -> Result<(), DecodeError> {
    if header.flags == expected {
        Ok(())
    } else {
        Err(DecodeError::InvalidFlags(header.kind))
    }
}

fn reason(byte: u8, set: &[ReasonCode]) -> Result<ReasonCode, DecodeError> {
    let code = ReasonCode::from_u8(byte).ok_or(DecodeError::UnknownReasonCode(byte))?;
    if set.contains(&code) {
        Ok(code)
    } else {
        Err(DecodeError::ReasonNotAllowed(byte))
    }
}

fn connect(mut reader: Reader, limits: &Limits) -> Result<Packet, DecodeError> {
    if reader.string()?.as_ref() != b"MQTT" {
        return Err(DecodeError::InvalidProtocolName);
    }
    let version = match reader.byte()? {
        4 => Version::V311,
        5 => Version::V5,
        level => return Err(DecodeError::UnsupportedProtocolLevel(level)),
    };
    let v5 = version == Version::V5;

    let flags = reader.byte()?;
    let has_will = flags & 0b0000_0100 != 0;
    let will_qos = (flags & 0b0001_1000) >> 3;
    let will_retain = flags & 0b0010_0000 != 0;
    let has_password = flags & 0b0100_0000 != 0;
    let has_username = flags & 0b1000_0000 != 0;
    // Reserved bit, will fields without a will; 3.1.1 also forbids a
    // password without a user name, MQTT 5 allows it (§3.1.2.9).
    if flags & 0b0000_0001 != 0
        || (!has_will && (will_qos != 0 || will_retain))
        || (!v5 && has_password && !has_username)
    {
        return Err(DecodeError::InvalidConnectFlags);
    }
    let will_qos = Qos::from_bits(will_qos).ok_or(DecodeError::InvalidQos(will_qos))?;

    let keep_alive_secs = reader.u16()?;
    let properties = if v5 {
        properties::connect(&mut reader, limits)?
    } else {
        ConnectProperties::default()
    };
    let client_id = reader.string()?;
    if client_id.len() > limits.max_client_id_bytes {
        return Err(DecodeError::ClientIdTooLong);
    }
    let client_id = ClientId::new(client_id).map_err(|_| DecodeError::InvalidUtf8)?;

    let will = if has_will {
        let will_properties = if v5 {
            properties::will(&mut reader, limits)?
        } else {
            Default::default()
        };
        Some(Will {
            topic: topic(reader.string()?, limits)?,
            payload: reader.binary()?,
            qos: will_qos,
            retain: will_retain,
            properties: will_properties,
        })
    } else {
        None
    };
    let username = has_username.then(|| reader.string()).transpose()?;
    let password = has_password.then(|| reader.binary()).transpose()?;
    // Nothing may follow the fields the flags announced.
    reader.finish()?;

    Ok(Packet::Connect(Box::new(Connect {
        version,
        client_id,
        clean_start: flags & 0b0000_0010 != 0,
        keep_alive_secs,
        will,
        username,
        password,
        properties,
    })))
}

fn connack(mut reader: Reader, v5: bool, limits: &Limits) -> Result<Packet, DecodeError> {
    let flags = reader.byte()?;
    if flags & 0b1111_1110 != 0 {
        return Err(DecodeError::InvalidFlags(2));
    }
    let session_present = flags & 1 != 0;
    let code = reader.byte()?;
    let reason = if v5 {
        reason(code, allowed::CONNACK)?
    } else {
        ReasonCode::from_connack_v311(code).ok_or(DecodeError::UnknownReasonCode(code))?
    };
    // A refused connection has no session (§3.2.2.1.1).
    if session_present && reason != ReasonCode::Success {
        return Err(DecodeError::InvalidFlags(2));
    }
    let properties = if v5 {
        properties::connack(&mut reader, limits)?
    } else {
        Default::default()
    };
    reader.finish()?;
    Ok(Packet::ConnAck(Box::new(ConnAck {
        session_present,
        reason,
        properties,
    })))
}

fn publish(
    flags: u8,
    mut reader: Reader,
    v5: bool,
    from_server: bool,
    limits: &Limits,
) -> Result<Packet, DecodeError> {
    let duplicate = flags & 0b1000 != 0;
    let retain = flags & 0b0001 != 0;
    let qos_bits = (flags & 0b0110) >> 1;
    let qos = Qos::from_bits(qos_bits).ok_or(DecodeError::InvalidQos(qos_bits))?;
    if duplicate && qos == Qos::AtMostOnce {
        return Err(DecodeError::InvalidFlags(3));
    }

    let name = reader.string()?;
    let id = (qos != Qos::AtMostOnce)
        .then(|| reader.packet_id())
        .transpose()?;
    let properties = if v5 {
        properties::publish(&mut reader, from_server, limits)?
    } else {
        None
    };
    // An empty topic name is only a stand-in for a Topic Alias (§3.3.2.1).
    let topic = if name.is_empty() {
        let aliased = properties.as_ref().is_some_and(|p| p.topic_alias.is_some());
        if !aliased {
            return Err(DecodeError::TopicNameInvalid);
        }
        None
    } else {
        Some(topic(name, limits)?)
    };
    let payload = payload(reader.rest(), limits);
    Ok(Packet::Publish(Publish {
        topic,
        id,
        qos,
        duplicate,
        retain,
        payload,
        properties,
    }))
}

/// A PUBACK: in MQTT 5 the reason code and the properties may each be left
/// out when they would say "success, nothing more" (§3.4.2.1).
fn puback(mut reader: Reader, v5: bool, limits: &Limits) -> Result<Packet, DecodeError> {
    let id = reader.packet_id()?;
    let (reason, properties) = if v5 && !reader.is_empty() {
        let reason = reason(reader.byte()?, allowed::PUBACK)?;
        let properties = if reader.is_empty() {
            Default::default()
        } else {
            properties::ack(&mut reader, limits)?
        };
        (reason, properties)
    } else {
        (ReasonCode::Success, Default::default())
    };
    reader.finish()?;
    Ok(Packet::PubAck(PubAck {
        id,
        reason,
        properties,
    }))
}

fn subscribe(mut reader: Reader, v5: bool, limits: &Limits) -> Result<Packet, DecodeError> {
    let id = reader.packet_id()?;
    let (subscription_identifier, user) = if v5 {
        properties::subscribe(&mut reader, limits)?
    } else {
        (None, Vec::new())
    };
    let mut filters = Vec::new();
    while !reader.is_empty() {
        if filters.len() >= limits.max_filters_per_subscribe {
            return Err(DecodeError::TooManyFilters);
        }
        let filter = filter(reader.string()?, limits)?;
        let options = reader.byte()?;
        let reserved = if v5 { 0b1100_0000 } else { 0b1111_1100 };
        if options & reserved != 0 {
            return Err(DecodeError::InvalidSubscriptionOptions);
        }
        let qos_bits = options & 0b11;
        let qos = Qos::from_bits(qos_bits).ok_or(DecodeError::InvalidQos(qos_bits))?;
        let retain_handling = match (options >> 4) & 0b11 {
            0 => RetainHandling::OnSubscribe,
            1 => RetainHandling::OnNewSubscription,
            2 => RetainHandling::Never,
            _ => return Err(DecodeError::InvalidSubscriptionOptions),
        };
        let no_local = options & 0b0100 != 0;
        // No Local on a shared subscription is a protocol error (§3.8.3.1).
        if no_local && filter.as_bytes().starts_with(b"$share/") {
            return Err(DecodeError::InvalidSubscriptionOptions);
        }
        filters.push((
            filter,
            SubscriptionOptions {
                qos,
                no_local,
                retain_as_published: options & 0b1000 != 0,
                retain_handling,
            },
        ));
    }
    if filters.is_empty() {
        return Err(DecodeError::EmptyFilterList);
    }
    Ok(Packet::Subscribe(Subscribe {
        id,
        subscription_identifier,
        user,
        filters,
    }))
}

fn suback(mut reader: Reader, v5: bool, limits: &Limits) -> Result<Packet, DecodeError> {
    let id = reader.packet_id()?;
    let properties = if v5 {
        properties::ack(&mut reader, limits)?
    } else {
        Default::default()
    };
    let mut reasons = Vec::new();
    while !reader.is_empty() {
        if reasons.len() >= limits.max_filters_per_subscribe {
            return Err(DecodeError::TooManyFilters);
        }
        let code = reader.byte()?;
        reasons.push(if v5 {
            reason(code, allowed::SUBACK)?
        } else {
            match code {
                0x00 => ReasonCode::Success,
                0x01 => ReasonCode::GrantedQos1,
                0x02 => ReasonCode::GrantedQos2,
                0x80 => ReasonCode::UnspecifiedError,
                _ => return Err(DecodeError::UnknownReasonCode(code)),
            }
        });
    }
    if reasons.is_empty() {
        return Err(DecodeError::EmptyFilterList);
    }
    Ok(Packet::SubAck(SubAck {
        id,
        reasons,
        properties,
    }))
}

fn unsubscribe(mut reader: Reader, v5: bool, limits: &Limits) -> Result<Packet, DecodeError> {
    let id = reader.packet_id()?;
    let user = if v5 {
        properties::unsubscribe(&mut reader, limits)?
    } else {
        Vec::new()
    };
    let mut filters = Vec::new();
    while !reader.is_empty() {
        if filters.len() >= limits.max_filters_per_subscribe {
            return Err(DecodeError::TooManyFilters);
        }
        filters.push(filter(reader.string()?, limits)?);
    }
    if filters.is_empty() {
        return Err(DecodeError::EmptyFilterList);
    }
    Ok(Packet::Unsubscribe(Unsubscribe { id, user, filters }))
}

fn unsuback(mut reader: Reader, v5: bool, limits: &Limits) -> Result<Packet, DecodeError> {
    let id = reader.packet_id()?;
    if !v5 {
        reader.finish()?;
        return Ok(Packet::UnsubAck(UnsubAck {
            id,
            reasons: Vec::new(),
            properties: Default::default(),
        }));
    }
    let properties = properties::ack(&mut reader, limits)?;
    let mut reasons = Vec::new();
    while !reader.is_empty() {
        if reasons.len() >= limits.max_filters_per_subscribe {
            return Err(DecodeError::TooManyFilters);
        }
        reasons.push(reason(reader.byte()?, allowed::UNSUBACK)?);
    }
    if reasons.is_empty() {
        return Err(DecodeError::EmptyFilterList);
    }
    Ok(Packet::UnsubAck(UnsubAck {
        id,
        reasons,
        properties,
    }))
}

/// A DISCONNECT. Empty, it is a normal disconnection; in MQTT 5 the reason
/// and the properties may each be left out the same way (§3.14.2.1).
fn disconnect(
    mut reader: Reader,
    v5: bool,
    from_server: bool,
    limits: &Limits,
) -> Result<Packet, DecodeError> {
    let mut reason_code = ReasonCode::Success;
    let mut properties = DisconnectProperties::default();
    if v5 && !reader.is_empty() {
        reason_code = reason(reader.byte()?, allowed::DISCONNECT)?;
        // "Disconnect with Will Message" is the client's to send.
        if from_server && reason_code == ReasonCode::DisconnectWithWill {
            return Err(DecodeError::ReasonNotAllowed(reason_code.code()));
        }
        if !reader.is_empty() {
            properties = properties::disconnect(&mut reader, from_server, limits)?;
        }
    }
    reader.finish()?;
    Ok(Packet::Disconnect(Disconnect {
        reason: reason_code,
        properties,
    }))
}

fn auth(mut reader: Reader, limits: &Limits) -> Result<Packet, DecodeError> {
    let mut reason_code = ReasonCode::Success;
    let mut properties = AuthProperties::default();
    if !reader.is_empty() {
        reason_code = reason(reader.byte()?, allowed::AUTH)?;
        if !reader.is_empty() {
            properties = properties::auth(&mut reader, limits)?;
        }
    }
    reader.finish()?;
    Ok(Packet::Auth(Auth {
        reason: reason_code,
        properties,
    }))
}

fn topic(bytes: Bytes, limits: &Limits) -> Result<Topic, DecodeError> {
    check_size(&bytes, limits)?;
    Topic::new(bytes).map_err(|_| DecodeError::TopicNameInvalid)
}

fn filter(bytes: Bytes, limits: &Limits) -> Result<TopicFilter, DecodeError> {
    check_size(&bytes, limits).map_err(|_| DecodeError::TopicFilterInvalid)?;
    TopicFilter::new(bytes).map_err(|_| DecodeError::TopicFilterInvalid)
}

fn check_size(bytes: &[u8], limits: &Limits) -> Result<(), DecodeError> {
    let levels = bytes
        .iter()
        .filter(|&&byte| byte == b'/')
        .count()
        .checked_add(1)
        .ok_or(DecodeError::TopicNameInvalid)?;
    if bytes.len() > limits.max_topic_bytes || levels > limits.max_topic_levels {
        return Err(DecodeError::TopicNameInvalid);
    }
    Ok(())
}

fn payload(bytes: Bytes, limits: &Limits) -> Bytes {
    // A small payload sliced out of the read buffer keeps the whole buffer
    // chunk alive while it waits in a slow subscriber's queue.
    // One copy here, not one per subscriber.
    if bytes.len() <= limits.payload_copy_threshold_bytes {
        Bytes::copy_from_slice(&bytes)
    } else {
        bytes
    }
}
