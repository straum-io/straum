//! Encoding packets for MQTT 3.1.1 or 5.
//!
//! For 3.1.1 what the version cannot carry is left out — properties,
//! reason strings, the reason code of a PUBACK — and what it can carry in
//! its own terms is mapped: a CONNACK refusal to one of its five return
//! codes, a SUBACK failure to 0x80. A packet 3.1.1 has no form for at all —
//! AUTH, a DISCONNECT from the server — is refused (`EncodeError::NotInVersion`):
//! the caller closes the connection instead (3.1.1 has no server DISCONNECT).

use bytes::{BufMut, BytesMut};

use crate::packet::{
    Auth, ConnAck, Connect, Disconnect, Packet, PubAck, Publish, Qos, SubAck, Subscribe, UnsubAck,
    Unsubscribe,
};
use crate::properties::{self, Writer};
use crate::reason::{ReasonCode, allowed};
use crate::wire::{MAX_VARINT, put_binary, put_varint};
use crate::{EncodeError, Sender, Version};

/// Appends `packet`, as `from` sends it, to `out`.
pub fn encode(
    packet: &Packet,
    version: Version,
    from: Sender,
    out: &mut BytesMut,
) -> Result<(), EncodeError> {
    let v5 = version == Version::V5;
    // The body goes to a scratch buffer to learn its length; write
    // in place behind a reserved length if an encode benchmark asks (phase 5).
    let mut body = BytesMut::new();
    let first = match packet {
        Packet::Connect(connect) => {
            self::connect(connect, &mut body)?;
            0x10
        }
        Packet::ConnAck(connack) => {
            self::connack(connack, v5, &mut body)?;
            0x20
        }
        Packet::Publish(publish) => self::publish(publish, v5, from, &mut body)?,
        Packet::PubAck(puback) => {
            self::puback(puback, v5, &mut body)?;
            0x40
        }
        Packet::Subscribe(subscribe) => {
            self::subscribe(subscribe, v5, &mut body)?;
            0x82
        }
        Packet::SubAck(suback) => {
            self::suback(suback, v5, &mut body)?;
            0x90
        }
        Packet::Unsubscribe(unsubscribe) => {
            self::unsubscribe(unsubscribe, v5, &mut body)?;
            0xA2
        }
        Packet::UnsubAck(unsuback) => {
            self::unsuback(unsuback, v5, &mut body)?;
            0xB0
        }
        Packet::PingReq => 0xC0,
        Packet::PingResp => 0xD0,
        Packet::Disconnect(disconnect) => {
            if !v5 && from == Sender::Server {
                return Err(EncodeError::NotInVersion);
            }
            if v5 {
                self::disconnect(disconnect, from, &mut body)?;
            }
            0xE0
        }
        Packet::Auth(auth) => {
            if !v5 {
                return Err(EncodeError::NotInVersion);
            }
            self::auth(auth, &mut body)?;
            0xF0
        }
    };
    let length = u32::try_from(body.len()).map_err(|_| EncodeError::TooLarge)?;
    if length > MAX_VARINT {
        return Err(EncodeError::TooLarge);
    }
    out.put_u8(first);
    put_varint(length, out)?;
    out.put_slice(&body);
    Ok(())
}

fn check(reason: ReasonCode, set: &[ReasonCode]) -> Result<(), EncodeError> {
    if set.contains(&reason) {
        Ok(())
    } else {
        Err(EncodeError::ReasonNotAllowed)
    }
}

fn connect(c: &Connect, out: &mut BytesMut) -> Result<(), EncodeError> {
    let v5 = c.version == Version::V5;
    if !v5 && c.password.is_some() && c.username.is_none() {
        return Err(EncodeError::NotInVersion);
    }
    put_binary(b"MQTT", out)?;
    out.put_u8(if v5 { 5 } else { 4 });
    let mut flags = 0u8;
    if c.clean_start {
        flags |= 0b0000_0010;
    }
    if let Some(will) = &c.will {
        flags |= 0b0000_0100 | ((will.qos as u8) << 3);
        if will.retain {
            flags |= 0b0010_0000;
        }
    }
    if c.password.is_some() {
        flags |= 0b0100_0000;
    }
    if c.username.is_some() {
        flags |= 0b1000_0000;
    }
    out.put_u8(flags);
    out.put_u16(c.keep_alive_secs);
    if v5 {
        properties::write_connect(&c.properties)?.finish(out)?;
    }
    put_binary(c.client_id.as_bytes(), out)?;
    if let Some(will) = &c.will {
        if v5 {
            properties::write_will(&will.properties)?.finish(out)?;
        }
        put_binary(will.topic.as_bytes(), out)?;
        put_binary(&will.payload, out)?;
    }
    if let Some(username) = &c.username {
        put_binary(username, out)?;
    }
    if let Some(password) = &c.password {
        put_binary(password, out)?;
    }
    Ok(())
}

fn connack(c: &ConnAck, v5: bool, out: &mut BytesMut) -> Result<(), EncodeError> {
    if c.session_present && c.reason != ReasonCode::Success {
        return Err(EncodeError::ReasonNotAllowed);
    }
    out.put_u8(u8::from(c.session_present));
    if v5 {
        check(c.reason, allowed::CONNACK)?;
        out.put_u8(c.reason.code());
        properties::write_connack(&c.properties)?.finish(out)?;
    } else {
        out.put_u8(c.reason.connack_v311());
    }
    Ok(())
}

/// Returns the first byte: the flags live in it.
fn publish(p: &Publish, v5: bool, from: Sender, out: &mut BytesMut) -> Result<u8, EncodeError> {
    if (p.qos == Qos::AtMostOnce) != p.id.is_none() {
        return Err(EncodeError::Invalid);
    }
    let aliased = v5
        && p.properties
            .as_ref()
            .is_some_and(|props| props.topic_alias.is_some());
    let has_ids = p
        .properties
        .as_ref()
        .is_some_and(|props| !props.subscription_identifiers.is_empty());
    if v5 && has_ids && from == Sender::Client {
        return Err(EncodeError::Invalid);
    }
    match &p.topic {
        Some(topic) => put_binary(topic.as_bytes(), out)?,
        None if aliased => put_binary(b"", out)?,
        None => return Err(EncodeError::Invalid),
    }
    if let Some(id) = p.id {
        out.put_u16(id.get());
    }
    if v5 {
        properties::write_publish(p.properties.as_deref())?.finish(out)?;
    }
    out.put_slice(&p.payload);
    let mut first = 0x30 | ((p.qos as u8) << 1);
    if p.duplicate {
        first |= 0b1000;
    }
    if p.retain {
        first |= 0b0001;
    }
    Ok(first)
}

fn puback(p: &PubAck, v5: bool, out: &mut BytesMut) -> Result<(), EncodeError> {
    out.put_u16(p.id.get());
    if !v5 {
        return Ok(());
    }
    check(p.reason, allowed::PUBACK)?;
    let properties = properties::write_ack(&p.properties)?;
    // The short forms: say nothing that "success, nothing more" says.
    if p.reason == ReasonCode::Success && properties.is_empty() {
        return Ok(());
    }
    out.put_u8(p.reason.code());
    if !properties.is_empty() {
        properties.finish(out)?;
    }
    Ok(())
}

fn subscribe(s: &Subscribe, v5: bool, out: &mut BytesMut) -> Result<(), EncodeError> {
    if s.filters.is_empty() {
        return Err(EncodeError::Invalid);
    }
    out.put_u16(s.id.get());
    if v5 {
        properties::write_subscribe(s.subscription_identifier, &s.user)?.finish(out)?;
    }
    for (filter, options) in &s.filters {
        put_binary(filter.as_bytes(), out)?;
        let mut byte = options.qos as u8;
        if v5 {
            if options.no_local {
                byte |= 0b0100;
            }
            if options.retain_as_published {
                byte |= 0b1000;
            }
            byte |= (options.retain_handling as u8) << 4;
        }
        out.put_u8(byte);
    }
    Ok(())
}

fn suback(s: &SubAck, v5: bool, out: &mut BytesMut) -> Result<(), EncodeError> {
    if s.reasons.is_empty() {
        return Err(EncodeError::Invalid);
    }
    out.put_u16(s.id.get());
    if v5 {
        properties::write_ack(&s.properties)?.finish(out)?;
    }
    for &reason in &s.reasons {
        check(reason, allowed::SUBACK)?;
        out.put_u8(if v5 {
            reason.code()
        } else {
            reason.suback_v311()
        });
    }
    Ok(())
}

fn unsubscribe(u: &Unsubscribe, v5: bool, out: &mut BytesMut) -> Result<(), EncodeError> {
    if u.filters.is_empty() {
        return Err(EncodeError::Invalid);
    }
    out.put_u16(u.id.get());
    if v5 {
        let mut w = Writer::new();
        w.user(&u.user)?;
        w.finish(out)?;
    }
    for filter in &u.filters {
        put_binary(filter.as_bytes(), out)?;
    }
    Ok(())
}

fn unsuback(u: &UnsubAck, v5: bool, out: &mut BytesMut) -> Result<(), EncodeError> {
    out.put_u16(u.id.get());
    if !v5 {
        return Ok(());
    }
    if u.reasons.is_empty() {
        return Err(EncodeError::Invalid);
    }
    properties::write_ack(&u.properties)?.finish(out)?;
    for &reason in &u.reasons {
        check(reason, allowed::UNSUBACK)?;
        out.put_u8(reason.code());
    }
    Ok(())
}

fn disconnect(d: &Disconnect, from: Sender, out: &mut BytesMut) -> Result<(), EncodeError> {
    check(d.reason, allowed::DISCONNECT)?;
    // What only the other side may send (§3.14.2): a session expiry and
    // "with will" are the client's, a server reference the server's.
    let misplaced = match from {
        Sender::Server => {
            d.properties.session_expiry_secs.is_some() || d.reason == ReasonCode::DisconnectWithWill
        }
        Sender::Client => d.properties.server_reference.is_some(),
    };
    if misplaced {
        return Err(EncodeError::Invalid);
    }
    let properties = properties::write_disconnect(&d.properties)?;
    if d.reason == ReasonCode::Success && properties.is_empty() {
        return Ok(());
    }
    out.put_u8(d.reason.code());
    if !properties.is_empty() {
        properties.finish(out)?;
    }
    Ok(())
}

fn auth(a: &Auth, out: &mut BytesMut) -> Result<(), EncodeError> {
    check(a.reason, allowed::AUTH)?;
    let properties = properties::write_auth(&a.properties)?;
    if a.reason == ReasonCode::Success && properties.is_empty() {
        return Ok(());
    }
    out.put_u8(a.reason.code());
    if !properties.is_empty() {
        properties.finish(out)?;
    }
    Ok(())
}
