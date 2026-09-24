//! Encoding of the packets the broker sends.
//!
//! Every function appends to the caller's buffer and never clears it: the
//! writer task fills one reused buffer with a batch and writes it once.
//! Lengths are derived from remote input, so the same arithmetic rules as in
//! `decode` apply.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use bytes::{BufMut, BytesMut};
use domain::Topic;

use domain::DeliveryGuarantee;
use protocol::SubscribeResult;

use crate::packet::{ConnAck, PacketId};
use crate::{MqttError, MqttLimits};

/// Largest value Remaining Length can express (3.1.1 §2.2.3).
const MAX_REMAINING_LENGTH: usize = 268_435_455;

const CONNACK: u8 = 0x20;
const PUBLISH: u8 = 0x30;
const PUBACK: u8 = 0x40;
const SUBACK: u8 = 0x90;
const UNSUBACK: u8 = 0xb0;
const PINGRESP: u8 = 0xd0;

pub fn connack(buf: &mut BytesMut, ack: ConnAck) {
    let (session_present, code) = match ack {
        ConnAck::Accepted { session_present } => (u8::from(session_present), 0x00),
        ConnAck::Rejected(reason) => (0, reason.code()),
    };
    buf.reserve(4);
    buf.put_u8(CONNACK);
    buf.put_u8(2);
    buf.put_u8(session_present);
    buf.put_u8(code);
}

/// RETAIN marks a stored message being handed to a new subscriber; DUP marks
/// one this session was already given and did not acknowledge.
pub fn publish(
    buf: &mut BytesMut,
    topic: &Topic,
    id: Option<PacketId>,
    payload: &[u8],
    duplicate: bool,
    retain: bool,
    limits: &MqttLimits,
) -> Result<(), MqttError> {
    let topic = topic.as_bytes();
    // Checked before a single byte is written: a refused packet must not
    // leave a half-encoded header in the writer's buffer.
    let topic_length = u16::try_from(topic.len()).map_err(|_| MqttError::TopicTooLong)?;

    let mut remaining = add(2, topic.len())?;
    if id.is_some() {
        remaining = add(remaining, 2)?;
    }
    remaining = add(remaining, payload.len())?;
    let start = reserve(buf, remaining, limits)?;

    let qos = if id.is_some() { 0b0010 } else { 0 };
    // DUP says the client may already have this one (3.1.1 §3.3.1.1).
    let duplicate = if duplicate { 0b1000 } else { 0 };
    let retain = u8::from(retain);
    buf.put_u8(PUBLISH | qos | retain | duplicate);
    put_remaining_length(buf, remaining);
    put_string(buf, topic, topic_length);
    if let Some(id) = id {
        buf.put_u16(id.get());
    }
    buf.put_slice(payload);

    debug_assert_eq!(buf.len(), start.saturating_add(len_of(remaining)));
    Ok(())
}

pub fn puback(buf: &mut BytesMut, id: PacketId) {
    acknowledgement(buf, PUBACK, id);
}

pub fn unsuback(buf: &mut BytesMut, id: PacketId) {
    acknowledgement(buf, UNSUBACK, id);
}

/// `results` has one entry per filter of the SUBACK'd SUBSCRIBE, in order.
pub fn suback(
    buf: &mut BytesMut,
    id: PacketId,
    results: &[SubscribeResult],
    limits: &MqttLimits,
) -> Result<(), MqttError> {
    let remaining = add(2, results.len())?;
    reserve(buf, remaining, limits)?;

    buf.put_u8(SUBACK);
    put_remaining_length(buf, remaining);
    buf.put_u16(id.get());
    for result in results {
        buf.put_u8(suback_code(*result));
    }
    Ok(())
}

pub fn pingresp(buf: &mut BytesMut) {
    buf.reserve(2);
    buf.put_u8(PINGRESP);
    buf.put_u8(0);
}

fn acknowledgement(buf: &mut BytesMut, kind: u8, id: PacketId) {
    buf.reserve(4);
    buf.put_u8(kind);
    buf.put_u8(2);
    buf.put_u16(id.get());
}

/// Reserves room for one whole packet and returns the buffer length before it.
fn reserve(buf: &mut BytesMut, remaining: usize, limits: &MqttLimits) -> Result<usize, MqttError> {
    if remaining > MAX_REMAINING_LENGTH {
        return Err(MqttError::PacketTooLarge);
    }
    let total = len_of(remaining);
    if total > limits.max_packet_bytes {
        return Err(MqttError::PacketTooLarge);
    }
    buf.reserve(total);
    Ok(buf.len())
}

/// Whole packet size for a body of `remaining` bytes.
fn len_of(remaining: usize) -> usize {
    let header = match remaining {
        0..=127 => 2,
        128..=16_383 => 3,
        16_384..=2_097_151 => 4,
        _ => 5,
    };
    remaining.saturating_add(header)
}

fn add(left: usize, right: usize) -> Result<usize, MqttError> {
    left.checked_add(right).ok_or(MqttError::PacketTooLarge)
}

fn put_remaining_length(buf: &mut BytesMut, mut value: usize) {
    loop {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "value % 128 cannot exceed 127"
        )]
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            return;
        }
    }
}

/// The caller computes the length, so a string that cannot be expressed
/// is refused before anything is written.
fn put_string(buf: &mut BytesMut, value: &[u8], length: u16) {
    debug_assert_eq!(usize::from(length), value.len());
    buf.put_u16(length);
    buf.put_slice(value);
}

/// SUBACK return codes (3.1.1 §3.9.3). The granted guarantee is the broker's
/// decision; only the encoding of it is MQTT's.
fn suback_code(result: SubscribeResult) -> u8 {
    match result {
        SubscribeResult::Granted(DeliveryGuarantee::AtMostOnce) => 0x00,
        SubscribeResult::Granted(DeliveryGuarantee::AtLeastOnce) => 0x01,
        SubscribeResult::Failure => 0x80,
    }
}
