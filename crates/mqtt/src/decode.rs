//! Incremental decoding of client packets.
//!
//! Every length in a packet is remote-controlled, so this module refuses
//! indexing, unchecked arithmetic and truncating casts: the compiler enforces
//! what review would otherwise have to catch (CLAUDE.md §3).
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use bytes::{Bytes, BytesMut};
use domain::{ClientId, DeliveryGuarantee, Topic, TopicFilter};

use crate::packet::{Connect, Packet, PacketId, Publish, Subscribe, Unsubscribe};
use crate::{MqttError, MqttLimits};

/// Remaining Length is at most four bytes (3.1.1 §2.2.3).
const MAX_LENGTH_BYTES: usize = 4;

/// Decodes one packet, consuming exactly its bytes from `buf`.
///
/// Returns `Ok(None)` while the packet is still incomplete; `buf` is then
/// untouched and the caller reads more bytes. An error means the connection
/// is unusable: there is no resynchronization point in an MQTT stream.
pub fn decode(buf: &mut BytesMut, limits: &MqttLimits) -> Result<Option<Packet>, MqttError> {
    let Some(header) = Header::peek(buf, limits)? else {
        return Ok(None);
    };
    if buf.len() < header.total {
        return Ok(None);
    }

    let mut reader = Reader(buf.split_to(header.total).freeze());
    let _fixed_header = reader.take(header.length)?;

    let packet = match header.kind {
        1 => connect(reader, limits)?,
        3 => publish(header.flags, reader, limits)?,
        4 => {
            require_flags(header, 0)?;
            let packet = Packet::PubAck(reader.packet_id()?);
            reader.finish()?;
            packet
        }
        8 => {
            require_flags(header, 0b0010)?;
            subscribe(reader, limits)?
        }
        10 => {
            require_flags(header, 0b0010)?;
            unsubscribe(reader, limits)?
        }
        12 => {
            require_flags(header, 0)?;
            reader.finish()?;
            Packet::PingReq
        }
        14 => {
            require_flags(header, 0)?;
            reader.finish()?;
            Packet::Disconnect
        }
        kind @ (2 | 5 | 6 | 7 | 9 | 11 | 13) => return Err(MqttError::UnexpectedPacketType(kind)),
        kind => return Err(MqttError::UnknownPacketType(kind)),
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
    /// `Ok(None)` means the fixed header itself is still incomplete.
    fn peek(buf: &[u8], limits: &MqttLimits) -> Result<Option<Self>, MqttError> {
        let Some(&first) = buf.first() else {
            return Ok(None);
        };
        let Some((remaining, length)) = remaining_length(buf)? else {
            return Ok(None);
        };
        let total = length
            .checked_add(remaining)
            .ok_or(MqttError::MalformedRemainingLength)?;

        // Checked before waiting for the rest: an oversized packet must not
        // pin a read buffer until it arrives.
        if total > limits.max_packet_bytes {
            return Err(MqttError::PacketTooLarge);
        }

        Ok(Some(Self {
            kind: first >> 4,
            flags: first & 0x0f,
            length,
            total,
        }))
    }
}

/// Returns the value and the length of the whole fixed header.
fn remaining_length(buf: &[u8]) -> Result<Option<(usize, usize)>, MqttError> {
    let mut value: usize = 0;
    let mut multiplier: usize = 1;

    for (index, &byte) in buf.iter().enumerate().skip(1).take(MAX_LENGTH_BYTES) {
        let part = usize::from(byte & 0x7f)
            .checked_mul(multiplier)
            .ok_or(MqttError::MalformedRemainingLength)?;
        value = value
            .checked_add(part)
            .ok_or(MqttError::MalformedRemainingLength)?;

        if byte & 0x80 == 0 {
            let length = index
                .checked_add(1)
                .ok_or(MqttError::MalformedRemainingLength)?;
            return Ok(Some((value, length)));
        }

        multiplier = multiplier
            .checked_mul(128)
            .ok_or(MqttError::MalformedRemainingLength)?;
    }

    // A fifth continuation byte cannot be part of a valid length.
    if buf.len() > MAX_LENGTH_BYTES {
        return Err(MqttError::MalformedRemainingLength);
    }
    Ok(None)
}

fn require_flags(header: Header, expected: u8) -> Result<(), MqttError> {
    if header.flags == expected {
        Ok(())
    } else {
        Err(MqttError::InvalidFlags(header.kind))
    }
}

fn connect(mut reader: Reader, limits: &MqttLimits) -> Result<Packet, MqttError> {
    if reader.string()?.as_ref() != b"MQTT" {
        return Err(MqttError::InvalidProtocolName);
    }
    let level = reader.byte()?;
    if level != 4 {
        return Err(MqttError::UnsupportedProtocolLevel(level));
    }

    let flags = reader.byte()?;
    let has_will = flags & 0b0000_0100 != 0;
    let will_qos = (flags & 0b0001_1000) >> 3;
    let will_retain = flags & 0b0010_0000 != 0;
    let has_password = flags & 0b0100_0000 != 0;
    let has_username = flags & 0b1000_0000 != 0;

    // Reserved bit, will fields without a will, password without a user name.
    if flags & 0b0000_0001 != 0
        || (!has_will && (will_qos != 0 || will_retain))
        || (has_password && !has_username)
    {
        return Err(MqttError::InvalidConnectFlags);
    }
    if will_qos > 2 {
        return Err(MqttError::InvalidQos(will_qos));
    }

    let keep_alive_secs = reader.u16()?;
    let client_id = reader.string()?;
    if client_id.len() > limits.max_client_id_bytes {
        return Err(MqttError::ClientIdTooLong);
    }
    let client_id = ClientId::new(client_id)?;

    if has_will {
        // Validated and dropped: the will is never published (§6).
        let _will_topic = topic(reader.string()?, limits)?;
        let _will_payload = reader.binary()?;
    }
    // Kept, not dropped: the runtime checks them (docs/configuration.md).
    let username = if has_username {
        Some(reader.string()?)
    } else {
        None
    };
    let password = if has_password {
        Some(reader.binary()?)
    } else {
        None
    };

    // Nothing may follow the fields the flags announced.
    reader.finish()?;

    Ok(Packet::Connect(Connect {
        client_id,
        clean_session: flags & 0b0000_0010 != 0,
        keep_alive_secs,
        username,
        password,
    }))
}

fn publish(flags: u8, mut reader: Reader, limits: &MqttLimits) -> Result<Packet, MqttError> {
    let duplicate = flags & 0b1000 != 0;
    let topic = topic(reader.string()?, limits)?;

    let id = match (flags & 0b0110) >> 1 {
        0 if duplicate => return Err(MqttError::InvalidFlags(3)),
        0 => None,
        1 => Some(reader.packet_id()?),
        2 => return Err(MqttError::Qos2Unsupported),
        qos => return Err(MqttError::InvalidQos(qos)),
    };

    // RETAIN is accepted and dropped: nothing is stored (§6).
    let payload = payload(reader.rest(), limits);
    Ok(Packet::Publish(Publish { topic, id, payload }))
}

fn subscribe(mut reader: Reader, limits: &MqttLimits) -> Result<Packet, MqttError> {
    let id = reader.packet_id()?;
    let mut filters = Vec::new();

    while !reader.is_empty() {
        if filters.len() >= limits.max_filters_per_subscribe {
            return Err(MqttError::TooManyFilters);
        }
        let filter = filter(reader.string()?, limits)?;
        let guarantee = match reader.byte()? {
            0 => DeliveryGuarantee::AtMostOnce,
            // A request for QoS 2 is granted QoS 1 (§6).
            1 | 2 => DeliveryGuarantee::AtLeastOnce,
            qos => return Err(MqttError::InvalidQos(qos)),
        };
        filters.push((filter, guarantee));
    }

    if filters.is_empty() {
        return Err(MqttError::EmptyFilterList);
    }
    Ok(Packet::Subscribe(Subscribe { id, filters }))
}

fn unsubscribe(mut reader: Reader, limits: &MqttLimits) -> Result<Packet, MqttError> {
    let id = reader.packet_id()?;
    let mut filters = Vec::new();

    while !reader.is_empty() {
        if filters.len() >= limits.max_filters_per_subscribe {
            return Err(MqttError::TooManyFilters);
        }
        filters.push(filter(reader.string()?, limits)?);
    }

    if filters.is_empty() {
        return Err(MqttError::EmptyFilterList);
    }
    Ok(Packet::Unsubscribe(Unsubscribe { id, filters }))
}

fn topic(bytes: Bytes, limits: &MqttLimits) -> Result<Topic, MqttError> {
    check_size(&bytes, limits)?;
    Ok(Topic::new(bytes)?)
}

fn filter(bytes: Bytes, limits: &MqttLimits) -> Result<TopicFilter, MqttError> {
    check_size(&bytes, limits)?;
    Ok(TopicFilter::new(bytes)?)
}

fn check_size(bytes: &[u8], limits: &MqttLimits) -> Result<(), MqttError> {
    if bytes.len() > limits.max_topic_bytes {
        return Err(MqttError::TopicTooLong);
    }
    let levels = bytes
        .iter()
        .filter(|&&byte| byte == b'/')
        .count()
        .checked_add(1)
        .ok_or(MqttError::TooManyTopicLevels)?;
    if levels > limits.max_topic_levels {
        return Err(MqttError::TooManyTopicLevels);
    }
    Ok(())
}

fn payload(bytes: Bytes, limits: &MqttLimits) -> Bytes {
    // A small payload sliced out of the read buffer keeps the whole buffer
    // chunk alive while it waits in a slow subscriber's queue (§9). One copy
    // here, not one per subscriber.
    if bytes.len() <= limits.payload_copy_threshold_bytes {
        Bytes::copy_from_slice(&bytes)
    } else {
        bytes
    }
}

/// Reads fields out of one packet body. Every field is bounds-checked; slices
/// share the buffer's allocation instead of copying it.
struct Reader(Bytes);

impl Reader {
    fn take(&mut self, count: usize) -> Result<Bytes, MqttError> {
        if self.0.len() < count {
            return Err(MqttError::Truncated);
        }
        Ok(self.0.split_to(count))
    }

    fn byte(&mut self) -> Result<u8, MqttError> {
        self.take(1)?.first().copied().ok_or(MqttError::Truncated)
    }

    fn u16(&mut self) -> Result<u16, MqttError> {
        let bytes = self.take(2)?;
        let bytes: [u8; 2] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| MqttError::Truncated)?;
        Ok(u16::from_be_bytes(bytes))
    }

    /// A length-prefixed MQTT string. U+0000 is forbidden in one
    /// (3.1.1 §1.5.3); UTF-8 validity is checked by the typed
    /// constructors that take the bytes.
    fn string(&mut self) -> Result<Bytes, MqttError> {
        let length = usize::from(self.u16()?);
        let bytes = self.take(length)?;
        if bytes.contains(&0) {
            return Err(MqttError::NulInString);
        }
        Ok(bytes)
    }

    /// Length-prefixed binary data: a will payload or a password.
    fn binary(&mut self) -> Result<Bytes, MqttError> {
        let length = usize::from(self.u16()?);
        self.take(length)
    }

    fn packet_id(&mut self) -> Result<PacketId, MqttError> {
        PacketId::new(self.u16()?).ok_or(MqttError::ZeroPacketId)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn rest(self) -> Bytes {
        self.0
    }

    fn finish(self) -> Result<(), MqttError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(MqttError::TrailingBytes)
        }
    }
}
