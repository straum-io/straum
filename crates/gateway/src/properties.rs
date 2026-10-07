//! MQTT 5 message properties to the record's properties section and back
//! (D-65). The section is `journal`'s format, `[id u8][len u16][value]`
//! with MQTT 5 identifiers; `apply` carries it and never reads it.
//!
//! What a message carries end to end: payload format indicator, message
//! expiry interval, content type, response topic, correlation data, user
//! properties. Topic alias and subscription identifiers are not part of the
//! message: one belongs to a connection, the other to a subscriber.
//!
//! Message expiry is carried as the publisher sent it and not enforced:
//! expiry is decided through the log by the sequencer (ADR-0008 step 7).

use bytes::{BufMut, Bytes};
use codec::{PublishProperties, UserProperty};
use domain::Topic;

const PAYLOAD_FORMAT: u8 = 0x01;
const MESSAGE_EXPIRY: u8 = 0x02;
const CONTENT_TYPE: u8 = 0x03;
const RESPONSE_TOPIC: u8 = 0x08;
const CORRELATION_DATA: u8 = 0x09;
const USER: u8 = 0x26;

/// A property longer than one section entry can hold (65 535 bytes): a
/// user property whose name and value together pass it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooLong;

/// The section for a publish's properties; empty when it has none.
pub fn encode(properties: Option<&PublishProperties>) -> Result<Bytes, TooLong> {
    let Some(p) = properties else {
        return Ok(Bytes::new());
    };
    let mut out = Vec::new();
    let mut put =
        |id: u8, value: &[u8]| journal::encode_property(id, value, &mut out).map_err(|_| TooLong);
    if p.payload_format_utf8 {
        put(PAYLOAD_FORMAT, &[1])?;
    }
    if let Some(expiry) = p.message_expiry_secs {
        put(MESSAGE_EXPIRY, &expiry.to_be_bytes())?;
    }
    if let Some(content_type) = &p.content_type {
        put(CONTENT_TYPE, content_type)?;
    }
    if let Some(topic) = &p.response_topic {
        put(RESPONSE_TOPIC, topic.as_bytes())?;
    }
    if let Some(data) = &p.correlation_data {
        put(CORRELATION_DATA, data)?;
    }
    for user in &p.user {
        // [name_len u16][name][value]: the value runs to the entry's end.
        let length = u16::try_from(user.name.len()).map_err(|_| TooLong)?;
        let mut value = Vec::with_capacity(
            2usize
                .saturating_add(user.name.len())
                .saturating_add(user.value.len()),
        );
        value.put_u16(length);
        value.put_slice(&user.name);
        value.put_slice(&user.value);
        put(USER, &value)?;
    }
    Ok(Bytes::from(out))
}

/// The properties a section holds, for an MQTT 5 subscriber. `None` when
/// there are none. The section was written by `encode` on some gateway;
/// an entry that does not read back is skipped rather than trusted.
pub fn decode(section: &[u8]) -> Option<Box<PublishProperties>> {
    if section.is_empty() {
        return None;
    }
    let mut p = PublishProperties::default();
    for entry in journal::Properties::new(section).iter() {
        let Ok((id, value)) = entry else { break };
        match id {
            PAYLOAD_FORMAT => p.payload_format_utf8 = value == [1],
            MESSAGE_EXPIRY => {
                p.message_expiry_secs = <[u8; 4]>::try_from(value).ok().map(u32::from_be_bytes);
            }
            CONTENT_TYPE => p.content_type = Some(Bytes::copy_from_slice(value)),
            RESPONSE_TOPIC => p.response_topic = Topic::new(Bytes::copy_from_slice(value)).ok(),
            CORRELATION_DATA => p.correlation_data = Some(Bytes::copy_from_slice(value)),
            USER => {
                let Some((length, rest)) = value.split_first_chunk::<2>() else {
                    continue;
                };
                let length = usize::from(u16::from_be_bytes(*length));
                if let (Some(name), Some(value)) = (rest.get(..length), rest.get(length..)) {
                    p.user.push(UserProperty {
                        name: Bytes::copy_from_slice(name),
                        value: Bytes::copy_from_slice(value),
                    });
                }
            }
            _ => {}
        }
    }
    (p != PublishProperties::default()).then(|| Box::new(p))
}
