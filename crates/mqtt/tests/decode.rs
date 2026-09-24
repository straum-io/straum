//! Decoder behavior against the public API, including every failure path of
//! docs/testing.md "MQTT codec".
//!
//! `allow-unwrap-in-tests` only covers code inside `#[test]` functions, so
//! the builders below need the allow explicitly.
#![allow(clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use domain::DeliveryGuarantee;
use mqtt::{MqttError, MqttLimits, Packet, PacketId, decode};

const CONNECT: u8 = 1;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const SUBSCRIBE: u8 = 8;
const UNSUBSCRIBE: u8 = 10;
const PINGREQ: u8 = 12;
const DISCONNECT: u8 = 14;

fn limits() -> MqttLimits {
    MqttLimits::default()
}

fn string(value: &[u8]) -> Vec<u8> {
    let mut out = u16::try_from(value.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(value);
    out
}

fn remaining_length(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = u8::try_from(value % 128).unwrap();
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn frame(kind: u8, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![(kind << 4) | flags];
    remaining_length(body.len(), &mut out);
    out.extend_from_slice(body);
    out
}

fn buffer(bytes: &[u8]) -> BytesMut {
    BytesMut::from(bytes)
}

fn decoded(bytes: &[u8]) -> Packet {
    let mut buf = buffer(bytes);
    let packet = decode(&mut buf, &limits()).unwrap().unwrap();
    assert!(buf.is_empty(), "decoder left {} bytes behind", buf.len());
    packet
}

fn rejected(bytes: &[u8]) -> MqttError {
    decode(&mut buffer(bytes), &limits()).unwrap_err()
}

fn connect_body(client_id: &[u8], flags: u8) -> Vec<u8> {
    let mut body = string(b"MQTT");
    body.push(4);
    body.push(flags);
    body.extend_from_slice(&60u16.to_be_bytes());
    body.extend_from_slice(&string(client_id));
    body
}

fn publish_body(topic: &[u8], id: Option<u16>, payload: &[u8]) -> Vec<u8> {
    let mut body = string(topic);
    if let Some(id) = id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    body.extend_from_slice(payload);
    body
}

#[test]
fn connect_minimal() {
    let packet = decoded(&frame(CONNECT, 0, &connect_body(b"client", 0b0000_0010)));
    let Packet::Connect(connect) = packet else {
        panic!("expected CONNECT, got {packet:?}");
    };
    assert_eq!(connect.client_id.as_str(), "client");
    assert!(connect.clean_session);
    assert_eq!(connect.keep_alive_secs, 60);
}

#[test]
fn connect_keeps_the_credentials_and_drops_the_will() {
    let mut body = connect_body(b"c", 0b1100_1110);
    body.extend_from_slice(&string(b"will/topic"));
    body.extend_from_slice(&string(b"bye"));
    body.extend_from_slice(&string(b"user"));
    body.extend_from_slice(&string(b"secret"));

    let packet = decoded(&frame(CONNECT, 0, &body));
    let Packet::Connect(connect) = packet else {
        panic!("expected CONNECT");
    };
    assert_eq!(connect.client_id.as_str(), "c");
    // The will is gone (§6); the credentials are not, because somebody
    // upstream has to check them.
    assert_eq!(connect.username.as_deref(), Some(&b"user"[..]));
    assert_eq!(connect.password.as_deref(), Some(&b"secret"[..]));
}

#[test]
fn connect_accepts_empty_client_id() {
    let packet = decoded(&frame(CONNECT, 0, &connect_body(b"", 0b0000_0010)));
    let Packet::Connect(connect) = packet else {
        panic!("expected CONNECT");
    };
    assert!(connect.client_id.is_empty());
}

#[test]
fn connect_failure_paths() {
    let mut wrong_name = string(b"MQIsdp");
    wrong_name.extend_from_slice(&[3, 0, 0, 60, 0, 0]);
    assert_eq!(
        rejected(&frame(CONNECT, 0, &wrong_name)),
        MqttError::InvalidProtocolName
    );

    let mut level = string(b"MQTT");
    level.extend_from_slice(&[5, 0, 0, 60]);
    level.extend_from_slice(&string(b"c"));
    assert_eq!(
        rejected(&frame(CONNECT, 0, &level)),
        MqttError::UnsupportedProtocolLevel(5)
    );

    // Reserved bit set, will flags without a will, password without a user name.
    for flags in [0b0000_0001, 0b0000_1000, 0b0100_0000] {
        assert_eq!(
            rejected(&frame(CONNECT, 0, &connect_body(b"c", flags))),
            MqttError::InvalidConnectFlags,
            "flags {flags:#010b}"
        );
    }

    let mut will_qos_3 = connect_body(b"c", 0b0001_1100);
    will_qos_3.extend_from_slice(&string(b"t"));
    will_qos_3.extend_from_slice(&string(b""));
    assert_eq!(
        rejected(&frame(CONNECT, 0, &will_qos_3)),
        MqttError::InvalidQos(3)
    );

    assert_eq!(
        rejected(&frame(CONNECT, 0, &connect_body(&[0xff], 0))),
        MqttError::ClientId(domain::InvalidClientId)
    );
    assert_eq!(
        rejected(&frame(CONNECT, 0, &connect_body(b"a\0b", 0))),
        MqttError::NulInString
    );

    let long = vec![b'x'; limits().max_client_id_bytes + 1];
    assert_eq!(
        rejected(&frame(CONNECT, 0, &connect_body(&long, 0))),
        MqttError::ClientIdTooLong
    );

    let mut trailing = connect_body(b"c", 0);
    trailing.push(0);
    assert_eq!(
        rejected(&frame(CONNECT, 0, &trailing)),
        MqttError::TrailingBytes
    );

    assert_eq!(rejected(&frame(CONNECT, 0b0001, &[])), MqttError::Truncated);
}

#[test]
fn publish_qos0_and_qos1() {
    let packet = decoded(&frame(PUBLISH, 0, &publish_body(b"a/b", None, b"hello")));
    let Packet::Publish(publish) = packet else {
        panic!("expected PUBLISH");
    };
    assert_eq!(publish.topic.as_str(), "a/b");
    assert_eq!(publish.payload, Bytes::from_static(b"hello"));
    assert_eq!(publish.id, None);
    assert_eq!(publish.guarantee(), DeliveryGuarantee::AtMostOnce);

    let packet = decoded(&frame(PUBLISH, 0b0010, &publish_body(b"a/b", Some(7), b"")));
    let Packet::Publish(publish) = packet else {
        panic!("expected PUBLISH");
    };
    assert!(publish.payload.is_empty());
    assert_eq!(publish.id, PacketId::new(7));
    assert_eq!(publish.guarantee(), DeliveryGuarantee::AtLeastOnce);
}

#[test]
fn publish_failure_paths() {
    assert_eq!(
        rejected(&frame(PUBLISH, 0b0100, &publish_body(b"a", Some(1), b""))),
        MqttError::Qos2Unsupported
    );
    assert_eq!(
        rejected(&frame(PUBLISH, 0b0110, &publish_body(b"a", Some(1), b""))),
        MqttError::InvalidQos(3)
    );
    // DUP must be 0 for QoS 0.
    assert_eq!(
        rejected(&frame(PUBLISH, 0b1000, &publish_body(b"a", None, b""))),
        MqttError::InvalidFlags(3)
    );
    assert_eq!(
        rejected(&frame(PUBLISH, 0b0010, &publish_body(b"a", Some(0), b""))),
        MqttError::ZeroPacketId
    );
    assert_eq!(
        rejected(&frame(PUBLISH, 0, &publish_body(b"a/+", None, b""))),
        MqttError::Topic(domain::TopicError::WildcardInTopic)
    );
    assert_eq!(
        rejected(&frame(PUBLISH, 0, &publish_body(b"", None, b""))),
        MqttError::Topic(domain::TopicError::Empty)
    );

    let long = vec![b'x'; limits().max_topic_bytes + 1];
    assert_eq!(
        rejected(&frame(PUBLISH, 0, &publish_body(&long, None, b""))),
        MqttError::TopicTooLong
    );

    let deep = vec![b'/'; limits().max_topic_levels];
    assert_eq!(
        rejected(&frame(PUBLISH, 0, &publish_body(&deep, None, b""))),
        MqttError::TooManyTopicLevels
    );
}

#[test]
fn small_payloads_are_copied_out_of_the_read_buffer() {
    let threshold = limits().payload_copy_threshold_bytes;

    for (size, copied) in [(threshold, true), (threshold + 1, false)] {
        let payload = vec![b'x'; size];
        let mut buf = buffer(&frame(PUBLISH, 0, &publish_body(b"t", None, &payload)));
        let range = buf.as_ptr_range();

        let packet = decode(&mut buf, &limits()).unwrap().unwrap();
        let Packet::Publish(publish) = packet else {
            panic!("expected PUBLISH");
        };
        assert_eq!(publish.payload.len(), size);
        assert_eq!(
            range.contains(&publish.payload.as_ptr()),
            !copied,
            "payload of {size} B"
        );
    }
}

#[test]
fn acknowledgement_and_keepalive_packets() {
    assert_eq!(
        decoded(&frame(PUBACK, 0, &9u16.to_be_bytes())),
        Packet::PubAck(PacketId::new(9).unwrap())
    );
    assert_eq!(decoded(&frame(PINGREQ, 0, &[])), Packet::PingReq);
    assert_eq!(decoded(&frame(DISCONNECT, 0, &[])), Packet::Disconnect);

    assert_eq!(
        rejected(&frame(PUBACK, 0, &0u16.to_be_bytes())),
        MqttError::ZeroPacketId
    );
    assert_eq!(rejected(&frame(PINGREQ, 0, b"x")), MqttError::TrailingBytes);
    assert_eq!(
        rejected(&frame(DISCONNECT, 0b0001, &[])),
        MqttError::InvalidFlags(DISCONNECT)
    );
}

#[test]
fn subscribe_maps_requested_qos() {
    let mut body = 1u16.to_be_bytes().to_vec();
    for (filter, qos) in [(&b"a/#"[..], 0u8), (b"b/+", 1), (b"c", 2)] {
        body.extend_from_slice(&string(filter));
        body.push(qos);
    }

    let Packet::Subscribe(subscribe) = decoded(&frame(SUBSCRIBE, 0b0010, &body)) else {
        panic!("expected SUBSCRIBE");
    };
    assert_eq!(subscribe.id, PacketId::new(1).unwrap());
    let granted: Vec<_> = subscribe
        .filters
        .iter()
        .map(|(filter, guarantee)| (filter.as_str(), *guarantee))
        .collect();
    assert_eq!(
        granted,
        vec![
            ("a/#", DeliveryGuarantee::AtMostOnce),
            ("b/+", DeliveryGuarantee::AtLeastOnce),
            // A request for QoS 2 is granted QoS 1.
            ("c", DeliveryGuarantee::AtLeastOnce),
        ]
    );
}

#[test]
fn subscribe_and_unsubscribe_failure_paths() {
    assert_eq!(
        rejected(&frame(SUBSCRIBE, 0b0010, &1u16.to_be_bytes())),
        MqttError::EmptyFilterList
    );
    assert_eq!(
        rejected(&frame(UNSUBSCRIBE, 0b0010, &1u16.to_be_bytes())),
        MqttError::EmptyFilterList
    );

    let mut bad_qos = 1u16.to_be_bytes().to_vec();
    bad_qos.extend_from_slice(&string(b"a"));
    bad_qos.push(3);
    assert_eq!(
        rejected(&frame(SUBSCRIBE, 0b0010, &bad_qos)),
        MqttError::InvalidQos(3)
    );

    let mut bad_filter = 1u16.to_be_bytes().to_vec();
    bad_filter.extend_from_slice(&string(b"a/#/b"));
    bad_filter.push(0);
    assert_eq!(
        rejected(&frame(SUBSCRIBE, 0b0010, &bad_filter)),
        MqttError::Topic(domain::TopicError::MultiLevelNotLast)
    );

    let mut many = 1u16.to_be_bytes().to_vec();
    for _ in 0..=limits().max_filters_per_subscribe {
        many.extend_from_slice(&string(b"a"));
        many.push(0);
    }
    assert_eq!(
        rejected(&frame(SUBSCRIBE, 0b0010, &many)),
        MqttError::TooManyFilters
    );

    // SUBSCRIBE and UNSUBSCRIBE carry reserved flags 0b0010.
    assert_eq!(
        rejected(&frame(SUBSCRIBE, 0, &1u16.to_be_bytes())),
        MqttError::InvalidFlags(SUBSCRIBE)
    );
}

#[test]
fn unsubscribe_decodes_filters() {
    let mut body = 3u16.to_be_bytes().to_vec();
    body.extend_from_slice(&string(b"a/b"));
    body.extend_from_slice(&string(b"c/#"));

    let Packet::Unsubscribe(unsubscribe) = decoded(&frame(UNSUBSCRIBE, 0b0010, &body)) else {
        panic!("expected UNSUBSCRIBE");
    };
    assert_eq!(unsubscribe.id, PacketId::new(3).unwrap());
    assert_eq!(unsubscribe.filters.len(), 2);
}

#[test]
fn packet_types_the_broker_never_receives() {
    for kind in [2u8, 5, 6, 7, 9, 11, 13] {
        assert_eq!(
            rejected(&frame(kind, 0, &[])),
            MqttError::UnexpectedPacketType(kind),
            "type {kind}"
        );
    }
    for kind in [0u8, 15] {
        assert_eq!(
            rejected(&frame(kind, 0, &[])),
            MqttError::UnknownPacketType(kind),
            "type {kind}"
        );
    }
}

#[test]
fn remaining_length_encodings() {
    // 1, 2, 3 and 4 byte lengths, each exercised through a real packet.
    for size in [0usize, 127, 128, 16_383, 16_384] {
        let payload = vec![b'x'; size];
        let bytes = frame(PUBLISH, 0, &publish_body(b"t", None, &payload));
        let Packet::Publish(publish) = decoded(&bytes) else {
            panic!("expected PUBLISH");
        };
        assert_eq!(publish.payload.len(), size);
    }

    // A fifth continuation byte cannot belong to a valid length.
    assert_eq!(
        decode(
            &mut buffer(&[0x30, 0x80, 0x80, 0x80, 0x80, 0x01]),
            &limits()
        )
        .unwrap_err(),
        MqttError::MalformedRemainingLength
    );
}

#[test]
fn oversized_packets_are_rejected_before_they_arrive() {
    let small = MqttLimits {
        max_packet_bytes: 64,
        ..MqttLimits::default()
    };

    // Only the fixed header is present; the announced body never has to arrive.
    let mut header = vec![0x30];
    remaining_length(1024, &mut header);
    let mut buf = buffer(&header);
    assert_eq!(
        decode(&mut buf, &small).unwrap_err(),
        MqttError::PacketTooLarge
    );

    let exact = vec![b'x'; 64 - 1 - 1 - 3];
    let bytes = frame(PUBLISH, 0, &publish_body(b"t", None, &exact));
    assert_eq!(bytes.len(), 64);
    assert!(decode(&mut buffer(&bytes), &small).unwrap().is_some());

    let one_over = frame(PUBLISH, 0, &publish_body(b"t", None, &[b'x'; 64 - 4]));
    assert_eq!(one_over.len(), 65);
    assert_eq!(
        decode(&mut buffer(&one_over), &small).unwrap_err(),
        MqttError::PacketTooLarge
    );
}

#[test]
fn a_packet_split_at_every_offset() {
    let packets = [
        frame(CONNECT, 0, &connect_body(b"client", 0b0000_0010)),
        frame(
            PUBLISH,
            0b0010,
            &publish_body(b"a/b/c", Some(4), b"payload"),
        ),
        frame(SUBSCRIBE, 0b0010, &{
            let mut body = 1u16.to_be_bytes().to_vec();
            body.extend_from_slice(&string(b"a/#"));
            body.push(1);
            body
        }),
    ];

    for bytes in packets {
        let whole = decoded(&bytes);
        for split in 0..bytes.len() {
            let mut buf = buffer(bytes.get(..split).unwrap());
            assert_eq!(
                decode(&mut buf, &limits()),
                Ok(None),
                "prefix of {split} bytes"
            );
            assert_eq!(buf.len(), split, "an incomplete packet was consumed");

            buf.extend_from_slice(bytes.get(split..).unwrap());
            assert_eq!(decode(&mut buf, &limits()), Ok(Some(whole.clone())));
            assert!(buf.is_empty());
        }
    }
}

#[test]
fn several_packets_in_one_buffer() {
    let mut bytes = frame(CONNECT, 0, &connect_body(b"c", 0b0000_0010));
    bytes.extend_from_slice(&frame(PUBLISH, 0, &publish_body(b"a", None, b"1")));
    bytes.extend_from_slice(&frame(PINGREQ, 0, &[]));
    bytes.extend_from_slice(&frame(DISCONNECT, 0, &[]));

    let mut buf = buffer(&bytes);
    let mut packets = Vec::new();
    // Terminates only because every successful decode consumes bytes.
    while let Some(packet) = decode(&mut buf, &limits()).unwrap() {
        packets.push(packet);
    }

    assert_eq!(packets.len(), 4);
    assert!(buf.is_empty());
    assert!(matches!(packets.first(), Some(Packet::Connect(_))));
    assert_eq!(packets.last(), Some(&Packet::Disconnect));
}

/// The invariants the fuzz targets in `fuzz/` assert, over generated input:
/// the decoder holds no state between calls, and it never claims progress
/// without consuming bytes.
mod properties {
    use super::*;
    use proptest::prelude::*;

    fn corpus() -> Vec<Vec<u8>> {
        let mut subscribe = 1u16.to_be_bytes().to_vec();
        subscribe.extend_from_slice(&string(b"a/#"));
        subscribe.push(1);

        vec![
            frame(CONNECT, 0, &connect_body(b"client", 0b0000_0010)),
            frame(PUBLISH, 0, &publish_body(b"a/b", None, b"small")),
            frame(
                PUBLISH,
                0b0010,
                &publish_body(b"a/b", Some(4), &[b'x'; 600]),
            ),
            frame(SUBSCRIBE, 0b0010, &subscribe),
            frame(PINGREQ, 0, &[]),
            frame(DISCONNECT, 0, &[]),
        ]
    }

    /// Decodes what it can; `Some` is the error that stopped it.
    fn drain(buf: &mut BytesMut, out: &mut Vec<Packet>) -> Option<MqttError> {
        loop {
            match decode(buf, &limits()) {
                Ok(Some(packet)) => out.push(packet),
                Ok(None) => return None,
                Err(error) => return Some(error),
            }
        }
    }

    proptest! {
        #[test]
        fn chunked_decoding_matches_whole(
            selection in prop::collection::vec(0usize..6, 0..5),
            noise in prop::collection::vec(any::<u8>(), 0..8),
            chunk in 1usize..24,
        ) {
            let corpus = corpus();
            let mut bytes = Vec::new();
            for index in selection {
                bytes.extend_from_slice(corpus.get(index).unwrap());
            }
            bytes.extend_from_slice(&noise);

            let mut whole = Vec::new();
            let whole_error = drain(&mut BytesMut::from(bytes.as_slice()), &mut whole);

            let mut streamed = Vec::new();
            let mut streamed_error = None;
            let mut buf = BytesMut::new();
            for part in bytes.chunks(chunk) {
                buf.extend_from_slice(part);
                streamed_error = drain(&mut buf, &mut streamed);
                if streamed_error.is_some() {
                    break;
                }
            }

            prop_assert_eq!(whole, streamed);
            prop_assert_eq!(whole_error, streamed_error);
        }

        #[test]
        fn arbitrary_bytes_never_panic(data in prop::collection::vec(any::<u8>(), 0..512)) {
            let mut buf = BytesMut::from(data.as_slice());
            loop {
                let before = buf.len();
                match decode(&mut buf, &limits()) {
                    Ok(Some(_)) => prop_assert!(buf.len() < before),
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }
}
