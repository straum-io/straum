//! Encoder output, byte for byte where the broker is the only speaker, and
//! through the decoder where both sides share a format.
//!
//! `allow-unwrap-in-tests` only covers code inside `#[test]` functions.
#![allow(clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use domain::{DeliveryGuarantee, Topic};
use mqtt::{
    ConnAck, ConnectRejected, MqttError, MqttLimits, Packet, PacketId, connack, decode, pingresp,
    puback, publish, suback, unsuback,
};
use protocol::SubscribeResult;

fn limits() -> MqttLimits {
    MqttLimits::default()
}

fn topic(value: &str) -> Topic {
    Topic::new(Bytes::copy_from_slice(value.as_bytes())).unwrap()
}

fn id(value: u16) -> PacketId {
    PacketId::new(value).unwrap()
}

#[test]
fn connack_bytes() {
    let cases = [
        (
            ConnAck::Accepted {
                session_present: false,
            },
            [0x20, 0x02, 0x00, 0x00],
        ),
        (
            ConnAck::Accepted {
                session_present: true,
            },
            [0x20, 0x02, 0x01, 0x00],
        ),
        (
            ConnAck::Rejected(ConnectRejected::UnacceptableProtocolVersion),
            [0x20, 0x02, 0x00, 0x01],
        ),
        (
            ConnAck::Rejected(ConnectRejected::IdentifierRejected),
            [0x20, 0x02, 0x00, 0x02],
        ),
        (
            ConnAck::Rejected(ConnectRejected::NotAuthorized),
            [0x20, 0x02, 0x00, 0x05],
        ),
    ];

    for (ack, expected) in cases {
        let mut buf = BytesMut::new();
        connack(&mut buf, ack);
        assert_eq!(buf.as_ref(), expected, "{ack:?}");
    }
}

#[test]
fn acknowledgement_bytes() {
    let mut buf = BytesMut::new();
    puback(&mut buf, id(0x1234));
    assert_eq!(buf.as_ref(), [0x40, 0x02, 0x12, 0x34]);

    let mut buf = BytesMut::new();
    unsuback(&mut buf, id(7));
    assert_eq!(buf.as_ref(), [0xb0, 0x02, 0x00, 0x07]);

    let mut buf = BytesMut::new();
    pingresp(&mut buf);
    assert_eq!(buf.as_ref(), [0xd0, 0x00]);
}

#[test]
fn suback_bytes() {
    let results = [
        SubscribeResult::Granted(DeliveryGuarantee::AtMostOnce),
        SubscribeResult::Granted(DeliveryGuarantee::AtLeastOnce),
        SubscribeResult::Failure,
    ];

    let mut buf = BytesMut::new();
    suback(&mut buf, id(9), &results, &limits()).unwrap();
    assert_eq!(buf.as_ref(), [0x90, 0x05, 0x00, 0x09, 0x00, 0x01, 0x80]);
}

#[test]
fn publish_round_trips_through_the_decoder() {
    // Payload sizes that need one, two and three Remaining Length bytes.
    for size in [0usize, 64, 200, 20_000] {
        for packet_id in [None, Some(id(3))] {
            let payload = vec![b'p'; size];
            let mut buf = BytesMut::new();
            publish(
                &mut buf,
                &topic("sensors/eu-west/rack12"),
                packet_id,
                &payload,
                false,
                &limits(),
            )
            .unwrap();

            let decoded = decode(&mut buf, &limits()).unwrap().unwrap();
            let Packet::Publish(decoded) = decoded else {
                panic!("expected PUBLISH");
            };
            assert_eq!(decoded.topic.as_str(), "sensors/eu-west/rack12");
            assert_eq!(decoded.id, packet_id);
            assert_eq!(decoded.payload.len(), size);
            assert!(buf.is_empty(), "encoder wrote more than one packet");
        }
    }
}

#[test]
fn puback_round_trips_through_the_decoder() {
    let mut buf = BytesMut::new();
    puback(&mut buf, id(42));
    assert_eq!(
        decode(&mut buf, &limits()).unwrap().unwrap(),
        Packet::PubAck(id(42))
    );
}

#[test]
fn encoders_append_to_a_batched_buffer() {
    let mut buf = BytesMut::new();
    connack(
        &mut buf,
        ConnAck::Accepted {
            session_present: false,
        },
    );
    publish(&mut buf, &topic("a/b"), None, b"one", false, &limits()).unwrap();
    pingresp(&mut buf);

    // The writer hands the whole batch to one write; nothing was overwritten.
    assert_eq!(buf.len(), 4 + 2 + 2 + 3 + 3 + 2);
    assert_eq!(buf.first(), Some(&0x20));
    assert_eq!(buf.last(), Some(&0x00));
}

#[test]
fn oversized_packets_are_refused() {
    let small = MqttLimits {
        max_packet_bytes: 32,
        ..MqttLimits::default()
    };
    let mut buf = BytesMut::new();
    assert_eq!(
        publish(&mut buf, &topic("a/b"), None, &[b'x'; 64], false, &small),
        Err(MqttError::PacketTooLarge)
    );

    let long = Bytes::from(vec![b'x'; 70_000]);
    assert_eq!(
        publish(
            &mut buf,
            &Topic::new(long).unwrap(),
            None,
            b"",
            false,
            &limits()
        ),
        Err(MqttError::TopicTooLong)
    );

    let results = vec![SubscribeResult::Failure; 64];
    assert_eq!(
        suback(&mut buf, id(1), &results, &small),
        Err(MqttError::PacketTooLarge)
    );
    assert!(
        buf.is_empty(),
        "a refused packet must not leave bytes behind"
    );
}

/// `decode(encode(p)) == p` for the one packet type both sides speak.
mod properties {
    use super::*;
    use proptest::prelude::*;

    fn level() -> impl Strategy<Value = String> {
        prop_oneof!["a", "sensors", "eu-west", "rack[0-9]{1,3}", ""].prop_map(String::from)
    }

    proptest! {
        #[test]
        fn publish_round_trip(
            levels in prop::collection::vec(level(), 1..6),
            payload in prop::collection::vec(any::<u8>(), 0..2048),
            packet_id in prop::option::of(1u16..=u16::MAX),
        ) {
            let Ok(topic) = Topic::new(Bytes::from(levels.join("/"))) else {
                return Ok(());
            };
            let packet_id = packet_id.map(|value| PacketId::new(value).unwrap());

            let mut buf = BytesMut::new();
            publish(&mut buf, &topic, packet_id, &payload, false, &limits()).unwrap();

            let decoded = decode(&mut buf, &limits()).unwrap().unwrap();
            prop_assert!(buf.is_empty());
            let Packet::Publish(decoded) = decoded else {
                panic!("expected PUBLISH");
            };
            prop_assert_eq!(decoded.topic, topic);
            prop_assert_eq!(decoded.id, packet_id);
            prop_assert_eq!(decoded.payload.as_ref(), payload.as_slice());
        }
    }
}
