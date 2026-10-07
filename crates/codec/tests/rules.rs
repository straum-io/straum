//! One protocol rule per test, in raw bytes where the rule is about bytes.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::num::NonZeroU16;

use bytes::{Bytes, BytesMut};
use codec::*;
use domain::{ClientId, Topic};

fn limits() -> Limits {
    Limits::default()
}

fn decode_v5(bytes: &[u8], from: Sender) -> Result<Option<Packet>, DecodeError> {
    decode(
        &mut BytesMut::from(bytes),
        Some(Version::V5),
        from,
        &limits(),
    )
}

fn encoded(packet: &Packet, version: Version, from: Sender) -> Vec<u8> {
    let mut out = BytesMut::new();
    encode(packet, version, from, &mut out).unwrap();
    out.to_vec()
}

fn varint(mut value: usize, out: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(value % 128).unwrap();
        value /= 128;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// PUBLISH QoS 0, topic "t", the given property section, payload "x".
fn publish_with(properties: &[u8]) -> Vec<u8> {
    let mut body = vec![0, 1, b't'];
    varint(properties.len(), &mut body);
    body.extend_from_slice(properties);
    body.push(b'x');
    let mut packet = vec![0x30];
    varint(body.len(), &mut packet);
    packet.extend(body);
    packet
}

#[test]
fn a_client_may_not_send_a_subscription_identifier() {
    let bytes = publish_with(&[0x0B, 5]);
    assert_eq!(
        decode_v5(&bytes, Sender::Client),
        Err(DecodeError::PropertyNotAllowed)
    );
    assert_eq!(
        DecodeError::PropertyNotAllowed.reason(),
        ReasonCode::ProtocolError
    );
    // From the server it is the matching subscription's identifier.
    assert!(decode_v5(&bytes, Sender::Server).unwrap().is_some());
}

#[test]
fn a_property_that_may_appear_once_is_refused_twice() {
    let bytes = publish_with(&[0x02, 0, 0, 0, 1, 0x02, 0, 0, 0, 2]);
    assert_eq!(
        decode_v5(&bytes, Sender::Client),
        Err(DecodeError::DuplicateProperty)
    );
    // User Properties repeat, in order.
    let bytes = publish_with(&[0x26, 0, 1, b'a', 0, 1, b'1', 0x26, 0, 1, b'a', 0, 1, b'2']);
    let Some(Packet::Publish(p)) = decode_v5(&bytes, Sender::Client).unwrap() else {
        panic!("a publish")
    };
    let user = &p.properties.unwrap().user;
    assert_eq!(user.len(), 2);
    assert_eq!(user[1].value, Bytes::from_static(b"2"));
}

#[test]
fn an_unknown_property_is_malformed_and_a_misplaced_one_a_protocol_error() {
    let unknown = publish_with(&[0x7F, 0]);
    assert_eq!(
        decode_v5(&unknown, Sender::Client),
        Err(DecodeError::UnknownProperty)
    );
    assert_eq!(
        DecodeError::UnknownProperty.reason(),
        ReasonCode::MalformedPacket
    );
    // Session Expiry belongs to CONNECT, CONNACK and DISCONNECT.
    let misplaced = publish_with(&[0x11, 0, 0, 0, 1]);
    assert_eq!(
        decode_v5(&misplaced, Sender::Client),
        Err(DecodeError::PropertyNotAllowed)
    );
}

#[test]
fn values_out_of_range_are_refused() {
    // Payload Format Indicator 2.
    let bytes = publish_with(&[0x01, 2]);
    assert_eq!(
        decode_v5(&bytes, Sender::Client),
        Err(DecodeError::InvalidProperty)
    );
    // Topic Alias 0 has its own reason (§3.3.2.3.4).
    let bytes = publish_with(&[0x23, 0, 0]);
    assert_eq!(
        decode_v5(&bytes, Sender::Client),
        Err(DecodeError::TopicAliasInvalid)
    );
    assert_eq!(
        DecodeError::TopicAliasInvalid.reason(),
        ReasonCode::TopicAliasInvalid
    );
    // A Response Topic is a topic: no wildcards.
    let bytes = publish_with(&[0x08, 0, 3, b'a', b'/', b'#']);
    assert_eq!(
        decode_v5(&bytes, Sender::Client),
        Err(DecodeError::InvalidProperty)
    );
}

#[test]
fn user_properties_are_bounded() {
    let mut properties = Vec::new();
    for _ in 0..=limits().max_user_properties {
        properties.extend_from_slice(&[0x26, 0, 0, 0, 0]);
    }
    let bytes = publish_with(&properties);
    assert_eq!(
        decode_v5(&bytes, Sender::Client),
        Err(DecodeError::TooManyUserProperties)
    );
}

#[test]
fn an_empty_topic_needs_a_topic_alias() {
    let mut body = vec![0, 0, 0, b'x'];
    let packet = |body: &[u8]| {
        let mut p = vec![0x30, u8::try_from(body.len()).unwrap()];
        p.extend_from_slice(body);
        p
    };
    assert_eq!(
        decode_v5(&packet(&body), Sender::Client),
        Err(DecodeError::TopicNameInvalid)
    );
    body = vec![0, 0, 3, 0x23, 0, 7, b'x'];
    let Some(Packet::Publish(p)) = decode_v5(&packet(&body), Sender::Client).unwrap() else {
        panic!("a publish")
    };
    assert_eq!(p.topic, None);
    assert_eq!(p.properties.unwrap().topic_alias, NonZeroU16::new(7));
}

#[test]
fn version_3_1_1_says_what_it_can_and_drops_the_rest() {
    // A CONNACK refusal 3.1.1 has no code for becomes "server unavailable".
    let refused = Packet::ConnAck(Box::new(ConnAck {
        session_present: false,
        reason: ReasonCode::ServerBusy,
        properties: ConnAckProperties {
            reason_string: Some(Bytes::from_static(b"busy")),
            ..Default::default()
        },
    }));
    assert_eq!(
        encoded(&refused, Version::V311, Sender::Server),
        vec![0x20, 2, 0, 0x03]
    );
    // A SUBACK failure is 0x80.
    let suback = Packet::SubAck(SubAck {
        id: PacketId::new(1).unwrap(),
        reasons: vec![ReasonCode::GrantedQos1, ReasonCode::NotAuthorized],
        properties: AckProperties::default(),
    });
    assert_eq!(
        encoded(&suback, Version::V311, Sender::Server),
        vec![0x90, 4, 0, 1, 0x01, 0x80]
    );
    // A PUBACK carries no reason.
    let puback = Packet::PubAck(PubAck {
        id: PacketId::new(9).unwrap(),
        reason: ReasonCode::NoMatchingSubscribers,
        properties: AckProperties::default(),
    });
    assert_eq!(
        encoded(&puback, Version::V311, Sender::Server),
        vec![0x40, 2, 0, 9]
    );
}

#[test]
fn version_3_1_1_has_no_auth_and_no_server_disconnect() {
    let disconnect = Packet::Disconnect(Disconnect {
        reason: ReasonCode::ServerShuttingDown,
        properties: DisconnectProperties::default(),
    });
    let mut out = BytesMut::new();
    assert_eq!(
        encode(&disconnect, Version::V311, Sender::Server, &mut out),
        Err(EncodeError::NotInVersion)
    );
    let auth = Packet::Auth(Auth {
        reason: ReasonCode::ContinueAuthentication,
        properties: AuthProperties::default(),
    });
    assert_eq!(
        encode(&auth, Version::V311, Sender::Client, &mut out),
        Err(EncodeError::NotInVersion)
    );
    let mut bytes = BytesMut::from(&[0xF0u8, 0][..]);
    assert_eq!(
        decode(&mut bytes, Some(Version::V311), Sender::Client, &limits()),
        Err(DecodeError::UnknownPacketType(15))
    );
}

#[test]
fn version_5_leaves_out_what_success_implies() {
    let puback = Packet::PubAck(PubAck {
        id: PacketId::new(9).unwrap(),
        reason: ReasonCode::Success,
        properties: AckProperties::default(),
    });
    assert_eq!(
        encoded(&puback, Version::V5, Sender::Client),
        vec![0x40, 2, 0, 9]
    );
    // Reason without properties is a form too.
    let Some(Packet::PubAck(p)) = decode_v5(&[0x40, 3, 0, 9, 0x10], Sender::Server).unwrap() else {
        panic!("a puback")
    };
    assert_eq!(p.reason, ReasonCode::NoMatchingSubscribers);
    let disconnect = Packet::Disconnect(Disconnect {
        reason: ReasonCode::Success,
        properties: DisconnectProperties::default(),
    });
    assert_eq!(
        encoded(&disconnect, Version::V5, Sender::Server),
        vec![0xE0, 0]
    );
}

fn connect(version: Version, username: Option<&'static [u8]>) -> Packet {
    Packet::Connect(Box::new(Connect {
        version,
        client_id: ClientId::new(Bytes::from_static(b"c")).unwrap(),
        clean_start: true,
        keep_alive_secs: 30,
        will: None,
        username: username.map(Bytes::from_static),
        password: Some(Bytes::from_static(b"secret")),
        properties: ConnectProperties::default(),
    }))
}

#[test]
fn a_password_without_a_user_name_is_version_5_only() {
    let packet = connect(Version::V5, None);
    let mut bytes = BytesMut::from(&encoded(&packet, Version::V5, Sender::Client)[..]);
    assert_eq!(
        decode(&mut bytes, None, Sender::Client, &limits()).unwrap(),
        Some(packet)
    );

    // The same flags at protocol level 4.
    let mut v311 = encoded(
        &connect(Version::V311, Some(b"u")),
        Version::V311,
        Sender::Client,
    );
    v311[9] &= !0b1000_0000;
    let mut bytes = BytesMut::from(&v311[..]);
    assert_eq!(
        decode(&mut bytes, None, Sender::Client, &limits()),
        Err(DecodeError::InvalidConnectFlags)
    );
}

#[test]
fn the_first_packet_is_a_connect_and_the_only_one() {
    let mut ping = BytesMut::from(&[0xC0u8, 0][..]);
    assert_eq!(
        decode(&mut ping, None, Sender::Client, &limits()),
        Err(DecodeError::NotConnected)
    );
    let again = encoded(
        &connect(Version::V5, Some(b"u")),
        Version::V5,
        Sender::Client,
    );
    assert_eq!(
        decode_v5(&again, Sender::Client),
        Err(DecodeError::UnexpectedPacketType(1))
    );
}

#[test]
fn an_unknown_protocol_level_is_unsupported_version() {
    let mut bytes = encoded(
        &connect(Version::V5, Some(b"u")),
        Version::V5,
        Sender::Client,
    );
    bytes[8] = 3;
    let mut buf = BytesMut::from(&bytes[..]);
    let error = decode(&mut buf, None, Sender::Client, &limits()).unwrap_err();
    assert_eq!(error, DecodeError::UnsupportedProtocolLevel(3));
    assert_eq!(error.reason(), ReasonCode::UnsupportedProtocolVersion);
}

#[test]
fn an_oversized_packet_is_refused_from_its_header() {
    let small = Limits {
        max_packet_bytes: 64,
        ..Limits::default()
    };
    // Remaining Length 200, and nothing of the body yet.
    let mut buf = BytesMut::from(&[0x30u8, 0xC8, 0x01][..]);
    assert_eq!(
        decode(&mut buf, Some(Version::V5), Sender::Client, &small),
        Err(DecodeError::PacketTooLarge)
    );
}

fn subscribe_with(options: u8, filter: &[u8]) -> Vec<u8> {
    let mut body = vec![0, 1, 0, 0, u8::try_from(filter.len()).unwrap()];
    body.extend_from_slice(filter);
    body.push(options);
    let mut packet = vec![0x82, u8::try_from(body.len()).unwrap()];
    packet.extend(body);
    packet
}

#[test]
fn subscription_options_reserved_values_are_refused() {
    let ok = subscribe_with(0b0010_1101, b"a/b");
    let Some(Packet::Subscribe(s)) = decode_v5(&ok, Sender::Client).unwrap() else {
        panic!("a subscribe")
    };
    let options = s.filters[0].1;
    assert_eq!(options.qos, Qos::AtLeastOnce);
    assert!(options.no_local && options.retain_as_published);
    assert_eq!(options.retain_handling, RetainHandling::Never);

    for bad in [0b0011_0000, 0b0100_0000, 0b0000_0011] {
        assert!(
            decode_v5(&subscribe_with(bad, b"a/b"), Sender::Client).is_err(),
            "{bad:#010b}"
        );
    }
    // No Local on a shared subscription.
    assert_eq!(
        decode_v5(&subscribe_with(0b0100, b"$share/g/a"), Sender::Client),
        Err(DecodeError::InvalidSubscriptionOptions)
    );
}

#[test]
fn disconnect_fields_belong_to_one_side() {
    // "With Will" is the client's.
    assert_eq!(
        decode_v5(&[0xE0, 1, 0x04], Sender::Server),
        Err(DecodeError::ReasonNotAllowed(0x04))
    );
    assert!(
        decode_v5(&[0xE0, 1, 0x04], Sender::Client)
            .unwrap()
            .is_some()
    );
    // Session Expiry is the client's, Server Reference the server's.
    let expiry = [0xE0, 7, 0x00, 5, 0x11, 0, 0, 0, 1];
    assert!(decode_v5(&expiry, Sender::Client).unwrap().is_some());
    assert_eq!(
        decode_v5(&expiry, Sender::Server),
        Err(DecodeError::PropertyNotAllowed)
    );
}

#[test]
fn qos_2_is_decoded_so_that_it_can_be_refused_with_its_reason() {
    let publish = Packet::Publish(Publish {
        topic: Some(Topic::new(Bytes::from_static(b"t")).unwrap()),
        id: PacketId::new(1),
        qos: Qos::ExactlyOnce,
        duplicate: false,
        retain: false,
        payload: Bytes::new(),
        properties: None,
    });
    let bytes = encoded(&publish, Version::V5, Sender::Client);
    assert_eq!(decode_v5(&bytes, Sender::Client).unwrap(), Some(publish));
    // PUBREC, PUBREL, PUBCOMP wait for phase 6.
    assert_eq!(
        decode_v5(&[0x62, 2, 0, 1], Sender::Client),
        Err(DecodeError::UnexpectedPacketType(6))
    );
}
