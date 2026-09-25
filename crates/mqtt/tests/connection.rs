//! Packet ↔ Command mapping and the per-connection identifier table.
//!
//! `allow-unwrap-in-tests` only covers code inside `#[test]` functions.
#![allow(clippy::unwrap_used)]

use bytes::{Bytes, BytesMut};
use domain::{ClientId, DeliveryGuarantee, DeliveryId, Topic, TopicFilter};
use mqtt::{
    Connection, Disposition, MqttError, MqttLimits, Packet, PacketId, PacketIds, Reaction, decode,
    encode_outbound, encode_pong,
};
use protocol::{
    Command, ConnectRejectReason, Delivery, DisconnectReason, Outbound, RequestId, Response,
    SubscribeResult,
};
use std::time::{Duration, Instant};

fn limits() -> MqttLimits {
    MqttLimits::default()
}

fn ids() -> PacketIds {
    PacketIds::with_capacity(4)
}

fn client_id(value: &str) -> ClientId {
    ClientId::new(Bytes::copy_from_slice(value.as_bytes())).unwrap()
}

fn topic(value: &str) -> Topic {
    Topic::new(Bytes::copy_from_slice(value.as_bytes())).unwrap()
}

fn filter(value: &str) -> TopicFilter {
    TopicFilter::new(Bytes::copy_from_slice(value.as_bytes())).unwrap()
}

fn id(value: u16) -> PacketId {
    PacketId::new(value).unwrap()
}

fn connect_packet() -> Packet {
    Packet::Connect(Box::new(mqtt::Connect {
        client_id: client_id("client"),
        clean_session: true,
        keep_alive_secs: 60,
        username: None,
        password: None,
        will: None,
    }))
}

/// A connection past the handshake.
fn connected() -> (Connection, PacketIds) {
    let mut connection = Connection::new(Instant::now(), &limits());
    let mut ids = ids();
    connection
        .on_packet(connect_packet(), Instant::now(), &mut ids)
        .unwrap();
    assert!(connection.is_connected());

    (connection, ids)
}
#[test]
fn connect_is_handed_over_once_and_not_as_a_command() {
    let mut connection = Connection::new(Instant::now(), &limits());
    let mut ids = ids();

    // A CONNECT is not a command until its credentials have been checked.
    assert_eq!(
        connection
            .on_packet(connect_packet(), Instant::now(), &mut ids)
            .unwrap(),
        Reaction::Connect(Box::new(mqtt::Connect {
            client_id: client_id("client"),
            clean_session: true,
            keep_alive_secs: 60,
            username: None,
            password: None,
            will: None,
        }))
    );
    assert_eq!(
        connection.on_packet(connect_packet(), Instant::now(), &mut ids),
        Err(MqttError::AlreadyConnected)
    );
}

#[test]
fn nothing_precedes_connect() {
    for packet in [
        Packet::PingReq,
        Packet::Disconnect,
        Packet::PubAck(id(1)),
        Packet::Publish(mqtt::Publish {
            topic: topic("a"),
            id: None,
            payload: Bytes::new(),
            retain: false,
        }),
    ] {
        let mut connection = Connection::new(Instant::now(), &limits());
        assert_eq!(
            connection.on_packet(packet, Instant::now(), &mut ids()),
            Err(MqttError::NotConnected)
        );
    }
}

#[test]
fn publish_carries_its_guarantee_and_acknowledgement() {
    let (mut connection, mut ids) = connected();

    let at_most_once = Packet::Publish(mqtt::Publish {
        topic: topic("a/b"),
        id: None,
        payload: Bytes::from_static(b"x"),
        retain: false,
    });
    let Reaction::Forward(Command::Publish { ack, publish }) = connection
        .on_packet(at_most_once, Instant::now(), &mut ids)
        .unwrap()
    else {
        panic!("expected a publish command");
    };
    assert_eq!(ack, None);
    assert_eq!(publish.guarantee, DeliveryGuarantee::AtMostOnce);
    assert_eq!(publish.topic.as_str(), "a/b");

    let at_least_once = Packet::Publish(mqtt::Publish {
        topic: topic("a/b"),
        id: Some(id(77)),
        payload: Bytes::from_static(b"x"),
        retain: false,
    });
    let Reaction::Forward(Command::Publish { ack, publish }) = connection
        .on_packet(at_least_once, Instant::now(), &mut ids)
        .unwrap()
    else {
        panic!("expected a publish command");
    };
    // The request id is the packet id the client will see in the PUBACK.
    assert_eq!(ack, Some(RequestId(77)));
    assert_eq!(publish.guarantee, DeliveryGuarantee::AtLeastOnce);
}

#[test]
fn subscriptions_keep_their_request_id() {
    let (mut connection, mut ids) = connected();

    let subscribe = Packet::Subscribe(mqtt::Subscribe {
        id: id(5),
        filters: vec![(filter("a/#"), DeliveryGuarantee::AtLeastOnce)],
    });
    assert_eq!(
        connection
            .on_packet(subscribe, Instant::now(), &mut ids)
            .unwrap(),
        Reaction::Forward(Command::Subscribe {
            request: RequestId(5),
            filters: vec![(filter("a/#"), DeliveryGuarantee::AtLeastOnce)],
        })
    );

    let unsubscribe = Packet::Unsubscribe(mqtt::Unsubscribe {
        id: id(6),
        filters: vec![filter("a/#")],
    });
    assert_eq!(
        connection
            .on_packet(unsubscribe, Instant::now(), &mut ids)
            .unwrap(),
        Reaction::Forward(Command::Unsubscribe {
            request: RequestId(6),
            filters: vec![filter("a/#")],
        })
    );
}

#[test]
fn keepalive_never_reaches_the_broker() {
    let (mut connection, mut ids) = connected();
    assert_eq!(
        connection
            .on_packet(Packet::PingReq, Instant::now(), &mut ids)
            .unwrap(),
        Reaction::Pong
    );

    let mut buf = BytesMut::new();
    encode_pong(&mut buf);
    assert_eq!(buf.as_ref(), [0xd0, 0x00]);
}

#[test]
fn disconnect_ends_the_conversation() {
    let (mut connection, mut ids) = connected();
    assert_eq!(
        connection
            .on_packet(Packet::Disconnect, Instant::now(), &mut ids)
            .unwrap(),
        Reaction::Forward(Command::Disconnect {
            reason: DisconnectReason::Client
        })
    );
    assert_eq!(
        connection.on_packet(Packet::PingReq, Instant::now(), &mut ids),
        Err(MqttError::AfterDisconnect)
    );

    assert_eq!(
        Connection::connection_lost(),
        Command::Disconnect {
            reason: DisconnectReason::ConnectionLost
        }
    );
}

#[test]
fn an_at_least_once_delivery_round_trips_through_the_identifier_table() {
    let (mut connection, mut ids) = connected();
    let mut buf = BytesMut::new();

    let delivery = Outbound::Deliver(Delivery {
        id: Some(DeliveryId(9_000_000_001)),
        topic: topic("a/b"),
        payload: Bytes::from_static(b"payload"),
        duplicate: false,
        retain: false,
    });
    assert_eq!(
        encode_outbound(&delivery, &mut ids, &mut buf, &limits()).unwrap(),
        Disposition::KeepOpen
    );
    assert_eq!(ids.len(), 1);

    // What went out on the wire is what the client acknowledges.
    let Packet::Publish(sent) = decode(&mut buf, &limits()).unwrap().unwrap() else {
        panic!("expected PUBLISH");
    };
    let sent_id = sent.id.unwrap();

    assert_eq!(
        connection
            .on_packet(Packet::PubAck(sent_id), Instant::now(), &mut ids)
            .unwrap(),
        Reaction::Forward(Command::Ack(DeliveryId(9_000_000_001)))
    );
    assert!(ids.is_empty());

    // The same acknowledgement twice is a violation, not a second release.
    assert_eq!(
        connection.on_packet(Packet::PubAck(sent_id), Instant::now(), &mut ids),
        Err(MqttError::UnknownPacketId)
    );
}

#[test]
fn at_most_once_deliveries_take_no_identifier() {
    let (_, mut ids) = connected();
    let mut buf = BytesMut::new();

    let delivery = Outbound::Deliver(Delivery {
        id: None,
        topic: topic("a/b"),
        payload: Bytes::from_static(b"payload"),
        duplicate: false,
        retain: false,
    });
    encode_outbound(&delivery, &mut ids, &mut buf, &limits()).unwrap();

    assert!(ids.is_empty());
    let Packet::Publish(sent) = decode(&mut buf, &limits()).unwrap().unwrap() else {
        panic!("expected PUBLISH");
    };
    assert_eq!(sent.id, None);
}

#[test]
fn replies_and_their_dispositions() {
    let mut ids = ids();
    let mut buf = BytesMut::new();

    let cases = [
        (
            Outbound::Reply(Response::Connected {
                session_present: false,
            }),
            Disposition::KeepOpen,
            vec![0x20, 0x02, 0x00, 0x00],
        ),
        (
            Outbound::Reply(Response::ConnectRejected(
                ConnectRejectReason::ServerUnavailable,
            )),
            Disposition::CloseAfterWrite,
            vec![0x20, 0x02, 0x00, 0x03],
        ),
        (
            Outbound::Reply(Response::PublishAccepted(RequestId(8))),
            Disposition::KeepOpen,
            vec![0x40, 0x02, 0x00, 0x08],
        ),
        (
            Outbound::Reply(Response::Subscribed {
                request: RequestId(2),
                granted: vec![
                    SubscribeResult::Granted(DeliveryGuarantee::AtLeastOnce),
                    SubscribeResult::Failure,
                ],
            }),
            Disposition::KeepOpen,
            vec![0x90, 0x04, 0x00, 0x02, 0x01, 0x80],
        ),
        (
            Outbound::Reply(Response::Unsubscribed(RequestId(3))),
            Disposition::KeepOpen,
            vec![0xb0, 0x02, 0x00, 0x03],
        ),
    ];

    for (outbound, expected, bytes) in cases {
        buf.clear();
        assert_eq!(
            encode_outbound(&outbound, &mut ids, &mut buf, &limits()).unwrap(),
            expected,
            "{outbound:?}"
        );
        assert_eq!(buf.as_ref(), bytes, "{outbound:?}");
    }
}

#[test]
fn a_broker_close_writes_nothing() {
    let mut buf = BytesMut::new();
    let close = Outbound::Close(protocol::CloseReason::SlowConsumer);

    assert_eq!(
        encode_outbound(&close, &mut ids(), &mut buf, &limits()).unwrap(),
        Disposition::CloseAfterWrite
    );
    assert!(buf.is_empty(), "3.1.1 has no server DISCONNECT");
}

#[test]
fn a_request_id_this_connection_never_issued_is_refused() {
    let mut buf = BytesMut::new();
    for request in [RequestId(0), RequestId(70_000)] {
        assert_eq!(
            encode_outbound(
                &Outbound::Reply(Response::PublishAccepted(request)),
                &mut ids(),
                &mut buf,
                &limits()
            ),
            Err(MqttError::ForeignRequestId)
        );
    }
}

#[test]
fn identifiers_are_bounded_reused_and_never_zero() {
    let mut table = PacketIds::with_capacity(2);
    let first = table.assign(DeliveryId(1)).unwrap();
    let second = table.assign(DeliveryId(2)).unwrap();
    assert_ne!(first, second);

    assert_eq!(table.assign(DeliveryId(3)), Err(MqttError::InflightFull));

    assert_eq!(table.release(first), Some(DeliveryId(1)));
    let third = table.assign(DeliveryId(3)).unwrap();
    assert_ne!(third, second);

    // Identifiers wrap around the whole 1..=65535 range without ever hitting 0.
    let mut table = PacketIds::with_capacity(1);
    for delivery in 0..70_000u64 {
        let assigned = table.assign(DeliveryId(delivery)).unwrap();
        assert!(assigned.get() != 0);
        assert_eq!(table.release(assigned), Some(DeliveryId(delivery)));
    }
}

fn connect_with_keep_alive(keep_alive_secs: u16) -> Packet {
    Packet::Connect(Box::new(mqtt::Connect {
        client_id: client_id("client"),
        clean_session: true,
        keep_alive_secs,
        username: None,
        password: None,
        will: None,
    }))
}

#[test]
fn a_connection_that_never_sends_connect_expires() {
    let start = Instant::now();
    let connection = Connection::new(start, &limits());

    // STRAUM_CONNECT_TIMEOUT_MS, counted from accept.
    assert_eq!(
        connection.deadline_at(),
        Some(start + Duration::from_secs(10))
    );
    assert!(!connection.is_expired(start + Duration::from_millis(9_999)));
    assert!(connection.is_expired(start + Duration::from_secs(10)));
}

#[test]
fn keep_alive_replaces_the_connect_deadline_and_follows_traffic() {
    let start = Instant::now();
    let mut connection = Connection::new(start, &limits());
    let mut ids = ids();

    connection
        .on_packet(connect_with_keep_alive(10), start, &mut ids)
        .unwrap();
    // 1.5 × 10 s, not the connect timeout any more.
    assert_eq!(
        connection.deadline_at(),
        Some(start + Duration::from_secs(15))
    );

    let later = start + Duration::from_secs(10);
    connection
        .on_packet(Packet::PingReq, later, &mut ids)
        .unwrap();
    assert_eq!(
        connection.deadline_at(),
        Some(later + Duration::from_secs(15))
    );
    assert!(!connection.is_expired(later + Duration::from_secs(14)));
    assert!(connection.is_expired(later + Duration::from_secs(15)));
}

#[test]
fn keep_alive_zero_leaves_a_connection_free_to_stay_silent() {
    let start = Instant::now();
    let mut connection = Connection::new(start, &limits());

    connection
        .on_packet(connect_with_keep_alive(0), start, &mut ids())
        .unwrap();

    assert_eq!(connection.deadline_at(), None);
    assert!(!connection.is_expired(start + Duration::from_secs(86_400)));
}
