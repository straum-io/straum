//! The gateway, one rule per test. MQTT bytes in and out through `codec`;
//! proposals are committed only when a test says so, by applying them with
//! `apply` — so every test can see what happens before commit, and after.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::num::NonZeroU16;
use std::time::Duration;

use apply::{Effect, Limits, Proposal, State};
use bytes::{Bytes, BytesMut};
use codec::{
    Connect, ConnectProperties, Packet, PacketId, PubAck, Publish, PublishProperties, Qos,
    ReasonCode, Sender, Subscribe, SubscriptionOptions, Version,
};
use domain::{ClientId, Topic, TopicFilter};
use gateway::{Action, Config, Gateway, Grant, Verdict};
use journal::{Chain, Entry, decode};

fn config() -> Config {
    Config {
        node: 1,
        first_session: 100,
        codec: codec::Limits::default(),
        max_connections: 8,
        connect_timeout: Duration::from_secs(10),
        max_pending: 4,
        max_pending_bytes: 1 << 20,
        max_total_pending_bytes: 1 << 20,
        commit_timeout: Duration::from_secs(30),
        retry_interval: Duration::from_secs(1),
        receive_maximum: NonZeroU16::new(8).unwrap(),
        topic_alias_maximum: 4,
        max_waiting: 4,
        max_waiting_bytes: 1 << 16,
        max_total_waiting_bytes: 1 << 20,
        max_retries: 8,
        authenticate: false,
        connect_through_log: false,
    }
}

const T0: Duration = Duration::ZERO;

/// A gateway, the log behind it (committed on demand), and its output.
struct Rig {
    gateway: Gateway,
    state: State,
    chain: Chain,
    /// Proposed, not yet committed.
    proposed: Vec<Proposal>,
    /// Bytes sent per connection, decoded.
    sent: Vec<(u64, Packet)>,
    closed: Vec<u64>,
    versions: std::collections::HashMap<u64, Version>,
    /// Credentials the gateway asked the driver to check.
    asked: Vec<(u64, Option<Bytes>, Option<Bytes>)>,
}

impl Rig {
    fn new(config: Config) -> Self {
        let mut rig = Rig {
            gateway: Gateway::new(config, T0),
            state: State::new(Limits::default()),
            chain: Chain::genesis(),
            proposed: Vec::new(),
            sent: Vec::new(),
            closed: Vec::new(),
            versions: Default::default(),
            asked: Vec::new(),
        };
        rig.drain();
        rig
    }

    fn ready() -> Self {
        let mut rig = Self::new(config());
        rig.commit();
        assert!(rig.gateway.ready());
        rig
    }

    fn drain(&mut self) {
        for action in self.gateway.take_actions() {
            match action {
                Action::Propose(p) => self.proposed.push(p),
                Action::Send { conn, bytes } => {
                    let version = self.versions.get(&conn).copied().unwrap_or(Version::V5);
                    let mut buf = BytesMut::from(&bytes[..]);
                    while let Some(packet) = codec::decode(
                        &mut buf,
                        Some(version),
                        Sender::Server,
                        &codec::Limits::default(),
                    )
                    .unwrap()
                    {
                        self.sent.push((conn, packet));
                    }
                }
                Action::Close { conn } => self.closed.push(conn),
                Action::Authenticate {
                    conn,
                    username,
                    password,
                } => self.asked.push((conn, username, password)),
            }
        }
    }

    /// Commits everything proposed so far and hands every effect over.
    fn commit(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        for proposal in std::mem::take(&mut self.proposed) {
            let mut body = Vec::new();
            proposal.encode(&mut body).unwrap();
            let mut records = Vec::new();
            self.chain
                .batch(
                    1,
                    &[Entry {
                        kind: proposal.kind(),
                        properties: proposal.properties(),
                        body: &body,
                    }],
                    &mut records,
                )
                .unwrap();
            for record in records {
                self.state
                    .apply(&decode(&record).unwrap(), &mut effects)
                    .unwrap();
            }
        }
        for effect in &effects {
            self.gateway.effect(effect, T0);
        }
        self.drain();
        effects
    }

    fn client(&mut self, conn: u64, packet: Packet, version: Version) {
        let mut out = BytesMut::new();
        codec::encode(&packet, version, Sender::Client, &mut out).unwrap();
        self.gateway.received(conn, &out, T0);
        self.drain();
    }

    fn connect_as(&mut self, conn: u64, id: &'static str, version: Version, keep_alive: u16) {
        self.versions.insert(conn, version);
        assert!(self.gateway.accept(conn, T0));
        let packet = Packet::Connect(Box::new(Connect {
            version,
            client_id: ClientId::new(Bytes::from_static(id.as_bytes())).unwrap(),
            clean_start: true,
            keep_alive_secs: keep_alive,
            will: None,
            username: None,
            password: None,
            properties: ConnectProperties::default(),
        }));
        self.client(conn, packet, version);
    }

    fn take_sent(&mut self) -> Vec<(u64, Packet)> {
        std::mem::take(&mut self.sent)
    }
}

fn publish(topic: &'static str, qos: Qos, id: u16) -> Packet {
    Packet::Publish(Publish {
        topic: Some(Topic::new(Bytes::from_static(topic.as_bytes())).unwrap()),
        id: (qos != Qos::AtMostOnce).then(|| PacketId::new(id).unwrap()),
        qos,
        duplicate: false,
        retain: false,
        payload: Bytes::from_static(b"m"),
        properties: None,
    })
}

fn subscribe(filters: &[(&'static str, Qos)]) -> Packet {
    Packet::Subscribe(Subscribe {
        id: PacketId::new(5).unwrap(),
        subscription_identifier: None,
        user: Vec::new(),
        filters: filters
            .iter()
            .map(|(f, q)| {
                (
                    TopicFilter::new(Bytes::from_static(f.as_bytes())).unwrap(),
                    SubscriptionOptions::qos(*q),
                )
            })
            .collect(),
    })
}

fn connack_reason(sent: &[(u64, Packet)], conn: u64) -> Option<ReasonCode> {
    sent.iter().find_map(|(c, p)| match p {
        Packet::ConnAck(ack) if *c == conn => Some(ack.reason),
        _ => None,
    })
}

#[test]
fn connections_wait_for_the_gateways_reset_to_commit() {
    let mut rig = Rig::new(config());
    assert!(matches!(
        &rig.proposed[..],
        [Proposal::NodeReset {
            node: 1,
            below: 100
        }]
    ));
    let proposed = rig.proposed.clone();
    rig.connect_as(1, "early", Version::V5, 0);
    assert_eq!(
        connack_reason(&rig.take_sent(), 1),
        Some(ReasonCode::ServerUnavailable)
    );
    assert_eq!(rig.closed, vec![1]);
    rig.proposed = proposed;
    rig.commit();
    rig.connect_as(2, "later", Version::V5, 0);
    assert_eq!(
        connack_reason(&rig.take_sent(), 2),
        Some(ReasonCode::Success)
    );
}

#[test]
fn a_puback_waits_for_the_commit() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "p", Version::V311, 0);
    rig.take_sent();
    rig.client(1, publish("t", Qos::AtLeastOnce, 9), Version::V311);
    assert!(rig.take_sent().is_empty(), "acknowledged before commit");
    // Counted (D-112): proposed, not yet acknowledged.
    let counted = rig.gateway.load().counted;
    assert_eq!((counted.publishes, counted.pubacks), (1, 0));
    rig.commit();
    assert!(
        matches!(&rig.take_sent()[..], [(1, Packet::PubAck(PubAck { id, .. }))] if id.get() == 9)
    );
    assert_eq!(rig.gateway.load().counted.pubacks, 1);
}

#[test]
fn a_suback_answers_every_filter_once_all_are_committed() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "s", Version::V5, 0);
    rig.take_sent();
    rig.client(
        1,
        subscribe(&[
            ("a", Qos::AtLeastOnce),
            ("$share/g/b", Qos::AtMostOnce),
            ("c", Qos::ExactlyOnce),
        ]),
        Version::V5,
    );
    assert!(rig.take_sent().is_empty());
    rig.commit();
    let sent = rig.take_sent();
    let Packet::SubAck(suback) = &sent[0].1 else {
        panic!("{sent:?}")
    };
    // QoS 2 is granted QoS 1; shared subscriptions are refused.
    assert_eq!(
        suback.reasons,
        vec![
            ReasonCode::GrantedQos1,
            ReasonCode::SharedSubscriptionsNotSupported,
            ReasonCode::GrantedQos1
        ]
    );
}

#[test]
fn a_qos1_delivery_is_acknowledged_through_the_log_until_its_effect_is_seen() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "sub", Version::V5, 0);
    rig.connect_as(2, "pub", Version::V5, 0);
    rig.client(1, subscribe(&[("t", Qos::AtLeastOnce)]), Version::V5);
    rig.commit();
    rig.take_sent();
    rig.client(2, publish("t", Qos::AtLeastOnce, 1), Version::V5);
    rig.commit();
    let delivery = rig
        .take_sent()
        .into_iter()
        .find_map(|(c, p)| match p {
            Packet::Publish(p) if c == 1 => p.id,
            _ => None,
        })
        .expect("delivered");
    let ack = Packet::PubAck(PubAck {
        id: delivery,
        reason: ReasonCode::Success,
        properties: Default::default(),
    });
    rig.client(1, ack, Version::V5);
    assert!(matches!(&rig.proposed[..], [Proposal::Ack { .. }]));
    // Lost on the way: the tick sends it again.
    rig.proposed.clear();
    rig.gateway.tick(Duration::from_secs(2));
    rig.drain();
    assert!(matches!(&rig.proposed[..], [Proposal::Ack { .. }]));
    let effects = rig.commit();
    assert!(effects.iter().any(|e| matches!(e, Effect::Acked { .. })));
    rig.gateway.tick(Duration::from_secs(4));
    rig.drain();
    assert!(rig.proposed.is_empty(), "retried after its effect");
    assert_eq!(rig.state.inflight(rig.gateway.session(1).unwrap()), 0);
}

#[test]
fn a_second_connection_with_the_same_client_id_takes_over() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "same", Version::V5, 0);
    let first = rig.gateway.session(1).unwrap();
    rig.connect_as(2, "same", Version::V5, 0);
    let sent = rig.take_sent();
    assert!(sent.iter().any(|(c, p)| *c == 1
        && matches!(p, Packet::Disconnect(d) if d.reason == ReasonCode::SessionTakenOver)));
    assert_eq!(rig.closed, vec![1]);
    assert!(
        rig.proposed
            .contains(&Proposal::SessionGone { origin: first })
    );
    assert_eq!(rig.gateway.client_ids().count(), 1);
}

#[test]
fn a_silent_client_is_closed_after_one_and_a_half_keep_alives() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "k", Version::V5, 10);
    rig.take_sent();
    rig.gateway.tick(Duration::from_secs(14));
    rig.drain();
    assert!(rig.closed.is_empty());
    rig.gateway.tick(Duration::from_secs(15));
    rig.drain();
    assert_eq!(rig.closed, vec![1]);
    assert!(rig.take_sent().iter().any(
        |(_, p)| matches!(p, Packet::Disconnect(d) if d.reason == ReasonCode::KeepAliveTimeout)
    ));
    let counted = rig.gateway.load().counted;
    assert_eq!((counted.closed, counted.closed_keep_alive), (1, 1));
}

/// A publish not committed within `commit_timeout` closes its publisher,
/// counted apart from other closes (D-112): a stall in commit shows as
/// such, not as clients leaving.
#[test]
fn a_publish_that_never_commits_closes_its_publisher_and_is_counted() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "p", Version::V311, 0);
    rig.client(1, publish("t", Qos::AtLeastOnce, 9), Version::V311);
    rig.gateway.tick(Duration::from_secs(31));
    rig.drain();
    assert_eq!(rig.closed, vec![1]);
    let counted = rig.gateway.load().counted;
    assert_eq!(
        (
            counted.closed,
            counted.closed_commit_timeout,
            counted.pubacks
        ),
        (1, 1, 0)
    );
}

#[test]
fn topic_aliases_stand_for_their_topic_and_an_unknown_one_closes() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "a", Version::V5, 0);
    let aliased = |topic: Option<&'static str>, alias: u16| {
        Packet::Publish(Publish {
            topic: topic.map(|t| Topic::new(Bytes::from_static(t.as_bytes())).unwrap()),
            id: None,
            qos: Qos::AtMostOnce,
            duplicate: false,
            retain: false,
            payload: Bytes::from_static(b"m"),
            properties: Some(Box::new(PublishProperties {
                topic_alias: NonZeroU16::new(alias),
                ..Default::default()
            })),
        })
    };
    rig.client(1, aliased(Some("long/topic"), 2), Version::V5);
    rig.client(1, aliased(None, 2), Version::V5);
    let topics: Vec<&str> = rig
        .proposed
        .iter()
        .filter_map(|p| match p {
            Proposal::Publish { topic, .. } => Some(topic.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(topics, vec!["long/topic", "long/topic"]);
    rig.client(1, aliased(None, 3), Version::V5);
    assert_eq!(rig.closed, vec![1]);
}

#[test]
fn deliveries_beyond_the_clients_receive_maximum_wait_for_its_acks() {
    let mut rig = Rig::ready();
    rig.versions.insert(1, Version::V5);
    assert!(rig.gateway.accept(1, T0));
    let connect = Packet::Connect(Box::new(Connect {
        version: Version::V5,
        client_id: ClientId::new(Bytes::from_static(b"slow")).unwrap(),
        clean_start: true,
        keep_alive_secs: 0,
        will: None,
        username: None,
        password: None,
        properties: ConnectProperties {
            receive_maximum: NonZeroU16::new(1).unwrap(),
            ..Default::default()
        },
    }));
    rig.client(1, connect, Version::V5);
    rig.connect_as(2, "pub", Version::V5, 0);
    rig.client(1, subscribe(&[("t", Qos::AtLeastOnce)]), Version::V5);
    rig.commit();
    rig.take_sent();
    rig.client(2, publish("t", Qos::AtLeastOnce, 1), Version::V5);
    rig.client(2, publish("t", Qos::AtLeastOnce, 2), Version::V5);
    rig.commit();
    let to_slow: Vec<PacketId> = rig
        .take_sent()
        .into_iter()
        .filter_map(|(c, p)| match p {
            Packet::Publish(p) if c == 1 => p.id,
            _ => None,
        })
        .collect();
    assert_eq!(to_slow.len(), 1, "one at a time");
    let ack = Packet::PubAck(PubAck {
        id: to_slow[0],
        reason: ReasonCode::Success,
        properties: Default::default(),
    });
    rig.client(1, ack, Version::V5);
    assert!(
        rig.take_sent()
            .iter()
            .any(|(c, p)| *c == 1 && matches!(p, Packet::Publish(_)))
    );
}

#[test]
fn qos_2_is_refused_with_its_reason() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "q2", Version::V5, 0);
    rig.take_sent();
    rig.client(1, publish("t", Qos::ExactlyOnce, 1), Version::V5);
    assert!(rig.take_sent().iter().any(
        |(_, p)| matches!(p, Packet::Disconnect(d) if d.reason == ReasonCode::QosNotSupported)
    ));
    assert_eq!(rig.closed, vec![1]);
}

#[test]
fn a_connection_at_its_pending_bound_is_not_read_until_commit() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "busy", Version::V311, 0);
    let mut bytes = BytesMut::new();
    for id in 1..=6u16 {
        codec::encode(
            &publish("t", Qos::AtLeastOnce, id),
            Version::V311,
            Sender::Client,
            &mut bytes,
        )
        .unwrap();
    }
    rig.gateway.received(1, &bytes, T0);
    rig.drain();
    // Four proposed, two left in the buffer; nothing dropped.
    assert_eq!(rig.proposed.len(), 4);
    assert!(!rig.gateway.wants_read(1));
    rig.commit();
    assert_eq!(rig.proposed.len(), 2, "the rest is read once there is room");
    rig.commit();
    let acks = rig
        .take_sent()
        .iter()
        .filter(|(_, p)| matches!(p, Packet::PubAck(_)))
        .count();
    assert_eq!(acks, 6);
    assert!(rig.gateway.wants_read(1));
}

#[test]
fn a_proposal_that_does_not_commit_in_time_closes_its_connection() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "lost", Version::V5, 0);
    rig.client(1, publish("t", Qos::AtLeastOnce, 1), Version::V5);
    rig.proposed.clear();
    rig.gateway.tick(Duration::from_secs(31));
    rig.drain();
    assert_eq!(rig.closed, vec![1]);
    assert_eq!(rig.gateway.load().pending, 0);
}

#[test]
fn a_session_nobody_can_name_is_refused_in_3_1_1() {
    let mut rig = Rig::ready();
    rig.versions.insert(1, Version::V311);
    assert!(rig.gateway.accept(1, T0));
    let connect = Packet::Connect(Box::new(Connect {
        version: Version::V311,
        client_id: ClientId::new(Bytes::new()).unwrap(),
        clean_start: false,
        keep_alive_secs: 0,
        will: None,
        username: None,
        password: None,
        properties: ConnectProperties::default(),
    }));
    rig.client(1, connect, Version::V311);
    assert_eq!(
        connack_reason(&rig.take_sent(), 1),
        Some(ReasonCode::ClientIdentifierNotValid)
    );
}

/// A client that takes one QoS 1 delivery and never acknowledges it holds
/// everything behind it; the at-most-once deliveries queued there are
/// bounded, and beyond the bound dropped and counted.
#[test]
fn deliveries_held_behind_a_stalled_client_are_bounded() {
    let mut rig = Rig::ready();
    rig.versions.insert(1, Version::V5);
    assert!(rig.gateway.accept(1, T0));
    let connect = Packet::Connect(Box::new(Connect {
        version: Version::V5,
        client_id: ClientId::new(Bytes::from_static(b"stuck")).unwrap(),
        clean_start: true,
        keep_alive_secs: 0,
        will: None,
        username: None,
        password: None,
        properties: ConnectProperties {
            receive_maximum: NonZeroU16::new(1).unwrap(),
            ..Default::default()
        },
    }));
    rig.client(1, connect, Version::V5);
    rig.connect_as(2, "pub", Version::V5, 0);
    rig.client(1, subscribe(&[("t", Qos::AtLeastOnce)]), Version::V5);
    rig.commit();
    // Two QoS 1: one goes, one waits for the receive maximum.
    rig.client(2, publish("t", Qos::AtLeastOnce, 1), Version::V5);
    rig.client(2, publish("t", Qos::AtLeastOnce, 2), Version::V5);
    rig.commit();
    // Then a stream of QoS 0, queued behind the waiting QoS 1.
    for _ in 0..20 {
        rig.client(2, publish("t", Qos::AtMostOnce, 0), Version::V5);
        rig.commit();
    }
    let load = rig.gateway.load();
    // max_waiting is 4: the waiting QoS 1 and three QoS 0.
    assert_eq!(load.dropped, 17);
    assert!(load.waiting_bytes <= 4);
}

/// While nothing commits, every closed session leaves a `SessionGone` to
/// retry; at the bound the gateway refuses new connections instead of
/// growing.
#[test]
fn lifecycle_retries_are_bounded_while_nothing_commits() {
    let mut rig = Rig::ready();
    let mut refused = false;
    for conn in 1..20u64 {
        rig.versions.insert(conn, Version::V5);
        if !rig.gateway.accept(conn, T0) {
            refused = true;
            break;
        }
        let packet = Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: ClientId::new(Bytes::from(format!("r{conn}"))).unwrap(),
            clean_start: true,
            keep_alive_secs: 0,
            will: None,
            username: None,
            password: None,
            properties: ConnectProperties::default(),
        }));
        rig.client(conn, packet, Version::V5);
        rig.gateway.closed(conn);
        rig.drain();
        // Never committed: the log is stalled.
        rig.proposed.clear();
    }
    assert!(refused, "accepted connections past the retry bound");
    assert!(rig.gateway.load().retries <= 8);
}

/// MQTT 5 message properties travel through the log to MQTT 5 subscribers
/// unchanged, and are dropped for 3.1.1 ones (D-65). Topic alias stays with
/// the publisher's connection.
#[test]
fn message_properties_reach_mqtt_5_subscribers() {
    use codec::UserProperty;
    let mut rig = Rig::ready();
    rig.connect_as(1, "v5", Version::V5, 0);
    rig.connect_as(2, "v311", Version::V311, 0);
    rig.connect_as(3, "pub", Version::V5, 0);
    rig.client(1, subscribe(&[("t", Qos::AtMostOnce)]), Version::V5);
    rig.client(2, subscribe(&[("t", Qos::AtMostOnce)]), Version::V311);
    rig.commit();
    rig.take_sent();
    let sent = PublishProperties {
        payload_format_utf8: true,
        message_expiry_secs: Some(60),
        topic_alias: NonZeroU16::new(1),
        response_topic: Some(Topic::new(Bytes::from_static(b"reply/here")).unwrap()),
        correlation_data: Some(Bytes::from_static(b"\x00\x01corr")),
        user: vec![
            UserProperty {
                name: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v1"),
            },
            UserProperty {
                name: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v2"),
            },
        ],
        subscription_identifiers: Vec::new(),
        content_type: Some(Bytes::from_static(b"text/plain")),
    };
    let publish = Packet::Publish(Publish {
        topic: Some(Topic::new(Bytes::from_static(b"t")).unwrap()),
        id: None,
        qos: Qos::AtMostOnce,
        duplicate: false,
        retain: false,
        payload: Bytes::from_static(b"m"),
        properties: Some(Box::new(sent.clone())),
    });
    rig.client(3, publish, Version::V5);
    rig.commit();
    let received: Vec<(u64, Option<Box<PublishProperties>>)> = rig
        .take_sent()
        .into_iter()
        .filter_map(|(c, p)| match p {
            Packet::Publish(p) => Some((c, p.properties)),
            _ => None,
        })
        .collect();
    let expected = PublishProperties {
        topic_alias: None,
        ..sent
    };
    assert_eq!(received, vec![(1, Some(Box::new(expected))), (2, None)]);
}

/// Step 7a (D-78 A): what this gateway cannot keep is refused at CONNECT,
/// not silently dropped — a will, a 3.1.1 session that outlives its
/// connection, an MQTT 5 session expiry. An MQTT 5 CleanStart=0 with no
/// expiry asks to keep nothing, and is accepted with SessionPresent=0.
#[test]
fn wills_persistent_sessions_and_expiry_are_refused_at_connect() {
    let will = || {
        Some(codec::Will {
            topic: Topic::new(Bytes::from_static(b"gone")).unwrap(),
            payload: Bytes::from_static(b"bye"),
            qos: Qos::AtMostOnce,
            retain: false,
            properties: codec::WillProperties::default(),
        })
    };
    let expiring = ConnectProperties {
        session_expiry_secs: 60,
        ..ConnectProperties::default()
    };
    let cases = [
        (
            Version::V5,
            true,
            will(),
            ConnectProperties::default(),
            false,
        ),
        (
            Version::V311,
            true,
            will(),
            ConnectProperties::default(),
            false,
        ),
        (
            Version::V311,
            false,
            None,
            ConnectProperties::default(),
            false,
        ),
        (Version::V5, true, None, expiring, false),
        (Version::V5, false, None, ConnectProperties::default(), true),
    ];
    for (conn, (version, clean_start, will, properties, accepted)) in (1..).zip(cases) {
        let mut rig = Rig::ready();
        rig.versions.insert(conn, version);
        assert!(rig.gateway.accept(conn, T0));
        let connect = Packet::Connect(Box::new(Connect {
            version,
            client_id: ClientId::new(Bytes::from_static(b"c")).unwrap(),
            clean_start,
            keep_alive_secs: 0,
            will,
            username: None,
            password: None,
            properties,
        }));
        rig.client(conn, connect, version);
        let expected = if accepted {
            ReasonCode::Success
        } else if version == Version::V311 {
            ReasonCode::ServerUnavailable
        } else {
            ReasonCode::ImplementationSpecificError
        };
        assert_eq!(
            connack_reason(&rig.take_sent(), conn),
            Some(expected),
            "case {conn}: {version:?}, clean start {clean_start}"
        );
    }
}

/// D-158 A: refuses publishing and subscribing under `denied`, allows the rest.
#[derive(Debug)]
struct Deny(&'static str);

impl Grant for Deny {
    fn may_publish(&self, topic: &Topic) -> bool {
        !topic.as_bytes().starts_with(self.0.as_bytes())
    }
    fn may_subscribe(&self, filter: &TopicFilter) -> bool {
        !filter.as_bytes().starts_with(self.0.as_bytes())
    }
}

fn connect_packet(id: &'static str, version: Version, user: Option<&'static str>) -> Packet {
    Packet::Connect(Box::new(Connect {
        version,
        client_id: ClientId::new(Bytes::from_static(id.as_bytes())).unwrap(),
        clean_start: true,
        keep_alive_secs: 0,
        will: None,
        username: user.map(|u| Bytes::from_static(u.as_bytes())),
        password: user.map(|_| Bytes::from_static(b"secret")),
        properties: ConnectProperties::default(),
    }))
}

impl Rig {
    fn authenticating() -> Self {
        let mut rig = Self::new(Config {
            authenticate: true,
            ..config()
        });
        rig.commit();
        assert!(rig.gateway.ready());
        rig
    }

    /// Connects through the credentials check with what `grant` allows.
    fn admitted(&mut self, conn: u64, version: Version, grant: Deny) {
        self.versions.insert(conn, version);
        assert!(self.gateway.accept(conn, T0));
        self.client(conn, connect_packet("c", version, Some("alice")), version);
        self.gateway
            .authenticated(conn, Verdict::Allowed(Box::new(grant)), T0);
        self.drain();
        assert_eq!(
            connack_reason(&self.take_sent(), conn),
            Some(ReasonCode::Success)
        );
    }
}

#[test]
fn a_connect_is_answered_only_once_its_credentials_are_checked() {
    let mut rig = Rig::authenticating();
    rig.versions.insert(1, Version::V5);
    assert!(rig.gateway.accept(1, T0));
    rig.client(
        1,
        connect_packet("c", Version::V5, Some("alice")),
        Version::V5,
    );
    assert_eq!(
        rig.asked,
        vec![(
            1,
            Some(Bytes::from_static(b"alice")),
            Some(Bytes::from_static(b"secret"))
        )]
    );
    assert!(rig.take_sent().is_empty(), "no CONNACK before the verdict");
    assert_eq!(rig.gateway.session(1), None);
    assert!(!rig.gateway.wants_read(1));
    rig.gateway
        .authenticated(1, Verdict::Allowed(Box::new(Deny("none"))), T0);
    rig.drain();
    assert_eq!(
        connack_reason(&rig.take_sent(), 1),
        Some(ReasonCode::Success)
    );
    assert!(rig.gateway.session(1).is_some());
}

#[test]
fn bad_credentials_and_a_refused_anonymous_client_are_told_so_and_closed() {
    for version in [Version::V5, Version::V311] {
        for (verdict, reason) in [
            (Verdict::BadCredentials, ReasonCode::BadUserNameOrPassword),
            (Verdict::NotAuthorized, ReasonCode::NotAuthorized),
        ] {
            let mut rig = Rig::authenticating();
            rig.versions.insert(1, version);
            assert!(rig.gateway.accept(1, T0));
            rig.client(1, connect_packet("c", version, None), version);
            assert_eq!(rig.asked.len(), 1);
            rig.gateway.authenticated(1, verdict, T0);
            rig.drain();
            // 3.1.1's return codes 4 and 5 decode to the same names.
            assert_eq!(connack_reason(&rig.take_sent(), 1), Some(reason));
            assert_eq!(rig.closed, vec![1]);
            assert!(rig.proposed.is_empty(), "a refused CONNECT writes nothing");
        }
    }
}

#[test]
fn what_follows_a_connect_waits_for_its_verdict() {
    let mut rig = Rig::authenticating();
    rig.versions.insert(1, Version::V5);
    assert!(rig.gateway.accept(1, T0));
    let mut bytes = BytesMut::new();
    for packet in [
        connect_packet("c", Version::V5, Some("alice")),
        publish("a/b", Qos::AtLeastOnce, 1),
    ] {
        codec::encode(&packet, Version::V5, Sender::Client, &mut bytes).unwrap();
    }
    rig.gateway.received(1, &bytes, T0);
    rig.drain();
    assert!(rig.proposed.is_empty(), "the publish waits for the verdict");
    rig.gateway
        .authenticated(1, Verdict::Allowed(Box::new(Deny("none"))), T0);
    rig.drain();
    assert!(matches!(&rig.proposed[..], [Proposal::Publish { .. }]));
}

#[test]
fn a_verdict_for_a_connection_that_timed_out_is_ignored() {
    let mut rig = Rig::authenticating();
    rig.versions.insert(1, Version::V5);
    assert!(rig.gateway.accept(1, T0));
    rig.client(
        1,
        connect_packet("c", Version::V5, Some("alice")),
        Version::V5,
    );
    rig.gateway.tick(Duration::from_secs(11));
    rig.drain();
    assert_eq!(rig.closed, vec![1]);
    rig.gateway.authenticated(
        1,
        Verdict::Allowed(Box::new(Deny("none"))),
        Duration::from_secs(11),
    );
    rig.drain();
    assert!(rig.take_sent().is_empty());
    assert_eq!(rig.gateway.session(1), None);
}

#[test]
fn a_publish_its_session_may_not_make_writes_nothing() {
    // MQTT 5: QoS 1 is answered "not authorized", QoS 0 dropped.
    let mut rig = Rig::authenticating();
    rig.admitted(1, Version::V5, Deny("secret/"));
    rig.client(1, publish("secret/x", Qos::AtLeastOnce, 7), Version::V5);
    rig.client(1, publish("secret/y", Qos::AtMostOnce, 0), Version::V5);
    assert!(rig.proposed.is_empty());
    let sent = rig.take_sent();
    assert!(matches!(
        &sent[..],
        [(
            1,
            Packet::PubAck(PubAck {
                reason: ReasonCode::NotAuthorized,
                ..
            })
        )]
    ));
    assert!(rig.closed.is_empty());
    rig.client(1, publish("open/x", Qos::AtLeastOnce, 8), Version::V5);
    assert!(matches!(&rig.proposed[..], [Proposal::Publish { .. }]));

    // 3.1.1 has no refusal to send: the connection closes.
    let mut rig = Rig::authenticating();
    rig.admitted(2, Version::V311, Deny("secret/"));
    rig.client(2, publish("secret/x", Qos::AtLeastOnce, 7), Version::V311);
    // The session ends (its `SessionGone`); the publish is never proposed.
    assert!(matches!(&rig.proposed[..], [Proposal::SessionGone { .. }]));
    assert_eq!(rig.closed, vec![2]);
}

#[test]
fn a_filter_its_session_may_not_subscribe_is_refused_and_the_others_proposed() {
    let mut rig = Rig::authenticating();
    rig.admitted(1, Version::V5, Deny("secret/"));
    rig.client(
        1,
        subscribe(&[("open/#", Qos::AtLeastOnce), ("secret/#", Qos::AtLeastOnce)]),
        Version::V5,
    );
    assert_eq!(rig.proposed.len(), 1, "only the allowed filter is proposed");
    rig.commit();
    let suback = rig.take_sent().into_iter().find_map(|(_, p)| match p {
        Packet::SubAck(ack) => Some(ack.reasons),
        _ => None,
    });
    assert_eq!(
        suback,
        Some(vec![ReasonCode::GrantedQos1, ReasonCode::NotAuthorized])
    );
}

#[test]
fn without_credentials_a_connect_is_answered_at_once() {
    let mut rig = Rig::ready();
    rig.connect_as(1, "c", Version::V5, 0);
    assert!(rig.asked.is_empty());
    assert_eq!(
        connack_reason(&rig.take_sent(), 1),
        Some(ReasonCode::Success)
    );
}

/// MQTT 5 §4.6: PUBACKs go out in the order the publishes came. A refusal
/// behind an allowed QoS 1 publish still waiting for its commit cannot go
/// first: the connection closes, telling why, and nothing more is proposed.
#[test]
fn a_refusal_behind_a_publish_awaiting_commit_closes_instead_of_overtaking_it() {
    let mut rig = Rig::authenticating();
    rig.admitted(1, Version::V5, Deny("secret/"));
    rig.client(1, publish("open/x", Qos::AtLeastOnce, 1), Version::V5);
    rig.client(1, publish("secret/x", Qos::AtLeastOnce, 2), Version::V5);
    let sent = rig.take_sent();
    assert!(
        !sent.iter().any(|(_, p)| matches!(p, Packet::PubAck(_))),
        "no PUBACK ahead of the first publish's: {sent:?}"
    );
    assert!(sent.iter().any(|(_, p)| matches!(
        p,
        Packet::Disconnect(d) if d.reason == ReasonCode::NotAuthorized
    )));
    assert_eq!(rig.closed, vec![1]);
    assert_eq!(
        rig.proposed
            .iter()
            .filter(|p| matches!(p, Proposal::Publish { .. }))
            .count(),
        1,
        "only the allowed publish"
    );
}
