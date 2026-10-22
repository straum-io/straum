//! Step 7c stage 3 at the gateway (ADR-0008 "Step 7c design", D-176 A,
//! D-177 A): CONNECT through the committed log, sessions across connections
//! and gateways. Two gateways share one log, committed only when a test says
//! so, with `apply`'s client ids on; one rule per test.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::num::NonZeroU16;
use std::time::Duration;

use apply::{ClientEffect, ConnectLimits, Effect, Limits, Proposal, State};
use bytes::{Bytes, BytesMut};
use codec::{
    Connect, ConnectProperties, Disconnect, DisconnectProperties, Packet, PacketId, PubAck,
    Publish, Qos, ReasonCode, Sender, Subscribe, SubscriptionOptions, Version,
};
use domain::{ClientId, Topic, TopicFilter};
use gateway::{Action, Config, Gateway};
use journal::{Chain, Entry, decode};

fn config(node: u8) -> Config {
    Config {
        node,
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
        connect_through_log: true,
    }
}

const T0: Duration = Duration::ZERO;

/// Two gateways, nodes 1 and 2, over one log.
struct Rig {
    gateways: Vec<Gateway>,
    state: State,
    chain: Chain,
    /// Proposed by either gateway, not yet committed, in order.
    proposed: Vec<Proposal>,
    /// (gateway, connection, packet) sent.
    sent: Vec<(usize, u64, Packet)>,
    closed: Vec<(usize, u64)>,
    versions: std::collections::HashMap<(usize, u64), Version>,
}

impl Rig {
    fn new(connect: ConnectLimits) -> Self {
        let mut rig = Rig {
            gateways: vec![Gateway::new(config(1), T0), Gateway::new(config(2), T0)],
            state: State::with_connect(Limits::default(), connect),
            chain: Chain::genesis(),
            proposed: Vec::new(),
            sent: Vec::new(),
            closed: Vec::new(),
            versions: Default::default(),
        };
        rig.drain();
        rig.commit();
        assert!(rig.gateways.iter().all(Gateway::ready));
        rig
    }

    fn ready() -> Self {
        Self::new(ConnectLimits::default())
    }

    fn drain(&mut self) {
        for g in 0..self.gateways.len() {
            for action in self.gateways[g].take_actions() {
                match action {
                    Action::Propose(p) => self.proposed.push(p),
                    Action::Send { conn, bytes } => {
                        let version = self
                            .versions
                            .get(&(g, conn))
                            .copied()
                            .unwrap_or(Version::V5);
                        let mut buf = BytesMut::from(&bytes[..]);
                        while let Some(packet) = codec::decode(
                            &mut buf,
                            Some(version),
                            Sender::Server,
                            &codec::Limits::default(),
                        )
                        .unwrap()
                        {
                            self.sent.push((g, conn, packet));
                        }
                    }
                    Action::Close { conn } => self.closed.push((g, conn)),
                    Action::Authenticate { .. } => {}
                }
            }
        }
    }

    /// Commits what was proposed, in order; every gateway is handed each
    /// record's client effects, then its effects, then the lsn applied.
    fn commit(&mut self) -> Vec<ClientEffect> {
        let mut all = Vec::new();
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
                let record = decode(&record).unwrap();
                let (mut effects, mut clients) = (Vec::<Effect>::new(), Vec::new());
                self.state
                    .apply_connect(&record, &mut effects, &mut clients)
                    .unwrap();
                for g in &mut self.gateways {
                    for effect in &clients {
                        g.client_effect(effect, T0);
                    }
                    for effect in &effects {
                        g.effect(effect, T0);
                    }
                    g.applied(record.lsn);
                }
                all.extend(clients);
            }
            self.drain();
        }
        all
    }

    fn send(&mut self, g: usize, conn: u64, packets: &[Packet]) {
        let version = self
            .versions
            .get(&(g, conn))
            .copied()
            .unwrap_or(Version::V5);
        let mut out = BytesMut::new();
        for packet in packets {
            codec::encode(packet, version, Sender::Client, &mut out).unwrap();
        }
        self.gateways[g].received(conn, &out, T0);
        self.drain();
    }

    /// A CONNECT on a new connection; not committed.
    fn connect(&mut self, g: usize, conn: u64, how: How) {
        assert!(self.gateways[g].accept(conn, T0));
        self.versions.insert((g, conn), how.version);
        let packet = Packet::Connect(Box::new(Connect {
            version: how.version,
            client_id: ClientId::new(Bytes::from_static(how.id.as_bytes())).unwrap(),
            clean_start: how.clean_start,
            keep_alive_secs: 0,
            will: None,
            username: None,
            password: None,
            properties: ConnectProperties {
                session_expiry_secs: how.expiry,
                ..ConnectProperties::default()
            },
        }));
        self.send(g, conn, &[packet]);
    }

    fn connack(&self, g: usize, conn: u64) -> Option<&codec::ConnAck> {
        self.sent.iter().find_map(|(at, c, p)| match p {
            Packet::ConnAck(ack) if *at == g && *c == conn => Some(&**ack),
            _ => None,
        })
    }

    fn publishes(&self, g: usize, conn: u64) -> Vec<&Publish> {
        self.sent
            .iter()
            .filter_map(|(at, c, p)| match p {
                Packet::Publish(p) if *at == g && *c == conn => Some(p),
                _ => None,
            })
            .collect()
    }

    fn subscribe(&mut self, g: usize, conn: u64, filter: &'static str) {
        self.send(
            g,
            conn,
            &[Packet::Subscribe(Subscribe {
                id: PacketId::new(1).unwrap(),
                subscription_identifier: None,
                user: Vec::new(),
                filters: vec![(
                    TopicFilter::new(Bytes::from_static(filter.as_bytes())).unwrap(),
                    SubscriptionOptions::qos(Qos::AtLeastOnce),
                )],
            })],
        );
        self.commit();
    }

    fn publish(&mut self, g: usize, conn: u64, payload: &'static str) {
        self.send(
            g,
            conn,
            &[Packet::Publish(Publish {
                topic: Some(Topic::new(Bytes::from_static(b"t")).unwrap()),
                id: PacketId::new(9),
                qos: Qos::AtLeastOnce,
                duplicate: false,
                retain: false,
                payload: Bytes::from_static(payload.as_bytes()),
                properties: None,
            })],
        );
        self.commit();
    }
}

fn goodbye() -> Packet {
    Packet::Disconnect(Disconnect {
        reason: ReasonCode::Success,
        properties: DisconnectProperties::default(),
    })
}

#[derive(Clone, Copy)]
struct How {
    version: Version,
    id: &'static str,
    clean_start: bool,
    expiry: u32,
}

fn v5(id: &'static str, clean_start: bool, expiry: u32) -> How {
    How {
        version: Version::V5,
        id,
        clean_start,
        expiry,
    }
}

fn v311(id: &'static str, clean_session: bool) -> How {
    How {
        version: Version::V311,
        id,
        clean_start: clean_session,
        expiry: 0,
    }
}

/// A connection open with a persisted session `id` subscribed to `t`, on
/// gateway `g`.
fn persisted(rig: &mut Rig, g: usize, conn: u64, id: &'static str) {
    rig.connect(g, conn, v5(id, true, 60));
    rig.commit();
    rig.subscribe(g, conn, "t");
}

/// Nothing is answered before the `Connect` commits; the CONNACK comes from
/// its effect, and `seen` is what the gateway had applied.
#[test]
fn a_connect_is_answered_only_from_its_committed_record() {
    let mut rig = Rig::ready();
    let seen = rig.state.applied();
    rig.connect(0, 1, v5("c", true, 0));
    assert!(rig.connack(0, 1).is_none(), "answered before commit");
    assert!(matches!(
        rig.proposed.as_slice(),
        [Proposal::Connect { seen: s, clean_start: true, persist: false, .. }] if *s == seen
    ));
    // Nothing more is read until then.
    assert!(!rig.gateways[0].wants_read(1));
    rig.commit();
    let ack = rig.connack(0, 1).unwrap();
    assert_eq!(ack.reason, ReasonCode::Success);
    assert!(!ack.session_present);
    assert_eq!(ack.properties.session_expiry_secs, None);
    assert!(rig.gateways[0].wants_read(1));
}

/// D-177 A: an MQTT 5 interval asks for persistence alone, Clean Start
/// alone for a new session; CONNACK says 0xFFFFFFFF for a persisted one.
/// 3.1.1's CleanSession is both.
#[test]
fn mqtt5_expiry_and_clean_start_map_to_persist_and_clean_start_apart() {
    for (how, clean_start, persist) in [
        (v5("a", true, 0), true, false),
        (v5("b", false, 0), false, false),
        (v5("c", true, 60), true, true),
        (v5("d", false, 60), false, true),
        (v5("e", true, u32::MAX), true, true),
        (v5("f", false, u32::MAX), false, true),
        (v311("g", true), true, false),
        (v311("h", false), false, true),
    ] {
        let mut rig = Rig::ready();
        rig.connect(0, 1, how);
        assert!(
            matches!(
                rig.proposed.as_slice(),
                [Proposal::Connect { clean_start: c, persist: p, .. }] if *c == clean_start && *p == persist
            ),
            "{}: {:?}",
            how.id,
            rig.proposed
        );
        rig.commit();
        let ack = rig.connack(0, 1).unwrap();
        assert_eq!(ack.reason, ReasonCode::Success, "{}", how.id);
        let expected = (persist && how.version == Version::V5).then_some(u32::MAX);
        assert_eq!(ack.properties.session_expiry_secs, expected, "{}", how.id);
    }
}

/// Clean Start 0 resumes a kept session; Clean Start 1 with an interval
/// starts a new, persisted one (MQTT 5 §3.1.2.4).
#[test]
fn clean_start_decides_whether_a_kept_session_is_resumed() {
    let mut rig = Rig::ready();
    persisted(&mut rig, 0, 1, "c");
    rig.send(0, 1, &[goodbye()]);
    rig.commit();
    rig.connect(0, 2, v5("c", false, 60));
    rig.commit();
    assert!(rig.connack(0, 2).unwrap().session_present);
    rig.send(0, 2, &[goodbye()]);
    rig.commit();
    rig.connect(0, 3, v5("c", true, 60));
    rig.commit();
    let ack = rig.connack(0, 3).unwrap();
    assert!(!ack.session_present);
    assert_eq!(ack.properties.session_expiry_secs, Some(u32::MAX));
}

/// A newer CONNECT through another gateway takes the session over: the old
/// connection is closed as taken over, and proposes nothing more.
#[test]
fn a_connect_through_another_gateway_takes_the_session_over() {
    let mut rig = Rig::ready();
    persisted(&mut rig, 0, 1, "c");
    rig.connect(1, 7, v5("c", false, 60));
    let clients = rig.commit();
    assert!(
        clients
            .iter()
            .any(|e| matches!(e, ClientEffect::TakenOver { .. }))
    );
    assert!(rig.closed.contains(&(0, 1)));
    assert!(rig.sent.iter().any(|(g, c, p)| *g == 0
        && *c == 1
        && matches!(p, Packet::Disconnect(d) if d.reason == ReasonCode::SessionTakenOver)));
    assert!(
        !rig.proposed
            .iter()
            .any(|p| matches!(p, Proposal::Disconnect { .. })),
        "a detached connection proposed a Disconnect"
    );
    assert!(rig.connack(1, 7).unwrap().session_present);
}

/// What the session had not acknowledged goes again on resume, DUP set,
/// before anything new; acknowledging it then works.
#[test]
fn unacknowledged_deliveries_go_again_with_dup_set_on_resume() {
    let mut rig = Rig::ready();
    persisted(&mut rig, 0, 1, "sub");
    rig.connect(0, 2, v5("pub", true, 0));
    rig.commit();
    rig.publish(0, 2, "one");
    assert_eq!(rig.publishes(0, 1).len(), 1);
    // The socket drops before the client acknowledges.
    rig.gateways[0].closed(1);
    rig.drain();
    rig.commit();
    rig.publish(0, 2, "two");
    rig.connect(1, 3, v5("sub", false, 60));
    rig.commit();
    let got: Vec<(bool, &[u8])> = rig
        .publishes(1, 3)
        .iter()
        .map(|p| (p.duplicate, p.payload.as_ref()))
        .collect();
    assert_eq!(got, vec![(true, &b"one"[..]), (false, &b"two"[..])]);
    // Resent with the identifier it was first sent with ([MQTT-4.4.0-1]).
    assert_eq!(
        rig.publishes(1, 3)[0].id,
        rig.publishes(0, 1)[0].id,
        "a resend took another packet identifier"
    );
    let ids: Vec<PacketId> = rig.publishes(1, 3).iter().map(|p| p.id.unwrap()).collect();
    let acks: Vec<Packet> = ids
        .into_iter()
        .map(|id| {
            Packet::PubAck(PubAck {
                id,
                reason: ReasonCode::Success,
                properties: Default::default(),
            })
        })
        .collect();
    rig.send(1, 3, &acks);
    rig.commit();
    assert_eq!(rig.state.inflight(rig.gateways[1].session(3).unwrap()), 0);
}

/// A refused `Connect` is answered at once and leaves nothing to undo:
/// stale is 0x88 (3.1.1: 0x03), the session limit 0x97 (3.1.1: 0x03).
#[test]
fn a_refused_connect_is_answered_with_its_reason_and_proposes_nothing_more() {
    // Stale: a CONNECT proposed before a newer one for its id committed.
    let mut rig = Rig::ready();
    rig.connect(0, 1, v5("c", true, 0));
    rig.connect(1, 2, v5("c", true, 0));
    rig.commit();
    let first = rig.connack(0, 1).unwrap().reason;
    let second = rig.connack(1, 2).unwrap().reason;
    assert_eq!(
        (first, second),
        (ReasonCode::Success, ReasonCode::ServerUnavailable)
    );
    assert!(rig.closed.contains(&(1, 2)));
    assert!(rig.proposed.is_empty());
    assert_eq!(rig.gateways[1].load().counted.connects_stale, 1);

    // The session limit, MQTT 5 and 3.1.1.
    let mut rig = Rig::new(ConnectLimits {
        max_persisted_sessions: 1,
        ..ConnectLimits::default()
    });
    rig.connect(0, 1, v5("a", true, 60));
    rig.commit();
    rig.connect(0, 2, v5("b", true, 60));
    rig.connect(0, 3, v311("c", false));
    rig.commit();
    assert_eq!(rig.connack(0, 2).unwrap().reason, ReasonCode::QuotaExceeded);
    assert_eq!(
        rig.connack(0, 3).unwrap().reason,
        ReasonCode::ServerUnavailable,
        "3.1.1 has 0x03 for it"
    );
    assert_eq!(rig.gateways[0].load().counted.connects_quota, 2);
}

/// D-177 A: a DISCONNECT that sets the expiry to 0 ends a persisted
/// session; one that raises it from 0 is a protocol error.
#[test]
fn an_expiry_of_zero_at_disconnect_ends_the_session() {
    let mut rig = Rig::ready();
    persisted(&mut rig, 0, 1, "c");
    rig.send(
        0,
        1,
        &[Packet::Disconnect(Disconnect {
            reason: ReasonCode::Success,
            properties: DisconnectProperties {
                session_expiry_secs: Some(0),
                ..DisconnectProperties::default()
            },
        })],
    );
    assert!(matches!(
        rig.proposed.as_slice(),
        [Proposal::Disconnect { end: true, .. }]
    ));
    rig.commit();
    assert!(rig.state.client(b"c").is_none());

    rig.connect(0, 2, v5("d", true, 0));
    rig.commit();
    rig.send(
        0,
        2,
        &[Packet::Disconnect(Disconnect {
            reason: ReasonCode::Success,
            properties: DisconnectProperties {
                session_expiry_secs: Some(30),
                ..DisconnectProperties::default()
            },
        })],
    );
    assert!(rig.sent.iter().any(|(g, c, p)| *g == 0
        && *c == 2
        && matches!(p, Packet::Disconnect(d) if d.reason == ReasonCode::ProtocolError)));
}

/// A closed connection proposes `Disconnect` until it is applied; one closed
/// before its `Connect` committed does too, so that `Connect` cannot leave
/// the client id held.
#[test]
fn a_closed_connection_is_detached_even_if_its_connect_commits_late() {
    let mut rig = Rig::ready();
    rig.connect(0, 1, v5("c", true, 60));
    // Closed while attaching: the `Connect` is still in the log's way.
    rig.gateways[0].closed(1);
    rig.drain();
    assert!(matches!(
        rig.proposed.as_slice(),
        [
            Proposal::Connect { .. },
            Proposal::Disconnect { end: false, .. }
        ]
    ));
    rig.commit();
    let view = rig.state.client(b"c").unwrap();
    assert_eq!(view.attached, None, "the late Connect left the id attached");
    assert_eq!(rig.gateways[0].load().retries, 0);
}

/// A connection whose `Connect` never commits is closed at the CONNECT
/// deadline, and its `Disconnect` goes after it.
#[test]
fn an_attaching_connection_times_out_at_the_connect_deadline() {
    let mut rig = Rig::ready();
    rig.connect(0, 1, v5("c", true, 0));
    let connect = rig.proposed.clone();
    rig.proposed.clear();
    rig.gateways[0].tick(Duration::from_secs(11));
    rig.drain();
    assert!(rig.closed.contains(&(0, 1)));
    assert!(matches!(
        rig.proposed.as_slice(),
        [Proposal::Disconnect { .. }]
    ));
    assert_eq!(connect.len(), 1);
}
