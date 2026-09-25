//! What the broker writes down about a session, and what it does with it
//! when the log hands it back.
//!
//! `allow-unwrap-in-tests` only covers code inside `#[test]` functions.
#![allow(clippy::unwrap_used)]

use auth::Permissions;
use broker::{Broker, BrokerLimits, SessionEvent, SessionRecord};
use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee, SessionId, Topic, TopicFilter};
use protocol::{
    Command, Connect, DisconnectReason, DurableToken, Outbound, Outbox, Publish, RequestId,
    Response,
};

use DeliveryGuarantee::{AtLeastOnce, AtMostOnce};

fn broker() -> (Broker, Outbox) {
    (Broker::new(BrokerLimits::default()), Outbox::new())
}

fn client(name: &str) -> ClientId {
    ClientId::new(Bytes::copy_from_slice(name.as_bytes())).unwrap()
}

fn topic(value: &str) -> Topic {
    Topic::new(Bytes::copy_from_slice(value.as_bytes())).unwrap()
}

fn filter(value: &str) -> TopicFilter {
    TopicFilter::new(Bytes::copy_from_slice(value.as_bytes())).unwrap()
}

fn connect(broker: &mut Broker, out: &mut Outbox, session: u64, name: &str, resume: bool) {
    connect_as(
        broker,
        out,
        session,
        name,
        resume,
        Permissions::unrestricted(),
    );
}

fn connect_as(
    broker: &mut Broker,
    out: &mut Outbox,
    session: u64,
    name: &str,
    resume: bool,
    permissions: Permissions,
) {
    broker.handle(
        SessionId(session),
        Command::Connect(Connect {
            client_id: client(name),
            permissions,
            resume,
            will: None,
        }),
        out,
    );
}

fn subscribe(broker: &mut Broker, out: &mut Outbox, session: u64, pattern: &str) {
    broker.handle(
        SessionId(session),
        Command::Subscribe {
            request: RequestId(1),
            filters: vec![(filter(pattern), AtLeastOnce)],
        },
        out,
    );
}

fn disconnect(broker: &mut Broker, out: &mut Outbox, session: u64) {
    broker.handle(
        SessionId(session),
        Command::Disconnect {
            reason: DisconnectReason::Client,
        },
        out,
    );
}

/// A durable publish, the way the runtime hands one over.
fn publish(broker: &mut Broker, out: &mut Outbox, session: u64, name: &str, correlation: u64) {
    broker.handle_durable(
        SessionId(session),
        Command::Publish {
            ack: Some(RequestId(7)),
            publish: Publish {
                topic: topic(name),
                payload: Bytes::from_static(b"payload"),
                guarantee: AtLeastOnce,
                retain: false,
            },
        },
        DurableToken(correlation),
        out,
    );
}

fn events(broker: &mut Broker) -> Vec<SessionEvent> {
    let mut drained = Vec::new();
    broker.drain_session_events(&mut drained);
    drained
}

fn deliveries(out: &mut Outbox) -> Vec<(SessionId, Topic)> {
    out.take()
        .filter_map(|(session, outbound)| match outbound {
            Outbound::Deliver(delivery) => Some((session, delivery.topic)),
            _ => None,
        })
        .collect()
}

#[test]
fn a_clean_session_is_never_written_down() {
    let (mut broker, mut out) = broker();
    connect(&mut broker, &mut out, 1, "device", false);
    subscribe(&mut broker, &mut out, 1, "a/#");
    disconnect(&mut broker, &mut out, 1);

    assert!(
        events(&mut broker).is_empty(),
        "a session nobody asked to keep reached the log"
    );
}

#[test]
fn a_kept_session_is_written_down_with_what_it_subscribed_to() {
    let (mut broker, mut out) = broker();
    connect(&mut broker, &mut out, 1, "device", true);
    subscribe(&mut broker, &mut out, 1, "a/#");
    disconnect(&mut broker, &mut out, 1);

    let written = events(&mut broker);
    let Some(SessionEvent::Kept(record)) = written.last() else {
        panic!("expected the session to be written down, got {written:?}");
    };
    assert_eq!(record.client, client("device"));
    assert_eq!(record.filters, vec![(filter("a/#"), AtLeastOnce)]);
}

#[test]
fn the_mark_is_the_record_before_the_oldest_one_still_owed() {
    let (mut broker, mut out) = broker();
    connect(&mut broker, &mut out, 1, "device", true);
    subscribe(&mut broker, &mut out, 1, "a/b");
    connect(&mut broker, &mut out, 2, "publisher", false);

    publish(&mut broker, &mut out, 2, "a/b", 40);
    publish(&mut broker, &mut out, 2, "a/b", 41);
    disconnect(&mut broker, &mut out, 1);

    let written = events(&mut broker);
    let Some(SessionEvent::Kept(record)) = written.last() else {
        panic!("expected the session to be written down");
    };
    assert_eq!(
        record.owes_from, 39,
        "the oldest record it still owes has to come back"
    );
}

#[test]
fn a_discarded_session_is_written_down_as_gone() {
    let (mut broker, mut out) = broker();
    connect(&mut broker, &mut out, 1, "device", true);
    subscribe(&mut broker, &mut out, 1, "a/#");
    disconnect(&mut broker, &mut out, 1);
    let _ = events(&mut broker);

    // A clean start throws away what was kept.
    connect(&mut broker, &mut out, 2, "device", false);

    assert_eq!(
        events(&mut broker),
        vec![SessionEvent::Gone(client("device"))]
    );
}

#[test]
fn a_restored_session_is_owed_what_came_after_its_mark() {
    let (mut broker, mut out) = broker();
    broker.restore_session(SessionRecord {
        client: client("device"),
        filters: vec![(filter("a/#"), AtLeastOnce)],
        owes_from: 10,
    });

    // Before the mark: already accounted for. After it: still owed.
    let message = Publish {
        topic: topic("a/b"),
        payload: Bytes::from_static(b"payload"),
        guarantee: AtLeastOnce,
        retain: false,
    };
    broker.restore_publish(9, &message, &mut out);
    broker.restore_publish(10, &message, &mut out);
    broker.restore_publish(11, &message, &mut out);
    broker.restore_publish(12, &message, &mut out);

    // Nothing is connected during a restore, so nothing was sent yet.
    assert!(deliveries(&mut out).is_empty());

    connect(&mut broker, &mut out, 1, "device", true);
    let produced: Vec<_> = out.take().collect();
    assert!(
        produced.iter().any(|(_, outbound)| matches!(
            outbound,
            Outbound::Reply(Response::Connected {
                session_present: true
            })
        )),
        "the restored session was not recognised: {produced:?}"
    );

    // Resuming hands over exactly the two records after the mark.
    let sent: Vec<_> = produced
        .iter()
        .filter(|(session, outbound)| {
            *session == SessionId(1) && matches!(outbound, Outbound::Deliver(_))
        })
        .collect();
    assert_eq!(sent.len(), 2, "expected two redeliveries, got {sent:?}");
}

#[test]
fn an_at_most_once_publish_is_never_owed_to_a_restored_session() {
    let (mut broker, mut out) = broker();
    broker.restore_session(SessionRecord {
        client: client("device"),
        filters: vec![(filter("a/#"), AtMostOnce)],
        owes_from: 0,
    });
    broker.restore_publish(
        1,
        &Publish {
            topic: topic("a/b"),
            payload: Bytes::from_static(b"payload"),
            guarantee: AtMostOnce,
            retain: false,
        },
        &mut out,
    );

    connect(&mut broker, &mut out, 1, "device", true);
    assert!(
        deliveries(&mut out).is_empty(),
        "at-most-once was kept for a session that was not there"
    );
}

#[test]
fn the_offline_limit_holds_on_the_way_back_in() {
    let mut broker = Broker::new(BrokerLimits {
        max_offline_sessions: 1,
        ..BrokerLimits::default()
    });
    assert!(broker.restore_session(SessionRecord {
        client: client("first"),
        filters: vec![(filter("a/#"), AtLeastOnce)],
        owes_from: 0,
    }));
    assert!(
        !broker.restore_session(SessionRecord {
            client: client("second"),
            filters: vec![(filter("b/#"), AtLeastOnce)],
            owes_from: 0,
        }),
        "the log was allowed to grow past the limit the live store has"
    );
}

#[test]
fn a_subscription_the_rules_no_longer_allow_does_not_come_back() {
    let (mut broker, mut out) = broker();
    broker.restore_session(SessionRecord {
        client: client("device"),
        filters: vec![(filter("secret/#"), AtLeastOnce)],
        owes_from: 0,
    });

    // The rules changed while this session was gone.
    connect_as(
        &mut broker,
        &mut out,
        1,
        "device",
        true,
        Permissions::from_rules(vec![(filter("public/#"), true, true)]),
    );
    out.take().for_each(drop);

    connect(&mut broker, &mut out, 2, "publisher", false);
    publish(&mut broker, &mut out, 2, "secret/leak", 1);

    assert!(
        deliveries(&mut out).is_empty(),
        "a revoked subscription kept delivering"
    );
}

/// The backlog is a bound, and reaching it loses messages. What must not
/// happen is losing them silently, or counting an offline session out of
/// the active ones it was never in.
#[test]
fn an_offline_session_that_overflows_is_counted_not_closed() {
    let mut broker = Broker::new(BrokerLimits {
        session_backlog_messages: 4,
        ..BrokerLimits::default()
    });
    let mut out = Outbox::new();
    broker.restore_session(SessionRecord {
        client: client("device"),
        filters: vec![(filter("a/#"), AtLeastOnce)],
        owes_from: 0,
    });

    let message = Publish {
        topic: topic("a/b"),
        payload: Bytes::from_static(b"payload"),
        guarantee: AtLeastOnce,
        retain: false,
    };
    for correlation in 1..=10 {
        broker.restore_publish(correlation, &message, &mut out);
    }

    let stats = broker.stats();
    assert_eq!(stats.backlog_refused, 6, "the refused messages were silent");
    assert_eq!(
        stats.active_sessions, 0,
        "an offline session was counted active"
    );
    assert_eq!(stats.offline_sessions, 1, "the session was thrown away");

    // What fits still comes back.
    connect(&mut broker, &mut out, 1, "device", true);
    assert_eq!(deliveries(&mut out).len(), 4);
}

/// A per-session limit times the session count is a product, not a bound
/// (CLAUDE.md §8). The backlogs get a budget of their own, and reaching it
/// must refuse further messages rather than grow.
#[test]
fn the_backlog_budget_bounds_every_session_together() {
    // Room for four payloads across the whole broker, however many sessions
    // ask for them.
    let mut broker = Broker::new(BrokerLimits {
        total_backlog_bytes: 4 * 7,
        session_backlog_messages: 1024,
        session_backlog_bytes: 1024,
        ..BrokerLimits::default()
    });
    let mut out = Outbox::new();
    for name in ["a", "b", "c", "d"] {
        assert!(broker.restore_session(SessionRecord {
            client: client(name),
            filters: vec![(filter("a/#"), AtLeastOnce)],
            owes_from: 0,
        }));
    }

    // Seven bytes each, four sessions: the first publish alone fills the
    // budget, because every session backlogs a copy.
    let message = Publish {
        topic: topic("a/b"),
        payload: Bytes::from_static(b"payload"),
        guarantee: AtLeastOnce,
        retain: false,
    };
    for correlation in 1..=10 {
        broker.restore_publish(correlation, &message, &mut out);
    }

    let stats = broker.stats();
    assert_eq!(
        stats.backlog_bytes, 28,
        "the budget was exceeded: {} bytes held",
        stats.backlog_bytes
    );
    assert!(
        stats.backlog_refused >= 36,
        "further messages were taken anyway: only {} refused",
        stats.backlog_refused
    );

    // And a session that goes stops holding its share.
    connect(&mut broker, &mut out, 1, "a", false);
    assert!(
        broker.stats().backlog_bytes < 28,
        "a discarded session kept its bytes against the budget"
    );
}

fn with_will(broker: &mut Broker, out: &mut Outbox, session: u64, name: &str, topic_name: &str) {
    broker.handle(
        SessionId(session),
        Command::Connect(Connect {
            client_id: client(name),
            permissions: Permissions::unrestricted(),
            resume: false,
            will: Some(protocol::Will {
                topic: topic(topic_name),
                payload: Bytes::from_static(b"gone"),
                guarantee: AtMostOnce,
                retain: false,
            }),
        }),
        out,
    );
}

fn end(broker: &mut Broker, out: &mut Outbox, session: u64, reason: DisconnectReason) {
    broker.handle(SessionId(session), Command::Disconnect { reason }, out);
}

/// 3.1.1 §3.1.2.5: the will is published when the connection ends any way
/// but a DISCONNECT.
#[test]
fn a_lost_connection_publishes_the_will() {
    let (mut broker, mut out) = broker();
    connect(&mut broker, &mut out, 1, "watcher", false);
    subscribe(&mut broker, &mut out, 1, "gone/#");
    out.take().for_each(drop);

    with_will(&mut broker, &mut out, 2, "device", "gone/device");
    end(&mut broker, &mut out, 2, DisconnectReason::ConnectionLost);

    let sent = deliveries(&mut out);
    assert_eq!(sent.len(), 1, "the will was not published: {sent:?}");
    assert_eq!(sent[0], (SessionId(1), topic("gone/device")));
}

/// And it is taken with a clean goodbye, not published.
#[test]
fn a_clean_disconnect_takes_the_will_with_it() {
    let (mut broker, mut out) = broker();
    connect(&mut broker, &mut out, 1, "watcher", false);
    subscribe(&mut broker, &mut out, 1, "gone/#");
    out.take().for_each(drop);

    with_will(&mut broker, &mut out, 2, "device", "gone/device");
    end(&mut broker, &mut out, 2, DisconnectReason::Client);

    assert!(
        deliveries(&mut out).is_empty(),
        "a client that said goodbye still had its will published"
    );
}

/// A protocol violation is not a goodbye either.
#[test]
fn a_protocol_violation_publishes_the_will() {
    let (mut broker, mut out) = broker();
    connect(&mut broker, &mut out, 1, "watcher", false);
    subscribe(&mut broker, &mut out, 1, "gone/#");
    out.take().for_each(drop);

    with_will(&mut broker, &mut out, 2, "device", "gone/device");
    end(
        &mut broker,
        &mut out,
        2,
        DisconnectReason::ProtocolViolation,
    );

    assert_eq!(deliveries(&mut out).len(), 1, "the will was not published");
}

/// It is published once, not once per way the session can end.
#[test]
fn the_will_is_published_at_most_once() {
    let (mut broker, mut out) = broker();
    connect(&mut broker, &mut out, 1, "watcher", false);
    subscribe(&mut broker, &mut out, 1, "gone/#");
    out.take().for_each(drop);

    with_will(&mut broker, &mut out, 2, "device", "gone/device");
    end(&mut broker, &mut out, 2, DisconnectReason::ConnectionLost);
    end(&mut broker, &mut out, 2, DisconnectReason::ConnectionLost);

    assert_eq!(deliveries(&mut out).len(), 1, "the will went out twice");
}
