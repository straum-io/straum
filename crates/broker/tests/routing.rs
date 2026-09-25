//! What the broker does with commands, seen through the outbox.
//!
//! `allow-unwrap-in-tests` only covers code inside `#[test]` functions.
#![allow(clippy::unwrap_used)]

use auth::Permissions;
use broker::{Broker, BrokerLimits};
use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee, DeliveryId, SessionId, Topic, TopicFilter};
use protocol::{
    CloseReason, Command, Connect, Delivery, DisconnectReason, DurableToken, Outbound, Outbox,
    Publish, RequestId, Response, SubscribeResult,
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

fn connect(broker: &mut Broker, out: &mut Outbox, session: u64, name: &str) -> SessionId {
    let session = SessionId(session);
    broker.handle(
        session,
        Command::Connect(Connect {
            client_id: client(name),
            permissions: Permissions::unrestricted(),
            resume: false,
            will: None,
        }),
        out,
    );
    session
}

fn subscribe(
    broker: &mut Broker,
    out: &mut Outbox,
    session: SessionId,
    pattern: &str,
    guarantee: DeliveryGuarantee,
) {
    broker.handle(
        session,
        Command::Subscribe {
            request: RequestId(1),
            filters: vec![(filter(pattern), guarantee)],
        },
        out,
    );
}

fn publish(
    broker: &mut Broker,
    out: &mut Outbox,
    session: SessionId,
    name: &str,
    payload: &'static [u8],
    guarantee: DeliveryGuarantee,
) {
    broker.handle(
        session,
        Command::Publish {
            ack: None,
            publish: Publish {
                topic: topic(name),
                payload: Bytes::from_static(payload),
                guarantee,
                retain: false,
            },
        },
        out,
    );
}

fn drain(out: &mut Outbox) -> Vec<(SessionId, Outbound)> {
    out.take().collect()
}

fn deliveries(out: &mut Outbox) -> Vec<(SessionId, Delivery)> {
    drain(out)
        .into_iter()
        .filter_map(|(session, outbound)| match outbound {
            Outbound::Deliver(delivery) => Some((session, delivery)),
            _ => None,
        })
        .collect()
}

#[test]
fn a_connection_is_answered_and_never_resumed() {
    let (mut broker, mut out) = broker();
    let session = connect(&mut broker, &mut out, 1, "a");

    assert_eq!(
        drain(&mut out),
        vec![(
            session,
            Outbound::Reply(Response::Connected {
                session_present: false
            })
        )]
    );
    assert_eq!(broker.stats().active_sessions, 1);
}

#[test]
fn one_identity_means_one_connection() {
    let (mut broker, mut out) = broker();
    let first = connect(&mut broker, &mut out, 1, "same");
    subscribe(&mut broker, &mut out, first, "a/b", AtMostOnce);
    let _ = drain(&mut out);

    let second = connect(&mut broker, &mut out, 2, "same");
    let effects = drain(&mut out);

    assert!(effects.contains(&(first, Outbound::Close(CloseReason::SessionTakenOver))));
    assert!(effects.contains(&(
        second,
        Outbound::Reply(Response::Connected {
            session_present: false
        })
    )));

    // The old session's subscriptions went with it.
    publish(&mut broker, &mut out, second, "a/b", b"x", AtMostOnce);
    assert!(deliveries(&mut out).is_empty());
    assert_eq!(broker.stats().active_sessions, 1);
}

#[test]
fn clients_without_an_identity_do_not_collide() {
    let (mut broker, mut out) = broker();
    for session in 1..=3 {
        broker.handle(
            SessionId(session),
            Command::Connect(Connect {
                client_id: ClientId::new(Bytes::new()).unwrap(),
                permissions: Permissions::unrestricted(),
                resume: false,
                will: None,
            }),
            &mut out,
        );
    }
    let _ = drain(&mut out);

    // Three assigned identities, so nobody took anybody over.
    assert_eq!(broker.stats().active_sessions, 3);
}

#[test]
fn a_subscriber_receives_what_it_asked_for() {
    let (mut broker, mut out) = broker();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(
        &mut broker,
        &mut out,
        subscriber,
        "sensors/+/temp",
        AtMostOnce,
    );

    assert_eq!(
        drain(&mut out).last().unwrap().1,
        Outbound::Reply(Response::Subscribed {
            request: RequestId(1),
            granted: vec![SubscribeResult::Granted(AtMostOnce)],
        })
    );

    let publisher = connect(&mut broker, &mut out, 2, "pub");
    let _ = drain(&mut out);
    publish(
        &mut broker,
        &mut out,
        publisher,
        "sensors/eu/temp",
        b"21",
        AtMostOnce,
    );

    let delivered = deliveries(&mut out);
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].0, subscriber);
    assert_eq!(delivered[0].1.topic.as_str(), "sensors/eu/temp");
    assert_eq!(delivered[0].1.payload, Bytes::from_static(b"21"));
    assert_eq!(delivered[0].1.id, None);
}

#[test]
fn a_topic_nobody_subscribed_to_goes_nowhere() {
    let (mut broker, mut out) = broker();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(&mut broker, &mut out, subscriber, "a/b", AtMostOnce);
    let _ = drain(&mut out);

    publish(&mut broker, &mut out, subscriber, "a/c", b"x", AtMostOnce);
    assert!(deliveries(&mut out).is_empty());
    assert_eq!(broker.stats().published_messages, 1);
    assert_eq!(broker.stats().delivered_messages, 0);
}

#[test]
fn reserved_topics_are_invisible_to_leading_wildcards() {
    let (mut broker, mut out) = broker();
    let wild = connect(&mut broker, &mut out, 1, "wild");
    subscribe(&mut broker, &mut out, wild, "#", AtMostOnce);
    let explicit = connect(&mut broker, &mut out, 2, "explicit");
    subscribe(&mut broker, &mut out, explicit, "$SYS/#", AtMostOnce);
    let _ = drain(&mut out);

    publish(&mut broker, &mut out, wild, "$SYS/uptime", b"9", AtMostOnce);
    let delivered = deliveries(&mut out);

    assert_eq!(delivered.len(), 1, "a wildcard reached a $-topic");
    assert_eq!(delivered[0].0, explicit);
}

#[test]
fn overlapping_subscriptions_deliver_once_at_the_strongest_grant() {
    let (mut broker, mut out) = broker();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(&mut broker, &mut out, subscriber, "a/b", AtMostOnce);
    subscribe(&mut broker, &mut out, subscriber, "a/+", AtLeastOnce);
    subscribe(&mut broker, &mut out, subscriber, "#", AtMostOnce);
    let _ = drain(&mut out);

    publish(&mut broker, &mut out, subscriber, "a/b", b"x", AtLeastOnce);
    let delivered = deliveries(&mut out);

    assert_eq!(
        delivered.len(),
        1,
        "three filters produced three deliveries"
    );
    assert!(delivered[0].1.id.is_some(), "the strongest grant lost");
}

#[test]
fn the_delivered_guarantee_is_the_weaker_of_the_two() {
    let (mut broker, mut out) = broker();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(&mut broker, &mut out, subscriber, "a/b", AtMostOnce);
    let _ = drain(&mut out);

    // Published at-least-once, granted at-most-once.
    publish(&mut broker, &mut out, subscriber, "a/b", b"x", AtLeastOnce);
    assert_eq!(deliveries(&mut out)[0].1.id, None);
}

#[test]
fn one_payload_is_shared_by_every_subscriber() {
    let (mut broker, mut out) = broker();
    for index in 0..16 {
        let session = connect(&mut broker, &mut out, index, &format!("sub-{index}"));
        subscribe(&mut broker, &mut out, session, "a/b", AtMostOnce);
    }
    let _ = drain(&mut out);

    publish(
        &mut broker,
        &mut out,
        SessionId(0),
        "a/b",
        b"payload",
        AtMostOnce,
    );
    let delivered = deliveries(&mut out);

    assert_eq!(delivered.len(), 16);
    let first = delivered[0].1.payload.as_ptr();
    for (_, delivery) in &delivered {
        assert_eq!(
            delivery.payload.as_ptr(),
            first,
            "fan-out copied the payload"
        );
    }
}

#[test]
fn unsubscribing_stops_the_deliveries() {
    let (mut broker, mut out) = broker();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(&mut broker, &mut out, subscriber, "a/#", AtMostOnce);
    let _ = drain(&mut out);

    broker.handle(
        subscriber,
        Command::Unsubscribe {
            request: RequestId(5),
            filters: vec![filter("a/#")],
        },
        &mut out,
    );
    assert_eq!(
        drain(&mut out).last().unwrap().1,
        Outbound::Reply(Response::Unsubscribed(RequestId(5)))
    );

    publish(&mut broker, &mut out, subscriber, "a/b", b"x", AtMostOnce);
    assert!(deliveries(&mut out).is_empty());
}

#[test]
fn a_disconnect_takes_the_whole_session_with_it() {
    let (mut broker, mut out) = broker();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(&mut broker, &mut out, subscriber, "a/b", AtLeastOnce);
    let publisher = connect(&mut broker, &mut out, 2, "pub");
    publish(&mut broker, &mut out, publisher, "a/b", b"x", AtLeastOnce);
    let _ = drain(&mut out);

    broker.handle(
        subscriber,
        Command::Disconnect {
            reason: DisconnectReason::ConnectionLost,
        },
        &mut out,
    );
    // Nothing is sent to a socket that is already gone.
    assert!(drain(&mut out).is_empty());

    publish(&mut broker, &mut out, publisher, "a/b", b"y", AtMostOnce);
    assert!(deliveries(&mut out).is_empty());
    assert_eq!(broker.stats().active_sessions, 1);

    // The identity is free again.
    let again = connect(&mut broker, &mut out, 3, "sub");
    assert!(
        !drain(&mut out)
            .iter()
            .any(|(_, effect)| matches!(effect, Outbound::Close(_)))
    );
    assert_eq!(again, SessionId(3));
}

#[test]
fn at_least_once_deliveries_wait_for_their_acknowledgement() {
    let limits = BrokerLimits {
        max_inflight: 2,
        ..BrokerLimits::default()
    };
    let mut broker = Broker::new(limits);
    let mut out = Outbox::new();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(&mut broker, &mut out, subscriber, "a/b", AtLeastOnce);
    let _ = drain(&mut out);

    for _ in 0..5 {
        publish(&mut broker, &mut out, subscriber, "a/b", b"x", AtLeastOnce);
    }
    let delivered = deliveries(&mut out);

    // Two in the window, three waiting in the backlog.
    assert_eq!(delivered.len(), 2);
    let first = delivered[0].1.id.unwrap();
    let second = delivered[1].1.id.unwrap();
    assert_ne!(first, second);

    broker.handle(subscriber, Command::Ack(first), &mut out);
    let released = deliveries(&mut out);
    assert_eq!(released.len(), 1, "the window did not refill");

    broker.handle(subscriber, Command::Ack(second), &mut out);
    assert_eq!(deliveries(&mut out).len(), 1);
}

#[test]
fn delivery_identifiers_are_never_reused() {
    let (mut broker, mut out) = broker();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(&mut broker, &mut out, subscriber, "a/b", AtLeastOnce);
    let _ = drain(&mut out);

    let mut seen: Vec<DeliveryId> = Vec::new();
    for _ in 0..20 {
        publish(&mut broker, &mut out, subscriber, "a/b", b"x", AtLeastOnce);
        for (_, delivery) in deliveries(&mut out) {
            let id = delivery.id.unwrap();
            assert!(!seen.contains(&id), "{id:?} came back");
            seen.push(id);
            broker.handle(subscriber, Command::Ack(id), &mut out);
            let _ = drain(&mut out);
        }
    }
    assert_eq!(seen.len(), 20);
}

#[test]
fn a_session_too_far_behind_is_closed() {
    let limits = BrokerLimits {
        max_inflight: 1,
        session_backlog_messages: 2,
        ..BrokerLimits::default()
    };
    let mut broker = Broker::new(limits);
    let mut out = Outbox::new();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(&mut broker, &mut out, subscriber, "a/b", AtLeastOnce);
    let _ = drain(&mut out);

    // One inflight, two in the backlog, and then one too many.
    for _ in 0..4 {
        publish(&mut broker, &mut out, subscriber, "a/b", b"x", AtLeastOnce);
    }
    let effects = drain(&mut out);

    assert!(effects.contains(&(subscriber, Outbound::Close(CloseReason::SlowConsumer))));
    assert_eq!(broker.stats().slow_consumer_closes, 1);
    assert_eq!(broker.stats().active_sessions, 0);
}

#[test]
fn the_subscription_limit_refuses_instead_of_growing() {
    let limits = BrokerLimits {
        max_subscriptions_per_session: 2,
        ..BrokerLimits::default()
    };
    let mut broker = Broker::new(limits);
    let mut out = Outbox::new();
    let session = connect(&mut broker, &mut out, 1, "sub");
    let _ = drain(&mut out);

    broker.handle(
        session,
        Command::Subscribe {
            request: RequestId(1),
            filters: vec![
                (filter("a"), AtMostOnce),
                (filter("b"), AtMostOnce),
                (filter("c"), AtMostOnce),
            ],
        },
        &mut out,
    );

    let Outbound::Reply(Response::Subscribed { granted, .. }) = drain(&mut out).remove(0).1 else {
        panic!("expected a SUBACK");
    };
    assert_eq!(
        granted,
        vec![
            SubscribeResult::Granted(AtMostOnce),
            SubscribeResult::Granted(AtMostOnce),
            SubscribeResult::Failure,
        ]
    );
}

#[test]
fn overflow_drops_what_may_be_dropped_and_closes_what_may_not() {
    let (mut broker, mut out) = broker();
    let session = connect(&mut broker, &mut out, 1, "sub");
    let _ = drain(&mut out);

    let at_most_once = Outbound::Deliver(Delivery {
        id: None,
        topic: topic("a/b"),
        payload: Bytes::from_static(b"x"),
        duplicate: false,
        retain: false,
    });
    broker.on_overflow(session, at_most_once, &mut out);
    assert!(
        drain(&mut out).is_empty(),
        "a droppable message closed a session"
    );
    assert_eq!(broker.stats().dropped_messages, 1);
    assert_eq!(broker.stats().active_sessions, 1);

    let at_least_once = Outbound::Deliver(Delivery {
        id: Some(DeliveryId(1)),
        topic: topic("a/b"),
        payload: Bytes::from_static(b"x"),
        duplicate: false,
        retain: false,
    });
    broker.on_overflow(session, at_least_once, &mut out);
    assert_eq!(
        drain(&mut out),
        vec![(session, Outbound::Close(CloseReason::SlowConsumer))]
    );
    assert_eq!(broker.stats().active_sessions, 0);
}

#[test]
fn a_reply_that_cannot_be_enqueued_closes_the_session() {
    let (mut broker, mut out) = broker();
    let session = connect(&mut broker, &mut out, 1, "sub");
    let _ = drain(&mut out);

    broker.on_overflow(
        session,
        Outbound::Reply(Response::Unsubscribed(RequestId(1))),
        &mut out,
    );
    assert_eq!(
        drain(&mut out),
        vec![(session, Outbound::Close(CloseReason::SlowConsumer))]
    );
}

#[test]
fn a_durable_publish_is_acknowledged_only_after_the_wal_says_so() {
    let (mut broker, mut out) = broker();
    let publisher = connect(&mut broker, &mut out, 1, "pub");
    let _ = drain(&mut out);

    for token in 1..=3u64 {
        broker.handle_durable(
            publisher,
            Command::Publish {
                ack: Some(RequestId(token as u32)),
                publish: Publish {
                    topic: topic("a/b"),
                    payload: Bytes::from_static(b"x"),
                    retain: false,
                    guarantee: AtLeastOnce,
                },
            },
            DurableToken(token),
            &mut out,
        );
    }
    // Routing is done, but nothing is durable yet.
    assert!(drain(&mut out).is_empty());

    broker.on_durable(DurableToken(2), &mut out);
    assert_eq!(
        drain(&mut out),
        vec![
            (
                publisher,
                Outbound::Reply(Response::PublishAccepted(RequestId(1)))
            ),
            (
                publisher,
                Outbound::Reply(Response::PublishAccepted(RequestId(2)))
            ),
        ]
    );

    broker.on_durable(DurableToken(3), &mut out);
    assert_eq!(
        drain(&mut out),
        vec![(
            publisher,
            Outbound::Reply(Response::PublishAccepted(RequestId(3)))
        )]
    );
}

#[test]
fn a_durable_acknowledgement_for_a_session_that_left_goes_nowhere() {
    let (mut broker, mut out) = broker();
    let publisher = connect(&mut broker, &mut out, 1, "pub");
    broker.handle_durable(
        publisher,
        Command::Publish {
            ack: Some(RequestId(1)),
            publish: Publish {
                topic: topic("a/b"),
                payload: Bytes::from_static(b"x"),
                retain: false,
                guarantee: AtLeastOnce,
            },
        },
        DurableToken(1),
        &mut out,
    );
    broker.handle(
        publisher,
        Command::Disconnect {
            reason: DisconnectReason::ConnectionLost,
        },
        &mut out,
    );
    let _ = drain(&mut out);

    broker.on_durable(DurableToken(1), &mut out);
    assert!(drain(&mut out).is_empty());
}

#[test]
fn a_publisher_keeps_its_order() {
    let (mut broker, mut out) = broker();
    let subscriber = connect(&mut broker, &mut out, 1, "sub");
    subscribe(&mut broker, &mut out, subscriber, "a/b", AtMostOnce);
    let publisher = connect(&mut broker, &mut out, 2, "pub");
    let _ = drain(&mut out);

    for index in 0..50u8 {
        broker.handle(
            publisher,
            Command::Publish {
                ack: None,
                publish: Publish {
                    topic: topic("a/b"),
                    payload: Bytes::from(vec![index]),
                    guarantee: AtMostOnce,
                    retain: false,
                },
            },
            &mut out,
        );
    }

    let order: Vec<u8> = deliveries(&mut out)
        .into_iter()
        .map(|(_, delivery)| delivery.payload[0])
        .collect();
    assert_eq!(order, (0..50).collect::<Vec<u8>>());
}

/// The index against a naive scan over every filter (docs/testing.md).
mod properties {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn level() -> impl Strategy<Value = String> {
        prop_oneof!["a", "b", "\\$SYS", "c"].prop_map(String::from)
    }

    fn pattern_level() -> impl Strategy<Value = String> {
        prop_oneof!["a", "b", "\\$SYS", "c", "\\+", "#"].prop_map(String::from)
    }

    fn patterns() -> impl Strategy<Value = Vec<String>> {
        prop::collection::vec(
            prop::collection::vec(pattern_level(), 1..4).prop_map(|levels| levels.join("/")),
            1..4,
        )
    }

    /// Who should receive this publish, and with which guarantee.
    fn reference(
        subscriptions: &[(SessionId, Vec<(TopicFilter, DeliveryGuarantee)>)],
        topic: &Topic,
        published: DeliveryGuarantee,
    ) -> BTreeMap<u64, DeliveryGuarantee> {
        let mut expected = BTreeMap::new();
        for (session, filters) in subscriptions {
            let mut best: Option<DeliveryGuarantee> = None;
            for (filter, guarantee) in filters {
                if filter.matches(topic) {
                    best = Some(best.map_or(*guarantee, |held| held.max(*guarantee)));
                }
            }
            if let Some(best) = best {
                expected.insert(session.0, published.min(best));
            }
        }
        expected
    }

    proptest! {
        #[test]
        fn the_index_agrees_with_a_scan_over_every_filter(
            sessions in prop::collection::vec(patterns(), 1..5),
            levels in prop::collection::vec(level(), 1..4),
            published in prop_oneof![Just(AtMostOnce), Just(AtLeastOnce)],
        ) {
            let Ok(published_topic) = Topic::new(Bytes::from(levels.join("/"))) else {
                return Ok(());
            };

            let (mut broker, mut out) = broker();
            let mut subscriptions = Vec::new();

            for (index, wanted) in sessions.iter().enumerate() {
                let session = connect(&mut broker, &mut out, index as u64, &format!("s{index}"));
                let mut held = Vec::new();
                for (position, text) in wanted.iter().enumerate() {
                    let Ok(parsed) = TopicFilter::new(Bytes::from(text.clone())) else {
                        continue;
                    };
                    // Alternate the grant so the strongest-wins rule is exercised.
                    let guarantee = if position % 2 == 0 { AtMostOnce } else { AtLeastOnce };
                    broker.handle(
                        session,
                        Command::Subscribe {
                            request: RequestId(1),
                            filters: vec![(parsed.clone(), guarantee)],
                        },
                        &mut out,
                    );
                    held.push((parsed, guarantee));
                }
                subscriptions.push((session, held));
            }
            let _ = drain(&mut out);

            // The publisher has to be a session like any other: an
            // unknown session is ignored, as it is everywhere else.
            let publisher = connect(&mut broker, &mut out, 900, "publisher");
            let _ = drain(&mut out);
            broker.handle(
                publisher,
                Command::Publish {
                    ack: None,
                    publish: Publish {
                        topic: published_topic.clone(),
                        payload: Bytes::from_static(b"x"),
                        guarantee: published,
                        retain: false,
                    },
                },
                &mut out,
            );

            let mut actual = BTreeMap::new();
            for (session, delivery) in deliveries(&mut out) {
                let guarantee = match delivery.id {
                    Some(_) => AtLeastOnce,
                    None => AtMostOnce,
                };
                // At most one delivery per session per publish.
                prop_assert!(
                    actual.insert(session.0, guarantee).is_none(),
                    "{session:?} was delivered to twice"
                );
            }

            prop_assert_eq!(actual, reference(&subscriptions, &published_topic, published));
        }
    }
}

#[test]
fn a_record_that_was_durable_before_the_command_arrived_is_acknowledged_at_once() {
    let (mut broker, mut out) = broker();
    let publisher = connect(&mut broker, &mut out, 1, "pub");
    let _ = drain(&mut out);

    // The WAL committed first: its token is already known.
    broker.on_durable(DurableToken(7), &mut out);
    assert!(drain(&mut out).is_empty());

    broker.handle_durable(
        publisher,
        Command::Publish {
            ack: Some(RequestId(1)),
            publish: Publish {
                topic: topic("a/b"),
                payload: Bytes::from_static(b"x"),
                retain: false,
                guarantee: AtLeastOnce,
            },
        },
        DurableToken(5),
        &mut out,
    );

    assert_eq!(
        drain(&mut out),
        vec![(
            publisher,
            Outbound::Reply(Response::PublishAccepted(RequestId(1)))
        )],
        "an already durable publish waited for a token that will never come"
    );
}

/// Access rules, enforced where SUBACK can express them.
mod authorisation {
    use super::*;

    fn restricted(publish: &str, subscribe: &str) -> Permissions {
        Permissions::from_rules(vec![
            (filter(publish), true, false),
            (filter(subscribe), false, true),
        ])
    }

    fn connect_as(
        broker: &mut Broker,
        out: &mut Outbox,
        session: u64,
        name: &str,
        permissions: Permissions,
    ) -> SessionId {
        let session = SessionId(session);
        broker.handle(
            session,
            Command::Connect(Connect {
                client_id: client(name),
                permissions,
                resume: false,
                will: None,
            }),
            out,
        );
        session
    }

    #[test]
    fn a_publish_outside_the_rules_is_dropped_but_still_acknowledged() {
        let (mut broker, mut out) = broker();
        let subscriber = connect(&mut broker, &mut out, 1, "sub");
        subscribe(&mut broker, &mut out, subscriber, "#", AtMostOnce);
        let publisher = connect_as(
            &mut broker,
            &mut out,
            2,
            "pub",
            restricted("sensors/+/temp", "nothing/#"),
        );
        let _ = drain(&mut out);

        broker.handle(
            publisher,
            Command::Publish {
                ack: Some(RequestId(9)),
                publish: Publish {
                    topic: topic("secrets/payroll"),
                    payload: Bytes::from_static(b"x"),
                    retain: false,
                    guarantee: AtLeastOnce,
                },
            },
            &mut out,
        );

        let effects = drain(&mut out);
        // Nobody received it...
        assert!(
            !effects
                .iter()
                .any(|(_, effect)| matches!(effect, Outbound::Deliver(_))),
            "an unauthorised publish was delivered"
        );
        // ...and the publisher is not left waiting for an acknowledgement
        // that 3.1.1 has no way to refuse.
        assert!(effects.contains(&(
            publisher,
            Outbound::Reply(Response::PublishAccepted(RequestId(9)))
        )));
        assert_eq!(broker.stats().unauthorized_publishes, 1);

        // What the rules do allow still goes through.
        broker.handle(
            publisher,
            Command::Publish {
                ack: None,
                publish: Publish {
                    topic: topic("sensors/eu/temp"),
                    payload: Bytes::from_static(b"21"),
                    guarantee: AtMostOnce,
                    retain: false,
                },
            },
            &mut out,
        );
        assert_eq!(deliveries(&mut out).len(), 1);
    }

    #[test]
    fn a_subscription_outside_the_rules_is_refused_per_filter() {
        let (mut broker, mut out) = broker();
        let session = connect_as(
            &mut broker,
            &mut out,
            1,
            "sub",
            restricted("nothing/#", "sensors/#"),
        );
        let _ = drain(&mut out);

        broker.handle(
            session,
            Command::Subscribe {
                request: RequestId(3),
                filters: vec![
                    (filter("sensors/eu/temp"), AtMostOnce),
                    (filter("secrets/#"), AtMostOnce),
                    (filter("#"), AtMostOnce),
                ],
            },
            &mut out,
        );

        let Outbound::Reply(Response::Subscribed { granted, .. }) = drain(&mut out).remove(0).1
        else {
            panic!("expected a SUBACK");
        };
        assert_eq!(
            granted,
            vec![
                SubscribeResult::Granted(AtMostOnce),
                SubscribeResult::Failure,
                // `#` would deliver everything, including what no rule covers.
                SubscribeResult::Failure,
            ]
        );
        assert_eq!(broker.stats().unauthorized_subscribes, 2);

        // The refused filters registered nothing: a publish to them arrives
        // nowhere.
        let publisher = connect(&mut broker, &mut out, 2, "pub");
        let _ = drain(&mut out);
        publish(
            &mut broker,
            &mut out,
            publisher,
            "secrets/payroll",
            b"x",
            AtMostOnce,
        );
        assert!(deliveries(&mut out).is_empty());
    }
}

/// The last message on a topic, kept for whoever subscribes next.
mod retained {
    use super::*;

    fn retain(
        broker: &mut Broker,
        out: &mut Outbox,
        session: SessionId,
        name: &str,
        payload: &'static [u8],
        guarantee: DeliveryGuarantee,
    ) {
        broker.handle(
            session,
            Command::Publish {
                ack: None,
                publish: Publish {
                    topic: topic(name),
                    payload: Bytes::from_static(payload),
                    guarantee,
                    retain: true,
                },
            },
            out,
        );
    }

    #[test]
    fn a_new_subscriber_hears_what_was_stored() {
        let (mut broker, mut out) = broker();
        let publisher = connect(&mut broker, &mut out, 1, "pub");
        retain(
            &mut broker,
            &mut out,
            publisher,
            "sensors/eu/temp",
            b"21",
            AtMostOnce,
        );
        let _ = drain(&mut out);
        assert_eq!(broker.stats().retained_messages, 1);

        let subscriber = connect(&mut broker, &mut out, 2, "sub");
        let _ = drain(&mut out);
        subscribe(&mut broker, &mut out, subscriber, "sensors/#", AtMostOnce);

        let delivered = deliveries(&mut out);
        assert_eq!(delivered.len(), 1, "the stored message was not replayed");
        assert_eq!(delivered[0].0, subscriber);
        assert_eq!(delivered[0].1.payload, Bytes::from_static(b"21"));
        // The client has to be able to tell stored state from live traffic.
        assert!(delivered[0].1.retain, "the replay was not marked retained");
    }

    #[test]
    fn the_replay_order_is_the_same_in_two_brokers_that_stored_the_same_thing() {
        // Same messages, opposite insertion order. A follower reconstructing
        // this state from the log must hand it to a subscriber in the same
        // order the leader did, and "the order the hash map felt like" is not
        // a property either of them can agree on (docs/replication.md §108).
        let names = [
            "sensors/eu/temp",
            "sensors/us/temp",
            "sensors/eu/humidity",
            "sensors/ap/temp",
            "sensors/us/humidity",
            "sensors/ap/humidity",
        ];

        let replayed = |reverse: bool| {
            let (mut broker, mut out) = broker();
            let publisher = connect(&mut broker, &mut out, 1, "pub");
            let mut order: Vec<&str> = names.to_vec();
            if reverse {
                order.reverse();
            }
            for name in order {
                retain(&mut broker, &mut out, publisher, name, b"v", AtMostOnce);
            }
            let _ = drain(&mut out);

            let subscriber = connect(&mut broker, &mut out, 2, "sub");
            let _ = drain(&mut out);
            subscribe(&mut broker, &mut out, subscriber, "sensors/#", AtMostOnce);
            deliveries(&mut out)
                .into_iter()
                .map(|(_, delivery)| delivery.topic)
                .collect::<Vec<_>>()
        };

        let forwards = replayed(false);
        let backwards = replayed(true);

        assert_eq!(forwards.len(), names.len());
        assert_eq!(forwards, backwards);

        // And the order is the one the state implies, not the one the first
        // publisher happened to use.
        let mut expected: Vec<&str> = names.to_vec();
        expected.sort_unstable();
        let actual: Vec<String> = forwards
            .iter()
            .map(|held| String::from_utf8_lossy(held.as_bytes()).into_owned())
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn live_traffic_is_not_marked_retained() {
        let (mut broker, mut out) = broker();
        let subscriber = connect(&mut broker, &mut out, 1, "sub");
        subscribe(&mut broker, &mut out, subscriber, "a/b", AtMostOnce);
        let _ = drain(&mut out);

        // Published with the flag, but this subscriber is already there.
        retain(
            &mut broker,
            &mut out,
            subscriber,
            "a/b",
            b"live",
            AtMostOnce,
        );
        let delivered = deliveries(&mut out);

        assert_eq!(delivered.len(), 1);
        assert!(!delivered[0].1.retain);
    }

    #[test]
    fn the_last_one_wins_and_an_empty_payload_forgets_it() {
        let (mut broker, mut out) = broker();
        let publisher = connect(&mut broker, &mut out, 1, "pub");
        retain(
            &mut broker,
            &mut out,
            publisher,
            "a/b",
            b"first",
            AtMostOnce,
        );
        retain(
            &mut broker,
            &mut out,
            publisher,
            "a/b",
            b"second",
            AtMostOnce,
        );
        let _ = drain(&mut out);
        assert_eq!(broker.stats().retained_messages, 1);

        let subscriber = connect(&mut broker, &mut out, 2, "sub");
        subscribe(&mut broker, &mut out, subscriber, "a/b", AtMostOnce);
        let delivered = deliveries(&mut out);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].1.payload, Bytes::from_static(b"second"));

        // 3.1.1 §3.3.1.3: retained with an empty payload deletes the message.
        retain(&mut broker, &mut out, publisher, "a/b", b"", AtMostOnce);
        let _ = drain(&mut out);
        assert_eq!(broker.stats().retained_messages, 0);

        let late = connect(&mut broker, &mut out, 3, "late");
        subscribe(&mut broker, &mut out, late, "a/b", AtMostOnce);
        assert!(
            deliveries(&mut out).is_empty(),
            "a deleted message came back"
        );
    }

    #[test]
    fn the_stored_guarantee_is_capped_by_the_grant() {
        let (mut broker, mut out) = broker();
        let publisher = connect(&mut broker, &mut out, 1, "pub");
        retain(&mut broker, &mut out, publisher, "a/b", b"x", AtLeastOnce);
        let _ = drain(&mut out);

        let weak = connect(&mut broker, &mut out, 2, "weak");
        subscribe(&mut broker, &mut out, weak, "a/b", AtMostOnce);
        assert_eq!(deliveries(&mut out)[0].1.id, None);

        let strong = connect(&mut broker, &mut out, 3, "strong");
        subscribe(&mut broker, &mut out, strong, "a/b", AtLeastOnce);
        assert!(deliveries(&mut out)[0].1.id.is_some());
    }

    #[test]
    fn the_store_refuses_to_grow_without_a_bound() {
        let limits = BrokerLimits {
            max_retained_messages: 2,
            ..BrokerLimits::default()
        };
        let mut broker = Broker::new(limits);
        let mut out = Outbox::new();
        let publisher = connect(&mut broker, &mut out, 1, "pub");

        for index in 0..5 {
            broker.handle(
                publisher,
                Command::Publish {
                    ack: None,
                    publish: Publish {
                        topic: topic(&format!("topic/{index}")),
                        payload: Bytes::from_static(b"x"),
                        guarantee: AtMostOnce,
                        retain: true,
                    },
                },
                &mut out,
            );
        }
        let _ = drain(&mut out);

        assert_eq!(broker.stats().retained_messages, 2);
        assert_eq!(broker.stats().retained_rejected, 3);

        // Replacing one that is already stored still works: the store is not
        // frozen, it is bounded.
        retain(
            &mut broker,
            &mut out,
            publisher,
            "topic/0",
            b"fresh",
            AtMostOnce,
        );
        let _ = drain(&mut out);
        assert_eq!(broker.stats().retained_messages, 2);
        assert_eq!(broker.stats().retained_rejected, 3);
    }

    #[test]
    fn a_byte_budget_bounds_it_too() {
        let limits = BrokerLimits {
            max_retained_bytes: 8,
            ..BrokerLimits::default()
        };
        let mut broker = Broker::new(limits);
        let mut out = Outbox::new();
        let publisher = connect(&mut broker, &mut out, 1, "pub");

        retain(
            &mut broker,
            &mut out,
            publisher,
            "a",
            b"12345678",
            AtMostOnce,
        );
        retain(&mut broker, &mut out, publisher, "b", b"9", AtMostOnce);
        let _ = drain(&mut out);

        assert_eq!(broker.stats().retained_messages, 1);
        assert_eq!(broker.stats().retained_rejected, 1);
    }

    #[test]
    fn a_publish_the_rules_refuse_is_not_stored_either() {
        let (mut broker, mut out) = broker();
        let publisher = SessionId(1);
        broker.handle(
            publisher,
            Command::Connect(Connect {
                client_id: client("pub"),
                permissions: Permissions::from_rules(vec![(filter("allowed/#"), true, false)]),
                resume: false,
                will: None,
            }),
            &mut out,
        );
        let _ = drain(&mut out);

        retain(
            &mut broker,
            &mut out,
            publisher,
            "secrets/key",
            b"x",
            AtMostOnce,
        );
        let _ = drain(&mut out);

        assert_eq!(broker.stats().retained_messages, 0);
        assert_eq!(broker.stats().unauthorized_publishes, 1);
    }
}

/// Sessions that outlive their connection.
mod persistent {
    use super::*;

    fn connect_resuming(
        broker: &mut Broker,
        out: &mut Outbox,
        session: u64,
        name: &str,
        resume: bool,
    ) -> SessionId {
        let session = SessionId(session);
        broker.handle(
            session,
            Command::Connect(Connect {
                client_id: client(name),
                permissions: Permissions::unrestricted(),
                resume,
                will: None,
            }),
            out,
        );
        session
    }

    fn disconnect(broker: &mut Broker, out: &mut Outbox, session: SessionId) {
        broker.handle(
            session,
            Command::Disconnect {
                reason: DisconnectReason::ConnectionLost,
            },
            out,
        );
        let _ = drain(out);
    }

    #[test]
    fn a_session_that_asked_to_be_kept_collects_what_it_missed() {
        let (mut broker, mut out) = broker();
        let first = connect_resuming(&mut broker, &mut out, 1, "device", true);
        subscribe(&mut broker, &mut out, first, "sensors/#", AtLeastOnce);
        let _ = drain(&mut out);

        disconnect(&mut broker, &mut out, first);
        assert_eq!(broker.stats().offline_sessions, 1);
        assert_eq!(broker.stats().active_sessions, 0);

        // Published while nobody is listening on that socket.
        let publisher = connect(&mut broker, &mut out, 2, "pub");
        let _ = drain(&mut out);
        publish(
            &mut broker,
            &mut out,
            publisher,
            "sensors/eu/temp",
            b"21",
            AtLeastOnce,
        );
        assert!(
            deliveries(&mut out).is_empty(),
            "delivered to a closed socket"
        );

        // The same identity comes back and asks for its session.
        let second = connect_resuming(&mut broker, &mut out, 3, "device", true);
        let effects = drain(&mut out);

        assert!(effects.contains(&(
            second,
            Outbound::Reply(Response::Connected {
                session_present: true
            })
        )));
        let delivered: Vec<_> = effects
            .into_iter()
            .filter_map(|(session, effect)| match effect {
                Outbound::Deliver(delivery) => Some((session, delivery)),
                _ => None,
            })
            .collect();
        assert_eq!(delivered.len(), 1, "what was queued did not arrive");
        assert_eq!(delivered[0].0, second);
        assert_eq!(delivered[0].1.payload, Bytes::from_static(b"21"));
        assert_eq!(broker.stats().offline_sessions, 0);
    }

    #[test]
    fn a_clean_start_throws_away_what_was_kept() {
        let (mut broker, mut out) = broker();
        let first = connect_resuming(&mut broker, &mut out, 1, "device", true);
        subscribe(&mut broker, &mut out, first, "sensors/#", AtLeastOnce);
        let _ = drain(&mut out);
        disconnect(&mut broker, &mut out, first);

        let publisher = connect(&mut broker, &mut out, 2, "pub");
        publish(
            &mut broker,
            &mut out,
            publisher,
            "sensors/eu/temp",
            b"21",
            AtLeastOnce,
        );
        let _ = drain(&mut out);

        // Reconnecting clean: nothing carried over, and no subscription either.
        let second = connect_resuming(&mut broker, &mut out, 3, "device", false);
        let effects = drain(&mut out);
        assert!(effects.contains(&(
            second,
            Outbound::Reply(Response::Connected {
                session_present: false
            })
        )));
        assert!(
            !effects
                .iter()
                .any(|(_, effect)| matches!(effect, Outbound::Deliver(_))),
            "a clean start received what the old session was owed"
        );

        publish(
            &mut broker,
            &mut out,
            publisher,
            "sensors/eu/temp",
            b"22",
            AtLeastOnce,
        );
        assert!(
            deliveries(&mut out).is_empty(),
            "the old subscription survived"
        );
        assert_eq!(broker.stats().offline_sessions, 0);
    }

    #[test]
    fn what_is_allowed_to_be_lost_is_lost_while_the_session_is_away() {
        let (mut broker, mut out) = broker();
        let session = connect_resuming(&mut broker, &mut out, 1, "device", true);
        subscribe(&mut broker, &mut out, session, "a/#", AtMostOnce);
        let _ = drain(&mut out);
        disconnect(&mut broker, &mut out, session);

        let publisher = connect(&mut broker, &mut out, 2, "pub");
        let _ = drain(&mut out);
        publish(&mut broker, &mut out, publisher, "a/b", b"x", AtMostOnce);
        let _ = drain(&mut out);

        assert_eq!(broker.stats().offline_dropped, 1);

        let back = connect_resuming(&mut broker, &mut out, 3, "device", true);
        let effects = drain(&mut out);
        assert!(effects.contains(&(
            back,
            Outbound::Reply(Response::Connected {
                session_present: true
            })
        )));
        assert!(
            !effects
                .iter()
                .any(|(_, effect)| matches!(effect, Outbound::Deliver(_))),
            "an at-most-once message survived an offline session"
        );
    }

    #[test]
    fn what_was_never_acknowledged_is_sent_again() {
        let (mut broker, mut out) = broker();
        let subscriber = connect_resuming(&mut broker, &mut out, 1, "device", true);
        subscribe(&mut broker, &mut out, subscriber, "a/#", AtLeastOnce);
        let publisher = connect(&mut broker, &mut out, 2, "pub");
        let _ = drain(&mut out);

        publish(&mut broker, &mut out, publisher, "a/b", b"x", AtLeastOnce);
        let first = deliveries(&mut out);
        assert_eq!(first.len(), 1);
        assert!(!first[0].1.duplicate);
        let id = first[0].1.id.unwrap();

        // The socket dies before the acknowledgement.
        disconnect(&mut broker, &mut out, subscriber);

        let back = connect_resuming(&mut broker, &mut out, 3, "device", true);
        let again = deliveries(&mut out);

        assert_eq!(again.len(), 1, "an unacknowledged message was forgotten");
        assert_eq!(again[0].0, back);
        assert_eq!(
            again[0].1.id,
            Some(id),
            "the identifier changed under the client"
        );
        assert!(
            again[0].1.duplicate,
            "the client was not told it may be a repeat"
        );
    }

    #[test]
    fn the_offline_store_refuses_to_grow_without_a_bound() {
        let limits = BrokerLimits {
            max_offline_sessions: 1,
            ..BrokerLimits::default()
        };
        let mut broker = Broker::new(limits);
        let mut out = Outbox::new();

        let first = connect_resuming(&mut broker, &mut out, 1, "one", true);
        let second = connect_resuming(&mut broker, &mut out, 2, "two", true);
        let _ = drain(&mut out);
        disconnect(&mut broker, &mut out, first);
        disconnect(&mut broker, &mut out, second);

        assert_eq!(broker.stats().offline_sessions, 1);
        assert_eq!(broker.stats().offline_refused, 1);

        // The one that was refused comes back to nothing.
        let back = connect_resuming(&mut broker, &mut out, 3, "two", true);
        assert!(drain(&mut out).contains(&(
            back,
            Outbound::Reply(Response::Connected {
                session_present: false
            })
        )));
    }

    #[test]
    fn a_resumed_session_is_authorised_again() {
        let (mut broker, mut out) = broker();
        let first = SessionId(1);
        broker.handle(
            first,
            Command::Connect(Connect {
                client_id: client("device"),
                permissions: Permissions::unrestricted(),
                resume: true,
                will: None,
            }),
            &mut out,
        );
        let _ = drain(&mut out);
        disconnect(&mut broker, &mut out, first);

        // The rules changed while it was away.
        let second = SessionId(2);
        broker.handle(
            second,
            Command::Connect(Connect {
                client_id: client("device"),
                permissions: Permissions::from_rules(vec![(filter("allowed/#"), true, true)]),
                resume: true,
                will: None,
            }),
            &mut out,
        );
        let _ = drain(&mut out);

        publish(
            &mut broker,
            &mut out,
            second,
            "forbidden/topic",
            b"x",
            AtMostOnce,
        );
        let _ = drain(&mut out);
        assert_eq!(
            broker.stats().unauthorized_publishes,
            1,
            "a resumed session kept its old permissions"
        );
    }
}

#[test]
fn a_session_with_no_name_is_never_kept() {
    let (mut broker, mut out) = broker();
    let session = SessionId(1);
    broker.handle(
        session,
        Command::Connect(Connect {
            client_id: ClientId::new(Bytes::new()).unwrap(),
            permissions: Permissions::unrestricted(),
            // The client asked for a session it will never be able to claim.
            resume: true,
            will: None,
        }),
        &mut out,
    );
    let _ = drain(&mut out);

    broker.handle(
        session,
        Command::Disconnect {
            reason: DisconnectReason::ConnectionLost,
        },
        &mut out,
    );

    assert_eq!(
        broker.stats().offline_sessions,
        0,
        "a session nobody can name was parked anyway"
    );
    assert_eq!(broker.stats().active_sessions, 0);
}
