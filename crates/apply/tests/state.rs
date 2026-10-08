//! The apply function, one rule per test.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use apply::{ApplyError, Effect, Limits, Options, Origin, Proposal, RetainHandling, State};
use bytes::Bytes;
use domain::{DeliveryGuarantee as G, Topic, TopicFilter};
use journal::{Chain, Entry, Kind, decode};
use proptest::prelude::*;

fn origin(node: u8, session: u64) -> Origin {
    Origin { node, session }
}

fn topic(s: &'static str) -> Topic {
    Topic::new(Bytes::from_static(s.as_bytes())).unwrap()
}

fn filter(s: &'static str) -> TopicFilter {
    TopicFilter::new(Bytes::from_static(s.as_bytes())).unwrap()
}

fn publish(from: Origin, t: &'static str, payload: &'static [u8], g: G, retain: bool) -> Proposal {
    Proposal::Publish {
        origin: from,
        request: 7,
        topic: topic(t),
        payload: Bytes::from_static(payload),
        properties: Bytes::new(),
        guarantee: g,
        retain,
    }
}

fn subscribe_with(to: Origin, f: &'static str, g: G, options: Options) -> Proposal {
    Proposal::Subscribe {
        origin: to,
        request: 8,
        filter: filter(f),
        guarantee: g,
        options,
    }
}

fn subscribe(to: Origin, f: &'static str, g: G) -> Proposal {
    subscribe_with(to, f, g, Options::default())
}

/// The proposals as one committed log: one batch each.
fn log(proposals: &[Proposal]) -> Vec<Vec<u8>> {
    let mut chain = Chain::genesis();
    let mut records = Vec::new();
    for proposal in proposals {
        let mut body = Vec::new();
        proposal.encode(&mut body).unwrap();
        let entry = Entry {
            kind: proposal.kind(),
            properties: proposal.properties(),
            body: &body,
        };
        chain.batch(1, &[entry], &mut records).unwrap();
    }
    records
}

fn run_with(limits: Limits, proposals: &[Proposal]) -> (State, Vec<Effect>) {
    let mut state = State::new(limits);
    let mut effects = Vec::new();
    for record in log(proposals) {
        state
            .apply(&decode(&record).unwrap(), &mut effects)
            .unwrap();
    }
    (state, effects)
}

fn run(proposals: &[Proposal]) -> (State, Vec<Effect>) {
    run_with(Limits::default(), proposals)
}

/// (to, id, guarantee, retain, lsn) of every delivery.
fn deliveries(effects: &[Effect]) -> Vec<(Origin, Option<u64>, G, bool, u64)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Deliver {
                to,
                id,
                guarantee,
                retain,
                lsn,
                ..
            } => Some((*to, *id, *guarantee, *retain, *lsn)),
            _ => None,
        })
        .collect()
}

#[test]
fn a_publish_reaches_each_matching_session_once_at_the_lesser_guarantee() {
    let a = origin(1, 1);
    let b = origin(2, 1);
    let (_, effects) = run(&[
        subscribe(a, "x/+", G::AtMostOnce),
        // Overlapping filters of one session: one delivery, the stronger.
        subscribe(a, "x/#", G::AtLeastOnce),
        subscribe(b, "y", G::AtLeastOnce),
        publish(origin(3, 9), "x/1", b"m", G::AtLeastOnce, false),
        publish(origin(3, 9), "x/1", b"n", G::AtMostOnce, false),
    ]);
    // Records: three subscribes (lsn 2, 4, 6), publishes at 8 and 10.
    assert_eq!(
        deliveries(&effects),
        vec![
            (a, Some(0), G::AtLeastOnce, false, 8),
            (a, None, G::AtMostOnce, false, 10)
        ]
    );
    let accepted: Vec<u64> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Accepted { to, lsn, .. } if *to == origin(3, 9) => Some(*lsn),
            _ => None,
        })
        .collect();
    assert_eq!(accepted, vec![8, 10]);
}

#[test]
fn a_retained_message_goes_to_the_next_subscriber_and_an_empty_one_removes_it() {
    let a = origin(1, 1);
    let (_, effects) = run(&[
        publish(origin(2, 2), "r", b"old", G::AtLeastOnce, true),
        publish(origin(2, 2), "r", b"new", G::AtLeastOnce, true),
        subscribe(a, "r", G::AtMostOnce),
    ]);
    // With the subscribe's lsn, at the lesser guarantee, flagged retained.
    assert_eq!(
        deliveries(&effects),
        vec![(a, None, G::AtMostOnce, true, 6)]
    );
    let (_, effects) = run(&[
        publish(origin(2, 2), "r", b"old", G::AtLeastOnce, true),
        publish(origin(2, 2), "r", b"", G::AtLeastOnce, true),
        subscribe(a, "r", G::AtMostOnce),
    ]);
    assert!(deliveries(&effects).is_empty());
}

#[test]
fn subscription_options_shape_what_is_delivered() {
    let a = origin(1, 1);
    // No Local: not the session's own publishes.
    let (_, effects) = run(&[
        subscribe_with(
            a,
            "t",
            G::AtMostOnce,
            Options {
                no_local: true,
                ..Options::default()
            },
        ),
        publish(a, "t", b"own", G::AtMostOnce, false),
        publish(origin(2, 2), "t", b"other", G::AtMostOnce, false),
    ]);
    assert_eq!(deliveries(&effects).len(), 1);
    // Retain As Published keeps the publisher's flag on a live delivery.
    let rap = Options {
        retain_as_published: true,
        ..Options::default()
    };
    let (_, effects) = run(&[
        subscribe_with(a, "t", G::AtMostOnce, rap),
        publish(origin(2, 2), "t", b"m", G::AtMostOnce, true),
    ]);
    assert!(deliveries(&effects)[0].3);
    // Retain Handling: only for a new subscription, or never.
    let new_only = Options {
        retain_handling: RetainHandling::OnNewSubscription,
        ..Options::default()
    };
    let (_, effects) = run(&[
        publish(origin(2, 2), "t", b"kept", G::AtMostOnce, true),
        subscribe_with(a, "t", G::AtMostOnce, new_only),
        subscribe_with(a, "t", G::AtMostOnce, new_only),
        subscribe_with(
            origin(1, 2),
            "t",
            G::AtMostOnce,
            Options {
                retain_handling: RetainHandling::Never,
                ..Options::default()
            },
        ),
    ]);
    assert_eq!(deliveries(&effects).len(), 1);
}

#[test]
fn at_least_once_waits_for_acks_behind_the_window_and_a_full_backlog_closes_the_session() {
    let a = origin(1, 1);
    let limits = Limits {
        max_inflight: 1,
        max_backlog: 1,
        ..Limits::default()
    };
    let p = |b: &'static [u8]| publish(origin(2, 2), "t", b, G::AtLeastOnce, false);
    let (state, effects) = run_with(
        limits,
        &[subscribe(a, "t", G::AtLeastOnce), p(b"1"), p(b"2")],
    );
    // One inflight, one waiting.
    assert_eq!(
        deliveries(&effects),
        vec![(a, Some(0), G::AtLeastOnce, false, 4)]
    );
    assert_eq!(state.inflight(a), 1);

    // The ack frees the window: the waiting message goes, caused by the ack.
    let (_, effects) = run_with(
        limits,
        &[
            subscribe(a, "t", G::AtLeastOnce),
            p(b"1"),
            p(b"2"),
            Proposal::Ack {
                origin: a,
                delivery: 0,
            },
            // A retried ack is a no-op.
            Proposal::Ack {
                origin: a,
                delivery: 0,
            },
        ],
    );
    assert_eq!(
        deliveries(&effects),
        vec![
            (a, Some(0), G::AtLeastOnce, false, 4),
            (a, Some(1), G::AtLeastOnce, false, 8)
        ]
    );
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::Acked { .. }))
            .count(),
        1
    );

    // A third message finds the backlog full: the session is closed and gone.
    let (state, effects) = run_with(
        limits,
        &[subscribe(a, "t", G::AtLeastOnce), p(b"1"), p(b"2"), p(b"3")],
    );
    assert!(effects.contains(&Effect::Close { to: a, lsn: 8 }));
    assert_eq!(state.sessions(), 0);
}

#[test]
fn the_global_queued_bound_counts_inflight_and_closes_whoever_would_exceed_it() {
    let limits = Limits {
        max_inflight: 1,
        max_total_queued_bytes: 3,
        ..Limits::default()
    };
    let (a, b) = (origin(1, 1), origin(1, 2));
    let (_, effects) = run_with(
        limits,
        &[
            subscribe(a, "t", G::AtLeastOnce),
            subscribe(b, "t", G::AtLeastOnce),
            publish(origin(2, 2), "t", b"xx", G::AtLeastOnce, false),
        ],
    );
    // Inflight counts: a's "xx" holds two of the three bytes, so b's copy
    // of the same message would make four (CLAUDE.md §7: a per-session
    // bound times the session count is not a bound).
    assert!(effects.contains(&Effect::Close { to: b, lsn: 6 }));
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::Close { to, .. } if *to == a))
    );
}

#[test]
fn a_gone_session_and_a_reset_node_leave_nothing_behind() {
    let (a, b, c) = (origin(1, 1), origin(1, 2), origin(2, 1));
    let (state, effects) = run(&[
        subscribe(a, "t", G::AtLeastOnce),
        subscribe(b, "t", G::AtLeastOnce),
        subscribe(c, "t", G::AtLeastOnce),
        publish(origin(3, 3), "t", b"m", G::AtLeastOnce, false),
        Proposal::SessionGone { origin: a },
        Proposal::NodeReset {
            node: 1,
            below: 100,
        },
    ]);
    assert_eq!(state.sessions(), 1, "only node 2's session is left");
    assert!(effects.contains(&Effect::Gone { to: a, lsn: 10 }));
    assert!(effects.contains(&Effect::Reset {
        node: 1,
        below: 100,
        lsn: 12
    }));
}

#[test]
fn bounds_refuse_subscriptions_and_keep_retained_messages_from_growing() {
    let limits = Limits {
        max_subscriptions_per_session: 1,
        max_retained: 1,
        ..Limits::default()
    };
    let a = origin(1, 1);
    let (_, effects) = run_with(
        limits,
        &[
            subscribe(a, "x", G::AtMostOnce),
            subscribe(a, "y", G::AtMostOnce),
            // Re-subscribing to a filter it has is not a new one.
            subscribe(a, "x", G::AtLeastOnce),
        ],
    );
    let granted: Vec<Option<G>> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Subscribed { granted, .. } => Some(*granted),
            _ => None,
        })
        .collect();
    assert_eq!(
        granted,
        vec![Some(G::AtMostOnce), None, Some(G::AtLeastOnce)]
    );

    let (_, effects) = run_with(
        limits,
        &[
            publish(origin(2, 2), "r1", b"a", G::AtMostOnce, true),
            // Delivered, not kept: one retained message is the bound.
            publish(origin(2, 2), "r2", b"b", G::AtMostOnce, true),
            subscribe(a, "#", G::AtMostOnce),
        ],
    );
    assert_eq!(deliveries(&effects).len(), 1);
}

#[test]
fn records_apply_in_order_once_and_unknown_ones_stop_the_node() {
    let records = log(&[subscribe(origin(1, 1), "t", G::AtMostOnce)]);
    let mut state = State::new(Limits::default());
    let mut out = Vec::new();
    assert!(matches!(
        state.apply(&decode(&records[1]).unwrap(), &mut out),
        Err(ApplyError::OutOfOrder {
            lsn: 2,
            expected: 1
        })
    ));
    state
        .apply(&decode(&records[0]).unwrap(), &mut out)
        .unwrap();
    state
        .apply(&decode(&records[1]).unwrap(), &mut out)
        .unwrap();
    assert!(matches!(
        state.apply(&decode(&records[1]).unwrap(), &mut out),
        Err(ApplyError::OutOfOrder { .. })
    ));

    let mut chain = Chain::genesis();
    let mut session = Vec::new();
    chain
        .batch(
            1,
            &[Entry {
                kind: Kind::Session,
                properties: &[],
                body: b"",
            }],
            &mut session,
        )
        .unwrap();
    let mut fresh = State::new(Limits::default());
    fresh
        .apply(&decode(&session[0]).unwrap(), &mut out)
        .unwrap();
    assert_eq!(
        fresh.apply(&decode(&session[1]).unwrap(), &mut out),
        Err(ApplyError::NotYet(Kind::Session))
    );
}

#[test]
fn a_body_that_does_not_decode_is_refused() {
    let mut chain = Chain::genesis();
    let mut records = Vec::new();
    chain
        .batch(
            1,
            &[Entry {
                kind: Kind::Publish,
                properties: &[],
                body: &[1, 2, 3],
            }],
            &mut records,
        )
        .unwrap();
    let mut state = State::new(Limits::default());
    let mut out = Vec::new();
    state
        .apply(&decode(&records[0]).unwrap(), &mut out)
        .unwrap();
    assert_eq!(
        state.apply(&decode(&records[1]).unwrap(), &mut out),
        Err(ApplyError::Malformed(2))
    );
}

fn any_proposal() -> impl Strategy<Value = Proposal> {
    let origin = (0u8..3, 0u64..4).prop_map(|(node, session)| Origin { node, session });
    let g = prop_oneof![Just(G::AtMostOnce), Just(G::AtLeastOnce)];
    let topic = "[a-c]{1,2}(/[a-c]{1,2}){0,2}".prop_map(|s| Topic::new(Bytes::from(s)).unwrap());
    let filter = prop_oneof![
        "[a-c]{1,2}(/[a-c]{1,2}){0,2}",
        "[a-c]{1,2}/\\+",
        "[a-c]/#",
        Just("#".to_string())
    ]
    .prop_map(|s| TopicFilter::new(Bytes::from(s)).unwrap());
    let options = (any::<bool>(), any::<bool>(), 0u8..3).prop_map(|(no_local, rap, rh)| Options {
        no_local,
        retain_as_published: rap,
        retain_handling: match rh {
            0 => RetainHandling::OnSubscribe,
            1 => RetainHandling::OnNewSubscription,
            _ => RetainHandling::Never,
        },
    });
    prop_oneof![
        4 => (origin.clone(), any::<u64>(), topic, proptest::collection::vec(any::<u8>(), 0..8), g.clone(), any::<bool>())
            .prop_map(|(origin, request, topic, payload, guarantee, retain)| Proposal::Publish {
                origin, request, topic, properties: Bytes::from(payload.iter().rev().copied().collect::<Vec<u8>>()), payload: Bytes::from(payload), guarantee, retain,
            }),
        2 => (origin.clone(), any::<u64>(), filter.clone(), g, options)
            .prop_map(|(origin, request, filter, guarantee, options)| Proposal::Subscribe { origin, request, filter, guarantee, options }),
        1 => (origin.clone(), any::<u64>(), filter)
            .prop_map(|(origin, request, filter)| Proposal::Unsubscribe { origin, request, filter }),
        2 => (origin.clone(), 0u64..6).prop_map(|(origin, delivery)| Proposal::Ack { origin, delivery }),
        1 => origin.prop_map(|origin| Proposal::SessionGone { origin }),
        1 => (0u8..3, 0u64..5).prop_map(|(node, below)| Proposal::NodeReset { node, below }),
    ]
}

proptest! {
    /// What a gateway proposes is what every node reads back.
    #[test]
    fn a_proposal_survives_the_record(proposal in any_proposal()) {
        let records = log(std::slice::from_ref(&proposal));
        prop_assert_eq!(Proposal::decode(&decode(&records[1]).unwrap()).unwrap(), proposal);
    }

    /// Two nodes applying the same log hold the same state and produce the
    /// same effects, in the same order; inflight never passes its bound.
    #[test]
    fn the_same_log_gives_the_same_state(proposals in proptest::collection::vec(any_proposal(), 0..60)) {
        let limits = Limits { max_inflight: 2, max_backlog: 2, ..Limits::default() };
        let (a, ea) = run_with(limits, &proposals);
        let (b, eb) = run_with(limits, &proposals);
        prop_assert_eq!(a.digest(), b.digest());
        prop_assert_eq!(&a, &b);
        prop_assert_eq!(ea, eb);
        for node in 0..3 {
            for session in 0..4 {
                let inflight = a.inflight(Origin { node, session });
                prop_assert!(inflight <= 2);
            }
        }
    }

    /// A snapshot taken anywhere in the log, decoded, and followed by the
    /// rest of the log gives the state and effects of applying the whole
    /// log (D-67 B): a node that restarts from one is indistinguishable.
    #[test]
    fn a_snapshot_and_the_log_after_it_give_the_whole_log(
        proposals in proptest::collection::vec(any_proposal(), 0..60),
        cut in any::<prop::sample::Index>(),
    ) {
        let limits = Limits { max_inflight: 2, max_backlog: 2, ..Limits::default() };
        let records = log(&proposals);
        let (whole, effects) = run_with(limits, &proposals);
        let at = cut.index(records.len() + 1);
        let mut state = State::new(limits);
        let mut before = Vec::new();
        for record in &records[..at] {
            state.apply(&decode(record).unwrap(), &mut before).unwrap();
        }
        let mut bytes = Vec::new();
        state.encode(&mut bytes);
        let mut restored = State::decode(limits, &bytes).unwrap();
        prop_assert_eq!(&restored, &state);
        let mut after = Vec::new();
        for record in &records[at..] {
            restored.apply(&decode(record).unwrap(), &mut after).unwrap();
        }
        prop_assert_eq!(&restored, &whole);
        before.extend(after);
        prop_assert_eq!(before, effects);
    }

    /// Arbitrary bytes never panic the decoder, and every strict prefix of a
    /// real snapshot is refused.
    #[test]
    fn a_damaged_snapshot_is_refused(
        proposals in proptest::collection::vec(any_proposal(), 0..20),
        noise in proptest::collection::vec(any::<u8>(), 0..64),
        cut in any::<prop::sample::Index>(),
    ) {
        let limits = Limits::default();
        let _ = State::decode(limits, &noise);
        let (state, _) = run_with(limits, &proposals);
        let mut bytes = Vec::new();
        state.encode(&mut bytes);
        let short = cut.index(bytes.len());
        prop_assert!(State::decode(limits, &bytes[..short]).is_err());
    }
}

/// A snapshot is refused under other limits or another format version.
#[test]
fn a_snapshot_under_other_limits_or_version_is_refused() {
    let (state, _) = run_with(
        Limits::default(),
        &[subscribe(origin(1, 1), "a", G::AtLeastOnce)],
    );
    let mut bytes = Vec::new();
    state.encode(&mut bytes);
    let other = Limits {
        max_inflight: 3,
        ..Limits::default()
    };
    assert_eq!(
        State::decode(other, &bytes),
        Err(apply::SnapshotError::Limits)
    );
    bytes[0] = 99;
    assert_eq!(
        State::decode(Limits::default(), &bytes),
        Err(apply::SnapshotError::Version(99))
    );
}

/// An at-most-once message does not overtake an earlier at-least-once one
/// that waits for room; with the backlog full it is dropped, not the
/// session (found by the gateway simulation).
#[test]
fn nothing_overtakes_a_message_that_waits() {
    let a = origin(1, 1);
    let limits = Limits {
        max_inflight: 1,
        max_backlog: 2,
        ..Limits::default()
    };
    let p = |b: &'static [u8], g: G| publish(origin(2, 2), "t", b, g, false);
    let (state, effects) = run_with(
        limits,
        &[
            subscribe(a, "t", G::AtLeastOnce),
            p(b"1", G::AtLeastOnce),
            p(b"2", G::AtLeastOnce),
            p(b"3", G::AtMostOnce),
            // The backlog is full: an at-most-once message is dropped.
            p(b"4", G::AtMostOnce),
            Proposal::Ack {
                origin: a,
                delivery: 0,
            },
        ],
    );
    let payloads: Vec<Bytes> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Deliver { payload, .. } => Some(payload.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        payloads,
        vec![
            Bytes::from_static(b"1"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"3")
        ]
    );
    assert_eq!(
        state.sessions(),
        1,
        "dropping at-most-once does not close the session"
    );
}

/// A publish's properties section reaches every delivery unchanged, live
/// and retained, and counts against the byte bounds (D-65).
#[test]
fn properties_travel_with_the_message() {
    let a = origin(1, 1);
    let with = |props: &'static [u8], retain: bool| Proposal::Publish {
        origin: origin(2, 2),
        request: 1,
        topic: topic("t"),
        payload: Bytes::from_static(b"m"),
        properties: Bytes::from_static(props),
        guarantee: G::AtMostOnce,
        retain,
    };
    let (_, effects) = run(&[
        subscribe(a, "t", G::AtMostOnce),
        with(b"live", false),
        with(b"kept", true),
        subscribe(origin(1, 2), "t", G::AtMostOnce),
    ]);
    let props: Vec<(Origin, Bytes)> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Deliver { to, properties, .. } => Some((*to, properties.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        props,
        vec![
            (a, Bytes::from_static(b"live")),
            (a, Bytes::from_static(b"kept")),
            (origin(1, 2), Bytes::from_static(b"kept")),
        ]
    );
    // Retained bytes count the properties: four bytes of properties and one
    // of payload do not fit a bound of four.
    let limits = Limits {
        max_retained_bytes: 4,
        ..Limits::default()
    };
    let (_, effects) = run_with(
        limits,
        &[with(b"kept", true), subscribe(a, "t", G::AtMostOnce)],
    );
    assert!(!effects.iter().any(|e| matches!(e, Effect::Deliver { .. })));
}

/// A new slot's `SlotStart` advances the position and changes nothing.
#[test]
fn a_slot_start_changes_no_state() {
    let (before, _) = run_with(
        Limits::default(),
        &[subscribe(origin(1, 1), "a", G::AtLeastOnce)],
    );
    let mut chain = Chain::genesis();
    let mut records = Vec::new();
    let proposal = subscribe(origin(1, 1), "a", G::AtLeastOnce);
    let mut body = Vec::new();
    proposal.encode(&mut body).unwrap();
    let entry = Entry {
        kind: proposal.kind(),
        properties: proposal.properties(),
        body: &body,
    };
    chain.batch(1, &[entry], &mut records).unwrap();
    let start = Entry {
        kind: Kind::SlotStart,
        properties: &[],
        body: &2u64.to_be_bytes(),
    };
    chain.batch(2, &[start], &mut records).unwrap();
    let mut state = State::new(Limits::default());
    let mut effects = Vec::new();
    for record in &records {
        state.apply(&decode(record).unwrap(), &mut effects).unwrap();
    }
    assert_eq!(state.applied(), 4);
    assert_eq!(state.sessions(), before.sessions());
    assert_eq!(effects.len(), 1, "only the subscription's own effect");
}
