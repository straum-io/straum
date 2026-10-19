//! Step 7c in `apply` (ADR-0008 "Step 7c design", D-146 A): client ids,
//! `Connect` and `Disconnect`, the fence, persisted sessions. One rule per
//! test, then invariants A and B and the snapshot as properties.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::collections::BTreeMap;

use apply::{
    ApplyError, ClientEffect, ConnectLimits, Effect, Limits, Origin, Proposal, Refusal, State,
};
use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee as G, Topic, TopicFilter};
use journal::{Chain, Entry, Kind, decode};
use proptest::prelude::*;

fn origin(node: u8, session: u64) -> Origin {
    Origin { node, session }
}

fn id(s: &'static str) -> ClientId {
    ClientId::new(Bytes::from_static(s.as_bytes())).unwrap()
}

fn connect(
    o: Origin,
    client: &'static str,
    seen: u64,
    clean_start: bool,
    persist: bool,
) -> Proposal {
    Proposal::Connect {
        origin: o,
        client_id: id(client),
        seen,
        clean_start,
        persist,
    }
}

fn subscribe(o: Origin, f: &'static str, g: G) -> Proposal {
    Proposal::Subscribe {
        origin: o,
        request: 8,
        filter: TopicFilter::new(Bytes::from_static(f.as_bytes())).unwrap(),
        guarantee: g,
        options: apply::Options::default(),
    }
}

fn publish(o: Origin, t: &'static str, payload: &'static [u8], g: G) -> Proposal {
    Proposal::Publish {
        origin: o,
        request: 7,
        topic: Topic::new(Bytes::from_static(t.as_bytes())).unwrap(),
        payload: Bytes::from_static(payload),
        properties: Bytes::new(),
        guarantee: g,
        retain: false,
    }
}

/// The proposals as one committed log, one batch each: the `i`-th
/// proposal is at lsn `2 * (i + 1)`.
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

/// The lsn of the `i`-th proposal (from 0).
fn at(i: u64) -> u64 {
    2 * (i + 1)
}

fn run_with(
    (limits, connect): (Limits, ConnectLimits),
    proposals: &[Proposal],
) -> (State, Vec<Effect>, Vec<ClientEffect>) {
    let mut state = State::with_connect(limits, connect);
    let mut effects = Vec::new();
    let mut clients = Vec::new();
    for record in log(proposals) {
        state
            .apply_connect(&decode(&record).unwrap(), &mut effects, &mut clients)
            .unwrap();
    }
    (state, effects, clients)
}

fn run(proposals: &[Proposal]) -> (State, Vec<Effect>, Vec<ClientEffect>) {
    run_with((Limits::default(), ConnectLimits::default()), proposals)
}

fn attached(state: &State, client: &str) -> Option<Origin> {
    state.client(client.as_bytes()).and_then(|c| c.attached)
}

#[test]
fn without_step_7c_a_connect_is_refused_as_not_yet_built() {
    let records = log(&[connect(origin(1, 1), "c", 0, true, false)]);
    let mut state = State::new(Limits::default());
    let mut out = Vec::new();
    state
        .apply(&decode(&records[0]).unwrap(), &mut out)
        .unwrap();
    assert_eq!(
        state.apply(&decode(&records[1]).unwrap(), &mut out),
        Err(ApplyError::NotYet(Kind::Connect))
    );
}

#[test]
fn a_connect_and_a_disconnect_round_trip_as_records() {
    let proposals = [
        connect(origin(3, 9), "client-1", 41, false, true),
        Proposal::Disconnect {
            origin: origin(3, 9),
        },
    ];
    for (proposal, record) in proposals
        .iter()
        .zip(log(&proposals).iter().skip(1).step_by(2))
    {
        assert_eq!(
            &Proposal::decode(&decode(record).unwrap()).unwrap(),
            proposal
        );
    }
}

fn long_id(bytes: usize) -> ClientId {
    ClientId::new(Bytes::from(vec![b'x'; bytes])).unwrap()
}

/// D-153 precision 8: 256 bytes is Straum's bound on a client id (the
/// codec's default `max_client_id_bytes`), not MQTT's maximum of 65 535. A
/// longer one is refused before it reaches the log, as a record from the
/// log, and from a snapshot.
#[test]
fn a_client_id_of_256_bytes_passes_and_257_is_refused_on_every_path() {
    assert_eq!(apply::MAX_CLIENT_ID_BYTES, 256);
    let connect_with = |id: ClientId| Proposal::Connect {
        origin: origin(1, 1),
        client_id: id,
        seen: 0,
        clean_start: true,
        persist: true,
    };

    // 256: encoded, committed, applied, and through a snapshot.
    let at_bound = connect_with(long_id(256));
    let (state, _, clients) = run(std::slice::from_ref(&at_bound));
    assert!(matches!(clients[..], [ClientEffect::Connected { .. }]));
    assert!(state.client(&[b'x'; 256]).is_some());
    let mut bytes = Vec::new();
    state.encode(&mut bytes);
    let decoded =
        State::decode_connect(Limits::default(), ConnectLimits::default(), &bytes).unwrap();
    assert_eq!(decoded, state);

    // 257: not encoded, so no gateway can write it to the log.
    assert!(connect_with(long_id(257)).encode(&mut Vec::new()).is_err());

    // 257 in a record anyway: malformed, the node stops rather than apply it.
    let mut body = Vec::new();
    at_bound.encode(&mut body).unwrap();
    let length = body.len() - 256 - 2;
    body[length..length + 2].copy_from_slice(&257u16.to_be_bytes());
    body.push(b'x');
    let mut records = Vec::new();
    Chain::genesis()
        .batch(
            1,
            &[Entry {
                kind: Kind::Connect,
                properties: &[],
                body: &body,
            }],
            &mut records,
        )
        .unwrap();
    let mut fresh = State::with_connect(Limits::default(), ConnectLimits::default());
    let mut out = Vec::new();
    let mut clients = Vec::new();
    fresh
        .apply_connect(&decode(&records[0]).unwrap(), &mut out, &mut clients)
        .unwrap();
    assert_eq!(
        fresh.apply_connect(&decode(&records[1]).unwrap(), &mut out, &mut clients),
        Err(ApplyError::Malformed(2))
    );

    // 257 in a snapshot: refused.
    let at = bytes
        .windows(258)
        .position(|w| w[..2] == 256u16.to_be_bytes() && w[2..].iter().all(|&b| b == b'x'))
        .unwrap();
    let mut long = bytes[..at].to_vec();
    long.extend_from_slice(&257u16.to_be_bytes());
    long.extend(std::iter::repeat_n(b'x', 257));
    long.extend_from_slice(&bytes[at + 258..]);
    assert!(State::decode_connect(Limits::default(), ConnectLimits::default(), &long).is_err());
}

#[test]
fn a_connect_attaches_its_origin() {
    let a = origin(1, 1);
    let (state, _, clients) = run(&[connect(a, "c", 0, true, false)]);
    assert_eq!(
        clients,
        vec![ClientEffect::Connected {
            to: a,
            session_present: false,
            lsn: at(0)
        }]
    );
    assert_eq!(attached(&state, "c"), Some(a));
    assert_eq!(state.attached_to(a), Some(&b"c"[..]));
}

#[test]
fn nothing_is_committed_for_an_origin_that_is_not_attached() {
    let a = origin(1, 1);
    let (state, _, clients) = run(&[
        subscribe(a, "t", G::AtLeastOnce),
        publish(a, "t", b"x", G::AtMostOnce),
        Proposal::Ack {
            origin: a,
            delivery: 0,
        },
    ]);
    assert_eq!(
        clients,
        vec![
            ClientEffect::Refused {
                to: a,
                request: Some(8),
                lsn: at(0)
            },
            ClientEffect::Refused {
                to: a,
                request: Some(7),
                lsn: at(1)
            },
            ClientEffect::Refused {
                to: a,
                request: None,
                lsn: at(2)
            },
        ]
    );
    assert_eq!(state.sessions(), 0);
}

#[test]
fn a_newer_connect_takes_the_session_over_and_the_old_origin_is_refused() {
    let (a, b) = (origin(1, 1), origin(2, 1));
    let (state, _, clients) = run(&[
        connect(a, "c", 0, true, false),
        subscribe(a, "t", G::AtMostOnce),
        // Node 2 had applied both before it proposed.
        connect(b, "c", at(1), false, false),
        publish(a, "t", b"late", G::AtMostOnce),
    ]);
    assert!(clients.contains(&ClientEffect::TakenOver { to: a, lsn: at(2) }));
    assert!(clients.contains(&ClientEffect::Connected {
        to: b,
        session_present: true,
        lsn: at(2)
    }));
    assert!(clients.contains(&ClientEffect::Refused {
        to: a,
        request: Some(7),
        lsn: at(3)
    }));
    assert_eq!(attached(&state, "c"), Some(b));
    assert_eq!(state.attached_to(a), None);
}

#[test]
fn a_clean_start_takeover_clears_the_session() {
    let (a, b, p) = (origin(1, 1), origin(2, 1), origin(3, 1));
    let (_, effects, clients) = run(&[
        connect(a, "c", 0, true, false),
        subscribe(a, "t", G::AtMostOnce),
        connect(b, "c", at(1), true, false),
        connect(p, "p", at(2), true, false),
        publish(p, "t", b"x", G::AtMostOnce),
    ]);
    assert!(clients.contains(&ClientEffect::Connected {
        to: b,
        session_present: false,
        lsn: at(2)
    }));
    assert!(!effects.iter().any(|e| matches!(e, Effect::Deliver { .. })));
}

#[test]
fn a_connect_whose_proposer_had_not_seen_the_last_change_is_refused() {
    let (a, b) = (origin(1, 1), origin(2, 1));
    let (state, _, clients) = run(&[
        connect(a, "c", 0, true, false),
        // Proposed before node 2 applied `a`'s Connect.
        connect(b, "c", 1, true, false),
    ]);
    assert!(clients.contains(&ClientEffect::ConnectRefused {
        to: b,
        reason: Refusal::Stale,
        lsn: at(1)
    }));
    assert!(!clients.contains(&ClientEffect::TakenOver { to: a, lsn: at(1) }));
    assert_eq!(attached(&state, "c"), Some(a));
}

#[test]
fn a_connect_older_than_the_window_is_refused() {
    let limits = (
        Limits::default(),
        ConnectLimits {
            connect_window: 3,
            ..ConnectLimits::default()
        },
    );
    let a = origin(1, 1);
    let (_, _, clients) = run_with(
        limits,
        &[
            connect(origin(9, 1), "other", 0, true, false),
            connect(origin(9, 2), "other2", at(0), true, false),
            // At lsn 6, seen 2: 4 positions, past the window of 3.
            connect(a, "c", 2, true, false),
        ],
    );
    assert!(clients.contains(&ClientEffect::ConnectRefused {
        to: a,
        reason: Refusal::Stale,
        lsn: at(2)
    }));
}

#[test]
fn a_seen_at_or_past_its_own_lsn_is_refused() {
    let a = origin(1, 1);
    let (_, _, clients) = run(&[connect(a, "c", at(0), true, false)]);
    assert_eq!(
        clients,
        vec![ClientEffect::ConnectRefused {
            to: a,
            reason: Refusal::Stale,
            lsn: at(0)
        }]
    );
}

#[test]
fn an_origin_connects_once() {
    let a = origin(1, 1);
    let (state, _, clients) = run(&[
        connect(a, "c", 0, true, false),
        connect(a, "d", at(0), true, false),
    ]);
    assert!(clients.contains(&ClientEffect::ConnectRefused {
        to: a,
        reason: Refusal::Stale,
        lsn: at(1)
    }));
    assert_eq!(state.client(b"d"), None);
}

#[test]
fn a_disconnect_removes_a_clean_entry_and_fences_a_connect_that_had_not_seen_it() {
    let (a, b, c) = (origin(1, 1), origin(2, 1), origin(2, 2));
    let (state, _, clients) = run(&[
        connect(a, "c", 0, true, false),
        Proposal::Disconnect { origin: a },
        // Seen the Connect, not the removal.
        connect(b, "c", at(0), true, false),
        connect(c, "c", at(1), true, false),
    ]);
    assert!(clients.contains(&ClientEffect::Disconnected {
        to: a,
        attached: true,
        lsn: at(1)
    }));
    assert!(clients.contains(&ClientEffect::ConnectRefused {
        to: b,
        reason: Refusal::Stale,
        lsn: at(2)
    }));
    assert!(clients.contains(&ClientEffect::Connected {
        to: c,
        session_present: false,
        lsn: at(3)
    }));
    assert_eq!(attached(&state, "c"), Some(c));
}

#[test]
fn a_disconnect_of_an_origin_not_attached_changes_nothing() {
    let a = origin(1, 1);
    let (state, _, clients) = run(&[Proposal::Disconnect { origin: a }]);
    assert_eq!(
        clients,
        vec![ClientEffect::Disconnected {
            to: a,
            attached: false,
            lsn: at(0)
        }]
    );
    assert_eq!(state.connect_fence(), (0, 0));
}

#[test]
fn a_persisted_session_waits_detached_and_resumes_with_what_arrived() {
    let (a, b, p) = (origin(1, 1), origin(2, 1), origin(3, 1));
    let (state, effects, clients) = run(&[
        connect(a, "c", 0, true, true),
        subscribe(a, "t", G::AtLeastOnce),
        Proposal::Disconnect { origin: a },
        connect(p, "p", at(2), true, false),
        publish(p, "t", b"1", G::AtLeastOnce),
        publish(p, "t", b"2", G::AtMostOnce),
        connect(b, "c", at(5), false, true),
    ]);
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::Deliver { lsn, .. } if *lsn < at(6))),
        "nothing goes out while detached"
    );
    assert!(clients.contains(&ClientEffect::Connected {
        to: b,
        session_present: true,
        lsn: at(6)
    }));
    let resumed: Vec<(Option<u64>, &[u8])> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Deliver {
                to, id, payload, ..
            } if *to == b => Some((*id, payload.as_ref())),
            _ => None,
        })
        .collect();
    assert_eq!(resumed, vec![(Some(0), &b"1"[..]), (None, &b"2"[..])]);
    assert_eq!(state.inflight(b), 1);
    assert_eq!(state.persisted(), (1, 0));
}

#[test]
fn a_detached_session_drops_an_at_most_once_message_it_has_no_room_for() {
    let limits = (
        Limits::default(),
        ConnectLimits {
            max_detached_queued_bytes: 1,
            ..ConnectLimits::default()
        },
    );
    let (a, p) = (origin(1, 1), origin(3, 1));
    let (state, _, clients) = run_with(
        limits,
        &[
            connect(a, "c", 0, true, true),
            subscribe(a, "t", G::AtMostOnce),
            Proposal::Disconnect { origin: a },
            connect(p, "p", at(2), true, false),
            publish(p, "t", b"1", G::AtMostOnce),
            publish(p, "t", b"22", G::AtMostOnce),
        ],
    );
    assert!(
        !clients
            .iter()
            .any(|e| matches!(e, ClientEffect::SessionDeleted { .. }))
    );
    assert_eq!(
        state.persisted(),
        (1, 1),
        "the first waits, the second dropped"
    );
}

#[test]
fn a_detached_session_with_no_room_for_an_at_least_once_message_is_deleted() {
    let limits = (
        Limits::default(),
        ConnectLimits {
            max_detached_queued_bytes: 1,
            ..ConnectLimits::default()
        },
    );
    let (a, b, p) = (origin(1, 1), origin(2, 1), origin(3, 1));
    let (state, _, clients) = run_with(
        limits,
        &[
            connect(a, "c", 0, true, true),
            subscribe(a, "t", G::AtLeastOnce),
            Proposal::Disconnect { origin: a },
            connect(p, "p", at(2), true, false),
            publish(p, "t", b"22", G::AtLeastOnce),
            connect(b, "c", at(4), false, true),
        ],
    );
    assert!(clients.contains(&ClientEffect::SessionDeleted { lsn: at(4) }));
    assert!(clients.contains(&ClientEffect::Connected {
        to: b,
        session_present: false,
        lsn: at(5)
    }));
    assert_eq!(state.connect_fence().1, 1, "the deletion left a tombstone");
}

#[test]
fn a_detachment_the_detached_budget_cannot_take_deletes_the_session() {
    let limits = (
        Limits {
            max_inflight: 1,
            ..Limits::default()
        },
        ConnectLimits {
            max_detached_queued_bytes: 0,
            ..ConnectLimits::default()
        },
    );
    let (a, p) = (origin(1, 1), origin(3, 1));
    let (state, _, clients) = run_with(
        limits,
        &[
            connect(a, "c", 0, true, true),
            subscribe(a, "t", G::AtLeastOnce),
            connect(p, "p", at(1), true, false),
            publish(p, "t", b"x", G::AtLeastOnce),
            Proposal::Disconnect { origin: a },
        ],
    );
    assert!(clients.contains(&ClientEffect::SessionDeleted { lsn: at(4) }));
    assert_eq!(state.client(b"c"), None);
    assert_eq!(state.persisted(), (0, 0));
}

#[test]
fn a_new_persisted_session_at_the_limit_is_refused_and_a_resumption_never_is() {
    let limits = (
        Limits::default(),
        ConnectLimits {
            max_persisted_sessions: 1,
            ..ConnectLimits::default()
        },
    );
    let (a, b, c) = (origin(1, 1), origin(2, 1), origin(2, 2));
    let (state, _, clients) = run_with(
        limits,
        &[
            connect(a, "c", 0, true, true),
            connect(b, "d", at(0), true, true),
            Proposal::Disconnect { origin: a },
            connect(c, "c", at(2), false, true),
        ],
    );
    assert!(clients.contains(&ClientEffect::ConnectRefused {
        to: b,
        reason: Refusal::Quota,
        lsn: at(1)
    }));
    assert_eq!(state.client(b"d"), None);
    assert!(clients.contains(&ClientEffect::Connected {
        to: c,
        session_present: true,
        lsn: at(3)
    }));
}

/// Found by the snapshot property: a clean entry taken over by a persisting
/// `Connect` adds a persisted session, so the limit applies to it too.
#[test]
fn a_clean_entry_made_persistent_at_the_limit_is_refused() {
    let limits = (
        Limits::default(),
        ConnectLimits {
            max_persisted_sessions: 1,
            ..ConnectLimits::default()
        },
    );
    let (a, b, c) = (origin(1, 1), origin(1, 2), origin(2, 1));
    let (state, _, clients) = run_with(
        limits,
        &[
            connect(a, "p", 0, true, true),
            connect(b, "q", at(0), true, false),
            connect(c, "q", at(1), false, true),
        ],
    );
    assert!(clients.contains(&ClientEffect::ConnectRefused {
        to: c,
        reason: Refusal::Quota,
        lsn: at(2)
    }));
    assert_eq!(attached(&state, "q"), Some(b));
    assert_eq!(state.persisted().0, 1);
}

#[test]
fn a_disconnect_applied_twice_acts_once() {
    let a = origin(1, 1);
    let (_, _, clients) = run(&[
        connect(a, "c", 0, true, false),
        Proposal::Disconnect { origin: a },
        Proposal::Disconnect { origin: a },
    ]);
    assert!(clients.contains(&ClientEffect::Disconnected {
        to: a,
        attached: true,
        lsn: at(1)
    }));
    assert!(clients.contains(&ClientEffect::Disconnected {
        to: a,
        attached: false,
        lsn: at(2)
    }));
}

#[test]
fn a_full_tombstone_queue_raises_the_floor_and_the_floor_fences() {
    let limits = (
        Limits::default(),
        ConnectLimits {
            max_connect_tombstones: 1,
            ..ConnectLimits::default()
        },
    );
    let (a, b, c) = (origin(1, 1), origin(1, 2), origin(2, 1));
    let (state, _, clients) = run_with(
        limits,
        &[
            connect(a, "x", 0, true, false),
            Proposal::Disconnect { origin: a },
            connect(b, "y", at(1), true, false),
            Proposal::Disconnect { origin: b },
            // Seen x's Connect, not its removal at lsn 4, which only the
            // floor now remembers.
            connect(c, "x", at(0), true, false),
        ],
    );
    assert_eq!(state.connect_fence(), (at(1), 1));
    assert!(clients.contains(&ClientEffect::ConnectRefused {
        to: c,
        reason: Refusal::Stale,
        lsn: at(4)
    }));
}

#[test]
fn tombstones_older_than_the_window_are_pruned_without_raising_the_floor() {
    let limits = (
        Limits::default(),
        ConnectLimits {
            connect_window: 4,
            ..ConnectLimits::default()
        },
    );
    let (a, b, c) = (origin(1, 1), origin(1, 2), origin(2, 1));
    let (state, _, _) = run_with(
        limits,
        &[
            connect(a, "x", 0, true, false),
            Proposal::Disconnect { origin: a },
            connect(b, "y", at(1), true, false),
            connect(c, "z", at(2), true, false),
            // At lsn 10 the horizon is 6: the tombstone at 4 goes.
            connect(origin(2, 2), "w", at(3), true, false),
        ],
    );
    assert_eq!(state.connect_fence(), (0, 0));
}

#[test]
fn session_gone_and_node_reset_detach_and_keep_a_persisted_session() {
    let (a, b, c) = (origin(1, 1), origin(1, 2), origin(2, 1));
    let (state, _, _) = run(&[
        connect(a, "p", 0, true, true),
        connect(b, "q", at(0), true, false),
        Proposal::SessionGone { origin: a },
        connect(c, "r", at(2), true, true),
        Proposal::NodeReset { node: 2, below: 5 },
    ]);
    let p = state.client(b"p").unwrap();
    assert_eq!((p.attached, p.persist), (None, true));
    assert_eq!(attached(&state, "q"), Some(b));
    assert_eq!(state.client(b"r").unwrap().attached, None);
    assert_eq!(state.persisted().0, 2);
}

#[test]
fn an_attached_persisted_session_that_overflows_is_closed_and_deleted() {
    let limits = (
        Limits {
            max_inflight: 1,
            max_backlog: 0,
            ..Limits::default()
        },
        ConnectLimits::default(),
    );
    let (a, p) = (origin(1, 1), origin(3, 1));
    let (state, effects, _) = run_with(
        limits,
        &[
            connect(a, "c", 0, true, true),
            subscribe(a, "t", G::AtLeastOnce),
            connect(p, "p", at(1), true, false),
            publish(p, "t", b"1", G::AtLeastOnce),
            publish(p, "t", b"2", G::AtLeastOnce),
        ],
    );
    assert!(effects.contains(&Effect::Close { to: a, lsn: at(4) }));
    assert_eq!(state.client(b"c"), None);
    assert_eq!(state.connect_fence().1, 1);
}

#[test]
fn a_snapshot_round_trips_client_ids_tombstones_and_detached_sessions() {
    let (a, b, p) = (origin(1, 1), origin(2, 1), origin(3, 1));
    let proposals = [
        connect(a, "c", 0, true, true),
        subscribe(a, "t", G::AtLeastOnce),
        Proposal::Disconnect { origin: a },
        connect(b, "d", at(2), true, false),
        Proposal::Disconnect { origin: b },
        connect(p, "p", at(4), true, false),
        publish(p, "t", b"1", G::AtLeastOnce),
    ];
    let (state, _, _) = run(&proposals);
    let mut bytes = Vec::new();
    state.encode(&mut bytes);
    assert_eq!(bytes[0], 2, "step 7c writes version 2");
    let decoded =
        State::decode_connect(Limits::default(), ConnectLimits::default(), &bytes).unwrap();
    assert!(
        State::decode(Limits::default(), &bytes).is_err(),
        "a node on State::new never takes a version 2 snapshot"
    );
    assert_eq!(decoded, state);
    assert_eq!(decoded.digest(), state.digest());

    // Without step 7c the snapshot is version 1, as before.
    let mut old = Vec::new();
    State::new(Limits::default()).encode(&mut old);
    assert_eq!(old[0], 1);
    assert!(State::decode(Limits::default(), &old).is_ok());
}

// --- Invariants A and B, and recovery, over random logs. ---

const CLIENTS: [&str; 3] = ["a", "b", "c"];

#[derive(Debug, Clone)]
enum Op {
    Connect {
        origin: Origin,
        client: usize,
        behind: u64,
        clean_start: bool,
        persist: bool,
    },
    Disconnect(Origin),
    Gone(Origin),
    Reset(u8),
    Subscribe(Origin, bool),
    Publish(Origin, bool),
    Ack(Origin, u64),
}

fn op() -> impl Strategy<Value = Op> {
    let o = (1u8..=2, 1u64..=4).prop_map(|(node, session)| Origin { node, session });
    prop_oneof![
        4 => (o.clone(), 0usize..3, 1u64..14, any::<bool>(), any::<bool>()).prop_map(
            |(origin, client, behind, clean_start, persist)| Op::Connect {
                origin,
                client,
                behind,
                clean_start,
                persist
            }
        ),
        2 => o.clone().prop_map(Op::Disconnect),
        1 => o.clone().prop_map(Op::Gone),
        1 => (1u8..=2).prop_map(Op::Reset),
        2 => (o.clone(), any::<bool>()).prop_map(|(o, q)| Op::Subscribe(o, q)),
        3 => (o.clone(), any::<bool>()).prop_map(|(o, q)| Op::Publish(o, q)),
        1 => (o, 0u64..3).prop_map(|(o, d)| Op::Ack(o, d)),
    ]
}

fn guarantee(at_least_once: bool) -> G {
    if at_least_once {
        G::AtLeastOnce
    } else {
        G::AtMostOnce
    }
}

/// The proposals of `ops`, each `Connect` with `seen` `behind` positions
/// before its own lsn (saturating at 0).
fn proposals(ops: &[Op]) -> Vec<Proposal> {
    ops.iter()
        .enumerate()
        .map(|(i, op)| match op {
            Op::Connect {
                origin,
                client,
                behind,
                clean_start,
                persist,
            } => connect(
                *origin,
                CLIENTS[*client],
                at(i as u64).saturating_sub(*behind),
                *clean_start,
                *persist,
            ),
            Op::Disconnect(o) => Proposal::Disconnect { origin: *o },
            Op::Gone(o) => Proposal::SessionGone { origin: *o },
            // Every session of the node: the simulated gateways restart.
            Op::Reset(node) => Proposal::NodeReset {
                node: *node,
                below: 100,
            },
            Op::Subscribe(o, q) => subscribe(*o, "t", guarantee(*q)),
            Op::Publish(o, q) => publish(*o, "t", b"m", guarantee(*q)),
            Op::Ack(o, d) => Proposal::Ack {
                origin: *o,
                delivery: *d,
            },
        })
        .collect()
}

fn small() -> (Limits, ConnectLimits) {
    (
        Limits {
            max_inflight: 1,
            max_backlog: 2,
            max_backlog_bytes: 2,
            max_total_queued_bytes: 6,
            ..Limits::default()
        },
        ConnectLimits {
            connect_window: 8,
            max_connect_tombstones: 2,
            max_persisted_sessions: 2,
            max_detached_queued_bytes: 2,
        },
    )
}

/// Invariants A and B, checked after every record from the effects and
/// from which entries exist, never from the fence's own bookkeeping.
#[derive(Default)]
struct Oracle {
    /// A: per client id, the origin of the last `Connect` that passed,
    /// until that origin was detached or its session deleted.
    expected: BTreeMap<&'static str, Origin>,
    /// B: per client id, the lsn of its last fence event: an applied
    /// `Connect`, or the removal of its entry.
    fenced: BTreeMap<&'static str, u64>,
}

fn name(client_id: &ClientId) -> &'static str {
    CLIENTS
        .iter()
        .find(|c| c.as_bytes() == client_id.as_bytes())
        .unwrap()
}

impl Oracle {
    fn check(
        &mut self,
        before: [bool; 3],
        state: &State,
        proposal: &Proposal,
        lsn: u64,
        effects: &[Effect],
        clients: &[ClientEffect],
    ) {
        for effect in clients {
            if let ClientEffect::Connected { to, .. } = effect {
                let Proposal::Connect {
                    client_id, seen, ..
                } = proposal
                else {
                    panic!("Connected from {proposal:?}");
                };
                let name = name(client_id);
                let last = self.fenced.get(name).copied().unwrap_or(0);
                assert!(
                    last <= *seen,
                    "invariant B: {name} changed at lsn {last}, after its Connect's seen {seen} (lsn {lsn})"
                );
                self.fenced.insert(name, lsn);
                self.expected.insert(name, *to);
            }
        }
        for effect in effects {
            if let Effect::Close { to, .. } = effect {
                self.expected.retain(|_, o| o != to);
            }
        }
        let detached = |o: &Origin| match proposal {
            Proposal::Disconnect { origin } | Proposal::SessionGone { origin } => origin == o,
            Proposal::NodeReset { node, below } => o.node == *node && o.session < *below,
            _ => false,
        };
        self.expected.retain(|_, o| !detached(o));
        for (i, name) in CLIENTS.iter().enumerate() {
            if state.client(name.as_bytes()).is_none() {
                self.expected.remove(name);
                if before[i] {
                    self.fenced.insert(name, lsn);
                }
            }
        }
        // A: the attachment is what the oracle expects, and `origins` is
        // its exact inverse.
        for name in CLIENTS {
            let actual = state.client(name.as_bytes()).and_then(|c| c.attached);
            assert_eq!(
                actual,
                self.expected.get(name).copied(),
                "invariant A for {name} at lsn {lsn}"
            );
            if let Some(o) = actual {
                assert_eq!(state.attached_to(o), Some(name.as_bytes()));
            }
        }
    }
}

fn present(state: &State) -> [bool; 3] {
    CLIENTS.map(|c| state.client(c.as_bytes()).is_some())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn invariants_a_and_b_hold_after_every_record(ops in proptest::collection::vec(op(), 1..80)) {
        let proposals = proposals(&ops);
        let records = log(&proposals);
        let mut state = State::with_connect(small().0, small().1);
        let mut oracle = Oracle::default();
        let (mut effects, mut clients) = (Vec::new(), Vec::new());
        for (batch, proposal) in records.chunks(2).zip(&proposals) {
            state.apply_connect(&decode(&batch[0]).unwrap(), &mut effects, &mut clients).unwrap();
            let record = decode(&batch[1]).unwrap();
            let before = present(&state);
            effects.clear();
            clients.clear();
            state.apply_connect(&record, &mut effects, &mut clients).unwrap();
            oracle.check(before, &state, proposal, record.lsn, &effects, &clients);
        }
    }

    /// A node that restarts from a snapshot taken anywhere in the log ends
    /// with the same state and the same effects as one that applied it all.
    #[test]
    fn a_snapshot_anywhere_recovers_the_same_state_and_effects(
        ops in proptest::collection::vec(op(), 1..80),
        cut in 0usize..80,
    ) {
        let proposals = proposals(&ops);
        let records = log(&proposals);
        let cut = (cut * 2).min(records.len());
        let mut whole = State::with_connect(small().0, small().1);
        let (mut whole_effects, mut whole_clients) = (Vec::new(), Vec::new());
        for record in &records {
            whole.apply_connect(&decode(record).unwrap(), &mut whole_effects, &mut whole_clients).unwrap();
        }
        let mut first = State::with_connect(small().0, small().1);
        let (mut effects, mut clients) = (Vec::new(), Vec::new());
        for record in &records[..cut] {
            first.apply_connect(&decode(record).unwrap(), &mut effects, &mut clients).unwrap();
        }
        let mut bytes = Vec::new();
        first.encode(&mut bytes);
        let mut restarted = State::decode_connect(small().0, small().1, &bytes).unwrap();
        prop_assert_eq!(&restarted, &first);
        for record in &records[cut..] {
            restarted.apply_connect(&decode(record).unwrap(), &mut effects, &mut clients).unwrap();
        }
        prop_assert_eq!(&restarted, &whole);
        prop_assert_eq!(restarted.digest(), whole.digest());
        prop_assert_eq!(effects, whole_effects);
        prop_assert_eq!(clients, whole_clients);
    }
}
