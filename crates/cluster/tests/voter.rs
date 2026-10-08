//! The voter across slots (ADR-0008 step 5), one rule per test. The
//! takeover simulation (`sim/tests/takeover.rs`) drives the same code
//! through crashes and partitions; these pin each rule down alone.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use cluster::{Config, Effect, Message, Voter};
use journal::{Chain, Entry, Kind, decode};

fn config(id: u8) -> Config {
    Config {
        id,
        nodes: vec![0, 1, 2],
        owner: 0,
        slot: 1,
        window: 16,
        max_queue: 16,
        max_batch: 4,
        recent_batches: 4,
        max_uncommitted: 64,
        takeover_ticks: 3,
    }
}

fn sends(effects: &[Effect]) -> Vec<(u8, Message)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Send { to, message } => Some((*to, message.clone())),
            _ => None,
        })
        .collect()
}

/// One batch of one publish in `slot`, continuing `chain`.
fn batch(chain: &mut Chain, slot: u64, body: &[u8]) -> Vec<Vec<u8>> {
    let mut records = Vec::new();
    let entry = Entry {
        kind: Kind::Publish,
        properties: &[],
        body,
    };
    chain.batch(slot, &[entry], &mut records).unwrap();
    records
}

#[test]
fn a_promise_is_answered_only_once_it_is_durable() {
    let mut v = Voter::new(config(1), Chain::genesis(), Vec::new(), 0);
    v.on_message(2, Message::Promise { slot: 5 });
    let effects = v.take_effects();
    assert!(effects.contains(&Effect::Promise { slot: 5 }));
    assert!(
        sends(&effects).is_empty(),
        "answered before the promise was durable"
    );
    v.on_promised(5);
    assert_eq!(v.slot(), 5);
    assert_eq!(
        sends(&v.take_effects()),
        vec![(
            2,
            Message::Promised {
                slot: 5,
                last_slot: 0,
                end: 0
            }
        )]
    );
}

#[test]
fn after_a_promise_nothing_of_a_lower_slot_is_taken_or_acknowledged() {
    let mut chain = Chain::genesis();
    let records = batch(&mut chain, 1, b"a");
    let mut v = Voter::new(config(1), Chain::genesis(), Vec::new(), 0);
    v.on_message(2, Message::Promise { slot: 2 });
    v.on_promised(2);
    v.take_effects();
    v.on_message(
        0,
        Message::Append {
            slot: 1,
            prev: 0,
            prev_slot: 0,
            records,
            commit: 0,
        },
    );
    assert_eq!(v.refused(), 1);
    assert!(v.take_effects().is_empty(), "took or acknowledged slot 1");
    assert_eq!(v.end(), 0);
}

#[test]
fn the_first_owner_takes_the_first_slot_and_starts_it_with_slot_start() {
    let mut v = Voter::new(config(0), Chain::genesis(), Vec::new(), 0);
    v.tick();
    assert!(v.take_effects().contains(&Effect::Promise { slot: 1 }));
    v.on_promised(1);
    let asked: Vec<u8> = sends(&v.take_effects())
        .into_iter()
        .map(|(to, _)| to)
        .collect();
    assert_eq!(asked, vec![1, 2]);
    // Itself and one more: a majority of three.
    v.on_message(
        1,
        Message::Promised {
            slot: 1,
            last_slot: 0,
            end: 0,
        },
    );
    assert!(v.leading());
    let appended: Vec<Vec<Vec<u8>>> = v
        .take_effects()
        .into_iter()
        .filter_map(|e| match e {
            Effect::Append { records, .. } => Some(records),
            _ => None,
        })
        .collect();
    assert_eq!(appended.len(), 1);
    let start = decode(&appended[0][1]).unwrap();
    assert_eq!((start.kind, start.slot, start.lsn), (Kind::SlotStart, 1, 2));
}

#[test]
fn a_candidate_catches_up_from_the_longest_log_of_a_majority() {
    let mut chain = Chain::genesis();
    let first = batch(&mut chain, 1, b"a");
    let second = batch(&mut chain, 1, b"b");
    // Node 1 promised slot 1 and holds nothing; slot 2 is its turn.
    let mut v = Voter::new(config(1), Chain::genesis(), Vec::new(), 1);
    for _ in 0..3 {
        v.tick();
    }
    assert!(v.take_effects().contains(&Effect::Promise { slot: 2 }));
    v.on_promised(2);
    v.take_effects();
    v.on_message(
        0,
        Message::Promised {
            slot: 2,
            last_slot: 1,
            end: 4,
        },
    );
    assert_eq!(
        sends(&v.take_effects()),
        vec![(
            0,
            Message::Fetch {
                slot: 2,
                last_slot: 0,
                end: 0
            }
        )]
    );
    for (asked, records) in [(0, first), (2, second)] {
        v.on_message(
            0,
            Message::Fetched {
                slot: 2,
                asked,
                after: asked,
                after_slot: if asked == 0 { 0 } else { 1 },
                records,
            },
        );
    }
    assert_eq!(v.end(), 4);
    assert!(!v.leading(), "led before the source said there is no more");
    v.on_message(
        0,
        Message::Fetched {
            slot: 2,
            asked: 4,
            after: 4,
            after_slot: 1,
            records: Vec::new(),
        },
    );
    assert!(v.leading());
    let last = v
        .take_effects()
        .into_iter()
        .filter_map(|e| match e {
            Effect::Append { records, .. } => Some(records),
            _ => None,
        })
        .next_back()
        .unwrap();
    let start = decode(&last[1]).unwrap();
    assert_eq!((start.kind, start.slot, start.lsn), (Kind::SlotStart, 2, 6));
}

#[test]
fn a_follower_cuts_only_where_the_slots_prove_the_logs_differ() {
    // Node 2 holds two batches of slot 1 and has promised slot 2, whose
    // owner is node 1.
    let mut chain = Chain::genesis();
    let mut log = batch(&mut chain, 1, b"a");
    log.extend(batch(&mut chain, 1, b"b"));
    let slots = vec![(1, 1)];
    let mut v = Voter::new(config(2), chain, slots, 2);
    let append = |prev, prev_slot, records| Message::Append {
        slot: 2,
        prev,
        prev_slot,
        records,
        commit: 0,
    };

    // A stale append: the owner's log agrees here through lsn 2 and its
    // next record is slot 1's too — the same record. Nothing is cut.
    let mut same = Chain::genesis();
    batch(&mut same, 1, b"a");
    let next = batch(&mut same, 1, b"b");
    v.on_message(1, append(2, 1, next));
    assert!(
        !v.take_effects()
            .iter()
            .any(|e| matches!(e, Effect::Truncate { .. }))
    );

    // The owner's next record after lsn 2 is in slot 2: this log's is in
    // slot 1, a different record. Cut at 2.
    let mut other = Chain::genesis();
    batch(&mut other, 1, b"a");
    let start = batch(&mut other, 2, b"start");
    v.on_message(1, append(2, 1, start));
    assert!(v.take_effects().contains(&Effect::Truncate { after: 2 }));
}
