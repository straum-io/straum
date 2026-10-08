//! The sequencer and the replica, driven by hand: one rule per test.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use cluster::{ClusterError, Config, Effect, Message, Proposal, Replica, Sequencer};
use journal::{Chain, Kind};

fn config(id: u8) -> Config {
    Config {
        id,
        nodes: vec![0, 1, 2],
        owner: 0,
        slot: 1,
        window: 100,
        max_queue: 8,
        max_batch: 4,
        recent_batches: 2,
        max_uncommitted: 1_000_000,
        takeover_ticks: 3,
    }
}

fn publish(body: &[u8]) -> Proposal {
    Proposal {
        kind: Kind::Publish,
        properties: Vec::new(),
        body: body.to_vec(),
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

fn appends(effects: &[Effect]) -> Vec<Vec<Vec<u8>>> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Append { records, .. } => Some(records.clone()),
            _ => None,
        })
        .collect()
}

/// A sequencer with both replicas known at position 0.
fn started() -> Sequencer {
    let mut s = Sequencer::new(config(0), Chain::genesis());
    for id in [1, 2] {
        s.on_message(
            id,
            Message::Ack {
                slot: 1,
                last_slot: 1,
                end: 0,
                durable: 0,
                cut: false,
            },
        );
    }
    s.take_effects();
    s
}

fn batch(s: &mut Sequencer, body: &[u8]) -> u64 {
    s.propose(publish(body)).unwrap();
    s.flush().unwrap();
    s.end()
}

#[test]
fn nothing_is_shipped_before_it_is_durable_on_the_sequencer() {
    let mut s = started();
    let end = batch(&mut s, b"a");
    let effects = s.take_effects();
    assert_eq!(appends(&effects).len(), 1);
    assert!(sends(&effects).is_empty(), "shipped before the local sync");
    s.on_durable(end);
    let shipped = sends(&s.take_effects());
    assert_eq!(shipped.len(), 2, "one batch to each replica");
}

#[test]
fn commit_needs_a_majority_durable_and_only_grows() {
    let mut s = started();
    let end = batch(&mut s, b"a");
    s.on_durable(end);
    s.take_effects();
    assert_eq!(s.commit(), 0, "one durable copy of three is not a commit");
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end,
            durable: end,
            cut: false,
        },
    );
    assert_eq!(s.commit(), end);
    assert!(s.take_effects().contains(&Effect::Committed(end)));
    // An old report arriving late does not move it back.
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end: 0,
            durable: 0,
            cut: false,
        },
    );
    assert_eq!(s.commit(), end);
}

#[test]
fn a_replica_takes_the_owners_batches_in_order_and_reports_its_position() {
    let mut s = started();
    let mut r = Replica::new(config(1), Chain::genesis());
    let first = batch(&mut s, b"a");
    let second = batch(&mut s, b"b");
    s.on_durable(second);
    let shipped = sends(&s.take_effects());
    let to_r: Vec<Message> = shipped
        .into_iter()
        .filter(|(to, _)| *to == 1)
        .map(|(_, m)| m)
        .collect();
    assert_eq!(to_r.len(), 2);

    // Out of order: the second batch first is not taken; the Ack says where
    // this log ends.
    r.on_message(0, to_r[1].clone());
    let effects = r.take_effects();
    assert!(appends(&effects).is_empty());
    assert_eq!(
        sends(&effects),
        vec![(
            0,
            Message::Ack {
                slot: 1,
                last_slot: 0,
                end: 0,
                durable: 0,
                cut: false,
            }
        )]
    );

    r.on_message(0, to_r[0].clone());
    r.on_message(0, to_r[1].clone());
    assert_eq!(appends(&r.take_effects()).len(), 2);
    assert_eq!(r.end(), second);
    r.on_durable(first);
    assert_eq!(
        sends(&r.take_effects()),
        vec![(
            0,
            Message::Ack {
                slot: 1,
                last_slot: 1,
                end: second,
                durable: first,
                cut: false,
            }
        )]
    );
}

#[test]
fn a_replica_refuses_another_node_another_slot_and_a_broken_chain() {
    let mut s = started();
    let end = batch(&mut s, b"a");
    s.on_durable(end);
    let Message::Append {
        records, commit, ..
    } = sends(&s.take_effects())[0].1.clone()
    else {
        panic!("an append")
    };
    let mut r = Replica::new(config(1), Chain::genesis());

    r.on_message(
        2,
        Message::Append {
            slot: 1,
            prev: 0,
            prev_slot: 0,
            records: records.clone(),
            commit,
        },
    );
    r.on_message(
        0,
        Message::Append {
            slot: 2,
            prev: 0,
            prev_slot: 0,
            records: records.clone(),
            commit,
        },
    );
    let mut forged = records.clone();
    forged[1][20] ^= 1; // a byte of the record's prev_hash
    r.on_message(
        0,
        Message::Append {
            slot: 1,
            prev: 0,
            prev_slot: 0,
            records: forged,
            commit,
        },
    );
    // Half a batch is not a position.
    r.on_message(
        0,
        Message::Append {
            slot: 1,
            prev: 0,
            prev_slot: 0,
            records: records[..1].to_vec(),
            commit,
        },
    );
    assert_eq!(r.refused(), 4);
    assert_eq!(r.end(), 0);
    assert!(appends(&r.take_effects()).is_empty());

    r.on_message(
        0,
        Message::Append {
            slot: 1,
            prev: 0,
            prev_slot: 0,
            records,
            commit,
        },
    );
    assert_eq!(r.end(), end);
}

#[test]
fn a_replica_knows_the_commit_only_as_far_as_its_own_log() {
    let mut r = Replica::new(config(1), Chain::genesis());
    r.on_message(
        0,
        Message::Append {
            slot: 1,
            prev: 0,
            prev_slot: 0,
            records: Vec::new(),
            commit: 50,
        },
    );
    assert_eq!(r.commit(), 0);
}

#[test]
fn a_replica_behind_memory_is_caught_up_from_the_log_and_lost_below_it() {
    let mut s = started();
    let mut end = 0;
    for body in [b"a", b"b", b"c", b"d"] {
        end = batch(&mut s, body);
    }
    // Only the last two batches are in memory (recent_batches = 2), so a
    // replica at 0 is caught up from the log, one batch per read.
    s.on_durable(end);
    let effects = s.take_effects();
    assert!(
        effects.contains(&Effect::Read { to: 2, from: 1 }),
        "{effects:?}"
    );

    // Reading it back: header and one record, lsn 1 and 2.
    let mut chain = Chain::genesis();
    let mut records = Vec::new();
    chain
        .batch(
            1,
            &[journal::Entry {
                kind: Kind::Publish,
                properties: &[],
                body: b"a",
            }],
            &mut records,
        )
        .unwrap();
    s.on_read(2, records);
    let effects = s.take_effects();
    assert!(
        effects.contains(&Effect::Read { to: 2, from: 3 }),
        "{effects:?}"
    );

    s.on_read_missing(2, 3);
    assert!(s.take_effects().contains(&Effect::Lost { to: 2 }));
    s.tick();
    assert!(
        sends(&s.take_effects()).iter().all(|(to, _)| *to != 2),
        "a lost replica is not shipped to"
    );
}

#[test]
fn a_replica_is_sent_no_more_than_its_window_beyond_its_durable() {
    let mut cfg = config(0);
    cfg.window = 2;
    cfg.recent_batches = 8;
    let mut s = Sequencer::new(cfg, Chain::genesis());
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end: 0,
            durable: 0,
            cut: false,
        },
    );
    let mut end = 0;
    for body in [b"a", b"b", b"c"] {
        end = batch(&mut s, body);
    }
    s.on_durable(end);
    let to_1 = sends(&s.take_effects())
        .into_iter()
        .filter(|(to, _)| *to == 1)
        .count();
    assert_eq!(to_1, 1, "a batch is two lsns: one fills the window");
}

#[test]
fn proposals_beyond_the_queue_are_refused_not_dropped() {
    let mut s = started();
    for _ in 0..8 {
        s.propose(publish(b"x")).unwrap();
    }
    assert_eq!(s.propose(publish(b"x")), Err(ClusterError::Full));
    s.flush().unwrap();
    assert!(s.propose(publish(b"x")).is_ok());
}

#[test]
fn a_restarted_sequencer_ships_nothing_until_a_replica_reports() {
    let mut chain = Chain::genesis();
    chain
        .batch(
            1,
            &[journal::Entry {
                kind: Kind::Publish,
                properties: &[],
                body: b"a",
            }],
            &mut Vec::new(),
        )
        .unwrap();
    let mut s = Sequencer::new(config(0), chain);
    assert_eq!(s.durable(), 2);
    s.on_durable(2);
    assert!(sends(&s.take_effects()).is_empty());
    s.tick();
    assert_eq!(
        sends(&s.take_effects()).len(),
        2,
        "heartbeats ask for positions"
    );
}

/// Reports never start a resend: with reordering, each stale batch draws a
/// report, and resending a window per report grew the traffic without bound
/// (found by the simulation). Going back is the heartbeat's.
#[test]
fn only_the_heartbeat_sends_again_what_a_replica_did_not_report() {
    let mut s = started();
    let end = batch(&mut s, b"a");
    s.on_durable(end);
    s.take_effects();
    for _ in 0..5 {
        s.on_message(
            1,
            Message::Ack {
                slot: 1,
                last_slot: 1,
                end: 0,
                durable: 0,
                cut: false,
            },
        );
    }
    let resent = sends(&s.take_effects())
        .into_iter()
        .filter(|(to, _)| *to == 1)
        .count();
    assert_eq!(resent, 0, "a stale report resent");
    s.tick();
    let to_1: Vec<Message> = sends(&s.take_effects())
        .into_iter()
        .filter(|(to, _)| *to == 1)
        .map(|(_, m)| m)
        .collect();
    assert!(
        to_1.iter()
            .any(|m| matches!(m, Message::Append { records, .. } if !records.is_empty())),
        "the heartbeat goes back: {to_1:?}"
    );
}

/// D-57 A: compact through the commit point but not past the slowest
/// replica, unless the byte bound says the log may not keep that much.
#[test]
fn the_compaction_floor_waits_for_the_slowest_replica_up_to_the_byte_bound() {
    let mut s = started();
    let mut end = 0;
    for body in [b"a", b"b", b"c", b"d"] {
        end = batch(&mut s, body);
    }
    s.on_durable(end);
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end,
            durable: end,
            cut: false,
        },
    );
    s.on_message(
        2,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end: 2,
            durable: 2,
            cut: false,
        },
    );
    assert_eq!(s.commit(), end);
    assert_eq!(
        s.compaction_floor(0),
        2,
        "the slowest replica holds the floor"
    );
    assert_eq!(s.compaction_floor(5), 5, "the byte bound moves it past");
    assert_eq!(
        s.compaction_floor(u64::MAX),
        end,
        "never past the commit point"
    );

    // A sequencer that has not heard from a replica keeps everything the
    // bound allows.
    let fresh = Sequencer::new(config(0), Chain::genesis());
    assert_eq!(fresh.compaction_floor(0), 0);
}

/// A late report that says less than the replica already reported durable
/// does not send the heartbeat back past that durable point: the records
/// there may be compacted, and the replica would be lost for nothing
/// (found by the simulation).
#[test]
fn the_heartbeat_never_goes_back_below_what_a_replica_reported_durable() {
    let mut cfg = config(0);
    cfg.recent_batches = 1;
    let mut s = Sequencer::new(cfg, Chain::genesis());
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end: 0,
            durable: 0,
            cut: false,
        },
    );
    let mut end = 0;
    for body in [b"a", b"b", b"c"] {
        end = batch(&mut s, body);
    }
    s.on_durable(end);
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end,
            durable: end,
            cut: false,
        },
    );
    s.take_effects();
    // The stale report arrives last.
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end: 2,
            durable: 2,
            cut: false,
        },
    );
    s.tick();
    let effects = s.take_effects();
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::Read { to: 1, .. })),
        "went back to the log for what replica 1 holds: {effects:?}"
    );
}

/// A read the replica's position overtook while it waited does not lose the
/// replica when the log no longer has what it asked for (found by the
/// simulation).
#[test]
fn a_stale_read_that_misses_does_not_lose_the_replica() {
    let mut cfg = config(0);
    cfg.recent_batches = 1;
    let mut s = Sequencer::new(cfg, Chain::genesis());
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end: 0,
            durable: 0,
            cut: false,
        },
    );
    let mut end = 0;
    for body in [b"a", b"b", b"c"] {
        end = batch(&mut s, body);
    }
    s.on_durable(end);
    assert!(s.take_effects().contains(&Effect::Read { to: 1, from: 1 }));
    // The replica turns out to hold everything already.
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end,
            durable: end,
            cut: false,
        },
    );
    s.take_effects();
    s.on_read_missing(1, 1);
    assert!(
        !s.take_effects()
            .iter()
            .any(|e| matches!(e, Effect::Lost { .. }))
    );
    assert_eq!(s.replica_durable(1), Some(end));
}

/// The uncommitted tail is bounded: at `max_uncommitted` lsns beyond the
/// commit point the sequencer makes no batch; proposals wait in the queue.
#[test]
fn no_batch_beyond_the_uncommitted_bound() {
    let mut cfg = config(0);
    cfg.max_uncommitted = 3;
    cfg.max_batch = 1;
    let mut s = Sequencer::new(cfg, Chain::genesis());
    for _ in 0..3 {
        s.propose(publish(b"x")).unwrap();
        s.flush().unwrap();
    }
    // Two batches of two lsns each; the third waits for a commit.
    assert_eq!(s.end(), 4);
    s.on_durable(4);
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end: 4,
            durable: 4,
            cut: false,
        },
    );
    s.flush().unwrap();
    assert_eq!(s.end(), 6, "room again once committed");
}
