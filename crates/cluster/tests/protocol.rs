//! The sequencer and the replica, driven by hand: one rule per test.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use cluster::{ClusterError, Config, Effect, LagBound, Message, Proposal, Replica, Sequencer};
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
        lag_bound: None,
    }
}

fn publish(body: &[u8]) -> Proposal {
    Proposal {
        kind: Kind::Data,
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
                report: 0,
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
            report: 0,
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
            report: 0,
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
                report: 0,
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
                report: 0,
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
        effects
            .iter()
            .any(|e| matches!(e, Effect::Read { to: 2, from: 1, .. })),
        "{effects:?}"
    );

    // Reading it back: header and one record, lsn 1 and 2.
    let mut chain = Chain::genesis();
    let mut records = Vec::new();
    chain
        .batch(
            1,
            &[journal::Entry {
                kind: Kind::Data,
                properties: &[],
                body: b"a",
            }],
            &mut records,
        )
        .unwrap();
    s.on_read(2, records);
    let effects = s.take_effects();
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::Read { to: 2, from: 3, .. })),
        "{effects:?}"
    );

    s.on_read_missing(2, 3);
    assert!(s.take_effects().contains(&Effect::Lost { to: 2 }));
    s.tick();
    // It still hears the heartbeat, so it does not fall silent and take
    // a slot over; it is shipped no records.
    assert!(
        sends(&s.take_effects()).iter().all(|(to, m)| *to != 2
            || matches!(m, Message::Append { records, .. } if records.is_empty())),
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
            report: 0,
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
                kind: Kind::Data,
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
                report: 0,
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
            report: 0,
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
            report: 0,
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
            report: 0,
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
            report: 0,
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
            report: 0,
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
            report: 0,
        },
    );
    let mut end = 0;
    for body in [b"a", b"b", b"c"] {
        end = batch(&mut s, body);
    }
    s.on_durable(end);
    assert!(
        s.take_effects()
            .iter()
            .any(|e| matches!(e, Effect::Read { to: 1, from: 1, .. }))
    );
    // The replica turns out to hold everything already.
    s.on_message(
        1,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end,
            durable: end,
            cut: false,
            report: 0,
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
            report: 0,
        },
    );
    s.flush().unwrap();
    assert_eq!(s.end(), 6, "room again once committed");
}

/// A read hands back whole batches from `from`: the first always, more
/// while they end at or below `upto` and the bytes are under the bound,
/// and nothing without a whole batch at `from` (D-81 A).
#[test]
fn a_read_hands_back_whole_batches_within_its_bounds() {
    use journal::{Chain, Entry, Kind};
    let mut chain = Chain::genesis();
    let mut records = Vec::new();
    for body in [b"a", b"b", b"c"] {
        let entries = [
            Entry {
                kind: Kind::Data,
                properties: &[],
                body,
            },
            Entry {
                kind: Kind::Data,
                properties: &[],
                body,
            },
        ];
        chain.batch(1, &entries, &mut records).unwrap();
    }
    // Batches at 1..=3, 4..=6, 7..=9.
    let read = |from: u64, upto: u64, bound: usize| {
        let mut batches = cluster::ReadBatches::new(from, upto, bound);
        for record in &records {
            batches.feed(record);
        }
        batches.finish().map(|r| {
            r.iter()
                .map(|r| journal::decode(r).unwrap().lsn)
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(read(1, 100, 1 << 20), Some((1..=9).collect()));
    assert_eq!(read(1, 6, 1 << 20), Some((1..=6).collect()), "past upto");
    assert_eq!(
        read(1, 1, 1 << 20),
        Some((1..=3).collect()),
        "the first batch always"
    );
    assert_eq!(
        read(1, 100, 1),
        Some((1..=3).collect()),
        "past the byte bound"
    );
    // Two batches' worth of bytes, less one: never past the bound.
    let two: usize = records[..6].iter().map(Vec::len).sum();
    assert_eq!(read(1, 100, two - 1), Some((1..=3).collect()));
    assert_eq!(read(1, 100, two), Some((1..=6).collect()));
    assert_eq!(read(4, 100, 1 << 20), Some((4..=9).collect()));
    assert_eq!(read(5, 100, 1 << 20), None, "not a batch's start");
    // A batch cut short by the end of what was fed is dropped.
    let mut short = cluster::ReadBatches::new(1, 100, 1 << 20);
    for record in &records[..7] {
        short.feed(record);
    }
    assert_eq!(short.finish().map(|r| r.len()), Some(6));
}

/// An answer to a read other than the one outstanding is ignored: it
/// neither clears the read that counts nor asks again. Taking it queued one
/// more read behind the rest for every stale answer, and a catch-up moved
/// one batch in many reads (D-85, cluster simulation seed 160).
#[test]
fn a_stale_read_answer_neither_clears_the_read_nor_asks_again() {
    let mut s = started();
    let mut end = 0;
    for body in [b"a", b"b", b"c", b"d"] {
        end = batch(&mut s, body);
    }
    s.on_durable(end);
    let effects = s.take_effects();
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::Read { to: 2, from: 1, .. })),
        "{effects:?}"
    );
    // The log as written: batches at 1–2 and 3–4.
    let mut chain = Chain::genesis();
    let mut first = Vec::new();
    let mut second = Vec::new();
    for (body, out) in [(b"a", &mut first), (b"b", &mut second)] {
        chain
            .batch(
                1,
                &[journal::Entry {
                    kind: Kind::Data,
                    properties: &[],
                    body,
                }],
                out,
            )
            .unwrap();
    }
    // An answer for a read from 3, as from one given up earlier.
    s.on_read(2, second);
    let effects = s.take_effects();
    assert!(
        !effects.iter().any(|e| matches!(e, Effect::Read { .. })),
        "a stale answer asked again: {effects:?}"
    );
    // The read that counts still counts.
    s.on_read(2, first);
    let effects = s.take_effects();
    assert!(
        sends(&effects)
            .iter()
            .any(|(to, m)| *to == 2 && matches!(m, Message::Append { prev: 0, .. })),
        "{effects:?}"
    );
}

fn ack(s: &mut Sequencer, id: u8, at: u64) {
    s.on_message(
        id,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end: at,
            durable: at,
            cut: false,
            report: 0,
        },
    );
}

/// Flow control (D-89 A2, ADR-0008 R1–R4): an attached replica more than
/// `L` behind the commit point holds new batches — never the commit of
/// what is appended — for at most `T` heartbeats, after which it is
/// detached; caught up again, it attaches again.
#[test]
fn flow_control_holds_new_batches_at_most_t_heartbeats_and_never_commit() {
    let mut cfg = config(0);
    cfg.max_uncommitted = 4;
    cfg.max_batch = 1;
    // Raised to `max_uncommitted + max_batch` = 5.
    cfg.lag_bound = Some(LagBound {
        records: 0,
        ticks: 2,
    });
    let mut s = Sequencer::new(cfg, Chain::genesis());
    for id in [1, 2] {
        ack(&mut s, id, 0);
    }
    // Both replicas catch up to the first commit and attach.
    let end = batch(&mut s, b"a");
    s.on_durable(end);
    ack(&mut s, 1, end);
    ack(&mut s, 2, end);
    let fell_behind = s.commit();
    assert_eq!(s.attached(), 2);
    // Replica 2 goes silent; replica 1 and the sequencer commit on.
    while s.commit() - fell_behind <= 5 {
        assert!(!s.holding_back());
        let end = batch(&mut s, b"b");
        s.on_durable(end);
        ack(&mut s, 1, end);
    }
    // Nothing queued, nothing held: an idle sequencer detaches no one.
    assert!(s.commit() - fell_behind > 5);
    assert!(!s.holding_back());
    for _ in 0..3 {
        s.tick();
    }
    assert_eq!(s.attached(), 2);
    // A proposal waits: held.
    let before = s.end();
    s.propose(publish(b"c")).unwrap();
    assert!(
        s.holding_back(),
        "replica 2 is {} behind",
        s.commit() - fell_behind
    );
    s.flush().unwrap();
    assert_eq!(s.end(), before, "a new batch made while held");
    s.tick();
    assert!(s.holding_back());
    // The `T`-th heartbeat detaches it (2, 3); the batch goes out.
    s.tick();
    assert!(!s.holding_back());
    assert_eq!(s.attached(), 1);
    s.flush().unwrap();
    assert!(s.end() > before);
    // Heard from again but still behind, it stays detached: attaching it
    // would start the hold again at once (R2, Lemma 2).
    ack(&mut s, 2, fell_behind);
    assert_eq!(s.attached(), 1);
    assert!(!s.holding_back());
    // Back and caught up to the commit point, it attaches again (6).
    let end = s.end();
    s.on_durable(end);
    ack(&mut s, 1, end);
    ack(&mut s, 2, end);
    assert_eq!(s.attached(), 2);
}

/// Commit is never held (D-89 (1)): during a hold, what is appended
/// commits once a majority holds it.
#[test]
fn a_hold_does_not_hold_the_commit_of_what_is_appended() {
    let mut cfg = config(0);
    cfg.max_uncommitted = 4;
    cfg.max_batch = 1;
    cfg.lag_bound = Some(LagBound {
        records: 0,
        ticks: 100,
    });
    let mut s = Sequencer::new(cfg, Chain::genesis());
    for id in [1, 2] {
        ack(&mut s, id, 0);
    }
    let end = batch(&mut s, b"a");
    s.on_durable(end);
    ack(&mut s, 1, end);
    ack(&mut s, 2, end);
    let fell_behind = s.commit();
    // Replica 1 acknowledges everything but the last batch.
    let mut last = end;
    loop {
        let end = batch(&mut s, b"b");
        if s.holding_back() {
            break;
        }
        s.on_durable(end);
        ack(&mut s, 1, last);
        last = end;
    }
    assert!(s.commit() < s.end(), "nothing appended is uncommitted");
    let end = s.end();
    ack(&mut s, 1, end);
    assert_eq!(
        s.commit(),
        end,
        "the hold kept a majority's record from committing"
    );
    assert!(s.commit() - fell_behind > 5);
}

/// The mechanism behind the cluster simulation's long cycles (D-90 B):
/// without an uncommitted bound, a sequencer that hears no replica keeps
/// appending, and the next slot commits only once a replica holds
/// everything up to that slot's first record — the whole tail. With the
/// bound, the tail stops at it.
#[test]
fn without_an_uncommitted_bound_a_new_slot_must_first_cover_the_whole_tail() {
    let mut cfg = config(0);
    cfg.max_queue = 1;
    let mut s = Sequencer::new(cfg.clone(), Chain::genesis());
    for _ in 0..500 {
        batch(&mut s, b"t");
        s.on_durable(s.end());
    }
    let tail = s.end();
    assert!(tail >= 500 && s.commit() == 0, "a tail of {tail}");
    // With the bound, the same sequencer stops at it.
    let mut bounded = cfg.clone();
    bounded.max_uncommitted = 16;
    let mut b = Sequencer::new(bounded, Chain::genesis());
    for _ in 0..500 {
        b.propose(publish(b"t")).ok();
        b.flush().unwrap();
        b.on_durable(b.end());
    }
    assert!(b.end() <= 16 + 2, "a bounded tail of {}", b.end());
    // The next slot starts after the tail; a replica that holds all of it
    // but that slot's first record counts for nothing yet.
    let mut next = cfg;
    next.slot = 2;
    let mut s2 = Sequencer::with(next, s.chain(), tail, 0, vec![(1, 1)]);
    batch(&mut s2, b"slot 2");
    s2.on_durable(s2.end());
    let report = |s2: &mut Sequencer, last_slot, at| {
        s2.on_message(
            1,
            Message::Ack {
                slot: 2,
                last_slot,
                end: at,
                durable: at,
                cut: false,
                report: 0,
            },
        );
    };
    report(&mut s2, 1, tail);
    assert_eq!(s2.commit(), 0, "committed before the replica held slot 2");
    let end = s2.end();
    report(&mut s2, 2, end);
    assert_eq!(s2.commit(), end);
}

/// R4 counts held heartbeats since the last batch, across holds: a replica
/// that catches up part of the way ends a hold, and the commit point can
/// pass it again with no batch made. Counted per hold, the wait would grow
/// past `T` (D-89 A2; found by the review).
#[test]
fn flow_control_counts_t_across_holds_until_a_batch() {
    let mut cfg = config(0);
    cfg.max_uncommitted = 4;
    cfg.max_batch = 1;
    // `L` raised to 5.
    cfg.lag_bound = Some(LagBound {
        records: 0,
        ticks: 2,
    });
    let mut s = Sequencer::new(cfg, Chain::genesis());
    for id in [1, 2] {
        ack(&mut s, id, 0);
    }
    let end = batch(&mut s, b"a");
    s.on_durable(end);
    ack(&mut s, 1, end);
    ack(&mut s, 2, end);
    // Replica 2 goes silent; replica 1 stays one batch behind the end, so
    // what is appended is not all committed when the hold starts.
    let mut last = end;
    loop {
        let end = batch(&mut s, b"b");
        if s.holding_back() {
            break;
        }
        s.on_durable(end);
        ack(&mut s, 1, last);
        last = end;
    }
    assert!(s.commit() < s.end());
    s.tick();
    assert!(s.holding_back());
    // Replica 2 catches up part of the way: the hold ends.
    let partway = s.commit() - 5;
    ack(&mut s, 2, partway);
    assert!(!s.holding_back());
    s.tick();
    // The commit point passes it again: held again, no batch made since.
    let end = s.end();
    ack(&mut s, 1, end);
    assert!(s.holding_back());
    // The second held heartbeat since the last batch is the `T`-th.
    s.tick();
    assert!(!s.holding_back(), "held past T heartbeats without a batch");
}
