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
        lag_bound: None,
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
                asked_slot: if asked == 0 { 0 } else { 1 },
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
            asked_slot: 1,
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

/// Delivers every effect at once: appends and promises durable, messages
/// sent. Returns the messages sent, in order.
fn pump(voters: &mut [Voter]) -> Vec<(u8, u8, Message)> {
    let mut sent = Vec::new();
    loop {
        let mut quiet = true;
        for index in 0..voters.len() {
            for effect in voters[index].take_effects() {
                quiet = false;
                let from = voters[index].id();
                match effect {
                    Effect::Append { records, .. } => {
                        let last = decode(records.last().unwrap()).unwrap().lsn;
                        voters[index].on_durable(last);
                    }
                    Effect::Promise { slot } => voters[index].on_promised(slot),
                    Effect::Send { to, message } => {
                        sent.push((from, to, message.clone()));
                        voters[usize::from(to)].on_message(from, message);
                    }
                    _ => {}
                }
            }
        }
        if quiet {
            return sent;
        }
    }
}

fn cluster() -> Vec<Voter> {
    let mut voters: Vec<Voter> = (0..3)
        .map(|id| Voter::new(config(id), Chain::genesis(), Vec::new(), 0))
        .collect();
    voters[0].tick();
    pump(&mut voters);
    assert!(voters[0].leading());
    // Heartbeats until every log holds the slot's start, committed.
    for _ in 0..2 {
        voters[0].tick();
        pump(&mut voters);
    }
    assert!(voters.iter().all(|v| v.end() == 2 && v.commit() == 2));
    voters
}

fn publish(body: &[u8]) -> cluster::Proposal {
    cluster::Proposal {
        kind: Kind::Publish,
        properties: Vec::new(),
        body: body.to_vec(),
    }
}

#[test]
fn a_handoff_ends_the_slot_and_the_successor_starts_the_next_without_a_promise_round() {
    let mut voters = cluster();
    voters[0].propose(publish(b"a")).unwrap();
    voters[0].hand_off().unwrap();
    assert_eq!(
        voters[0].propose(publish(b"b")),
        Err(cluster::ClusterError::NotLeader),
        "a sealed slot took a proposal"
    );
    // Queued before the seal: written, then `SlotEnd`.
    voters[0].flush().unwrap();
    pump(&mut voters);
    // The heartbeat that carries the commit past `SlotEnd`.
    for v in voters.iter_mut() {
        v.tick();
    }
    let sent = pump(&mut voters);
    assert!(!voters[0].leading(), "the old sequencer did not step down");
    assert!(voters[1].leading(), "the successor did not take over");
    assert_eq!(voters[1].slot(), 2);
    assert!(
        !sent
            .iter()
            .any(|(_, _, m)| matches!(m, Message::Promise { .. })),
        "a handoff asked for promises: {sent:?}"
    );
    // The new slot commits, and its log is the old one plus `SlotStart`:
    // each batch a header and one record — publish at 4, `SlotEnd` at 6,
    // `SlotStart` at 8.
    for _ in 0..2 {
        voters[1].tick();
        pump(&mut voters);
    }
    assert_eq!(voters[1].commit(), 8);
    assert!(voters.iter().all(|v| v.slot() == 2 && v.end() == 8));
}

#[test]
fn a_successor_takes_over_only_a_committed_slot_end() {
    let mut voters = cluster();
    voters[0].hand_off().unwrap();
    let records: Vec<Vec<u8>> = voters[0]
        .take_effects()
        .into_iter()
        .find_map(|e| match e {
            Effect::Append { records, .. } => Some(records),
            _ => None,
        })
        .unwrap();
    let end = decode(&records[1])
        .map(|r| (r.kind, r.slot, r.lsn))
        .unwrap();
    assert_eq!((end.0, end.1), (Kind::SlotEnd, 1));
    let end = end.2;
    // Shipped, not committed: the successor holds it and waits.
    voters[1].on_message(
        0,
        Message::Append {
            slot: 1,
            prev: 2,
            prev_slot: 1,
            records,
            commit: 2,
        },
    );
    voters[1].on_durable(end);
    let effects = voters[1].take_effects();
    assert!(
        !effects.iter().any(|e| matches!(e, Effect::Promise { .. })),
        "took over on an uncommitted SlotEnd"
    );
    voters[1].on_message(
        0,
        Message::Append {
            slot: 1,
            prev: end,
            prev_slot: 1,
            records: Vec::new(),
            commit: end,
        },
    );
    assert!(
        voters[1]
            .take_effects()
            .contains(&Effect::Promise { slot: 2 })
    );
}

#[test]
fn only_a_sequencer_with_another_owner_next_can_hand_off() {
    let mut voters = cluster();
    assert_eq!(voters[0].hand_off(), Ok(()));
    let mut alone = Voter::new(
        Config {
            nodes: vec![0],
            ..config(0)
        },
        Chain::genesis(),
        Vec::new(),
        0,
    );
    alone.tick();
    alone.on_promised(1);
    assert!(alone.leading());
    assert_eq!(alone.hand_off(), Err(cluster::ClusterError::NoSuccessor));
    assert_eq!(voters[1].hand_off(), Err(cluster::ClusterError::NotLeader));
}

/// The slot table forgets what compaction removed, but keeps the slot that
/// holds the floor (D-79 A).
#[test]
fn compaction_prunes_the_slot_table_to_the_floor() {
    let table = vec![(1, 1), (2, 10), (3, 20), (4, 30)];
    let pruned = |floor| {
        let mut slots = table.clone();
        cluster::prune_slots(&mut slots, floor);
        slots
    };
    assert_eq!(pruned(0), table);
    assert_eq!(pruned(9), table);
    assert_eq!(pruned(10), vec![(2, 10), (3, 20), (4, 30)]);
    assert_eq!(pruned(25), vec![(3, 20), (4, 30)]);
    // The last slot always stays: the log's end is in it.
    assert_eq!(pruned(1000), vec![(4, 30)]);
    assert_eq!(
        cluster::merge_slots(&pruned(25), &[(3, 26), (4, 30), (5, 40)]),
        vec![(3, 20), (4, 30), (5, 40)]
    );
}

/// Ticks until the voter asks to promise a slot of its own.
fn ticks_to_takeover(v: &mut Voter) -> (u32, u64) {
    for tick in 1..1000 {
        v.tick();
        if let Some(slot) = v.take_effects().iter().find_map(|e| match e {
            Effect::Promise { slot } => Some(*slot),
            _ => None,
        }) {
            return (tick, slot);
        }
    }
    panic!("no takeover in 1000 ticks");
}

/// A takeover that does not end in a commit makes the next one wait twice
/// as long: a fixed wait shorter than a takeover's round trips had each
/// candidate passed over before it finished, for ever (D-85, cluster
/// simulation seed 4). Time still only starts a takeover.
#[test]
fn a_takeover_that_does_not_commit_doubles_the_wait_for_the_next() {
    // Node 1 has promised slot 1, whose owner (0) is silent.
    let mut v = Voter::new(config(1), Chain::genesis(), Vec::new(), 1);
    // Its turn is slot 2, after `takeover_ticks` (3) of silence.
    assert_eq!(ticks_to_takeover(&mut v), (3, 2));
    v.on_promised(2);
    // Node 2 promises too: a majority answered, and the takeover goes on to
    // fetch, but never commits. Its next turn is slot 5, three turns on: 9
    // ticks at the first wait, 18 at the doubled one.
    v.on_message(
        2,
        Message::Promised {
            slot: 2,
            last_slot: 1,
            end: 0,
        },
    );
    v.take_effects();
    assert_eq!(v.patience(), 2);
    assert_eq!(ticks_to_takeover(&mut v), (18, 5));
}

/// A takeover no majority answers — a partition — leaves the wait as it
/// was: patience grown there kept nodes waiting up to 64× after the network
/// healed (D-88 A).
#[test]
fn a_takeover_no_majority_answers_leaves_the_wait_as_it_was() {
    let mut v = Voter::new(config(1), Chain::genesis(), Vec::new(), 1);
    // Cut off: turn after turn, its own promise only.
    let mut slot = 1;
    for _ in 0..10 {
        let (ticks, next) = ticks_to_takeover(&mut v);
        assert_eq!(u64::from(ticks), 3 * (next - slot), "slot {next}");
        v.on_promised(next);
        v.take_effects();
        assert_eq!(v.patience(), 1);
        slot = next;
    }
}

/// A read is matched by whom it is for and where it starts: the answer to
/// a sequencer's catch-up read reaches the sequencer even while a fetch
/// from the same replica, from before this node led, waits for another
/// position (D-85, cluster simulation seed 1384).
#[test]
fn a_read_answer_reaches_the_sequencer_past_an_older_fetch_from_the_same_replica() {
    let mut voters = cluster();
    // Node 0's log grows past lsn 5.
    for body in [b"a", b"b", b"c"] {
        voters[0].propose(publish(body)).unwrap();
        voters[0].flush().unwrap();
        pump(&mut voters);
    }
    assert!(voters[0].end() >= 8);
    // A fetch from replica 2 that node 0 serves from its log, from lsn 5:
    // held as being served while the read is out.
    voters[0].on_message(
        2,
        Message::Fetch {
            slot: 1,
            last_slot: 1,
            end: 4,
        },
    );
    let held: Vec<Effect> = voters[0].take_effects();
    assert!(
        held.iter()
            .any(|e| matches!(e, Effect::Read { to: 2, from: 5, .. })),
        "not held: {held:?}"
    );
    // Whatever it asked, an answer from lsn 1 is not that fetch's: as the
    // sequencer's catch-up read it is shipped, not answered as `Fetched`.
    let mut chain = Chain::genesis();
    let records = batch(&mut chain, 1, b"x");
    voters[0].on_read(2, records);
    let sent = sends(&voters[0].take_effects());
    assert!(
        !sent
            .iter()
            .any(|(to, m)| *to == 2 && matches!(m, Message::Fetched { .. })),
        "an answer from lsn 1 went to a fetch from lsn 5: {sent:?} (held {held:?})"
    );
}

/// A fetch for a slot older than the one promised is not served: it comes
/// from a candidate passed over since, and serving it handed records not
/// yet durable here to a replica first (D-85, invariant 6, cluster
/// simulation seed 4629).
#[test]
fn a_fetch_for_an_older_slot_than_promised_is_not_served() {
    // A log of one batch, lsns 1–2, having promised slot 3.
    let mut chain = Chain::genesis();
    batch(&mut chain, 1, b"a");
    let mut v = Voter::new(config(1), chain, Vec::new(), 3);
    v.take_effects();
    v.on_message(
        2,
        Message::Fetch {
            slot: 2,
            last_slot: 0,
            end: 0,
        },
    );
    assert!(
        v.take_effects().is_empty(),
        "a fetch for slot 2 was served after promising 3"
    );
    v.on_message(
        2,
        Message::Fetch {
            slot: 3,
            last_slot: 0,
            end: 0,
        },
    );
    assert!(
        !v.take_effects().is_empty(),
        "the current slot's fetch was not served"
    );
}

/// A sequencer whose `SlotEnd` committed keeps heartbeating while its
/// successor does not hold `SlotEnd` yet, for at most `takeover_ticks`
/// heartbeats, then steps down anyway (D-85, takeover simulation seed
/// 4905).
#[test]
fn a_handoff_waits_a_bounded_while_for_the_successor() {
    let mut voters = cluster();
    voters[0].hand_off().unwrap();
    // Node 2 holds `SlotEnd` and commits it with node 0; node 1, the
    // successor, hears nothing.
    let records: Vec<Vec<u8>> = voters[0]
        .take_effects()
        .into_iter()
        .find_map(|e| match e {
            Effect::Append { records, .. } => Some(records),
            _ => None,
        })
        .unwrap();
    let end = decode(records.last().unwrap()).unwrap().lsn;
    voters[0].on_durable(end);
    voters[0].on_message(
        2,
        Message::Ack {
            slot: 1,
            last_slot: 1,
            end,
            durable: end,
            cut: false,
            report: 10,
        },
    );
    assert!(voters[0].commit() >= end);
    voters[0].take_effects();
    // Three heartbeats still lead; the fourth gives up waiting.
    for tick in 1..=3 {
        voters[0].tick();
        assert!(voters[0].leading(), "stepped down at heartbeat {tick}");
    }
    voters[0].tick();
    assert!(!voters[0].leading());
}

/// Voters with their logs: what `pump` does, plus reads served from the log
/// each voter appended, and links a test can cut. No cut of a log is
/// expected in the scenarios that use it.
struct Net {
    voters: Vec<Voter>,
    logs: Vec<std::collections::BTreeMap<u64, Vec<u8>>>,
    down: Vec<bool>,
}

impl Net {
    fn start() -> Self {
        let mut net = Net {
            voters: (0..3)
                .map(|id| Voter::new(config(id), Chain::genesis(), Vec::new(), 0))
                .collect(),
            logs: vec![std::collections::BTreeMap::new(); 3],
            down: vec![false; 3],
        };
        for _ in 0..3 {
            net.voters[0].tick();
            net.run(&mut |_, _, _| true);
        }
        assert!(net.voters.iter().all(|v| v.commit() == 2));
        net
    }

    /// Until quiet; a message `deliver` refuses is lost.
    fn run(&mut self, deliver: &mut dyn FnMut(u8, u8, &Message) -> bool) {
        // Bounded: a broken rule must fail the test, not hang it.
        for _ in 0..100_000 {
            let mut quiet = true;
            for i in 0..self.voters.len() {
                let effects = self.voters[i].take_effects();
                if self.down[i] {
                    continue;
                }
                for effect in effects {
                    quiet = false;
                    let from = self.voters[i].id();
                    match effect {
                        Effect::Append { records, .. } => {
                            let mut last = 0;
                            for r in records {
                                last = decode(&r).unwrap().lsn;
                                self.logs[i].insert(last, r);
                            }
                            self.voters[i].on_durable(last);
                        }
                        Effect::Promise { slot } => self.voters[i].on_promised(slot),
                        Effect::Send { to, message } => {
                            let to_index = usize::from(to);
                            if !self.down[to_index] && deliver(from, to, &message) {
                                self.voters[to_index].on_message(from, message);
                            }
                        }
                        Effect::Read { to, from, upto } => {
                            let mut read = cluster::ReadBatches::new(from, upto, 1 << 20);
                            for record in self.logs[i].range(from..).map(|(_, r)| r) {
                                if read.done() {
                                    break;
                                }
                                read.feed(record);
                            }
                            match read.finish() {
                                Some(records) => self.voters[i].on_read(to, records),
                                None => self.voters[i].on_read_missing(to, from),
                            }
                        }
                        Effect::Truncate { .. } => panic!("node {from} cut its log"),
                        _ => {}
                    }
                }
            }
            if quiet {
                return;
            }
        }
        panic!("the voters never went quiet");
    }

    fn leader(&self) -> Option<usize> {
        (0..3).find(|&i| !self.down[i] && self.voters[i].leading())
    }
}

/// A takeover while a replica is being caught up (D-95 A): the new slot is
/// built on the longest log of the majority that promised — never on the
/// stale prefix the replica holds — every record committed before stays
/// where it was on every node, the new slot commits only once the replica
/// holds its `SlotStart`, and the old sequencer's late batch is refused by
/// both: it commits nothing past the fence.
#[test]
fn a_takeover_during_a_catch_up_builds_on_no_stale_prefix_and_keeps_the_fence() {
    // Node 1 holds the whole committed log; node 2, the stale prefix,
    // must catch up from node 1 before its slot can start.
    for taker in [1, 2] {
        takeover_during_a_catch_up(taker);
    }
}

fn takeover_during_a_catch_up(taker: usize) {
    let mut net = Net::start();
    // Node 2 is cut off while slot 1 commits three batches with node 1.
    let cut = |from: u8, to: u8, _: &Message| from != 2 && to != 2;
    for body in [b"a", b"b", b"c"] {
        net.voters[0].propose(publish(body)).unwrap();
        net.voters[0].flush().unwrap();
        net.run(&mut cut.clone());
    }
    net.voters[0].tick();
    net.run(&mut cut.clone());
    let committed = net.voters[0].commit();
    assert!(committed > 2 && net.voters[1].commit() == committed);
    let before = net.logs[0].clone();
    // Node 2 comes back; one batch of its catch-up arrives, then node 0
    // dies.
    let mut shipped = 0;
    for _ in 0..20 {
        net.voters[0].tick();
        net.run(&mut |from, to, m| {
            if let (0, 2, Message::Append { records, .. }) = (from, to, m) {
                if !records.is_empty() {
                    shipped += 1;
                    return shipped == 1;
                }
            }
            true
        });
        if shipped > 0 {
            break;
        }
    }
    let partway = net.voters[2].end();
    assert!(partway > 2 && partway < committed, "node 2 at {partway}");
    net.down[0] = true;
    // Only `taker` counts the silence: it takes the next slot over.
    let mut taken = false;
    for _ in 0..200 {
        net.voters[taker].tick();
        net.run(&mut |_, _, _| true);
        if net
            .leader()
            .is_some_and(|l| net.voters[l].slot() > 1 && net.voters[l].commit() > committed)
        {
            taken = true;
            break;
        }
    }
    assert!(taken, "taker {taker}: no later slot committed");
    let leader = net.leader().unwrap();
    assert_eq!(leader, taker, "the slot went to another node");
    // No stale prefix: what slot 1 committed is in place, byte for byte, on
    // both; the new slot starts right after it.
    for node in [1, 2] {
        for (lsn, record) in before.range(..=committed) {
            assert_eq!(
                net.logs[node].get(lsn),
                Some(record),
                "node {node} lsn {lsn}"
            );
        }
    }
    let start = net.logs[leader]
        .values()
        .map(|r| decode(r).unwrap())
        .find(|r| r.slot > 1)
        .unwrap();
    assert_eq!(
        start.lsn,
        committed + 1,
        "the new slot does not follow the committed log"
    );
    assert!(
        net.logs[2].contains_key(&start.lsn),
        "slot {} committed before node 2 held its start",
        start.slot
    );
    // The fence: node 0 comes back still believing it leads slot 1 and
    // ships a new batch; nodes 1 and 2 refuse it, and it commits nothing.
    net.down[0] = false;
    let refused = (net.voters[1].refused(), net.voters[2].refused());
    let logs = (net.logs[1].clone(), net.logs[2].clone());
    net.voters[0].propose(publish(b"late")).unwrap();
    net.voters[0].flush().unwrap();
    net.voters[0].tick();
    net.run(&mut |from, _, _| from == 0);
    assert_eq!(
        (net.logs[1].clone(), net.logs[2].clone()),
        logs,
        "a late slot-1 batch was taken"
    );
    assert!(net.voters[1].refused() > refused.0 && net.voters[2].refused() > refused.1);
    assert_eq!(
        net.voters[0].commit(),
        committed,
        "the old sequencer committed past the fence"
    );
}

/// The first slot above `from` that `owner` owns.
fn slot_of(owner: u8, from: u64) -> u64 {
    (from + 1..)
        .find(|&s| config(1).owner_of(s) == Some(owner))
        .unwrap()
}

/// D-98 A: an append from the owner of a newer slot counts as that
/// sequencer being alive while this node makes its promise durable — but
/// only as that: before the promise is durable nothing is appended,
/// acknowledged or committed. A slow disk made the promise slow, the node
/// counted silence meanwhile and started takeovers it could not finish.
#[test]
fn a_newer_slots_append_is_heard_but_takes_nothing_before_the_promise() {
    let mut v = Voter::new(config(1), Chain::genesis(), Vec::new(), 1);
    let newer = slot_of(0, 1);
    v.tick();
    v.tick();
    let mut chain = Chain::genesis();
    v.on_message(
        0,
        Message::Append {
            slot: newer,
            prev: 0,
            prev_slot: 0,
            records: batch(&mut chain, newer, b"a"),
            commit: 1,
        },
    );
    let effects = v.take_effects();
    assert!(
        effects
            .iter()
            .all(|e| matches!(e, Effect::Promise { slot } if *slot == newer)),
        "more than the promise before it is durable: {effects:?}"
    );
    assert!(effects.iter().any(|e| matches!(e, Effect::Promise { .. })));
    assert_eq!((v.end(), v.commit()), (0, 0));
    // Heard: two more heartbeats of silence are not yet the three a
    // takeover waits for.
    v.tick();
    v.tick();
    assert_eq!(
        v.taking(),
        None,
        "a takeover started though the sequencer was just heard"
    );
}

/// D-98 A, the other side: a sequencer heard once and then gone is still
/// taken over — being heard resets the silence, it does not stop counting.
#[test]
fn a_newer_slots_owner_heard_once_then_silent_is_taken_over() {
    let mut v = Voter::new(config(1), Chain::genesis(), Vec::new(), 1);
    let newer = slot_of(0, 1);
    v.on_message(
        0,
        Message::Append {
            slot: newer,
            prev: 0,
            prev_slot: 0,
            records: Vec::new(),
            commit: 0,
        },
    );
    v.on_promised(newer);
    v.take_effects();
    let (_, taken) = ticks_to_takeover(&mut v);
    assert!(taken > newer, "took slot {taken}, not past {newer}");
}

/// D-98 A, the state it changes: the sequencer of the newer slot is heard
/// once and dies while this node's promise of its slot is still pending —
/// a slow disk. Being heard reset silence once; it does not keep it reset:
/// once the promise lands, the node takes the slot after it over.
#[test]
fn a_newer_slots_owner_dying_while_the_promise_is_pending_is_taken_over() {
    let mut v = Voter::new(config(1), Chain::genesis(), Vec::new(), 1);
    let newer = slot_of(0, 1);
    v.on_message(
        0,
        Message::Append {
            slot: newer,
            prev: 0,
            prev_slot: 0,
            records: Vec::new(),
            commit: 0,
        },
    );
    v.take_effects();
    // The owner is gone; the promise is still being synced.
    for _ in 0..20 {
        v.tick();
    }
    v.take_effects();
    // Late, the promise lands.
    v.on_promised(newer);
    v.take_effects();
    let (_, taken) = ticks_to_takeover(&mut v);
    assert!(taken > newer, "took slot {taken}, not past {newer}");
}
