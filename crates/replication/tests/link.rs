//! The leader's link under arbitrary interleavings (D-28, stage 2).
//!
//! A follower that is a model — it holds exactly what it was sent, makes it
//! durable when the test says so and reports whatever it likes within what
//! it holds — against the real `Link`, driven by random commits, live
//! batches, drops from the live queue, catch-ups, reports and reconnects.
//! After every step the invariants that make a quorum acknowledgement true
//! are checked.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use proptest::prelude::*;
use replication::{Link, Live, Reconciled};

#[derive(Debug, Clone)]
enum Step {
    /// The leader's log commits a batch of this many records.
    Commit(u64),
    /// The next live batch arrives, or was dropped by the queue.
    Live { dropped: bool },
    /// The runtime catches up from the log.
    CatchUp,
    /// The follower syncs what it has received and reports.
    Report,
    /// The connection dies and the follower reconnects.
    Reconnect,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        (1u64..5).prop_map(Step::Commit),
        any::<bool>().prop_map(|dropped| Step::Live { dropped }),
        Just(Step::CatchUp),
        Just(Step::Report),
        Just(Step::Reconnect),
    ]
}

proptest! {
    #[test]
    fn the_follower_never_holds_what_the_leader_has_not_made_durable(
        steps in proptest::collection::vec(step(), 1..200)
    ) {
        // The leader's durable position, and the live batches committed but
        // not yet looked at by the link.
        let mut local = 0u64;
        let mut queued: Vec<(u64, u64)> = Vec::new();
        // What the follower holds as received, and as durable.
        let mut received = 0u64;
        let mut durable = 0u64;
        let (mut link, _) = Link::connect(0, 0, 2).unwrap();
        let mut watermark = 0u64;

        for step in steps {
            match step {
                Step::Commit(records) => {
                    queued.push((local, local + records));
                    local += records;
                }
                Step::Live { dropped } => {
                    if queued.is_empty() {
                        continue;
                    }
                    let (previous, last) = queued.remove(0);
                    if !dropped && link.live(previous, last) == Live::Send {
                        prop_assert_eq!(previous, received, "a live batch left a hole");
                        link.on_sent(last);
                        received = last;
                    }
                }
                Step::CatchUp => {
                    if let Some((from, until)) = link.behind(local) {
                        prop_assert_eq!(from, received + 1, "catch-up left a hole");
                        link.on_sent(until);
                        received = until;
                    }
                }
                Step::Report => {
                    durable = received;
                    if let Some(advance) = link.on_report(durable, local).unwrap() {
                        if let Some(reached) = advance.watermark {
                            prop_assert!(reached > watermark, "the watermark went back");
                            watermark = reached;
                        }
                    }
                }
                Step::Reconnect => {
                    // What it synced it keeps; what it only received it loses.
                    received = durable;
                    let (fresh, outcome) = Link::connect(durable, local, 2).unwrap();
                    link = fresh;
                    if let Some(reached) = link.begin(local) {
                        prop_assert!(reached >= watermark);
                        watermark = watermark.max(reached);
                    }
                    if durable < local {
                        prop_assert_eq!(outcome, Reconciled::Resend { from: durable + 1 });
                    }
                }
            }
            // The follower never holds a record before the leader has made
            // it durable (§112, D-22).
            prop_assert!(received <= local, "shipped {} with only {} durable", received, local);
            // A quorum of two is never ahead of either copy.
            prop_assert!(watermark <= local && watermark <= durable);
            prop_assert_eq!(link.sent(), received);
        }
    }
}
