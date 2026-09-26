//! Positions, and the ways they must not move.

use replication::{Accepted, Follower, Peer, PeerError, Records, Refused, quorum_watermark};

/// The batch is opaque to the state machine, so its contents never matter
/// here — only the range the frame claims.
fn frame(previous: u64, last: u64) -> Records<'static> {
    Records {
        previous,
        last,
        batch: b"opaque",
    }
}

/// The follower's durable position after appending a frame and letting the
/// log finish the durability operation that covers it.
fn append_and_sync(follower: &mut Follower, previous: u64, last: u64) -> u64 {
    match follower.accept(frame(previous, last)) {
        Ok(Accepted::Append { through }) => {
            follower.on_durable(through);
        }
        Ok(Accepted::Acknowledge { .. }) => {}
        Err(refused) => panic!("{refused}"),
    }
    follower.durable()
}

#[test]
fn a_contiguous_stream_advances_and_a_repeat_changes_nothing() {
    let mut follower = Follower::resuming_from(1000);

    assert_eq!(append_and_sync(&mut follower, 1000, 1100), 1100);
    assert_eq!(append_and_sync(&mut follower, 1100, 1200), 1200);
    // The leader did not hear the last acknowledgement and sent it again.
    // Nothing is written twice and the position does not move.
    assert_eq!(append_and_sync(&mut follower, 1000, 1100), 1200);
    assert_eq!(append_and_sync(&mut follower, 1200, 1300), 1300);
}

#[test]
fn a_frame_that_leaves_a_gap_advances_nothing() {
    let mut follower = Follower::resuming_from(1000);

    assert_eq!(
        follower.accept(frame(1100, 1200)),
        Err(Refused::Gap {
            expected: 1000,
            found: 1100
        })
    );

    // Not written, not durable, and not acknowledged: a prefix with a hole in
    // it is not a prefix, and the leader has to continue from 1000.
    assert_eq!(follower.written(), 1000);
    assert_eq!(follower.durable(), 1000);
    assert_eq!(follower.on_durable(1200), None);
    assert_eq!(follower.durable(), 1000);
}

#[test]
fn a_frame_that_straddles_the_position_is_refused_rather_than_merged() {
    let mut follower = Follower::resuming_from(1000);
    assert!(follower.accept(frame(1000, 1100)).is_ok());

    // The leader re-batched: it wants to send 1001..1150 as one frame over a
    // follower that already wrote through 1100. Overlapping writes are how a
    // log gets corrupted, so this is the leader's problem to re-frame.
    assert_eq!(
        follower.accept(frame(1000, 1150)),
        Err(Refused::Straddles {
            expected: 1100,
            found: 1000,
            through: 1150
        })
    );
    assert_eq!(follower.written(), 1100);
}

#[test]
fn written_is_not_durable() {
    let mut follower = Follower::resuming_from(0);
    assert_eq!(
        follower.accept(frame(0, 50)),
        Ok(Accepted::Append { through: 50 })
    );

    // Handed to the log and nothing more. This is the gap a crash loses, and
    // acknowledging it would be the whole defect this design exists to avoid.
    assert_eq!(follower.written(), 50);
    assert_eq!(follower.durable(), 0);

    assert_eq!(follower.on_durable(50), Some(50));
    assert_eq!(follower.durable(), 50);
    // The same report again is not a second advance.
    assert_eq!(follower.on_durable(50), None);
}

#[test]
fn durability_is_clamped_to_what_was_written() {
    let mut follower = Follower::resuming_from(0);
    assert!(follower.accept(frame(0, 50)).is_ok());

    // A log reporting past what this follower handed it is talking about
    // records this follower never took.
    assert_eq!(follower.on_durable(9_000), Some(50));
    assert_eq!(follower.durable(), 50);
}

#[test]
fn a_repeated_frame_below_the_position_is_acknowledged_not_written() {
    let mut follower = Follower::resuming_from(1000);
    assert!(follower.accept(frame(1000, 1100)).is_ok());
    follower.on_durable(1100);

    assert_eq!(
        follower.accept(frame(1000, 1100)),
        Ok(Accepted::Acknowledge { durable: 1100 })
    );
    assert_eq!(follower.written(), 1100);
}

mod leader {
    use super::*;

    #[test]
    fn a_peer_advances_only_on_what_it_confirms() {
        let mut peer = Peer::starting_at(100);
        peer.on_sent(400);

        assert_eq!(peer.in_flight(), 300);
        assert_eq!(peer.on_durable(250), Ok(true));
        assert_eq!(peer.durable(), 250);
        assert_eq!(peer.in_flight(), 150);

        // A duplicate frame produces a duplicate acknowledgement. Harmless.
        assert_eq!(peer.on_durable(250), Ok(false));
        assert_eq!(peer.durable(), 250);
    }

    #[test]
    fn a_peer_cannot_claim_what_it_was_never_sent() {
        let mut peer = Peer::starting_at(100);
        peer.on_sent(200);

        assert_eq!(
            peer.on_durable(300),
            Err(PeerError::AheadOfWhatWasSent {
                claimed: 300,
                sent: 200
            })
        );
        assert_eq!(peer.durable(), 100);
    }

    #[test]
    fn a_peer_cannot_go_backwards() {
        let mut peer = Peer::starting_at(100);
        peer.on_sent(500);
        assert_eq!(peer.on_durable(400), Ok(true));

        // Durable does not become undurable. By the time this arrives the
        // leader may already have called 400 quorum-durable, and there is no
        // way to unsay it.
        assert_eq!(
            peer.on_durable(300),
            Err(PeerError::Regressed {
                claimed: 300,
                held: 400
            })
        );
        assert_eq!(peer.durable(), 400);
    }
}

mod quorum {
    use super::*;

    #[test]
    fn it_is_the_position_a_quorum_holds_not_the_highest_one_anybody_does() {
        // Three replicas at 200, 140 and 60. Two of them hold everything
        // through 140; a maximum would have said 200.
        assert_eq!(quorum_watermark(200, &[140, 60], 2), 140);
        assert_eq!(quorum_watermark(200, &[140, 60], 3), 60);
    }

    #[test]
    fn it_never_exceeds_what_this_node_holds() {
        // A peer claiming more than the leader ever had cannot lift the
        // position: the leader never sent it, so it cannot be durable there.
        assert_eq!(quorum_watermark(100, &[9_000, 9_000], 2), 100);
    }

    #[test]
    fn a_quorum_that_cannot_be_formed_holds_nothing() {
        assert_eq!(quorum_watermark(500, &[], 2), 0);
        assert_eq!(quorum_watermark(500, &[400], 3), 0);
    }

    #[test]
    fn alone_is_its_own_quorum() {
        assert_eq!(quorum_watermark(500, &[], 1), 500);
        assert_eq!(quorum_watermark(500, &[100], 1), 500);
    }

    #[test]
    fn a_peer_that_has_nothing_does_not_drag_the_position_down_below_a_quorum() {
        // Four replicas, three required. The straggler at 0 is outvoted.
        assert_eq!(quorum_watermark(900, &[900, 900, 0], 3), 900);
        assert_eq!(quorum_watermark(900, &[900, 500, 0], 3), 500);
    }
}

mod reconciliation {
    use super::*;
    use replication::{Incompatible, Reconciled};

    #[test]
    fn a_follower_that_kept_up_needs_nothing_resent() {
        let (peer, outcome) = Peer::connected(400, 400).unwrap();
        assert_eq!(outcome, Reconciled::InSync);
        assert_eq!(peer.durable(), 400);
        assert_eq!(peer.in_flight(), 0);
    }

    #[test]
    fn a_follower_that_fell_behind_is_resent_only_the_suffix() {
        let (peer, outcome) = Peer::connected(400, 900).unwrap();
        assert_eq!(outcome, Reconciled::Resend { from: 401 });
        // And the leader's idea of the peer starts from what the peer said,
        // not from what it remembered before the connection dropped.
        assert_eq!(peer.sent(), 400);
        assert_eq!(peer.durable(), 400);
    }

    #[test]
    fn a_follower_that_synced_but_never_reported_is_not_sent_it_twice() {
        // The awkward one: the follower completed its durability operation
        // and the connection died before the acknowledgement left. The leader
        // last heard 400.
        let mut before = Peer::starting_at(0);
        before.on_sent(900);
        assert_eq!(before.on_durable(400), Ok(true));

        // On reconnect the follower says what it actually holds, and that is
        // the answer that counts.
        let (after, outcome) = Peer::connected(900, 900).unwrap();
        assert_eq!(outcome, Reconciled::InSync);
        assert_eq!(after.durable(), 900);
    }

    #[test]
    fn a_follower_holding_more_than_this_log_wrote_is_refused() {
        // Two logs that are not the same history. Without an identity on the
        // log there is no way to tell that from one that went backwards, and
        // refusing is the answer that cannot lose data.
        assert_eq!(
            Peer::connected(900, 400),
            Err(Incompatible {
                follower: 900,
                leader: 400
            })
        );
    }

    #[test]
    fn a_fresh_follower_is_sent_everything() {
        let (peer, outcome) = Peer::connected(0, 250).unwrap();
        assert_eq!(outcome, Reconciled::Resend { from: 1 });
        assert_eq!(peer.durable(), 0);
    }

    #[test]
    fn two_empty_logs_agree() {
        assert_eq!(Peer::connected(0, 0).unwrap().1, Reconciled::InSync);
    }
}
