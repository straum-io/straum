//! The quorum watermark: what it is, and what it must never do.

use replication::Quorum;

/// The example the design was written against: the `required`-th largest
/// position, counting this node.
#[test]
fn the_watermark_is_the_position_a_quorum_holds() {
    let mut quorum = Quorum::new(3, 3);

    // Each step, and what the third copy sits at after it.
    assert_eq!(quorum.on_local(120), None, "one copy is not three");
    assert_eq!(quorum.on_peer(0, 117), None, "two are not three either");
    // 120, 117, 115 — the third copy has arrived.
    assert_eq!(quorum.on_peer(1, 115), Some(115));
    // The slowest peer joining below the watermark moves nothing.
    assert_eq!(quorum.on_peer(2, 90), None);

    assert_eq!(quorum.watermark(), 115);
}

#[test]
fn it_is_not_a_maximum_and_not_an_average() {
    let mut quorum = Quorum::new(3, 3);
    quorum.on_local(120);
    quorum.on_peer(0, 117);
    quorum.on_peer(1, 115);
    quorum.on_peer(2, 90);

    assert_ne!(quorum.watermark(), 120, "a maximum");
    assert_ne!(quorum.watermark(), 90, "the slowest");
    assert_ne!(quorum.watermark(), (120 + 117 + 115 + 90) / 4, "an average");
}

#[test]
fn it_never_exceeds_what_this_node_holds() {
    let mut quorum = Quorum::new(2, 2);
    quorum.on_local(100);
    // A peer claiming more than the leader ever wrote cannot lift the
    // position: the leader never sent it, so it cannot be durable there.
    quorum.on_peer(0, 9_000);
    assert_eq!(quorum.watermark(), 100);
}

/// The case that matters most. A quorum was reached, a replica went away,
/// and the position it helped establish stays established: an
/// acknowledgement cannot be taken back.
#[test]
fn a_replica_going_away_never_lowers_what_was_already_declared() {
    let mut quorum = Quorum::new(2, 3);
    quorum.on_local(100);
    quorum.on_peer(0, 100);
    quorum.on_peer(1, 100);
    assert_eq!(quorum.watermark(), 100);

    quorum.on_disconnected(1);
    // The leader keeps going and the remaining peer follows.
    assert_eq!(quorum.on_local(105), None, "the watermark moved on its own");
    assert_eq!(quorum.on_peer(0, 103), None);

    assert_eq!(
        quorum.watermark(),
        100,
        "a position that was declared durable was withdrawn"
    );
}

/// A disconnected peer's bytes are still on its disk. The link being down
/// does not un-write them, so its last known position keeps counting.
#[test]
fn a_disconnected_peer_still_counts_for_what_it_wrote() {
    let mut quorum = Quorum::new(2, 2);
    quorum.on_local(50);
    quorum.on_peer(0, 50);
    quorum.on_disconnected(0);

    // Two copies at 50 — the second one is offline, not erased.
    assert_eq!(quorum.watermark(), 50);
    // But nothing new can reach a quorum through it.
    assert!(!quorum.reachable());
}

#[test]
fn reachability_is_about_now_and_the_watermark_is_about_what_happened() {
    let mut quorum = Quorum::new(2, 2);
    assert!(!quorum.reachable(), "nobody is connected yet");

    quorum.on_local(10);
    quorum.on_peer(0, 10);
    assert!(quorum.reachable());
    assert_eq!(quorum.watermark(), 10);

    quorum.on_disconnected(0);
    assert!(!quorum.reachable(), "the only other copy is gone");
    assert_eq!(quorum.watermark(), 10, "what it wrote is still written");

    // And it comes back.
    quorum.on_peer(0, 10);
    assert!(quorum.reachable());
}

#[test]
fn one_copy_required_is_this_node_alone() {
    let mut quorum = Quorum::new(0, 1);
    assert!(quorum.reachable());
    assert_eq!(quorum.on_local(77), Some(77));
    assert_eq!(quorum.watermark(), 77);
}

#[test]
fn more_copies_than_exist_is_never_durable() {
    let mut quorum = Quorum::new(1, 3);
    quorum.on_local(100);
    assert_eq!(quorum.on_peer(0, 100), None);
    assert_eq!(quorum.watermark(), 0);
    assert!(!quorum.reachable());
}

#[test]
fn a_repeated_report_advances_nothing() {
    let mut quorum = Quorum::new(1, 2);
    quorum.on_local(40);
    assert_eq!(quorum.on_peer(0, 40), Some(40));
    assert_eq!(quorum.on_peer(0, 40), None, "the same report moved it");
    assert_eq!(quorum.on_peer(0, 30), None, "a stale report moved it");
    assert_eq!(quorum.watermark(), 40);
}

/// Each advance is reported once, so a caller emits one event per real
/// move rather than one per commit.
#[test]
fn only_a_real_advance_is_reported() {
    let mut quorum = Quorum::new(1, 2);
    assert_eq!(quorum.on_local(10), None, "no peer has it yet");
    assert_eq!(quorum.on_peer(0, 10), Some(10));
    assert_eq!(quorum.on_local(20), None, "the peer is still at 10");
    assert_eq!(quorum.on_peer(0, 20), Some(20));
}

#[test]
fn an_unknown_peer_is_ignored_rather_than_believed() {
    let mut quorum = Quorum::new(1, 2);
    quorum.on_local(100);
    assert_eq!(quorum.on_peer(9, 100), None);
    assert_eq!(quorum.watermark(), 0);
}

/// Four replicas, three required: the straggler is outvoted rather than
/// holding the position down to its own.
#[test]
fn a_straggler_does_not_drag_a_quorum_down_to_itself() {
    let mut quorum = Quorum::new(3, 3);
    quorum.on_local(900);
    quorum.on_peer(0, 900);
    quorum.on_peer(1, 900);
    quorum.on_peer(2, 0);
    assert_eq!(quorum.watermark(), 900);

    let mut slower = Quorum::new(3, 3);
    slower.on_local(900);
    slower.on_peer(0, 900);
    slower.on_peer(1, 500);
    slower.on_peer(2, 0);
    assert_eq!(slower.watermark(), 500, "the third copy is at 500");
}

/// The same positions, a different requirement: asking for every copy
/// moves the watermark down to the slowest.
#[test]
fn requiring_every_copy_waits_for_the_slowest() {
    let mut two = Quorum::new(2, 2);
    two.on_local(200);
    two.on_peer(0, 140);
    two.on_peer(1, 60);
    assert_eq!(two.watermark(), 140);

    let mut all = Quorum::new(2, 3);
    all.on_local(200);
    all.on_peer(0, 140);
    all.on_peer(1, 60);
    assert_eq!(all.watermark(), 60);
}
