//! A leader and a follower, each on its own simulated disk, linked by the
//! production state machines and nothing else (D-28, stage 2).
//!
//! No socket, no task, no clock. The leader's log, the follower's log, the
//! leader's `Link` and the follower's `Follower` are the production code;
//! what connects them here is a direct hand-over of frames, which is what
//! the TCP adapter does once the bytes are framed. Cutting a frame at every
//! byte is the matrix's job, over a real socket; this file is about what
//! power loss does to the two logs.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::io::Write;
use std::path::{Path, PathBuf};

use replication::{Accepted, Advance, Follower, Link, Reconciled, Records};
use storage::simulated::SimDisk;
use storage::{Access, Disk, DiskFile};
use storage::{Retain, Wal, WalLimits, allocated_on, replay_addressable_on, replay_on};

fn path() -> PathBuf {
    PathBuf::from("/sim/node.wal")
}

struct Node {
    disk: SimDisk,
    wal: Wal<SimDisk>,
}

impl Node {
    fn new() -> Self {
        let disk = SimDisk::new();
        let wal = Wal::open_on(disk.clone(), &path(), WalLimits::default())
            .unwrap()
            .0;
        Self { disk, wal }
    }

    /// The machine loses power and the process restarts on what survived.
    fn power_loss(self) -> Self {
        let disk = self.disk;
        drop(self.wal);
        disk.power_loss(0);
        let wal = Wal::open_on(disk.clone(), &path(), WalLimits::default())
            .unwrap()
            .0;
        Self { disk, wal }
    }

    /// Commits `bodies` as one batch; `sync` false is `written`.
    fn commit(&mut self, bodies: &[&str], sync: bool) -> u64 {
        for body in bodies {
            self.wal.stage(body.as_bytes()).unwrap();
        }
        self.wal.commit_with(sync).unwrap().0.durable_sequence
    }

    fn bodies(&self) -> Vec<String> {
        let mut seen = Vec::new();
        replay_on(&self.disk, &path(), WalLimits::default(), &mut |payload| {
            seen.push(String::from_utf8_lossy(payload).into_owned())
        })
        .unwrap();
        seen
    }
}

/// What the TCP adapter does for a catch-up: reads the range `Link::behind`
/// names out of the leader's log and hands it over as one frame. Every
/// decision is `Link`'s; what is copied from the runtime here is the
/// reading and the framing, which is input and output. Returns what the
/// follower's report moved, or the floor the follower was refused at.
fn catch_up(
    leader: &Node,
    link: &mut Link,
    follower: &mut Node,
    state: &mut Follower,
) -> Result<Option<Advance>, u64> {
    let local = leader.wal.durable_sequence();
    let Some((from, until)) = link.behind(local) else {
        return Ok(None);
    };
    let mut batch = Vec::new();
    let mut refused = None;
    replay_addressable_on(
        &leader.disk,
        &path(),
        WalLimits::default(),
        &mut |floor| {
            let served = Link::serves(from, floor);
            if !served {
                refused = Some(floor);
            }
            served
        },
        &mut |sequence, framed| {
            if (from..=until).contains(&sequence) {
                batch.extend_from_slice(framed);
            }
        },
    )
    .unwrap();
    if let Some(floor) = refused {
        return Err(floor);
    }
    let frame = Records {
        previous: from - 1,
        last: until,
        batch: &batch,
    };
    link.on_sent(until);
    // The follower's side: the state machine decides, the log appends and
    // syncs, and only then is a position reported.
    match state.accept(frame).unwrap() {
        Accepted::Append { .. } => {
            follower.wal.stage_framed(&batch).unwrap();
            let durable = follower.wal.commit_with(true).unwrap().0.durable_sequence;
            match state.on_durable(durable) {
                Some(reported) => Ok(link.on_report(reported, local).unwrap()),
                None => Ok(None),
            }
        }
        Accepted::Acknowledge { .. } => Ok(None),
    }
}

/// S6, `quorum`: whatever the quorum acknowledged survives a power loss of
/// either node, because both had synced it before the watermark covered it.
#[test]
fn a_quorum_acknowledgement_survives_either_node_losing_power() {
    let mut leader = Node::new();
    let mut follower = Node::new();
    let mut state = Follower::resuming_from(0);
    let (mut link, _) = Link::connect(0, 0, 2).unwrap();
    link.begin(0);

    leader.commit(&["a", "b", "c"], true);
    let advance = catch_up(&leader, &mut link, &mut follower, &mut state)
        .unwrap()
        .unwrap();
    // What `Link` hands the broker as acknowledgeable.
    assert_eq!(advance.watermark, Some(3));

    // One more, written and synced on the leader, never shipped: not
    // acknowledged under `quorum`.
    leader.commit(&["d"], true);

    let follower = follower.power_loss();
    assert_eq!(follower.bodies(), ["a", "b", "c"]);
    let leader = leader.power_loss();
    assert_eq!(leader.bodies(), ["a", "b", "c", "d"]);
}

/// S6′ (D-29): `written` with a follower, and the leader loses power.
///
/// `written` acknowledges once a batch reaches the operating system, and
/// the leader's durable sequence counts it, so catch-up ships it. The
/// follower syncs it. The leader's power loss takes it from the leader, the
/// leader hands the same sequences to different records, and the two logs
/// disagree about what a sequence names. The handshake does refuse the
/// follower — it reports more than the leader holds — but only while it is
/// ahead; a leader that has since written past it would not notice.
#[test]
fn written_with_a_follower_diverges_when_the_leader_loses_power() {
    let mut leader = Node::new();
    let mut follower = Node::new();
    let mut state = Follower::resuming_from(0);
    let (mut link, _) = Link::connect(0, 0, 2).unwrap();
    link.begin(0);

    // `written`: no sync on the leader.
    leader.commit(&["a", "b", "c"], false);
    catch_up(&leader, &mut link, &mut follower, &mut state).unwrap();
    assert_eq!(
        follower.bodies(),
        ["a", "b", "c"],
        "the follower synced them"
    );

    let mut leader = leader.power_loss();
    assert!(
        leader.bodies().is_empty(),
        "the leader lost what it never synced"
    );

    // The leader carries on, reusing the sequences.
    leader.commit(&["x", "y"], false);
    // While the follower is ahead, the handshake refuses it.
    assert!(Link::connect(state.durable(), leader.wal.durable_sequence(), 2).is_err());

    // Once the leader has written past it, the handshake has nothing to go
    // on: positions agree as numbers and disagree as records.
    leader.commit(&["z", "w"], false);
    let (mut link, outcome) =
        Link::connect(state.durable(), leader.wal.durable_sequence(), 2).unwrap();
    assert_eq!(outcome, Reconciled::Resend { from: 4 });
    link.begin(leader.wal.durable_sequence());
    catch_up(&leader, &mut link, &mut follower, &mut state).unwrap();
    // And the follower now holds a history that never existed anywhere:
    // the old leader's first three records under the new leader's fourth.
    assert_eq!(leader.bodies(), ["x", "y", "z", "w"]);
    assert_eq!(follower.bodies(), ["a", "b", "c", "w"]);
}

/// The same with `fsynced`: what was shipped was synced on the leader first,
/// so a power loss takes nothing the follower holds, and no sequence is
/// handed out twice.
#[test]
fn fsynced_with_a_follower_does_not_diverge() {
    let mut leader = Node::new();
    let mut follower = Node::new();
    let mut state = Follower::resuming_from(0);
    let (mut link, _) = Link::connect(0, 0, 2).unwrap();
    link.begin(0);

    leader.commit(&["a", "b", "c"], true);
    catch_up(&leader, &mut link, &mut follower, &mut state).unwrap();
    let mut leader = leader.power_loss();
    assert_eq!(leader.bodies(), ["a", "b", "c"]);
    leader.commit(&["d"], true);

    let (mut link, outcome) =
        Link::connect(state.durable(), leader.wal.durable_sequence(), 2).unwrap();
    assert_eq!(outcome, Reconciled::Resend { from: 4 });
    link.begin(leader.wal.durable_sequence());
    catch_up(&leader, &mut link, &mut follower, &mut state).unwrap();
    assert_eq!(follower.bodies(), leader.bodies());
}

/// The rewrite the runtime makes under `quorum` for a follower at `p`: the
/// log stays readable from `p + 1`. The one line of policy this file copies
/// rather than runs; the runtime's `Retention` is crate-private.
fn rewrite_keeping_from(node: &mut Node, from: u64) -> u64 {
    node.wal
        .compact(0, Retain { from, bytes: 0 }, &mut |_| false)
        .unwrap()
        .floor
}

/// S7, `quorum`: the follower is away while the leader writes and rewrites
/// its log. Everything past the follower's own position stays readable, so
/// it is caught up when it returns and the quorum forms again (D-20).
#[test]
fn a_quorum_follower_away_across_rewrites_is_caught_up() {
    let mut leader = Node::new();
    let mut follower = Node::new();
    let mut state = Follower::resuming_from(0);
    let (mut link, _) = Link::connect(0, 0, 2).unwrap();
    link.begin(0);
    leader.commit(&["a", "b"], true);
    catch_up(&leader, &mut link, &mut follower, &mut state).unwrap();
    let away_at = state.durable();
    assert_eq!(away_at, 2);

    // Away: the leader writes and rewrites, keeping what the follower needs.
    for round in 0..3 {
        leader.commit(&["x", "y", "z"], true);
        let floor = rewrite_keeping_from(&mut leader, away_at + 1);
        assert_eq!(
            floor,
            away_at + 1,
            "round {round}: the floor passed the follower"
        );
    }
    // And the leader restarts on what it synced: the floor comes back from
    // the header, not from memory.
    let leader = leader.power_loss();
    assert_eq!(
        allocated_on(&leader.disk, &path())
            .unwrap()
            .retention_floor(),
        away_at + 1
    );

    let local = leader.wal.durable_sequence();
    let (mut link, outcome) = Link::connect(state.durable(), local, 2).unwrap();
    assert_eq!(outcome, Reconciled::Resend { from: 3 });
    link.begin(local);
    let advance = catch_up(&leader, &mut link, &mut follower, &mut state)
        .expect("the follower was refused")
        .unwrap();
    assert_eq!(advance.durable, local);
    assert_eq!(advance.watermark, Some(local));
}

/// S8: a follower further behind than what the log kept. The rewrite left
/// nothing it needs readable, `Link` refuses it at the floor, and nothing is
/// sent.
#[test]
fn a_follower_below_the_floor_is_refused() {
    let mut leader = Node::new();
    leader.commit(&["a", "b", "c", "d"], true);
    // Below `quorum` the runtime keeps a byte budget; nothing, here.
    let floor = rewrite_keeping_from(&mut leader, u64::MAX);
    assert_eq!(floor, 5);
    assert_eq!(
        allocated_on(&leader.disk, &path())
            .unwrap()
            .retention_floor(),
        5
    );
    leader.commit(&["e"], true);
    let local = leader.wal.durable_sequence();

    // At 0 it needs 1, which has no name any more.
    let mut follower = Node::new();
    let mut state = Follower::resuming_from(0);
    let (mut link, _) = Link::connect(0, local, 2).unwrap();
    assert_eq!(
        catch_up(&leader, &mut link, &mut follower, &mut state),
        Err(5)
    );
    assert_eq!(link.sent(), 0, "something was sent to a refused follower");
    assert!(follower.bodies().is_empty());

    // The operator gives it a fresh copy of the leader's log (D-23): it
    // reconnects holding everything, and what is written next reaches it.
    let copy = leader.disk.contents(&path()).unwrap();
    let disk = SimDisk::new();
    {
        let mut file = disk.open(&path(), Access::Replace).unwrap();
        file.write_all(&copy).unwrap();
        file.sync_all().unwrap();
        disk.sync_directory(Path::new("/sim")).unwrap();
    }
    let wal = Wal::open_on(disk.clone(), &path(), WalLimits::default())
        .unwrap()
        .0;
    let mut follower = Node { disk, wal };
    let mut state = Follower::resuming_from(follower.wal.durable_sequence());
    let (mut link, outcome) = Link::connect(state.durable(), local, 2).unwrap();
    assert_eq!(outcome, Reconciled::InSync);
    link.begin(local);
    leader.commit(&["f"], true);
    let advance = catch_up(&leader, &mut link, &mut follower, &mut state)
        .expect("a fresh copy was refused")
        .unwrap();
    assert_eq!(advance.durable, local + 1);
    assert_eq!(follower.bodies(), leader.bodies());
}
