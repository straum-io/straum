//! The log on a simulated disk (D-28, stage 1): scenarios S1–S3 of the
//! simulation, run against the production log code.
//!
//! Each test walks every injection point it names rather than sampling one,
//! and checks what the durability level promises — no more:
//!
//! * `fsynced`: a commit that returned is in the log after any crash,
//!   including a power loss with a torn tail (I3);
//! * `written`: a commit that returned survives the process, not the power
//!   (I2);
//! * after a failed write or sync nothing more is accepted (I8);
//! * a rewrite interrupted anywhere leaves a log that recovers with every
//!   number it ever handed out (I7, I10).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use storage::simulated::SimDisk;
use storage::{Retain, StorageError, Wal, WalLimits, allocated_on, replay_on};

fn path() -> PathBuf {
    PathBuf::from("/sim/log.wal")
}

fn record(index: u64) -> Vec<u8> {
    format!("record-{index}").into_bytes()
}

fn open(disk: &SimDisk) -> (Wal<SimDisk>, storage::Recovery) {
    Wal::open_on(disk.clone(), &path(), WalLimits::default()).unwrap()
}

/// The bodies the log on `disk` recovers, in order.
fn recovered(disk: &SimDisk) -> Vec<Vec<u8>> {
    let mut seen = Vec::new();
    replay_on(disk, &path(), WalLimits::default(), &mut |payload| {
        seen.push(payload.to_vec())
    })
    .unwrap();
    seen
}

/// Stages and commits records `from..from + count`.
fn commit(wal: &mut Wal<SimDisk>, from: u64, count: u64, sync: bool) -> u64 {
    for index in from..from + count {
        wal.stage(&record(index)).unwrap();
    }
    wal.commit_with(sync).unwrap().0.durable_sequence
}

/// Recovery is a prefix of what was written, never something else: no
/// record is invented, reordered or half-read out of a torn tail.
fn assert_prefix(seen: &[Vec<u8>]) {
    for (at, body) in seen.iter().enumerate() {
        assert_eq!(
            body,
            &record(at as u64),
            "record {at} is not what was written"
        );
    }
}

/// S1, `fsynced`: a power loss at any point after a commit returned, with
/// any torn tail of a commit still in flight, keeps every returned commit.
#[test]
fn a_returned_fsynced_commit_survives_any_power_loss() {
    // How large the in-flight batch is on disk, so every torn length can be
    // tried.
    let probe = SimDisk::new();
    let (mut wal, _) = open(&probe);
    commit(&mut wal, 0, 6, true);
    let before = probe.contents(&path()).unwrap().len();
    commit(&mut wal, 6, 3, false);
    let in_flight = probe.contents(&path()).unwrap().len() - before;

    for torn in 0..=in_flight {
        let disk = SimDisk::new();
        let (mut wal, _) = open(&disk);
        // Two acknowledged batches, then one written and not synced.
        assert_eq!(commit(&mut wal, 0, 3, true), 3);
        assert_eq!(commit(&mut wal, 3, 3, true), 6);
        commit(&mut wal, 6, 3, false);
        drop(wal);

        disk.power_loss(torn);
        let seen = recovered(&disk);
        assert!(seen.len() >= 6, "torn {torn}: an fsynced commit was lost");
        assert!(seen.len() <= 9, "torn {torn}: a record was invented");
        assert_prefix(&seen);

        // And the log continues after what it recovered, not after what it
        // had before: the numbers of the lost tail were never acknowledged.
        let (mut wal, recovery) = open(&disk);
        assert_eq!(recovery.sequence, seen.len() as u64);
        let next = seen.len() as u64;
        assert_eq!(commit(&mut wal, next, 1, true), next + 1);
    }
}

/// S1, `written`: a commit that returned without a sync survives the
/// process going away — the bytes are in the operating system — and is
/// promised nothing across a power loss, where only a prefix remains.
#[test]
fn a_returned_written_commit_survives_the_process_not_the_power() {
    let disk = SimDisk::new();
    let (mut wal, _) = open(&disk);
    assert_eq!(commit(&mut wal, 0, 4, false), 4);
    drop(wal);
    // The process is gone; the machine is not.
    assert_eq!(recovered(&disk).len(), 4);

    let (mut wal, _) = open(&disk);
    commit(&mut wal, 4, 4, false);
    drop(wal);
    disk.power_loss(0);
    let seen = recovered(&disk);
    // The first open synced the header and nothing since: no record was
    // ever synced, and none is owed.
    assert!(seen.len() <= 8);
    assert_prefix(&seen);
}

/// S1: a batch staged and never committed is not in the log after either
/// kind of crash.
#[test]
fn a_staged_batch_is_not_in_the_log() {
    let disk = SimDisk::new();
    let (mut wal, _) = open(&disk);
    commit(&mut wal, 0, 2, true);
    wal.stage(&record(2)).unwrap();
    drop(wal);
    assert_eq!(recovered(&disk).len(), 2);
    disk.power_loss(usize::MAX);
    assert_eq!(recovered(&disk).len(), 2);
}

/// S2: a write that fails poisons the log. Nothing more is staged or
/// committed, and what was durable before is still there.
#[test]
fn a_failed_write_accepts_nothing_more() {
    let disk = SimDisk::new();
    let (mut wal, _) = open(&disk);
    commit(&mut wal, 0, 2, true);

    disk.fail_write_after(0);
    wal.stage(&record(2)).unwrap();
    assert!(wal.commit_with(true).is_err());
    assert!(matches!(wal.stage(&record(3)), Err(StorageError::Poisoned)));
    assert!(matches!(wal.commit_with(true), Err(StorageError::Poisoned)));
    drop(wal);

    disk.power_loss(0);
    assert_eq!(recovered(&disk).len(), 2);
}

/// S2: a sync that fails poisons the log, and the batch it was covering is
/// not durable: a power loss takes it.
#[test]
fn a_failed_sync_accepts_nothing_more_and_covers_nothing() {
    let disk = SimDisk::new();
    let (mut wal, _) = open(&disk);
    commit(&mut wal, 0, 2, true);

    disk.fail_sync_after(0);
    wal.stage(&record(2)).unwrap();
    assert!(wal.commit_with(true).is_err());
    assert_eq!(
        wal.durable_sequence(),
        2,
        "a failed sync moved the durable sequence"
    );
    assert!(matches!(wal.stage(&record(3)), Err(StorageError::Poisoned)));
    drop(wal);

    disk.power_loss(0);
    assert_eq!(recovered(&disk), vec![record(0), record(1)]);
}

/// A log of ten durable records, `retained` of them kept as the suffix of
/// the next rewrite.
fn ten(disk: &SimDisk) -> Wal<SimDisk> {
    let (mut wal, _) = open(disk);
    assert_eq!(commit(&mut wal, 0, 10, true), 10);
    wal
}

/// What every interrupted rewrite has to leave: a log that recovers, names
/// its allocation high-water correctly, and continues the numbering.
fn assert_recovers_whole(disk: &SimDisk, context: &str) {
    let seen = recovered(disk);
    // Either the whole old log (ten records) or the whole new one (the
    // suffix 7..=10): never a mixture, never a half-written file.
    assert!(
        seen == (0..10).map(record).collect::<Vec<_>>()
            || seen == (6..10).map(record).collect::<Vec<_>>(),
        "{context}: recovered {} records that are neither log",
        seen.len()
    );
    let allocated = allocated_on(disk, &path()).unwrap();
    let floor = allocated.retention_floor();
    assert!(floor == 1 || floor == 7, "{context}: floor {floor}");
    let (_, recovery) = open(disk);
    assert_eq!(recovery.sequence, 10, "{context}: a number was handed back");
}

/// And the log, reopened, continues the numbering rather than repeating it.
fn assert_continues(disk: &SimDisk, context: &str) {
    let (mut wal, _) = open(disk);
    assert_eq!(
        commit(&mut wal, 10, 1, true),
        11,
        "{context}: a number repeated"
    );
}

fn rewrite(wal: &mut Wal<SimDisk>) -> Result<storage::Compaction, StorageError> {
    wal.compact(0, Retain { from: 7, bytes: 0 }, &mut |_| false)
}

/// S3: every write of a rewrite fails in turn. The old log stays the log,
/// and the writer carries on appending to it.
#[test]
fn a_rewrite_that_cannot_write_leaves_the_old_log() {
    // How many writes a rewrite of this log makes.
    let probe = SimDisk::new();
    let mut wal = ten(&probe);
    let (writes, _) = probe.operations();
    rewrite(&mut wal).unwrap();
    let rewrite_writes = probe.operations().0 - writes;
    assert!(rewrite_writes > 0);

    for failing in 0..rewrite_writes {
        let disk = SimDisk::new();
        let mut wal = ten(&disk);
        disk.fail_write_after(failing);
        assert!(
            rewrite(&mut wal).is_err(),
            "write {failing} did not fail the rewrite"
        );
        drop(wal);
        assert_recovers_whole(&disk, &format!("write {failing}"));
        disk.power_loss(0);
        assert_recovers_whole(&disk, &format!("write {failing}, power lost"));
        assert_continues(&disk, &format!("write {failing}"));
    }
}

/// S3: the rewrite's own sync fails, or the directory sync after the
/// rename does, and the power goes. Either log may be the one that comes
/// back — both are whole, and neither repeats a number.
#[test]
fn a_rewrite_interrupted_at_any_sync_recovers_whole() {
    let probe = SimDisk::new();
    let mut wal = ten(&probe);
    let (_, syncs) = probe.operations();
    rewrite(&mut wal).unwrap();
    let rewrite_syncs = probe.operations().1 - syncs;
    assert!(rewrite_syncs >= 2, "file sync and directory sync");

    for failing in 0..rewrite_syncs {
        for torn in [0, usize::MAX] {
            let disk = SimDisk::new();
            let mut wal = ten(&disk);
            disk.fail_sync_after(failing);
            // The directory sync failing is not an error of the rewrite —
            // the rename already happened — so either outcome is allowed.
            let _ = rewrite(&mut wal);
            drop(wal);
            let context = format!("sync {failing}, torn {torn}");
            assert_recovers_whole(&disk, &context);
            disk.power_loss(torn);
            assert_recovers_whole(&disk, &format!("{context}, power lost"));
            assert_continues(&disk, &context);
        }
    }
}

/// S3: a rewrite that completed is the log after a power loss, with the
/// suffix addressable from the floor.
#[test]
fn a_completed_rewrite_survives_the_power() {
    let disk = SimDisk::new();
    let mut wal = ten(&disk);
    assert_eq!(rewrite(&mut wal).unwrap().floor, 7);
    drop(wal);
    disk.power_loss(usize::MAX);
    assert_eq!(recovered(&disk), (6..10).map(record).collect::<Vec<_>>());
    assert_eq!(allocated_on(&disk, &path()).unwrap().retention_floor(), 7);
}

/// The clock the batching reads is the disk's. On a simulated disk a commit
/// takes exactly the time the simulation says, so the adaptation sees the
/// same latencies on every run.
#[test]
fn a_commit_is_timed_by_the_disk_clock() {
    let disk = SimDisk::new();
    let (mut wal, _) = open(&disk);
    disk.set_latency(Duration::from_millis(1), Duration::from_millis(5));
    for index in 0..3 {
        wal.stage(&record(index)).unwrap();
    }
    let (_, write, sync) = wal.commit_with(true).unwrap();
    // One write of the whole batch, one sync.
    assert_eq!(write, Duration::from_millis(1));
    assert_eq!(sync, Duration::from_millis(5));
    let (_, _, unsynced) = {
        wal.stage(&record(3)).unwrap();
        wal.commit_with(false).unwrap()
    };
    assert_eq!(unsynced, Duration::ZERO);
}

/// Nothing the simulated disk does depends on anything but the calls made
/// to it: the same calls give the same bytes.
#[test]
fn the_same_calls_give_the_same_disk() {
    let run = || {
        let disk = SimDisk::new();
        let mut wal = ten(&disk);
        rewrite(&mut wal).unwrap();
        commit(&mut wal, 10, 3, false);
        drop(wal);
        disk.power_loss(5);
        disk.contents(Path::new("/sim/log.wal"))
    };
    assert_eq!(run(), run());
}

/// D-31: the directory sync after a rewrite fails. The rename already put
/// the new file under the name, and nothing says it stays there after a
/// power loss, so the log is poisoned: nothing more is acknowledged, and
/// everything acknowledged before survives the power loss. It used to
/// return `Ok`, take an fsynced commit on the new file, and lose it.
#[test]
fn a_rewrite_whose_directory_sync_fails_poisons_the_log() {
    let disk = SimDisk::new();
    let mut wal = ten(&disk);
    // The rewrite syncs the new file, then the directory: fail the second.
    disk.fail_sync_after(1);
    assert!(matches!(
        rewrite(&mut wal),
        Err(StorageError::DirectoryNotDurable(_))
    ));
    assert!(matches!(
        wal.stage(&record(10)),
        Err(StorageError::Poisoned)
    ));
    assert!(matches!(wal.commit_with(true), Err(StorageError::Poisoned)));
    drop(wal);

    disk.power_loss(0);
    let seen = recovered(&disk);
    assert!(
        seen == (0..10).map(record).collect::<Vec<_>>()
            || seen == (6..10).map(record).collect::<Vec<_>>(),
        "recovered {} records that are neither log",
        seen.len()
    );
}

/// D-30: a new log is durable as a name, not only as bytes. Syncing a file
/// says nothing about the directory entry that names it (POSIX), so a log
/// created and power-cut before any directory sync would simply be gone.
#[test]
fn a_new_log_survives_a_power_loss() {
    let disk = SimDisk::new();
    let (mut wal, _) = open(&disk);
    assert_eq!(commit(&mut wal, 0, 2, true), 2);
    drop(wal);
    disk.power_loss(0);
    assert_eq!(recovered(&disk), vec![record(0), record(1)]);
}
