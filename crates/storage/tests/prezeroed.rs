//! D-162 A, a prototype: zeros written ahead of the log (`preallocate_bytes`)
//! change nothing a reader or a recovery sees. Recovery stops at the first
//! zero frame as at any torn tail, a torn overwrite of the zeros is cut at its
//! first bad frame, compaction keeps the same suffix, and a sealed segment
//! holds no zeros.
#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use std::path::PathBuf;

use storage::simulated::SimDisk;
use storage::{OsDisk, Retain, SegmentLimits, Segments, Wal, WalLimits, replay_on};

const AHEAD: u64 = 4096;

fn ahead() -> WalLimits {
    WalLimits {
        preallocate_bytes: AHEAD,
        ..WalLimits::default()
    }
}

fn record(index: u64) -> Vec<u8> {
    format!("record-{index}").into_bytes()
}

fn commit<D: storage::Disk>(wal: &mut Wal<D>, from: u64, count: u64, sync: bool) {
    for index in from..from + count {
        wal.stage(&record(index)).unwrap();
    }
    wal.commit_with(sync).unwrap();
}

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "straum-prezeroed-{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&path);
    path
}

/// On a real file: the zeros are there, recovery stops before them, and
/// appending after a reopen continues the same log.
#[test]
fn a_log_with_zeros_ahead_recovers_exactly_what_it_committed() {
    let path = scratch("recovers");
    let (mut wal, _) = Wal::open(&path, ahead()).unwrap();
    for batch in 0..10 {
        commit(&mut wal, batch * 5, 5, true);
    }
    let durable = wal.durable_offset();
    drop(wal);
    let length = std::fs::metadata(&path).unwrap().len();
    // Zeros run past the data, by at most one extension.
    assert!(
        length > durable && length - durable <= AHEAD,
        "zeros written ahead: {length} vs {durable}"
    );

    let (mut wal, recovery) = Wal::open(&path, ahead()).unwrap();
    assert_eq!(recovery.records, 50);
    assert_eq!(recovery.durable_offset, durable);
    commit(&mut wal, 50, 5, true);
    drop(wal);

    let mut seen = Vec::new();
    replay_on(&OsDisk, &path, ahead(), &mut |payload| {
        seen.push(payload.to_vec())
    })
    .unwrap();
    assert_eq!(seen, (0..55).map(record).collect::<Vec<_>>());
    std::fs::remove_file(&path).unwrap();
}

/// What a power loss can leave in pre-zeroed space, built as bytes on a real
/// file: the simulated disk cannot produce it, since it reverts any unsynced
/// overwrite inside the synced length whole. A record whose header persisted
/// with its body still zeros fails its checksum; a record persisted past a
/// gap of zeros is never reached. Recovery keeps what was synced, invents
/// nothing, and reopening cuts the rest, so a stale record past a gap
/// cannot come back after later appends. Whether ext4 can persist these
/// shapes, and others, is not shown here: recovery depends on the frame and
/// its checksum, as at any torn tail.
#[test]
fn a_torn_overwrite_of_the_zeros_is_cut_at_the_first_bad_frame() {
    for case in ["header without body", "record past a gap"] {
        let path = scratch(&format!("torn-{}", case.replace(' ', "-")));
        let (mut wal, _) = Wal::open(&path, ahead()).unwrap();
        commit(&mut wal, 0, 20, true);
        let durable = wal.durable_offset();
        drop(wal);
        let mut bytes = std::fs::read(&path).unwrap();
        let mut framed = Vec::new();
        storage::encode_for_benchmarks(&record(99), &mut framed);
        let at = usize::try_from(durable).unwrap();
        match case {
            "header without body" => bytes[at..at + 8].copy_from_slice(&framed[..8]),
            _ => bytes[at + 64..at + 64 + framed.len()].copy_from_slice(&framed),
        }
        std::fs::write(&path, &bytes).unwrap();

        let (mut wal, recovery) = Wal::open(&path, ahead()).unwrap();
        assert_eq!(recovery.records, 20, "{case}");
        assert_eq!(recovery.durable_offset, durable, "{case}");
        // Reopening cut the file back to its log: the stale frame is gone.
        assert_eq!(std::fs::metadata(&path).unwrap().len(), durable, "{case}");
        commit(&mut wal, 20, 1, true);
        drop(wal);
        let mut seen = Vec::new();
        replay_on(&OsDisk, &path, ahead(), &mut |payload| {
            seen.push(payload.to_vec())
        })
        .unwrap();
        assert_eq!(seen, (0..21).map(record).collect::<Vec<_>>(), "{case}");
        std::fs::remove_file(&path).unwrap();
    }
}

/// Compaction measures what it keeps from the file's end: with zeros ahead
/// it keeps the same suffix as without.
#[test]
fn compaction_keeps_the_same_suffix_with_zeros_ahead() {
    let outcome = |limits: WalLimits| {
        let disk = SimDisk::new();
        let path = PathBuf::from("/sim/log.wal");
        let (mut wal, _) = Wal::open_on(disk, &path, limits).unwrap();
        for batch in 0..20 {
            commit(&mut wal, batch * 10, 10, true);
        }
        let retain = Retain {
            from: u64::MAX,
            bytes: 300,
        };
        wal.compact(0, retain, &mut |_| false).unwrap()
    };
    let with = outcome(ahead());
    let without = outcome(WalLimits::default());
    assert_eq!(with.retained_bytes, without.retained_bytes);
    assert_eq!(with.records_after, without.records_after);
    assert_eq!(with.bytes_after, without.bytes_after);
}

/// A segment is sealed without its zeros: its file is exactly its log.
#[test]
fn a_sealed_segment_holds_no_zeros() {
    let directory = scratch("segments");
    std::fs::create_dir_all(&directory).unwrap();
    let limits = SegmentLimits {
        wal: ahead(),
        segment_bytes: 200,
    };
    let mut log = Segments::open(OsDisk, &directory, limits, &mut |_, _| {}).unwrap();
    let mut key = 1;
    for _ in 0..10 {
        log.begin(key).unwrap();
        for i in 0..3 {
            log.stage(&record(key + i)).unwrap();
        }
        log.commit(true).unwrap();
        key += 3;
    }
    let keys: Vec<u64> = log.keys().collect();
    assert!(keys.len() > 2);
    drop(log);
    let mut files: Vec<_> = std::fs::read_dir(&directory)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    let (last, sealed) = files.split_last().unwrap();
    for file in sealed {
        let (_, recovery) = Wal::open(file, WalLimits::default()).unwrap();
        assert_eq!(
            std::fs::metadata(file).unwrap().len(),
            recovery.durable_offset,
            "{file:?} kept zeros"
        );
    }
    assert!(std::fs::metadata(last).unwrap().len() > 0);
    let mut seen = Vec::new();
    Segments::open(OsDisk, &directory, limits, &mut |_, r| {
        seen.push(r.to_vec())
    })
    .unwrap();
    assert_eq!(seen, (1..key).map(record).collect::<Vec<_>>());
    std::fs::remove_dir_all(&directory).unwrap();
}
