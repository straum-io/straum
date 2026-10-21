//! ADR-0009 (D-170 A): zeros written ahead of the log (`preallocate_bytes`)
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
/// file, two shapes by hand (the sector-torn power losses below reach them
/// and others on the simulated disk). A record whose header persisted
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

/// ADR-0009, D-76: a reader of the active segment stops at its written end,
/// not the file's, which with zeros ahead runs further. A frame placed right
/// after the log, as a batch written after the walk began would be, is not
/// read.
#[test]
fn a_reader_of_the_active_segment_stops_at_its_written_end() {
    let directory = scratch("read-bound");
    std::fs::create_dir_all(&directory).unwrap();
    let limits = SegmentLimits {
        wal: ahead(),
        segment_bytes: 1 << 20,
    };
    let mut log = Segments::open(OsDisk, &directory, limits, &mut |_, _| {}).unwrap();
    log.begin(1).unwrap();
    for i in 0..5 {
        log.stage(&record(i)).unwrap();
    }
    log.commit(true).unwrap();
    let (key, written) = log.sizes().last().unwrap();
    // A valid frame past the written end, inside the zeros.
    let file = std::fs::read_dir(&directory)
        .unwrap()
        .map(|e| e.unwrap().path())
        .next()
        .unwrap();
    assert!(
        std::fs::metadata(&file).unwrap().len() > written,
        "zeros ahead"
    );
    let mut framed = Vec::new();
    storage::encode_for_benchmarks(&record(99), &mut framed);
    let mut bytes = std::fs::read(&file).unwrap();
    let at = usize::try_from(written).unwrap();
    bytes[at..at + framed.len()].copy_from_slice(&framed);
    std::fs::write(&file, &bytes).unwrap();

    let mut seen = Vec::new();
    log.read_while(key, None, &mut |r, _| {
        seen.push(r.to_vec());
        true
    })
    .unwrap();
    assert_eq!(seen, (0..5).map(record).collect::<Vec<_>>(), "read_while");
    let mut all = Vec::new();
    log.read(key, &mut |r| all.push(r.to_vec())).unwrap();
    assert_eq!(all, seen, "read");
    std::fs::remove_dir_all(&directory).unwrap();
}

/// ADR-0009: a power loss with writes in flight in any order, sector by
/// sector, over pre-zeroed space (`SimDisk::power_loss_sectors`): every
/// synced batch survives, nothing is invented, and the log goes on after.
/// 2 000 seeds, each with unsynced batches overwriting synced zeros.
#[test]
fn a_torn_overwrite_of_pre_zeroed_space_keeps_every_synced_batch() {
    let path = PathBuf::from("/sim/log.wal");
    for seed in 0..2_000u64 {
        let disk = SimDisk::new();
        let (mut wal, _) = Wal::open_on(disk.clone(), &path, ahead()).unwrap();
        // Synced: the zeros ahead are durable too.
        commit(&mut wal, 0, 30, true);
        let unsynced = 1 + seed % 5;
        for batch in 0..unsynced {
            commit(&mut wal, 30 + batch * 7, 7, false);
        }
        drop(wal);
        disk.power_loss_sectors(seed);
        let mut seen = Vec::new();
        replay_on(&disk, &path, ahead(), &mut |p| seen.push(p.to_vec())).unwrap();
        assert!(
            seen.len() >= 30,
            "seed {seed}: lost a synced record ({})",
            seen.len()
        );
        for (at, body) in seen.iter().enumerate() {
            assert_eq!(
                body,
                &record(at as u64),
                "seed {seed}: record {at} invented"
            );
        }
        // The log goes on from what recovery kept.
        let (mut wal, recovery) = Wal::open_on(disk.clone(), &path, ahead()).unwrap();
        let next = recovery.records;
        commit(&mut wal, next, 3, true);
        drop(wal);
        let mut again = Vec::new();
        replay_on(&disk, &path, ahead(), &mut |p| again.push(p.to_vec())).unwrap();
        assert_eq!(
            again,
            (0..next + 3).map(record).collect::<Vec<_>>(),
            "seed {seed}: after the restart"
        );
    }
}

/// The extension window: with less than one batch of zeros ahead, every
/// unsynced batch first extends the file with zeros and then overwrites
/// them, so a power loss lands between `zero_to` and the next sync, with
/// the file at either length (`power_loss_sectors`). The same three
/// properties hold.
#[test]
fn a_power_loss_during_a_zero_extension_keeps_every_synced_batch() {
    let path = PathBuf::from("/sim/log.wal");
    let short = WalLimits {
        preallocate_bytes: 64,
        ..WalLimits::default()
    };
    let mut lengths = std::collections::BTreeSet::new();
    for seed in 0..2_000u64 {
        let disk = SimDisk::new();
        let (mut wal, _) = Wal::open_on(disk.clone(), &path, short).unwrap();
        commit(&mut wal, 0, 30, true);
        let synced_length = disk.contents(&path).unwrap().len();
        for batch in 0..(1 + seed % 5) {
            commit(&mut wal, 30 + batch * 7, 7, false);
        }
        drop(wal);
        disk.power_loss_sectors(seed);
        lengths.insert(disk.contents(&path).unwrap().len() > synced_length);
        let mut seen = Vec::new();
        replay_on(&disk, &path, short, &mut |p| seen.push(p.to_vec())).unwrap();
        assert!(seen.len() >= 30, "seed {seed}: lost a synced record");
        for (at, body) in seen.iter().enumerate() {
            assert_eq!(
                body,
                &record(at as u64),
                "seed {seed}: record {at} invented"
            );
        }
        let (mut wal, recovery) = Wal::open_on(disk.clone(), &path, short).unwrap();
        let next = recovery.records;
        commit(&mut wal, next, 3, true);
        drop(wal);
        let mut again = Vec::new();
        replay_on(&disk, &path, short, &mut |p| again.push(p.to_vec())).unwrap();
        assert_eq!(
            again,
            (0..next + 3).map(record).collect::<Vec<_>>(),
            "seed {seed}"
        );
    }
    assert_eq!(lengths.len(), 2, "the file came back at both lengths");
}

/// The seal window: unsynced batches cross `segment_bytes`, so the old
/// segment is trimmed and sealed and a new one begun, then the power goes.
/// Recovery over every segment keeps each synced record, invents nothing,
/// and the log goes on.
#[test]
fn a_power_loss_across_a_segment_seal_keeps_every_synced_batch() {
    let limits = SegmentLimits {
        wal: WalLimits {
            preallocate_bytes: 256,
            ..WalLimits::default()
        },
        segment_bytes: 300,
    };
    let open = |disk: &SimDisk| {
        let mut seen = Vec::new();
        let log = Segments::open(disk.clone(), &PathBuf::from("/sim"), limits, &mut |_, r| {
            seen.push(r.to_vec())
        })
        .unwrap();
        (log, seen)
    };
    let write = |log: &mut Segments<SimDisk>, from: u64, batches: u64, sync: bool| {
        let mut key = from;
        for _ in 0..batches {
            log.begin(key).unwrap();
            for i in 0..4 {
                log.stage(&record(key + i)).unwrap();
            }
            log.commit(sync).unwrap();
            key += 4;
        }
        key
    };
    let mut sealed_after_sync = 0;
    for seed in 0..1_000u64 {
        let disk = SimDisk::new();
        let (mut log, _) = open(&disk);
        let synced = write(&mut log, 0, 3, true);
        let before = log.keys().count();
        write(&mut log, synced, 2 + seed % 6, false);
        if log.keys().count() > before {
            sealed_after_sync += 1;
        }
        // Records before the last segment's first were sealed: synced by the seal.
        let sealed_through = log.keys().last().unwrap_or(0);
        drop(log);
        disk.power_loss_sectors(seed);
        let (mut log, seen) = open(&disk);
        let synced = usize::try_from(synced.max(sealed_through)).unwrap();
        assert!(
            seen.len() >= synced,
            "seed {seed}: lost a synced record ({})",
            seen.len()
        );
        for (at, body) in seen.iter().enumerate() {
            assert_eq!(
                body,
                &record(at as u64),
                "seed {seed}: record {at} invented"
            );
        }
        let next = seen.len() as u64;
        write(&mut log, next, 1, true);
        drop(log);
        let (_, again) = open(&disk);
        assert_eq!(
            again,
            (0..next + 4).map(record).collect::<Vec<_>>(),
            "seed {seed}"
        );
    }
    assert!(
        sealed_after_sync > 500,
        "the unsynced batches crossed a seal: {sealed_after_sync}"
    );
}
