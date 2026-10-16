//! The segmented log (unified-stream §3, ADR-0008 step 2) on a simulated disk.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

use storage::simulated::SimDisk;
use storage::{SegmentLimits, Segments, WalLimits};

fn dir() -> PathBuf {
    PathBuf::from("/sim")
}

fn limits() -> SegmentLimits {
    SegmentLimits {
        wal: WalLimits::default(),
        segment_bytes: 200,
    }
}

fn open(disk: &SimDisk) -> (Segments<SimDisk>, Vec<(u64, Vec<u8>)>) {
    let mut seen = Vec::new();
    let log = Segments::open(disk.clone(), &dir(), limits(), &mut |key, record| {
        seen.push((key, record.to_vec()))
    })
    .unwrap();
    (log, seen)
}

/// Batches of `per` 40-byte records, keyed by the number of the first record.
fn write(log: &mut Segments<SimDisk>, from: u64, batches: u64, per: u64, sync: bool) -> u64 {
    let mut key = from;
    for _ in 0..batches {
        log.begin(key).unwrap();
        for i in 0..per {
            log.stage(&record(key + i)).unwrap();
        }
        log.commit(sync).unwrap();
        key += per;
    }
    key
}

fn record(n: u64) -> Vec<u8> {
    let mut bytes = n.to_be_bytes().to_vec();
    bytes.resize(40, 0xab);
    bytes
}

fn numbers(seen: &[(u64, Vec<u8>)]) -> Vec<u64> {
    seen.iter()
        .map(|(_, r)| u64::from_be_bytes(r[..8].try_into().unwrap()))
        .collect()
}

#[test]
fn a_new_segment_starts_at_a_batch_boundary_and_recovery_reads_them_in_order() {
    let disk = SimDisk::new();
    let (mut log, _) = open(&disk);
    let end = write(&mut log, 1, 10, 3, true);
    let keys: Vec<u64> = log.keys().collect();
    assert!(keys.len() > 2, "{keys:?}");
    // Every segment is named by the first record of a batch.
    assert!(keys.iter().all(|k| (k - 1) % 3 == 0), "{keys:?}");
    drop(log);

    let (_, seen) = open(&disk);
    assert_eq!(numbers(&seen), (1..end).collect::<Vec<_>>());
    // Each record is reported with the segment it lives in.
    assert!(
        seen.iter()
            .all(|(key, r)| *key <= u64::from_be_bytes(r[..8].try_into().unwrap()))
    );
}

#[test]
fn removal_takes_whole_segments_from_the_front_and_stops_at_a_pin() {
    let disk = SimDisk::new();
    let (mut log, _) = open(&disk);
    write(&mut log, 1, 10, 3, true);
    let keys: Vec<u64> = log.keys().collect();

    // A floor inside the second segment removes only the first.
    assert_eq!(log.remove_through(keys[1]).unwrap(), 1);
    assert_eq!(log.keys().next(), Some(keys[1]));

    // A reader on the next segment holds it, and everything after it.
    let pin = log.pin(keys[1]);
    assert_eq!(log.remove_through(u64::MAX).unwrap(), 0);
    log.unpin(pin);
    // Never the active segment, nor the last sealed one: it says where the
    // log stands if the active one turns out to hold only a torn batch.
    assert_eq!(log.remove_through(u64::MAX).unwrap(), keys.len() - 3);
    assert_eq!(
        log.keys().collect::<Vec<_>>(),
        keys[keys.len() - 2..].to_vec()
    );
    assert_eq!(log.segment_of(1), None, "removed records are not found");
}

#[test]
fn a_torn_batch_is_cut_and_writing_continues_after_it() {
    let disk = SimDisk::new();
    let (mut log, _) = open(&disk);
    let end = write(&mut log, 1, 2, 3, true);
    log.begin(end).unwrap();
    log.stage(&record(end)).unwrap();
    log.stage(&record(end + 1)).unwrap();
    log.commit(true).unwrap();
    // The caller finds the batch half there and cuts it.
    log.truncate_tail(2).unwrap();
    write(&mut log, end, 1, 3, true);
    drop(log);
    let (_, seen) = open(&disk);
    assert_eq!(numbers(&seen), (1..end + 3).collect::<Vec<_>>());
}

/// A segment is made durable before the next one starts: a power loss can
/// cost the unsynced end of the log, never a hole in the middle of it.
#[test]
fn a_power_loss_never_leaves_a_hole_between_segments() {
    for torn in [0, 7, 40, 1000] {
        let disk = SimDisk::new();
        let (mut log, _) = open(&disk);
        write(&mut log, 1, 12, 3, false);
        drop(log);
        disk.power_loss(torn);
        let (_, seen) = open(&disk);
        let got = numbers(&seen);
        assert_eq!(
            got,
            (1..=got.len() as u64).collect::<Vec<_>>(),
            "torn {torn}"
        );
        assert!(!got.is_empty(), "sealed segments are durable");
    }
}

#[test]
fn files_that_are_not_segments_are_ignored() {
    let disk = SimDisk::new();
    let (mut log, _) = open(&disk);
    write(&mut log, 1, 2, 3, true);
    drop(log);
    use storage::{Access, Disk};
    disk.open(Path::new("/sim/notes.txt"), Access::Create)
        .unwrap();
    let (_, seen) = open(&disk);
    assert_eq!(numbers(&seen).len(), 6);
}

/// The same log on the real file system: `OsDisk::list` finds the segments
/// and skips what is not one.
#[test]
fn segments_on_the_operating_systems_disk_recover_in_order() {
    use storage::OsDisk;
    let directory = std::env::temp_dir().join(format!(
        "straum-segments-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("notes.txt"), b"not a segment").unwrap();

    let mut log = Segments::open(OsDisk, &directory, limits(), &mut |_, _| {}).unwrap();
    let mut key = 1;
    for _ in 0..10 {
        log.begin(key).unwrap();
        for i in 0..3 {
            log.stage(&record(key + i)).unwrap();
        }
        log.commit(true).unwrap();
        key += 3;
    }
    assert!(log.keys().count() > 2);
    drop(log);

    let mut seen = Vec::new();
    Segments::open(OsDisk, &directory, limits(), &mut |segment, r| {
        seen.push((segment, r.to_vec()))
    })
    .unwrap();
    assert_eq!(numbers(&seen), (1..key).collect::<Vec<_>>());
    std::fs::remove_dir_all(&directory).unwrap();
}

/// A tail spanning segments is cut at a batch boundary: later segments go
/// whole, the one holding the boundary is cut, and writing continues there.
#[test]
fn a_tail_is_cut_across_segments_and_writing_continues_after_it() {
    let disk = SimDisk::new();
    let (mut log, _) = open(&disk);
    let end = write(&mut log, 1, 12, 3, true);
    let before: Vec<u64> = log.keys().collect();
    assert!(before.len() > 3, "{before:?}");
    // Inside the second segment, at a batch's end.
    let last = before[1] + 2;
    log.truncate_after(last).unwrap();
    assert!(
        log.keys().all(|k| k <= last),
        "{:?}",
        log.keys().collect::<Vec<_>>()
    );
    let next = write(&mut log, last + 1, 2, 3, true);
    drop(log);
    let (_, seen) = open(&disk);
    assert_eq!(numbers(&seen), (1..next).collect::<Vec<_>>());
    assert!(next < end);

    // Back to nothing: every segment goes, and the log starts again.
    let (mut log, _) = open(&disk);
    log.truncate_after(0).unwrap();
    assert_eq!(log.keys().count(), 0);
    write(&mut log, 1, 1, 3, true);
    drop(log);
    let (_, seen) = open(&disk);
    assert_eq!(numbers(&seen), vec![1, 2, 3]);
}

/// A cut is as durable as what it keeps: a power loss right after it, with
/// nothing synced since, recovers the log exactly as cut.
#[test]
fn a_cut_survives_a_power_loss() {
    for torn in [0, 1, 7, 40, 299] {
        let disk = SimDisk::new();
        let (mut log, _) = open(&disk);
        // One segment: 14 records in batches of 1, 2, 3...
        let mut key = 1;
        for per in [1u64, 2, 3, 2, 3, 3] {
            key = write(&mut log, key, 1, per, true);
        }
        assert_eq!(key, 15);
        log.truncate_after(9).unwrap();
        drop(log);
        disk.power_loss(torn);
        let (_, seen) = open(&disk);
        assert_eq!(numbers(&seen), (1..=9).collect::<Vec<_>>(), "torn {torn}");
    }
}

/// D-142 A: a read that stops early, and one resumed from an end offset an
/// earlier read handed out, see exactly the records the whole walk sees;
/// an offset outside the file is refused, and one at its end yields
/// nothing.
#[test]
fn a_read_stopped_early_or_resumed_from_its_offset_matches_the_whole_walk() {
    let disk = SimDisk::new();
    let (mut log, _) = open(&disk);
    let mut key = write(&mut log, 1, 1, 2, true);
    // A large segment: everything below goes into the first.
    let big = SegmentLimits {
        wal: WalLimits::default(),
        segment_bytes: 1 << 20,
    };
    drop(log);
    let mut log = Segments::open(disk.clone(), &dir(), big, &mut |_, _| {}).unwrap();
    key = write(&mut log, key, 6, 3, true);
    let segment = log.keys().next().unwrap();

    let mut whole: Vec<(u64, Vec<u8>)> = Vec::new();
    log.read_while(segment, None, &mut |r, end| {
        whole.push((end, r.to_vec()));
        true
    })
    .unwrap();
    assert_eq!(whole.len() as u64, key - 1);

    for stop in 0..whole.len() {
        // Stopped after `stop + 1` records: those, and nothing past them.
        let mut first: Vec<(u64, Vec<u8>)> = Vec::new();
        log.read_while(segment, None, &mut |r, end| {
            first.push((end, r.to_vec()));
            first.len() <= stop
        })
        .unwrap();
        assert_eq!(first, whole[..=stop]);
        // Resumed from the last one's end: the rest, as the whole walk had it.
        let mut rest: Vec<(u64, Vec<u8>)> = Vec::new();
        log.read_while(segment, Some(first[stop].0), &mut |r, end| {
            rest.push((end, r.to_vec()));
            true
        })
        .unwrap();
        assert_eq!(rest, whole[stop + 1..]);
    }

    let end = whole.last().unwrap().0;
    let mut none = 0;
    log.read_while(segment, Some(end), &mut |_, _| {
        none += 1;
        true
    })
    .unwrap();
    assert_eq!(none, 0);
    assert!(
        log.read_while(segment, Some(end + 1), &mut |_, _| true)
            .is_err()
    );
    assert!(log.read_while(segment, Some(0), &mut |_, _| true).is_err());
}
