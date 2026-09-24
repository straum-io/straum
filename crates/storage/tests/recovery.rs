//! Crash recovery: which prefix of the log is guaranteed, and which tail is
//! not (docs/storage.md §92).
//!
//! A crash is simulated by doing to the file what a crash does: cut it short,
//! flip a byte, leave a half-written record behind. The property under test
//! is always the same — recovery returns the longest prefix of complete,
//! checksum-clean records and refuses to invent anything past it.
#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use std::fs;
use std::path::{Path, PathBuf};

use storage::{StorageError, Wal, WalLimits};

fn scratch(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "straum-recovery-{name}-{}-{:?}.wal",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = fs::remove_file(&path);
    path
}

/// Writes `records` records and returns the file's bytes.
fn log_with(path: &Path, records: usize) -> Vec<u8> {
    let (mut wal, _) = Wal::open(path, WalLimits::default()).unwrap();
    for index in 0..records {
        wal.stage(format!("record-{index}").as_bytes()).unwrap();
    }
    if records > 0 {
        wal.commit().unwrap();
    }
    drop(wal);
    fs::read(path).unwrap()
}

fn reopen(path: &Path) -> storage::Recovery {
    let (_, recovery) = Wal::open(path, WalLimits::default()).unwrap();
    recovery
}

#[test]
fn a_fresh_log_starts_empty_and_identifiable() {
    let path = scratch("fresh");
    let recovery = reopen(&path);

    assert_eq!(recovery.records, 0);
    assert_eq!(recovery.discarded_bytes, 0);
    assert_eq!(fs::read(&path).unwrap(), b"STRAUMW1");
    let _ = fs::remove_file(&path);
}

#[test]
fn a_clean_log_recovers_completely() {
    let path = scratch("clean");
    log_with(&path, 100);

    let recovery = reopen(&path);
    assert_eq!(recovery.records, 100);
    assert_eq!(recovery.discarded_bytes, 0);
    let _ = fs::remove_file(&path);
}

/// The interesting one: a crash can cut the file anywhere.
#[test]
fn every_truncation_yields_a_prefix_never_a_lie() {
    let path = scratch("truncate");
    let bytes = log_with(&path, 20);
    let clean = reopen(&path).records;

    for cut in 8..bytes.len() {
        fs::write(&path, &bytes[..cut]).unwrap();
        let recovery = reopen(&path);

        assert!(
            recovery.records <= clean,
            "cut at {cut}: invented records out of a truncated file"
        );
        // Whatever survived has to be a real prefix: reading it again agrees.
        let again = reopen(&path);
        assert_eq!(again.records, recovery.records, "cut at {cut}");
        assert_eq!(again.discarded_bytes, 0, "cut at {cut}: tail left behind");
    }
    let _ = fs::remove_file(&path);
}

#[test]
fn a_half_written_final_record_is_discarded() {
    let path = scratch("partial");
    let mut bytes = log_with(&path, 5);
    let full = bytes.len();

    // A record whose header promises more than the file holds.
    bytes.truncate(full - 3);
    fs::write(&path, &bytes).unwrap();

    let recovery = reopen(&path);
    assert_eq!(recovery.records, 4);
    assert_eq!(
        recovery.discarded_bytes as usize,
        bytes.len() - recovery.durable_offset as usize
    );
    let _ = fs::remove_file(&path);
}

#[test]
fn a_corrupted_payload_ends_the_durable_prefix() {
    let path = scratch("corrupt");
    let mut bytes = log_with(&path, 10);
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(&path, &bytes).unwrap();

    let recovery = reopen(&path);
    assert_eq!(recovery.records, 9, "the corrupt record was accepted");
    assert!(recovery.discarded_bytes > 0);
    let _ = fs::remove_file(&path);
}

#[test]
fn an_impossible_length_ends_the_durable_prefix() {
    let path = scratch("length");
    let bytes = log_with(&path, 3);
    let mut bytes = bytes;
    // Rewrite the first record's length as something no record can be.
    bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    fs::write(&path, &bytes).unwrap();

    let recovery = reopen(&path);
    assert_eq!(recovery.records, 0);
    let _ = fs::remove_file(&path);
}

#[test]
fn a_foreign_file_is_refused_not_guessed_at() {
    let path = scratch("foreign");
    fs::write(&path, b"this is not a write-ahead log at all").unwrap();

    assert!(matches!(
        Wal::open(&path, WalLimits::default()),
        Err(StorageError::NotAWal)
    ));
    let _ = fs::remove_file(&path);
}

#[test]
fn recovery_cuts_the_tail_so_the_log_keeps_growing() {
    let path = scratch("continue");
    let mut bytes = log_with(&path, 5);
    bytes.extend_from_slice(b"garbage that a crash left behind");
    fs::write(&path, &bytes).unwrap();

    // Recover, append, and crash again.
    let (mut wal, recovery) = Wal::open(&path, WalLimits::default()).unwrap();
    assert_eq!(recovery.records, 5);
    assert!(recovery.discarded_bytes > 0);
    wal.stage(b"after-recovery").unwrap();
    wal.commit().unwrap();
    drop(wal);

    let recovery = reopen(&path);
    assert_eq!(recovery.records, 6, "the appended record did not survive");
    assert_eq!(recovery.discarded_bytes, 0);
    let _ = fs::remove_file(&path);
}

#[test]
fn staged_but_never_committed_records_do_not_exist() {
    let path = scratch("staged");
    let (mut wal, _) = Wal::open(&path, WalLimits::default()).unwrap();
    wal.stage(b"one").unwrap();
    wal.commit().unwrap();
    // Staged after the last commit: the process dies here.
    wal.stage(b"two").unwrap();
    wal.stage(b"three").unwrap();
    drop(wal);

    let recovery = reopen(&path);
    assert_eq!(recovery.records, 1);
    let _ = fs::remove_file(&path);
}

#[test]
fn the_durable_offset_only_moves_forward() {
    let path = scratch("monotonic");
    let (mut wal, _) = Wal::open(&path, WalLimits::default()).unwrap();

    let mut previous = wal.durable_offset();
    for round in 0..50u64 {
        wal.stage(format!("record-{round}").as_bytes()).unwrap();
        let (committed, _, _) = wal.commit().unwrap();
        assert!(committed.durable_offset > previous);
        assert_eq!(committed.durable_sequence, round + 1);
        previous = committed.durable_offset;
    }
    let _ = fs::remove_file(&path);
}
