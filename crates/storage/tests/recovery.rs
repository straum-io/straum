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
    assert_eq!(fs::read(&path).unwrap(), b"STRAUMW2");
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

#[test]
fn replay_hands_over_the_durable_prefix_in_order() {
    let path = scratch("replay");
    log_with(&path, 5);

    let mut seen = Vec::new();
    let recovery = storage::replay(&path, WalLimits::default(), &mut |record| {
        seen.push(String::from_utf8_lossy(record).into_owned());
    })
    .unwrap();

    assert_eq!(recovery.records, 5);
    assert_eq!(
        seen,
        (0..5).map(|i| format!("record-{i}")).collect::<Vec<_>>(),
        "the log was not replayed in commit order"
    );
    let _ = fs::remove_file(&path);
}

#[test]
fn replay_stops_where_recovery_stops() {
    let path = scratch("replay-tail");
    let mut bytes = log_with(&path, 4);
    // A crash left half a record and some noise behind.
    bytes.truncate(bytes.len() - 2);
    bytes.extend_from_slice(b"garbage");
    fs::write(&path, &bytes).unwrap();

    let mut seen = 0;
    let recovery = storage::replay(&path, WalLimits::default(), &mut |_| seen += 1).unwrap();

    assert_eq!(recovery.records, 3);
    assert_eq!(seen, 3, "the tail was offered as if it were a record");
    let _ = fs::remove_file(&path);
}

#[test]
fn replay_of_nothing_is_not_a_failure() {
    let path = scratch("replay-missing");
    let recovery = storage::replay(&path, WalLimits::default(), &mut |_| {}).unwrap();
    assert_eq!(recovery.records, 0);
}

#[test]
fn a_log_in_the_previous_format_is_refused() {
    let path = scratch("old-format");
    let mut bytes = log_with(&path, 3);
    // Only the version differs; every record would still parse.
    bytes[..8].copy_from_slice(b"STRAUMW1");
    fs::write(&path, &bytes).unwrap();

    assert!(matches!(
        storage::replay(&path, WalLimits::default(), &mut |_| {}),
        Err(StorageError::NotAWal)
    ));
    assert!(matches!(
        Wal::open(&path, WalLimits::default()),
        Err(StorageError::NotAWal)
    ));
    let _ = fs::remove_file(&path);
}

/// The bytes one log produced, framed and ready to be appended to another —
/// what a replication frame carries (docs/replication.md §114).
fn framed(payloads: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for payload in payloads {
        storage::encode_for_benchmarks(payload, &mut out);
    }
    out
}

#[test]
fn a_replicated_run_lands_in_the_log_unchanged() {
    let path = scratch("framed");
    let (mut wal, _) = Wal::open(&path, WalLimits::default()).unwrap();

    let run = framed(&[b"one", b"two", b"three"]);
    assert_eq!(wal.stage_framed(&run).unwrap(), 3);
    let (committed, _, _) = wal.commit().unwrap();

    // The sequence is a record count, and it counted the records itself
    // rather than believing a number that came over a network.
    assert_eq!(committed.durable_sequence, 3);
    drop(wal);

    // Byte for byte what the other log held: the checksums were not
    // recomputed, so they are still the ones that log wrote.
    let file = fs::read(&path).unwrap();
    assert_eq!(&file[8..], &run[..]);

    let mut seen = Vec::new();
    let recovery = storage::replay(&path, WalLimits::default(), &mut |payload| {
        seen.push(payload.to_vec());
    })
    .unwrap();
    assert_eq!(recovery.records, 3);
    assert_eq!(
        seen,
        vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
    );
    let _ = fs::remove_file(&path);
}

#[test]
fn replicated_and_local_records_share_one_batch() {
    let path = scratch("mixed");
    let (mut wal, _) = Wal::open(&path, WalLimits::default()).unwrap();

    wal.stage(b"local").unwrap();
    wal.stage_framed(&framed(&[b"remote-a", b"remote-b"]))
        .unwrap();
    wal.stage(b"local-again").unwrap();
    let (committed, _, _) = wal.commit().unwrap();

    assert_eq!(committed.records, 4);
    assert_eq!(committed.durable_sequence, 4);
    let _ = fs::remove_file(&path);
}

#[test]
fn a_run_that_does_not_decompose_is_refused_and_stages_nothing() {
    let path = scratch("ragged");
    let (mut wal, _) = Wal::open(&path, WalLimits::default()).unwrap();

    let whole = framed(&[b"one", b"two"]);
    for broken in [
        // A record cut in half: appending this would put a fragment in the
        // middle of a log, where truncation cannot reach it.
        whole[..whole.len() - 1].to_vec(),
        // Nothing at all.
        Vec::new(),
        // A length that runs past the end of what was sent.
        {
            let mut lying = whole.clone();
            lying[0..4].copy_from_slice(&9_000u32.to_le_bytes());
            lying
        },
        // A zero-length record, which the writer never produces.
        {
            let mut empty = whole.clone();
            empty[0..4].copy_from_slice(&0u32.to_le_bytes());
            empty
        },
    ] {
        assert!(matches!(
            wal.stage_framed(&broken),
            Err(StorageError::NotFramedRecords)
        ));
        assert_eq!(
            wal.staged_records(),
            0,
            "a refused run left something behind"
        );
        assert_eq!(wal.staged_bytes(), 0);
    }

    // And the log still works afterwards.
    assert_eq!(wal.stage_framed(&whole).unwrap(), 2);
    let _ = fs::remove_file(&path);
}
