//! Logical identity across physical compaction (docs/replication.md §116).
//!
//! Compaction removes records on purpose. What it must never do is let the
//! log hand out a number it has handed out before, because everything that
//! points at a record — a session's mark, a replication position — stops
//! meaning one record the moment two records can answer to the same name.
//!
//! The property under test is always the same: after any number of rewrites
//! and restarts, the next identifier is above every identifier ever issued.
#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use std::fs;
use std::path::{Path, PathBuf};

use storage::{Retain, Wal, WalLimits};

fn scratch(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "straum-identity-{name}-{}-{:?}.wal",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = fs::remove_file(&path);
    path
}

fn open(path: &Path) -> Wal {
    Wal::open(path, WalLimits::default()).unwrap().0
}

/// Stages `records` records and commits them, returning the last sequence.
fn append(wal: &mut Wal, from: usize, records: usize) -> u64 {
    for index in from..from + records {
        wal.stage(format!("record-{index}").as_bytes()).unwrap();
    }
    wal.commit().unwrap().0.durable_sequence
}

#[test]
fn a_sequence_that_existed_before_compaction_is_never_handed_out_again() {
    // The reproduction of C12, in the shape it was found in.
    let path = scratch("c12");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 5), 5);

    // Records 3, 4 and 5 go; 1 and 2 stay.
    let mut seen = 0;
    wal.compact(0, Retain::NONE, &mut |_| {
        seen += 1;
        seen <= 2
    })
    .unwrap();
    drop(wal);

    let (mut wal, recovery) = Wal::open(&path, WalLimits::default()).unwrap();
    // Two records physically, five ever allocated. Before STRAUMW3 both of
    // these read 2, and the next record was handed sequence 3 — a number
    // three other records had already had.
    assert_eq!(recovery.records, 2, "the file should hold two records");
    assert_eq!(recovery.sequence, 5, "five were allocated in this log");

    assert_eq!(wal.stage(b"after").unwrap(), 6);
    let _ = fs::remove_file(&path);
}

#[test]
fn a_correlation_that_existed_before_compaction_is_never_handed_out_again() {
    // The correlation is opaque to the log — it lives inside record bodies —
    // so the owner writes its high-water down and gets it back. Compaction
    // dropping every record that carried a high correlation must not lower
    // it.
    let path = scratch("correlation");
    let mut wal = open(&path);
    append(&mut wal, 0, 5);

    wal.compact(5_000, Retain::NONE, &mut |_| false).unwrap();
    drop(wal);

    let (mut wal, recovery) = Wal::open(&path, WalLimits::default()).unwrap();
    assert_eq!(recovery.records, 0, "nothing survived");
    assert_eq!(recovery.correlation, 5_000);

    // And a caller that does not track correlations cannot lower it.
    wal.compact(0, Retain::NONE, &mut |_| false).unwrap();
    drop(wal);
    assert_eq!(
        Wal::open(&path, WalLimits::default())
            .unwrap()
            .1
            .correlation,
        5_000
    );
    let _ = fs::remove_file(&path);
}

#[test]
fn an_empty_physical_log_still_knows_what_it_allocated() {
    let path = scratch("empty");
    let mut wal = open(&path);
    append(&mut wal, 0, 12);
    wal.compact(900, Retain::NONE, &mut |_| false).unwrap();
    drop(wal);

    let (mut wal, recovery) = Wal::open(&path, WalLimits::default()).unwrap();
    assert_eq!(recovery.records, 0);
    assert_eq!(recovery.sequence, 12);
    assert_eq!(recovery.correlation, 900);
    assert_eq!(wal.stage(b"next").unwrap(), 13);
    let _ = fs::remove_file(&path);
}

#[test]
fn repeated_compaction_never_rewinds_either_value() {
    let path = scratch("repeated");
    let mut written = 0usize;
    let mut sequence = 0u64;
    let mut correlation = 0u64;

    for round in 1..=5u64 {
        let (mut wal, recovery) = Wal::open(&path, WalLimits::default()).unwrap();
        assert_eq!(
            recovery.sequence, sequence,
            "round {round}: the log forgot what it had allocated"
        );
        assert_eq!(recovery.correlation, correlation, "round {round}");

        sequence = append(&mut wal, written, 4);
        written += 4;
        assert_eq!(sequence, written as u64, "round {round}");

        // Keep one record of the four, and raise the correlation each time.
        correlation = round * 1_000;
        let mut seen = 0;
        wal.compact(correlation, Retain::NONE, &mut |_| {
            seen += 1;
            seen == 1
        })
        .unwrap();
        drop(wal);
    }

    // Twenty allocated and one survives — each round keeps the first record
    // of the whole file — and the next one still continues after all twenty.
    let (mut wal, recovery) = Wal::open(&path, WalLimits::default()).unwrap();
    assert_eq!(recovery.records, 1);
    assert_eq!(recovery.sequence, 20);
    assert_eq!(recovery.correlation, 5_000);
    assert_eq!(wal.stage(b"twenty-one").unwrap(), 21);
    let _ = fs::remove_file(&path);
}

#[test]
fn every_sequence_ever_issued_stays_issued_across_rewrites() {
    // What replication will rest on: the same number must never name two
    // records over the life of the log (docs/replication.md §114).
    let path = scratch("unique");
    let mut issued: Vec<u64> = Vec::new();
    let mut written = 0usize;

    for round in 0..6 {
        let mut wal = open(&path);
        for index in written..written + 3 {
            issued.push(wal.stage(format!("record-{index}").as_bytes()).unwrap());
        }
        written += 3;
        wal.commit().unwrap();

        // Every other round throws almost everything away.
        if round % 2 == 1 {
            let mut seen = 0;
            wal.compact(0, Retain::NONE, &mut |_| {
                seen += 1;
                seen == 1
            })
            .unwrap();
        }
        drop(wal);
    }

    let mut unique = issued.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        issued.len(),
        "a sequence was handed out twice: {issued:?}"
    );
    assert!(
        issued.windows(2).all(|pair| pair[1] > pair[0]),
        "sequences went backwards: {issued:?}"
    );
    let _ = fs::remove_file(&path);
}

#[test]
fn a_damaged_header_is_refused_rather_than_read_as_a_smaller_number() {
    let path = scratch("damaged");
    let mut wal = open(&path);
    append(&mut wal, 0, 4);
    wal.compact(77, Retain::NONE, &mut |_| false).unwrap();
    drop(wal);

    let clean = fs::read(&path).unwrap();
    // Every byte of the allocation state, one at a time. A header that reads
    // as a plausible smaller number is the whole failure mode.
    for at in 8..storage::HEADER_BYTES {
        let mut damaged = clean.clone();
        damaged[at] ^= 0x01;
        fs::write(&path, &damaged).unwrap();
        assert!(
            matches!(
                Wal::open(&path, WalLimits::default()),
                Err(storage::StorageError::CorruptHeader)
            ),
            "byte {at} was flipped and the log opened anyway"
        );
    }
    let _ = fs::remove_file(&path);
}

#[test]
fn a_log_in_the_previous_format_is_refused_not_guessed_at() {
    // STRAUMW2 carried no allocation state, so continuing one would mean
    // inventing the number this whole header exists to remember.
    let path = scratch("straumw2");
    let mut old = b"STRAUMW2".to_vec();
    storage::encode_for_benchmarks(b"a record from before", &mut old);
    fs::write(&path, &old).unwrap();

    assert!(matches!(
        Wal::open(&path, WalLimits::default()),
        Err(storage::StorageError::NotAWal)
    ));
    let _ = fs::remove_file(&path);
}
