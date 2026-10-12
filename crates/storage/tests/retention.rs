//! The retention floor: where a sequence-addressed reader may start
//! (docs/replication.md §130).
//!
//! Compaction keeps a filtered set, not a prefix. Everything it leaves behind
//! is still a valid record and still recovers, but the file no longer says
//! which number any of those records was written under. So the log has two
//! regions, and the difference between them is not "present" and "absent" —
//! it is "addressable" and "not addressable".
//!
//! The one failure these tests exist to prevent is a reader deriving a
//! sequence from a position in the file. That was the live defect: a base of
//! `sequence - records` labels the survivors 8, 9, 10 when they were written
//! as 1, 5, 9, and a follower fed those frames diverges without an error
//! anywhere.
#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use std::fs;
use std::path::{Path, PathBuf};

use storage::{Retain, Wal, WalLimits, allocated, replay, replay_addressable};

/// A test's log file, removed when the test ends, passing or failing
/// (D-121): the assertion's message is the diagnostic, not the file.
struct Scratch(PathBuf);

impl std::ops::Deref for Scratch {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for Scratch {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn scratch(name: &str) -> Scratch {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "straum-retention-{name}-{}-{:?}.wal",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = fs::remove_file(&path);
    Scratch(path)
}

fn open(path: &Path) -> Wal {
    Wal::open(path, WalLimits::default()).unwrap().0
}

fn append(wal: &mut Wal, from: usize, records: usize) -> u64 {
    for index in from..from.saturating_add(records) {
        wal.stage(format!("record-{index}").as_bytes()).unwrap();
    }
    wal.commit().unwrap().0.durable_sequence
}

fn floor(path: &Path) -> u64 {
    allocated(path).unwrap().retention_floor()
}

/// Every sequence the log will hand to a reader, with the body it names.
fn addressable(path: &Path) -> Vec<(u64, Vec<u8>)> {
    let mut seen = Vec::new();
    replay_addressable(
        path,
        WalLimits::default(),
        &mut |_| true,
        &mut |sequence, framed| seen.push((sequence, framed.to_vec())),
    )
    .unwrap();
    seen
}

/// Every record body still physically in the file, addressable or not.
fn present(path: &Path) -> Vec<Vec<u8>> {
    let mut seen = Vec::new();
    replay(path, WalLimits::default(), &mut |payload| {
        seen.push(payload.to_vec())
    })
    .unwrap();
    seen
}

#[test]
fn an_untouched_log_is_addressable_from_one() {
    let path = scratch("untouched");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 4), 4);
    drop(wal);

    assert_eq!(floor(&path), 1);
    let sequences: Vec<u64> = addressable(&path).into_iter().map(|(seq, _)| seq).collect();
    assert_eq!(sequences, vec![1, 2, 3, 4]);
}

#[test]
fn a_compacted_region_has_no_sequences_at_all() {
    // The exact case the defect was found in: ten records, three survive,
    // and the survivors are neither the first three nor contiguous.
    let path = scratch("filtered");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 10), 10);

    // Keeps the records written as sequences 1, 5 and 9.
    let mut index = 0u64;
    wal.compact(0, Retain::NONE, &mut |_| {
        index = index.saturating_add(1);
        matches!(index, 1 | 5 | 9)
    })
    .unwrap();
    drop(wal);

    // Three records are still in the file and still recover.
    assert_eq!(present(&path).len(), 3);

    // None of them can be named. The floor is above everything the log has
    // ever allocated, not above the survivors: `sequence - records` would
    // have said 8 here, which is a number record 1 never had.
    assert_eq!(floor(&path), 11);
    assert!(addressable(&path).is_empty());
}

#[test]
fn records_appended_after_a_rewrite_continue_the_numbering() {
    let path = scratch("continue");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 10), 10);
    let mut index = 0u64;
    wal.compact(0, Retain::NONE, &mut |_| {
        index = index.saturating_add(1);
        matches!(index, 1 | 5 | 9)
    })
    .unwrap();

    // The first record written after the rewrite is 11, which is the floor.
    assert_eq!(append(&mut wal, 100, 3), 13);
    drop(wal);

    assert_eq!(floor(&path), 11);
    let seen = addressable(&path);
    let sequences: Vec<u64> = seen.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(sequences, vec![11, 12, 13]);

    // And each sequence names the body that was written under it: the
    // survivors must not have shifted the numbering by three.
    for (offset, (_, framed)) in seen.iter().enumerate() {
        let body = format!("record-{}", 100usize.saturating_add(offset));
        assert!(
            framed.windows(body.len()).any(|w| w == body.as_bytes()),
            "sequence {} does not carry {body}",
            seen[offset].0
        );
    }
}

#[test]
fn the_boundary_record_belongs_to_the_compacted_region() {
    // Off by one here is the whole bug in miniature: the last record a
    // rewrite kept sits at `header.records`, and calling it addressable
    // would give it the header's own sequence.
    let path = scratch("boundary");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 3), 3);
    wal.compact(0, Retain::NONE, &mut |payload| payload == b"record-2")
        .unwrap();
    drop(wal);

    assert_eq!(present(&path).len(), 1);
    assert_eq!(floor(&path), 4);
    assert!(addressable(&path).is_empty());
}

#[test]
fn the_floor_only_ever_rises() {
    let path = scratch("monotonic");
    let mut wal = open(&path);
    let mut previous = floor(&path);
    assert_eq!(previous, 1);

    for round in 0..5 {
        append(&mut wal, round * 10, 6);
        // Keep the middle two of whatever is there, so the survivor count
        // moves in both directions across rounds.
        let mut index = 0u64;
        wal.compact(0, Retain::NONE, &mut |_| {
            index = index.saturating_add(1);
            matches!(index, 3 | 4)
        })
        .unwrap();

        let now = allocated(&path).unwrap().retention_floor();
        assert!(
            now >= previous,
            "round {round}: floor went backwards, {previous} -> {now}"
        );
        previous = now;

        // A reopened log agrees with the file: the floor is written down,
        // not derived from what happens to be in memory.
        drop(wal);
        assert_eq!(floor(&path), previous);
        wal = open(&path);
    }
}

#[test]
fn a_reader_that_refuses_the_floor_is_handed_no_records() {
    let path = scratch("refused");
    let mut wal = open(&path);
    append(&mut wal, 0, 4);
    drop(wal);

    let mut seen_floor = None;
    let mut records = 0usize;
    replay_addressable(
        &path,
        WalLimits::default(),
        &mut |floor| {
            seen_floor = Some(floor);
            false
        },
        &mut |_, _| records = records.saturating_add(1),
    )
    .unwrap();

    // The floor arrives before the first record, which is what lets a caller
    // refuse a range against the same file it would otherwise be reading.
    assert_eq!(seen_floor, Some(1));
    assert_eq!(records, 0);
}

#[test]
fn a_log_that_never_existed_has_a_floor_and_no_records() {
    let path = scratch("missing");
    assert_eq!(floor(&path), 1);
    assert!(addressable(&path).is_empty());
}

#[test]
fn a_rewrite_during_a_read_cannot_change_what_the_read_is_answering() {
    // The floor and the records have to come out of one file, or a caller
    // validates a range against one log and then reads another. Nothing here
    // waits for a duration: the rewrite happens while the reader is stopped
    // inside `ready`, which is precisely the window that matters.
    use std::sync::mpsc;

    let path = scratch("race");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 6), 6);

    let (reached, floor_seen) = mpsc::channel();
    let (rewritten, proceed) = mpsc::channel();

    let reading = path.to_path_buf();
    let reader = std::thread::spawn(move || {
        let mut seen = Vec::new();
        let mut floor = 0;
        replay_addressable(
            &reading,
            WalLimits::default(),
            &mut |value| {
                floor = value;
                reached.send(value).unwrap();
                proceed.recv().unwrap();
                true
            },
            &mut |sequence, _| seen.push(sequence),
        )
        .unwrap();
        (floor, seen)
    });

    assert_eq!(floor_seen.recv().unwrap(), 1);

    // Everything the reader is about to walk is compacted away underneath it.
    let mut index = 0u64;
    wal.compact(0, Retain::NONE, &mut |_| {
        index = index.saturating_add(1);
        index == 2
    })
    .unwrap();
    rewritten.send(()).unwrap();

    let (checked, seen) = reader.join().unwrap();
    assert_eq!(checked, 1);
    assert_eq!(
        seen,
        vec![1, 2, 3, 4, 5, 6],
        "the read answered a different log than the check"
    );

    // And the next reader is told the truth about the log as it now is.
    drop(wal);
    assert_eq!(floor(&path), 7);
    assert!(addressable(&path).is_empty());
}

/// Keeps the records written as 1, 5 and 9 below the suffix, when they fall
/// below it at all.
fn odd_ones() -> impl FnMut(&[u8]) -> bool {
    let mut index = 0u64;
    move |_| {
        index = index.saturating_add(1);
        matches!(index, 1 | 5 | 9)
    }
}

#[test]
fn a_retained_suffix_keeps_the_numbers_it_was_written_under() {
    let path = scratch("suffix");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 10), 10);
    let before = addressable(&path);

    let outcome = wal
        .compact(0, Retain { from: 7, bytes: 0 }, &mut odd_ones())
        .unwrap();
    assert_eq!(outcome.floor, 7);

    // Same sequences, same framed bytes: what a follower is sent from the
    // suffix is exactly what it would have been sent before the rewrite.
    assert_eq!(floor(&path), 7);
    assert_eq!(addressable(&path), before[6..].to_vec());
    // Below the suffix the filter still decided: 1 and 5 kept, 9 is in the
    // suffix anyway.
    assert_eq!(present(&path).len(), 2 + 4);

    // Appends continue the numbering after the suffix, not after the
    // survivors.
    assert_eq!(append(&mut wal, 100, 2), 12);
    drop(wal);
    let sequences: Vec<u64> = addressable(&path).iter().map(|(seq, _)| *seq).collect();
    assert_eq!(sequences, vec![7, 8, 9, 10, 11, 12]);

    // And a reopen agrees with the writer about where it stands.
    let (_, recovery) = Wal::open(&path, WalLimits::default()).unwrap();
    assert_eq!(recovery.sequence, 12);
}

#[test]
fn a_byte_bound_keeps_the_longest_suffix_that_fits() {
    let path = scratch("bytes");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 10), 10);
    // Every record is "record-N" plus the same framing, so they are all one
    // size; four of them fit, five do not.
    let size = u64::try_from(addressable(&path)[0].1.len()).unwrap();

    let outcome = wal
        .compact(
            0,
            Retain {
                from: u64::MAX,
                bytes: 4 * size + size - 1,
            },
            &mut |_| false,
        )
        .unwrap();
    drop(wal);

    assert_eq!(outcome.floor, 7);
    assert_eq!(outcome.retained_bytes, 4 * size);
    let sequences: Vec<u64> = addressable(&path).iter().map(|(seq, _)| *seq).collect();
    assert_eq!(sequences, vec![7, 8, 9, 10]);
    // What recovery reports as addressable is what the rewrite kept.
    let (_, recovery) = Wal::open(&path, WalLimits::default()).unwrap();
    assert_eq!(
        recovery.durable_offset - recovery.addressable_offset,
        outcome.retained_bytes
    );
}

#[test]
fn the_floor_never_moves_down() {
    let path = scratch("monotonic");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 10), 10);
    wal.compact(0, Retain { from: 7, bytes: 0 }, &mut |_| false)
        .unwrap();

    // A later rewrite asks for more than the log still has a name for. The
    // records below 7 lost their numbers in the first rewrite and cannot be
    // given them back, so the floor stays where it was.
    let outcome = wal
        .compact(
            0,
            Retain {
                from: 1,
                bytes: u64::MAX,
            },
            &mut |_| true,
        )
        .unwrap();
    assert_eq!(outcome.floor, 7);
    drop(wal);
    assert_eq!(floor(&path), 7);
    let sequences: Vec<u64> = addressable(&path).iter().map(|(seq, _)| *seq).collect();
    assert_eq!(sequences, vec![7, 8, 9, 10]);
}

#[test]
fn retaining_nothing_is_the_old_rewrite() {
    let path = scratch("none");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 10), 10);
    let outcome = wal.compact(0, Retain::NONE, &mut odd_ones()).unwrap();
    drop(wal);
    assert_eq!(outcome.floor, 11);
    assert_eq!(outcome.retained_bytes, 0);
    assert!(addressable(&path).is_empty());
    assert_eq!(present(&path).len(), 3);
}

/// A read stops where the file ended when the read began. Catch-up reads
/// the log a writer is appending to; a scan that read until `read` came
/// back empty chased the tail under load and never finished, so nothing
/// was shipped and a follower that stopped reading was never cut off
/// (D-76, `replication_matrix` test 41 in the Linux container).
#[test]
fn a_read_ends_where_the_file_ended_when_it_began() {
    let path = scratch("growing");
    let mut wal = open(&path);
    assert_eq!(append(&mut wal, 0, 4), 4);
    let mut seen = 0;
    replay_addressable(&path, WalLimits::default(), &mut |_| true, &mut |_, _| {
        seen += 1;
        // Every record read makes another, as a busy leader does; bounded
        // so a scan that chases them fails instead of running for ever.
        if seen < 1000 {
            append(&mut wal, 100 + seen, 1);
        }
    })
    .unwrap();
    assert_eq!(seen, 4, "the read took records appended after it began");
    drop(wal);
    let _ = fs::remove_file(&path);
}
