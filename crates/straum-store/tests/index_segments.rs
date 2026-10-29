//! Recovery of the shared index segments (docs/format.md, "Index segments"):
//! they are derived from the WAL and never synced, so every kind of damage
//! to them is repaired from the WAL and reported, in every write mode;
//! records of WAL frames that recovery cut stay stale for good.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use std::path::Path;

use common::{
    INDEX_ATTRS_RECORD, INDEX_BLOCK_RECORD, TempDir, edit, find_index_record, index_segments,
    reseal_index_record, snapshot, wal_frame_starts,
};
use straum_store::{Config, Message, RecoveryAction, Store, StoreError, WriteMode};

fn payload(tag: u8, i: u64) -> Vec<u8> {
    format!("{tag}-{i}").into_bytes()
}

/// Every third message has a key and a header (in the index's attribute
/// records once its block is full).
fn key(tag: u8, i: u64) -> Option<Vec<u8>> {
    i.is_multiple_of(3)
        .then(|| format!("k{tag}-{i}").into_bytes())
}

const HEADERS: [(&[u8], &[u8]); 1] = [(b"h", b"v")];

/// Writes messages `from..to` of `tag` to topic "t", one batch.
async fn put(store: &mut Store, tag: u8, from: u64, to: u64) {
    let bodies: Vec<(Vec<u8>, Option<Vec<u8>>)> =
        (from..to).map(|i| (payload(tag, i), key(tag, i))).collect();
    let batch: Vec<Message> = bodies
        .iter()
        .map(|(p, k)| Message {
            topic: b"t",
            payload: p,
            key: k.as_deref(),
            headers: if k.is_some() { &HEADERS } else { &[] },
        })
        .collect();
    store.put_batch(&batch, from).await.unwrap();
}

/// Each seq's payload, key and headers; message `seq` of `tag` was written
/// as message `seq - offset`.
async fn assert_entries(store: &Store, tags: &[(u8, std::ops::Range<u64>, u64)]) {
    for (tag, range, offset) in tags {
        for seq in range.clone() {
            let got = store.read_entry(b"t", seq).await.unwrap().unwrap();
            let key = key(*tag, seq - offset);
            let headers = if key.is_some() {
                vec![(b"h".to_vec(), b"v".to_vec())]
            } else {
                vec![]
            };
            assert_eq!(
                (got.payload, got.key, got.headers),
                (payload(*tag, seq - offset), key, headers),
                "seq {seq}"
            );
        }
    }
}

async fn assert_reads(store: &Store, tags: &[(u8, std::ops::Range<u64>)]) {
    let tags: Vec<_> = tags.iter().map(|(t, r)| (*t, r.clone(), 0)).collect();
    assert_entries(store, &tags).await;
}

/// A WAL tail cut by recovery took the frame that completed block 0, whose
/// record was already written. New frames reuse that frame number and write
/// block 0 again with other entries; the old record must never be taken
/// for a record of the new frames (it would disagree with the WAL).
#[compio::test]
async fn records_of_a_cut_wal_tail_stay_stale() {
    let dir = TempDir::new("index-stale");
    let config = Config::default();
    {
        let mut store = Store::open(&dir.0, config).await.unwrap();
        put(&mut store, 1, 0, 60).await;
        // Frame 1 completes block 0: its record has stamp 2.
        put(&mut store, 1, 60, 70).await;
    }
    assert_eq!(find_index_record(&dir.0, INDEX_BLOCK_RECORD, 0, 0).stamp, 2);
    let wal = dir.0.join(common::WAL0);
    let bytes = std::fs::read(&wal).unwrap();
    let last = *wal_frame_starts(&bytes).last().unwrap();
    std::fs::write(&wal, &bytes[..last + 10]).unwrap();

    let mut store = Store::open(&dir.0, config).await.unwrap();
    let actions = &store.recovery_report().actions;
    assert!(
        matches!(
            actions[0],
            RecoveryAction::TornTailTruncated { frame_seq: 1, .. }
        ),
        "{actions:?}"
    );
    assert_eq!(
        actions[1..],
        // Block 0's record and its attribute record.
        [RecoveryAction::StaleIndexRecordsFenced {
            records: 2,
            segment: 1
        }]
    );
    assert_eq!(store.topic_range(b"t"), Some((0, 60)));
    // Frame 1 again, completing block 0 with other entries.
    put(&mut store, 2, 60, 70).await;
    drop(store);
    for round in 0..2 {
        let store = Store::open(&dir.0, config).await.unwrap();
        assert!(
            store.recovery_report().is_empty(),
            "round {round}: {:?}",
            store.recovery_report()
        );
        assert_reads(&store, &[(1, 0..60), (2, 60..70)]).await;
    }
}

/// The documented limit: the fence's header is the only proof that the old
/// record is stale. Damaged after new frames reused its frame number, open
/// fails with `TopicMismatch` and changes nothing.
#[compio::test]
async fn a_damaged_fence_header_fails_open() {
    let dir = TempDir::new("index-fence-damaged");
    let config = Config::default();
    {
        let mut store = Store::open(&dir.0, config).await.unwrap();
        put(&mut store, 1, 0, 60).await;
        put(&mut store, 1, 60, 70).await;
    }
    let wal = dir.0.join(common::WAL0);
    let bytes = std::fs::read(&wal).unwrap();
    let last = *wal_frame_starts(&bytes).last().unwrap();
    std::fs::write(&wal, &bytes[..last + 10]).unwrap();
    {
        // Repeats of messages 0..10 as entries 60..70: other object
        // numbers than the stale record's.
        let mut store = Store::open(&dir.0, config).await.unwrap();
        put(&mut store, 1, 0, 10).await;
    }
    edit(&index_segments(&dir.0)[1], |b| b[13] ^= 1);
    let clean = snapshot(&dir.0);
    // Repairs due as well (a torn tail, a tmp file, an orphan segment):
    // the index check fails before any of them is made.
    edit(&wal, |b| b.extend_from_slice(&[0xa5; 10]));
    let orphan = dir.0.join("seg-00000000000000ff.dat");
    let tmp = dir.0.join(format!("{}.tmp", common::WAL0));
    std::fs::write(&orphan, b"orphan").unwrap();
    std::fs::write(&tmp, b"half-created").unwrap();
    let before = snapshot(&dir.0);
    let err = Store::open(&dir.0, config).await.err().unwrap();
    assert!(
        matches!(err, straum_store::StoreError::TopicMismatch { seq: 60, .. }),
        "{err}"
    );
    assert!(snapshot(&dir.0) == before, "files changed");
    edit(&wal, |b| b.truncate(b.len() - 10));
    std::fs::remove_file(&orphan).unwrap();
    std::fs::remove_file(&tmp).unwrap();
    let before = clean;

    // The operator's way out (D-229 A): rebuild the index from the WAL.
    let segments = index_segments(&dir.0);
    let rebuild = Config {
        rebuild_index: true,
        ..config
    };
    let store = Store::open(&dir.0, rebuild).await.unwrap();
    let actions = &store.recovery_report().actions;
    let dropped = actions
        .iter()
        .filter(|a| matches!(a, RecoveryAction::IndexSegmentDropped { .. }))
        .count();
    assert_eq!(dropped, segments.len(), "{actions:?}");
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, RecoveryAction::TopicIndexRepaired { .. })),
        "{actions:?}"
    );
    assert_entries(&store, &[(1, 0..60, 0), (1, 60..70, 60)]).await;
    drop(store);
    let after = snapshot(&dir.0);
    for (path, bytes) in &before {
        let name = path.file_name().unwrap().to_str().unwrap();
        if !name.starts_with("idx-") {
            assert_eq!(after.get(path), Some(bytes), "{name} changed");
        }
    }
    let store = Store::open(&dir.0, config).await.unwrap();
    assert!(
        store.recovery_report().is_empty(),
        "{:?}",
        store.recovery_report()
    );
}

fn modes() -> [WriteMode; 3] {
    [
        WriteMode::Sequential,
        WriteMode::ParallelSync,
        WriteMode::INLINE_SMALL,
    ]
}

/// Three segments of index records (blocks 0 to 9 with pages of 2).
async fn build(dir: &Path, mode: WriteMode) -> Config {
    let config = Config {
        write_mode: mode,
        index_segment_max_bytes: 400,
        seek_page_entries: 2,
        ..Config::default()
    };
    let mut store = Store::open(dir, config).await.unwrap();
    for b in 0..10 {
        put(&mut store, 1, b * 64, (b + 1) * 64).await;
    }
    put(&mut store, 1, 640, 650).await;
    assert!(index_segments(dir).len() >= 3);
    config
}

type Damage = fn(&Path);

/// Every damage leaves the WAL and data segments alone, is repaired from
/// the WAL and reported, and the next open is clean.
#[compio::test]
async fn damaged_index_segments_are_repaired_and_reported() {
    let cases: [(&str, Damage); 5] = [
        ("middle segment deleted", |d| {
            std::fs::remove_file(&index_segments(d)[1]).unwrap()
        }),
        ("every segment deleted", |d| {
            for p in index_segments(d) {
                std::fs::remove_file(p).unwrap();
            }
        }),
        ("header damaged", |d| {
            edit(&index_segments(d)[0], |b| b[10] ^= 1)
        }),
        ("segment cut inside a record", |d| {
            edit(&index_segments(d)[1], |b| b.truncate(b.len() - 7))
        }),
        ("unsynced pages zeroed", |d| {
            edit(&index_segments(d)[2], |b| {
                let n = b.len();
                b[30..n].fill(0)
            })
        }),
    ];
    for mode in modes() {
        for (what, damage) in cases {
            let dir = TempDir::new("index-damage");
            let config = build(&dir.0, mode).await;
            damage(&dir.0);
            let wal_before = std::fs::read(dir.0.join(common::WAL0)).unwrap();
            let store = Store::open(&dir.0, config).await.unwrap();
            let actions = &store.recovery_report().actions;
            assert!(
                actions
                    .iter()
                    .any(|a| matches!(a, RecoveryAction::TopicIndexRepaired { .. })),
                "{mode:?} {what}: {actions:?}"
            );
            assert_eq!(
                std::fs::read(dir.0.join(common::WAL0)).unwrap(),
                wal_before,
                "{mode:?} {what}"
            );
            assert_reads(&store, &[(1, 0..650)]).await;
            drop(store);
            let before = snapshot(&dir.0);
            let store = Store::open(&dir.0, config).await.unwrap();
            assert!(
                store.recovery_report().is_empty(),
                "{mode:?} {what}: {:?}",
                store.recovery_report()
            );
            assert!(snapshot(&dir.0) == before, "{mode:?} {what}: files changed");
        }
    }
}

/// Keys and headers of a full block live in its attribute record, whose CRC
/// is checked on every read (D-235 A, D-250 A): damage after open is an
/// error for that entry, never wrong data, and the store stays usable.
#[compio::test]
async fn a_damaged_attribute_record_fails_the_read() {
    let dir = TempDir::new("index-attrs-read");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    put(&mut store, 1, 0, 130).await;
    let rec = find_index_record(&dir.0, INDEX_ATTRS_RECORD, 0, 1);
    // A byte of a key ("k1-…") of block 1.
    edit(&rec.path, |b| {
        let at = rec.body.start + b[rec.body.clone()].iter().position(|&x| x == b'k').unwrap();
        b[at] ^= 1;
    });
    let err = store.read_entry(b"t", 66).await.unwrap_err();
    assert!(
        matches!(err, StoreError::TopicIndexCorrupt { block: 1, .. }),
        "{err}"
    );
    // Its payload, block 0 and the incomplete block still read.
    assert_eq!(store.read(b"t", 66).await.unwrap(), Some(payload(1, 66)));
    assert_reads(&store, &[(1, 0..64), (1, 128..130)]).await;
    put(&mut store, 1, 130, 131).await;
}

/// At open the attribute records are checked against the WAL like their
/// blocks: a damaged one is re-derived and reported, one that is intact
/// but disagrees fails open with `TopicMismatch` and changes no file.
#[compio::test]
async fn attribute_records_are_checked_at_open() {
    let config = Config::default();
    let fresh = |tag: &'static str| async move {
        let dir = TempDir::new(tag);
        let mut store = Store::open(&dir.0, config).await.unwrap();
        put(&mut store, 1, 0, 130).await;
        drop(store);
        dir
    };
    // Damaged (CRC fails): the segment is read up to it, and the blocks
    // from there on are repaired from the WAL.
    let dir = fresh("index-attrs-damaged").await;
    let rec = find_index_record(&dir.0, INDEX_ATTRS_RECORD, 0, 0);
    edit(&rec.path, |b| b[rec.body.end - 1] ^= 1);
    let wal = std::fs::read(dir.0.join(common::WAL0)).unwrap();
    let store = Store::open(&dir.0, config).await.unwrap();
    assert!(
        store.recovery_report().actions.iter().any(|a| matches!(
            a,
            RecoveryAction::TopicIndexRepaired {
                blocks_rewritten: 2,
                ..
            }
        )),
        "{:?}",
        store.recovery_report()
    );
    assert_reads(&store, &[(1, 0..130)]).await;
    drop(store);
    assert_eq!(std::fs::read(dir.0.join(common::WAL0)).unwrap(), wal);
    let store = Store::open(&dir.0, config).await.unwrap();
    assert!(store.recovery_report().is_empty());
    drop(store);

    type Change = fn(&Path);
    let cases: [(&str, Change, u64); 2] = [
        // Another key byte, CRC recomputed: written that way.
        (
            "an intact attribute record that disagrees",
            |d| {
                let rec = find_index_record(d, INDEX_ATTRS_RECORD, 0, 1);
                edit(&rec.path, |b| {
                    let at = rec.body.start
                        + b[rec.body.clone()].iter().position(|&x| x == b'k').unwrap();
                    b[at] ^= 1;
                    reseal_index_record(b, rec.offset, rec.size);
                })
            },
            66,
        ),
        // The block record without its attribute record's location.
        (
            "a block record that lost its attribute location",
            |d| {
                let rec = find_index_record(d, INDEX_BLOCK_RECORD, 0, 1);
                edit(&rec.path, |b| {
                    let loc = rec.body.end - 16;
                    // Shorten the body by 16 bytes: `len` is one byte here.
                    b[rec.offset + 1] -= 16;
                    b.drain(loc..rec.body.end);
                    reseal_index_record(b, rec.offset, rec.size - 16);
                })
            },
            66,
        ),
    ];
    for (what, change, seq) in cases {
        let dir = fresh("index-attrs-mismatch").await;
        change(&dir.0);
        let before = snapshot(&dir.0);
        let err = Store::open(&dir.0, config).await.err().unwrap();
        assert!(
            matches!(err, StoreError::TopicMismatch { seq: s, .. } if s == seq),
            "{what}: {err}"
        );
        assert!(snapshot(&dir.0) == before, "{what}: files changed");
    }
}
