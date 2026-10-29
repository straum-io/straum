//! Recovery of the shared index segments (docs/format.md, "Index segments"):
//! they are derived from the WAL and never synced, so every kind of damage
//! to them is repaired from the WAL and reported, in every write mode;
//! records of WAL frames that recovery cut stay stale for good.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use std::path::Path;

use common::{
    INDEX_BLOCK_RECORD, TempDir, edit, find_index_record, index_segments, snapshot,
    wal_frame_starts,
};
use straum_store::{Config, Message, RecoveryAction, Store, WriteMode};

fn payload(tag: u8, i: u64) -> Vec<u8> {
    format!("{tag}-{i}").into_bytes()
}

/// Writes messages `from..to` of `tag` to topic "t", one batch.
async fn put(store: &mut Store, tag: u8, from: u64, to: u64) {
    let bodies: Vec<Vec<u8>> = (from..to).map(|i| payload(tag, i)).collect();
    let batch: Vec<Message> = bodies
        .iter()
        .map(|p| Message {
            topic: b"t",
            payload: p,
            ..Default::default()
        })
        .collect();
    store.put_batch(&batch, from).await.unwrap();
}

async fn assert_reads(store: &Store, tags: &[(u8, std::ops::Range<u64>)]) {
    for (tag, range) in tags {
        for seq in range.clone() {
            let got = store.read(b"t", seq).await.unwrap();
            assert_eq!(got, Some(payload(*tag, seq)), "seq {seq}");
        }
    }
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
        [RecoveryAction::StaleIndexRecordsFenced {
            records: 1,
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
    let before = snapshot(&dir.0);
    let err = Store::open(&dir.0, config).await.err().unwrap();
    assert!(
        matches!(err, straum_store::StoreError::TopicMismatch { seq: 60, .. }),
        "{err}"
    );
    assert!(snapshot(&dir.0) == before, "files changed");

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
    for seq in 0..70 {
        let want = payload(1, if seq < 60 { seq } else { seq - 60 });
        assert_eq!(store.read(b"t", seq).await.unwrap(), Some(want), "{seq}");
    }
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
