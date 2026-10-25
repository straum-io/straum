//! Directory states a SIGKILL cannot produce, built directly: the last WAL
//! frame is durable but the segment bytes it introduces are missing or
//! wrong, or an object of an earlier, acknowledged frame is gone. Run for
//! every write mode. Only a ParallelSync last frame may be durable before
//! its objects: it is dropped and reported. In Sequential and InlineSmall
//! (segment objects synced first) the same state is corruption: an error,
//! and no file changes.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use std::path::{Path, PathBuf};

use std::collections::BTreeMap;

use common::{TempDir, copy_dir, snapshot, wal_frame_starts};
use straum_store::{
    Config, DropReason, FrameMode, Message, ObjectId, RecoveryAction, Store, StoreError, WriteMode,
};

const BATCHES: u8 = 4;
/// Changes a store directory in place.
type Damage = fn(&Path);
/// Segment of the object the last frame introduces.
const LAST: u64 = BATCHES as u64 - 1;
/// Above the InlineSmall threshold used below: these objects go to segments.
const PAYLOAD: usize = 5000;

fn modes() -> [WriteMode; 3] {
    [
        WriteMode::Sequential,
        WriteMode::ParallelSync,
        WriteMode::INLINE_SMALL,
    ]
}

fn config(mode: WriteMode) -> Config {
    Config {
        // Every object gets a segment of its own: object i is in segment i.
        segment_max_bytes: 1,
        write_mode: mode,
        ..Config::default()
    }
}

fn payload(i: u8) -> Vec<u8> {
    vec![i; PAYLOAD]
}

fn segment(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("seg-{id:016x}.dat"))
}

/// One batch per frame, one new object per batch.
async fn build(dir: &Path, mode: WriteMode) {
    let mut store = Store::open(dir, config(mode)).await.unwrap();
    for i in 0..BATCHES {
        let p = payload(i);
        let batch = [Message {
            topic: b"t",
            payload: &p,
            ..Default::default()
        }];
        store.put_batch(&batch, u64::from(i)).await.unwrap();
    }
    assert_eq!(store.segment_count(), usize::from(BATCHES));
}

/// Applies `damage` to a copy of a freshly built store and opens it.
/// A damaged copy of a freshly built store, opened.
struct Opened {
    res: Result<Store, StoreError>,
    /// Directory contents right before and right after `Store::open`.
    files_before: BTreeMap<PathBuf, Vec<u8>>,
    files_after: BTreeMap<PathBuf, Vec<u8>>,
    wal_before: Vec<u8>,
    wal_after: Vec<u8>,
    work: TempDir,
}

async fn open_damaged(mode: WriteMode, damage: impl FnOnce(&Path)) -> Opened {
    let src = TempDir::new("fault-src");
    build(&src.0, mode).await;
    let work = TempDir::new("fault");
    copy_dir(&src.0, &work.0);
    damage(&work.0);
    let files_before = snapshot(&work.0);
    let wal_before = std::fs::read(work.0.join("wal.log")).unwrap();
    let res = Store::open(&work.0, config(mode)).await;
    let files_after = snapshot(&work.0);
    let wal_after = std::fs::read(work.0.join("wal.log")).unwrap();
    Opened {
        res,
        files_before,
        files_after,
        wal_before,
        wal_after,
        work,
    }
}

async fn assert_last_frame_dropped(store: &Store, wal_before: &[u8], wal_after: &[u8], what: &str) {
    let last = *wal_frame_starts(wal_before).last().unwrap();
    assert_eq!(
        wal_after,
        &wal_before[..last],
        "{what}: WAL truncated at the last frame"
    );
    let n = u64::from(BATCHES);
    assert_eq!(store.topic_range(b"t"), Some((0, n - 1)), "{what}");
    for i in 0..BATCHES - 1 {
        let got = store.read(b"t", u64::from(i)).await.unwrap();
        assert_eq!(got, Some(payload(i)), "{what}: earlier frame {i}");
    }
}

/// The per-mode rule for a last frame whose object is missing or wrong.
async fn assert_last_frame_outcome(mode: WriteMode, o: Opened, reason: DropReason, what: &str) {
    let object = ObjectId::of(&payload(BATCHES - 1));
    if mode == WriteMode::ParallelSync {
        let store = o.res.unwrap_or_else(|e| panic!("{what}: {e}"));
        assert_last_frame_dropped(&store, &o.wal_before, &o.wal_after, what).await;
        let last = *wal_frame_starts(&o.wal_before).last().unwrap();
        let actions = &store.recovery_report().actions;
        assert_eq!(
            actions.first(),
            Some(&RecoveryAction::FrameDropped {
                frame_seq: u64::from(BATCHES) - 1,
                offset: last as u64,
                mode: FrameMode::ParallelSync,
                object,
                reason,
                bytes_removed: (o.wal_before.len() - last) as u64,
            }),
            "{what}: {actions:?}"
        );
        assert!(
            !segment(&o.work.0, LAST).exists(),
            "{what}: orphan segment deleted"
        );
    } else {
        match (o.res, reason) {
            (Err(StoreError::MissingObject(id)), DropReason::ObjectMissing)
            | (Err(StoreError::HashMismatch(id)), DropReason::ObjectHashMismatch) => {
                assert_eq!(id, object, "{what}");
            }
            (res, _) => panic!("{what}: expected corruption error, got {:?}", res.err()),
        }
        assert!(o.files_after == o.files_before, "{what}: files changed");
    }
}

#[compio::test]
async fn last_frame_with_missing_segment_bytes() {
    for mode in modes() {
        let cases: [(&str, Damage); 3] = [
            ("segment file gone", |d| {
                std::fs::remove_file(segment(d, LAST)).unwrap()
            }),
            ("segment empty", |d| {
                std::fs::write(segment(d, LAST), b"").unwrap()
            }),
            ("segment half written", |d| {
                let f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(segment(d, LAST))
                    .unwrap();
                f.set_len(PAYLOAD as u64 / 2).unwrap();
            }),
        ];
        for (what, damage) in cases {
            let what = format!("{mode:?}: {what}");
            let o = open_damaged(mode, damage).await;
            assert_last_frame_outcome(mode, o, DropReason::ObjectMissing, &what).await;
        }
    }
}

#[compio::test]
async fn last_frame_with_wrong_segment_bytes() {
    for mode in modes() {
        let what = format!("{mode:?}: wrong bytes");
        let o = open_damaged(mode, |d| {
            std::fs::write(segment(d, LAST), vec![0xee; PAYLOAD]).unwrap();
        })
        .await;
        assert_last_frame_outcome(mode, o, DropReason::ObjectHashMismatch, &what).await;
    }
}

/// A torn frame after the last complete one means that frame was
/// acknowledged (the next one only starts after it). Its missing object is
/// corruption even in ParallelSync.
#[compio::test]
async fn parallel_sync_frame_followed_by_torn_tail_is_not_dropped() {
    let o = open_damaged(WriteMode::ParallelSync, |d| {
        std::fs::remove_file(segment(d, LAST)).unwrap();
        let mut wal = std::fs::read(d.join("wal.log")).unwrap();
        wal.extend_from_slice(&[0x5a; 30]);
        std::fs::write(d.join("wal.log"), wal).unwrap();
    })
    .await;
    assert!(
        matches!(o.res, Err(StoreError::MissingObject(_))),
        "{:?}",
        o.res.err()
    );
    assert!(o.files_after == o.files_before, "files changed");
}

#[compio::test]
async fn earlier_frame_object_missing_is_an_error() {
    for mode in modes() {
        for (what, damage) in [
            (
                "file gone",
                (|d: &Path| std::fs::remove_file(segment(d, 1)).unwrap()) as fn(&Path),
            ),
            ("file truncated", |d: &Path| {
                std::fs::write(segment(d, 1), b"short").unwrap()
            }),
        ] {
            let what = format!("{mode:?}: earlier object {what}");
            let o = open_damaged(mode, damage).await;
            assert!(
                matches!(o.res, Err(StoreError::MissingObject(_))),
                "{what}: {:?}",
                o.res.err()
            );
            assert!(o.files_after == o.files_before, "{what}: files changed");
        }
    }
}

/// Content of an earlier object is not hashed at open (that would read
/// every object); a mismatch is reported when the object is read.
#[compio::test]
async fn earlier_frame_object_with_wrong_bytes_fails_on_read() {
    for mode in modes() {
        let o = open_damaged(mode, |d| {
            std::fs::write(segment(d, 1), vec![0xee; PAYLOAD]).unwrap();
        })
        .await;
        let store = o.res.unwrap();
        assert!(
            store.recovery_report().is_empty(),
            "{mode:?}: nothing repaired"
        );
        let err = store.read(b"t", 1).await.unwrap_err();
        assert!(
            matches!(err, StoreError::HashMismatch(_)),
            "{mode:?}: {err}"
        );
    }
}

/// InlineSmall: the last frame's inline bytes are covered by the frame CRC.
/// Damaging them is a torn last frame; earlier inline objects stay readable.
#[compio::test]
async fn inline_small_damaged_last_frame_is_dropped() {
    let dir = TempDir::new("fault-inline");
    let config = Config {
        write_mode: WriteMode::INLINE_SMALL,
        ..Config::default()
    };
    {
        let mut store = Store::open(&dir.0, config).await.unwrap();
        for i in 0..BATCHES {
            let p = vec![i; 100];
            let batch = [Message {
                topic: b"t",
                payload: &p,
                ..Default::default()
            }];
            store.put_batch(&batch, u64::from(i)).await.unwrap();
        }
        assert_eq!(store.segment_count(), 0, "all objects inline");
        assert_eq!(
            store.stats().fsyncs,
            u64::from(BATCHES),
            "one fdatasync per batch"
        );
    }
    let path = dir.0.join("wal.log");
    let mut wal = std::fs::read(&path).unwrap();
    let last = *wal_frame_starts(&wal).last().unwrap();
    let len = wal.len();
    wal[len - 10] ^= 0xff; // inside the last inline object's bytes
    std::fs::write(&path, &wal).unwrap();
    let store = Store::open(&dir.0, config).await.unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), last as u64);
    assert_eq!(
        store.recovery_report().actions.first(),
        Some(&RecoveryAction::TornTailTruncated {
            frame_seq: u64::from(BATCHES) - 1,
            offset: last as u64,
            bytes_removed: (len - last) as u64,
        })
    );
    assert_eq!(store.topic_range(b"t"), Some((0, u64::from(BATCHES) - 1)));
    for i in 0..BATCHES - 1 {
        assert_eq!(
            store.read(b"t", u64::from(i)).await.unwrap(),
            Some(vec![i; 100])
        );
    }
}
