//! The WAL as numbered files (`wal-<first frame seq>.log`): rotation at
//! `Config::wal_file_max_bytes`, reads of inline blocks and of keys and
//! headers from earlier files, and recovery across files. Only the last
//! file's last frame may be torn or dropped; damage anywhere else, a
//! missing file or a gap is an error with no file changed.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use std::path::{Path, PathBuf};

use common::{TempDir, snapshot, wal_frame_starts};
use straum_store::{Config, Entry, Message, RecoveryAction, Store, StoreError, WriteMode};

const MODES: [WriteMode; 3] = [
    WriteMode::Sequential,
    WriteMode::ParallelSync,
    WriteMode::INLINE_SMALL,
];

/// Every frame in a file of its own.
fn config(mode: WriteMode) -> Config {
    Config {
        write_mode: mode,
        wal_file_max_bytes: 1,
        ..Config::default()
    }
}

/// WAL files of `dir`, by first frame seq.
fn wal_files(dir: &Path) -> Vec<(u64, PathBuf)> {
    let mut files: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?;
            let hex = name.strip_prefix("wal-")?.strip_suffix(".log")?;
            Some((u64::from_str_radix(hex, 16).ok()?, p.clone()))
        })
        .collect();
    files.sort();
    files
}

fn payload(i: u64) -> Vec<u8> {
    format!("message {i} {}", "x".repeat((i % 50) as usize)).into_bytes()
}

const HEADERS: [(&[u8], &[u8]); 1] = [(b"h", b"v")];

/// Batches `from..to`, one message each, every other one with a key and a
/// header.
async fn put(store: &mut Store, from: u64, to: u64) {
    for i in from..to {
        let p = payload(i);
        let keyed = i % 2 == 0;
        let m = Message {
            topic: b"t",
            payload: &p,
            key: keyed.then_some(&b"k"[..]),
            headers: if keyed { &HEADERS } else { &[] },
        };
        store.put_batch(&[m], i).await.unwrap();
    }
}

async fn check(store: &Store, live: std::ops::Range<u64>) {
    assert_eq!(store.topic_range(b"t"), Some((live.start, live.end)));
    for i in live {
        let keyed = i % 2 == 0;
        let want = Entry {
            timestamp: i,
            key: keyed.then(|| b"k".to_vec()),
            headers: if keyed {
                vec![(b"h".to_vec(), b"v".to_vec())]
            } else {
                vec![]
            },
            payload: payload(i),
        };
        assert_eq!(store.read_entry(b"t", i).await.unwrap(), Some(want), "{i}");
    }
}

#[compio::test]
async fn frames_rotate_into_files_named_by_their_first_frame() {
    for mode in MODES {
        let dir = TempDir::new("walfiles-rotate");
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        put(&mut store, 0, 6).await;
        assert_eq!(store.release_before(b"t", 2).await.unwrap(), 2);
        store.compact().await.unwrap();
        check(&store, 2..6).await;
        drop(store);
        let files = wal_files(&dir.0);
        // Six batches and a release (and a compaction frame where a
        // segment held a dead object): each in a file of its own, named by
        // its frame's seq.
        assert!(files.len() >= 7, "{mode:?}: {files:?}");
        for (k, (id, path)) in files.iter().enumerate() {
            assert_eq!(*id, k as u64, "{mode:?}");
            let bytes = std::fs::read(path).unwrap();
            assert_eq!(wal_frame_starts(&bytes).len(), 1, "{mode:?} {k}");
        }
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        check(&store, 2..6).await;
        // Inline blocks of earlier files read back after reopen.
        if mode == WriteMode::INLINE_SMALL {
            assert_eq!(
                store.is_inline(&straum_store::ObjectId::of(&payload(3))),
                Some(true)
            );
        }
    }
}

#[compio::test]
async fn a_file_holds_frames_up_to_its_limit() {
    let dir = TempDir::new("walfiles-limit");
    let limit = 2048;
    let config = Config {
        wal_file_max_bytes: limit,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    put(&mut store, 0, 100).await;
    drop(store);
    let files = wal_files(&dir.0);
    assert!(files.len() > 2, "{files:?}");
    for (id, path) in &files[..files.len() - 1] {
        let len = std::fs::metadata(path).unwrap().len();
        let frames = wal_frame_starts(&std::fs::read(path).unwrap());
        let last = *frames.last().unwrap() as u64;
        assert!(last < limit && len >= limit, "file {id}: {len} bytes");
    }
    let store = Store::open(&dir.0, config).await.unwrap();
    check(&store, 0..100).await;
}

#[compio::test]
async fn only_the_last_files_tail_is_cut() {
    for mode in MODES {
        let dir = TempDir::new("walfiles-torn");
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        put(&mut store, 0, 4).await;
        drop(store);
        let files = wal_files(&dir.0);
        let (last_id, last) = files.last().unwrap().clone();
        let bytes = std::fs::read(&last).unwrap();
        std::fs::write(&last, &bytes[..bytes.len() - 3]).unwrap();
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert_eq!(
            store.recovery_report().actions,
            [RecoveryAction::TornTailTruncated {
                frame_seq: 3,
                file: last_id,
                offset: 24,
                bytes_removed: bytes.len() as u64 - 27,
            }],
            "{mode:?}"
        );
        check(&store, 0..3).await;
        drop(store);
        // The next frame goes into the emptied file and later ones on.
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty());
        put(&mut store, 3, 5).await;
        check(&store, 0..5).await;
        drop(store);
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        check(&store, 0..5).await;
    }
}

#[compio::test]
async fn damage_before_the_last_file_is_an_error_and_changes_nothing() {
    let dir = TempDir::new("walfiles-damage");
    let mut store = Store::open(&dir.0, config(WriteMode::Sequential))
        .await
        .unwrap();
    put(&mut store, 0, 4).await;
    drop(store);
    let files = wal_files(&dir.0);
    let open = || Store::open(&dir.0, config(WriteMode::Sequential));

    // A cut, and a flipped bit, at the end of a file another one follows.
    for damage in [
        &(|b: &mut Vec<u8>| b.truncate(b.len() - 1)) as &dyn Fn(&mut Vec<u8>),
        &|b: &mut Vec<u8>| {
            let n = b.len();
            b[n - 1] ^= 1;
        },
        &|b: &mut Vec<u8>| b.extend_from_slice(b"junk"),
    ] {
        let work = TempDir::new("walfiles-damage-work");
        common::copy_dir(&dir.0, &work.0);
        let path = work.0.join(files[1].1.file_name().unwrap());
        common::edit(&path, damage);
        let before = snapshot(&work.0);
        let res = Store::open(&work.0, config(WriteMode::Sequential)).await;
        assert!(
            matches!(
                res,
                Err(StoreError::WalCorrupted {
                    file: 1,
                    next_file: 2,
                    next_offset: 24,
                    ..
                })
            ),
            "{:?}",
            res.err()
        );
        assert!(snapshot(&work.0) == before, "files changed");
    }
    assert!(open().await.is_ok());
}

#[compio::test]
async fn a_missing_or_foreign_file_is_an_error_and_changes_nothing() {
    let dir = TempDir::new("walfiles-gap");
    let mut store = Store::open(&dir.0, config(WriteMode::Sequential))
        .await
        .unwrap();
    put(&mut store, 0, 4).await;
    drop(store);
    let files = wal_files(&dir.0);
    let name = |k: usize| files[k].1.file_name().unwrap().to_owned();
    type Damage<'a> = &'a dyn Fn(&Path);
    let cases: [(&str, Damage); 5] = [
        ("middle file missing", &|d| {
            std::fs::remove_file(d.join(name(1))).unwrap()
        }),
        ("first file missing", &|d| {
            std::fs::remove_file(d.join(name(0))).unwrap()
        }),
        ("a file under another frame's name", &|d| {
            std::fs::rename(d.join(name(3)), d.join("wal-0000000000000007.log")).unwrap()
        }),
        ("a damaged header", &|d| {
            common::edit(&d.join(name(2)), |b| b[9] ^= 1)
        }),
        ("the single wal.log of an earlier format", &|d| {
            std::fs::write(d.join("wal.log"), b"old").unwrap()
        }),
    ];
    for (what, damage) in cases {
        let work = TempDir::new("walfiles-gap-work");
        common::copy_dir(&dir.0, &work.0);
        damage(&work.0);
        let before = snapshot(&work.0);
        let res = Store::open(&work.0, config(WriteMode::Sequential)).await;
        assert!(
            matches!(
                res,
                Err(StoreError::CorruptWal { offset: 0, .. }) | Err(StoreError::UnsupportedFormat)
            ),
            "{what}: {:?}",
            res.err()
        );
        assert!(snapshot(&work.0) == before, "{what}: files changed");
    }
}

#[compio::test]
async fn an_empty_last_file_and_a_leftover_tmp_file() {
    let dir = TempDir::new("walfiles-empty");
    let mut store = Store::open(&dir.0, config(WriteMode::Sequential))
        .await
        .unwrap();
    put(&mut store, 0, 3).await;
    drop(store);
    // A crash after the next file was created and before its frame: a file
    // holding only its header. And a creation cut before its rename.
    let header = std::fs::read(dir.0.join(common::WAL0)).unwrap()[..24].to_vec();
    std::fs::write(dir.0.join("wal-0000000000000003.log"), &header).unwrap();
    std::fs::write(dir.0.join("wal-0000000000000004.log.tmp"), &header[..10]).unwrap();
    let mut store = Store::open(&dir.0, config(WriteMode::Sequential))
        .await
        .unwrap();
    assert_eq!(
        store.recovery_report().actions,
        [RecoveryAction::WalTmpRemoved]
    );
    check(&store, 0..3).await;
    put(&mut store, 3, 5).await;
    drop(store);
    let store = Store::open(&dir.0, config(WriteMode::Sequential))
        .await
        .unwrap();
    assert!(store.recovery_report().is_empty());
    check(&store, 0..5).await;
    assert_eq!(wal_files(&dir.0).len(), 5);
}

#[compio::test]
async fn a_parallel_frame_followed_by_a_file_is_not_dropped() {
    // A ParallelSync frame whose block is missing is dropped only when
    // nothing follows it; a later WAL file proves it was acknowledged.
    let dir = TempDir::new("walfiles-parallel");
    let mode = WriteMode::ParallelSync;
    let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
    put(&mut store, 0, 3).await;
    drop(store);
    let segments: Vec<PathBuf> = std::fs::read_dir(&dir.0)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with("seg-"))
        .collect();
    // The last batch's block is the end of the only segment.
    let seg = &segments[0];
    let len = std::fs::metadata(seg).unwrap().len();
    let cut = {
        let work = TempDir::new("walfiles-parallel-work");
        common::copy_dir(&dir.0, &work.0);
        let path = work.0.join(seg.file_name().unwrap());
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 1)
            .unwrap();
        work
    };
    // Nothing after the frame: dropped and reported.
    let work = TempDir::new("walfiles-parallel-drop");
    common::copy_dir(&cut.0, &work.0);
    let store = Store::open(&work.0, config(mode)).await.unwrap();
    assert!(
        matches!(
            store.recovery_report().actions[..],
            [
                RecoveryAction::FrameDropped {
                    frame_seq: 2,
                    file: 2,
                    ..
                },
                ..
            ]
        ),
        "{:?}",
        store.recovery_report()
    );
    drop(store);
    // An empty WAL file after it: corruption, nothing changed.
    let header = std::fs::read(cut.0.join(common::WAL0)).unwrap()[..24].to_vec();
    std::fs::write(cut.0.join("wal-0000000000000003.log"), &header).unwrap();
    let before = snapshot(&cut.0);
    let res = Store::open(&cut.0, config(mode)).await;
    assert!(
        matches!(
            res,
            Err(StoreError::CorruptBlock { .. }) | Err(StoreError::MissingObject(_))
        ),
        "{:?}",
        res.err()
    );
    assert!(snapshot(&cut.0) == before, "files changed");
}
