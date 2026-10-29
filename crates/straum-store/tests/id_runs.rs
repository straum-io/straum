//! Id runs (ADR-0014 step 6): the memtable of ids since the last
//! checkpoint goes to `idrun-<n>.run` (sorted entries in pages, fences and a
//! binary fuse filter in the footer); deduplication looks ids up in the
//! memtable, then in each run whose filter answers yes. Past eight runs a
//! checkpoint merges them, dropping reclaimed ids. In every write mode.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use std::path::Path;

use common::{TempDir, edit, snapshot};
use straum_store::{Config, Message, Store, StoreError, WriteMode};

const MODES: [WriteMode; 3] = [
    WriteMode::Sequential,
    WriteMode::ParallelSync,
    WriteMode::INLINE_SMALL,
];

fn config(mode: WriteMode) -> Config {
    Config {
        write_mode: mode,
        ..Config::default()
    }
}

fn body(i: u64) -> Vec<u8> {
    format!("body {i} {}", "q".repeat((i % 50) as usize)).into_bytes()
}

async fn put(store: &mut Store, topic: &[u8], bodies: &[u64]) -> Vec<u64> {
    let payloads: Vec<Vec<u8>> = bodies.iter().map(|&i| body(i)).collect();
    let batch: Vec<Message> = payloads
        .iter()
        .map(|p| Message {
            topic,
            payload: p,
            ..Default::default()
        })
        .collect();
    store.put_batch(&batch, 1).await.unwrap()
}

fn names(dir: &Path, prefix: &str) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with(prefix))
        .collect();
    v.sort();
    v
}

/// A repeat of content stored before a checkpoint is found in the id run,
/// before and after reopening; content reclaimed since is stored again
/// under a new number; one number per live id throughout.
#[compio::test]
async fn repeats_are_found_in_the_runs_and_reclaimed_ids_are_misses() {
    for mode in MODES {
        let dir = TempDir::new("idruns-dedup");
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        put(&mut store, b"a", &(0..300).collect::<Vec<_>>()).await;
        put(&mut store, b"b", &[1000]).await;
        store.checkpoint().await.unwrap();
        assert_eq!(names(&dir.0, "idrun-").len(), 1, "{mode:?}");
        let before = store.stats().messages_deduplicated;
        put(&mut store, b"a", &[5, 150, 299]).await;
        assert_eq!(store.stats().messages_deduplicated - before, 3, "{mode:?}");
        assert_eq!(store.object_count(), 301);
        drop(store);
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        put(&mut store, b"a", &[7, 1000]).await;
        assert_eq!(store.stats().messages_deduplicated, 2, "{mode:?}");
        assert_eq!(store.object_count(), 301);
        // Topic b's only entry released and compacted: its object (in a
        // run) is reclaimed, so a repeat stores it again.
        store.release_before(b"b", 2).await.unwrap();
        let reclaimable = mode != WriteMode::INLINE_SMALL;
        store.compact().await.unwrap();
        let seqs = put(&mut store, b"b", &[1000]).await;
        assert_eq!(store.read(b"b", seqs[0]).await.unwrap(), Some(body(1000)));
        if reclaimable {
            assert_eq!(store.object_count(), 301, "{mode:?}: stored again");
        }
        store.checkpoint().await.unwrap();
        drop(store);
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        assert_eq!(store.read(b"b", seqs[0]).await.unwrap(), Some(body(1000)));
    }
}

/// Past eight runs a checkpoint merges every run into one and deletes the
/// others; old content still deduplicates, and reopening is clean.
#[compio::test]
async fn runs_are_merged_past_the_bound() {
    for mode in MODES {
        let dir = TempDir::new("idruns-merge");
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        for k in 0..12u64 {
            put(&mut store, b"a", &[k * 10, k * 10 + 1]).await;
            store.checkpoint().await.unwrap();
            assert!(names(&dir.0, "idrun-").len() <= 8, "{mode:?} after {k}");
        }
        let before = store.stats().messages_deduplicated;
        put(&mut store, b"a", &[0, 1, 111]).await;
        assert_eq!(store.stats().messages_deduplicated - before, 3, "{mode:?}");
        drop(store);
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        put(&mut store, b"a", &[50]).await;
        assert_eq!(store.stats().messages_deduplicated, 1, "{mode:?}");
        assert_eq!(store.object_count(), 24);
    }
}

/// A missing, cut or damaged id run, or a resealed entry naming another
/// number, fails open with no file changed.
#[compio::test]
async fn damaged_id_runs_fail_open_and_change_nothing() {
    let dir = TempDir::new("idruns-damage");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    put(&mut store, b"a", &(0..20).collect::<Vec<_>>()).await;
    store.checkpoint().await.unwrap();
    put(&mut store, b"a", &(20..40).collect::<Vec<_>>()).await;
    store.checkpoint().await.unwrap();
    drop(store);
    let run = |d: &Path| d.join(&names(d, "idrun-")[0]);
    type Change = fn(&Path, &dyn Fn(&Path) -> std::path::PathBuf);
    let cases: [(&str, Change); 5] = [
        ("missing", |d, r| std::fs::remove_file(r(d)).unwrap()),
        ("cut", |d, r| edit(&r(d), |b| b.truncate(b.len() - 1))),
        ("page damaged", |d, r| edit(&r(d), |b| b[40] ^= 1)),
        ("footer damaged", |d, r| {
            edit(&r(d), |b| {
                let at = b.len() - 10;
                b[at] ^= 1;
            })
        }),
        // Entry 0's number changed, its page resealed: the WAL comparison
        // finds it.
        ("entry resealed with another number", |d, r| {
            edit(&r(d), |b| {
                b[24 + 32] ^= 0x7f;
                let end = (24 + 4084).min(b.len());
                let entries_end = end.min(24 + 20 * 40);
                let crc =
                    crc32c::crc32c_append(crc32c::crc32c(&0u64.to_le_bytes()), &b[24..entries_end]);
                b[entries_end..entries_end + 4].copy_from_slice(&crc.to_le_bytes());
            })
        }),
    ];
    for (what, change) in cases {
        let work = TempDir::new("idruns-damage-work");
        common::copy_dir(&dir.0, &work.0);
        change(&work.0, &run);
        let before = snapshot(&work.0);
        let res = Store::open(&work.0, Config::default()).await;
        assert!(
            matches!(
                res,
                Err(StoreError::CorruptCheckpoint { .. } | StoreError::CheckpointMismatch { .. })
            ),
            "{what}: {:?}",
            res.err()
        );
        assert!(snapshot(&work.0) == before, "{what}: files changed");
    }
}

/// A run page damaged after open: the lookup that reads it fails with
/// `CorruptCheckpoint` and the store refuses further use.
#[compio::test]
async fn a_run_page_damaged_after_open_fails_the_lookup() {
    let dir = TempDir::new("idruns-damage-after-open");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    put(&mut store, b"a", &(0..20).collect::<Vec<_>>()).await;
    store.checkpoint().await.unwrap();
    drop(store);
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    let path = dir.0.join(&names(&dir.0, "idrun-")[0]);
    edit(&path, |b| b[40] ^= 1);
    let payload = body(3);
    let batch = [Message {
        topic: b"a",
        payload: &payload,
        ..Default::default()
    }];
    let res = store.put_batch(&batch, 1).await;
    assert!(
        matches!(res, Err(StoreError::CorruptCheckpoint { .. })),
        "{:?}",
        res.err()
    );
    assert!(matches!(
        store.put_batch(&batch, 1).await,
        Err(StoreError::Poisoned)
    ));
}
