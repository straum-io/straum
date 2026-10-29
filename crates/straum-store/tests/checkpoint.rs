//! Checkpoints (ADR-0014 step 4, docs/format.md "Checkpoint"): the object
//! tables, relocation runs and manifest written by `Store::checkpoint`,
//! checked at open against the replay of the frames they cover; damage is
//! an error with no file changed, files of an interrupted checkpoint are
//! removed and reported, a damaged manifest falls back to the older one or,
//! with none, to the WAL. Interim rule: no WAL file is deleted.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use std::path::{Path, PathBuf};

use common::{TempDir, edit, snapshot};
use straum_store::{Config, Message, RecoveryAction, Store, StoreError, WriteMode};

const MODES: [WriteMode; 3] = [
    WriteMode::Sequential,
    WriteMode::ParallelSync,
    WriteMode::INLINE_SMALL,
];

fn payload(i: u64) -> Vec<u8> {
    // Every fifth message repeats the body of one a batch earlier:
    // deduplicated, so releasing a batch leaves some of its objects live.
    let i = if i % 5 == 4 && i >= 32 { i - 32 } else { i };
    format!("message {i} {}", "y".repeat((i % 40) as usize)).into_bytes()
}

/// Messages `from..to` to topic "t" in one batch at time `from`.
async fn put(store: &mut Store, from: u64, to: u64) {
    let bodies: Vec<Vec<u8>> = (from..to).map(payload).collect();
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

async fn check(store: &Store, live: std::ops::Range<u64>) {
    assert_eq!(store.topic_range(b"t"), Some((live.start, live.end)));
    for seq in live {
        assert_eq!(
            store.read(b"t", seq).await.unwrap(),
            Some(payload(seq)),
            "{seq}"
        );
    }
}

fn names(dir: &Path, prefix: &str) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with(prefix))
        .collect();
    names.sort();
    names
}

fn config(mode: WriteMode) -> Config {
    Config {
        write_mode: mode,
        ..Config::default()
    }
}

/// Two checkpoints with a compaction between them that reclaims and moves
/// covered objects; returns the live range.
async fn build(dir: &Path, mode: WriteMode) -> std::ops::Range<u64> {
    let mut store = Store::open(dir, config(mode)).await.unwrap();
    for b in 0..4 {
        put(&mut store, b * 30, (b + 1) * 30).await;
    }
    store.checkpoint().await.unwrap();
    assert_eq!(store.checkpoint_number(), Some(1));
    // Releases half of the first two batches' blocks; compaction then
    // reclaims their dead objects and moves the live ones of partly dead
    // blocks: covered numbers change place.
    assert_eq!(store.release_before(b"t", 45).await.unwrap(), 60);
    store.compact().await.unwrap();
    put(&mut store, 120, 150).await;
    store.checkpoint().await.unwrap();
    put(&mut store, 150, 160).await;
    check(&store, 60..160).await;
    60..160
}

#[compio::test]
async fn checkpoints_reopen_clean_in_every_mode() {
    for mode in MODES {
        let dir = TempDir::new("checkpoint-clean");
        let live = build(&dir.0, mode).await;
        assert_eq!(
            names(&dir.0, "checkpoint-"),
            ["checkpoint-0000000000000002"]
        );
        assert_eq!(names(&dir.0, "objtab-").len(), 2, "{mode:?}");
        if mode != WriteMode::INLINE_SMALL {
            // Inline blocks are not compacted; segment blocks are.
            assert_eq!(names(&dir.0, "reloc-"), ["reloc-0000000000000002.run"]);
        }
        // No WAL file is deleted (the interim rule).
        assert_eq!(names(&dir.0, "wal-")[0], common::WAL0);
        let before = snapshot(&dir.0);
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(
            store.recovery_report().is_empty(),
            "{mode:?}: {:?}",
            store.recovery_report()
        );
        assert!(snapshot(&dir.0) == before, "{mode:?}: files changed");
        assert_eq!(store.checkpoint_number(), Some(2));
        check(&store, live.clone()).await;
        // Relocations after the loaded checkpoint go to the next one.
        assert_eq!(store.release_before(b"t", 130).await.unwrap(), 90);
        store.compact().await.unwrap();
        store.checkpoint().await.unwrap();
        drop(store);
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        assert_eq!(store.checkpoint_number(), Some(3));
        check(&store, 150..160).await;
    }
}

type Change = fn(&Path);

fn path_of(dir: &Path, prefix: &str, k: usize) -> PathBuf {
    dir.join(&names(dir, prefix)[k])
}

/// Recomputes the CRC of page `page` (whole 4 KiB pages before it) of a
/// table or run with entries of `width` bytes.
fn reseal_page(bytes: &mut [u8], page: usize, width: usize) {
    let page_len = (4096 - 4) / width * width + 4;
    let start = 24 + page * page_len;
    let end = (start + page_len).min(bytes.len());
    let crc = crc32c::crc32c_append(
        crc32c::crc32c(&(page as u64).to_le_bytes()),
        &bytes[start..end - 4],
    );
    bytes[end - 4..end].copy_from_slice(&crc.to_le_bytes());
}

fn reseal_manifest(bytes: &mut [u8]) {
    let n = bytes.len() - 4;
    let crc = crc32c::crc32c(&bytes[..n]);
    bytes[n..].copy_from_slice(&crc.to_le_bytes());
}

#[compio::test]
async fn damaged_checkpoint_files_fail_open_and_change_nothing() {
    let dir = TempDir::new("checkpoint-damage");
    build(&dir.0, WriteMode::Sequential).await;
    let corrupt = |e: &StoreError| matches!(e, StoreError::CorruptCheckpoint { checkpoint: 2, .. });
    let mismatch =
        |e: &StoreError| matches!(e, StoreError::CheckpointMismatch { checkpoint: 2, .. });
    type Expect = fn(&StoreError) -> bool;
    let cases: [(&str, Change, Expect); 10] = [
        (
            "table missing",
            |d| std::fs::remove_file(path_of(d, "objtab-", 1)).unwrap(),
            corrupt,
        ),
        (
            "table cut",
            |d| edit(&path_of(d, "objtab-", 0), |b| b.truncate(b.len() - 3)),
            corrupt,
        ),
        (
            "table page damaged",
            |d| edit(&path_of(d, "objtab-", 0), |b| b[40] ^= 1),
            corrupt,
        ),
        (
            "table header damaged",
            |d| edit(&path_of(d, "objtab-", 1), |b| b[14] ^= 1),
            corrupt,
        ),
        (
            "run missing",
            |d| std::fs::remove_file(path_of(d, "reloc-", 0)).unwrap(),
            corrupt,
        ),
        (
            "run cut",
            |d| edit(&path_of(d, "reloc-", 0), |b| b.truncate(b.len() - 2)),
            corrupt,
        ),
        (
            "run page damaged",
            |d| edit(&path_of(d, "reloc-", 0), |b| b[30] ^= 1),
            corrupt,
        ),
        // A covered object that did not move (no run lists it): its block
        // changed and the page resealed; only the
        // comparison with the WAL finds it.
        (
            "table entry disagrees",
            |d| {
                edit(&path_of(d, "objtab-", 0), |b| {
                    b[24 + 44 * 90 + 32] ^= 1;
                    reseal_page(b, 0, 44);
                })
            },
            mismatch,
        ),
        // A relocation run entry with another block, resealed.
        (
            "run entry disagrees",
            |d| {
                edit(&path_of(d, "reloc-", 0), |b| {
                    b[24 + 8] ^= 1;
                    reseal_page(b, 0, 20);
                })
            },
            mismatch,
        ),
        // The manifest claims a frame the WAL does not hold.
        (
            "covers more frames than the WAL holds",
            |d| {
                edit(&path_of(d, "checkpoint-", 0), |b| {
                    b[20] = b[20].wrapping_add(100);
                    reseal_manifest(b);
                })
            },
            corrupt,
        ),
    ];
    for (what, change, expect) in cases {
        let work = TempDir::new("checkpoint-damage-work");
        common::copy_dir(&dir.0, &work.0);
        change(&work.0);
        // A repair due as well (a leftover tmp manifest): not made either.
        std::fs::write(work.0.join("checkpoint-0000000000000003.tmp"), b"half").unwrap();
        let before = snapshot(&work.0);
        let err = match Store::open(&work.0, Config::default()).await {
            Ok(_) => panic!("{what}: opened"),
            Err(e) => e,
        };
        assert!(expect(&err), "{what}: {err}");
        assert!(snapshot(&work.0) == before, "{what}: files changed");
    }
}

/// A crash inside a checkpoint leaves a tmp manifest, unlisted tables or
/// runs, or (after the rename) the previous manifest: each is deleted at
/// open and reported, and the data reads back.
#[compio::test]
async fn files_of_an_interrupted_checkpoint_are_removed_and_reported() {
    let dir = TempDir::new("checkpoint-orphans");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    put(&mut store, 0, 30).await;
    store.checkpoint().await.unwrap();
    let first = std::fs::read(path_of(&dir.0, "checkpoint-", 0)).unwrap();
    put(&mut store, 30, 60).await;
    store.checkpoint().await.unwrap();
    drop(store);
    std::fs::write(dir.0.join("checkpoint-0000000000000001"), first).unwrap();
    std::fs::write(dir.0.join("checkpoint-0000000000000003.tmp"), b"half").unwrap();
    std::fs::write(dir.0.join("objtab-00000000000000ff.tab"), b"orphan").unwrap();
    std::fs::write(dir.0.join("reloc-0000000000000003.run"), b"orphan").unwrap();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    let mut removed: Vec<String> = store
        .recovery_report()
        .actions
        .iter()
        .map(|a| match a {
            RecoveryAction::CheckpointFileRemoved { file } => file.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    removed.sort();
    assert_eq!(
        removed,
        [
            "checkpoint-0000000000000001",
            "checkpoint-0000000000000003.tmp",
            "objtab-00000000000000ff.tab",
            "reloc-0000000000000003.run",
        ]
    );
    assert_eq!(store.checkpoint_number(), Some(2));
    check(&store, 0..60).await;
    drop(store);
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert!(store.recovery_report().is_empty());

    // Checkpoint files without a WAL file: refused like segments.
    let work = TempDir::new("checkpoint-no-wal");
    std::fs::copy(
        path_of(&dir.0, "checkpoint-", 0),
        work.0.join("checkpoint-0000000000000002"),
    )
    .unwrap();
    let before = snapshot(&work.0);
    let res = Store::open(&work.0, Config::default()).await;
    assert!(
        matches!(res, Err(StoreError::CorruptWal { .. })),
        "{:?}",
        res.err()
    );
    assert!(snapshot(&work.0) == before);
}

/// Interim rule (step 4): with no manifest, tables and runs are files of a
/// first checkpoint that never completed. Removed and reported; the WAL
/// still holds everything. Once step 6b deletes WAL files, tables without a
/// manifest must be refused when WAL files they would cover are gone.
#[compio::test]
async fn tables_without_a_manifest_are_orphans_while_the_wal_is_whole() {
    let dir = TempDir::new("checkpoint-no-manifest");
    build(&dir.0, WriteMode::Sequential).await;
    std::fs::remove_file(path_of(&dir.0, "checkpoint-", 0)).unwrap();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    let removed = store
        .recovery_report()
        .actions
        .iter()
        .filter(|a| matches!(a, RecoveryAction::CheckpointFileRemoved { .. }))
        .count();
    assert_eq!(removed, 3, "{:?}", store.recovery_report());
    assert_eq!(store.checkpoint_number(), None);
    check(&store, 60..160).await;
}

/// A directory of another format version with a manifest is
/// `UnsupportedFormat`, not a damaged checkpoint.
#[compio::test]
async fn another_version_with_a_manifest_is_unsupported() {
    let dir = TempDir::new("checkpoint-version");
    build(&dir.0, WriteMode::Sequential).await;
    for name in names(&dir.0, "wal-") {
        edit(&dir.0.join(name), |b| b[8] ^= 1);
    }
    // The manifest of the same other version, intact.
    edit(&path_of(&dir.0, "checkpoint-", 0), |b| {
        b[8] ^= 1;
        reseal_manifest(b);
    });
    let res = Store::open(&dir.0, Config::default()).await;
    assert!(
        matches!(res, Err(StoreError::UnsupportedFormat)),
        "{:?}",
        res.err()
    );
}

fn actions(store: &Store) -> Vec<RecoveryAction> {
    store.recovery_report().actions.clone()
}

fn removed(actions: &[RecoveryAction]) -> Vec<String> {
    let mut files: Vec<String> = actions
        .iter()
        .filter_map(|a| match a {
            RecoveryAction::CheckpointFileRemoved { file } => Some(file.clone()),
            _ => None,
        })
        .collect();
    files.sort();
    files
}

const MANIFEST_DAMAGE: [(&str, Change); 3] = [
    ("CRC", |p| edit(p, |b| b[13] ^= 1)),
    ("cut", |p| edit(p, |b| b.truncate(b.len() - 1))),
    ("empty", |p| std::fs::write(p, b"").unwrap()),
];

/// `build` with checkpoint 1's manifest left in place, as a crash between
/// checkpoint 2's rename and the deletion of manifest 1 leaves it.
async fn build_with_both_manifests(dir: &Path, mode: WriteMode) -> std::ops::Range<u64> {
    let mut store = Store::open(dir, config(mode)).await.unwrap();
    for b in 0..4 {
        put(&mut store, b * 30, (b + 1) * 30).await;
    }
    store.checkpoint().await.unwrap();
    let first = std::fs::read(path_of(dir, "checkpoint-", 0)).unwrap();
    assert_eq!(store.release_before(b"t", 45).await.unwrap(), 60);
    store.compact().await.unwrap();
    put(&mut store, 120, 150).await;
    store.checkpoint().await.unwrap();
    put(&mut store, 150, 160).await;
    drop(store);
    std::fs::write(dir.join("checkpoint-0000000000000001"), first).unwrap();
    60..160
}

/// A damaged newest manifest with an intact older one: open uses the older
/// checkpoint (checked against the WAL as always), reports the fallback and
/// deletes the damaged manifest and the files only it listed. The next
/// checkpoint relocates what moved since the older one.
#[compio::test]
async fn a_damaged_newest_manifest_falls_back_to_the_older_one() {
    for mode in MODES {
        for (what, damage) in MANIFEST_DAMAGE {
            let dir = TempDir::new("checkpoint-fallback");
            let live = build_with_both_manifests(&dir.0, mode).await;
            damage(&dir.0.join("checkpoint-0000000000000002"));
            let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
            let acts = actions(&store);
            assert_eq!(
                acts[0],
                RecoveryAction::CheckpointManifestDamaged {
                    checkpoint: 2,
                    fallback: Some(1),
                },
                "{mode:?} {what}"
            );
            assert_eq!(store.checkpoint_number(), Some(1), "{mode:?} {what}");
            assert_eq!(
                names(&dir.0, "checkpoint-"),
                ["checkpoint-0000000000000001"]
            );
            assert_eq!(names(&dir.0, "objtab-"), ["objtab-0000000000000000.tab"]);
            assert!(names(&dir.0, "reloc-").is_empty(), "{mode:?} {what}");
            let files = removed(&acts);
            assert!(
                files.contains(&"checkpoint-0000000000000002".to_owned()),
                "{files:?}"
            );
            assert_eq!(acts.len(), 1 + files.len(), "{mode:?} {what}: {acts:?}");
            check(&store, live.clone()).await;
            store.checkpoint().await.unwrap();
            drop(store);
            let store = Store::open(&dir.0, config(mode)).await.unwrap();
            assert!(store.recovery_report().is_empty(), "{mode:?} {what}");
            assert_eq!(store.checkpoint_number(), Some(2));
            check(&store, live.clone()).await;
        }
    }
}

/// The only manifest damaged: while the WAL holds every frame (until
/// ADR-0014 step 6b), the checkpoint is ignored and the state rebuilt from
/// the WAL, reported; its files are deleted. (From step 6b the same state
/// with WAL files deleted is `CorruptCheckpoint`: tested with step 6b.)
#[compio::test]
async fn a_damaged_only_manifest_is_ignored_while_the_wal_is_whole() {
    for (what, damage) in MANIFEST_DAMAGE {
        let dir = TempDir::new("checkpoint-damaged-only");
        let live = build(&dir.0, WriteMode::Sequential).await;
        damage(&path_of(&dir.0, "checkpoint-", 0));
        let before = names(&dir.0, "");
        let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
        let acts = actions(&store);
        assert_eq!(
            acts[0],
            RecoveryAction::CheckpointManifestDamaged {
                checkpoint: 2,
                fallback: None,
            },
            "{what}"
        );
        let expect: Vec<String> = before
            .into_iter()
            .filter(|n| {
                ["checkpoint-", "objtab-", "reloc-"]
                    .iter()
                    .any(|p| n.starts_with(p))
            })
            .collect();
        assert_eq!(removed(&acts), expect, "{what}");
        assert_eq!(acts.len(), 1 + expect.len(), "{what}: {acts:?}");
        assert_eq!(store.checkpoint_number(), None);
        check(&store, live.clone()).await;
        store.checkpoint().await.unwrap();
        drop(store);
        let store = Store::open(&dir.0, Config::default()).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{what}");
        assert_eq!(store.checkpoint_number(), Some(1));
    }
}

/// Both manifests damaged: none is intact, and while the WAL is whole the
/// checkpoint is ignored, each damaged manifest reported.
#[compio::test]
async fn two_damaged_manifests_are_ignored_while_the_wal_is_whole() {
    let dir = TempDir::new("checkpoint-two-damaged");
    let live = build_with_both_manifests(&dir.0, WriteMode::Sequential).await;
    for n in ["checkpoint-0000000000000001", "checkpoint-0000000000000002"] {
        edit(&dir.0.join(n), |b| b[13] ^= 1);
    }
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    let acts = actions(&store);
    let damaged = |checkpoint| RecoveryAction::CheckpointManifestDamaged {
        checkpoint,
        fallback: None,
    };
    assert_eq!(acts[..2], [damaged(2), damaged(1)]);
    assert_eq!(store.checkpoint_number(), None);
    check(&store, live).await;
}

/// A fallback is refused like any checkpoint when the older checkpoint is
/// itself damaged or disagrees with the WAL: an error, no file changed
/// (the damaged newest manifest and the orphans stay as well).
#[compio::test]
async fn a_fallback_to_a_damaged_older_checkpoint_fails_open_and_changes_nothing() {
    let dir = TempDir::new("checkpoint-fallback-refused");
    build_with_both_manifests(&dir.0, WriteMode::Sequential).await;
    edit(&dir.0.join("checkpoint-0000000000000002"), |b| b[13] ^= 1);
    type Expect = fn(&StoreError) -> bool;
    let cases: [(&str, Change, Expect); 2] = [
        (
            "the table both list damaged",
            |d| edit(&path_of(d, "objtab-", 0), |b| b[40] ^= 1),
            |e| matches!(e, StoreError::CorruptCheckpoint { checkpoint: 1, .. }),
        ),
        (
            "the older table disagrees with the WAL",
            |d| {
                edit(&path_of(d, "objtab-", 0), |b| {
                    b[24 + 44 * 90 + 32] ^= 1;
                    reseal_page(b, 0, 44);
                })
            },
            |e| matches!(e, StoreError::CheckpointMismatch { checkpoint: 1, .. }),
        ),
    ];
    for (what, change, expect) in cases {
        let work = TempDir::new("checkpoint-fallback-refused-work");
        common::copy_dir(&dir.0, &work.0);
        change(&work.0);
        std::fs::write(work.0.join("checkpoint-0000000000000003.tmp"), b"half").unwrap();
        let before = snapshot(&work.0);
        let err = match Store::open(&work.0, Config::default()).await {
            Ok(_) => panic!("{what}: opened"),
            Err(e) => e,
        };
        assert!(expect(&err), "{what}: {err}");
        assert!(snapshot(&work.0) == before, "{what}: files changed");
    }
}
