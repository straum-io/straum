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

/// The checkpoint tables and runs (objects and blocks) and dictionary files.
fn all_tables(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = ["objtab-", "reloc-", "blktab-", "blkrel-", "dict-", "idrun-"]
        .iter()
        .flat_map(|p| names(dir, p))
        .collect();
    v.sort();
    v
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
        assert_eq!(names(&dir.0, "blktab-").len(), 2, "{mode:?}");
        if mode != WriteMode::INLINE_SMALL {
            // Inline blocks are not compacted; segment blocks are.
            assert_eq!(names(&dir.0, "reloc-"), ["reloc-0000000000000002.run"]);
            assert_eq!(names(&dir.0, "blkrel-"), ["blkrel-0000000000000002.run"]);
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
    let block_mismatch =
        |e: &StoreError| matches!(e, StoreError::CheckpointBlockMismatch { checkpoint: 2, .. });
    let cases: [(&str, Change, Expect); 24] = [
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
        (
            "block table missing",
            |d| std::fs::remove_file(path_of(d, "blktab-", 1)).unwrap(),
            corrupt,
        ),
        (
            "block table page damaged",
            |d| edit(&path_of(d, "blktab-", 0), |b| b[40] ^= 1),
            corrupt,
        ),
        (
            "block table entry of an unknown kind",
            |d| {
                edit(&path_of(d, "blktab-", 0), |b| {
                    b[24 + 45 * 3] = 7;
                    reseal_page(b, 0, 45);
                })
            },
            corrupt,
        ),
        (
            "block table cut",
            |d| edit(&path_of(d, "blktab-", 0), |b| b.truncate(b.len() - 3)),
            corrupt,
        ),
        (
            "block run page damaged",
            |d| edit(&path_of(d, "blkrel-", 0), |b| b[30] ^= 1),
            corrupt,
        ),
        (
            "next segment number not above the live segments",
            |d| {
                edit(&path_of(d, "checkpoint-", 0), |b| {
                    b[52..60].fill(0);
                    reseal_manifest(b);
                })
            },
            corrupt,
        ),
        (
            "dict_train_bytes",
            |d| {
                edit(&path_of(d, "checkpoint-", 0), |b| {
                    b[60] ^= 1;
                    reseal_manifest(b);
                })
            },
            corrupt,
        ),
        (
            "dictless_raw",
            |d| {
                edit(&path_of(d, "checkpoint-", 0), |b| {
                    b[68] ^= 1;
                    reseal_manifest(b);
                })
            },
            corrupt,
        ),
        (
            "a dictionary the WAL does not have",
            |d| {
                edit(&path_of(d, "checkpoint-", 0), |b| {
                    b[76] = 1;
                    reseal_manifest(b);
                })
            },
            corrupt,
        ),
        (
            "a live segment the WAL does not reference",
            |d| {
                edit(&path_of(d, "checkpoint-", 0), |b| {
                    let at = b.len() - 12;
                    b[at] ^= 0x40;
                    reseal_manifest(b);
                })
            },
            corrupt,
        ),
        (
            "block run missing",
            |d| std::fs::remove_file(path_of(d, "blkrel-", 0)).unwrap(),
            corrupt,
        ),
        (
            "block run cut",
            |d| edit(&path_of(d, "blkrel-", 0), |b| b.truncate(b.len() - 2)),
            corrupt,
        ),
        // A block's CRC changed in its table entry, resealed: only the
        // comparison with the WAL finds it.
        (
            "block table entry disagrees",
            |d| {
                edit(&path_of(d, "blktab-", 0), |b| {
                    b[24 + 45 * 3 + 37] ^= 1;
                    reseal_page(b, 0, 45);
                })
            },
            block_mismatch,
        ),
        // A block run entry with another offset, resealed.
        (
            "block run entry disagrees",
            |d| {
                edit(&path_of(d, "blkrel-", 0), |b| {
                    b[24 + 16] ^= 1;
                    reseal_page(b, 0, 24);
                })
            },
            block_mismatch,
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
    let files = all_tables(&dir.0).len();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    let removed = store
        .recovery_report()
        .actions
        .iter()
        .filter(|a| matches!(a, RecoveryAction::CheckpointFileRemoved { .. }))
        .count();
    assert_eq!(removed, files, "{:?}", store.recovery_report());
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

const MANIFEST_DAMAGE: [(&str, Change); 4] = [
    ("CRC", |p| edit(p, |b| b[13] ^= 1)),
    ("cut", |p| edit(p, |b| b.truncate(b.len() - 1))),
    ("empty", |p| std::fs::write(p, b"").unwrap()),
    // An intact manifest under another checkpoint's name.
    ("misnamed", |p| {
        let name = p.file_name().unwrap().to_str().unwrap();
        let n = u64::from_str_radix(name.strip_prefix("checkpoint-").unwrap(), 16).unwrap();
        let other = p.with_file_name(format!("checkpoint-{:016x}", n - 1));
        let bytes = if n > 1 && other.exists() {
            std::fs::read(&other).unwrap()
        } else {
            let mut b = std::fs::read(p).unwrap();
            b[12] ^= 1;
            reseal_manifest(&mut b);
            b
        };
        std::fs::write(p, bytes).unwrap();
    }),
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

fn damaged(checkpoint: u64, fallback: Option<u64>, quarantine: &str) -> RecoveryAction {
    RecoveryAction::CheckpointManifestDamaged {
        checkpoint,
        fallback,
        quarantine: quarantine.to_owned(),
    }
}

/// The files `actions` report as moved into `quarantine`, sorted.
fn moved(actions: &[RecoveryAction], quarantine: &str) -> Vec<String> {
    let mut files: Vec<String> = actions
        .iter()
        .filter_map(|a| match a {
            RecoveryAction::CheckpointFileQuarantined {
                file,
                quarantine: q,
            } if q == quarantine => Some(file.clone()),
            _ => None,
        })
        .collect();
    files.sort();
    files
}

/// The files in quarantine directory `q`, sorted.
fn quarantine_files(dir: &Path, q: &str) -> Vec<String> {
    names(&dir.join(q), "")
}

const Q2: &str = "quarantine-0000000000000002";

/// A damaged newest manifest with an intact older one: open uses the older
/// checkpoint (checked against the WAL as always), reports the fallback,
/// and moves the damaged manifest, unchanged, with the table and run only
/// it lists into `quarantine-<n>`, each moved file reported. Later opens do
/// not report it again; `quarantined_files` counts it until an operator
/// removes it. The next checkpoint takes a new number.
#[compio::test]
async fn a_damaged_newest_manifest_falls_back_to_the_older_one() {
    for mode in MODES {
        for (what, damage) in MANIFEST_DAMAGE {
            let dir = TempDir::new("checkpoint-fallback");
            let live = build_with_both_manifests(&dir.0, mode).await;
            damage(&dir.0.join("checkpoint-0000000000000002"));
            let bad = std::fs::read(dir.0.join("checkpoint-0000000000000002")).unwrap();
            let mut expect = own_files(&dir.0);
            expect.push("checkpoint-0000000000000002".to_owned());
            expect.sort();
            let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
            let acts = actions(&store);
            assert_eq!(acts[0], damaged(2, Some(1), Q2), "{mode:?} {what}");
            assert_eq!(moved(&acts, Q2), expect, "{mode:?} {what}");
            assert!(removed(&acts).is_empty(), "{mode:?} {what}: {acts:?}");
            assert_eq!(acts.len(), 1 + expect.len(), "{mode:?} {what}: {acts:?}");
            assert_eq!(quarantine_files(&dir.0, Q2), expect);
            assert_eq!(
                std::fs::read(dir.0.join(Q2).join("checkpoint-0000000000000002")).unwrap(),
                bad,
                "kept as it was"
            );
            assert_eq!(store.quarantined_files(), expect.len() as u64);
            assert_eq!(store.checkpoint_number(), Some(1), "{mode:?} {what}");
            assert_eq!(
                names(&dir.0, "checkpoint-"),
                ["checkpoint-0000000000000001"]
            );
            assert_eq!(
                all_tables(&dir.0),
                [
                    "blktab-0000000000000000.tab",
                    "idrun-0000000000000001.run",
                    "objtab-0000000000000000.tab"
                ],
                "{mode:?} {what}"
            );
            check(&store, live.clone()).await;
            store.checkpoint().await.unwrap();
            assert_eq!(store.checkpoint_number(), Some(3), "never repeats 2");
            drop(store);
            for _ in 0..2 {
                let store = Store::open(&dir.0, config(mode)).await.unwrap();
                assert!(store.recovery_report().is_empty(), "{mode:?} {what}");
                assert_eq!(store.quarantined_files(), expect.len() as u64);
                assert_eq!(store.checkpoint_number(), Some(3));
                check(&store, live.clone()).await;
            }
            assert_eq!(
                names(&dir.0, "checkpoint-"),
                ["checkpoint-0000000000000003"]
            );
            assert_eq!(quarantine_files(&dir.0, Q2), expect, "unchanged");
            // Removed by an operator: no longer counted.
            std::fs::remove_dir_all(dir.0.join(Q2)).unwrap();
            let store = Store::open(&dir.0, config(mode)).await.unwrap();
            assert!(store.recovery_report().is_empty());
            assert_eq!(store.quarantined_files(), 0);
        }
    }
}

/// The only manifest damaged: while the WAL holds every frame (until
/// ADR-0014 step 6b), the checkpoint is ignored and the state rebuilt from
/// the WAL, reported; the manifest moves into quarantine with every table
/// and run, in every write mode. (From step 6b the same state with WAL
/// files deleted is `CorruptCheckpoint`: tested with step 6b.)
#[compio::test]
async fn a_damaged_only_manifest_is_ignored_while_the_wal_is_whole() {
    for mode in MODES {
        for (what, damage) in MANIFEST_DAMAGE {
            let dir = TempDir::new("checkpoint-damaged-only");
            let live = build(&dir.0, mode).await;
            damage(&path_of(&dir.0, "checkpoint-", 0));
            let mut expect: Vec<String> = names(&dir.0, "")
                .into_iter()
                .filter(|n| {
                    [
                        "checkpoint-",
                        "objtab-",
                        "reloc-",
                        "blktab-",
                        "blkrel-",
                        "dict-",
                        "idrun-",
                    ]
                    .iter()
                    .any(|p| n.starts_with(p))
                })
                .collect();
            expect.sort();
            let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
            let acts = actions(&store);
            assert_eq!(acts[0], damaged(2, None, Q2), "{mode:?} {what}");
            assert_eq!(moved(&acts, Q2), expect, "{mode:?} {what}");
            assert!(removed(&acts).is_empty(), "{mode:?} {what}");
            assert_eq!(store.checkpoint_number(), None);
            check(&store, live.clone()).await;
            store.checkpoint().await.unwrap();
            assert_eq!(store.checkpoint_number(), Some(3));
            drop(store);
            let store = Store::open(&dir.0, config(mode)).await.unwrap();
            assert!(store.recovery_report().is_empty(), "{mode:?} {what}");
            assert_eq!(store.checkpoint_number(), Some(3));
            assert_eq!(store.quarantined_files(), expect.len() as u64);
            check(&store, live.clone()).await;
        }
    }
}

/// Both manifests damaged: none is intact, and while the WAL is whole the
/// checkpoint is ignored; each damaged manifest gets its own quarantine,
/// the newest one the tables and runs (they are cumulative).
#[compio::test]
async fn two_damaged_manifests_are_ignored_while_the_wal_is_whole() {
    for mode in MODES {
        let dir = TempDir::new("checkpoint-two-damaged");
        let live = build_with_both_manifests(&dir.0, mode).await;
        for n in ["checkpoint-0000000000000001", "checkpoint-0000000000000002"] {
            edit(&dir.0.join(n), |b| b[13] ^= 1);
        }
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        let acts = actions(&store);
        let q1 = "quarantine-0000000000000001";
        assert_eq!(acts[0], damaged(2, None, Q2));
        assert!(acts.contains(&damaged(1, None, q1)));
        assert_eq!(moved(&acts, q1), ["checkpoint-0000000000000001"]);
        assert!(moved(&acts, Q2).len() >= 3, "{acts:?}");
        assert_eq!(store.checkpoint_number(), None);
        check(&store, live).await;
        assert!(names(&dir.0, "checkpoint-").is_empty());
        assert!(all_tables(&dir.0).is_empty());
    }
}

/// An existing, completed quarantine (it holds its manifest) is never
/// overwritten: a new quarantine of the same manifest number takes the next
/// free suffix (`.1`, then `.2`).
/// Quarantine directories alone, with no WAL file, are `CorruptWal`, as
/// other checkpoint files.
#[compio::test]
async fn a_taken_quarantine_name_takes_the_next_suffix() {
    for mode in MODES {
        let dir = TempDir::new("checkpoint-quarantine-taken");
        build_with_both_manifests(&dir.0, mode).await;
        edit(&dir.0.join("checkpoint-0000000000000002"), |b| b[13] ^= 1);
        for q in [Q2, "quarantine-0000000000000002.1"] {
            std::fs::create_dir(dir.0.join(q)).unwrap();
            std::fs::write(dir.0.join(q).join("checkpoint-0000000000000002"), b"old").unwrap();
        }
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        let q = "quarantine-0000000000000002.2";
        assert_eq!(actions(&store)[0], damaged(2, Some(1), q));
        assert!(quarantine_files(&dir.0, q).contains(&"checkpoint-0000000000000002".to_owned()));
        for old in [Q2, "quarantine-0000000000000002.1"] {
            assert_eq!(
                quarantine_files(&dir.0, old),
                ["checkpoint-0000000000000002"],
                "untouched"
            );
        }
        assert_eq!(
            store.quarantined_files(),
            2 + quarantine_files(&dir.0, q).len() as u64
        );
    }

    let work = TempDir::new("checkpoint-quarantine-no-wal");
    std::fs::create_dir(work.0.join(Q2)).unwrap();
    std::fs::write(work.0.join(Q2).join("checkpoint-0000000000000002"), b"x").unwrap();
    let before = snapshot(&work.0);
    let res = Store::open(&work.0, Config::default()).await;
    assert!(
        matches!(res, Err(StoreError::CorruptWal { .. })),
        "{:?}",
        res.err()
    );
    assert!(snapshot(&work.0) == before);
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

fn resumed(
    checkpoint: u64,
    fallback: Option<u64>,
    quarantine: &str,
    found: &[String],
) -> RecoveryAction {
    RecoveryAction::CheckpointQuarantineResumed {
        checkpoint,
        fallback,
        quarantine: quarantine.to_owned(),
        found: found.to_vec(),
    }
}

/// The files the newest checkpoint of `build_with_both_manifests` alone
/// lists: its object and block tables and (with segment blocks) its runs.
fn own_files(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = all_tables(dir)
        .into_iter()
        .filter(|n| {
            n != "objtab-0000000000000000.tab"
                && n != "blktab-0000000000000000.tab"
                && n != "idrun-0000000000000001.run"
        })
        .collect();
    v.sort();
    v
}

/// An open interrupted while quarantining (its directory exists, the
/// damaged manifest still in place), at each point an open can stop: right
/// after creating the directory, or after moving some or all of the tables
/// and runs. The next open completes it in the same directory, reports the
/// files found there and the moves it makes, deletes nothing, and the open
/// after it reports nothing. In every write mode.
#[compio::test]
async fn an_interrupted_quarantine_is_completed_in_its_directory() {
    for mode in MODES {
        for moved_first in [0usize, 1, 2] {
            let dir = TempDir::new("checkpoint-quarantine-interrupted");
            let live = build_with_both_manifests(&dir.0, mode).await;
            edit(&dir.0.join("checkpoint-0000000000000002"), |b| b[13] ^= 1);
            let own = own_files(&dir.0);
            let moved_first = moved_first.min(own.len());
            std::fs::create_dir(dir.0.join(Q2)).unwrap();
            for f in &own[..moved_first] {
                std::fs::rename(dir.0.join(f), dir.0.join(Q2).join(f)).unwrap();
            }
            let store = Store::open(&dir.0, config(mode)).await.unwrap();
            let acts = actions(&store);
            let found = own[..moved_first].to_vec();
            assert_eq!(
                acts[0],
                resumed(2, Some(1), Q2, &found),
                "{mode:?} {moved_first}"
            );
            let mut now: Vec<String> = own[moved_first..].to_vec();
            now.push("checkpoint-0000000000000002".to_owned());
            now.sort();
            assert_eq!(moved(&acts, Q2), now, "{mode:?} {moved_first}");
            assert!(removed(&acts).is_empty(), "{mode:?}: {acts:?}");
            let mut all = own.clone();
            all.push("checkpoint-0000000000000002".to_owned());
            all.sort();
            assert_eq!(quarantine_files(&dir.0, Q2), all, "{mode:?} {moved_first}");
            assert!(!dir.0.join("quarantine-0000000000000002.1").exists());
            assert_eq!(store.quarantined_files(), all.len() as u64);
            check(&store, live.clone()).await;
            drop(store);
            let store = Store::open(&dir.0, config(mode)).await.unwrap();
            assert!(store.recovery_report().is_empty(), "{mode:?}");
        }
    }
}

/// A checkpoint whose manifest failed its read-back, interrupted while
/// quarantining (the manifest intact on disk, its own table and run moved):
/// the open completes the quarantine, falls back to the previous manifest
/// and reports it, in every write mode. If a file the manifest lists is
/// neither in place nor in that quarantine, the state is not explained: an
/// error, no file changed. Two interrupted quarantines of one manifest are
/// an error too.
#[compio::test]
async fn an_interrupted_write_time_quarantine_is_completed_and_falls_back() {
    for mode in MODES {
        let dir = TempDir::new("checkpoint-quarantine-write-time");
        let live = build_with_both_manifests(&dir.0, mode).await;
        let own = own_files(&dir.0);
        std::fs::create_dir(dir.0.join(Q2)).unwrap();
        for f in &own {
            std::fs::rename(dir.0.join(f), dir.0.join(Q2).join(f)).unwrap();
        }

        let missing = TempDir::new("checkpoint-quarantine-write-time-missing");
        common::copy_dir(&dir.0, &missing.0);
        std::fs::remove_file(missing.0.join(Q2).join(&own[0])).unwrap();
        let before = snapshot(&missing.0);
        let res = Store::open(&missing.0, config(mode)).await;
        assert!(
            matches!(
                res,
                Err(StoreError::CorruptCheckpoint { checkpoint: 2, .. })
            ),
            "{mode:?}: {:?}",
            res.err()
        );
        assert!(snapshot(&missing.0) == before, "{mode:?}: files changed");

        let twice = TempDir::new("checkpoint-quarantine-write-time-twice");
        common::copy_dir(&dir.0, &twice.0);
        std::fs::create_dir(twice.0.join("quarantine-0000000000000002.1")).unwrap();
        let before = snapshot(&twice.0);
        let res = Store::open(&twice.0, config(mode)).await;
        assert!(
            matches!(
                res,
                Err(StoreError::CorruptCheckpoint { checkpoint: 2, .. })
            ),
            "{mode:?}: {:?}",
            res.err()
        );
        assert!(snapshot(&twice.0) == before, "{mode:?}: files changed");

        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        let acts = actions(&store);
        assert_eq!(acts[0], resumed(2, Some(1), Q2, &own), "{mode:?}");
        assert_eq!(moved(&acts, Q2), ["checkpoint-0000000000000002"]);
        assert_eq!(store.checkpoint_number(), Some(1));
        check(&store, live).await;
        drop(store);
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
    }
}

/// The write-time quarantine interrupted earlier: right after the
/// directory's creation, or after only the first of its files moved. The
/// next open completes it in place, moving the rest, falls back to the
/// previous manifest and reports both, in every write mode.
#[compio::test]
async fn an_early_interrupted_write_time_quarantine_is_completed() {
    for mode in MODES {
        let own_len = {
            let probe = TempDir::new("checkpoint-quarantine-probe");
            build_with_both_manifests(&probe.0, mode).await;
            own_files(&probe.0).len()
        };
        for moved_first in 0..own_len {
            let dir = TempDir::new("checkpoint-quarantine-write-early");
            let live = build_with_both_manifests(&dir.0, mode).await;
            let own = own_files(&dir.0);
            std::fs::create_dir(dir.0.join(Q2)).unwrap();
            for f in &own[..moved_first] {
                std::fs::rename(dir.0.join(f), dir.0.join(Q2).join(f)).unwrap();
            }
            let store = Store::open(&dir.0, config(mode)).await.unwrap();
            let acts = actions(&store);
            assert_eq!(
                acts[0],
                resumed(2, Some(1), Q2, &own[..moved_first]),
                "{mode:?} {moved_first}"
            );
            let mut now: Vec<String> = own[moved_first..].to_vec();
            now.push("checkpoint-0000000000000002".to_owned());
            now.sort();
            assert_eq!(moved(&acts, Q2), now, "{mode:?} {moved_first}");
            assert!(removed(&acts).is_empty(), "{mode:?}: {acts:?}");
            assert_eq!(store.checkpoint_number(), Some(1));
            check(&store, live.clone()).await;
        }
    }
}

/// One open with an interrupted quarantine of the older manifest and a new
/// damage of the newer one: no intact manifest remains, so the state is
/// rebuilt from the WAL (`fallback: None`); the interrupted quarantine is
/// completed in its directory and the new one, the newest, takes the
/// tables and runs. In every write mode.
#[compio::test]
async fn an_interrupted_and_a_new_quarantine_in_one_open() {
    for mode in MODES {
        let dir = TempDir::new("checkpoint-quarantine-both");
        let live = build_with_both_manifests(&dir.0, mode).await;
        let q1 = "quarantine-0000000000000001";
        std::fs::create_dir(dir.0.join(q1)).unwrap();
        edit(&dir.0.join("checkpoint-0000000000000002"), |b| b[13] ^= 1);
        let mut tables: Vec<String> = all_tables(&dir.0);
        tables.push("checkpoint-0000000000000002".to_owned());
        tables.sort();
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        let acts = actions(&store);
        assert_eq!(acts[0], damaged(2, None, Q2), "{mode:?}");
        assert!(
            acts.contains(&resumed(1, None, q1, &[])),
            "{mode:?}: {acts:?}"
        );
        assert_eq!(moved(&acts, q1), ["checkpoint-0000000000000001"]);
        assert_eq!(moved(&acts, Q2), tables, "{mode:?}");
        assert!(removed(&acts).is_empty(), "{mode:?}: {acts:?}");
        assert_eq!(store.checkpoint_number(), None);
        check(&store, live).await;
        drop(store);
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
    }
}

/// A file (not a directory) under a quarantine name is not explained: open
/// fails before any change.
#[compio::test]
async fn a_quarantine_name_that_is_not_a_directory_fails_open() {
    let dir = TempDir::new("checkpoint-quarantine-file");
    build_with_both_manifests(&dir.0, WriteMode::Sequential).await;
    std::fs::write(dir.0.join(Q2), b"x").unwrap();
    std::fs::write(dir.0.join("checkpoint-0000000000000009.tmp"), b"half").unwrap();
    let before = snapshot(&dir.0);
    let res = Store::open(&dir.0, Config::default()).await;
    assert!(
        matches!(res, Err(StoreError::CorruptCheckpoint { .. })),
        "{:?}",
        res.err()
    );
    assert!(snapshot(&dir.0) == before, "files changed");
}

/// The dictionary goes to `dict-<n>.bin` at the first checkpoint after
/// training, listed by every later manifest; the next open compares it with
/// the WAL. A missing or damaged dictionary file fails open with no file
/// changed. In every write mode.
#[compio::test]
async fn the_dictionary_file_is_written_once_and_checked() {
    for mode in MODES {
        let dir = TempDir::new("checkpoint-dict");
        let cfg = Config {
            dict_train_bytes: 2048,
            ..config(mode)
        };
        let mut store = Store::open(&dir.0, cfg).await.unwrap();
        put(&mut store, 0, 60).await;
        store.checkpoint().await.unwrap();
        assert!(
            names(&dir.0, "dict-").is_empty(),
            "{mode:?}: no training yet"
        );
        for b in 1..6 {
            put(&mut store, b * 60, (b + 1) * 60).await;
        }
        assert!(store.dictionary_bytes().is_some(), "{mode:?}: trained");
        store.checkpoint().await.unwrap();
        put(&mut store, 360, 400).await;
        store.checkpoint().await.unwrap();
        assert_eq!(names(&dir.0, "dict-"), ["dict-0000000000000001.bin"]);
        drop(store);
        let store = Store::open(&dir.0, cfg).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        check(&store, 0..400).await;
        drop(store);
        // The newest manifest claims no dictionary on a trained store.
        {
            let work = TempDir::new("checkpoint-dict-none");
            common::copy_dir(&dir.0, &work.0);
            edit(&path_of(&work.0, "checkpoint-", 0), |b| {
                b[76..84].fill(0);
                reseal_manifest(b);
            });
            let before = snapshot(&work.0);
            let res = Store::open(&work.0, cfg).await;
            assert!(
                matches!(
                    res,
                    Err(StoreError::CorruptCheckpoint { checkpoint: 3, .. })
                ),
                "{mode:?}: {:?}",
                res.err()
            );
            assert!(snapshot(&work.0) == before, "{mode:?}: files changed");
        }
        for (what, change) in [
            (
                "missing",
                (|p: &Path| std::fs::remove_file(p).unwrap()) as fn(&Path),
            ),
            ("damaged", |p: &Path| edit(p, |b| b[30] ^= 1)),
            // Intact, but other bytes than the WAL's PutDict.
            ("resealed with other bytes", |p: &Path| {
                edit(p, |b| {
                    b[30] ^= 1;
                    reseal_manifest(b);
                })
            }),
        ] {
            let work = TempDir::new("checkpoint-dict-work");
            common::copy_dir(&dir.0, &work.0);
            change(&work.0.join("dict-0000000000000001.bin"));
            let before = snapshot(&work.0);
            let res = Store::open(&work.0, cfg).await;
            assert!(
                matches!(
                    res,
                    Err(StoreError::CorruptCheckpoint { checkpoint: 3, .. })
                ),
                "{mode:?} {what}: {:?}",
                res.err()
            );
            assert!(
                snapshot(&work.0) == before,
                "{mode:?} {what}: files changed"
            );
        }
    }
}

/// A block allocated after a checkpoint and reclaimed before the next is a
/// number with no block in the next block table (all ones); the open
/// matches it with the WAL. In every write mode.
#[compio::test]
async fn a_block_reclaimed_between_checkpoints_is_a_hole_in_the_table() {
    for mode in MODES {
        let dir = TempDir::new("checkpoint-block-hole");
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        put(&mut store, 0, 30).await;
        store.checkpoint().await.unwrap();
        // Unique bodies, released and compacted away before the next one.
        let bodies: Vec<Vec<u8>> = (0..3)
            .map(|i| format!("short-lived {i} {}", "z".repeat(5000)).into_bytes())
            .collect();
        let batch: Vec<Message> = bodies
            .iter()
            .map(|p| Message {
                topic: b"u",
                payload: p,
                ..Default::default()
            })
            .collect();
        store.put_batch(&batch, 1).await.unwrap();
        store.release_before(b"u", 2).await.unwrap();
        store.compact().await.unwrap();
        put(&mut store, 30, 40).await;
        store.checkpoint().await.unwrap();
        drop(store);
        let table = std::fs::read(path_of(&dir.0, "blktab-", 1)).unwrap();
        let holes = table[24..]
            .chunks(45)
            .filter(|e| e.len() == 45 && e.iter().all(|&b| b == 0xff))
            .count();
        if mode != WriteMode::INLINE_SMALL {
            assert!(holes >= 1, "{mode:?}: no hole in {} bytes", table.len());
        }
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        check(&store, 0..40).await;
    }
}
