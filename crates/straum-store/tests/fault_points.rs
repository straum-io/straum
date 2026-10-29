//! Deterministic fault injection (owner, required before ADR-0014 step
//! 6b): a crash at every checkpoint step (object table, relocation run,
//! block table, block run, id run, dictionary file, the directory sync
//! after them, the tmp manifest, the rename and its sync, the deletion of
//! the previous manifest and its sync, the deletion of merged runs), in
//! the write-time quarantine, in a quarantine at open, and before open
//! returns its report. Each is followed by reopen and full verification:
//! every acknowledged message reads back, every file recovery deleted or
//! moved is in the report, a second open changes nothing, and the next
//! checkpoint and reopen are clean. In every write mode.
//!
//! A fault stops the operation right after the named step, with every
//! earlier step done and durable. States where a crash also loses an
//! unsynced directory entry are built from these by removing the entry.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use std::collections::BTreeSet;
use std::path::Path;

use common::{TempDir, edit};
use straum_store::{
    Config, FaultPoint, Message, RecoveryAction, Store, StoreError, WriteMode, arm_fault,
    disarm_fault,
};

const MODES: [WriteMode; 3] = [
    WriteMode::Sequential,
    WriteMode::ParallelSync,
    WriteMode::INLINE_SMALL,
];

fn config(mode: WriteMode) -> Config {
    Config {
        write_mode: mode,
        // Trains early: the dictionary file comes with the second
        // checkpoint, the one the faults hit.
        dict_train_bytes: 4096,
        recent_id_cache_entries: 0,
        ..Config::default()
    }
}

fn body(i: u64) -> Vec<u8> {
    format!("message {i} {}", "w".repeat((i % 60) as usize)).into_bytes()
}

/// Acknowledged messages: (topic, seq, body index).
type Live = Vec<(&'static [u8], u64, u64)>;

async fn put(store: &mut Store, topic: &'static [u8], from: u64, to: u64, live: &mut Live) {
    let bodies: Vec<Vec<u8>> = (from..to).map(body).collect();
    let batch: Vec<Message> = bodies
        .iter()
        .map(|p| Message {
            topic,
            payload: p,
            ..Default::default()
        })
        .collect();
    let seqs = store.put_batch(&batch, from).await.unwrap();
    live.extend(seqs.into_iter().zip(from..to).map(|(s, i)| (topic, s, i)));
}

/// A store with one checkpoint and work since: new objects, a released
/// and compacted prefix (relocated objects and blocks), the dictionary
/// trained after the first checkpoint, and three earlier id runs of one
/// tier, so the next checkpoint merges them.
async fn prepare(dir: &Path, mode: WriteMode) -> (Store, Live) {
    let mut store = Store::open(dir, config(mode)).await.unwrap();
    let mut live = Live::new();
    for k in 0..3 {
        put(&mut store, b"a", k * 20, k * 20 + 20, &mut live).await;
        store.checkpoint().await.unwrap();
    }
    // Timestamps are the batches' first numbers: 0 and 20 fall below 25.
    assert_eq!(store.release_before(b"a", 25).await.unwrap(), 40);
    live.retain(|&(t, _, i)| !(t == b"a" && i < 40));
    store.compact().await.unwrap();
    // 60 new ids, the earlier runs' tier (log4 of 20 to 63 is 2): the next
    // checkpoint merges the four. The second batch repeats a body and
    // trains the dictionary (about 4.8 KiB raw stored by then).
    put(&mut store, b"b", 100, 160, &mut live).await;
    put(&mut store, b"b", 100, 101, &mut live).await;
    assert!(store.dictionary_bytes().is_some(), "{mode:?}: trained");
    (store, live)
}

fn names(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect()
}

/// Reopens and verifies: every live message reads back, every top-level
/// file that disappeared is named by a report action, a second open is
/// clean, the next checkpoint and its reopen are clean.
async fn reopen_and_verify(dir: &Path, mode: WriteMode, live: &Live, what: &str) {
    disarm_fault();
    let before = names(dir);
    let store = Store::open(dir, config(mode))
        .await
        .unwrap_or_else(|e| panic!("{what}: {e}"));
    let after = names(dir);
    let actions = store.recovery_report().actions.clone();
    let named: BTreeSet<String> = actions
        .iter()
        .filter_map(|a| match a {
            RecoveryAction::CheckpointFileRemoved { file }
            | RecoveryAction::CheckpointFileQuarantined { file, .. } => Some(file.clone()),
            RecoveryAction::OrphanSegmentRemoved { segment } => {
                Some(format!("seg-{segment:016x}.dat"))
            }
            _ => None,
        })
        .collect();
    for gone in before.difference(&after) {
        assert!(
            named.contains(gone) || gone.ends_with(".tmp") && gone.starts_with("wal-"),
            "{what}: {gone} changed with no report entry ({actions:?})"
        );
    }
    for &(topic, seq, i) in live {
        assert_eq!(
            store.read(topic, seq).await.unwrap(),
            Some(body(i)),
            "{what}: {topic:?}/{seq}"
        );
    }
    drop(store);
    let mut store = Store::open(dir, config(mode)).await.unwrap();
    assert!(
        store.recovery_report().is_empty(),
        "{what}: second open {:?}",
        store.recovery_report()
    );
    store.checkpoint().await.unwrap();
    drop(store);
    let store = Store::open(dir, config(mode)).await.unwrap();
    assert!(
        store.recovery_report().is_empty(),
        "{what}: after a checkpoint"
    );
    for &(topic, seq, i) in live {
        assert_eq!(
            store.read(topic, seq).await.unwrap(),
            Some(body(i)),
            "{what}"
        );
    }
}

const CHECKPOINT_POINTS: [FaultPoint; 13] = [
    FaultPoint::CheckpointObjectTable,
    FaultPoint::CheckpointRelocationRun,
    FaultPoint::CheckpointBlockTable,
    FaultPoint::CheckpointBlockRun,
    FaultPoint::CheckpointIdRun,
    FaultPoint::CheckpointDictionary,
    FaultPoint::CheckpointFilesSynced,
    FaultPoint::CheckpointManifestTmp,
    FaultPoint::CheckpointRenamed,
    FaultPoint::CheckpointRenameSynced,
    FaultPoint::CheckpointPreviousDeleted,
    FaultPoint::CheckpointPreviousDeleteSynced,
    FaultPoint::CheckpointMergedRunsDeleted,
];

/// A crash at every checkpoint step: the checkpoint fails (a point it
/// does not reach in a mode, such as a relocation run without segment
/// blocks, lets it complete), the store refuses further use, and the
/// reopen recovers everything.
#[compio::test]
async fn a_crash_at_every_checkpoint_step_recovers() {
    for mode in MODES {
        let mut fired = Vec::new();
        for point in CHECKPOINT_POINTS {
            let dir = TempDir::new("fault-checkpoint");
            let (mut store, live) = prepare(&dir.0, mode).await;
            arm_fault(point, 1);
            let res = store.checkpoint().await;
            if res.is_err() {
                fired.push(point);
                assert!(
                    matches!(store.checkpoint().await, Err(StoreError::Poisoned)),
                    "{mode:?} {point:?}"
                );
            }
            drop(store);
            reopen_and_verify(&dir.0, mode, &live, &format!("{mode:?} {point:?}")).await;
        }
        // Inline blocks are not compacted, so no run is written for them.
        let unreached: Vec<FaultPoint> = CHECKPOINT_POINTS
            .into_iter()
            .filter(|p| !fired.contains(p))
            .collect();
        let allowed: &[FaultPoint] = match mode {
            WriteMode::InlineSmall { .. } => &[
                FaultPoint::CheckpointRelocationRun,
                FaultPoint::CheckpointBlockRun,
            ],
            _ => &[],
        };
        assert!(
            unreached.iter().all(|p| allowed.contains(p)),
            "{mode:?}: points never reached {unreached:?}"
        );
    }
}

/// A crash right after the tmp manifest's rename, when the rename's
/// directory entry is lost (LazyFS applies renames at once, so only this
/// hook reaches it): the tmp manifest is back, the new manifest gone.
#[compio::test]
async fn a_lost_rename_recovers_from_the_previous_manifest() {
    for mode in MODES {
        let dir = TempDir::new("fault-lost-rename");
        let (mut store, live) = prepare(&dir.0, mode).await;
        let prev = store.checkpoint_number().unwrap();
        arm_fault(FaultPoint::CheckpointRenamed, 1);
        assert!(store.checkpoint().await.is_err());
        drop(store);
        let new = format!("checkpoint-{:016x}", prev + 1);
        std::fs::rename(dir.0.join(&new), dir.0.join(format!("{new}.tmp"))).unwrap();
        reopen_and_verify(&dir.0, mode, &live, &format!("{mode:?} lost rename")).await;
    }
}

/// The write-time quarantine (a manifest that does not read back), alone
/// and interrupted after its directory, after each moved file and after
/// each synced group: the checkpoint returns an error, the previous
/// manifest stays in force, and the reopen completes and reports an
/// interrupted quarantine.
#[compio::test]
async fn the_write_time_quarantine_recovers_at_every_step() {
    for mode in MODES {
        let mut cases: Vec<(&str, Option<(FaultPoint, u32)>)> = vec![
            ("alone", None),
            ("dir created", Some((FaultPoint::QuarantineDirCreated, 1))),
            (
                "first group synced",
                Some((FaultPoint::QuarantineGroupSynced, 1)),
            ),
        ];
        for k in 1..=4 {
            cases.push(("moved", Some((FaultPoint::QuarantineMoved, k))));
        }
        for (what, then) in cases {
            let dir = TempDir::new("fault-write-quarantine");
            let (mut store, live) = prepare(&dir.0, mode).await;
            let prev = store.checkpoint_number();
            arm_fault(FaultPoint::CheckpointReadBackMismatch, 1);
            if let Some((p, k)) = then {
                arm_fault(p, k);
            }
            assert!(store.checkpoint().await.is_err(), "{mode:?} {what}");
            drop(store);
            let what = format!("{mode:?} write-time quarantine {what} {then:?}");
            reopen_and_verify(&dir.0, mode, &live, &what).await;
            let store = Store::open(&dir.0, config(mode)).await.unwrap();
            assert!(store.checkpoint_number() > prev, "{what}");
            assert!(store.quarantined_files() > 0, "{what}: kept");
        }
    }
}

/// A damaged newest manifest quarantined at open, the open interrupted
/// after the directory, after each moved file, after the first synced
/// group, and before it returns its report: the next open completes the
/// quarantine in the same directory. A crash after the last move and
/// before the report leaves those moves unreported (the window D-260's
/// recovery log closes); the data is intact either way.
#[compio::test]
async fn a_quarantine_at_open_recovers_at_every_step() {
    for mode in MODES {
        let mut cases: Vec<(FaultPoint, u32)> = vec![
            (FaultPoint::QuarantineDirCreated, 1),
            (FaultPoint::QuarantineGroupSynced, 1),
            (FaultPoint::QuarantineGroupSynced, 2),
            (FaultPoint::OpenBeforeReport, 1),
        ];
        for k in 1..=4 {
            cases.push((FaultPoint::QuarantineMoved, k));
        }
        for (point, k) in cases {
            let dir = TempDir::new("fault-open-quarantine");
            let (mut store, live) = prepare(&dir.0, mode).await;
            store.checkpoint().await.unwrap();
            let n = store.checkpoint_number().unwrap();
            drop(store);
            // The newest manifest damaged, with the previous one kept (a
            // crash between the rename and the previous manifest's
            // deletion leaves it so).
            edit(&dir.0.join(format!("checkpoint-{n:016x}")), |b| b[13] ^= 1);
            arm_fault(point, k);
            let res = Store::open(&dir.0, config(mode)).await;
            let what = format!("{mode:?} open quarantine {point:?} {k}");
            // Every case stops the open: the quarantine moves the manifest,
            // its tables and runs, at least four files.
            assert!(res.is_err(), "{what}: the fault did not fire");
            drop(res);
            let complete = point == FaultPoint::OpenBeforeReport
                || (point == FaultPoint::QuarantineGroupSynced && k == 2);
            if complete {
                // The D-260 window: complete, but its report was lost.
                disarm_fault();
                let store = Store::open(&dir.0, config(mode)).await.unwrap();
                assert!(store.recovery_report().is_empty(), "{what}");
                assert!(store.quarantined_files() >= 4, "{what}");
                drop(store);
            } else {
                disarm_fault();
                let store = Store::open(&dir.0, config(mode)).await.unwrap();
                assert!(
                    matches!(
                        store.recovery_report().actions.first(),
                        Some(RecoveryAction::CheckpointQuarantineResumed { .. })
                    ),
                    "{what}: {:?}",
                    store.recovery_report()
                );
                drop(store);
            }
            reopen_and_verify(&dir.0, mode, &live, &what).await;
        }
    }
}

/// Crashes that also lose unsynced directory entries: the new tables and
/// runs of a checkpoint whose directory sync never ran, and the unlink of
/// the previous manifest whose sync never ran (it is back on disk).
#[compio::test]
async fn lost_directory_entries_recover() {
    for mode in MODES {
        let dir = TempDir::new("fault-lost-entries");
        let (mut store, live) = prepare(&dir.0, mode).await;
        let before = names(&dir.0);
        arm_fault(FaultPoint::CheckpointDictionary, 1);
        assert!(store.checkpoint().await.is_err(), "{mode:?}");
        drop(store);
        for new in names(&dir.0).difference(&before) {
            std::fs::remove_file(dir.0.join(new)).unwrap();
        }
        reopen_and_verify(&dir.0, mode, &live, &format!("{mode:?} lost new entries")).await;

        let dir = TempDir::new("fault-lost-unlink");
        let (mut store, live) = prepare(&dir.0, mode).await;
        let prev = store.checkpoint_number().unwrap();
        let prev_name = format!("checkpoint-{prev:016x}");
        let prev_bytes = std::fs::read(dir.0.join(&prev_name)).unwrap();
        arm_fault(FaultPoint::CheckpointPreviousDeleted, 1);
        assert!(store.checkpoint().await.is_err(), "{mode:?}");
        drop(store);
        std::fs::write(dir.0.join(&prev_name), prev_bytes).unwrap();
        disarm_fault();
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        // The previous manifest, and the id runs the merge superseded,
        // deleted as files the manifest in use does not list.
        let acts = &store.recovery_report().actions;
        assert!(
            acts.contains(&RecoveryAction::CheckpointFileRemoved {
                file: prev_name.clone()
            }) && acts
                .iter()
                .all(|a| matches!(a, RecoveryAction::CheckpointFileRemoved { .. })),
            "{mode:?}: {acts:?}"
        );
        drop(store);
        reopen_and_verify(&dir.0, mode, &live, &format!("{mode:?} lost unlink")).await;
    }
}
