//! `Store::recovery_report` lists every change recovery made; a clean open
//! reports nothing.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use std::path::Path;

use common::{TempDir, snapshot, wal_frame_starts};
use straum_store::{Config, Message, RecoveryAction, Store, StoreError};

async fn build(dir: &Path) {
    let mut store = Store::open(dir, Config::default()).await.unwrap();
    assert!(
        store.recovery_report().is_empty(),
        "new store: nothing to recover"
    );
    for i in 0..3u8 {
        let p = [i; 32];
        let batch = [Message {
            topic: b"t",
            payload: &p,
            ..Default::default()
        }];
        store.put_batch(&batch, u64::from(i)).await.unwrap();
    }
}

#[compio::test]
async fn clean_open_reports_nothing_and_changes_nothing() {
    let dir = TempDir::new("report-clean");
    build(&dir.0).await;
    let before = snapshot(&dir.0);
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert!(
        store.recovery_report().is_empty(),
        "{:?}",
        store.recovery_report()
    );
    assert!(snapshot(&dir.0) == before, "files changed");
}

#[compio::test]
async fn torn_tail_is_truncated_and_reported() {
    let dir = TempDir::new("report-torn");
    build(&dir.0).await;
    let path = dir.0.join("wal.log");
    let wal = std::fs::read(&path).unwrap();
    let last = *wal_frame_starts(&wal).last().unwrap();
    let cut = last + 10;
    std::fs::write(&path, &wal[..cut]).unwrap();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    let actions = &store.recovery_report().actions;
    assert_eq!(
        actions.first(),
        Some(&RecoveryAction::TornTailTruncated {
            frame_seq: 2,
            offset: last as u64,
            bytes_removed: 10,
        }),
        "{actions:?}"
    );
    // The dropped entry was in the tail, which is in memory only.
    assert_eq!(actions.len(), 1, "{actions:?}");
    drop(store);
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert!(store.recovery_report().is_empty(), "second open is clean");
}

#[compio::test]
async fn orphans_and_lost_files_are_reported() {
    let dir = TempDir::new("report-orphans");
    build(&dir.0).await;
    std::fs::write(dir.0.join("seg-00000000000000ff.dat"), b"orphan").unwrap();
    // An index segment whose header never became durable.
    std::fs::write(dir.0.join("idx-0000000000000007.dat"), b"STRMIDX").unwrap();
    std::fs::write(dir.0.join("wal.log.tmp"), b"half-created").unwrap();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    let actions = &store.recovery_report().actions;
    assert!(
        actions.contains(&RecoveryAction::WalTmpRemoved),
        "{actions:?}"
    );
    assert!(
        actions.contains(&RecoveryAction::OrphanSegmentRemoved { segment: 0xff }),
        "{actions:?}"
    );
    assert!(
        actions.contains(&RecoveryAction::IndexSegmentRemoved {
            segment: 7,
            bytes: 7
        }),
        "{actions:?}"
    );
    assert_eq!(actions.len(), 3, "{actions:?}");
    assert!(!dir.0.join("idx-0000000000000007.dat").exists());
    assert_eq!(store.read(b"t", 2).await.unwrap(), Some(vec![2; 32]));
}

#[compio::test]
async fn old_format_version_is_unsupported() {
    let dir = TempDir::new("report-old");
    build(&dir.0).await;
    let path = dir.0.join("wal.log");
    let mut wal = std::fs::read(&path).unwrap();
    wal[8..12].copy_from_slice(&1u32.to_le_bytes());
    let crc = crc32c::crc32c(&wal[..20]);
    wal[20..24].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &wal).unwrap();
    let before = snapshot(&dir.0);
    let err = Store::open(&dir.0, Config::default()).await.err().unwrap();
    assert!(matches!(err, StoreError::UnsupportedFormat), "{err}");
    assert!(snapshot(&dir.0) == before, "files changed");
}
