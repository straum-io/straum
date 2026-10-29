//! D-227 A: data segments and earlier WAL files are opened on demand for
//! reads, at most `Config::data_open_files` at a time, plus the segment and
//! the WAL file being written.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::{Rng, TempDir};
use straum_store::{Config, Message, Store, WriteMode};

fn payload(i: u64) -> Vec<u8> {
    let mut rng = Rng(i | 1);
    (0..200).map(|_| rng.next() as u8).collect()
}

/// One message per batch; `bound` open files for reads.
async fn write(store: &mut Store, from: u64, to: u64) {
    for i in from..to {
        let p = payload(i);
        let m = Message {
            topic: b"t",
            payload: &p,
            ..Default::default()
        };
        store.put_batch(&[m], i).await.unwrap();
    }
}

async fn read_all(store: &Store, live: std::ops::Range<u64>, bound: usize) {
    // Out of order, so the least recently used handle is closed often.
    let mut rng = Rng(7);
    for _ in 0..3 * (live.end - live.start) {
        let i = live.start + rng.below(live.end - live.start);
        assert_eq!(store.read(b"t", i).await.unwrap(), Some(payload(i)), "{i}");
        // The bound, plus the segment and the WAL file being written.
        assert!(
            store.open_data_files() <= bound + 2,
            "{}",
            store.open_data_files()
        );
    }
}

#[compio::test]
async fn reads_across_more_segments_than_the_bound() {
    for mode in [WriteMode::Sequential, WriteMode::ParallelSync] {
        let dir = TempDir::new("openfiles-segments");
        let config = Config {
            write_mode: mode,
            // A segment per batch or two.
            segment_max_bytes: 256,
            data_open_files: 2,
            ..Config::default()
        };
        let mut store = Store::open(&dir.0, config).await.unwrap();
        write(&mut store, 0, 40).await;
        assert!(store.segment_count() > 10, "{}", store.segment_count());
        read_all(&store, 0..40, 2).await;
        drop(store);
        let store = Store::open(&dir.0, config).await.unwrap();
        assert!(store.recovery_report().is_empty());
        assert!(store.open_data_files() <= 3, "only the WAL file after open");
        read_all(&store, 0..40, 2).await;
    }
}

#[compio::test]
async fn compaction_with_one_open_file() {
    let dir = TempDir::new("openfiles-compact");
    let config = Config {
        segment_max_bytes: 512,
        data_open_files: 1,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    write(&mut store, 0, 30).await;
    assert_eq!(store.release_before(b"t", 15).await.unwrap(), 15);
    let stats = store.compact().await.unwrap();
    assert!(stats.segments_rewritten > 0);
    read_all(&store, 15..30, 1).await;
    write(&mut store, 30, 40).await;
    drop(store);
    let store = Store::open(&dir.0, config).await.unwrap();
    assert!(store.recovery_report().is_empty());
    read_all(&store, 15..40, 1).await;
}

#[compio::test]
async fn inline_blocks_across_more_wal_files_than_the_bound() {
    let dir = TempDir::new("openfiles-wal");
    let config = Config {
        write_mode: WriteMode::INLINE_SMALL,
        // A WAL file per frame: every block lies in its own WAL file.
        wal_file_max_bytes: 1,
        data_open_files: 2,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    write(&mut store, 0, 30).await;
    assert_eq!(
        store.is_inline(&straum_store::ObjectId::of(&payload(3))),
        Some(true)
    );
    read_all(&store, 0..30, 2).await;
    drop(store);
    let store = Store::open(&dir.0, config).await.unwrap();
    read_all(&store, 0..30, 2).await;
}
