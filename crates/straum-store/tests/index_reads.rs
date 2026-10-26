//! Live topic entries are read from the shared index segments, not held in
//! memory: a full block's record is checked against its CRC, topic and
//! block number on every read, and so is every seek page on the way to it;
//! the entries of the last, incomplete block come from memory.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::{INDEX_BLOCK_RECORD, INDEX_SEEK_PAGE, TempDir, edit, find_index_record};
use straum_store::{Config, Message, Store, StoreError};

fn msg<'a>(topic: &'a [u8], payload: &'a [u8]) -> Message<'a> {
    Message { topic, payload }
}

fn payload(i: u64) -> Vec<u8> {
    format!("message {i}").into_bytes()
}

/// Writes `n` messages to topic `t` in batches of 10; a batch's time is
/// its first message's index.
async fn fill(store: &mut Store, n: u64) {
    let mut i = 0;
    while i < n {
        let bodies: Vec<Vec<u8>> = (i..(i + 10).min(n)).map(payload).collect();
        let batch: Vec<Message> = bodies.iter().map(|b| msg(b"t", b)).collect();
        store.put_batch(&batch, i).await.unwrap();
        i += bodies.len() as u64;
    }
}

#[compio::test]
async fn reads_span_full_blocks_and_the_tail_before_and_after_reopen() {
    let dir = TempDir::new("index-reads");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    // 2 full blocks of 64 and a tail of 2.
    fill(&mut store, 130).await;
    for round in 0..2 {
        assert_eq!(store.topic_range(b"t"), Some((0, 130)));
        for seq in [0, 1, 63, 64, 100, 127, 128, 129] {
            let got = store.read(b"t", seq).await.unwrap();
            assert_eq!(got, Some(payload(seq)), "round {round} seq {seq}");
        }
        assert_eq!(store.read(b"t", 130).await.unwrap(), None);
        drop(store);
        store = Store::open(&dir.0, Config::default()).await.unwrap();
    }
    // Releasing across a block boundary reads the oldest entries from the
    // file; released entries read as None, the rest as before.
    assert_eq!(store.release_before(b"t", 70).await.unwrap(), 70);
    assert_eq!(store.topic_range(b"t"), Some((70, 130)));
    assert_eq!(store.read(b"t", 69).await.unwrap(), None);
    assert_eq!(store.read(b"t", 70).await.unwrap(), Some(payload(70)));
    assert_eq!(
        store.index_entry(b"t", 15).await.unwrap().map(|e| e.1),
        Some(10)
    );
}

#[compio::test]
async fn a_full_block_damaged_after_open_fails_every_read_of_it() {
    let dir = TempDir::new("index-corrupt");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    fill(&mut store, 130).await;
    drop(store);
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert!(store.recovery_report().is_empty());
    // Flip a byte inside block 0's record while the store is open.
    let rec = find_index_record(&dir.0, INDEX_BLOCK_RECORD, 0, 0);
    edit(&rec.path, |b| b[rec.body.start + 3] ^= 0xff);
    let wal_before = std::fs::read(dir.0.join("wal.log")).unwrap();

    let corrupt = |e: StoreError| matches!(e, StoreError::TopicIndexCorrupt { block: 0, .. });
    assert!(corrupt(store.read(b"t", 5).await.unwrap_err()));
    assert!(corrupt(store.index_entry(b"t", 5).await.unwrap_err()));
    assert!(corrupt(store.entry(b"t", 5).await.unwrap_err()));
    assert!(corrupt(store.release_before(b"t", 70).await.unwrap_err()));
    // Nothing was written, and block 1 and the tail still read.
    assert_eq!(std::fs::read(dir.0.join("wal.log")).unwrap(), wal_before);
    assert_eq!(store.read(b"t", 100).await.unwrap(), Some(payload(100)));
    assert_eq!(store.read(b"t", 129).await.unwrap(), Some(payload(129)));
}

/// Two locations per seek page: 20 full blocks need pages on four levels,
/// and a lookup of an old block reads up to four pages.
fn tiny_pages() -> Config {
    Config {
        seek_page_entries: 2,
        ..Config::default()
    }
}

#[compio::test]
async fn paged_seek_tables_find_every_entry() {
    let dir = TempDir::new("index-seek");
    let mut store = Store::open(&dir.0, tiny_pages()).await.unwrap();
    // 20 full blocks and a tail of 5.
    fill(&mut store, 20 * 64 + 5).await;
    for round in 0..2 {
        for seq in 0..20 * 64 + 5 {
            let got = store.read(b"t", seq).await.unwrap();
            assert_eq!(got, Some(payload(seq)), "round {round} seq {seq}");
        }
        // Backwards too, so the per-topic block cache never helps.
        for seq in (0..20 * 64).rev().step_by(37) {
            let got = store.read(b"t", seq).await.unwrap();
            assert_eq!(got, Some(payload(seq)), "round {round} seq {seq}");
        }
        drop(store);
        store = Store::open(&dir.0, tiny_pages()).await.unwrap();
        assert!(store.recovery_report().is_empty(), "round {round}");
    }
    // Another page size at open rewrites the pages (reported); the blocks
    // stay.
    drop(store);
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert!(
        store.recovery_report().is_empty(),
        "20 blocks: no full page of 256"
    );
    assert_eq!(store.read(b"t", 1000).await.unwrap(), Some(payload(1000)));
}

#[compio::test]
async fn a_damaged_seek_page_fails_the_blocks_under_it() {
    let dir = TempDir::new("index-seek-corrupt");
    let mut store = Store::open(&dir.0, tiny_pages()).await.unwrap();
    // Blocks 0..20: level-0 pages hold 2 blocks each, so page 1 holds
    // blocks 2 and 3, and the level-1 page 0 (blocks 0 to 3) points at it.
    fill(&mut store, 20 * 64 + 5).await;
    drop(store);
    let store = Store::open(&dir.0, tiny_pages()).await.unwrap();
    let page = common::index_records(&dir.0)
        .into_iter()
        .find(|r| {
            r.kind == INDEX_SEEK_PAGE
                && r.number == 1
                && std::fs::read(&r.path).unwrap()[r.body.start] == 0
        })
        .unwrap();
    edit(&page.path, |b| b[page.body.start + 3] ^= 0xff);
    let corrupt = |e: StoreError, want| matches!(e, StoreError::TopicIndexCorrupt { block, .. } if block == want);
    assert!(corrupt(store.read(b"t", 2 * 64).await.unwrap_err(), 2));
    assert!(corrupt(store.read(b"t", 3 * 64 + 9).await.unwrap_err(), 3));
    // Blocks under other pages, and the tail, still read.
    for seq in [0, 64 + 1, 4 * 64, 19 * 64 + 63, 20 * 64 + 4] {
        assert_eq!(store.read(b"t", seq).await.unwrap(), Some(payload(seq)));
    }
    // At open the damaged page ends what is read of the segment: it and
    // every record after it (blocks 4 to 19, and their pages) are
    // rewritten and reported.
    drop(store);
    let store = Store::open(&dir.0, tiny_pages()).await.unwrap();
    assert_eq!(
        store.recovery_report().actions,
        [straum_store::RecoveryAction::TopicIndexRepaired {
            topic: b"t".to_vec(),
            blocks_rewritten: 16,
            pages_rewritten: 17,
        }]
    );
    assert_eq!(
        store.read(b"t", 2 * 64).await.unwrap(),
        Some(payload(2 * 64))
    );
}

/// With one segment per batch and at most two kept open for reads, reading
/// every block of 20 segments never holds more than three files open.
#[compio::test]
async fn open_index_files_stay_bounded() {
    let dir = TempDir::new("index-fds");
    let config = Config {
        index_segment_max_bytes: 1,
        index_open_files: 2,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    for b in 0..20u64 {
        let bodies: Vec<Vec<u8>> = (b * 64..(b + 1) * 64).map(payload).collect();
        let batch: Vec<Message> = bodies.iter().map(|p| msg(b"t", p)).collect();
        store.put_batch(&batch, b).await.unwrap();
    }
    assert_eq!(common::index_segments(&dir.0).len(), 20);
    for round in 0..2 {
        for seq in (0..20 * 64).step_by(17) {
            assert_eq!(store.read(b"t", seq).await.unwrap(), Some(payload(seq)));
            assert!(store.open_index_files() <= 3, "round {round}");
        }
        drop(store);
        store = Store::open(&dir.0, config).await.unwrap();
        assert_eq!(store.open_index_files(), 0, "nothing open after open");
    }
}

/// `release_before` stops reading entries as soon as one more release would
/// not fit a frame: `FrameTooLarge`, and nothing is written.
#[compio::test]
async fn a_release_too_large_for_a_frame_fails_before_writing() {
    let dir = TempDir::new("index-release-bound");
    let config = Config {
        max_frame_bytes: 4096,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    fill(&mut store, 2000).await;
    let wal_before = std::fs::read(dir.0.join("wal.log")).unwrap();
    assert!(matches!(
        store.release_before(b"t", u64::MAX).await,
        Err(StoreError::FrameTooLarge)
    ));
    assert_eq!(std::fs::read(dir.0.join("wal.log")).unwrap(), wal_before);
    // A release that fits still works.
    assert_eq!(store.release_before(b"t", 10).await.unwrap(), 10);
}
