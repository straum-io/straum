//! Live topic entries are read from the topic index file, not held in
//! memory: entries of full blocks are checked against the block's CRC on
//! every read, the entries of the last, incomplete block come from memory.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::TempDir;
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
    // Flip a byte inside block 0 of the index while the store is open.
    let index = dir.0.join("topics").join(format!("{:02x}.idx", b't'));
    let mut bytes = std::fs::read(&index).unwrap();
    bytes[3] ^= 0xff;
    std::fs::write(&index, &bytes).unwrap();
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

/// A seek table of at most 2 entries: with 20 full blocks the stride grows
/// to 16, so reads walk up to 15 blocks from the nearest kept offset.
fn tiny_seek() -> Config {
    Config {
        seek_table_max_entries: 2,
        ..Config::default()
    }
}

#[compio::test]
async fn a_bounded_seek_table_still_finds_every_entry() {
    let dir = TempDir::new("index-seek");
    let mut store = Store::open(&dir.0, tiny_seek()).await.unwrap();
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
        store = Store::open(&dir.0, tiny_seek()).await.unwrap();
    }
}

#[compio::test]
async fn a_damaged_block_fails_every_walk_through_it() {
    let dir = TempDir::new("index-seek-corrupt");
    let mut store = Store::open(&dir.0, tiny_seek()).await.unwrap();
    fill(&mut store, 20 * 64 + 5).await;
    drop(store);
    let store = Store::open(&dir.0, tiny_seek()).await.unwrap();
    // Find block 2's start by decoding from the file start (each full
    // block ends with a 4-byte CRC after its 64 entries).
    let index = dir.0.join("topics").join(format!("{:02x}.idx", b't'));
    let mut bytes = std::fs::read(&index).unwrap();
    let mut at = 0;
    for _ in 0..2 {
        let mut entries = 0;
        while entries < 2 * 64 {
            // Two varints per entry.
            while bytes[at] & 0x80 != 0 {
                at += 1;
            }
            at += 1;
            entries += 1;
        }
        at += 4;
    }
    bytes[at + 1] ^= 0xff;
    std::fs::write(&index, &bytes).unwrap();
    let corrupt = |e: StoreError| matches!(e, StoreError::TopicIndexCorrupt { block: 2, .. });
    // Blocks 0 and 1 lie before the damage in the same span: still read.
    assert_eq!(store.read(b"t", 64).await.unwrap(), Some(payload(64)));
    // Block 2 itself, and block 5, whose walk passes block 2: errors.
    assert!(corrupt(store.read(b"t", 2 * 64 + 1).await.unwrap_err()));
    assert!(corrupt(store.read(b"t", 5 * 64).await.unwrap_err()));
    // The tail is in memory.
    assert_eq!(
        store.read(b"t", 20 * 64 + 4).await.unwrap(),
        Some(payload(20 * 64 + 4))
    );
}
