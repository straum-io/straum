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
