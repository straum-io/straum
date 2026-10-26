//! `Config::max_block_raw_bytes`: a batch's new objects split into blocks
//! within the limit, an oversized object alone, duplicates across the
//! blocks of a batch, compaction within the same limit, and frames with
//! several blocks under damage in every write mode.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::{Rng, TempDir, snapshot};
use straum_store::{
    Config, DropReason, Message, ObjectId, RecoveryAction, Store, StoreError, WriteMode,
};

fn msg<'a>(topic: &'a [u8], payload: &'a [u8]) -> Message<'a> {
    Message { topic, payload }
}

fn random(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = Rng(seed);
    (0..len).map(|_| rng.next() as u8).collect()
}

const MODES: [WriteMode; 3] = [
    WriteMode::Sequential,
    WriteMode::ParallelSync,
    WriteMode::INLINE_SMALL,
];

#[compio::test]
async fn a_batch_over_the_limit_is_split_in_order() {
    for mode in MODES {
        let dir = TempDir::new("cap-split");
        let config = Config {
            write_mode: mode,
            // 100-byte payloads take 101 raw bytes: ten per block.
            max_block_raw_bytes: 1024,
            ..Config::default()
        };
        let payloads: Vec<Vec<u8>> = (0..35).map(|i| random(i + 1, 100)).collect();
        let mut store = Store::open(&dir.0, config).await.unwrap();
        let batch: Vec<Message> = payloads.iter().map(|p| msg(b"t", p)).collect();
        let seqs = store.put_batch(&batch, 1).await.unwrap();
        assert_eq!(seqs, (0..35).collect::<Vec<_>>());
        assert_eq!(store.block_count(), 4, "{mode:?}: 10 + 10 + 10 + 5");
        for round in 0..2 {
            for (seq, p) in payloads.iter().enumerate() {
                let got = store.read(b"t", seq as u64).await.unwrap();
                assert_eq!(got.as_ref(), Some(p), "{mode:?} round {round} seq {seq}");
            }
            drop(store);
            store = Store::open(&dir.0, config).await.unwrap();
            assert!(store.recovery_report().is_empty());
            assert_eq!(store.block_count(), 4);
        }
    }
}

#[compio::test]
async fn an_object_over_the_limit_is_a_block_of_its_own() {
    let dir = TempDir::new("cap-oversized");
    let config = Config {
        max_block_raw_bytes: 1024,
        ..Config::default()
    };
    let (small, big, tail) = (random(1, 100), random(2, 5000), random(3, 100));
    let mut store = Store::open(&dir.0, config).await.unwrap();
    store
        .put_batch(&[msg(b"t", &small), msg(b"t", &big), msg(b"t", &tail)], 1)
        .await
        .unwrap();
    assert_eq!(store.block_count(), 3, "small | big alone | tail");
    for (seq, p) in [&small, &big, &tail].into_iter().enumerate() {
        assert_eq!(
            store.read(b"t", seq as u64).await.unwrap().as_ref(),
            Some(p)
        );
    }
}

#[compio::test]
async fn duplicates_across_the_blocks_of_a_batch_are_one_object() {
    let dir = TempDir::new("cap-dedup");
    let config = Config {
        max_block_raw_bytes: 512,
        ..Config::default()
    };
    let distinct: Vec<Vec<u8>> = (0..12).map(|i| random(i + 10, 100)).collect();
    // Every payload twice, the repeat several blocks later.
    let order: Vec<usize> = (0..12).chain(0..12).collect();
    let mut store = Store::open(&dir.0, config).await.unwrap();
    let batch: Vec<Message> = order.iter().map(|&i| msg(b"t", &distinct[i])).collect();
    store.put_batch(&batch, 1).await.unwrap();
    assert_eq!(store.object_count(), 12);
    assert_eq!(
        store.block_count(),
        3,
        "12 new objects of 101 bytes, 5 per block"
    );
    assert_eq!(store.stats().messages_deduplicated, 12);
    for p in &distinct {
        assert_eq!(store.refcount(&ObjectId::of(p)), Some(2));
    }
    for (seq, &i) in order.iter().enumerate() {
        assert_eq!(
            store.read(b"t", seq as u64).await.unwrap().as_ref(),
            Some(&distinct[i])
        );
    }
}

#[compio::test]
async fn compaction_rewrites_within_the_limit() {
    for mode in [WriteMode::Sequential, WriteMode::ParallelSync] {
        let dir = TempDir::new("cap-compact");
        let config = Config {
            write_mode: mode,
            max_block_raw_bytes: 1024,
            ..Config::default()
        };
        let payloads: Vec<Vec<u8>> = (0..40).map(|i| random(i + 100, 100)).collect();
        let mut store = Store::open(&dir.0, config).await.unwrap();
        // Alternating topics: releasing "dead" leaves every block half live.
        let batch: Vec<Message> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| msg(if i % 2 == 0 { b"dead" } else { b"live" }, p))
            .collect();
        store.put_batch(&batch, 1).await.unwrap();
        assert_eq!(store.block_count(), 4);
        assert_eq!(store.release_before(b"dead", 2).await.unwrap(), 20);
        let stats = store.compact().await.unwrap();
        assert_eq!(stats.objects_dropped, 20);
        // 20 live objects of 101 bytes, 10 per block.
        assert_eq!(store.block_count(), 2, "{mode:?}");
        for round in 0..2 {
            for (seq, p) in payloads.iter().skip(1).step_by(2).enumerate() {
                let got = store.read(b"live", seq as u64).await.unwrap();
                assert_eq!(got.as_ref(), Some(p), "{mode:?} round {round}");
            }
            drop(store);
            store = Store::open(&dir.0, config).await.unwrap();
            assert!(
                store.recovery_report().is_empty(),
                "{:?}",
                store.recovery_report()
            );
            assert_eq!(store.block_count(), 2);
        }
    }
}

/// The last frame holds three blocks: incompressible, compressible (inline
/// in InlineSmall, under its 4096-byte threshold) and incompressible; each
/// segment block in a segment of its own. Losing the last block's segment
/// drops the frame in ParallelSync and is `MissingObject` otherwise, with
/// no file changed.
#[compio::test]
async fn a_frame_of_several_blocks_missing_one_follows_its_write_mode() {
    for mode in MODES {
        let dir = TempDir::new("cap-damage");
        let config = Config {
            write_mode: mode,
            segment_max_bytes: 1,
            max_block_raw_bytes: 6000,
            ..Config::default()
        };
        let first = random(1, 5000);
        let (a, zeros, c) = (random(2, 5000), vec![0u8; 3000], random(3, 5000));
        let mut store = Store::open(&dir.0, config).await.unwrap();
        store.put_batch(&[msg(b"t", &first)], 1).await.unwrap();
        store
            .put_batch(&[msg(b"t", &a), msg(b"t", &zeros), msg(b"t", &c)], 2)
            .await
            .unwrap();
        assert_eq!(store.block_count(), 4, "{mode:?}");
        drop(store);
        let last = std::fs::read_dir(&dir.0)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("seg-"))
            .max()
            .unwrap();
        std::fs::remove_file(dir.0.join(&last)).unwrap();
        let before = snapshot(&dir.0);
        match (mode, Store::open(&dir.0, config).await) {
            (WriteMode::ParallelSync, Ok(store)) => {
                let dropped = store.recovery_report().actions.iter().any(|a| {
                    matches!(
                        a,
                        RecoveryAction::FrameDropped {
                            reason: DropReason::ObjectMissing,
                            frame_seq: 1,
                            ..
                        }
                    )
                });
                assert!(dropped, "{:?}", store.recovery_report());
                assert_eq!(store.read(b"t", 0).await.unwrap(), Some(first.clone()));
                assert_eq!(
                    store.read(b"t", 1).await.unwrap(),
                    None,
                    "the frame is gone"
                );
                assert_eq!(store.block_count(), 1);
            }
            (WriteMode::ParallelSync, Err(e)) => panic!("ParallelSync: {e}"),
            (_, res) => {
                assert!(
                    matches!(res, Err(StoreError::MissingObject(_))),
                    "{mode:?}: {:?}",
                    res.err()
                );
                assert!(snapshot(&dir.0) == before, "{mode:?}: files changed");
            }
        }
    }
}

/// One batch, two blocks: a segment block of two incompressible payloads
/// and an inline block of one compressible payload. After its topic is
/// released and the store compacted, the segment block's objects are live
/// and the inline block's dead object stays in the index (compaction
/// rewrites segments only).
#[compio::test]
async fn a_batch_can_mix_inline_and_segment_blocks() {
    let dir = TempDir::new("cap-mixed-inline");
    let config = Config {
        write_mode: WriteMode::InlineSmall { threshold: 64 },
        max_block_raw_bytes: 256,
        ..Config::default()
    };
    let (a, b, c) = (random(1, 100), random(2, 100), vec![7u8; 200]);
    let mut store = Store::open(&dir.0, config).await.unwrap();
    store
        .put_batch(&[msg(b"t0", &a), msg(b"t0", &b), msg(b"t2", &c)], 1)
        .await
        .unwrap();
    assert_eq!(store.block_count(), 2);
    let inline = |s: &Store, p: &[u8]| s.is_inline(&ObjectId::of(p));
    assert_eq!(inline(&store, &a), Some(false));
    assert_eq!(inline(&store, &b), Some(false));
    assert_eq!(inline(&store, &c), Some(true));
    assert_eq!(store.release_before(b"t2", 2).await.unwrap(), 1);
    store.compact().await.unwrap();
    assert_eq!(store.refcount(&ObjectId::of(&c)), Some(0));
    assert_eq!(store.refcount(&ObjectId::of(&a)), Some(1));
    assert_eq!(store.read(b"t0", 1).await.unwrap().as_deref(), Some(&b[..]));
}
