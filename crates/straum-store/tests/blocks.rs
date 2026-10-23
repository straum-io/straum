//! Compressed blocks: one per batch of new objects, deduplication within and
//! across blocks, compaction of partly dead blocks, the dictionary.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::{Rng, TempDir};
use straum_store::{Config, Message, ObjectId, Store, WriteMode};

fn msg<'a>(topic: &'a [u8], payload: &'a [u8]) -> Message<'a> {
    Message { topic, payload }
}

fn modes() -> [WriteMode; 3] {
    [
        WriteMode::Sequential,
        WriteMode::ParallelSync,
        WriteMode::INLINE_SMALL,
    ]
}

/// Log-like lines: similar but distinct, so blocks compress and a
/// dictionary can be trained on them.
fn line(i: u64) -> Vec<u8> {
    format!(
        "2026-10-23 12:{:02}:{:02}.{:03} sshd[{}]: Failed password for invalid user u{} from 10.{}.{}.{} port {} ssh2",
        i % 60,
        (i / 60) % 60,
        i % 1000,
        20_000 + i % 977,
        i % 131,
        i % 7,
        i % 251,
        (i * 7) % 253,
        30_000 + (i * 13) % 30_000
    )
    .into_bytes()
}

#[compio::test]
async fn duplicates_within_and_across_blocks() {
    for mode in modes() {
        let dir = TempDir::new("blocks-dedup");
        let config = Config {
            write_mode: mode,
            ..Config::default()
        };
        let (a, b) = (line(1), line(2));
        let mut store = Store::open(&dir.0, config).await.unwrap();
        // Within one batch: `a` three times, `b` once. One block, two
        // objects.
        let seqs = store
            .put_batch(
                &[msg(b"t", &a), msg(b"u", &a), msg(b"t", &b), msg(b"t", &a)],
                1,
            )
            .await
            .unwrap();
        assert_eq!(seqs, [0, 0, 1, 2], "{mode:?}");
        let s = store.stats();
        let new = (a.len() + b.len()) as u64;
        assert_eq!(
            (s.payload_bytes_new, s.messages_deduplicated),
            (new, 2),
            "{mode:?}"
        );
        assert_eq!(store.object_count(), 2);
        // Block bytes: in segments, or inline in the WAL (not metadata).
        let block_bytes = |s: straum_store::Stats| {
            s.segment_bytes + s.wal_bytes + s.topic_bytes - s.metadata_bytes
        };
        let after_first = block_bytes(s);
        // Across blocks: only known bytes, so no block and no segment write.
        store
            .put_batch(&[msg(b"t", &b), msg(b"v", &a)], 2)
            .await
            .unwrap();
        assert_eq!(
            block_bytes(store.stats()),
            after_first,
            "{mode:?}: no new block"
        );
        assert_eq!(store.refcount(&ObjectId::of(&a)), Some(4));
        assert_eq!(store.refcount(&ObjectId::of(&b)), Some(2));
        // A third batch mixes a known and a new payload: a block of one.
        let c = line(3);
        store
            .put_batch(&[msg(b"t", &a), msg(b"t", &c)], 3)
            .await
            .unwrap();
        assert_eq!(store.object_count(), 3);
        drop(store);
        let store = Store::open(&dir.0, config).await.unwrap();
        for (topic, seq, want) in [
            (&b"t"[..], 0, &a),
            (b"t", 1, &b),
            (b"t", 2, &a),
            (b"t", 3, &b),
            (b"t", 4, &a),
            (b"t", 5, &c),
            (b"u", 0, &a),
            (b"v", 0, &a),
        ] {
            let got = store.read(topic, seq).await.unwrap();
            assert_eq!(got.as_ref(), Some(want), "{mode:?}: {topic:?}/{seq}");
        }
    }
}

#[compio::test]
async fn compaction_rewrites_partly_dead_blocks() {
    for mode in [WriteMode::Sequential, WriteMode::ParallelSync] {
        let dir = TempDir::new("blocks-compact");
        let config = Config {
            write_mode: mode,
            // Every block in a segment of its own.
            segment_max_bytes: 1,
            ..Config::default()
        };
        let lines: Vec<Vec<u8>> = (0..12).map(line).collect();
        let mut store = Store::open(&dir.0, config).await.unwrap();
        // Block 0: lines 0..4 to "dead", 4..8 to "live". Block 1: 8..12, all
        // to "dead". Block 2: a line to "live" only.
        let first: Vec<Message> = lines[..8]
            .iter()
            .enumerate()
            .map(|(i, l)| msg(if i < 4 { b"dead" } else { b"live" }, l))
            .collect();
        store.put_batch(&first, 1).await.unwrap();
        let second: Vec<Message> = lines[8..].iter().map(|l| msg(b"dead", l)).collect();
        store.put_batch(&second, 2).await.unwrap();
        let only = line(99);
        store.put_batch(&[msg(b"live", &only)], 3).await.unwrap();
        assert_eq!(store.segment_count(), 3);
        assert_eq!(store.release_before(b"dead", 10).await.unwrap(), 8);

        let stats = store.compact().await.unwrap();
        assert_eq!(stats.objects_dropped, 8, "{mode:?}");
        // Block 0 rewritten, block 1 dropped; block 2 untouched.
        assert_eq!(stats.segments_rewritten, 2, "{mode:?}");
        assert!(stats.bytes_reclaimed > 0);
        assert_eq!(store.segment_count(), 2, "{mode:?}");
        assert_eq!(store.object_count(), 5);
        assert_eq!(
            store.compact().await.unwrap().segments_rewritten,
            0,
            "idempotent"
        );
        for (seq, l) in lines[4..8].iter().chain([&only]).enumerate() {
            let got = store.read(b"live", seq as u64).await.unwrap();
            assert_eq!(got.as_ref(), Some(l), "{mode:?}: live/{seq}");
        }
        for l in lines[..4].iter().chain(&lines[8..]) {
            assert_eq!(store.refcount(&ObjectId::of(l)), None, "dead reclaimed");
        }

        drop(store);
        let mut store = Store::open(&dir.0, config).await.unwrap();
        assert!(
            store.recovery_report().is_empty(),
            "{:?}",
            store.recovery_report()
        );
        for (seq, l) in lines[4..8].iter().chain([&only]).enumerate() {
            let got = store.read(b"live", seq as u64).await.unwrap();
            assert_eq!(got.as_ref(), Some(l), "{mode:?}: after restart live/{seq}");
        }
        // The moved objects can be released and compacted again.
        assert_eq!(store.release_before(b"live", 10).await.unwrap(), 5);
        store.compact().await.unwrap();
        assert_eq!((store.object_count(), store.segment_count()), (0, 0));
        // A dead object's bytes are stored again as a new object.
        store.put_batch(&[msg(b"x", &lines[0])], 11).await.unwrap();
        assert_eq!(store.read(b"x", 0).await.unwrap(), Some(lines[0].clone()));
    }
}

#[compio::test]
async fn dictionary_is_trained_once_and_survives_restart() {
    for mode in modes() {
        let dir = TempDir::new("blocks-dict");
        let config = Config {
            write_mode: mode,
            dict_train_bytes: 32 << 10,
            ..Config::default()
        };
        let mut store = Store::open(&dir.0, config).await.unwrap();
        let mut n = 0;
        // About 100 bytes a line, 64 lines a batch: training runs at the
        // start of the sixth batch.
        while store.dictionary_bytes().is_none() {
            let batch: Vec<Vec<u8>> = (n..n + 64).map(line).collect();
            let msgs: Vec<Message> = batch.iter().map(|p| msg(b"t", p)).collect();
            store.put_batch(&msgs, n).await.unwrap();
            n += 64;
            assert!(n < 64 * 20, "{mode:?}: no training");
        }
        let dict = store.dictionary_bytes().unwrap();
        assert!(dict > 0, "{mode:?}: a dictionary was built");
        // Later blocks use it.
        for _ in 0..3 {
            let batch: Vec<Vec<u8>> = (n..n + 64).map(line).collect();
            let msgs: Vec<Message> = batch.iter().map(|p| msg(b"t", p)).collect();
            store.put_batch(&msgs, n).await.unwrap();
            n += 64;
        }
        // Half of the entries released, so compaction rewrites blocks with
        // the dictionary and drops others.
        store.release_before(b"t", n / 2).await.unwrap();
        store.compact().await.unwrap();
        drop(store);

        let store = Store::open(&dir.0, config).await.unwrap();
        assert_eq!(
            store.dictionary_bytes(),
            Some(dict),
            "{mode:?}: read from the WAL"
        );
        assert!(store.recovery_report().is_empty());
        let (first, next) = store.topic_range(b"t").unwrap();
        assert_eq!(next, n);
        for seq in first..next {
            let got = store.read(b"t", seq).await.unwrap();
            assert_eq!(got, Some(line(seq)), "{mode:?}: t/{seq}");
        }
    }
}

/// Uniform payloads give zstd nothing to train on: the attempt is recorded
/// (an empty dictionary) and never repeated.
#[compio::test]
async fn failed_training_is_recorded_once() {
    let dir = TempDir::new("blocks-nodict");
    let config = Config {
        dict_train_bytes: 64,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    store.put_batch(&[msg(b"t", &[1; 100])], 0).await.unwrap();
    assert_eq!(store.dictionary_bytes(), None);
    store.put_batch(&[msg(b"t", &[2; 100])], 1).await.unwrap();
    assert_eq!(store.dictionary_bytes(), Some(0));
    let wal = store.stats().wal_bytes;
    store.put_batch(&[msg(b"t", &[3; 100])], 2).await.unwrap();
    assert!(
        store.stats().wal_bytes - wal < 200,
        "no second dictionary record"
    );
    drop(store);
    let store = Store::open(&dir.0, config).await.unwrap();
    assert_eq!(store.dictionary_bytes(), Some(0));
    assert_eq!(store.read(b"t", 2).await.unwrap(), Some(vec![3; 100]));
}

#[compio::test]
async fn reads_work_without_a_block_cache() {
    let dir = TempDir::new("blocks-nocache");
    let config = Config {
        block_cache_blocks: 0,
        ..Config::default()
    };
    let mut rng = Rng(5);
    let payloads: Vec<Vec<u8>> = (0..40)
        .map(|_| (0..1 + rng.below(300)).map(|_| rng.next() as u8).collect())
        .collect();
    let mut store = Store::open(&dir.0, config).await.unwrap();
    for (i, chunk) in payloads.chunks(8).enumerate() {
        let msgs: Vec<Message> = chunk.iter().map(|p| msg(b"t", p)).collect();
        store.put_batch(&msgs, i as u64).await.unwrap();
    }
    for (seq, p) in payloads.iter().enumerate().rev() {
        assert_eq!(
            store.read(b"t", seq as u64).await.unwrap().as_ref(),
            Some(p)
        );
    }
}
