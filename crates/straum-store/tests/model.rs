//! Random sequences of publish, release, compact and restart, checked after
//! every step against an in-memory model: same reads, same refcounts, same
//! topic sequences.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design
mod common;

use std::collections::HashMap;

use common::TempDir;
use proptest::prelude::*;
use straum_store::{Config, Message, ObjectId, Store, WriteMode};

const TOPICS: [&[u8]; 3] = [b"a", b"b", b"c"];

/// Few distinct payloads so dedup happens often; sizes cross the tiny
/// segment limit so segments rotate and objects get segments of their own.
fn payloads() -> Vec<Vec<u8>> {
    [0usize, 1, 7, 33, 100, 300]
        .iter()
        .enumerate()
        .map(|(i, &len)| vec![i as u8; len])
        .collect()
}

#[derive(Clone, Debug)]
enum Op {
    Put(Vec<(usize, usize)>),
    Release { topic: usize, age: u64 },
    Compact,
    Restart,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => prop::collection::vec((0..TOPICS.len(), 0..6usize), 0..8).prop_map(Op::Put),
        2 => (0..TOPICS.len(), 0..6u64).prop_map(|(topic, age)| Op::Release { topic, age }),
        1 => Just(Op::Compact),
        1 => Just(Op::Restart),
    ]
}

#[derive(Default)]
struct ModelTopic {
    first_live: u64,
    /// Every entry ever published: (payload index, timestamp).
    entries: Vec<(usize, u64)>,
}

#[derive(Default)]
struct Model {
    topics: HashMap<usize, ModelTopic>,
    /// Payloads in the store's object index, and whether stored in an
    /// inline block. Dead objects in segment blocks leave it at compaction;
    /// those in inline blocks never do.
    indexed: HashMap<usize, bool>,
}

impl Model {
    fn live_counts(&self) -> HashMap<usize, u64> {
        let mut counts = HashMap::new();
        for t in self.topics.values() {
            for &(p, _) in &t.entries[t.first_live as usize..] {
                *counts.entry(p).or_default() += 1;
            }
        }
        counts
    }
}

async fn check(store: &Store, model: &Model, payloads: &[Vec<u8>]) {
    for (ti, name) in TOPICS.iter().enumerate() {
        let Some(t) = model.topics.get(&ti) else {
            assert_eq!(store.topic_range(name), None);
            continue;
        };
        let next = t.entries.len() as u64;
        assert_eq!(store.topic_range(name), Some((t.first_live, next)));
        for seq in 0..next + 1 {
            let want = (seq >= t.first_live && seq < next)
                .then(|| payloads[t.entries[seq as usize].0].clone());
            assert_eq!(
                store.read(name, seq).await.unwrap(),
                want,
                "topic {ti} seq {seq}"
            );
        }
    }
    let counts = model.live_counts();
    for (i, p) in payloads.iter().enumerate() {
        let want = model
            .indexed
            .contains_key(&i)
            .then(|| counts.get(&i).copied().unwrap_or(0));
        assert_eq!(
            store.refcount(&ObjectId::of(p)),
            want,
            "refcount of payload {i}"
        );
    }
    assert_eq!(store.object_count(), model.indexed.len());
}

fn store_matches_model(ops: Vec<Op>, mode: WriteMode) {
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = TempDir::new("model");
        let config = Config {
            segment_max_bytes: 64,
            write_mode: mode,
            // Training runs after a few batches; on these uniform payloads
            // it may build no dictionary, which is a path too.
            dict_train_bytes: 256,
            // Small, so reads also go through eviction.
            block_cache_blocks: 2,
            // Sparse seek table after the first block: lookups walk blocks.
            seek_table_max_entries: 1,
            // Payloads of up to 300 bytes: batches split into several
            // blocks, and the largest payloads are blocks of their own.
            max_block_raw_bytes: 128,
            ..Config::default()
        };
        let payloads = payloads();
        let mut model = Model::default();
        let mut store = Store::open(&dir.0, config).await.unwrap();
        for (now, op) in (1u64..).zip(ops) {
            match op {
                Op::Put(msgs) => {
                    let batch: Vec<Message> = msgs
                        .iter()
                        .map(|&(t, p)| Message {
                            topic: TOPICS[t],
                            payload: &payloads[p],
                        })
                        .collect();
                    let seqs = store.put_batch(&batch, now).await.unwrap();
                    for (&(t, p), seq) in msgs.iter().zip(seqs) {
                        // Whether an object's block went inline depends on
                        // the block's compressed size, and a batch can have
                        // both kinds: the model reads it from the store.
                        let inline = store.is_inline(&ObjectId::of(&payloads[p])).unwrap();
                        model.indexed.entry(p).or_insert(inline);
                        let mt = model.topics.entry(t).or_default();
                        assert_eq!(seq, mt.entries.len() as u64, "assigned seq");
                        let ts = mt.entries.last().map_or(now, |&(_, ts)| ts.max(now));
                        mt.entries.push((p, ts));
                    }
                }
                Op::Release { topic, age } => {
                    let cutoff = now.saturating_sub(age);
                    let released = store.release_before(TOPICS[topic], cutoff).await.unwrap();
                    let mut want = 0;
                    if let Some(mt) = model.topics.get_mut(&topic) {
                        while (mt.first_live as usize) < mt.entries.len()
                            && mt.entries[mt.first_live as usize].1 < cutoff
                        {
                            mt.first_live += 1;
                            want += 1;
                        }
                    }
                    assert_eq!(released, want);
                }
                Op::Compact => {
                    store.compact().await.unwrap();
                    let live = model.live_counts();
                    model
                        .indexed
                        .retain(|p, inline| *inline || live.contains_key(p));
                }
                Op::Restart => {
                    drop(store);
                    store = Store::open(&dir.0, config).await.unwrap();
                }
            }
            check(&store, &model, &payloads).await;
        }
    });
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    fn sequential_matches_model(ops in prop::collection::vec(op(), 1..40)) {
        store_matches_model(ops, WriteMode::Sequential);
    }

    #[test]
    fn parallel_sync_matches_model(ops in prop::collection::vec(op(), 1..40)) {
        store_matches_model(ops, WriteMode::ParallelSync);
    }

    /// Every block inline.
    #[test]
    fn inline_small_matches_model(ops in prop::collection::vec(op(), 1..40)) {
        store_matches_model(ops, WriteMode::InlineSmall { threshold: u32::MAX });
    }

    /// Inline and segment blocks mixed (`mixed_threshold_makes_both_kinds`).
    #[test]
    fn inline_small_mixed_matches_model(ops in prop::collection::vec(op(), 1..40)) {
        store_matches_model(ops, WriteMode::InlineSmall { threshold: MIXED_THRESHOLD });
    }
}

/// Between the compressed sizes of small and large blocks of `payloads()`.
const MIXED_THRESHOLD: u32 = 24;

/// With `MIXED_THRESHOLD`, a block of one 1-byte payload goes inline and a
/// block of every payload goes to a segment, before and after training.
#[test]
fn mixed_threshold_makes_both_kinds() {
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let dir = TempDir::new("model-mixed");
        let config = Config {
            segment_max_bytes: 64,
            write_mode: WriteMode::InlineSmall {
                threshold: MIXED_THRESHOLD,
            },
            dict_train_bytes: 256,
            ..Config::default()
        };
        let mut store = Store::open(&dir.0, config).await.unwrap();
        let payloads = payloads();
        let mut kinds = Vec::new();
        for round in 0..3u8 {
            // Fresh bytes each round, so every block holds new objects.
            let fresh: Vec<Vec<u8>> = payloads
                .iter()
                .map(|p| p.iter().map(|b| b ^ (0x40 + round)).collect())
                .collect();
            for batch in [&fresh[1..2], &fresh[..]] {
                let msgs: Vec<Message> = batch
                    .iter()
                    .map(|p| Message {
                        topic: b"a",
                        payload: p,
                    })
                    .collect();
                let before = store.stats().segment_bytes;
                store.put_batch(&msgs, 1).await.unwrap();
                kinds.push(store.stats().segment_bytes > before);
            }
        }
        assert!(store.dictionary_bytes().is_some(), "training ran");
        assert_eq!(kinds, [false, true, false, true, false, true], "segment?");
    });
}
