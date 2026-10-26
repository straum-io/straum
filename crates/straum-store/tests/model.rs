//! Random sequences of publish, release, compact and restart, checked after
//! every step against an in-memory model: same reads, same refcounts, same
//! topic sequences.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design
mod common;

use std::collections::HashMap;

use common::TempDir;
use proptest::prelude::*;
use straum_store::{Config, Entry, Message, ObjectId, Store, WriteMode};

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
    /// (topic, payload, attributes) per message: `ATTRS[attributes]`.
    Put(Vec<(usize, usize, usize)>),
    Release {
        topic: usize,
        age: u64,
    },
    Compact,
    Restart,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => prop::collection::vec((0..TOPICS.len(), 0..6usize, 0..ATTRS.len()), 0..8).prop_map(Op::Put),
        2 => (0..TOPICS.len(), 0..6u64).prop_map(|(topic, age)| Op::Release { topic, age }),
        1 => Just(Op::Compact),
        1 => Just(Op::Restart),
    ]
}

type Attrs = (
    Option<&'static [u8]>,
    &'static [(&'static [u8], &'static [u8])],
);

/// A publication's key and headers: none, an empty key, a key and two
/// headers with a repeated name. The same payload under different ones is
/// still one object.
const ATTRS: [Attrs; 3] = [
    (None, &[]),
    (Some(b""), &[]),
    (Some(b"k"), &[(b"h", b"1"), (b"h", b"")]),
];

#[derive(Default)]
struct ModelTopic {
    first_live: u64,
    /// Every entry ever published: (payload index, timestamp, attributes).
    entries: Vec<(usize, u64, usize)>,
}

#[derive(Default)]
struct Model {
    topics: HashMap<usize, ModelTopic>,
    /// Payloads in the store's object index, and whether stored inline.
    /// Dead segment objects leave it at compaction; inline ones never do.
    indexed: HashMap<usize, bool>,
    inline_threshold: Option<usize>,
}

impl Model {
    fn live_counts(&self) -> HashMap<usize, u64> {
        let mut counts = HashMap::new();
        for t in self.topics.values() {
            for &(p, _, _) in &t.entries[t.first_live as usize..] {
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
            let want = (seq >= t.first_live && seq < next).then(|| {
                let (p, timestamp, a) = t.entries[seq as usize];
                let (key, headers) = ATTRS[a];
                Entry {
                    timestamp,
                    key: key.map(<[u8]>::to_vec),
                    headers: headers
                        .iter()
                        .map(|(n, v)| (n.to_vec(), v.to_vec()))
                        .collect(),
                    payload: payloads[p].clone(),
                }
            });
            assert_eq!(
                store.read_entry(name, seq).await.unwrap(),
                want,
                "entry {ti}/{seq}"
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
            ..Config::default()
        };
        let payloads = payloads();
        let mut model = Model {
            inline_threshold: match mode {
                WriteMode::InlineSmall { threshold } => Some(threshold as usize),
                _ => None,
            },
            ..Model::default()
        };
        let mut store = Store::open(&dir.0, config).await.unwrap();
        for (now, op) in (1u64..).zip(ops) {
            match op {
                Op::Put(msgs) => {
                    let batch: Vec<Message> = msgs
                        .iter()
                        .map(|&(t, p, a)| Message {
                            topic: TOPICS[t],
                            payload: &payloads[p],
                            key: ATTRS[a].0,
                            headers: ATTRS[a].1,
                        })
                        .collect();
                    let seqs = store.put_batch(&batch, now).await.unwrap();
                    for (&(t, p, a), seq) in msgs.iter().zip(seqs) {
                        let inline = model
                            .inline_threshold
                            .is_some_and(|th| payloads[p].len() <= th);
                        model.indexed.entry(p).or_insert(inline);
                        let mt = model.topics.entry(t).or_default();
                        assert_eq!(seq, mt.entries.len() as u64, "assigned seq");
                        let ts = mt.entries.last().map_or(now, |&(_, ts, _)| ts.max(now));
                        mt.entries.push((p, ts, a));
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

    /// Threshold 64: payloads of 0, 1, 7 and 33 bytes go inline, 100 and 300
    /// to segments.
    #[test]
    fn inline_small_matches_model(ops in prop::collection::vec(op(), 1..40)) {
        store_matches_model(ops, WriteMode::InlineSmall { threshold: 64 });
    }
}
