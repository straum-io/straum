//! Random sequences of publish, release, compaction steps, checkpoints and
//! restart, checked after every step against an in-memory model: same
//! reads, same object index, same topic sequences. Compaction steps
//! interleave with puts (ADR-0014 step 5): an object a batch references
//! during a compaction survives it.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design
mod common;

use std::collections::{HashMap, HashSet};

use common::TempDir;
use proptest::prelude::*;
use straum_store::{CompactStep, Config, Entry, Message, ObjectId, Store, WriteMode};

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
    /// One `compact_step`; batches and other ops run between steps.
    CompactStep,
    Checkpoint,
    Restart,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Up to 32 messages, so topics fill several 64-entry index blocks.
        4 => prop::collection::vec((0..TOPICS.len(), 0..6usize, 0..ATTRS.len()), 0..32)
            .prop_map(Op::Put),
        2 => (0..TOPICS.len(), 0..6u64).prop_map(|(topic, age)| Op::Release { topic, age }),
        1 => Just(Op::Compact),
        3 => Just(Op::CompactStep),
        1 => Just(Op::Checkpoint),
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
    /// Payloads in the store's object index, and whether stored in an
    /// inline block. Dead objects in segment blocks leave it at compaction;
    /// those in inline blocks never do.
    indexed: HashMap<usize, bool>,
    compaction: Option<ModelCompaction>,
}

/// A compaction in progress, as the model follows it.
struct ModelCompaction {
    /// Payloads in the object index when the mark phase began.
    candidates: HashSet<usize>,
    /// Topics still to mark, in the store's (name) order.
    topics: Vec<usize>,
    /// Payloads marked: live in a marked topic, or put since the start.
    marked: HashSet<usize>,
}

impl Model {
    fn start_compaction(&mut self) {
        if self.compaction.is_none() {
            let mut topics: Vec<usize> = self.topics.keys().copied().collect();
            topics.sort_by_key(|&t| TOPICS[t]);
            self.compaction = Some(ModelCompaction {
                candidates: self.indexed.keys().copied().collect(),
                topics,
                marked: HashSet::new(),
            });
        }
    }

    /// The next topic's live payloads are marked.
    fn mark_next(&mut self) {
        let Some(c) = self.compaction.as_mut() else {
            unreachable!("started")
        };
        let t = c.topics.remove(0);
        if let Some(mt) = self.topics.get(&t) {
            for &(p, _, _) in &mt.entries[mt.first_live as usize..] {
                c.marked.insert(p);
            }
        }
    }

    /// Done: unmarked candidates in segment blocks were reclaimed.
    fn finish_compaction(&mut self) {
        if let Some(c) = self.compaction.take() {
            self.indexed
                .retain(|p, inline| *inline || !c.candidates.contains(p) || c.marked.contains(p));
        }
    }

    /// A payload the sweep in progress may or may not have reclaimed yet.
    fn uncertain(&self, p: usize) -> bool {
        self.compaction.as_ref().is_some_and(|c| {
            c.candidates.contains(&p)
                && !c.marked.contains(&p)
                && self.indexed.get(&p) == Some(&false)
        })
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
    let mut present = 0;
    for (i, p) in payloads.iter().enumerate() {
        let has = store.contains(&ObjectId::of(p));
        present += usize::from(has);
        if !model.uncertain(i) {
            assert_eq!(has, model.indexed.contains_key(&i), "payload {i} stored");
        }
    }
    assert_eq!(store.object_count(), present, "one number per payload");
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
            block_cache_bytes: 2048,
            // Seek pages of two locations: lookups read several pages.
            seek_page_entries: 2,
            // Index segments rotate between batches.
            index_segment_max_bytes: 256,
            index_open_files: 2,
            // Payloads of up to 300 bytes: batches split into several
            // blocks, and the largest payloads are blocks of their own.
            max_block_raw_bytes: 128,
            // A new WAL file every frame or two: inline blocks, keys and
            // headers are read from earlier files.
            wal_file_max_bytes: 512,
            // Two open data files: reads and compaction reopen them.
            data_open_files: 2,
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
                        .map(|&(t, p, a)| Message {
                            topic: TOPICS[t],
                            payload: &payloads[p],
                            key: ATTRS[a].0,
                            headers: ATTRS[a].1,
                        })
                        .collect();
                    let seqs = store.put_batch(&batch, now).await.unwrap();
                    for (&(t, p, a), seq) in msgs.iter().zip(seqs) {
                        if let Some(c) = model.compaction.as_mut() {
                            c.marked.insert(p);
                        }
                        // Whether an object's block went inline depends on
                        // the block's compressed size, and a batch can have
                        // both kinds: the model reads it from the store.
                        let inline = store.is_inline(&ObjectId::of(&payloads[p])).unwrap();
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
                    model.start_compaction();
                    while model
                        .compaction
                        .as_ref()
                        .is_some_and(|c| !c.topics.is_empty())
                    {
                        model.mark_next();
                    }
                    model.finish_compaction();
                    store.compact().await.unwrap();
                }
                Op::CompactStep => {
                    model.start_compaction();
                    match store.compact_step().await.unwrap() {
                        CompactStep::Marked => model.mark_next(),
                        CompactStep::Swept => {}
                        CompactStep::Done(_) => model.finish_compaction(),
                    }
                    assert_eq!(store.compacting(), model.compaction.is_some());
                }
                Op::Checkpoint => store.checkpoint().await.unwrap(),
                Op::Restart => {
                    drop(store);
                    store = Store::open(&dir.0, config).await.unwrap();
                    // A compaction in progress ends; what its sweep had
                    // reclaimed so far stays reclaimed.
                    for (i, p) in payloads.iter().enumerate() {
                        if model.uncertain(i) && !store.contains(&ObjectId::of(p)) {
                            model.indexed.remove(&i);
                        }
                    }
                    model.compaction = None;
                }
            }
            check(&store, &model, &payloads).await;
        }
    });
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    #[test]
    #[ignore = "long: 128 proptest cases"]
    fn sequential_matches_model(ops in prop::collection::vec(op(), 1..40)) {
        store_matches_model(ops, WriteMode::Sequential);
    }

    #[test]
    #[ignore = "long: 128 proptest cases"]
    fn parallel_sync_matches_model(ops in prop::collection::vec(op(), 1..40)) {
        store_matches_model(ops, WriteMode::ParallelSync);
    }

    /// Every block inline.
    #[test]
    #[ignore = "long: 128 proptest cases"]
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
                        ..Default::default()
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
