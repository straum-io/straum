//! Mark-and-sweep compaction (ADR-0014 step 5): the mark phase marks the
//! objects of every topic's live entries, one topic per `compact_step`;
//! numbers allocated after it began, and every number a Publish references
//! while the compaction is in progress, are live. Batches run between the
//! steps, in every write mode.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::{Rng, TempDir};
use straum_store::{CompactStep, Config, Message, ObjectId, Store, StoreError, WriteMode};

const MODES: [WriteMode; 3] = [
    WriteMode::Sequential,
    WriteMode::ParallelSync,
    WriteMode::INLINE_SMALL,
];

/// Incompressible and above the InlineSmall threshold: every object goes to
/// a segment in every mode, so compaction can reclaim it.
fn body(seed: u64) -> Vec<u8> {
    let mut rng = Rng(seed + 1);
    (0..5000).map(|_| rng.next() as u8).collect()
}

fn config(mode: WriteMode) -> Config {
    Config {
        write_mode: mode,
        ..Config::default()
    }
}

async fn put(store: &mut Store, topic: &[u8], bodies: &[Vec<u8>]) -> Vec<u64> {
    let batch: Vec<Message> = bodies
        .iter()
        .map(|p| Message {
            topic,
            payload: p,
            ..Default::default()
        })
        .collect();
    store.put_batch(&batch, 1).await.unwrap()
}

async fn step(store: &mut Store) -> CompactStep {
    store.compact_step().await.unwrap()
}

async fn finish(store: &mut Store) -> straum_store::CompactStats {
    loop {
        if let CompactStep::Done(stats) = step(store).await {
            return stats;
        }
    }
}

async fn reads(store: &Store, topic: &[u8], seqs: &[u64], bodies: &[Vec<u8>]) {
    for (seq, b) in seqs.iter().zip(bodies) {
        assert_eq!(store.read(topic, *seq).await.unwrap().as_ref(), Some(b));
    }
}

/// A batch written during the mark phase and one written after it, before
/// the sweep, keep every object, although they share a segment with dead
/// objects the sweep reclaims.
#[compio::test]
async fn batches_between_mark_and_sweep_keep_all_their_objects() {
    for mode in MODES {
        let dir = TempDir::new("compaction-new");
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        let dead: Vec<Vec<u8>> = (0..4).map(body).collect();
        put(&mut store, b"a", &dead).await;
        let kept = vec![body(10)];
        put(&mut store, b"b", &kept).await;
        assert_eq!(store.release_before(b"a", 2).await.unwrap(), 4);

        assert_eq!(step(&mut store).await, CompactStep::Marked, "topic a");
        assert!(store.compacting());
        let during: Vec<Vec<u8>> = (20..23).map(body).collect();
        let during_seqs = put(&mut store, b"a", &during).await;
        assert_eq!(step(&mut store).await, CompactStep::Marked, "topic b");
        let after: Vec<Vec<u8>> = (30..33).map(body).collect();
        let after_seqs = put(&mut store, b"c", &after).await;
        let stats = finish(&mut store).await;
        assert!(!store.compacting());

        assert_eq!(stats.objects_dropped, 4, "{mode:?}");
        for b in &dead {
            assert!(
                !store.contains(&ObjectId::of(b)),
                "{mode:?}: dead reclaimed"
            );
        }
        for b in kept.iter().chain(&during).chain(&after) {
            assert!(store.contains(&ObjectId::of(b)), "{mode:?}");
        }
        reads(&store, b"b", &[0], &kept).await;
        reads(&store, b"a", &during_seqs, &during).await;
        reads(&store, b"c", &after_seqs, &after).await;
        drop(store);
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        reads(&store, b"a", &during_seqs, &during).await;
        reads(&store, b"c", &after_seqs, &after).await;
    }
}

/// During the mark phase (after its topic was marked) and after it, a batch
/// deduplicates onto an old object with no live entry: the Publish marks
/// it, so it survives the sweep and reads back; one that is not
/// referenced again is reclaimed.
#[compio::test]
async fn an_old_object_referenced_during_the_compaction_survives_the_sweep() {
    for mode in MODES {
        let dir = TempDir::new("compaction-old");
        let mut store = Store::open(&dir.0, config(mode)).await.unwrap();
        let old: Vec<Vec<u8>> = (0..3).map(body).collect();
        put(&mut store, b"a", &old).await;
        put(&mut store, b"b", &[body(10)]).await;
        assert_eq!(store.release_before(b"a", 2).await.unwrap(), 3);

        assert_eq!(step(&mut store).await, CompactStep::Marked, "topic a");
        let s0 = put(&mut store, b"a", &old[..1]).await;
        assert_eq!(step(&mut store).await, CompactStep::Marked, "topic b");
        let s1 = put(&mut store, b"a", &old[1..2]).await;
        assert_eq!(store.stats().messages_deduplicated, 2, "{mode:?}");
        finish(&mut store).await;

        assert!(store.contains(&ObjectId::of(&old[0])), "{mode:?}");
        assert!(store.contains(&ObjectId::of(&old[1])), "{mode:?}");
        assert!(!store.contains(&ObjectId::of(&old[2])), "{mode:?}");
        reads(&store, b"a", &s0, &old[..1]).await;
        reads(&store, b"a", &s1, &old[1..2]).await;
        drop(store);
        let store = Store::open(&dir.0, config(mode)).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        reads(&store, b"a", &s0, &old[..1]).await;
        reads(&store, b"a", &s1, &old[1..2]).await;
    }
}

/// Releasing between the mark and the sweep leaves the object live for
/// this compaction (marked) and reclaimed by the next.
#[compio::test]
async fn an_entry_released_after_its_topic_was_marked_is_reclaimed_next_time() {
    let dir = TempDir::new("compaction-release");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    let x = body(1);
    put(&mut store, b"a", std::slice::from_ref(&x)).await;
    put(&mut store, b"a", &[body(2)]).await;
    assert_eq!(step(&mut store).await, CompactStep::Marked);
    store.release_before(b"a", 2).await.unwrap();
    finish(&mut store).await;
    assert!(store.contains(&ObjectId::of(&x)), "marked: kept");
    assert_eq!(store.compact().await.unwrap().objects_dropped, 2);
    assert!(!store.contains(&ObjectId::of(&x)));
}

/// A damaged full index block fails the mark phase with
/// `TopicIndexCorrupt`: nothing is written, the compaction ends, the store
/// stays usable and no object is reclaimed.
#[compio::test]
async fn a_damaged_index_block_fails_the_mark_phase_and_changes_nothing() {
    let dir = TempDir::new("compaction-damaged-index");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    let bodies: Vec<Vec<u8>> = (0..70).map(|i| format!("m{i}").into_bytes()).collect();
    put(&mut store, b"a", &bodies).await;
    drop(store);
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    // Damaged while the store is open: found when the mark phase reads it.
    let rec = common::find_index_record(&dir.0, common::INDEX_BLOCK_RECORD, 0, 0);
    common::edit(&rec.path, |b| b[rec.body.start + 3] ^= 0xff);
    let before = common::snapshot(&dir.0);
    let err = store.compact_step().await.unwrap_err();
    assert!(matches!(err, StoreError::TopicIndexCorrupt { .. }), "{err}");
    assert!(!store.compacting());
    assert!(common::snapshot(&dir.0) == before, "files changed");
    assert_eq!(store.object_count(), 70);
    put(&mut store, b"b", &[body(1)]).await;
}
