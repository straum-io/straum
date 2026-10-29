//! The memory budget and the checkpoint triggers (ADR-0014 step 7, §4,
//! D-207 B, D-209 A): a checkpoint before the next write once the WAL
//! since the last one or the memtable reaches its bound, one at `close`
//! if anything changed; beyond the filters' share the oldest runs' filters
//! are dropped and deduplication against them degrades, in every write
//! mode.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::{Rng, TempDir};
use straum_store::{Config, Message, ObjectId, Store, WriteMode};

const MODES: [WriteMode; 3] = [
    WriteMode::Sequential,
    WriteMode::ParallelSync,
    WriteMode::INLINE_SMALL,
];

/// Incompressible and above the InlineSmall threshold: every object goes
/// to a segment in every mode, so compaction can reclaim it.
fn body(i: u64) -> Vec<u8> {
    let mut rng = Rng(i + 1);
    (0..5000).map(|_| rng.next() as u8).collect()
}

async fn put(store: &mut Store, topic: &[u8], i: u64) -> u64 {
    let p = body(i);
    let m = Message {
        topic,
        payload: &p,
        ..Default::default()
    };
    store.put_batch(&[m], 1).await.unwrap()[0]
}

/// No automatic checkpoint unless a test sets one.
fn manual(mode: WriteMode) -> Config {
    Config {
        write_mode: mode,
        checkpoint_wal_bytes: 0,
        ..Config::default()
    }
}

/// The WAL trigger: with a bound of one byte every write after a frame
/// first checkpoints, nothing changed means none; without it none runs.
/// The count since the last checkpoint survives a reopen (read from the
/// WAL past the covered frame).
#[compio::test]
async fn the_wal_trigger_checkpoints_before_the_next_write() {
    for mode in MODES {
        let dir = TempDir::new("budget-wal");
        let mut store = Store::open(&dir.0, manual(mode)).await.unwrap();
        for i in 0..3 {
            put(&mut store, b"a", i).await;
        }
        assert_eq!(store.checkpoint_number(), None, "{mode:?}: no trigger");
        drop(store);
        let every = Config {
            checkpoint_wal_bytes: 1,
            ..manual(mode)
        };
        let mut store = Store::open(&dir.0, every).await.unwrap();
        put(&mut store, b"a", 3).await;
        let first = store.checkpoint_number().expect("the reopened WAL counts");
        put(&mut store, b"a", 4).await;
        assert_eq!(store.checkpoint_number(), Some(first + 1), "{mode:?}");
        drop(store);
        let store = Store::open(&dir.0, every).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        for i in 0..5 {
            assert_eq!(store.read(b"a", i).await.unwrap(), Some(body(i)));
        }
    }
}

/// The memtable trigger: a budget whose memtable share holds three
/// entries checkpoints before the fourth new id's write.
#[compio::test]
async fn the_memtable_share_checkpoints_before_the_next_write() {
    for mode in MODES {
        let dir = TempDir::new("budget-memtable");
        let config = Config {
            memory_budget_bytes: 4 * 3 * 112,
            ..manual(mode)
        };
        let mut store = Store::open(&dir.0, config).await.unwrap();
        for i in 0..3 {
            put(&mut store, b"a", i).await;
        }
        assert_eq!(store.checkpoint_number(), None, "{mode:?}");
        put(&mut store, b"a", 3).await;
        assert!(store.checkpoint_number().is_some(), "{mode:?}");
    }
}

/// `close` checkpoints what changed and nothing when nothing did.
#[compio::test]
async fn close_checkpoints_if_anything_changed() {
    for mode in MODES {
        let dir = TempDir::new("budget-close");
        let mut store = Store::open(&dir.0, manual(mode)).await.unwrap();
        put(&mut store, b"a", 0).await;
        store.close().await.unwrap();
        let store = Store::open(&dir.0, manual(mode)).await.unwrap();
        let n = store.checkpoint_number().expect("close checkpointed");
        store.close().await.unwrap();
        let store = Store::open(&dir.0, manual(mode)).await.unwrap();
        assert_eq!(store.checkpoint_number(), Some(n), "{mode:?}: unchanged");
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        assert_eq!(store.read(b"a", 0).await.unwrap(), Some(body(0)));
    }
}

/// Degraded deduplication (ADR-0014 §4, D-207 B, test plan): a filters'
/// share too small for any filter leaves the checkpointed runs unsearched,
/// so a repeat of old content is stored again under a new number; both
/// numbers read back, compaction reclaims the released one and keeps the
/// other, and a restart (the WAL holding the id under two numbers, before
/// and after a checkpoint) restores the same state. A larger budget at
/// open searches the runs again.
#[compio::test]
async fn beyond_the_filters_share_a_repeat_is_stored_again() {
    for mode in MODES {
        let dir = TempDir::new("budget-degraded");
        // Every share at a few bytes: a checkpoint before every write and no
        // filter kept. No recent-id cache, which would answer the repeat.
        let tiny = Config {
            memory_budget_bytes: 16,
            recent_id_cache_entries: 0,
            ..manual(mode)
        };
        let mut store = Store::open(&dir.0, tiny).await.unwrap();
        put(&mut store, b"a", 0).await;
        put(&mut store, b"a", 1).await;
        assert!(store.unsearched_id_runs() > 0, "{mode:?}");
        put(&mut store, b"b", 0).await;
        assert_eq!(store.stats().messages_deduplicated, 0, "{mode:?}");
        assert_eq!(store.object_count(), 3, "{mode:?}: a second number");
        let id = ObjectId::of(&body(0));
        let check = async |store: &Store, a_live: bool| {
            assert_eq!(store.read(b"b", 0).await.unwrap(), Some(body(0)));
            assert_eq!(store.entry(b"b", 0).await.unwrap(), Some(id));
            if a_live {
                assert_eq!(store.read(b"a", 0).await.unwrap(), Some(body(0)));
                assert_eq!(store.entry(b"a", 0).await.unwrap(), Some(id));
            }
            assert!(store.contains(&id));
        };
        check(&store, true).await;
        drop(store);

        // A restart with the WAL past the last checkpoint.
        let mut store = Store::open(&dir.0, tiny).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        assert_eq!(store.object_count(), 3, "{mode:?}");
        check(&store, true).await;

        assert_eq!(store.release_before(b"a", 2).await.unwrap(), 2);
        let stats = store.compact().await.unwrap();
        assert_eq!(stats.objects_dropped, 2, "{mode:?}: a's two numbers");
        assert_eq!(store.object_count(), 1, "{mode:?}");
        check(&store, false).await;
        store.checkpoint().await.unwrap();
        drop(store);

        // A restart from the checkpoint.
        let store = Store::open(&dir.0, tiny).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        assert_eq!(store.object_count(), 1, "{mode:?}");
        check(&store, false).await;
        drop(store);

        // The default budget keeps every filter: the repeat deduplicates.
        let mut store = Store::open(
            &dir.0,
            Config {
                recent_id_cache_entries: 0,
                ..manual(mode)
            },
        )
        .await
        .unwrap();
        assert_eq!(store.unsearched_id_runs(), 0, "{mode:?}");
        put(&mut store, b"c", 0).await;
        assert_eq!(store.stats().messages_deduplicated, 1, "{mode:?}");
        assert_eq!(store.object_count(), 1, "{mode:?}");
    }
}
