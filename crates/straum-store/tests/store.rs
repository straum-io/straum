#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::{
    INDEX_BLOCK_RECORD, Rng, TempDir, copy_dir, edit, find_index_record, index_segments,
    reseal_index_record, snapshot, wal_frame_starts,
};
use straum_store::{Config, Message, ObjectId, RecoveryAction, Store, StoreError};

fn msg<'a>(topic: &'a str, payload: &'a [u8]) -> Message<'a> {
    Message {
        topic: topic.as_bytes(),
        payload,
        ..Default::default()
    }
}

async fn read(store: &Store, topic: &str, seq: u64) -> Option<Vec<u8>> {
    store.read(topic.as_bytes(), seq).await.unwrap()
}

#[compio::test]
async fn dedup_counts_references_and_keeps_every_sequence() {
    let dir = TempDir::new("dedup");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    let batch = [
        msg("a", b"x"),
        msg("a", b"x"),
        msg("b", b"x"),
        msg("a", b"y"),
    ];
    let seqs = store.put_batch(&batch, 1).await.unwrap();
    assert_eq!(
        seqs,
        [0, 1, 0, 2],
        "repeated bytes get new topic_seq entries"
    );
    assert!(store.contains(&ObjectId::of(b"x")));
    assert!(store.contains(&ObjectId::of(b"y")));
    assert_eq!(store.object_count(), 2);
    assert_eq!(
        store.stats().payload_bytes_new,
        2,
        "each distinct object stored once"
    );
    let block_bytes = store.stats().segment_bytes;
    assert!(block_bytes > 0, "one compressed block");
    for (topic, seq, want) in [
        ("a", 0, b"x"),
        ("a", 1, b"x"),
        ("b", 0, b"x"),
        ("a", 2, b"y"),
    ] {
        assert_eq!(read(&store, topic, seq).await.as_deref(), Some(&want[..]));
    }
    assert_eq!(read(&store, "a", 3).await, None);
    assert_eq!(read(&store, "zzz", 0).await, None);

    // A later batch with known bytes writes nothing to the segment.
    store.put_batch(&[msg("c", b"y")], 2).await.unwrap();
    assert_eq!(store.stats().segment_bytes, block_bytes);
    assert!(store.contains(&ObjectId::of(b"y")));
}

#[compio::test]
async fn one_fsync_per_file_per_batch() {
    let dir = TempDir::new("fsync");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    // The first batch also creates the segment: + one directory fsync.
    store.put_batch(&[msg("t", b"first")], 0).await.unwrap();
    assert_eq!(store.stats().fsyncs, 3);

    let payloads: Vec<Vec<u8>> = (0..64u32).map(|i| i.to_le_bytes().to_vec()).collect();
    let batch: Vec<Message> = payloads.iter().map(|p| msg("t", p)).collect();
    let before = store.stats().fsyncs;
    store.put_batch(&batch, 1).await.unwrap();
    // The first full index block also creates the index segment: its
    // header sync and a directory fsync. Index records are not synced.
    assert_eq!(
        store.stats().fsyncs - before,
        4,
        "segment + WAL + index segment creation, not per message"
    );

    // Only known bytes: nothing for the segment, one WAL fsync.
    let before = store.stats().fsyncs;
    store.put_batch(&batch, 2).await.unwrap();
    assert_eq!(store.stats().fsyncs - before, 1);

    let before = store.stats();
    store.put_batch(&[], 3).await.unwrap();
    assert_eq!(store.stats(), before, "an empty batch does no I/O");
}

#[compio::test]
async fn invalid_input_is_rejected_without_poisoning() {
    let dir = TempDir::new("invalid");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    let long = "x".repeat(straum_store::MAX_TOPIC + 1);
    for topic in ["", long.as_str()] {
        let err = store.put_batch(&[msg(topic, b"p")], 0).await.unwrap_err();
        assert!(matches!(err, StoreError::InvalidTopic));
    }
    store.put_batch(&[msg("ok", b"p")], 0).await.unwrap();
}

#[compio::test]
async fn state_survives_restart() {
    let dir = TempDir::new("restart");
    let config = Config {
        segment_max_bytes: 16,
        ..Config::default()
    };
    {
        let mut store = Store::open(&dir.0, config).await.unwrap();
        for i in 0..20u8 {
            let p = vec![i % 7; 1 + usize::from(i % 7)];
            store
                .put_batch(&[msg("t", &p), msg("u", &p)], u64::from(i))
                .await
                .unwrap();
        }
        assert!(store.segment_count() > 1, "segments rotated");
        assert_eq!(store.release_before(b"t", 5).await.unwrap(), 5);
    }
    let mut store = Store::open(&dir.0, config).await.unwrap();
    assert_eq!(store.topic_range(b"t"), Some((5, 20)));
    assert_eq!(store.topic_range(b"u"), Some((0, 20)));
    for i in 0..20u8 {
        let p = vec![i % 7; 1 + usize::from(i % 7)];
        let t = read(&store, "t", u64::from(i)).await;
        assert_eq!(t, (i >= 5).then(|| p.clone()));
        assert_eq!(read(&store, "u", u64::from(i)).await, Some(p));
    }
    // Sequences continue; timestamps stay non-decreasing per topic.
    assert_eq!(store.put_batch(&[msg("t", b"new")], 0).await.unwrap(), [20]);
}

#[compio::test]
async fn compaction_reclaims_released_objects() {
    let dir = TempDir::new("compact");
    let config = Config {
        segment_max_bytes: 8,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    store
        .put_batch(&[msg("a", b"dead-1"), msg("b", b"live-1")], 1)
        .await
        .unwrap();
    store
        .put_batch(&[msg("a", b"dead-2"), msg("b", b"live-2")], 2)
        .await
        .unwrap();
    // Shared with a live entry: must survive.
    store.put_batch(&[msg("a", b"live-1")], 3).await.unwrap();
    assert_eq!(store.release_before(b"a", 3).await.unwrap(), 2);
    assert!(store.contains(&ObjectId::of(b"dead-1")));

    // Each batch block is now partly dead: its live object is recompressed
    // into a new block.
    let stats = store.compact().await.unwrap();
    assert_eq!(stats.objects_dropped, 2);
    assert_eq!(stats.segments_rewritten, 2);
    assert!(stats.bytes_reclaimed > 0);
    assert!(!store.contains(&ObjectId::of(b"dead-1")));
    assert!(store.contains(&ObjectId::of(b"live-1")));
    assert_eq!(store.object_count(), 2);
    assert_eq!(
        store.compact().await.unwrap().segments_rewritten,
        0,
        "idempotent"
    );

    drop(store);
    let mut store = Store::open(&dir.0, config).await.unwrap();
    assert_eq!(read(&store, "a", 0).await, None);
    assert_eq!(read(&store, "a", 2).await.as_deref(), Some(&b"live-1"[..]));
    assert_eq!(read(&store, "b", 0).await.as_deref(), Some(&b"live-1"[..]));
    assert_eq!(read(&store, "b", 1).await.as_deref(), Some(&b"live-2"[..]));
    // Released bytes can be stored again after their object was reclaimed.
    assert_eq!(
        store.put_batch(&[msg("a", b"dead-1")], 4).await.unwrap(),
        [3]
    );
    assert_eq!(read(&store, "a", 3).await.as_deref(), Some(&b"dead-1"[..]));
}

#[compio::test]
async fn orphans_are_reclaimed_not_errors() {
    let dir = TempDir::new("orphan");
    {
        let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
        store.put_batch(&[msg("t", b"kept")], 0).await.unwrap();
    }
    // A segment no WAL record references (crash before the WAL fsync).
    let stray = dir.0.join("seg-00000000000000ff.dat");
    std::fs::write(&stray, b"orphan bytes").unwrap();
    // Orphan bytes at the end of a referenced segment.
    let seg = dir.0.join("seg-0000000000000000.dat");
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes.extend_from_slice(b"torn object");
    std::fs::write(&seg, bytes).unwrap();

    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert!(!stray.exists(), "unreferenced segment deleted at open");
    let stats = store.compact().await.unwrap();
    assert_eq!(stats.bytes_reclaimed, 11);
    assert_eq!(read(&store, "t", 0).await.as_deref(), Some(&b"kept"[..]));
    // Segment ids are never reused, the stray one included.
    store.put_batch(&[msg("t", b"next")], 1).await.unwrap();
    assert!(dir.0.join("seg-0000000000000101.dat").exists());
}

#[compio::test]
async fn missing_object_fails_recovery() {
    let dir = TempDir::new("missing");
    {
        let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
        store.put_batch(&[msg("t", b"payload")], 0).await.unwrap();
        // A later frame, so the damaged object belongs to an acknowledged
        // earlier frame (a damaged last frame is dropped instead).
        store.put_batch(&[msg("t", b"payload")], 1).await.unwrap();
    }
    let seg = dir.0.join("seg-0000000000000000.dat");
    std::fs::write(&seg, b"pay").unwrap();
    let err = Store::open(&dir.0, Config::default()).await.err().unwrap();
    assert!(matches!(err, StoreError::MissingObject(_)), "{err}");
}

#[compio::test]
async fn corrupted_object_fails_read() {
    let dir = TempDir::new("bitrot");
    {
        let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
        store.put_batch(&[msg("t", b"payload")], 0).await.unwrap();
        // A later frame, so the damaged object belongs to an acknowledged
        // earlier frame (a damaged last frame is dropped instead).
        store.put_batch(&[msg("t", b"payload")], 1).await.unwrap();
    }
    let seg = dir.0.join("seg-0000000000000000.dat");
    let mut bytes = std::fs::read(&seg).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x40;
    std::fs::write(&seg, bytes).unwrap();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    let err = store.read(b"t", 0).await.unwrap_err();
    assert!(
        matches!(err, StoreError::CorruptBlock { block: 0, .. }),
        "{err}"
    );
}

#[compio::test]
async fn torn_tail_at_every_offset() {
    for last in ["publish", "release", "compact"] {
        let dir = TempDir::new("torn-src");
        let config = Config {
            segment_max_bytes: 32,
            ..Config::default()
        };
        {
            let mut store = Store::open(&dir.0, config).await.unwrap();
            for i in 0..10u8 {
                let p = vec![i; 10];
                store
                    .put_batch(&[msg("keep", &p), msg("tmp", &[i, i])], u64::from(i))
                    .await
                    .unwrap();
            }
            match last {
                "publish" => {
                    store
                        .put_batch(&[msg("keep", b"last one")], 10)
                        .await
                        .unwrap();
                }
                "release" => {
                    assert_eq!(store.release_before(b"tmp", 1).await.unwrap(), 1);
                }
                _ => {
                    store.release_before(b"tmp", 100).await.unwrap();
                    let pre = TempDir::new("torn-pre");
                    copy_dir(&dir.0, &pre.0);
                    assert!(store.compact().await.unwrap().segments_rewritten > 0);
                    // Compaction unlinks old segments only after the WAL
                    // fsync. A torn Compact frame means that fsync never
                    // returned, so the old files are still there: restore
                    // them to build the state a crash actually leaves.
                    for entry in std::fs::read_dir(&pre.0).unwrap() {
                        let name = entry.unwrap().file_name();
                        let target = dir.0.join(&name);
                        if name.to_string_lossy().starts_with("seg-") && !target.exists() {
                            std::fs::copy(pre.0.join(&name), target).unwrap();
                        }
                    }
                }
            }
        }
        let wal = std::fs::read(dir.0.join(common::WAL0)).unwrap();
        let starts = wal_frame_starts(&wal);
        let last_start = *starts.last().unwrap();
        for cut in last_start..wal.len() {
            let work = TempDir::new("torn-cut");
            copy_dir(&dir.0, &work.0);
            std::fs::write(work.0.join(common::WAL0), &wal[..cut]).unwrap();
            let store = match Store::open(&work.0, config).await {
                Ok(store) => store,
                Err(e) => panic!("{last}: recovery failed at cut {cut}: {e}"),
            };
            let len = std::fs::metadata(work.0.join(common::WAL0)).unwrap().len();
            assert_eq!(
                len, last_start as u64,
                "{last}: torn tail truncated at cut {cut}"
            );
            // Everything before the last record is intact.
            for i in 0..10u8 {
                let got = read(&store, "keep", u64::from(i)).await;
                assert_eq!(got, Some(vec![i; 10]), "{last}: cut {cut}, keep/{i}");
            }
            match last {
                "publish" => assert_eq!(read(&store, "keep", 10).await, None),
                "release" => assert_eq!(store.topic_range(b"tmp"), Some((0, 10))),
                _ => assert_eq!(store.topic_range(b"tmp"), Some((10, 10))),
            }
            drop(store);
            // The repaired directory opens again unchanged.
            Store::open(&work.0, config).await.unwrap();
        }
    }
}

/// Platform-specific: compio must use io_uring on Linux and the polling
/// driver on macOS. Listed in the README.
#[cfg(target_os = "linux")]
#[test]
fn linux_runtime_uses_io_uring() {
    let rt = compio::runtime::Runtime::new().unwrap();
    assert_eq!(rt.driver_type(), compio::driver::DriverType::IoUring);
}

#[cfg(target_os = "macos")]
#[test]
fn macos_runtime_uses_polling() {
    let rt = compio::runtime::Runtime::new().unwrap();
    assert_eq!(rt.driver_type(), compio::driver::DriverType::Poll);
}

#[compio::test]
async fn inline_small_stores_small_objects_in_the_wal() {
    let dir = TempDir::new("inline");
    let config = Config {
        write_mode: straum_store::WriteMode::InlineSmall { threshold: 64 },
        ..Config::default()
    };
    // Incompressible: its block stays above the threshold.
    let mut rng = Rng(7);
    let big: Vec<u8> = (0..200).map(|_| rng.next() as u8).collect();
    {
        let mut store = Store::open(&dir.0, config).await.unwrap();
        // An inline block only: no segment, one fdatasync (the WAL).
        store
            .put_batch(&[msg("t", b"small"), msg("t", b"tiny")], 0)
            .await
            .unwrap();
        assert_eq!(store.segment_count(), 0);
        assert_eq!(store.stats().fsyncs, 1);
        assert_eq!(store.stats().segment_bytes, 0);
        // The block of the large object goes to a segment.
        store
            .put_batch(&[msg("t", &big), msg("u", b"small")], 1)
            .await
            .unwrap();
        assert_eq!(store.segment_count(), 1);
        // Dedup does not care where the bytes live.
        assert!(store.contains(&ObjectId::of(b"small")));
        assert_eq!(read(&store, "t", 0).await.as_deref(), Some(&b"small"[..]));
        assert_eq!(read(&store, "t", 2).await, Some(big.clone()));
        assert_eq!(store.release_before(b"t", 2).await.unwrap(), 3);
        assert_eq!(store.release_before(b"u", 2).await.unwrap(), 1);
        let stats = store.compact().await.unwrap();
        assert_eq!(stats.objects_dropped, 1, "only the segment block's object");
        // Inline blocks are not reclaimed: still indexed with no references.
        assert!(store.contains(&ObjectId::of(b"tiny")));
        assert!(!store.contains(&ObjectId::of(&big)));
        store.put_batch(&[msg("v", b"tiny")], 3).await.unwrap();
    }
    let store = Store::open(&dir.0, config).await.unwrap();
    assert_eq!(read(&store, "v", 0).await.as_deref(), Some(&b"tiny"[..]));
    assert!(store.contains(&ObjectId::of(b"tiny")));
    assert!(store.contains(&ObjectId::of(b"small")));
}

/// topic_seq is implicit in the index: an entry moved to another position
/// fails its CRC (which covers the position) and is rewritten from the WAL.
#[compio::test]
async fn dedup_counters_for_repeated_and_distinct_payloads() {
    use straum_store::WriteMode;
    const N: u64 = 7;
    let payload = vec![9u8; 300];
    for mode in [
        WriteMode::Sequential,
        WriteMode::ParallelSync,
        WriteMode::INLINE_SMALL,
    ] {
        let dir = TempDir::new("dedup-counters");
        let config = Config {
            write_mode: mode,
            ..Config::default()
        };
        let mut store = Store::open(&dir.0, config).await.unwrap();
        // The same payload N times: 3 in one batch, then one per batch.
        let three = [msg("t", &payload), msg("u", &payload), msg("t", &payload)];
        store.put_batch(&three, 0).await.unwrap();
        for i in 3..N {
            store.put_batch(&[msg("t", &payload)], i).await.unwrap();
        }
        let s = store.stats();
        let len = payload.len() as u64;
        assert_eq!(s.messages_accepted, N, "{mode:?}");
        assert_eq!(s.messages_deduplicated, N - 1, "{mode:?}");
        assert_eq!(s.payload_bytes_received, N * len, "{mode:?}");
        assert_eq!(s.payload_bytes_new, len, "{mode:?}");
        assert_eq!(s.payload_bytes_saved, (N - 1) * len, "{mode:?}");
        // Inline: the block's bytes are in the WAL but are not metadata.
        if mode == WriteMode::INLINE_SMALL {
            assert!(s.metadata_bytes < s.wal_bytes + s.topic_bytes, "{mode:?}");
        } else {
            assert_eq!(s.metadata_bytes, s.wal_bytes + s.topic_bytes, "{mode:?}");
        }

        // Distinct payloads: nothing deduplicated.
        let dir = TempDir::new("dedup-distinct");
        let mut store = Store::open(&dir.0, config).await.unwrap();
        let payloads: Vec<Vec<u8>> = (0..N as u8).map(|i| vec![i; 50]).collect();
        let batch: Vec<Message> = payloads.iter().map(|p| msg("t", p)).collect();
        store.put_batch(&batch, 0).await.unwrap();
        let s = store.stats();
        assert_eq!(s.messages_accepted, N);
        assert_eq!(s.messages_deduplicated, 0);
        assert_eq!(s.payload_bytes_saved, 0);
        assert_eq!(s.payload_bytes_new, N * 50);

        // Counters start at zero on open.
        drop(store);
        let store = Store::open(&dir.0, config).await.unwrap();
        assert_eq!(store.stats(), Default::default());
    }
}

/// `n` messages with distinct payloads `i.to_le_bytes()` to topic "t", one
/// batch, timestamp 7.
async fn write_messages(dir: &std::path::Path, n: u32) {
    let payloads: Vec<[u8; 4]> = (0..n).map(u32::to_le_bytes).collect();
    let batch: Vec<Message> = payloads.iter().map(|p| msg("t", p)).collect();
    let mut store = Store::open(dir, Config::default()).await.unwrap();
    store.put_batch(&batch, 7).await.unwrap();
}

/// An intact block record holding other entries than the WAL was written
/// that way: an error, and no file changes.
#[compio::test]
async fn index_block_disagreeing_with_wal_fails_recovery() {
    let dir = TempDir::new("topic-mismatch");
    write_messages(&dir.0, 64).await;
    let rec = find_index_record(&dir.0, INDEX_BLOCK_RECORD, 0, 0);
    // Entry 0 holds object number 0 as a 1-byte varint; make it 1 and
    // reseal, so the record is intact but disagrees with the WAL.
    edit(&rec.path, |b| {
        assert_eq!(b[rec.body.start], 0);
        b[rec.body.start] = 1;
        reseal_index_record(b, rec.offset, rec.size);
    });
    let before = snapshot(&dir.0);
    let err = Store::open(&dir.0, Config::default()).await.err().unwrap();
    assert!(
        matches!(&err, StoreError::TopicMismatch { topic, seq: 0 } if topic == b"t"),
        "{err}"
    );
    assert!(snapshot(&dir.0) == before, "files changed");
}

/// An intact record of a durable frame for a block the WAL does not have.
#[compio::test]
async fn index_block_beyond_the_wal_fails_recovery() {
    let dir = TempDir::new("topic-beyond");
    write_messages(&dir.0, 64).await;
    let rec = find_index_record(&dir.0, INDEX_BLOCK_RECORD, 0, 0);
    // The number field follows kind, len (2 bytes: 64 entries are more
    // than 127 bytes) and topic: block 0 → 5.
    edit(&rec.path, |b| {
        assert_eq!(b[rec.offset + 4], 0);
        b[rec.offset + 4] = 5;
        reseal_index_record(b, rec.offset, rec.size);
    });
    let before = snapshot(&dir.0);
    let err = Store::open(&dir.0, Config::default()).await.err().unwrap();
    assert!(
        matches!(err, StoreError::IndexSegmentCorrupt { segment: 0, .. }),
        "{err}"
    );
    assert!(snapshot(&dir.0) == before, "files changed");
}

#[compio::test]
async fn lost_index_segment_is_rebuilt_from_wal() {
    let dir = TempDir::new("topic-rebuild");
    write_messages(&dir.0, 100).await;
    let segments = index_segments(&dir.0);
    assert_eq!(segments.len(), 1);
    let good = std::fs::read(&segments[0]).unwrap();
    // Unsynced pages lost: the record cut and zero-extended.
    let mut cut = good.clone();
    cut.truncate(good.len() - 3);
    cut.resize(good.len(), 0);
    std::fs::write(&segments[0], cut).unwrap();
    for round in 0..2 {
        if round == 1 {
            // Then the segments are deleted altogether.
            for path in index_segments(&dir.0) {
                std::fs::remove_file(path).unwrap();
            }
        }
        let store = Store::open(&dir.0, Config::default()).await.unwrap();
        assert_eq!(
            store.recovery_report().actions,
            [RecoveryAction::TopicIndexRepaired {
                topic: b"t".to_vec(),
                blocks_rewritten: 1,
                pages_rewritten: 0,
            }],
            "round {round}"
        );
        for seq in [0, 63, 64, 99] {
            assert_eq!(
                read(&store, "t", seq).await.as_deref(),
                Some(&(seq as u32).to_le_bytes()[..])
            );
        }
        drop(store);
        let store = Store::open(&dir.0, Config::default()).await.unwrap();
        assert!(store.recovery_report().is_empty(), "round {round}: clean");
    }
    // The rewritten record is the original one, in a new segment.
    let rebuilt = std::fs::read(index_segments(&dir.0)[0].clone()).unwrap();
    assert_eq!(rebuilt[24..], good[24..], "re-encoded byte for byte");
}

/// A damaged record ends what is read of its segment: the records after it
/// are rewritten from the WAL, those before it are kept.
#[compio::test]
async fn a_damaged_record_ends_the_scan_of_its_segment() {
    let dir = TempDir::new("topic-damaged");
    write_messages(&dir.0, 3 * 64).await;
    let rec = find_index_record(&dir.0, INDEX_BLOCK_RECORD, 0, 1);
    edit(&rec.path, |b| b[rec.body.start + 5] ^= 0xff);
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert_eq!(
        store.recovery_report().actions,
        [RecoveryAction::TopicIndexRepaired {
            topic: b"t".to_vec(),
            blocks_rewritten: 2,
            pages_rewritten: 0,
        }]
    );
    for seq in 0..3 * 64u32 {
        assert_eq!(
            read(&store, "t", u64::from(seq)).await.as_deref(),
            Some(&seq.to_le_bytes()[..])
        );
    }
    drop(store);
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert!(store.recovery_report().is_empty());
}

#[compio::test]
async fn index_lookup_by_position() {
    let dir = TempDir::new("topic-lookup");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    // 150 distinct objects (numbers 0..150), then 30 repeats of object 5.
    let payloads: Vec<[u8; 4]> = (0..150u32).map(u32::to_le_bytes).collect();
    for (i, chunk) in payloads.chunks(50).enumerate() {
        let batch: Vec<Message> = chunk.iter().map(|p| msg("t", p)).collect();
        store.put_batch(&batch, 1000 + i as u64).await.unwrap();
    }
    let repeat = [msg("t", &payloads[5]); 30];
    store.put_batch(&repeat, 2000).await.unwrap();
    for seq in 0..150u64 {
        let want = Some((seq, 1000 + seq / 50));
        assert_eq!(store.index_entry(b"t", seq).await.unwrap(), want, "{seq}");
    }
    for seq in 150..180u64 {
        assert_eq!(store.index_entry(b"t", seq).await.unwrap(), Some((5, 2000)));
    }
    assert_eq!(store.index_entry(b"t", 180).await.unwrap(), None);
    assert_eq!(store.index_entry(b"nope", 0).await.unwrap(), None);
    // The same after a restart: block offsets are rebuilt at open.
    drop(store);
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert_eq!(store.index_entry(b"t", 177).await.unwrap(), Some((5, 2000)));
    assert_eq!(store.index_entry(b"t", 64).await.unwrap(), Some((64, 1001)));
}
