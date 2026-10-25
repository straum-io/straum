#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::{TempDir, copy_dir, wal_frame_starts};
use straum_store::{Config, Message, ObjectId, Store, StoreError};

fn msg<'a>(topic: &'a str, payload: &'a [u8]) -> Message<'a> {
    Message {
        topic: topic.as_bytes(),
        payload,
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
    assert_eq!(store.refcount(&ObjectId::of(b"x")), Some(3));
    assert_eq!(store.refcount(&ObjectId::of(b"y")), Some(1));
    assert_eq!(store.object_count(), 2);
    assert_eq!(
        store.stats().segment_bytes,
        2,
        "each distinct object written once"
    );
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
    assert_eq!(store.stats().segment_bytes, 2);
    assert_eq!(store.refcount(&ObjectId::of(b"y")), Some(2));
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
    assert_eq!(
        store.stats().fsyncs - before,
        2,
        "segment + WAL, not per message"
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
    assert_eq!(store.refcount(&ObjectId::of(b"dead-1")), Some(0));

    let stats = store.compact().await.unwrap();
    assert_eq!(stats.objects_dropped, 2);
    assert_eq!(stats.bytes_reclaimed, 12);
    assert_eq!(store.refcount(&ObjectId::of(b"dead-1")), None);
    assert_eq!(store.refcount(&ObjectId::of(b"live-1")), Some(2));
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
    std::fs::write(&seg, b"PAYLOAD").unwrap();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    let err = store.read(b"t", 0).await.unwrap_err();
    assert!(matches!(err, StoreError::HashMismatch(_)), "{err}");
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
        let wal = std::fs::read(dir.0.join("wal.log")).unwrap();
        let starts = wal_frame_starts(&wal);
        let last_start = *starts.last().unwrap();
        for cut in last_start..wal.len() {
            let work = TempDir::new("torn-cut");
            copy_dir(&dir.0, &work.0);
            std::fs::write(work.0.join("wal.log"), &wal[..cut]).unwrap();
            let store = match Store::open(&work.0, config).await {
                Ok(store) => store,
                Err(e) => panic!("{last}: recovery failed at cut {cut}: {e}"),
            };
            let len = std::fs::metadata(work.0.join("wal.log")).unwrap().len();
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
        write_mode: straum_store::WriteMode::InlineSmall { threshold: 16 },
        ..Config::default()
    };
    let big = vec![7u8; 100];
    {
        let mut store = Store::open(&dir.0, config).await.unwrap();
        // Inline only: no segment, one fdatasync (the WAL).
        store
            .put_batch(&[msg("t", b"small"), msg("t", b"tiny")], 0)
            .await
            .unwrap();
        assert_eq!(store.segment_count(), 0);
        assert_eq!(store.stats().fsyncs, 1);
        assert_eq!(store.stats().segment_bytes, 0);
        // Mixed: the large object goes to a segment.
        store
            .put_batch(&[msg("t", &big), msg("u", b"small")], 1)
            .await
            .unwrap();
        assert_eq!(store.segment_count(), 1);
        // Dedup does not care where the bytes live.
        assert_eq!(store.refcount(&ObjectId::of(b"small")), Some(2));
        assert_eq!(read(&store, "t", 0).await.as_deref(), Some(&b"small"[..]));
        assert_eq!(read(&store, "t", 2).await, Some(big.clone()));
        assert_eq!(store.release_before(b"t", 2).await.unwrap(), 3);
        assert_eq!(store.release_before(b"u", 2).await.unwrap(), 1);
        let stats = store.compact().await.unwrap();
        assert_eq!(stats.objects_dropped, 1, "only the segment object");
        // Inline objects are not reclaimed: still indexed with no references.
        assert_eq!(store.refcount(&ObjectId::of(b"tiny")), Some(0));
        assert_eq!(store.refcount(&ObjectId::of(&big)), None);
        store.put_batch(&[msg("v", b"tiny")], 3).await.unwrap();
    }
    let store = Store::open(&dir.0, config).await.unwrap();
    assert_eq!(read(&store, "v", 0).await.as_deref(), Some(&b"tiny"[..]));
    assert_eq!(store.refcount(&ObjectId::of(b"tiny")), Some(1));
    assert_eq!(store.refcount(&ObjectId::of(b"small")), Some(0));
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
        // Inline: the object's bytes are in the WAL but are not metadata.
        let inline = if mode == WriteMode::INLINE_SMALL {
            len
        } else {
            0
        };
        assert_eq!(
            s.metadata_bytes,
            s.wal_bytes + s.topic_bytes - inline,
            "{mode:?}"
        );

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
/// batch, timestamp 7. Returns the topic index bytes.
async fn index_of_messages(dir: &std::path::Path, n: u32) -> Vec<u8> {
    let payloads: Vec<[u8; 4]> = (0..n).map(u32::to_le_bytes).collect();
    let batch: Vec<Message> = payloads.iter().map(|p| msg("t", p)).collect();
    let mut store = Store::open(dir, Config::default()).await.unwrap();
    store.put_batch(&batch, 7).await.unwrap();
    std::fs::read(dir.join("topics/74.idx")).unwrap()
}

/// docs/format.md: a sealed block ends with CRC32C over the block number
/// (u64 LE) and the block's entry bytes.
fn reseal_block(block: &mut [u8], number: u64) {
    let body = block.len() - 4;
    let crc = crc32c::crc32c_append(crc32c::crc32c(&number.to_le_bytes()), &block[..body]);
    block[body..].copy_from_slice(&crc.to_le_bytes());
}

#[compio::test]
async fn topic_index_disagreeing_with_wal_fails_recovery() {
    let dir = TempDir::new("topic-mismatch");
    // Exactly one sealed block: 64 entries and its CRC.
    let mut bytes = index_of_messages(&dir.0, 64).await;
    // Entry 0 holds object number 0 as a 1-byte varint; make it 1 and
    // re-seal, so the block is valid but disagrees with the WAL.
    assert_eq!(bytes[0], 0);
    bytes[0] = 1;
    reseal_block(&mut bytes, 0);
    std::fs::write(dir.0.join("topics/74.idx"), &bytes).unwrap();
    let err = Store::open(&dir.0, Config::default()).await.err().unwrap();
    assert!(
        matches!(err, StoreError::TopicMismatch { seq: 0, .. }),
        "{err}"
    );
}

#[compio::test]
async fn lost_topic_index_is_rebuilt_from_wal() {
    let dir = TempDir::new("topic-rebuild");
    let good = index_of_messages(&dir.0, 100).await;
    let path = dir.0.join("topics/74.idx");
    // Unsynced pages lost: cut inside the open block, zero-extended.
    let mut bytes = good.clone();
    bytes.truncate(good.len() - 3);
    bytes.resize(good.len(), 0);
    std::fs::write(&path, bytes).unwrap();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert_eq!(
        read(&store, "t", 99).await.as_deref(),
        Some(&99u32.to_le_bytes()[..])
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        good,
        "repaired byte for byte"
    );
    drop(store);
    std::fs::remove_file(&path).unwrap();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert_eq!(
        read(&store, "t", 0).await.as_deref(),
        Some(&0u32.to_le_bytes()[..])
    );
    assert_eq!(std::fs::read(&path).unwrap(), good, "rebuilt byte for byte");
}

/// topic_seq is implicit: a block copied to another block position fails
/// its CRC (which covers the block number) and is rewritten from the WAL.
#[compio::test]
async fn index_block_at_wrong_position_is_rewritten() {
    let dir = TempDir::new("topic-moved");
    let good = index_of_messages(&dir.0, 128).await;
    let path = dir.0.join("topics/74.idx");
    // Block 0 ends with the CRC after its 64th entry: find it by decoding
    // through the public lookup, i.e. by reopening the store.
    let block1 = {
        let store = Store::open(&dir.0, Config::default()).await.unwrap();
        assert_eq!(store.index_entry(b"t", 64).await.unwrap(), Some((64, 7)));
        // Two equal-length sealed blocks (same entry sizes): half the file.
        good.len() / 2
    };
    let moved = [&good[..block1], &good[..block1]].concat();
    std::fs::write(&path, &moved).unwrap();
    let store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        good,
        "rewritten from the WAL"
    );
    assert!(
        store.recovery_report().actions.contains(
            &straum_store::RecoveryAction::TopicIndexRepaired {
                path: path.clone(),
                entries_kept: 64,
                bytes_truncated: (moved.len() - block1) as u64,
                entries_rewritten: 64,
            }
        ),
        "{:?}",
        store.recovery_report()
    );
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
