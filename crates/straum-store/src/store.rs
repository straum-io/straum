use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::Poll;

use compio::BufResult;
use compio::buf::{IntoInner, IoBuf};
use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};

use crate::StoreError;
use crate::codec::{
    self, FRAME_HEADER, FrameBuilder, FrameMode, INDEX_BLOCK, IndexWriter, MAX_TOPIC, NO_SEGMENT,
    ObjectId, RECORD_COMPACT_BASE, RECORD_PUBLISH, RECORD_PUT_INLINE_BASE, RECORD_PUT_OBJECT,
    RECORD_PUT_TOPIC_BASE, RECORD_RELEASE, Record, WAL_FILE_HEADER,
};

const WAL_FILE: &str = "wal.log";
const WAL_TMP: &str = "wal.log.tmp";
const TOPICS_DIR: &str = "topics";

/// How a batch's object bytes and its WAL frame are made durable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WriteMode {
    /// Segment write and fdatasync, then WAL write and fdatasync. A durable
    /// frame never references bytes that are not durable.
    #[default]
    Sequential,
    /// Segment and WAL written and fdatasync'ed concurrently; the batch is
    /// acknowledged after both. A crash can leave a durable last frame whose
    /// segment bytes are not: recovery detects it and drops that frame (it
    /// was never acknowledged).
    ParallelSync,
    /// Experimental. New objects of at most `threshold` bytes are stored in
    /// the WAL frame itself; larger ones go to segments, committed as in
    /// `Sequential`. A batch of inline objects only needs one fdatasync (the
    /// WAL). Inline bytes are never reclaimed: that needs a WAL checkpoint.
    InlineSmall { threshold: u32 },
}

impl WriteMode {
    /// `InlineSmall` with the default threshold.
    pub const INLINE_SMALL: Self = Self::InlineSmall { threshold: 4096 };

    fn inline_threshold(self) -> Option<usize> {
        match self {
            Self::InlineSmall { threshold } => Some(threshold as usize),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// A segment is sealed once the next object would push it past this size.
    /// An object larger than this gets a segment of its own.
    pub segment_max_bytes: u64,
    /// Largest WAL frame a batch, release or compaction step may produce.
    /// Checked before any I/O; a larger operation fails with
    /// `StoreError::FrameTooLarge`.
    pub max_frame_bytes: u32,
    pub write_mode: WriteMode,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            segment_max_bytes: 64 << 20,
            max_frame_bytes: 64 << 20,
            write_mode: WriteMode::Sequential,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Message<'a> {
    pub topic: &'a [u8],
    pub payload: &'a [u8],
    /// The publication's key; `Some(&[])` is a present, empty key. Key and
    /// headers belong to the publication, not to the object: the object is
    /// the payload alone (its BLAKE3 id), stored once.
    pub key: Option<&'a [u8]>,
    /// The publication's headers `(name, value)`, in order.
    pub headers: &'a [(&'a [u8], &'a [u8])],
}

/// One live entry as `Store::read_entry` returns it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Entry {
    pub timestamp: u64,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub payload: Vec<u8>,
}

/// One topic's counters, from memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TopicStats {
    /// The first live seq.
    pub first_live: u64,
    /// The seq the next entry gets.
    pub next_seq: u64,
    /// Bytes of the topic's index file.
    pub index_bytes: u64,
}

/// Counters since `open`, in memory only: they start at zero on every open
/// and are not persisted. Recovery work is not counted. Message counters
/// cover acknowledged batches only.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Stats {
    /// `fdatasync`/`fsync` calls on files and directories.
    pub fsyncs: u64,
    pub segment_bytes: u64,
    /// All WAL bytes, inline object bytes included.
    pub wal_bytes: u64,
    pub topic_bytes: u64,
    pub messages_accepted: u64,
    /// Messages whose object already existed (also earlier in the same
    /// batch): a reference was added, no bytes written.
    pub messages_deduplicated: u64,
    pub payload_bytes_received: u64,
    /// Payload bytes stored as new objects (in segments or inline).
    pub payload_bytes_new: u64,
    /// Payload bytes not written because the object already existed.
    pub payload_bytes_saved: u64,
    /// WAL and topic index bytes that are not object bytes: `wal_bytes +
    /// topic_bytes` minus the inline object bytes in the WAL.
    pub metadata_bytes: u64,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct CompactStats {
    pub segments_rewritten: u64,
    pub objects_dropped: u64,
    pub bytes_reclaimed: u64,
}

/// Where an object's bytes live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Location {
    Segment {
        id: u64,
        offset: u64,
    },
    /// Absolute offset in `wal.log` (an inline object).
    Wal {
        offset: u64,
    },
}

/// Every change recovery made to the store directory during `Store::open`.
/// Empty: recovery changed nothing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub actions: Vec<RecoveryAction>,
}

impl RecoveryReport {
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryAction {
    /// A leftover `wal.log.tmp` from an interrupted WAL creation was deleted.
    WalTmpRemoved,
    /// A torn last frame (short or failing its CRC) was cut off.
    TornTailTruncated {
        /// The sequence number the torn frame would have had.
        frame_seq: u64,
        offset: u64,
        bytes_removed: u64,
    },
    /// A complete last frame was dropped: written in ParallelSync, its object
    /// bytes were not all durable, so it was never acknowledged.
    FrameDropped {
        frame_seq: u64,
        offset: u64,
        mode: FrameMode,
        object: ObjectId,
        reason: DropReason,
        bytes_removed: u64,
    },
    /// A segment file no WAL record references was deleted.
    OrphanSegmentRemoved { segment: u64 },
    /// An index file of a topic the WAL does not know was deleted.
    UnknownTopicIndexRemoved { path: PathBuf },
    /// A topic index was cut and/or rewritten from the WAL.
    TopicIndexRepaired {
        path: PathBuf,
        entries_kept: u64,
        bytes_truncated: u64,
        entries_rewritten: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    ObjectMissing,
    ObjectHashMismatch,
}

struct Object {
    /// Full BLAKE3 id; WAL Publish records and topic indexes use the
    /// object number instead.
    id: ObjectId,
    loc: Location,
    len: u32,
    /// Live topic entries pointing at this object.
    refs: u64,
}

struct Topic {
    /// Topic number from the topic's PutTopic record.
    number: u64,
    file: File,
    /// Encoder state after the last entry (released ones included).
    index: IndexWriter,
    /// Bytes in the index file.
    index_len: u64,
    /// File offset of every block, so entry `seq` is found by reading one
    /// block (`Store::index_entry`). Rebuilt at open, which reads the whole
    /// index anyway to check it against the WAL.
    block_starts: Vec<u64>,
    first_live: u64,
    last_timestamp: u64,
    // Every live entry is held in memory. If the live set outgrows RAM,
    // reads can be served from the index file (`Store::index_entry`).
    live: VecDeque<Live>,
    /// Encoded entries of the current batch, written after the WAL fsync.
    pending: Vec<u8>,
}

/// A topic entry in memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Live {
    number: u64,
    timestamp: u64,
    /// Absolute offset and length of the publication's key and headers in
    /// `wal.log` (a Publish record's tail); length 0: neither. The WAL is
    /// never rewritten, so the offset stays valid.
    attrs_at: u64,
    attrs_len: u32,
}

struct Active {
    id: u64,
    file: File,
    /// Bytes assigned to objects, buffered ones included.
    len: u64,
    /// Bytes on disk.
    written: u64,
}

/// One shard: a WAL, data segments and per-topic indexes in one directory.
///
/// `!Send` by design (compio is thread-per-core). A sharded deployment runs one
/// independent `Store` per core, each with its own directory.
///
/// Any I/O error during a mutation leaves the in-memory state ahead of the
/// disk, so the store refuses further calls (`StoreError::Poisoned`) until it
/// is reopened; recovery then rebuilds the state from disk.
pub struct Store {
    dir: PathBuf,
    config: Config,
    wal: File,
    wal_len: u64,
    /// Sequence number of the next WAL frame.
    next_frame: u64,
    /// A WAL frame is being written. Frames are written one at a time and
    /// each is durable before the next starts; recovery relies on it (only
    /// the last frame can be torn).
    in_flight: bool,
    /// Object number → object. Numbers are never reused and survive
    /// compaction; only locations change.
    objects: HashMap<u64, Object>,
    /// BLAKE3 id → object number, for deduplication.
    ids: HashMap<ObjectId, u64>,
    next_object: u64,
    next_topic: u64,
    /// Open handles of every segment the WAL references.
    segments: HashMap<u64, File>,
    active: Option<Active>,
    next_segment: u64,
    topics: HashMap<Vec<u8>, Topic>,
    wal_buf: Vec<u8>,
    seg_buf: Vec<u8>,
    stats: Stats,
    /// Inline object bytes written to the WAL since open (InlineSmall).
    inline_bytes: u64,
    recovery: RecoveryReport,
    poisoned: bool,
}

impl Store {
    /// Opens or creates the store in `dir`, recovering from the WAL.
    ///
    /// A WAL without this version's file header is `UnsupportedFormat`. A
    /// torn last frame is truncated; a bad frame followed by a valid one is
    /// `WalCorrupted` and nothing is modified. If the last complete frame
    /// introduces an object that is missing or fails its BLAKE3, the frame is
    /// dropped only when it was written in ParallelSync and nothing follows
    /// it (it was never acknowledged); otherwise that is corruption
    /// (`MissingObject`, `HashMismatch`) and no file is changed. Objects of
    /// earlier frames are checked for presence and bounds only; their content
    /// is verified on read. Recovery then deletes segment files the WAL does
    /// not reference, repairs topic index files from the WAL and recomputes
    /// refcounts. Every change is listed in `recovery_report()`.
    pub async fn open(dir: impl AsRef<Path>, config: Config) -> Result<Self, StoreError> {
        let dir = dir.as_ref().to_path_buf();
        let mut report = RecoveryReport::default();
        compio::fs::create_dir_all(dir.join(TOPICS_DIR)).await?;
        let wal_path = dir.join(WAL_FILE);
        let wal = open_wal(&dir, &mut report).await?;
        let bytes = compio::fs::read(&wal_path).await?;
        if !codec::is_current_wal(&bytes) {
            return Err(StoreError::UnsupportedFormat);
        }

        // Only the last frame can be torn: a frame is written only after the
        // previous one is durable. A bad frame with a valid frame anywhere
        // after it is mid-log damage; the file is then left untouched.
        let mut frames = Vec::new();
        let mut pos = WAL_FILE_HEADER;
        // Next object number at the start of each frame: the Publish delta
        // base. Object numbers only grow, so it is the highest seen + 1.
        let mut next_object = 0u64;
        while pos < bytes.len() {
            let offset = pos as u64;
            let Some(frame) = codec::decode_frame(&bytes[pos..]) else {
                if let Some(next) = codec::next_valid_frame(&bytes, pos) {
                    return Err(StoreError::WalCorrupted {
                        offset,
                        next_frame: next as u64,
                    });
                }
                break;
            };
            let corrupt = |reason| StoreError::CorruptWal { offset, reason };
            if frame.seq != frames.len() as u64 {
                return Err(corrupt("frame sequence number out of order"));
            }
            let mode = codec::frame_mode(&frame).map_err(corrupt)?;
            let records = codec::decode_records(&frame, next_object).map_err(corrupt)?;
            for rec in &records {
                if let Record::PutObject { number, .. } | Record::PutInline { number, .. } = rec {
                    next_object = next_object.max(number.saturating_add(1));
                }
            }
            pos += frame.size();
            frames.push((offset, mode, records));
        }
        let torn = pos < bytes.len();

        // The last complete frame's new objects must be fully in their
        // segments with a matching BLAKE3. Only a ParallelSync frame with
        // nothing after it can lack them legitimately (crash before it was
        // acknowledged); it is dropped. Anywhere else the frame was durable
        // after its object bytes, or was followed by another frame and so
        // acknowledged: corruption, reported with no file changed.
        let mut dropped = None;
        if let Some((offset, mode, records)) = frames.last()
            && let Some((object, reason)) = first_bad_object(&dir, records).await?
        {
            if *mode != FrameMode::ParallelSync || torn {
                return Err(match reason {
                    DropReason::ObjectMissing => StoreError::MissingObject(object),
                    DropReason::ObjectHashMismatch => StoreError::HashMismatch(object),
                });
            }
            dropped = Some(RecoveryAction::FrameDropped {
                frame_seq: frames.len() as u64 - 1,
                offset: *offset,
                mode: *mode,
                object,
                reason,
                bytes_removed: bytes.len() as u64 - offset,
            });
            pos = *offset as usize;
            frames.pop();
        }
        match dropped {
            Some(action) => report.actions.push(action),
            None if torn => report.actions.push(RecoveryAction::TornTailTruncated {
                frame_seq: frames.len() as u64,
                offset: pos as u64,
                bytes_removed: (bytes.len() - pos) as u64,
            }),
            None => {}
        }
        let wal_len = pos as u64;
        if pos < bytes.len() {
            wal.set_len(wal_len).await?;
            wal.sync_all().await?;
        }

        let mut replay = Replay::default();
        let next_frame = frames.len() as u64;
        for (offset, _, records) in frames {
            let body_at = offset + FRAME_HEADER as u64;
            for rec in records {
                replay
                    .apply(rec, body_at)
                    .map_err(|reason| StoreError::CorruptWal { offset, reason })?;
            }
        }

        let segments = open_segments(&dir, &mut replay, &mut report).await?;
        let topics = open_topics(&dir, &replay, &mut report).await?;
        check_refcounts(
            &replay.objects,
            topics
                .values()
                .flat_map(|t| t.live.iter().map(|e| &e.number)),
        )?;

        Ok(Self {
            dir,
            config,
            wal,
            wal_len,
            next_frame,
            in_flight: false,
            next_segment: replay.max_segment.map_or(0, |m| m + 1),
            objects: replay.objects,
            ids: replay.ids,
            next_object: replay.next_object,
            next_topic: replay.topic_names.len() as u64,
            segments,
            active: None,
            topics,
            wal_buf: Vec::new(),
            seg_buf: Vec::new(),
            stats: Stats::default(),
            inline_bytes: 0,
            recovery: report,
            poisoned: false,
        })
    }

    /// Durably appends a batch and returns the topic sequence number of each
    /// message, in order.
    ///
    /// Group commit: new objects go to the active segment in one write per
    /// segment, followed by one `fdatasync` per segment; then one WAL frame
    /// holding all records of the batch, one write and one `fdatasync`. Only then does this
    /// return. Bytes already stored (same BLAKE3 hash) are not written again.
    /// Timestamps are `now_ms`, raised per topic to keep them non-decreasing.
    pub async fn put_batch(
        &mut self,
        batch: &[Message<'_>],
        now_ms: u64,
    ) -> Result<Vec<u64>, StoreError> {
        self.usable()?;
        for m in batch {
            if m.topic.is_empty() || m.topic.len() > MAX_TOPIC {
                return Err(StoreError::InvalidTopic);
            }
            u32::try_from(m.payload.len()).map_err(|_| StoreError::ObjectTooLarge)?;
        }
        // Upper bound: every object counted as new.
        let frame = self.batch_frame_bytes(batch);
        self.check_frame(frame)?;
        let res = self.put_batch_inner(batch, now_ms).await;
        self.poison_on_err(res)
    }

    async fn put_batch_inner(
        &mut self,
        batch: &[Message<'_>],
        now_ms: u64,
    ) -> Result<Vec<u64>, StoreError> {
        self.begin_write();
        let mut seqs = Vec::with_capacity(batch.len());
        let mut wal_buf = std::mem::take(&mut self.wal_buf);
        wal_buf.clear();
        let mut frame = FrameBuilder::begin(&mut wal_buf, self.next_object);
        let mut attrs = Vec::new();
        let wal_len = self.wal_len;
        // Added to `stats` once the batch is durable.
        let (mut deduplicated, mut new_bytes, mut inline_bytes) = (0, 0, 0);
        for m in batch {
            let id = ObjectId::of(m.payload);
            let known = self.ids.get(&id).copied();
            match known {
                Some(_) => deduplicated += 1,
                None => new_bytes += m.payload.len() as u64,
            }
            let number = match known {
                Some(number) => number,
                None => {
                    let number = self.next_object;
                    self.next_object += 1;
                    // Checked in put_batch.
                    let len = m.payload.len() as u32;
                    let inline = self.config.write_mode.inline_threshold();
                    let loc = if inline.is_some_and(|t| m.payload.len() <= t) {
                        // The frame starts at `wal_len`, at index 0 of the buffer.
                        let put = Record::PutInline {
                            id,
                            number,
                            data: m.payload,
                            data_at: 0,
                        };
                        frame.record(&mut wal_buf, &put)?;
                        // The object's bytes end the record just written;
                        // the frame starts at `wal_len`, at index 0.
                        let offset = self.wal_len + (wal_buf.len() - m.payload.len()) as u64;
                        inline_bytes += m.payload.len() as u64;
                        Location::Wal { offset }
                    } else {
                        let (segment, offset) = self.reserve(u64::from(len)).await?;
                        self.seg_buf.extend_from_slice(m.payload);
                        let put = Record::PutObject {
                            id,
                            number,
                            segment,
                            offset,
                            len,
                        };
                        frame.record(&mut wal_buf, &put)?;
                        Location::Segment {
                            id: segment,
                            offset,
                        }
                    };
                    let object = Object {
                        id,
                        loc,
                        len,
                        refs: 0,
                    };
                    self.objects.insert(number, object);
                    self.ids.insert(id, number);
                    number
                }
            };
            if let Some(object) = self.objects.get_mut(&number) {
                object.refs += 1;
            }

            let new_topic = !self.topics.contains_key(m.topic);
            let topic = self.topic_mut(m.topic).await?;
            if new_topic {
                let put = Record::PutTopic {
                    number: topic.number,
                    name: m.topic,
                };
                frame.record(&mut wal_buf, &put)?;
            }
            let seq = topic.first_live + topic.live.len() as u64;
            let timestamp = now_ms.max(topic.last_timestamp);
            topic.last_timestamp = timestamp;
            let at = topic.index_len + topic.pending.len() as u64;
            if topic.index.push(&mut topic.pending, number, timestamp) {
                topic.block_starts.push(at);
            }
            attrs.clear();
            codec::encode_attrs(&mut attrs, m.key, m.headers);
            let publish = Record::Publish {
                topic: topic.number,
                seq,
                number,
                timestamp,
                attrs: &attrs,
                attrs_at: 0,
            };
            frame.record(&mut wal_buf, &publish)?;
            // The attributes end the record just written; the frame starts
            // at `wal_len`, at index 0 of the buffer.
            let attrs_len = u32::try_from(attrs.len()).map_err(|_| StoreError::FrameTooLarge)?;
            topic.live.push_back(Live {
                number,
                timestamp,
                attrs_at: wal_len + (wal_buf.len() - attrs.len()) as u64,
                attrs_len,
            });
            seqs.push(seq);
        }
        match self.config.write_mode {
            // InlineSmall keeps Sequential's order for segment objects; with
            // only inline objects the segment flush is a no-op.
            WriteMode::Sequential => {
                self.flush_segment().await?;
                self.append_frame(wal_buf, frame, FrameMode::Sequential)
                    .await?;
            }
            WriteMode::InlineSmall { .. } => {
                self.flush_segment().await?;
                self.append_frame(wal_buf, frame, FrameMode::InlineSmall)
                    .await?;
            }
            WriteMode::ParallelSync => self.commit_parallel(wal_buf, frame).await?,
        }
        self.write_topic_entries().await?;
        self.in_flight = false;
        let received: u64 = batch.iter().map(|m| m.payload.len() as u64).sum();
        let s = &mut self.stats;
        s.messages_accepted += batch.len() as u64;
        s.messages_deduplicated += deduplicated;
        s.payload_bytes_received += received;
        s.payload_bytes_new += new_bytes;
        s.payload_bytes_saved += received - new_bytes;
        self.inline_bytes += inline_bytes;
        Ok(seqs)
    }

    /// Releases the live entries of `topic` with a timestamp below `cutoff_ms`
    /// (a prefix, since timestamps are non-decreasing per topic). One WAL
    /// frame and one `fdatasync` for all of them; a release that would not
    /// fit in `max_frame_bytes` fails with `FrameTooLarge` before any change. Returns how many were
    /// released. Objects whose refcount drops to zero are reclaimed by
    /// [`Store::compact`].
    pub async fn release_before(
        &mut self,
        topic: &[u8],
        cutoff_ms: u64,
    ) -> Result<u64, StoreError> {
        self.usable()?;
        if let Some(t) = self.topics.get(topic) {
            let n = t
                .live
                .iter()
                .take_while(|e| e.timestamp < cutoff_ms)
                .count();
            self.check_frame(FRAME_HEADER + n * RECORD_RELEASE)?;
        }
        let res = self.release_inner(topic, cutoff_ms).await;
        self.poison_on_err(res)
    }

    async fn release_inner(&mut self, name: &[u8], cutoff_ms: u64) -> Result<u64, StoreError> {
        self.begin_write();
        let Some(topic) = self.topics.get_mut(name) else {
            self.in_flight = false;
            return Ok(0);
        };
        let mut wal_buf = std::mem::take(&mut self.wal_buf);
        wal_buf.clear();
        let mut frame = FrameBuilder::begin(&mut wal_buf, self.next_object);
        let mut released = 0;
        while let Some(&Live {
            number, timestamp, ..
        }) = topic.live.front()
        {
            if timestamp >= cutoff_ms {
                break;
            }
            let release = Record::Release {
                topic: topic.number,
                seq: topic.first_live,
            };
            frame.record(&mut wal_buf, &release)?;
            topic.live.pop_front();
            topic.first_live += 1;
            if let Some(object) = self.objects.get_mut(&number) {
                object.refs -= 1;
            }
            released += 1;
        }
        self.append_frame(wal_buf, frame, FrameMode::Sequential)
            .await?;
        self.in_flight = false;
        Ok(released)
    }

    /// Reads the bytes of entry `seq` of `topic`; `None` if the entry was
    /// never published or was released. The object's hash is verified.
    pub async fn read(&self, topic: &[u8], seq: u64) -> Result<Option<Vec<u8>>, StoreError> {
        self.usable()?;
        match self.entry_number(topic, seq) {
            Some(number) => self.read_object(number).await.map(|(_, b)| Some(b)),
            None => Ok(None),
        }
    }

    /// Mark and sweep. Marks objects with a non-zero refcount; every segment
    /// holding a dead object or unreferenced bytes (orphans of a crash) is
    /// rewritten with only its live objects:
    ///
    /// 1. write the new segment, `fdatasync` it, `fsync` the directory
    /// 2. append a frame with one Compact record to the WAL, `fdatasync` it
    /// 3. switch the index to the new segment, delete the old file
    ///
    /// A crash before step 2 leaves an unreferenced new segment and a crash
    /// after it a stale old one; recovery deletes either.
    pub async fn compact(&mut self) -> Result<CompactStats, StoreError> {
        self.usable()?;
        let res = self.compact_inner().await;
        self.poison_on_err(res)
    }

    async fn compact_inner(&mut self) -> Result<CompactStats, StoreError> {
        let mut stats = CompactStats::default();
        // Seal the active segment so it can be compacted too. Every batch
        // flushed it, so nothing is buffered.
        self.active = None;
        let mut by_segment: HashMap<u64, Vec<u64>> = HashMap::new();
        // Inline objects stay in the WAL and in the index even when dead:
        // reclaiming them needs a WAL checkpoint, which does not exist yet.
        for (&number, object) in &self.objects {
            if let Location::Segment { id: segment, .. } = object.loc {
                by_segment.entry(segment).or_default().push(number);
            }
        }
        let mut ids: Vec<u64> = self.segments.keys().copied().collect();
        ids.sort_unstable();
        for old in ids {
            let Some(old_file) = self.segments.get(&old) else {
                continue;
            };
            let file_len = old_file.metadata().await?.len();
            let members = by_segment.remove(&old).unwrap_or_default();
            let mut live = Vec::new();
            let mut live_bytes = 0;
            for number in &members {
                if let Some(o) = self.objects.get(number).filter(|o| o.refs > 0) {
                    live.push(*number);
                    live_bytes += u64::from(o.len);
                }
            }
            // Objects never overlap, so equal sizes mean no unreferenced byte.
            if live.len() == members.len() && live_bytes == file_len {
                continue;
            }

            self.check_frame(FRAME_HEADER + RECORD_COMPACT_BASE + 40 * live.len())?;
            self.begin_write();
            // `(id, new offset)` for the Compact record; numbers to relocate.
            let mut kept = Vec::with_capacity(live.len());
            let mut moved = Vec::with_capacity(live.len());
            let new = if live.is_empty() {
                NO_SEGMENT
            } else {
                let new = self.next_segment;
                self.next_segment += 1;
                let mut buf = Vec::with_capacity(live_bytes as usize);
                for &number in &live {
                    // Verifies the hash: corruption is not copied forward.
                    let (id, bytes) = self.read_object(number).await?;
                    kept.push((id, buf.len() as u64));
                    moved.push((number, buf.len() as u64));
                    buf.extend_from_slice(&bytes);
                }
                let file = create_segment(&self.dir, new).await?;
                let n = buf.len() as u64;
                write_all(&file, buf, 0).await.0?;
                file.sync_data().await?;
                sync_dir(&self.dir).await?;
                self.stats.fsyncs += 2;
                self.stats.segment_bytes += n;
                self.segments.insert(new, file);
                new
            };

            let mut wal_buf = std::mem::take(&mut self.wal_buf);
            wal_buf.clear();
            let mut frame = FrameBuilder::begin(&mut wal_buf, self.next_object);
            let compact = Record::Compact { old, new, kept };
            frame.record(&mut wal_buf, &compact)?;
            self.append_frame(wal_buf, frame, FrameMode::Sequential)
                .await?;
            self.in_flight = false;

            for number in &members {
                if self.objects.get(number).is_some_and(|o| o.refs == 0)
                    && let Some(o) = self.objects.remove(number)
                {
                    self.ids.remove(&o.id);
                    stats.objects_dropped += 1;
                }
            }
            for (number, offset) in moved {
                if let Some(o) = self.objects.get_mut(&number) {
                    o.loc = Location::Segment { id: new, offset };
                }
            }
            self.segments.remove(&old);
            // Not followed by a directory fsync: if the unlink is lost, the
            // WAL no longer references the file and recovery deletes it.
            compio::fs::remove_file(segment_path(&self.dir, old)).await?;
            stats.segments_rewritten += 1;
            stats.bytes_reclaimed += file_len - live_bytes;
        }
        Ok(stats)
    }

    pub fn stats(&self) -> Stats {
        let mut s = self.stats;
        s.metadata_bytes = s.wal_bytes + s.topic_bytes - self.inline_bytes;
        s
    }

    /// What recovery changed when this store was opened.
    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// Live topic entries: `first_live..next_seq`.
    pub fn topic_range(&self, topic: &[u8]) -> Option<(u64, u64)> {
        self.topics
            .get(topic)
            .map(|t| (t.first_live, t.first_live + t.live.len() as u64))
    }

    pub fn topics(&self) -> impl Iterator<Item = &[u8]> {
        self.topics.keys().map(Vec::as_slice)
    }

    /// The object a live entry points at.
    pub fn entry(&self, topic: &[u8], seq: u64) -> Option<ObjectId> {
        let number = self.entry_number(topic, seq)?;
        self.objects.get(&number).map(|o| o.id)
    }

    /// Entry `seq` of `topic` as stored in its index file: `(object number,
    /// timestamp)`, released entries included. Reads one block (at most 64
    /// entries); `None` past the last entry.
    pub async fn index_entry(
        &self,
        topic: &[u8],
        seq: u64,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        self.usable()?;
        let Some(t) = self.topics.get(topic) else {
            return Ok(None);
        };
        let block = seq / INDEX_BLOCK;
        let Some(b) = usize::try_from(block).ok() else {
            return Ok(None);
        };
        let Some(&start) = t.block_starts.get(b) else {
            return Ok(None);
        };
        let end = t.block_starts.get(b + 1).copied().unwrap_or(t.index_len);
        let len = (end - start) as usize;
        let BufResult(res, buf) = t
            .file
            .read_exact_at(Vec::with_capacity(len).slice(..len), start)
            .await;
        res?;
        let decoded = codec::decode_index_block(&buf.into_inner(), block);
        Ok(decoded.entries.get((seq % INDEX_BLOCK) as usize).copied())
    }

    fn entry_number(&self, topic: &[u8], seq: u64) -> Option<u64> {
        self.live(topic, seq).map(|e| e.number)
    }

    fn live(&self, topic: &[u8], seq: u64) -> Option<Live> {
        let t = self.topics.get(topic)?;
        let idx = usize::try_from(seq.checked_sub(t.first_live)?).ok()?;
        t.live.get(idx).copied()
    }

    /// Entry `seq` of `topic` with its timestamp, key and headers; `None` if
    /// the entry was never published or was released. The payload's hash
    /// is verified. Key and headers are read from the WAL.
    pub async fn read_entry(&self, topic: &[u8], seq: u64) -> Result<Option<Entry>, StoreError> {
        self.usable()?;
        let Some(e) = self.live(topic, seq) else {
            return Ok(None);
        };
        let (_, payload) = self.read_object(e.number).await?;
        let (key, headers) = if e.attrs_len == 0 {
            (None, Vec::new())
        } else {
            let len = e.attrs_len as usize;
            let BufResult(res, buf) = self
                .wal
                .read_exact_at(Vec::with_capacity(len).slice(..len), e.attrs_at)
                .await;
            res?;
            codec::decode_attrs(&buf.into_inner()).map_err(|reason| StoreError::CorruptWal {
                offset: e.attrs_at,
                reason,
            })?
        };
        Ok(Some(Entry {
            timestamp: e.timestamp,
            key,
            headers,
            payload,
        }))
    }

    /// Counters of `topic`; `None` if it has no entry.
    pub fn topic_stats(&self, topic: &[u8]) -> Option<TopicStats> {
        self.topics.get(topic).map(|t| TopicStats {
            first_live: t.first_live,
            next_seq: t.first_live + t.live.len() as u64,
            index_bytes: t.index_len,
        })
    }

    /// The upper bound of the WAL frame `put_batch` checks against
    /// `Config::max_frame_bytes` before any I/O. Additive over messages:
    /// the bound of two batches together is the sum of their bounds minus
    /// the bound of an empty batch, so a caller can grow a batch message by
    /// message. A batch over the limit fails with `FrameTooLarge`.
    pub fn batch_frame_bytes(&self, batch: &[Message<'_>]) -> usize {
        let inline = self.config.write_mode.inline_threshold();
        FRAME_HEADER
            + batch
                .iter()
                .map(|m| {
                    let put = match inline {
                        Some(t) if m.payload.len() <= t => RECORD_PUT_INLINE_BASE + m.payload.len(),
                        _ => RECORD_PUT_OBJECT,
                    };
                    // A new topic adds its PutTopic (counted per message: an
                    // upper bound).
                    let topic = if self.topics.contains_key(m.topic) {
                        0
                    } else {
                        RECORD_PUT_TOPIC_BASE + m.topic.len()
                    };
                    put + RECORD_PUBLISH + codec::attrs_len(m.key, m.headers) + topic
                })
                .sum::<usize>()
    }

    pub fn max_frame_bytes(&self) -> u32 {
        self.config.max_frame_bytes
    }

    /// `None` if the object is not in the index (never stored, or reclaimed).
    pub fn refcount(&self, id: &ObjectId) -> Option<u64> {
        let number = self.ids.get(id)?;
        self.objects.get(number).map(|o| o.refs)
    }

    pub fn object_count(&self) -> usize {
        self.objects.len()
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    fn check_frame(&self, bytes: usize) -> Result<(), StoreError> {
        if bytes > self.config.max_frame_bytes as usize {
            return Err(StoreError::FrameTooLarge);
        }
        Ok(())
    }

    /// Marks the start of a WAL frame. A frame still in flight here means an
    /// earlier operation's future was dropped mid-way: the next frame would
    /// start before the previous one is durable, which recovery does not
    /// handle (only the last frame may be torn).
    fn begin_write(&mut self) {
        debug_assert!(
            !self.in_flight,
            "a WAL frame started before the previous one was durable"
        );
        if self.in_flight {
            self.poisoned = true;
        }
        self.in_flight = true;
    }

    fn usable(&self) -> Result<(), StoreError> {
        if self.poisoned || self.in_flight {
            return Err(StoreError::Poisoned);
        }
        Ok(())
    }

    fn poison_on_err<T>(&mut self, res: Result<T, StoreError>) -> Result<T, StoreError> {
        if res.is_err() {
            self.poisoned = true;
        }
        res
    }

    /// Reads and verifies object `number`; returns its id and bytes.
    async fn read_object(&self, number: u64) -> Result<(ObjectId, Vec<u8>), StoreError> {
        let object = self
            .objects
            .get(&number)
            .ok_or(StoreError::UnknownObjectNumber(number))?;
        let id = &object.id;
        let (file, offset) = match object.loc {
            Location::Segment {
                id: segment,
                offset,
            } => (
                self.segments
                    .get(&segment)
                    .ok_or(StoreError::MissingObject(*id))?,
                offset,
            ),
            Location::Wal { offset } => (&self.wal, offset),
        };
        let len = object.len as usize;
        // `slice` caps the read at `len` even if the allocation is larger.
        let BufResult(res, buf) = file
            .read_exact_at(Vec::with_capacity(len).slice(..len), offset)
            .await;
        match res {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(StoreError::MissingObject(*id));
            }
            res => res?,
        }
        let buf = buf.into_inner();
        if ObjectId::of(&buf) != *id {
            return Err(StoreError::HashMismatch(*id));
        }
        Ok((*id, buf))
    }

    /// Assigns `len` bytes in the active segment, sealing it or creating a new
    /// one as needed. Returns `(segment, offset)`.
    async fn reserve(&mut self, len: u64) -> Result<(u64, u64), StoreError> {
        if self
            .active
            .as_ref()
            .is_some_and(|a| a.len > 0 && a.len + len > self.config.segment_max_bytes)
        {
            self.flush_segment().await?;
            self.active = None;
        }
        if self.active.is_none() {
            let id = self.next_segment;
            self.next_segment += 1;
            let file = create_segment(&self.dir, id).await?;
            // The WAL will reference this file: its directory entry must be
            // durable before the WAL record is.
            sync_dir(&self.dir).await?;
            self.stats.fsyncs += 1;
            self.segments.insert(id, file.clone());
            self.active = Some(Active {
                id,
                file,
                len: 0,
                written: 0,
            });
        }
        let Some(active) = self.active.as_mut() else {
            unreachable!("set above")
        };
        let offset = active.len;
        active.len += len;
        Ok((active.id, offset))
    }

    /// Writes buffered object bytes to the active segment and syncs it.
    async fn flush_segment(&mut self) -> Result<(), StoreError> {
        let Some(active) = self.active.as_mut() else {
            return Ok(());
        };
        if self.seg_buf.is_empty() {
            return Ok(());
        }
        let n = self.seg_buf.len() as u64;
        let (res, mut buf) = write_all(
            &active.file,
            std::mem::take(&mut self.seg_buf),
            active.written,
        )
        .await;
        buf.clear();
        self.seg_buf = buf;
        res?;
        active.file.sync_data().await?;
        active.written += n;
        self.stats.fsyncs += 1;
        self.stats.segment_bytes += n;
        Ok(())
    }

    /// Completes the frame in `buf`, appends it and syncs the WAL. `buf`
    /// becomes the reusable buffer again. An empty frame is not written.
    async fn append_frame(
        &mut self,
        mut buf: Vec<u8>,
        frame: FrameBuilder,
        mode: FrameMode,
    ) -> Result<(), StoreError> {
        if frame.is_empty() {
            buf.clear();
            self.wal_buf = buf;
            return Ok(());
        }
        frame.finish(&mut buf, self.next_frame, mode)?;
        let n = buf.len() as u64;
        let (res, buf) = write_all(&self.wal, buf, self.wal_len).await;
        self.wal_buf = buf;
        res?;
        self.wal.sync_data().await?;
        self.wal_len += n;
        self.next_frame += 1;
        self.stats.fsyncs += 1;
        self.stats.wal_bytes += n;
        Ok(())
    }

    /// ParallelSync commit: the active segment's buffered bytes and the frame
    /// are written and fdatasync'ed as two concurrent chains (both syncs in
    /// flight at once on io_uring). Returns after both completed. A new
    /// segment's directory entry was already synced in `reserve`.
    async fn commit_parallel(
        &mut self,
        mut buf: Vec<u8>,
        frame: FrameBuilder,
    ) -> Result<(), StoreError> {
        if frame.is_empty() {
            buf.clear();
            self.wal_buf = buf;
            return Ok(());
        }
        frame.finish(&mut buf, self.next_frame, FrameMode::ParallelSync)?;
        let wal_n = buf.len() as u64;
        let (wal, wal_at) = (self.wal.clone(), self.wal_len);
        let seg_buf = std::mem::take(&mut self.seg_buf);
        let seg_n = seg_buf.len() as u64;
        let seg = self
            .active
            .as_ref()
            .filter(|_| seg_n > 0)
            .map(|a| (a.file.clone(), a.written));
        let wal_side = async {
            let (res, buf) = write_all(&wal, buf, wal_at).await;
            res?;
            wal.sync_data().await?;
            io::Result::Ok(buf)
        };
        let seg_side = async {
            let Some((file, at)) = &seg else {
                return io::Result::Ok(seg_buf);
            };
            let (res, buf) = write_all(file, seg_buf, *at).await;
            res?;
            file.sync_data().await?;
            Ok(buf)
        };
        let (wal_res, seg_res) = join2(wal_side, seg_side).await;
        self.wal_buf = wal_res?;
        let mut seg_buf = seg_res?;
        seg_buf.clear();
        self.seg_buf = seg_buf;
        self.wal_len += wal_n;
        self.next_frame += 1;
        self.stats.fsyncs += 1;
        self.stats.wal_bytes += wal_n;
        if let Some(active) = self.active.as_mut().filter(|_| seg.is_some()) {
            active.written += seg_n;
            self.stats.fsyncs += 1;
            self.stats.segment_bytes += seg_n;
        }
        Ok(())
    }

    /// Topic index files are derived from the WAL and repaired by recovery, so
    /// they are written after the WAL fsync and not synced themselves.
    async fn write_topic_entries(&mut self) -> Result<(), StoreError> {
        // Visits every topic per batch; with many topics, tracking the ones
        // the batch touched would bound this by the batch size.
        for topic in self.topics.values_mut() {
            if topic.pending.is_empty() {
                continue;
            }
            let n = topic.pending.len() as u64;
            let (res, mut buf) = write_all(
                &topic.file,
                std::mem::take(&mut topic.pending),
                topic.index_len,
            )
            .await;
            buf.clear();
            topic.pending = buf;
            res?;
            topic.index_len += n;
            self.stats.topic_bytes += n;
        }
        Ok(())
    }

    async fn topic_mut(&mut self, name: &[u8]) -> Result<&mut Topic, StoreError> {
        if !self.topics.contains_key(name) {
            // Recovery removed any index file of a topic the WAL does not know.
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(topic_path(&self.dir, name))
                .await?;
            let number = self.next_topic;
            self.next_topic += 1;
            self.topics.insert(
                name.to_vec(),
                Topic {
                    number,
                    file,
                    index: IndexWriter::default(),
                    index_len: 0,
                    block_starts: Vec::new(),
                    first_live: 0,
                    last_timestamp: 0,
                    live: VecDeque::new(),
                    pending: Vec::new(),
                },
            );
        }
        match self.topics.get_mut(name) {
            Some(topic) => Ok(topic),
            None => unreachable!("inserted above"),
        }
    }
}

/// State rebuilt from the WAL.
#[derive(Default)]
struct Replay<'a> {
    objects: HashMap<u64, Object>,
    ids: HashMap<ObjectId, u64>,
    /// One above the highest object number in the WAL: numbers of objects
    /// compaction dropped are not reused either.
    next_object: u64,
    topics: HashMap<&'a [u8], ReplayTopic>,
    /// Topic number → name, from PutTopic records; numbers are contiguous.
    topic_names: Vec<&'a [u8]>,
    topic_numbers: HashMap<&'a [u8], u64>,
    /// Segments the WAL references.
    segments: HashSet<u64>,
    /// Highest segment id seen in the WAL or on disk; ids are never reused.
    max_segment: Option<u64>,
}

#[derive(Default)]
struct ReplayTopic {
    number: u64,
    /// Every published entry, released ones included.
    entries: Vec<Live>,
    first_live: u64,
}

impl<'a> Replay<'a> {
    fn see(&mut self, segment: u64) {
        self.max_segment = self.max_segment.max(Some(segment));
    }

    fn topic_name(&self, number: u64) -> Result<&'a [u8], &'static str> {
        usize::try_from(number)
            .ok()
            .and_then(|i| self.topic_names.get(i).copied())
            .ok_or("topic number without a PutTopic")
    }

    fn add_object(&mut self, number: u64, object: Object) -> Result<(), &'static str> {
        if number < self.next_object {
            return Err("object number not above every earlier one");
        }
        if self.ids.insert(object.id, number).is_some() {
            return Err("new object already in the index");
        }
        self.next_object = number + 1;
        self.objects.insert(number, object);
        Ok(())
    }

    /// `body_at`: WAL offset of the frame body holding `rec`.
    fn apply(&mut self, rec: Record<'a>, body_at: u64) -> Result<(), &'static str> {
        match rec {
            Record::PutObject {
                id,
                number,
                segment,
                offset,
                len,
            } => {
                if segment == NO_SEGMENT {
                    return Err("PutObject into the reserved segment id");
                }
                let object = Object {
                    id,
                    loc: Location::Segment {
                        id: segment,
                        offset,
                    },
                    len,
                    refs: 0,
                };
                self.add_object(number, object)?;
                self.segments.insert(segment);
                self.see(segment);
            }
            Record::PutInline {
                id,
                number,
                data,
                data_at,
            } => {
                // Under a valid frame CRC a mismatch is a writer bug.
                if ObjectId::of(data) != id {
                    return Err("inline object does not match its id");
                }
                let object = Object {
                    id,
                    loc: Location::Wal {
                        offset: body_at + data_at as u64,
                    },
                    len: u32::try_from(data.len()).map_err(|_| "inline object too large")?,
                    refs: 0,
                };
                self.add_object(number, object)?;
            }
            Record::PutTopic { number, name } => {
                if number != self.topic_names.len() as u64 {
                    return Err("topic number out of sequence");
                }
                if self.topic_numbers.insert(name, number).is_some() {
                    return Err("topic name already has a number");
                }
                self.topic_names.push(name);
            }
            Record::Publish {
                topic,
                seq,
                number,
                timestamp,
                attrs,
                attrs_at,
            } => {
                let name = self.topic_name(topic)?;
                let t = self.topics.entry(name).or_default();
                t.number = topic;
                if seq != t.entries.len() as u64 {
                    return Err("Publish out of sequence");
                }
                let object = self
                    .objects
                    .get_mut(&number)
                    .ok_or("Publish references an object not in the index")?;
                object.refs += 1;
                t.entries.push(Live {
                    number,
                    timestamp,
                    attrs_at: body_at + attrs_at as u64,
                    attrs_len: u32::try_from(attrs.len()).map_err(|_| "attributes too long")?,
                });
            }
            Record::Release { topic, seq } => {
                let topic = self.topic_name(topic)?;
                let t = self
                    .topics
                    .get_mut(topic)
                    .ok_or("Release of an unknown topic")?;
                let &Live { number, .. } = usize::try_from(seq)
                    .ok()
                    .and_then(|i| t.entries.get(i))
                    .filter(|_| seq == t.first_live)
                    .ok_or("Release out of order")?;
                t.first_live += 1;
                let object = self
                    .objects
                    .get_mut(&number)
                    .filter(|o| o.refs > 0)
                    .ok_or("Release of an entry whose object has no references")?;
                object.refs -= 1;
            }
            Record::Compact { old, new, kept } => {
                if !self.segments.remove(&old) {
                    return Err("Compact of a segment the WAL does not reference");
                }
                if new == NO_SEGMENT {
                    if !kept.is_empty() {
                        return Err("Compact keeps objects without a new segment");
                    }
                } else if !self.segments.insert(new) {
                    return Err("Compact into a segment already in use");
                }
                self.see(old);
                if new != NO_SEGMENT {
                    self.see(new);
                }
                let kept: HashMap<ObjectId, u64> = kept.into_iter().collect();
                let mut matched = 0;
                let mut bad = false;
                let ids = &mut self.ids;
                self.objects.retain(|_, o| {
                    if !matches!(o.loc, Location::Segment { id, .. } if id == old) {
                        return true;
                    }
                    match (kept.get(&o.id), o.refs > 0) {
                        (Some(&offset), true) => {
                            o.loc = Location::Segment { id: new, offset };
                            matched += 1;
                            true
                        }
                        (None, false) => {
                            ids.remove(&o.id);
                            false
                        }
                        _ => {
                            bad = true;
                            true
                        }
                    }
                });
                if bad || matched != kept.len() {
                    return Err("Compact kept set differs from the live objects of the segment");
                }
            }
        }
        Ok(())
    }
}

/// Deletes segment files the WAL does not reference, opens the others and
/// checks that every indexed object lies within its segment file.
async fn open_segments(
    dir: &Path,
    replay: &mut Replay<'_>,
    report: &mut RecoveryReport,
) -> Result<HashMap<u64, File>, StoreError> {
    // Blocking directory listing: only at open, and compio 0.19 has no
    // async read_dir without the extra fs-dir feature.
    let mut on_disk = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some(id) = name.to_str().and_then(parse_segment_name) {
            on_disk.push(id);
        }
    }
    let mut removed = false;
    for &id in &on_disk {
        replay.see(id);
        if !replay.segments.contains(&id) {
            // Bytes no durable WAL record points at: a crash before the WAL
            // fsync of a batch or of a compaction, or after a compaction's
            // WAL record but before the old file was unlinked.
            compio::fs::remove_file(segment_path(dir, id)).await?;
            report
                .actions
                .push(RecoveryAction::OrphanSegmentRemoved { segment: id });
            removed = true;
        }
    }
    if removed {
        sync_dir(dir).await?;
    }

    let mut segments = HashMap::new();
    let mut lens = HashMap::new();
    for &id in &replay.segments {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_path(dir, id))
            .await
        {
            Ok(file) => {
                lens.insert(id, file.metadata().await?.len());
                segments.insert(id, file);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    for o in replay.objects.values() {
        let id = &o.id;
        // Inline objects were read from the WAL frames themselves.
        let Location::Segment {
            id: segment,
            offset,
        } = o.loc
        else {
            continue;
        };
        let end = offset.checked_add(u64::from(o.len));
        if lens
            .get(&segment)
            .is_none_or(|&len| end.is_none_or(|end| end > len))
        {
            return Err(StoreError::MissingObject(*id));
        }
    }
    Ok(segments)
}

/// Opens every topic index, checks it against the WAL and repairs it:
/// entries missing or torn (the file is not synced) are rewritten from the
/// WAL; entries beyond the WAL (their records were in a truncated tail) are
/// cut. A checksummed entry that disagrees with the WAL is an error.
async fn open_topics(
    dir: &Path,
    replay: &Replay<'_>,
    report: &mut RecoveryReport,
) -> Result<HashMap<Vec<u8>, Topic>, StoreError> {
    for entry in std::fs::read_dir(dir.join(TOPICS_DIR))? {
        let entry = entry?;
        let known = entry
            .file_name()
            .to_str()
            .and_then(|n| n.strip_suffix(".idx"))
            .and_then(unhex)
            .is_some_and(|name| replay.topics.contains_key(name.as_slice()));
        if !known {
            // A topic whose first Publish never became durable.
            compio::fs::remove_file(entry.path()).await?;
            report
                .actions
                .push(RecoveryAction::UnknownTopicIndexRemoved { path: entry.path() });
        }
    }

    let mut topics = HashMap::new();
    for (&name, t) in &replay.topics {
        let path = topic_path(dir, name);
        let bytes = match compio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        // The encoding is deterministic: re-encode the index from the WAL
        // and compare bytes. The file must equal a prefix of it.
        let mut index = IndexWriter::default();
        let mut expected = Vec::new();
        let mut block_starts = Vec::new();
        for &Live {
            number, timestamp, ..
        } in &t.entries
        {
            let at = expected.len();
            if index.push(&mut expected, number, timestamp) {
                block_starts.push(at as u64);
            }
        }
        let differ = bytes
            .iter()
            .zip(&expected)
            .position(|(a, b)| a != b)
            .unwrap_or(bytes.len().min(expected.len()));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;
        if differ == expected.len() && bytes.len() > expected.len() {
            // Entries beyond the WAL (their records were in a truncated tail).
            report.actions.push(RecoveryAction::TopicIndexRepaired {
                path: path.clone(),
                entries_kept: t.entries.len() as u64,
                bytes_truncated: (bytes.len() - expected.len()) as u64,
                entries_rewritten: 0,
            });
            file.set_len(expected.len() as u64).await?;
        } else if differ < bytes.len() || differ < expected.len() {
            // Block holding the first difference, and its start.
            let block = block_starts
                .iter()
                .rposition(|&start| start as usize <= differ)
                .unwrap_or(0);
            let start = block_starts.get(block).map_or(0, |&s| s as usize);
            let want = codec::decode_index_block(&expected[start..], block as u64);
            let have = bytes
                .get(start..)
                .map(|b| codec::decode_index_block(b, block as u64));
            // A complete block with a valid CRC that disagrees with the WAL
            // was written that way: corruption. Anything else is an
            // unsynced or torn tail and is rewritten.
            if want.sealed
                && let Some(have) = have.filter(|h| h.sealed)
            {
                let at = want
                    .entries
                    .iter()
                    .zip(&have.entries)
                    .position(|(a, b)| a != b)
                    .unwrap_or(0);
                return Err(StoreError::TopicMismatch {
                    path,
                    seq: block as u64 * INDEX_BLOCK + at as u64,
                });
            }
            // Keep whole entries of this block that lie before the difference.
            let kept = want
                .ends
                .iter()
                .take_while(|&&end| start + end <= differ)
                .count();
            let keep = start + kept.checked_sub(1).map_or(0, |k| want.ends[k]);
            let entries_kept = block as u64 * INDEX_BLOCK + kept as u64;
            report.actions.push(RecoveryAction::TopicIndexRepaired {
                path: path.clone(),
                entries_kept,
                bytes_truncated: bytes.len().saturating_sub(keep) as u64,
                entries_rewritten: t.entries.len() as u64 - entries_kept,
            });
            if bytes.len() != keep {
                file.set_len(keep as u64).await?;
            }
            if keep < expected.len() {
                write_all(&file, expected[keep..].to_vec(), keep as u64)
                    .await
                    .0?;
            }
        }
        let first_live = usize::try_from(t.first_live).unwrap_or(usize::MAX);
        topics.insert(
            name.to_vec(),
            Topic {
                number: t.number,
                file,
                index,
                index_len: expected.len() as u64,
                block_starts,
                first_live: t.first_live,
                last_timestamp: t.entries.last().map_or(0, |e| e.timestamp),
                live: t.entries.iter().skip(first_live).copied().collect(),
                pending: Vec::new(),
            },
        );
    }
    Ok(topics)
}

/// Refcounts from WAL replay (Publish +1, Release -1) must equal the number
/// of live topic index entries pointing at each object.
fn check_refcounts<'a>(
    objects: &HashMap<u64, Object>,
    live: impl Iterator<Item = &'a u64>,
) -> Result<(), StoreError> {
    let mut counted: HashMap<u64, u64> = HashMap::new();
    for number in live {
        *counted.entry(*number).or_default() += 1;
    }
    for (number, o) in objects {
        let topics = counted.remove(number).unwrap_or(0);
        if topics != o.refs {
            return Err(StoreError::RefcountMismatch {
                id: o.id,
                wal: o.refs,
                topics,
            });
        }
    }
    match counted.into_keys().next() {
        Some(number) => Err(StoreError::UnknownObjectNumber(number)),
        None => Ok(()),
    }
}

/// Opens `wal.log`, creating it with its file header if missing. A new WAL is
/// written to `wal.log.tmp`, synced and renamed, so `wal.log` never exists
/// without a complete header.
async fn open_wal(dir: &Path, report: &mut RecoveryReport) -> Result<File, StoreError> {
    let path = dir.join(WAL_FILE);
    let tmp = dir.join(WAL_TMP);
    match compio::fs::remove_file(&tmp).await {
        Ok(()) => report.actions.push(RecoveryAction::WalTmpRemoved),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    if !path.exists() {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .await?;
        write_all(&file, codec::wal_file_header().to_vec(), 0)
            .await
            .0?;
        file.sync_all().await?;
        compio::fs::rename(&tmp, &path).await?;
    }
    // Makes the topics directory and a new WAL file durable.
    sync_dir(dir).await?;
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .await?)
}

/// Polls both futures until both are done, so their I/O is submitted
/// together. Two futures only: no dependency for a general join.
async fn join2<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let (mut a, mut b) = (pin!(a), pin!(b));
    let (mut ra, mut rb) = (None, None);
    std::future::poll_fn(|cx| {
        if ra.is_none()
            && let Poll::Ready(v) = a.as_mut().poll(cx)
        {
            ra = Some(v);
        }
        if rb.is_none()
            && let Poll::Ready(v) = b.as_mut().poll(cx)
        {
            rb = Some(v);
        }
        if ra.is_some() && rb.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    match (ra, rb) {
        (Some(a), Some(b)) => (a, b),
        _ => unreachable!("poll_fn returned Ready only with both results"),
    }
}

/// The first segment object the records introduce that is not fully in its
/// segment file or whose bytes do not match its BLAKE3 id. Used on the last
/// WAL frame only; earlier frames get a presence and bounds check
/// (`open_segments`), their content is verified on read.
async fn first_bad_object(
    dir: &Path,
    records: &[Record<'_>],
) -> Result<Option<(ObjectId, DropReason)>, StoreError> {
    for rec in records {
        let Record::PutObject {
            id,
            segment,
            offset,
            len,
            ..
        } = rec
        else {
            continue;
        };
        let file = match File::open(segment_path(dir, *segment)).await {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Ok(Some((*id, DropReason::ObjectMissing)));
            }
            Err(e) => return Err(e.into()),
        };
        let len = *len as usize;
        let BufResult(res, buf) = file
            .read_exact_at(Vec::with_capacity(len).slice(..len), *offset)
            .await;
        match res {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(Some((*id, DropReason::ObjectMissing)));
            }
            res => res?,
        }
        if ObjectId::of(&buf.into_inner()) != *id {
            return Ok(Some((*id, DropReason::ObjectHashMismatch)));
        }
    }
    Ok(None)
}

async fn write_all(file: &File, buf: Vec<u8>, pos: u64) -> (io::Result<()>, Vec<u8>) {
    let mut file = file;
    let BufResult(res, buf) = file.write_all_at(buf, pos).await;
    (res, buf)
}

async fn create_segment(dir: &Path, id: u64) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(segment_path(dir, id))
        .await
}

/// Makes created, renamed or removed directory entries durable.
async fn sync_dir(dir: &Path) -> io::Result<()> {
    OpenOptions::new()
        .read(true)
        .open(dir)
        .await?
        .sync_all()
        .await
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("seg-{id:016x}.dat"))
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("seg-")?.strip_suffix(".dat")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

fn topic_path(dir: &Path, topic: &[u8]) -> PathBuf {
    let mut name: String = topic.iter().map(|b| format!("{b:02x}")).collect();
    name.push_str(".idx");
    dir.join(TOPICS_DIR).join(name)
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(id: ObjectId, refs: u64) -> Object {
        Object {
            id,
            loc: Location::Segment { id: 0, offset: 0 },
            len: 1,
            refs,
        }
    }

    #[test]
    fn refcount_check_fails_loudly_on_mismatch() {
        let (a, b) = (ObjectId::of(b"a"), ObjectId::of(b"b"));
        let objects = HashMap::from([(0, object(a, 2)), (1, object(b, 0))]);
        assert!(check_refcounts(&objects, [0, 0].iter()).is_ok());
        assert!(matches!(
            check_refcounts(&objects, [0].iter()),
            Err(StoreError::RefcountMismatch {
                wal: 2,
                topics: 1,
                ..
            })
        ));
        assert!(matches!(
            check_refcounts(&objects, [0, 0, 1].iter()),
            Err(StoreError::RefcountMismatch {
                id,
                wal: 0,
                topics: 1,
            }) if id == b
        ));
        assert!(matches!(
            check_refcounts(&objects, [0, 0, 7].iter()),
            Err(StoreError::UnknownObjectNumber(7))
        ));
    }

    fn put(id: ObjectId, number: u64, offset: u64) -> Record<'static> {
        Record::PutObject {
            id,
            number,
            segment: 0,
            offset,
            len: 1,
        }
    }

    fn publish(seq: u64, number: u64) -> Record<'static> {
        Record::Publish {
            topic: 0,
            seq,
            number,
            timestamp: 0,
            attrs: &[],
            attrs_at: 0,
        }
    }

    /// A replay with topic 0 = "t" defined.
    fn replay_with_topic() -> Replay<'static> {
        let mut r = Replay::default();
        r.apply(
            Record::PutTopic {
                number: 0,
                name: b"t",
            },
            0,
        )
        .unwrap();
        r
    }

    #[test]
    fn replay_checks_the_topic_table() {
        let mut r = Replay::default();
        assert_eq!(
            r.apply(publish(0, 0), 0),
            Err("topic number without a PutTopic")
        );
        let put = |number, name| Record::PutTopic { number, name };
        assert_eq!(
            r.apply(put(1, &b"t"[..]), 0),
            Err("topic number out of sequence")
        );
        r.apply(put(0, b"t"), 0).unwrap();
        assert_eq!(
            r.apply(put(1, b"t"), 0),
            Err("topic name already has a number")
        );
        r.apply(put(1, b"u"), 0).unwrap();
        assert_eq!(r.topic_names, vec![&b"t"[..], &b"u"[..]]);
    }

    #[test]
    fn replay_rejects_inconsistent_logs() {
        let id = ObjectId::of(b"x");

        let mut r = replay_with_topic();
        assert_eq!(
            r.apply(publish(0, 0), 0),
            Err("Publish references an object not in the index")
        );

        let mut r = replay_with_topic();
        r.apply(put(id, 0, 0), 0).unwrap();
        assert_eq!(
            r.apply(put(id, 1, 1), 0),
            Err("new object already in the index"),
            "same id twice"
        );
        assert_eq!(
            r.apply(put(ObjectId::of(b"y"), 0, 1), 0),
            Err("object number not above every earlier one"),
            "number reused"
        );
        r.apply(publish(0, 0), 0).unwrap();
        assert_eq!(
            r.apply(publish(0, 0), 0),
            Err("Publish out of sequence"),
            "duplicate seq"
        );
        assert_eq!(
            r.apply(publish(2, 0), 0),
            Err("Publish out of sequence"),
            "gap"
        );
        let release = |seq| Record::Release { topic: 0, seq };
        assert_eq!(r.apply(release(1), 0), Err("Release out of order"));
        r.apply(release(0), 0).unwrap();
        // The object is dead now: a Compact that keeps it is inconsistent.
        let keep = Record::Compact {
            old: 0,
            new: 1,
            kept: vec![(id, 0)],
        };
        assert!(r.apply(keep, 0).is_err());
    }

    #[test]
    fn replay_applies_compaction_and_keeps_numbers() {
        let (a, b) = (ObjectId::of(b"a"), ObjectId::of(b"b"));
        let mut r = replay_with_topic();
        r.apply(put(a, 0, 0), 0).unwrap();
        r.apply(put(b, 1, 1), 0).unwrap();
        r.apply(publish(0, 0), 0).unwrap();
        r.apply(
            Record::Compact {
                old: 0,
                new: 1,
                kept: vec![(a, 0)],
            },
            0,
        )
        .unwrap();
        assert!(!r.objects.contains_key(&1), "dead object dropped");
        assert!(!r.ids.contains_key(&b), "and its id");
        let o = &r.objects[&0];
        assert_eq!(
            (o.id, o.loc, o.refs),
            (a, Location::Segment { id: 1, offset: 0 }, 1)
        );
        assert_eq!(r.segments, HashSet::from([1]));
        assert_eq!(r.max_segment, Some(1));
        // Number 1 belonged to the dropped object and is not reused.
        assert_eq!(r.next_object, 2);
    }

    #[test]
    fn segment_and_topic_names_round_trip() {
        let dir = Path::new("/d");
        let p = segment_path(dir, 0xab);
        let name = p.file_name().unwrap().to_str().unwrap();
        assert_eq!(parse_segment_name(name), Some(0xab));
        assert_eq!(parse_segment_name("seg-1.dat"), None);
        let t = topic_path(dir, &[0, 0xff, b'a']);
        assert_eq!(t, Path::new("/d/topics/00ff61.idx"));
        assert_eq!(unhex("00ff61"), Some(vec![0, 0xff, b'a']));
        assert_eq!(unhex("0"), None);
        assert_eq!(unhex("zz"), None);
    }
}
