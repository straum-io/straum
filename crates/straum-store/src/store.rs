use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Instant;

use compio::BufResult;
use compio::buf::{IntoInner, IoBuf};
use compio::fs::{File, OpenOptions};
use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};

use crate::StoreError;
use crate::block::{self, Codecs};
use crate::codec::{
    self, FRAME_HEADER, FrameBuilder, FrameMode, INDEX_BLOCK, INDEX_HEADER, INDEX_RECORD_BLOCK,
    INDEX_RECORD_SEEK_PAGE, IndexLoc, IndexWriter, MAX_TOPIC, MAX_VARINT, NO_DICT, NO_SEGMENT,
    ObjectId, RECORD_COMPACT_BASE, RECORD_COMPACT_KEPT, RECORD_COMPACT_REWRITTEN, RECORD_PUBLISH,
    RECORD_PUT_BLOCK, RECORD_PUT_DICT_BASE, RECORD_PUT_INLINE_BLOCK_BASE, RECORD_PUT_OBJECT,
    RECORD_PUT_TOPIC_BASE, RECORD_RELEASE, Record, Rewritten, WAL_FILE_HEADER,
};

/// The single WAL file of formats before 13: refused, never read.
const LEGACY_WAL_FILE: &str = "wal.log";
/// Bounds of `Config::seek_page_entries`.
const SEEK_PAGE_MIN: usize = 2;
const SEEK_PAGE_MAX: usize = 4096;

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
    /// Experimental. A batch's compressed block of at most `threshold`
    /// bytes is stored in the WAL frame itself; a larger one goes to a
    /// segment, committed as in `Sequential`. A batch with an inline block
    /// only needs one fdatasync (the WAL). Inline blocks are never
    /// reclaimed: that needs a WAL checkpoint.
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
    /// Upper bound on the memory of the decompressed blocks kept for reads
    /// (raw bytes, object offsets and a fixed 160 bytes per block for the
    /// cache's own entries). The least recently used blocks are evicted
    /// once it would be exceeded; a block larger than this is not kept, so
    /// 0 keeps none. The only limit: the number of blocks follows from it.
    pub block_cache_bytes: usize,
    /// An index segment holding at least this many bytes is sealed before
    /// the next batch, and the next index record starts a new one; a
    /// segment can exceed it by one batch's index records.
    pub index_segment_max_bytes: u64,
    /// Index segments kept open for reads, least recently used closed
    /// first; the segment being written is open as well. 0 is taken as 1.
    pub index_open_files: usize,
    /// Data segments and earlier WAL files kept open for reads (blocks,
    /// keys and headers), least recently used closed first; the segment
    /// and the WAL file being written are open as well. 0 is taken as 1.
    pub data_open_files: usize,
    /// Locations per seek page (default 256): a topic's index block is
    /// found through at most one seek page read up to this number squared
    /// blocks, two up to its cube, and so on. Clamped to 2..=4096. Changing
    /// it between opens rewrites every seek page at open (reported).
    pub seek_page_entries: usize,
    /// Raw bytes of new objects stored without a dictionary after which the
    /// store trains its dictionary, once, on the objects of those blocks.
    /// 0: never. Recorded in a new WAL's file header; on reopen the WAL's
    /// value is used (`ConfigOverride::DictTrainBytes`).
    pub dict_train_bytes: u64,
    /// Upper bound on a block's raw size: the new objects of a batch, and
    /// the live objects compaction moves, are split in order into blocks
    /// of at most this many raw bytes, so reading one object decompresses
    /// at most this much. An object larger than the limit is a block of
    /// its own; objects are never split. 0 is taken as 1 (every object
    /// alone). Default 16 KiB: random and `dedup_scan` reads 2–3× faster
    /// than at 64 KiB, for 10–17 % more disk bytes on 1–10 KiB log lines
    /// (D-191). 64 KiB stays a setting for topics where space matters more
    /// than read latency.
    pub max_block_raw_bytes: u64,
    /// A WAL file holding at least this many bytes is closed before the
    /// next frame, which starts a new file (`wal-<frame seq>.log`), so a
    /// checkpoint can later reclaim WAL space by deleting whole files. A
    /// file can exceed it by one frame. A file always holds at least one
    /// frame before the next starts.
    pub wal_file_max_bytes: u64,
    /// The operator's rebuild of the derived topic index (D-229 A): open
    /// deletes every index segment (`IndexSegmentDropped`) and re-derives
    /// every topic's index blocks and seek pages from the WAL
    /// (`TopicIndexRepaired`). The way out of a `TopicMismatch` at open
    /// caused by damaged index segments. Every open with it set rebuilds;
    /// set it for one open.
    pub rebuild_index: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            segment_max_bytes: 64 << 20,
            max_frame_bytes: 64 << 20,
            write_mode: WriteMode::Sequential,
            block_cache_bytes: 64 << 20,
            index_segment_max_bytes: 64 << 20,
            index_open_files: 64,
            data_open_files: 256,
            seek_page_entries: 256,
            dict_train_bytes: 1 << 20,
            max_block_raw_bytes: 16 << 10,
            wal_file_max_bytes: 64 << 20,
            rebuild_index: false,
        }
    }
}

/// Splits objects, given their raw block entry lengths in order, into
/// consecutive ranges of at most `cap` bytes each; an entry larger than
/// `cap` is a range of its own. Greedy: a range is closed when the next
/// entry would not fit, so two consecutive ranges together exceed `cap`
/// and there are at most `2 * total / cap + 1` of them.
fn block_ranges(lens: impl IntoIterator<Item = usize>, cap: u64) -> Vec<Range<usize>> {
    let cap = cap.max(1) as usize;
    let mut ranges = Vec::new();
    let (mut start, mut size, mut i) = (0, 0usize, 0);
    for len in lens {
        if i > start && size + len > cap {
            ranges.push(start..i);
            (start, size) = (i, 0);
        }
        size += len;
        i += 1;
    }
    if i > start {
        ranges.push(start..i);
    }
    ranges
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
    /// Bytes of the topic's index: its records in the index segments (full
    /// blocks and seek pages) and its encoded last, incomplete block.
    pub index_bytes: u64,
}

/// When the last batch's syncs ran, in nanoseconds from the start of its
/// commit (`Store::last_commit_timing`): for diagnosing write modes, not
/// for decisions inside the store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitTiming {
    /// The segment `fdatasync`s of the batch (from the first start to the
    /// last end); `None` if the batch wrote no segment bytes.
    pub segment_sync: Option<(u64, u64)>,
    /// The WAL `fdatasync`; `None` if the batch wrote no frame.
    pub wal_sync: Option<(u64, u64)>,
}

impl CommitTiming {
    /// Both syncs ran and their intervals intersect.
    pub fn overlapped(&self) -> bool {
        matches!(
            (self.segment_sync, self.wal_sync),
            (Some((a0, a1)), Some((b0, b1))) if a0 < b1 && b0 < a1
        )
    }
}

/// Widens `within` to cover `start..end`, as nanoseconds since `from`.
fn widen(within: &mut Option<(u64, u64)>, from: Instant, start: Instant, end: Instant) {
    let ns = |t: Instant| t.saturating_duration_since(from).as_nanos() as u64;
    let (a, b) = (ns(start), ns(end));
    *within = Some(within.map_or((a, b), |(x, y)| (x.min(a), y.max(b))));
}

/// Counters since `open`, in memory only: they start at zero on every open
/// and are not persisted. Recovery work is not counted. Message counters
/// cover acknowledged batches only.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Stats {
    /// `fdatasync`/`fsync` calls on files and directories.
    pub fsyncs: u64,
    /// Bytes written to segments: compressed blocks.
    pub segment_bytes: u64,
    /// All WAL bytes, inline blocks included.
    pub wal_bytes: u64,
    pub topic_bytes: u64,
    pub messages_accepted: u64,
    /// Messages whose object already existed (also earlier in the same
    /// batch): a reference was added, no bytes written.
    pub messages_deduplicated: u64,
    pub payload_bytes_received: u64,
    /// Payload bytes stored as new objects, before compression.
    pub payload_bytes_new: u64,
    /// Payload bytes not written because the object already existed.
    pub payload_bytes_saved: u64,
    /// WAL and topic index bytes that are not object bytes: `wal_bytes +
    /// topic_bytes` minus the inline blocks in the WAL.
    pub metadata_bytes: u64,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct CompactStats {
    pub segments_rewritten: u64,
    pub objects_dropped: u64,
    pub bytes_reclaimed: u64,
}

/// Where a block's bytes live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Location {
    Segment {
        id: u64,
        offset: u64,
    },
    /// Inside a WAL frame (an inline block).
    Wal(WalPos),
}

/// A byte position in the WAL: the file (named by the sequence number of
/// its first frame) and the offset in it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct WalPos {
    file: u64,
    offset: u64,
}

/// A compressed run of objects: the new objects of one batch, or the live
/// objects compaction moved out of partly dead blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Block {
    loc: Location,
    /// Stored (compressed) bytes.
    len: u64,
    raw_len: u64,
    dict: u64,
    /// CRC32C of the raw block.
    crc: u32,
    /// Objects the block holds, dead ones included.
    objects: u64,
}

/// A decompressed block and the byte range of each of its objects.
struct Decoded {
    raw: Vec<u8>,
    spans: Vec<Range<usize>>,
}

impl Decoded {
    /// Memory held, as counted against `Config::block_cache_bytes`: the
    /// heap bytes plus `CACHE_ENTRY_OVERHEAD`.
    fn bytes(&self) -> usize {
        self.raw.capacity()
            + self.spans.capacity() * size_of::<Range<usize>>()
            + CACHE_ENTRY_OVERHEAD
    }
}

/// Bytes counted per cached block beyond its heap buffers: the `Decoded`
/// and its `Rc` header, the map entries and their share of the tables,
/// rounded up. Counting them keeps the cache within its byte limit even
/// when blocks are tiny.
const CACHE_ENTRY_OVERHEAD: usize = 160;

/// Decompressed blocks kept for reads within `Config::block_cache_bytes`,
/// least recently used evicted first. A map by block number and an ordered
/// map by last use: a byte limit of 64 MiB holds about 4 000 blocks of
/// 16 KiB, too many for a linear scan per read.
struct BlockCache {
    /// Block number → (block, last use).
    entries: HashMap<u64, (Rc<Decoded>, u64)>,
    /// Last use → block number; the first entry is the least recently used.
    order: BTreeMap<u64, u64>,
    tick: u64,
    bytes: usize,
    max_bytes: usize,
}

impl BlockCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: BTreeMap::new(),
            tick: 0,
            bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, number: u64) -> Option<Rc<Decoded>> {
        let (d, used) = self.entries.get_mut(&number)?;
        self.order.remove(used);
        self.tick += 1;
        *used = self.tick;
        self.order.insert(self.tick, number);
        Some(Rc::clone(d))
    }

    fn insert(&mut self, number: u64, d: Rc<Decoded>) {
        self.remove(number);
        let size = d.bytes();
        if size > self.max_bytes {
            return;
        }
        while self.bytes + size > self.max_bytes {
            let Some((_, old)) = self.order.pop_first() else {
                break;
            };
            if let Some((old, _)) = self.entries.remove(&old) {
                self.bytes -= old.bytes();
            }
        }
        self.bytes += size;
        self.tick += 1;
        self.order.insert(self.tick, number);
        self.entries.insert(number, (d, self.tick));
    }

    fn remove(&mut self, number: u64) {
        if let Some((old, used)) = self.entries.remove(&number) {
            self.order.remove(&used);
            self.bytes -= old.bytes();
        }
    }
}

/// Every change recovery made to the store directory during `Store::open`,
/// and every configuration value the WAL overrode.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub actions: Vec<RecoveryAction>,
    /// Not changes to the directory: `is_empty` ignores them.
    pub config_overrides: Vec<ConfigOverride>,
}

impl RecoveryReport {
    /// Recovery changed nothing in the directory.
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}

/// A `Config` value replaced at open by the one the WAL recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigOverride {
    /// The dictionary training threshold is fixed when the WAL is created
    /// (its file header), so a reopen cannot move a trigger that has not
    /// fired. `configured` was ignored; `stored` is used.
    DictTrainBytes { configured: u64, stored: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryAction {
    /// A leftover `wal-<seq>.log.tmp` from an interrupted WAL file
    /// creation was deleted.
    WalTmpRemoved,
    /// A torn last frame (short or failing its CRC) was cut off.
    TornTailTruncated {
        /// The sequence number the torn frame would have had.
        frame_seq: u64,
        /// The last WAL file, by its first frame's sequence number.
        file: u64,
        offset: u64,
        bytes_removed: u64,
    },
    /// A complete last frame was dropped: written in ParallelSync, its object
    /// bytes were not all durable, so it was never acknowledged.
    FrameDropped {
        frame_seq: u64,
        /// The last WAL file, by its first frame's sequence number.
        file: u64,
        offset: u64,
        mode: FrameMode,
        object: ObjectId,
        reason: DropReason,
        bytes_removed: u64,
    },
    /// A segment file no WAL record references was deleted.
    OrphanSegmentRemoved { segment: u64 },
    /// An index segment without an intact header was deleted. With
    /// `bytes` at most the header's 24, its creation stopped before the
    /// header sync and it held no record; with more, its header was damaged
    /// and the records it held are re-derived from the WAL
    /// (`TopicIndexRepaired`).
    IndexSegmentRemoved { segment: u64, bytes: u64 },
    /// Index records of WAL frames that recovery cut (or that never became
    /// durable) were found. Index segment `segment` was created so that they
    /// stay recognised as stale once new frames reuse those frame numbers.
    /// The records themselves are not changed.
    StaleIndexRecordsFenced { records: u64, segment: u64 },
    /// An index segment was deleted because `Config::rebuild_index` asked
    /// for the index to be rebuilt from the WAL; `bytes` its length. The
    /// rebuilt blocks and pages follow as `TopicIndexRepaired`.
    IndexSegmentDropped { segment: u64, bytes: u64 },
    /// Full index blocks and seek pages of a topic that no index segment
    /// held intact were re-encoded from the WAL and appended.
    TopicIndexRepaired {
        topic: Vec<u8>,
        blocks_rewritten: u64,
        pages_rewritten: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropReason {
    /// The block holding the object is missing or short.
    ObjectMissing,
    /// The object's bytes do not match its BLAKE3 id.
    ObjectHashMismatch,
    /// The block does not decompress, or fails its length or CRC check.
    BlockCorrupted,
}

struct Object {
    /// Full BLAKE3 id; WAL Publish records and topic indexes use the
    /// object number instead.
    id: ObjectId,
    block: u64,
    /// Position in the block.
    index: u64,
    /// Live topic entries pointing at this object.
    refs: u64,
}

/// A topic index block's number and its `(object number, timestamp)`
/// entries.
type CachedBlock = (u64, Vec<(u64, u64)>);

struct Topic {
    /// Topic number from the topic's PutTopic record.
    number: u64,
    /// Encoder state after the last entry (released ones included): the
    /// next seq and the last timestamp.
    index: IndexWriter,
    /// The encoded entries of the last, incomplete index block (at most
    /// 63): it has no index record yet, so its entries are read from here.
    tail: Vec<u8>,
    /// Seek pages not yet written. `spine[0]` holds the locations of the
    /// full blocks after the last level-0 seek page; `spine[l]` those of the
    /// level-(l − 1) pages after the last level-l page. Full blocks are
    /// covered oldest first by the top level's entries, newest last by
    /// `spine[0]`: a lookup reads `l` seek pages for a block under
    /// `spine[l]`. Each level holds fewer than `seek_page_entries`.
    spine: Vec<Vec<IndexLoc>>,
    first_live: u64,
    /// The last full block read.
    read_cache: RefCell<Option<CachedBlock>>,
    /// Bytes of the topic's index records (blocks and seek pages).
    record_bytes: u64,
    /// Where the key and headers of each live entry that has them lie in
    /// the WAL: `(seq, offset, length)`, in seq order. Entries without
    /// either are not listed.
    attrs: VecDeque<AttrsAt>,
}

/// Where an entry's key and headers lie: a Publish record's tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AttrsAt {
    seq: u64,
    at: WalPos,
    len: u32,
}

struct Active {
    id: u64,
    file: File,
    /// Bytes assigned to objects, buffered ones included.
    len: u64,
    /// Bytes on disk.
    written: u64,
}

/// One shard: a WAL, data segments and index segments in one directory.
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
    /// The WAL file being appended to, its first frame's sequence number
    /// and its length.
    wal: File,
    wal_id: u64,
    wal_len: u64,
    /// Data segments and earlier WAL files open for reads.
    files: OpenFiles,
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
    /// Block number → block. Numbers are never reused.
    blocks: HashMap<u64, Block>,
    next_block: u64,
    codecs: RefCell<Codecs>,
    cache: RefCell<BlockCache>,
    /// Raw bytes of new objects stored in blocks without a dictionary: the
    /// training trigger (`Config::dict_train_bytes`).
    dictless_raw: u64,
    /// Length of the dictionary once training ran; `Some(0)` if it built
    /// none. Training runs once.
    dictionary: Option<usize>,
    next_topic: u64,
    /// Every segment the WAL references (and that exists).
    segments: HashSet<u64>,
    active: Option<Active>,
    next_segment: u64,
    topics: HashMap<Vec<u8>, Topic>,
    index: IndexSegments,
    wal_buf: Vec<u8>,
    seg_buf: Vec<u8>,
    stats: Stats,
    /// Inline block bytes written to the WAL since open (InlineSmall).
    inline_bytes: u64,
    /// Start of the current batch's commit, and its syncs so far.
    commit_start: Instant,
    timing: CommitTiming,
    recovery: RecoveryReport,
    poisoned: bool,
}

impl Store {
    /// Opens or creates the store in `dir`, recovering from the WAL.
    ///
    /// A WAL without this version's file header is `UnsupportedFormat`. A
    /// torn last frame is truncated; a bad frame followed by a valid one is
    /// `WalCorrupted` and nothing is modified. If the last complete frame
    /// introduces a block that is missing, does not decode, or holds an
    /// object that fails its BLAKE3, the frame is dropped only when it was
    /// written in ParallelSync and nothing follows it (it was never
    /// acknowledged); otherwise that is corruption (`MissingObject`,
    /// `CorruptBlock`, `HashMismatch`) and no file is changed. Blocks of
    /// earlier frames are checked for presence and bounds only; their content
    /// is verified on read. Recovery then deletes segment files the WAL does
    /// not reference, checks the index segments against the WAL, appends
    /// the index blocks and seek pages they lack, and recomputes refcounts.
    /// Every change is listed in `recovery_report()`.
    pub async fn open(dir: impl AsRef<Path>, mut config: Config) -> Result<Self, StoreError> {
        let dir = dir.as_ref().to_path_buf();
        let mut report = RecoveryReport::default();
        compio::fs::create_dir_all(&dir).await?;
        let files = read_wal_files(&dir, &mut report, config.dict_train_bytes).await?;
        let stored = files
            .first()
            .and_then(|(_, bytes)| codec::decode_wal_header(bytes))
            .ok_or(StoreError::UnsupportedFormat)?;
        for (id, bytes) in &files[1..] {
            match codec::decode_wal_header(bytes) {
                None => return Err(StoreError::UnsupportedFormat),
                Some(v) if v != stored => {
                    return Err(StoreError::CorruptWal {
                        file: *id,
                        offset: 0,
                        reason: "WAL files disagree on dict_train_bytes",
                    });
                }
                Some(_) => {}
            }
        }
        if stored != config.dict_train_bytes {
            report
                .config_overrides
                .push(ConfigOverride::DictTrainBytes {
                    configured: config.dict_train_bytes,
                    stored,
                });
            config.dict_train_bytes = stored;
        }

        // Only the last frame can be torn: a frame is written only after the
        // previous one is durable, and a WAL file is created only after the
        // last frame of the previous file is. A bad frame with a valid frame
        // after it, or in a file that another file follows, is mid-log
        // damage; the files are then left untouched.
        let mut frames = Vec::new();
        let last = files.len() - 1;
        let (last_id, last_len) = (files[last].0, files[last].1.len());
        let mut pos = WAL_FILE_HEADER;
        // Next object and block numbers at the start of each frame: delta
        // bases. Both only grow, so each is the highest seen + 1.
        let mut next_object = 0u64;
        let mut next_block = 0u64;
        let mut dicts = Vec::new();
        for (k, (id, bytes)) in files.iter().enumerate() {
            let id = *id;
            if id != frames.len() as u64 {
                return Err(StoreError::CorruptWal {
                    file: id,
                    offset: 0,
                    reason: if k == 0 {
                        "the first WAL file does not start at frame 0"
                    } else {
                        "WAL file does not continue the previous one"
                    },
                });
            }
            pos = WAL_FILE_HEADER;
            while pos < bytes.len() {
                let offset = pos as u64;
                let Some(frame) = codec::decode_frame(&bytes[pos..]) else {
                    let next = match codec::next_valid_frame(bytes, pos) {
                        Some(next) => Some((id, next as u64)),
                        None => files
                            .get(k + 1)
                            .map(|(next, _)| (*next, WAL_FILE_HEADER as u64)),
                    };
                    if let Some((next_file, next_offset)) = next {
                        return Err(StoreError::WalCorrupted {
                            file: id,
                            offset,
                            next_file,
                            next_offset,
                        });
                    }
                    break;
                };
                let corrupt = |reason| StoreError::CorruptWal {
                    file: id,
                    offset,
                    reason,
                };
                if frame.seq != frames.len() as u64 {
                    return Err(corrupt("frame sequence number out of order"));
                }
                let mode = codec::frame_mode(&frame).map_err(corrupt)?;
                let records =
                    codec::decode_records(&frame, next_object, next_block).map_err(corrupt)?;
                for rec in &records {
                    match rec {
                        Record::PutObject { number, .. } => {
                            next_object = next_object.max(number.saturating_add(1));
                        }
                        Record::PutBlock { number, .. } | Record::PutInlineBlock { number, .. } => {
                            next_block = next_block.max(number.saturating_add(1));
                        }
                        Record::Compact { rewritten, .. } => {
                            for r in rewritten {
                                next_block = next_block.max(r.block.saturating_add(1));
                            }
                        }
                        Record::PutDict { data, .. } => dicts.push(*data),
                        _ => {}
                    }
                }
                pos += frame.size();
                frames.push((WalPos { file: id, offset }, mode, records));
            }
        }
        // `pos`: the end of the last file's valid frames.
        let torn = pos < last_len;

        // The last complete frame's new blocks must be fully stored, decode,
        // and hold objects with a matching BLAKE3. Only a ParallelSync frame
        // with nothing after it, not even a WAL file, can lack them
        // legitimately (crash before it was acknowledged); it is dropped.
        // Anywhere else the frame was durable after its block bytes, or was
        // followed by another frame or file and so acknowledged: corruption,
        // reported with no file changed.
        let mut dropped = None;
        let mut check = codecs_for(&dicts)?;
        if let Some((at, mode, records)) = frames.last()
            && let Some(bad) = first_bad_object(&dir, records, &mut check).await?
        {
            if *mode != FrameMode::ParallelSync || torn || at.file != last_id {
                return Err(match bad.reason {
                    DropReason::ObjectMissing => StoreError::MissingObject(bad.object),
                    DropReason::ObjectHashMismatch => StoreError::HashMismatch(bad.object),
                    DropReason::BlockCorrupted => StoreError::CorruptBlock {
                        block: bad.block,
                        reason: bad.why,
                    },
                });
            }
            dropped = Some(RecoveryAction::FrameDropped {
                frame_seq: frames.len() as u64 - 1,
                file: last_id,
                offset: at.offset,
                mode: *mode,
                object: bad.object,
                reason: bad.reason,
                bytes_removed: last_len as u64 - at.offset,
            });
            pos = at.offset as usize;
            frames.pop();
        }
        match dropped {
            Some(action) => report.actions.push(action),
            None if torn => report.actions.push(RecoveryAction::TornTailTruncated {
                frame_seq: frames.len() as u64,
                file: last_id,
                offset: pos as u64,
                bytes_removed: (last_len - pos) as u64,
            }),
            None => {}
        }
        let wal = OpenOptions::new()
            .read(true)
            .write(true)
            .open(wal_path(&dir, last_id))
            .await?;
        let wal_len = pos as u64;
        if pos < last_len {
            wal.set_len(wal_len).await?;
            wal.sync_all().await?;
        }

        let mut replay = Replay::default();
        let next_frame = frames.len() as u64;
        for (at, _, records) in frames {
            let body_at = at.offset + FRAME_HEADER as u64;
            let corrupt = |reason| StoreError::CorruptWal {
                file: at.file,
                offset: at.offset,
                reason,
            };
            replay.file = at.file;
            for rec in records {
                replay.apply(rec, body_at).map_err(corrupt)?;
            }
            replay.finish_frame().map_err(corrupt)?;
        }
        let codecs = codecs_for(&replay.dicts)?;
        let dictionary = replay.dicts.last().map(|d| d.len());

        let segments = open_segments(&dir, &mut replay, &mut report).await?;
        let (index, topics) = open_index(&dir, &replay, next_frame, &config, &mut report).await?;
        check_refcounts(
            &replay.objects,
            replay.topics.values().flat_map(|t| {
                let first_live = usize::try_from(t.first_live).unwrap_or(usize::MAX);
                t.entries.iter().skip(first_live).map(|(number, _)| number)
            }),
        )?;

        let files = OpenFiles::new(&dir, config.data_open_files);
        Ok(Self {
            dir,
            config,
            wal,
            wal_id: last_id,
            wal_len,
            files,
            next_frame,
            in_flight: false,
            next_segment: replay.max_segment.map_or(0, |m| m + 1),
            objects: replay.objects,
            ids: replay.ids,
            next_object: replay.next_object,
            blocks: replay.blocks,
            next_block: replay.next_block,
            codecs: RefCell::new(codecs),
            cache: RefCell::new(BlockCache::new(config.block_cache_bytes)),
            dictless_raw: replay.dictless_raw,
            dictionary,
            next_topic: replay.topic_names.len() as u64,
            segments,
            active: None,
            topics,
            index,
            wal_buf: Vec::new(),
            seg_buf: Vec::new(),
            stats: Stats::default(),
            inline_bytes: 0,
            commit_start: Instant::now(),
            timing: CommitTiming::default(),
            recovery: report,
            poisoned: false,
        })
    }

    /// Durably appends a batch and returns the topic sequence number of each
    /// message, in order.
    ///
    /// Group commit: the batch's new objects (bytes not already stored, by
    /// BLAKE3 hash) form one block, compressed with zstd, written to the
    /// active segment in one write and one `fdatasync`; then one WAL frame
    /// holding all records of the batch, one write and one `fdatasync`. Only
    /// then does this return. Known bytes only add a reference. Timestamps
    /// are `now_ms`, raised per topic to keep them non-decreasing.
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
        let frame = self.batch_frame_bytes(batch);
        self.check_frame(frame)?;
        // The dictionary record goes into the batch's frame only if it fits
        // there: training waits for a batch small enough, so the frame
        // limit never refuses a batch because training is due.
        let dict = RECORD_PUT_DICT_BASE + block::DICT_SIZE;
        let train = !batch.is_empty()
            && self.should_train()
            && frame + dict <= self.config.max_frame_bytes as usize;
        let res = self.put_batch_inner(batch, now_ms, train).await;
        self.poison_on_err(res)
    }

    async fn put_batch_inner(
        &mut self,
        batch: &[Message<'_>],
        now_ms: u64,
        train: bool,
    ) -> Result<Vec<u64>, StoreError> {
        self.begin_write();
        self.rotate_wal().await?;
        self.commit_start = Instant::now();
        self.timing = CommitTiming::default();
        // Only between batches: every record in a sealed segment then
        // describes frames before the next segment's `created_at`.
        self.index.seal_if_full();
        let dict = if train {
            Some(self.train_dictionary().await?)
        } else {
            None
        };
        let (frame_object, frame_block) = (self.next_object, self.next_block);
        // New objects, in order of first appearance, are split into blocks
        // of at most `max_block_raw_bytes` raw bytes.
        let mut numbers = Vec::with_capacity(batch.len());
        // Payloads of the new objects, in order.
        let mut fresh: Vec<&[u8]> = Vec::new();
        // Added to `stats` once the batch is durable.
        let (mut deduplicated, mut new_bytes) = (0, 0);
        for m in batch {
            let id = ObjectId::of(m.payload);
            if let Some(&number) = self.ids.get(&id) {
                deduplicated += 1;
                numbers.push((number, false));
                continue;
            }
            let number = self.next_object;
            self.next_object += 1;
            // Block and index are set below, once the blocks are known.
            let object = Object {
                id,
                block: 0,
                index: 0,
                refs: 0,
            };
            self.objects.insert(number, object);
            self.ids.insert(id, number);
            fresh.push(m.payload);
            new_bytes += m.payload.len() as u64;
            numbers.push((number, true));
        }
        let ranges = block_ranges(
            fresh.iter().map(|p| codec::block_entry_len(p)),
            self.config.max_block_raw_bytes,
        );
        // (block number, raw block) per range; object `k` of the new ones is
        // in the block whose range holds `k`.
        let mut blocks: Vec<(u64, Vec<u8>, u64)> = Vec::with_capacity(ranges.len());
        let mut placed = Vec::with_capacity(fresh.len());
        for range in &ranges {
            let block = self.next_block + blocks.len() as u64;
            let mut raw = Vec::new();
            for (index, p) in fresh[range.clone()].iter().enumerate() {
                codec::push_block_object(&mut raw, p);
                placed.push((block, index as u64));
            }
            blocks.push((block, raw, range.len() as u64));
        }
        let mut placed = placed.into_iter();
        for (number, new) in &numbers {
            if *new
                && let (Some(o), Some((block, index))) =
                    (self.objects.get_mut(number), placed.next())
            {
                o.block = block;
                o.index = index;
            }
        }

        let mut seqs = Vec::with_capacity(batch.len());
        // Index blocks the batch completes: (topic, block number, entries).
        let mut filled = Vec::new();
        let mut wal_buf = std::mem::take(&mut self.wal_buf);
        wal_buf.clear();
        let mut frame = FrameBuilder::begin(&mut wal_buf, frame_object, frame_block);
        if let Some(data) = &dict {
            // The only dictionary: number 1.
            let number = 1;
            frame.record(&mut wal_buf, &Record::PutDict { number, data })?;
            if !data.is_empty() {
                self.codecs.get_mut().add_dictionary(number, data)?;
            }
            self.dictionary = Some(data.len());
        }
        let mut attrs = Vec::new();
        let (wal_id, wal_len) = (self.wal_id, self.wal_len);
        let mut inline_bytes = 0;
        for &(block, ref raw, new_objects) in &blocks {
            self.next_block += 1;
            let codecs = self.codecs.get_mut();
            let packed = codecs.compress(raw)?;
            let dict = codecs.dict();
            let crc = crc32c::crc32c(raw);
            let (len, raw_len) = (packed.len() as u64, raw.len() as u64);
            let inline = self.config.write_mode.inline_threshold();
            let loc = if inline.is_some_and(|t| packed.len() <= t) {
                let put = Record::PutInlineBlock {
                    number: block,
                    raw_len,
                    dict,
                    crc,
                    data: &packed,
                    data_at: 0,
                };
                frame.record(&mut wal_buf, &put)?;
                inline_bytes += len;
                // The block's bytes end the record just written; the frame
                // starts at `wal_len`, at index 0 of the buffer.
                Location::Wal(WalPos {
                    file: self.wal_id,
                    offset: self.wal_len + (wal_buf.len() - packed.len()) as u64,
                })
            } else {
                let (segment, offset) = self.reserve(len).await?;
                self.seg_buf.extend_from_slice(&packed);
                let put = Record::PutBlock {
                    number: block,
                    segment,
                    offset,
                    len,
                    raw_len,
                    dict,
                    crc,
                };
                frame.record(&mut wal_buf, &put)?;
                Location::Segment {
                    id: segment,
                    offset,
                }
            };
            let b = Block {
                loc,
                len,
                raw_len,
                dict,
                crc,
                objects: new_objects,
            };
            self.blocks.insert(block, b);
            if dict == NO_DICT {
                self.dictless_raw += raw_len;
            }
        }

        for (m, (number, new)) in batch.iter().zip(numbers) {
            let Some(object) = self.objects.get_mut(&number) else {
                unreachable!("found or inserted above")
            };
            object.refs += 1;
            if new {
                let put = Record::PutObject {
                    id: object.id,
                    number,
                    block: object.block,
                    index: object.index,
                };
                frame.record(&mut wal_buf, &put)?;
            }

            let new_topic = !self.topics.contains_key(m.topic);
            let topic = self.topic_mut(m.topic);
            if new_topic {
                let put = Record::PutTopic {
                    number: topic.number,
                    name: m.topic,
                };
                frame.record(&mut wal_buf, &put)?;
            }
            let seq = topic.index.seq();
            let timestamp = now_ms.max(topic.index.last_timestamp());
            if topic.index.push(&mut topic.tail, number, timestamp) {
                filled.push((m.topic, seq / INDEX_BLOCK, std::mem::take(&mut topic.tail)));
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
            if !attrs.is_empty() {
                // The attributes end the record just written; the frame
                // starts at `wal_len`, at index 0 of the buffer.
                let len = u32::try_from(attrs.len()).map_err(|_| StoreError::FrameTooLarge)?;
                let at = WalPos {
                    file: wal_id,
                    offset: wal_len + (wal_buf.len() - attrs.len()) as u64,
                };
                topic.attrs.push_back(AttrsAt { seq, at, len });
            }
            seqs.push(seq);
        }
        // Their records describe this batch's frame: the stamp is the frame
        // number after it.
        let stamp = self.next_frame + 1;
        for (name, block, body) in filled {
            self.add_index_block(name, block, &body, stamp).await?;
        }
        match self.config.write_mode {
            // InlineSmall keeps Sequential's order for a segment block; with
            // an inline block the segment flush is a no-op.
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
        // Index records are derived from the WAL and re-derived at open:
        // written after the WAL fsync, not synced.
        self.stats.topic_bytes += self.index.flush().await?;
        self.in_flight = false;
        // Recent objects are the likeliest reads.
        for (block, raw, _) in blocks {
            let spans = codec::split_block(&raw)
                .map_err(|reason| StoreError::CorruptBlock { block, reason })?;
            self.cache_insert(block, Rc::new(Decoded { raw, spans }));
        }
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

    /// Whether training is due: the raw bytes stored without a dictionary
    /// reached `Config::dict_train_bytes` and training has not run. The
    /// next non-empty `put_batch` whose frame leaves room for the
    /// dictionary record trains. For tests and fault injection.
    pub fn trains_next_batch(&self) -> bool {
        self.should_train()
    }

    fn should_train(&self) -> bool {
        self.dictionary.is_none()
            && self.config.dict_train_bytes > 0
            && self.dictless_raw >= self.config.dict_train_bytes
    }

    /// Trains the dictionary on the objects of the blocks stored without
    /// one, in block order, up to `block::DICT_SAMPLE_CAP` bytes. Depends
    /// only on durable state, so a batch retried after a crash trains the
    /// same dictionary; replay reads it from the WAL and never trains.
    async fn train_dictionary(&self) -> Result<Vec<u8>, StoreError> {
        let mut numbers: Vec<u64> = self
            .blocks
            .iter()
            .filter(|(_, b)| b.dict == NO_DICT)
            .map(|(&n, _)| n)
            .collect();
        numbers.sort_unstable();
        let mut decoded = Vec::new();
        let mut total = 0;
        for n in numbers {
            if total >= block::DICT_SAMPLE_CAP {
                break;
            }
            let d = self.block(n).await?;
            total += d.raw.len();
            decoded.push(d);
        }
        let mut samples = Vec::new();
        let mut bytes = 0;
        'blocks: for d in &decoded {
            for span in &d.spans {
                if bytes + span.len() > block::DICT_SAMPLE_CAP {
                    break 'blocks;
                }
                bytes += span.len();
                samples.push(&d.raw[span.clone()]);
            }
        }
        Ok(block::train(&samples))
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
        // The entries to release, oldest first, read before anything is
        // written: a damaged index block fails the call here, not midway.
        // Collection stops as soon as one more release would not fit a
        // frame, so memory stays within the frame bound.
        let mut numbers = Vec::new();
        if let Some(t) = self.topics.get(topic) {
            for seq in t.first_live..t.index.seq() {
                match self.topic_entry(topic, t, seq).await? {
                    Some((number, ts)) if ts < cutoff_ms => {
                        self.check_frame(FRAME_HEADER + (numbers.len() + 1) * RECORD_RELEASE)?;
                        numbers.push(number);
                    }
                    _ => break,
                }
            }
        }
        let res = self.release_inner(topic, &numbers).await;
        self.poison_on_err(res)
    }

    async fn release_inner(&mut self, name: &[u8], numbers: &[u64]) -> Result<u64, StoreError> {
        self.begin_write();
        self.rotate_wal().await?;
        let Some(topic) = self.topics.get_mut(name) else {
            self.in_flight = false;
            return Ok(0);
        };
        let mut wal_buf = std::mem::take(&mut self.wal_buf);
        wal_buf.clear();
        let mut frame = FrameBuilder::begin(&mut wal_buf, self.next_object, self.next_block);
        let mut released = 0;
        for &number in numbers {
            let release = Record::Release {
                topic: topic.number,
                seq: topic.first_live,
            };
            frame.record(&mut wal_buf, &release)?;
            topic.first_live += 1;
            if topic
                .attrs
                .front()
                .is_some_and(|a| a.seq < topic.first_live)
            {
                topic.attrs.pop_front();
            }
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
        match self.entry_number(topic, seq).await? {
            Some(number) => self.read_object(number).await.map(|(_, b)| Some(b)),
            None => Ok(None),
        }
    }

    /// Mark and sweep. Marks objects with a non-zero refcount. Every segment
    /// holding a block with a dead object, or unreferenced bytes (orphans of
    /// a crash), is rewritten: blocks whose objects are all live are copied
    /// unchanged, blocks whose objects are all dead are dropped, and the
    /// live objects of partly dead blocks are recompressed together into one
    /// new block:
    ///
    /// 1. write the new segment, `fdatasync` it, `fsync` the directory
    /// 2. append a frame with one Compact record to the WAL, `fdatasync` it
    /// 3. switch the index to the new segment, delete the old file
    ///
    /// A crash before step 2 leaves an unreferenced new segment and a crash
    /// after it a stale old one; recovery deletes either. Inline blocks stay
    /// in the WAL: reclaiming them needs a WAL checkpoint.
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
        let mut by_segment: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
        for (&number, b) in &self.blocks {
            if let Location::Segment { id, offset } = b.loc {
                by_segment.entry(id).or_default().push((offset, number));
            }
        }
        // Block → its objects: `(index, object number, live)`.
        let mut members: HashMap<u64, Vec<(u64, u64, bool)>> = HashMap::new();
        for (&number, o) in &self.objects {
            members
                .entry(o.block)
                .or_default()
                .push((o.index, number, o.refs > 0));
        }
        let mut ids: Vec<u64> = self.segments.iter().copied().collect();
        ids.sort_unstable();
        for old in ids {
            let file_len = compio::fs::metadata(segment_path(&self.dir, old))
                .await?
                .len();
            let mut blocks = by_segment.remove(&old).unwrap_or_default();
            blocks.sort_unstable();
            let (mut kept, mut partial, mut dead) = (Vec::new(), Vec::new(), Vec::new());
            let mut stored = 0;
            for &(_, n) in &blocks {
                stored += self.blocks.get(&n).map_or(0, |b| b.len);
                let objs = members.get(&n).map_or(&[][..], Vec::as_slice);
                let live = objs.iter().filter(|o| o.2).count();
                if live == 0 {
                    dead.push(n);
                } else if live == objs.len() {
                    kept.push(n);
                } else {
                    partial.push(n);
                }
            }
            // Blocks never overlap, so equal sizes mean no unreferenced byte.
            if partial.is_empty() && dead.is_empty() && stored == file_len {
                continue;
            }
            // Live objects of partly dead blocks, in block then index order.
            let mut moved = Vec::new();
            for n in &partial {
                let mut objs: Vec<_> = members
                    .get(n)
                    .map_or(&[][..], Vec::as_slice)
                    .iter()
                    .filter(|o| o.2)
                    .copied()
                    .collect();
                objs.sort_unstable();
                moved.extend(objs.into_iter().map(|(_, number, _)| number));
            }
            // At most one rewritten block per moved object.
            let rewrite_bound = (RECORD_COMPACT_REWRITTEN + MAX_VARINT) * moved.len();
            self.check_frame(
                FRAME_HEADER
                    + RECORD_COMPACT_BASE
                    + RECORD_COMPACT_KEPT * kept.len()
                    + rewrite_bound,
            )?;
            self.begin_write();
            self.rotate_wal().await?;
            let mut kept_at = Vec::with_capacity(kept.len());
            let mut rewritten = Vec::new();
            let mut new_len = 0;
            let new = if kept.is_empty() && moved.is_empty() {
                NO_SEGMENT
            } else {
                let new = self.next_segment;
                self.next_segment += 1;
                let mut buf = Vec::new();
                for &n in &kept {
                    // Verified first: corruption is not copied forward.
                    let objs = members.get(&n).map_or(&[][..], Vec::as_slice);
                    let stored = self.load_verified(n, objs).await?;
                    kept_at.push((n, buf.len() as u64));
                    buf.extend_from_slice(&stored);
                }
                let mut objects = Vec::with_capacity(moved.len());
                for &number in &moved {
                    // Verifies the hash.
                    objects.push(self.read_object(number).await?.1);
                }
                // The same limit as for new blocks.
                for range in block_ranges(
                    objects.iter().map(|b| codec::block_entry_len(b)),
                    self.config.max_block_raw_bytes,
                ) {
                    let mut raw = Vec::new();
                    for bytes in &objects[range.clone()] {
                        codec::push_block_object(&mut raw, bytes);
                    }
                    let codecs = self.codecs.get_mut();
                    let packed = codecs.compress(&raw)?;
                    let block = self.next_block;
                    self.next_block += 1;
                    rewritten.push(Rewritten {
                        block,
                        offset: buf.len() as u64,
                        len: packed.len() as u64,
                        raw_len: raw.len() as u64,
                        dict: codecs.dict(),
                        crc: crc32c::crc32c(&raw),
                        objects: moved[range].to_vec(),
                    });
                    buf.extend_from_slice(&packed);
                }
                let file = create_segment(&self.dir, new).await?;
                new_len = buf.len() as u64;
                write_all(&file, buf, 0).await.0?;
                file.sync_data().await?;
                sync_dir(&self.dir).await?;
                self.stats.fsyncs += 2;
                self.stats.segment_bytes += new_len;
                self.segments.insert(new);
                new
            };

            let mut wal_buf = std::mem::take(&mut self.wal_buf);
            wal_buf.clear();
            let mut frame = FrameBuilder::begin(&mut wal_buf, self.next_object, self.next_block);
            let compact = Record::Compact {
                old,
                new,
                kept: kept_at.clone(),
                rewritten: rewritten.clone(),
            };
            frame.record(&mut wal_buf, &compact)?;
            self.append_frame(wal_buf, frame, FrameMode::Sequential)
                .await?;
            self.in_flight = false;

            for n in dead.iter().chain(&partial) {
                for &(_, number, live) in members.get(n).map_or(&[][..], Vec::as_slice) {
                    if !live && let Some(o) = self.objects.remove(&number) {
                        self.ids.remove(&o.id);
                        stats.objects_dropped += 1;
                    }
                }
                self.blocks.remove(n);
                self.uncache(*n);
            }
            for (n, offset) in kept_at {
                if let Some(b) = self.blocks.get_mut(&n) {
                    b.loc = Location::Segment { id: new, offset };
                }
            }
            for r in rewritten {
                for (index, number) in (0..).zip(&r.objects) {
                    if let Some(o) = self.objects.get_mut(number) {
                        o.block = r.block;
                        o.index = index;
                    }
                }
                let b = Block {
                    loc: Location::Segment {
                        id: new,
                        offset: r.offset,
                    },
                    len: r.len,
                    raw_len: r.raw_len,
                    dict: r.dict,
                    crc: r.crc,
                    objects: r.objects.len() as u64,
                };
                self.blocks.insert(r.block, b);
            }
            self.segments.remove(&old);
            self.files.forget(FileKey::Data(old));
            // Not followed by a directory fsync: if the unlink is lost, the
            // WAL no longer references the file and recovery deletes it.
            compio::fs::remove_file(segment_path(&self.dir, old)).await?;
            stats.segments_rewritten += 1;
            stats.bytes_reclaimed += file_len.saturating_sub(new_len);
        }
        Ok(stats)
    }

    pub fn stats(&self) -> Stats {
        let mut s = self.stats;
        s.fsyncs += self.index.fsyncs;
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
            .map(|t| (t.first_live, t.index.seq()))
    }

    pub fn topics(&self) -> impl Iterator<Item = &[u8]> {
        self.topics.keys().map(Vec::as_slice)
    }

    /// The object a live entry points at.
    pub async fn entry(&self, topic: &[u8], seq: u64) -> Result<Option<ObjectId>, StoreError> {
        self.usable()?;
        let number = self.entry_number(topic, seq).await?;
        Ok(number.and_then(|n| self.objects.get(&n)).map(|o| o.id))
    }

    /// Entry `seq` of `topic` as stored in its index: `(object number,
    /// timestamp)`, released entries included. Reads one index block and at
    /// most one seek page up to `seek_page_entries`² full blocks (one more
    /// page per further power); `None` past the last entry.
    pub async fn index_entry(
        &self,
        topic: &[u8],
        seq: u64,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        self.usable()?;
        let Some(t) = self.topics.get(topic) else {
            return Ok(None);
        };
        self.topic_entry(topic, t, seq).await
    }

    /// Entry `seq` of topic `name` (released entries included): from the
    /// last, incomplete block in memory, or from a full block's index
    /// record, which must match its CRC, topic and block number
    /// (`TopicIndexCorrupt` otherwise). `None` at or past the next seq.
    async fn topic_entry(
        &self,
        name: &[u8],
        t: &Topic,
        seq: u64,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let next = t.index.seq();
        if seq >= next {
            return Ok(None);
        }
        let block = seq / INDEX_BLOCK;
        let at = (seq % INDEX_BLOCK) as usize;
        let corrupt = |block| StoreError::TopicIndexCorrupt {
            topic: name.to_vec(),
            block,
        };
        if block == next / INDEX_BLOCK {
            let entries = codec::decode_index_entries(&t.tail).ok_or_else(|| corrupt(block))?;
            return Ok(entries.get(at).copied());
        }
        if let Some((b, entries)) = t.read_cache.borrow().as_ref()
            && *b == block
        {
            return Ok(entries.get(at).copied());
        }
        let loc = self.locate(t, block).await?.ok_or_else(|| corrupt(block))?;
        let entries = self
            .index
            .record(loc, INDEX_RECORD_BLOCK, t.number, block)
            .await?
            .and_then(|body| codec::decode_index_entries(&body))
            .filter(|e| e.len() as u64 == INDEX_BLOCK)
            .ok_or_else(|| corrupt(block))?;
        let entry = entries.get(at).copied();
        *t.read_cache.borrow_mut() = Some((block, entries));
        Ok(entry)
    }

    /// Where full block `block` of `t` lies: from the spine, through the
    /// seek pages under it. `None` if the block is not covered or a seek
    /// page on the way is damaged (missing, failing its CRC, or another
    /// topic's, level's or number's).
    async fn locate(&self, t: &Topic, block: u64) -> Result<Option<IndexLoc>, StoreError> {
        let fan = self.index.fanout as u64;
        let mut start = 0u64;
        for (level, locs) in t.spine.iter().enumerate().rev() {
            let span = fan.saturating_pow(level as u32);
            let covered = span.saturating_mul(locs.len() as u64);
            if block >= start.saturating_add(covered) {
                start = start.saturating_add(covered);
                continue;
            }
            let rel = block - start;
            let Some(&first) = usize::try_from(rel / span).ok().and_then(|i| locs.get(i)) else {
                return Ok(None);
            };
            let (mut loc, mut rel) = (first, rel % span);
            for lower in (0..level as u32).rev() {
                let span = fan.saturating_pow(lower);
                let page = block / fan.saturating_pow(lower + 1);
                let Some(body) = self
                    .index
                    .record(loc, INDEX_RECORD_SEEK_PAGE, t.number, page)
                    .await?
                    .filter(|b| codec::seek_page_shape(b) == Some((lower as u8, fan as usize)))
                else {
                    return Ok(None);
                };
                let Some(next) = usize::try_from(rel / span)
                    .ok()
                    .and_then(|i| codec::seek_page_loc(&body, i))
                else {
                    return Ok(None);
                };
                (loc, rel) = (next, rel % span);
            }
            return Ok(Some(loc));
        }
        Ok(None)
    }

    async fn entry_number(&self, topic: &[u8], seq: u64) -> Result<Option<u64>, StoreError> {
        let Some(t) = self.topics.get(topic) else {
            return Ok(None);
        };
        if seq < t.first_live {
            return Ok(None);
        }
        Ok(self
            .topic_entry(topic, t, seq)
            .await?
            .map(|(number, _)| number))
    }

    /// Entry `seq` of `topic` with its timestamp, key and headers; `None` if
    /// the entry was never published or was released. The payload's hash
    /// is verified. Key and headers are read from the WAL.
    pub async fn read_entry(&self, topic: &[u8], seq: u64) -> Result<Option<Entry>, StoreError> {
        self.usable()?;
        let Some(t) = self.topics.get(topic) else {
            return Ok(None);
        };
        if seq < t.first_live {
            return Ok(None);
        }
        let Some((number, timestamp)) = self.topic_entry(topic, t, seq).await? else {
            return Ok(None);
        };
        let (_, payload) = self.read_object(number).await?;
        let (key, headers) = match t.attrs.binary_search_by_key(&seq, |a| a.seq) {
            Err(_) => (None, Vec::new()),
            Ok(i) => {
                let AttrsAt { at, len, .. } = t.attrs[i];
                let corrupt = |reason| StoreError::CorruptWal {
                    file: at.file,
                    offset: at.offset,
                    reason,
                };
                let file = self
                    .wal_file(at.file)
                    .await?
                    .ok_or(corrupt("WAL file of a key or headers is missing"))?;
                let len = len as usize;
                let BufResult(res, buf) = file
                    .read_exact_at(Vec::with_capacity(len).slice(..len), at.offset)
                    .await;
                res?;
                codec::decode_attrs(&buf.into_inner()).map_err(corrupt)?
            }
        };
        Ok(Some(Entry {
            timestamp,
            key,
            headers,
            payload,
        }))
    }

    /// Counters of `topic`; `None` if it has no entry.
    pub fn topic_stats(&self, topic: &[u8]) -> Option<TopicStats> {
        self.topics.get(topic).map(|t| TopicStats {
            first_live: t.first_live,
            next_seq: t.index.seq(),
            index_bytes: t.record_bytes + t.tail.len() as u64,
        })
    }

    /// The upper bound of the WAL frame `put_batch` checks against
    /// `Config::max_frame_bytes` before any I/O: every object counted as
    /// new and as a block of its own (inline up to the threshold and zstd's
    /// bound for its raw bytes). A due dictionary is added only to a batch
    /// whose frame then still fits the limit, so it is not counted here.
    /// Additive over messages: the bound of two batches together is the
    /// sum of their bounds minus the bound of an empty batch, so a caller
    /// can grow a batch message by message. A batch over the limit fails
    /// with `FrameTooLarge`.
    pub fn batch_frame_bytes(&self, batch: &[Message<'_>]) -> usize {
        let inline = self.config.write_mode.inline_threshold();
        FRAME_HEADER
            + batch
                .iter()
                .map(|m| {
                    let raw = MAX_VARINT + m.payload.len();
                    let block = match inline {
                        Some(t) => RECORD_PUT_BLOCK
                            .max(RECORD_PUT_INLINE_BLOCK_BASE + t.min(block::compress_bound(raw))),
                        None => RECORD_PUT_BLOCK,
                    };
                    // A new topic adds its PutTopic (counted per message: an
                    // upper bound).
                    let topic = if self.topics.contains_key(m.topic) {
                        0
                    } else {
                        RECORD_PUT_TOPIC_BASE + m.topic.len()
                    };
                    block
                        + RECORD_PUT_OBJECT
                        + RECORD_PUBLISH
                        + codec::attrs_len(m.key, m.headers)
                        + topic
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

    /// Whether the object's block is stored in the WAL (InlineSmall);
    /// `None` if the object is not in the index. Dead objects of such
    /// blocks are never reclaimed by compaction.
    pub fn is_inline(&self, id: &ObjectId) -> Option<bool> {
        let number = self.ids.get(id)?;
        let o = self.objects.get(number)?;
        Some(matches!(self.blocks.get(&o.block)?.loc, Location::Wal(_)))
    }

    /// Index segment files the store holds open: at most
    /// `Config::index_open_files` for reads, plus the one being written.
    pub fn open_index_files(&self) -> usize {
        self.index.files.len() + usize::from(self.index.active.is_some())
    }

    /// Data segments and earlier WAL files the store holds open: at most
    /// `Config::data_open_files` for reads, plus the segment and the WAL
    /// file being written.
    pub fn open_data_files(&self) -> usize {
        self.files.len() + usize::from(self.active.is_some()) + 1
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Blocks the store holds (batch blocks and compaction's rewritten
    /// blocks).
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// When the last `put_batch`'s segment and WAL syncs ran.
    pub fn last_commit_timing(&self) -> CommitTiming {
        self.timing
    }

    /// Length of the store's compression dictionary; `None` before training
    /// ran (`Config::dict_train_bytes`), `Some(0)` if training built none.
    pub fn dictionary_bytes(&self) -> Option<usize> {
        self.dictionary
    }

    /// A handle of WAL file `id`: the one being appended to, or an earlier
    /// one from the open files. `None` if the file does not exist.
    async fn wal_file(&self, id: u64) -> Result<Option<File>, StoreError> {
        if id == self.wal_id {
            return Ok(Some(self.wal.clone()));
        }
        self.files.get(FileKey::Wal(id)).await
    }

    /// Before a frame is built: once the WAL file holds a frame and at
    /// least `Config::wal_file_max_bytes`, the next frame starts a new file
    /// named by its sequence number. Positions in the frame (inline blocks,
    /// keys and headers) are taken after this.
    async fn rotate_wal(&mut self) -> Result<(), StoreError> {
        let len = self.wal_len;
        if len <= WAL_FILE_HEADER as u64 || len < self.config.wal_file_max_bytes {
            return Ok(());
        }
        let id = self.next_frame;
        let file = create_wal_file(&self.dir, id, self.config.dict_train_bytes).await?;
        self.stats.fsyncs += 2;
        self.wal = file;
        self.wal_id = id;
        self.wal_len = WAL_FILE_HEADER as u64;
        Ok(())
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
        let d = self.block(object.block).await?;
        let span = usize::try_from(object.index)
            .ok()
            .and_then(|i| d.spans.get(i))
            .ok_or(StoreError::CorruptBlock {
                block: object.block,
                reason: "object index past the end of its block",
            })?;
        let bytes = d.raw[span.clone()].to_vec();
        if ObjectId::of(&bytes) != object.id {
            return Err(StoreError::HashMismatch(object.id));
        }
        Ok((object.id, bytes))
    }

    /// Block `number`, decompressed and checked, from the cache or disk.
    async fn block(&self, number: u64) -> Result<Rc<Decoded>, StoreError> {
        if let Some(d) = self.cached(number) {
            return Ok(d);
        }
        let b = self.stored_block(number)?;
        let stored = self.read_stored(number, b).await?;
        let d = decode_block(&mut self.codecs.borrow_mut(), b, &stored).map_err(|reason| {
            StoreError::CorruptBlock {
                block: number,
                reason,
            }
        })?;
        let d = Rc::new(d);
        self.cache_insert(number, Rc::clone(&d));
        Ok(d)
    }

    /// The stored bytes of block `number` after checking that it decodes
    /// and that each of `objects` matches its BLAKE3 id.
    async fn load_verified(
        &self,
        number: u64,
        objects: &[(u64, u64, bool)],
    ) -> Result<Vec<u8>, StoreError> {
        let b = self.stored_block(number)?;
        let stored = self.read_stored(number, b).await?;
        let d = decode_block(&mut self.codecs.borrow_mut(), b, &stored).map_err(|reason| {
            StoreError::CorruptBlock {
                block: number,
                reason,
            }
        })?;
        for &(index, object, _) in objects {
            let Some(o) = self.objects.get(&object) else {
                continue;
            };
            let span = usize::try_from(index).ok().and_then(|i| d.spans.get(i));
            if span.is_none_or(|span| ObjectId::of(&d.raw[span.clone()]) != o.id) {
                return Err(StoreError::HashMismatch(o.id));
            }
        }
        Ok(stored)
    }

    fn stored_block(&self, number: u64) -> Result<&Block, StoreError> {
        self.blocks.get(&number).ok_or(StoreError::CorruptBlock {
            block: number,
            reason: "block not in the index",
        })
    }

    async fn read_stored(&self, number: u64, b: &Block) -> Result<Vec<u8>, StoreError> {
        let missing = StoreError::CorruptBlock {
            block: number,
            reason: "stored bytes missing or truncated",
        };
        let (file, offset) = match b.loc {
            Location::Segment { id, offset } if self.segments.contains(&id) => {
                (self.files.get(FileKey::Data(id)).await?, offset)
            }
            Location::Segment { .. } => (None, 0),
            Location::Wal(at) => (self.wal_file(at.file).await?, at.offset),
        };
        let Some(file) = file else {
            return Err(missing);
        };
        let len = usize::try_from(b.len).map_err(|_| StoreError::CorruptBlock {
            block: number,
            reason: "block too large for this platform",
        })?;
        // `slice` caps the read at `len` even if the allocation is larger.
        let BufResult(res, buf) = file
            .read_exact_at(Vec::with_capacity(len).slice(..len), offset)
            .await;
        match res {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Err(missing),
            res => {
                res?;
                Ok(buf.into_inner())
            }
        }
    }

    fn cached(&self, number: u64) -> Option<Rc<Decoded>> {
        self.cache.borrow_mut().get(number)
    }

    fn cache_insert(&self, number: u64, d: Rc<Decoded>) {
        self.cache.borrow_mut().insert(number, d);
    }

    fn uncache(&self, number: u64) {
        self.cache.borrow_mut().remove(number);
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
            self.segments.insert(id);
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
        let t = Instant::now();
        active.file.sync_data().await?;
        widen(
            &mut self.timing.segment_sync,
            self.commit_start,
            t,
            Instant::now(),
        );
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
        let t = Instant::now();
        self.wal.sync_data().await?;
        widen(
            &mut self.timing.wal_sync,
            self.commit_start,
            t,
            Instant::now(),
        );
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
            let t = Instant::now();
            wal.sync_data().await?;
            io::Result::Ok((buf, (t, Instant::now())))
        };
        let seg_side = async {
            let Some((file, at)) = &seg else {
                return io::Result::Ok((seg_buf, None));
            };
            let (res, buf) = write_all(file, seg_buf, *at).await;
            res?;
            let t = Instant::now();
            file.sync_data().await?;
            Ok((buf, Some((t, Instant::now()))))
        };
        let (wal_res, seg_res) = join2(wal_side, seg_side).await;
        let (wal_buf, (w0, w1)) = wal_res?;
        self.wal_buf = wal_buf;
        widen(&mut self.timing.wal_sync, self.commit_start, w0, w1);
        let (mut seg_buf, seg_span) = seg_res?;
        if let Some((s0, s1)) = seg_span {
            widen(&mut self.timing.segment_sync, self.commit_start, s0, s1);
        }
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

    /// Appends the index record of full block `block` of topic `name` and
    /// its location to the topic's spine; a level that fills becomes a seek
    /// page, whose location moves up a level.
    async fn add_index_block(
        &mut self,
        name: &[u8],
        block: u64,
        body: &[u8],
        stamp: u64,
    ) -> Result<(), StoreError> {
        let created_at = self.next_frame;
        let Some(number) = self.topics.get(name).map(|t| t.number) else {
            unreachable!("the batch created the topic")
        };
        let mut loc = self
            .index
            .append(INDEX_RECORD_BLOCK, number, block, stamp, body, created_at)
            .await?;
        let fan = self.index.fanout;
        for level in 0u32.. {
            let Some(t) = self.topics.get_mut(name) else {
                unreachable!("the batch created the topic")
            };
            t.record_bytes += u64::from(loc.len);
            if t.spine.len() <= level as usize {
                t.spine.push(Vec::new());
            }
            let locs = &mut t.spine[level as usize];
            locs.push(loc);
            if locs.len() < fan {
                break;
            }
            let body = codec::seek_page_body(level as u8, locs);
            locs.clear();
            // The page ends with `block`: its number at its level.
            let page = (block + 1) / (fan as u64).saturating_pow(level + 1) - 1;
            loc = self
                .index
                .append(
                    INDEX_RECORD_SEEK_PAGE,
                    number,
                    page,
                    stamp,
                    &body,
                    created_at,
                )
                .await?;
        }
        Ok(())
    }

    fn topic_mut(&mut self, name: &[u8]) -> &mut Topic {
        // The key is allocated only for a new topic.
        if !self.topics.contains_key(name) {
            let number = self.next_topic;
            self.next_topic += 1;
            let topic = Topic {
                number,
                index: IndexWriter::default(),
                tail: Vec::new(),
                spine: Vec::new(),
                first_live: 0,
                read_cache: RefCell::new(None),
                record_bytes: 0,
                attrs: VecDeque::new(),
            };
            self.topics.insert(name.to_vec(), topic);
        }
        match self.topics.get_mut(name) {
            Some(topic) => topic,
            None => unreachable!("inserted above"),
        }
    }
}

/// A file the store opens on demand for reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileKey {
    Data(u64),
    Wal(u64),
    Index(u64),
}

impl FileKey {
    fn path(self, dir: &Path) -> PathBuf {
        match self {
            Self::Data(id) => segment_path(dir, id),
            Self::Wal(first) => wal_path(dir, first),
            Self::Index(id) => index_path(dir, id),
        }
    }
}

/// Files open for reads, at most `max` at a time, least recently used
/// closed first (D-227 A). A linear scan: the bounds are a few hundred. A
/// read in progress holds its handle until it completes, so concurrent
/// reads can each keep one more open.
struct OpenFiles {
    dir: PathBuf,
    files: RefCell<VecDeque<(FileKey, File)>>,
    max: usize,
}

impl OpenFiles {
    fn new(dir: &Path, max: usize) -> Self {
        Self {
            dir: dir.to_path_buf(),
            files: RefCell::new(VecDeque::new()),
            max: max.max(1),
        }
    }

    /// A handle of `key`, opening it (and closing the least recently used)
    /// if needed; `None` if the file does not exist.
    async fn get(&self, key: FileKey) -> Result<Option<File>, StoreError> {
        {
            let mut files = self.files.borrow_mut();
            if let Some(at) = files.iter().position(|(k, _)| *k == key)
                && let Some(entry) = files.remove(at)
            {
                let file = entry.1.clone();
                files.push_back(entry);
                return Ok(Some(file));
            }
        }
        let file = match File::open(key.path(&self.dir)).await {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut files = self.files.borrow_mut();
        while files.len() >= self.max {
            files.pop_front();
        }
        files.push_back((key, file.clone()));
        Ok(Some(file))
    }

    /// Closes `key`'s handle, if open: before its file is deleted.
    fn forget(&self, key: FileKey) {
        self.files.borrow_mut().retain(|(k, _)| *k != key);
    }

    fn len(&self) -> usize {
        self.files.borrow().len()
    }
}

/// The shared index segments `idx-<id>.dat`: full topic index blocks and
/// seek pages of every topic, appended to one segment at a time.
struct IndexSegments {
    dir: PathBuf,
    /// The segment being appended to. Only a segment created by this
    /// `Store` is appended to; it is created when the first record needs it.
    active: Option<Active>,
    next_id: u64,
    /// Records not yet written to `active`: written after the WAL fsync.
    buf: Vec<u8>,
    /// Segments open for reads.
    files: OpenFiles,
    max_bytes: u64,
    /// `Config::seek_page_entries`, clamped.
    fanout: usize,
    /// Syncs of segment creation since open.
    fsyncs: u64,
}

impl IndexSegments {
    fn new(dir: &Path, config: &Config) -> Self {
        Self {
            dir: dir.to_path_buf(),
            active: None,
            next_id: 0,
            buf: Vec::new(),
            files: OpenFiles::new(dir, config.index_open_files),
            max_bytes: config.index_segment_max_bytes,
            fanout: config.seek_page_entries.clamp(SEEK_PAGE_MIN, SEEK_PAGE_MAX),
            fsyncs: 0,
        }
    }

    /// Creates the next segment with its header, synced with its directory
    /// entry before any record goes in: an intact header proves the
    /// segment's `created_at`, which recovery relies on.
    async fn start(&mut self, created_at: u64) -> Result<(), StoreError> {
        let id = self.next_id;
        if u32::try_from(id).is_err() {
            return Err(io::Error::other("index segment ids exhausted").into());
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(index_path(&self.dir, id))
            .await?;
        self.next_id += 1;
        write_all(&file, codec::index_header(created_at).to_vec(), 0)
            .await
            .0?;
        file.sync_data().await?;
        sync_dir(&self.dir).await?;
        self.fsyncs += 2;
        let len = INDEX_HEADER as u64;
        self.active = Some(Active {
            id,
            file,
            len,
            written: len,
        });
        Ok(())
    }

    /// Seals the active segment once it holds `max_bytes`; call only
    /// between batches, with `buf` written.
    fn seal_if_full(&mut self) {
        if self
            .active
            .as_ref()
            .is_some_and(|a| a.len >= self.max_bytes)
        {
            self.active = None;
        }
    }

    /// Buffers a record; starts a segment (`created_at`) if none is active.
    async fn append(
        &mut self,
        kind: u8,
        topic: u64,
        number: u64,
        stamp: u64,
        body: &[u8],
        created_at: u64,
    ) -> Result<IndexLoc, StoreError> {
        if self.active.is_none() {
            self.start(created_at).await?;
        }
        let Some(active) = self.active.as_mut() else {
            unreachable!("started above")
        };
        let size = codec::put_index_record(&mut self.buf, kind, topic, number, stamp, body);
        let loc = IndexLoc {
            // Checked in `start`; a record is at most a few KiB.
            segment: active.id as u32,
            len: size as u32,
            offset: active.len,
        };
        active.len += size as u64;
        Ok(loc)
    }

    /// Writes the buffered records (no sync); returns the bytes written.
    async fn flush(&mut self) -> Result<u64, StoreError> {
        let Some(active) = self.active.as_mut() else {
            return Ok(0);
        };
        if self.buf.is_empty() {
            return Ok(0);
        }
        let n = self.buf.len() as u64;
        let (res, mut buf) =
            write_all(&active.file, std::mem::take(&mut self.buf), active.written).await;
        buf.clear();
        self.buf = buf;
        res?;
        active.written += n;
        Ok(n)
    }

    /// A handle of segment `id`: the active one, or from the open files.
    /// `None` if the file does not exist.
    async fn file(&self, id: u64) -> Result<Option<File>, StoreError> {
        if let Some(a) = self.active.as_ref().filter(|a| a.id == id) {
            return Ok(Some(a.file.clone()));
        }
        self.files.get(FileKey::Index(id)).await
    }

    /// The body of the record at `loc` if it is intact (CRC) and is the
    /// `kind` record of `topic` with `number`; `None` otherwise, including a
    /// missing or short file.
    async fn record(
        &self,
        loc: IndexLoc,
        kind: u8,
        topic: u64,
        number: u64,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(file) = self.file(u64::from(loc.segment)).await? else {
            return Ok(None);
        };
        let len = loc.len as usize;
        let BufResult(res, buf) = file
            .read_exact_at(Vec::with_capacity(len).slice(..len), loc.offset)
            .await;
        match res {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            res => res?,
        }
        let buf = buf.into_inner();
        Ok(codec::decode_index_record(&buf)
            .filter(|r| {
                r.kind == kind && r.topic == topic && r.number == number && r.size == buf.len()
            })
            .map(|r| r.body.to_vec()))
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
    blocks: HashMap<u64, Block>,
    /// One above the highest block number in the WAL.
    next_block: u64,
    /// Blocks introduced by the frame being replayed.
    frame_blocks: Vec<u64>,
    /// Dictionaries by number − 1; empty: training built none.
    dicts: Vec<&'a [u8]>,
    /// Raw bytes of batch blocks stored without a dictionary.
    dictless_raw: u64,
    topics: HashMap<&'a [u8], ReplayTopic>,
    /// Topic number → name, from PutTopic records; numbers are contiguous.
    topic_names: Vec<&'a [u8]>,
    topic_numbers: HashMap<&'a [u8], u64>,
    /// Segments the WAL references.
    segments: HashSet<u64>,
    /// Highest segment id seen in the WAL or on disk; ids are never reused.
    max_segment: Option<u64>,
    /// The WAL file whose frames are being replayed.
    file: u64,
}

#[derive(Default)]
struct ReplayTopic {
    number: u64,
    /// Every published entry, released ones included:
    /// `(object number, timestamp)`.
    entries: Vec<(u64, u64)>,
    first_live: u64,
    /// Key and headers of the entries that have them, released ones
    /// included.
    attrs: Vec<AttrsAt>,
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

    fn add_block(&mut self, number: u64, block: Block) -> Result<(), &'static str> {
        if number < self.next_block {
            return Err("block number not above every earlier one");
        }
        let known = block.dict == NO_DICT
            || usize::try_from(block.dict - 1)
                .ok()
                .and_then(|i| self.dicts.get(i))
                .is_some_and(|d| !<[u8]>::is_empty(d));
        if !known {
            return Err("block references an unknown dictionary");
        }
        self.next_block = number + 1;
        self.blocks.insert(number, block);
        Ok(())
    }

    /// A block introduced by a batch frame: `PutObject` records follow.
    fn add_batch_block(&mut self, number: u64, block: Block) -> Result<(), &'static str> {
        self.add_block(number, block)?;
        if block.dict == NO_DICT {
            self.dictless_raw += block.raw_len;
        }
        self.frame_blocks.push(number);
        Ok(())
    }

    /// Checks at the end of each frame: every block it introduced holds at
    /// least one object.
    fn finish_frame(&mut self) -> Result<(), &'static str> {
        for n in self.frame_blocks.drain(..) {
            if self.blocks.get(&n).is_none_or(|b| b.objects == 0) {
                return Err("block without objects");
            }
        }
        Ok(())
    }

    /// `body_at`: offset in WAL file `self.file` of the frame body holding
    /// `rec`.
    fn apply(&mut self, rec: Record<'a>, body_at: u64) -> Result<(), &'static str> {
        match rec {
            Record::PutDict { number, data } => {
                if number != self.dicts.len() as u64 + 1 {
                    return Err("dictionary number out of sequence");
                }
                self.dicts.push(data);
            }
            Record::PutBlock {
                number,
                segment,
                offset,
                len,
                raw_len,
                dict,
                crc,
            } => {
                if segment == NO_SEGMENT {
                    return Err("PutBlock into the reserved segment id");
                }
                let block = Block {
                    loc: Location::Segment {
                        id: segment,
                        offset,
                    },
                    len,
                    raw_len,
                    dict,
                    crc,
                    objects: 0,
                };
                self.add_batch_block(number, block)?;
                self.segments.insert(segment);
                self.see(segment);
            }
            Record::PutInlineBlock {
                number,
                raw_len,
                dict,
                crc,
                data,
                data_at,
            } => {
                let block = Block {
                    loc: Location::Wal(WalPos {
                        file: self.file,
                        offset: body_at + data_at as u64,
                    }),
                    len: data.len() as u64,
                    raw_len,
                    dict,
                    crc,
                    objects: 0,
                };
                self.add_batch_block(number, block)?;
            }
            Record::PutObject {
                id,
                number,
                block,
                index,
            } => {
                if !self.frame_blocks.contains(&block) {
                    return Err("PutObject outside the frame of its block");
                }
                let b = self
                    .blocks
                    .get_mut(&block)
                    .ok_or("PutObject references an unknown block")?;
                if index != b.objects {
                    return Err("object index out of order");
                }
                b.objects += 1;
                let object = Object {
                    id,
                    block,
                    index,
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
                t.entries.push((number, timestamp));
                if !attrs.is_empty() {
                    let len = u32::try_from(attrs.len()).map_err(|_| "attributes too long")?;
                    let at = WalPos {
                        file: self.file,
                        offset: body_at + attrs_at as u64,
                    };
                    t.attrs.push(AttrsAt { seq, at, len });
                }
            }
            Record::Release { topic, seq } => {
                let topic = self.topic_name(topic)?;
                let t = self
                    .topics
                    .get_mut(topic)
                    .ok_or("Release of an unknown topic")?;
                let &(number, _) = usize::try_from(seq)
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
            Record::Compact {
                old,
                new,
                kept,
                rewritten,
            } => self.compact(old, new, kept, rewritten)?,
        }
        Ok(())
    }

    /// Applies a Compact record. The writer copies blocks whose objects are
    /// all live (`kept`), drops blocks whose objects are all dead, and moves
    /// the live objects of the other blocks of `old` into `rewritten`; any
    /// other combination is corruption.
    fn compact(
        &mut self,
        old: u64,
        new: u64,
        kept: Vec<(u64, u64)>,
        rewritten: Vec<Rewritten>,
    ) -> Result<(), &'static str> {
        if !self.segments.remove(&old) {
            return Err("Compact of a segment the WAL does not reference");
        }
        if new == NO_SEGMENT {
            if !kept.is_empty() || !rewritten.is_empty() {
                return Err("Compact keeps blocks without a new segment");
            }
        } else if !self.segments.insert(new) {
            return Err("Compact into a segment already in use");
        }
        self.see(old);
        if new != NO_SEGMENT {
            self.see(new);
        }
        let old_blocks: HashSet<u64> = self
            .blocks
            .iter()
            .filter(|(_, b)| matches!(b.loc, Location::Segment { id, .. } if id == old))
            .map(|(&n, _)| n)
            .collect();
        let kept_set: HashMap<u64, u64> = kept.iter().copied().collect();
        if kept_set.len() != kept.len() || kept.iter().any(|(n, _)| !old_blocks.contains(n)) {
            return Err("Compact keeps a block that is not in the old segment");
        }
        // Object number → (rewritten block, index in it).
        let moved: HashMap<u64, (u64, u64)> = rewritten
            .iter()
            .flat_map(|r| r.objects.iter().zip(0..).map(|(&n, i)| (n, (r.block, i))))
            .collect();
        let listed: usize = rewritten.iter().map(|r| r.objects.len()).sum();
        if rewritten.iter().any(|r| r.objects.is_empty()) || moved.len() != listed {
            return Err("Compact rewrites a block with no or repeated objects");
        }
        let mut relocated = 0;
        let mut bad = None;
        let ids = &mut self.ids;
        self.objects.retain(|number, o| {
            if !old_blocks.contains(&o.block) {
                return true;
            }
            if kept_set.contains_key(&o.block) {
                if o.refs == 0 {
                    bad = Some("Compact keeps a block with dead objects");
                }
                return true;
            }
            match (moved.get(number), o.refs > 0) {
                (Some(&(block, index)), true) => {
                    o.block = block;
                    o.index = index;
                    relocated += 1;
                    true
                }
                (None, false) => {
                    ids.remove(&o.id);
                    false
                }
                _ => {
                    bad = Some("Compact moved set differs from the live objects of the segment");
                    true
                }
            }
        });
        if let Some(reason) = bad {
            return Err(reason);
        }
        if relocated != moved.len() {
            return Err("Compact moved set differs from the live objects of the segment");
        }
        self.blocks
            .retain(|n, _| !old_blocks.contains(n) || kept_set.contains_key(n));
        for (n, offset) in kept {
            if let Some(b) = self.blocks.get_mut(&n) {
                b.loc = Location::Segment { id: new, offset };
            }
        }
        for r in rewritten {
            let block = Block {
                loc: Location::Segment {
                    id: new,
                    offset: r.offset,
                },
                len: r.len,
                raw_len: r.raw_len,
                dict: r.dict,
                crc: r.crc,
                objects: r.objects.len() as u64,
            };
            self.add_block(r.block, block)?;
        }
        Ok(())
    }
}

/// Deletes segment files the WAL does not reference, opens the others and
/// checks that every block lies within its segment file.
async fn open_segments(
    dir: &Path,
    replay: &mut Replay<'_>,
    report: &mut RecoveryReport,
) -> Result<HashSet<u64>, StoreError> {
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

    // Lengths only: segments are opened for reads on demand (D-227 A).
    let mut segments = HashSet::new();
    let mut lens = HashMap::new();
    for &id in &replay.segments {
        match compio::fs::metadata(segment_path(dir, id)).await {
            Ok(meta) => {
                lens.insert(id, meta.len());
                segments.insert(id);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    // The lowest-numbered object of each block names it in errors.
    let mut first: HashMap<u64, (u64, ObjectId)> = HashMap::new();
    for (&number, o) in &replay.objects {
        let e = first.entry(o.block).or_insert((number, o.id));
        if number < e.0 {
            *e = (number, o.id);
        }
    }
    for (n, b) in &replay.blocks {
        // Inline blocks are inside the WAL frames themselves.
        let Location::Segment {
            id: segment,
            offset,
        } = b.loc
        else {
            continue;
        };
        let end = offset.checked_add(b.len);
        if lens
            .get(&segment)
            .is_none_or(|&len| end.is_none_or(|end| end > len))
        {
            return Err(match first.get(n) {
                Some(&(_, id)) => StoreError::MissingObject(id),
                None => StoreError::CorruptBlock {
                    block: *n,
                    reason: "stored bytes missing or truncated",
                },
            });
        }
    }
    Ok(segments)
}

/// A topic at open: its name, its entries from the WAL, its first live
/// seq and where its entries' key and headers lie.
type WalTopic<'r> = (&'r [u8], &'r [(u64, u64)], u64, &'r [AttrsAt]);

/// A seek page found in an index segment: its location and body.
type FoundPage<'b> = (IndexLoc, &'b [u8]);

/// Checks the index segments against the WAL and rebuilds every topic's
/// state. Index records are derived from the WAL and never synced, so an
/// unwritten, torn or damaged record ends what is read of its segment, and
/// the blocks and seek pages no segment holds intact are re-encoded and
/// appended (`TopicIndexRepaired`). A record whose stamp is above its
/// segment's limit (the lowest `created_at` of the later segments and the
/// WAL's frame count) describes frames recovery cut: it is stale and
/// ignored. Any other intact block record must equal the WAL's encoding of
/// its block: otherwise `TopicMismatch` or `IndexSegmentCorrupt`, and no
/// file is changed.
async fn open_index(
    dir: &Path,
    replay: &Replay<'_>,
    next_frame: u64,
    config: &Config,
    report: &mut RecoveryReport,
) -> Result<(IndexSegments, HashMap<Vec<u8>, Topic>), StoreError> {
    let mut index = IndexSegments::new(dir, config);
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        if let Some(id) = entry?.file_name().to_str().and_then(parse_index_name) {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    index.next_id = ids.last().map_or(0, |m| m + 1);
    if config.rebuild_index {
        // Nothing of the old index is read: it is derived from the WAL
        // again in full below. Ids are not reused.
        for &id in &ids {
            let path = index_path(dir, id);
            let bytes = compio::fs::metadata(&path).await?.len();
            compio::fs::remove_file(&path).await?;
            report
                .actions
                .push(RecoveryAction::IndexSegmentDropped { segment: id, bytes });
        }
        if !ids.is_empty() {
            sync_dir(dir).await?;
        }
        ids.clear();
    }
    // (id, created_at, bytes)
    let mut segs = Vec::new();
    let mut orphans = Vec::new();
    for id in ids {
        let bytes = compio::fs::read(index_path(dir, id)).await?;
        match codec::decode_index_header(&bytes) {
            Some(created_at) => segs.push((id, created_at, bytes)),
            None => orphans.push((id, bytes.len() as u64)),
        }
    }

    // Topics by number with their WAL entries; every PutTopic comes with
    // the topic's first Publish.
    let topics: Vec<WalTopic<'_>> = replay
        .topic_names
        .iter()
        .map(|&name| {
            replay
                .topics
                .get(name)
                .map_or((name, &[][..], 0, &[][..]), |t| {
                    (name, &t.entries[..], t.first_live, &t.attrs[..])
                })
        })
        .collect();
    let block = INDEX_BLOCK as usize;
    let mut blocks: Vec<Vec<Option<IndexLoc>>> = topics
        .iter()
        .map(|(_, entries, _, _)| vec![None; entries.len() / block])
        .collect();
    let mut pages: HashMap<(u64, u8, u64), Vec<FoundPage<'_>>> = HashMap::new();
    // fence[i]: the lowest created_at of the segments after segment i.
    let mut fence = vec![u64::MAX; segs.len()];
    for i in (1..segs.len()).rev() {
        fence[i - 1] = fence[i].min(segs[i].1);
    }
    let mut unfenced = 0;
    for (i, (id, _, bytes)) in segs.iter().enumerate() {
        let limit = fence[i].min(next_frame);
        let mut pos = INDEX_HEADER;
        while let Some(rec) = codec::decode_index_record(&bytes[pos..]) {
            let offset = pos as u64;
            pos += rec.size;
            if rec.stamp > limit {
                // Stale. Fenced already if a later segment's created_at
                // lies below its stamp.
                if rec.stamp <= fence[i] {
                    unfenced += 1;
                }
                continue;
            }
            let corrupt = |reason| StoreError::IndexSegmentCorrupt {
                segment: *id,
                offset,
                reason,
            };
            let loc = IndexLoc {
                segment: u32::try_from(*id).map_err(|_| corrupt("segment id above u32"))?,
                len: u32::try_from(rec.size).map_err(|_| corrupt("record too large"))?,
                offset,
            };
            match rec.kind {
                INDEX_RECORD_BLOCK => {
                    let t = usize::try_from(rec.topic)
                        .ok()
                        .filter(|&t| t < topics.len())
                        .ok_or_else(|| corrupt("index block of a topic the WAL does not know"))?;
                    let slot = usize::try_from(rec.number)
                        .ok()
                        .and_then(|b| blocks[t].get_mut(b))
                        .ok_or_else(|| corrupt("index block beyond the topic's full blocks"))?;
                    let first = rec.number as usize * block;
                    let (name, entries, _, _) = topics[t];
                    let want = &entries[first..first + block];
                    let got = codec::decode_index_entries(rec.body)
                        .ok_or_else(|| corrupt("index block does not decode"))?;
                    if got != want {
                        // Written that way: the record describes durable
                        // frames only.
                        let at = got.iter().zip(want).take_while(|(a, b)| a == b).count();
                        return Err(StoreError::TopicMismatch {
                            topic: name.to_vec(),
                            seq: (first + at) as u64,
                        });
                    }
                    slot.get_or_insert(loc);
                }
                INDEX_RECORD_SEEK_PAGE => {
                    let (level, _) = codec::seek_page_shape(rec.body)
                        .ok_or_else(|| corrupt("seek page does not decode"))?;
                    pages
                        .entry((rec.topic, level, rec.number))
                        .or_default()
                        .push((loc, rec.body));
                }
                _ => return Err(corrupt("unknown index record kind")),
            }
        }
    }

    // Changes start here, after every check passed.
    for &(id, bytes) in &orphans {
        compio::fs::remove_file(index_path(dir, id)).await?;
        report
            .actions
            .push(RecoveryAction::IndexSegmentRemoved { segment: id, bytes });
    }
    if !orphans.is_empty() {
        sync_dir(dir).await?;
    }
    if unfenced > 0 {
        // New frames will reuse the stale records' frame numbers: a segment
        // created now, at `next_frame`, keeps them stale for good.
        index.start(next_frame).await?;
        report
            .actions
            .push(RecoveryAction::StaleIndexRecordsFenced {
                records: unfenced,
                segment: index.next_id - 1,
            });
    }

    let fan = index.fanout;
    let mut out = HashMap::with_capacity(topics.len());
    for (number, ((name, entries, first_live, attrs), found)) in
        (0u64..).zip(topics.iter().zip(blocks))
    {
        let mut blocks_rewritten = 0;
        let mut locs = Vec::with_capacity(found.len());
        for (b, slot) in (0u64..).zip(found) {
            let loc = match slot {
                Some(loc) => loc,
                None => {
                    let first = b as usize * block;
                    let mut body = Vec::new();
                    let mut w = IndexWriter::resume(b * INDEX_BLOCK, 0, 0);
                    for &(n, ts) in &entries[first..first + block] {
                        w.push(&mut body, n, ts);
                    }
                    blocks_rewritten += 1;
                    index
                        .append(INDEX_RECORD_BLOCK, number, b, next_frame, &body, next_frame)
                        .await?
                }
            };
            locs.push(loc);
        }
        let mut record_bytes: u64 = locs.iter().map(|l| u64::from(l.len)).sum();
        // Seek pages level by level; a page is reused if a segment holds
        // one with exactly the expected locations.
        let mut pages_rewritten = 0;
        let mut spine = Vec::new();
        let mut level_locs = locs;
        for level in 0u8.. {
            let mut upper = Vec::with_capacity(level_locs.len() / fan);
            for (k, group) in (0u64..).zip(level_locs.chunks_exact(fan)) {
                let want = codec::seek_page_body(level, group);
                let reused = pages
                    .get(&(number, level, k))
                    .and_then(|c| c.iter().find(|(_, body)| *body == want.as_slice()));
                let loc = match reused {
                    Some(&(loc, _)) => loc,
                    None => {
                        pages_rewritten += 1;
                        index
                            .append(
                                INDEX_RECORD_SEEK_PAGE,
                                number,
                                k,
                                next_frame,
                                &want,
                                next_frame,
                            )
                            .await?
                    }
                };
                record_bytes += u64::from(loc.len);
                upper.push(loc);
            }
            spine.push(level_locs[upper.len() * fan..].to_vec());
            if upper.is_empty() {
                break;
            }
            level_locs = upper;
        }
        if blocks_rewritten + pages_rewritten > 0 {
            report.actions.push(RecoveryAction::TopicIndexRepaired {
                topic: name.to_vec(),
                blocks_rewritten,
                pages_rewritten,
            });
        }
        let tail_start = entries.len() - entries.len() % block;
        let (prev_number, prev_timestamp) =
            tail_start.checked_sub(1).map_or((0, 0), |i| entries[i]);
        let mut writer = IndexWriter::resume(tail_start as u64, prev_number, prev_timestamp);
        let mut tail = Vec::new();
        for &(n, ts) in &entries[tail_start..] {
            writer.push(&mut tail, n, ts);
        }
        out.insert(
            name.to_vec(),
            Topic {
                number,
                index: writer,
                tail,
                spine,
                first_live: *first_live,
                read_cache: RefCell::new(None),
                record_bytes,
                attrs: attrs
                    .iter()
                    .filter(|a| a.seq >= *first_live)
                    .copied()
                    .collect(),
            },
        );
    }
    index.flush().await?;
    // Recovery work is not counted in `Stats`.
    index.fsyncs = 0;
    Ok((index, out))
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

/// Reads every WAL file `wal-<first frame seq>.log`, in order, creating
/// the first with its file header if there is none. Leftover
/// `wal-*.log.tmp` files of an interrupted creation are deleted
/// (`WalTmpRemoved`). A directory holding the single `wal.log` of an
/// earlier format is `UnsupportedFormat`, nothing changed.
async fn read_wal_files(
    dir: &Path,
    report: &mut RecoveryReport,
    dict_train_bytes: u64,
) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
    if dir.join(LEGACY_WAL_FILE).exists() {
        return Err(StoreError::UnsupportedFormat);
    }
    let (mut ids, mut tmps) = (Vec::new(), Vec::new());
    let mut segments = false;
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(id) = parse_wal_name(name) {
            ids.push(id);
        } else if name.strip_suffix(".tmp").and_then(parse_wal_name).is_some() {
            tmps.push(name.to_owned());
        } else if parse_segment_name(name).is_some() || parse_index_name(name).is_some() {
            segments = true;
        }
    }
    if ids.is_empty() && segments {
        // A new store would delete the segments as orphans: refuse.
        return Err(StoreError::CorruptWal {
            file: 0,
            offset: 0,
            reason: "no WAL file, but data or index segments exist",
        });
    }
    for tmp in &tmps {
        compio::fs::remove_file(dir.join(tmp)).await?;
        report.actions.push(RecoveryAction::WalTmpRemoved);
    }
    ids.sort_unstable();
    if ids.is_empty() {
        create_wal_file(dir, 0, dict_train_bytes).await?;
        ids.push(0);
    } else {
        // Makes a removed tmp file, and the directory, durable.
        sync_dir(dir).await?;
    }
    let mut files = Vec::with_capacity(ids.len());
    for id in ids {
        files.push((id, compio::fs::read(wal_path(dir, id)).await?));
    }
    Ok(files)
}

/// Creates WAL file `wal-<first>.log` holding only its file header: written
/// to `wal-<first>.log.tmp`, synced, renamed, and the directory synced, so
/// a WAL file never exists without a complete header. Returns it open for
/// reads and appends.
async fn create_wal_file(dir: &Path, first: u64, dict_train_bytes: u64) -> io::Result<File> {
    let path = wal_path(dir, first);
    let tmp = path.with_extension("log.tmp");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await?;
    write_all(&file, codec::wal_file_header(dict_train_bytes).to_vec(), 0)
        .await
        .0?;
    file.sync_all().await?;
    compio::fs::rename(&tmp, &path).await?;
    sync_dir(dir).await?;
    OpenOptions::new().read(true).write(true).open(&path).await
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

/// Codecs for the dictionaries of a WAL, numbered from 1 in order; an empty
/// one (training built none) gets no decoder.
fn codecs_for(dicts: &[&[u8]]) -> Result<Codecs, StoreError> {
    let mut codecs = Codecs::new()?;
    for (number, dict) in (1..).zip(dicts) {
        if !<[u8]>::is_empty(dict) {
            codecs.add_dictionary(number, dict)?;
        }
    }
    Ok(codecs)
}

/// Decompresses a block's stored bytes and checks them against the WAL: raw
/// length, CRC32C, layout and object count.
fn decode_block(codecs: &mut Codecs, b: &Block, stored: &[u8]) -> Result<Decoded, &'static str> {
    let raw = codecs.decompress(b.dict, stored, b.raw_len, b.crc)?;
    let spans = codec::split_block(&raw)?;
    if spans.len() as u64 != b.objects {
        return Err("block holds another number of objects than the WAL says");
    }
    Ok(Decoded { raw, spans })
}

/// A block of the last frame that cannot back its objects.
struct BadObject {
    /// The first object of the block, or the object that failed its hash.
    object: ObjectId,
    block: u64,
    reason: DropReason,
    why: &'static str,
}

/// The first block the records introduce that is not fully stored, does not
/// decode, or holds an object whose bytes do not match its BLAKE3 id. Used
/// on the last WAL frame only; earlier frames get a presence and bounds
/// check (`open_segments`), their content is verified on read.
async fn first_bad_object(
    dir: &Path,
    records: &[Record<'_>],
    codecs: &mut Codecs,
) -> Result<Option<BadObject>, StoreError> {
    for rec in records {
        let (number, loc, len, raw_len, dict, crc, inline) = match rec {
            Record::PutBlock {
                number,
                segment,
                offset,
                len,
                raw_len,
                dict,
                crc,
            } => (
                *number,
                Some((*segment, *offset)),
                *len,
                *raw_len,
                *dict,
                *crc,
                None,
            ),
            Record::PutInlineBlock {
                number,
                raw_len,
                dict,
                crc,
                data,
                ..
            } => (
                *number,
                None,
                data.len() as u64,
                *raw_len,
                *dict,
                *crc,
                Some(*data),
            ),
            _ => continue,
        };
        // In index order: replay rejects any other.
        let objects: Vec<ObjectId> = records
            .iter()
            .filter_map(|r| match r {
                Record::PutObject { id, block, .. } if *block == number => Some(*id),
                _ => None,
            })
            .collect();
        // Replay rejects a block without objects.
        let Some(&first) = objects.first() else {
            continue;
        };
        let bad = |object, reason, why| {
            Ok(Some(BadObject {
                object,
                block: number,
                reason,
                why,
            }))
        };
        let stored = match (inline, loc) {
            (Some(data), _) => data.to_vec(),
            (None, Some((segment, offset))) => {
                let file = match File::open(segment_path(dir, segment)).await {
                    Ok(file) => file,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        return bad(first, DropReason::ObjectMissing, "");
                    }
                    Err(e) => return Err(e.into()),
                };
                let Ok(len) = usize::try_from(len) else {
                    return bad(first, DropReason::ObjectMissing, "");
                };
                let BufResult(res, buf) = file
                    .read_exact_at(Vec::with_capacity(len).slice(..len), offset)
                    .await;
                match res {
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        return bad(first, DropReason::ObjectMissing, "");
                    }
                    res => res?,
                }
                buf.into_inner()
            }
            (None, None) => continue,
        };
        let block = Block {
            loc: Location::Wal(WalPos::default()),
            len,
            raw_len,
            dict,
            crc,
            objects: objects.len() as u64,
        };
        let d = match decode_block(codecs, &block, &stored) {
            Ok(d) => d,
            Err(why) => return bad(first, DropReason::BlockCorrupted, why),
        };
        for (id, span) in objects.iter().zip(&d.spans) {
            if ObjectId::of(&d.raw[span.clone()]) != *id {
                return bad(*id, DropReason::ObjectHashMismatch, "");
            }
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

fn wal_path(dir: &Path, first: u64) -> PathBuf {
    dir.join(format!("wal-{first:016x}.log"))
}

fn parse_wal_name(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

fn index_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("idx-{id:016x}.dat"))
}

fn parse_index_name(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("idx-")?.strip_suffix(".dat")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(id: ObjectId, refs: u64) -> Object {
        Object {
            id,
            block: 0,
            index: 0,
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

    fn put_block(number: u64, segment: u64, offset: u64) -> Record<'static> {
        Record::PutBlock {
            number,
            segment,
            offset,
            len: 1,
            raw_len: 2,
            dict: NO_DICT,
            crc: 0,
        }
    }

    fn put(id: ObjectId, number: u64, block: u64, index: u64) -> Record<'static> {
        Record::PutObject {
            id,
            number,
            block,
            index,
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
        assert_eq!(
            r.apply(put(id, 0, 0, 0), 0),
            Err("PutObject outside the frame of its block")
        );
        r.apply(put_block(0, 0, 0), 0).unwrap();
        assert_eq!(
            r.apply(put(id, 0, 0, 1), 0),
            Err("object index out of order")
        );
        r.apply(put(id, 0, 0, 0), 0).unwrap();
        assert_eq!(
            r.apply(put(id, 1, 0, 1), 0),
            Err("new object already in the index"),
            "same id twice"
        );
        assert_eq!(
            r.apply(put(ObjectId::of(b"y"), 0, 0, 2), 0),
            Err("object number not above every earlier one"),
            "number reused"
        );
        r.finish_frame().unwrap();
        assert_eq!(
            r.apply(put(ObjectId::of(b"z"), 5, 0, 3), 0),
            Err("PutObject outside the frame of its block"),
            "an earlier frame's block"
        );
        assert_eq!(
            r.apply(put_block(0, 0, 1), 0),
            Err("block number not above every earlier one")
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
        // The object is dead now: a Compact that keeps its block is
        // inconsistent.
        let keep = Record::Compact {
            old: 0,
            new: 1,
            kept: vec![(0, 0)],
            rewritten: vec![],
        };
        assert!(r.apply(keep, 0).is_err());

        let mut r = Replay::default();
        r.apply(put_block(3, 0, 0), 0).unwrap();
        assert_eq!(r.finish_frame(), Err("block without objects"));
        let mut b = put_block(4, 0, 0);
        if let Record::PutBlock { dict, .. } = &mut b {
            *dict = 1;
        }
        assert_eq!(r.apply(b, 0), Err("block references an unknown dictionary"));
        assert_eq!(
            r.apply(
                Record::PutDict {
                    number: 2,
                    data: b"d"
                },
                0
            ),
            Err("dictionary number out of sequence")
        );
    }

    /// Segment 0 holds block 0 (objects a live, b dead), block 1 (c live)
    /// and block 2 (d dead).
    fn replay_for_compaction() -> (Replay<'static>, [ObjectId; 4]) {
        let ids = [b"a", b"b", b"c", b"d"].map(|b| ObjectId::of(b));
        let mut r = replay_with_topic();
        r.apply(put_block(0, 0, 0), 0).unwrap();
        r.apply(put(ids[0], 0, 0, 0), 0).unwrap();
        r.apply(put(ids[1], 1, 0, 1), 0).unwrap();
        r.apply(put_block(1, 0, 1), 0).unwrap();
        r.apply(put(ids[2], 2, 1, 0), 0).unwrap();
        r.apply(put_block(2, 0, 2), 0).unwrap();
        r.apply(put(ids[3], 3, 2, 0), 0).unwrap();
        r.finish_frame().unwrap();
        r.apply(publish(0, 0), 0).unwrap();
        r.apply(publish(1, 2), 0).unwrap();
        (r, ids)
    }

    fn rewritten(objects: Vec<u64>) -> Vec<Rewritten> {
        vec![Rewritten {
            block: 3,
            offset: 1,
            len: 1,
            raw_len: 2,
            dict: NO_DICT,
            crc: 0,
            objects,
        }]
    }

    #[test]
    fn replay_applies_compaction_and_keeps_numbers() {
        let (mut r, ids) = replay_for_compaction();
        r.apply(
            Record::Compact {
                old: 0,
                new: 1,
                kept: vec![(1, 0)],
                rewritten: rewritten(vec![0]),
            },
            0,
        )
        .unwrap();
        assert!(!r.objects.contains_key(&1), "dead object dropped");
        assert!(!r.objects.contains_key(&3), "dead block's object dropped");
        assert!(!r.ids.contains_key(&ids[1]), "and its id");
        let a = &r.objects[&0];
        assert_eq!((a.id, a.block, a.index, a.refs), (ids[0], 3, 0, 1));
        let c = &r.objects[&2];
        assert_eq!((c.block, c.index), (1, 0), "kept block keeps its objects");
        assert_eq!(r.blocks[&1].loc, Location::Segment { id: 1, offset: 0 });
        assert_eq!(r.blocks[&3].loc, Location::Segment { id: 1, offset: 1 });
        assert!(!r.blocks.contains_key(&0) && !r.blocks.contains_key(&2));
        assert_eq!(r.segments, HashSet::from([1]));
        assert_eq!(r.max_segment, Some(1));
        // Numbers of dropped objects and blocks are not reused.
        assert_eq!((r.next_object, r.next_block), (4, 4));
    }

    #[test]
    fn replay_rejects_inconsistent_compaction() {
        let compact = |kept, rewritten| Record::Compact {
            old: 0,
            new: 1,
            kept,
            rewritten,
        };
        for (what, rec) in [
            ("live object left behind", compact(vec![(1, 0)], vec![])),
            (
                "dead object moved",
                compact(vec![(1, 0)], rewritten(vec![0, 1])),
            ),
            (
                "block with a dead object kept",
                compact(vec![(0, 0), (1, 0)], vec![]),
            ),
            (
                "block of another segment",
                compact(vec![(1, 0), (9, 0)], rewritten(vec![0])),
            ),
            (
                "object moved twice",
                compact(vec![(1, 0)], rewritten(vec![0, 0])),
            ),
            ("live block dropped", compact(vec![], rewritten(vec![0]))),
        ] {
            let (mut r, _) = replay_for_compaction();
            assert!(r.apply(rec, 0).is_err(), "{what}");
        }
    }

    fn decoded(raw_len: usize) -> Rc<Decoded> {
        Rc::new(Decoded {
            raw: vec![0; raw_len],
            spans: Vec::new(),
        })
    }

    /// A block of `raw_len` heap bytes as the cache counts it.
    fn counted(raw_len: usize) -> usize {
        raw_len + CACHE_ENTRY_OVERHEAD
    }

    fn cached_numbers(c: &BlockCache) -> Vec<u64> {
        c.order.values().copied().collect()
    }

    #[test]
    fn block_cache_does_not_keep_a_block_larger_than_its_byte_limit() {
        let limit = counted(1000);
        let mut c = BlockCache::new(limit);
        c.insert(1, decoded(400));
        c.insert(2, decoded(1001));
        assert!(c.get(2).is_none());
        assert_eq!(
            cached_numbers(&c),
            [1],
            "the oversized block evicts nothing"
        );
        assert_eq!(c.bytes, counted(400));
        c.insert(3, decoded(1000));
        assert_eq!(
            cached_numbers(&c),
            [3],
            "a block of exactly the limit is kept"
        );
        assert_eq!(c.bytes, limit);
    }

    #[test]
    fn block_cache_evicts_least_recently_used_by_bytes() {
        let mut c = BlockCache::new(counted(300) * 3 + 100);
        for n in 1..=3 {
            c.insert(n, decoded(300));
        }
        assert!(c.get(1).is_some(), "1 becomes the most recently used");
        c.insert(4, decoded(300));
        assert_eq!(cached_numbers(&c), [3, 1, 4], "2 evicted");
        c.insert(5, decoded(600));
        assert_eq!(cached_numbers(&c), [4, 5]);
        c.remove(4);
        assert_eq!(cached_numbers(&c), [5]);
        assert_eq!(c.bytes, counted(600));
    }

    #[test]
    fn block_cache_holds_many_small_blocks_up_to_its_bytes_and_none_at_zero() {
        // 16 KiB blocks in a 1 MiB cache: the bytes govern, not a count.
        let limit = 1 << 20;
        let mut c = BlockCache::new(limit);
        for n in 0..200 {
            c.insert(n, decoded(16 << 10));
        }
        let fit = limit / counted(16 << 10);
        assert_eq!(c.entries.len(), fit);
        assert_eq!(c.order.len(), fit);
        assert!(c.bytes <= limit);
        assert_eq!(
            cached_numbers(&c),
            (200 - fit as u64..200).collect::<Vec<_>>()
        );
        c.insert(199, decoded(30));
        assert_eq!(c.entries.len(), fit, "replaced, not added");
        // Tiny blocks: the per-entry overhead keeps the count bounded.
        let mut tiny = BlockCache::new(1 << 16);
        for n in 0..10_000 {
            tiny.insert(n, decoded(1));
        }
        assert_eq!(tiny.entries.len(), (1 << 16) / counted(1));
        let mut none = BlockCache::new(0);
        none.insert(1, decoded(10));
        assert!(none.get(1).is_none());
        assert_eq!(none.bytes, 0);
    }

    #[test]
    fn block_ranges_fill_up_to_the_cap_and_isolate_oversized_objects() {
        assert_eq!(block_ranges([], 10), Vec::<Range<usize>>::new());
        assert_eq!(block_ranges([3, 3, 3, 3], 10), [0..3, 3..4]);
        assert_eq!(
            block_ranges([10, 1], 10),
            [0..1, 1..2],
            "exactly the cap fits"
        );
        assert_eq!(
            block_ranges([1, 25, 1, 1], 10),
            [0..1, 1..2, 2..4],
            "oversized alone"
        );
        assert_eq!(block_ranges([25], 10), vec![0..1]);
        assert_eq!(
            block_ranges([1, 1, 1], 0),
            [0..1, 1..2, 2..3],
            "0 is taken as 1"
        );
        // Two consecutive ranges together exceed the cap.
        let lens = [4, 7, 4, 7, 4, 7];
        let r = block_ranges(lens, 10);
        for w in r.windows(2) {
            let size = |x: &Range<usize>| lens[x.clone()].iter().sum::<usize>();
            assert!(size(&w[0]) + size(&w[1]) > 10, "{r:?}");
        }
    }

    #[test]
    fn segment_and_index_names_round_trip() {
        let dir = Path::new("/d");
        let p = segment_path(dir, 0xab);
        let name = p.file_name().unwrap().to_str().unwrap();
        assert_eq!(parse_segment_name(name), Some(0xab));
        assert_eq!(parse_segment_name("seg-1.dat"), None);
        let p = index_path(dir, 0xcd);
        assert_eq!(p, Path::new("/d/idx-00000000000000cd.dat"));
        let name = p.file_name().unwrap().to_str().unwrap();
        assert_eq!(parse_index_name(name), Some(0xcd));
        assert_eq!(parse_index_name("idx-cd.dat"), None);
    }
}
