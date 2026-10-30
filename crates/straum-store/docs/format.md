# On-disk formats

All integers are little-endian. Every variable-length record carries a length
prefix. CRC32C is Castagnoli (crate `crc32c`). An object id is the 32-byte
BLAKE3 hash of the object's raw bytes. Code: `src/codec.rs` (WAL, raw
blocks, topic index entries), `src/block.rs` (zstd) and `src/store.rs` (file
names, segments).

## Store directory

```text
<dir>/
├── wal-<seq:016x>.log           the log, in files named by their first frame's sequence number
├── wal-<seq:016x>.log.tmp       only while a WAL file is being created; deleted at open
├── seg-<id:016x>.dat            data segments; id is a u64 in hex, 16 digits
├── idx-<id:016x>.dat            index segments: topic index blocks, attribute records and seek pages of every topic
├── objtab-<first:016x>.tab      object tables written by checkpoints, by their first object number
├── blktab-<first:016x>.tab      block tables written by checkpoints, by their first block number
├── blkrel-<n:016x>.run          block relocation runs, by the checkpoint that wrote them
├── dict-<number:016x>.bin       the dictionary, written by the first checkpoint after training
├── idrun-<n:016x>.run           id runs (id → object number), by the checkpoint that wrote them
├── reloc-<n:016x>.run           relocation runs, by the checkpoint that wrote them
├── checkpoint-<n:016x>          the checkpoint manifest (one; n from 1, never repeated)
├── checkpoint-<n:016x>.tmp      only while a manifest is being written; deleted at open
└── quarantine-<n:016x>[.<k>]/   a damaged manifest and the tables and runs it may list, moved
                                 unchanged; kept for diagnosis until an operator removes it, never read
```

A topic name is 1 to 100 bytes (`MAX_TOPIC`). Segment ids are never reused:
the next id is the highest seen in the WAL or on disk plus one. Index segment
ids are a separate sequence: the highest on disk plus one, at most
`u32::MAX`.

## Blocks

Objects are stored in blocks of at most `Config::max_block_raw_bytes` raw
bytes (default 16 KiB; 64 KiB stays a setting). The new objects of one batch (`put_batch`), in order
of first appearance in the batch, are split in that order into blocks: a
block is closed when the next object's entry would take it over the limit,
and an object whose entry alone is over the limit is a block of its own
(objects are never split). Compaction moves the live objects of partly dead
blocks into blocks under the same limit. A batch whose objects are all known
makes no block. All blocks of a batch are in its one WAL frame and follow
the frame's durability order and recovery rules, as a single block does.

The limit bounds the work of reading one object: a read decompresses one
block. It is not recorded in the WAL; changing it affects new blocks only.

Raw block: every object as `len varint | bytes`, concatenated; object `i` of
the block is the `i`-th. A zero-length object takes one byte.

Stored block: the raw block compressed with zstd level 1 as one zstd frame,
with the store's dictionary once one exists. The WAL records the stored
length, the raw length, the dictionary number (0: none) and the CRC32C of
the raw block. Reading an object decompresses its block, checks the raw
length, the CRC and that the block splits into exactly the recorded number
of objects (any failure is `CorruptBlock`, never data), then checks the
object's BLAKE3 (`HashMismatch`). The last decompressed blocks are cached,
least recently used first out, within `Config::block_cache_bytes` (default
64 MiB) of raw bytes, object offsets and 160 bytes per block for the
cache's entries; no count limit. A block larger than the byte limit is not
cached.

Block numbers start at 0 and are never reused: the next number is the
highest in the WAL plus one.

### Dictionary

One dictionary per store, trained once. When the raw bytes of new objects
stored in blocks without a dictionary reach the threshold in the WAL file
header (`Config::dict_train_bytes` when the WAL was created; default 1 MiB;
0 disables training), the next non-empty batch whose frame still fits
`Config::max_frame_bytes` with the PutDict record (its upper bound,
64 KiB + 21 bytes) added trains a
dictionary of at most 64 KiB on the objects of those blocks, in block order,
at most 10 MiB of samples. The batch's frame carries it in a PutDict record
before its block, and every later block, compaction's included, uses it.
Earlier blocks keep no dictionary. Training reads only durable blocks, so a
batch retried after a crash trains the same dictionary; replay reads the
dictionary from the WAL and never trains. If zstd builds no dictionary (too
few or too uniform samples), the PutDict record is empty and training is not
tried again.

## Data segment `seg-<id>.dat`

Stored blocks concatenated, without headers or framing:

```text
offset 0                                                       file len
| block A (len_A bytes) | block B (len_B bytes) | ... | orphan tail? |
```

Only the WAL knows where a block lies (PutBlock and Compact records). Bytes
no WAL record points to (orphans of a crash before the WAL fsync) are
allowed: compaction removes them.

## WAL `wal-<seq>.log`

The WAL is a sequence of files. Each is named by the sequence number of
its first frame (16 hex digits) and holds a file header (24 bytes), then
frames. One frame is one batch of messages (`put_batch`), one
`release_before` call, or one compaction step (one rewritten segment).
Frame sequence numbers run on across files: a file starts with the frame
after the previous file's last.

**Rotation.** Before a write builds its frame, a file that holds at least
one frame and at least `Config::wal_file_max_bytes` (default 64 MiB) is
closed and a new file is created, named by the next frame's sequence
number; the frame goes there. A file can exceed the limit by one frame.
Positions inside frames (inline blocks, keys and headers) name the file
and the offset in it. Files are created as below, so a WAL file never
exists without a complete header, and a file exists only after the last
frame of the file before it is durable. No WAL file is deleted yet: a
checkpoint will delete the files wholly before the frame it covers
(ADR-0014 §3).

### File header

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `magic` = `53 54 52 4d 57 41 4c 00` (`"STRMWAL\0"`) |
| 8 | 4 | `version` u32 = 21 |
| 12 | 8 | `dict_train_bytes` u64: the dictionary training threshold |
| 20 | 4 | `crc` u32: CRC32C of bytes `[0..20)` |

`dict_train_bytes` is written from `Config::dict_train_bytes` when the
WAL's first file is created, then copied into every later file's header,
and never changes; files that disagree are `CorruptWal`. At open the store uses the header's
value: a different `Config::dict_train_bytes` is ignored and listed in
`RecoveryReport::config_overrides` (`ConfigOverride::DictTrainBytes`), so a
reopen cannot move a trigger that has not fired.

A new WAL file is written to `wal-<seq>.log.tmp` with its header, synced
(`fsync`), renamed to `wal-<seq>.log`, and then the directory is synced. A
WAL file that does not start with an intact 24-byte header of this version
(an empty file, another version, a header CRC mismatch), and a directory
holding the single `wal.log` of a version before 13, are
`UnsupportedFormat`, and no file is changed. Earlier versions are not supported: 1 had no mode byte in the
frame, 2 full ids in Publish and in the topic index, 3 `topic_seq` in topic
index entries, 4 fixed-width Publish and Release with the topic name, 5
fixed-width PutObject and PutInline, 6 a topic index of 20-byte entries, 7
uncompressed objects located by segment offset (PutObject type 1, PutInline
type 5, Compact type 4) and Publish without the publication's key and
headers; 8 is two versions: on `main` version 7 with key and headers, on
the block branch a 16-byte header without `dict_train_bytes`; 9 at most one
rewritten block per Compact record, 10 one index file per topic
(`topics/<hex>.idx`), 11 Publish without the publication's key and
headers, 12 one WAL file, `wal.log`, 13 keys and headers of full index
blocks only in the WAL (no attribute records), 14 no checkpoint files, 15 reference counts
checked at open and in Compact replay, 16 damaged manifests deleted and
checkpoint numbers reused after a fallback (so an older build must not
write a directory that holds quarantined manifests), 17 a damaged manifest
quarantined alone as `checkpoint-<n>.damaged` (its tables and runs deleted), 18 no block
tables, block relocation runs or dictionary file, and a manifest without the next
segment number, the training state, the dictionary and the live segments, 19 no id
runs (every id in memory), 20 each id under at most one live number (no
degraded deduplication: replay refused a new object whose id was already
indexed, and an id run held an id once). The number keeps increasing
with each format change on this line; none is reused (D-251 A).

### Frame

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `len` u32: body length |
| 4 | 8 | `seq` u64: frame number, 0, 1, 2, … without gaps |
| 12 | 4 | `count` u32: number of records in the body |
| 16 | 1 | `mode` u8: the frame's write mode, 1 Sequential, 2 ParallelSync, 3 InlineSmall |
| 17 | 4 | `crc` u32: CRC32C of bytes `[0..17)` of the frame, then of the body |
| 21 | `len` | body: `count` records in a row |

A frame takes `21 + len` bytes. An unknown `mode` under a valid CRC is
`CorruptWal`.

`mode` tells whether the frame's block could become durable after the frame
itself:

| `mode` | Written by | Block bytes durable before the frame? |
|---|---|---|
| 1 Sequential | a batch in Sequential mode; every `release_before` and compaction frame | yes: the segment fdatasync precedes the frame write; release frames carry no block, and compaction's new segment is synced before its frame |
| 2 ParallelSync | a batch in ParallelSync mode | no: the segment and WAL fdatasyncs run concurrently |
| 3 InlineSmall | a batch in InlineSmall mode | an inline block is inside the frame (CRC); a segment block: yes, Sequential order (`flush_segment`, then `append_frame` in `put_batch_inner`) |

`Config::max_frame_bytes` (default 64 MiB) sets the largest frame. An
operation that would produce a larger frame is rejected with `FrameTooLarge`
before any I/O; for a batch the bound counts every object as new, an inline
block at its threshold or zstd's bound, and a dictionary at 64 KiB when
training is due.

### Record

| Field | Size |
|---|---|
| `len`: payload length, varint | 1–10 |
| `type` u8: 2 Publish, 3 Release, 6 PutTopic, 7 PutBlock, 8 PutInlineBlock, 9 PutObject, 10 PutDict, 11 Compact | 1 |
| payload by type, below | `len` |

A record has no checksum of its own: the frame CRC covers it. Types 1, 4 and
5 belong to version 7 and earlier; under a valid frame CRC any unknown type
is `CorruptWal`.

**varint**: unsigned LEB128, 7 bits per byte, low bits first, the high bit of
a byte meaning "another byte follows"; a u64 takes 1–10 bytes (0–127 one byte,
128 two bytes `80 01`, 2^63 ten bytes). Every value has exactly one encoding: a
truncated varint, one longer than 10 bytes or overflowing u64, and an overlong
one (a last byte `00` with length above 1, such as `80 00`) are corruption
(`CorruptWal`). **zigzag**: a signed value as a varint, 0, −1, 1, −2, … →
0, 1, 2, 3, …. A payload must be read in full, and so must a frame body:
trailing bytes are corruption.

### Object number

Every new object gets a local `object_number` (u64): 0, 1, 2, … in write
order, never reused (the number of an object removed by compaction is not
reused either). Compaction does not change it (only the block and index).
The full BLAKE3 id is written once, in PutObject; Publish and the topic
index refer to the number. In memory: id → number (deduplication), number →
(block, index), and block number → (location, lengths, dictionary,
CRC, object count); all are rebuilt from the WAL. The next number after open
is the highest in the WAL plus one (the WAL is never cut; a future WAL
checkpoint has to store it explicitly).

### Delta bases within a frame

A frame starts with the next object number and the next block number (known
from earlier frames). A PutBlock or PutInlineBlock number is a delta from the
next expected block number (normally 0); it then becomes the frame's current
block, with next index 0. A PutObject number is a delta from the next
expected object number (normally 0). A PutObject block is a zigzag delta from
the frame's current block (normally 0), and its index a delta from the
current block's next index (normally 0); the PutObject then sets both.

### 7 — PutBlock (payload 10–64 bytes)

| Field | Size |
|---|---|
| Δ`block_number`, varint: from the expected next block, normally 0 | 1–10 |
| `segment` varint (never `u64::MAX`) | 1–10 |
| `offset` varint: offset in the segment | 1–10 |
| `len` varint: stored (compressed) length | 1–10 |
| `raw_len` varint | 1–10 |
| `dict` varint: dictionary number, 0 none | 1–10 |
| `crc` u32: CRC32C of the raw block | 4 |

The block number must be above every earlier one. The PutObject records of
its objects follow in the same frame, in index order 0, 1, 2, …; a block
without objects is `CorruptWal`.

### 8 — PutInlineBlock (payload 7–34 + stored length)

Only in `WriteMode::InlineSmall { threshold }` (default 4096): a batch's
block whose stored length is at most the threshold is stored inside the WAL
frame.

| Field | Size |
|---|---|
| Δ`block_number`, varint: as in PutBlock | 1–10 |
| `raw_len` varint | 1–10 |
| `dict` varint | 1–10 |
| `crc` u32 | 4 |
| the stored block, to the end of the payload | the rest |

The block's location is its WAL file and the offset of its stored bytes
in that file. **Inline blocks are never reclaimed.** Compaction rewrites
segments only; reclaiming WAL space needs a WAL checkpoint and truncation,
which do not exist yet.

### 9 — PutObject (payload 35–62 bytes)

| Field | Size |
|---|---|
| `object_id` (BLAKE3), full | 32 |
| Δ`object_number`, varint: from the expected next number, normally 0 | 1–10 |
| Δ`block`, zigzag varint: normally 0 | 1–10 |
| Δ`index`, varint: normally 0 | 1–10 |

The number must be above every earlier number in the WAL, the block must be
introduced by the same frame, and the index must be that block's next one;
otherwise `CorruptWal`. A known id is not written again: Publish refers to
its number.

### 10 — PutDict (payload 1–10 + dictionary length)

| Field | Size |
|---|---|
| `dict_number` varint: 1, 2, … in order | 1–10 |
| dictionary bytes, to the end of the payload; empty: training built none | 0–65536 |

Written before the first block that uses it, in the same frame. A block
referring to a dictionary number without a non-empty PutDict before it is
`CorruptWal`. The writer creates at most one (number 1).

### 6 — PutTopic (payload 1–10 + name length)

| Field | Size |
|---|---|
| `topic_number` varint: 0, 1, 2, … in order, without gaps | 1–10 |
| topic name, 1..=100 bytes (to the end of the payload) | 1–100 |

Written once, in the frame of the topic's first publication, before its first
Publish. A number is never repeated, and a name gets a number once. Recovery
builds the number → name table from these records; a Publish or Release with
a number that has no PutTopic is `CorruptWal`.

### 2 — Publish (payload 4–40 bytes + attributes)

| Field | Size |
|---|---|
| `topic_number` varint | 1–10 |
| `topic_seq` varint | 1–10 |
| Δ`object_number`, zigzag varint | 1–10 |
| Δ`timestamp`, zigzag varint (ms, given by the caller; does not decrease within a topic) | 1–10 |
| attributes: the publication's key and headers, to the end of the payload | 0 if neither |

Attributes (`codec::encode_attrs`), present only if the publication has a
key or at least one header:

| Field | Encoding |
|---|---|
| `key` | varint `length + 1`, then the bytes; `0`: no key (a present empty key is `1`) |
| `count` | varint, the number of headers |
| per header | `name`: varint length, bytes; `value`: varint length, bytes |

A key and headers belong to the publication, not to the object: the object
is the payload alone, identified by its BLAKE3 id and stored once. The same
payload published with different keys or headers is one object with one
Publish per publication, each with its own attributes. Attributes are
neither deduplicated nor compressed. Present-but-empty attributes (`00 00`)
are `CorruptWal`: they are stored as nothing.

Deltas are taken within the frame, from the previous Publish of the same
frame. For a frame's first Publish, the object number base is the next object
number when the frame starts, and the time base is 0, so the time is written
in full. Deltas are computed modulo 2^64. A repeated message referring to an
old object gives a negative number delta.

### 3 — Release (payload 2–20 bytes)

| Field | Size |
|---|---|
| `topic_number` varint | 1–10 |
| `topic_seq` varint: always the topic's first live entry | 1–10 |

On replay the released entry's object must still be in the object index (a
live entry's object is never reclaimed); otherwise `CorruptWal`.

### 11 — Compact (payload 4–31, + 2–20 per kept block, + per rewritten block)

| Field | Size |
|---|---|
| `old` varint: the rewritten segment | 1–10 |
| `new` varint: the new segment; `u64::MAX` = none | 1–10 |
| `kept` varint: number of blocks copied unchanged | 1–10 |
| per kept block: `block_number` varint, `offset` varint in `new` | 2–20 each |
| `rewritten` varint: number of rewritten blocks | 1–10 |
| per rewritten block: `block_number`, `offset`, `len`, `raw_len`, `dict` varints, `crc` u32 | 9–54 |
| per rewritten block: `count` varint, then `count` object numbers, varints, in index order | 1–10 + 1–10 each |

On replay, with `B` the blocks of `old`: every kept block is in `B`; the
objects of the rewritten blocks, each listed once, are objects of the blocks
of `B` that are not kept, and no rewritten block is empty; every other
object of `B` is reclaimed and removed with its id. Blocks of `B` not kept
are removed. Any other combination is `CorruptWal`. With `new` =
`u64::MAX` there are no kept or rewritten blocks. No reference counts are
kept (ADR-0014 step 5, D-206 A), so replay does not check which objects
were live: a kept block may hold dead objects and a moved object may be
dead (a compaction keeps what it marked). After replay every live topic
entry's object must be in the object index; otherwise
`UnknownObjectNumber`, and no file is changed.

### Example: frame 9 (ParallelSync) with a new topic `ab` and two Publish records, next object number 300 at the frame start

```text
00: 11 00 00 00              len = 17 (body)
04: 09 00 00 00 00 00 00 00  frame seq = 9
0c: 03 00 00 00              count = 3
10: 02                       mode = ParallelSync
11: xx xx xx xx              crc32c([00..11) + body)
15: 03 06 00 61 62           PutTopic: len 3, topic 0, "ab"
1a: 04 02 00 01 00 04        Publish: len 4, topic 0, seq 1, Δnumber 0 (300), time 2 (zigzag 4)
20: 04 02 00 02 02 00        Publish: len 4, topic 0, seq 2, Δnumber +1 (301), Δtime 0
```

38 bytes in total; checked by the test `frame_layout_matches_format_doc`.

### Write invariant

A frame starts only after the previous frame is durable (its fdatasync
returned): one writer, sequential group commit (`&mut self`, the `in_flight`
flag with `debug_assert!` in `Store::begin_write`), and a WAL file is
created only after the last frame of the previous file is durable. Hence
only the last frame of the last file can be unacknowledged.

### Recovery

Every check below runs before recovery changes any file: when open fails,
every file is as it was, including the repairs it found due on the way
(a torn tail, a leftover tmp file, an orphan segment). `wal-*.log.tmp`
files are deleted (`WalTmpRemoved`). The WAL files are read in
sequence-number order; the first must be named 0, and each later one must
be named by the frame after the previous file's last; otherwise
`CorruptWal { file, offset: 0, .. }` (a missing, extra or renamed file),
and no file is changed. A directory with no WAL file but data or index
segments is `CorruptWal` as well (a new WAL would make the segments
orphans); with neither, open creates `wal-0000000000000000.log`. In each
file reading starts at byte 24. A frame is bad if its header or body is
short or its CRC does not match.

1. A bad frame in the last WAL file with no frame with a valid CRC readable
   from any later byte up to the end of the file is a torn tail: the file
   is cut at the start of that frame (`set_len` + `fsync`). This is the only change recovery makes to the
   WAL besides rule 4. Any damage inside the last frame, in any byte order (for
   example, the first half unwritten and the second written), gives a torn
   tail, not an error: records without a checksum of their own are never
   taken for a frame.
2. A bad frame followed by a frame with a valid CRC, or in a file that
   another WAL file follows, means the middle is damaged: error
   `WalCorrupted { file, offset, next_file, next_offset }` (where reading
   could continue), and no file is changed.
3. A frame with a valid CRC but a `seq` out of order (a gap or a repeat),
   unparsable records, or records contradicting earlier frames is the error
   `CorruptWal { file, offset, reason }`, and no file is changed.
4. The last complete frame: every block it introduces (PutBlock or
   PutInlineBlock) must be stored in full, decompress to its recorded raw
   length and CRC, split into its recorded objects, and each object's BLAKE3
   must equal its `object_id`. If not:
   * `mode` = ParallelSync and nothing follows the frame, neither in its
     file nor as a later WAL file: the frame was never acknowledged (a crash between the WAL and segment
     fdatasyncs). The WAL is cut at the frame start, the segment bytes become
     orphans for compaction, and the recovery report gets `FrameDropped`
     (reason `ObjectMissing`, `BlockCorrupted` or `ObjectHashMismatch`);
   * `mode` = Sequential or InlineSmall: the bytes were durable before the
     frame, so this is corruption: error `MissingObject` (block absent or
     short), `CorruptBlock` (does not decode) or `HashMismatch`, and no file
     is changed;
   * a torn tail or a later WAL file follows the frame: the next write had
     started, so this frame was acknowledged (a frame or WAL file starts
     only after the previous frame is durable). Corruption in any mode: an
     error, and no file is changed.

   Residual ParallelSync risk: an acknowledged last frame whose block bytes
   are later corrupted on disk cannot be told from a crash before
   acknowledgement; such a frame is dropped (and reported).
   Once a frame is dropped, the frame before it was acknowledged (one
   followed it) and is now the last: its blocks are checked as in rule 4,
   and a failure is an error, no file changed (straum#135; without the
   check the next open would drop that frame as well).
5. Blocks of earlier frames are checked at open for presence and bounds
   only: a segment block must lie in full within its segment file, otherwise
   `MissingObject` (naming the block's lowest-numbered object; an error, not a
   repair). Their content is not decompressed at open; it is checked on every
   read (`CorruptBlock`, `HashMismatch`).

### Recovery report

`Store::recovery_report()` lists every change recovery made at open; an empty
report means recovery changed nothing. No change is made without an entry.

| Action | When |
|---|---|
| `WalTmpRemoved` | a `wal-<seq>.log.tmp` of an interrupted WAL file creation was deleted (one entry per file) |
| `TornTailTruncated { frame_seq, file, offset, bytes_removed }` | a torn last frame of the last WAL file `file` was cut (rule 1) |
| `FrameDropped { frame_seq, file, offset, mode, object, reason, bytes_removed }` | a complete last ParallelSync frame was dropped (rule 4) |
| `OrphanSegmentRemoved { segment }` | a segment file the WAL does not refer to was deleted |
| `IndexSegmentRemoved { segment, bytes }` | an index segment without an intact header was deleted (`bytes` its length) |
| `StaleIndexRecordsFenced { records, segment }` | index records of cut WAL frames were found; index segment `segment` was created to keep them stale |
| `IndexSegmentDropped { segment, bytes }` | an index segment was deleted because `Config::rebuild_index` was set (the index is rebuilt from the WAL) |
| `TopicIndexRepaired { topic, blocks_rewritten, pages_rewritten }` | a topic's index blocks or seek pages were re-encoded from the WAL and appended |
| `CheckpointFileRemoved { file }` | a checkpoint file the checkpoint used does not list was deleted: an earlier manifest, a `checkpoint-<n>.tmp`, or a table or run of a checkpoint that did not complete |
| `CheckpointManifestDamaged { checkpoint, fallback, quarantine }` | manifest `checkpoint-<checkpoint>` failed its CRC, version or name check, or did not decode; open used manifest `fallback` instead, or none (`None`: the state is rebuilt from the WAL, which holds every frame); the manifest and the files it may list moved, unchanged, into the directory `quarantine` |
| `CheckpointFileQuarantined { file, quarantine }` | checkpoint file `file` was moved, unchanged, into the quarantine directory `quarantine` (one entry per file) |
| `CheckpointQuarantineResumed { checkpoint, fallback, quarantine, found }` | an interrupted quarantine of manifest `checkpoint` was completed in its directory `quarantine`; `found`: the files an interrupted open had moved there; open used manifest `fallback` instead, or none |

`config_overrides` lists `Config` values the WAL replaced, which change no
file: `DictTrainBytes { configured, stored }` when `Config::dict_train_bytes`
differs from the WAL header's value (`stored` is used).

The report is kept in the open `Store`'s memory and is not written to disk.

## Topic index

A topic's index is the sequence of its entries `(object number,
timestamp)`, one per published message, released ones included; entry
`topic_seq` is entry `topic_seq mod 64` of block `topic_seq div 64`.
`topic_seq` is not stored. Blocks of 64 entries are encoded as:

| Part | Encoding |
|---|---|
| entry 0 | `object_number` varint, `timestamp` varint |
| entries 1–63 | zigzag(`object_number` − previous) varint, (`timestamp` − previous) varint |

Time does not decrease within a topic, so the time delta is unsigned. New
objects come in order (number delta +1), so an entry is usually 2 bytes and
the first entry of a block about 3 + 6 bytes. The encoding is
deterministic: the same entries always give the same bytes.

A full block is stored once, as a record in the shared index segments,
with the keys and headers of its entries in an attribute record beside it.
The last, incomplete block (at most 63 entries) is kept in memory, encoded,
and read from there; it is rebuilt from the WAL at open.

## Index segment `idx-<id>.dat`

Append-only files shared by all topics: full topic index blocks, their
attribute records and seek pages. A 24-byte header, then records.

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `magic` = `53 54 52 4d 49 44 58 00` (`"STRMIDX\0"`) |
| 8 | 4 | `version` u32 = 20 (the WAL version) |
| 12 | 8 | `created_at` u64: the number of the first WAL frame its records may describe (the batch's frame for a segment started while writing a batch's records, the frame count for one started at open) |
| 20 | 4 | `crc` u32: CRC32C of bytes `[0..20)` |

Record:

| Field | Size |
|---|---|
| `kind` u8: 1 block, 2 seek page, 3 attributes | 1 |
| `len` varint: body length | 1–10 |
| `topic` varint: topic number | 1–10 |
| `number` varint: block number (kinds 1 and 3), or the seek page's number within its level | 1–10 |
| `stamp` varint: the WAL frame number after the frames the record describes | 1–10 |
| body | `len` |
| `crc` u32: CRC32C of every byte of the record before it | 4 |

A block's body is its 64 encoded entries, followed, if any of them has a
key or headers, by the 16-byte location of the block's attribute record
(`segment u32 | len u32 | offset u64`, as in a seek page). The CRC covers
the topic and block number, so a record is never taken for another block.
An attribute record (kind 3, the block's topic and number) holds the keys
and headers of the block's entries that have them (D-235 A, D-250 A):
`count varint` (1–64), then per such entry, in position order,
`position u8` (0–63, the entry's index in the block), `len varint` and the
`len` bytes of its attributes, encoded as in a Publish record (never
empty). Its CRC is the group CRC of those keys and headers: one per full
block, checked on every read. It is written just before its block record,
in the same write and with the same stamp. A seek page's
body is `level u8` followed by `Config::seek_page_entries` (default 256)
locations of 16 bytes: `segment u32 | len u32 | offset u64`, the whole
record's length included. A level-0 page holds the locations of blocks
`256k … 256k + 255` (page `k`); a level-`l` page those of level-(`l` − 1)
pages `256k … 256k + 255`, that is of blocks `256^(l+1)·k` onwards. Only
full pages are written.

**Writing.** A batch's records go to the active segment in one write after
the WAL fsync, not synced. Their stamp is the batch frame's number + 1. A
segment is created (`create_new`, header, `fdatasync`, directory fsync)
when the first record needs one, so its header is durable before any record
in it; a store appends only to a segment it created. Before a batch, a
segment holding at least `Config::index_segment_max_bytes` (default 64 MiB)
is sealed; the next record starts a new one.

**Lookup by `topic_seq`.** Per topic the store keeps in memory the seek
pages not yet written (the spine): for each level, the locations after the
level's last page, fewer than 256 each. The top level's entries cover the
oldest blocks. A block under the spine's level `l` is found by reading `l`
seek pages, then the block: one page up to 65 536 blocks (about 4.2 M
entries), two up to about 1.07 G entries. Every page and block read must be
intact (CRC) and be the expected kind, topic, level and number; otherwise
the read fails with `TopicIndexCorrupt` and returns none of its entries.
The last full block read is cached per topic. At most
`Config::index_open_files` (default 64) segments are open for reads, least
recently used closed first, plus the segment being written; a read in
progress holds its handle until it completes, so concurrent reads can each
add one.

**Keys and headers.** `Store::read_entry` of an entry of a full block
reads the block, then its attribute record, which must be intact and be
the block's (kind, topic, number) and well-formed; otherwise the read fails
with `TopicIndexCorrupt` (that entry only; the store stays usable).
`Store::read`, `Store::index_entry` and `release_before` read the block
only. For the incomplete block, each entry with a key or headers keeps the
WAL position and length of its Publish record's attributes in memory
(`(seq, WAL file, offset, length)`, at most 63 per topic, released ones
included); `read_entry` reads them from that WAL file, which is not yet
deleted. When a batch completes a block, its attribute record is built
after the batch's WAL write, from the frame still in memory for that
batch's entries and by one WAL read per entry of earlier batches, and the
positions are dropped. An error there (a read or a segment creation) comes
after the frame is durable: the batch fails and the store refuses further
use, and the next open recovers the batch from the WAL.
Replay rebuilds the positions from the WAL.

**Recovery.** Index segments are derived from the WAL. At open:

1. A segment without an intact header is deleted (`IndexSegmentRemoved`).
   At most 24 bytes long, its creation stopped before the header sync and
   it held no record. Longer, its header was damaged; its records are
   re-derived from the WAL like any other lost record, but its `created_at`
   is lost too (see the limit below).
2. Each segment is read in order up to its first record that is short,
   malformed or fails its CRC (unwritten, torn or damaged bytes).
3. A record is stale if its stamp is above its segment's limit: the lowest
   `created_at` of the later segments, and the WAL's frame count. A stale
   record describes WAL frames that recovery cut; it is never used. If a
   stale record is not already covered by a later segment's `created_at`,
   a segment is created now (`created_at` = the frame count;
   `StaleIndexRecordsFenced`), so that frames written later under the same
   numbers do not make it look current.
4. Every other block record must be a block the WAL has, with the WAL's
   entries: otherwise `TopicMismatch` (other entries) or
   `IndexSegmentCorrupt` (an unknown topic, a block beyond the topic's full
   blocks, an unknown kind, a body that does not decode), and no file is
   changed. Its attribute location must be present exactly when the WAL
   gives one of its entries a key or headers (otherwise `TopicMismatch`).
   An attribute record found intact at that location, of the block's topic
   and number, must hold exactly the WAL's keys and headers
   (`TopicMismatch` otherwise; `IndexSegmentCorrupt` if it does not
   decode or the location holds another intact record); one that is not
   there intact (unwritten, torn or damaged, like
   any unsynced record) makes the block count as not found, so both are
   re-derived (step 6). The block's location is used (the first intact
   one, if several). Attribute records are checked only through their
   block record.
5. Seek pages are rebuilt from the block locations, level by level; a page
   found intact with exactly the expected locations is reused.
6. Blocks and pages not found are re-encoded from the WAL and appended with
   stamp = the frame count (`TopicIndexRepaired`).

Open reads every index segment and replays every topic's entries; open time
and open's peak memory (every index segment is held in memory during the
check) grow with the history (ADR-0014: until checkpoints).

**Limit.** A segment's `created_at` is the only proof that the stale
records before it are stale. If the header of a fencing segment is damaged
after new frames reused the stale records' frame numbers, those records
look current at the next open, disagree with the WAL, and open fails with
`TopicMismatch` (no file changed): an error, never wrong data
(`tests/index_segments.rs`, `a_damaged_fence_header_fails_open`).

**Rebuild (D-229 A).** The operator's way out: open with
`Config::rebuild_index` set (the broker: the flag `--store-rebuild-index`,
one start only; refused in its configuration file). Open then deletes every index segment before reading any
(`IndexSegmentDropped { segment, bytes }` each) and re-derives every
topic's blocks and seek pages from the WAL into new index segments
(`TopicIndexRepaired`). The rebuild changes no other file; the repairs
of every open (a torn tail, orphans) apply as usual. It is safe whenever
the WAL itself opens, since the index holds nothing the WAL does not, and
every WAL, live-object and segment check runs before the first deletion. If
the rebuilding open is interrupted (some segments deleted, others not),
open again with the setting. Without the setting, open never deletes an
intact index segment.

## Checkpoint (ADR-0014 steps 4 and 4a)

`Store::checkpoint`, between batches, writes the object table of the object
numbers allocated since the last checkpoint and the relocation run of the
numbers that checkpoint already covered whose place compaction changed
since; the same for blocks (block table, block relocation run, D-255 A);
the dictionary file once, at the first checkpoint after training; then the
manifest. Interim rule until ADR-0014 step 6b: no WAL file
is deleted and open still replays the whole WAL, so the checkpoint is
checked against it at every open; the in-memory object maps stay.

Triggers (ADR-0014 step 7, D-209 A): before a batch, a release or a
compaction step, a checkpoint runs if a frame was written since the last
one and either the WAL bytes since it reached
`Config::checkpoint_wal_bytes` (default 256 MiB, counted at open from the
frames past the covered one) or the memtable reached its share of
`Config::memory_budget_bytes` (a quarter: 112 bytes per new id with its
dense-table entry, 16 per relocated number); its error is the call's, with
nothing of the call written, and, as after any failed checkpoint, the
store is then poisoned (`Poisoned` until reopened). `Store::close` checkpoints if a frame was
written since the last one.

Memory budget (ADR-0014 §4, D-207 B): the id runs' fences and, newest
first, their filters stay within seven sixteenths of the budget; the
filters of the oldest runs that do not fit are dropped at open and after
each checkpoint, and the write path no longer searches those runs
(`Store::unsearched_id_runs`). A repeat of content whose id lives only in
them is stored again under a new number; reads go by number, so they are
unaffected, and the runs keep every pair, so a merge or an open with a
larger budget searches them again. Replay therefore accepts an id under a
new number in a later frame; within one frame an id repeats never (a
batch deduplicates against its own new objects), and a repeat there is
`CorruptWal`.

Object and block tables and their relocation runs share a 24-byte header
and a page layout:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `magic` = `"STRMOBJ\0"` (object table), `"STRMREL\0"` (relocation run), `"STRMBLK\0"` (block table) or `"STRMBRL\0"` (block relocation run) |
| 8 | 4 | `version` u32 = 20 (the WAL version) |
| 12 | 8 | `first` u64: the table's first object or block number, or the number of the checkpoint that wrote the run (also in the file name) |
| 20 | 4 | `crc` u32: CRC32C of bytes `[0..20)` |

Then pages of at most 4 KiB: as many whole entries as fit in 4092 bytes,
then a CRC32C u32 of the page number (u64, from 0) followed by the page's
entries; the last page holds the rest (at least one entry).

| File | Entry | Bytes |
|---|---|---|
| object table | `id [32] \| block u64 \| index u32`, one per number from `first`, in order; a number compaction had reclaimed by then: a zero id and `block`, `index` all ones | 44 (93 per page) |
| relocation run | `number u64 \| block u64 \| index u32`: the number's place at the checkpoint, `block` and `index` all ones if reclaimed; numbers strictly increasing | 20 (204 per page) |
| block table | `kind u8 \| at u64 \| offset u64 \| len u64 \| raw_len u64 \| dict u32 \| crc u32 \| objects u32`, one per block number from `first`: kind 0 a segment block (`at` the segment id), kind 1 an inline block (`at` the WAL file, `offset` in it); `len` the stored and `raw_len` the raw length, `dict` the dictionary number (0 none), `crc` the raw block's CRC32C, `objects` its object count; a number with no block (reclaimed, or a hole a rewrite left): all 45 bytes ones | 45 (90 per page) |
| block relocation run | `number u64 \| segment u64 \| offset u64`: a covered block compaction moved (its new place) or reclaimed (all ones); numbers strictly increasing | 24 (170 per page) |

A number's place is the newest run that lists it, else its table's entry
(the id comes from the table: ids never change). A block's place is the
same: its lengths, dictionary, CRC and object count come from its table
(compaction copies a kept block unchanged), its segment and offset from
the newest block run that lists it. Inline blocks stay at their WAL
position until step 8 moves them into segments.

Dictionary file `dict-<number>.bin`: `magic "STRMDIC\0" | version u32 |
number u64 | len u64 | dictionary bytes | crc u32` (CRC32C of every byte
before it); length 0 if training built no dictionary. Written once, by the
first checkpoint after training (the WAL's PutDict record holds it until
then), and listed by every later manifest.

Id run `idrun-<n>.run` (ADR-0014 step 6): the 24-byte header (`magic`
`"STRMIDR\0"`, `first` = `n`), then pages of `id [32] | number u64`
entries (40 bytes, 102 per page) sorted strictly by id, then number (an
id repeats under several numbers after degraded deduplication, ADR-0014
§4), then a footer: `pages u64 |
fence u64 per page | filter | len u32 | crc u32` (`len` the footer's
bytes before it; the CRC32C over the footer and `len`). A fence is the
first 8 bytes of a page's first id, big-endian. The filter is a binary
fuse filter with 8-bit fingerprints (Graf and Lemire, arity 3) over each
id's key, its bytes 8 to 16 read little-endian (`seed u64 | segment_length
u32 | segment_count_length u32 | len u32 | fingerprints`, `len` =
`segment_count_length + 2 × segment_length`, `segment_length` a power of
two dividing `segment_count_length`): with `h = fmix64(key + seed)`
(MurmurHash3's finalizer: `h ^= h >> 33; h *= 0xff51afd7ed558ccd; h ^= h >>
33; h *= 0xc4ceb9fe1a85ec53; h ^= h >> 33`, wrapping), positions `h0 =
(h × segment_count_length) >> 64`, `h1 = (h0 + segment_length) ^ ((h >> 18)
& (segment_length − 1))`, `h2 = (h0 + 2 × segment_length) ^ (h &
(segment_length − 1))`, and fingerprint `f = (h ^ (h >> 32)) as u8`, the
key may be in the run if `f ^ fp[h0] ^ fp[h1] ^ fp[h2] == 0`; about 9 bits
per id, 0.39 % false positives. A checkpoint writes the memtable (the live
ids allocated since the last one) as a run. Runs merge by size tier
(ADR-0014 §1): a run's tier is `floor(log4(entries))`; when the new run's
tier would hold four runs, the new run absorbs the other three, and the
result absorbs the three of its own tier the same way, one k-way merge
over their pages that drops ids whose objects compaction reclaimed, the
output written page by page. Each tier keeps at most three runs. The
merged runs are deleted after the new manifest (a crash before leaves them
unlisted: deleted at open, `CheckpointFileRemoved`). A lookup checks the memtable, then each run from newest
to oldest whose filter answers yes: the run's fences give the one page to
read (a page whose CRC fails is `CorruptCheckpoint`), and a number found
whose object compaction reclaimed is a miss.

Manifest `checkpoint-<n>`: `magic "STRMCKP\0" | version u32 | n u64 |
covered u64 | next_object u64 | next_block u64 | next_topic u64 |
next_segment u64 | dict_train_bytes u64 | dictless_raw u64 | dict u64 |
tables u32 | (first u64, count u64) per table | runs u32 | (checkpoint u64,
count u64) per run | block tables u32 | (first u64, count u64) per block
table | block runs u32 | (checkpoint u64, count u64) per block run | id
runs u32 | (checkpoint u64, count u64) per id run | segments u32 | id u64
per live data segment | crc u32` (CRC32C of every
byte before it). `covered`: the frames below it are covered; the tables,
in order, cover object numbers `0..next_object` and the block tables block
numbers `0..next_block` without gaps; the runs are listed oldest first;
`next_segment` is above every segment the covered frames reference;
`dict_train_bytes` is the WAL header's value and `dictless_raw` the raw
bytes of batch blocks stored without a dictionary (the training trigger);
`dict` names the dictionary file, 0 before training; `segments` are the
data segments the covered frames reference, ascending.

**Writing.** New table and run files are created (`create_new`), written
and `fdatasync`ed, then the directory is synced; the manifest is written to
`checkpoint-<n>.tmp`, `fdatasync`ed, renamed to `checkpoint-<n>` and the
directory synced; then `checkpoint-<n>` is read back and must decode to what was
written (a manifest that does not moves into a quarantine directory with
this checkpoint's new table and run, as below; the previous manifest stays
in force and the checkpoint returns `CorruptCheckpoint`, its report; an
interruption is completed by the next open); only then is `checkpoint-<n − 1>` deleted and the directory
synced, so a damaged newest manifest has an older one to fall back to. The
new table and run are read back, and must hold the entries written, before
the manifest tmp file is written, so a failure there leaves them unlisted.
A crash before the rename leaves the previous checkpoint valid and the new
files unlisted; after it, the previous manifest is unlisted. A checkpoint
that fails makes the store refuse further use. After a read-back mismatch
the previous manifest is the one in use; after another failure (an I/O
error, or one after the previous manifest's deletion) either the new or
the previous manifest is, both intact.

**Recovery.** Before replay, the manifests are read newest first. A
manifest that fails its CRC, is of another version or does not name
itself `n` is damaged (`CheckpointManifestDamaged`): open passes over it
to the next older one, and uses the newest intact manifest. With none
intact, the checkpoint is ignored while the WAL holds every frame (its
files are then orphans, and replay rebuilds everything from the WAL);
once WAL files can be deleted (ADR-0014 step 6b), that state is
`CorruptCheckpoint`, and so is a manifest whose `covered` is below the
first frame the WAL files hold. Every table and run the manifest in use
lists must be present, with an intact header naming it, intact pages and
exactly the listed count, the tables contiguous from 0 to `next_object`,
the runs ordered and their numbers increasing and below `next_object`;
the same for block tables (contiguous from 0 to `next_block`, every entry
of a known kind) and block runs (below `next_block`); every listed id run
present with an intact header naming it, intact pages and footer, its
entries sorted, its fences the pages' first ids and its filter finding
every id; a listed dictionary
file present, intact and naming its number; otherwise `CorruptCheckpoint`
and no file is changed: the fallback is for
a damaged manifest only. `covered` above the WAL's frame count is
`CorruptCheckpoint`. When replay reaches frame `covered`, the checkpoint
must match the state the WAL gives there: the next object, block and topic
numbers, the live segments and `next_segment` above them, `dict_train_bytes`,
`dictless_raw` and the dictionary's bytes (`CorruptCheckpoint`), every
object number's place, reclaimed or `(id, block, index)`
(`CheckpointMismatch { number }`), every live object below `next_object`
in exactly one id run under its id (`CheckpointMismatch { number }`), and
every block number's place and
layout (`CheckpointBlockMismatch { block }`). Covered numbers and blocks
whose place differs at the end of the WAL go to the next checkpoint's
runs.
After every check, the checkpoint files the manifest in use does not list
are deleted (`CheckpointFileRemoved`): earlier manifests, tmp manifests,
tables and runs of an interrupted checkpoint.

**Quarantine** (D-257 A as amended by the owner). A damaged manifest is
never deleted: it moves, unchanged, into a quarantine directory
`quarantine-<n>` with the tables and runs it may list. Tables and runs are
cumulative, so the newest manifest quarantined may list every object
table at or above the used manifest's `next_object`, every block table at
or above its `next_block`, every run, block run and id run numbered above
the used manifest up to its own, and the dictionary file if the used manifest lists
none: they move with it (a table that cannot be
attributed with certainty, such as a later interrupted checkpoint's, is
kept there too); runs above it are orphans. The move: create the directory
and sync the store directory; move the tables and runs, sync both
directories; move the manifest last, sync both.

- *Completed* quarantine: the directory holds `checkpoint-<n>`. It is never
  read, never changed and not reported again; a new quarantine of the same
  number uses the next free name, `quarantine-<n>.1`, `.2`, ….
- *Interrupted* quarantine: the directory exists without `checkpoint-<n>`,
  and `checkpoint-<n>` is still in the store directory (a crash after the
  directory's creation and before the manifest's move, at open, at write
  time, or while completing an earlier one). Open completes it in that
  same directory, syncing the store directory first: it moves the files the quarantine takes that are still in
  place, then the manifest, and reports the files it found there
  (`CheckpointQuarantineResumed`, `found`: the interrupted open's report was
  lost) and each move (`CheckpointFileQuarantined`). It then uses the
  newest manifest not quarantined, as for a damaged one. A crash during
  the completion leaves an interrupted quarantine again, completed by the
  next open. If the manifest is intact (a read-back failure at write time),
  every table and run it lists must be in place or in that directory;
  otherwise the state is unexplained, `CorruptCheckpoint`, no file
  changed. Two interrupted quarantines of one manifest are
  `CorruptCheckpoint` too. A quarantine directory without its manifest
  while no `checkpoint-<n>` is in the store directory is kept, unread.

Every change is reported at least once: by the open that made it, or, when
that open crashed before moving the manifest, as `found` by the open that
completes the quarantine. Not covered: a crash after the manifest's move
(the quarantine is then complete) and before `Store::open` returns its
report, or before the checkpoint returns its error; those moves are in no
report, and only `quarantined_files` and the broker's warning show them
(the same window as every other repair recovery makes; it stands until
the recovery log of ADR-0014 §3, D-260 B, lands; the fault injection
that tests this window, `tests/fault_points.rs`, is in place). "At least once" (owner): exactly once without a crash, a
repeat after an interrupted open allowed, never an omission. A
quarantine name that is not a directory is `CorruptCheckpoint`, no file
changed.
`Store::quarantined_files` counts the files in all quarantine directories
at open. Checkpoint numbers do not repeat (the next is above every
manifest, tmp manifest, run and quarantine directory on disk). With no manifest
at all, every table and run is such a file (a first checkpoint that did
not complete); while no WAL file is deleted the WAL still holds everything
they described (from step 6b they must be refused when WAL files they
would cover are gone, as above). Checkpoint files without any WAL file are
`CorruptWal`, as segments are.

## Write order (one batch)

`Config::write_mode` sets the mode. Before the steps below: deduplication
by BLAKE3 id, the dictionary if training is due, and the batch's new
objects compressed into one block.

`Sequential` (default):

```text
1. the block → the active segment, one write
   (a new segment: create_new + directory fsync)
2. fdatasync of every touched segment
3. the batch's frame → one write to the WAL file (a new WAL file, if the
   current one is full: created as in "WAL", before the frame is built)
4. fdatasync of the WAL file
5. index records of the blocks the batch completed → the active index
   segment, one write (no fsync); a new index segment is created and synced
   before step 1
6. acknowledgement
```

`InlineSmall`: Sequential order. If the block is inline (or the batch has
no new object), steps 1–2 do not happen: one fdatasync per batch.

`ParallelSync`:

```text
1. a new segment: create_new + directory fsync, before the frame that
   refers to it is written
2. a segment sealed mid-batch (rotation): write + fdatasync
3. two chains at once:
     the active segment's buffer → write → segment fdatasync
     the batch's frame → write to the WAL file → its fdatasync
4. both chains done → index records (no fsync)
5. acknowledgement
```

A crash between steps 3 and 5 can leave a durable frame without durable
block bytes. Such a frame is the last in the file (the next batch does not
start before the previous one is acknowledged), and recovery drops it (rule 4
above).

## Metadata per message

For a batch of `b` messages:

| Part | Bytes |
|---|---|
| PutObject (new object only) | 2 + 32 + Δnumber 1 + Δblock 1 + Δindex 1 |
| PutBlock (one per batch with new objects) | 2 + Δnumber 1 + segment 1 + offset 1–4 (segments up to 64 MiB) + length, raw length 1–5 each + dictionary 1 + CRC 4, shared by `b` |
| Publish | 2 + 4…40: topic number and seq 1–10 each; number and time deltas usually 1 byte each; the time of a frame's first Publish about 6 bytes for millisecond time; plus the attributes: 0 bytes without key and headers, 2 for an empty key, `1 + key` + 1 for a key below 127 bytes and no headers |
| frame header | 21 / `b` |
| topic index entry | ~2 (+ ~9 for a block's first entry and about 13 bytes of record header and CRC per 64 entries; a seek page adds 16 bytes per 64 entries) |
| PutTopic | once per topic |
| PutDict | once per store, up to 64 KiB + 3 |

A message with already known bytes has no PutObject and adds nothing to a
block. `Stats::metadata_bytes` counts WAL and topic index bytes except inline
blocks (index records are counted in `topic_bytes`); the dictionary counts as metadata. `examples/meta_bytes.rs` measures
these from the store's write counters (a 5-byte topic, millisecond time).

## Compaction (mark and sweep, ADR-0014 step 5)

`Store::compact_step` runs one step; `Store::compact` runs steps until the
compaction is done. Batches, releases and checkpoints may run between
steps; the compaction's state is in memory only, so a restart ends it
(what it completed is in the WAL).

Numbers below `threshold`, the next object number when the compaction
began, are compacted in ranges of at most eight per byte of the bitmap's
share of `Config::memory_budget_bytes` (a sixteenth: 16 MiB, about 134 M
numbers, by default; ADR-0014 §1), in order; each range runs a mark phase
over every topic and then a sweep of every segment. Numbers outside the
range count as live in it: those below were swept by earlier ranges, those
above wait for later ones, whose mark phase sees every entry written
meanwhile.

1. **Mark**, one topic per step, in name order (topics that existed when
   the range's mark phase began): the object number of every live entry,
   read from the topic index, gets a bit in the range's bitmap if the
   range holds it. A full
   index block that fails its check is `TopicIndexCorrupt`, a number the
   object index lacks `UnknownObjectNumber`; either ends the compaction
   with nothing written and the store usable.
2. **Live** during the compaction: a marked number, every number outside
   the range (at or above `threshold` included), and every number of the
   range a Publish of a batch references while the compaction is in
   progress (marked as the batch is built), so
   a batch written between the mark and the sweep never loses an object,
   new or old (D-206 A with the owner's requirement).
3. **Sweep**, one data segment per step, in id order: the segments that
   exist once the range's mark phase is complete (the active segment is
   sealed then); one more step then starts the next range. Each is rewritten as below if one of its blocks holds an object
   that is not live, or the file holds bytes no block covers.

A segment is rewritten as follows.

```text
1. blocks whose objects are all live → copied unchanged to seg-<new>.dat
   (decoded and hash-checked first); live objects of partly dead blocks
   (hash-checked) → one new block, compressed, appended;
   fdatasync, directory fsync
2. a frame with one Compact record → the WAL file, fdatasync
3. the in-memory index switches to new; seg-<old>.dat is deleted
```

Blocks whose objects are all dead are not copied. A crash before step 2
leaves `new`, which the WAL does not refer to; a crash after it leaves `old`,
which the WAL no longer refers to. Both are deleted at open.
