# On-disk formats

All integers are little-endian. Every variable-length record carries a length
prefix. CRC32C is Castagnoli (crate `crc32c`). An object id is the 32-byte
BLAKE3 hash of the object's raw bytes. Code: `src/codec.rs` (WAL, raw
blocks, topic index entries), `src/block.rs` (zstd) and `src/store.rs` (file
names, segments).

## Store directory

```text
<dir>/
├── wal.log                      the log (one file, not rotated)
├── wal.log.tmp                  only while a WAL is being created; deleted at open
├── seg-<id:016x>.dat            data segments; id is a u64 in hex, 16 digits
└── topics/
    └── <hex(topic)>.idx         a topic index; the name is the topic name's bytes in hex
```

A topic name is 1 to 100 bytes (`MAX_TOPIC`), so the hex file name (up to 200
characters plus `.idx`) fits NAME_MAX = 255. Segment ids are never reused: the
next id is the highest seen in the WAL or on disk plus one.

## Blocks

Objects are stored in blocks of at most `Config::max_block_raw_bytes` raw
bytes (default 64 KiB). The new objects of one batch (`put_batch`), in order
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
least recently used first out, within two limits: `Config::block_cache_blocks`
(default 16) blocks and `Config::block_cache_bytes` (default 64 MiB) of raw
bytes and object offsets. A block larger than the byte limit is not cached.

Block numbers start at 0 and are never reused: the next number is the
highest in the WAL plus one.

### Dictionary

One dictionary per store, trained once. When the raw bytes of new objects
stored in blocks without a dictionary reach the threshold in the WAL file
header (`Config::dict_train_bytes` when the WAL was created; default 1 MiB;
0 disables training), the next non-empty batch trains a
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

## WAL `wal.log`

A file header (24 bytes), then frames. One frame is one batch of messages
(`put_batch`), one `release_before` call, or one compaction step (one
rewritten segment).

### File header

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `magic` = `53 54 52 4d 57 41 4c 00` (`"STRMWAL\0"`) |
| 8 | 4 | `version` u32 = 10 |
| 12 | 8 | `dict_train_bytes` u64: the dictionary training threshold |
| 20 | 4 | `crc` u32: CRC32C of bytes `[0..20)` |

`dict_train_bytes` is written once, from `Config::dict_train_bytes`, when
the WAL is created, and never changes. At open the store uses the header's
value: a different `Config::dict_train_bytes` is ignored and listed in
`RecoveryReport::config_overrides` (`ConfigOverride::DictTrainBytes`), so a
reopen cannot move a trigger that has not fired.

A new WAL is written to `wal.log.tmp`, synced (`fsync`), renamed to `wal.log`,
and then the directory is synced. So `wal.log` never exists without a complete
header. A file that does not start with an intact 24-byte header of this
version (a WAL without a header, an empty file, another version, a header
CRC mismatch) is `UnsupportedFormat`, and the file is not changed. Earlier versions are not supported: 1 had no mode byte in the
frame, 2 full ids in Publish and in the topic index, 3 `topic_seq` in topic
index entries, 4 fixed-width Publish and Release with the topic name, 5
fixed-width PutObject and PutInline, 6 a topic index of 20-byte entries, 7
uncompressed objects located by segment offset (PutObject type 1, PutInline
type 5, Compact type 4), 8 a 16-byte header without `dict_train_bytes`, 9
at most one rewritten block per Compact record.

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
(block, index, refcount), and block number → (location, lengths, dictionary,
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

The block's location is the absolute offset of its stored bytes in
`wal.log`. **Inline blocks are never reclaimed.** Compaction rewrites
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

### 2 — Publish (payload 4–40 bytes)

| Field | Size |
|---|---|
| `topic_number` varint | 1–10 |
| `topic_seq` varint | 1–10 |
| Δ`object_number`, zigzag varint | 1–10 |
| Δ`timestamp`, zigzag varint (ms, given by the caller; does not decrease within a topic) | 1–10 |

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

On replay, with `B` the blocks of `old`: every kept block is in `B` and holds
only live objects (refcount > 0); the objects of the rewritten blocks
together, each listed once, are exactly the live objects of the other blocks
of `B`, and no rewritten block is empty; every other object of `B` is
dead and is removed with its id. Blocks of `B` not kept are removed. Any
other combination is `CorruptWal`. With `new` = `u64::MAX` there are no kept
or rewritten blocks.

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
flag with `debug_assert!` in `Store::begin_write`). Hence only the last frame
of the file can be unacknowledged.

### Recovery

Reading starts at byte 24. A frame is bad if its header or body is short or
its CRC does not match.

1. A bad frame with no frame with a valid CRC readable from any later byte up
   to the end of the file is a torn tail: the file is cut at the start of that
   frame (`set_len` + `fsync`). This is the only change recovery makes to the
   WAL besides rule 4. Any damage inside the last frame, in any byte order (for
   example, the first half unwritten and the second written), gives a torn
   tail, not an error: records without a checksum of their own are never
   taken for a frame.
2. A bad frame followed by a frame with a valid CRC means the middle is
   damaged: error `WalCorrupted { offset, next_frame }`, and the file is not
   changed.
3. A frame with a valid CRC but a `seq` out of order (a gap or a repeat),
   unparsable records, or records contradicting earlier frames is the error
   `CorruptWal { offset, reason }`, and the file is not changed.
4. The last complete frame: every block it introduces (PutBlock or
   PutInlineBlock) must be stored in full, decompress to its recorded raw
   length and CRC, split into its recorded objects, and each object's BLAKE3
   must equal its `object_id`. If not:
   * `mode` = ParallelSync and nothing follows the frame in the file: the
     frame was never acknowledged (a crash between the WAL and segment
     fdatasyncs). The WAL is cut at the frame start, the segment bytes become
     orphans for compaction, and the recovery report gets `FrameDropped`
     (reason `ObjectMissing`, `BlockCorrupted` or `ObjectHashMismatch`);
   * `mode` = Sequential or InlineSmall: the bytes were durable before the
     frame, so this is corruption: error `MissingObject` (block absent or
     short), `CorruptBlock` (does not decode) or `HashMismatch`, and no file
     is changed;
   * a torn tail follows the frame: the next frame had started, so this one
     was acknowledged (a frame starts only after the previous one is durable).
     Corruption in any mode: an error, and no file is changed.

   Residual ParallelSync risk: an acknowledged last frame whose block bytes
   are later corrupted on disk cannot be told from a crash before
   acknowledgement; such a frame is dropped (and reported).
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
| `WalTmpRemoved` | `wal.log.tmp` of an interrupted WAL creation was deleted |
| `TornTailTruncated { frame_seq, offset, bytes_removed }` | a torn last frame was cut (rule 1) |
| `FrameDropped { frame_seq, offset, mode, object, reason, bytes_removed }` | a complete last ParallelSync frame was dropped (rule 4) |
| `OrphanSegmentRemoved { segment }` | a segment file the WAL does not refer to was deleted |
| `UnknownTopicIndexRemoved { path }` | an index of a topic absent from the WAL was deleted |
| `TopicIndexRepaired { path, entries_kept, bytes_truncated, entries_rewritten }` | a topic index was cut and/or rewritten from the WAL |

`config_overrides` lists `Config` values the WAL replaced, which change no
file: `DictTrainBytes { configured, stored }` when `Config::dict_train_bytes`
differs from the WAL header's value (`stored` is used).

The report is kept in the open `Store`'s memory and is not written to disk.

## Topic index `topics/<hex>.idx`

A sequence of blocks of 64 entries; entry `topic_seq` is entry
`topic_seq mod 64` of block `topic_seq div 64`. `topic_seq` is not stored: the
file starts at seq 0 and is never cut (released entries stay in it; retention
writes only the WAL). The last block may be incomplete.

Block:

| Part | Encoding |
|---|---|
| entry 0 | `object_number` varint, `timestamp` varint |
| entries 1–63 | zigzag(`object_number` − previous) varint, (`timestamp` − previous) varint |
| after the 64th entry | `crc` u32: CRC32C of the block number (u64 LE, not written), then of the block's entry bytes |

Time does not decrease within a topic, so the time delta is unsigned. New
objects come in order (number delta +1), so an entry is usually 2 bytes; the
first entry of a block about 3 + 6 bytes; the CRC is 4 bytes per 64 entries.
The block number is part of the CRC, so a block at the wrong position fails
the check. The encoding is deterministic: the same entries always give the
same bytes.

**Lookup by `topic_seq`.** Block start offsets are kept in memory (8 bytes per
block): block `topic_seq div 64` is read with one positional read and decoded
up to the entry (at most 64), O(1). The table is built at open (the index is
read in full anyway for the check against the WAL) and extended on write.
`Store::index_entry(topic, seq)`.

**Reads.** Live entries are not held in memory: every read of an entry
(`Store::read`, `Store::entry`, `Store::index_entry`, and `release_before`
finding the entries to release) takes it from the index file. A full block
is used only if its CRC matches; otherwise the read fails with
`TopicIndexCorrupt` and returns none of its entries (damage after open,
which the check at open cannot see). The entries of the last, incomplete
block, which has no CRC, are kept in memory (at most 63 per topic) and read
from there. The last full block read is cached per topic.

The file is derived from the WAL: written after the WAL fsync and not synced
itself. At open the index is re-encoded from the WAL and compared with the
file byte by byte:

* the file equals the encoding: nothing is done;
* the file is longer and matches the whole encoding: the extra bytes (entries
  from a truncated WAL tail) are cut;
* in the first differing block the file holds 64 entries with a valid CRC
  while the WAL requires a full block with other entries: error
  `TopicMismatch` (the block was written that way);
* otherwise (an unwritten, zeroed, cut or foreign block) the file is cut at the
  end of the block's last whole matching entry and the rest is rewritten from
  the WAL; the report gets `TopicIndexRepaired`.

The last, incomplete block has no CRC of its own: a wrong entry in it cannot
be told from an unwritten one and is rewritten from the WAL (and reported),
not treated as an error.

## Write order (one batch)

`Config::write_mode` sets the mode. Before the steps below: deduplication
by BLAKE3 id, the dictionary if training is due, and the batch's new
objects compressed into one block.

`Sequential` (default):

```text
1. the block → the active segment, one write
   (a new segment: create_new + directory fsync)
2. fdatasync of every touched segment
3. the batch's frame → one write to wal.log
4. fdatasync of wal.log
5. topic index writes (no fsync)
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
     the batch's frame → write to wal.log → wal.log fdatasync
4. both chains done → topic index writes (no fsync)
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
| Publish | 2 + 4…40: topic number and seq 1–10 each; number and time deltas usually 1 byte each; the time of a frame's first Publish about 6 bytes for millisecond time |
| frame header | 21 / `b` |
| topic index entry | ~2 (+ ~9 for a block's first entry and a 4-byte CRC per 64 entries) |
| PutTopic | once per topic |
| PutDict | once per store, up to 64 KiB + 3 |

A message with already known bytes has no PutObject and adds nothing to a
block. `Stats::metadata_bytes` counts WAL and topic index bytes except inline
blocks; the dictionary counts as metadata. `examples/meta_bytes.rs` measures
these from the store's write counters (a 5-byte topic, millisecond time).

## Compaction order (per rewritten segment)

A segment is rewritten when one of its blocks holds a dead object or the
file holds bytes no block covers.

```text
1. blocks whose objects are all live → copied unchanged to seg-<new>.dat
   (decoded and hash-checked first); live objects of partly dead blocks
   (hash-checked) → one new block, compressed, appended;
   fdatasync, directory fsync
2. a frame with one Compact record → wal.log, fdatasync
3. the in-memory index switches to new; seg-<old>.dat is deleted
```

Blocks whose objects are all dead are not copied. A crash before step 2
leaves `new`, which the WAL does not refer to; a crash after it leaves `old`,
which the WAL no longer refers to. Both are deleted at open.
