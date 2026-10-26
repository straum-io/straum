# On-disk formats

All integers are little-endian. Every variable-length record carries a length
prefix. CRC32C is Castagnoli (crate `crc32c`). An object id is the 32-byte
BLAKE3 hash of the object's raw bytes. Code: `src/codec.rs` (WAL, topic index
entries) and `src/store.rs` (file names, segments).

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

## Data segment `seg-<id>.dat`

The raw bytes of objects concatenated, without headers or framing:

```text
offset 0                                                     file len
| object A (len_A bytes) | object B (len_B bytes) | ... | orphan tail? |
```

Only the WAL knows where an object lies (PutObject and Compact records).
Bytes no WAL record points to (orphans of a crash before the WAL fsync) are
allowed: compaction removes them. A zero-length object takes 0 bytes.

## WAL `wal.log`

A file header (16 bytes), then frames. One frame is one batch of messages
(`put_batch`), one `release_before` call, or one compaction step (one
rewritten segment).

### File header

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `magic` = `53 54 52 4d 57 41 4c 00` (`"STRMWAL\0"`) |
| 8 | 4 | `version` u32 = 8 |
| 12 | 4 | `crc` u32: CRC32C of bytes `[0..12)` |

A new WAL is written to `wal.log.tmp`, synced (`fsync`), renamed to `wal.log`,
and then the directory is synced. So `wal.log` never exists without a complete
header. A file that does not start with exactly these 16 bytes (a WAL without
a header, an empty file, another version) is `UnsupportedFormat`, and the file
is not changed. Earlier versions are not supported: 1 had no mode byte in the
frame, 2 full ids in Publish and in the topic index, 3 `topic_seq` in topic
index entries, 4 fixed-width Publish and Release with the topic name, 5
fixed-width PutObject and PutInline, 6 a topic index of 20-byte entries, 7
Publish without the publication's key and headers.

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

`mode` tells whether the frame's object bytes could become durable after the
frame itself:

| `mode` | Written by | Object bytes durable before the frame? |
|---|---|---|
| 1 Sequential | a batch in Sequential mode; every `release_before` and compaction frame | yes: the segment fdatasync precedes the frame write; release and compaction frames carry no new bytes (compaction's new segment is synced before its frame) |
| 2 ParallelSync | a batch in ParallelSync mode | no: the segment and WAL fdatasyncs run concurrently |
| 3 InlineSmall | a batch in InlineSmall mode | objects in the WAL are inside the frame (CRC); objects in segments: yes, Sequential order (`flush_segment`, then `append_frame` in `put_batch_inner`) |

`Config::max_frame_bytes` (default 64 MiB) sets the largest frame. An
operation that would produce a larger frame is rejected with `FrameTooLarge`
before any I/O; for a batch the bound counts every object as new.

### Record

| Field | Size |
|---|---|
| `len`: payload length, varint | 1–10 |
| `type` u8: 1 PutObject, 2 Publish, 3 Release, 4 Compact, 5 PutInline, 6 PutTopic | 1 |
| payload by type, below | `len` |

A record has no checksum of its own: the frame CRC covers it.

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
reused either). Compaction does not change it (only the location). The full
BLAKE3 id is written once, in PutObject or PutInline; Publish and the topic
index refer to the number. In memory: id → number (deduplication) and number →
(location, length, refcount); both are rebuilt from PutObject/PutInline. The
next number after open is the highest in the WAL plus one (the WAL is never
cut; a future WAL checkpoint has to store it explicitly).

### 1 — PutObject (payload 36–72 bytes)

| Field | Size |
|---|---|
| `object_id` (BLAKE3), full | 32 |
| Δ`object_number`, varint: from the expected next number, normally 0 | 1–10 |
| `segment` varint (never `u64::MAX`) | 1–10 |
| `offset` varint: offset in the segment | 1–10 |
| `len` varint: object length, ≤ u32::MAX | 1–5 |

The expected next number at the frame start is the next object number when
the frame starts; after each PutObject/PutInline it is that record's number
plus one. Numbers are assigned in order, so the delta is normally 0 (one
byte). The number must be above every earlier number in the WAL, otherwise
`CorruptWal`.

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
number when the frame starts (known from earlier frames: the highest number in
PutObject/PutInline plus one), and the time base is 0, so the time is written
in full. Deltas are computed modulo 2^64. A repeated message referring to an
old object gives a negative number delta.

### 3 — Release (payload 2–20 bytes)

| Field | Size |
|---|---|
| `topic_number` varint | 1–10 |
| `topic_seq` varint: always the topic's first live entry | 1–10 |

### 4 — Compact (payload 20 + 40·`count` bytes)

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `old` u64: the rewritten segment |
| 8 | 8 | `new` u64: the new segment; `u64::MAX` = none (no live objects) |
| 16 | 4 | `count` u32 |
| 20 + 40·i | 32 | `object_id` of the i-th kept object |
| 52 + 40·i | 8 | `offset` u64 in `new` |

Objects of `old` not listed are removed. On replay the listed set must equal
the objects of `old` with refcount > 0, otherwise it is an error.

### 5 — PutInline (payload 33–42 + object length)

Only in `WriteMode::InlineSmall { threshold }` (default 4096): a new object no
larger than the threshold is stored inside the WAL frame.

| Field | Size |
|---|---|
| `object_id` (BLAKE3), full | 32 |
| Δ`object_number`, varint: as in PutObject | 1–10 |
| the object's bytes, to the end of the payload | the rest |

The object length is the record's `len` minus 32 and minus the length of the
number varint. The object index records the storage kind and location:
segment + offset (PutObject) or the absolute offset of the object's bytes in
`wal.log` (PutInline). On replay the BLAKE3 of the bytes is compared with
`object_id`; a mismatch under a valid frame CRC is `CorruptWal`.
Deduplication does not depend on the location: a known id adds a reference.

**Objects in the WAL are never reclaimed.** Compaction rewrites segments only;
a PutInline object with no references stays in the index and in the WAL.
Reclaiming them needs a WAL checkpoint and truncation, which do not exist yet.

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

Reading starts at byte 16. A frame is bad if its header or body is short or
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
4. The last complete frame: every object it introduces (PutObject) must lie in
   full in its segment, and the BLAKE3 of the bytes read must equal
   `object_id`. If not:
   * `mode` = ParallelSync and nothing follows the frame in the file: the
     frame was never acknowledged (a crash between the WAL and segment
     fdatasyncs). The WAL is cut at the frame start, the segment bytes become
     orphans for compaction, and the recovery report gets `FrameDropped`;
   * `mode` = Sequential or InlineSmall: the bytes were durable before the
     frame, so this is corruption: error `MissingObject` (object absent or
     short) or `HashMismatch` (wrong bytes), and no file is changed;
   * a torn tail follows the frame: the next frame had started, so this one
     was acknowledged (a frame starts only after the previous one is durable).
     Corruption in any mode: an error, and no file is changed.

   Residual ParallelSync risk: an acknowledged last frame whose object bytes
   are later corrupted on disk cannot be told from a crash before
   acknowledgement; such a frame is dropped (and reported).
5. Objects of earlier frames are checked at open for presence and bounds
   only: the object must lie in full within its segment file, otherwise
   `MissingObject` (an error, not a repair). Their content is not hashed at
   open; it is checked on every read (`HashMismatch`).

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

The index holds no key or headers. In memory each live entry keeps the WAL
offset and length of its Publish record's attributes (16 bytes more per
live entry than without them); `Store::read_entry` reads them from
`wal.log`, which is never rewritten. Replay rebuilds those offsets from the
WAL.

**Lookup by `topic_seq`.** Block start offsets are kept in memory (8 bytes per
block): block `topic_seq div 64` is read with one positional read and decoded
up to the entry (at most 64), O(1). The table is built at open (the index is
read in full anyway for the check against the WAL) and extended on write.
`Store::index_entry(topic, seq)`.

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

`Config::write_mode` sets the mode.

`Sequential` (default):

```text
1. new objects → the active segment, one write per segment
   (a new segment: create_new + directory fsync)
2. fdatasync of every touched segment
3. the batch's frame → one write to wal.log
4. fdatasync of wal.log
5. topic index writes (no fsync)
6. acknowledgement
```

`InlineSmall`: Sequential order. If the batch holds only objects stored in the
WAL, steps 1–2 do not happen: one fdatasync per batch.

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

A crash between steps 3 and 5 can leave a durable frame without durable object
bytes. Such a frame is the last in the file (the next batch does not start
before the previous one is acknowledged), and recovery drops it (rule 4
above).

## Metadata per message

For a batch of `b` messages:

| Part | Bytes |
|---|---|
| PutObject (new object only) | 2 + 32 + Δnumber 1 + segment 1 + offset 1–4 (segments up to 64 MiB) + length 1–5 |
| Publish | 2 + 4…40: topic number and seq 1–10 each; number and time deltas usually 1 byte each; the time of a frame's first Publish about 6 bytes for millisecond time; plus the attributes: 0 bytes without key and headers, 2 for an empty key, `1 + key` + 1 for a key below 127 bytes and no headers |
| frame header | 21 / `b` |
| topic index entry | ~2 (+ ~9 for a block's first entry and a 4-byte CRC per 64 entries) |
| PutTopic | once per topic |

A message with already known bytes has no PutObject. The metadata hardly
depends on the message size: only the varint lengths of the segment offset and
the object length do. In `InlineSmall`, for an object up to the threshold,
PutInline replaces PutObject: 2–3 + 33 bytes of metadata plus the object's
bytes in the WAL; nothing goes to a segment.

`examples/meta_bytes.rs` measures these from the store's write counters (a
5-byte topic, millisecond time).

## Compaction order (per rewritten segment)

```text
1. live objects (hash checked) → seg-<new>.dat, fdatasync, directory fsync
2. a frame with one Compact record → wal.log, fdatasync
3. the in-memory index switches to new; seg-<old>.dat is deleted
```

A crash before step 2 leaves `new`, which the WAL does not refer to; a crash
after it leaves `old`, which the WAL no longer refers to. Both are deleted at
open.
