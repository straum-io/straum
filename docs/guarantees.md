# Guarantees of Straum version 0

What the broker in this repository promises today: one node, protocol
version 0 ([protocol.md](protocol.md)), the straum-store engine with WAL
format 13 ([format.md](../crates/straum-store/docs/format.md)). Anything
this page does not state is not guaranteed. Section 7 lists the changes
under way that will alter it.

The statements hold on Linux. macOS is a development platform: there the
store's `fdatasync` does not flush the drive's cache, so an acknowledgement
on macOS does not mean the data is on stable storage.

## 1. What an acknowledgement means

A Produce declares its acknowledgement level (`ack_level`), and the level
defines what the acknowledgement promises:

| Level | The acknowledgement means | Version 0 |
|---|---|---|
| `durable` | the messages are stored and synchronized to stable storage on the node (`fdatasync`) after the group commit | the only level |
| `replicated` | the messages are committed by a majority of the cluster's nodes | refused (no cluster) |
| `written` | the messages are accepted and written without synchronization | refused (no interval mode) |

A Produce with `written` or `replicated` is refused with
`UnsupportedAckLevel` before anything is stored. The rest of this page
describes `durable`.

- The broker commits the Produce requests of all connections together, in
  groups. A group is one `put_batch` call of the store and one WAL frame;
  a request never spans two groups. There is no timer: a request that
  arrives alone is committed at once.
- `ProduceOk` is sent after the group's commit returned, which is after
  the group's new message bodies and its WAL frame were written and
  `fdatasync`ed on this node. There is no other copy: version 0 has one
  node and no replication.
- A Fetch returns only messages whose group commit has returned.
- If the response to a Produce is lost (connection closed, timeout), the
  messages may or may not be stored. Version 0 does not check
  `producer_id` and `producer_seq`, so resending the request may store
  its messages twice, under new seqs.
- If a write to disk fails, that group's requests get `StorageError`, and
  so does every later Produce, CreateTopic, and Fetch that reads a stored
  message, until the broker is restarted (the store refuses all use after
  a failed write; a Fetch with nothing to read still waits and answers
  empty). At restart the store is recovered from disk (section 3). A group
  answered with `StorageError` may still be stored: the failed write can
  come after its WAL frame was durable (for example while its index
  records were written), and restart then recovers the group, as for any
  request whose answer was lost.

### Write modes

The broker setting `store_write_mode` selects how a group reaches disk. All
three modes acknowledge after both `fdatasync`s returned; they differ in
the order of the syncs, and so in what recovery may remove.

| Mode | Order before the acknowledgement | What recovery removes after a process crash (damage at rest: see the residual risks) |
|---|---|---|
| `sequential` (default) | new bodies written to a data segment and synced; then the WAL frame written and synced | only a WAL frame that was never acknowledged (a torn last frame) |
| `parallel` (ParallelSync) | the segment sync and the WAL sync run at the same time | a torn last frame, as above; and a complete last frame whose bodies are not on disk (a crash between the two syncs): it was never acknowledged, it is dropped and reported as `FrameDropped` |
| `inline` (InlineSmall, experimental) | a compressed block of new bodies up to 4096 bytes inside the WAL frame, larger blocks in segments as in `sequential` | as `sequential` |

Residual risks:

- **The last WAL frame.** Damage to the last frame of the WAL after it was
  written (a bit flip at rest, a partial overwrite) cannot be told from a
  write torn by a crash. Recovery cuts the frame, in every mode, and
  reports `TornTailTruncated`; if it had been acknowledged, its messages
  are lost.
- **ParallelSync.** If an acknowledged last frame's bodies are later
  damaged on disk, recovery cannot tell that from a crash between the two
  syncs: the frame is dropped and reported as `FrameDropped`, and its
  acknowledged messages are lost. In `sequential` and `inline` the same
  state is reported as corruption and the store refuses to open.
- In both cases the seqs of the removed frame are assigned again to later
  messages.
- **InlineSmall** stores small bodies in the WAL, which is never cut:
  their space is never reclaimed. The mode is for benchmarks.

## 2. What survives a crash

**Tested: the process killed.** Two tests kill a writer with SIGKILL at
random moments and check every acknowledged write after each restart:

- the store alone, in each of the three write modes: every acknowledged
  message is read back with its bytes, no seq is assigned twice, every
  live entry's body is stored (compaction steps run between batches);
- the broker binary, in the default `sequential` mode, with two producers
  sending messages with keys, headers and repeated bodies: every
  acknowledged message is fetched with its seq, timestamp, key, headers
  and body. 200 kills per run (50 at the push validation level).

**Not tested: power loss, a kernel crash, and a drive that loses or
reorders writes it reported as flushed.** Those depend on `fdatasync`
and the drive honouring cache flushes. No test cuts power or simulates a
lost page cache. The broker's end-to-end crash test does not run in the
`parallel` or `inline` modes.

## 3. Recovery

At startup the store replays the WAL from the beginning
([format.md](../crates/straum-store/docs/format.md), "Recovery").

- **What it changes.** Only these changes are made, and each is listed in
  the recovery report, which the broker prints to stderr at startup (one
  line per change):

  | Report entry | Change |
  |---|---|
  | `TornTailTruncated` | the last frame of the last WAL file, short or failing its CRC with no valid frame after it, is cut |
  | `FrameDropped` | a complete last ParallelSync frame whose bodies are missing or wrong is cut |
  | `OrphanSegmentRemoved` | a data segment the WAL does not refer to is deleted |
  | `IndexSegmentRemoved` | an index segment without an intact header is deleted; the index records it held are rewritten from the WAL (`TopicIndexRepaired`) |
  | `IndexSegmentDropped` | only when the operator asked for a rebuild (`--store-rebuild-index`, a command-line flag for one start; a `store_rebuild_index` line in the configuration file is refused): every index segment is deleted and the index rewritten from the WAL (`TopicIndexRepaired`) |
  | `StaleIndexRecordsFenced` | index records of WAL frames that recovery cut were found; a new, empty index segment is created so they stay ignored once new frames reuse those frame numbers |
  | `TopicIndexRepaired` | full index blocks and seek pages of a topic that no index segment holds intact (the index is derived from the WAL and not synced) are re-encoded from the WAL and appended to a new index segment |
  | `WalTmpRemoved` | a `wal-<seq>.log.tmp` of an interrupted WAL file creation is deleted |
  | `CheckpointFileRemoved` | a checkpoint file the checkpoint in use does not list (an earlier manifest, a tmp manifest, a table or run of an interrupted checkpoint) is deleted |
  | `CheckpointManifestDamaged` | a checkpoint manifest, read newest first, fails its CRC, version or name check, or does not decode (one entry per such manifest): the newest intact older manifest is used instead; with none, the checkpoint is ignored and the state rebuilt from the WAL, which still holds every frame (no WAL file is deleted yet). The manifest is not deleted: it moves, unchanged, into a quarantine directory `quarantine-<n>` (an existing one is never overwritten; the next free suffix `.1`, `.2`, … is used) |
  | `CheckpointFileQuarantined` | each file moved into that quarantine: the damaged manifest and the tables and runs it may list, kept for diagnosis |
  | `CheckpointQuarantineResumed` | a quarantine an earlier open or checkpoint began and did not finish (its directory exists, the manifest still in place) is completed in the same directory; the files found there are listed, so every change is reported at least once |

  The last four concern the store's checkpoints (ADR-0014), which the
  broker does not write yet. A quarantine is reported at least once (exactly once without a crash), by the open
  that made it, or, if that open was interrupted before moving the
  manifest, by the open that completes it (a crash after the manifest's
  move and before the report is returned leaves those moves unreported,
  like any repair in that window; the warning and the count still show
  them); while quarantined
  files exist, the broker prints one
  startup warning (`straum-server: warning: …`, not a recovery line) until
  an operator removes them. An empty report means nothing was changed.
- **What it refuses.** Damage anywhere else stops the store from opening,
  and no file is changed: a damaged frame followed by a valid one
  or in a WAL file that another follows (`WalCorrupted`), a valid frame
  that is out of sequence or contradicts earlier frames, a missing or
  extra WAL file, or data or index segments with no WAL file at all
  (`CorruptWal`; a new WAL would make those segments orphans, so the
  directory is refused instead of emptied), a body of the last frame missing or wrong
  in `sequential` or `inline` mode (`MissingObject`, `HashMismatch`), a
  body of an earlier frame missing or cut short (`MissingObject`), a
  block of the last frame that does not decompress or fails its checks
  (`CorruptBlock`), an intact index block that disagrees with the WAL
  (`TopicMismatch`) or that the WAL cannot have (`IndexSegmentCorrupt`), a
  WAL of another format version (`UnsupportedFormat`), a table or run
  that the checkpoint in use lists missing or damaged
  (`CorruptCheckpoint`; the fallback is for a damaged manifest only), a
  checkpoint that disagrees with the WAL
  (`CheckpointMismatch`). The broker then
  does not start. A damaged header of an index segment that fences stale
  records also fails open with `TopicMismatch`; the operator then starts the
  broker once with the flag `--store-rebuild-index`, which rebuilds the index
  from the WAL and reports each deleted index segment
  (`IndexSegmentDropped`) and rebuilt topic (D-229 A).
- **Detected on read.** At open, earlier frames' bodies are checked only
  for presence and length. Every read checks a body's block (length,
  CRC32C, layout) and the body's BLAKE3 id, and every full index block,
  seek page or attribute record (the keys and headers of a full index
  block's messages, one CRC per block) read against its CRC, topic and
  number (`TopicIndexCorrupt`);
  a failure is `StorageError` in the Fetch response, and the broker keeps
  serving other reads and writes.
- **Not detected on read.** Keys and headers of the newest messages of a
  partition (those not yet in a full index block of 64) are read from the
  WAL at fetch time and checked only for a valid encoding; the WAL frame's
  CRC is checked at open, not on each read. Damage to them after open that
  keeps a valid encoding is returned to the client as is. Timestamps and
  seqs are served from memory built at open.

## 4. Ordering, seqs and timestamps

- **Order** is per partition, by seq. The messages of one Produce get
  consecutive seqs with no other message between them. Two Produce
  requests, from different connections or pipelined on one, are ordered
  as the broker commits them; the protocol does not promise that
  pipelined requests commit in the order they were sent.
- **Seqs** start at 0 in each partition and increase by one per message.
  Version 0 releases nothing (retention is recorded, not enforced), so
  every acknowledged message stays readable at its seq, apart from the
  residual risks of section 1.
- **Timestamps** are assigned by the broker when a group commits, in
  milliseconds since the Unix epoch; clients cannot set them. All
  messages of one group get the same timestamp. A timestamp is never
  below the last one assigned or stored, across restarts too: if the
  system clock steps back, timestamps stay at the last value until the
  clock passes it. They are stored in the WAL and fetched unchanged after
  a restart.

## 5. Deduplication

- The store keeps each distinct body once, identified by the BLAKE3 hash
  of the body bytes alone.
- A key and headers belong to each publication: the same body sent with
  different keys or headers is stored once and fetched with each
  publication's own key and headers. Keys and headers are neither
  deduplicated nor compressed.
- Every publication gets its own seq and timestamp, whether its body was
  new or not. Deduplication is not visible in Produce or Fetch; it shows
  only in `BrokerStats` (`messages_deduplicated`, `body_bytes_new`).

## 6. Limits of version 0

- **One node, one partition per topic, replication factor 1.** A topic
  with more partitions or replicas is refused with `Unsupported`.
  `DeleteTopic` is refused with `Unsupported`.
- **Loopback only, no authentication.** The only mechanism is `none`; the
  broker refuses to start with a `listen` address that is not loopback.
  There is no TLS.
- **Client-kept offsets.** The broker stores no consumer positions; a
  consumer keeps its next seq itself.
- **No idempotent produce.** See section 1: a resent Produce may be stored
  twice.
- **Unstable formats.** The protocol (version 0), the WAL format and the
  topic index format may change incompatibly. A data directory written by
  another WAL format version is refused (`UnsupportedFormat`), not
  converted; there is no upgrade path yet.
- **Memory grows with stored data.** The store keeps, per stored body,
  entries in memory (the object index), and nothing bounds them: the
  broker does not refuse writes as memory grows. Topic entries are read
  from disk; a message with a key or headers keeps 32 bytes in memory
  only until its index block of 64 is full (at most 63 per partition). Measured with 100-byte bodies (store alone, batches of 512,
  10 million unique messages, macOS) before topic entries moved to disk:
  the object index took about 178 bytes allocated per stored body with
  compressed blocks (ADR-0014). A Linux process limited to 2 GiB was
  killed before 10 million unique messages.
- **Disk grows with stored data.** The WAL files are never deleted,
  and nothing is released (retention by age is not enforced, topics
  cannot be deleted). Startup time grows with the WAL, which is replayed
  in full.

## 7. Changes under way

Each item changes this page when it is merged.

- **A cap on stored data**, with writes refused at the cap: decided as a
  stopgap for memory growth, not built.
- **Retention by age** for the next broker version: decided, not built.
  Messages below a partition's `start_seq` are then no longer readable.
- **WAL checkpoint and disk-resident indexes**: memory bounded by a
  configured budget instead of the number of stored messages (default
  256 MiB), the WAL in files that checkpoints reclaim, startup time bounded
  by the checkpoint interval. Beyond the budget, deduplication becomes
  partial: a repeat of old content may be stored again as a new body. This
  stays invisible to clients. In progress on the branch `store-core`
  (ADR-0014): topic entries are already read from shared index segments
  instead of memory.
- **Protocol version 0 layouts**: `ListTopics` pagination and a bound on
  the topics of a `Metadata` request
  ([#93](https://github.com/straum-io/straum/issues/93)); open questions
  on client behavior, among them whether clients resend `CreateTopic`
  after a lost connection
  ([#92](https://github.com/straum-io/straum/issues/92)).
