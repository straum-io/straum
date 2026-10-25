# straum-store

A content-addressed log storage engine: one shard, local only, no network
protocol. Runtime: compio 0.19.2, io_uring on Linux and the polling driver on
macOS (development only). No tokio, including transitively.

- Every message is an object addressed by the full 32-byte BLAKE3 hash of its
  bytes. The same bytes are stored once; publishing them again adds a
  reference and a new `topic_seq` entry.
- The new objects of each batch form one block, compressed with zstd level 1
  and, once trained, a 64 KiB dictionary (`Config::dict_train_bytes`).
  Reads decompress the block and check its CRC and the object's BLAKE3; the
  last blocks read are cached (`Config::block_cache_blocks`, default 16,
  and `Config::block_cache_bytes`, default 64 MiB; a larger block is not
  cached). The training threshold is fixed in the WAL file header when the
  WAL is created.
- Group commit: one segment `fdatasync` and one WAL `fdatasync` per batch;
  a batch is acknowledged only after both.
- Recovery from the WAL cuts a torn tail and checks topic indexes and
  reference counts against it. Every change recovery makes is listed in
  `Store::recovery_report()`.
- Time-based retention, and mark-and-sweep compaction that survives a crash
  at any point: blocks whose objects are all dead are dropped, the live
  objects of partly dead blocks are recompressed into a new block.

On-disk formats and recovery rules: [docs/format.md](docs/format.md).

Write modes (`Config::write_mode`):

- `Sequential` (default): the segment is synced before the WAL frame is
  written.
- `ParallelSync`: the segment and WAL syncs run concurrently. Recovery drops
  a last frame whose block did not reach disk.
- `InlineSmall`: experimental, for benchmarks only, never a default. A block
  no larger than the threshold is written into the WAL and never reclaimed,
  because there is no WAL checkpoint yet.

## Space on logs without duplicates

Deduplication saves space only on exact repeats. Every new message also
carries metadata (WAL and topic index): 47.8–48.4 bytes per message in the
measurement below, 32 of them the id. Measured on three public Loghub logs
(OpenStack, SSH, BGL) as they are, with almost no exact repeats, batches of
64, `tools/dedup-survey`:

| Dataset | Store / plain log | Store / plain log with zstd (1 MiB chunks) |
|---|---|---|
| OpenStack | 0.254 | 3.13 |
| SSH | 0.508 | 7.26 |
| BGL | 0.406 | 4.47 |

Without block compression the same store was 1.162–1.415 times the plain
log and 14.4–20.2 times the compressed one. The remaining gap to the
compressed log is mostly the per-message metadata, which compression does
not touch. These logs are a proxy; they say nothing about the duplicate
share of real traffic.

## Build and test

```bash
cargo build
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
scripts/container/linux.sh --switch-machine test --workspace   # Linux, io_uring
```

The container script and its Podman machine setup are described in
`scripts/container/README.md`.

Test variables: `STRAUM_STORE_CRASH_ITERATIONS` (default 200),
`STRAUM_STORE_CRASH_SEED` (default 24301), `PROPTEST_CASES` (default 128).

| File | Covers |
|---|---|
| `src/codec.rs` | BLAKE3; WAL record encoding, one-byte deltas in batch records; rejection of every flipped bit and every truncated prefix; raw block layout; topic index entries |
| `src/block.rs` | compression round trip with and without a dictionary; deterministic training; a damaged block, a wrong length or CRC is rejected |
| `src/store.rs` | reference count reconciliation; replay rejecting an inconsistent WAL (block and object order, dictionaries); applying and rejecting Compact; file names |
| `tests/store.rs` | deduplication; one fsync per file per batch; restart; rotation; compaction; orphans; a corrupted block fails the read; corrupted indexes; WAL truncation at every byte of the last record (Publish, Release, Compact) |
| `tests/model.rs` | proptest: random publish, release, compact and restart against an in-memory model, 128 cases per write mode, with dictionary training and a two-block cache |
| `tests/crash.rs` | a writer in a child process killed with SIGKILL at a random moment, checked after each restart; one test per write mode; the dictionary is trained during the run |
| `tests/wal_corruption.rs` | any flipped bit in a middle frame gives `WalCorrupted` and leaves the file unchanged; in the last frame, truncation; a lost first half of the last frame is a torn tail; truncation at every byte; a `seq` gap; an old format gives `UnsupportedFormat`; `max_frame_bytes` |
| `tests/fault_injection.rs` | hand-built states: a missing, short or damaged block of the last frame drops the frame in ParallelSync and is an error otherwise; a missing block of an earlier frame gives `MissingObject`, a damaged one `CorruptBlock` on read; every write mode |
| `tests/blocks.rs` | duplicates within one block and across blocks; compaction of partly dead blocks, before and after restart; dictionary training once, read back from the WAL; a failed training recorded once; reads without a block cache |
| `tests/recovery_report.rs` | a clean open reports and changes nothing; a torn tail, orphan segments and lost files are reported; an old format version gives `UnsupportedFormat` |

### Platform-specific tests

| Test | Platform | Checks |
|---|---|---|
| `tests/store.rs::linux_runtime_uses_io_uring` | `#[cfg(target_os = "linux")]` | compio selected the io_uring driver |
| `tests/store.rs::macos_runtime_uses_polling` | `#[cfg(target_os = "macos")]` | compio selected the polling driver |
| `tests/crash.rs::*` | Unix | `Child::kill` is SIGKILL on Unix |

All other tests run on every platform. On macOS, `sync_data` is `fdatasync`,
which does not flush the drive cache, so durability holds on Linux only.

## Counters

`Store::stats()` returns counters since open: accepted messages,
deduplicated messages (the object already existed, including earlier in the
same batch), message bytes received, stored as new objects (before
compression) and saved by deduplication, metadata bytes (WAL and topic
indexes, excluding blocks stored in the WAL; the dictionary counts), and
fsyncs and bytes per file (segment bytes are compressed blocks).
`Store::dictionary_bytes()` gives the dictionary's length once trained. Counters live in memory
only, reset on every open, and count a message once its batch is
acknowledged. Nothing is logged or formatted per message.

`cargo run --release -p straum-store --example meta_bytes -- <empty dir>`
prints metadata bytes per message from these counters as CSV, for every
write mode, sizes {100, 1024, 10240} and batches {1, 64}.

## Benchmark

```bash
cargo run --release -p straum-store-bench -- \
    --dir /path/on/measured/disk --out <machine>-<date>.csv
```

Modes: `raw` (payloads copied into one buffer, one positional write and one
`fdatasync` per batch to one file), `hashcopy` (`raw` plus BLAKE3 per
message and CRC32C per batch), and `sequential`, `parallel`, `inline` (the
store in `Sequential`, `ParallelSync` or `InlineSmall` mode). Matrix: sizes
{100, 1024, 10240} × batches {1, 8, 64, 512}; a run lasts up to 1000 batches
or 1 GiB, at least 100 batches; 5 repetitions, then a `median` row.

CSV columns: `mode, msg_size, batch, rep, messages, batches, elapsed_ns,
msgs_per_s, fsyncs, fsyncs_per_batch, bytes_written, bytes_per_msg,
wal_bytes_per_msg, batch_p50_ns, batch_p99_ns, rep_min_msgs_per_s,
rep_max_msgs_per_s, spread_pct, dedup_msgs, bytes_saved,
meta_bytes_per_msg`. `rep_min_msgs_per_s`, `rep_max_msgs_per_s` and
`spread_pct` (max − min over median msgs/s, in percent) are filled in the
`median` row only. `dedup_msgs`, `bytes_saved` and `meta_bytes_per_msg` come
from `Store::stats()` and are 0 for `raw` and `hashcopy`. `elapsed_ns` sums
the batch write calls only. `bytes_written` for store modes is segments plus
WAL plus indexes.

Results from macOS, or from a container on macOS, show direction only.

## Dependencies

| Crate | Why |
|---|---|
| compio =0.19.2 | runtime and file I/O (`runtime`, `fs`, `macros`, `io-uring`); pinned because every API used was checked against this release |
| blake3 | object ids |
| crc32c | WAL and topic index checksums; the bench's `hashcopy` mode |
| thiserror | the `StoreError` domain enum |
| zstd =0.14.3 | block compression and dictionary training (default features off, `zdict_builder` on); exact version because stored bytes and sizes depend on it |
| proptest (dev) | the model test |
| crc32c (dev) | the topic index corruption test recomputes CRCs |

`tools/dedup-survey` and `tools/block-survey` (workspace crates, `publish =
false`) use the same zstd version: the first for the zstd-compressed
plain-log baseline, the second to measure block sizes, levels and
dictionaries on unique payloads, the measurement behind the block design.
No histogram crate: percentiles come from a sorted `Vec<u64>`.
