# straum-store

A content-addressed log storage engine. Phase 1: one shard, local only, no
network protocol. Runtime: compio 0.19.2, io_uring on Linux and the polling
driver on macOS (development only). No tokio, including transitively.

- An object is the BLAKE3 hash of its bytes. The same bytes are stored once;
  publishing them again adds a reference and a new `topic_seq` entry.
- A publication may carry a key and headers (`Message::key`,
  `Message::headers`). They are stored with its Publish record in the WAL,
  not with the object: the same bytes under different keys are one object,
  and `Store::read_entry` returns each publication with its own key,
  headers and timestamp. Without key and headers a publication costs no
  extra byte (measured with `examples/meta_bytes`); with them, the key and
  headers are neither deduplicated nor compressed.
- Group commit: one segment `fdatasync` and one WAL `fdatasync` per batch;
  a batch is acknowledged only after both.
- Recovery from the WAL cuts a torn tail and checks topic indexes and
  reference counts against it. Every change recovery makes is listed in
  `Store::recovery_report()`.
- Time-based retention, and mark-and-sweep compaction that survives a crash
  at any point.

On-disk formats and recovery rules: [docs/format.md](docs/format.md).

Write modes (`Config::write_mode`):

- `Sequential` (default): the segment is synced before the WAL frame is
  written.
- `ParallelSync`: the segment and WAL syncs run concurrently. Recovery drops
  a last frame whose object bytes did not reach disk.
- `InlineSmall`: experimental, for benchmarks only, never a default. Small
  objects are written into the WAL and never reclaimed, because there is no
  WAL checkpoint yet.

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
| `src/codec.rs` | BLAKE3; WAL record encoding; rejection of every flipped bit and every truncated prefix; topic index entries |
| `src/store.rs` | reference count reconciliation; replay rejecting an inconsistent WAL; applying Compact; file names |
| `tests/store.rs` | deduplication; one fsync per file per batch; restart; rotation; compaction; orphans; corrupted objects and indexes; WAL truncation at every byte of the last record (Publish, Release, Compact) |
| `tests/model.rs` | proptest: random publish (with no key, an empty key, or a key and headers), release, compact and restart against an in-memory model, 128 cases per write mode |
| `tests/publication.rs` | keys and headers through restart, release and compaction in every write mode; one object for the same payload under different keys |
| `tests/crash.rs` | a writer in a child process killed with SIGKILL at a random moment, checked after each restart; one test per write mode |
| `tests/wal_corruption.rs` | any flipped bit in a middle frame gives `WalCorrupted` and leaves the file unchanged; in the last frame, truncation; a lost first half of the last frame is a torn tail; truncation at every byte; a `seq` gap; an old format gives `UnsupportedFormat`; `max_frame_bytes` |
| `tests/fault_injection.rs` | hand-built ParallelSync states: missing or wrong object bytes of the last frame drop the frame; a missing object of an earlier frame gives `MissingObject`; every write mode |
| `tests/recovery_report.rs` | a clean open reports and changes nothing; a torn tail, orphan segments and lost files are reported; an old format version gives `UnsupportedFormat` |

### Platform-specific tests

| Test | Platform | Checks |
|---|---|---|
| `tests/store.rs::linux_runtime_uses_io_uring` | `#[cfg(target_os = "linux")]` | compio selected the io_uring driver |
| `tests/store.rs::macos_runtime_uses_polling` | `#[cfg(target_os = "macos")]` | compio selected the polling driver |
| `tests/crash.rs::*` | Unix | `Child::kill` is SIGKILL on Unix |

CI on GitHub runners skips `linux_runtime_uses_io_uring` (the runner's
io_uring policy is not under the repository's control);
`scripts/container/linux.sh` runs it.

All other tests run on every platform. On macOS, `sync_data` is `fdatasync`,
which does not flush the drive cache, so durability holds on Linux only.

## Counters

`Store::stats()` returns counters since open: accepted messages,
deduplicated messages (the object already existed, including earlier in the
same batch), message bytes received, written as new objects and saved by
deduplication, metadata bytes (WAL and topic indexes, excluding object bytes
stored in the WAL), and fsyncs and bytes per file. Counters live in memory
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
| proptest (dev) | the model test |
| crc32c (dev) | the topic index corruption test recomputes CRCs |

`tools/dedup-survey` (a separate workspace crate, `publish = false`) also
depends on `zstd` =0.14.3, used only for the zstd-compressed plain-log
baseline. The version is exact because compressed sizes depend on it. No
histogram crate: percentiles come from a sorted `Vec<u64>`.
