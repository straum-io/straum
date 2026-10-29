# straum-broker

The Straum broker, version 0: one node, one shard, the binary protocol of
[docs/protocol.md](../../docs/protocol.md) over TCP, on the
[straum-store](../straum-store/README.md) engine. Runtime: compio, io_uring
on Linux, the polling driver on macOS (development only).

```bash
cargo run --release -p straum-broker -- \
    --data-dir /var/lib/straum --cluster-id prod --node-id 1 --listen 127.0.0.1:7171
```

The listen address must be a loopback address: version 0 has no
authentication, so the broker refuses any other address until
authentication exists.

## How it works

- **One shard on one core.** One compio runtime thread owns the store (it
  is `!Send`). Connections, the group commit and waiting fetches are tasks
  on that thread; there is no other thread and no cross-thread
  communication.
- **Group commit across connections.** Admitted Produce requests of every
  connection go to one queue. A single commit task takes everything queued
  while the previous group was committing, up to `max_group_bytes`,
  `max_group_messages` and the store's frame limit, and calls
  `Store::put_batch` once per group. Each request is answered after its
  group's commit returns (`durable`: data and WAL `fdatasync`ed). There is
  no timer: a lone request commits at once, a busy broker commits larger
  groups.
- **Backpressure.** Every request read counts against its connection's
  `max_inflight_bytes` until its response is written (a Fetch also reserves
  `max_fetch_bytes` for its response); every Produce counts against the
  global `max_pending_bytes` until it is answered. At either limit the
  broker stops reading from that socket; nothing is buffered without bound.
- **Fetch** reads from `max(from_seq, start_seq)` up to `max_bytes` of
  encoded messages and at most `max_fetch_messages` messages. With no
  data it waits up to `max_wait_ms`, woken by the next commit of that
  partition (no polling); at most `max_fetch_waiters` fetches wait at once.
- **Topics** are created explicitly (CreateTopic). Their configuration is a
  record in the store's reserved topic `m/topics`, so it is in the WAL and
  replays the same on any node. Partition `p` of topic `name` is the store
  topic `t/<name>/<p>`; version 0 supports one partition per topic.
- **Placement** goes through an explicit table `(topic, partition) →
  (leader, replicas, shard, leader_epoch)`; in version 0 every entry is this
  node, epoch 1.
- **Identity.** `cluster_id` and `node_id` are written to
  `<data_dir>/identity` on the first start; a later start with others is
  refused.
- **Timestamps** are assigned at group commit, never below the last one
  stored, and recorded through the store.
- **Startup**: identity check, store open, the store's recovery report on
  stderr once, `straum-broker: compio driver <driver>`, then
  `straum-broker: listening on <address>`. **SIGTERM or SIGINT**: no new
  connections or requests, queued groups committed, waiting fetches
  answered, answers written (up to `shutdown_timeout_ms`), store closed.
  Nothing is logged per message.
- A failed store write makes every later write answer `StorageError` until
  the broker is restarted (the store refuses writes after an I/O error).

## Configuration

`--config <file>` reads `key = value` lines (`#` comments); every key is
also a flag, `--key value`, with `-` or `_`; flags win.

| Key | Default | |
|---|---|---|
| `data_dir` | required | identity file and the store (`store/`) |
| `cluster_id` | required | at most 64 bytes |
| `node_id` | required | not 0 |
| `listen` | `127.0.0.1:7171` | loopback only: version 0 has no authentication, so another address is refused at startup |
| `advertised_host`, `advertised_port` | from `listen` | the address Metadata reports |
| `max_frame_bytes` | 8 MiB | larger frames: `FrameTooLarge`, connection closed |
| `max_message_bytes` | 1 MiB | body |
| `max_key_bytes` | 1024 | |
| `max_headers` | 64 | per message |
| `max_header_name_bytes` | 256 | |
| `max_header_bytes` | 16 KiB | names and values of one message |
| `max_group_bytes` | 4 MiB | a group always takes at least one request |
| `max_group_messages` | 4096 | |
| `max_inflight_bytes` | 16 MiB | per connection; at least `max_frame_bytes + max_fetch_bytes` |
| `max_pending_bytes` | 64 MiB | all connections; at least `max_frame_bytes` |
| `max_fetch_bytes` | 4 MiB | caps a Fetch's `max_bytes` |
| `max_fetch_wait_ms` | 30000 | caps a Fetch's `max_wait_ms` |
| `max_fetch_waiters` | 1024 | more waiting fetches: `Overloaded` |
| `max_fetch_messages` | 10000 | messages per Fetch response, at least 1 |
| `max_connections` | 1024 | more are accepted and closed |
| `shutdown_timeout_ms` | 5000 | |
| `store_write_mode` | `sequential` | `sequential`, `parallel` (ParallelSync; not recommended on disks without power-loss protection, see the store's README), `inline` (InlineSmall, experimental) |
| `store_segment_max_bytes` | 64 MiB | |
| `store_max_frame_bytes` | 64 MiB | the store's WAL frame limit; at least `max_frame_bytes`. The store trains its compression dictionary only in a group whose frame leaves 64 KiB + 21 bytes free, so keep it well above `max_group_bytes` |

**Rebuilding the index.** `--store-rebuild-index` (a flag without a
value, on the command line only) rebuilds the store's topic index from its
WAL at this one start: the way out of a `TopicMismatch` at startup
(D-229 A). The index segments are deleted and re-derived; the WAL and data
are not touched. The startup output says so and lists the recovery
actions: one `IndexSegmentDropped` per deleted segment and one
`TopicIndexRepaired` per rebuilt topic. The next start without the flag
opens normally. If the rebuilding start is interrupted, start again with
the flag. A `store_rebuild_index` line in the configuration file, or the
flag with a value, is refused at startup (exit status 2), so a rebuild
never repeats at every start unnoticed.

## Tests

| Test | Checks |
|---|---|
| unit tests | configuration, identity file, topic records and placement, the single-threaded mutex, event and one-shot |
| `tests/broker.rs` | produce and fetch with keys and headers; concurrent producers get distinct contiguous seqs; fetch waiting and timeout; admin requests agree; a restart keeps topics, messages and timestamps; another identity refuses to start |
| `tests/errors.rs` | every error code version 0 sends, each from a request; `StorageError` from damaged object bytes |
| `tests/backpressure.rs` | a client that never reads: pending bytes within the bound (from BrokerStats), reading stops, other connections served |
| `tests/durability.rs` (long) | the binary as a child process, killed with SIGKILL at random moments while producing, restarted: every acknowledged message is fetched with its seq, timestamp, key, headers and body; `STRAUM_BROKER_CRASH_ITERATIONS` (200), `STRAUM_BROKER_CRASH_SEED` (24301) |
| `tests/conformance.rs` (long) | every scenario of `conformance/scenarios/`, each against a fresh broker process |

Long tests are `#[ignore = "long: ..."]` and run with
`cargo test --workspace -- --include-ignored` (`scripts/validate.sh push`
or `pr`, and CI on every PR).

## Benchmark

`crates/straum-broker-bench` is the load generator:
`straum-broker-bench --addr <host:port>` with sizes, producers and messages
per Produce (see its source header). `scripts/bench/broker-e2e.sh` runs the
whole matrix on a Linux machine from a fresh clone:

```bash
scripts/bench/broker-e2e.sh --out ~/broker-e2e            # full matrix
scripts/bench/broker-e2e.sh --out ~/broker-e2e-quick --quick
```

It checks its preconditions (Linux; io_uring not disabled; `rustc` and
`cargo` at least the workspace `rust-version`, printing the rustup command
if missing; `--out` on a local block device filesystem; free space), never
uses sudo or installs anything, records the machine in `env.md`, samples
load and CPU frequency, builds the checked-out commit in a worktree under
`<dir>/work`, and for every size {100, 1024, 10240} × producers {1, 4, 16} ×
messages per Produce {1, 64}, 5 repetitions, starts a broker on a fresh data
directory on that disk, runs the load generator over loopback for 10 s (or
1 GiB of bodies), and stops the broker with SIGTERM. Results:
`e2e.csv` (msgs/s, ack latency p50/p99, groups and fsyncs per second),
`e2e-median.csv`, `broker-cpu.csv` (the broker's CPU time per message, from
`/proc`), `quiet.csv`, `env.md` and `commands.log`. The broker and the load
generator share the machine's CPUs. The exit status is 0 only if every step
succeeded; `<dir>/work` is deleted at the end unless `--keep`.

Results from macOS, or from a container on macOS, show direction only.

## Dependencies

| Crate | Why |
|---|---|
| straum-store, straum-proto | the engine and the protocol |
| compio =0.19.2, features `net`, `time`, `signal` | TCP, timeouts of waiting fetches, SIGTERM and SIGINT; the same pinned compio as the engine, adding its `compio-net` and `compio-signal` crates, which bring `nix` (through `compio-signal`) and `either` (through `compio-net`) |
| crc32c | the identity file's checksum; already used by the engine |
| thiserror | the startup error enum |

Single-threaded synchronization (an async mutex over the store, events,
one-shot replies) is in `src/sync.rs` on the standard library: no channel
or executor crate.
