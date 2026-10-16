# Straum

Straum is a replicated message broker written in Rust: MQTT 3.1.1 and
MQTT 5 over TCP, a Cargo workspace of crates with enforced dependency
boundaries, sans-IO cores, a segmented log with group commit and compaction,
and a gateway/sequencer cluster where the committed log is the only source
of truth (ADR-0008). The old leader/follower broker was removed at step 8
(D-111); what remains is experimental and not production-ready. This file is the working guide; the
architecture is in `docs/architecture/unified-stream.md`.

## Sources, in order of precedence

```text
1. docs/architecture/unified-stream.md   architecture, target model, rules of work
2. CLAUDE.md                             this file: working rules
3. docs/adr/                             decisions; every deviation from (1) is one
4. everything else                       CONSTITUTION.md, docs/*.md, .claude/
```

Where two disagree, the higher one decides and the lower one is corrected.
Decisions marked **(initial)** are the current choice. Change one only with a
benchmark or a concrete requirement, and update this file in the same change.
CONSTITUTION.md is level 4 (D-35): its rules apply where they do not
contradict (1), and an ADR that replaces one of them says so.

Details live in:

```text
docs/architecture/current-implementation.md   the old broker as it was (removed at step 8, D-111); history
docs/adr/                 architecture decision records
docs/configuration.md     every setting, env var, default and limit
docs/testing.md           test layers, MQTT conformance, fuzzing, simulation
docs/algorithms.md        how a data structure is chosen; normative
docs/benchmarks.md        harness, workload matrix, measurement protocol, reporting
docs/memory.md            what a connection allocates, and what the budget really bounds
docs/performance.md       the current measured baseline
docs/profiling.md         which tool answers which question, per platform
docs/storage.md           the write-ahead log: group commit, durability, compaction
docs/replication.md       the old leader/follower replication (removed at step 8, D-111); history
docs/linux-tuning.md      kernel parameters and socket options for production hosts
docs/ecosystem.md         straum-cli and straum-console: the brief and where it starts (D-140)
docs/baselines/           recorded benchmark baselines, one file per machine
```

## Philosophy (unified-stream §1)

- The cluster is one mechanism, not a set of autonomous nodes.
- The log is the only source of truth: every observable effect — PUBACK,
  delivery, retained, session state, inflight QoS 1, expiry, will, a change
  of sequencer or of membership — is the application of a committed record.
- A node is a deterministic function of (committed log + input events).
- Predictability before peak speed. Time never decides safety: clocks and
  timeouts only trigger transitions; decisions go through the log and the
  quorum.
- Correctness is shown by deterministic simulation, performance by
  measurement on Linux.

## Current phase

The rewrite to gateway/sequencer, ADR-0008 (D-48 C). Step 8 — removing
the old leader/follower path — was done early, on 2026-10-11 (D-111),
overriding D-51 A's and D-75's conditions: the server now runs only the new
path. Steps 1 (`journal`: record v4, every record carrying the previous batch's hash), 2
(`storage::Segments`, `cluster`: sequencer and replica, three nodes in `sim`)
3a (`codec`: MQTT 3.1.1 and 5 over one packet model), 3b-1 (`apply`: the apply
function over committed records) and 3b-2 (`gateway`: MQTT clients over the
log, end to end in `sim`) are done; step 4 (`node`: the runtime driver —
links, sequencer or replica, the log on disk, the gateway over TCP — three
nodes on loopback in tests, local snapshots and compaction, D-67 B) is
done; step 5 (`cluster::Voter`: ungraceful takeover by durable promises,
`SlotStart` and truncation by the slot table, D-69 A) is done and runs
in every simulation (D-70 A; its takeover criterion is liveness after a
fault, 9 in 10 per bounded seed group, D-81 A and D-82 A, and no
stall in any cycle a majority can recover, with the liveness contract in
ADR-0008, D-85; patience grows only after a majority promised, D-88 A; an append
from a newer slot's owner counts as heard, D-98 A;
flow control on replica lag is built in `cluster`, off by default, with
its invariants proved in ADR-0008, D-89; its D-57 A deviation is closed by D-73 A and
D-79 A); step 6 (planned handoff by
`SlotEnd`, optional rotation) is done.
Step 7a (D-78 A, D-102 A–D-105 A): the server can start a node on the new
path behind `STRAUM_EXPERIMENTAL_CLUSTER=1` — experimental, off by default,
not production-ready, flow control off; wills, persistent sessions and
expiry refused at CONNECT until 7c; MQTT and links on loopback unless an
operator acknowledges an unauthenticated network (D-107). The progress of
each step is recorded in ADR-0008. There is no single-node mode: the path
needs three voters (D-104 A), and ADR-0006's durability levels went with
the old path — a publish is acknowledged once a majority holds it.

## Scope

MQTT 3.1.1 and MQTT 5 over TCP, TLS optional. MQTT 5 is designed in from
the start (ADR-0003): properties, reason codes and reason strings are part
of the model, and the WAL record carries a properties section. QoS 2 and
shared subscriptions are phase 6, but the log model allows them now. Out of
scope: WebSocket, CRDT or eventual consistency, safety built on physical
time.

## Rules of work (unified-stream §7)

- Small verifiable steps. Each step carries tests, simulation scenarios and
  unchanged benchmarks.
- No claim of better performance without a measurement on Linux. Numbers
  from macOS or Docker are marked preliminary.
- Every deviation from unified-stream.md is recorded in an ADR and in the
  report.
- No node gets logic that changes shared state other than through the log.
  If a task seems to need it, stop and propose options.
- A task ends with: what was done; what was verified, and with what; open
  risks; the next step as 2–3 worked options with their trade-offs, for
  Stanislav to choose from (decision cards, decision-options skill).
- Every task that changes files ends with the task-closeout skill; a Stop
  hook enforces it. Do not commit unless asked.
- Do not do work the task did not ask for. Notice it, and make it a card.
- Answers of the form `D-<n>: <choice>` refer to `docs/decisions/pending.md`.

## Unsafe (unified-stream §4.1, ADR-0002)

`unsafe_code = "forbid"` in every crate by default. Unsafe is allowed only
when it is justified and provably sound — no safe caller of the public API
can cause UB — and all of these hold: a Linux profile shows a gain safe code
does not give, with the safe version kept as a baseline behind a feature
flag; it lives in a small crate of its own (`straum-sys-*`) with a safe
public API; every block has `// SAFETY:` and every `unsafe fn` a `# Safety`
section; it is tested under Miri where possible, otherwise with sanitizers
and fuzzing, and its failures are scenarios in the simulation; and it has an
ADR, with Stanislav's final consent. Never unsafe to silence the borrow
checker, and never as an early step in the priority order of section 11.

Sections 3–6 and 16 of the previous version of this file moved to
`docs/architecture/current-implementation.md`. The sections below keep their
numbers.

---

# 1. Workspace

```text
straum/
├── Cargo.toml
├── CLAUDE.md
├── CONSTITUTION.md
├── clippy.toml
├── crates/
│   ├── domain/       # topics, client ids, shared value types
│   ├── auth/         # credentials and ACL; not wired yet (D-110)
│   ├── transport/    # TCP listener, socket options, TLS
│   ├── storage/      # the segmented log: Wal per segment, group commit, compaction
│   ├── journal/      # record v4 bodies and the prev_hash chain (ADR-0008)
│   ├── cluster/      # sans-IO sequencer, replica, voter (ADR-0008 steps 2, 5, 6)
│   ├── codec/        # MQTT 3.1.1 and 5, one packet model (ADR-0008 step 3a)
│   ├── apply/        # the apply function over committed records (ADR-0008 step 3b-1)
│   ├── gateway/      # sans-IO MQTT gateway over codec and apply (ADR-0008 step 3b-2)
│   ├── node/         # publish = false; the driver: links, log on disk, gateway over TCP (step 4)
│   ├── server/       # the binary: one node of the cluster path (step 7a, D-111)
│   └── sim/          # publish = false; deterministic simulation (ADR-0007, ADR-0008)
├── fuzz/                    # cargo-fuzz, separate workspace, nightly only
├── docs/
└── scripts/
    ├── check-boundaries.sh
    └── linux.sh
```

Root `Cargo.toml`:

```toml
[workspace]
members = ["crates/*"]
resolver = "3"

[workspace.package]
edition = "2024"
rust-version = "1.85"

# Unsafe policy: docs/architecture/unified-stream.md §4.1, ADR-0002.
# `forbid` everywhere until a crate is exempted under that policy; an
# exempted crate does not inherit this table (ADR-0002 describes its own).
# The other three are inert while nothing is exempt and become binding the
# moment something is.
[workspace.lints.rust]
unsafe_code = "forbid"
unsafe_op_in_unsafe_fn = "deny"

[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
dbg_macro = "deny"
todo = "deny"
undocumented_unsafe_blocks = "deny"
multiple_unsafe_ops_per_block = "deny"

# Frozen build configuration for all performance work. Changing any of these
# values is itself a benchmark variable (section 11).
[profile.release]
lto = "thin"
codegen-units = 1
panic = "unwind"

# bench inherits release; only debug info is added, which does not change codegen.
[profile.bench]
debug = "line-tables-only"
```

Every member crate declares:

```toml
[package]
edition.workspace = true
rust-version.workspace = true

[lints]
workspace = true
```

`clippy.toml`:

```toml
allow-unwrap-in-tests = true
allow-expect-in-tests = true
```

A genuinely impossible invariant in production code is marked on the smallest
possible item:

```rust
#[expect(clippy::expect_used, reason = "length checked by decode_header above")]
```

Root-level `tests/` and `benches/` are not used: in a virtual workspace they
belong to no package and cargo does not run them. Cross-crate tests go to
the crate that composes them (`node`, `server`).

---

# 2. Dependency Boundaries

```text
server
└── node
    ├── gateway ── apply, codec, domain, journal
    ├── apply ──── domain, journal
    ├── cluster ── journal
    ├── codec ──── domain
    ├── storage
    └── transport

sim (tests) → gateway, codec, apply, cluster, journal, storage, domain
auth ──────── domain   (no dependents yet; D-110)
```

| Crate | Internal deps | External deps (baseline) | Forbidden, including transitive |
|---|---|---|---|
| domain | — | bytes, thiserror | tokio, every other workspace crate |
| auth | domain | bytes, thiserror, argon2 | tokio, every other workspace crate |
| transport | — | bytes, thiserror, tokio, tracing, socket2, tokio-rustls, rustls-pemfile | every other workspace crate |
| storage | — | bytes, thiserror, crc32fast | tokio, every other workspace crate |
| journal | — | thiserror, xxhash-rust | tokio, every other workspace crate |
| cluster | journal | thiserror | tokio, every other workspace crate |
| codec | domain | bytes, thiserror | tokio, every other workspace crate |
| apply | domain, journal | bytes, thiserror | tokio, every other workspace crate |
| gateway | apply, codec, domain, journal | bytes | tokio, every other workspace crate |
| node | apply, cluster, codec, gateway, journal, storage, transport | bytes, crc32fast, thiserror, tokio, tracing | server, sim |
| server | node | thiserror, tokio, tracing, tracing-subscriber | — |
| sim | storage, journal, cluster, apply, codec, gateway, domain | — | tokio, transport, node, server |

`scripts/check-boundaries.sh` enforces the forbidden column. It is part of
validation.

Keeping tokio out of the cores (`cluster`, `codec`, `apply`, `gateway`) is
deliberate: they are synchronous, deterministic and benchmarkable without a
runtime or a scheduler in the measurement, and the simulation drives them.

`socket2` in transport: socket options tokio does not expose (TCP keepalive
timing, `TCP_USER_TIMEOUT`). It is already a transitive dependency of tokio.

`node` re-exports what `server` needs (`NodeConfig`, `Experimental`,
`ConfigError`), so the server depends only on `node`.

Allowed dev-dependencies, each with a concrete use:

```text
proptest                              domain, codec, journal, apply: codec round-trip, topic matching, apply invariants
divan                                 micro benchmarks, allocation counting
rumqttc (default-features = false)    node tests only: independent MQTT client
```

```text
tokio-rustls transport: TLS. Not something to hand-roll, and rustls is pure
             Rust with no system OpenSSL. Built on `ring` (no cmake, no
             NASM) — which does compile C, so the cross-target clippy for
             Linux only runs in the container now (§17).
rustls-pemfile transport: reading the certificate and key from PEM.
crc32fast    storage: WAL record checksums; node: the snapshot file's checksum. The hand-rolled table version
             measured 244 MB/s against 15 GB/s here — below NVMe write
             throughput, so it would have capped the WAL. Same polynomial,
             identical values, one transitive dependency (cfg-if).
xxhash-rust  journal: xxh3-128 for the batch chain `prev_hash` (D-50, ADR-0008).
             Non-cryptographic: it detects divergence and corruption, not
             an adversary. `default-features = false, features = ["xxh3"]`,
             no transitive dependencies. Contains unsafe; judged as a
             dependency under ADR-0002 (D-50).
```

Not added by default, each requires a concrete reason:

```text
serde prost rmp-serde dashmap parking_lot crossbeam rayon ahash
criterion anyhow libc nix rustix jemalloc mimalloc
```

The allocator stays the system allocator. Replacing it is a benchmark variable
and a separate change, not part of an optimization.

Before adding any dependency consider allocations, copies, transitive
dependencies, compile time, binary size, cross-compilation to Linux, and
maintenance cost. State the reason in the change report.

---

# 7. Invariants

* **Ordering.** Messages published by one session to one topic are delivered to
  each subscriber in publish order. Any concurrency change must preserve this.
* **Nothing is acknowledged before it is committed:** a PUBACK is the
  application of a committed record, and a commit needs a majority of the
  voters to hold it durably (unified-stream §1, §5).
* **Memory is bounded** under any input: per connection, per session, and
  globally — the gateway's pending proposals and waiting deliveries in total
  (`max_total_pending_bytes`, `max_total_waiting_bytes`), `apply`'s queued
  bytes in total, and the transport's connection-buffer budget for read and
  write buffers together. Each node also holds its own proposals awaiting
  commit, at most its share `max_queue / voters` (D-109 B, D-120 A), a
  count without a byte budget of its own. Each global budget exists because a per-session
  limit times the session count is a product, not a bound.
* **Malformed input never panics**, never reads out of bounds, never allocates
  from an unchecked remote length, never loops without progress — MQTT
  packets (`codec`), link frames (`cluster::wire`) and records (`journal`).
* **Fan-out does not copy payloads** in held memory: N subscribers share
  one allocation.
* **Closing a session** removes all of its subscriptions and inflight state
  through the log, as every effect.

---

# 8. Limits and Backpressure

Every limit, its default, its owner crate and its env var are defined in
`docs/configuration.md`. Changing a default is a behavior change.

Remote-controlled values are checked by the owning crate before allocation or
insertion. There are no unbounded queues, including channels, `Vec`s grown from
remote input, and maps keyed by remote data.

Overflow policy **(initial)**, the new path:

| Situation | Action |
|---|---|
| A publisher's proposals awaiting commit at `max_pending`/`max_pending_bytes`, or all at `max_total_pending_bytes` | the gateway stops reading that connection (`Gateway::wants_read`); nothing dropped |
| A proposal not committed within `commit_timeout` | the publisher's connection is closed; the client resends (at-least-once); nothing uncommitted is acknowledged |
| A proposal while the slot changes hands | held by the node that made it, in order, and sent to the next sequencer once the old slot is closed in the committed log; never sent twice into one slot (D-109 B) |
| A node's own proposals at its share of the queue (`max_queue / voters`, never 0) | the node reads none of its MQTT clients until one commits (backpressure, D-120 A; keep-alives are not read either, D-123); a proposal made meanwhile without a read (a lifecycle retry) is refused and counted (`Status::refused`), never acknowledged |
| The sequencer's queue full (`max_queue`) | the forward is dropped and counted (`Voter::forwards_refused`); once a later one of its node commits it is given up, handed back and counted (`Status::given_up`), never reordered; with nothing later it waits for the slot to change (stranded, D-120); `commit_timeout` closes its publisher |
| Uncommitted tail at `max_uncommitted` | the sequencer makes no new batch; proposals wait in its queue |
| At-most-once deliveries behind a QoS 1 delivery at `max_waiting`/`max_waiting_bytes`, or all at `max_total_waiting_bytes` | dropped and counted (`Load::dropped`) |
| A client's write queue full (`write_queue`) | the connection is closed (a writer that makes no progress) |
| A peer's link queue full (`link_queue`) | the frame is dropped; the protocol resends at the next heartbeat — except a forward, which is lost as in the row above |
| Connection-buffer budget exceeded | the growth is refused before it is allocated; the connection is closed |
| A replica further behind than `retain_bytes` | the log is compacted past it; it is lost and needs a state copy (D-57 A, step 7b) |

Tests: the gateway's rules (`gateway/tests/rules.rs`), the uncommitted
bound (`cluster/tests/protocol.rs`), retention and loss (the cluster
simulation); not every row has a test of its own on the new path yet.

---

# 9. Ownership and Copies

* `Bytes` for shared immutable payloads, `&[u8]` when borrowing suffices,
  `Bytes::slice()` / `split_to().freeze()` over copying a sub-range.
* Move instead of clone. `Bytes::clone()` only for intentional shared ownership,
  and remember it is an atomic refcount operation, not free.
* No `bytes → String → parse`; parse from bytes.
* Buffers are reused, not reallocated per operation: read buffers, write
  buffers, the `Outbox`, and per-publish scratch state.

**Small payloads.** A payload sliced from the read buffer keeps the whole buffer
chunk alive while it sits in a slow subscriber's queue: a 64 B message can pin
16 KiB. Payloads ≤ `codec::Limits::payload_copy_threshold_bytes` are copied once at
ingress into an exact-size allocation. That is one copy per publish, not per
subscriber. The threshold is chosen by benchmark and an RSS test.

**Encoding.** The writer copies the payload into its write buffer per subscriber
unless vectored writes win in a benchmark. The no-copy rule in section 7 is
about memory held by the broker.

Do not claim code is "zero-copy" or "allocation-free" unless an allocation count
or a benchmark shows it. Zero-copy is not automatically faster: one `memcpy` of
64 bytes can beat an atomic refcount plus a scattered write.

---

# 10. Hot Path, Concurrency, Async

Hot path:

```text
TCP read → decode → validate → propose → sequence → append and replicate → commit → apply → route → encode → TCP write
```

Every allocation, copy, clone, lock, channel hop, task spawn, syscall and
(de)serialization on it needs a reason.

Nothing accidental on the hot path: no logging or formatting, no string
construction, no configuration lookup, no re-validation of data validated at
construction, no temporary collections, no work whose result is discarded.
Do not compute the same value twice unless recomputation measured cheaper than
keeping it.

* The router decides who receives a message; delivery is separate. No temporary
  collections, no topic clones beyond refcounts. Do not assume a hash map beats
  a short linear scan, and do not pick a structure by asymptotics alone: lookup
  cost, iteration cost, footprint, cache locality, allocation behavior and
  contention all count. Measure the actual access pattern.
* No locks by default. Prefer ownership, batching, bounded channels. No global
  lock around subscriptions, sessions, routing or queues. Never hold a lock
  across `.await`.
* Atomics are not free. Before adding one: why is the state shared, what is the
  contention pattern, can ownership remove it.
* No lock-free structures without a benchmark against the simple version.
* Syscalls are expensive boundaries; batch reads and writes. For network paths
  track messages/sec, syscalls/sec and bytes/sec separately.
* A function is `async` only if it awaits I/O or another async operation. The
  cores — `cluster`, `codec`, `apply`, `gateway` — are synchronous (enforced:
  no tokio).
* Moving code into a helper does not make the hot path smaller. Generated code
  is what matters.

---

# 11. Performance Work

Performance is an architectural requirement, measured as useful work per CPU
cycle. Process rules are mandatory; the full protocol is in
`docs/benchmarks.md`, tooling in `docs/profiling.md`.

## Loop

```text
1. record a baseline (docs/baselines/<machine>.md)
2. profile to find the actual bottleneck — do not guess
3. make ONE meaningful change
4. measure: alternate baseline / candidate, at least 5 rounds
5. compare medians, not best runs
6. run correctness tests
7. run neighboring benchmarks from the matrix
8. accept, or revert and investigate
```

The first good number is not a result. It may be noise, frequency drift, cache
warm-up, allocator state, scheduling, or the compiler deleting the work being
measured. A change is accepted only after repeated measurement.

## Rules

* No optimization without a recorded baseline. One variable at a time: source
  change, compiler flags, profile settings, thread count and workload are
  separate variables.
* A benchmark run on macOS shows direction only. Accepted numbers come from a
  quiet Linux host, never from a container on macOS.
* Noise floor **(initial)**: on a pinned, quiet Linux host treat differences
  below 3% as noise; on a development Mac, below 10%. If variance is larger than
  the effect, fix the environment before drawing a conclusion.
* No greedy optimization. `+20%` on one benchmark with `-40%` on another is a
  regression unless the trade-off is documented and matches the real workload.
  Optimizing routing means re-running the whole routing matrix.
* Allocation and memory behavior are reported alongside throughput. A throughput
  win with significant allocation growth is a trade-off, not an improvement.
* Benchmarks must measure real work. Consume results with `black_box`; do not
  add artificial complexity to defeat the optimizer.
* Never benchmark with debug logging enabled, unless logging overhead is the
  subject.
* Keep benchmarks in the repository, including ones that expose regressions.
  A benchmark that disproves an optimization is a useful result.

## Priority order

```text
1.  remove work that does not contribute to the result
2.  remove allocations
3.  remove copies
4.  improve ownership flow
5.  improve data locality
6.  reduce synchronization and contention
7.  batch (syscalls, wakeups, channel hops)
8.  improve algorithm or data structure
9.  reduce branches, dispatch and call overhead where measurable
10. inspect generated code
11. low-level techniques
```

The fastest instruction is the one that does not execute. Do not jump to unsafe,
SIMD or manual memory management. Those sit at the end of the list and require
lifting `unsafe_code = "forbid"` under unified-stream §4.1 (ADR-0002), on top of a measured gain on
a quiet Linux host across the relevant matrix.

## Claims

Only numbers with a workload. Forbidden without a measurement attached:
"much faster", "blazing fast", "highly optimized", "near-zero overhead",
"zero-cost", "production-grade performance".

```text
64 B publish, 1 publisher, 10 subscribers:
1.18 M msg/s → 1.31 M msg/s, median of 5 rounds, +11.0%
```

## Done

A performance change is complete when the baseline was recorded, the bottleneck
identified, correctness tests pass, measurements were repeated and their
variance evaluated, neighboring workloads were checked, allocation and memory
behavior were considered, the result is reproducible from the report, and any
non-obvious optimization is documented in the code with the reason it exists.

Report with the template in `docs/benchmarks.md`. If the change produced no
meaningful improvement, say that. Do not select benchmarks to make a change look
successful.

---

# 12. State Machines and Errors

Connection and session lifecycles are explicit enums, not booleans. Invalid
transitions are rejected explicitly and tested.

Errors: `thiserror`, domain-specific (`MqttError`, `TransportError`,
`BrokerError`, `ConfigError`, ...). No `anyhow`, no string errors, no
`Box<dyn Error>` where a concrete type is practical. `unwrap`/`expect` only in
tests or behind `#[expect(..., reason = "...")]` (section 1).

Error construction must not allocate or format on the hot path; formatting
happens where the error is logged, not where it is created.

---

# 13. Platforms: Linux Production, macOS Development

Production targets: `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`.
Development happens on macOS (`aarch64-apple-darwin`). Everything must build,
pass tests and run on both.

## Platform code

* All OS-specific code lives in `transport/src/sys/`
  (`mod.rs`, `linux.rs`, `macos.rs`). No `#[cfg(target_os)]` elsewhere.
* Only the OS call or file read is gated. Parsing and decisions are portable
  functions tested on macOS with fixture strings.

```rust
// sys/mod.rs: portable, unit-tested everywhere
pub fn parse_somaxconn(raw: &str) -> Result<u32, SysError> { ... }

// sys/linux.rs: thin, reads /proc/sys/net/core/somaxconn
#[cfg(target_os = "linux")]
pub fn somaxconn() -> Result<u32, SysError> { ... }
```

* No `libc`/`unsafe` for probing. Linux values come from `/proc/sys/...` and
  `/proc/self/limits`.

## Tuning model

Two layers, strictly separated:

1. **Socket and process settings**: the node applies them (`transport`); the
   server reads only the `STRAUM_CLUSTER_*` settings (`docs/configuration.md`).
2. **Kernel parameters** (`sysctl`, `RLIMIT_NOFILE`): set by the operator.
   Straum **never writes** them: it needs privileges and changes the whole
   host.

The old broker read the relevant kernel parameters at startup, resolved
`auto` values from them and warned where configuration exceeded what the
kernel allows; that went with it at step 8 (D-111), and the server does not
probe the kernel yet. The probes (`transport/src/sys/`) and
`docs/linux-tuning.md` remain.

## Developing on macOS

* Linux-gated code is compile-checked on macOS with a cross-target clippy run
  (section 17). One-time setup: `rustup target add x86_64-unknown-linux-gnu`.
* Linux-only tests run in a container (podman or docker): `scripts/linux.sh test --workspace`.
  It uses a separate `target` volume so macOS and Linux artifacts never mix.
* If a cross-target check fails because a dependency compiles C in a build
  script, run the check in the container instead. Do not install a cross
  toolchain.
* Performance work on macOS is exploratory only. Apple Silicon schedules work
  across performance and efficiency cores with no portable pinning, and Docker
  Desktop adds a VM with virtualized time and network. Numbers that justify a
  decision come from a Linux host (`docs/benchmarks.md`).
* Profiling tools differ per platform; see `docs/profiling.md`. `perf` and
  `cargo flamegraph` are Linux-host tools and are not available through Docker
  Desktop on macOS.
* macOS shells default to `ulimit -n 256`. Raise it for local load tests.

---

# 14. Traits, Generics, Unsafe

Start concrete. A trait needs two real implementations, a real test boundary,
or a concrete architectural requirement. No speculative `trait Transport`,
`trait Protocol`, `trait Broker`, `trait Queue`. Dynamic dispatch on the hot
path needs a measured reason.

Unsafe: see "Unsafe" at the top of this file, unified-stream §4.1 and
ADR-0002.

---

# 15. Instrumentation and Metrics

`tracing` for lifecycle, configuration, kernel checks, protocol and transport
errors, shutdown. Never per message in the default path. No `?`-formatted
payloads or topics inside high-frequency code. Release builds cap the compiled
level (`tracing` release max level feature) so hot-path macros compile away.

Metrics: plain atomics (`Relaxed`), no framework. An atomic per message is
itself a cost: prefer per-task counters aggregated on read where a benchmark
shows contention.

A latency is a gauge of the latest value, not a counter: the average of a
bimodal latency is a number that never happened.

The old broker's counters went with it at step 8 (D-111). The new path
measures in the node loop (`node::metrics`, D-112): plain integers and
fixed-bucket histograms owned by one task, no atomics, nothing allocated per
event, monotonic time, and nothing read back into a decision. A snapshot is
built only when asked (`Node::metrics`) and served as Prometheus text written
by hand on `STRAUM_CLUSTER_METRICS_LISTEN`, off by default (§2: no
framework). A node's own proposals are counted by state, never mixed:
refused (never accepted), accepted and awaiting commit, committed, given up.
The series, each with a HELP line, are in `node/src/metrics.rs`.

---

# 17. Workflow

For every change:

1. Read the affected crate, its `Cargo.toml`, and consumers of the affected API.
2. Identify data flow, ownership, concurrency, hot path.
3. Read existing tests and benchmarks.
4. Pick the smallest correct implementation in the crate that owns the behavior.
5. Write tests, including failure paths.
6. Implement.
7. If the hot path changed: baseline, measure, follow section 11.
8. Validate.

Do not:

* create crates or files before inspecting the workspace
* rewrite working code for style
* add a dependency or an abstraction without a reason
* change public APIs, limits, defaults, protocol semantics, queue or error
  behavior silently
* remove or weaken a benchmark because it shows a regression
* remove tests because the implementation changed
* mix unrelated changes, and never mix a build-configuration change with a
  source change in the same measurement

If existing code establishes a convention, follow it. If behavior is ambiguous
and neither this file, `docs/`, nor existing tests decide it, stop and ask.

## Validation

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
# transport depends on ring, which compiles C: this one only runs in the
# container (§13), not against the cross target on macOS.
scripts/linux.sh clippy --workspace --all-targets --all-features -- -D warnings
# Benchmarks are built but not run. `harness = false` targets are plain
# programs that `cargo test --all-targets` runs too. divan recognises that
# and does one short pass; the hand-written load generators
# (storage/benches/commit.rs, node/benches/cap.rs, server/benches/puback.rs) refuse to run as a debug
# build, which is what `cargo test` produces (D-4). `--tests` selects every target with `test = true` — library
# and binary unit tests, integration tests — and benches default to `false`.
# Setting `test = false` on a bench does not help: `--all-targets` runs it
# anyway (checked).
cargo test --workspace --all-targets --no-run
cargo test --workspace --tests
scripts/check-boundaries.sh

# when code under transport/src/sys/ or other Linux-only behavior changed:
scripts/linux.sh test --workspace --tests

# when the hot path changed: the relevant benchmarks, named in the report
cargo bench -p <crate> --bench <name>
```

## Reporting

```text
Changed:
- ...

Tests:
- ...

Benchmarks:
- <crate>/<bench>, platform: <os, cpu>   (or: not applicable)

Performance:
- full template from docs/benchmarks.md, or: no performance-sensitive change

Notes:
- new dependencies and why; deviations; open questions
```

Short and factual.

## Definition of Done

* the owning crate implements it; boundaries hold (`check-boundaries.sh`)
* fmt, clippy (host and Linux target), tests pass
* failure paths tested
* Linux-only code tested in the container if touched
* hot-path changes carry a completed performance report (section 11)
* no new allocations, copies, locks, async boundaries, syscalls or dependencies
  without a stated reason
* `CLAUDE.md` and `docs/` match the actual behavior

---

# 18. Writing Style

Comments explain why, not what.

```rust
// Bounded: a slow consumer must not turn a traffic spike into
// unbounded memory growth.
queue.push(message);
```

A non-obvious optimization carries the measurement that justifies it:

```rust
// Linear scan: faster than a map up to ~32 subscribers on the routing
// benchmark (64 B payload, 2026-09, see docs/baselines/).
```

Public docs state behavior, ownership, errors, blocking/async behavior,
invariants, limits, and performance-sensitive decisions. Short, concrete
engineering language: a word that makes a claim has to be a claim somebody
measured, which rules out both the vague speed claims of section 11 and the
adjectives that only sound like one.

---

# 19. Principles

```text
ownership over synchronization
borrowing over cloning
buffer reuse over allocation
batching over per-message scheduling
bounded queues over unbounded queues
concrete types over premature traits
explicit lifecycle over hidden tasks
enforcement by tooling over enforcement by prose
measurement over assumptions
repeated measurement over one good number
simple code over clever code
```

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
- Graphify resolves symbols by name, not by type: `mpsc::Sender` and `codec::Sender` become one node, and `use domain::Durability` an import of `durability.rs`. Check bridges, god nodes and import cycles by hand before acting on them (D-75).
