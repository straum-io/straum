# Straum

Straum is a message broker written in Rust.

One deployable server binary, built from a Cargo workspace of crates with
enforced dependency boundaries. The first external protocol is
**MQTT 3.1.1 over TCP**. The broker itself is independent of MQTT and TCP.

`CONSTITUTION.md`, in this directory, governs how the work is done: what
counts as evidence, what may not be optimized without it, and which
invariants are contractual. It sits above this file. Where the two appear to
disagree, the constitution decides and this file is what gets corrected.

This file holds rules and decisions. Details live in:

```text
docs/configuration.md   every setting, env var, default and limit
docs/testing.md         test layers, MQTT conformance, fuzzing
docs/algorithms.md      how a data structure is chosen; normative, see §28
docs/benchmarks.md      harness, workload matrix, measurement protocol, reporting
docs/memory.md          what a connection allocates, and what the budget really bounds
docs/performance.md     the current measured baseline
docs/profiling.md       which tool answers which question, per platform
docs/storage.md         the write-ahead log: group commit, durability, compaction
docs/cluster.md         architecture for clustering; nothing in it is built yet
docs/replication.md     durable replication: the audit, and what contradicts it
docs/linux-tuning.md    kernel parameters and socket options for production hosts
docs/baselines/         recorded benchmark baselines, one file per machine
```

Decisions marked **(initial)** are the current choice. Change one only with a
benchmark or a concrete requirement, and update this file in the same change.

---

# 1. Workspace

```text
straum/
├── Cargo.toml
├── CLAUDE.md
├── CONSTITUTION.md
├── clippy.toml
├── crates/
│   ├── domain/
│   ├── protocol/
│   ├── auth/
│   ├── mqtt/
│   ├── transport/
│   ├── storage/
│   ├── replication/
│   ├── broker/
│   ├── runtime/
│   ├── server/
│   └── e2e/          # publish = false; e2e tests, network benchmarks
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

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
dbg_macro = "deny"
todo = "deny"

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
`e2e`.

---

# 2. Dependency Boundaries

```text
server
└── runtime
    ├── broker
    │   ├── protocol ── domain, auth
    │   └── domain
    ├── mqtt
    │   ├── protocol
    │   └── domain
    ├── transport
    ├── storage
    └── replication

e2e (dev only) → runtime, rumqttc
```

| Crate | Internal deps | External deps (baseline) | Forbidden, including transitive |
|---|---|---|---|
| domain | — | bytes, thiserror | tokio, every other workspace crate |
| auth | domain | bytes, thiserror, argon2 | tokio, every other workspace crate |
| protocol | domain, auth | bytes, thiserror | tokio, mqtt, transport, broker, runtime, server |
| mqtt | domain, protocol | bytes, thiserror | tokio, transport, broker, runtime, server |
| transport | — | bytes, thiserror, tokio, tracing, socket2, tokio-rustls, rustls-pemfile | protocol, mqtt, broker, runtime, server |
| storage | — | bytes, thiserror, crc32fast | tokio, protocol, mqtt, transport, replication, broker, runtime, server |
| replication | — | thiserror, crc32fast | tokio, every other workspace crate |
| broker | domain, protocol, auth | bytes, thiserror, tracing | tokio, mqtt, transport, runtime, server, storage |
| runtime | domain, protocol, mqtt, transport, broker, storage, replication | thiserror, tokio, tracing | server |
| server | runtime | tokio, tracing, tracing-subscriber | — |

`scripts/check-boundaries.sh` enforces the forbidden column. It is part of
validation.

Forbidding tokio in `mqtt` and `broker` is deliberate: the codec
and the broker are synchronous, deterministic and benchmarkable without a
runtime or a scheduler in the measurement.

`socket2` in transport: socket options tokio does not expose (TCP keepalive
timing, `TCP_USER_TIMEOUT`). It is already a transitive dependency of tokio.

`runtime` re-exports the config types that `server` needs, so the
server depends only on the runtime.

Allowed dev-dependencies, each with a concrete use:

```text
proptest                              core, mqtt, broker, replication: codec round-trip, topic matching, routing invariants
divan                                 micro benchmarks, allocation counting
rumqttc (default-features = false)    e2e only: independent MQTT client
```

```text
tokio-rustls transport: TLS. Not something to hand-roll, and rustls is pure
             Rust with no system OpenSSL. Built on `ring` (no cmake, no
             NASM) — which does compile C, so the cross-target clippy for
             Linux only runs in the container now (§17).
rustls-pemfile transport: reading the certificate and key from PEM.
crc32fast    storage: WAL record checksums. The hand-rolled table version
             measured 244 MB/s against 15 GB/s here — below NVMe write
             throughput, so it would have capped the WAL. Same polynomial,
             identical values, one transitive dependency (cfg-if).
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

# 3. Crate Responsibilities

## domain

Domain types and pure logic. No I/O, no clock, no async.

```rust
Message  Topic  TopicFilter  ClientId  SessionId  DeliveryId  DeliveryGuarantee
```

`Topic` and `TopicFilter` are validated on construction and cheap to clone
(backed by `Bytes`). Validation happens once, at construction; nothing
downstream re-validates or re-parses. Topic filter matching is a pure function
here; the routing index lives in the broker.

`DeliveryGuarantee` is `AtMostOnce | AtLeastOnce`. MQTT QoS numbers are mapped
to it in `mqtt`.

## protocol

The contract between protocol implementations and the broker (section 5).
No MQTT vocabulary: `Connected`, not `ConnAck`.

## mqtt

A **sans-IO** MQTT 3.1.1 implementation:

* packet decoding and encoding
* validation
* per-connection protocol state and packet identifiers
* keep-alive and connect deadlines, computed from a `std::time::Instant`
  passed in by the caller
* mapping packets ↔ `Command` / `Outbound`

It never reads the clock, never touches a socket, never spawns. `Instant::now()`
is called only in the runtime.

The decoder module additionally declares:

```rust
#![deny(clippy::indexing_slicing, clippy::arithmetic_side_effects, clippy::cast_possible_truncation)]
```

Remote-controlled lengths must go through checked arithmetic and bounds-checked
access. The compiler enforces it, not review.

## transport

TCP only:

* listener, accept loop, connection lifecycle
* socket options (section 13)
* kernel limit probing on Linux (section 13)
* owned read/write halves that move bytes

It exposes connection reader/writer wrappers with byte-level operations.
`TcpStream` does not leave the crate. No MQTT, no broker semantics.

Accept errors `EMFILE`/`ENFILE` back off (sleep, rate-limited log). An accept
loop must never spin on a persistent error.

## storage

The write-ahead log: sequential append, adaptive group commit, durable
offset, recovery of the durable prefix. Rules in `docs/storage.md`.

A byte offset is a location and a sequence is an identity: compaction rewrites
the file and never rewinds what the log has allocated. The `STRAUMW3` header
carries that state, so a restart after a rewrite continues the numbering
instead of repeating it (docs/storage.md §91).

It does blocking file I/O and reads the clock, because storage latency is
the signal the batching adapts to (§95). It owns no threads: the runtime
gives it one, and `sync_data` never runs inside the broker task.

## replication

The replication wire format, **sans-IO**: no sockets, no tasks, no clock, and
no workspace dependency at all — what it frames is opaque bytes.

One logical frame carries one complete WAL commit batch, exactly as
`commit_with` handed it to `write()`. Replication re-encodes nothing and
introduces no ordering of its own: the WAL sequence is the identity, and a
frame names the range it covers. A frame may cross the network in any number
of pieces; a partial read is never a partial durability boundary.

It also holds the state machine that decides what a position means: ranges,
not messages, and every position a contiguous prefix on both sides of the
wire. Received is not durable and written is not durable. Rules in
`docs/replication.md`.

## auth

Credentials and per-topic authorisation: password hashing (argon2id) and
the rules a session is allowed to act under. It reads no files and no
environment — the server parses both and hands over typed values.

`Permissions` travels inside `protocol::Connect`, which is why `protocol`
depends on this crate: a CONNECT is not a command until somebody has
checked what it carries.

---

## broker

A synchronous state machine: sessions, subscriptions, routing index,
at-least-once inflight tracking, overflow policy.

It owns no tasks and no channels. It receives commands and writes effects into
an `Outbox` supplied by the caller.

## runtime

Tokio: tasks, channels, the broker loop, connection reader/writer tasks, timers,
shutdown, metrics registry. The only crate that spawns.

## server

Composition root: parse configuration from environment, initialize tracing,
log kernel checks, wire, run, handle signals. Very little logic.

## e2e

`publish = false`. End-to-end tests and network benchmarks through a real TCP
socket, using an independent MQTT client.

---

# 4. Task Model and Data Flow (initial)

```text
  per connection                                  one broker task
┌──────────────────────────────┐   Command     ┌──────────────────────────┐
│ reader task                  │  bounded mpsc │ loop:                    │
│  socket read → BytesMut      │ ────────────► │  recv_many(batch)        │
│  mqtt decode → Command       │  await send   │  Broker::handle → Outbox │
│  keep-alive, connect timeout │               │  flush Outbox:           │
└──────────────────────────────┘               │   try_send per session   │
┌──────────────────────────────┐   Outbound    └────────────┬─────────────┘
│ writer task                  │ ◄──────────────────────────┘
│  recv_many → mqtt encode     │   bounded mpsc per session
│  → socket write              │
└──────────────────────────────┘
```

Rules:

* **Reader → broker** is bounded; the reader awaits `send`. Backpressure
  propagates to the publisher's TCP window. Intentional.
* **Broker → writer** is bounded per session. The broker loop uses `try_send`
  only and never awaits a single session. A full queue goes to the overflow
  policy (section 8).
* Reader and writer are separate tasks so a client that stops reading cannot
  block us from reading its packets, and vice versa.
* Outgoing packet identifiers are assigned by the writer; PUBACKs arrive at the
  reader. **(initial)** The per-connection packet-id table is shared by the two
  tasks as `Arc<std::sync::Mutex<_>>`: uncontended, never held across `.await`.
  Forwarding PUBACKs reader → writer over a channel is the alternative; switch
  only on benchmark evidence.
* The broker loop drains up to `STRAUM_BROKER_BATCH` commands per wakeup. The
  writer drains up to `STRAUM_WRITER_BATCH` items and writes once: batching
  amortizes both scheduling and syscalls.
* One broker task means no locks on broker state and trivial ordering. It is the
  likely first throughput limit; measure the broker benchmark before optimizing
  the connection path. Sharding is a later, benchmarked change that must
  preserve ordering (section 7) and handle wildcard subscriptions across shards.
* No task per message. No spawning outside the runtime.

---

# 5. Protocol Contract

Shape (not final API; semantics are fixed):

```rust
/// Opaque correlation chosen by the protocol layer and echoed back.
/// The broker never interprets it.
pub struct RequestId(pub u32);

pub enum Command {
    Connect(Connect),
    /// `ack: Some` => the sender wants confirmation that the broker accepted it.
    Publish { ack: Option<RequestId>, publish: Publish },
    Subscribe { request: RequestId, filters: Vec<(TopicFilter, DeliveryGuarantee)> },
    Unsubscribe { request: RequestId, filters: Vec<TopicFilter> },
    /// The client confirmed an at-least-once delivery.
    Ack(DeliveryId),
    Disconnect { reason: DisconnectReason }, // Client | ConnectionLost | ProtocolViolation
}

pub enum Outbound {
    Reply(Response),     // to the session that issued the command
    Deliver(Delivery),   // a message for a subscriber session
    Close(CloseReason),  // the broker decided to end this session
}

/// Issued by the runtime for a publish it is persisting, echoed back by the
/// WAL once durable. The broker holds the reply until then.
pub struct DurableToken(pub u64);

pub enum Response {
    Connected { session_present: bool },
    ConnectRejected(ConnectRejectReason),
    PublishAccepted(RequestId),
    Subscribed { request: RequestId, granted: Vec<SubscribeResult> },
    Unsubscribed(RequestId),
}

pub struct Delivery {
    pub id: Option<DeliveryId>, // Some iff AtLeastOnce; the guarantee follows from it
    pub topic: Topic,
    pub payload: Bytes,
}
```

Broker API:

```rust
impl Broker {
    pub fn handle(&mut self, session: SessionId, cmd: Command, out: &mut Outbox);
    /// A publish the runtime is persisting: the reply is held back until
    /// `on_durable` covers `durable` (docs/storage.md §79).
    pub fn handle_durable(&mut self, session: SessionId, cmd: Command, durable: DurableToken, out: &mut Outbox);
    /// Everything up to and including `token` is durable.
    pub fn on_durable(&mut self, token: DurableToken, out: &mut Outbox);
    pub fn on_overflow(&mut self, session: SessionId, rejected: Outbound, out: &mut Outbox);
}
```

Semantics:

* `SessionId` is allocated by the runtime per connection. `ClientId` is the
  client-visible identity; the broker maps between them.
* `PublishAccepted` is emitted after routing completed for all matched sessions
  (enqueued or handled by overflow policy). With `Durability::Fsynced` it is
  additionally withheld until `on_durable` covers the publish: routing having
  finished is not a durability statement (docs/storage.md §79).
* A `Delivery` with `id: Some` stays in the session's inflight set until
  `Command::Ack`. At most `STRAUM_MAX_INFLIGHT` per session; further
  at-least-once deliveries wait in the broker's session backlog.
* `Disconnect { ConnectionLost }` is sent by the runtime when the socket closes.
  A session that asked to be resumed is kept without a connection, collecting
  what it is owed; anything else is removed with its subscriptions and its
  inflight set in the same call (§7).
* `Outbound::Close` makes the runtime close the connection.
* Keep-alive, PINGREQ and PINGRESP never reach the broker.
* `Outbox` is owned by the caller and reused across iterations; it is cleared,
  not reallocated.

---

# 6. MQTT 3.1.1 Scope

Packets: CONNECT, CONNACK, PUBLISH, PUBACK, SUBSCRIBE, SUBACK, UNSUBSCRIBE,
UNSUBACK, PINGREQ, PINGRESP, DISCONNECT. QoS 0 and QoS 1.

Being built, in this order (the adoption path, not a wish list):

1. TLS, then authentication with per-topic authorisation.
2. Retained messages and persistent sessions are built, and both survive a
   restart. A kept session is written down as its subscriptions plus the
   last log record it finished with; what it is owed is replayed from the
   log rather than stored twice (docs/storage.md §90a). Compaction respects
   that mark and runs both at startup and beside the writer once the log
   passes STRAUM_WAL_COMPACT_BYTES (docs/storage.md §104).
3. Will publishing is built.

Out of scope: MQTT 5, QoS 2, shared subscriptions, WebSocket. Each needs
a concrete requirement before it moves out of this line.

A feature stays in the table below as a documented deviation until it is
built, so the gap between what is written here and what runs is always
visible.

Decisions and documented deviations:

| Situation | Behavior |
|---|---|
| Protocol level ≠ 4 | CONNACK 0x01, close |
| First packet is not CONNECT, or a second CONNECT | close |
| CleanSession = 0 | the session is kept when the connection ends and resumed on the next CONNECT from that identity; SessionPresent = 1 |
| CleanSession = 1 with a kept session | the kept session is discarded; SessionPresent = 0 |
| Offline session store full | the session is not kept; `offline_refused` counts it |
| At-most-once for an offline session | dropped (3.1.1 allows it); `offline_dropped` counts it |
| Unacknowledged at-least-once on resume | sent again with DUP = 1 |
| Empty ClientId, CleanSession = 1 | broker assigns an id |
| Empty ClientId, CleanSession = 0 | CONNACK 0x02, close — a session nobody can name again cannot be asked for (3.1.1 §3.1.3.1) |
| ClientId already connected | old session closed (takeover) |
| Will flag | published when the connection ends any way but a DISCONNECT (3.1.1 §3.1.2.5); a clean goodbye takes it with it |
| Will on a kept session | replaced by whatever the next CONNECT carries; it belongs to the connection, not the session |
| Will and durability | routed like any other message but never written to the log: the connection that would carry a durable token is the one that just went away |
| Username / password | checked against the credential file before the broker sees the session |
| Bad user name or password | CONNACK 0x04, close |
| Anonymous where it is not allowed | CONNACK 0x05, close |
| Publish outside the access rules | dropped, PUBACK still sent (3.1.1 cannot refuse a publish) |
| Subscribe outside the access rules | SUBACK 0x80 for that filter |
| PUBLISH with QoS 2 | close |
| SUBSCRIBE requesting QoS 2 | granted QoS 1 |
| RETAIN = 1 on PUBLISH | stored as the last message for that topic and replayed to the next subscriber; an empty payload deletes it |
| Retained store full | the message is delivered but not stored; `retained_rejected` counts it |
| Overlapping subscriptions of one session | one delivery, maximum granted QoS |
| Delivered QoS | min(publish QoS, granted QoS) |
| Topics starting with `$` | not matched by `#` or `+` at the first level |
| TLS configured | the listener speaks TLS only; plaintext and TLS do not share a port |
| Keep-alive | close if nothing received within 1.5 × keep-alive; 0 disables |
| No CONNECT within `STRAUM_CONNECT_TIMEOUT_MS` | close |
| Broker-initiated end of session | TCP close (3.1.1 has no server DISCONNECT) |

Overlapping-subscription deduplication is on the hot path: it must not allocate
per publish (for example, a generation counter per session entry).

---

# 7. Invariants

* **Ordering.** Messages published by one session to one topic are delivered to
  each subscriber in publish order. Any concurrency change must preserve this.
* **Memory is bounded** under any input: per connection, per session, and
  globally: `STRAUM_TOTAL_QUEUED_BYTES` on the way out,
  `STRAUM_TOTAL_CONNECTION_BUFFER_BYTES` on the way in — read and write
  buffers together, because a budget that counts one half of a resource is
  not a bound on it (docs/memory.md) — and `STRAUM_TOTAL_BACKLOG_BYTES`
  for what the broker holds on behalf of sessions that are behind. Each
  global budget exists because a per-session limit times the session count
  is a product, not a bound.
* **Malformed input never panics**, never reads out of bounds, never allocates
  from an unchecked remote length, never loops without progress.
* **Fan-out does not copy payloads** in broker-held memory: N subscribers share
  one allocation.
* **Closing a session** removes all of its subscriptions and inflight state in
  the same `handle` call.

---

# 8. Limits and Backpressure

Every limit, its default, its owner crate and its env var are defined in
`docs/configuration.md`. Changing a default is a behavior change.

Remote-controlled values are checked by the owning crate before allocation or
insertion. There are no unbounded queues, including channels, `Vec`s grown from
remote input, and maps keyed by remote data.

Queued bytes are tracked per session and globally with atomic counters:
incremented by the broker loop on enqueue, decremented by the writer on dequeue.
The mechanism is in the runtime; the decision is in `Broker::on_overflow`.

Overflow policy **(initial)**:

| Situation | Action |
|---|---|
| At-most-once delivery, session queue full (messages or bytes) | drop, `dropped_messages += 1` |
| At-least-once delivery, session backlog full (messages or bytes) | `Close(SlowConsumer)`, counted in `backlog_refused` and logged with the limit that was reached |
| Reply cannot be enqueued | `Close(SlowConsumer)` |
| Global queued-bytes budget exceeded | same as a full session queue |
| Global backlog-bytes budget exceeded | same as a full session backlog |
| Global read-bytes budget exceeded | the growth is refused before it is allocated; the connection is closed |
| Broker command queue full | reader awaits; nothing dropped |
| WAL queue full | the submitter awaits; a publish is never acknowledged early to make room |
| Writer makes no progress for `STRAUM_WRITE_STALL_TIMEOUT_MS` | close connection |

Every path in this table has a test.

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
16 KiB. Payloads ≤ `STRAUM_PAYLOAD_COPY_THRESHOLD_BYTES` are copied once at
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
TCP read → decode → validate → Message → route → enqueue → encode → TCP write
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
* A function is `async` only if it awaits I/O or another async operation. Core,
  protocol, mqtt and broker are synchronous (enforced: no tokio).
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
lifting `unsafe_code = "forbid"` under section 14, on top of a measured gain on
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

1. **Socket and process settings**: the broker applies them from `STRAUM_*`
   env vars (`docs/configuration.md`).
2. **Kernel parameters** (`sysctl`, `RLIMIT_NOFILE`): set by the operator.
   The broker **never writes** them: it needs privileges and changes the whole
   host.

On Linux at startup the broker reads the relevant kernel parameters, logs them
once at `info`, resolves `auto` values from them, and warns where configuration
exceeds what the kernel allows (the kernel clamps silently, for example backlog
above `net.core.somaxconn`). The full list is in `docs/linux-tuning.md`.

On macOS the probes return "unavailable"; `auto` falls back to fixed defaults.
A Linux-only option explicitly set on macOS is ignored with a warning at startup,
so the same env file works on both.

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

`unsafe_code = "forbid"` workspace-wide. It is lifted for one module only when
all of these hold:

1. a benchmark on a quiet Linux host shows the gain, across the relevant matrix
2. the safe version stays in the repository as the comparison baseline
3. the unsafe block is isolated in one module with documented invariants
4. the invariants are tested, and the module is covered by fuzzing where it
   parses remote input

Never unsafe to silence the borrow checker, and never as an early step in the
priority order of section 11.

---

# 15. Instrumentation and Metrics

`tracing` for lifecycle, configuration, kernel checks, protocol and transport
errors, shutdown. Never per message in the default path. No `?`-formatted
payloads or topics inside high-frequency code. Release builds cap the compiled
level (`tracing` release max level feature) so hot-path macros compile away.

Metrics: plain atomics (`Relaxed`), no framework. An atomic per message is
itself a cost: prefer per-task counters aggregated on read where a benchmark
shows contention.

```text
counters: connections_accepted  protocol_errors  transport_errors
          bytes_in  bytes_out  slow_consumer_closes  backlog_refused
          authentication_failures  keepalive_closes
          wal_commits  wal_records_committed  wal_bytes_committed
          wal_sync_nanos  wal_commit_failures  session_records_dropped
          wal_compactions
gauges:   active_sessions  queued_messages  queued_bytes  backlog_bytes
          wal_durable_sequence
```

The broker keeps its own counters (`published_messages`,
`delivered_messages`, `dropped_messages` and the rest of `broker::Stats`);
the runtime mirrors out only what an operator acts on.

---

# 16. Shutdown

The runtime coordinates; nothing relies on `Drop` for application shutdown.

```text
1. stop accepting connections
2. stop reading new commands
3. let writers drain for STRAUM_SHUTDOWN_GRACE_MS
4. close sessions
5. close sockets
6. stop the broker loop (it holds a log submitter of its own)
7. commit what the WAL already accepted, then stop its writer and the
   remaining tasks
8. exit
```

With `STRAUM_DURABILITY=memory`, at-least-once messages still inflight at
shutdown are lost — an acknowledgement there means routed, not written. With
`written` the record reached the operating system, so it survives this
process but not the machine. With `fsynced` an acknowledged publish is
already on the device and comes back at the next start (docs/storage.md §79).
What is lost at every level is a delivery the subscriber had not
acknowledged: it is sent again with DUP.

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
cargo test --workspace --all-targets
scripts/check-boundaries.sh

# when code under transport/src/sys/ or other Linux-only behavior changed:
scripts/linux.sh test --workspace --all-targets

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
