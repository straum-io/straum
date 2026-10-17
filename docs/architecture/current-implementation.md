# Straum: the current implementation

> **History.** This document describes the old leader/follower broker, removed at step 8 on 2026-10-11 (D-111): the crates `broker`, `protocol`, `mqtt`, `replication`, `runtime` and `e2e` are gone, and its code is in git history (commit ffeda02 and earlier). What runs now is the cluster path of ADR-0008, documented in the ADR, `docs/configuration.md` and `docs/testing.md`. Kept as a record of what was built, measured and decided.

What the code does today, as of 2026-10-04. The target is
`docs/architecture/unified-stream.md`, which takes precedence wherever the
two differ; ADR-0001 records the move. These sections were in `rules.md`
(§3–§6 and §16) until it was rewritten for the unified stream; they moved
here unchanged so that nothing was dropped — except that they were taken
from the working tree, which held two edits not yet in history: the
connection-buffer row (formerly "read-bytes") and the reference to
`docs/storage.md` §90b (formerly §90a). Both are in the same commit as this
file.

Some passages describe work that was staged but not yet committed when this
file was written: storage's `Disk` boundary and `simulated::SimDisk`, and
`replication::Link` and `Redial` (D-28 stages 1–2; D-39).

Where this file describes something the target replaces, it says so in a
"Superseded by" line at the start of the section. The description stays
because it is still what runs.

> **Superseded in part by docs/architecture/unified-stream.md §2.1, §4.** The crate split and the sans-IO cores stay (§4: "sans-IO ядро СОХРАНЯЕТСЯ"); the roles gateway and sequencer and the runtime as a driver only are the target.

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
instead of repeating it (docs/storage.md §91). A rewrite keeps a filtered
set rather than a prefix, so what it leaves behind still recovers but can no
longer be named: below the retention floor the log has bytes and no addresses
(docs/replication.md §130).

It does blocking file I/O and reads the clock, because storage latency is
the signal the batching adapts to (§95) — both through its `Disk` boundary,
never around it: `OsDisk` is `std::fs` and the OS clock, `simulated::SimDisk`
a disk in memory with simulated time, for deterministic simulation (D-28,
D-30). It owns no threads: the runtime gives it one, and
`sync_data` never runs inside the broker task.

## replication

The replication wire format, **sans-IO**: no sockets, no tasks, no clock, and
no workspace dependency at all — what it frames is opaque bytes.

One logical frame carries one complete WAL commit batch, exactly as
`commit_with` handed it to `write()`. Replication re-encodes nothing and
introduces no ordering of its own: the WAL sequence is the identity, and a
frame names the range it covers. A frame may cross the network in any number
of pieces; a partial read is never a partial durability boundary.

It also holds the state machines that decide what a position means — ranges,
not messages, and every position a contiguous prefix on both sides of the
wire — and what the leader's side of a link does next (`Link`, `Redial`;
D-28). Received is not durable and written is not durable. Rules in
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

> **Superseded by docs/architecture/unified-stream.md §2.1, §2.3 (target).** One broker task per node stays the single-node model; in the target, PUBLISH is forwarded to the sequencer and PUBACK and delivery follow commit.

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

> **Superseded by docs/architecture/unified-stream.md §2.7, §6.** MQTT 5 is in scope from the start (codec model, properties, reason codes, versioned WAL records). The table below is what 3.1.1 does today; "Out of scope: MQTT 5, QoS 2, shared subscriptions" no longer holds — MQTT 5 is in scope, QoS 2 and shared subscriptions are phase 6 with the log model allowing them from the start (ADR-0003).

# 6. MQTT 3.1.1 Scope

Packets: CONNECT, CONNACK, PUBLISH, PUBACK, SUBSCRIBE, SUBACK, UNSUBSCRIBE,
UNSUBACK, PINGREQ, PINGRESP, DISCONNECT. QoS 0 and QoS 1.

Being built, in this order (the adoption path, not a wish list):

1. TLS, then authentication with per-topic authorisation.
2. Retained messages and persistent sessions are built, and both survive a
   restart. A kept session is written down as its subscriptions plus the
   last log record it finished with; what it is owed is replayed from the
   log rather than stored twice (docs/storage.md §90b). Compaction respects
   that mark and runs both at startup and beside the writer once
   STRAUM_WAL_COMPACT_BYTES were appended since the last rewrite, keeping
   what a follower still needs (docs/storage.md §104, docs/replication.md §118).
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

# 16. Shutdown

The runtime coordinates; nothing relies on `Drop` for application shutdown.

```text
0. close publish admission: a publisher waiting for room ends now, not
   at the end of the grace period (D-10)
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

