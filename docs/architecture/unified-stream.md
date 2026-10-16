# Straum: Architectural Vision — "A Single Stream"

Straum is a Rust message broker: MQTT 3.1.1 and MQTT 5 over TCP, Cargo workspace, Rust 2024, sans-IO core, WAL with group commit and compaction, durable leader/follower replication implemented. By default, `unsafe_code = forbid`; exceptions are permitted only under the policy in section 4.1. This document sets the direction of the architecture and takes precedence over previous clustering plans.

## 1. Philosophy

A Straum cluster is a single mechanism, not a collection of autonomous nodes.

- **The log is the single source of truth.** No node mutates shared state bypassing the log. Every observable effect is the result of applying a committed record: PUBACK, delivery to a subscriber, retained, session state, inflight QoS1, session and message expiry, will, sequencer change, cluster membership change.
- **A node is a deterministic function** of (committed log + input events). Two nodes with the same log prefix are in the same state.
- **Predictability matters more than peak speed.** The order of successors is known in advance, transfer of power is a routine operation, and the reaction to failure is unambiguous.
- **Time does not participate in safety.** Clocks and timeouts only initiate transitions. Decisions are made through the log and quorum.
- **Correctness is proven by simulation, performance by measurements on Linux.** Without data, no claims of "faster."

The positioning we are building and must be able to prove:
"A replicable MQTT 3.1.1/5 broker where PUBACK means commit on a quorum, a scheduled leader change does not sever client connections, correctness is verified by deterministic simulation, and unsafe is isolated, justified, and verified."

Honest assessment: the protocol is a variant of Viewstamped Replication / Multi-Paxos with a deterministic epoch owner, with ideas from Solana (leader schedule, skip certificate from Votor, Turbine, Gulf Stream). This is not a new consensus. Consensus remains in one place — in the change of owner upon failure.

## 2. Target Model

### 2.1 Roles: gateway and sequencer
- Every node operates as a **gateway**: TCP, (later) TLS, auth, MQTT sessions, fan-out to its own subscribers.
- In each slot, exactly one node operates as the **sequencer**: it orders and replicates the log.
- The gateway forwards PUBLISH to the current sequencer (the Gulf Stream idea: the schedule is known, and at the slot boundary one can send to the next one). A change of sequencer does NOT sever client connections.
- PUBACK to the client is sent by its gateway when it sees the record committed.
- A VIP or load balancer is only routing, never a source of write authority. The primary mechanism is forwarding through the gateway. For MQTT 5 clients, Server Reference and the reason codes "Use another server" / "Server moved" are additionally permissible, but only as an optimization, not as a condition of correctness.

### 2.2 Slots, ownership, and fencing
- Within configuration C, a deterministic order of successors is defined:
  `owner(slot) = schedule_C[slot]`.
- Each WAL record carries `(slot, lsn, prev_hash)`:
  - `lsn` is global and monotonic, NOT reset in a new slot;
  - `prev_hash` is computed over the group commit batch, not per message;
  - a `slot → start_lsn` table is needed to find the divergence point and for truncation (analogous to the leader-epoch cache in Kafka).
- A follower accepts a record only from the owner of the current slot and only if `prev_hash` matches.
- `current_slot` and issued promises are stored in the node's metadata and are fsynced BEFORE responding.

### 2.3 Commit
- Commit = durable ack from a majority (for N=3, that's 2 nodes). Commit is prefix-based.
- PUBACK and delivery to subscribers are performed ONLY after commit. No forks and no rollbacks of already-delivered data.
- Retained and other reads: either through committed state or under a lease (a lease is the only permissible place for physical time, with an explicit drift bound).

### 2.4 Transfer of power
- **Graceful (scheduled).** Sequencer A sets a fence, drains in-flight, and writes `SlotEnd(k, last_lsn, hash)` to the log. This record is committed by ordinary replication and serves as a handoff certificate. Successor B, upon seeing the commit of `SlotEnd`, writes `SlotStart(k+1)` and begins work. There is no separate election round.
- **Ungraceful (failure, analogous to a skip certificate).** The timeout has expired, there is no `SlotEnd`. Successor B requests from a majority a durable promise "not to accept records of slots ≤ k" together with log tails, takes the maximum by `(slot, lsn)`, catches up, and writes `SlotStart(k+1, from=…)`.
- **Dead successor:** the next one in the schedule does the same for k+2.
- Rotation by schedule is NOT the default mode. By default, the sequencer is stable. Scheduled transfer is used for maintenance and as an optional "continuous failover drill" mode with long windows.

### 2.5 Configuration and membership
- Cluster membership and the schedule are stored in a separate metadata log and are changed through the same mechanism, infrequently.
- For two nodes with data, a third witness node without data is needed: on two nodes, safe automatic failover is impossible.

### 2.6 Replication topology
- For 3–5 voting nodes: a star (sequencer → all directly).
- A tree (Turbine) only for non-voting replicas and edge nodes; it does not affect commit.

### 2.7 MQTT 5 from day one
MQTT 5 is built into the architecture from the start, even if individual features are implemented later.

- **Codec.** The sans-IO MQTT crate is versioned (3.1.1 and 5) with a shared internal packet model. Properties, reason codes, and reason strings are part of the model, not an add-on. Internal broker errors are mapped to reason codes.
- **WAL record format.** It is versioned, and a message record has an extensible properties section from day one: user properties, content type, response topic, correlation data, payload format indicator, message expiry. Changing the WAL format later is far more expensive.
- **Time in the protocol is resolved through the log.** Session Expiry, Message Expiry, and Will Delay are NOT executed autonomously by nodes. The gateway writes events (Disconnect, etc.) to the log, and expiry and will publication are fixed by the sequencer with `SessionExpired`, `MessageExpired`, `WillPublished` records. All nodes apply the same decision.
- **Subscriptions** are stored in the log together with options: No Local, Retain As Published, Retain Handling, Subscription Identifier.
- **Session takeover** (reconnecting with the same client id to a different gateway) goes through a log record so that there is exactly one live session in the cluster. This also applies to 3.1.1.
- **Assigned Client Identifier** is unique in the cluster and is issued deterministically, for example as (node_id, monotonic counter) or through the log.
- **State of a specific connection is not replicated:** Topic Alias, Receive Maximum, Maximum Packet Size, keep alive. This is local gateway state, and by specification it does not survive reconnection. Inflight QoS1, however, is replicated.
- **Enhanced authentication (AUTH)** is designed as a separate sans-IO state machine, compatible with a future Argon2id.
- **QoS2 and shared subscriptions** are not implemented now, but the log model must allow for them: reserve record types for QoS2 state (PUBREC/PUBREL) and for deterministic assignment of messages to members of a shared group based on the committed group membership.

## 3. Storage (segmented WAL)
- Keep group commit and compaction. Compaction must not truncate a segment that someone is reading: a reader that can be in progress during a truncation or compaction holds a pin (`Segments::pin`, a refcount). Reads serialised with them on the log thread need none; a position they keep to resume from is dropped before the log is cut or a segment removed (D-142 A).
- The hot path remains allocation-free (current measurement: +24–33 ns, about 0.1% of ~26 µs fsynced publish on M1, possibly in Docker; requires confirmation on Linux, including multi-publisher contention and `STRAUM_MAX_PENDING_DURABLE`).
- Buffers of known size are created via an exact `with_capacity`.
- Do not use mmap on the write path. mmap for reading sealed segments is permissible after profiling and under the unsafe policy (section 4.1).
- Candidates for Linux tuning (after profiling): fdatasync instead of fsync, segment preallocation, O_DIRECT with aligned buffers.

## 4. Runtime
- The sans-IO core is PRESERVED. This is a mandatory condition for deterministic simulation. Protocol logic must not be interwoven with `.await`.
- The runtime is only a driver (the quinn-proto / quinn model). tokio is permissible as a driver, but not mandatory.
- We do not use tokio-uring: the project is effectively frozen.
- io_uring, if needed, is integrated via the `io-uring` crate from its own event loop, with a thread-per-core model, after profiling on Linux.
- `unsafe_code = forbid` is the default for all crates. Exceptions are permitted only under section 4.1.

### 4.1 Unsafe policy
unsafe is permitted if it is justified and provably sound, that is, no safe Rust code calling the public API can cause UB.
Mandatory conditions:

1. **Justification by measurement.** Profiling on Linux shows a gain that the safe variant does not provide. The safe variant is retained as a baseline behind a feature flag and is compared against the unsafe variant in benchmarks.
2. **Isolation.** unsafe lives only in separate small crates (for example, `straum-sys-*`) with a safe public API. All other crates retain `forbid`.
3. **Documentation.** Every `unsafe` block is accompanied by a `// SAFETY:` comment with the invariants it relies on, and every `unsafe fn` has a `# Safety` section. `unsafe_op_in_unsafe_fn = deny` and the clippy lints `undocumented_unsafe_blocks` and `multiple_unsafe_ops_per_block` are enabled.
4. **Verification.** Tests under Miri where possible. For code that Miri does not support (syscalls, io_uring, mmap) — sanitizers and fuzzing. Failure scenarios are also verified in the simulation harness.
5. **Decision.** An ADR is written for each use (what, why, what invariants, what measurement). Final approval is given by Stanislav.

Likely candidates: mmap of sealed segments with refcounting against compaction, io_uring with buffer ownership, aligned buffers for O_DIRECT.

## 5. Deterministic Simulation (mandatory)
The harness runs sans-IO nodes with controlled clocks, network, and disk, and iterates over seeds. The minimum set of failures:
- crash between fsync of the promise and sending the response;
- crash of A before, during, and after writing `SlotEnd`;
- `SlotEnd` written but not committed;
- partition {A} | {B, C} with A alive and continuing to write;
- a lagging and a dead successor, several dead in a row;
- clock jump forward and backward, a long process pause;
- message loss and reordering, partial disk write;
- compaction during catch-up;
- session takeover on different gateways simultaneously;
- session expiry and will delay during a sequencer change.

Invariants are checked after every step:
1. A committed record is never lost and never changes position.
2. In a single slot, there is at most one sequencer with committed records.
3. The prefixes of the committed log on all nodes match.
4. PUBACK and delivery never precede commit.
5. `lsn` is monotonic, `prev_hash` forms a continuous chain.
6. No record of slot ≤ k is committed after a majority promise for k+1.
7. In the cluster, at any moment, there is at most one live session per client id.
8. Time-dependent decisions (expiry, will) are made once and applied identically on all nodes.

## 6. Phases
0. **Audit and foundation.** Gap analysis and ADR, updated CLAUDE.md. Simulation harness for the existing leader/follower replication with invariants 1, 3, 5. Versioned WAL record format with a properties section. MQTT codec model with properties and reason codes.
1. **Fencing and quorum.** `(slot, lsn, prev_hash)` format, durable promise, 3 nodes with majority commit, ungraceful transition, truncation by the slot table.
2. **Graceful handoff.** `SlotEnd`/`SlotStart`, successor schedule, optional rotation.
3. **Gateway/sequencer separation.** Forwarding, PUBACK on commit, sessions, inflight QoS1 and retained through the log, reconnection to any node. MQTT 5 semantics: expiry and will through the log, session takeover, subscription options, Assigned Client Identifier.
4. **Metadata log.** Membership change, witness.
5. **Performance on Linux.** Profiling (perf, flamegraph, off-CPU), multi-publisher contention, `STRAUM_MAX_PENDING_DURABLE`, disk tuning, then decisions on runtime, io_uring, and justified unsafe.
6. **MQTT 5 extensions.** QoS2, shared subscriptions, enhanced auth.

Out of scope: WebSocket, CRDT/eventual consistency, security built on physical time. QoS2 and shared subscriptions are implemented in phase 6, but the log model accounts for them from the very beginning.

## 7. Working Rules
- Small, verifiable steps. Each step: tests, simulation scenarios, unchanged benchmarks.
- Do not claim a performance improvement without measurements on Linux. Mark measurements on macOS and in Docker as preliminary.
- Record any deviation from this document in an ADR and in the report.
- No node receives logic that mutates shared state bypassing the log. If a task seems to require this, stop and propose options.
- At the end of each task:
  1. what was done;
  2. what was verified and how;
  3. open risks;
  4. recommendations for the next step in the form of 2–3 elaborated options with trade-offs, from which Stanislav will choose.