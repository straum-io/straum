# ADR-0008: Rewrite the broker and replication for gateway/sequencer now

- Status: accepted (direction); steps 1–6 done, 7a experimental, step 8 (the
  old path removed) done early, D-111 (see Progress); 7b and 7c open
- Date: 2026-10-04
- Decided by: Stanislav (D-48: C)
- Deviations from: docs/architecture/unified-stream.md §6 (the order of
  phases) and §7 ("маленькие проверяемые шаги" — kept within the rewrite,
  but the rewrite itself is one large change of direction). Readings, not
  deviations, recorded in Progress: §5 invariant 1 holds above the
  compaction floor — a record compacted after commit counts as held (D-57 A)
  — and the transport delivers nothing addressed to a process to its
  successor after a restart
- Amends: ADR-0001 (Consequences: "development follows the phases of §6,
  starting with phase 0"), ADR-0003 (Consequences: "phase 0 delivers the
  codec model"), ADR-0006 (Consequences: "phase 1 adds the cluster-mode
  configuration"), ADR-0007 (Decision: phase 0 as three tasks)

## Context

The broker today is one node's state machine: it routes and delivers a
publish at once, keeps sessions, retained messages and wills in memory and
writes them to the log afterwards, and holds a PUBACK until the configured
durability level. Replication is one static leader shipping its WAL to one
follower, with no slots, no fencing and no takeover. Unified-stream §1 asks
for the opposite shape: the log is the only source of truth, every observable
effect is the application of a committed record, and a node is a
deterministic function of the committed log and its inputs. ADR-0001…0007
planned to get there phase by phase; D-48 C decides to rewrite the broker and
replication for that shape now.

## Target

Every piece that decides is sans-IO (ADR-0004); the runtime moves bytes.

```text
client ─TCP─► gateway (every node)                        sequencer (owner of the slot)
              ├ MQTT 3.1.1 and 5 codec, one model          ├ assigns lsn, builds batches
              ├ connection-local state (keep-alive,        ├ prev_hash per batch
              │   topic alias, receive maximum)            ├ ships batches to replicas (star)
              ├ proposes: Publish, Subscribe, Unsubscribe, ├ commit = durable on a majority
              │   Connect (with will registration),        ├ decides time: writes SessionExpired,
              │   Disconnect, Ack                       ─► │   MessageExpired, WillPublished
              ├ applies committed records ◄── commit ──────┤ (later) promises, takeover,
              │   (the same deterministic function           │   SlotEnd/SlotStart
              │    on every node)
              ├ delivers to its own subscribers after commit
              └ PUBACK to its client when its record is committed
replica (every node): accepts batches from the slot's owner only, checks
prev_hash, makes them durable, acknowledges; truncates a divergent suffix by
the slot table.
```

- **Log record v4.** (As planned; the record as built is in Progress, step 1.) `storage` keeps the byte framing (length, checksum),
  recovery and segments; a new sans-IO crate owns record bodies v4: every
  record carries `(slot, lsn)`, every commit batch a header with `prev_hash`
  over the previous batch (§2.2), a publish record an extensible properties
  section (§2.7, ADR-0003).
- **The broker becomes the apply function**: subscriptions with their options,
  sessions, inflight QoS 1 and retained messages are state derived from
  committed records only. Delivery and PUBACK follow commit (ADR-0005).
- **Cluster mode** is three voting nodes in the first cut (ADR-0006: in a
  cluster only majority commit; `memory`/`written`/`fsynced` refused). Two
  data nodes need a witness (§2.5).

## Step plan (proposed)

Each step lands with unit tests, simulation scenarios over the `sim`
harness extended to N nodes, and unchanged benchmarks on the paths it does not
touch; the old path keeps working until a step replaces it (D-49).

1. Log record v4 with `(slot, lsn)`, properties, and a batch header with
   `prev_hash` (D-50 decides the hash); `sim` checks invariant 5 in full.
2. Sans-IO sequencer and replica for three nodes, star topology, majority
   commit, a fixed slot owner; a segmented WAL whose compaction never
   truncates a segment being read (refcount, §3); `sim` with three nodes:
   invariants 1, 3, 5, and compaction during catch-up (§5).
3. Split (D-59 A). 3a: the MQTT 5 codec model (properties, reason codes,
   reason strings; 3.1.1 on the same model) in a new crate. 3b, split again
   (D-62 C), with the contract types moved into it (D-61 A): 3b-1 the broker
   as the apply function of committed records and the protocol-neutral
   contract, in `sim` without MQTT; 3b-2 the sans-IO gateway over `codec`:
   proposals, PUBACK on commit, delivery after commit, and a bound on
   proposals awaiting commit per publisher and in total, in messages and in
   bytes (rules.md §7, §8; step 2 bounds the sequencer's queue by count
   only, which is enough while no remote input reaches it);
   `sim`: invariant 4. Invariant 7 at this step means one gateway: at most one
   live session per client id within it; across gateways it waits for step 7.
4. The runtime driver for cluster mode: inter-node links, forwarding; e2e
   tests over TCP. Its configuration and startup checks (ADR-0006) moved to
   step 7, where the server stops refusing cluster mode (D-68 A).
5. Ungraceful takeover: durable promises with fsync before the reply, the
   slot table and truncation; `sim`: invariants 2 and 6, partitions. A
   sequencer restart becomes a takeover, and only then may the sequencer
   ship a batch in parallel with its own sync; until here it syncs first
   (D-58 A). Taking it waits for unified-stream phase 5 and a Linux
   measurement (D-71 A, amending D-58 A).
6. Graceful handoff: `SlotEnd`/`SlotStart`, the successor schedule.
7. Sessions, takeover across gateways, expiry and will through the log
   (the gateway proposes; the sequencer writes `SessionExpired`,
   `MessageExpired`, `WillPublished`); Assigned Client Identifier; `sim`:
   invariants 7 and 8. Cluster configuration from the environment and its
   startup checks (ADR-0006), moved here from step 4 (D-68 A).
   7b. State copy (D-60 A): a replica lost below the compaction floor
   rejoins from a snapshot of the applied state at a committed lsn — a
   deterministic function of the committed log (§1) — plus the log after it,
   resuming with `Chain::resume` there. After step 7 because sessions,
   expiry and wills enter the log only then. Local snapshots exist from
   step 4 (D-67 B, revising D-60 A for them): every node restarts from its
   own; 7b ships one to a replica that lost the log, and the format, which
   is versioned, changes at step 7. `sim`: a lost replica comes back and the
   cluster commits with it.
8. The old leader/follower path is removed (D-51 A): a single node runs as
   a cluster of one on the new path (commit = local fsync, delivery after
   commit; `memory` and `written` go, revising ADR-0006, D-36 A and the D-42
   draft). The two-node `quorum` pair stays on the old path until a witness
   exists (phase 4 of unified-stream §6), so step 8 removes the old path only
   after that. Until then the old path is frozen — bug fixes only (D-75).
   Two more conditions come before removal (D-75): the guarantees of the
   D-28 chain (C1–C4, today `sim/tests/chain.rs` over the old broker and
   runtime) are carried into simulation invariants of the new path, and the
   new path has a durability benchmark comparable with the old path's
   baseline (`e2e/benches/durability.rs`, docs/baselines/).

**Cluster mode was test-only until step 7a; since then it is experimental
and off by default.** Before it, wills and sessions are
not yet in the log, and before step 5 there is no fencing: a cluster would have
observable effects outside the log (§1). Until then cluster mode is reachable
from the simulation and the e2e tests only, and refused by the server's
configuration. Since step 7a (D-78 A, D-102 A–D-105 A) the server can start
a node on the new path behind `STRAUM_EXPERIMENTAL_CLUSTER=1`: experimental
and off by default, not production-ready, flow control off. It refuses
wills, persistent sessions and session expiry at CONNECT rather than handle
them locally, until step 7c puts them in the log.

Phases 4–6 of unified-stream §6 (metadata log and witness; Linux performance;
MQTT 5 extensions) are unchanged and follow step 8.

## Consequences

- Phase 0 tasks 2 (WAL v4) and 3 (MQTT 5 codec model) are folded in: task 2
  is step 1, task 3 opens step 3, before the gateway is built on it.
- The pending decisions taken in the old model are withdrawn and re-read at
  their step: D-44 (PUBACK in the oracle) at step 3; D-45 (runtime glue in the
  harness) at steps 2–4, which move that glue into sans-IO crates; D-46 (C0,
  C5 of the old PUBACK chain) at step 3; D-47 (what "lost" means under
  compaction) at step 2. D-33 (`written` with a follower) was withdrawn (D-55 A):
  the defect stays in the old path until step 8, reproduced by `written_with_a_follower_diverges_when_the_leader_loses_power` in `crates/runtime/tests/simulated_link.rs`;
  the new path has no `written` in a cluster.
- Until step 8, two paths exist; `current-implementation.md` describes the
  old one, this ADR the new one.

## Progress

**Step 1 (2026-10-05): done, not yet wired.** Crate `journal` (named so:
`log` collides with the crates.io crate already in the lock file): record v4
`[version=4][kind][slot][lsn][prev_hash][properties_len][properties][body]`,
kinds including those reserved for phase 6. Every record carries `prev_hash`,
the hash of the previous batch: xxh3-128 over all of that batch's record
bytes, header included (D-50; D-56 A, which reads §2.2 whole — "every record
carries prev_hash" and "computed per batch, not per message" — and replaced
the per-record hash of D-54 B). A batch header counts the batch's records, so
a commit is never half applied.
`Chain` (hash, next lsn, slot) for the writer, which refuses a slot going
back, and `Verifier` for recovery and replicas, which also refuses it when
resumed from the middle of a log. A batch cut
short is a tail, not an error; a gap, a reorder, a foreign history, a slot
going back or a record outside a batch is refused. Evidence:
`crates/journal/tests/format.rs` (12 tests, two of them properties) and
`crates/sim/tests/journal.rs` — batches through `Wal<SimDisk>`, power loss
with torn tails, recovery verified against the chain, and every synced batch
recovered with the hash it was committed with; 50 seeds by default, 1216
power losses, 153 of them mid-batch. xxh3-128 is checked against its
reference digest of empty input, and the bytes of one batch are pinned.

Nothing in the running server writes v4: the broker and replication still
write `STRAUMW3` bodies. The batch hash reaches a hot path when the
runtime drives the new path (step 4); the `storage/commit` baseline is taken
then, on Linux.

**Step 2 (2026-10-05): done in simulation, D-53 A.** Three pieces:

- `storage::Segments`: the log as segment files, each one a `Wal` (framing,
  group commit, recovery and the D-43 sync at open are the single file's). A
  segment starts only at a batch boundary and is made durable before the
  next one starts, so a power loss costs the end of the log, never a hole.
  Space comes back by removing whole segments from the front; removal stops
  at the first segment a reader has pinned (§3: the refcount) and always
  keeps the last sealed segment, which holds the chain position if the
  active one has only a torn batch to cut. `Disk` gained `list`, the one
  operation recovery of a directory of segments needs.
- `cluster`, a new sans-IO crate: `Sequencer` (fixed owner, batches through
  `journal::Chain`, star replication, commit = highest lsn durable on a
  majority, only grows) and `Replica` (accepts only the owner, only the
  current slot, only the continuation of its log, only an unbroken chain,
  only whole batches). Bounded: queued proposals, batches kept in memory,
  and a per-replica window; a replica further behind is read back from the
  log one batch at a time.
- `crates/sim/tests/cluster.rs`: three nodes on simulated disks, a network
  that drops, delays and reorders, process crashes, power losses with torn
  writes, partitions, and compaction during catch-up. Checked after every
  event: invariant 1 (a committed record never changes and stays durable on
  a majority, or compacted after commit), 3 (every node agrees with the
  committed log wherever they overlap), 5 (every recovered log one chain),
  storage keeping its word at every restart, and the refcount (a pinned
  segment is never found gone). After healing, every replica not lost holds
  the whole log, committed; with too few voters left, commit stays put.
  30 seeds × 1500 events by default. The 1000-seed sweep in release passed:
  692 132 lsns committed, 11 333 restarts, 5 690 power losses, 598 torn
  batches cut, 432 compactions held back by a pin, 4 970 segments removed,
  1 306 replicas lost below a floor. That run compacted to any floor up to
  the commit point; D-57 A then set the policy. The sweep after D-56 A,
  D-57 A and the three fixes below: 693 823 lsns committed, 11 179 restarts,
  5 636 power losses, 613 torn batches cut, 1 067 compactions held back by a
  pin, 5 883 segments removed, 1 180 replicas lost. The simulation checks the
  rule on its own terms — every floor at most min(commit, max(slowest
  replica, byte floor)) — and that a replica is lost only below the byte
  floor (5 000 bytes in the simulation); breaking the rule fails it at seed 0.

Decided inside the step, recorded here:

- **The sequencer ships only what is durable in its own log.** A replica's
  log is then always a prefix of the sequencer's durable log, and a
  sequencer that loses power loses nothing a replica holds. Shipping in
  parallel with the local sync needs the takeover of step 5 on every
  sequencer restart (D-58).
- **Resends start only at the heartbeat** (go-back-N). The first version
  resent a window on every position report; with reordering each stale
  batch drew a report and each report a window, and the simulation found the
  traffic growing without bound. A report now only moves a position forward.
- **A catch-up that makes no progress over a heartbeat period releases its
  pin**, so a replica that died half way does not hold the disk. If its
  segment is then removed, the replica is lost: it needs a copy of the state,
  which no step has yet.
- Messages are values; their wire encoding comes with the runtime driver
  (step 4).
- **Retention (D-57 A, re-reading the withdrawn D-47).** The sequencer
  compacts through the commit point but not past the slowest replica it
  serves, unless the log would keep more than a byte bound; the driver
  measures the bound on segment sizes, the sequencer decides the floor
  (`Sequencer::compaction_floor`). A replica is lost only below a floor the
  bound forced, which the simulation checks. A record compacted after commit
  counts as held: invariant 1 is read above the floor. The new path takes
  the bound from `STRAUM_WAL_RETAIN_BYTES` when step 4 configures it.
- **A heartbeat never goes back below what a replica reported durable.**
  A late report can say less than the replica holds; going back that far
  asked the log for compacted records and lost a healthy replica (found by
  the simulation once D-57 A made losses checkable).
- **A read the replica overtook while it waited does not lose it** when it
  misses; only the outstanding read counts (found by the simulation).
- **Nothing addressed to a process reaches its successor**: a connection
  does not outlive its process, as with TCP. A sequencer that heard a report
  from before its restart went to the log for records the previous process
  had compacted (found by the simulation). The simulation drops what is
  addressed to a crashed node, or sent to it while it is down; what the dead
  process sent still arrives at the others, late and reordered. Step 4's
  driver must keep this.

Not covered yet: slot changes, promises and truncation by the slot table
(step 5); no node applies records yet (step 3). The old path is unchanged.

**Step 3a (2026-10-05): done; its contract types moved to 3b-1 (D-61 A).** Crate `codec`, new beside `mqtt`,
which the running server still uses (D-49 A):

- One packet model for MQTT 3.1.1 and 5, all twelve packets either side
  sends, decoded and encoded in both directions so that a simulated client
  speaks through the same code the gateway will. A 3.1.1 packet is the same
  value with empty properties; encoding for 3.1.1 drops what it cannot carry
  and maps what it can (a CONNACK refusal to one of its five return codes, a
  SUBACK failure to 0x80) and refuses what it has no form for (AUTH, a
  DISCONNECT from the server).
- Properties are typed per packet: a property a packet may not carry has no
  field and is refused as a protocol error; unknown identifiers are
  malformed; a property allowed once is refused twice; values out of range
  are refused; User Properties are bounded (`max_user_properties`, 32).
  Side-specific fields are checked both ways: a client may not send a
  Subscription Identifier on a PUBLISH, nor a server a Session Expiry or
  "with will" on a DISCONNECT.
- Every decode error says which reason code an MQTT 5 peer is told
  (`DecodeError::reason`). QoS 2 is decoded so that the gateway can refuse it
  with "QoS not supported"; PUBREC/PUBREL/PUBCOMP wait for phase 6.
- The protocol-neutral contract types between the gateway and the apply
  function were left out of 3a and, by D-61 A, are defined in 3b-1 with
  their first user.

Evidence: `crates/codec/tests/roundtrip.rs` (any packet either side may send
in either version survives encode and decode, 2000 cases; split anywhere it
decodes whole; arbitrary bytes never panic) and `crates/codec/tests/rules.rs`
(16 rules, one test each); the fuzz target `codec_stream` ran 5.5 million
inputs in two minutes under the documented memory limits with no finding.
Not measured: decode and encode cost against `mqtt`. The encoder builds each
body and property section in a scratch buffer, one allocation per packet;
a PUBLISH with properties boxes them, one allocation. Both are marked in the
code and wait for an encode/decode benchmark before step 4 puts the codec on
the hot path.

**Step 3b-1 (2026-10-06): done, D-62 C and D-61 A.** Crate `apply`, new
beside `broker`:

- The protocol-neutral contract: a gateway proposes `Proposal`s (publish,
  subscribe, unsubscribe), each the body of one `journal` record of its kind,
  tagged with an `Origin` — a session at one gateway node — and the
  gateway's own request id; applying a committed record yields `Effect`s
  (accepted, subscribed, unsubscribed, deliver), each addressed to an
  `Origin`. Every node computes all of them; each gateway acts on its own.
- `State`: subscriptions and retained messages derived from committed
  records only, applied in lsn order exactly once. One delivery per matching
  session at the stronger of its overlapping grants, never above the
  publisher's; retained messages go to the next subscriber with the
  subscribe's lsn; an empty retained publish removes it; fan-out shares one
  payload. Ordered maps make the effects the same, in the same order, on
  every node. A record that does not decode, or of a kind a later step
  applies, stops the node instead of being skipped.
- `crates/sim/tests/apply.rs`: three nodes, each `cluster` plus `apply`,
  clients at every gateway, proposals forwarded to the sequencer over a
  network that drops and reorders, crashes and power losses. At every effect
  a node produces for a live session it checks invariant 4 (the record is
  committed and durable on a majority), that the effect is what an
  independent reference model of the committed log says, in order, and that
  every node that applied lsn L holds the same state there. A mutation that
  applies appended rather than committed records fails at seed 0; one that
  ignores the subscriber's grant fails at seed 1. The 1000-seed sweep in
  release passed: 206 808 lsns committed, 50 124 effects checked (31 595
  deliveries), 23 137 restarts, 11 754 power losses, 44 905 forwarded
  proposals lost.

Not yet: sessions and inflight at-least-once state (step 7) — a subscription
belongs to an `Origin` and lives until unsubscribed; the state is unbounded —
subscriptions per origin, retained messages — and nothing closes an
origin, so a closed session's subscriptions stay (rules.md §7, §8) until
3b-2 brings session close and the bounds (D-63); a routing index (every
subscription is tried per publish, marked in the code); the log in this
simulation is never compacted, since rebuilding state after compaction
needs the state copy of step 7b.

**Step 3b-2 (2026-10-06): done, D-63 A and D-64 A.** Crate `gateway`, new,
and `apply` extended:

- `apply` now holds all session state the log decides: subscriptions with
  their options (No Local, Retain As Published, Retain Handling),
  at-least-once inflight per session and a backlog behind it, committed
  `Ack`s releasing them, `SessionGone` and a gateway's `NodeReset` dropping a
  session's everything. Every bound is decided in `apply`, from the log, so
  every node refuses the same thing at the same lsn: subscriptions per
  session and in total (a refusal is a SUBACK failure), retained messages in
  count and bytes (beyond, delivered but not kept), inflight per session,
  backlog per session in messages and bytes, and inflight and backlog bytes
  of all sessions in one budget (beyond, an at-most-once message is dropped
  and an at-least-once one closes the session as a slow consumer — the old
  path's overflow rules, now effects of committed records). `NodeReset` has
  record kind 9 of its own; `Disconnect` stays the per-session event of
  step 7.
- `gateway`: MQTT 3.1.1 and 5 connections over `codec`, proposals to the
  log, committed effects back as packets. A PUBACK, SUBACK, UNSUBACK or
  delivery is sent only for a committed effect. Connection-local state stays
  local: keep-alive, topic aliases, the client's receive maximum, packet
  identifiers. One live session per client id within the gateway, the newer
  taking over. A connection whose proposals are at their bound is not read
  (`wants_read`); no proposal is dropped. At-most-once deliveries held
  behind a QoS 1 delivery that waits for the client's receive maximum are
  bounded per connection and in total, and dropped and counted beyond; at
  the bound of lifecycle proposals awaiting commit, new connections are
  refused. A proposal not committed within
  `commit_timeout` closes its connection; `Ack`, `SessionGone` and
  `NodeReset` are retried with exponential backoff until their effect is
  seen. A gateway accepts connections once its `NodeReset` is committed, so
  nothing an earlier incarnation left can reach a new session.
- `crates/sim/tests/gateway.rs`: MQTT clients, half 3.1.1 and half 5,
  through `codec` against three nodes, with takeovers, clean goodbyes,
  dropped sockets, crashes and power losses. Checked at every packet:
  invariant 4 at the wire, invariant 7 within a gateway, per-session order
  per topic; at the end every session is gone from every node's state. A
  mutation that acknowledges a publish when it is read fails at seed 0. The
  1000-seed sweep in release passed: 138 451 packets checked at the wire,
  7 339 deliveries, 3 714 PUBACKs, 3 487 takeovers, 115 721 connections
  (58 009 MQTT 5, 57 712 3.1.1), 8 985 restarts, 4 542 power losses; the
  `apply` and `cluster` sweeps passed again after the changes below.

Found by these simulations and fixed:

- **Ordering in `apply`**: an at-most-once delivery overtook an earlier
  at-least-once one waiting in the backlog. Now nothing overtakes what
  waits (rules.md §7).
- **Resend storms in `cluster`**: under a slow network every heartbeat
  resent a window to replicas that were only slow. A resend now needs a
  whole period without progress, and each resend without progress doubles
  the wait, capped at 8 heartbeats. Gateway retries back off the same way.
- **Unbounded memory, found by review**: the gateway's waiting deliveries
  and its pending `SessionGone` retries had no bound, and `apply`'s inflight
  state was bounded per session only. All three are bounded now, as above.
- **Forwarding must keep order**: a gateway's link to the sequencer may lose
  its tail but never reorder, or one session's publishes commit out of
  order (`gateway::Action::Propose`; a requirement on step 4's driver, like
  the one in `cluster::Message`).
- Three harness bugs, not protocol bugs: uniformly random delivery from
  one queue made latency grow with the queue (delivery is now mostly in
  order, 20% any message); effects handed to a gateway re-entrantly
  reordered them (proposals of the sequencer node's own gateway now loop
  back after the step); and dropping a message with `swap_remove` moved
  another ahead of its predecessors.

Not yet, and how each answers a client until then (cluster mode is
test-only, refused by the server's configuration):

- Sessions outlive no connection (step 7): a 3.1.1 CleanSession=0 is
  answered with SessionPresent=0 and the session ends with the connection;
  an MQTT 5 client asking for a session expiry is told 0 in CONNACK.
- Wills are accepted and not published (step 7).
- Credentials are not checked: `auth` is wired with the runtime (step 4).
  Enhanced authentication is refused (CONNACK "bad authentication method").
- Subscription identifiers and shared subscriptions are refused, and
  CONNACK says so; QoS 2 is refused with "QoS not supported".
- MQTT 5 message expiry is carried through the log but not enforced
  (D-65 A: carried, not enforced until step 7, where expiry enters the log).
- The server sends no topic aliases; every subscription is tried per
  publish (a routing index is phase 5).
- An assigned client id (`straum-<node>-<session>`) can equal one a client
  chose; the newer then takes the other over. Ids unique in the cluster come
  with step 7.
- `apply`'s limits are part of what the log means, but each node reads its
  own: nodes with different limits would diverge silently (only the
  simulation's per-lsn state check would see it). Step 4 refuses a link
  between nodes whose limits differ (done); phase 4's metadata log then
  holds them.

**D-65 A (2026-10-07): MQTT 5 message properties through the log.** A
publish's payload format, expiry, content type, response topic,
correlation data and user properties go into the record's properties
section (`gateway::properties`), opaque to `apply`, and come back to MQTT 5
subscribers; a 3.1.1 subscriber gets the payload alone. They count with the
payload against every byte bound. Evidence: `crates/gateway/tests/rules.rs`
and `crates/apply/tests/state.rs` (pass-through), and the MQTT simulation,
which derives each message's properties from its payload and checks them at
every MQTT 5 delivery; 1000 seeds.

**Step 4 (2026-10-07): done, test-only; D-67 B, D-68 A.** Crate `node`: one tokio task per
node owns the core, the applied state and the gateway; the log on disk runs
on a thread of its own (recovery verifies the chain and cuts a torn batch,
appends are group-committed); links carry `cluster::wire` frames, one TCP
connection per direction, opened by a HELLO with the node, the slot and a
digest of the `apply` limits. A forwarded proposal travels on the one link
to the sequencer, so it keeps order; a link that breaks drops what was
queued for it. MQTT connections are read only on a credit the node loop
grants while the gateway wants to read, and written through a bounded
queue; a client that lets it fill is closed. The gateway's `first_session`
comes from an incarnation counter made durable before use. Recovery
verifies the chain record by record as it reads, holding one record at a
time. Baselines of `codec`, `apply` and `gateway` are in
`docs/baselines/apple-m1-dev.md`, on macOS and in the Linux container, both
preliminary (rules.md §11: no quiet Linux host). Evidence: `crates/node/tests/cluster.rs`, five tests
over real sockets and files, stable over eight runs; removing the hand-off
of applied effects to the gateway fails the MQTT test. The MQTT test
checks every PUBACK against the node's applied acceptances: the node
publishes a record's effects before its gateway acts on them. The cluster
simulation runs one seed in three with an uncommitted bound of 4–16; 1000
seeds pass; how often the bound held a batch back is not counted.

**D-67 B (2026-10-07): snapshots and compaction on the new path.**
`apply::State::encode`/`decode` write the applied state at its lsn in a
versioned format that recomputes every derived counter and refuses other
limits or another version. Every `snapshot_every` applied records the node
loop encodes the state, if it is no further than the local durable log; the
WAL thread writes it as one file (`[magic][crc32][body]`, written aside,
synced, renamed, directory synced) and only then removes the segments
wholly below `min(snapshot, floor)`. The floor is the sequencer's
`compaction_floor` over the byte bound `retain_bytes` (D-57 A); a replica
keeps the same byte bound's worth below its commit point, since in step 5
it may take the slot over and serve catch-up. No pins: a catch-up read is
one batch, read whole by the WAL thread, which also does the removal, in
order, so no removal can cut a read in half; a replica whose next batch
went is lost, as in D-57 A. A start loads the snapshot and applies the log after
it, and refuses one that does not meet the log. Range reads for that start
from the segment holding their first record, no longer from the first
segment. Evidence: two properties in `crates/apply/tests/state.rs` (a
snapshot anywhere plus the rest of the log gives the whole log's state and
effects; damaged or short snapshots refused, with a mutation caught); the
apply simulation takes snapshots at random durable points, compacts its
in-memory log below them (a compacted record is emptied, so reading one
fails the run), and restarts nodes from snapshot and suffix, every lsn
after checked against the other nodes (1000 seeds: 9052 restarts from a
snapshot, 8141 of them over a compacted log; a node ignoring its snapshot
fails); `crates/node/tests/cluster.rs`
`compacted_nodes_restart_from_their_snapshots` — every node removes the
start of its log, the sequencer and a replica restart and converge with the
node that did not; a node ignoring its snapshot fails to start there.

Not yet in step 4:

- Configuration and its startup checks moved to step 7 (D-68 A):
  `NodeConfig` is built by tests; the one startup check done is the limits
  digest at every link.
- A proposal the sequencer's full queue refuses is dropped, not held (a
  slot change no longer drops one, D-109 B). No acknowledgement is lost: the gateway closes a publisher whose publish does
  not commit within `commit_timeout` and resends its lifecycle proposals.
  `cluster::Sequencer::propose` no longer promises that the caller waits.
- Credentials are not checked (`auth` is not wired); metrics are not
  exported; the `storage/commit` baseline with the batch hash is not taken
  on Linux.
- `Status.digest` hashes the whole applied state whenever it changed: a
  test hook, to be taken off the loop before an end-to-end baseline.
- A snapshot is encoded whole on the node loop and held whole in memory
  until written: a pause and a buffer as large as the state, which
  `apply::Limits` bound. Streaming it is for when a measurement asks.
- ~~A sequencer that stops stops the cluster~~: step 5, below.

**Step 5 (2026-10-08): done, test-only; D-69 A.** `cluster::Voter` runs a
node across slots (unified-stream §2.2, §2.4 ungraceful):

- **Schedule.** `owner_of(slot)`: the configured owner for the first slot,
  then the voters in turn. After `takeover_ticks` heartbeats without the
  owner, the next node in the schedule takes the next slot; each further
  period of silence passes the turn on (a dead successor). Time only
  starts a takeover.
- **Durable promise.** `Effect::Promise` is made durable by the driver
  (`node`: a `promise` file, synced) before `on_promised` lets the node
  answer; from then it takes, acknowledges and leads nothing of a lower
  slot (invariant 6).
- **Takeover.** The candidate collects promises and log ends from a
  majority, catches up (`Fetch`/`Fetched`, one batch at a time) from the
  log that ends latest by `(slot, lsn)`, and writes `SlotStart` as the
  slot's first batch (past the uncommitted bound, which an inherited tail
  can fill).
- **Commit.** A sequencer commits only through a record of its own slot:
  the commit point must reach its `SlotStart`, and only replicas whose
  logs end in its slot count. Older records commit with it, never by
  counting their copies (Raft's current-term rule).
- **Truncation by the slot table.** A replica whose log ends in an older
  slot is placed where the slot tables say the logs meet; every `Append`
  and `Fetched` carries the slot of the sender's record at `prev`, and
  the receiver cuts only where slots prove the records differ, at batch
  boundaries, never below what it knows committed
  (`storage::Segments::truncate_after`, across segments). A replica that
  cut says so in every report until it takes a batch (`Ack.cut`); a
  sequencer probes a replica whose last slot it does not have before
  shipping to it.
- **Restart.** A restarted node follows; it never leads its old slot
  again, and starts knowing as committed what it has applied (snapshot
  and log), so its cut guard is not blind until the first append.
- **Reading of the step plan.** The successor schedule, listed under step
  6, comes with step 5: an ungraceful takeover needs it (§2.4, "мёртвый
  преемник: следующий по расписанию"). Step 6 keeps `SlotEnd` and planned
  rotation. The schedule is the configured first owner, then the voters
  in list order, until the metadata log (§2.5) holds it. The sequencer
  still syncs before it ships: D-71 A defers D-58's parallel ship to
  unified-stream phase 5, where a Linux measurement decides it.

Found by the takeover simulation and fixed, each with its seed in the
code: a `SlotStart` held back by the uncommitted bound (deadlock); a meet
point computed from one side's slot table only; a replica's position and
durability that could not go back after it cut; cuts on agreement rather
than proven difference (lost committed copies to a stale report); cuts
inside a batch; a takeover started while a cut was pending; a slot
committing an older slot's records by counting copies. The apply
simulation found that a stale report of an empty log placed a replica
below the compaction floor (a missing read for a replica in repair now
waits for the next report instead of losing it).

Evidence: `crates/sim/tests/takeover.rs` — three voters on in-memory logs
with crashes, power losses, promises made durable by nodes that die
before answering, partitions, loss and reordering, cuts and reads a
crash can fall between; invariants 1 (no copy of a committed record is
ever cut), 2, 3, 5, 6 (by durability when a majority's promises take
effect) after every step, and convergence after healing; 200 000 seeds
pass (686 820 takeovers, 170 939 cuts, 26 952 of them below the commit
point, 899 548 promises never answered). Three mutations — counting every
replica toward commit, catching up from the shortest log, taking a lower
slot after a promise — and the own-slot commit rule are each caught.
`crates/cluster/tests/voter.rs` (5 rules), `crates/storage/tests/
segments.rs` (a cut across segments), `crates/node/tests/cluster.rs`
`another_node_takes_the_slot_over_when_the_sequencer_stops` (real
sockets and files; disabling takeover fails it); the existing cluster,
apply and gateway simulations (fixed owner) pass 1000 seeds.

**D-70 A (2026-10-08): step 5 hardened in simulation.** The cluster
simulation (segmented log on simulated disks, compaction, pinned
catch-up readers) and the apply and MQTT simulations run three
`cluster::Voter`s instead of a fixed owner: promises, cuts and takeovers
alongside compaction, snapshots and gateways forwarding to a changing
owner. They found, and the code now handles, each with its seed in a
comment:

- *Batch boundaries:* a cut point is a boundary only in the log that
  named it; the voter now names where logs provably differ and the driver
  rounds down to a boundary of its own (safe: within a slot a batch is
  the same batch everywhere). `Effect::Truncate` says so.
- *Compaction and cuts:* compaction stops one record short of the commit
  point, so a cut never leaves a log with no record to say where it
  stands; a restarted voter knows as committed what its node applied
  (`knowing_commit`); the committed prefix agrees whatever a compacted
  slot table says; an unknown slot (0) at a record proves nothing.
- *Stale reports:* reports are ordered (`Ack.report`: the node's
  incarnation, durable, in the high bits, a count below) and a report not
  newer than the last one taken is ignored; a fresh cut always
  repositions a replica, a repeat reships nothing; a replica with a cut in
  flight does not report; a fetch answer echoes the asker's whole
  position, slot included.
- *Placement:* a replica's durability holds back compaction only once
  its log ends in the sequencer's slot (the meet point is an estimate); a
  replica whose log meets this one below what is kept is lost (D-57 A);
  one that holds records past what is kept in a slot this log no longer
  knows is lost too — a deviation from D-57 A, below; a missing
  read for a replica in repair forgets its position once, then loses it.
- *Liveness:* lost replicas still hear heartbeats, so they do not fall
  silent and break every slot at their turn; deliveries in the
  simulations keep queue order (a newest-first queue livelocked a replica
  that needed the oldest append).
- *Step-down:* a sequencer that steps down hands over its pending
  effects (defensive: no driver leaves effects pending, and no seed
  shows it).

Evidence: `STRAUM_SIM_SEEDS=10000` passes for the cluster simulation
(retention bound 1000 bytes, 100 seeds by default: 45 533 takeovers,
14 042 cuts, 2 873 segments removed, 677 compactions during a takeover,
640 held by a pin, 941 replicas lost within D-57's rules), the apply
simulation (36 139 takeovers, 12 579 cuts, 5 789 restarts of a compacted
log, 62 935 deliveries checked) and the MQTT simulation (39 059 slot
takeovers, 9 298 cuts, 49 217 deliveries checked), and 100 000 seeds for
the takeover simulation. Mutations caught by sweeps: unordered reports, a fetch answer
checked by end alone, compaction counting replicas not in the slot. The
apply and MQTT simulations use `takeover_ticks = 6`. **Criterion not
met:** takeovers run at about 0.2–0.3 per hundred steps, below the
card's "at least one per hundred"; biasing crashes and partitions toward
the sequencer moved the cluster simulation only to 0.32, since faults
themselves are a few percent of events. Raising it further is a choice
between takeover rate and what else the runs check (D-72). D-72 B was measured
and failed: sequencer faults 4–16 per hundred events lifted fault-heavy seeds
only to 0.40–0.42, and extra heartbeats lowered the rate, since a takeover
needs a hundred-plus events of round trips. D-80 B, liveness per fault, was
measured too: within 600 events a later slot committed after 50% of
sequencer faults with other faults paused (86% within 6 000), and 4% with
them running. Failed quiet cycles show a long uncommitted tail fetched one
batch per round trip (cause not established). Open as D-81.

**D-81 A (2026-10-09): the criterion is liveness after a fault.** The
cluster simulation runs fault cycles in two seeds in three: the sequencer
crashes or is cut off, nobody else is meanwhile (loss, reordering,
compaction and its restart go on), and a later slot must commit within
2 000 events in at least 9 cycles in 10 — asserted. 10 000 seeds:

| | before | reads of several batches |
|---|---|---|
| all cycles | 80.4% (one seed group) | 93.5% (6 807 of 7 281) |
| uncommitted bound 4–16 | — | 98.4% |
| no uncommitted bound | 80.4% | 87.7% |

Failover correlates with the uncommitted tail a new sequencer must move:
with `max_uncommitted` bounded, 98.4% of faults are followed within the
bound; without one, 87.7%. The mechanism behind the remaining
unbounded-group failures is not traced. **Deviation, recorded:** the
gate holds over both groups pooled (one to one by seed); the unbounded
group alone misses 9 in 10 (D-82; closed by D-82 A below, with that
group gated at its measured level). Cycles allow no second crash or
partition while they run, so a cycle in which a second node fails and a
majority survives is not measured. The change that moved the
rate: `Effect::Read` carries `upto` (the sequencer's durable log and the
replica's window), and the driver hands back whole batches within a byte
bound — or one batch, if that alone is larger (`cluster::ReadBatches`,
`NodeConfig.read_bytes`, which has no default yet, D-83) — instead of one;
catch-up and a candidate's fetch take fewer round trips. Two hypotheses
measured and dropped: a candidate keeping followers from counting silence
while it catches up (88.9% against 87.7%), and a window of 64 instead of
24 (89.2%). The sweep also found a liveness bug in `apply`: an
acknowledgement retried after it was applied, or for a session that is
gone, was answered with nothing, and the gateway retried it for ever
(MQTT simulation, seed 1189); every committed `Ack` is now answered with
`Acked`.

**D-82 A (2026-10-09): `max_uncommitted` bounds failover too, and the
gate is per group.** The cluster simulation runs three seed groups, all
with fault cycles: an uncommitted bound of 4–16, none (coverage), and the
default `cluster::DEFAULT_MAX_UNCOMMITTED`. Measured at 10 000 seeds for
candidate defaults: 64 → 94.1%, 96 → 91.1%, 128 → 89.1%, 256 and above
the same as none (87.7%; tails in a 1 500-step run rarely grow longer).
The default is 64, the largest with a margin above 9 in 10; 96, the
largest that passes, is the decision's literal wording — the choice is
open (D-84). Gates: 9 in
10 for the 4–16 group (98.4%) and the default group (94.1%); 85% for the
group with no bound (87.1%) — for that group alone the measured level,
amending D-81 A. A group's gate is asserted from 300 cycles: the default
run's 30 or so move 3 points a cycle, so the sweep carries the gate. The
default is in records and in simulation events; its cost in throughput
on a real host is not measured (D-84), and `node` does not use it until
configuration from the environment arrives in step 7a, with `read_bytes`
(D-83 A).

**Liveness contract (D-85, 2026-10-09).** What the new path promises after a
sequencer fails, and under which assumptions.

1. *Safety* — always, under any schedule of crashes, power losses,
   partitions, loss, duplication and reordering: invariants 1–6 of
   unified-stream §5 (one committed history, nothing committed beyond what a
   majority holds durably, no commit by a sequencer of slot k once a majority
   promised past k, every recovered log one chain), D-57 A (a replica lost
   only past the byte bound), and PUBACK only after commit. Checked after
   every simulation event; no liveness change below weakens any of them.

2. *Eventual liveness* — once the following hold from some point on, the
   commit point advances again:
   - a majority of voters is up, mutually reachable, and *able*: not lost
     and not behind every log's compaction floor (such a replica needs a
     copy of the state, step 7b, not built);
   - every message is delivered eventually if resent often enough
     (fair-lossy links: loss, duplication and reordering allowed, as the
     heartbeat resends);
   - every log write and sync completes or fails, and every read of the
     local log is answered;
   - every node keeps taking heartbeats and events;
   - the uncommitted tail is within `max_uncommitted`, the retained log
     within its byte bound.
   - a takeover's promise round — `Promise` out, the promises made
     durable and back — fits within `takeover_ticks` heartbeats once in a
     while, and its remaining round trips — a catch-up of the uncommitted
     tail, `SlotStart` made durable — within `takeover_ticks × 64`: a
     takeover's wait for silence doubles after each one that a majority
     promised and that did not end in a commit (`patience`, D-88 A),
     capped at 64× so that a dead sequencer is still passed over within a
     bounded time. A takeover no majority answers leaves the wait as it
     was, so a partition does not lengthen the recovery after it; the
     price is that the promise round itself gets no doubling. Past either
     bound, eventual progress is not guaranteed.
   No bound on time follows from these: messages may take arbitrarily
   long, and progress comes once a takeover's round trips fit the wait.

3. *Bounded liveness* — progress within a stated time needs, on top: a
   round trip, a log sync and a catch-up of the uncommitted tail each
   shorter than `takeover_ticks` heartbeats, and patience at 1 (a commit
   since the last failed takeover). Under them a takeover finishes in about
   `takeover_ticks` heartbeats plus the catch-up. Nothing in the
   simulation's event counts maps to wall-clock time.

4. *Performance objectives* — throughput, and failover time from a
   sequencer's stop to the next commit (proposed: 9 in 10 within 1 s,
   D-84 A). Measured only on a quiet Linux host; not measured yet.

A scenario that does not meet (2)'s assumptions is classified, never counted
as recovered: in the cluster simulation, a fault cycle whose live nodes hold
no able majority is reported as "needs a state copy (7b)" with its seed.

**Failover stalls: causes found and fixed (2026-10-09).** The fault
cycles were changed from a bounded rate to the eventual criterion above:
a cycle counts from the moment a majority is up and reachable, runs until
a later slot commits, and stalls when nothing progresses for 20 000
events — progress being a higher commit point, or a node not sequencing
whose log passes the furthest it reached while not sequencing — or when
nothing has committed by 500 000 events. Healing works the same way and
has settled once every node able to catch up has committed through the
cluster's commit point at the first new commit. (Two coarser measures,
the lowest end and the summed ends, mistook a change of sequencer for a
stall: seeds 160, 6376.) Before the fixes, 3 000 seeds gave 173 cycles
with no commit after 100 000 events. Each fix below is shown by a seed
that fails without it, the others in place; candidate fixes that a
10 000-seed sweep showed unneeded were taken out again (progress
keepalives on serving a fetch, on appending fetched records, and a
candidate's `Promise` re-sent to the other voters — the last one also let
a candidate that can send but never hear keep every peer from counting
silence).

1. *A fixed wait for silence* (`Voter::tick`). A takeover needs round
   trips — promises, a catch-up, `SlotStart` made durable — and the next
   node in the schedule started the next slot after `takeover_ticks` of
   silence whatever stage the running one had reached. Where a round trip
   outlasts that wait every candidate is passed over before it finishes,
   for ever: seed 4, slots 10 to 1 268 in 100 000 events; without the
   fix, healing churns through 3 520 slots (seed 643), and 3 000 seeds
   take 2 826 s instead of 8. Fix: `patience`, doubled by every takeover
   started by silence, up to 64×, back to 1 once the commit point moves;
   since D-88 A, by every such takeover that a majority promised (seeds 4
   and 643 still recover).
2. *Stale read answers taken* (`Sequencer::on_read`). An answer to a read
   given up since cleared the read outstanding and asked again, queueing
   one more read behind the rest for every stale answer; a catch-up moved
   one batch in many reads, and a replica it needed for commit never
   caught up (seed 160: `reading: Some(302)` behind dozens of reads for
   297). Fix: only the answer to the read outstanding is taken.
3. *Reads matched by replica only* (`Voter::on_read`). The answer to a
   sequencer's catch-up read went to a fetch from the same replica still
   held from before this node led; the sequencer waited for it for ever
   (seed 1 384). Fix: matched by replica and position, and given to both
   when both asked for it.
4. *A fetch for an older slot served across a promise* (`Voter::serve`),
   a safety fault. A candidate passed over still fetched; the node it
   asked, by then the next slot's sequencer, served it from its log,
   records not yet synced included. The replica held them durably first,
   so once a majority had promised past the slot the sequencer's own later
   sync still let it commit them — invariant 6 (seed 4 629: slot 9 raised
   the commit point to 23 past the fence at 21; reached once the other
   fixes changed the schedules). Fix: a fetch for a slot below the one
   promised is not served.
5. *A handoff stepping down before its successor held `SlotEnd`*
   (`Voter::tick`). A successor that had just cut its log was left with no
   one to catch up from, and the slot went by silence (takeover
   simulation, seed 4 905). Fix: the sequencer keeps heartbeating until
   the successor holds `SlotEnd` durably, at most `takeover_ticks`
   heartbeats.
6. *Checks that could not settle or judged too early* (the simulations,
   not the protocol): a rotating sequencer is never quiet at one end
   (seed 1); a stale sequencer's uncommitted end was taken as healing's
   goal (seeds 15, 16); healing went on proposing while a follower caught
   up; a commit handed over at step-down was judged by the role after it
   (seed 4 629 — now judged by the committed record's slot, which must
   have been this node's in this life, and still under that slot's
   fence).

A cycle that stops without a majority able to catch up — a replica lost
past the byte bound, needing a state copy (step 7b) — is classified, not
counted recovered, and only when that came within `LOSS_GRACE` (5 000)
events of the last progress, above the 99th percentile of recovery
(2 933): a replica passed by compaction after progress had been absent
longer would count as a stall. In the sweep the latest such loss came 572
events after the last progress; 146 of the 162 nearest cases had made no
progress yet, the replica being compacted past a median of 214 events
into the cycle (this simulation keeps 1 000 bytes behind commit).

Checked, 2026-10-10 (D-87 A): every node keeping a classified cycle's
majority from catching up had been passed by the byte bound, as D-57 A
allows (forced, or lost below the byte-bound floor) — any other would
count as a stall; once the node cut off comes back, 500 of the 504
cycles regain a majority able to catch up and commit, as the test now
requires, 3 still have none, and 1 is the slow case below. With a
realistic bound, 100 000 bytes instead of 1 000, no cycle of 18 524 needs
a state copy and none stalls (`a_realistic_retention_bound_needs_no_
state_copy`, 100 seeds, in every `cargo test`); within 2 000 events
100%, 99.9%, 93.5% for the three groups. One cycle (seed 8956, no
uncommitted bound) is still getting further at the 500 000-event cap:
its sequencer appends faster than the only replica it can commit with
catches up — what `max_uncommitted` bounds, off in that group — and it
commits at about 1 300 000 events (an ignored test reruns it with the cap
raised); such "slow" cycles are reported apart and allowed only where
there is no uncommitted bound. Patience after recovery (D-86 A): at the
start of healing, runs whose most patient node was at 16×, 32× or 64× took
up to 300, 389 and 1 790 rounds (heartbeats) to the first commit, against
at most 66–188 below that: patience grown while takeovers failed for want
of a reachable majority makes recovery after the partition heals slow
(D-88). Both readings were corrected the same day — see "D-88 A, D-90 B"
below: the long heals were catch-ups, not patience, and the slow cycles
are now stalls unless they commit within a derived cap.

After them, 10 000 seeds: 17 934 cycles recovered, 0 stalled, 1 slow
(seed 8956), 504 classified as needing a state copy (the first ten seeds
are printed, all are in the run's statistics); events from a majority up to the next
commit: p50 353, p90 855, p99 2 933, max 125 134. Within 2 000 events:
98.4% (uncommitted bound 4–16), 96.1% (64), 91.0% (none) — a simulation
measure, not time. Regression, in every `cargo test`:
`sim/tests/cluster.rs::every_seed_that_stalled_recovers` (seeds 1, 4, 7,
15, 16, 22, 79, 91, 160, 643, 1 384, 4 629, every invariant checked),
the main sweep's "no stall" assertion,
`sim/tests/takeover.rs::a_handoff_waits_for_a_successor_catching_up`;
unit tests in `cluster/tests/protocol.rs` (a stale read answer) and
`cluster/tests/voter.rs` (patience, read routing, an older slot's fetch,
the bounded handoff wait), each failing without its fix.
`STRAUM_SIM_TRACE=1` prints the cluster simulation's trace, and each
node's state at a stall.

**D-88 A, D-90 B (2026-10-10).** *Patience* now doubles only for a
takeover a majority promised (`Voter::decide`), not for every takeover
started by silence. Compared on the same 10 000 seeds, with healing split
by seed group: under the old rule, every heal over 105 rounds was in the
group without an uncommitted bound (up to 7 790 rounds), and in the
bounded groups 64 runs reached 64× and healed within 41 rounds — the
1 790 rounds that motivated D-88 were a catch-up across an unbounded
tail, not patience. Under D-88 A no node in a bounded group passes 16×
(once; it healed in 1 round), against 64× in 64 runs before; the bounded
groups' heals are unchanged (at most 105 rounds); 0 stalls; 98.5%,
96.3%, 90.4% within 2 000 events (98.4%, 96.1%, 91.0% before); takeover
(20 000), apply and MQTT (10 000) sweeps pass. Regression:
`a_takeover_no_majority_answers_leaves_the_wait_as_it_was` (cluster/tests/voter.rs).
The contract above now asks the promise round to fit `takeover_ticks`.

*Slow cycles* are no longer allowed (D-90 B). Raising the cap to
50 000 000, every cycle of 10 000 seeds commits. The long ones are all in
the group without an uncommitted bound: a sequencer without a majority
kept appending (0.55 records an event), the next slot that commits starts
past that whole tail, and takeovers during the catch-up move that start
further on; the catch-up then runs at a steady rate — measured from the
start of the slot that commits, 10.8–13.0 events a record per replica
caught up over the 24 catch-ups of 2 000 records or more (the sequencer
reads its log for one replica at a time: two caught up together take
twice as long). The longest: 50 675 records, 602 635 events (seed 1 831).
So the cap is 5 000 000 events, a cycle at it is a stall, and every
catch-up of 2 000 records or more must cost at most 24 events a record
per replica — twice the typical cost, so a catch-up at half speed fails;
at that cost the cap admits 4.1 times the longest catch-up seen. The
seeds that outlasted the old cap (802, 1 831, 4 714; seed 8 956 no longer
does under D-88 A) run to their commit in the ignored
`a_tail_outrunning_its_catch_up_commits_eventually` (about 100 s);
`without_an_uncommitted_bound_a_new_slot_must_first_cover_the_whole_tail`
(cluster/tests/protocol.rs) shows the mechanism in milliseconds. The
default `max_uncommitted` (64) makes it impossible: the tail is bounded.

**D-91 A.** `node::DEFAULT_RETAIN_BYTES` = 64 MiB, the old path's default,
read once step 7a reads configuration; a starting value, not a measured
optimum.

**D-85 B, D-89 A1 (2026-10-10): bounding replica lag — invariants and
their proof; built as D-89 A2, below.** D-85 chose flow control as the direction on
condition that what it bounds is defined first and shown never to block
commit for a majority able to commit. D-89 A1 fixes the definitions and
the rule, and proves the user's six conditions; implementing them is D-89
A2, a separate step.

*Definitions.* A slot `k` with sequencer `S`, `N` voters, majority
`M = ⌊N/2⌋ + 1`. `E` is the last lsn `S` appended; `C` its commit point;
`D_S` its durable end. A replica `r` is *placed* when its last report ended
in slot `k` (`in_slot`), is not lost and not probing — its log is then a
prefix of `S`'s — and `D_r` is the durable end it last reported; an
unplaced replica has no `D_r`. `C` and every `D_r` only grow. `F` is `S`'s
compaction floor.

- Three candidate measures: **unacknowledged** `E − D_r` — in flight it is
  already bounded by `window`; owed, it is lag plus the uncommitted tail
  `E − C`, which `max_uncommitted` bounds; **lag** `C − D_r` — how far `r`
  is behind what is committed; **catch-up room** `D_r − F` — whether `S`'s
  log still holds what `r` needs (below `F`, `r` is lost and needs a state
  copy, D-57 A). The one bounded is **lag**: a survivor in the next slot's
  majority must hold everything to `C` before that slot commits (§2.3,
  `SlotStart`), so lag is the catch-up a failover owes. Catch-up room is
  not bounded by flow control: it is retention's (D-57 A, D-91 A).
- An **able majority** is `M` voters, `S` counted, that are up, reach
  each other, and are placed. Only an able majority's durability moves
  `C`. A replica that is reachable but unplaced — its log ends in an older
  slot, it is probing, cut back, or lost — counts toward no commit, with or
  without flow control: network availability is not enough.

*The rule.* Parameters `L` (records) and `T` (heartbeats of `S`), and
`max_uncommitted` set, with `L ≥ max_uncommitted + max_batch`. Each
replica is *attached* or *detached*; only `S` holds this, per slot.

- **R1 — start.** At the start of a slot every replica is detached.
- **R2 — attach.** A detached replica attaches when it is placed, its lag
  is 0 (`D_r ≥ C`), and `C` has reached the slot's first lsn
  (`SlotStart` committed).
- **R3 — hold.** `flush` makes no batch of queued proposals while some
  attached replica has lag `> L`. `SlotStart` and the batches of a sealed
  slot (`SlotEnd`) are never held. Nothing else reads attachment. With
  nothing queued nothing is held and R4 counts nothing: an idle sequencer
  detaches no one.
- **R4 — detach.** When, since the last batch, `flush` has been held for
  `T` heartbeats in all — across holds, however each ended — every
  attached replica with lag `> 0` detaches; only a batch resets the
  count. A replica that becomes unplaced or lost detaches at once. (Counted
  per hold, a replica that catches up part of the way would end a hold and
  the commit point could pass it again with no batch made, each hold
  starting from zero; found by the review of D-89 A2.)

*Lemma 1 — at most `N − M` replicas lag.* `C` is the `M`-th highest of
`D_S` and the placed replicas' `D_r` (or lower, until the slot's first
record commits), so at least `M` of those values are `≥ C`, and at most
`N − M` placed replicas have `D_r < C`. Each replica whose durability `C`
rests on has lag 0. Flow control can only ever name a replica outside the
majority that committed. For three voters: at most one.

*Lemma 2 — an attached replica cannot start a hold within one.* While
`C` is at or past the slot's first lsn, every batch is made with
`E − C < max_uncommitted` and adds at most `max_batch`, and `C` only
grows, so `E − C < max_uncommitted + max_batch ≤ L` at every point after.
During a hold `E` does not move. A replica with lag 0 at any moment of the
hold — attached then (R2), or kept at R4 — has `D_r ≥ C` then, so for the
rest of the hold its lag is at most `E − D_r ≤ E − C < L`.

*The six conditions (D-89).*

1. **Commit of records already appended is never delayed by a lagging
   replica.** `C` is computed from `D_S` and the placed replicas' `D_r`
   (`advance_commit`); shipping (`ship`, `tick`'s resend), syncing
   (`on_durable`) and the reports do not read attachment or the hold (R3).
   A record appended commits once an able majority holds it durably —
   with no later append needed, because the slot's own first record, which
   `C` must reach first, is `SlotStart`, never held. Flow control cannot
   remove, delay or reorder a message, a write or a sync that commit
   depends on.
2. **The wait before new appends is bounded by `T`.** A hold exists only
   while some attached replica has lag `> L`. At the `T`-th held heartbeat
   since the last batch, R4 detaches every attached replica with lag
   `> 0`; those left have lag 0 and, by Lemma 2, cannot exceed `L` before
   `E` moves; nor can a replica attaching meanwhile. So between two
   batches flow control holds at most `T` heartbeats of `S` in all. The
   bound is per batch, not per proposal: a proposal queued behind others
   waits up to `T` before each batch ahead of it, at least one batch
   being made between those waits.
3. **After `T` the lagging replica is detached and holds nothing.** R4,
   and R3 names attached replicas only. It re-attaches only at lag 0 (R2).
4. **Detachment changes nothing in quorum, durability, fencing or
   acknowledgement safety.** Attachment is read by `flush` alone. The
   commit rule (quorum), what is written and synced (durability), the
   slot, promise and fetch rules (`Voter`, fencing, invariant 6), and
   `Effect::Committed` — the only source of a PUBACK — are untouched;
   compaction still keeps a detached replica's records up to the byte bound
   (D-57 A). A hold only makes a batch later than it would have been, the
   same as proposals arriving later: a schedule every simulation already
   produces. Invariants 1–6 and D-57 A hold with flow control as without.
5. **No bounded failover without a state copy is promised where the
   remaining replicas cannot form the majority it needs.** When `S` fails,
   the next slot commits once an able majority of the survivors holds
   everything to `C` and `SlotStart` (§2.3). A bound on that catch-up,
   `L + max_uncommitted + max_batch` records per survivor plus the
   uncommitted tail, holds only if every survivor the new majority needs
   was attached at the fault — an attached replica's lag is below `L` when
   a batch is made, and grows by at most `E − C` after it. With three
   voters, both survivors. If one was detached, the catch-up spans its
   whole lag: bounded by retention, not by `L`. If it is below the floor,
   it needs a state copy (7b) and only the other survivors' majority, if
   any, is promised anything (eventual liveness, "able majority", above).
   Flow control does not change which majority is able.
6. **A detached replica's return is safe.** It is shipped as any replica
   — from memory, or read from the log within `window` — since shipping
   does not read attachment. Its catch-up uses only the paths it uses
   today; if its position is below `F` it is lost (`Effect::Lost`) and
   needs the state copy (7b, D-57 A). It holds nothing until it re-attaches
   at lag 0, and by Lemma 2 cannot then start a hold until the next batch.

*Not a decision by time.* `T` counts heartbeats and only triggers a
transition — a replica detaching — which changes no committed state and
no safety rule (§1). A stale report understates `D_r` and can only
detach a replica earlier: the failover bound of (5) is lost for it, never
safety (4). Without `max_uncommitted` (the simulation's uncapped seed
group) Lemma 2 does not hold and neither does (2)'s bound: flow control
requires an uncommitted bound.

*What it costs.* Throughput while an attached replica is slow: up to `T`
heartbeats of no new batch per hold. `L`, `T` and that cost come from the
Linux measurement (D-84 A, D-89 A2); the cap benchmark must show failover
within the target with every survivor attached. A2's simulation checks:
no fault cycle with an able majority stalls (D-85), no hold lasts more
than `T` heartbeats, and every safety invariant, with flow control on.

**D-89 A2 (2026-10-10): built, off by default.** `cluster::Config::
lag_bound: Option<LagBound>` (`records` = `L`, raised to
`max_uncommitted + max_batch`; `ticks` = `T`); `None` everywhere outside
the simulation until the Linux measurement sets `L` and `T`. R1–R4 are in
`Sequencer` alone: `attached` per replica, set by R2 after every commit
computation and cleared where a replica stops being placed or is lost;
`holding_back` (R3) read only by `flush`, and never for a sealed slot;
R4 in `tick`, its count reset only by a batch. Checked: the cluster
simulation runs it in half of each bounded seed group, `L` at most 7
above its floor and `T` 2–8, and after every heartbeat counts the
heartbeats a sequencer held with its slot and end unchanged — whatever
ended a hold in between — and asserts they stay below `T`: a hold that
started again without a batch, which is what a failure of Lemma 2 would
produce, fails the run (checked: attaching without the lag-0 condition of
R2 fails it); every safety invariant is checked as for every seed. 10 000
seeds: 0 stalls, 64 heartbeats held in 21 stretches between batches,
none reaching `T`; liveness
per group as without it. Unit tests (cluster/tests/protocol.rs): holds last at most `T` heartbeats
between two batches, counted across holds; the replica detaches, stays
detached while behind, and attaches again once caught up; commit of what
is appended goes on during a hold — each failing without its rule
(R2's lag-0 condition, R3, R4, the count across holds). Not
measured: throughput and failover on Linux (D-84 A), and holds are rare
at this simulation's sizes — 21 stretches in 10 000 seeds.

**D-93 A, D-95 A, D-97 A (2026-10-10): checks before 7a.**

*D-93 A — a slow replica, where flow control must act*
(`a_slow_replica_is_detached_at_t_and_rejoins_while_the_majority_commits`,
cluster simulation). Each run, four times:
- proposals pause until one sequencer counts both replicas;
- one replica's disk turns slow for 3 000 events — 99 of its 100 writes
  are deferred to a later write, its promises with them, as `node`'s WAL thread syncs
  appends before a promise;
- then fast again, 10 000 events under the same load;
- then proposals pause until it is counted again.

No crash, partition or message loss; retention 100 000 bytes; no
rotation and 30 heartbeats of silence before a takeover (see the
takeover finding below). 200 seeds with flow control on, 800 windows:
- **Detach and majority progress:** in every window the sequencer held
  new batches, and R4 detached the slow replica at the `T`-th held
  heartbeat — checked as that replica, by the sequencer the window settled
  on. The commit point moved after the moment of the detach in all 800,
  with the other replica. No takeover, no replica lost.
- **Rejoin:** with proposals paused, the replica caught up and was counted
  again in every window (p50 3 201, max 8 502 events).
- **Under continuing load** it is caught up from the log but closed the gap
  to the commit point in only 74 of 800 windows; the largest lag after
  the slowness was p50 295, max 550 records. Its catch-up runs at about
  the rate the sequencer appends: the race of D-90 B, in a bounded group
  this time. `max_uncommitted` bounds the uncommitted tail, not how far a
  detached replica trails, and L2 forbids holding appends for a detached
  replica. So under sustained load a detached replica is safe and served,
  but may not re-attach: it keeps no bounded failover (condition 5) until
  the load drops, and with a slower catch-up it would fall behind until
  retention passes it and it needs a state copy. A design decision (D-100).
- **Settling was needed:** under load the slower replica is rarely at
  lag 0, which R2 asks before attaching, so flow control often counts
  only one replica.
**Closed only at that timing.** With the base timing (3 heartbeats,
rotation) majority progress is not shown: a slow disk deposes healthy
sequencers (the finding below), with flow control on or off. Whether that
keeps 7a waiting is D-101's question.
The default `cargo test` runs 5 seeds (20 windows); `STRAUM_SIM_SLOW_SEEDS=200` is
the sweep.

*A takeover finding.* With the base timing, three heartbeats of
silence, this simulation's random delivery takes the slot over about every
300 events even with no fault (several times in a 3 000-event window), and
every new slot starts with all replicas detached (R1). A slow disk triples
that (145–200 takeovers a run against 35–53, 95 of 100 writes deferred, the same with flow control off
and under the patience rule before D-88 A, so neither caused it). The
mechanism: a replica whose promise of the newer slot is not durable yet
takes that slot's appends as a reason to promise, not as a heartbeat —
`on_append` resets silence only when following — so on a slow disk it
counts silence while the sequencer is talking to it, starts takeovers of
its own it cannot finish, and its patience stays at 1 because no majority
ever answers them (D-88 A). Safety is untouched; a healthy sequencer can
be deposed (D-98, D-99).

*D-95 A — a takeover during a catch-up*
(`a_takeover_during_a_catch_up_builds_on_no_stale_prefix_and_keeps_the_fence`,
cluster/tests/voter.rs). Three voters with their logs and reads served
from them, run twice — once with node 1 (the whole committed log) taking
the slot over, once with node 2 (the stale prefix), only the taker
counting silence, and the taker asserted:
- slot 1 commits three batches with node 1 while node 2 is cut off;
- node 2 gets one batch of its catch-up, then node 0 dies;
- nodes 1 and 2 take the next slot over.

Checked:
- every record committed in slot 1 is in place, byte for byte, on both
  logs;
- the new slot's `SlotStart` follows the committed log directly;
- it commits only once node 2 holds it (with node 0 down this follows
  from the majority of two; it is asserted on the final state);
- node 0, back and still believing it leads slot 1, has its late batch
  refused by both, and commits nothing past the fence;
- no log is ever cut.

Mutations: a candidate catching up from the shortest log instead of the
longest, or a follower taking an older slot's appends, each fail it.

*D-92 A — what the misses of the 2 000-event measure are.* A correction
first: the 90.4% that D-92 called a 0.4-point margin over the 90% gate
belongs to the group without an uncommitted bound, whose gate is 85% (D-82
A); the default-bound group (64) is at 96.3%. The 10 000-seed sweep's
misses, every fault cycle classified:

| Group (gate) | Within 2 000 events | Never committed in the cycle (state copy) | Committed after 2 000 |
|---|---|---|---|
| bound 4–16 (90%) | 98.5% | 100 | 0 |
| bound 64 (90%) | 96.3% | 211 | 1 |
| none (85%) | 90.4% | 171 | 361 |

- **State-copy cycles.** Every cycle that never commits within the cycle
  is on the state-copy path: a replica passed by the simulation's
  1 000-byte retention bound, with no majority left that can catch up.
  With 100 000 bytes there are none (D-87 A). The contract promises
  nothing here until 7b.
- **The long cycles of the group without a bound.** 324 of the 361 spend
  most of their time catching up the uncommitted tail of the slot that
  commits (mean 289 records). The other 37 spend it reaching that slot:
  - 7.7 slots tried and 2.6 takeovers on average, patience 4–8;
  - a tail of 134 records at the fault;
  - candidates passed over while catching up that tail, so patience grows
    as D-88 A intends.
- **The one long cycle of the group with bound 64** (seed 203, 2 037
  events) is tail-driven as well. The surviving replica held the isolated
  sequencer's uncommitted slot-1 tail (66 records), was cut back when the
  new slot's log proved it different, and caught up 71 records from the
  log in about 1 250 events. The table's catch-up measure took its end
  before the cut and so undercounts it.
  Both are covered by eventual liveness (the contract's assumptions
  hold); a bound on time needs `max_uncommitted`, which production
  always sets.

No category is unexplained and none stalls; patience is not changed.

*D-97 A — the run that did not finish.* The machine slept: the power log
shows "Entering Sleep state due to 'Idle Sleep'" at 03:26:56, seconds
after the run started at 03:26, and sleep until 04:32 but for a 6-second
maintenance wake. The background job was stopped at its one-hour limit
inside that sleep; the next run, started at 04:33, took 189 s. Evidence
that nothing else was involved:
- the code of that run, reconstructed (the tree had no commit; the diff
  is in `docs/decisions/d97-unfinished-run.md`) and rerun alone, took
  201 s;
- two copies under contention (with a third sweep beside them) took 378
  and 382 s;
- all three gave identical per-seed outcomes for all 10 000 seeds, so the
  simulation is deterministic and no seed hangs;
- slowest seed 1 831, 33 s alone and 78 s under contention; peak resident
  memory 171 MB;
- one voluntary context switch, so no waiting on a lock;
- no disk I/O, as the disks are simulated.
Long runs are now wrapped in `caffeinate -i`.
Its parameters, the code it ran and the power log are kept in
`docs/decisions/d97-unfinished-run.md`.
What stays uncertain: the code is a reconstruction, not a commit; the
cause rests on the power log, and no run reproduced a hang.

**D-98 A (2026-10-10): an append from a newer slot's owner is a sign of
life.** In `Voter::on_append`, an append from the owner of a slot above
the one promised now resets silence while the promise of that slot is made
durable. It changes nothing else: the node still promises first, and
appends, acknowledges and commits nothing before the promise is durable.
A sign of life is not a durable promise, and only the second enters the
commit rules (the user's condition).

Checked against D-98's conditions, at the standard timing (3 heartbeats,
rotation where the seed rotates), against the same runs without a slow
disk:

The 10% allowance in the gate is a stated allowance, taken from the spread
of the fixed build (+3% over 20 seeds, −6% over 100), not a measured noise
floor; before D-98 A the same comparison is +47% to +57%.

| 400 windows (100 seeds) | Takeovers started by silence | Won | Started by the slowed replica | Won by it |
|---|---|---|---|---|
| no slow disk | 4 313 | 2 228 | 1 520 | 787 |
| slow disk, before D-98 A | 6 543 | 1 023 | 4 781 | 36 |
| slow disk, after D-98 A | 4 074 | 970 | 2 492 | 31 |

1. **The scenario reproduces at the standard timing:** +47% takeovers
   started by silence before D-98 A.
2. **Takeovers are back at the level without a slow disk:** −6% (+3% over
   20 seeds). The slowed replica still starts more takeovers than it would
   on a fast disk, but wins fewer than the baseline. They are attempts its
   own slow promise cannot finish, and they depose no one.
3. **Commit while the sequencer is healthy:** the commit point moved after
   every detach (173 of 173). A replica heard by a newer slot's owner
   appends, acknowledges and commits nothing before its promise is durable
   (`a_newer_slots_append_is_heard_but_takes_nothing_before_the_promise`,
   which fails without D-98 A). Every simulation still checks invariants
   1–6 after every event.
4. **Fencing and late messages:** unchanged code paths. The D-95 test (an
   old sequencer's late batch refused, nothing committed past the fence),
   `a_fetch_for_an_older_slot_than_promised_is_not_served` and
   `after_a_promise_nothing_of_a_lower_slot_is_taken_or_acknowledged` all
   pass, as do the cluster (10 000), takeover (20 000), apply and MQTT
   (10 000) sweeps.
5. **A dead sequencer is still taken over:** being heard resets silence
   once, it does not stop the count. Shown with the promise already
   durable (`a_newer_slots_owner_heard_once_then_silent_is_taken_over`) and
   in the state D-98 A changes: the owner dies while the promise is still
   pending, and once it lands the node takes over
   (`a_newer_slots_owner_dying_while_the_promise_is_pending_is_taken_over`); every
   fault cycle of the cluster sweep crashes or cuts off its sequencer, and
   none stalls.

The comparison is `a_slow_disk_adds_no_takeovers_at_the_standard_timing`:
- it fails at +10% takeovers over the baseline, if the slowed replica wins
  more than it does on a fast disk, or if commit stops after a detach;
- before D-98 A it fails at +57% (20 seeds);
- it is ignored in the default debug run (four minutes) and takes 9 s in
  release: `cargo test --release -p sim --test cluster -- --ignored
  a_slow_disk`.

The cluster sweep after D-98 A, 10 000 seeds, every schedule shifted by it:
17 624 cycles recovered, 0 stalled, 490 on the state-copy path (486
committed once the node cut off came back); within 2 000 events 98.5%,
96.3%, 90.5% (98.5%, 96.3%, 90.4% before). The D-92 classification above
was made before D-98 A.

**Step 7a (D-78 A, D-102 A–D-105 A), 2026-10-10.** The server can start a
node on the new path. Experimental and off by default, not production-ready:
flow control is off (`lag_bound: None`), timing defaults are placeholders,
nothing is measured on Linux.
- **The switch** (D-103 A): `STRAUM_EXPERIMENTAL_CLUSTER=1`.
  - `server` depends on `node` directly.
  - `runtime`, the frozen old path, may not reach `node`, and `node` may not
    reach `runtime`; `check-boundaries.sh` enforces both, so `server` alone
    joins the two paths.
  - In this mode the broker is not started.
  - D-78 A's precondition, D-72 settled, holds: D-80 B replaced the rate
    criterion with liveness per fault cycle, made concrete by D-81 A and
    D-82 A (now under the D-85 contract).
- **Configuration** (D-104 A):
  - variables: `STRAUM_CLUSTER_NODE_ID`, `_PEERS` (`id=address,…`, every
    voter), `_LISTEN` (default: this node's address in the peers),
    `_MQTT_LISTEN`, `_DATA_DIR`, `_HEARTBEAT_MS` (50), `_TAKEOVER_TICKS` (3),
    `_RETAIN_BYTES` (64 MiB, D-91 A), and `STRAUM_READ_BYTES` (64 KiB,
    D-83 A); `max_uncommitted` 64 (D-82 A);
  - any other `STRAUM_*` variable is refused as unused by this path, never
    ignored;
  - the internal sizes are fixed and untuned (`node::experimental`):
    batches of 16, packets up to 256 KiB, frames up to 8 MiB.
- **Refused before anything opens** (`NodeConfig::check`, run by the server
  and by `Node::start`):
  - fewer than three voters, or an even count; a voter listed twice; this
    node not among them;
  - a heartbeat outside 1 ms–10 s, a takeover after 0 heartbeats (the
    server also bounds the operator's setting to 1–1000; tests may wait
    longer), `max_uncommitted` of 0;
  - `read_bytes` above half the frame — this replaces the silent clamp
    (D-83 A);
  - a largest batch that does not fit one frame.
- **Consistency across nodes:** HELLO now carries
  `[ version u16 ][ node u8 ][ slot u64 ][ config u64 ]`.
  - `config` is `node::cluster_digest`: the voters, the first slot and its
    owner, heartbeat, `takeover_ticks`, `max_uncommitted`, `window`,
    `max_batch`, `max_frame`, flow control, and the apply limits.
  - Left out as local to each node: `retain_bytes` and `read_bytes` (how
    much one node keeps and reads), addresses, and the data directory.
  - A link with another version or digest is refused.
  - A data directory keeps the digest it was first used with (its
    `cluster` file), and a node started over another cluster's data is
    refused before it opens its log.
- **Refused at CONNECT** (`gateway`): a will, a 3.1.1 CleanSession=0, an
  MQTT 5 session expiry. Answered `ImplementationSpecificError`, or
  "server unavailable" in 3.1.1. Message expiry stays carried, not enforced
  (D-65).
- **Operator handoff** (D-105 A): SIGUSR1 calls `Node::hand_off`.
  - It is a protocol handoff: the queue, then `SlotEnd`; the sequencer
    steps down once that is committed and the successor holds it.
  - A node that is not sequencing logs the refusal. SIGTERM and Ctrl-C stop
    the node.

Checked:
- **Tests:**
  - node: `a_link_from_another_frame_version_is_refused`,
    `a_data_directory_of_another_cluster_is_refused`,
    `an_operator_handoff_moves_the_slot_and_every_node_keeps_serving`, and
    `config.rs` (every startup rule);
  - gateway: `wills_persistent_sessions_and_expiry_are_refused_at_connect`;
  - server: the switch, a missing setting, an old-path setting, and invalid
    voters, timing and read sizes, each refused.
- **A manual smoke run,** three server processes on loopback:
  - a QoS 1 publish on node 2 reached a subscriber on node 0;
  - SIGUSR1 handed the slot over from node 0, and the other two logged
    that they do not sequence;
  - a publish on node 1 then reached node 2;
  - a will and a CleanSession=0 CONNECT were refused (return code 3);
  - SIGTERM stopped all three cleanly.

**Since 2026-10-11 (D-106, D-107, D-108):**
- **End to end:** `crates/server/tests/cluster.rs` starts three real
  server processes and checks: a QoS 1 publish acknowledged and delivered
  across nodes; a will and a CleanSession=0 refused; PUBACK waits for the
  commit — with the sequencer (node 0) alive and both replicas stopped, a
  publish is neither acknowledged nor delivered, and once one replica
  restarts over its data the same publish, on the same connection, is
  acknowledged and delivered (a gateway that acknowledged on receipt fails
  it); the SIGUSR1 handoff and publishing after it; a node refused over another cluster's
  data, and its links refused from fresh data; a clean rejoin and clean
  stops. About 11 s at first; about 7 s since D-109 B removed the resend.
- **Found by it:** the first publish after a handoff is dropped, not held,
  every time — a proposal made while the slot changes hands goes nowhere
  (ADR-0008, "Not yet in step 4"). No PUBACK comes for it; the gateway
  closes the connection after `commit_timeout` (30 s), and an
  at-least-once client resends. Fixed by D-109 B: the test now requires
  the first sending to be acknowledged.
- **D-108, the unstable start:** not reproduced. The failure was one run of
  the container's node tests, two tests failing at a node's start with no
  error text kept. Since then:
  - the tests report `Node::start`'s error;
  - 24 container runs and every host run passed, so the cause is not
    established;
  - one candidate is removed anyway: the helper bound port 0 and released
    it before the node bound it, so a test running in parallel could be
    handed the same port. Each test binary now takes ports from a block of
    its own below the ephemeral range, numbered by a counter. Nothing
    retries: a port taken by anything else still fails the start, with its
    error.
- **Exposure:** the MQTT listener defaults to loopback; any address beyond
  loopback — MQTT, links or peers — is refused at startup unless
  `STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1` says so on purpose. Acceptable
  use until credentials and TLS exist (D-110): an isolated, trusted
  network.

Not yet:
- TLS and credentials on the new path (D-110);
- metrics;
- step 7c (sessions, expiry and wills through the log);
- any Linux measurement.

**Step 8 (D-111), 2026-10-11: the old path is removed — early.** At the
user's request, to end conflicts in decision-making, before its conditions
held. D-51 A and D-75 had set three:
- a witness for the two-node `quorum` pair;
- D-28 C1–C4 carried into new-path invariants;
- a durability benchmark comparable with the old path's.
None held, and all three are overridden.

Removed:
- the crates `broker`, `protocol`, `mqtt`, `replication`, `runtime` and
  `e2e`;
- the old leader/follower simulation (`sim`'s library, `sim/tests/seeds.rs`)
  and the D-28 chain check (`sim/tests/chain.rs`);
- the fuzz targets on `mqtt`;
- `scripts/compare-brokers.sh` and `scripts/crash-survival.sh`;
- the broker half of `server`.

Kept: `storage` (its `Wal` is each segment of the new log), `transport`,
`domain`, and `auth`, for D-110.

The server now runs only the cluster path. It refuses to start without
`STRAUM_EXPERIMENTAL_CLUSTER=1`: the switch is the operator's acknowledgment
that the path is experimental, not a security mechanism (D-107).

Lost with the old path, until rebuilt on the new one:
- a single-node mode — the new path needs three voters (D-104 A);
- client TLS and credentials (D-110);
- metrics (`STRAUM_METRICS_ADDR`);
- kernel probes at startup;
- ADR-0006's durability levels (`memory`, `written`, `fsynced`, `quorum`)
  — the new path acknowledges once a majority holds a record durably;
- the old benchmarks and their baselines, kept in `docs/` as history.

The docs that described only the old path carry a "History" banner. The
code is in git history (commit ffeda02 and earlier).

**D-109 B (2026-10-11): a node holds its proposals across a slot change.**
Before, a proposal made while the slot changed hands — at a node with no
sequencer known, or forwarded to one that had just sealed or died — was
dropped; its publisher waited `commit_timeout` (30 s). Now each node keeps
its own proposals in an outbox (`cluster/src/outbox.rs` since D-117 B) until it applies
their records from the committed log:
- **Confirmation:** an entry leaves when the identical record (kind,
  properties, body) is applied. Every gateway proposal names its node and
  session, and a publish or a filter its request, so another node's record
  never matches one.
- **Deduplication:** an entry is sent again only once a record of a later
  slot has committed. The committed log is a prefix that never changes, so
  the slot it went to can no longer gain a record: a resend never commits a
  second copy. A client's own resend is a new proposal, as at-least-once
  allows.
- **Order and fencing:** entries go strictly in order, and none while an
  earlier one waits for its slot to be decided. Forwarding waits until the
  owner's slot has a committed record — before that the owner may not
  sequence yet. A sequencer takes its own entries only into a slot the
  committed log has not passed. The protocol's fencing is unchanged: the
  outbox only decides what to send, and when.
- **Losses it still has:** a proposal a full queue refused, or a link lost
  in its tail, while a later one of the same node committed in the same
  slot. Sending it again would put it behind the later one, so it is given
  up and logged; its publisher is closed by `commit_timeout`, as before.
- **Bound:** `cluster::Config::max_queue` entries per node, the sequencer's
  own queue bound; a proposal beyond it is refused. One copy of each
  proposal per send, not measured (D-112).
- **What it does not drop:** an entry stays when `commit_timeout` closes
  its publisher, so it may still commit after the client has resent it as
  a new proposal — a duplicate at-least-once allows. If the client reconnected
  elsewhere, that old copy may commit after the client's newer messages;
  per-session order holds, since the resend comes from a new session. The gateway's
  lifecycle retries (`NodeReset`, session gone, delivery acks, every
  `retry_interval`) each add an entry too; they are idempotent in `apply`,
  and the bound caps how many wait.
- **Deviation (unified-stream §1, §7), closed by D-117 B:** the outbox
  first lived in the tokio driver (`node`), which no deterministic
  simulation runs.
- **Closed narrowly (D-119 A):** resolved for the reported handoff stall.
  Closing it does not show fencing, proposal loss, memory bounds or every
  takeover case correct; D-117 B below is what the simulation shows.

Checked:
- unit tests of the outbox: confirmation, resend only after the slot
  closed and in order, a later commit showing earlier ones lost, the bound;
- `a_handoff_loses_no_proposal_and_commits_each_once`: 200 proposals on
  each of three nodes, the handoff between the halves; each commits once,
  in its node's order, and no record of an older slot commits after a newer
  one's;
- `a_sequencer_failure_loses_no_proposal_the_others_hold`: the sequencer
  stopped between the halves — in five runs it had 144–172 records
  appended past its commit as it stopped;
  the same checks, and the old sequencer restarted converges on that log;
- both fail with the resend replaced by the old drop (checked by mutation:
  proposals 34–140 of one node were lost);
- the server end-to-end test now requires the first publish after the
  handoff to be acknowledged without a resend.

**D-117 B (2026-10-12): the outbox in `cluster`, driven by the takeover
simulation.** The outbox moved from `node` into `cluster`
(`cluster/src/outbox.rs`, owned by `Voter`), so the simulation runs the
code a node runs. A node's own proposals go through `Voter::submit`; a
forward is a protocol message, `Message::Forward { slot, proposal }`,
sent as any other; each applied record goes back through
`Voter::applied`, which hands back the proposals it shows lost (D-115 A:
never dropped unseen; `node::Status` counts them, `given_up`, and the
outbox's refusals, `refused`). The node only carries the effects out.

- **Found in the move:** a forward carried no slot. One still in flight
  after its node gave it up, or sent it again, could reach the same owner
  leading a later slot and be taken there — committing after it was given
  up, or twice. A sequencer now takes a forward only into the slot it was
  sent for; the frame carries it (`WIRE_VERSION` 2). The simulation
  catches the old rule at seed 14165
  (`a_forward_for_an_older_slot_is_not_taken_into_a_later_one`).
- **Invariant 7 in `sim/tests/takeover.rs`:** every node proposes through
  its outbox; forwards cross the same network as the protocol — dropped,
  cut off by partitions, sent to a fenced owner still running, lost with
  a crash — in order per link, as TCP delivers. Checked at every commit:
  a proposal commits at most once; one node incarnation's proposals
  commit in order; a proposal given up never commits. At the end, with no
  new proposals, each is committed, given up, refused or orphaned by its
  node's crash — or stranded, below.
- **Swept:** 20 000 seeds pass. In 2 000 seeds: 1 187 814 own proposals,
  124 549 committed in a later slot than their node had promised when it
  made them, 1 948 given up, 16 741 stranded.
- **Mutations it catches:** sending again before the old slot is closed
  (a duplicate commit, seed 11); taking a forward into another slot (seed
  14165). The loopback tests of D-109 B still catch the old drop.
- **Found by the simulation, not fixed (D-120):** a proposal is stranded
  when its forward is lost — the link, or a sequencer's queue full or
  sealed — and nothing after it commits in that slot while the slot stays
  open. Nothing then shows it lost, and nothing may send it again; it
  waits until a later proposal of its node commits (then it is given up)
  or the slot changes (then it is sent again). Its publisher is closed by
  `commit_timeout`, and the client's resend is what unblocks it. Every
  stranded proposal in the sweep had a forward lost; any other wait fails
  the run.
- **Found by the simulation, not fixed (D-120):** after a slot change
  every node sends its whole outbox again at once; the sequencer's queue
  (`max_queue`) is as large as one outbox, so the burst overflows it and
  the overflow is lost as above.

**D-116 A, D-118 A (2026-10-12): failing tests keep what explains them.**
The node tests' `Cluster` lets its nodes go and removes their data on
drop; the server test kills its processes on drop and, on a failure,
prints every node's log from every life and keeps the directory. The
first failure they caught: the server test's SIGTERM reached the node
started from another cluster's data before its handler was installed
(fixed: the test waits for "node started"). The second: the D-118 failure
again — a restarted node 1 took the slot over 0.66 s after it started,
while node 0 was alive and sequencing. The server test at a6986ea fails
the same assertion in D-113's loop (its own logs show only the symptom:
it predates D-118 A), so it predates D-109; the likely mechanism
is the link redial backoff (up to 1 s) outlasting the takeover window
(150 ms), not yet measured (D-122).

**D-113 A (2026-10-12): D-108 closes as not reproduced.** 100 container
runs of the node and server tests on the current tree, and 100 runs of the
server test at a6986ea (before D-109) beside them, every run's output kept
(in `<tmp>/d113/{current,base}/run-*.log`, temporary, not in the repository), nothing retried:
- current tree: 94 passed; the 6 failures are all the server test at its
  handoff — node 0 no longer the sequencer when SIGUSR1 comes (D-122);
- a6986ea: 95 passed; the 5 failures are the same assertion;
- the node tests failed in none of the 100 runs, and no node failed to
  start in any of the 200: D-108's failure did not recur after the port
  helper's fix.
The handoff failure comes at about the same rate on both trees, so it is
not a D-109 regression: D-118's question is answered, and its likely
cause is D-122.

**D-122 A, D-120 A, D-121 A (2026-10-12).**
- **D-122 A, the redial backoff:** a link to a down peer is redialled
  with a backoff that never exceeds one heartbeat (it was 1 s). A peer
  that restarts now hears the sequencer within a heartbeat, inside the
  `takeover_ticks` of silence after which it would take a live owner's
  slot. `a_restarted_replica_leaves_the_slot_with_its_live_owner` (a
  replica down 3 s, then restarted; the slot must stay with its owner)
  failed 6 runs of 6 before the change and passes 8 of 8 after it. The
  protocol is unchanged — fencing, promises and the committed prefix
  are not touched; a takeover of an owner that is really gone is still
  checked by `another_node_takes_the_slot_over_when_the_sequencer_stops`
  and by invariant 6 in every simulation. The cost: one failed connect
  per heartbeat toward a peer that is down.
  Two more tests put a delaying proxy on the sequencer's link to the
  restarted node. A connection a heartbeat late leaves the slot with its
  owner. A connection a second late lets the restarted node take the
  next slot (or node 2, if the turn passes), and the old owner, still
  running, is fenced: nothing of its slot commits after the new slot
  began, it stops sequencing, and a publish proposed at it commits once,
  through the new owner. The proxy accepts at once, so these test
  tolerance of a late connection, not the backoff; the restart test is
  the backoff's regression test (it fails 3 of 3 with the 1 s cap).
- **D-120 A, each node's share of the queue:** a node's outbox holds at
  most `max_queue / voters`, rounded down and never 0 (341 of 1 024 with
  three voters), so every outbox resent at once after a slot change fits
  the sequencer's queue. A full outbox is backpressure: the node gives no
  read credit to any MQTT client until it has room; a proposal made with
  no read behind it (a lifecycle retry) is refused and counted. In 2 000
  seeds of the takeover simulation: 16 741 stranded and 1 948 given up
  with the whole `max_queue` per node; 0 stranded and 7 given up with the
  share (the simulation offers proposals faster than any outbox drains, so
  its refusals at a full outbox — 829 715 — have no read backpressure in
  front of them as a node does). A seed with give-ups is pinned
  (`a_proposal_shown_lost_is_given_up_and_never_commits`).
  A queue below one proposal per voter is refused at startup, so the
  shares always fit. While the outbox is full the node reads none of its
  clients, keep-alives included: a client idle through a long full spell
  can be closed for silence it did not cause (D-123).
- **D-121 A:** the storage and transport tests remove the files and
  directories they write when they end, passing or failing; a run used
  to leave about 20 of them.
- **Under load, after D-122 A:** the D-113 setup again — 100 container
  runs of the node and server tests beside 100 of the server test as load
  — passed 200 of 200. The same setup failed the handoff 6 times in 100
  before the fix. As in D-113, the load loop is the faster one and ended
  first, so the last runs of the other had no load beside them.

**D-112 A (2026-10-12): metrics on the new path.** The node loop measures
(`node/src/metrics.rs`), the cores count (`cluster::VoterStats`,
`gateway::Counted`), and a snapshot is rendered as Prometheus text only
when asked — `Node::metrics`, or HTTP on `STRAUM_CLUSTER_METRICS_LISTEN`,
off by default and loopback unless an unauthenticated network is
acknowledged (D-107). Owned by one task: no atomics, no allocation per
event, monotonic time, nothing read back into a decision.
- **Commit and durability:** log end, durable, commit, applied, the
  uncommitted tail; histograms of a node's own proposal from accepted to
  applied (`straum_proposal_commit_seconds`), of a batch from appended to
  durable here (`straum_append_durable_seconds`) and to known committed
  (`straum_append_commit_seconds`). The three split a PUBACK's delay: a
  slow commit with a fast local disk is the quorum, not the disk.
- **Proposal states, never mixed:** refused at a full outbox (never
  accepted), accepted and awaiting commit (`straum_outbox_entries`),
  committed, given up — counted separately; the gateway's own lifecycle
  proposals go through the same outbox and are counted with them.
- **Outbox:** entries, sent, bytes, capacity, resends after a slot closed;
  losses by name — forwards a sequencer's full or sealed queue refused,
  forwards dropped at a full link queue (with every link drop counted),
  and the give-ups they lead to.
- **Replication:** each replica's durable lsn and its lag behind the
  commit point, as the sequencer last heard; a catch-up rate is the
  scraper's derivative of it.
- **Takeover:** takeovers by silence started, completed (timed:
  `straum_takeover_seconds`) and abandoned; successions by `SlotEnd`;
  slots led; times stepped down (handed over or fenced, not told apart).
- **MQTT:** connections, pending, waiting bytes; publishes proposed,
  PUBACKs, deliveries, dropped at-most-once deliveries; closes, and of
  them for keep-alive and for `commit_timeout`.
- **D-123:** spells of withheld reads — count, total, the longest (the
  open one included) — beside the keep-alive closes.
- **The HTTP listener:** at most 4 scrapes at once, a permit taken before
  the accept, read and write each bounded by 2 s — a burst of silent
  connections waits in the kernel's backlog and is closed, never held
  (tested). An addition: D-112 asked for metrics, not for a listener
  (D-126).
- **Not yet:** a handoff by `SlotEnd` is counted but not timed, and
  stepping down does not tell a handover from fencing (D-125). The
  overhead of measuring is not yet measured on a quiet Linux host (D-124,
  D-127): an `Instant::now()` per
  own proposal, per append, per durable report and per loop pass, and
  two outbox reads per applied record.
- **D-124, preliminary:** `scripts/overhead.sh` (071db00 against this
  change, five alternating rounds, `cap` and the new MQTT benchmark
  `server/benches/puback.rs`) in the Linux container on the development
  Mac: every median within 3.3 % — throughput −1.0 % and +0.6 %, PUBACK
  p50 +3.3 %, server CPU per PUBACK +1.0 % — inside that machine's 10 %
  noise floor (`docs/baselines/apple-m1-dev.md`). Not accepted until a
  quiet Linux host repeats it.
- **The accepted run (D-127 A):** `scripts/linux-run.sh` on a bare-metal
  Linux host — the host recorded, its noise, the overhead, the cost of
  scraping, and a diagnostic run. Known quirk in the diagnostics: the first
  owner starting slot 1 at startup counts as a takeover by silence
  (D-125 separates the kinds).
  Medians come with their spread (min and max between rounds). If the
  noise run's spread is near the effect sought, the run stops there
  (`STRAUM_STOP_AFTER_NOISE=1`) and steadier conditions are found first;
  the threshold is D-129. The diagnostic run was steady load only, with no
  handoff and no takeover to time apart — until D-128, below.
- **D-128 A, D-129 A (2026-10-13):** the diagnostic run now injects a
  planned handoff (SIGUSR1 to the sequencer) and a killed sequencer
  (SIGKILL, started again 2 s later), and reports each apart — the longest
  wait for a PUBACK, for the event node's clients and for the others' —
  with every publish accounted: acknowledged (late past 1 s, or not),
  retransmitted after a reconnect, unacknowledged after a 15 s drain, and
  duplicate acknowledgements counted apart, never as successes. The noise
  run gates the host by D-129 (`scripts/noise.awk`): difference
  `|median_A − median_B| / median_A` at most 3 %, each copy's spread
  `(max − min) / median` below 3 %, on every quantity; each overhead is
  then read against the smallest effect that host can detect. Read by that
  rule, the earlier container comparison was too noisy to say anything
  (spreads 2–29 %; failover p50 bimodal, D-130).
- **Found by the event run (D-128), in the container, preliminary:** a
  handoff paused every client about 150 ms; a killed sequencer paused the
  surviving nodes' clients 150–260 ms (the takeover). The killed node,
  started again 2 s later, served its clients only after catching up — and
  under about 80 000 publishes/s it could not: its clients gave up after
  30 s in 5 of 6 runs, with nothing duplicated. With the load stopped its
  commit moved 62 143, 156 865 and 68 499 positions in 10 s — a catch-up
  of about 6 000–16 000 a second, far below the write rate. Likely, not
  confirmed: one read of `read_bytes` (64 KiB) per heartbeat (50 ms) —
  about 13 000 records a second (D-131).
- **D-130 A:** failover is a distribution, not gated: `cap.rs` reports the
  share of failovers within 1 s, p50 and p90 (50 failovers a round in
  `scripts/overhead.sh`), and diagnoses the slow mode apart — slots the
  cluster moved on (more than one: a candidate's turn passed), takeovers
  abandoned, the survivors' lag at the stop. `scripts/noise.awk` reports
  failover and gates the rest; p90's spread between rounds shows when 50
  is too few. The baseline build prints share and p50, not p90.
- **D-133 A (container, 3 rounds per size, preliminary):** medians, with
  per-round ranges in `docs/baselines/apple-m1-dev.md`. Lag rate under
  load −3 555 / −4 494 / −3 467 positions a second at 64 KiB / 512 KiB /
  4 MiB, ranges overlapping; recovery 9 453 / 6 910 / 6 850 a second;
  PUBACK p50 0.72 / 0.68 / 0.74 ms, p99 18.6 / 17.4 / 19.3 ms; commit p99
  on the running nodes 16–33 ms (bucket bounds); CPU of all three nodes
  29.0 / 29.1 / 29.0 s over the 27.9 s window. Read size still shows no
  effect. What the CPU shows, in every run: the sequencer used one full
  core (about 28.2 s of 27.9 s) while the catching-up replica used about
  0.3 s and the third node 0.5 s — the replica is nearly idle, the
  sequencer's one node loop saturated. Likely, not proven: catch-up is
  limited at the sequencer, by its single loop serving both the load and
  the catch-up. One outlier: 512 KiB round 1 wrote 335 MB on node 0 and
  recovered at 0 — reported, not explained.
- **D-133 B (in-process `cap.rs` catch-up mode, container, 3 rounds,
  preliminary):** a replica stopped 2 s under load and started again, its
  own lag read every second. The window is the limiter: at 256 (7a's
  value) the lag shrank by about 2 050 positions a second under load and
  recovered at about 6 400 a second after it — near what `puback` saw
  through the server; at 1 024, about 25 000 under load and 30 000 after;
  at 4 096, about 31 000 under load, caught up before the load stopped. The
  three rounds agree within a few percent. Commit latency at the sequencer
  showed no change at any window (p50 at the 1.024 ms bucket, p99 at the
  131 ms bucket — coarse bounds). Why 4× the window gave about 12× the rate
  is not explained. A different harness from `puback.rs` (no MQTT, a stop
  by `shutdown`). Raising the window is a behaviour change and has a memory
  cost (D-135); D-131 B does not address it and stays deferred.
- **D-135 A (in-process, container, 3 rounds × windows 256 / 1 024 /
  4 096 × heartbeats 25 / 50 / 100 ms, preliminary; D-135 stays open):**
  - *Supported (preliminary):*
    - At windows 256 and 1 024 the heartbeat does not pace catch-up:
      positions per heartbeat scale with its length (256: about 145 /
      280–370 / 555–720) while the rate per second does not (under load
      −1 820 to −2 397 a second at 256, −22 708 to −27 682 at 1 024, at
      every heartbeat); heartbeats with a lag and no movement were
      0.2–7 %. D-131 B is not what limits it there.
    - At those windows catch-up fits window ÷ an effective round trip of
      about 40 ms: 256 → about 6 400 a second, observed 5 600–7 300 after
      the load; 1 024 → about 25 600, observed about 30 000 in the runs
      that had not drained (the others' figure is lag at stop ÷ 10, a
      lower bound). Under load the lag changes by that rate less the
      cluster's write rate, about 4 500 here; the "12×" of D-133 B is
      that difference of a difference, not a superlinear window.
    - Bytes in flight to a replica, the most seen, resends included:
      0.13–0.45 MB (256), 0.6–1.8 MB (1 024), 2.7–6.9 MB (4 096);
      positions in flight on average 19–48, 113–227, 100–750 — the window
      is not used in full on average.
    - The outlier of D-133 A, from its data: the one run where the
      restarted node caught up inside the window (lag 0 at about 49 s);
      its clients came back and the cluster ran at full load — about
      200 000 positions a second — for the rest, hence 335 MB written and
      more CPU; its "recovery 0" meant nothing left to recover (now
      reported as `caught_up`).
  - *Not established:*
    - Where the 40 ms round trip comes from.
    - What the window costs in commit latency: p99 67–109 ms in 26 of 27
      trials and about 100 ms at every window, with no control trial
      without a lagging replica, so the window's share cannot be
      separated; one trial (256, 25 ms, first round) 166 ms, p99.9 276,
      max 318; p99.9 and max of about 200–250 ms at 4 096 with a 25 ms
      heartbeat in two of three trials, against about 110 elsewhere.
    - Heartbeat pacing at 4 096: positions per heartbeat did not scale
      (about 1 800–4 000 at every heartbeat), the gain flattened (about
      −25 000 to −30 000 under load; perhaps the sequencer's full core of
      D-133 A), and the 100 ms runs did not catch up under load (lag at
      stop about 200 000) while the 25 and 50 ms runs did.
  - *Open:*
    - The sequencer's commit p50 is bimodal across trials — about 1 ms in
      19, 55–85 ms in 8: every trial at 25 ms with windows 256 and 1 024,
      two at 50 ms, none at 4 096 or at 100 ms.
    - In D-133 A, catch-up quickened once the lag fell below about
      90 000.
  - *The compared tree moved:* the instrumentation behind these numbers
    (`node::metrics::Progress` and `Flight`, the per-send byte count in
    the node loop) is in the `node` loop D-124 compares; its cost is not
    measured. Whether D-124's candidate includes it is D-137.
- **D-136 A (2026-10-16; in-process, container, 3 rounds × windows 256 /
  1 024 / 4 096 at 50 ms, each trial with a control and no replica
  restarted; preliminary):** the stages of catch-up, stamped at each
  boundary on the three nodes (`node::metrics::Trace`, on only while a
  run asks — `Node::trace_start` / `trace_take`) and joined by position
  in `cap`. Off, the node loop reads no clock for it (one branch a stamp
  site, one match a sent message); the log thread reads the clock three
  times a group commit and twice a catch-up read, always.
  - *Supported (preliminary):*
    - Catch-up is no round trip: it is one read from the log at a time.
      The sequencer asks the next read as soon as it ships the previous
      one (`next_ask_K` about 0 ms); a read neither waits behind the log
      thread (`read_queue_W` about 0.04 ms) nor for the replica's report.
    - The read is the stage: 14–25 ms each (`read_K`; the read cycle the
      same plus about 0.1 ms).
    - What a read brings: whole batches from the replica's next position
      up to its durable position plus the window — 96 positions at 256,
      about 410–440 at 1 024, about 454 at 4 096 (there `cap`'s 64 KiB
      `read_bytes` bounds it). The "40 ms round trip" of D-135 A is not a
      stage; the window acts through what fits in one read.
    - The cost of catch-up, against the control: the sequencer's commit
      p99 1.5–2.4 ms with no replica restarted (6.8 in one trial), 52–170
      ms during catch-up; commit p50 1.0–2.0 ms in every control, and 73–116
      ms in three catch-up trials. The window is not what costs:
      the commit tail goes with catch-up at every window.
      The control's stage rows filled the trace (2^19 stamps a node; 0.4–1.1
      million dropped) early in its window, so they are matched to the
      catch-up rows in setup, not in time; its commit samples cover the
      whole window.
  - *Not established:*
    - Why a read costs 14–25 ms. The code says `Segments::read` replays
      the segment from its start through `ReadBatches`, whatever the read
      needs — up to `segment_bytes` (64 MiB, also in 7a) a read; that is
      read from the code, not measured (`read_K` was not set against the
      position in the segment).
    - The rate model, positions per read ÷ read time: it fits window 256
      (96 ÷ about 15 ms ≈ 6 400 a second; 6 446–6 563 measured after the
      load) and round 3 at 1 024 (27 040). Rounds 1 and 2 at 1 024 do not
      test it: their lag drained within the 10 s after the load (lag at
      stop 39 195 and 151 320), so their 3 920 and 15 132 are lag at stop
      ÷ 10, lower bounds.
    - That the commit tail is the catch-up reads holding the log thread,
      which they share with the live group commits: likely, as nothing
      else differs from the control, but no commit is stamped against a
      read.
    - The replica's appends waiting 10–20 ms at p90 for its log thread
      (`append_queue_W`) during catch-up and not in the control: what
      holds that thread is not stamped (a read-back for apply is a
      candidate).
    - The sync: about 0 ms in this container, so a real disk's fsync is
      not represented in any stage.
  - *Open:* D-135. The stage is measured — the sequencer's serial read
    from the log, with no queueing and no wait for the report — but the
    cause inside it is read from the code; D-135 stays open until that is
    confirmed (D-139).
- **D-139 A (2026-10-16; the D-136 A run again, 3 rounds × 256 / 1 024
  / 4 096 at 50 ms with paired controls; container, preliminary):** each
  catch-up read timed where it reached its start and where it was
  complete, with the records and bytes scanned before and after
  (`wal::Scan`, counted on every read: two adds and a branch a record);
  the replica's read-backs for apply (`Range`) timed and counted; the
  replica's waiting appends attributed by overlap to what its log thread
  was doing.
  - *Supported (preliminary):*
    - A read's time is the segment it replays, not what it returns. The
      part kept takes 0.01–0.04 ms; reaching the read's start takes 5–17
      ms over 16–62 MB, and the replay goes on to the segment's end for
      another 5–15 ms over 20–53 MB — 55–63 MB scanned a read, about the
      whole 64 MiB segment, at 3.4–4.7 GB/s. Where the bytes scanned
      vary (1 024, 4 096), read time follows them: r 0.91–0.997, about
      0.29–0.54 ms a MB. This explains the 14–25 ms of D-136 A: what a
      read needs is under 0.3 % of it.
    - The replica's log thread is held by its read-backs for apply: 99 %
      of its appends' waiting during catch-up (5.1–5.6 s a trial)
      overlaps them. A read-back brings 1 024 records (`MAX_RANGE`) and
      visits 55–61 MB doing it — the same replay from the segment's
      start — 17–21 ms each, 520–620 a trial. In the controls there are
      none, and an append waits about 0.04 ms.
  - *Not established:* the cost on a cold disk — the segments were in the
    page cache — and the sync's, about 0 in this container.
  - *Closes D-135:* the 14–25 ms read cost is measured — each read
    replays its segment from the start and on to its end. The catch-up
    rate is bounded by that serial read together with the replica's wait
    on its own read-backs (one a read, p90 13–16 ms); which of the two
    dominates is not separated yet. The optimisation is a card (D-142).
- **D-142 A (2026-10-16):** catch-up reads and read-backs stop once they
  have what they need, and sequential ones resume where the last left
  off.
  - *What changed:*
    - `storage`: `scan` takes a resume offset and a per-record check
      that can stop the walk; `Segments::read_while` hands records over
      with their end offsets until told to stop, from the segment's start
      or from such an offset. A resumed walk numbers nothing (sequence
      `None`).
    - `node::wal`: `read_batches` and `read_range` stop once `ReadBatches`
      is done, or once a read-back has `MAX_RANGE` records or passed what
      it wants. A cursor per replica being caught up and one for apply's
      read-back — a segment, an end offset, the next position — start a
      read that continues there. A cursor read that finds nothing is
      tried again from the segment's start. At most voters + 1 cursors.
    - D-139 A's always-on per-read counting (`wal::Scan`) is removed;
      read-backs keep their two clock reads, like reads (D-136 A).
  - *Unified-stream §3*, amended by the user (2026-10-16) to say what it
    meant: a reader that can be in progress during a truncation or
    compaction holds a pin; reads serialised with them on the log thread
    need none. These reads — the only readers of a live log — are such:
    every read, read-back, truncation and compaction runs on the one log
    thread, one after another, so no read is in progress while a segment
    is cut or removed. A cursor is a position, not an open reader: it
    holds no file and pins nothing, and truncation and compaction drop
    every cursor before they change a segment. A read from a cursor that
    no longer matches falls back to the scan (tested: a stale cursor
    across a cut and a rewrite gives the scan's answer). The refcount
    `Segments` already has (`pin`) stays for readers that span commands —
    the simulation's — and is not needed here. A reader on another
    thread (mmap, a separate reader, §3's "likely candidates") would need
    it; none exists.
  - *Measured to protocol (rules.md §11; in-process, container,
    preliminary): base (D-139's path) against both, five alternating
    rounds, windows 256 / 1 024 / 4 096 with paired controls, and cap's
    failover mode; medians over the rounds:*

    | | base | both |
    |---|---|---|
    | read p50 (256 / 1 024 / 4 096) | 13.8 / 16.6 / 16.1 ms | 0.02 / 0.03 / 0.04 ms |
    | catch-up commit p99 | 89 / 103 / 105 ms | 1.8 / 1.9 / 2.0 ms |
    | commits a second in catch-up | 6 400 / 9 600 / 12 200 | 311 000 / 310 000 / 308 000 |
    | failovers within 1 s (cap 64, 10 a round) | 4 of 10, every round | 10 of 10, every round |
    | survivor lag at the stop | about 80 000 | 0 |

    Controls without a restart: commit p99 1.4–3.2 ms, 306 000–312 000
    commits a second, both trees; publishes a second in the failover
    mode the same in both (about 310 000–327 000). With both, the
    restarted replica caught up under load in every trial — at 1 024 and
    4 096 before the first sample, one second in — so those catch-up
    figures compare against a replica with nothing left to read; window
    256 is the comparison that holds. The failover result was not looked
    for: base's survivors lagged about 80 000 positions behind the
    sequencer, slow catch-up reads that kept them there, and the new
    majority had to close that before committing; with both they lag 0,
    and D-84 A's target (9 in 10 within 1 s) holds in this harness.
  - *Each change alone (three interleaved rounds, 256 and 1 024, before the
    protocol run):*

    | | read p50 | catch-up commit p99 | commits a second in catch-up |
    |---|---|---|---|
    | base (D-139's path) | 13–16 ms | 80–96 ms | 6 000–11 400 |
    | early stop | 7–9 ms | 60–90 ms | 16 000–59 000 |
    | cursor | 4–10 ms | 50–82 ms | 8 000–142 000 |
    | both | 0.02–0.03 ms | 1.4–1.9 ms | 313 000–324 000 |

    Controls without a restart: commit p99 1.3–1.9 ms and about 315 000
    commits a second in every tree. With both, the lagging replica caught
    up under load in every trial (at 1 024 before the first sample, one
    second in); its appends waited 0.04 ms at p50 and a read-back took
    0.07 ms, against 11–14 ms and 15–18 ms in base. Each change alone
    removed one half of the replay and left the other: the early stop the
    part after a read, the cursor the part before it. Alone, each also let
    the closed-loop load commit faster, so the lag could grow at window
    256 while the cluster wrote more.
  - *Not established:* a cold disk (the segments were in the page cache);
    the sync, about 0 in this container; a Linux host (D-138: these
    numbers support the change, not a default).
  - *Open risk:* the single-thread argument is enforced by the code's
    structure, not by `Segments`: a future reader on another thread —
    §3's mmap or a separate reader — must take a pin, and an offset it
    holds across a truncation or compaction is only safe because a stale
    one fails to a scan; a segment removed under such a reader is not.
- **Step 7c design (D-144 A, 2026-10-16) — for agreement; implementation
  not authorised until D-146.** The direction D-144 A accepted, and the four
  definitions it asked for before any code. Revised after the guardian
  review: the first fence did not survive an entry's removal, and
  re-proposing a stale `Connect` brought back the late takeover.
  - *State in `apply`:*
    - Per client id: `clients: BTreeMap<ClientId, Client>`, `Client
      { attached: Option<Origin>, connected_at: u64, session:
      Option<Persisted> }`. `connected_at` is the lsn of the last `Connect`
      applied for that client id. An entry exists while a connection is
      attached or a persisted session exists.
    - `origins: BTreeMap<Origin, ClientId>`, the attached origins. Bounded
      by live connections.
    - Clean-session state stays keyed by `Origin`.
    - The fence: tombstones and a floor, below.
  - *Records:* `Connect {origin, client_id, seen, clean_start, persist}` and
    `Disconnect {origin}`. `seen` is the proposing gateway's applied lsn
    when it proposed. The gateway answers CONNECT only from the effect of
    applying the committed `Connect`.
  - **(2) First, the order — which `Connect` wins.**
    - For a client id, a `Connect` committed at lsn `L` applies only if its
      proposer had applied every earlier change to that client id:
      - `L - seen <= W`: proposed within the last `W` positions; `W` is an
        `apply::Limits` field like `max_connect_tombstones`: part of the
        replicated decision, the same on every node, in the configuration
        digest, and listed in `docs/configuration.md` when built;
      - `seen >=` the lsn of the last change to that client id — its
        `connected_at` if an entry exists, otherwise its tombstone's
        removal lsn, otherwise the global floor.
    - Any other `Connect` is refused as stale (`Refused {to, Stale}`) and
      changes nothing. The gateway answers that CONNECT at once with
      CONNACK 0x88 Server unavailable (v5) or 0x03 (3.1.1). There is no
      re-proposal: an old CONNECT packet is never re-armed, and the client
      decides whether to reconnect.
    - So the winner is the last `Connect` to commit among those proposed
      by a gateway that had seen everything before it. A `Connect` proposed
      before a newer one committed never wins.
    - *Tombstones and the floor:*
      - when an entry is removed, a tombstone `(client_id, lsn)` goes into
        a queue of at most `max_connect_tombstones` (a new
        `apply::Limits` field);
      - tombstones older than `W` positions are pruned, since a `Connect`
        that could need them is refused by the `W` rule anyway;
      - when the queue is full, the oldest is pruned and the global floor
        rises to its lsn.

      So a client id with no entry and no tombstone is checked against the
      floor: no `Connect` older than any removal it could have missed
      passes.
    - *Memory:* `max_connect_tombstones` × (a client id of at most 256 bytes
      + an lsn), and one u64. Bounded under any input.
    - *Cost:* a legitimate reconnect through a gateway that has not yet
      applied that client id's last change, or proposed more than `W`
      positions before it commits, is refused once; the client reconnects.
      Under a removal rate the queue cannot hold for `W` positions, the
      floor refuses more connects that way. That is visible (a counter),
      never unsafe. `W` and `max_connect_tombstones` are sized from a
      measurement in stage 2 (D-146).
    - **Invariant B (checked in the simulation after every record):** a
      `Connect` changes a client id's state only if no *fence event* for that
      client id committed after its `seen` — a fence event being exactly what
      the rule checks: an applied `Connect`, or the removal of the entry
      (clean disconnect, overflow deletion, `SessionGone`, `NodeReset`), or a
      rise of the floor. A detachment that keeps a persisted session, and
      deliveries to its backlog, are not fence events. The rule and the
      simulation check use this one definition. It never replaces a newer attachment,
      never clears newer session state, and never recreates a removed one
      it had not seen removed.
  - **(1) Atomicity of `Connect` and `Disconnect`.**
    - Each record is one state transition in `apply`, all or nothing: a
      deterministic function of the record and the state before it, the
      same at the same lsn on every node. A record that does not decode
      stops the node, as now.
    - One origin's records reach the log in the order they were proposed,
      at most once; an earlier one that does not commit is given up, never
      reordered (`cluster::outbox`, D-109 B, D-117 B).
    - The rules:
      - `Connect(O)`, if it passes (2): it attaches `O`. An origin attached
        before it is detached and told `Close {SessionTakenOver}`.
      - `Disconnect(O)` acts only if `O` is attached: it detaches `O`.
        Otherwise it changes nothing, and is counted.
      - `SessionGone(O)` and `NodeReset` detach likewise, and drop clean
        state as now.
    - **A `Disconnect` racing a `Connect` not yet complete.**
      - Same origin: the gateway proposes `Disconnect(O)` only after
        `Connect(O)`, so the log has them in that order. Both commit:
        attached, then detached. `Connect` given up or refused: the
        `Disconnect` finds `O` not attached and changes nothing.
      - Different origins (`Disconnect(O1)` and `Connect(O2)`): log order
        decides, and either order ends with `O2` attached if its `Connect`
        passes (2).
    - A gateway keeps a `Disconnect` for a closed connection in its outbox
      until it applies the detachment, or sees `O` not attached. One per
      closed connection: bounded by the outbox.
    - **Records from an origin that is not attached** — `Publish`,
      `Subscribe`, `Unsubscribe`, `Ack` from a connection taken over or
      never attached — are refused by `apply`: no route, no subscription,
      no acknowledgement. `Refused {to, request}` makes its gateway close
      the connection. A taken-over client served for a while by a lagging
      gateway gets nothing committed in its name. Every connection is
      attached once its `Connect` applies, since every CONNECT goes
      through the log after 7c.
    - **Invariant A (checked in the simulation after every record):** for
      every client id, `attached` is the origin of the last applied
      `Connect` for it that passed (2). That holds unless, after it, that
      origin was detached — by its `Disconnect`, `SessionGone` or
      `NodeReset` — or its session was deleted (overflow under (3)). At most
      one origin is attached per client id, and `origins` is its exact
      inverse.
  - **(3) Overflow of the disconnected budget.**
    - The budget: `max_detached_queued_bytes`, inside
      `max_total_queued_bytes`. It counts the inflight and backlog bytes of
      sessions with no attachment.
    - **Attaching never overflows:** the detached budget is part of the
      total, so moving a session's bytes from detached to attached leaves
      the total unchanged and no bound is crossed. A resumption is always
      CONNACK `session_present = 1`. Detaching moves bytes the other way.
      If the detached budget has no room, the session is deleted as below,
      at once, by the record that detached it.
    - **At-most-once delivery** to a detached session with no room in its
      per-session backlog or in the detached budget: that message alone is
      dropped and counted (`Load::dropped`, as now). The session stays.
    - **At-least-once delivery** with no room, or a detachment the budget
      cannot take: the session is deleted whole.
      - Its subscriptions go, and the global count drops by them.
      - Its inflight (sent, unacknowledged) and its backlog — every waiting
        message, at most once and at least once — are discarded, and their
        bytes leave the budgets.
      - The client id's entry goes and leaves a tombstone, so (2) still
        fences it, and it no longer counts against
        `max_persisted_sessions`.
      - Counted as `sessions_dropped`.

      An attached persistent session that overflows is closed and deleted
      the same way, which is today's rule for any session.
    - **The next CONNACK**, for a `Connect` with `persist` that passes (2):
      - an entry exists: it resumes, `session_present = 1`. A resumption is
        never refused for the limit.
      - no entry, under the limit: a new, empty session,
        `session_present = 0`;
      - no entry, at the limit: refused, nothing created — v5 0x97 Quota
        exceeded; 3.1.1 0x03 Server unavailable.

      A `Connect` that fails (2) is refused as stale, 0x88 / 0x03, before
      any of these. "Accepted with `session_present = 0`" means no session
      was kept: never created, or deleted. MQTT has no code to say which,
      and none is invented; the counter shows deletions to the operator.
    - The 3.1.1 code checked: `codec` maps every refusal outside 3.1.1's
      five codes to 0x03 (`ReasonCode::connack_v311`). The spec's meaning,
      "the MQTT service is unavailable", tells a client to retry later
      without implying anything about its credentials or client id. That
      fits a refusal at a capacity limit or a stale refusal, and is the
      code 3.1.1 has for it.
  - **(4) Write and recovery failure — what guarantees "everywhere or
    nowhere".** Not the record format; the commit rule and the takeover
    rule:
    - *Applied only when committed.* A node applies records only up to the
      commit point it knows (the node loop: `while applied < commit`). The
      commit point moves only when a majority of voters holds the record
      durably within the sequencer's own slot (`Sequencer::advance_commit`;
      `commit_needs_a_majority_durable_and_only_grows`). A replica knows
      it only as far as its own log goes
      (`a_replica_knows_the_commit_only_as_far_as_its_own_log`).
    - *Committed survives.* A new sequencer builds on the longest log of a
      majority that promised, and that majority meets the one that
      committed (`a_candidate_catches_up_from_the_longest_log_of_a_majority`,
      `a_takeover_during_a_catch_up_builds_on_no_stale_prefix_and_keeps_the_fence`).
      A replica cuts its log only where the slots prove it differs, never
      below the commit point it knows
      (`a_follower_cuts_only_where_the_slots_prove_the_logs_differ`). The
      cluster simulation checks invariant 1 — a committed record is
      durable on a majority and never cut — after every event, through
      crashes, power losses with torn writes, partitions and compaction.
    - *Uncommitted may vanish, unobserved.* A `Connect` that never commits
      may be cut from a log. No node applied it, and no CONNACK came from
      it.
    - *Partial write.* A torn batch at a log's tail is cut on recovery. A
      failed write stops the node (`Event::Failed`). Its `NodeReset` on
      restart detaches every origin it had.
    - *Restart.* Persisted sessions, tombstones and the floor are part of
      the replicated state and of every snapshot (the snapshot format's
      version goes up). A restarted node rebuilds them from its snapshot
      and log, and its state digest must equal the others'.
    - So: a `Connect` is in the committed log of every node, at one lsn,
      with the same effect — or in none, and nobody acted on it.
    - The limits of that claim:
      - It holds in the model the simulation tests: crash-stop, lost and
        reordered messages, torn writes, partitions, at most a minority
        lost for good. It is not a formal proof, and a majority losing its
        disks at once is outside the model.
      - A replica compacted past `retain_bytes` (D-57 A) cannot rebuild the
        state from its log until 7b's state copy exists. It is lost, as
        today, and persisted sessions don't change that.
  - *Tests required with the implementation:* invariants A and B in the
    gateway and cluster simulations. Scenarios:
    - two `Connect`s racing for one client id through two gateways;
    - a late `Connect` after a slot change (D-109 B's held proposals);
    - a late `Connect` after the entry was removed (clean disconnect, and
      overflow);
    - tombstone-queue overflow raising the floor;
    - `Disconnect` racing `Connect`;
    - records from a taken-over origin;
    - restart while detached;
    - the limit, refusing and then resuming;
    - detached overflow deleting a session, and the next CONNACK.

    Plus `apply` rules one per test, the snapshot round-trip, and the
    stable-contract update with straum-cli (`Connect` and `Disconnect`
    leave the reserved block).
- **D-143 B (2026-10-16; in-process, container, preliminary):** catch-up
  with every node's log evicted from the page cache before the restart
  (`cap`'s `STRAUM_BENCH_COLD=1`: `sync`, then `dd iflag=nocache count=0`
  on each file; `drop_caches` is refused in the container). With D-142 A,
  cold and warm agree in three rotated rounds at 256 and 1 024 — reads
  0.02–0.03 ms, catch-up commit p99 1.6–3.6 ms, the replica's appends
  waiting 0.04–0.05 ms, caught up under load every time (at 1 024
  mostly before the first sample, so window 256 is the comparison). Base
  cold read no slower than base warm had (10–15 ms against 13.8–16.6), so
  the run does not show that cold reads cost more here at all. *Not
  established:* a physical cold disk — the disk here is the VM's, whose
  image macOS may cache, and the eviction is one pass before the restart.
  With the cursor each record is read from the disk once, so what remains
  of the cold-disk risk is one sequential read of the lag, not a rescan per
  read.
- **D-137 A (2026-10-16):** D-124's candidate is the tree as it is, with
  the D-132, D-135 A and D-136 A instrumentation in the node loop. This
  amends D-131's freeze; the instrumentation's own cost is not isolated,
  and D-124's result does not compare with the metrics-only (D-112)
  candidate D-127 step (2) named. Since 2026-10-16, D-124 gates only
  accepted performance claims and tuning defaults, not feature work
  (D-138).
- **D-132 A (2026-10-15): a lagging replica, observable from both sides.**
  A replica now keeps the highest commit point an owner sent it, not capped
  at its own log's end (`Voter::heard_commit`). The node samples progress
  once a heartbeat — not per event — and exports: its own lag behind that
  commit point, how fast the lag changes (positions a second, over at least
  a second), the time since its log end last moved, whether it is catching
  up, and whether its gateway serves (its own reset applied). As the
  sequencer it exports the same per replica: lag, rate, the time since its
  durable position last moved. Nothing acts on them; no client is closed
  on the age of the last progress. Tests: a voter test (the commit heard,
  nothing past the log taken); a unit test of the progress tracker; a node
  test — a replica stopped while the sequencer commits shows there with a
  lag and a growing age, and once restarted reports itself behind, then
  caught up and serving. Limits, stated: `straum_catching_up` is
  `lag > 0`, an indicator only — a healthy replica under load is a few
  positions behind too; what separates catch-up from normal replication is
  lag, rate and age together, and no threshold is set yet (D-132: measure
  first). A replica's availability (`straum_serving`) is visible only on
  that replica; the sequencer exports each replica's lag, rate and age.
  D-124's candidate build now includes this sampling — once a heartbeat,
  one small allocation for the replica list, not per message.
- **D-131 A, first measurement (container, one run per size,
  preliminary):** the restarted node's lag shrank under load at every read
  size — 64 KiB: 263 503 → 190 981 in 17 s; 512 KiB: 271 049 → 230 980;
  4 MiB: 264 288 → 194 603 — and after the load stopped it caught up at
  7 400, 4 500 and 7 100 positions a second. So it is slow, not unable:
  about a minute behind under this load, longer than its clients' 30 s.
  Read size did not show as the limiter in this one preliminary run per
  size: a 64× larger read gave a similar rate, while 512 KiB was 40 %
  slower than both ends, so one run cannot establish a non-effect. The
  running nodes' PUBACK per size: 64 KiB p50 1.153 ms, p99 25.7 ms,
  43 485/s; 512 KiB 1.030 ms, 27.6 ms, 48 755/s; 4 MiB 0.955 ms, 17.0 ms,
  54 045/s; commit latency per size was not read. Next suspect, from the
  code, not measured: the replication window of 256 positions in flight
  beyond what the replica reported durable (D-133).

Checked: unit tests of the histogram and every measurement; the voter's
and gateway's counters in their tests; two node tests —
`metrics_follow_proposals_from_accepted_to_committed` (accepted equals
committed, nothing refused or given up, every histogram observed, replicas
caught up, the text served over HTTP) and
`metrics_show_an_outbox_overflow_and_where_the_delay_is` (both replicas
stopped: the outbox fills to its share, the rest is refused and counted,
reads are withheld; batches still become durable locally and none commits
— the delay is at the quorum; a replica back, all commit, the spell ends).
The corrected fencing test passed 100 of 100 under load, beside 23 runs of
the node tests (D-112's go-ahead).

**D-84 A (2026-10-09): the production cap comes from a Linux host.**
`node/benches/cap.rs` measures throughput and failover (the sequencer
stopped in order while the load runs, timed to a commit in a later slot)
per cap, and the largest survivor lag sampled just before the stop. No Linux host was available;
container runs are preliminary (docs/baselines/apple-m1-dev.md). They
suggest what the simulation does not show: failover misses 1 s where a
survivor lagged tens of thousands of records at the fault, whatever the
cap — `max_uncommitted` bounds the tail past the commit point, not how
far a replica outside the committing majority falls behind (D-85).

The new seed group also found a recovery bug in `node`: a log holding
only a torn batch of a newer slot came back as an empty log claiming
that slot, and refused for ever the committed log of an older one
(cluster simulation, seed 4045). Recovery now starts the chain at slot
0 — `prev_hash` binds the first batch, and complete records set the
slot as they are fed (`wal::tests::a_torn_first_batch_leaves_a_log_of_no_slot`,
which fails with the old rule). Compactions
during a takeover are counted in the cluster simulation (677 at 10 000
seeds); the apply simulation counts them and finds none, since only a
sequencer compacts there, and the MQTT simulation does not compact.

Not yet in step 5:

- **Deviation from D-57 A — unplaceable replicas. Closed by D-73 A and
  D-79 A (2026-10-09).** A slot table rebuilt from a compacted log no
  longer knew where older slots began, and a replica whose log ended in
  such a slot was lost inside the byte bound (488 of 941 losses in the
  cluster sweep, exempted from the D-57 A check). Now the node keeps the
  committed log's slot table — built from the committed records it
  applies — and writes it in its snapshot file ahead of `apply`'s state
  (`[version=2][slots u32]{[slot][first lsn]}[state]`; `apply`'s own
  snapshot format is unchanged, and a body from before, which starts with
  `apply`'s version 1, is refused). Deviation from the D-79 card, which
  put the table after the state: it goes first, because `apply`'s body
  carries no length of its own to find the table after it. A restarted node starts from
  `cluster::merge_slots`: the snapshot's table, then the slots the log
  after it begins. Every compaction reports the floor it reached
  (`wal::Event::Compacted`), and `cluster::prune_slots` drops from the
  node's table and from the voter's and sequencer's in-memory tables
  (`Voter::on_compacted`) the slots that ended below it, keeping the one
  that holds the floor. So every table is bounded by the slot changes in
  the retained log, with no count bound and no remainder. Keeping the
  entry at the floor matters: dropping it too (the rule one lsn looser)
  loses a replica inside the bound at seed 1678, as does a restart
  without the snapshot's table. Evidence: the cluster simulation, which
  models the node's tables and calls `on_compacted`, has no exemption;
  10 000 seeds pass with every loss inside the D-57 A rule (1 016
  losses), and three rotating runs of 1 000 000 steps cross about 2 000
  slot changes each with no table longer than 9 entries. The apply
  simulation keeps the table as the node does. `crates/node/tests/cluster.rs`:
  nodes that rotated and compacted past slot changes restart from their
  snapshots and converge; a node unit test round-trips the snapshot body
  and refuses a damaged one.
- The ACK and FETCHED frames changed layout without a frame version: no
  deployment speaks them yet; step 7, which wires the path into the
  server, needs a version or a HELLO refusal.
- A candidate whose source compacted what it
  needs waits for a later slot (state copy, 7b); `Status.digest` and the
  effects hook are still on the node loop.

**Step 6 (2026-10-08): done, test-only.** Planned handoff (unified-stream
§2.4 graceful) on `cluster::Voter`:

- **`hand_off()`** seals the sequencer: proposals are refused
  (`NotLeader`), what was queued is written, then `SlotEnd` alone in its
  batch — body `last_lsn`, and its `prev_hash` is the hash of the whole
  log before it, so the record is the handoff certificate
  `SlotEnd(k, last_lsn, hash)`. Refused with `NoSuccessor` when the next
  slot's owner is the node itself (a cluster of one).
- **Step-down.** The sequencer leads until `SlotEnd` commits, sends the
  heartbeat that carries that commit point, and then follows. That
  heartbeat is its last in the slot; if it is lost, the successor takes
  over by silence, through the ungraceful path of step 5.
- **Successor.** A follower whose log holds its slot's `SlotEnd`
  committed — its commit point applies only to a log ending in the
  owner's slot, and nothing follows `SlotEnd` there, so the log holds the
  whole slot — and which owns the next slot makes its promise of that
  slot durable and writes `SlotStart` at once, asking no one. That is
  what the promise round of a takeover finds out: a log holding every
  committed record. A majority that promised past the slot meanwhile
  refuses the new slot as it would any older one (invariant 6).
- **Rotation.** `Voter::rotate_every(ticks)` and
  `NodeConfig.rotate_every` hand the slot over every so many heartbeats;
  off by default (§2.4: a sequencer is stable).
- `apply` and the reference models treat `SlotEnd` as they treat
  `SlotStart`: a position in the log, no state.

Evidence: `crates/cluster/tests/voter.rs` (a handoff ends the slot and
the successor starts the next without any `Promise` message; only a
committed `SlotEnd` is taken over; who may hand off); the takeover
simulation hands slots over at random (5% of proposal events), so
crashes, power losses and partitions fall before, during and after
`SlotEnd`, and `SlotEnd`s are written and never committed (both counted
and required by the sweep, unified-stream §5). It checks a new
invariant 2a, "a `SlotEnd` closes its slot" — nothing of slot k follows
slot k's `SlotEnd` in the committed log — and ends every seed with a planned handoff in the healed
cluster that must pass the next slot to its owner without a promise
round; 20 000 seeds pass (68 334 handoffs started, 33 374 `SlotEnd`s
committed, 10 556 slots whose `SlotEnd` was written and never committed,
70 951 crashes of a node holding an uncommitted `SlotEnd`, 20 762 slots
handed over without a promise round — 762 of
them amid faults, the rest after healing). The cluster simulation rotates every fourth seed
every 4 heartbeats (segments, compaction and cuts around `SlotEnd`);
10 000 seeds pass, as do 10 000 of the apply and MQTT simulations. `crates/node/tests/cluster.rs`: three nodes over TCP with
takeovers by silence out of reach rotate at least three times while
publishes commit, on one log.

The `hash` of `SlotEnd(k, last_lsn, hash)` is the record's `prev_hash`,
not a body field: alone in its batch, that header field is the hash of
the whole log before it. Before D-73 A and D-79 A closed the D-57 A deviation, 2 000 cluster
seeds lost 92 replicas as unplaceable with rotation on one seed in four,
89 without.

Not in step 6: an operator command to hand off (it comes with the
server wiring, step 7); a successor that restarts between learning the
commit and promising forgets the `SlotEnd` and takes over by silence.

