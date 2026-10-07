# ADR-0008: Rewrite the broker and replication for gateway/sequencer now

- Status: accepted (direction); steps 1, 2, 3a, 3b-1 and 3b-2 done, step 4
  done in part (see Progress); D-49…D-66 decided or withdrawn; open: D-67,
  D-68, D-69
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
   bytes (CLAUDE.md §7, §8; step 2 bounds the sequencer's queue by count
   only, which is enough while no remote input reaches it);
   `sim`: invariant 4. Invariant 7 at this step means one gateway: at most one
   live session per client id within it; across gateways it waits for step 7.
4. The runtime driver for cluster mode: inter-node links, forwarding,
   configuration and its startup checks (ADR-0006); e2e tests over TCP.
5. Ungraceful takeover: durable promises with fsync before the reply, the
   slot table and truncation; `sim`: invariants 2 and 6, partitions. A
   sequencer restart becomes a takeover, and only then may the sequencer
   ship a batch in parallel with its own sync; until here it syncs first
   (D-58 A).
6. Graceful handoff: `SlotEnd`/`SlotStart`, the successor schedule.
7. Sessions, takeover across gateways, expiry and will through the log
   (the gateway proposes; the sequencer writes `SessionExpired`,
   `MessageExpired`, `WillPublished`); Assigned Client Identifier; `sim`:
   invariants 7 and 8.
   7b. State copy (D-60 A): a replica lost below the compaction floor
   rejoins from a snapshot of the applied state at a committed lsn — a
   deterministic function of the committed log (§1) — plus the log after it,
   resuming with `Chain::resume` there. After step 7 because sessions,
   expiry and wills enter the log only then; a snapshot format fixed earlier
   would change again. `sim`: a lost replica comes back and the cluster
   commits with it.
8. The old leader/follower path is removed (D-51 A): a single node runs as
   a cluster of one on the new path (commit = local fsync, delivery after
   commit; `memory` and `written` go, revising ADR-0006, D-36 A and the D-42
   draft). The two-node `quorum` pair stays on the old path until a witness
   exists (phase 4 of unified-stream §6), so step 8 removes the old path only
   after that.

**Cluster mode is test-only until step 7.** Before it, wills and sessions are
not yet in the log, and before step 5 there is no fencing: a cluster would have
observable effects outside the log (§1). Until then cluster mode is reachable
from the simulation and the e2e tests only, and refused by the server's
configuration.

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
origin, so a closed session's subscriptions stay (CLAUDE.md §7, §8) until
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
  waits (CLAUDE.md §7).
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

**Step 4 (2026-10-07): done in part, test-only; open: D-67, D-68.** Crate `node`: one tokio task per
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
preliminary (CLAUDE.md §11: no quiet Linux host). Evidence: `crates/node/tests/cluster.rs`, five tests
over real sockets and files, stable over eight runs; removing the hand-off
of applied effects to the gateway fails the MQTT test. The MQTT test
checks every PUBACK against the node's applied acceptances: the node
publishes a record's effects before its gateway acts on them. The cluster
simulation runs one seed in three with an uncommitted bound of 4–16; 1000
seeds pass; how often the bound held a batch back is not counted.

Not yet in step 4:

- **The log is never compacted (D-67).** A node rebuilds its applied state
  from lsn 1 on every start, so removing segments would leave it nothing to
  rebuild from until state copy (step 7b). Disk use grows with the log
  (CLAUDE.md §7); recovery memory does not. A restart reads the log back
  1024 records at a time, each read scanning from the first segment:
  restart time is quadratic in the log's length.
- **Configuration and its startup checks (D-68).** `NodeConfig` is built by
  tests; nothing reads the environment, and the server refuses cluster mode
  until step 7. The one startup check done is the limits digest at every
  link.
- A proposal the sequencer's full queue refuses is dropped, not held. No
  acknowledgement is lost: the gateway closes a publisher whose publish does
  not commit within `commit_timeout` and resends its lifecycle proposals.
  `cluster::Sequencer::propose` no longer promises that the caller waits.
- Credentials are not checked (`auth` is not wired); metrics are not
  exported; the `storage/commit` baseline with the batch hash is not taken
  on Linux.
- `Status.digest` hashes the whole applied state whenever it changed: a
  test hook, to be taken off the loop before an end-to-end baseline.
- A sequencer that stops stops the cluster until it returns (takeover is
  step 5).
