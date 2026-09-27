# Cluster

Architecture document, written before any implementation. Nothing here is
built. Its purpose is to decide the shape before code makes the decision by
accident.

The governing rule is the one the storage contract already implies and the
brief states outright:

> Do not change storage semantics to fit a consensus algorithm.

Every choice below is checked against that. Where a distributed requirement
and §73–§104 disagree, the distributed requirement gives way or the design
is wrong.

---

## 70. What the cluster is for

Three things a single node cannot do:

1. A client connects to any node and receives messages published at any other.
2. A durable client that reconnects finds its session, wherever it lands.
3. Losing one node does not lose the service.

Only the third needs replication. The first two need **routing** and
**ownership**, which are different problems with much cheaper answers — and
conflating them is how a broker ends up putting every publish through a
consensus log.

---

## 71. State classification

Derived from the code, not from assumptions (`crates/broker/src/lib.rs`
`struct Broker`, `crates/broker/src/session.rs` `struct Session`).

| State | Lives in | Kind | Must replicate | Why |
|---|---|---|---|---|
| TCP connection, TLS session | `transport` | ephemeral, node-local | no | a local effect |
| Decoder state, `PacketIds` | `mqtt::Connection` | ephemeral, node-local | no | bound to one socket |
| `SessionId` | `AtomicU64` in the runtime | ephemeral, node-local | no | nondeterministic; never leaves the node |
| Subscriptions | `Index`, `Session.filters` | persistent | **index only** | decides which node holds subscribers for a topic |
| Retained messages | `Broker.retained` | persistent | **yes** | a subscriber on any node must see the last value |
| Session ownership | implied by `by_client` | persistent | **yes, strongly** | two owners of one ClientId is a split state MQTT cannot express |
| Session contents (filters, mark) | `Session` | persistent | with ownership | moves at handover, not continuously |
| Inflight, backlog | `Session.inflight`, `.backlog` | derived | no | rebuilt from the mark and the log (§13) |
| `next_delivery`, `generation`, `seen` | `Broker` | derived | no | determined by command order |
| WAL sequence, durable offset | `storage::Wal` | persistent, node-local | no | §24: a local durability boundary, not a commit index |
| Metrics | `runtime::Metrics` | ephemeral, node-local | no | a local effect |

The table's conclusion is the design: **nothing on the message path needs
consensus.** One small, low-rate thing does — who owns a client identity.

---

## 72. The mark problem

This is the constraint that shapes everything, and it comes from the
existing session model rather than from distribution.

`Session.owes_from` is a **correlation into the local WAL**
(`crates/broker/src/lib.rs:676`, `session.rs:31`). Recovery works because the
messages the mark refers to are in the same log the mark indexes.

On another node that log does not exist. A session cannot be moved by
copying its mark: the number means nothing there.

Three ways out, and only two are acceptable:

1. **Sticky sessions.** A session lives on one node for its lifetime. If that
   node is down, that session is unavailable until it returns — its state is
   in that node's WAL and comes back with it. No payload replication, no
   change to §13, no change to storage.
2. **Replicate the backlog.** Move the owed messages with the session. Bounded
   by `STRAUM_SESSION_BACKLOG_BYTES` per session and
   `STRAUM_TOTAL_BACKLOG_BYTES` in total, so memory stays bounded — but the
   *rate* is the message rate for offline sessions, not the connect rate.
3. **A cluster-global log.** Makes the mark meaningful everywhere, by putting
   every publish through an ordered replicated log. **Rejected**: it is the
   architecture §17 warns against, it would redefine the durable offset
   (§24), and rmqtt's own published numbers show the cost — 156k msg/s in
   Raft mode against a standalone figure an order of magnitude higher.

**Decision: (1) for the first cluster, (2) only if measurement shows the
availability gap matters.** (1) is not a compromise invented for
convenience; it is the current single-node behaviour extended unchanged,
which is exactly what the governing rule asks for.

---

## 73a. Ownership without consensus on the message path

ClientId hashes to a partition; the partition maps to a node through the
membership view. That node is the sole owner of every session in it.

```text
ClientId ──hash──► partition ──view[epoch]──► owner node
```

No agreement is needed per connect: the hash decides. Agreement is needed
only when the **view** changes, which is a cluster event, not a message
event.

The fence is an **epoch**. Every view carries one, monotonically increasing.
A node acts as owner only for the epoch it believes is current, and stamps
that epoch on every ownership assertion. A node that has been superseded and
does not know it will be rejected by its peers because its epoch is stale —
the same fencing-token discipline a lease system uses, without a lease
clock. Correctness never depends on wall-clock time (§37).

**Membership for the first cluster is static**: a configured node list, an
operator-advanced epoch. That is honest for a first version and needs no
consensus algorithm at all. Automatic failover is a later change that needs
agreement on the view, and *that* — a rare, small decision — is the only
place a consensus protocol could earn its place. It would sit beside the
broker, never on the message path.

---

## 74a. What crosses the network, and on which channel

Three channels with different requirements. Keeping them separate is what
stops one from imposing its batching on another (§29).

| Channel | Carries | Ordering | Durability | Rate |
|---|---|---|---|---|
| **Forward** | publishes to nodes with matching subscribers | per source node, per topic | none — it is delivery, not storage | message rate |
| **State** | retained writes, session records, ownership assertions | total, per key | local WAL of the receiving node | connect / subscribe / retain rate |
| **View** | membership and epoch | total | configuration | operator rate |

The forward channel is the only one at message rate, and it carries **no
durability promise**. A message forwarded to node B is delivered by B's
ordinary routing path and is durable on B only under B's own configuration.
That keeps §3's two axes orthogonal: forwarding changes neither
`DeliveryGuarantee` nor `Durability`.

---

## 75a. Where the WAL sits

§23 asks four architectures to be compared. Against the mark problem and the
governing rule they are not close.

**A — replication log above a local WAL.** Two logs, two recovery models,
two truncation rules. The local WAL's durable offset and the replicated
commit index would have to be reconciled at every restart (§24 forbids
conflating them, so the reconciliation is real work). Rejected: the
duplication buys nothing that (C) does not.

**B — the replication log *is* the durable log.** Durable acknowledgement
becomes quorum acknowledgement. This redefines `fsynced` (§25 forbids),
deletes adaptive group commit as the pacing mechanism (§5 forbids), and
makes a network round trip part of a durability guarantee that is documented
as a local `sync_data()`. Rejected outright — this is precisely the change
the governing rule names.

**C — the local WAL stays exactly what it is; replication is a separate
channel carrying state commands.** The WAL keeps its sequence, its durable
offset, its recovery boundary, its compaction. Replicated state commands
arrive on the state channel and are written to the receiving node's WAL as
ordinary records, through the record kinds that already exist. **Chosen.**

**D — no replication; partitioned ownership only.** Honest and it is what the
first cluster is, with (C) as the increment that adds fault tolerance.

Under (C), §73–§104 are untouched. A replicated retained write lands on node
B as the same `kind=1, FLAG_RETAIN` record that a local publish would
produce. A replicated session record lands as `kind=2`. Recovery, compaction
and the mark all keep working because nothing about the record format or the
durability boundary changed.

---

## 76a. Acknowledgement semantics

The three local levels keep their exact meanings (§4). Distribution adds
states that are **named separately** rather than folded into them (§25):

```text
locally accepted   the broker routed it
written            the record reached the operating system   (local)
fsynced            sync_data() covering it completed         (local)
forwarded          handed to the peers that have subscribers
replicated         a state command reached a peer
```

`fsynced` never means "replicated". A cluster-wide durability level, if one
is ever wanted, gets its own name and its own contract; it does not borrow
`fsynced`'s.

MQTT has no way to express any of this to a client, which is why it stays
configuration rather than protocol.

---

## 77a. Determinism

An audit of the broker as a state machine turned up one real defect, and it
exists today (§22):

`Broker::replay_retained` (`crates/broker/src/lib.rs:734`) iterates
`self.retained`, a `HashMap`, and delivers in iteration order. `HashMap`
seeds its hasher per process, so two nodes given identical commands replay
retained messages to a new subscriber **in different orders** and consume
`next_delivery` differently.

Harmless on one node — MQTT orders messages per topic, not across topics —
and fatal to a replicated state machine. Fixing it is cheap (an ordered map,
or sorting the selection) and must be measured, because the retained path
runs on every SUBSCRIBE.

Everything else checks out: `Instant::now()` is called only in the runtime,
never in the broker; `SessionId` never leaves the node; `next_delivery`,
`generation` and `next_client` are determined by command order.

---

## 78a. Failure model

| Event | Outcome |
|---|---|
| Node loses its network | Its sessions are unreachable. It keeps serving clients that can still reach it, but its epoch goes stale, so it cannot assert ownership against peers. |
| Minority partition | Cannot advance the view, so cannot take ownership of anything it does not already hold. Existing sessions keep working locally; they are simply invisible to the other side. |
| Node crashes | Its sessions are unavailable until it restarts. Its WAL brings them back — retained state and sessions both, exactly as today. |
| Forward channel drops | Messages to that peer's subscribers are lost. This is at-most-once behaviour on a path that promises nothing more; a QoS 1 subscriber on the far node does not get a redelivery it was never told about. **This is the sharpest edge in the design and it must be stated in the configuration docs, not discovered.** |
| State channel drops | The peer's retained and session state goes stale. It catches up when the channel returns, ordered by key. |
| Two nodes claim one ClientId | Impossible while epochs are fenced: the stale claimant is rejected. |

**Split brain is prevented, not resolved.** No merge procedure exists because
no state is allowed to fork.

---

## 79a. Memory

Every queue the cluster introduces is bounded, with a stated behaviour when
full (§35):

| Queue | Bound | When full |
|---|---|---|
| Forward send, per peer | bytes | drop, count it — forwarding promises nothing |
| Forward receive | bytes | stop reading the socket; TCP backpressure reaches the sender |
| State send, per peer | bytes | block the sender; state must not be silently dropped |
| State receive | bytes | TCP backpressure |
| Handover buffer | one session's `session_backlog_bytes` | refuse the handover, leave the session where it is |

No unbounded buffering, and a slow peer costs the leader a bounded amount of
memory rather than a growing one (§36).

---

## 80a. What this costs when the cluster is off

Nothing (§27). With one node configured, there are no peers, so no forward
channel, no state channel and no view. The ownership hash resolves every
ClientId to the local node. The publish path is the code that exists today,
unchanged — no extra channel, no leader check, no epoch comparison per
message.

That is a property to keep and a benchmark to run: the standalone numbers in
`docs/baselines/apple-m1-dev.md` must not move.

---

## 81a. Performance model and benchmark plan

Expected costs, to be measured rather than assumed:

* **Forwarded publish:** one extra encode, one extra network write per peer
  that has a matching subscriber. Nodes without subscribers receive nothing —
  the subscription index exists to make that true.
* **State command:** one extra WAL record on each receiving node, at connect
  and subscribe rate.
* **Ownership:** a hash on connect. Nothing per message.

Benchmarks to add, following `docs/benchmarks.md` §4 and the measurement
protocol in §7:

```text
1 node                     the existing baseline, must not regress
3 nodes, publisher and subscriber on the same node
3 nodes, publisher and subscriber on different nodes
3 nodes, fan-out across all three
one peer slow
one peer disconnected and reconnecting
```

Reported separately from standalone, always. A distributed throughput number
must never be allowed to hide a standalone regression (§66).

---

## 82a. Open questions

Two, and they are the user's to answer because they are product decisions,
not engineering ones.

**1. A durable client reconnects to a node that is not its owner, and the
owner is unreachable. Accept or refuse?**

Accepting risks two live copies of one session. Refusing means the client
cannot connect while its owner is down.

*Recommendation: refuse.* MQTT clients retry; a broker that silently forks a
session produces duplicate deliveries and lost acknowledgements that surface
much later and are very hard to diagnose. Availability here is recoverable by
waiting; correctness is not recoverable at all.

**2. Must a message acknowledged `fsynced` on one node survive the permanent
loss of that node?**

If yes, the message path needs replication and this whole design changes —
that is architecture (B), with the costs §25 and §5 forbid.

*Recommendation: no, for the first cluster.* State it plainly in the docs:
`fsynced` is a local durability guarantee, and the cluster provides
availability, not cross-node durability. A broker that promises more than it
delivers is worse than one that promises less.

---

## 83a. Order of work

This file is about **session ownership** across nodes. Durable replication
of the log turned out to be a separable question and was built first; it is
in `docs/replication.md`, and steps 1 and 3 below were answered there.
Nothing specific to ownership is built.

1. ~~Fix the retained-iteration determinism defect (§77a)~~ — done, sorted
   by topic before delivery (docs/replication.md §108, C6).
2. Command/state/transition boundary, single node, no networking.
3. ~~Static two-node membership, forward channel only~~ — built as durable
   log replication rather than command forwarding
   (docs/replication.md §119). Ownership still forwards nothing.
4. Subscription index and its gossip.
5. State channel: retained and session records.
6. Epoch fencing and ownership.
7. Failure matrix (§57), then performance.

Step 2 changes nothing observable and can start immediately. Step 4 is the
first one that needs a decision on question 1 above.
