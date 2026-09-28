# Replication

What it takes to replicate durably, and what had to be fixed first. It began
as the Phase 1–2 audit — the trace, and the map of what the implementation
contradicted — and the sections after §110 are the work that followed.
The table in §108 carries each finding's current state.

`docs/cluster.md` (§70–§83a) decides *who owns a session* and what crosses the
network for ownership. This file is about a different question: **what it takes
for an acknowledged message to exist on more than one machine**, and where the
existing implementation already allows that and where it contradicts it.

Numbering continues `docs/storage.md`, which ends at §104, because this is a
statement about the log.

---

## 105. What was traced

The whole path from a PUBLISH arriving on a socket to the PUBACK leaving it,
in code, before proposing anything.

```text
mqtt::Connection::read           packet → Reaction::Forward(Command::Publish)
connection.rs:257                durability != Memory ? submit : skip
wal.rs:52                        correlation = next.fetch_add(1) + 1
wal.rs:57                        records.send(record).await         ← backpressure
wal.rs:58                        returns DurableToken(correlation)
connection.rs:272                Event::Command { session, command, durable }
broker_loop.rs:105               Broker::handle_durable(session, cmd, token)
lib.rs:575                       route(&publish, token.0)           ← mark for sessions
lib.rs:586                       pending_durable.push_back((token, session, request))
   ── the reply is now withheld ──
commit.rs:124                    commit_available: recv, stage, drain, commit
wal.rs:147 (storage)             mem::take(batch) → write(&batch) → put back
wal.rs:157 (storage)             sync_data() if sync
commit.rs:145                    Commit { last_correlation, durable_offset, .. }
runtime/wal.rs:647               blocking_send(Event::Durable(last_correlation))
broker_loop.rs:113               Broker::on_durable(token)
lib.rs:292                       pop every pending ≤ token → PublishAccepted
mqtt encode                      PUBACK
```

Three properties of that path matter more than the rest:

* The **acknowledgement is already decoupled from routing**. `pending_durable`
  exists, is a queue, and is released by a watermark from outside the broker.
  Anything that can delay `Event::Durable` can delay a PUBACK without the
  broker knowing why.
* The **batch is one contiguous buffer**. `Wal::stage` appends framed records
  into a single reused `Vec<u8>` (`storage/src/wal.rs:54`), and `commit_with`
  moves it out and hands `&batch` to the write. The identical slice can be
  handed to a network sender at that point.
* **`Event::Durable` is one line** (`runtime/src/wal.rs:647`). It is the only
  place in the system where "this is durable" is declared.

## 106. What each crate owns on this path

| Crate | Owns | Knows about durability |
|---|---|---|
| `domain` | `Durability` (3 variants) | the vocabulary only |
| `protocol` | `DurableToken`, `Response::PublishAccepted` | the contract |
| `broker` | `pending_durable`, `durable_high_water` | *when* to reply, never *why* |
| `storage` | framing, staging, `sync_data`, compaction | the local device |
| `runtime` | the WAL thread, the token allocator, `Event::Durable` | the wiring |

`broker` does not know a disk exists, `storage` does not know a broker exists,
and neither knows about tokio. That separation is what makes a network hop
insertable at all.

## 107. Where replication fits without touching storage semantics

Three seams, all in `runtime`, all already present:

1. **Fan-out point** — `Committer::commit_available` has the batch as a
   contiguous slice between the write and the report. Sending it is a channel
   send, not a second serialisation.
2. **Gate point** — `report()` decides that a commit is durable. Quorum is that
   decision waiting on one more condition.
3. **Floor point** — `live_set()` already computes the oldest correlation the
   log may not drop (`runtime/src/wal.rs:482`). A follower that is behind is
   another reason not to drop, expressed in the units already used.

Consequence, and it is the main finding of this audit:

> **A `quorum` durability level requires no change to `storage`, no change to
> `broker`, and no change to the `protocol` contract.** It is one variant in
> `domain::Durability`, a condition on `report()`, and a new sans-IO codec
> crate wired by `runtime` — the same shape `mqtt` already has.

That is what "do not change storage semantics to fit consensus" looks like when
it holds.

## 108. Contradiction map

Against `docs/storage.md` §73–§104 and `CLAUDE.md` §7–§8.

| # | Finding | Class | Blocks replication? |
|---|---|---|---|
| C1 | `Commit.last_correlation` was a running maximum, not a watermark: a publish could be acknowledged before its record was written (§110) | **correctness / durability** | **fixed** — contiguous prefix in `Committer` |
| C2 | Correlations were consumed by `fetch_add` before the record was guaranteed to enter the queue; `try_submit_session` returning `false` burned one permanently | **correctness** | **fixed** — the queue slot is reserved first |
| C3 | Byte offsets are not stable across compaction: `Wal::compact` rewrites the file and recomputes `durable_offset` (`storage/src/wal.rs:206`) | architecture | yes — correlation is identity, offset is local location, and only the first may cross the network |
| C4 | `shrink()` compacted on size alone; nothing held the log for a follower that had not received those records | architecture | **partly fixed** — compaction is suspended while a replica is behind (§118); a retention policy is still open |
| C5 | Correlations are per-node (`Submitter.next` starts at the local recovered maximum) and `Session.owes_from` is a correlation | architecture | not for one source; yes for multi-writer |
| C6 | `Broker::replay_retained` iterated a `HashMap` | correctness (determinism) | **fixed** — the matches are sorted by topic before delivery |
| C7 | `StorageError::Poisoned` is terminal for the log; there is no equivalent for "quorum lost", and the two are different classes of state — local storage unrecoverable versus distributed availability | architecture | yes — losing a peer must withhold acknowledgement, never kill the WAL |
| C8 | `pending_durable` has no explicit bound; its depth today is limited by the WAL channel, which a network round-trip widens | performance / §8 | no, but the limit must be named before the window grows |
| C9 | `CLAUDE.md` §16 shutdown had no step that flushed replication | correctness | **fixed** — the leader drains after the WAL writer stops, and what may still be leader-only is stated (§120) |
| C10 | No metric left the process; `Metrics` had no exporter and no caller in `server` | observability | **fixed** — Prometheus text on `STRAUM_METRICS_ADDR`, including the replication gauges |
| C11 | `docs/storage.md` §80 defines three modes; a fourth changes a table that four documents quote | documentation | no |
| C12 | **The WAL sequence regressed across compaction plus restart**, and so did the correlation: both were derived from what survived a rewrite | **correctness / durability** | **fixed** — `STRAUMW3` carries the allocation state in the header (§116) |

C6 and C10 were already on the work order in `docs/cluster.md` §83a. C1, C2,
C3, C4, C7 and C9 are new.

Two neighbouring places were checked for the same defect and are clean. The
routing index is a trie of `Vec`s, so matching is already ordered. And
`wal::restore` does iterate a `HashMap` into `Broker::restore_retained`, which
re-applies the retained limits — but `scan` applied the same limits in log
order first, so the set already fits and every insert is accepted whatever the
order. That holds only because two independently written bound checks agree;
if either moves, the order starts deciding which retained messages survive a
restart.

## 109. What is *not* contradicted

Worth stating, because it is what makes the rest cheap:

* §73–§78 adaptive group commit needs no change. Replication rides the batch
  the disk already paced; it does not ask for a batch of its own.
* §79 "never acknowledge before the durability operation completes" is
  strengthened, not redefined: quorum adds a second condition to the same gate.
* §91 "the WAL writer is the authoritative ordering point" is exactly the
  property replication needs, and C1/C2 are the places the implementation does
  not yet honour it.
* §99 invariants 1–7 stay verbatim. Invariant 1 is what C1 violates today.

## 110. C1 in detail

`Committer::commit_available` kept `highest_correlation = max(seen)` and
reported it as `last_correlation`. The comment said this was so "submission
order does not have to match correlation order". That is safe only if
correlation order *does* match commit order.

It did not. `Submitter::submit` took its correlation with `fetch_add` and
*then* awaited a bounded channel:

```text
task A   fetch_add → 99        (preempted: the WAL queue is full)
task B   fetch_add → 100 → send → committed → Commit{last_correlation: 100}
broker   on_durable(100)       durable_high_water = 100
task A   send(99)              still queued, not written
task A   Event::Command{durable: Some(99)}
broker   99 > 100 is false  →  PublishAccepted immediately
```

The client is told its message is `fsynced` while the record sits in a channel.
A crash there loses an acknowledged message. `lib.rs:584` is where the filter
discards the token; the broker is correct given what it was told.

The window is not rare: it opens whenever a submitter blocks on a full queue,
which is the designed behaviour under load (`CLAUDE.md` §8).

### Closed, in this order

Two changes, neither needing a lock on the publish path:

1. **Reserve the slot, then take the number.** `Sender::reserve().await` →
   `fetch_add` → `Permit::send`, and `try_reserve` on the session path. A
   correlation is now consumed only when the record is certain to be queued,
   so the sequence has no holes. This is what C2 was: `try_submit_session`
   returning `false` is an ordinary, expected outcome the broker loop retries,
   and every refusal used to burn a number that no commit could ever cover.
2. **Report a contiguous prefix, not a maximum.** `Committer` keeps
   `durable_correlation` and a `BTreeSet` of correlations that arrived above a
   gap. The ordinary path is `correlation == durable_correlation + 1` and the
   set stays empty, so it never allocates until one record overtakes another.
   `Committer::new` takes the recovered watermark, because after a restart the
   first correlation is not 1.

Neither alone is enough. (1) narrows the window without closing it — two
threads holding permits can still push in either order. (2) is unsafe while
(1) leaves holes: one burned correlation would stall every acknowledgement in
the process, permanently.

What it looks like, with 99 numbered first but queued last:

```text
submitted        99  100  101  102
committed            100  101
contiguous        98                  ← 99 is still in the channel

then 99 commits
contiguous                 101        ← 102 has not been committed
```

Three tests were written against the old behaviour first and fail under it:
`a_correlation_that_overtook_another_acknowledges_neither_early`,
`the_prefix_jumps_the_whole_run_when_the_gap_closes` (both in
`storage/src/commit.rs`) and
`a_refused_session_record_does_not_consume_a_correlation`
(`runtime/src/wal.rs`). A fourth,
`a_recovered_log_continues_its_prefix_rather_than_restarting_it`, guards the
new constructor argument rather than witnessing the old defect.

## 111. The rule this leaves behind

A durable position is a **contiguous prefix or it is nothing**. Neither of
these is ever correct without a proof that the operand is already a prefix:

```text
quorum_offset  = max(local_offset, follower_offset)
durable_offset = committed.last()
```

The quorum watermark is not a maximum and not an average. It is the highest
position `p` such that a quorum of replicas each hold a contiguous prefix
covering `p`:

```text
local WAL   [0 ................ N]   contiguous durable prefix
replica A   [0 ................ N]
replica B   [0 .......... M]
replica C   [0 ..... K]

quorum (2 of 3)  →  M
```

and that value, and only that value, feeds the existing `Event::Durable`:

```text
local WAL writer
      │ local durable prefix
      ▼
replication pipeline
      │ follower durable prefixes
      ▼
quorum watermark
      │
      ▼
Event::Durable → broker → PUBACK
```

`storage` never learns why the watermark has the value it has. That boundary
is the whole design.

## 112. Open for Phase 3

1. **Where the quorum watermark is computed.** Either `report()` gates on it,
   or a separate task owns `Event::Durable` entirely. The latter keeps
   `storage` unaware that peers exist, which is the property §111 depends on.
2. **How the batch reaches the network.** Not an architectural choice —
   a measurement between three candidates:

   ```text
   A. Bytes::copy_from_slice(batch)     one memcpy per batch
   B. buffer rotation / ownership       no copy, a lifetime scheme to own
   C. vectored representation           no copy, scattered writes
   ```

   and the comparison is not throughput alone:

   ```text
   allocations/message      allocations/batch
   CPU/message              CPU/byte
   RSS                      batch size
   PUBACK latency           local WAL throughput
   replication throughput
   ```

   Zero-copy is not the goal. The goal is minimum total CPU and memory
   movement while memory stays bounded and latency stays predictable. If one
   `copy_from_slice` turns an ownership scheme into a straight pipeline and
   sits below the measured bottleneck, it is the right answer.
3. **The compaction floor (C4).** Adding "the lowest correlation any peer has
   not confirmed" to the `min` in `live_set()` is two lines. The policy is
   not: a peer that is down must not hold the log open forever, so there has
   to be a **minimum available correlation** the log admits to, and a follower
   below it needs a defined recovery path — a snapshot, or a refusal — not an
   attempt to read history that no longer exists.
4. **Whether followers replay into a `Broker`.** `restore()` already rebuilds
   full broker state from a log, so a follower that does this continuously is
   a warm standby for free; one that only stores bytes is cheaper. C6 had to be
   fixed either way before the first could be trusted.
5. **Shutdown (C9).** The sequence has to gain a replication drain and a final
   quorum position before the WAL writer stops. The exact order must follow
   Straum's real ownership and lifecycle — the `JoinSet`, the weak sender, the
   submitter the broker loop holds — not a sketch.

## 113. Order of work

```text
1. contiguous durable watermark + reserve-then-number       done
2. deterministic retained replay (C6)                       done
3. replication codec, sans-IO, no peers yet                 done
4. replication state machine                                done
5. STRAUMW3: logical identity survives compaction (C12)     done
6. one source, one follower, static membership              done
7. quorum watermark; Durability::Quorum                     next
8. compaction holds the log for a lagging peer (C4)         done
   a retention policy, and a path for one too far behind    open
9. failure tests: peer down, slow, returning (§121)         done
   shutdown with a lagging peer (C9, §120)                  done
10. replication measured against the local baseline (§122)  done
   the same for quorum, once there is one                   open
```

Step 4 is separate from step 3 on purpose. A codec guarantees nothing by
itself; every danger lives in the transitions:

```text
sent → received → written → fsynced → reported → quorum
```

The protocol must never be able to turn `received` or `written` into
`durable`. That is a state machine with its own tests, not a side effect of
parsing frames correctly.

Steps 1–2 are worth doing whether or not replication is ever built. Whether
Straum needs a consensus layer at all — for dynamic leadership and partition
handling — is a question for after one leader and one follower work, and it
is a different question from durable replication.

## 114. The frame (decided)

One logical replication frame represents **one complete WAL commit batch**.

The frame carries the already encoded batch the WAL commit path produced. The
replication layer does not re-encode individual records and does not read
them. A follower acknowledges durability only for a complete, validated frame:
the protocol may move a frame over any number of network reads and writes, but
partial network reception is never a partial logical durability boundary.

The WAL sequence the log already assigns is the authoritative ordering
identity. Replication introduces no sequence of its own unless a measured or
correctness requirement proves one necessary. An acknowledgement names the
highest contiguous WAL sequence whose complete frame the follower has made
durable — the same rule as §111, applied on the other side of the wire.

```text
records:   [ 1 ][ previous u64 ][ last u64 ][ bytes u32 ][ crc u32 ][ batch ]
             └──────────── covered by the crc ───────────┘         └── and this
durable:   [ 2 ][ sequence u64 ][ crc u32 ]
```

Why `previous` as well as `last`: the receiving state machine has to be able
to refuse a frame that does not continue what it holds, and the codec cannot
know what that is. `last - previous` is the record count, because the WAL
advances its sequence once per staged record — nothing parses the batch to
learn it.

**The frame carries its own integrity check, covering the payload.** The
requirement is the property, not the algorithm: CRC32 is what it uses now
because `crc32fast` is already in the tree and runs on the CPU's CRC
instructions, and replacing it is a benchmark, not a redesign.

A per-record CRC32 inside the batch does not stand in for it. The two answer
different questions:

```text
record crc:   is this one WAL record intact?
frame crc:    are these the bytes that were sent, under this header?
```

A whole record replaced by another well-formed record — valid framing, valid
record CRC — passes every per-record check and is exactly what the frame
checksum exists to refuse. `a_whole_record_swapped_for_another_well_formed_one_is_refused`
is that case.

The length is the one field acted on before the checksum can be computed,
because it says where the batch ends and therefore how much the reader is
asked to buffer (CLAUDE.md §8). Corrupted upwards it is either past the limit
or it makes the decoder ask for more bytes, which is the same answer a frame
still in flight gets; the checksum refuses it once that many bytes exist. A
read deadline, not the codec, is what stops a peer from parking the reader
there forever.

**What the follower must not do is hash the same bytes a third time.** It
verifies the frame once, before writing, and then appends the batch to its own
log verbatim — the records inside already carry the CRCs the leader wrote, and
re-staging them through `Wal::stage` would recompute every one of them. That
is a constraint on §113 step 5, and the reason the follower needs an append
path for pre-framed bytes rather than the ordinary staging path.

The header is written separately from the batch so the codec does not decide
§112.2 by itself: `encode_records_header` reads the batch to checksum it and
never copies it into the output, so the leader can still copy it after the
header, hand its buffer over, or write the two vectored.

No version or handshake yet. The kind byte leaves room for one, and where it
belongs is the state machine (§113 step 4), not the frame.

## 115. The state machine

Ranges, never messages. The unit is `(previous, last]` of WAL sequences and
every position is a contiguous prefix — "everything through N" — which is the
same rule §111 states for the local log, applied on the other side of the
wire. Nothing here tracks an individual record, so a peer costs two `u64`s
rather than a table of what is outstanding.

### Follower

```text
frame arrives
      │
      ├─ last  ≤ written   → Acknowledge(durable)     nothing written twice
      ├─ previous > written → Refused::Gap            position unchanged
      ├─ previous < written → Refused::Straddles      position unchanged
      └─ previous = written → Append(through = last)  written = last
                                   │
                              the log syncs
                                   │
                            on_durable(n) → durable = min(n, written)
                                   │
                              Durable(durable) goes out
```

Two positions, and the distance between them is the point:

```text
written   handed to the log. What a crash here loses.
durable   covered by a completed durability operation. The only number
          that ever leaves this node.
```

`received` is not durable and `written` is not durable. The only thing that
moves `durable` is the log finishing the operation that covers the range, and
`on_durable` clamps to `written` — a log reporting past what this follower
handed it is talking about somebody else's records.

A repeated frame writes nothing and re-sends the current position, because a
leader asking twice is a leader that did not hear the first answer. A frame
that starts past the position is refused rather than buffered: a prefix with a
hole is not a prefix. A frame that starts below the position and ends above it
is refused too — that is the leader having re-batched the same records
differently, and overlapping writes are how a log gets corrupted.

### Leader

```text
sent      handed to the network for this peer
durable   this peer confirmed. Counts towards a quorum.
in flight (durable, sent]
```

There is deliberately no third watermark for "written but not synced over
there". This protocol has one acknowledgement and it means durable (§114); a
field for anything else would be a guess with a name on it. An acknowledgement
above `sent` and an acknowledgement below the last one are both refused: the
first claims what was never sent, and the second says a durable position
became undurable, which by then the leader may already have called
quorum-durable and cannot unsay.

### Quorum

The highest `p` such that `required` replicas each hold a contiguous prefix
covering `p`, this node counted as one:

```text
local 200, peers 140 and 60, required 2   →  140
local 200, peers 140 and 60, required 3   →   60
local 100, peers 9000 and 9000            →  100   never above local
required 3 with two replicas              →    0   no quorum, nothing held
```

Never a maximum, never an average, and never above what this node holds: the
leader cannot call a position durable anywhere if it does not hold it here,
because it never sent it.

### What this does not yet solve

A follower that crashes mid-append recovers to the last complete **record**,
which need not be a frame boundary. Its position is then inside a frame, and
every frame the leader sends is refused as `Straddles` — correctly, but the
live path cannot get it going again. Closing that is §113 step 5's job and it
has one shape: the leader reads its own log from the position the follower
reported and re-frames from exactly there. The state machine names the
condition rather than papering over it, which is the point of having one.

## 116. C12: the identity does not survive compaction

Found while wiring §113 step 5, by testing the premise rather than reading it.
`Wal::open` sets `sequence` from the number of records the file holds:

```text
sequence in the process after compacting 5 records to 2:   5
records in the file after the restart:                     2
sequence after the restart:                                2
```

Inside one process the sequence is monotonic, and across a restart *without*
compaction it is too, because the record count is unchanged. Compaction breaks
it: the log keeps fewer records than were ever written, so after the next
start the log hands out numbers it has already used.

For replication that is the worst shape a defect can take, because nothing
reports it. A leader that compacted and restarted ships `(5000, 5050]` to a
follower holding through 6000. The follower's rule in §115 says `last <=
written`, so it treats the frame as an already-held duplicate, acknowledges
6000, and writes nothing. Both sides are internally consistent and the records
are gone.

The correlation has the same shape and the same cause. `wal::restore` resumes
from the highest correlation any surviving record carries, and `should_keep`
drops an at-most-once publish and a session tombstone whatever their age — so
if the last record written was either of those, the high-water comes back
lower than it went out. It is not a defect for the session marks, which are
bounded below by the compaction floor, but it is one for anything that needs
an identity that is unique over the life of the log.

What fixes it is the same in both cases: **persist the high-water rather than
deriving it from what survived**. The file header is the place — it already
exists, it is already versioned, and the comment on `MAGIC` already says that
a change to what the bytes mean changes the magic. A `STRAUMW3` header
carrying the sequence base, with compaction writing the pre-compaction value
into the file it produces, makes `sequence = base + records` correct across
any number of rewrites.

### Resolved

`STRAUMW3` carries the allocation state in the file header:

```text
[ STRAUMW3 ][ sequence u64 BE ][ records u64 BE ][ correlation u64 BE ][ crc32 u32 BE ]
```

`sequence` is the highest sequence allocated when the file was written and
`records` is how many it started with, so a reader finding `n` records knows
the live sequence is `sequence + (n - records)`. Two fields rather than one
base, because a file that says what it started with can be checked against
what it holds. `correlation` is a floor the owner of the record bodies writes
down and takes back, since the log never reads them; recovery uses the larger
of it and whatever the surviving records carry. The checksum covers the three
numbers: a damaged high-water reads as a plausible smaller number, which is
the exact failure this header exists to prevent.

The header is written when the file is created and when it is rewritten, and
at no other time. Nothing was added to the append path, no record was added
to the log, and no lock or synchronisation was added anywhere.

`STRAUMW2` logs are refused rather than migrated, under the policy the magic
already had: they carry no allocation state, so continuing one would mean
inventing the number the header exists to remember.

Seven tests in `storage/tests/identity.rs` and two in `runtime/src/wal.rs`
cover it, including the original reproduction. The two runtime ones go through
the real `should_keep`, so they prove the specific claim that an at-most-once
publish and a session tombstone — the records compaction drops whatever their
age — cannot take the high-water down with them; both fail with the old
behaviour, reporting 2 where 4 is correct.

`docs/storage.md` §91 now separates the three things this confused: a byte
offset is a location, a sequence and a correlation are identities, and
compaction changes the first while never rewinding the others.

## 117. A log does not say which log it is

`Peer::connected` settles a freshly connected follower against the leader's
own durable sequence:

```text
reported == local   →  InSync
reported <  local   →  Resend { from: reported + 1 }
reported >  local   →  Incompatible, refused
```

The follower's own answer is taken as the truth about the follower, and
nothing the leader remembered about it is used. That is what makes the
awkward case cheap: a follower that completed its durability operation and
lost the connection before the acknowledgement left is durable, says so on
reconnect, and is not sent those records a second time. The separation
between `written`, `fsynced` and `reported` earns its keep exactly here.

The refused case is the honest one. `reported > local` is **not**
automatically corruption — a leader restarted onto an empty log looks exactly
like a follower that invented records — and this implementation cannot tell
those apart, because **a log carries no identity of its own**. Two logs both
sitting at sequence 100 are indistinguishable, and there is no field anywhere
that says which history they belong to.

So it is refused rather than guessed at. Giving the log an identity — a
random id in the `STRAUMW3` header, written once at creation and carried
across every rewrite — is the fix, and it is a second change to a format
that has just changed, so it is recorded here rather than made on the way
past. Until it exists, a leader whose log was replaced cannot be told from
one that is simply behind, and the answer that cannot lose data is to stop.

## 118. C4: keeping the bytes a replica still needs

§116 made the *numbers* survive compaction. It did nothing for the bytes, and
the two are different promises:

```text
sequence 150 still means one particular record        §116
the bytes of record 150 are still on this disk        §118
```

A leader that compacts away 100..200 while a follower is durable through 99
can tell the follower that sequence 100 exists, and cannot send it.

Compaction is therefore suspended while a replica is behind: `shrink()`
compares how far the log has been replicated against its own durable
sequence and does nothing until they meet. With no replica the position is
`u64::MAX` and nothing changes for a single node.

This is the blunt answer and deliberately so. It is not a retention policy:
a follower that stays away keeps the log growing until an operator notices,
where the alternative — dropping the records it needs — is silent and
unrecoverable. A real policy needs a minimum available sequence the log
admits to, and a defined path for a follower below it (a snapshot, or a
refusal). That is still open, and it is the reason C4 is marked partly fixed
rather than fixed.

## 119. The TCP adapter

The socket decides nothing. `replication` holds the wire format and the state
machine; `runtime::replication` moves bytes between that and a
`transport::Connection`, and `transport` gained an outbound `connect` because
a `TcpStream` still does not leave that crate.

```text
leader                                          follower
  WAL commit                                      accept
     │ committed_batch()                             │
     ├─ ship() → bounded queue                    Durable(own sequence)  ─┐
     │                                                                    │
     │  ◄── Durable(Y) ────────────────────────────────────────────────── ┘
     │  Peer::connected(Y, local)                     │
     │  Resend{from} → catch_up reads the log         │
     ├─ frame ───────────────────────────────────►  Follower::accept
     │                                              Append → stage_framed
     │                                                 │  commit + sync_data
     │  ◄── Durable(through) ──────────────────────────┘
     └─ Peer::on_durable → replicated watermark
```

**The live queue is a shortcut; the log is the source of truth.** `ship` is
`try_reserve` and never blocks: a follower that cannot keep up must not stall
the local durability path, because in this phase a PUBACK does not wait for
replication and making it wait by accident would be the worst kind of change.
A batch that does not fit is counted in `replication_shipments_dropped` and
is not lost — §118 keeps the bytes, and `catch_up` reads them back. The same
path recovers a gap noticed in `stream`, so there is one mechanism rather
than two.

Catch-up reads the leader's own log on a blocking thread and hands frames
over a bounded channel: the log is never held in memory and a slow socket
stops the reader instead of growing a queue. `storage::allocated` reads the
36-byte header alone to learn which sequence the file's records start from,
so finding the resend point costs no extra pass. `storage::replay_frames`
hands over each record with its framing intact — the leader sends the bytes
it holds and encodes nothing twice. Compaction now uses the same path, which
removed a re-encode it never needed either.

**What advances what.** On the follower, only the completed durability
operation: the writer thread reports `commit.durable_sequence` after
`sync_data`, and only then does a `Durable` frame leave. On the leader, only
that frame: a successful write and a delivered TCP segment advance `sent` and
nothing else.

A follower's log is fed through the ordinary `Committer`, with
`WalRecord::replicated` telling it to append the run byte for byte instead of
framing it again. It has no broker, no MQTT listener and no publishers of its
own; it is a WAL endpoint, and a warm standby is a later question (§112.4).

Because catch-up re-frames from exactly the position the follower reports,
the §115 gap — a follower that crashed mid-append recovering to a record
boundary that is not a frame boundary — is closed: the leader frames from
wherever the follower actually is.

## 120. Shutdown

The replication tasks are drained **after** the WAL writer is joined, and the
leader task deliberately does not watch the shutdown signal while it is
streaming. The writer dropping its end of the shipment queue is what tells it
there is nothing more coming, so a shutdown ships what was committed rather
than dropping it.

What may remain on the leader alone when shutdown completes: anything the
follower never acknowledged. Replication is asynchronous in this phase, a
PUBACK never waited for it, and nothing here changes what `fsynced` promises
— it is still a statement about this node's disk.

## 121. The failure matrix over TCP

The determinism comes from the test speaking the wire protocol itself, not
from hooks inside the broker. One side is a real node and the other is the
test, so "close the socket with half a frame in flight", "deliver three
frames in one write" and "claim a position you do not have" are ordinary
calls rather than races. The production path gained nothing for it.

The synchronisation point is the protocol. A follower sends a durability
report only after its own `sync_data` returned, so **receiving that report is
the observable transition**. Absence is never proved against a clock: it is
proved against the log file and against what the next handshake reports.
Every test in `e2e/tests/replication_matrix.rs` finishes in milliseconds; the
deadlines in it exist so a broken run fails with a message instead of
hanging, and nothing asserts on one.

| # | Case | Covered by |
|---|---|---|
| 1 | leader starts, follower starts | `replication.rs::a_commit_on_the_leader_reaches_the_follower_disk` |
| 2 | empty leader + empty follower | `an_empty_follower_reports_nothing_and_stays_empty` |
| 3 | follower behind by N | `a_follower_that_is_behind_is_caught_up_from_the_log` |
| 4 | follower caught up | `a_follower_that_is_level_is_sent_nothing` |
| 5 | follower ahead | `a_follower_that_is_ahead_is_refused` |
| 6 | incompatible WAL identity | **not covered — the feature does not exist** (§117) |
| 7 | disconnect during frame | `a_frame_cut_off_by_the_connection_never_reaches_the_log` |
| 8 | disconnect before follower write | same test: an incomplete frame is not a frame, so the two name one instant |
| 9 | disconnect after write, before fsync | **not covered by a TCP test** — see below |
| 10 | disconnect after fsync | `a_restarted_follower_recovers_its_boundary_from_its_own_log` |
| 11 | disconnect before the report reaches the leader | `a_report_that_never_arrived_is_not_a_reason_to_resend` |
| 12 | reconnect after fsync with no report | same test, second connection |
| 13 | duplicate complete frame | `a_duplicate_frame_is_acknowledged_and_not_written_again` |
| 14 | partial TCP frame | `a_frame_split_across_every_byte_boundary_still_arrives_whole`, `a_frame_cut_off_by_the_connection_never_reaches_the_log` |
| 15 | several frames in one read | `three_frames_in_one_write_are_all_taken_in_order` |
| 16 | corrupted frame | `a_corrupted_frame_ends_the_link_and_changes_nothing`, `a_corrupted_length_commits_nothing_while_it_waits` |
| 17 | out-of-order frame | `a_frame_that_skips_ahead_does_not_advance_anything` |
| 18 | gap | same test: a frame that starts past the position is the same refusal |
| 19 | follower restart | `a_restarted_follower_recovers_its_boundary_from_its_own_log` |
| 20 | leader restart | `a_restarted_leader_reconciles_from_its_log` |
| 21 | follower permanently unavailable | `a_follower_that_never_answers_is_bounded_and_invisible` |
| 22 | follower slower than leader | `a_slow_follower_is_bounded_and_eventually_level` |
| 23 | catch-up bytes gone | `a_follower_past_the_end_of_the_history_is_refused_explicitly` |

Startup order is covered both ways: the follower first in `replication.rs`,
the leader first in `a_leader_that_starts_first_waits_for_the_follower`.

### The two that are not covered, and why

**Row 9, disconnect after the write but before the fsync.** There is no
instant between them to disconnect at. `Wal::commit_with` writes and syncs
inside one call and returns `Committed` only if both succeeded; the
follower's report is built from `commit.durable_sequence`, which does not
exist until then. Reaching the gap from outside would need a pause inside
`storage`, which means a feature flag on a production crate for one test.
What holds the invariant is structural, and the storage half of it is tested:
`staged_but_never_committed_records_do_not_exist`. Recording it as covered by
a TCP test would be a lie about what was run.

**Row 6, incompatible WAL identity.** A log carries no identity (§117), so
there is nothing to make incompatible. The detectable half — a follower
claiming more than this leader ever wrote — is row 5 and is covered. When the
header gains an identity this row becomes testable, and until then a test for
it would be testing a fiction.

### What the boundedness test actually asserts

`a_follower_that_never_answers_is_bounded_and_invisible` does not count
dropped shipments against the publish count: group commit merges publishes,
so 200 messages became far fewer batches and the first version of the test
asserted something that was never going to be true. The exact statement is

```text
shipments dropped + queue capacity >= commits
```

every batch the log committed either fitted in the fixed-size queue or was
counted as dropped, and nothing is holding batches anywhere else.

## 122. What replication costs

**Direction only.** A development Mac, two nodes in one process on one host
over loopback. That is not a network measurement and nothing here justifies a
decision on its own (CLAUDE.md §11); a number that does has to come from a
quiet Linux host with the two nodes on separate machines.

```text
Machine    Apple M1, 8 logical cores (4 performance, 4 efficiency), no pinning
Memory     16 GiB
OS         macOS 26.6.2, Darwin 25.6.0
Storage    APFS on the internal NVMe; sync_data is F_FULLFSYNC, a full flush
Toolchain  rustc 1.97.1, bench profile (release + line tables), no RUSTFLAGS
Straum     STRAUM_DURABILITY=fsynced on both nodes, defaults otherwise
Harness    crates/e2e/benches/replicate.rs, one process, loopback TCP
Method     one warm-up run of 200 messages discarded per row, then the
           measured run; one publisher, QoS 1; metrics taken as deltas across
           the measured run so the warm-up is not in the averages
```

Latencies in milliseconds. `ack` is publish to PUBACK, which is what a client
sees and what this phase must not change. `rep` is publish to the leader
hearing the follower call it durable — the whole path, not a TCP write.
`m/lcmt` and `m/fcmt` are messages per leader and per follower commit.

| workload | mode | msg/s | MB/s | ack50 | ack95 | ack99 | rep50 | rep95 | rep99 | m/lcmt | B/lcmt | m/fcmt |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| small 64 B | local | 14 528 | 0.93 | 0.00 | 0.02 | 3.66 | — | — | — | 50.0 | 4 750 | — |
| small 64 B | replicated | 7 500 | 0.48 | 0.00 | 0.01 | 7.66 | 66.67 | 81.77 | 86.51 | 51.3 | 4 872 | 51.3 |
| medium 1 KiB | local | 12 702 | 13.01 | 0.00 | 0.03 | 3.84 | — | — | — | 51.3 | 54 103 | — |
| medium 1 KiB | replicated | 7 621 | 7.80 | 0.00 | 0.02 | 7.63 | 66.00 | 78.75 | 81.86 | 50.0 | 52 750 | 50.0 |
| large 64 KiB | local | 16 899 | 1 107 | 0.01 | 0.07 | 1.11 | — | — | — | 19.0 | 1 248 895 | — |
| large 64 KiB | replicated | 14 575 | 955 | 0.01 | 0.06 | 1.05 | 110.19 | 172.20 | 173.64 | 20.0 | 1 311 340 | 20.0 |
| sustained 64 B | local | 12 304 | 0.79 | 0.00 | 0.03 | 3.68 | — | — | — | 50.0 | 4 750 | — |
| sustained 64 B | replicated | 6 421 | 0.41 | 0.00 | 0.02 | 7.79 | 69.83 | 78.68 | 82.79 | 50.0 | 4 750 | 50.1 |
| burst 64 B | local | 12 135 | 0.78 | 0.00 | 0.02 | 0.66 | — | — | — | 50.0 | 4 750 | — |
| burst 64 B | replicated | 6 555 | 0.42 | 0.00 | 0.01 | 7.75 | 66.81 | 75.85 | 79.78 | 50.0 | 4 750 | 50.0 |

### What the numbers say

**Group commit survives the crossing, which was the thing most at risk.**
`m/fcmt` tracks `m/lcmt` row for row: 50 messages that became one leader
durability operation become **one** follower durability operation, not 50.
A frame is a commit on both sides, so the leader's disk still paces the
follower's and replication added no second batching policy.

**Throughput roughly halves on small payloads and barely moves on large
ones** — 14 528 → 7 500 at 64 B, 16 899 → 14 575 at 64 KiB. The shape fits
two logs doing `F_FULLFSYNC` on the same device: at 64 B the run is one flush
after another and adding a second log nearly doubles the flushes per message,
while at 64 KiB each flush already carries 1.2 MB and the second one costs
proportionally less.

**PUBACK is unchanged, which is the point.** `ack50` stays at 0.00 ms in
every row: the client is acknowledged from the local log and never waits for
the follower. `ack99` rising from ~3.7 ms to ~7.7 ms on the small rows is the
second log competing for the same device, not replication entering the
acknowledgement path.

**Replication durability costs about 66 ms at p50 on this machine.** That is
two full flushes of a device whose flush was measured at ~4 ms plus queueing,
so it is dominated by the second fsync and by this host's flush behaviour
under load, not by the wire. On 64 KiB it is 110 ms, where each commit is
1.3 MB.

### CPU and memory, and what was not measured

```text
650.21 real   52.11 user   4.50 sys      the whole `cargo bench` invocation
569 MB maximum resident set size
30.5 MB peak memory footprint
```

These cover cargo, the compiler and the benchmark process together, so they
bound the benchmark rather than describe it. **Per-process CPU and RSS for
the leader and the follower separately were not measured**, because both run
in one process here; separating them needs two processes, which is the Linux
two-host setup this baseline is not. Per-message CPU is not reported for the
same reason. Wall time is twelve times CPU time, which is the expected shape
for a workload that spends its life in `fsync`.

### Allocation and copying, by audit

Not instrumented — `divan`'s allocation counter does not reach across a
tokio runtime and two nodes — so this is an inspection of the path, recorded
as such:

```text
Wal::committed_batch      no copy. Two buffers rotate at the commit.
ship                      one Bytes::copy_from_slice per batch, and only
                          after try_reserve succeeds, so a full queue
                          costs nothing.
encode_records_header     25 bytes into a reused Vec. Reads the batch to
                          checksum it; does not copy it.
TCP write                 two writes per frame, header then batch. The
                          batch is written from the Bytes, not copied again.
follower decode           borrows out of the read buffer.
take                      one Bytes::copy_from_slice per frame, to hand the
                          bytes to the WAL thread.
Wal::stage_framed         extend_from_slice into the batch buffer. No
                          re-encoding, no checksum recomputed.
```

So **two full-batch copies per batch**, one on each side, both crossing a
sync/async boundary. Neither is per record. Whether a rotation of owned
buffers removes either is still §112.2 and still unmeasured; the numbers
above do not single it out, since the path is dominated by two fsyncs.

### The slow-follower run did not test what it was meant to

```text
20000 x 1 KiB, the follower reading the socket and acknowledging nothing
  leader throughput     667 msg/s
  in flight             20 000 sequences
  shipments dropped     0
  leader commits        400
```

A follower that is slow to *acknowledge* is not slow to *read*: it drained
the socket, so the leader's queue never filled and nothing was dropped. The
scenario that exercises the bound is one that stops reading, and this is not
it. The assertion the run printed — `dropped + queue >= commits` — is
therefore false here (`0 + 64 >= 400`) without anything being wrong, which
makes it the wrong assertion for this scenario rather than a failure.

**So the bounded-queue path is not demonstrated by any test yet.** The
integration test `a_follower_that_never_answers_is_bounded_and_invisible`
does stop the bytes at the door, but 200 publishes became fewer than 64
batches and the queue never filled there either. What is established is that
the queue has a fixed capacity and that `ship` refuses rather than grows;
what is not established, by measurement, is the behaviour once it is full.

The 667 msg/s that run reported was a defect in this harness, not in the
broker. §124 has it.

## 123. Backpressure, and the defect it found

A follower that is slow to *acknowledge* still drains its socket, so it puts
pressure nowhere. Reaching the queue needs one that stops reading: then the
socket buffers fill, the leader's `write_all` blocks, the task stops taking
from the queue, and `ship` starts refusing.

`a_follower_that_stops_reading_fills_the_queue_and_still_loses_nothing`
does that with 3 000 × 16 KiB and `written` durability — the producer has to
outrun the stalled consumer, and two fsyncs per batch would let the consumer
keep up by accident. Measured at the stall: **278 commits, 211 refused, the
queue sitting at its bound of 64.** That is the first time the bound has been
reached by anything in this repository.

The queue depth is now a gauge (`replication_queued`, one store per batch and
never per record), so the bound is observed rather than inferred. The
accounting that looked obvious is wrong and is not asserted: `refused +
capacity` does not equal `commits`, because batches the task already handed
to the kernel are in the socket buffer — the kernel's memory, not the
broker's. What is asserted is that the queue sits at its bound, and that what
was accepted stays within a small multiple of the capacity rather than
growing with the producer.

### The defect

The test did not pass. The leader never caught up, and the reason was in the
production path rather than the test:

```text
queue overflows      → the 64 batches still in it are the oldest
reader resumes       → every one of them is stale, last <= sent, skipped
publishing has ended → no new batch ever arrives
                     → the gap is never noticed, and the follower stays
                       behind for as long as the leader is idle
```

Catch-up was triggered by *an event* — a newly arrived batch whose range did
not continue — and after an overflow no such batch exists. Being behind is a
**state**, so the leader now compares `peer.sent()` against its own durable
sequence at the top of the streaming loop and reads the log back whenever
they differ. The in-branch trigger is gone; a stale shipment is simply
skipped, because the top of the loop has already dealt with it.

With that, the run recovers every refused batch out of the log, in order, and
the follower ends byte for byte identical to the leader — which is the whole
claim that "dropped is not lost" rests on, and which nothing had tested.

## 124. The 667 msg/s anomaly was the harness

Worked through the order in `.claude/skills/performance-investigation`, and
two hypotheses died on the way, which is the point of having an order.

**Reproduced.** 667 msg/s twice, to three figures. Re-running the whole table
at the same time showed no regression from the catch-up change in `ee08533`
(7 470 / 7 774 / 17 078 against 7 500 / 7 621 / 14 575), which killed the
first hypothesis — that the top-of-loop catch-up had started reading the log
per batch. Read from the source it looked certain; measured, it was nothing.

**Verified the workload.** The table had no row of the same shape, so a slow
follower and the sheer volume were not separable. Adding `sustained 1 KiB`
gave the control: 20 000 × 1 KiB with an ordinary follower is 6 323 msg/s.
Volume was not it.

**Counters.** `replication_catchups` was **0** — the catch-up path never ran
at all, which killed the first hypothesis a second time, properly. Sync was
4.49 ms, normal. Time per commit was 75 ms, so 70 ms was somewhere else.

**CPU.** 30.12 s wall against 0.24 s user and 0.45 s sys: the process was
**98% idle**. That eliminated allocation, copying, the codec and every other
kind of work in one measurement. A sample confirmed it — all eight tokio
workers parked, the WAL thread blocked in `records.recv()`, waiting.

**The minimal experiment.** One variable: the follower answers with a report
carrying the position it already holds, so `Peer::on_durable` changes
nothing and the only difference is that the leader's socket becomes
readable. Result: **667 msg/s either way, 75.0 ms per commit either way.**
Refuted, and the identical 75.0 was the tell — 400 commits at exactly 75 ms
is exactly 30 seconds, which is a duration something imposes, not a rate
something achieves.

**The cause.** The table reports `Run::elapsed`, which is the publish loop.
This section timed around the whole call, which also includes
`client.disconnect()` and waiting for rumqttc's event loop to drain 20 000
outstanding QoS 1 publishes. Two numbers measuring different spans.

```text
                   reported   actual
throughput         667        11 300 msg/s
per commit         75.0 ms    4.4 ms      — which is the fsync, 4.5 ms
client teardown    —          28.2 s      — where the thirty seconds went
```

667 was 20 000 messages divided by 1.8 s of publishing plus 28.2 s of
teardown. It never described the broker. Everything reconciles: 400 commits
at 4.4 ms is 1.76 s, and 11 300 msg/s over 1.77 s is 20 000 messages.

The corrected number also says something worth keeping: **11 300 msg/s is
higher than the 6 323 an ordinary follower gives**, because this follower has
no log and never calls `sync_data`. The gap between them is the second
fsync, and nothing else.

The harness now reports the publish loop, as the table does, and prints the
teardown separately so it cannot be mistaken for throughput again.

## 125. Allocation, measured

§122 audited the path by reading it, and said so. Three of its rows are now
counted instead. `divan`'s allocation profiler prints a `max alloc` line
whenever a benchmark allocates — `runtime/pipeline` shows 3 per publish, so
the absence of that line below is a zero and not a profiler that is off.

`replication/benches/frame.rs`, Apple M1, bench profile:

| | allocations | throughput |
|---|---|---|
| `encode_records_header`, 256 records | **0** | 12.55 GB/s |
| `decode`, 256 records | **0** | 7.38 GB/s |
| `Follower::accept` + `on_durable` | **0** | 0.872 ns |
| `Peer::on_durable` × 64 | **0** | 104.9 ns |

**The state machine does not scale with the range.** `Follower::accept` over
a frame spanning 1 record, 256 records and 65 536 records measures 0.872 ns
every time, identical to three decimals. That is §115's claim — ranges, not
messages — as a number rather than a design statement.

Both codec figures are checksum-bound: the throughput is what `crc32fast`
does over the batch, and the header itself is 25 bytes into a buffer the
caller already owns.

### What the frame checksum costs

§114 decided to cover the payload and left the cost open. For a 1 MiB
batch:

```text
leader    1 MiB / 12.55 GB/s =  84 µs   against a 4.5 ms fsync   1.9%
follower  1 MiB /  7.38 GB/s = 142 µs   against a 4.5 ms fsync   3.2%
```

Two to three percent of each side's commit cycle to know that a frame is
the frame that was sent, including the case a per-record CRC cannot catch:
a whole record replaced by another well-formed one. That is the number the
decision was made without, and it does not change the decision.

### Appending a framed run against re-encoding it

`storage/benches/record.rs`, 256 B payloads:

| records | re-encode | append framed | |
|---|---|---|---|
| 1 | 156.2 ns | 93.4 ns | 1.7× |
| 16 | 812.2 ns | 220.9 ns | 3.7× |
| 256 | 11.36 µs | 2.33 µs | **4.9×** |

The gap widens with the record count because re-encoding pays a CRC per
record while appending pays one `memcpy` and a walk over the length fields.
At 256 records — 64 KiB, an ordinary batch — not re-encoding is worth 4.9×
on the follower's ingest path. It is also what compaction gained when it
moved to `replay_frames` and stopped re-encoding records it was only
copying forward.

### Still by inspection

Three rows of §122 are not measured here, because they sit behind tokio or a
file and `divan` reaches neither:

```text
Wal::committed_batch   the buffer rotation, no copy
ship                   one Bytes::copy_from_slice per batch
take                   one Bytes::copy_from_slice per frame
```

They remain what §122 called them: an audit. Counting them needs a harness
that can profile allocations across a runtime, which is not this.

## 126. Where the quorum watermark is computed

§112.1 asked whether `report()` gates on the quorum or a separate task owns
`Event::Durable`. It is the separate task, and it lives in `replication` as
a sans-IO state machine:

```text
local WAL: committed + fsynced ──┐
                                 ├──► Quorum ──► watermark moved ──► Event::Durable
peer reports ────────────────────┘                                        │
                                                                          ▼
                                                                  pending_durable
                                                                          │
                                                                          ▼
                                                                       PUBACK
```

Storage never learns that peers exist. `Peer::on_durable` still means one
thing — this peer has written and synced everything through a position —
and nothing about whether that is enough. Putting the quorum question in
`report()` would have let the wire format decide the runtime's durability
semantics, which is now `CONSTITUTION.md` §30.

**Nothing polls.** The watermark is recomputed when the local position moves
or a peer reports, and `on_local`/`on_peer` return the new value only when
it advanced — so the caller emits one event per real advance rather than one
per commit, and there is no timer between a commit becoming durable and an
acknowledgement going out.

### The watermark

The `required`-th largest position, this node counted as one copy:

```text
local  120
peer A 117
peer B 115   ← the third copy
peer C  90
                 required 3  →  115
```

Not a maximum, not the latest report, not an average — and the tests assert
it is none of those by name. It is meaningful only because every position
counts a **contiguous durable prefix** (§111): a peer reports where its
prefix ends, never which records it happens to hold.

Never above `local`. A leader cannot call a position durable anywhere when
it does not hold it itself, because it never sent it.

### Monotonic, and why that is not a convenience

```text
local 100, A 100, B 100, required 3   →  100 declared

B disconnects
local 105, A 103, B 100 (last known)  →  still 100, never 97, never 0
```

An acknowledgement cannot be taken back. A position once declared durable
stays declared even when the replica that helped establish it goes away, so
`Event::Durable(100)` is never followed by `Event::Durable(97)`.

### Known position is not connected

The two are tracked separately because they answer different questions. A
peer that is not connected has not lost what it already wrote — the bytes
are on its disk whatever the link is doing — so its last known position
keeps counting towards the watermark. What it cannot do is acknowledge
anything new, so it stops counting towards whether a quorum is
**reachable**.

```text
watermark()   what already happened, monotonic
reachable()   whether something published now can still reach a quorum
```

### Losing a quorum is not a poisoned log (C7)

`reachable()` going false stops new `Quorum` acknowledgements and nothing
else. A partition, a peer restarting, a follower that is merely slow: none
is terminal, and none touches `StorageError::Poisoned`, which is this node's
storage being unusable. Sharing a failure mode between them would make a
network hiccup permanent.

### One implementation

The free `quorum_watermark` function this replaced is gone. It computed the
same value from a slice, had its own tests, and was used by nothing but
them — two implementations of one calculation is what
`docs/algorithms.md` §5 and `CONSTITUTION.md` §22 both forbid. Its cases
that were not already covered moved into the coordinator's tests.

Recomputed on each event rather than maintained incrementally: the
membership is a handful of entries and this runs on a commit or a report,
never per message. An order-statistics structure is a change to make when a
benchmark asks for it.
