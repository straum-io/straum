# Durable Storage

> **Target: docs/architecture/unified-stream.md §3** — a segmented WAL, compaction
> that never truncates a segment somebody reads (refcount), versioned records
> with a properties section (§2.7, ADR-0003), and `(slot, lsn, prev_hash)` on
> every record (§2.2). This document describes the single-file `STRAUMW3` log as
> built.

Rules for the write-ahead log, adaptive group commit and durable
acknowledgement. Section numbers are the ones these rules were issued under
and are referenced from code comments.

`straum-storage` implements §73–§84 and §91–§99. Sections §85–§90 describe
architecture that is not built yet; they are recorded so the parts that exist
do not foreclose them.

---

## 0. Conflicts with CLAUDE.md, resolved

These rules arrived after the broker design was fixed. Where they disagreed
with it, the resolution is below and the affected section of `CLAUDE.md` was
updated in the same change.

| Conflict | Resolution |
|---|---|
| §15 says at-least-once messages inflight at shutdown are lost, "no persistence". §79 requires a durable acknowledgement. | Both stand. Durability is opt-in per `Durability`; with `memory` the old behavior is unchanged. `fsynced` changes when the acknowledgement is sent, not what the broker routes. |
| §5 emits `PublishAccepted` once routing completed. §79 forbids acknowledging before the durability operation completes. | With `fsynced`, routing completing is no longer sufficient: the reply is withheld until the durable boundary covers the record. `Command::Publish` carries no new field; the runtime passes a `DurableToken` alongside the command and the broker releases it on `Broker::on_durable`. |
| §4 makes the broker a single synchronous task that must never block. `sync_data()` blocks for milliseconds. | The WAL never runs inside `Broker::handle`. It owns a dedicated thread, and its completions enter the broker loop as events. This is why the durable acknowledgement is a second entry point rather than a return value. |
| §2 gives every crate a layer. A WAL is neither transport (TCP only) nor broker (no I/O). | A new crate, `storage`. It does blocking file I/O and reads the clock to measure storage latency, both through its `Disk` boundary (§95); it owns no threads. |
| §7 bounds memory globally. Recovery of a large log would read it whole. | Recovery scans in a window: memory is the window plus one record, never the log. |
| `DeliveryGuarantee` (`AtMostOnce`/`AtLeastOnce`) and `Durability` (`memory`/`written`/`fsynced`) both sound like "how safe is this message". | They are orthogonal axes and must never be conflated. Delivery guarantee is about redelivery to a subscriber; durability is about surviving a crash. MQTT 3.1.1 has no wire representation for the second, so it is configuration, not a protocol field. |

---

## 73. Adaptive group commit

The completion of the previous durability operation is the primary batching
boundary. Neither a fixed timer nor a fixed record count is the normal reason
to flush.

```text
receive first record → start a batch → take everything currently available
→ write → sync_data() → the disk is free again → take what accumulated → ...
```

The batch size adapts to real storage latency and the real arrival rate.

## 74. The commit cycle

```rust
loop {
    let first = rx.recv()?;
    let mut batch = Batch::new(first);
    while batch.bytes() < max_batch_bytes {
        match rx.try_recv() {
            Ok(record) => batch.push(record),
            Err(_) => break,
        }
    }
    write_batch(&mut file, &batch)?;
    file.sync_data()?;
    acknowledge_fsynced(&batch);
}
```

A conceptual model, not a mandatory implementation; a different data structure
or I/O strategy is allowed with a benchmark behind it. The invariant is:

> A completed `sync_data()` releases the writer to consume the records
> accumulated while the previous sync was in progress.

Never "wait 1 ms, flush" or "wait for 100 messages, flush" as the primary
mechanism.

## 75. Low load

One record available means one record committed. No waiting for a timer, a
count, a size threshold or another producer. The natural batch size at low
load approaches one.

## 76. High load

The next batch consumes what accumulated during the previous storage
operation. No durability operation per message when messages naturally pile up
during a sync. The batching factor emerges from `incoming rate × storage
latency`.

## 77. Batch limits

`max_batch_bytes` and `max_linger` are upper safety boundaries, not the
primary mechanism. `max_batch_bytes` means "take what is available while the
batch stays below this", never "wait until this is reached". `max_linger` must
never delay an otherwise committable batch.

Straum has no linger at all: nothing in the commit path waits, so there is no
artificial delay to bound.

## 78. Adaptive batch sizing

A useful starting model is `target_batch_bytes ≈ throughput × sync_latency`
(1 GB/s × 1 ms ≈ 1 MB). It is an estimate, not a law; real behavior is
measured. Signals: sync latency, write throughput, batch size, queue depth,
bytes and messages committed.

An EWMA may estimate storage latency. Do not resize on every operation.
Adaptation must be stable and bounded; reacting to individual measurements
oscillates.

Straum estimates throughput as `bytes / write time`, so the target reduces to
`bytes × sync_ns / write_ns` — integer arithmetic, no assumed device speed —
recomputed every 32 commits and clamped to `[min_batch_bytes,
max_batch_bytes]`.

## 79. Durable acknowledgement

```text
message → WAL write → sync_data() → durable offset advances → fsynced ack
```

Never acknowledge before the sync for `durability = fsynced`. A failed
`sync_data()` is a durability failure for that commit cycle and is never
silently ignored.

## 80. Durability modes

| Mode | Meaning |
|---|---|
| `memory` | The message exists in broker memory. No persistence guarantee. |
| `written` | The WAL record was submitted to the operating system. It may not survive a machine or storage failure. |
| `fsynced` | The durability operation covering the record completed on this node. Only this advances the local durable acknowledgement boundary. |
| `quorum` | The durability operation completed on `required_copies` nodes, this one counted among them. Built on `fsynced`: the local log is still synced, and the acknowledgement additionally waits for the quorum watermark to cover the record (docs/replication.md §126). |

Vague words — "safe", "saved", "persistent" — are not used without naming the
event that establishes the guarantee.

`quorum` is a statement about a set of machines; the other three are
statements about one. What makes it meaningful is that every position in it
counts a contiguous durable prefix, so "a quorum holds everything through
N" is a claim about N records and not about N being somebody's latest
report (docs/replication.md §126).

What `fsynced` costs is platform-dependent and must be stated, not assumed:
Rust's `File::sync_data` is `fdatasync` on Linux and `F_FULLFSYNC` on macOS,
which flushes the drive cache. The 4 ms sync latency measured on the
development Mac is a full flush, not a lie the OS told us.

## 81. Durable offset

The WAL maintains a durable offset. A record beyond it is not durably
persisted and must never be represented as such. It is the recovery boundary.

Durability is never inferred from queue state, acknowledgement state, memory
state, or file length alone.

The offset is a position in *this* file, and compaction rewrites the file, so
it identifies a location and never a record. What identifies a record is its
correlation, which the record carries in its own body and keeps across a
rewrite.

Correlations are therefore what an acknowledgement is measured against, and
the measure is a **contiguous prefix**, not a maximum. A submitter takes its
correlation and then queues the record, so correlation 100 can reach the log
while 99 is still in the channel; reporting 100 as the boundary would
acknowledge 99 along with it. `Commit::durable_correlation` is the highest
correlation such that every correlation below it has been committed, and
`Committer` holds the ones that arrived above a gap until the gap closes
(docs/replication.md §110). A correlation is consumed only once its record is
certain to be queued, which is what keeps the sequence dense enough for a
prefix to mean anything.

## 82. Visibility is not durability

A message may be visible in memory before its record is durable. If consumers
can observe non-durable messages, that is explicit, documented and tested.
Durable consumer semantics expose data only up to the durable boundary.

## 83. Architecture

```text
producers → WAL queue → sequential WAL writer → sync_data() → durable offset
```

One WAL for all topics. The primary durable write path stays sequential.

## 84. No fsync per topic

Per-topic durability operations produce many syncs, randomized I/O and poor
batching. Topics share one WAL batch; the durability boundary is global to it.

## 85. Per-topic memory tail (not built)

A bounded in-memory tail per topic lets subscribers near the write head read
without touching storage. It must be bounded and must never become a second
copy of the WAL.

## 86. Per-topic materialized segments (not built)

Per-topic segments are materialized asynchronously from the WAL. The
materializer never blocks the commit path and is never a prerequisite for an
`fsynced` acknowledgement. The WAL owns durability; segments are a read and
retention optimization.

## 87. Retention (not built)

Retention operates on whole segments that can be removed atomically. Avoid
rewriting large files to drop expired records.

## 88. Segment size (not built)

Segments are bounded by bytes, not message count: 1000 × 64 B and 1000 × 1 MB
are different workloads. A record larger than the segment size must not break
rotation.

## 89. Large messages (not built)

Large payloads may live outside the WAL, with the record carrying metadata and
a blob reference. The ordering is then `blob durability → WAL reference
durability`; a durable reference to a missing blob is never allowed unless the
protocol explicitly permits it. Requires dedicated crash-recovery tests.

## 90. Large message threshold (not built)

`inline_threshold` is a policy boundary, not an assumption. Benchmark 64 KiB,
256 KiB, 1 MiB and others; the default must be justified by measurement for
the intended workload.

## 90a. Record layout and replay (this implementation)

The log is a byte log; the runtime decides what a record contains:

```text
publish: [ 1 ][ flags ][ correlation: u64 BE ][ topic len: u16 BE ][ topic ][ payload ]
session: [ 2 ][ flags ][ correlation: u64 BE ][ client len: u16 BE ][ client ]
         [ owes from: u64 BE ][ (filter len: u16 BE)(filter)(guarantee: u8) ... ]
```

`kind` is 1 for a publish and 2 for session state, and exists so the log
can carry more later; `flags` holds the retain bit, the delivery
guarantee, and — on a session record — whether the identity keeps
nothing any more. Framing, checksum and ordering are the log's
(`record.rs`); the meaning of the bytes is the runtime's. One copy is
made per durable publish to put the two pieces in one buffer, and one
more when the batch is built. Collapsing those two is a measurable
change, not an obvious one.

The correlation is written into the record rather than derived from the
record's position, and costs eight bytes each. The alternative is a lock
on the publish path: publishes are submitted from many connection tasks
at once, so numbering them in step with the queue means serialising them
at a second point. Eight bytes is the cheaper of the two, and a later
change can win them back by delta-encoding within a batch.

On startup the durable prefix is replayed before anything is served, and
the retained messages it holds become state again — last word per topic
wins, an empty payload deletes, and the same bounds apply on the way in
as on the way out.

## 90b. Sessions

A session that asked to be resumed is written down by what it subscribed
to and one number: the last record it had finished with. The messages it
is owed are **not** written a second time — they are already in the log
as publishes, and the mark says where to start reading them again. A
restart restores the session offline with its subscriptions, then replays
the at-least-once publishes after its mark through the ordinary routing
path, so they land in its backlog exactly as they did live.

A record is written when the session's subscriptions change and when it
goes offline. Going offline is the moment the mark is exactly right:
nothing is being handed out any more, so what it still owes is what it
owes. A crash while the session was online replays from its last
subscription change instead, which redelivers messages it had already
acknowledged — allowed at least-once, and the cost of not writing a
record per acknowledgement.

Two consequences worth stating plainly:

* **The log cannot be trimmed past the oldest mark.** Compaction respects
  it — see §104.
* **Session records can be dropped under pressure.** The broker loop
  submits them without waiting, because the log thread reports back
  through that same loop — waiting there would be waiting on itself.
  What does not fit is retried next wakeup, and beyond a bounded backlog
  the oldest go and `session_records_dropped` counts them. A dropped
  record means that session is not restored. State is lost, never
  corrupted.

A change to what these bytes mean changes the file magic with them
(`STRAUMW3`), so a log from an older format is refused rather than misread.
`STRAUMW2` is refused rather than migrated: it carried no allocation state,
and continuing one would mean inventing the number the header exists to
remember (§91).

## 104. Compaction

An append-only log that records every version of everything grows without a
bound. Compaction rewrites it keeping only what is still the present word:

* the one session record that is current for each identity, and nothing for
  an identity whose tombstone says it keeps nothing;
* the one publish that is the current retained message for each topic;
* publishes above the oldest session mark — those are still owed.

Everything else is history. A retained message that was overwritten two
hundred times leaves one record; a session that subscribed, unsubscribed and
resubscribed leaves one record.

`storage::compact` does the mechanical half and knows nothing about what a
record means: it is handed a predicate and rewrites the file in order. What
to keep is the runtime's answer, computed from the same scan that restores
state at startup, so the two can never disagree about which record is
current.

The rewrite goes to a second file that is made durable before `rename`
replaces the original, and the directory entry is synced after. A crash at
any point leaves either the whole old log or the whole new one — a
half-rewritten log is not a state this can produce.

If that directory sync fails, the log is poisoned, as after a failed data
sync (D-9): the name already points at the new file and nothing says it will
after a power loss, so a record committed there could vanish with it. It
used to be ignored, and on the simulated disk an fsynced record acknowledged
after it was lost (D-31). Beside the writer it stops the writer at once,
through the same path as a failed commit (invariant 7): the queue closes
before the failure is counted, and nothing further is acknowledged (D-32).

A new log's directory is synced when the file is created, for the same
reason: POSIX promises nothing about a name from a sync of the file (D-30).

It runs at startup, once the file is at least `STRAUM_WAL_COMPACT_BYTES`
(default 64 MiB, `0` disables it), and on the writer thread between two
commits, once that many bytes were appended since the last rewrite.
Appended, not the size of the file: a file that has to stay large — a
retained suffix, or a live set above the threshold — would otherwise be
rewritten after every commit (D-18). Commits wait while it runs.

With a follower configured, a rewrite keeps a suffix of the log verbatim
under the sequences it was written with, so the follower can still be
caught up from it; only what lies below the suffix is filtered
(docs/replication.md §118, §130).

Correlations and sequences are both preserved across a rewrite. The numbering
continues from the highest either has ever reached, not the highest that
survived — the rewrite writes both forward into the new file's header (§91).
A reused correlation would make a session's mark point at a different record
than the one it meant, and a reused sequence would make a replication
position name two records at once.

## 91. Ordering and identity

Every committed record has a position, and `WAL order == commit order`.
Concurrent writers must not create ambiguous ordering: the WAL writer is the
authoritative ordering point.

Three things are easy to confuse and are not the same:

```text
byte offset       where a record physically sits in this file today
WAL sequence      which record it is, in the order the writer accepted them
correlation       what the submitter called it, carried in the record body
```

The byte offset is a **location**. Compaction rewrites the file, so it means
nothing across a rewrite and nothing at all to another machine.

The sequence and the correlation are **identities**, and they are independent
of each other. Neither is derived from the other and neither is derived from
how many records the file happens to contain. The sequence is the log's own
count of what it has accepted; the correlation is the submitter's, opaque to
the log, which only stores it.

An identity a rewrite did not carry forward is gone even when its record is
not. Compaction keeps a filtered set, not a prefix, so the records it leaves
behind still recover and still replay — they simply have no sequence any
more, because nothing in the file maps a survivor to the number it was
written under. `retention_floor` is where the log can still be read by
sequence, and it is above everything the file physically holds after a
rewrite (docs/replication.md §130).

**Compaction changes the physical representation and never rewinds a logical
identity.** After a rewrite the file holds fewer records than were ever
allocated, and that difference is what the header carries:

```text
[ STRAUMW3 ][ sequence u64 BE ][ records u64 BE ][ correlation u64 BE ][ crc32 u32 BE ]
```

`sequence` is the highest sequence allocated when the file was written and
`records` is how many records it started with, so a reader finding `n` records
knows the live sequence is `sequence + (n - records)`. `correlation` is a
floor: the log cannot read record bodies, so the owner writes down its own
high-water and takes the larger of that and what the surviving records carry.
The checksum covers the three numbers, because a damaged high-water reads as a
plausible smaller number and would repeat identifiers in silence.

The header is written when the file is created and when it is rewritten. The
append path never touches it, so none of this costs a running broker anything.

This was not true before `STRAUMW3`: both values were derived from what
survived compaction, so a restart after one handed out numbers that had
already been used (docs/replication.md §116).

## 92. Crash recovery

Every change to the WAL format or the durability path requires crash-recovery
tests. At minimum: crash before write, during write, before sync, during sync,
after sync; partial final record; truncated WAL; invalid record length;
corrupted checksum; a blob reference without its blob.

Recovery distinguishes the valid durable prefix from an incomplete or corrupt
tail, and never treats the file length as that boundary.

## 93. WAL benchmarks

Workloads: single message, low rate, sustained high rate, bursts, mixed sizes,
large messages, multiple producers, slow and fast storage.

Measure: messages/sec, bytes/sec, fsyncs/sec, **messages per sync**, **bytes
per sync**, p50/p95/p99 latency, allocation rate, CPU. Throughput alone is not
an evaluation.

## 94. Adaptive batching benchmarks

The suite must show batching adapting: idle, low, moderate, high, burst during
fsync. Expected: small batches and low latency when idle; larger batches and
roughly one durability operation per storage cycle under load. A fixed target
batch size chosen because it looked good on one machine is not a result.

## 95. Storage latency

Measured, never assumed: write duration, sync duration, total commit duration,
batch size. The broker must expose enough to tell CPU, WAL encoding, memory
copies, storage write, sync latency and queue contention apart. Do not
optimize the broker while the device is the limit.

The log reads the clock through the same `Disk` boundary as the device it is
timing (`crates/storage/src/disk.rs`). On `OsDisk` that is the operating
system's monotonic clock and nothing changes. On `simulated::SimDisk` it is
simulated time, which writes and syncs advance by a latency the simulation
sets, so the adaptation sees the same latencies on every run of the same
seed (D-28, stage 1; mechanism accepted in D-30, in place of the caller
passing durations in). The simulated disk follows POSIX strictly for names:
a sync of a file does not make its name durable, only a directory sync
does.

## 96. CPU efficiency

Minimize record copying, temporary allocations, repeated length and checksum
work, buffer growth, per-record syscalls, synchronization and queue
operations. Prefer one batch, one sequential write, one durability operation,
many acknowledgements. A record is never an independent asynchronous task.

## 97. CPU and batching together

Do not optimize only the fsync count. Track cycles per message, cycles per
byte, messages per sync, bytes per sync. A batching algorithm that saves syncs
while burning CPU on batch construction is not automatically an improvement.

## 98. Batch construction

Avoid unnecessary copies; a batch should reference or own records without
rebuilding their payloads. Where vectored I/O is used, build the vectors
without first copying payloads into a contiguous temporary. Vectored I/O is
not assumed to be faster: benchmark one contiguous write against a vectored
write against several writes for the real workload.

## 99. Invariants

1. No message acknowledged as `fsynced` exists beyond the durable boundary.
2. A successful durability operation advances the boundary monotonically.
3. Records accumulated during the previous durability operation are eligible
   for the next batch.
4. An idle broker never waits for an artificial timer before committing.
5. One slow topic does not force every other topic into its own cycle.
6. The WAL stays globally ordered.
7. A failed durability operation never produces successful `fsynced`
   acknowledgements.

   That includes the publishes that come after it. A failure stops the
   log's writer, which closes its queue before the failure is even counted;
   a reader that then gets no token does not pass the publish to the
   broker — without a token the broker would acknowledge it at once — but
   waits, unanswered, until shutdown (D-9). Tested by
   `runtime::tests::a_publish_after_the_log_failed_is_never_acknowledged`.

## 100. Separation of concerns

```text
WAL           durability
memory tail   low-latency reads
segments      read locality and retention
blob store    large payloads
```

No component owns more than one of these.

## 101. Definition of done for storage performance

Adaptive group commit preserved; low-load latency, high-load throughput,
messages per sync, bytes per sync, sync latency and CPU cost measured;
allocation behavior checked; crash recovery still correct; durable
acknowledgement semantics still correct; benchmarks repeated and compared
against the previous baseline; regressions checked under several workloads.
One high-throughput number is not evidence.

## 102. The design question

> "What caused the previous sync to complete, and what accumulated while it
> was running?"

not

> "Has enough time passed?" or "Have enough messages accumulated?"

The storage device provides the pacing signal. The algorithm adapts to it
instead of imposing a cadence on it.
