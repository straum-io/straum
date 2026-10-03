# Connection memory

An audit of what one established connection allocates, who owns it, and
whether the declared budget bounds it. Per `CONSTITUTION.md` §29 this is a
correctness question about the stated model, not performance tuning, and
nothing here is optimized.

Status: **resolved.** The audit is kept as it was written; §8 records what
changed and what it cost.

---

## 1. Allocation map

Everything one established, authenticated connection owns.

| # | Allocation | Where | Initial | Grows to | Returned? | Counted by |
|---|---|---|---|---|---|---|
| 1 | read buffer | `ReadBuffer::new` via `Accepted::establish` | `read_buffer_bytes` | `max_packet_bytes` | yes, `reclaim()` back to the target | **`total_read_bytes`** |
| 2 | write buffer | `connection.rs:318` | `read_buffer_bytes` | largest batch ever encoded | **no** — `clear()` keeps capacity | **nothing** |
| 3 | writer batch | `connection.rs:319` | `writer_batch × size_of::<Egress>` | fixed | n/a | nothing |
| 4 | egress channel | `connection.rs:83` | tokio block, 32 slots | `session_queue_messages` slots | on drain | messages and payload bytes by `session_queue_*`, not the slots |
| 5 | packet ids | `connection.rs:85` | empty | `max_inflight × 16 B` | no | nothing |
| 6 | mqtt state | `Protocol::new` | ≤ 64 B (`footprint.rs`) | fixed | n/a | nothing |
| 7 | two task futures | reader and writer in a `JoinSet` | compiler-determined | fixed | n/a | nothing |

## 2. Ownership map

```text
read buffer     allocated by transport at establish, owned by the Reader,
                charge released on Drop — a caller cannot forget it
write buffer    allocated by the runtime's writer task, owned by it, lives
                as long as the task
writer batch    the writer task; drained every iteration, capacity kept
egress channel  sender in the broker loop, receiver in the writer task
packet ids      Arc<Mutex<_>>, shared by the reader and the writer
mqtt state      the reader task
```

Only #1 is transport's and only #1 is charged. #2 is the same kind of
object, the same initial size, and belongs to nobody's budget.

## 3. What the configuration actually controls

```text
STRAUM_READ_BUFFER_BYTES   read buffer capacity   (#1)
                         + write buffer capacity  (#2)   ← undocumented

STRAUM_TOTAL_READ_BYTES    #1 across all connections      ← #2 excluded
```

The name says read. The value sizes both. `docs/configuration.md` has no
entry for a write buffer at all, so an operator halving "the read buffer"
halves the write buffer without being told.

## 4. The bound, as it actually is

Per connection, transport-owned capacity, with `R` =
`read_buffer_bytes`, `P` = `max_packet_bytes`, `Q` = `session_queue_bytes`:

```text
read      ∈ [R, P]     charged, reclaimed to R when the packet is out of the way
write     ∈ [R, Q+f]   uncharged, never reclaimed; f is framing overhead
```

The write buffer's high-water is the largest batch it ever encoded. One
batch is at most `writer_batch` items, and what those items can weigh is
bounded by the session queue, so `Q` is the ceiling — reached once, kept for
the life of the connection.

What the budget promises against what holds:

```text
promised   Σ read                     ≤ total_read_bytes
holds      Σ read                     ≤ total_read_bytes
           Σ write                    ≤ max_connections × Q      ← unbounded in the sense that matters
```

`max_connections × Q` is a product, not a budget. `transport/src/read.rs`
opens by saying exactly that about the read side:

> Per-connection limits alone are not a bound: a limit times the connection
> count is a product, not a budget.

The write side is the thing that comment warns about.

With the defaults, at the read budget's own ceiling:

```text
total_read_bytes  1 GiB
R                 16 KiB
connections       1 GiB / 16 KiB = 65 536 before a connection is refused
steady buffers    65 536 × 32 KiB = 2 GiB          twice the declared budget
worst-case write  65 536 × 8 MiB  = 512 GiB        the product above
```

## 5. Measured, against the model

`docs/baselines/apple-m1-dev.md` has the numbers. 15 000 idle connections,
linear, 53.4 kB each, and dropping `STRAUM_READ_BUFFER_BYTES` from 16 KiB to
4 KiB took it to 32.8 kB — a fall of 20.6 kB where one buffer would have
given 12 kB. Two buffers move with that setting, which is the model above
observed from outside.

The measurement agrees with the model and does not establish it. The model
comes from the allocation map; the RSS curve is a check on it (§29).

Steady-state cost of an idle connection:

```text
2 × R    32 KiB at the default, and 20.6 kB of it measured — 38%
~26 kB   everything else, unattributed; see the baseline for 2026-09-27
```

The remainder is not attributed to specific objects here. Attributing it
needs a heap profile; the candidates are #3 through #7 plus allocator
rounding, and Apple Silicon's 16 KiB pages flatter a 16 KiB allocation.

## 6. Classification

Against the four possibilities the audit was asked to decide between:

* **B — misleading naming.** `STRAUM_READ_BUFFER_BYTES` sizes two buffers
  and names one. The write buffer is undocumented.
* **D — the budget counts one component of a larger resource.**
  `STRAUM_TOTAL_READ_BYTES` bounds the read half of a connection's transport
  memory and is described as the global bound on it.
* **C — an incorrectly bounded path.** The write buffer's retained capacity
  is bounded only by an unrelated limit (`session_queue_bytes`) and is never
  returned, so across connections it is `max_connections × Q`.

Not **A**.

None of this is an unbounded leak: every term has a ceiling. The defect is
that the ceiling is a product, that it is 512× the declared budget at the
limit, and that the declared budget is presented as the bound.

## 7. Open, for the decision that follows

**A test that proves the discrepancy needs a counter that does not exist
yet.** The write buffer is charged to nothing, so nothing can be asserted
about it from outside; `ReadBudget::used()` reports what it was told, which
is the read half, and it is telling the truth about the wrong quantity.
Proving the gap without RSS requires the write buffer to be charged — which
is the fix. The honest position is that the audit's evidence is the
allocation map plus the RSS curve, and that the test arrives with the
change.

**Compatibility.** Splitting the setting is a behaviour change for anyone
who has tuned `STRAUM_READ_BUFFER_BYTES`: today it moves both buffers, and
after a split it would move one. Keeping the write buffer defaulted to the
read buffer's value preserves current behaviour for an unset variable, and
the coupling stops being silent because it is then written down.

**Shape of the smallest change that makes the invariant true.** This was
written for the decision and is what §8 below records as done:

```text
1. charge the write buffer to the same budget the read buffer uses
2. give it its own setting, defaulting to the read buffer's value
3. rename the budget for what it then bounds
4. reclaim it, as the read buffer already does
```

Steps 1 and 4 make the statement true. Steps 2 and 3 make it honest. Whether
all four belong in one change is the next decision.

---

## 8. Resolved

One change, because the four parts are not four optimizations — they are one
invariant being restored.

**The write buffer moved into `transport::Writer`.** It is allocated where
the budget lives, beside the read buffer, from the same `ConnectionBuffer`
type that already charged, settled and released correctly. The runtime's
writer task encodes into `writer.buf_mut()` and calls `writer.flush()`
instead of owning a `BytesMut` nobody counted.

**Both halves are charged before either is allocated.** A connection that
cannot be given both is refused both, through the admission path that
already existed, rather than discovered afterwards in RSS.

**`flush` reclaims.** `clear()` frees nothing, which is how the buffer used
to keep the high-water mark of every batch it had ever encoded for the life
of the connection. `ConnectionBuffer::reclaim` now settles unconditionally,
because the write side grows through `BytesMut` itself rather than through
`reserve`, and that growth has to be accounted where it happens.

**Names now describe what they control.**

```text
STRAUM_READ_BUFFER_BYTES              the read buffer, and nothing else
STRAUM_WRITE_BUFFER_BYTES             new, default 16384, independent
STRAUM_TOTAL_CONNECTION_BUFFER_BYTES  was STRAUM_TOTAL_READ_BYTES
```

The two sizes are deliberately **not** aliased in the configuration. Making
the write buffer default to whatever the read buffer is set to would have
fixed the name and left the coupling, which is the half-fix worth avoiding.
The default is its own constant, so an unset configuration behaves as it did
and a tuned one no longer moves two resources with one number.

The invariant that now holds:

```text
Σ retained read capacity + Σ retained write capacity
    ≤ STRAUM_TOTAL_CONNECTION_BUFFER_BYTES
```

and it stops there. Packet-id tables, session state, task futures and
channel metadata are outside it and are not claimed by it — §29 forbids
calling them bounded by a mechanism that does not manage them. They remain
the unattributed ~21 kB of §5.

### The behaviour change, stated plainly

At the default budget the connection ceiling halves:

```text
before   1 GiB / 16 KiB = 65 536 connections, process holding ~3.5 GB
after    1 GiB / 32 KiB = 32 768 connections, process holding ~1.75 GB
```

The old ceiling was never real. Raising the budget restores the old count
honestly; leaving it restores the old memory. Either is a decision an
operator can now make, which is the difference.

`STRAUM_TOTAL_READ_BYTES` is not accepted any more. An unknown variable is a
startup error here, so a broker configured with the old name stops rather
than quietly running under a bound that means something else.

### Tests

`transport/tests/listener.rs`:

```text
both_halves_of_a_connection_are_charged          8192 + 4096, and 0 after drop
the_read_setting_does_not_move_the_write_buffer  the coupling is gone
a_connection_that_fits_only_one_buffer_is_refused  admission, not RSS
the_write_buffer_returns_what_one_large_batch_forced  512 KiB forced, 4096 kept
```

None of these could be written before the change: the write buffer was
charged to nothing, so there was nothing to assert against. That was §7's
point, and it is why the tests arrive with the fix rather than before it.

---

## 9. Pending acknowledgements (C8)

Not connection memory, but the same question in the same form: does the
limit bound what it claims to.

The broker keeps one entry for every publish it has routed and not yet
acknowledged because the record is not yet durable enough — `pending_durable`
in `crates/broker/src/lib.rs`, 24 bytes an entry. Under `fsynced` that is a
sync's worth of publishes. Under `quorum` it is a network round trip's worth,
and a follower that stops confirming makes it everything published since.

What bounded it before:

```text
STRAUM_MAX_AWAITING_QUORUM   65 536   counts commits
records per commit           up to STRAUM_WAL_QUEUE, 65 536
pending_durable              ≤ 65 536 × 65 536                ← a product, not a bound
```

`awaiting_quorum` counts commits because it pairs each commit's sequence
with its correlation; one pair stands for however many publishes the group
commit merged. Its limit bounded the pairs and never the publishes behind
them.

What bounds it now:

```text
STRAUM_MAX_PENDING_DURABLE   65 536 publishes, 1.5 MiB    (initial)
```

The broker cannot refuse a command it has been handed without losing it, so
the bound is enforced before the handing. A publish that will be
acknowledged takes a permit at its own connection's reader **before its
record goes to the log**; the broker loop hands the permit back when the
acknowledgement goes, or at once if it was never held because the record
was durable before the broker saw the publish. Every entry in
`pending_durable` therefore stands for one permit taken, and the queue
cannot pass the limit.

At the limit a publisher waits at its own reader, and that wait reaches its
TCP window. **Nobody else waits** (D-7): subscriptions, QoS 0 publishes,
acknowledgements of deliveries and disconnects all keep moving, because the
broker loop never stops taking commands. Nothing is dropped, and no record
past the limit reaches the log — the log holds what the broker may
acknowledge, not more.

The first version of this bound (D-6 A) stopped the broker loop taking
commands instead. That held memory but stopped every client, not only the
publishers it was about; D-7 moved it to admission.

The log's reports are on a channel of their own. When `awaiting_quorum` is
full the loop stops reading those reports, and only those: the log writer
waits, durable publishers wait behind it, and every other client goes on.
On the command channel the same stop would halt everyone.

A publisher parked on a permit watches no shutdown signal, so shutdown
closes the permits first, before anything waits: the parked publisher ends
at once instead of when `STRAUM_SHUTDOWN_GRACE_MS` runs out, and its
publish, which never reached the log, is sent again by the client as after
any shutdown (D-10, CLAUDE.md §16). Row 30 checks shutdown under the
default five-second grace finishes in under a second; without the close it
took 5.003 s.

`awaiting_quorum` keeps its own limit and its own job: the link from
sequence to correlation, and backpressure on the log when no watermark
arrives. It is not a memory budget for acknowledgements.

The value is **(initial)**: 65 536 matches the other queue limits and costs
1.5 MiB. The quorum measurement (D-6, step B) is what chooses it.

### Tests

`crates/runtime/src/broker_loop.rs`:

```text
a_permit_comes_back_when_the_record_was_durable_before_its_publish
   the report reaches the loop before its publish; the acknowledgement is
   never held, so its permit must return. Without the return it leaks.
```

`crates/e2e/tests/replication_matrix.rs`:

```text
28 pending_acknowledgements_stop_at_the_limit_and_lose_nothing
   limit 20, 100 publishes, follower silent: 20 held, exactly 20 records in
   the log, none acknowledged; as the quorum arrives all 100 are written and
   acknowledged. Without admission the log and the queue reach 100.
29 a_tiny_pending_bound_does_not_wedge_fsynced
   limit 2, 200 publishes under fsynced, every PUBACK arrives.
30 a_held_bound_does_not_stop_shutdown
   limit 5, quorum never confirmed: shutdown completes.
31 a_full_bound_does_not_stop_other_clients
   limit 5 and full: another client still connects, subscribes and receives
   a QoS 0 message. With the D-6 A gate it could not even connect.
```

`a_report_behind_its_watermark_is_still_released`, in the same unit tests,
covers the release side: a report queued after the watermark that already
covers it is released, where before it was held for good
(docs/replication.md §128).

### What the bound also bounds: the log past a `quorum` follower

Disk, not memory, and it follows from the same permit. Under `quorum` a
rewrite keeps everything past the follower's own durable reports
(docs/replication.md §118, D-20), so how much log a follower that is away
pins is how much was written past it. Only two kinds of record reach the
log: a publish that will be acknowledged, which holds a permit from the
moment before it is submitted until its acknowledgement goes, and a session
record, which takes none. While the follower is away nothing is
acknowledged, so no permit comes back:

```text
log past the follower  ≤  STRAUM_MAX_PENDING_DURABLE × record size  +  session records
                       ≤  65 536 × (STRAUM_WAL_MAX_RECORD_BYTES + framing)  at most
```

The record-size factor is the ceiling, not the expectation: at the 1.5 MiB
the pending queue was sized for, small publishes pin a few MiB of log.
Session records are bounded by the session limits, not by this; a kept
session writes one when it changes, not per message. Below `quorum` no
follower position pins anything — the last `STRAUM_WAL_RETAIN_BYTES` are
kept whoever is behind (D-18).
