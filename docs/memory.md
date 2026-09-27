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
