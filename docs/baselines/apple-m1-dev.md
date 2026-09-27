# apple-m1-dev

A development Mac, not a benchmark host. Everything here shows **direction
only** (CLAUDE.md §11, docs/benchmarks.md §6): Apple Silicon moves threads
between performance and efficiency cores with no portable pinning, so these
numbers justify no decision on their own. They exist so a later regression
has something to be compared against other than memory.

Machine:   Apple M1, 8 logical cores (4 performance, 4 efficiency), no pinning
Toolchain: rustc 1.97.1, profile release (lto=thin, codegen-units=1), no RUSTFLAGS
Sysctls:   untouched; `docs/linux-tuning.md` does not apply to macOS

## 2026-09-25 — ab34d51

The first entry, recorded at the start of an optimization pass so the pass
has something to measure against.

### In-memory pipeline (`runtime/pipeline`, divan)

Decode → map → route → deliver → encode, no sockets and no channels. The
at-least-once rows acknowledge each message; before that they did not, and
the inflight window filled once and sent everything afterwards to the
backlog, so the row measured something other than its name.

| benchmark | workload | median | spread (5 rounds) | alloc/op |
|---|---|---|---|---|
| publish_consume | 64 B, 1 sub | 305.7 ns | — | 3 / 128 B |
| publish_consume | 1 KiB, 1 sub | 243 ns | — | 1 / 40 B |
| publish_consume | 64 B, 100 subs | 3.389 µs | — | 3 / 128 B |
| publish_consume_at_least_once | 64 B, 1 sub | 261.7 ns | 251–311 ns | — |
| publish_consume_at_least_once | 64 B, 100 subs | 3.185 µs | 3.185–3.186 µs | — |

The 100-subscriber row is stable to 0.03% across rounds, which makes it the
usable instrument of the two; the 1-subscriber row varies by 20%.

### Through a socket (`e2e/loadgen`, release server)

1 publisher, 1 subscriber, unthrottled, QoS 1, 64 B payload — the point
where this machine saturates the broker with everything still delivered.

| durability | msg/s | p50 | p95 | p99 |
|---|---|---|---|---|
| memory | 183,912 | 5.07 ms | 5.48 ms | 5.93 ms |
| written | 180,997 | 5.18 ms | 5.62 ms | 5.98 ms |
| fsynced | 11,313 | 82.98 ms | 100.64 ms | 300.42 ms |

4 publishers × 4 subscribers, 20,000/s, QoS 1, memory: 479,952–479,996 of
480,000 delivered, p50 1.35–1.38 ms over five rounds.

### Where the CPU goes

15 s of the 1×1 unthrottled workload above, measured with `/usr/bin/time -l`
rather than a profiler, because the answer needs no symbols:

```text
19.22 real    6.59 user    13.02 sys
1,625,283 involuntary context switches over 2.82 M messages
```

**System time is twice user time**, and there are 0.58 context switches per
message. The broker is syscall- and scheduler-bound at this workload; its own
logic is about a third of the CPU it uses.

Two hypotheses were tested against that and one survived:

* *Raising the batch caps will amortize the wakeups* — **falsified**.
  `STRAUM_BROKER_BATCH`/`STRAUM_WRITER_BATCH` at 256/64, 1024/256 and
  4096/1024 give 186,095 / 186,752 / 186,834 msg/s with 1.29 M context
  switches in every case. The cap is not what limits batch size; pipeline
  depth is, and a single publisher waiting for its acknowledgement has none
  to give.
* *Concurrency amortizes them instead* — **held**. At a fixed 60,000/s,
  one publisher costs 0.537 context switches per message and four cost
  0.262.

A `samply` profile of the same run is flat: the largest single frame in
straum's own code is `Broker::dispatch` at 1.6% of straum frames, with
tokio's scheduler machinery (`worker::run`, `poll_notified`,
`AtomicWaker::register`) about as large in total, and `bytes` refcount
traffic at 1.3%. There is no hotspot to remove.

**Conclusion of that pass: no change made.** The measured bottleneck is not
in straum's code, and the one architectural lever for it — batching — is
already working as §4 intends. Reported here rather than left out, because a
pass that finds nothing is a result and the next person should not repeat it.

## 2026-09-25 — contiguous durable watermark

Not an optimization: a correctness fix (docs/replication.md §110) measured to
see whether it cost anything. `storage/commit`, `high (unpaced)` row, records
per second, five alternating rounds each with a discarded warm-up run:

```text
candidate  666521  799052  712507  716152  764345   median 716152
baseline   556065  799479  707895  666644  669577   median 669577
```

**No conclusion drawn.** The medians differ by 7%, the candidate spread is
20% and the baseline spread 44%, and an earlier three-round pass on the same
machine put the same change 10% the other way. The variance is larger than
the effect in both directions, which under CLAUDE.md §11 means the
environment, not the change, is what was measured. Expected, given the fast
path is a compare-and-store where it used to be a `max` and the out-of-order
set never allocates while submission order holds.

Recorded so the next person does not repeat it on this machine. A number that
would justify anything has to come from a quiet Linux host.

## 2026-09-26 — what an idle connection costs

The question was whether this broker could hold a million connections. The
part that can be measured here is the slope.

Two processes: `straum-server` in release, and `e2e/benches/connhold`, which
opens TCP connections, sends a CONNECT on each, waits for its CONNACK and
holds them. RSS is the server's alone, sampled with `ps -o rss=` from a third
shell — sampling it from inside the process holding the sockets is its own
experiment, and it was the first thing to fail.

`STRAUM_DURABILITY=memory`, `STRAUM_MAX_CONNECTIONS=20000`,
`STRAUM_TOTAL_READ_BYTES=2 GiB`, idle RSS ≈ 2.8 MB.

| connections | RSS over idle | per connection |
|---|---|---|
| 2 500 | 131 536 KiB | 53 877 B |
| 5 000 | 262 304 KiB | 53 720 B |
| 8 860 | 462 112 KiB | 53 409 B |
| 15 000 | 782 448 KiB | 53 415 B |

Linear, with a marginal cost of **53.3 KB per idle connection** between the
first and last points. Nothing about the curve bends in this range.

**The read buffer is 30% of it.** `STRAUM_READ_BUFFER_BYTES` is 16 KiB, and
the connection costs 53.4 KB. Dropping the setting to 4 KiB gives:

```text
read_buffer 4096, 10 000 connections: 32 838 B per connection
```

which is 20.6 KB less. That is close to the 24 KB two 12 KiB reductions would
give, so **the setting sizes two buffers, not one**: `connection.rs:318`
allocates the writer's encode buffer with `read_buffer_bytes` as well. An
operator halving "the read buffer" halves the write buffer with it, and
`docs/configuration.md` has no entry for a write buffer at all.

That leaves roughly **25 KB per connection that is neither buffer**. Not
attributed here: the candidates are the two tokio task futures, the
per-session channel's first block, `Arc<Mutex<PacketIds>>`, the broker's
`Session` with its two map entries, and allocator rounding — Apple Silicon
uses 16 KiB pages, which flatters a 16 KiB allocation and penalises a 4 KiB
one. Attributing it needs a heap profile, which this was not.

### What it implies, and what it does not

```text
at 16 KiB buffers   100 000 connections ≈ 5.3 GB      1 000 000 ≈ 53 GB
at  4 KiB buffers   100 000 connections ≈ 3.3 GB      1 000 000 ≈ 33 GB
```

Extrapolation from 15 000, so it assumes the line stays straight for two more
orders of magnitude, which nothing here shows.

**`STRAUM_TOTAL_READ_BYTES` bounds a third of the cost.** It counts read
buffers only, so its 1 GiB default stops new connections at 65 536 while the
process is holding about 3.5 GB. The global budget exists precisely because
per-connection limits times the connection count are a product and not a
bound (CLAUDE.md §7); it is bounding the wrong product by a factor of three.

### Why 50 000 and 100 000 are absent

Not reachable on this machine without root:

```text
net.inet.ip.portrange  49152–65535   16 384 ephemeral ports to one address
kern.maxfilesperproc   61 440
kern.maxfiles          122 880       two descriptors per connection
```

Widening the port range or adding a loopback alias both need `sudo`, and the
runs above already had to wait for TIME_WAIT to drain between them — two
consecutive runs exhaust the pool. Those counts belong on a Linux host with
several client addresses.

## 2026-09-27 — an idle connection, after the buffer-budget fix

The same method as the previous entry: `straum-server` in release in its own
process, `e2e/benches/connhold` holding connections, RSS sampled from a third
shell. `STRAUM_DURABILITY=memory`, `STRAUM_MAX_CONNECTIONS=20000`,
`STRAUM_TOTAL_CONNECTION_BUFFER_BYTES=4 GiB`, idle RSS ≈ 2.8 MB.

| connections | per connection |
|---|---|
| 2 500 | 54 650 B |
| 5 000 | 54 490 B |
| 10 000 | 54 180 B |
| 15 000 | 54 187 B |

Still linear. **54.2 kB against 53.4 kB before the change** — about 770 B
more per connection, 1.4%. The internal spread of each set is ~0.9%, so the
increase is real but small, and it buys the buffer being charged, settled
and reclaimed rather than merely allocated. No attempt is made here to
account for the 770 B; it is smaller than the unattributed remainder below.

### What each buffer actually costs

Only possible now that the two sizes are independent. 5 000 connections,
one variable at a time:

| read | write | per connection | against the 16/16 baseline |
|---|---|---|---|
| 16 KiB | 16 KiB | 54 490 B | — |
| 4 KiB | 16 KiB | 46 252 B | read returns **8 238 B** of a nominal 12 288 |
| 16 KiB | 4 KiB | 42 159 B | write returns **12 331 B** of a nominal 12 288 |
| 4 KiB | 4 KiB | 33 935 B | both return **20 555 B** |

They are additive: 8 238 + 12 331 = 20 569 against 20 555 measured, which is
0.07% apart. So the two buffers are independent in memory as well as in
configuration, which is the property the split was for.

**The write buffer returns its full nominal 12 KiB; the read buffer returns
8.2 kB of it.** Why they differ is not established here. Apple Silicon's
16 KiB page is the obvious suspect — a 16 KiB allocation is exactly one page
and a 4 KiB one shares — but that would apply to both, and it does not.
Attributing the 4 kB gap needs a heap profile. It is recorded as open rather
than explained.

### Against the budget

The two buffers are **38%** of an idle connection: 20.6 kB of 54.2 kB. The
budget charges 32 KiB per connection at the defaults and the process holds
54.2 kB, so the budget covers 60% of the real cost — and says so. That is
the whole difference from the previous entry, where it covered 30% and
claimed to be the bound.

The remaining ~26 kB is still unattributed: packet-id tables, session state,
two task futures, channel metadata and allocator rounding. `CONSTITUTION.md`
§29 forbids calling those bounded by a budget that does not manage them, so
they are named as outside it in `docs/memory.md` rather than folded in.

The prediction in `docs/memory.md` §8 held: at the default 1 GiB budget the
ceiling is 32 768 connections, and 32 768 × 54.2 kB is 1.78 GB against the
~1.75 GB predicted.
