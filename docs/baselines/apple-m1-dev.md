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

## 2026-09-27 — CPU and RSS per node

The earlier claim that this needed a two-host Linux stand was wrong. Only
the *in-process* benchmark cannot separate the nodes; two `straum-server`
processes and `loadgen` — which is a separate binary for exactly this
reason — can.

Two release servers on loopback, one publisher, no subscribers, QoS 1,
`fsynced` on both, 20 s. CPU is the process's own `utime`/`stime` delta
around the run, RSS its `ps` value at the end. One connection, so RSS here
is a floor and not the per-connection curve.

**Paced to the same rate**, because a fixed-duration run at a lower rate
divides the same standing overhead among fewer messages and inflates
per-message CPU. 5 000/s, 100 000 messages, both configurations:

| workload | node | user | sys | CPU/msg | RSS |
|---|---|---|---|---|---|
| 1 KiB | local leader | 0.61 s | 1.73 s | 23.40 µs | 4 096 KiB |
| 1 KiB | replicated leader | 0.70 s | 1.56 s | 22.60 µs | 5 824 KiB |
| 1 KiB | replicated follower | 0.15 s | 0.80 s | 9.50 µs | 5 232 KiB |
| 64 B | local leader | 0.55 s | 1.66 s | 22.10 µs | 3 472 KiB |
| 64 B | replicated leader | 0.55 s | 1.25 s | 18.00 µs | 3 872 KiB |
| 64 B | replicated follower | 0.10 s | 0.63 s | 7.30 µs | 3 488 KiB |

**Replication costs the leader no measurable CPU.** 22.60 against 23.40 at
1 KiB, inside the spread and on the low side of it. The whole cost is the
follower: 9.5 µs per message at 1 KiB, 7.3 at 64 B — about 40% of the
leader's, which is a node that runs a log and a socket and none of the MQTT
parsing, routing or client connections.

That is consistent with the throughput table: replication halves throughput
because of the second `fsync`, not because it does much more work. The
device is the cost; the code is not.

**System time is three to four times user time** in every row. The same
shape the very first profile in this project found, and the reason batching
is the lever that matters here.

### Unpaced, for the record

The same runs without `--rate`, so each side went as fast as it could:

| workload | node | messages in 20 s | CPU/msg |
|---|---|---|---|
| 1 KiB | local leader | 240 665 | 13.96 µs |
| 1 KiB | replicated leader | 125 144 | 21.02 µs |
| 1 KiB | replicated follower | 125 144 | 8.63 µs |

12 033 against 6 257 msg/s, which agrees with the in-process benchmark's
12 702 against 7 621 — two harnesses, independently written, on the same
shape of workload.

These rows must not be read as "replication costs 7 µs more per message on
the leader". They cannot be compared: the two ran at different rates, and
at 12 000/s the batches are bigger, so the standing overhead is spread
thinner. The paced table above is the comparison; this one is here because
leaving it out would hide why the paced numbers are higher than the
unpaced ones.

## 2026-09-27 — a no-op atomic on the write path

Found by walking the hot path for what had changed since the pass that
concluded "no change made". What had changed was mine: `f9ddc3d` put
`ConnectionBuffer::reclaim` on every `flush`, and `reclaim` calls `settle`,
which in the steady state did

```rust
self.budget.release(self.charged - capacity)   // release(0)
```

— a read-modify-write of zero on a counter shared by every connection in
the process, once per socket write. That change was reported as "no
performance-sensitive change". It was on the write path, and it was not
benchmarked.

The fix is a guard: when capacity equals the charge there is nothing to
settle. Behaviour is identical — `charged = capacity` was already true in
that branch.

`transport/benches/read_buffer.rs`, bench profile, medians of 100 samples:

| | before | after | |
|---|---|---|---|
| 64 flushes at steady size | 216.6 ns | 76.98 ns | 2.81× |
| 64 flushes, 1 thread | 212.7 ns | 109.2 ns | 1.95× |
| 64 flushes, 8 threads, median | 290.7 ns | 114 ns | 2.55× |
| 64 flushes, 8 threads, mean | 665 ns | 180.3 ns | 3.69× |
| 64 flushes, 8 threads, slowest | 8.207 µs | 3.541 µs | 2.32× |

Per flush: **3.38 ns → 1.20 ns**. And at eight threads the median is now
114 ns against 109 at one thread — the contention is gone, where before it
was 291 against 213.

Neighbours unchanged: `steady_large` 24.32 µs → 24.36 µs, and the read-path
rows are untouched because they grow the buffer, so the guard never fires
for them. 282 tests pass.

**No end-to-end improvement is claimed, and none was measured.** At one
connection and 12 000 msg/s with ~50 messages a flush, this is 240 flushes
a second and half a microsecond an hour; it is invisible and will stay
invisible. What justifies it is not a throughput number:

* it removes work that provably does nothing (CLAUDE.md §11, first
  priority), and
* the cost it removes scales with the connection count rather than with
  throughput, which is the shape the eight-thread rows show and the shape
  this project keeps having to be careful about.

A machine with ten thousand connections writing is where it would be
measurable, and this is not that machine.

## 2026-10-02 — D-6 B: durability levels, before and after admission

The measurement D-6 asked for once the pending-acknowledgement bound
existed: what a publish costs at `memory`, `fsynced` and `quorum`, before
and after admission (D-7), and what admission itself costs.

### Variants

```text
v0  9dfa3bb                    before: no pending bound, no admission
v1  this branch                after: admission (D-7) and everything since 9dfa3bb
v2  v1 with admission removed  no permit taken, none returned; one variable against v1
```

`v0 → v1` is the before/after D-6 asked for, but it carries more than
admission: C4, D-1, the reports channel and the watermark fix also lie
between them. `v2 → v1` changes admission alone, so it is the estimate of
its cost.

`memory` takes no permit — a `memory` PUBACK waits for nothing — so for
`memory` both comparisons are a control: any difference there is noise or
the other changes, not admission.

### Method

- Harness `e2e/benches/durability.rs`, the same source built into all
  three variants, release profile, each in its own target directory, all
  built before the first run.
- One publisher, QoS 1, rumqttc's in-flight window of 100. Timed from the
  first publish to the last PUBACK; the client's teardown is outside it.
  Latency is per packet id: PUBLISH written to PUBACK received.
- 64 B / 1 KiB × 20 000 messages, 64 KiB × 2 000 (`memory` ×10, so a point
  lasts long enough for `ps`); 200-message warm-up per point.
- `quorum`: leader and follower in one process on loopback, two copies
  required, follower `fsynced`; quorum reachable throughout.
- 5 rounds; the order of the three variants rotates every round. Load
  average 3–4 (desktop), no other benchmark running.
- `pending_durable` sampled from the metrics endpoint every 20 ms. It
  peaked at **100** at every durable point — the client's in-flight window
  — far below the 65 536 limit. Admission was measured as a cost, not as
  backpressure (the D-6 B requirement).

Environment: Apple M1, 8 cores, macOS 26.6.2, rustc 1.97.1. Direction
only: CLAUDE.md §11 puts the noise floor here at 10%.

### Results, medians of 5

```text
mode     size   v0 msg/s  v1 msg/s  v2 msg/s   v0→v1   v2→v1   p50 ms v0/v1     CPU µs/msg v0/v1/v2
memory     64    308 440   305 792   305 911   +0.9%   +0.0%   0.041/0.041      7.65/7.70/7.70
memory   1024    283 748   275 843   279 171   +2.8%   +1.2%   0.044/0.045      8.25/8.50/8.40
memory  65536     59 600    59 273    56 933   +0.5%   -4.1%   0.760/0.760     39.5/39.0/41.5
fsynced    64     12 069    11 972    11 957   +0.8%   -0.1%   7.849/7.899     25.0/26.0/25.0
fsynced  1024     11 530    11 377    11 419   +1.3%   +0.4%   8.262/8.424     32.0/33.5/33.0
fsynced 65536      4 402     4 270     4 259   +3.0%   -0.3%  12.879/13.089     185/190/185
quorum     64      5 391     5 192     5 399   +3.7%   +3.8%  17.458/17.419     44.5/44.0/43.0
quorum   1024      4 351     4 285     4 388   +1.5%   +2.3%  23.186/23.096     61.5/62.5/62.5
quorum  65536      2 442     2 514     2 429   -2.9%   -3.5%  36.288/35.499      345/340/355
```

`v0→v1` and `v2→v1` are `(baseline − after) / baseline`: positive means
v1 is slower. The spread of msg/s across the five rounds, max−min over the
median, was 1.8–18.7% per variant and point — wider than every difference
in the table.

PUBACK latency, medians of the per-run percentiles, ms (p50 / p95 / p99):

```text
mode     size   v0                         v1                         v2
memory     64    0.041 /  0.088 /  0.160    0.041 /  0.087 /  0.138    0.041 /  0.089 /  0.144
memory   1024    0.044 /  0.077 /  0.140    0.045 /  0.078 /  0.132    0.045 /  0.079 /  0.142
memory  65536    0.760 /  1.074 /  1.222    0.760 /  1.052 /  1.233    0.774 /  1.138 /  1.328
fsynced    64    7.849 /  9.521 / 13.008    7.899 /  9.569 / 11.524    7.893 /  9.434 / 11.321
fsynced  1024    8.262 /  9.996 / 11.283    8.424 /  9.978 / 12.333    8.231 /  9.996 / 14.014
fsynced 65536   12.879 / 68.088 / 83.262   13.089 / 70.162 / 84.591   13.488 / 69.086 / 80.098
quorum     64   17.458 / 25.387 / 28.034   17.419 / 26.122 / 29.085   16.829 / 25.888 / 28.762
quorum   1024   23.186 / 28.368 / 32.054   23.096 / 28.475 / 33.549   22.854 / 29.165 / 32.224
quorum  65536   36.288 / 67.848 / 78.740   35.499 / 66.994 / 75.113   36.340 / 66.657 / 77.597
```

Per point, unchanged across variants:

```text
fsynced   64/1 KiB  50 records per commit, sync median 3.9 ms
fsynced   64 KiB    38 records per commit, sync median 4.9–5.2 ms
quorum    64 B      40–42 records per leader commit, sync 3.9–4.0 ms
quorum    1 KiB     34–35 records per leader commit, sync 7.2–7.5 ms
quorum    64 KiB    29 records per leader commit, sync 6.0–6.8 ms
quorum    watermark reached every record in all 45 runs
every run         every PUBACK arrived; no shipment dropped
```

### Admission on its own

`runtime/benches/admission.rs`, divan, 1024 publishes per iteration, three
runs agreeing within 0.5%:

```text
baseline (the loop, no semaphore)            3.25 µs / 1024
permit per publish, returned one at a time  36.65 µs / 1024   → 32.6 ns per publish
permit per publish, returned 50 at a time   28.20 µs / 1024   → 24.4 ns per publish
allocations per publish                      0
```

50 at a time is the `fsynced` shape above; one at a time is the record that
was durable before its publish arrived.

### Conclusion

- **overhead_admission** = 24–33 ns per durable publish, measured, with no
  allocation — **uncontended**: one thread takes and returns the permits.
  Against the CPU a publish costs here that is about 0.1% at `fsynced` 64 B
  (26 µs), 0.06% at `quorum` 64 B (44 µs), and less for larger payloads, for
  one publisher. With many publishers, readers on several threads take
  permits while the broker task returns them, and the semaphore's cache line
  moves between cores; that contention is not measured here. `memory` pays
  nothing: it takes no permit.
- End to end, no difference between v0, v1 and v2 is resolvable on this
  machine. Every difference is inside its own round-to-round spread and the
  signs are mixed, including in `memory`, where admission does not run at
  all — which is what an effect of ~0.1% under a 10% noise floor looks like.
- `fsynced` is the sync: about 4 ms per commit, 50 records per commit,
  ~12 000 msg/s with one publisher whose window is 100. `quorum` adds the
  follower's sync on the same disk and the round trip: ~5 400 msg/s at
  64 B, p50 17 ms.
- Not measured here, and needed before any of these numbers is accepted: the
  same matrix on a quiet Linux host (CLAUDE.md §11).

### Fan-out: 1 publisher, 10 subscribers

The neighbouring workload for the broker loop A changed (D-6, Статус A):
v0 against v1, `memory` and `fsynced`, ten QoS 0 subscribers on the topic,
the same harness (`STRAUM_BENCH_SUBSCRIBERS=10`), 5 alternating rounds.
The PUBACK follows routing to all ten, so the interval pays for the fan-out.

```text
mode     size   v0 msg/s  v1 msg/s   v0→v1   spread v0/v1   p50/p99 ms v0 → v1             CPU µs/msg v0/v1
memory     64    160 682   160 103   +0.4%   12.2%/13.6%     0.385/0.737 → 0.398/0.749     33.45/33.55
memory   1024    139 081   141 241   -1.6%    8.0%/10.6%     0.381/0.816 → 0.387/0.820     39.60/39.05
memory  65536      4 448     4 436   +0.3%    3.0%/ 6.6%    19.068/32.705 → 20.581/34.806   1268/1253
fsynced    64     12 049    12 020   +0.2%    1.4%/ 1.3%     7.862/11.195 → 7.826/14.129      55.0/55.0
fsynced  1024     11 474    11 468   +0.1%    3.2%/ 2.9%     8.183/13.078 → 8.145/11.348      68.5/68.5
fsynced 65536      4 347     4 528   -4.2%   13.4%/16.4%    14.802/79.217 → 14.364/69.045      790/765
```

No difference is resolvable: every one is inside its round spread, with
mixed signs.

One run in ten (v1, `memory` 64 KiB) delivered 199 902 of 200 000. Not a
loss: under eight busy loops on the cores the same point drops in every run,
and a diagnostic build printing the broker's `dropped_messages` matched the
shortfall exactly every time (21 931 and 21 931, 13 337 and 13 337, ...).
A subscriber starved of CPU falls behind, its queue fills, and QoS 0 is
dropped — the overflow policy of CLAUDE.md §8. It is also invisible to an
operator today: the runtime does not export `dropped_messages` (card D-14).

The quiet run itself is attributed by that reproduction, not by its own
counter: the harness could not read `dropped_messages` in it, and the
event did not recur in eight quiet runs. The run stays in the medians
above. Since D-14 and D-15 the harness records it per row (`dropped`, the
last column): under eight busy loops, at every one of nine points over
three runs, the delivery shortfall equalled that column exactly — for
example 2 246 and 2 246, 19 781 and 19 781, 0 and 0.

### An anomaly, explained

`quorum` 1 KiB showed one catch-up from the log in one run out of five, in
all three variants, with no shipment dropped. Two mechanisms, both already
in 9dfa3bb and neither related to admission. A diagnostic build printed the
queue at each catch-up:

```text
CATCHUP sent=11550 local=11645 queued_shipments=1
CATCHUP sent=1086  local=1138  queued_shipments=0
```

1. `queued_shipments=1`: the replication task, woken by a follower report,
   checks `sent < local` at the top of its loop (§123) before it reads the
   shipment already waiting in its queue.
2. `queued_shipments=0`: the WAL writer publishes `local` before it queues
   the shipment, and the task looks in between.

In both cases the task reads from the log records that were about to
arrive, and the live shipment is then skipped as stale. Nothing is lost.
The cost is an extra log read, at most two per 20 000 messages here.

The attribution is measured, not read. A build that queues the shipment
before publishing `local` — the fix for mechanism 2 — did not remove the
catch-ups (6 in 24 runs against 4 in 24), which is what showed there was a
second cause. With the queue printed at each catch-up:

```text
v1 order (6 runs)              4 × queued_shipments=0   4 × queued_shipments=1
shipment queued first (10 runs) 0 × queued_shipments=0   5 × queued_shipments=1
```

The reorder removes every mechanism-2 catch-up; what remains is mechanism
1 alone.

### The previous replication table was measured wrong

`e2e/benches/replicate.rs` stops its clock when the last publish has been
*queued in the client*, not when its PUBACK arrives, and its `ack50/95/99`
columns are the time `publish()` took to queue. Reproduced today: its
`large 64 KiB local` row reads 12 136–15 736 msg/s with `ack50` = 0.01 ms.
An `fsynced` PUBACK cannot arrive in 10 µs when the sync takes ~5 ms. The
same point measured to the last PUBACK is 4 402 msg/s. The numbers in
docs/performance.md that came from that harness overstate throughput, by
up to ~4× at 64 KiB.

## 2026-10-02 — `replicate.rs` re-measured to the last PUBACK (D-11)

The harness now times from the first publish to the last PUBACK and pairs
each PUBACK with its PUBLISH by packet id; `rep` starts when the nth PUBLISH
was written. Same workloads as before, `fsynced`, `replicated` = an
asynchronous follower (a PUBACK does not wait for it). Medians of 5 runs.

```text
workload         mode         msg/s   range         ack50  ack95  ack99   rep50  rep99  m/lcmt
small 64 B       local       11 843   11178-12912    7.91   9.52   9.91      —      —    50.0
small 64 B       replicated   6 478    6352-6971    14.90  18.00  20.92   26.75  32.98   50.0
medium 1 KiB     local       11 700   10637-12654    8.60   9.96  10.05      —      —    50.0
medium 1 KiB     replicated   6 155    5990-6830    15.95  18.27  18.92   27.93  31.76   50.0
large 64 KiB     local        3 702    3445-3703    29.71  34.74  36.83      —      —    19.0
large 64 KiB     replicated   1 860    1823-1925    58.91  81.61  83.64   79.88 102.72   20.0
sustained 64 B   local       12 071   12040-12246    7.81   9.45  12.17      —      —    50.0
sustained 64 B   replicated   6 244    6226-6335    15.83  17.80  22.95   27.77  37.90   50.0
sustained 1 KiB  local       11 519   11197-11591    8.24   9.88  11.71      —      —    50.0
sustained 1 KiB  replicated   6 293    6181-6331    15.75  18.45  23.08   27.65  36.78   50.0
burst 64 B       local       12 114   11452-12498    8.03   9.82  12.88      —      —    50.0
burst 64 B       replicated   6 442    6099-6570    15.73  17.84  19.84   26.87  32.75   50.0
slow follower    (1 KiB, follower acknowledging nothing)   11 443 msg/s, range 11429-11494
```

What the correction changed, against the withdrawn table:

- **Long runs barely moved.** `sustained 1 KiB replicated` 6 323 → 6 293,
  `slow follower` 11 300 → 11 443. With 20 000 messages the client's queue
  tail the old clock missed is a small share of the run.
- **Short runs and large payloads did.** `large 64 KiB local` 16 899 →
  3 702, `replicated` 14 575 → 1 860; `small 64 B replicated` 7 500 → 6 478.
- `large 64 KiB local` here (3 702) is below `durability.rs` `fsynced`
  64 KiB (4 270) for a reason the counters show: 19 records per commit
  against 38. This run is 400 messages, and the adaptive batch starts at
  `STRAUM_WAL_INITIAL_BATCH_BYTES` (1 MiB, 16 records of 64 KiB) and does
  not grow far in a run that short. A different workload, not noise —
  confirmed by a control (D-16): the same workload at 2 000 messages,
  alternated with 400 over five pairs, gives 38.5 records per commit and
  4 842 msg/s (range 4 207–4 890), against 19.0 and 3 779 at 400. The
  `durability.rs` figure, 4 270, sits inside that range. To repeat it:
  change `replicate.rs:58` (the `large 64 KiB` workload) from `400` to
  `2_000`, build, run `STRAUM_BENCH_ONLY=large <binary> --bench` five
  times alternating with the unchanged build, and revert the edit.
- Hypothesis, not measured: an asynchronous follower roughly halves the
  leader's throughput and doubles its PUBACK latency (`ack50` 7.9 → 14.9 ms)
  although a PUBACK does not wait for it. Leader and follower are one
  process on one disk here, so each sync competes with the other's; on two
  machines that cost should not exist. A two-host run decides it.

## D-24: rewrites with a retained suffix — direction only (2026-10-03)

`STRAUM_BENCH_ONLY=replicated cargo bench -p e2e --bench durability`:
`fsynced` with an asynchronous follower on the same loopback, rewrites at the
64 MiB default, run long enough for two or three of them. `retain-64mib` is
the default `STRAUM_WAL_RETAIN_BYTES`; `retain-zero` is the same run with
`STRAUM_WAL_RETAIN_BYTES=0`, so a rewrite copies the live set only.

**One round, run one after the other rather than alternated, on this
machine.** It shows direction and nothing more (CLAUDE.md §11); the
accepted numbers come from the Linux run (D-13, D-17).

```text
payload  retain   count      msg/s   p50     p99     p99.9    max      rewrites
64 B     64 MiB   1 572 864  8 284   11.7    22.1    30.9     243.1    2
64 B     0        1 572 864  6 780   15.4    23.0    31.2     135.0    2
1 KiB    64 MiB     185 043  6 735   15.3    24.0    142.2    201.7    2
1 KiB    0          185 043  5 237   18.8    28.8    54.1     63.3     2
64 KiB   64 MiB       3 070  3 087   22.1    108.5   109.3    109.4    3
64 KiB   0            3 070  2 357   33.6    103.0   117.7    117.8    3
```

Latencies in ms, PUBACK per packet. What it suggests, as a hypothesis: the
retained suffix shows up in the slowest PUBACKs at 64 B and 1 KiB — a commit
waiting behind a rewrite that copies and syncs up to 64 MiB — and not in the
median. The throughput difference runs the other way and is larger than
anything the rewrite could explain, which with one sequential round says
more about this machine than about the change.

The rewrite's own duration was not in this table: the harness gained a
column for it (`wal_rewrite_nanos`, the latest rewrite, live-set scan
included) after these rows were taken. It holds the latest rewrite only:
with two or three rewrites a run, the column is one sample, not the
per-rewrite distribution D-24 is after. The Linux run reports it.

## D-28 stage 1: the log behind a `Disk` boundary — no measurable change (2026-10-04)

`storage` became generic over `Disk` (`OsDisk` in production, static
dispatch) and reads its clock through it. Baseline (HEAD 4279a35) and
candidate binaries alternated, 5 rounds, this machine, medians:

```text
benches/commit.rs, records/s      baseline   candidate
idle (one at a time)                   263         264
low 1k/s                             1 001       1 001
moderate 50k/s                      47 427      47 372
high (unpaced)                     664 782     717 634
high 4 KiB payload                  51 559      49 524
burst                              175 518     172 245

benches/scan.rs, median per run
scan_large 10 000                 3.777 ms    3.635 ms
scan_small 10 000                 211.6 µs    211.1 µs
scan_small 100 000                1.999 ms    2.072 ms
```

Every difference is inside the 10% noise floor of this machine; the
largest, `high (unpaced)`, runs in the candidate's favour. `benches/record.rs`
was run once per side as a control. It exercises code this change did not
touch, and its runs differed by up to 4× on single-nanosecond cases, which
is this machine's noise and not a result. Allocations per scan: one of
1.114 MB on both.

## D-28 stage 2: the leader's link decisions in `replication::Link` — no measurable change (2026-10-04)

The decisions of the leader's replication task moved from `runtime` into
`replication::Link` and `replication::Redial`; behaviour unchanged.
Baseline (before the move) and candidate binaries alternated, 5 rounds, this
machine, medians, msg/s:

```text
replicate.rs                 local              replicated
                          base     cand       base     cand
small 64 B              11 958   11 621      6 157    6 252
medium 1 KiB            10 682   10 800      5 935    5 990
large 64 KiB             3 573    3 633      1 894    1 961
sustained 64 B          11 736   11 421      6 106    6 121
sustained 1 KiB         10 994   10 970      6 104    6 172
burst 64 B              11 858   11 734      6 134    6 240

durability.rs, quorum     base     cand
64 B                     5 081    5 418
1 KiB                    4 201    4 169
64 KiB                   2 160    2 181
```

Every difference is within ±3.5% except quorum 64 B (+6.6%), whose
baseline spread across rounds was 4 567–5 478 (20%); all inside this
machine's noise floor. No change is claimed.

## ADR-0008 step 4 baseline: `codec`, `apply`, `gateway` before the hot path (2026-10-07)

Taken before step 4 puts the new path's code behind sockets (D-66 A).
Allocation bytes per operation are the stable measure on this machine
(they do not move between runs); times are shown for direction only and
varied by an order of magnitude between two runs of the same 64 B decode
(388 ns, then 10.67 µs), which is this Mac, not the code. `cargo bench -p
codec --bench codec`, `-p apply --bench apply`, `-p gateway --bench
gateway`, against `-p mqtt --bench decode` and `--bench encode`.

| Operation | Old path | New path | Note |
|---|---|---|---|
| CONNECT decode | 216 B | 456 B | properties struct and its defaults |
| PUBLISH 3.1.1 decode, 64 B | 104 B | 104 B | same: the payload copy below the threshold |
| PUBLISH 3.1.1 decode, 1 KiB | 40 B | 40 B | same |
| PUBLISH 5 decode, 64 B / 1 KiB, one user property | — | 520 B / 456 B | the boxed properties |
| SUBSCRIBE decode, 1 / 8 filters | 200 B / 200 B | 200 B / 200 B | same |
| PUBLISH 3.1.1 encode | 0 B | 16 B | the scratch body buffer marked `ponytail` in `encode.rs` |
| PUBLISH 5 encode | — | 8 B–16 B | scratch buffers |
| apply, one publish, at most once, 1 / 10 / 100 sessions | — | 674 B / 1.97 KB / 14.93 KB | ~150 B per delivery effect |
| apply, one publish, at least once, 1 / 10 / 100 sessions | — | 1.92 KB / 14.45 KB / 139.7 KB | ~1.4 KB per session: inflight copy and map nodes |
| gateway, delivery out, 64 B / 1 KiB | — | 528 B / 528 B | encode buffer and action |
| gateway, publish in, 64 B / 1 KiB | — | 715 B / 1.48 KB | decode, proposal body, action |

Medians (direction only): 3.1.1 publish decode 126–150 ns new against
131–134 ns old; encode 246–263 ns new against 41–66 ns old — the scratch
buffer and the general packet path; apply per at-least-once session about
0.45 µs at 100 sessions. Two candidates for phase 5, each to be measured on
Linux before it is changed: the encode scratch buffer, and the inflight
copy per at-least-once session in `apply`.

### The same benchmarks in the Linux container (preliminary)

`scripts/linux.sh bench -p codec -p apply -p gateway` (rust:1.85-bookworm in
the container VM on this Mac, 8 vCPU). It is Linux, but not a quiet Linux host:
CLAUDE.md §11 accepts no decision on these times. Allocation bytes match
the macOS run except the 3.1.1 encode (8 B here, 16 B there), the 5 encode
(16 B, 8–16 B there) and the gateway's 1 KiB publish in (1.551 KB, 1.48 KB
there); why is not investigated. Medians of divan's 100 samples:

| Benchmark | Median | Allocations / bytes |
|---|---|---|
| codec_connect_decode | 219.9 ns | 2 / 456 B |
| codec_publish_decode_v311 64 B / 1 KiB | 149 / 164.7 ns | 2 / 104 B, 1 / 40 B |
| codec_publish_decode_v5 64 B / 1 KiB | 298.5 / 306.4 ns | 4 / 520 B, 3 / 456 B |
| codec_publish_encode_v311 64 B / 1 KiB | 156.7 / 236.7 ns | 1 / 8 B |
| codec_publish_encode_v5 64 B / 1 KiB | 363.3 / 449.2 ns | 2 / 16 B |
| codec_subscribe_decode 1 / 8 filters | 242.7 ns / 1.207 µs | 2 / 200 B |
| apply_publish_at_most_once 1 / 10 / 100 subscribers | 483.2 ns / 2.353 µs / 20.49 µs | 6 / 674 B, 6 / 1.97 KB, 6 / 14.93 KB |
| apply_publish_at_least_once 1 / 10 / 100 subscribers | 597.7 ns / 3.122 µs / 39.23 µs | 7 / 1.922 KB, 16 / 14.45 KB, 106 / 139.7 KB |
| gateway_publish_in 64 B / 1 KiB | 275 / 384.6 ns | 3.9 / 714.7 B, 2.9 / 1.551 KB |
| gateway_deliver_out 64 B / 1 KiB | 414.9 / 488.4 ns | 3 / 528 B |
