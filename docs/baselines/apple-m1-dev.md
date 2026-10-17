# apple-m1-dev

A development Mac, not a benchmark host. Everything here shows **direction
only** (rules.md §11, docs/benchmarks.md §6): Apple Silicon moves threads
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
the effect in both directions, which under rules.md §11 means the
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
bound (rules.md §7); it is bounding the wrong product by a factor of three.

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
two task futures, channel metadata and allocator rounding. `engineering-rules.md`
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

* it removes work that provably does nothing (rules.md §11, first
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
only: rules.md §11 puts the noise floor here at 10%.

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
  same matrix on a quiet Linux host (rules.md §11).

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
dropped — the overflow policy of rules.md §8. It is also invisible to an
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
machine.** It shows direction and nothing more (rules.md §11); the
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
rules.md §11 accepts no decision on these times. Allocation bytes match
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

## `node/benches/cap.rs` — `max_uncommitted` against throughput and failover (D-84 A), preliminary

2026-10-09, Linux container (`scripts/linux.sh`, podman on Apple M1), so
**preliminary**: a VM's time and network (rules.md §11). Not a basis
for the default; D-84 A needs a quiet Linux host. Three nodes on
loopback, 64 B publishes, a closed loop of 512 in flight, 2 s × 3 rounds
(median); 10 failovers per cap: the sequencer is stopped while the load
runs (an orderly `shutdown` — no process kill in-process), timed to a
commit in a later slot on the others; heartbeat 50 ms, takeover after 3
heartbeats. The lag column is the largest survivor lag sampled just
before a stop, from separate nodes' status channels — an approximation.

```text
cap   publishes/s  failovers ≤ 1 s  p50 / max (ms)  survivor lag before the stop (records)
64        254 727       6 of 10      195 / 4 141           58 175
96        312 829       2 of 10      195 / 7 037           80 275
128       275 952       7 of 10      245 / 2 676           26 910
256       307 270       8 of 10      236 / 1 157           15 015
1024       16 261       0 of 10        - / -              231 985
4096        6 723      10 of 10      165 / 205                 64
```

What it shows, as a hypothesis until a Linux host repeats it:

- Failover misses 1 s where a survivor lagged far behind: a new slot
  commits only once the new majority holds the whole log, and while the
  old sequencer lived the slow replica was outside the majority that
  committed, so nothing kept it close. The lag does not follow the cap.
  `max_uncommitted` bounds the tail past the commit point, not how far a
  replica that is not needed for commit falls behind (D-85).
- Throughput collapses at caps 1024 and 4096 (16 k and 7 k against about
  300 k), the same in four runs; cause not found.
- A single run on macOS (one round, 3 failovers) gave 4 216 publishes/s at
  cap 64 and 2 237 at 4096, every failover ≈ 200 ms.

## D-124, the cost of measuring — preliminary (2026-10-12)

`scripts/overhead.sh .bench/base . 5` in the Linux container
(`rust:1.85-bookworm`, podman on this Mac): base 071db00 (no metrics),
head with D-112's metrics; five alternating rounds, the order flipped each
round; medians. `cap` at cap 64 (3 throughput rounds of 3 s, 5 failovers);
`puback` with 6 publishers (window 16, 64 B), 3 QoS 0 subscribers, 3 s
warm-up, 10 s measured.

| | base | head | |
|---|---|---|---|
| cap publishes/s | 241 971 | 239 667 | −1.0 % |
| cap failover p50 | 309 ms | 305 ms | −1.3 % |
| PUBACKs/s | 91 293 | 91 847 | +0.6 % |
| publish→PUBACK p50 | 0.980 ms | 1.012 ms | +3.3 % |
| publish→PUBACK p99 | 1.945 ms | 1.985 ms | +2.1 % |
| server CPU per 1 000 PUBACKs | 20.43 ms | 20.63 ms | +1.0 % |

Every difference is inside this machine's 10 % noise floor (rules.md
§11): a result, not proof of zero overhead. Read by D-129 A's rule
(`scripts/noise.awk`), the run could not have detected a 3 % effect at all:
each copy's spread between rounds was 2–29 % per quantity, and failover
p50's over 500 % (it is bimodal: about 300 ms, or about 1 975 ms). The
medians being within 3.3 % says nothing about the cost of measuring. Outliers: head round 3
(80 053 PUBACKs/s, p50 1.142 ms); one failover round near 1 980 ms in each
build. Not accepted until a quiet Linux host repeats it (D-124 A).

The same `puback` run on macOS directly (not in the container) gave 2 344
PUBACKs/s and a p50 of 40 ms, about 40 times slower than the container run
on the same machine: an effect of the environment, not diagnosed — not
attributed to any one macOS component (D-127). A lead, not a conclusion: the metrics
put a batch's append→durable p50 at about 16 ms on macOS directly (one run
of `STRAUM_BENCH_SECONDS=4 STRAUM_BENCH_WARMUP=2 STRAUM_BENCH_SCRAPE_MS=1000
cargo bench -p server --bench puback`, its bucket bound 16.384 ms) and
0.128 ms in the container (D-127's dry run, one round). One run each; no
diagnosis of the gap follows from them.

## D-133 A, catch-up by read size — preliminary (2026-10-15)

`puback` with `STRAUM_BENCH_EVENTS=kill`, 60 s, scraping every 1 s, in the
Linux container; three rounds per size, the order turned each round. Per
round, for the killed and restarted node: the lag's rate under load,
positions a second (negative: shrinking), and its recovery rate in the 10 s
after the load.

| read_bytes | lag rate under load (rounds) | recovery (rounds) |
|---|---|---|
| 64 KiB | −3 369, −3 577, −3 555 | 9 453, 14 498, 6 781 |
| 512 KiB | −4 668, −4 494, −3 837 | 0, 9 800, 6 910 |
| 4 MiB | −4 225, −3 341, −3 467 | 10 024, 6 582, 6 850 |

CPU over the 27.9 s from the restart, every run: the sequencer about
28.2 s (one full core), the catching-up replica about 0.3 s, the third node
about 0.5 s. The 512 KiB round 1 outlier: node 0 used 5.9 s and wrote
335 MB, and its recovery read 0. Not accepted; direction only.

## D-133 B, catch-up by replication window — preliminary (2026-10-15)

`STRAUM_BENCH_MODE=catchup cargo bench -p node --bench cap` in the Linux
container: three nodes in-process, closed-loop load of 512 at the
sequencer, a replica stopped 2 s and started again, its own lag read every
second for 20 s under load, then 10 s after; three rounds, the order turned.

| window | lag rate under load (rounds) | lag at the load's stop | recovery after |
|---|---|---|---|
| 256 | −2 079, −2 045, −2 035 | 817 765, 702 130, 801 255 | 6 413, 6 394, 6 374 |
| 1 024 | −25 153, −24 737, −25 290 | 294 515, 309 595, 320 580 | 29 452, 30 960, 32 058 |
| 4 096 | −30 835, −31 121, −31 205 | 0, 0, 0 | (caught up) |

Sequencer commit latency at every window: p50 1.024 ms, p99 131 ms (bucket
bounds). Not accepted; direction only.

## D-135 A, catch-up mechanism and cost — preliminary (2026-10-15)

`STRAUM_BENCH_MODE=catchup STRAUM_BENCH_HEARTBEATS=25,50,100 cargo bench -p node --bench cap` in the Linux container; three rounds, combinations turned each round. One line per trial: lag rate under load, lag at the load's stop, recovery, positions per tick, share of still heartbeats, bytes in flight (mean, most), positions in flight (mean), commit latency from raw samples (p50, p99, p99.9, max, ms).

```text
round=1 w=256 hb=25 rate=-2042 stop=528645 rec=7343 ppt=160 still=0.039 inflB=5763 inflMax=428168 inflPos=45 c50=65.167 c99=165.523 c999=276.280 cmax=318.264
round=2 w=256 hb=25 rate=-1892 stop=617045 rec=5900 ppt=144 still=0.002 inflB=3293 inflMax=428168 inflPos=26 c50=84.891 c99=103.105 c999=107.074 cmax=108.815
round=3 w=256 hb=25 rate=-1885 stop=625170 rec=5867 ppt=143 still=0.002 inflB=3705 inflMax=452870 inflPos=29 c50=85.257 c99=103.934 c999=106.615 cmax=107.032
round=1 w=256 hb=50 rate=-2370 stop=869700 rec=6868 ppt=371 still=0.007 inflB=5763 inflMax=263488 inflPos=45 c50=62.498 c99=79.343 c999=83.616 cmax=86.536
round=2 w=256 hb=50 rate=-1820 stop=762450 rec=5620 ppt=272 still=0.015 inflB=2470 inflMax=263488 inflPos=19 c50=1.095 c99=107.775 c999=110.853 cmax=113.481
round=3 w=256 hb=50 rate=-1884 stop=772915 rec=5854 ppt=282 still=0.018 inflB=4117 inflMax=263488 inflPos=32 c50=1.099 c99=103.690 c999=107.938 cmax=114.605
round=1 w=256 hb=100 rate=-2397 stop=743275 rec=7297 ppt=718 still=0.015 inflB=6175 inflMax=131744 inflPos=48 c50=1.307 c99=83.066 c999=89.852 cmax=94.036
round=2 w=256 hb=100 rate=-1857 stop=816335 rec=5750 ppt=555 still=0.015 inflB=4940 inflMax=164680 inflPos=39 c50=1.071 c99=105.354 c999=107.415 cmax=113.099
round=3 w=256 hb=100 rate=-1867 stop=809575 rec=5809 ppt=559 still=0.015 inflB=4940 inflMax=164680 inflPos=39 c50=1.093 c99=104.426 c999=112.718 cmax=119.388
round=1 w=1024 hb=25 rate=-27682 stop=132340 rec=13234 ppt=814 still=0.011 inflB=28819 inflMax=1770310 inflPos=227 c50=59.849 c99=81.929 c999=84.619 cmax=92.391
round=2 w=1024 hb=25 rate=-25935 stop=109330 rec=10933 ppt=747 still=0.010 inflB=20173 inflMax=1613864 inflPos=159 c50=55.498 c99=108.784 c999=117.227 cmax=124.780
round=3 w=1024 hb=25 rate=-25962 stop=119795 rec=11980 ppt=749 still=0.010 inflB=26348 inflMax=1671502 inflPos=208 c50=58.740 c99=105.499 c999=110.880 cmax=115.420
round=1 w=1024 hb=50 rate=-27015 stop=244270 rec=24427 ppt=1591 still=0.018 inflB=21408 inflMax=1111590 inflPos=169 c50=1.127 c99=79.719 c999=81.541 cmax=84.947
round=2 w=1024 hb=50 rate=-25365 stop=146900 rec=14690 ppt=1469 still=0.007 inflB=20173 inflMax=1053952 inflPos=159 c50=70.128 c99=106.508 c999=152.151 cmax=155.475
round=3 w=1024 hb=50 rate=-23101 stop=316030 rec=30522 ppt=1329 still=0.015 inflB=14409 inflMax=996314 inflPos=113 c50=1.108 c99=103.950 c999=134.358 cmax=152.128
round=1 w=1024 hb=100 rate=-26162 stop=307450 rec=30745 ppt=3088 still=0.015 inflB=15232 inflMax=625784 inflPos=120 c50=1.128 c99=81.238 c999=84.288 cmax=89.440
round=2 w=1024 hb=100 rate=-23166 stop=322335 rec=31607 ppt=2678 still=0.010 inflB=14409 inflMax=658720 inflPos=113 c50=1.147 c99=103.896 c999=109.861 cmax=115.959
round=3 w=1024 hb=100 rate=-22708 stop=371085 rec=29833 ppt=2612 still=0.015 inflB=23055 inflMax=625784 inflPos=182 c50=1.086 c99=104.597 c999=105.752 cmax=106.542
round=1 w=4096 hb=25 rate=-19907 stop=0 rec=caught_up ppt=3889 still=0.014 inflB=172914 inflMax=6908326 inflPos=682 c50=1.038 c99=67.437 c999=81.205 cmax=232.144
round=2 w=4096 hb=25 rate=-15902 stop=0 rec=caught_up ppt=2377 still=0.070 inflB=95102 inflMax=6751880 inflPos=750 c50=1.027 c99=104.415 c999=199.980 cmax=250.634
round=3 w=4096 hb=25 rate=-15370 stop=0 rec=caught_up ppt=3177 still=0.006 inflB=25937 inflMax=6595434 inflPos=204 c50=1.063 c99=88.366 c999=106.949 cmax=119.726
round=1 w=4096 hb=50 rate=-25135 stop=0 rec=caught_up ppt=4039 still=0.005 inflB=12762 inflMax=4199340 inflPos=100 c50=1.060 c99=103.464 c999=105.006 cmax=105.905
round=2 w=4096 hb=50 rate=-29590 stop=0 rec=caught_up ppt=1844 still=0.020 inflB=20585 inflMax=4273446 inflPos=162 c50=1.127 c99=101.723 c999=103.006 cmax=104.496
round=3 w=4096 hb=50 rate=-27967 stop=0 rec=caught_up ppt=2346 still=0.018 inflB=20585 inflMax=4215808 inflPos=162 c50=1.066 c99=106.284 c999=114.224 cmax=122.606
round=1 w=4096 hb=100 rate=-24689 stop=218790 rec=21879 ppt=2823 still=0.015 inflB=25937 inflMax=2692518 inflPos=204 c50=1.090 c99=107.668 c999=114.332 cmax=117.002
round=2 w=4096 hb=100 rate=-25793 stop=198770 rec=19877 ppt=2948 still=0.015 inflB=25937 inflMax=2659582 inflPos=204 c50=1.085 c99=102.828 c999=105.927 cmax=108.505
round=3 w=4096 hb=100 rate=-25296 stop=210340 rec=21034 ppt=2894 still=0.015 inflB=25937 inflMax=2659582 inflPos=204 c50=1.095 c99=104.530 c999=109.216 cmax=111.809
```

Not accepted; direction only.

## D-136 A, catch-up stages and control — preliminary (2026-10-16)

`STRAUM_BENCH_MODE=catchup STRAUM_BENCH_WINDOWS=256,1024,4096 cargo bench -p node --bench cap` in the Linux container, heartbeat 50 ms, three rounds; each trial paired with a control (same timing, no replica restarted), the pair's order turned each round. Per trial: lag, recovery and commit latency (ms, from raw samples). Per stage row (`stages`): each append shipped to a replica followed to the durable report covering it, matched by position over the process's one clock, p50/p90/p99 in ms; `_W` waiting, `_K` work; `path=log` is catch-up (a read before the send), `path=memory` the live path; `read_cycle` is one read asked to the next, `positions_per_read_mean` what each brought.

```text
round=1 catchup window=256 heartbeat_ms=50 lag_first=456430 lag_rate_under_load=-1369 lag_at_stop=430300 recovery_rate=3891 positions_per_tick=204 still_tick_share=0.005 bytes_in_flight_mean=0 bytes_in_flight_max=263488 positions_in_flight_mean=0 commit_samples=84288 commit_p50_ms=115.928 commit_p99_ms=152.048 commit_p999_ms=163.923 commit_max_ms=165.129
round=1 stages run=catchup window=256 heartbeat_ms=50 peer=lagging path=log n=841 round_trip=23.66/26.51/33.75 read_queue_W=0.03/0.04/0.13 read_K=23.17/24.61/27.41 read_return_W=0.08/0.10/0.14 ship_K=0.00/0.01/0.01 link_out_W=0.09/0.15/0.25 replica_loop_K=0.02/0.02/0.06 append_queue_W=0.04/0.40/9.24 write_K=0.03/0.07/0.20 sync_K=0.00/0.00/0.01 durable_return_W=0.02/0.41/0.52 report_K=0.00/0.00/0.00 link_back_W=0.05/0.10/0.18 next_ask_K=0.00/0.00/0.01 read_cycle=23.27/24.72/28.39 positions_per_read_mean=97
round=1 stages run=catchup window=256 heartbeat_ms=50 peer=healthy path=memory n=1316 round_trip=0.18/0.28/0.42 link_out_W=0.07/0.12/0.18 replica_loop_K=0.01/0.01/0.03 append_queue_W=0.03/0.07/0.13 write_K=0.02/0.03/0.08 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.07 report_K=0.00/0.00/0.00 link_back_W=0.02/0.05/0.10
round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=7652 still_tick_share=0.000 bytes_in_flight_mean=6998 bytes_in_flight_max=16468 positions_in_flight_mean=55 commit_samples=3007992 commit_p50_ms=2.019 commit_p99_ms=6.801 commit_p999_ms=21.330 commit_max_ms=104.934
round=1 stages run=control window=256 heartbeat_ms=50 peer=lagging path=memory n=46998 round_trip=0.29/0.50/1.08 link_out_W=0.10/0.18/0.34 replica_loop_K=0.01/0.02/0.06 append_queue_W=0.04/0.10/0.35 write_K=0.02/0.06/0.13 sync_K=0.00/0.00/0.01 durable_return_W=0.02/0.07/0.21 report_K=0.00/0.00/0.00 link_back_W=0.06/0.16/0.32
round=1 stages run=control window=256 heartbeat_ms=50 peer=healthy path=memory n=46999 round_trip=0.29/0.48/1.08 link_out_W=0.10/0.17/0.33 replica_loop_K=0.01/0.03/0.07 append_queue_W=0.04/0.10/0.33 write_K=0.02/0.06/0.13 sync_K=0.00/0.00/0.01 durable_return_W=0.02/0.06/0.19 report_K=0.00/0.00/0.00 link_back_W=0.06/0.16/0.31
round=1 catchup window=1024 heartbeat_ms=50 lag_first=397995 lag_rate_under_load=-18783 lag_at_stop=39195 recovery_rate=3920 positions_per_tick=1094 still_tick_share=0.013 bytes_in_flight_mean=14821 bytes_in_flight_max=1053952 positions_in_flight_mean=117 commit_samples=133376 commit_p50_ms=73.013 commit_p99_ms=169.722 commit_p999_ms=236.454 commit_max_ms=277.715
round=1 stages run=catchup window=1024 heartbeat_ms=50 peer=lagging path=log n=1064 round_trip=18.68/43.83/74.39 read_queue_W=0.03/0.06/0.28 read_K=16.36/24.62/36.98 read_return_W=0.08/0.11/0.20 ship_K=0.01/0.01/0.01 link_out_W=0.16/0.28/0.71 replica_loop_K=0.08/0.15/0.29 append_queue_W=0.22/20.87/34.70 write_K=0.14/0.24/0.41 sync_K=0.00/0.00/0.00 durable_return_W=0.17/0.58/0.96 report_K=0.00/0.00/0.00 link_back_W=0.06/0.12/0.32 next_ask_K=0.00/0.00/0.01 read_cycle=16.49/24.83/37.73 positions_per_read_mean=411
round=1 stages run=catchup window=1024 heartbeat_ms=50 peer=healthy path=memory n=2084 round_trip=0.18/0.34/0.70 link_out_W=0.07/0.11/0.30 replica_loop_K=0.01/0.02/0.04 append_queue_W=0.03/0.07/0.39 write_K=0.02/0.04/0.09 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.23 report_K=0.00/0.00/0.00 link_back_W=0.02/0.05/0.19
round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=12037 still_tick_share=0.000 bytes_in_flight_mean=6998 bytes_in_flight_max=16468 positions_in_flight_mean=55 commit_samples=4734562 commit_p50_ms=1.405 commit_p99_ms=2.091 commit_p999_ms=3.229 commit_max_ms=16.007
round=1 stages run=control window=1024 heartbeat_ms=50 peer=lagging path=log n=1 round_trip=8.63/8.63/8.63 read_queue_W=0.05/0.05/0.05 read_K=8.27/8.27/8.27 read_return_W=0.05/0.05/0.05 ship_K=0.01/0.01/0.01 link_out_W=0.12/0.12/0.12 replica_loop_K=0.01/0.01/0.01 append_queue_W=0.03/0.03/0.03 write_K=0.03/0.03/0.03 sync_K=0.00/0.00/0.00 durable_return_W=0.04/0.04/0.04 report_K=0.00/0.00/0.00 link_back_W=0.02/0.02/0.02
round=1 stages run=control window=1024 heartbeat_ms=50 peer=lagging path=memory n=47652 round_trip=0.20/0.30/0.46 link_out_W=0.08/0.11/0.14 replica_loop_K=0.01/0.02/0.03 append_queue_W=0.03/0.04/0.19 write_K=0.02/0.03/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.14 report_K=0.00/0.00/0.00 link_back_W=0.05/0.10/0.18
round=1 stages run=control window=1024 heartbeat_ms=50 peer=healthy path=log n=1 round_trip=15.30/15.30/15.30 read_queue_W=8.32/8.32/8.32 read_K=6.71/6.71/6.71 read_return_W=0.08/0.08/0.08 ship_K=0.01/0.01/0.01 link_out_W=0.08/0.08/0.08 replica_loop_K=0.01/0.01/0.01 append_queue_W=0.03/0.03/0.03 write_K=0.02/0.02/0.02 sync_K=0.00/0.00/0.00 durable_return_W=0.00/0.00/0.00 report_K=0.00/0.00/0.00 link_back_W=0.04/0.04/0.04
round=1 stages run=control window=1024 heartbeat_ms=50 peer=healthy path=memory n=47651 round_trip=0.19/0.27/0.41 link_out_W=0.07/0.11/0.13 replica_loop_K=0.01/0.02/0.03 append_queue_W=0.03/0.04/0.13 write_K=0.02/0.03/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.12 report_K=0.00/0.00/0.00 link_back_W=0.04/0.09/0.17
round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=423043
round=1 catchup window=4096 heartbeat_ms=50 lag_first=191295 lag_rate_under_load=-10054 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=4988 still_tick_share=0.018 bytes_in_flight_mean=440930 bytes_in_flight_max=3853512 positions_in_flight_mean=2544 commit_samples=1809758 commit_p50_ms=1.435 commit_p99_ms=52.001 commit_p999_ms=203.294 commit_max_ms=480.218
round=1 stages run=catchup window=4096 heartbeat_ms=50 peer=lagging path=log n=787 round_trip=23.53/33.23/419.72 read_queue_W=0.03/0.04/0.16 read_K=8.73/17.36/25.06 read_return_W=0.07/0.09/0.12 ship_K=0.01/0.07/0.16 link_out_W=0.18/0.35/0.62 replica_loop_K=0.09/0.14/0.54 append_queue_W=12.83/21.27/23.42 write_K=0.21/0.63/0.93 sync_K=0.00/0.00/0.00 durable_return_W=0.33/0.42/0.59 report_K=0.00/0.00/0.00 link_back_W=0.07/0.10/0.20 next_ask_K=0.00/0.00/0.01 read_cycle=8.97/33.28/55.58 positions_per_read_mean=452
round=1 stages run=catchup window=4096 heartbeat_ms=50 peer=lagging path=memory n=8945 round_trip=13.08/21.98/25.61 link_out_W=0.09/0.50/0.75 replica_loop_K=0.01/0.03/0.04 append_queue_W=12.07/20.85/23.79 write_K=0.69/0.90/1.06 sync_K=0.00/0.00/0.00 durable_return_W=0.02/0.08/0.43 report_K=0.00/0.00/0.00 link_back_W=0.04/0.10/0.24
round=1 stages run=catchup window=4096 heartbeat_ms=50 peer=healthy path=memory n=12623 round_trip=0.16/0.22/0.50 link_out_W=0.06/0.10/0.17 replica_loop_K=0.01/0.01/0.03 append_queue_W=0.03/0.04/0.14 write_K=0.02/0.03/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.07 report_K=0.00/0.00/0.00 link_back_W=0.02/0.05/0.07
round=1 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=11874 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=24702 positions_in_flight_mean=68 commit_samples=4692736 commit_p50_ms=1.412 commit_p99_ms=2.341 commit_p999_ms=3.325 commit_max_ms=9.150
round=1 stages run=control window=4096 heartbeat_ms=50 peer=lagging path=memory n=47654 round_trip=0.20/0.29/0.46 link_out_W=0.08/0.11/0.15 replica_loop_K=0.01/0.01/0.03 append_queue_W=0.03/0.04/0.19 write_K=0.02/0.03/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.14 report_K=0.00/0.00/0.00 link_back_W=0.05/0.09/0.17
round=1 stages run=control window=4096 heartbeat_ms=50 peer=healthy path=memory n=47654 round_trip=0.20/0.27/0.43 link_out_W=0.07/0.11/0.14 replica_loop_K=0.01/0.02/0.03 append_queue_W=0.03/0.05/0.19 write_K=0.02/0.03/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.11 report_K=0.00/0.00/0.00 link_back_W=0.04/0.09/0.16
round=1 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=404722
round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=12961 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=24702 positions_in_flight_mean=68 commit_samples=5211871 commit_p50_ms=1.301 commit_p99_ms=2.368 commit_p999_ms=5.019 commit_max_ms=27.014
round=2 stages run=control window=1024 heartbeat_ms=50 peer=lagging path=memory n=47650 round_trip=0.20/0.28/0.45 link_out_W=0.08/0.11/0.14 replica_loop_K=0.01/0.01/0.03 append_queue_W=0.03/0.04/0.17 write_K=0.02/0.03/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.14 report_K=0.00/0.00/0.00 link_back_W=0.05/0.09/0.17
round=2 stages run=control window=1024 heartbeat_ms=50 peer=healthy path=memory n=47650 round_trip=0.20/0.27/0.41 link_out_W=0.07/0.11/0.13 replica_loop_K=0.01/0.02/0.03 append_queue_W=0.03/0.04/0.19 write_K=0.02/0.03/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.12 report_K=0.00/0.00/0.00 link_back_W=0.04/0.09/0.16
round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=623954
round=2 catchup window=1024 heartbeat_ms=50 lag_first=625950 lag_rate_under_load=-24934 lag_at_stop=151320 recovery_rate=15132 positions_per_tick=1451 still_tick_share=0.013 bytes_in_flight_mean=14409 bytes_in_flight_max=1053952 positions_in_flight_mean=113 commit_samples=110848 commit_p50_ms=82.372 commit_p99_ms=105.184 commit_p999_ms=198.114 commit_max_ms=217.598
round=2 stages run=catchup window=1024 heartbeat_ms=50 peer=lagging path=log n=1331 round_trip=17.15/32.40/37.24 read_queue_W=0.04/0.05/0.08 read_K=16.64/17.28/18.08 read_return_W=0.08/0.10/0.12 ship_K=0.00/0.01/0.01 link_out_W=0.12/0.16/0.33 replica_loop_K=0.05/0.09/0.14 append_queue_W=0.11/14.25/18.50 write_K=0.08/0.14/0.20 sync_K=0.00/0.00/0.00 durable_return_W=0.05/0.31/0.36 report_K=0.00/0.00/0.00 link_back_W=0.05/0.07/0.10 next_ask_K=0.00/0.00/0.01 read_cycle=16.76/17.42/18.29 positions_per_read_mean=436
round=2 stages run=catchup window=1024 heartbeat_ms=50 peer=healthy path=memory n=1732 round_trip=0.13/0.24/0.39 link_out_W=0.05/0.08/0.22 replica_loop_K=0.01/0.01/0.03 append_queue_W=0.02/0.05/0.27 write_K=0.01/0.02/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.14 report_K=0.00/0.00/0.00 link_back_W=0.01/0.03/0.10
round=2 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15801 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=16468 positions_in_flight_mean=68 commit_samples=6250816 commit_p50_ms=1.059 commit_p99_ms=1.945 commit_p999_ms=3.945 commit_max_ms=7.905
round=2 stages run=control window=4096 heartbeat_ms=50 peer=lagging path=memory n=47642 round_trip=0.15/0.21/0.34 link_out_W=0.06/0.07/0.11 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.03/0.05/0.08 write_K=0.01/0.02/0.05 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.09 report_K=0.00/0.00/0.00 link_back_W=0.02/0.07/0.11
round=2 stages run=control window=4096 heartbeat_ms=50 peer=healthy path=memory n=47642 round_trip=0.17/0.21/0.36 link_out_W=0.07/0.09/0.12 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.02/0.04/0.09 write_K=0.01/0.02/0.05 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.08 report_K=0.00/0.00/0.00 link_back_W=0.04/0.07/0.11
round=2 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1063754
round=2 catchup window=4096 heartbeat_ms=50 lag_first=610740 lag_rate_under_load=-32092 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=3292 still_tick_share=0.018 bytes_in_flight_mean=18114 bytes_in_flight_max=4273446 positions_in_flight_mean=143 commit_samples=772288 commit_p50_ms=1.088 commit_p99_ms=79.584 commit_p999_ms=85.811 commit_max_ms=115.657
round=2 stages run=catchup window=4096 heartbeat_ms=50 peer=lagging path=log n=1618 round_trip=13.12/24.08/28.87 read_queue_W=0.04/0.05/0.10 read_K=12.62/13.41/15.14 read_return_W=0.08/0.10/0.11 ship_K=0.00/0.01/0.01 link_out_W=0.12/0.17/0.38 replica_loop_K=0.05/0.08/0.12 append_queue_W=0.04/9.50/13.70 write_K=0.08/0.13/0.17 sync_K=0.00/0.00/0.00 durable_return_W=0.04/0.31/0.36 report_K=0.00/0.00/0.00 link_back_W=0.05/0.07/0.13 next_ask_K=0.00/0.00/0.00 read_cycle=12.75/13.53/15.34 positions_per_read_mean=454
round=2 stages run=catchup window=4096 heartbeat_ms=50 peer=lagging path=memory n=9127 round_trip=0.15/0.24/0.46 link_out_W=0.06/0.07/0.16 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.03/0.05/0.10 write_K=0.01/0.02/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.04/0.10 report_K=0.00/0.00/0.00 link_back_W=0.02/0.07/0.14
round=2 stages run=catchup window=4096 heartbeat_ms=50 peer=healthy path=memory n=12067 round_trip=0.15/0.22/0.44 link_out_W=0.06/0.09/0.16 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.02/0.05/0.20 write_K=0.01/0.02/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.09 report_K=0.00/0.00/0.00 link_back_W=0.02/0.07/0.12
round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15946 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=24702 positions_in_flight_mean=65 commit_samples=6334935 commit_p50_ms=1.051 commit_p99_ms=1.655 commit_p999_ms=3.120 commit_max_ms=7.060
round=2 stages run=control window=256 heartbeat_ms=50 peer=lagging path=memory n=47649 round_trip=0.15/0.21/0.32 link_out_W=0.06/0.07/0.11 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.03/0.05/0.07 write_K=0.01/0.02/0.04 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.09 report_K=0.00/0.00/0.00 link_back_W=0.02/0.07/0.10
round=2 stages run=control window=256 heartbeat_ms=50 peer=healthy path=memory n=47649 round_trip=0.17/0.21/0.33 link_out_W=0.07/0.09/0.11 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.02/0.04/0.10 write_K=0.01/0.02/0.04 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.08 report_K=0.00/0.00/0.00 link_back_W=0.04/0.07/0.11
round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1098389
round=2 catchup window=256 heartbeat_ms=50 lag_first=786110 lag_rate_under_load=-2103 lag_at_stop=746005 recovery_rate=6563 positions_per_tick=320 still_tick_share=0.018 bytes_in_flight_mean=4940 bytes_in_flight_max=205850 positions_in_flight_mean=39 commit_samples=196980 commit_p50_ms=1.140 commit_p99_ms=92.184 commit_p999_ms=97.798 commit_max_ms=108.164
round=2 stages run=catchup window=256 heartbeat_ms=50 peer=lagging path=log n=1335 round_trip=15.12/28.85/32.36 read_queue_W=0.04/0.05/0.09 read_K=14.58/15.14/16.66 read_return_W=0.09/0.10/0.12 ship_K=0.00/0.00/0.01 link_out_W=0.08/0.12/0.24 replica_loop_K=0.01/0.02/0.03 append_queue_W=0.05/13.13/16.95 write_K=0.03/0.05/0.08 sync_K=0.00/0.00/0.01 durable_return_W=0.04/0.33/0.38 report_K=0.00/0.00/0.00 link_back_W=0.05/0.07/0.11 next_ask_K=0.00/0.00/0.00 read_cycle=14.70/15.26/16.91 positions_per_read_mean=96
round=2 stages run=catchup window=256 heartbeat_ms=50 peer=healthy path=memory n=3077 round_trip=0.12/0.18/0.32 link_out_W=0.05/0.08/0.14 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.02/0.04/0.08 write_K=0.01/0.02/0.05 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.02/0.05 report_K=0.00/0.00/0.00 link_back_W=0.01/0.03/0.05
round=3 catchup window=4096 heartbeat_ms=50 lag_first=609375 lag_rate_under_load=-32009 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=2468 still_tick_share=0.018 bytes_in_flight_mean=20585 bytes_in_flight_max=4273446 positions_in_flight_mean=162 commit_samples=456007 commit_p50_ms=1.093 commit_p99_ms=87.039 commit_p999_ms=120.236 commit_max_ms=138.991
round=3 stages run=catchup window=4096 heartbeat_ms=50 peer=lagging path=log n=1614 round_trip=13.06/27.06/40.19 read_queue_W=0.04/0.05/0.10 read_K=12.09/14.42/18.29 read_return_W=0.09/0.10/0.12 ship_K=0.00/0.01/0.01 link_out_W=0.13/0.17/0.39 replica_loop_K=0.05/0.09/0.19 append_queue_W=0.05/11.66/16.20 write_K=0.08/0.13/0.19 sync_K=0.00/0.00/0.00 durable_return_W=0.05/0.31/0.37 report_K=0.00/0.00/0.00 link_back_W=0.05/0.07/0.16 next_ask_K=0.00/0.00/0.00 read_cycle=13.51/14.54/18.52 positions_per_read_mean=454
round=3 stages run=catchup window=4096 heartbeat_ms=50 peer=lagging path=memory n=4127 round_trip=0.15/0.20/0.30 link_out_W=0.06/0.07/0.11 replica_loop_K=0.01/0.01/0.01 append_queue_W=0.04/0.04/0.06 write_K=0.01/0.02/0.03 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.09 report_K=0.00/0.00/0.00 link_back_W=0.02/0.07/0.10
round=3 stages run=catchup window=4096 heartbeat_ms=50 peer=healthy path=memory n=7126 round_trip=0.14/0.21/0.39 link_out_W=0.06/0.09/0.15 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.02/0.04/0.24 write_K=0.01/0.02/0.06 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.09 report_K=0.00/0.00/0.00 link_back_W=0.02/0.07/0.11
round=3 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15882 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=24702 positions_in_flight_mean=58 commit_samples=6253632 commit_p50_ms=1.063 commit_p99_ms=1.670 commit_p999_ms=3.554 commit_max_ms=28.945
round=3 stages run=control window=4096 heartbeat_ms=50 peer=lagging path=memory n=47642 round_trip=0.15/0.21/0.29 link_out_W=0.06/0.07/0.10 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.04/0.05/0.07 write_K=0.01/0.02/0.04 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.09 report_K=0.00/0.00/0.00 link_back_W=0.02/0.07/0.10
round=3 stages run=control window=4096 heartbeat_ms=50 peer=healthy path=memory n=47642 round_trip=0.16/0.20/0.29 link_out_W=0.06/0.09/0.10 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.02/0.04/0.07 write_K=0.01/0.02/0.04 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.07 report_K=0.00/0.00/0.00 link_back_W=0.04/0.07/0.10
round=3 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1065007
round=3 catchup window=256 heartbeat_ms=50 lag_first=794560 lag_rate_under_load=-2076 lag_at_stop=755040 recovery_rate=6446 positions_per_tick=314 still_tick_share=0.015 bytes_in_flight_mean=2058 bytes_in_flight_max=263488 positions_in_flight_mean=16 commit_samples=202886 commit_p50_ms=1.115 commit_p99_ms=98.572 commit_p999_ms=121.204 commit_max_ms=125.006
round=3 stages run=catchup window=256 heartbeat_ms=50 peer=lagging path=log n=1302 round_trip=15.88/28.52/32.35 read_queue_W=0.04/0.05/0.10 read_K=14.74/15.62/20.56 read_return_W=0.09/0.10/0.13 ship_K=0.00/0.00/0.01 link_out_W=0.09/0.13/0.30 replica_loop_K=0.01/0.02/0.04 append_queue_W=0.05/12.71/16.70 write_K=0.03/0.05/0.13 sync_K=0.00/0.00/0.00 durable_return_W=0.04/0.33/0.38 report_K=0.00/0.00/0.00 link_back_W=0.05/0.07/0.13 next_ask_K=0.00/0.00/0.00 read_cycle=14.86/15.74/20.71 positions_per_read_mean=96
round=3 stages run=catchup window=256 heartbeat_ms=50 peer=healthy path=memory n=3170 round_trip=0.12/0.18/0.32 link_out_W=0.05/0.09/0.14 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.02/0.04/0.08 write_K=0.01/0.02/0.05 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.02/0.04 report_K=0.00/0.00/0.00 link_back_W=0.01/0.03/0.06
round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15984 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=16468 positions_in_flight_mean=61 commit_samples=6344150 commit_p50_ms=1.055 commit_p99_ms=1.582 commit_p999_ms=3.303 commit_max_ms=6.479
round=3 stages run=control window=256 heartbeat_ms=50 peer=lagging path=memory n=47640 round_trip=0.15/0.21/0.30 link_out_W=0.06/0.07/0.10 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.04/0.04/0.06 write_K=0.01/0.02/0.03 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.09 report_K=0.00/0.00/0.00 link_back_W=0.02/0.07/0.11
round=3 stages run=control window=256 heartbeat_ms=50 peer=healthy path=memory n=47640 round_trip=0.17/0.21/0.30 link_out_W=0.07/0.09/0.11 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.02/0.04/0.07 write_K=0.01/0.02/0.04 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.02/0.08 report_K=0.00/0.00/0.00 link_back_W=0.04/0.07/0.11
round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1103041
round=3 catchup window=1024 heartbeat_ms=50 lag_first=755430 lag_rate_under_load=-25432 lag_at_stop=270400 recovery_rate=27040 positions_per_tick=1479 still_tick_share=0.020 bytes_in_flight_mean=41170 bytes_in_flight_max=1053952 positions_in_flight_mean=325 commit_samples=218496 commit_p50_ms=1.030 commit_p99_ms=97.156 commit_p999_ms=146.754 commit_max_ms=160.496
round=3 stages run=catchup window=1024 heartbeat_ms=50 peer=lagging path=log n=1387 round_trip=14.72/29.89/36.89 read_queue_W=0.04/0.05/0.09 read_K=14.20/15.38/18.11 read_return_W=0.08/0.10/0.12 ship_K=0.00/0.01/0.01 link_out_W=0.13/0.17/0.43 replica_loop_K=0.05/0.09/0.15 append_queue_W=0.11/13.77/17.85 write_K=0.10/0.14/0.20 sync_K=0.00/0.00/0.00 durable_return_W=0.06/0.31/0.38 report_K=0.00/0.00/0.00 link_back_W=0.05/0.07/0.13 next_ask_K=0.00/0.00/0.00 read_cycle=14.35/15.49/18.96 positions_per_read_mean=427
round=3 stages run=catchup window=1024 heartbeat_ms=50 peer=healthy path=memory n=3414 round_trip=0.11/0.18/0.39 link_out_W=0.05/0.07/0.18 replica_loop_K=0.01/0.01/0.03 append_queue_W=0.01/0.04/0.22 write_K=0.01/0.02/0.05 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.10 report_K=0.00/0.00/0.00 link_back_W=0.02/0.03/0.07
round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15806 still_tick_share=0.000 bytes_in_flight_mean=9057 bytes_in_flight_max=24702 positions_in_flight_mean=71 commit_samples=6270848 commit_p50_ms=1.066 commit_p99_ms=1.514 commit_p999_ms=2.889 commit_max_ms=5.844
round=3 stages run=control window=1024 heartbeat_ms=50 peer=lagging path=memory n=47649 round_trip=0.15/0.20/0.30 link_out_W=0.06/0.07/0.10 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.04/0.04/0.06 write_K=0.01/0.02/0.03 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.03/0.09 report_K=0.00/0.00/0.00 link_back_W=0.02/0.07/0.10
round=3 stages run=control window=1024 heartbeat_ms=50 peer=healthy path=memory n=47648 round_trip=0.17/0.20/0.30 link_out_W=0.07/0.09/0.10 replica_loop_K=0.01/0.01/0.02 append_queue_W=0.02/0.04/0.07 write_K=0.01/0.02/0.04 sync_K=0.00/0.00/0.00 durable_return_W=0.01/0.02/0.08 report_K=0.00/0.00/0.00 link_back_W=0.04/0.07/0.11
round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1071739
```

Not accepted; direction only. fsync costs about 0 in this container (`sync_K`), so the replica's sync stage is not represented. Rows marked `stamps_dropped` filled the trace (2^19 stamps a node) early in the window; their samples come from its first part.

## D-139 A, where a catch-up read spends its time — preliminary (2026-10-16)

The D-136 A run again (`STRAUM_BENCH_MODE=catchup STRAUM_BENCH_WINDOWS=256,1024,4096`, 50 ms, three rounds, each trial with its control), with two more rows per trial. `scan`: the sequencer's reads for the lagging replica — read time split where the read reached its start (`before_ms`), kept what it needed (`keep_ms`) and went on to the segment's end (`after_ms`); the bytes scanned before and after; the scan rate over them; read time against bytes scanned (Pearson r, least-squares ms a MB). `blocking`: the lagging replica's appends waiting for its log thread, the wait attributed by overlap to the thread's earlier groups, read-backs for apply (`Range`), reads, and none of these; and the read-backs themselves.

```text
round=1 catchup window=256 heartbeat_ms=50 lag_first=788645 lag_rate_under_load=-2120 lag_at_stop=748280 recovery_rate=6550 positions_per_tick=318 still_tick_share=0.018 bytes_in_flight_mean=3293 bytes_in_flight_max=263488 positions_in_flight_mean=26 commit_samples=203264 commit_p50_ms=1.112 commit_p99_ms=91.557 commit_p999_ms=97.567 commit_max_ms=100.049
round=1 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1318 read_ms=14.78/15.13/16.09 before_ms=6.12/7.61/7.96 keep_ms=0.01/0.02/0.02 after_ms=8.63/10.22/10.95 before_MB=23.91/30.42/31.88 after_MB=39.22/45.61/47.04 scan_MBps=4265 r=0.030 ms_per_MB=3.007 intercept_ms=-174.98
round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1318 waited=1318 wait_ms=0.05/13.63/17.58 wait_total_ms=5091 by_groups_ms=0 by_range_ms=5061 by_reads_ms=0 unattributed_ms=30 ranges=610 range_ms=17.61/18.12/19.40 range_visited_MB_mean=55.1 range_records_mean=1024
round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15706 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=24702 positions_in_flight_mean=65 commit_samples=6242560 commit_p50_ms=1.056 commit_p99_ms=2.013 commit_p999_ms=3.889 commit_max_ms=85.517
round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1060598
round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65563 waited=65563 wait_ms=0.04/0.04/0.07 wait_total_ms=2196 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2195 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
round=1 catchup window=1024 heartbeat_ms=50 lag_first=732745 lag_rate_under_load=-21937 lag_at_stop=315185 recovery_rate=29833 positions_per_tick=1264 still_tick_share=0.020 bytes_in_flight_mean=20585 bytes_in_flight_max=1053952 positions_in_flight_mean=162 commit_samples=185856 commit_p50_ms=1.104 commit_p99_ms=109.258 commit_p999_ms=175.719 commit_max_ms=195.918
round=1 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1165 read_ms=17.46/18.02/20.04 before_ms=9.04/15.39/17.18 keep_ms=0.03/0.04/0.05 after_ms=8.63/12.65/13.93 before_MB=31.11/56.40/62.45 after_MB=31.96/50.10/55.28 scan_MBps=3664 r=0.939 ms_per_MB=0.540 intercept_ms=-16.38
round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1159 waited=1159 wait_ms=0.64/15.28/19.33 wait_total_ms=5577 by_groups_ms=0 by_range_ms=5546 by_reads_ms=0 unattributed_ms=31 ranges=624 range_ms=19.19/19.84/22.86 range_visited_MB_mean=59.8 range_records_mean=1024
round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15790 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=57638 positions_in_flight_mean=61 commit_samples=6217216 commit_p50_ms=1.073 commit_p99_ms=1.523 commit_p999_ms=2.903 commit_max_ms=9.102
round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1049780
round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65539 waited=65539 wait_ms=0.04/0.04/0.06 wait_total_ms=2221 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2218 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
round=1 catchup window=4096 heartbeat_ms=50 lag_first=549380 lag_rate_under_load=-28173 lag_at_stop=13130 recovery_rate=1313 positions_per_tick=1605 still_tick_share=0.018 bytes_in_flight_mean=20173 bytes_in_flight_max=4273446 positions_in_flight_mean=159 commit_samples=166912 commit_p50_ms=40.257 commit_p99_ms=110.283 commit_p999_ms=115.010 commit_max_ms=117.833
round=1 stages run=catchup window=4096 heartbeat_ms=50 scan peer=lagging reads=1422 read_ms=17.49/18.19/19.00 before_ms=7.67/15.55/17.35 keep_ms=0.03/0.04/0.05 after_ms=4.99/11.02/12.83 before_MB=28.49/54.89/62.27 after_MB=19.70/38.84/46.11 scan_MBps=3674 r=0.997 ms_per_MB=0.330 intercept_ms=-2.87
round=1 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=1414 waited=1414 wait_ms=0.05/13.85/18.54 wait_total_ms=5212 by_groups_ms=0 by_range_ms=5168 by_reads_ms=0 unattributed_ms=44 ranges=614 range_ms=18.68/19.05/19.93 range_visited_MB_mean=59.9 range_records_mean=1024
round=1 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15999 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=526976 positions_in_flight_mean=68 commit_samples=6350400 commit_p50_ms=1.056 commit_p99_ms=1.446 commit_p999_ms=3.079 commit_max_ms=8.792
round=1 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1100195
round=1 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65564 waited=65564 wait_ms=0.04/0.05/0.07 wait_total_ms=2245 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2242 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15577 still_tick_share=0.000 bytes_in_flight_mean=9057 bytes_in_flight_max=16468 positions_in_flight_mean=71 commit_samples=6182784 commit_p50_ms=1.071 commit_p99_ms=1.867 commit_p999_ms=4.088 commit_max_ms=16.247
round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1034736
round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65566 waited=65566 wait_ms=0.04/0.04/0.07 wait_total_ms=2258 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2256 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
round=2 catchup window=1024 heartbeat_ms=50 lag_first=722670 lag_rate_under_load=-21572 lag_at_stop=311610 recovery_rate=29852 positions_per_tick=1242 still_tick_share=0.017 bytes_in_flight_mean=17291 bytes_in_flight_max=1053952 positions_in_flight_mean=136 commit_samples=184768 commit_p50_ms=1.107 commit_p99_ms=115.291 commit_p999_ms=142.498 commit_max_ms=152.859
round=2 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1141 read_ms=17.62/18.63/20.33 before_ms=9.10/16.15/17.69 keep_ms=0.03/0.04/0.05 after_ms=8.82/12.79/14.13 before_MB=31.31/56.50/62.44 after_MB=31.76/47.39/51.32 scan_MBps=3550 r=0.724 ms_per_MB=0.386 intercept_ms=-6.32
round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1139 waited=1139 wait_ms=0.31/15.68/19.54 wait_total_ms=5521 by_groups_ms=0 by_range_ms=5491 by_reads_ms=0 unattributed_ms=30 ranges=600 range_ms=19.32/20.43/22.96 range_visited_MB_mean=59.6 range_records_mean=1024
round=2 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15952 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=16468 positions_in_flight_mean=58 commit_samples=6315914 commit_p50_ms=1.055 commit_p99_ms=1.960 commit_p999_ms=3.911 commit_max_ms=21.137
round=2 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1090727
round=2 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65599 waited=65597 wait_ms=0.03/0.05/0.09 wait_total_ms=2165 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2163 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
round=2 catchup window=4096 heartbeat_ms=50 lag_first=561795 lag_rate_under_load=-26578 lag_at_stop=56030 recovery_rate=5603 positions_per_tick=1516 still_tick_share=0.018 bytes_in_flight_mean=23055 bytes_in_flight_max=4273446 positions_in_flight_mean=182 commit_samples=160832 commit_p50_ms=36.868 commit_p99_ms=108.744 commit_p999_ms=115.143 commit_max_ms=118.417
round=2 stages run=catchup window=4096 heartbeat_ms=50 scan peer=lagging reads=1345 read_ms=17.30/18.00/19.18 before_ms=7.05/15.28/17.21 keep_ms=0.03/0.04/0.05 after_ms=5.57/12.85/14.73 before_MB=24.35/55.36/62.33 after_MB=21.57/46.96/53.82 scan_MBps=3621 r=0.994 ms_per_MB=0.290 intercept_ms=-0.69
round=2 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=1337 waited=1337 wait_ms=0.11/14.63/19.16 wait_total_ms=5312 by_groups_ms=0 by_range_ms=5269 by_reads_ms=0 unattributed_ms=42 ranges=555 range_ms=19.30/20.10/22.45 range_visited_MB_mean=60.4 range_records_mean=1024
round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15727 still_tick_share=0.000 bytes_in_flight_mean=6175 bytes_in_flight_max=16468 positions_in_flight_mean=48 commit_samples=6218834 commit_p50_ms=1.070 commit_p99_ms=1.688 commit_p999_ms=3.422 commit_max_ms=21.456
round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1050627
round=2 stages run=control window=256 heartbeat_ms=50 scan peer=lagging reads=1 read_ms=9.34/9.34/9.34 before_ms=9.33/9.33/9.33 keep_ms=0.00/0.00/0.00 after_ms=0.01/0.01/0.01 before_MB=35.61/35.61/35.61 after_MB=0.00/0.00/0.00 scan_MBps=3813 r=0.000 ms_per_MB=0.000 intercept_ms=9.34
round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65573 waited=65572 wait_ms=0.04/0.04/0.07 wait_total_ms=2190 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2189 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
round=2 catchup window=256 heartbeat_ms=50 lag_first=699270 lag_rate_under_load=-1676 lag_at_stop=667290 recovery_rate=5237 positions_per_tick=257 still_tick_share=0.005 bytes_in_flight_mean=2470 bytes_in_flight_max=263488 positions_in_flight_mean=19 commit_samples=116225 commit_p50_ms=88.856 commit_p99_ms=130.565 commit_p999_ms=152.807 commit_max_ms=156.952
round=2 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1067 read_ms=17.92/20.22/25.79 before_ms=4.88/6.64/7.94 keep_ms=0.01/0.02/0.03 after_ms=13.23/15.37/19.32 before_MB=16.44/21.72/22.91 after_MB=46.67/51.83/52.99 scan_MBps=3409 r=-0.002 ms_per_MB=-0.901 intercept_ms=75.41
round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1066 waited=1066 wait_ms=0.20/15.84/21.18 wait_total_ms=5219 by_groups_ms=0 by_range_ms=5192 by_reads_ms=0 unattributed_ms=27 ranges=564 range_ms=19.12/21.66/28.27 range_visited_MB_mean=56.8 range_records_mean=1024
round=3 catchup window=4096 heartbeat_ms=50 lag_first=545545 lag_rate_under_load=-27317 lag_at_stop=25545 recovery_rate=2554 positions_per_tick=1558 still_tick_share=0.018 bytes_in_flight_mean=28819 bytes_in_flight_max=4273446 positions_in_flight_mean=227 commit_samples=163968 commit_p50_ms=41.557 commit_p99_ms=110.074 commit_p999_ms=114.447 commit_max_ms=118.044
round=3 stages run=catchup window=4096 heartbeat_ms=50 scan peer=lagging reads=1382 read_ms=17.41/18.12/19.16 before_ms=7.56/15.60/17.24 keep_ms=0.03/0.04/0.05 after_ms=5.26/11.59/13.08 before_MB=26.97/55.15/62.36 after_MB=19.98/41.00/48.09 scan_MBps=3589 r=0.984 ms_per_MB=0.291 intercept_ms=-0.60
round=3 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=1374 waited=1374 wait_ms=0.11/13.86/18.74 wait_total_ms=5130 by_groups_ms=0 by_range_ms=5088 by_reads_ms=0 unattributed_ms=42 ranges=600 range_ms=18.82/19.51/20.82 range_visited_MB_mean=59.9 range_records_mean=1024
round=3 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15806 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=526976 positions_in_flight_mean=65 commit_samples=6271998 commit_p50_ms=1.066 commit_p99_ms=1.554 commit_p999_ms=3.268 commit_max_ms=23.532
round=3 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1069740
round=3 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65565 waited=65565 wait_ms=0.04/0.04/0.07 wait_total_ms=2234 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2231 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
round=3 catchup window=256 heartbeat_ms=50 lag_first=556335 lag_rate_under_load=-2254 lag_at_stop=513435 recovery_rate=7122 positions_per_tick=347 still_tick_share=0.015 bytes_in_flight_mean=4940 bytes_in_flight_max=263488 positions_in_flight_mean=39 commit_samples=209600 commit_p50_ms=1.185 commit_p99_ms=83.290 commit_p999_ms=87.692 commit_max_ms=91.294
round=3 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1454 read_ms=13.34/13.72/14.61 before_ms=5.81/7.28/7.81 keep_ms=0.01/0.01/0.02 after_ms=7.41/8.96/9.64 before_MB=25.73/32.91/34.53 after_MB=37.40/44.29/45.81 scan_MBps=4713 r=0.040 ms_per_MB=3.825 intercept_ms=-228.03
round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1454 waited=1454 wait_ms=0.05/12.49/16.44 wait_total_ms=5058 by_groups_ms=0 by_range_ms=5022 by_reads_ms=0 unattributed_ms=35 ranges=619 range_ms=16.70/17.31/18.24 range_visited_MB_mean=54.9 range_records_mean=1024
round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15991 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=16468 positions_in_flight_mean=65 commit_samples=6345558 commit_p50_ms=1.028 commit_p99_ms=2.417 commit_p999_ms=4.931 commit_max_ms=62.552
round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1101008
round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65718 waited=65718 wait_ms=0.02/0.05/0.13 wait_total_ms=1926 by_groups_ms=5 by_range_ms=0 by_reads_ms=0 unattributed_ms=1921 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
round=3 catchup window=1024 heartbeat_ms=50 lag_first=769210 lag_rate_under_load=-25468 lag_at_stop=284440 recovery_rate=28444 positions_per_tick=1482 still_tick_share=0.018 bytes_in_flight_mean=20173 bytes_in_flight_max=1053952 positions_in_flight_mean=159 commit_samples=207484 commit_p50_ms=1.155 commit_p99_ms=90.298 commit_p999_ms=106.980 commit_max_ms=118.512
round=3 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1403 read_ms=14.42/14.90/16.64 before_ms=5.90/12.95/14.42 keep_ms=0.03/0.04/0.04 after_ms=8.12/11.86/13.37 before_MB=24.34/55.04/62.30 after_MB=37.30/51.83/57.90 scan_MBps=4372 r=0.914 ms_per_MB=0.299 intercept_ms=-4.20
round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1401 waited=1401 wait_ms=0.05/13.44/17.32 wait_total_ms=5220 by_groups_ms=0 by_range_ms=5177 by_reads_ms=0 unattributed_ms=43 ranges=522 range_ms=17.48/18.16/22.77 range_visited_MB_mean=61.2 range_records_mean=1024
round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15667 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=41170 positions_in_flight_mean=61 commit_samples=6193365 commit_p50_ms=1.069 commit_p99_ms=1.805 commit_p999_ms=3.982 commit_max_ms=10.048
round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1040177
round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65560 waited=65559 wait_ms=0.04/0.04/0.07 wait_total_ms=2209 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2206 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
```

Not accepted; direction only. The segments were in the page cache (scan rates of 3.4–4.7 GB/s); a read from a cold disk is not represented. At window 256 the bytes scanned per read hardly vary (the whole segment each time), so r there says nothing.

## D-142 A, early stop and read cursor, each alone and together — preliminary (2026-10-16)

Four source trees of the same change, interleaved round by round (round 1: base, early, cursor, both; round 2 reversed; round 3: early, both, base, cursor): `base` with both switched off — D-139's path, a read replaying its segment from the start and on to its end; `early` stopping a read and a read-back once they have what they need; `cursor` resuming a sequential read where the last one left off; `both`. `STRAUM_BENCH_MODE=catchup STRAUM_BENCH_WINDOWS=256,1024 cargo bench -p node --bench cap` in the Linux container, 50 ms, one round per tree per round, each trial with its control. The `scan` rows still carry D-139 A's split of read time (before the read's start, kept, after it); that counting was removed once this was recorded.

```text
variant=base round=1 catchup window=256 heartbeat_ms=50 lag_first=933205 lag_rate_under_load=-2319 lag_at_stop=889070 recovery_rate=7258 positions_per_tick=349 still_tick_share=0.015 bytes_in_flight_mean=3293 bytes_in_flight_max=205850 positions_in_flight_mean=26 commit_samples=228352 commit_p50_ms=0.913 commit_p99_ms=84.750 commit_p999_ms=95.238 commit_max_ms=105.689
variant=base round=1 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1448 read_ms=13.38/14.00/15.11 before_ms=7.83/9.43/9.75 keep_ms=0.01/0.02/0.02 after_ms=5.32/6.86/7.22 before_MB=35.64/42.80/44.41 after_MB=27.49/34.49/36.09 scan_MBps=4685 r=0.011 ms_per_MB=1.519 intercept_ms=-82.41
variant=base round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1447 waited=1447 wait_ms=0.06/11.92/15.79 wait_total_ms=4892 by_groups_ms=0 by_range_ms=4860 by_reads_ms=0 unattributed_ms=32 ranges=694 range_ms=15.72/16.65/20.43 range_visited_MB_mean=54.6 range_records_mean=1024
variant=base round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16949 still_tick_share=0.000 bytes_in_flight_mean=6998 bytes_in_flight_max=16468 positions_in_flight_mean=55 commit_samples=6376068 commit_p50_ms=1.057 commit_p99_ms=1.519 commit_p999_ms=3.125 commit_max_ms=5.961
variant=base round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1115404
variant=base round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65619 waited=65619 wait_ms=0.04/0.04/0.07 wait_total_ms=2120 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2117 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=base round=1 catchup window=1024 heartbeat_ms=50 lag_first=746785 lag_rate_under_load=-25064 lag_at_stop=268710 recovery_rate=26871 positions_per_tick=1446 still_tick_share=0.017 bytes_in_flight_mean=32936 bytes_in_flight_max=1053952 positions_in_flight_mean=260 commit_samples=199616 commit_p50_ms=1.126 commit_p99_ms=94.985 commit_p999_ms=107.855 commit_max_ms=138.175
variant=base round=1 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1355 read_ms=15.27/15.70/16.75 before_ms=7.13/13.61/15.26 keep_ms=0.03/0.04/0.05 after_ms=7.51/10.64/11.51 before_MB=26.66/55.30/62.33 after_MB=35.17/48.64/54.44 scan_MBps=4240 r=0.977 ms_per_MB=0.516 intercept_ms=-17.10
variant=base round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1349 waited=1349 wait_ms=0.11/13.81/17.70 wait_total_ms=5328 by_groups_ms=0 by_range_ms=5293 by_reads_ms=0 unattributed_ms=35 ranges=597 range_ms=17.75/18.32/20.16 range_visited_MB_mean=59.6 range_records_mean=1024
variant=base round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15661 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=24702 positions_in_flight_mean=68 commit_samples=6189655 commit_p50_ms=1.066 commit_p99_ms=1.436 commit_p999_ms=3.281 commit_max_ms=218.908
variant=base round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1037637
variant=base round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65580 waited=65580 wait_ms=0.04/0.04/0.06 wait_total_ms=2220 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2216 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=early round=1 catchup window=256 heartbeat_ms=50 lag_first=822055 lag_rate_under_load=7599 lag_at_stop=966680 recovery_rate=8298 positions_per_tick=676 still_tick_share=0.018 bytes_in_flight_mean=7410 bytes_in_flight_max=205850 positions_in_flight_mean=58 commit_samples=519808 commit_p50_ms=1.222 commit_p99_ms=59.679 commit_p999_ms=63.085 commit_max_ms=63.842
variant=early round=1 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=2617 read_ms=7.13/9.80/10.41 before_ms=7.11/9.78/10.39 keep_ms=0.01/0.02/0.02 after_ms=0.01/0.01/0.01 before_MB=32.95/45.89/48.79 after_MB=0.00/0.00/0.00 scan_MBps=4614 r=0.997 ms_per_MB=0.204 intercept_ms=0.44
variant=early round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=2616 waited=2616 wait_ms=0.03/5.21/9.86 wait_total_ms=3293 by_groups_ms=0 by_range_ms=3232 by_reads_ms=0 unattributed_ms=61 ranges=602 range_ms=7.05/14.81/16.22 range_visited_MB_mean=27.0 range_records_mean=1024
variant=early round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15935 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=32936 positions_in_flight_mean=65 commit_samples=6312064 commit_p50_ms=1.060 commit_p99_ms=1.389 commit_p999_ms=2.761 commit_max_ms=43.984
variant=early round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1088782
variant=early round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65577 waited=65577 wait_ms=0.04/0.04/0.06 wait_total_ms=2245 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2244 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=early round=1 catchup window=1024 heartbeat_ms=50 lag_first=768170 lag_rate_under_load=-40361 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=4701 still_tick_share=0.018 bytes_in_flight_mean=15644 bytes_in_flight_max=996314 positions_in_flight_mean=123 commit_samples=1185970 commit_p50_ms=1.078 commit_p99_ms=69.439 commit_p999_ms=83.992 commit_max_ms=87.772
variant=early round=1 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=2253 read_ms=7.63/12.72/14.25 before_ms=7.59/12.68/14.21 keep_ms=0.03/0.04/0.05 after_ms=0.01/0.01/0.01 before_MB=32.33/56.63/62.48 after_MB=0.00/0.00/0.00 scan_MBps=4376 r=0.995 ms_per_MB=0.227 intercept_ms=0.08
variant=early round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=16683 waited=16683 wait_ms=0.04/0.05/8.40 wait_total_ms=4108 by_groups_ms=38 by_range_ms=3514 by_reads_ms=0 unattributed_ms=555 ranges=623 range_ms=6.74/16.10/17.98 range_visited_MB_mean=26.7 range_records_mean=1024
variant=early round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16072 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=16468 positions_in_flight_mean=65 commit_samples=6353351 commit_p50_ms=1.059 commit_p99_ms=1.459 commit_p999_ms=2.860 commit_max_ms=6.814
variant=early round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1106289
variant=early round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65575 waited=65575 wait_ms=0.04/0.04/0.07 wait_total_ms=2252 by_groups_ms=5 by_range_ms=0 by_reads_ms=0 unattributed_ms=2247 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=cursor round=1 catchup window=256 heartbeat_ms=50 lag_first=948415 lag_rate_under_load=36306 lag_at_stop=1639755 recovery_rate=12939 positions_per_tick=710 still_tick_share=0.007 bytes_in_flight_mean=9880 bytes_in_flight_max=263488 positions_in_flight_mean=78 commit_samples=1144256 commit_p50_ms=1.075 commit_p99_ms=65.259 commit_p999_ms=74.385 commit_max_ms=76.282
variant=cursor round=1 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=2589 read_ms=9.23/11.31/12.36 before_ms=0.04/0.05/0.11 keep_ms=0.01/0.01/0.03 after_ms=9.18/11.26/12.30 before_MB=0.00/0.00/0.00 after_MB=47.14/59.94/62.81 scan_MBps=5237 r=0.999 ms_per_MB=0.192 intercept_ms=-0.04
variant=cursor round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=2588 waited=2588 wait_ms=0.11/9.02/10.81 wait_total_ms=6939 by_groups_ms=0 by_range_ms=6869 by_reads_ms=0 unattributed_ms=70 ranges=828 range_ms=7.81/12.81/15.56 range_visited_MB_mean=35.2 range_records_mean=1024
variant=cursor round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15965 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=16468 positions_in_flight_mean=65 commit_samples=6304512 commit_p50_ms=1.060 commit_p99_ms=1.518 commit_p999_ms=3.112 commit_max_ms=10.821
variant=cursor round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1086211
variant=cursor round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65580 waited=65579 wait_ms=0.04/0.04/0.07 wait_total_ms=2245 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2244 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=cursor round=1 catchup window=1024 heartbeat_ms=50 lag_first=649935 lag_rate_under_load=-34149 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=8798 still_tick_share=0.005 bytes_in_flight_mean=28819 bytes_in_flight_max=1062186 positions_in_flight_mean=227 commit_samples=2845632 commit_p50_ms=1.029 commit_p99_ms=50.002 commit_p999_ms=68.939 commit_max_ms=78.940
variant=cursor round=1 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1965 read_ms=5.73/11.08/12.22 before_ms=0.05/0.05/0.11 keep_ms=0.03/0.04/0.10 after_ms=5.64/10.98/12.12 before_MB=0.00/0.00/0.00 after_MB=23.80/43.26/47.76 scan_MBps=4138 r=0.986 ms_per_MB=0.258 intercept_ms=-0.36
variant=cursor round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=43695 waited=43695 wait_ms=0.03/0.05/3.59 wait_total_ms=5225 by_groups_ms=112 by_range_ms=3735 by_reads_ms=0 unattributed_ms=1378 ranges=613 range_ms=8.53/16.76/18.61 range_visited_MB_mean=32.7 range_records_mean=1024
variant=cursor round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16543 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=16468 positions_in_flight_mean=61 commit_samples=6504000 commit_p50_ms=1.021 commit_p99_ms=1.755 commit_p999_ms=3.021 commit_max_ms=8.138
variant=cursor round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1169919
variant=cursor round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65579 waited=65577 wait_ms=0.03/0.05/0.08 wait_total_ms=2094 by_groups_ms=4 by_range_ms=0 by_reads_ms=0 unattributed_ms=2090 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=both round=1 catchup window=256 heartbeat_ms=50 lag_first=471704 lag_rate_under_load=-24802 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=18225 still_tick_share=0.015 bytes_in_flight_mean=6587 bytes_in_flight_max=263488 positions_in_flight_mean=52 commit_samples=6487444 commit_p50_ms=1.021 commit_p99_ms=1.599 commit_p999_ms=4.423 commit_max_ms=67.343
variant=both round=1 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1338369
variant=both round=1 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=16149 read_ms=0.02/0.02/0.08 before_ms=0.01/0.02/0.07 keep_ms=0.00/0.01/0.02 after_ms=0.00/0.00/0.01 before_MB=0.00/0.00/0.00 after_MB=0.00/0.00/0.00 scan_MBps=221 r=0.992 ms_per_MB=0.276 intercept_ms=0.02
variant=both round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67113 waited=67101 wait_ms=0.03/0.05/0.11 wait_total_ms=2074 by_groups_ms=47 by_range_ms=0 by_reads_ms=0 unattributed_ms=2027 ranges=633 range_ms=0.07/0.08/0.14 range_visited_MB_mean=0.1 range_records_mean=1023
variant=both round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16138 still_tick_share=0.019 bytes_in_flight_mean=10292 bytes_in_flight_max=32936 positions_in_flight_mean=81 commit_samples=6335772 commit_p50_ms=1.019 commit_p99_ms=1.851 commit_p999_ms=3.661 commit_max_ms=515.620
variant=both round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1100573
variant=both round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65602 waited=65601 wait_ms=0.03/0.05/0.08 wait_total_ms=3620 by_groups_ms=1550 by_range_ms=0 by_reads_ms=0 unattributed_ms=2070 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=both round=1 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=18113 still_tick_share=0.018 bytes_in_flight_mean=14409 bytes_in_flight_max=1053952 positions_in_flight_mean=113 commit_samples=6436224 commit_p50_ms=1.027 commit_p99_ms=1.917 commit_p999_ms=3.748 commit_max_ms=15.503
variant=both round=1 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1162631
variant=both round=1 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=3992 read_ms=0.03/0.06/0.15 before_ms=0.01/0.03/0.10 keep_ms=0.01/0.03/0.11 after_ms=0.00/0.00/0.02 before_MB=0.00/0.00/0.00 after_MB=0.00/0.00/0.00 scan_MBps=221 r=0.906 ms_per_MB=0.284 intercept_ms=0.04
variant=both round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65705 waited=65702 wait_ms=0.03/0.05/0.10 wait_total_ms=2168 by_groups_ms=72 by_range_ms=0 by_reads_ms=0 unattributed_ms=2096 ranges=641 range_ms=0.07/0.08/0.12 range_visited_MB_mean=0.1 range_records_mean=1022
variant=both round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16357 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=131744 positions_in_flight_mean=61 commit_samples=6472573 commit_p50_ms=1.024 commit_p99_ms=1.851 commit_p999_ms=3.283 commit_max_ms=6.641
variant=both round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1154260
variant=both round=1 stages run=control window=1024 heartbeat_ms=50 scan peer=lagging reads=86 read_ms=0.03/0.07/0.29 before_ms=0.02/0.03/0.12 keep_ms=0.01/0.04/0.12 after_ms=0.00/0.00/0.16 before_MB=0.00/0.00/0.13 after_MB=0.00/0.00/0.00 scan_MBps=65 r=0.742 ms_per_MB=1.908 intercept_ms=0.04
variant=both round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65572 waited=65572 wait_ms=0.03/0.05/0.08 wait_total_ms=2794 by_groups_ms=723 by_range_ms=0 by_reads_ms=0 unattributed_ms=2071 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=both round=2 catchup window=256 heartbeat_ms=50 lag_first=474760 lag_rate_under_load=-24963 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=18218 still_tick_share=0.018 bytes_in_flight_mean=10292 bytes_in_flight_max=205850 positions_in_flight_mean=81 commit_samples=6485248 commit_p50_ms=1.019 commit_p99_ms=1.853 commit_p999_ms=3.551 commit_max_ms=8.595
variant=both round=2 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1330788
variant=both round=2 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=16663 read_ms=0.02/0.03/0.08 before_ms=0.01/0.02/0.07 keep_ms=0.00/0.01/0.02 after_ms=0.00/0.00/0.01 before_MB=0.00/0.00/0.00 after_MB=0.00/0.00/0.00 scan_MBps=71 r=0.906 ms_per_MB=0.260 intercept_ms=0.02
variant=both round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67265 waited=67252 wait_ms=0.03/0.05/0.11 wait_total_ms=2080 by_groups_ms=66 by_range_ms=0 by_reads_ms=0 unattributed_ms=2014 ranges=640 range_ms=0.07/0.07/0.09 range_visited_MB_mean=0.1 range_records_mean=1023
variant=both round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16166 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=16468 positions_in_flight_mean=65 commit_samples=6392896 commit_p50_ms=1.035 commit_p99_ms=1.825 commit_p999_ms=2.928 commit_max_ms=8.979
variant=both round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1123201
variant=both round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65568 waited=65568 wait_ms=0.03/0.05/0.08 wait_total_ms=2083 by_groups_ms=13 by_range_ms=0 by_reads_ms=0 unattributed_ms=2070 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=both round=2 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=18146 still_tick_share=0.005 bytes_in_flight_mean=8645 bytes_in_flight_max=1053952 positions_in_flight_mean=68 commit_samples=6441106 commit_p50_ms=1.014 commit_p99_ms=1.915 commit_p999_ms=3.727 commit_max_ms=75.638
variant=both round=2 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1168108
variant=both round=2 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=3786 read_ms=0.03/0.06/0.15 before_ms=0.01/0.03/0.10 keep_ms=0.01/0.03/0.10 after_ms=0.00/0.00/0.01 before_MB=0.00/0.00/0.00 after_MB=0.00/0.00/0.00 scan_MBps=267 r=0.932 ms_per_MB=0.230 intercept_ms=0.04
variant=both round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65783 waited=65775 wait_ms=0.03/0.05/0.10 wait_total_ms=2144 by_groups_ms=43 by_range_ms=13 by_reads_ms=0 unattributed_ms=2088 ranges=651 range_ms=0.06/0.07/0.10 range_visited_MB_mean=0.1 range_records_mean=1024
variant=both round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17192 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=32936 positions_in_flight_mean=61 commit_samples=6741462 commit_p50_ms=0.991 commit_p99_ms=1.767 commit_p999_ms=3.346 commit_max_ms=14.153
variant=both round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1268165
variant=both round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65711 waited=65711 wait_ms=0.02/0.05/0.11 wait_total_ms=1903 by_groups_ms=4 by_range_ms=0 by_reads_ms=0 unattributed_ms=1898 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=cursor round=2 catchup window=256 heartbeat_ms=50 lag_first=816270 lag_rate_under_load=-6491 lag_at_stop=692705 recovery_rate=10118 positions_per_tick=941 still_tick_share=0.018 bytes_in_flight_mean=6998 bytes_in_flight_max=263488 positions_in_flight_mean=55 commit_samples=372820 commit_p50_ms=12.340 commit_p99_ms=69.008 commit_p999_ms=71.449 commit_max_ms=72.216
variant=cursor round=2 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=3893 read_ms=4.68/10.93/11.73 before_ms=0.04/0.05/0.08 keep_ms=0.01/0.01/0.02 after_ms=4.63/10.88/11.68 before_MB=0.00/0.00/0.00 after_MB=23.85/58.31/62.64 scan_MBps=5213 r=0.999 ms_per_MB=0.191 intercept_ms=0.02
variant=cursor round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=3893 waited=3893 wait_ms=0.04/1.05/9.84 wait_total_ms=2137 by_groups_ms=0 by_range_ms=2000 by_reads_ms=0 unattributed_ms=136 ranges=652 range_ms=5.05/13.39/14.89 range_visited_MB_mean=27.8 range_records_mean=1024
variant=cursor round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16545 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=24702 positions_in_flight_mean=65 commit_samples=6576512 commit_p50_ms=1.017 commit_p99_ms=1.589 commit_p999_ms=3.251 commit_max_ms=5.402
variant=cursor round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1200920
variant=cursor round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65576 waited=65576 wait_ms=0.03/0.04/0.07 wait_total_ms=2040 by_groups_ms=5 by_range_ms=0 by_reads_ms=0 unattributed_ms=2035 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=cursor round=2 catchup window=1024 heartbeat_ms=50 lag_first=682825 lag_rate_under_load=-35879 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=7714 still_tick_share=0.010 bytes_in_flight_mean=20173 bytes_in_flight_max=1053952 positions_in_flight_mean=159 commit_samples=2413273 commit_p50_ms=1.027 commit_p99_ms=50.433 commit_p999_ms=72.883 commit_max_ms=102.731
variant=cursor round=2 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=2599 read_ms=4.08/10.21/13.71 before_ms=0.04/0.06/0.10 keep_ms=0.03/0.06/0.13 after_ms=4.01/10.11/13.58 before_MB=0.00/0.00/0.00 after_MB=19.89/48.46/61.67 scan_MBps=4701 r=0.992 ms_per_MB=0.220 intercept_ms=-0.13
variant=cursor round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=34269 waited=34269 wait_ms=0.03/0.05/7.66 wait_total_ms=7056 by_groups_ms=2 by_range_ms=6012 by_reads_ms=0 unattributed_ms=1043 ranges=646 range_ms=9.53/15.74/18.28 range_visited_MB_mean=36.2 range_records_mean=1024
variant=cursor round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16568 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=16468 positions_in_flight_mean=61 commit_samples=6571884 commit_p50_ms=1.014 commit_p99_ms=1.675 commit_p999_ms=3.065 commit_max_ms=20.670
variant=cursor round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1198575
variant=cursor round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65583 waited=65582 wait_ms=0.03/0.04/0.07 wait_total_ms=1999 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=1998 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=early round=2 catchup window=256 heartbeat_ms=50 lag_first=822510 lag_rate_under_load=-198 lag_at_stop=818740 recovery_rate=7336 positions_per_tick=546 still_tick_share=0.018 bytes_in_flight_mean=4940 bytes_in_flight_max=205850 positions_in_flight_mean=39 commit_samples=326528 commit_p50_ms=8.212 commit_p99_ms=70.079 commit_p999_ms=73.077 commit_max_ms=74.965
variant=early round=2 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=2193 read_ms=8.71/11.43/12.03 before_ms=8.69/11.41/12.02 keep_ms=0.01/0.02/0.02 after_ms=0.01/0.01/0.01 before_MB=35.27/46.09/48.54 after_MB=0.00/0.00/0.00 scan_MBps=4014 r=0.996 ms_per_MB=0.227 intercept_ms=0.79
variant=early round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=2192 waited=2191 wait_ms=0.06/7.16/12.26 wait_total_ms=3408 by_groups_ms=0 by_range_ms=3336 by_reads_ms=0 unattributed_ms=72 ranges=650 range_ms=6.53/15.97/18.07 range_visited_MB_mean=26.4 range_records_mean=1024
variant=early round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15716 still_tick_share=0.000 bytes_in_flight_mean=9057 bytes_in_flight_max=16468 positions_in_flight_mean=71 commit_samples=6451840 commit_p50_ms=1.028 commit_p99_ms=1.834 commit_p999_ms=3.455 commit_max_ms=7.949
variant=early round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1148215
variant=early round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65563 waited=65563 wait_ms=0.03/0.04/0.07 wait_total_ms=2045 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2044 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=early round=2 catchup window=1024 heartbeat_ms=50 lag_first=769730 lag_rate_under_load=-40418 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=2796 still_tick_share=0.018 bytes_in_flight_mean=15232 bytes_in_flight_max=1053952 positions_in_flight_mean=120 commit_samples=452224 commit_p50_ms=1.058 commit_p99_ms=90.163 commit_p999_ms=101.690 commit_max_ms=111.886
variant=early round=2 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=2212 read_ms=8.91/14.88/16.86 before_ms=8.87/14.84/16.81 keep_ms=0.04/0.04/0.05 after_ms=0.01/0.01/0.01 before_MB=32.90/56.77/62.48 after_MB=0.00/0.00/0.00 scan_MBps=3774 r=0.995 ms_per_MB=0.268 intercept_ms=-0.07
variant=early round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=5161 waited=5161 wait_ms=0.03/0.50/12.92 wait_total_ms=3565 by_groups_ms=2 by_range_ms=3388 by_reads_ms=0 unattributed_ms=174 ranges=641 range_ms=7.13/16.85/19.58 range_visited_MB_mean=26.5 range_records_mean=1024
variant=early round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16562 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=16468 positions_in_flight_mean=68 commit_samples=6501110 commit_p50_ms=1.033 commit_p99_ms=1.578 commit_p999_ms=2.996 commit_max_ms=5.922
variant=early round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1169147
variant=early round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65582 waited=65581 wait_ms=0.03/0.04/0.07 wait_total_ms=2055 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2054 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=base round=2 catchup window=256 heartbeat_ms=50 lag_first=808470 lag_rate_under_load=-2412 lag_at_stop=762515 recovery_rate=7382 positions_per_tick=361 still_tick_share=0.017 bytes_in_flight_mean=4528 bytes_in_flight_max=205850 positions_in_flight_mean=35 commit_samples=214274 commit_p50_ms=1.124 commit_p99_ms=79.823 commit_p999_ms=82.660 commit_max_ms=84.507
variant=base round=2 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1499 read_ms=12.96/13.18/13.62 before_ms=5.51/7.06/7.39 keep_ms=0.01/0.02/0.02 after_ms=7.36/9.01/9.43 before_MB=25.42/32.83/34.50 after_MB=37.70/44.97/46.62 scan_MBps=4854 r=0.021 ms_per_MB=0.921 intercept_ms=-45.14
variant=base round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1498 waited=1498 wait_ms=0.03/10.57/14.52 wait_total_ms=3999 by_groups_ms=0 by_range_ms=3966 by_reads_ms=0 unattributed_ms=33 ranges=613 range_ms=14.81/14.98/15.83 range_visited_MB_mean=54.9 range_records_mean=1024
variant=base round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16127 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=24702 positions_in_flight_mean=58 commit_samples=6388677 commit_p50_ms=1.053 commit_p99_ms=1.357 commit_p999_ms=2.736 commit_max_ms=4.974
variant=base round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1121746
variant=base round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65586 waited=65586 wait_ms=0.04/0.04/0.06 wait_total_ms=2205 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2204 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=base round=2 catchup window=1024 heartbeat_ms=50 lag_first=732420 lag_rate_under_load=-25090 lag_at_stop=253500 recovery_rate=25350 positions_per_tick=1453 still_tick_share=0.020 bytes_in_flight_mean=20173 bytes_in_flight_max=1111590 positions_in_flight_mean=159 commit_samples=200960 commit_p50_ms=1.126 commit_p99_ms=95.859 commit_p999_ms=96.832 commit_max_ms=97.161
variant=base round=2 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1340 read_ms=15.54/15.76/16.08 before_ms=7.09/13.90/15.52 keep_ms=0.03/0.04/0.05 after_ms=7.46/10.70/11.96 before_MB=27.45/55.41/62.33 after_MB=34.18/49.69/55.62 scan_MBps=4216 r=0.982 ms_per_MB=0.584 intercept_ms=-21.26
variant=base round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1338 waited=1338 wait_ms=0.11/13.14/17.29 wait_total_ms=5158 by_groups_ms=0 by_range_ms=5125 by_reads_ms=0 unattributed_ms=32 ranges=623 range_ms=17.44/17.58/18.02 range_visited_MB_mean=59.8 range_records_mean=1024
variant=base round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15892 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=24702 positions_in_flight_mean=68 commit_samples=6282493 commit_p50_ms=1.049 commit_p99_ms=1.331 commit_p999_ms=2.763 commit_max_ms=400.361
variant=base round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1076793
variant=base round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65580 waited=65580 wait_ms=0.04/0.04/0.06 wait_total_ms=2239 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2236 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=early round=3 catchup window=256 heartbeat_ms=50 lag_first=836615 lag_rate_under_load=4992 lag_at_stop=931710 recovery_rate=8155 positions_per_tick=649 still_tick_share=0.017 bytes_in_flight_mean=6587 bytes_in_flight_max=263488 positions_in_flight_mean=52 commit_samples=466508 commit_p50_ms=1.234 commit_p99_ms=61.482 commit_p999_ms=64.430 commit_max_ms=64.919
variant=early round=3 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=2541 read_ms=7.43/10.03/10.63 before_ms=7.42/10.02/10.61 keep_ms=0.01/0.02/0.02 after_ms=0.01/0.01/0.01 before_MB=33.72/46.27/49.10 after_MB=0.00/0.00/0.00 scan_MBps=4533 r=0.998 ms_per_MB=0.205 intercept_ms=0.53
variant=early round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=2541 waited=2541 wait_ms=0.03/5.56/9.76 wait_total_ms=3196 by_groups_ms=0 by_range_ms=3141 by_reads_ms=0 unattributed_ms=55 ranges=614 range_ms=6.89/14.70/16.59 range_visited_MB_mean=26.8 range_records_mean=1024
variant=early round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16170 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=24702 positions_in_flight_mean=65 commit_samples=6413170 commit_p50_ms=1.047 commit_p99_ms=1.320 commit_p999_ms=3.101 commit_max_ms=5.224
variant=early round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1132160
variant=early round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65552 waited=65552 wait_ms=0.04/0.04/0.06 wait_total_ms=2228 by_groups_ms=0 by_range_ms=0 by_reads_ms=0 unattributed_ms=2228 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=early round=3 catchup window=1024 heartbeat_ms=50 lag_first=787540 lag_rate_under_load=-41376 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=3865 still_tick_share=0.018 bytes_in_flight_mean=24290 bytes_in_flight_max=1053952 positions_in_flight_mean=191 commit_samples=847616 commit_p50_ms=1.075 commit_p99_ms=77.707 commit_p999_ms=91.765 commit_max_ms=94.759
variant=early round=3 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=2283 read_ms=8.23/13.70/15.42 before_ms=8.19/13.67/15.38 keep_ms=0.03/0.04/0.05 after_ms=0.01/0.01/0.01 before_MB=31.68/56.52/62.45 after_MB=0.00/0.00/0.00 scan_MBps=4104 r=0.993 ms_per_MB=0.248 intercept_ms=-0.11
variant=early round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=11308 waited=11308 wait_ms=0.04/0.05/10.24 wait_total_ms=3977 by_groups_ms=21 by_range_ms=3590 by_reads_ms=0 unattributed_ms=367 ranges=628 range_ms=7.18/16.64/18.53 range_visited_MB_mean=26.6 range_records_mean=1024
variant=early round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16194 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=24702 positions_in_flight_mean=68 commit_samples=6387220 commit_p50_ms=1.050 commit_p99_ms=1.340 commit_p999_ms=3.028 commit_max_ms=29.267
variant=early round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1120874
variant=early round=3 stages run=control window=1024 heartbeat_ms=50 scan peer=lagging reads=1 read_ms=5.86/5.86/5.86 before_ms=5.85/5.85/5.85 keep_ms=0.01/0.01/0.01 after_ms=0.01/0.01/0.01 before_MB=17.29/17.29/17.29 after_MB=0.00/0.00/0.00 scan_MBps=2953 r=0.000 ms_per_MB=0.000 intercept_ms=5.86
variant=early round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65581 waited=65580 wait_ms=0.04/0.04/0.06 wait_total_ms=2205 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2202 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=both round=3 catchup window=256 heartbeat_ms=50 lag_first=504140 lag_rate_under_load=-26507 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=18161 still_tick_share=0.018 bytes_in_flight_mean=8645 bytes_in_flight_max=263488 positions_in_flight_mean=68 commit_samples=6410624 commit_p50_ms=1.050 commit_p99_ms=1.402 commit_p999_ms=2.723 commit_max_ms=7.491
variant=both round=3 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1330490
variant=both round=3 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=18543 read_ms=0.02/0.02/0.06 before_ms=0.01/0.02/0.04 keep_ms=0.00/0.01/0.01 after_ms=0.00/0.00/0.01 before_MB=0.00/0.00/0.00 after_MB=0.00/0.00/0.00 scan_MBps=57 r=0.949 ms_per_MB=0.247 intercept_ms=0.02
variant=both round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67080 waited=67068 wait_ms=0.03/0.04/0.10 wait_total_ms=2133 by_groups_ms=54 by_range_ms=0 by_reads_ms=0 unattributed_ms=2079 ranges=613 range_ms=0.07/0.07/0.08 range_visited_MB_mean=0.1 range_records_mean=1023
variant=both round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16226 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=24702 positions_in_flight_mean=68 commit_samples=6417536 commit_p50_ms=1.044 commit_p99_ms=1.336 commit_p999_ms=2.669 commit_max_ms=4.855
variant=both round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1133601
variant=both round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65569 waited=65569 wait_ms=0.04/0.04/0.06 wait_total_ms=2246 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2245 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=both round=3 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17760 still_tick_share=0.015 bytes_in_flight_mean=12762 bytes_in_flight_max=1053952 positions_in_flight_mean=100 commit_samples=6264064 commit_p50_ms=1.046 commit_p99_ms=1.540 commit_p999_ms=3.028 commit_max_ms=219.937
variant=both round=3 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1088628
variant=both round=3 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=4252 read_ms=0.03/0.05/0.14 before_ms=0.01/0.02/0.07 keep_ms=0.01/0.03/0.09 after_ms=0.00/0.00/0.01 before_MB=0.00/0.00/0.00 after_MB=0.00/0.00/0.00 scan_MBps=507 r=0.973 ms_per_MB=0.236 intercept_ms=0.03
variant=both round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65681 waited=65680 wait_ms=0.04/0.04/0.08 wait_total_ms=2216 by_groups_ms=36 by_range_ms=0 by_reads_ms=0 unattributed_ms=2180 ranges=625 range_ms=0.07/0.07/0.09 range_visited_MB_mean=0.1 range_records_mean=1023
variant=both round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16237 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=32936 positions_in_flight_mean=61 commit_samples=6446208 commit_p50_ms=1.044 commit_p99_ms=1.322 commit_p999_ms=2.969 commit_max_ms=4.681
variant=both round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1145918
variant=both round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65556 waited=65556 wait_ms=0.04/0.04/0.06 wait_total_ms=2245 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2244 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=base round=3 catchup window=256 heartbeat_ms=50 lag_first=810810 lag_rate_under_load=-2334 lag_at_stop=766350 recovery_rate=7291 positions_per_tick=354 still_tick_share=0.017 bytes_in_flight_mean=6587 bytes_in_flight_max=205850 positions_in_flight_mean=52 commit_samples=212032 commit_p50_ms=1.129 commit_p99_ms=81.100 commit_p999_ms=82.429 commit_max_ms=84.857
variant=base round=3 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1476 read_ms=13.18/13.39/13.61 before_ms=5.65/7.18/7.54 keep_ms=0.01/0.01/0.02 after_ms=7.43/9.09/9.41 before_MB=25.54/32.83/34.47 after_MB=37.59/44.66/46.25 scan_MBps=4776 r=0.019 ms_per_MB=0.811 intercept_ms=-37.96
variant=base round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1475 waited=1475 wait_ms=0.04/11.08/14.80 wait_total_ms=4127 by_groups_ms=0 by_range_ms=4093 by_reads_ms=0 unattributed_ms=34 ranges=616 range_ms=15.19/15.35/15.68 range_visited_MB_mean=54.8 range_records_mean=1024
variant=base round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16140 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=32936 positions_in_flight_mean=65 commit_samples=6391026 commit_p50_ms=1.046 commit_p99_ms=1.328 commit_p999_ms=3.199 commit_max_ms=33.504
variant=base round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1123475
variant=base round=3 stages run=control window=256 heartbeat_ms=50 scan peer=lagging reads=2 read_ms=4.02/4.02/4.02 before_ms=3.99/3.99/3.99 keep_ms=0.01/0.01/0.01 after_ms=0.01/0.01/0.01 before_MB=10.61/10.61/10.61 after_MB=0.01/0.01/0.01 scan_MBps=3314 r=-1.000 ms_per_MB=-260.927 intercept_ms=2772.92
variant=base round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65568 waited=65568 wait_ms=0.04/0.04/0.06 wait_total_ms=2173 by_groups_ms=4 by_range_ms=0 by_reads_ms=0 unattributed_ms=2168 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=base round=3 catchup window=1024 heartbeat_ms=50 lag_first=655005 lag_rate_under_load=-26747 lag_at_stop=144105 recovery_rate=14410 positions_per_tick=1570 still_tick_share=0.010 bytes_in_flight_mean=23466 bytes_in_flight_max=1062186 positions_in_flight_mean=185 commit_samples=124416 commit_p50_ms=66.037 commit_p99_ms=92.853 commit_p999_ms=211.386 commit_max_ms=226.714
variant=base round=3 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1474 read_ms=14.84/15.14/16.09 before_ms=7.11/13.12/14.86 keep_ms=0.03/0.04/0.05 after_ms=6.04/9.27/10.04 before_MB=29.07/54.62/62.28 after_MB=28.37/42.95/46.95 scan_MBps=4320 r=0.774 ms_per_MB=0.324 intercept_ms=-5.34
variant=base round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1466 waited=1466 wait_ms=0.11/13.28/17.38 wait_total_ms=5545 by_groups_ms=0 by_range_ms=5507 by_reads_ms=0 unattributed_ms=39 ranges=624 range_ms=17.45/17.65/22.41 range_visited_MB_mean=60.0 range_records_mean=1024
variant=base round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16205 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=16468 positions_in_flight_mean=61 commit_samples=6398400 commit_p50_ms=1.046 commit_p99_ms=1.379 commit_p999_ms=2.855 commit_max_ms=4.424
variant=base round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1125751
variant=base round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65576 waited=65576 wait_ms=0.04/0.04/0.06 wait_total_ms=2225 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2223 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=cursor round=3 catchup window=256 heartbeat_ms=50 lag_first=719485 lag_rate_under_load=-3253 lag_at_stop=657540 recovery_rate=20408 positions_per_tick=481 still_tick_share=0.005 bytes_in_flight_mean=3293 bytes_in_flight_max=263488 positions_in_flight_mean=26 commit_samples=160560 commit_p50_ms=47.969 commit_p99_ms=81.854 commit_p999_ms=83.468 commit_max_ms=88.345
variant=cursor round=3 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1981 read_ms=9.87/13.12/13.74 before_ms=0.04/0.04/0.05 keep_ms=0.01/0.01/0.01 after_ms=9.82/13.08/13.69 before_MB=0.00/0.00/0.00 after_MB=34.97/44.75/46.96 scan_MBps=3521 r=0.996 ms_per_MB=0.297 intercept_ms=-0.44
variant=cursor round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1980 waited=1979 wait_ms=0.02/1.80/10.24 wait_total_ms=1257 by_groups_ms=0 by_range_ms=1206 by_reads_ms=0 unattributed_ms=52 ranges=610 range_ms=5.15/12.50/14.42 range_visited_MB_mean=27.6 range_records_mean=1024
variant=cursor round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16064 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=32936 positions_in_flight_mean=68 commit_samples=6435584 commit_p50_ms=1.042 commit_p99_ms=1.344 commit_p999_ms=2.858 commit_max_ms=17.539
variant=cursor round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1141402
variant=cursor round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65555 waited=65555 wait_ms=0.04/0.05/0.06 wait_total_ms=2266 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2265 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
variant=cursor round=3 catchup window=1024 heartbeat_ms=50 lag_first=758810 lag_rate_under_load=-39830 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=6013 still_tick_share=0.015 bytes_in_flight_mean=20173 bytes_in_flight_max=1004548 positions_in_flight_mean=159 commit_samples=1725824 commit_p50_ms=1.041 commit_p99_ms=72.153 commit_p999_ms=95.397 commit_max_ms=103.338
variant=cursor round=3 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=2078 read_ms=6.99/13.95/16.71 before_ms=0.04/0.05/0.06 keep_ms=0.02/0.03/0.07 after_ms=6.93/13.88/16.62 before_MB=0.00/0.00/0.00 after_MB=24.88/50.44/59.68 scan_MBps=3590 r=0.997 ms_per_MB=0.286 intercept_ms=-0.15
variant=cursor round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=25002 waited=25002 wait_ms=0.04/0.05/3.37 wait_total_ms=2813 by_groups_ms=22 by_range_ms=1942 by_reads_ms=0 unattributed_ms=849 ranges=622 range_ms=6.68/14.04/16.11 range_visited_MB_mean=31.6 range_records_mean=1024
variant=cursor round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16284 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=41170 positions_in_flight_mean=65 commit_samples=6442741 commit_p50_ms=1.041 commit_p99_ms=1.351 commit_p999_ms=2.919 commit_max_ms=44.011
variant=cursor round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1144806
variant=cursor round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65596 waited=65596 wait_ms=0.04/0.05/0.06 wait_total_ms=2228 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2225 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
```

Commits a second during catch-up (`commit_samples` over the 20 s): base 6 000–11 400, controls about 315 000, early 16 000–59 000, cursor 8 000–142 000, both 313 000–324 000. Not accepted; direction only. Segments in the page cache; a cold disk is not represented.

## D-142 A to protocol, base against both — preliminary (2026-10-16)

rules.md §11 run: `base` (commit 0c5f007, D-139's path) against `both` (D-142 A as merged), five rounds with the order alternated, each round: `STRAUM_BENCH_MODE=catchup STRAUM_BENCH_WINDOWS=256,1024,4096` at 50 ms with paired controls, then cap's failover mode (`STRAUM_BENCH_CAPS=64 STRAUM_BENCH_FAILOVERS=10`). Linux container, 19:25–20:08; the macOS power log shows no sleep in that window (an earlier attempt that straddled sleeps was discarded). `t=` is when each bench invocation started.

```text
t=1792167931 variant=base round=1 catchup window=256 heartbeat_ms=50 lag_first=618150 lag_rate_under_load=-1465 lag_at_stop=590265 recovery_rate=5191 positions_per_tick=220 still_tick_share=0.015 bytes_in_flight_mean=4528 bytes_in_flight_max=271722 positions_in_flight_mean=35 commit_samples=155587 commit_p50_ms=1.301 commit_p99_ms=172.644 commit_p999_ms=194.253 commit_max_ms=203.789
t=1792167931 variant=base round=1 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=913 read_ms=19.12/27.56/32.90 before_ms=1.82/26.72/31.90 keep_ms=0.01/0.02/0.02 after_ms=16.53/18.34/19.60 before_MB=5.59/62.06/63.01 after_MB=57.52/62.03/63.01 scan_MBps=2944 r=-0.293 ms_per_MB=-261.324 intercept_ms=16515.32
t=1792167931 variant=base round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=913 waited=913 wait_ms=0.08/20.66/25.43 wait_total_ms=5425 by_groups_ms=0 by_range_ms=5396 by_reads_ms=0 unattributed_ms=29 ranges=453 range_ms=22.42/26.79/30.96 range_visited_MB_mean=61.1 range_records_mean=1024
t=1792167931 variant=base round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=10538 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=32936 positions_in_flight_mean=61 commit_samples=4605209 commit_p50_ms=1.395 commit_p99_ms=3.211 commit_p999_ms=5.665 commit_max_ms=39.692
t=1792167931 variant=base round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=366110
t=1792167931 variant=base round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65727 waited=65724 wait_ms=0.03/0.06/0.19 wait_total_ms=2450 by_groups_ms=8 by_range_ms=0 by_reads_ms=0 unattributed_ms=2441 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792167931 variant=base round=1 catchup window=1024 heartbeat_ms=50 lag_first=497185 lag_rate_under_load=-15729 lag_at_stop=197015 recovery_rate=19702 positions_per_tick=912 still_tick_share=0.018 bytes_in_flight_mean=23055 bytes_in_flight_max=1053952 positions_in_flight_mean=182 commit_samples=140288 commit_p50_ms=1.267 commit_p99_ms=140.923 commit_p999_ms=160.111 commit_max_ms=161.096
t=1792167931 variant=base round=1 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=866 read_ms=22.41/23.25/26.66 before_ms=8.26/15.70/26.26 keep_ms=0.05/0.05/0.07 after_ms=14.06/20.33/22.05 before_MB=22.19/41.47/62.66 after_MB=40.93/58.71/62.54 scan_MBps=2784 r=-0.012 ms_per_MB=-0.437 intercept_ms=50.24
t=1792167931 variant=base round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=861 waited=861 wait_ms=4.78/21.38/24.90 wait_total_ms=6744 by_groups_ms=0 by_range_ms=6718 by_reads_ms=0 unattributed_ms=25 ranges=476 range_ms=25.03/25.53/26.54 range_visited_MB_mean=63.1 range_records_mean=1024
t=1792167931 variant=base round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=14165 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=16468 positions_in_flight_mean=58 commit_samples=5775620 commit_p50_ms=1.102 commit_p99_ms=1.728 commit_p999_ms=3.062 commit_max_ms=296.485
t=1792167931 variant=base round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=862852
t=1792167931 variant=base round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65598 waited=65598 wait_ms=0.03/0.04/0.08 wait_total_ms=2140 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2138 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792167931 variant=base round=1 catchup window=4096 heartbeat_ms=50 lag_first=555555 lag_rate_under_load=-28405 lag_at_stop=14755 recovery_rate=1476 positions_per_tick=1618 still_tick_share=0.018 bytes_in_flight_mean=25937 bytes_in_flight_max=4215808 positions_in_flight_mean=204 commit_samples=170048 commit_p50_ms=42.676 commit_p99_ms=111.015 commit_p999_ms=138.517 commit_max_ms=157.274
t=1792167931 variant=base round=1 stages run=catchup window=4096 heartbeat_ms=50 scan peer=lagging reads=1435 read_ms=16.88/17.88/19.79 before_ms=8.07/15.35/16.99 keep_ms=0.03/0.04/0.05 after_ms=4.89/9.89/11.91 before_MB=29.00/54.88/62.31 after_MB=19.94/38.10/45.43 scan_MBps=3729 r=0.977 ms_per_MB=0.318 intercept_ms=-2.52
t=1792167931 variant=base round=1 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=1427 waited=1427 wait_ms=0.05/14.18/19.32 wait_total_ms=5717 by_groups_ms=0 by_range_ms=5680 by_reads_ms=0 unattributed_ms=37 ranges=620 range_ms=18.87/20.32/25.78 range_visited_MB_mean=60.4 range_records_mean=1024
t=1792167931 variant=base round=1 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15442 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=107042 positions_in_flight_mean=58 commit_samples=6123601 commit_p50_ms=1.069 commit_p99_ms=2.108 commit_p999_ms=4.878 commit_max_ms=159.813
t=1792167931 variant=base round=1 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1009609
t=1792167931 variant=base round=1 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65623 waited=65622 wait_ms=0.04/0.05/0.08 wait_total_ms=2277 by_groups_ms=29 by_range_ms=0 by_reads_ms=0 unattributed_ms=2247 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792168139 variant=base round=1 failovermode 64                     309937          4 of 10            timeout / timeout    86190   p90=timeout
t=1792168139 variant=base round=1 failovermode failover mode=fast count=4 slots_moved=1.25 takeovers_abandoned=0.25 lag_at_stop=0 timed_out=0
t=1792168139 variant=base round=1 failovermode failover mode=slow count=6 slots_moved=1.50 takeovers_abandoned=0.33 lag_at_stop=49140 timed_out=6
t=1792168219 variant=both round=1 catchup window=256 heartbeat_ms=50 lag_first=306735 lag_rate_under_load=-16127 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17593 still_tick_share=0.005 bytes_in_flight_mean=8645 bytes_in_flight_max=263488 positions_in_flight_mean=68 commit_samples=6283904 commit_p50_ms=1.060 commit_p99_ms=1.815 commit_p999_ms=3.609 commit_max_ms=15.871
t=1792168219 variant=both round=1 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1210356
t=1792168219 variant=both round=1 stages run=catchup window=256 heartbeat_ms=50 scan reads=14884 read_ms=0.02/0.02/0.07
t=1792168219 variant=both round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=66803 waited=66789 wait_ms=0.03/0.05/0.11 wait_total_ms=2177 by_groups_ms=60 by_range_ms=13 by_reads_ms=0 unattributed_ms=2104 ranges=596 range_ms=0.07/0.08/0.15 range_records_mean=1024
t=1792168219 variant=both round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15726 still_tick_share=0.000 bytes_in_flight_mean=6998 bytes_in_flight_max=32936 positions_in_flight_mean=55 commit_samples=6222976 commit_p50_ms=1.059 commit_p99_ms=1.753 commit_p999_ms=3.425 commit_max_ms=176.996
t=1792168219 variant=both round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1051491
t=1792168219 variant=both round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65571 waited=65571 wait_ms=0.03/0.05/0.08 wait_total_ms=2243 by_groups_ms=4 by_range_ms=0 by_reads_ms=0 unattributed_ms=2239 ranges=0 range_ms=- range_records_mean=0
t=1792168219 variant=both round=1 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17491 still_tick_share=0.015 bytes_in_flight_mean=8234 bytes_in_flight_max=996314 positions_in_flight_mean=65 commit_samples=6215453 commit_p50_ms=1.066 commit_p99_ms=1.934 commit_p999_ms=3.677 commit_max_ms=7.600
t=1792168219 variant=both round=1 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1060361
t=1792168219 variant=both round=1 stages run=catchup window=1024 heartbeat_ms=50 scan reads=3868 read_ms=0.03/0.05/0.15
t=1792168219 variant=both round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65702 waited=65700 wait_ms=0.03/0.05/0.10 wait_total_ms=2307 by_groups_ms=48 by_range_ms=0 by_reads_ms=0 unattributed_ms=2259 ranges=610 range_ms=0.07/0.08/0.11 range_records_mean=1022
t=1792168219 variant=both round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15288 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=57638 positions_in_flight_mean=65 commit_samples=6017475 commit_p50_ms=1.093 commit_p99_ms=1.966 commit_p999_ms=3.740 commit_max_ms=7.370
t=1792168219 variant=both round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=965337
t=1792168219 variant=both round=1 stages run=control window=1024 heartbeat_ms=50 scan reads=2 read_ms=3.35/3.35/3.35
t=1792168219 variant=both round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65582 waited=65581 wait_ms=0.03/0.05/0.09 wait_total_ms=2290 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2288 ranges=0 range_ms=- range_records_mean=0
t=1792168219 variant=both round=1 catchup window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16782 still_tick_share=0.007 bytes_in_flight_mean=8645 bytes_in_flight_max=4215808 positions_in_flight_mean=68 commit_samples=5916435 commit_p50_ms=1.098 commit_p99_ms=2.227 commit_p999_ms=6.296 commit_max_ms=26.297
t=1792168219 variant=both round=1 stages run=catchup window=4096 heartbeat_ms=50 stamps_dropped=928573
t=1792168219 variant=both round=1 stages run=catchup window=4096 heartbeat_ms=50 scan reads=2209 read_ms=0.05/0.09/0.18
t=1792168219 variant=both round=1 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=65609 waited=65607 wait_ms=0.04/0.05/0.11 wait_total_ms=2381 by_groups_ms=39 by_range_ms=14 by_reads_ms=0 unattributed_ms=2328 ranges=602 range_ms=0.07/0.08/0.18 range_records_mean=1024
t=1792168219 variant=both round=1 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15782 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=16468 positions_in_flight_mean=58 commit_samples=6222203 commit_p50_ms=1.070 commit_p99_ms=1.701 commit_p999_ms=3.272 commit_max_ms=7.970
t=1792168219 variant=both round=1 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1051187
t=1792168219 variant=both round=1 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65576 waited=65576 wait_ms=0.04/0.05/0.07 wait_total_ms=2270 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2267 ranges=0 range_ms=- range_records_mean=0
t=1792168428 variant=both round=1 failovermode 64                     309065         10 of 10                180 / 199        0   p90=199
t=1792168428 variant=both round=1 failovermode failover mode=fast count=10 slots_moved=1.00 takeovers_abandoned=0.00 lag_at_stop=0 timed_out=0
t=1792168449 variant=both round=2 catchup window=256 heartbeat_ms=50 lag_first=477295 lag_rate_under_load=-25096 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17534 still_tick_share=0.018 bytes_in_flight_mean=9057 bytes_in_flight_max=205850 positions_in_flight_mean=71 commit_samples=6217920 commit_p50_ms=1.068 commit_p99_ms=1.668 commit_p999_ms=3.202 commit_max_ms=8.079
t=1792168449 variant=both round=2 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1195662
t=1792168449 variant=both round=2 stages run=catchup window=256 heartbeat_ms=50 scan reads=17089 read_ms=0.02/0.02/0.08
t=1792168449 variant=both round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67008 waited=66993 wait_ms=0.03/0.05/0.11 wait_total_ms=2200 by_groups_ms=65 by_range_ms=0 by_reads_ms=0 unattributed_ms=2134 ranges=609 range_ms=0.06/0.07/0.17 range_records_mean=1023
t=1792168449 variant=both round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15937 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=24702 positions_in_flight_mean=58 commit_samples=6273920 commit_p50_ms=1.058 commit_p99_ms=1.910 commit_p999_ms=3.615 commit_max_ms=30.952
t=1792168449 variant=both round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1072834
t=1792168449 variant=both round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65600 waited=65600 wait_ms=0.03/0.05/0.09 wait_total_ms=2210 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2208 ranges=0 range_ms=- range_records_mean=0
t=1792168449 variant=both round=2 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17414 still_tick_share=0.005 bytes_in_flight_mean=8234 bytes_in_flight_max=1053952 positions_in_flight_mean=65 commit_samples=6170168 commit_p50_ms=1.067 commit_p99_ms=1.970 commit_p999_ms=4.120 commit_max_ms=12.318
t=1792168449 variant=both round=2 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1044997
t=1792168449 variant=both round=2 stages run=catchup window=1024 heartbeat_ms=50 scan reads=3710 read_ms=0.03/0.05/0.15
t=1792168449 variant=both round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65734 waited=65728 wait_ms=0.04/0.05/0.09 wait_total_ms=2297 by_groups_ms=31 by_range_ms=12 by_reads_ms=0 unattributed_ms=2255 ranges=619 range_ms=0.07/0.08/0.14 range_records_mean=1024
t=1792168449 variant=both round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15415 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=65872 positions_in_flight_mean=65 commit_samples=6100316 commit_p50_ms=1.075 commit_p99_ms=2.092 commit_p999_ms=3.568 commit_max_ms=5.977
t=1792168449 variant=both round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=999553
t=1792168449 variant=both round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65579 waited=65579 wait_ms=0.04/0.05/0.09 wait_total_ms=2294 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2293 ranges=0 range_ms=- range_records_mean=0
t=1792168449 variant=both round=2 catchup window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17404 still_tick_share=0.018 bytes_in_flight_mean=6998 bytes_in_flight_max=4158170 positions_in_flight_mean=55 commit_samples=6161022 commit_p50_ms=1.066 commit_p99_ms=1.997 commit_p999_ms=4.242 commit_max_ms=17.276
t=1792168449 variant=both round=2 stages run=catchup window=4096 heartbeat_ms=50 stamps_dropped=1023391
t=1792168449 variant=both round=2 stages run=catchup window=4096 heartbeat_ms=50 scan reads=2309 read_ms=0.04/0.11/0.20
t=1792168449 variant=both round=2 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=65595 waited=65594 wait_ms=0.03/0.05/0.12 wait_total_ms=2300 by_groups_ms=32 by_range_ms=0 by_reads_ms=0 unattributed_ms=2268 ranges=615 range_ms=0.07/0.08/0.15 range_records_mean=1022
t=1792168449 variant=both round=2 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15695 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=16468 positions_in_flight_mean=65 commit_samples=6250101 commit_p50_ms=1.066 commit_p99_ms=1.728 commit_p999_ms=3.450 commit_max_ms=6.030
t=1792168449 variant=both round=2 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1063044
t=1792168449 variant=both round=2 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65591 waited=65589 wait_ms=0.04/0.04/0.07 wait_total_ms=2196 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2194 ranges=0 range_ms=- range_records_mean=0
t=1792168659 variant=both round=2 failovermode 64                     312285         10 of 10                171 / 200        0   p90=200
t=1792168659 variant=both round=2 failovermode failover mode=fast count=10 slots_moved=1.00 takeovers_abandoned=0.00 lag_at_stop=0 timed_out=0
t=1792168679 variant=base round=2 catchup window=256 heartbeat_ms=50 lag_first=800800 lag_rate_under_load=-2127 lag_at_stop=760305 recovery_rate=7109 positions_per_tick=331 still_tick_share=0.018 bytes_in_flight_mean=4117 bytes_in_flight_max=263488 positions_in_flight_mean=32 commit_samples=207040 commit_p50_ms=1.155 commit_p99_ms=89.111 commit_p999_ms=94.178 commit_max_ms=98.989
t=1792168679 variant=base round=2 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1396 read_ms=13.81/14.65/16.41 before_ms=5.89/7.23/7.77 keep_ms=0.01/0.01/0.02 after_ms=8.11/9.69/10.85 before_MB=24.55/31.44/32.99 after_MB=38.58/45.12/46.60 scan_MBps=4523 r=0.036 ms_per_MB=5.817 intercept_ms=-353.18
t=1792168679 variant=base round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1396 waited=1396 wait_ms=0.06/13.04/16.96 wait_total_ms=5089 by_groups_ms=0 by_range_ms=5052 by_reads_ms=0 unattributed_ms=38 ranges=613 range_ms=16.98/18.04/20.37 range_visited_MB_mean=55.0 range_records_mean=1024
t=1792168679 variant=base round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=14647 still_tick_share=0.000 bytes_in_flight_mean=9057 bytes_in_flight_max=32936 positions_in_flight_mean=71 commit_samples=6264576 commit_p50_ms=1.053 commit_p99_ms=1.409 commit_p999_ms=3.171 commit_max_ms=232.384
t=1792168679 variant=base round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1069646
t=1792168679 variant=base round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65547 waited=65547 wait_ms=0.04/0.04/0.06 wait_total_ms=2210 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2207 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792168679 variant=base round=2 catchup window=1024 heartbeat_ms=50 lag_first=650845 lag_rate_under_load=-26852 lag_at_stop=139750 recovery_rate=13975 positions_per_tick=1572 still_tick_share=0.005 bytes_in_flight_mean=14409 bytes_in_flight_max=1111590 positions_in_flight_mean=113 commit_samples=121920 commit_p50_ms=63.670 commit_p99_ms=95.537 commit_p999_ms=101.873 commit_max_ms=102.828
t=1792168679 variant=base round=2 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1462 read_ms=15.28/15.72/16.82 before_ms=6.86/13.42/15.14 keep_ms=0.03/0.04/0.05 after_ms=6.06/9.72/11.15 before_MB=27.55/54.68/62.29 after_MB=28.22/42.78/47.57 scan_MBps=4221 r=0.994 ms_per_MB=0.326 intercept_ms=-5.06
t=1792168679 variant=base round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1459 waited=1459 wait_ms=0.08/13.33/17.75 wait_total_ms=5458 by_groups_ms=0 by_range_ms=5420 by_reads_ms=0 unattributed_ms=39 ranges=603 range_ms=17.89/18.18/19.70 range_visited_MB_mean=59.8 range_records_mean=1024
t=1792168679 variant=base round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15663 still_tick_share=0.012 bytes_in_flight_mean=7822 bytes_in_flight_max=131744 positions_in_flight_mean=61 commit_samples=6138240 commit_p50_ms=1.052 commit_p99_ms=1.944 commit_p999_ms=4.082 commit_max_ms=365.956
t=1792168679 variant=base round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1015887
t=1792168679 variant=base round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65566 waited=65565 wait_ms=0.04/0.05/0.07 wait_total_ms=2249 by_groups_ms=4 by_range_ms=0 by_reads_ms=0 unattributed_ms=2245 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792168679 variant=base round=2 catchup window=4096 heartbeat_ms=50 lag_first=605800 lag_rate_under_load=-31834 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=2454 still_tick_share=0.018 bytes_in_flight_mean=23466 bytes_in_flight_max=4273446 positions_in_flight_mean=185 commit_samples=450752 commit_p50_ms=1.095 commit_p99_ms=88.055 commit_p999_ms=90.792 commit_max_ms=101.491
t=1792168679 variant=base round=2 stages run=catchup window=4096 heartbeat_ms=50 scan peer=lagging reads=1608 read_ms=10.28/14.62/15.06 before_ms=7.25/12.67/14.03 keep_ms=0.03/0.04/0.05 after_ms=4.31/7.97/9.71 before_MB=31.76/54.11/62.17 after_MB=21.30/38.41/44.92 scan_MBps=4569 r=0.997 ms_per_MB=0.280 intercept_ms=-3.20
t=1792168679 variant=base round=2 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=5683 waited=5683 wait_ms=0.04/2.18/14.71 wait_total_ms=5279 by_groups_ms=0 by_range_ms=5093 by_reads_ms=0 unattributed_ms=186 ranges=623 range_ms=16.94/17.32/18.15 range_visited_MB_mean=61.0 range_records_mean=1024
t=1792168679 variant=base round=2 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15777 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=16468 positions_in_flight_mean=58 commit_samples=6206912 commit_p50_ms=1.070 commit_p99_ms=1.732 commit_p999_ms=2.870 commit_max_ms=20.063
t=1792168679 variant=base round=2 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1044683
t=1792168679 variant=base round=2 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65577 waited=65577 wait_ms=0.04/0.05/0.08 wait_total_ms=2290 by_groups_ms=9 by_range_ms=0 by_reads_ms=0 unattributed_ms=2281 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792168887 variant=base round=2 failovermode 64                     316596          4 of 10            timeout / timeout    81640   p90=timeout
t=1792168887 variant=base round=2 failovermode failover mode=fast count=4 slots_moved=1.50 takeovers_abandoned=0.25 lag_at_stop=0 timed_out=0
t=1792168887 variant=base round=2 failovermode failover mode=slow count=6 slots_moved=1.50 takeovers_abandoned=0.50 lag_at_stop=44308 timed_out=5
t=1792168963 variant=base round=3 catchup window=256 heartbeat_ms=50 lag_first=731250 lag_rate_under_load=-2363 lag_at_stop=686270 recovery_rate=7213 positions_per_tick=358 still_tick_share=0.005 bytes_in_flight_mean=4528 bytes_in_flight_max=263488 positions_in_flight_mean=35 commit_samples=126144 commit_p50_ms=66.865 commit_p99_ms=82.342 commit_p999_ms=93.217 commit_max_ms=105.118
t=1792168963 variant=base round=3 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1480 read_ms=13.32/13.61/13.88 before_ms=5.82/7.34/7.71 keep_ms=0.01/0.01/0.02 after_ms=7.35/9.14/9.51 before_MB=26.24/33.55/35.20 after_MB=36.89/44.11/45.72 scan_MBps=4735 r=0.041 ms_per_MB=3.457 intercept_ms=-204.88
t=1792168963 variant=base round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1479 waited=1479 wait_ms=0.04/11.92/15.74 wait_total_ms=4710 by_groups_ms=0 by_range_ms=4676 by_reads_ms=0 unattributed_ms=34 ranges=620 range_ms=15.78/16.07/16.35 range_visited_MB_mean=54.9 range_records_mean=1024
t=1792168963 variant=base round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15950 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=32936 positions_in_flight_mean=61 commit_samples=6310208 commit_p50_ms=1.056 commit_p99_ms=1.616 commit_p999_ms=2.791 commit_max_ms=10.122
t=1792168963 variant=base round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1088677
t=1792168963 variant=base round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65573 waited=65573 wait_ms=0.04/0.04/0.07 wait_total_ms=2257 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2256 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792168963 variant=base round=3 catchup window=1024 heartbeat_ms=50 lag_first=724555 lag_rate_under_load=-24160 lag_at_stop=264680 recovery_rate=26468 positions_per_tick=1402 still_tick_share=0.018 bytes_in_flight_mean=23055 bytes_in_flight_max=1111590 positions_in_flight_mean=182 commit_samples=198336 commit_p50_ms=1.140 commit_p99_ms=98.725 commit_p999_ms=99.754 commit_max_ms=100.044
t=1792168963 variant=base round=3 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1303 read_ms=15.98/16.35/16.62 before_ms=7.41/14.25/15.83 keep_ms=0.03/0.04/0.05 after_ms=7.85/10.91/11.88 before_MB=27.98/55.58/62.32 after_MB=34.25/48.63/54.41 scan_MBps=4077 r=0.986 ms_per_MB=0.544 intercept_ms=-18.16
t=1792168963 variant=base round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1301 waited=1301 wait_ms=0.13/14.19/18.30 wait_total_ms=5511 by_groups_ms=0 by_range_ms=5476 by_reads_ms=0 unattributed_ms=35 ranges=620 range_ms=18.36/18.65/18.97 range_visited_MB_mean=59.8 range_records_mean=1024
t=1792168963 variant=base round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15688 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=16468 positions_in_flight_mean=65 commit_samples=6193887 commit_p50_ms=1.069 commit_p99_ms=1.820 commit_p999_ms=3.085 commit_max_ms=6.002
t=1792168963 variant=base round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1039818
t=1792168963 variant=base round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65570 waited=65568 wait_ms=0.04/0.05/0.09 wait_total_ms=2225 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2224 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792168963 variant=base round=3 catchup window=4096 heartbeat_ms=50 lag_first=592735 lag_rate_under_load=-31145 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=2548 still_tick_share=0.018 bytes_in_flight_mean=23466 bytes_in_flight_max=4215808 positions_in_flight_mean=185 commit_samples=500928 commit_p50_ms=1.082 commit_p99_ms=90.113 commit_p999_ms=92.181 commit_max_ms=111.406
t=1792168963 variant=base round=3 stages run=catchup window=4096 heartbeat_ms=50 scan peer=lagging reads=1569 read_ms=14.47/15.00/15.35 before_ms=7.19/13.06/14.50 keep_ms=0.03/0.04/0.05 after_ms=4.36/8.21/10.00 before_MB=31.40/54.11/62.24 after_MB=20.87/37.57/44.57 scan_MBps=4440 r=0.989 ms_per_MB=0.283 intercept_ms=-3.00
t=1792168963 variant=base round=3 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=6481 waited=6481 wait_ms=0.04/0.24/15.73 wait_total_ms=5636 by_groups_ms=0 by_range_ms=5427 by_reads_ms=0 unattributed_ms=209 ranges=626 range_ms=17.25/17.59/18.32 range_visited_MB_mean=60.9 range_records_mean=1024
t=1792168963 variant=base round=3 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16107 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=24702 positions_in_flight_mean=58 commit_samples=6359808 commit_p50_ms=1.050 commit_p99_ms=1.397 commit_p999_ms=3.063 commit_max_ms=4.895
t=1792168963 variant=base round=3 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1109616
t=1792168963 variant=base round=3 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65553 waited=65553 wait_ms=0.04/0.04/0.06 wait_total_ms=2208 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2207 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792169171 variant=base round=3 failovermode 64                     317970          4 of 10            timeout / timeout    81055   p90=timeout
t=1792169171 variant=base round=3 failovermode failover mode=fast count=4 slots_moved=1.25 takeovers_abandoned=0.25 lag_at_stop=0 timed_out=0
t=1792169171 variant=base round=3 failovermode failover mode=slow count=6 slots_moved=1.50 takeovers_abandoned=0.33 lag_at_stop=44125 timed_out=5
t=1792169248 variant=both round=3 catchup window=256 heartbeat_ms=50 lag_first=516035 lag_rate_under_load=-27133 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16843 still_tick_share=0.018 bytes_in_flight_mean=9880 bytes_in_flight_max=263488 positions_in_flight_mean=78 commit_samples=5962496 commit_p50_ms=1.093 commit_p99_ms=2.234 commit_p999_ms=4.453 commit_max_ms=16.738
t=1792169248 variant=both round=3 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1081519
t=1792169248 variant=both round=3 stages run=catchup window=256 heartbeat_ms=50 scan reads=16475 read_ms=0.02/0.03/0.11
t=1792169248 variant=both round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67016 waited=67002 wait_ms=0.03/0.05/0.12 wait_total_ms=2275 by_groups_ms=77 by_range_ms=0 by_reads_ms=0 unattributed_ms=2198 ranges=590 range_ms=0.06/0.08/0.25 range_records_mean=1023
t=1792169248 variant=both round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15612 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=32936 positions_in_flight_mean=65 commit_samples=6158446 commit_p50_ms=1.068 commit_p99_ms=2.169 commit_p999_ms=3.841 commit_max_ms=8.194
t=1792169248 variant=both round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1024746
t=1792169248 variant=both round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65603 waited=65603 wait_ms=0.03/0.05/0.09 wait_total_ms=2258 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2254 ranges=0 range_ms=- range_records_mean=0
t=1792169248 variant=both round=3 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16578 still_tick_share=0.005 bytes_in_flight_mean=8645 bytes_in_flight_max=1053952 positions_in_flight_mean=68 commit_samples=5857246 commit_p50_ms=1.101 commit_p99_ms=2.428 commit_p999_ms=4.880 commit_max_ms=14.252
t=1792169248 variant=both round=3 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=917355
t=1792169248 variant=both round=3 stages run=catchup window=1024 heartbeat_ms=50 scan reads=3582 read_ms=0.03/0.07/0.17
t=1792169248 variant=both round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65815 waited=65810 wait_ms=0.04/0.05/0.12 wait_total_ms=2379 by_groups_ms=43 by_range_ms=24 by_reads_ms=0 unattributed_ms=2312 ranges=600 range_ms=0.07/0.12/0.39 range_records_mean=1024
t=1792169248 variant=both round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15436 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=16468 positions_in_flight_mean=65 commit_samples=6068083 commit_p50_ms=1.083 commit_p99_ms=2.088 commit_p999_ms=4.508 commit_max_ms=15.215
t=1792169248 variant=both round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=986686
t=1792169248 variant=both round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65573 waited=65573 wait_ms=0.04/0.05/0.09 wait_total_ms=2259 by_groups_ms=8 by_range_ms=0 by_reads_ms=0 unattributed_ms=2251 ranges=0 range_ms=- range_records_mean=0
t=1792169248 variant=both round=3 catchup window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16527 still_tick_share=0.018 bytes_in_flight_mean=7822 bytes_in_flight_max=4215808 positions_in_flight_mean=61 commit_samples=5807806 commit_p50_ms=1.095 commit_p99_ms=2.633 commit_p999_ms=4.724 commit_max_ms=14.515
t=1792169248 variant=both round=3 stages run=catchup window=4096 heartbeat_ms=50 stamps_dropped=875212
t=1792169248 variant=both round=3 stages run=catchup window=4096 heartbeat_ms=50 scan reads=2357 read_ms=0.04/0.09/0.19
t=1792169248 variant=both round=3 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=65570 waited=65570 wait_ms=0.04/0.05/0.12 wait_total_ms=2409 by_groups_ms=27 by_range_ms=0 by_reads_ms=0 unattributed_ms=2383 ranges=624 range_ms=0.07/0.07/0.08 range_records_mean=1022
t=1792169248 variant=both round=3 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15545 still_tick_share=0.000 bytes_in_flight_mean=9057 bytes_in_flight_max=32936 positions_in_flight_mean=71 commit_samples=6166897 commit_p50_ms=1.072 commit_p99_ms=1.978 commit_p999_ms=3.820 commit_max_ms=37.939
t=1792169248 variant=both round=3 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1027934
t=1792169248 variant=both round=3 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65555 waited=65553 wait_ms=0.04/0.04/0.08 wait_total_ms=2233 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2230 ranges=0 range_ms=- range_records_mean=0
t=1792169457 variant=both round=3 failovermode 64                     327494         10 of 10                153 / 198        0   p90=198
t=1792169457 variant=both round=3 failovermode failover mode=fast count=10 slots_moved=1.00 takeovers_abandoned=0.00 lag_at_stop=0 timed_out=0
t=1792169478 variant=both round=4 catchup window=256 heartbeat_ms=50 lag_first=475085 lag_rate_under_load=-24979 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17691 still_tick_share=0.018 bytes_in_flight_mean=9057 bytes_in_flight_max=263488 positions_in_flight_mean=71 commit_samples=6253508 commit_p50_ms=1.069 commit_p99_ms=1.790 commit_p999_ms=3.692 commit_max_ms=8.257
t=1792169478 variant=both round=4 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1219471
t=1792169478 variant=both round=4 stages run=catchup window=256 heartbeat_ms=50 scan reads=17863 read_ms=0.02/0.02/0.06
t=1792169478 variant=both round=4 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=66993 waited=66980 wait_ms=0.03/0.05/0.11 wait_total_ms=2089 by_groups_ms=54 by_range_ms=0 by_reads_ms=0 unattributed_ms=2035 ranges=615 range_ms=0.06/0.07/0.09 range_records_mean=1022
t=1792169478 variant=both round=4 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15777 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=32936 positions_in_flight_mean=65 commit_samples=6225920 commit_p50_ms=1.066 commit_p99_ms=1.761 commit_p999_ms=3.824 commit_max_ms=15.933
t=1792169478 variant=both round=4 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1053478
t=1792169478 variant=both round=4 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65573 waited=65572 wait_ms=0.04/0.04/0.08 wait_total_ms=2277 by_groups_ms=16 by_range_ms=0 by_reads_ms=0 unattributed_ms=2261 ranges=0 range_ms=- range_records_mean=0
t=1792169478 variant=both round=4 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17515 still_tick_share=0.005 bytes_in_flight_mean=9469 bytes_in_flight_max=1053952 positions_in_flight_mean=74 commit_samples=6195761 commit_p50_ms=1.068 commit_p99_ms=1.768 commit_p999_ms=3.547 commit_max_ms=13.397
t=1792169478 variant=both round=4 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1058543
t=1792169478 variant=both round=4 stages run=catchup window=1024 heartbeat_ms=50 scan reads=3789 read_ms=0.03/0.05/0.14
t=1792169478 variant=both round=4 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65792 waited=65788 wait_ms=0.04/0.04/0.09 wait_total_ms=2330 by_groups_ms=44 by_range_ms=17 by_reads_ms=0 unattributed_ms=2270 ranges=603 range_ms=0.07/0.08/0.22 range_records_mean=1024
t=1792169478 variant=both round=4 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15682 still_tick_share=0.000 bytes_in_flight_mean=9057 bytes_in_flight_max=16468 positions_in_flight_mean=71 commit_samples=6175486 commit_p50_ms=1.073 commit_p99_ms=1.481 commit_p999_ms=3.239 commit_max_ms=160.364
t=1792169478 variant=both round=4 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1031833
t=1792169478 variant=both round=4 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65562 waited=65562 wait_ms=0.04/0.04/0.06 wait_total_ms=2225 by_groups_ms=0 by_range_ms=0 by_reads_ms=0 unattributed_ms=2224 ranges=0 range_ms=- range_records_mean=0
t=1792169478 variant=both round=4 catchup window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17688 still_tick_share=0.018 bytes_in_flight_mean=7410 bytes_in_flight_max=4158170 positions_in_flight_mean=58 commit_samples=6268888 commit_p50_ms=1.062 commit_p99_ms=1.647 commit_p999_ms=3.851 commit_max_ms=29.483
t=1792169478 variant=both round=4 stages run=catchup window=4096 heartbeat_ms=50 stamps_dropped=1068270
t=1792169478 variant=both round=4 stages run=catchup window=4096 heartbeat_ms=50 scan reads=2362 read_ms=0.04/0.08/0.17
t=1792169478 variant=both round=4 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=65577 waited=65577 wait_ms=0.04/0.04/0.09 wait_total_ms=2302 by_groups_ms=43 by_range_ms=0 by_reads_ms=0 unattributed_ms=2259 ranges=611 range_ms=0.07/0.08/0.09 range_records_mean=1022
t=1792169478 variant=both round=4 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15797 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=32936 positions_in_flight_mean=61 commit_samples=6255872 commit_p50_ms=1.071 commit_p99_ms=1.456 commit_p999_ms=3.101 commit_max_ms=6.800
t=1792169478 variant=both round=4 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1065629
t=1792169478 variant=both round=4 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65575 waited=65575 wait_ms=0.04/0.04/0.06 wait_total_ms=2212 by_groups_ms=12 by_range_ms=0 by_reads_ms=0 unattributed_ms=2199 ranges=0 range_ms=- range_records_mean=0
t=1792169687 variant=both round=4 failovermode 64                     320546         10 of 10                195 / 198        0   p90=198
t=1792169687 variant=both round=4 failovermode failover mode=fast count=10 slots_moved=1.00 takeovers_abandoned=0.00 lag_at_stop=0 timed_out=0
t=1792169708 variant=base round=4 catchup window=256 heartbeat_ms=50 lag_first=731120 lag_rate_under_load=-2221 lag_at_stop=688805 recovery_rate=7193 positions_per_tick=349 still_tick_share=0.005 bytes_in_flight_mean=2058 bytes_in_flight_max=263488 positions_in_flight_mean=16 commit_samples=127666 commit_p50_ms=67.470 commit_p99_ms=82.952 commit_p999_ms=85.946 commit_max_ms=86.650
t=1792169708 variant=base round=4 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1473 read_ms=13.40/13.66/14.25 before_ms=5.81/7.31/7.62 keep_ms=0.01/0.01/0.02 after_ms=7.44/9.12/9.54 before_MB=25.78/33.05/34.70 after_MB=37.34/44.22/45.79 scan_MBps=4706 r=0.054 ms_per_MB=2.981 intercept_ms=-174.72
t=1792169708 variant=base round=4 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1472 waited=1472 wait_ms=0.05/12.41/16.33 wait_total_ms=5025 by_groups_ms=0 by_range_ms=4992 by_reads_ms=0 unattributed_ms=33 ranges=620 range_ms=16.47/16.83/17.60 range_visited_MB_mean=54.9 range_records_mean=1024
t=1792169708 variant=base round=4 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16041 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=16468 positions_in_flight_mean=65 commit_samples=6352192 commit_p50_ms=1.045 commit_p99_ms=1.365 commit_p999_ms=3.044 commit_max_ms=175.961
t=1792169708 variant=base round=4 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1106463
t=1792169708 variant=base round=4 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65556 waited=65556 wait_ms=0.04/0.04/0.06 wait_total_ms=2208 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2204 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792169708 variant=base round=4 catchup window=1024 heartbeat_ms=50 lag_first=717665 lag_rate_under_load=-23064 lag_at_stop=277225 recovery_rate=27722 positions_per_tick=1346 still_tick_share=0.018 bytes_in_flight_mean=20173 bytes_in_flight_max=1111590 positions_in_flight_mean=159 commit_samples=192512 commit_p50_ms=1.132 commit_p99_ms=103.001 commit_p999_ms=109.497 commit_max_ms=124.672
t=1792169708 variant=base round=4 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1242 read_ms=16.55/17.00/18.48 before_ms=8.44/14.79/16.55 keep_ms=0.03/0.04/0.05 after_ms=8.42/11.39/12.67 before_MB=29.17/55.97/62.43 after_MB=33.96/48.20/53.74 scan_MBps=3873 r=0.962 ms_per_MB=0.462 intercept_ms=-12.36
t=1792169708 variant=base round=4 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1240 waited=1240 wait_ms=0.14/14.44/18.48 wait_total_ms=5618 by_groups_ms=0 by_range_ms=5586 by_reads_ms=0 unattributed_ms=32 ranges=620 range_ms=18.44/18.77/20.55 range_visited_MB_mean=59.8 range_records_mean=1024
t=1792169708 variant=base round=4 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16019 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=41170 positions_in_flight_mean=58 commit_samples=6338112 commit_p50_ms=1.057 commit_p99_ms=1.431 commit_p999_ms=2.642 commit_max_ms=5.006
t=1792169708 variant=base round=4 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1100307
t=1792169708 variant=base round=4 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65583 waited=65583 wait_ms=0.04/0.04/0.06 wait_total_ms=2244 by_groups_ms=6 by_range_ms=0 by_reads_ms=0 unattributed_ms=2238 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792169708 variant=base round=4 catchup window=4096 heartbeat_ms=50 lag_first=551135 lag_rate_under_load=-28564 lag_at_stop=7475 recovery_rate=748 positions_per_tick=1627 still_tick_share=0.018 bytes_in_flight_mean=17291 bytes_in_flight_max=4273446 positions_in_flight_mean=136 commit_samples=169984 commit_p50_ms=41.134 commit_p99_ms=108.393 commit_p999_ms=110.448 commit_max_ms=114.192
t=1792169708 variant=base round=4 stages run=catchup window=4096 heartbeat_ms=50 scan peer=lagging reads=1443 read_ms=17.37/17.85/18.41 before_ms=7.95/15.33/17.25 keep_ms=0.03/0.04/0.05 after_ms=4.68/10.35/12.37 before_MB=29.11/54.82/62.31 after_MB=19.54/38.10/45.48 scan_MBps=3724 r=0.998 ms_per_MB=0.323 intercept_ms=-2.70
t=1792169708 variant=base round=4 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=1435 waited=1435 wait_ms=0.04/13.67/18.39 wait_total_ms=5157 by_groups_ms=0 by_range_ms=5118 by_reads_ms=0 unattributed_ms=39 ranges=619 range_ms=18.65/18.95/19.50 range_visited_MB_mean=60.1 range_records_mean=1024
t=1792169708 variant=base round=4 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15853 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=32936 positions_in_flight_mean=61 commit_samples=6247477 commit_p50_ms=1.063 commit_p99_ms=1.623 commit_p999_ms=3.276 commit_max_ms=42.475
t=1792169708 variant=base round=4 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1061915
t=1792169708 variant=base round=4 stages run=control window=4096 heartbeat_ms=50 scan peer=lagging reads=2 read_ms=12.19/12.19/12.19 before_ms=12.18/12.18/12.18 keep_ms=0.01/0.01/0.01 after_ms=0.01/0.01/0.01 before_MB=32.65/32.65/32.65 after_MB=0.01/0.01/0.01 scan_MBps=3039 r=-1.000 ms_per_MB=-641.464 intercept_ms=20959.24
t=1792169708 variant=base round=4 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65571 waited=65570 wait_ms=0.04/0.04/0.07 wait_total_ms=2228 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2227 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792169916 variant=base round=4 failovermode 64                     320849          4 of 10            timeout / timeout    80015   p90=timeout
t=1792169916 variant=base round=4 failovermode failover mode=fast count=4 slots_moved=1.25 takeovers_abandoned=0.25 lag_at_stop=0 timed_out=0
t=1792169916 variant=base round=4 failovermode failover mode=slow count=6 slots_moved=1.50 takeovers_abandoned=0.33 lag_at_stop=43137 timed_out=5
t=1792169992 variant=base round=5 catchup window=256 heartbeat_ms=50 lag_first=682695 lag_rate_under_load=-2042 lag_at_stop=643825 recovery_rate=6641 positions_per_tick=314 still_tick_share=0.005 bytes_in_flight_mean=6175 bytes_in_flight_max=263488 positions_in_flight_mean=48 commit_samples=115008 commit_p50_ms=73.341 commit_p99_ms=110.744 commit_p999_ms=126.110 commit_max_ms=128.763
t=1792169992 variant=base round=5 stages run=catchup window=256 heartbeat_ms=50 scan peer=lagging reads=1310 read_ms=14.65/16.72/20.75 before_ms=6.16/7.34/7.90 keep_ms=0.01/0.02/0.02 after_ms=8.56/11.08/13.80 before_MB=24.15/30.62/32.07 after_MB=38.98/45.29/46.67 scan_MBps=4184 r=0.001 ms_per_MB=0.426 intercept_ms=-11.78
t=1792169992 variant=base round=5 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1309 waited=1309 wait_ms=0.05/14.15/20.49 wait_total_ms=5348 by_groups_ms=0 by_range_ms=5319 by_reads_ms=0 unattributed_ms=29 ranges=613 range_ms=17.26/20.87/25.95 range_visited_MB_mean=55.0 range_records_mean=1024
t=1792169992 variant=base round=5 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15309 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=32936 positions_in_flight_mean=58 commit_samples=6016284 commit_p50_ms=1.079 commit_p99_ms=2.160 commit_p999_ms=8.403 commit_max_ms=24.875
t=1792169992 variant=base round=5 stages run=control window=256 heartbeat_ms=50 stamps_dropped=965113
t=1792169992 variant=base round=5 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65561 waited=65561 wait_ms=0.04/0.04/0.06 wait_total_ms=2225 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2224 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792169992 variant=base round=5 catchup window=1024 heartbeat_ms=50 lag_first=723775 lag_rate_under_load=-22813 lag_at_stop=289380 recovery_rate=28938 positions_per_tick=1320 still_tick_share=0.018 bytes_in_flight_mean=20585 bytes_in_flight_max=1053952 positions_in_flight_mean=162 commit_samples=193152 commit_p50_ms=1.099 commit_p99_ms=114.819 commit_p999_ms=169.287 commit_max_ms=188.289
t=1792169992 variant=base round=5 stages run=catchup window=1024 heartbeat_ms=50 scan peer=lagging reads=1198 read_ms=16.85/18.00/21.14 before_ms=8.74/15.49/17.01 keep_ms=0.03/0.04/0.05 after_ms=8.67/11.81/13.06 before_MB=29.68/56.25/62.47 after_MB=33.44/48.61/53.94 scan_MBps=3738 r=0.559 ms_per_MB=0.515 intercept_ms=-15.07
t=1792169992 variant=base round=5 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1196 waited=1196 wait_ms=0.13/14.21/18.04 wait_total_ms=5216 by_groups_ms=0 by_range_ms=5185 by_reads_ms=0 unattributed_ms=31 ranges=619 range_ms=17.90/18.85/23.79 range_visited_MB_mean=59.7 range_records_mean=1024
t=1792169992 variant=base round=5 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15520 still_tick_share=0.000 bytes_in_flight_mean=6998 bytes_in_flight_max=16468 positions_in_flight_mean=55 commit_samples=6139068 commit_p50_ms=1.082 commit_p99_ms=1.638 commit_p999_ms=3.735 commit_max_ms=34.354
t=1792169992 variant=base round=5 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1016458
t=1792169992 variant=base round=5 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65568 waited=65568 wait_ms=0.04/0.04/0.07 wait_total_ms=2253 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2250 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792169992 variant=base round=5 catchup window=4096 heartbeat_ms=50 lag_first=563550 lag_rate_under_load=-29617 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=1848 still_tick_share=0.020 bytes_in_flight_mean=23466 bytes_in_flight_max=4215808 positions_in_flight_mean=185 commit_samples=244608 commit_p50_ms=1.161 commit_p99_ms=105.007 commit_p999_ms=151.216 commit_max_ms=170.031
t=1792169992 variant=base round=5 stages run=catchup window=4096 heartbeat_ms=50 scan peer=lagging reads=1496 read_ms=16.11/17.08/19.42 before_ms=7.50/14.32/16.12 keep_ms=0.03/0.04/0.05 after_ms=4.66/9.98/11.62 before_MB=29.35/54.53/62.31 after_MB=19.77/38.85/46.52 scan_MBps=3936 r=0.984 ms_per_MB=0.309 intercept_ms=-2.75
t=1792169992 variant=base round=5 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=2624 waited=2624 wait_ms=0.04/9.69/17.56 wait_total_ms=5336 by_groups_ms=0 by_range_ms=5257 by_reads_ms=0 unattributed_ms=78 ranges=611 range_ms=18.12/19.04/23.45 range_visited_MB_mean=60.3 range_records_mean=1024
t=1792169992 variant=base round=5 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15125 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=35666 positions_in_flight_mean=58 commit_samples=6097717 commit_p50_ms=1.083 commit_p99_ms=1.979 commit_p999_ms=3.581 commit_max_ms=23.215
t=1792169992 variant=base round=5 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=998744
t=1792169992 variant=base round=5 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65577 waited=65577 wait_ms=0.04/0.04/0.07 wait_total_ms=2269 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2267 ranges=0 range_ms=- range_visited_MB_mean=-0.0 range_records_mean=0
t=1792170199 variant=base round=5 failovermode 64                     314919          4 of 10            timeout / timeout    79300   p90=timeout
t=1792170199 variant=base round=5 failovermode failover mode=fast count=4 slots_moved=1.25 takeovers_abandoned=0.00 lag_at_stop=0 timed_out=0
t=1792170199 variant=base round=5 failovermode failover mode=slow count=6 slots_moved=1.50 takeovers_abandoned=0.33 lag_at_stop=43061 timed_out=5
t=1792170275 variant=both round=5 catchup window=256 heartbeat_ms=50 lag_first=492505 lag_rate_under_load=-25896 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17524 still_tick_share=0.018 bytes_in_flight_mean=8234 bytes_in_flight_max=263488 positions_in_flight_mean=65 commit_samples=6204610 commit_p50_ms=1.063 commit_p99_ms=2.034 commit_p999_ms=3.906 commit_max_ms=21.726
t=1792170275 variant=both round=5 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1187304
t=1792170275 variant=both round=5 stages run=catchup window=256 heartbeat_ms=50 scan reads=16983 read_ms=0.02/0.03/0.10
t=1792170275 variant=both round=5 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67058 waited=67041 wait_ms=0.03/0.05/0.12 wait_total_ms=2198 by_groups_ms=71 by_range_ms=0 by_reads_ms=0 unattributed_ms=2127 ranges=612 range_ms=0.07/0.08/0.15 range_records_mean=1023
t=1792170275 variant=both round=5 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=14321 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=16468 positions_in_flight_mean=61 commit_samples=5756608 commit_p50_ms=1.098 commit_p99_ms=2.985 commit_p999_ms=6.349 commit_max_ms=24.832
t=1792170275 variant=both round=5 stages run=control window=256 heartbeat_ms=50 stamps_dropped=855464
t=1792170275 variant=both round=5 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65544 waited=65544 wait_ms=0.03/0.05/0.11 wait_total_ms=2383 by_groups_ms=7 by_range_ms=0 by_reads_ms=0 unattributed_ms=2375 ranges=0 range_ms=- range_records_mean=0
t=1792170275 variant=both round=5 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17491 still_tick_share=0.018 bytes_in_flight_mean=8645 bytes_in_flight_max=1053952 positions_in_flight_mean=68 commit_samples=6202682 commit_p50_ms=1.066 commit_p99_ms=1.930 commit_p999_ms=4.437 commit_max_ms=20.635
t=1792170275 variant=both round=5 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1054585
t=1792170275 variant=both round=5 stages run=catchup window=1024 heartbeat_ms=50 scan reads=3924 read_ms=0.03/0.05/0.14
t=1792170275 variant=both round=5 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65739 waited=65735 wait_ms=0.03/0.05/0.11 wait_total_ms=2276 by_groups_ms=46 by_range_ms=0 by_reads_ms=0 unattributed_ms=2231 ranges=599 range_ms=0.07/0.08/0.25 range_records_mean=1023
t=1792170275 variant=both round=5 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15926 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=131744 positions_in_flight_mean=61 commit_samples=6358392 commit_p50_ms=1.048 commit_p99_ms=2.017 commit_p999_ms=5.526 commit_max_ms=19.758
t=1792170275 variant=both round=5 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1105322
t=1792170275 variant=both round=5 stages run=control window=1024 heartbeat_ms=50 scan reads=69 read_ms=0.03/0.09/9.60
t=1792170275 variant=both round=5 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65690 waited=65689 wait_ms=0.03/0.04/0.12 wait_total_ms=2676 by_groups_ms=624 by_range_ms=0 by_reads_ms=0 unattributed_ms=2052 ranges=0 range_ms=- range_records_mean=0
t=1792170275 variant=both round=5 catchup window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17406 still_tick_share=0.018 bytes_in_flight_mean=8234 bytes_in_flight_max=4215808 positions_in_flight_mean=65 commit_samples=6171269 commit_p50_ms=1.067 commit_p99_ms=1.625 commit_p999_ms=3.222 commit_max_ms=192.632
t=1792170275 variant=both round=5 stages run=catchup window=4096 heartbeat_ms=50 stamps_dropped=1026643
t=1792170275 variant=both round=5 stages run=catchup window=4096 heartbeat_ms=50 scan reads=2307 read_ms=0.04/0.09/0.17
t=1792170275 variant=both round=5 stages run=catchup window=4096 heartbeat_ms=50 blocking peer=lagging appends=65579 waited=65578 wait_ms=0.04/0.04/0.08 wait_total_ms=2325 by_groups_ms=15 by_range_ms=0 by_reads_ms=0 unattributed_ms=2310 ranges=616 range_ms=0.07/0.08/0.16 range_records_mean=1023
t=1792170275 variant=both round=5 control window=4096 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15912 still_tick_share=0.000 bytes_in_flight_mean=6587 bytes_in_flight_max=16468 positions_in_flight_mean=52 commit_samples=6305303 commit_p50_ms=1.058 commit_p99_ms=1.485 commit_p999_ms=3.267 commit_max_ms=37.142
t=1792170275 variant=both round=5 stages run=control window=4096 heartbeat_ms=50 stamps_dropped=1086552
t=1792170275 variant=both round=5 stages run=control window=4096 heartbeat_ms=50 blocking peer=lagging appends=65593 waited=65593 wait_ms=0.04/0.04/0.07 wait_total_ms=2206 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2205 ranges=0 range_ms=- range_records_mean=0
t=1792170485 variant=both round=5 failovermode 64                     315356         10 of 10                188 / 203        0   p90=203
t=1792170485 variant=both round=5 failovermode failover mode=fast count=10 slots_moved=1.00 takeovers_abandoned=0.00 lag_at_stop=0 timed_out=0
```

Medians over the five rounds — read p50: 13.8 / 16.6 / 16.1 ms base, 0.02 / 0.03 / 0.04 ms both (256 / 1 024 / 4 096); catch-up commit p99: 89 / 103 / 105 ms base, 1.8 / 1.9 / 2.0 ms both; commits a second in catch-up: 6 400 / 9 600 / 12 200 base, 311 000 / 310 000 / 308 000 both (controls 306 000–312 000). Failover mode: base 4 of 10 within 1 s in every round, survivor lag about 80 000; both 10 of 10, p50 153–195 ms, lag 0. Not accepted; direction only; page cache warm.

## D-143 B, catch-up with the log evicted from the page cache — preliminary (2026-10-16)

`STRAUM_BENCH_COLD=1`: before the replica restarts (and at the same point in the control), `sync` and then every file of every node's data directory evicted from the page cache with GNU `dd iflag=nocache count=0` (`posix_fadvise(DONTNEED)`; checked with `fincore`: 64 MiB resident → 0, a 64 MiB read 25 ms cold against 3 ms warm). `drop_caches` is refused in this container, even `--privileged`. Three rotated rounds of `both` cold, `both` warm and `base` (0c5f007) cold, windows 256 and 1 024, 50 ms, paired controls; 20:31–20:53, no sleep in the power log.

```text
t=1792171949 variant=both-cold round=1 catchup window=256 heartbeat_ms=50 lag_first=428274 lag_rate_under_load=-22518 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17436 still_tick_share=0.005 bytes_in_flight_mean=9880 bytes_in_flight_max=263488 positions_in_flight_mean=78 commit_samples=6067966 commit_p50_ms=1.062 commit_p99_ms=2.487 commit_p999_ms=6.644 commit_max_ms=139.264
t=1792171949 variant=both-cold round=1 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1151073
t=1792171949 variant=both-cold round=1 stages run=catchup window=256 heartbeat_ms=50 scan reads=18149 read_ms=0.02/0.02/0.07
t=1792171949 variant=both-cold round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67104 waited=67089 wait_ms=0.03/0.05/0.11 wait_total_ms=2132 by_groups_ms=50 by_range_ms=9 by_reads_ms=0 unattributed_ms=2072 ranges=607 range_ms=0.05/0.06/0.11 range_records_mean=1024
t=1792171949 variant=both-cold round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16072 still_tick_share=0.000 bytes_in_flight_mean=6587 bytes_in_flight_max=32936 positions_in_flight_mean=52 commit_samples=6324001 commit_p50_ms=1.061 commit_p99_ms=1.588 commit_p999_ms=3.339 commit_max_ms=12.122
t=1792171949 variant=both-cold round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1094351
t=1792171949 variant=both-cold round=1 stages run=control window=256 heartbeat_ms=50 scan reads=3 read_ms=0.04/11.22/11.22
t=1792171949 variant=both-cold round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65588 waited=65588 wait_ms=0.04/0.04/0.06 wait_total_ms=2195 by_groups_ms=4 by_range_ms=0 by_reads_ms=0 unattributed_ms=2191 ranges=0 range_ms=- range_records_mean=0
t=1792171949 variant=both-cold round=1 catchup window=1024 heartbeat_ms=50 lag_first=97955 lag_rate_under_load=-5150 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16636 still_tick_share=0.005 bytes_in_flight_mean=13586 bytes_in_flight_max=1053952 positions_in_flight_mean=107 commit_samples=5772628 commit_p50_ms=1.068 commit_p99_ms=3.322 commit_p999_ms=7.226 commit_max_ms=55.348
t=1792171949 variant=both-cold round=1 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=883614
t=1792171949 variant=both-cold round=1 stages run=catchup window=1024 heartbeat_ms=50 scan reads=4127 read_ms=0.03/0.09/0.23
t=1792171949 variant=both-cold round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65939 waited=65930 wait_ms=0.03/0.05/0.17 wait_total_ms=2341 by_groups_ms=51 by_range_ms=48 by_reads_ms=0 unattributed_ms=2242 ranges=686 range_ms=0.08/0.12/1.65 range_records_mean=1024
t=1792171949 variant=both-cold round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16250 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=74106 positions_in_flight_mean=58 commit_samples=6159765 commit_p50_ms=1.021 commit_p99_ms=3.543 commit_p999_ms=10.432 commit_max_ms=32.815
t=1792171949 variant=both-cold round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1021575
t=1792171949 variant=both-cold round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65724 waited=65724 wait_ms=0.03/0.04/0.12 wait_total_ms=1924 by_groups_ms=33 by_range_ms=0 by_reads_ms=0 unattributed_ms=1891 ranges=0 range_ms=- range_records_mean=0
t=1792172090 variant=both-warm round=1 catchup window=256 heartbeat_ms=50 lag_first=656239 lag_rate_under_load=-34505 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16591 still_tick_share=0.018 bytes_in_flight_mean=10704 bytes_in_flight_max=263488 positions_in_flight_mean=84 commit_samples=5792600 commit_p50_ms=1.067 commit_p99_ms=3.587 commit_p999_ms=13.761 commit_max_ms=52.956
t=1792172090 variant=both-warm round=1 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1020466
t=1792172090 variant=both-warm round=1 stages run=catchup window=256 heartbeat_ms=50 scan reads=18417 read_ms=0.02/0.05/0.16
t=1792172090 variant=both-warm round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67213 waited=67197 wait_ms=0.03/0.05/0.19 wait_total_ms=2442 by_groups_ms=127 by_range_ms=0 by_reads_ms=0 unattributed_ms=2316 ranges=695 range_ms=0.06/0.07/0.10 range_records_mean=1023
t=1792172090 variant=both-warm round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15157 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=32936 positions_in_flight_mean=65 commit_samples=6039230 commit_p50_ms=1.077 commit_p99_ms=2.222 commit_p999_ms=6.676 commit_max_ms=44.176
t=1792172090 variant=both-warm round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=974132
t=1792172090 variant=both-warm round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65592 waited=65592 wait_ms=0.04/0.04/0.08 wait_total_ms=2290 by_groups_ms=4 by_range_ms=0 by_reads_ms=0 unattributed_ms=2286 ranges=0 range_ms=- range_records_mean=0
t=1792172090 variant=both-warm round=1 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17107 still_tick_share=0.015 bytes_in_flight_mean=8234 bytes_in_flight_max=996314 positions_in_flight_mean=65 commit_samples=6044840 commit_p50_ms=1.073 commit_p99_ms=2.236 commit_p999_ms=8.335 commit_max_ms=44.811
t=1792172090 variant=both-warm round=1 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=987888
t=1792172090 variant=both-warm round=1 stages run=catchup window=1024 heartbeat_ms=50 scan reads=3988 read_ms=0.03/0.05/0.12
t=1792172090 variant=both-warm round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65724 waited=65720 wait_ms=0.04/0.04/0.11 wait_total_ms=2344 by_groups_ms=25 by_range_ms=0 by_reads_ms=0 unattributed_ms=2319 ranges=613 range_ms=0.07/0.08/0.09 range_records_mean=1023
t=1792172090 variant=both-warm round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15502 still_tick_share=0.000 bytes_in_flight_mean=6998 bytes_in_flight_max=24702 positions_in_flight_mean=55 commit_samples=6108881 commit_p50_ms=1.070 commit_p99_ms=2.045 commit_p999_ms=6.038 commit_max_ms=39.993
t=1792172090 variant=both-warm round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1003962
t=1792172090 variant=both-warm round=1 stages run=control window=1024 heartbeat_ms=50 scan reads=2 read_ms=7.43/7.43/7.43
t=1792172090 variant=both-warm round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65614 waited=65614 wait_ms=0.04/0.04/0.09 wait_total_ms=2262 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2261 ranges=0 range_ms=- range_records_mean=0
t=1792172231 variant=base-cold round=1 catchup window=256 heartbeat_ms=50 lag_first=814710 lag_rate_under_load=-2964 lag_at_stop=758290 recovery_rate=8974 positions_per_tick=446 still_tick_share=0.007 bytes_in_flight_mean=4528 bytes_in_flight_max=263488 positions_in_flight_mean=35 commit_samples=167360 commit_p50_ms=52.103 commit_p99_ms=89.658 commit_p999_ms=137.764 commit_max_ms=157.491
t=1792172231 variant=base-cold round=1 stages run=catchup window=256 heartbeat_ms=50 scan reads=1841 read_ms=10.33/11.04/17.76
t=1792172231 variant=base-cold round=1 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1841 waited=1841 wait_ms=0.03/3.67/10.40 wait_total_ms=1706 by_groups_ms=0 by_range_ms=1654 by_reads_ms=0 unattributed_ms=52 ranges=583 range_ms=10.22/10.83/13.23 range_records_mean=1024
t=1792172231 variant=base-cold round=1 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=14436 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=24702 positions_in_flight_mean=61 commit_samples=6153088 commit_p50_ms=1.063 commit_p99_ms=2.099 commit_p999_ms=6.947 commit_max_ms=56.959
t=1792172231 variant=base-cold round=1 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1021788
t=1792172231 variant=base-cold round=1 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65576 waited=65576 wait_ms=0.04/0.04/0.07 wait_total_ms=2226 by_groups_ms=6 by_range_ms=0 by_reads_ms=0 unattributed_ms=2221 ranges=0 range_ms=- range_records_mean=0
t=1792172231 variant=base-cold round=1 catchup window=1024 heartbeat_ms=50 lag_first=726180 lag_rate_under_load=-28881 lag_at_stop=176410 recovery_rate=17641 positions_per_tick=1740 still_tick_share=0.015 bytes_in_flight_mean=9469 bytes_in_flight_max=996314 positions_in_flight_mean=74 commit_samples=188967 commit_p50_ms=53.596 commit_p99_ms=88.658 commit_p999_ms=176.551 commit_max_ms=192.751
t=1792172231 variant=base-cold round=1 stages run=catchup window=1024 heartbeat_ms=50 scan reads=1631 read_ms=12.28/13.03/18.32
t=1792172231 variant=base-cold round=1 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1627 waited=1627 wait_ms=0.10/9.51/12.57 wait_total_ms=4561 by_groups_ms=0 by_range_ms=4511 by_reads_ms=0 unattributed_ms=50 ranges=650 range_ms=12.77/12.94/18.47 range_records_mean=1024
t=1792172231 variant=base-cold round=1 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15052 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=16468 positions_in_flight_mean=58 commit_samples=5990592 commit_p50_ms=1.070 commit_p99_ms=2.009 commit_p999_ms=6.904 commit_max_ms=316.163
t=1792172231 variant=base-cold round=1 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=953847
t=1792172231 variant=base-cold round=1 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65549 waited=65549 wait_ms=0.04/0.04/0.07 wait_total_ms=2275 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2273 ranges=0 range_ms=- range_records_mean=0
t=1792172371 variant=both-warm round=2 catchup window=256 heartbeat_ms=50 lag_first=467415 lag_rate_under_load=-24576 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17562 still_tick_share=0.018 bytes_in_flight_mean=9469 bytes_in_flight_max=263488 positions_in_flight_mean=74 commit_samples=6224625 commit_p50_ms=1.070 commit_p99_ms=1.606 commit_p999_ms=3.612 commit_max_ms=43.385
t=1792172371 variant=both-warm round=2 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1201723
t=1792172371 variant=both-warm round=2 stages run=catchup window=256 heartbeat_ms=50 scan reads=17293 read_ms=0.02/0.02/0.09
t=1792172371 variant=both-warm round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67007 waited=66996 wait_ms=0.03/0.04/0.11 wait_total_ms=2191 by_groups_ms=60 by_range_ms=0 by_reads_ms=0 unattributed_ms=2131 ranges=578 range_ms=0.07/0.07/0.15 range_records_mean=1023
t=1792172371 variant=both-warm round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15215 still_tick_share=0.000 bytes_in_flight_mean=8645 bytes_in_flight_max=16468 positions_in_flight_mean=68 commit_samples=5958579 commit_p50_ms=1.071 commit_p99_ms=2.763 commit_p999_ms=10.782 commit_max_ms=53.611
t=1792172371 variant=both-warm round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=940585
t=1792172371 variant=both-warm round=2 stages run=control window=256 heartbeat_ms=50 scan reads=2 read_ms=25.78/25.78/25.78
t=1792172371 variant=both-warm round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65576 waited=65576 wait_ms=0.04/0.04/0.10 wait_total_ms=2331 by_groups_ms=16 by_range_ms=0 by_reads_ms=0 unattributed_ms=2316 ranges=0 range_ms=- range_records_mean=0
t=1792172371 variant=both-warm round=2 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17184 still_tick_share=0.015 bytes_in_flight_mean=13174 bytes_in_flight_max=1053952 positions_in_flight_mean=104 commit_samples=6134251 commit_p50_ms=1.077 commit_p99_ms=1.824 commit_p999_ms=4.359 commit_max_ms=29.944
t=1792172371 variant=both-warm round=2 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1024498
t=1792172371 variant=both-warm round=2 stages run=catchup window=1024 heartbeat_ms=50 scan reads=3698 read_ms=0.03/0.05/0.16
t=1792172371 variant=both-warm round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65707 waited=65705 wait_ms=0.04/0.04/0.10 wait_total_ms=2323 by_groups_ms=55 by_range_ms=0 by_reads_ms=0 unattributed_ms=2268 ranges=622 range_ms=0.07/0.08/0.11 range_records_mean=1023
t=1792172371 variant=both-warm round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15415 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=24702 positions_in_flight_mean=65 commit_samples=6128338 commit_p50_ms=1.080 commit_p99_ms=1.916 commit_p999_ms=4.693 commit_max_ms=22.479
t=1792172371 variant=both-warm round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1011825
t=1792172371 variant=both-warm round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65578 waited=65578 wait_ms=0.04/0.04/0.08 wait_total_ms=2253 by_groups_ms=5 by_range_ms=0 by_reads_ms=0 unattributed_ms=2248 ranges=0 range_ms=- range_records_mean=0
t=1792172512 variant=base-cold round=2 catchup window=256 heartbeat_ms=50 lag_first=786435 lag_rate_under_load=-3062 lag_at_stop=728130 recovery_rate=9293 positions_per_tick=459 still_tick_share=0.005 bytes_in_flight_mean=1646 bytes_in_flight_max=263488 positions_in_flight_mean=13 commit_samples=170733 commit_p50_ms=51.325 commit_p99_ms=69.948 commit_p999_ms=83.118 commit_max_ms=93.161
t=1792172512 variant=base-cold round=2 stages run=catchup window=256 heartbeat_ms=50 scan reads=1893 read_ms=10.15/10.91/13.44
t=1792172512 variant=base-cold round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1892 waited=1892 wait_ms=0.03/4.89/10.34 wait_total_ms=2199 by_groups_ms=0 by_range_ms=2149 by_reads_ms=0 unattributed_ms=50 ranges=625 range_ms=10.23/11.45/14.43 range_records_mean=1024
t=1792172512 variant=base-cold round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15797 still_tick_share=0.000 bytes_in_flight_mean=9469 bytes_in_flight_max=16468 positions_in_flight_mean=74 commit_samples=6217600 commit_p50_ms=1.052 commit_p99_ms=2.107 commit_p999_ms=5.588 commit_max_ms=61.739
t=1792172512 variant=base-cold round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1049718
t=1792172512 variant=base-cold round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65555 waited=65555 wait_ms=0.04/0.04/0.07 wait_total_ms=2223 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2221 ranges=0 range_ms=- range_records_mean=0
t=1792172512 variant=base-cold round=2 catchup window=1024 heartbeat_ms=50 lag_first=714350 lag_rate_under_load=-31780 lag_at_stop=109460 recovery_rate=10946 positions_per_tick=1858 still_tick_share=0.010 bytes_in_flight_mean=23055 bytes_in_flight_max=1053952 positions_in_flight_mean=182 commit_samples=157504 commit_p50_ms=56.368 commit_p99_ms=94.194 commit_p999_ms=225.765 commit_max_ms=254.886
t=1792172512 variant=base-cold round=2 stages run=catchup window=1024 heartbeat_ms=50 scan reads=1640 read_ms=12.15/13.03/21.51
t=1792172512 variant=base-cold round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1640 waited=1640 wait_ms=0.02/7.21/10.52 wait_total_ms=2788 by_groups_ms=0 by_range_ms=2733 by_reads_ms=0 unattributed_ms=54 ranges=610 range_ms=10.51/11.06/24.60 range_records_mean=1024
t=1792172512 variant=base-cold round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16063 still_tick_share=0.000 bytes_in_flight_mean=9469 bytes_in_flight_max=16468 positions_in_flight_mean=74 commit_samples=6331968 commit_p50_ms=1.048 commit_p99_ms=1.707 commit_p999_ms=3.440 commit_max_ms=7.457
t=1792172512 variant=base-cold round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1098052
t=1792172512 variant=base-cold round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65558 waited=65556 wait_ms=0.04/0.05/0.07 wait_total_ms=2251 by_groups_ms=6 by_range_ms=0 by_reads_ms=0 unattributed_ms=2245 ranges=0 range_ms=- range_records_mean=0
t=1792172652 variant=both-cold round=2 catchup window=256 heartbeat_ms=50 lag_first=452790 lag_rate_under_load=-23807 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17824 still_tick_share=0.000 bytes_in_flight_mean=9057 bytes_in_flight_max=263488 positions_in_flight_mean=71 commit_samples=6237696 commit_p50_ms=1.064 commit_p99_ms=1.749 commit_p999_ms=3.485 commit_max_ms=8.581
t=1792172652 variant=both-cold round=2 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1215849
t=1792172652 variant=both-cold round=2 stages run=catchup window=256 heartbeat_ms=50 scan reads=17704 read_ms=0.02/0.03/0.09
t=1792172652 variant=both-cold round=2 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67196 waited=67176 wait_ms=0.03/0.05/0.11 wait_total_ms=2206 by_groups_ms=60 by_range_ms=18 by_reads_ms=0 unattributed_ms=2129 ranges=559 range_ms=0.05/0.06/0.17 range_records_mean=1024
t=1792172652 variant=both-cold round=2 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15974 still_tick_share=0.000 bytes_in_flight_mean=6998 bytes_in_flight_max=16468 positions_in_flight_mean=55 commit_samples=6285116 commit_p50_ms=1.058 commit_p99_ms=1.535 commit_p999_ms=3.578 commit_max_ms=47.300
t=1792172652 variant=both-cold round=2 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1078789
t=1792172652 variant=both-cold round=2 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65577 waited=65576 wait_ms=0.04/0.04/0.06 wait_total_ms=2200 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2200 ranges=0 range_ms=- range_records_mean=0
t=1792172652 variant=both-cold round=2 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17280 still_tick_share=0.005 bytes_in_flight_mean=8234 bytes_in_flight_max=1053952 positions_in_flight_mean=65 commit_samples=6052983 commit_p50_ms=1.071 commit_p99_ms=2.232 commit_p999_ms=8.278 commit_max_ms=41.605
t=1792172652 variant=both-cold round=2 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1002488
t=1792172652 variant=both-cold round=2 stages run=catchup window=1024 heartbeat_ms=50 scan reads=4040 read_ms=0.03/0.06/0.17
t=1792172652 variant=both-cold round=2 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65779 waited=65770 wait_ms=0.04/0.05/0.11 wait_total_ms=2327 by_groups_ms=58 by_range_ms=13 by_reads_ms=0 unattributed_ms=2256 ranges=609 range_ms=0.06/0.08/0.60 range_records_mean=1024
t=1792172652 variant=both-cold round=2 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15863 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=16468 positions_in_flight_mean=61 commit_samples=6280949 commit_p50_ms=1.065 commit_p99_ms=1.490 commit_p999_ms=3.046 commit_max_ms=5.456
t=1792172652 variant=both-cold round=2 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1076457
t=1792172652 variant=both-cold round=2 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65551 waited=65551 wait_ms=0.04/0.04/0.06 wait_total_ms=2199 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2196 ranges=0 range_ms=- range_records_mean=0
t=1792172794 variant=base-cold round=3 catchup window=256 heartbeat_ms=50 lag_first=730275 lag_rate_under_load=-3047 lag_at_stop=672295 recovery_rate=8890 positions_per_tick=455 still_tick_share=0.005 bytes_in_flight_mean=2058 bytes_in_flight_max=205850 positions_in_flight_mean=16 commit_samples=168446 commit_p50_ms=51.469 commit_p99_ms=88.907 commit_p999_ms=142.399 commit_max_ms=168.225
t=1792172794 variant=base-cold round=3 stages run=catchup window=256 heartbeat_ms=50 scan reads=1875 read_ms=10.20/10.54/17.15
t=1792172794 variant=base-cold round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=1874 waited=1874 wait_ms=0.03/5.58/10.05 wait_total_ms=2435 by_groups_ms=0 by_range_ms=2388 by_reads_ms=0 unattributed_ms=47 ranges=604 range_ms=10.06/10.48/14.12 range_records_mean=1024
t=1792172794 variant=base-cold round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16219 still_tick_share=0.000 bytes_in_flight_mean=7410 bytes_in_flight_max=24702 positions_in_flight_mean=58 commit_samples=6400938 commit_p50_ms=1.042 commit_p99_ms=1.582 commit_p999_ms=3.401 commit_max_ms=16.913
t=1792172794 variant=base-cold round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1126802
t=1792172794 variant=base-cold round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65560 waited=65560 wait_ms=0.04/0.04/0.06 wait_total_ms=2218 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2215 ranges=0 range_ms=- range_records_mean=0
t=1792172794 variant=base-cold round=3 catchup window=1024 heartbeat_ms=50 lag_first=747045 lag_rate_under_load=-27009 lag_at_stop=232960 recovery_rate=23296 positions_per_tick=1599 still_tick_share=0.003 bytes_in_flight_mean=23055 bytes_in_flight_max=1111590 positions_in_flight_mean=182 commit_samples=102528 commit_p50_ms=73.824 commit_p99_ms=100.575 commit_p999_ms=123.003 commit_max_ms=127.843
t=1792172794 variant=base-cold round=3 stages run=catchup window=1024 heartbeat_ms=50 scan reads=1479 read_ms=14.65/15.17/17.79
t=1792172794 variant=base-cold round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=1476 waited=1476 wait_ms=0.11/12.70/17.13 wait_total_ms=5542 by_groups_ms=0 by_range_ms=5501 by_reads_ms=0 unattributed_ms=42 ranges=632 range_ms=16.69/17.36/19.92 range_records_mean=1024
t=1792172794 variant=base-cold round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15664 still_tick_share=0.000 bytes_in_flight_mean=8234 bytes_in_flight_max=16468 positions_in_flight_mean=65 commit_samples=6152605 commit_p50_ms=1.062 commit_p99_ms=2.139 commit_p999_ms=6.184 commit_max_ms=47.360
t=1792172794 variant=base-cold round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1022358
t=1792172794 variant=base-cold round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65558 waited=65558 wait_ms=0.04/0.04/0.06 wait_total_ms=2187 by_groups_ms=2 by_range_ms=0 by_reads_ms=0 unattributed_ms=2184 ranges=0 range_ms=- range_records_mean=0
t=1792172935 variant=both-cold round=3 catchup window=256 heartbeat_ms=50 lag_first=432705 lag_rate_under_load=-22751 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17699 still_tick_share=0.010 bytes_in_flight_mean=9880 bytes_in_flight_max=263488 positions_in_flight_mean=78 commit_samples=6218153 commit_p50_ms=1.066 commit_p99_ms=1.773 commit_p999_ms=4.436 commit_max_ms=30.460
t=1792172935 variant=both-cold round=3 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1207192
t=1792172935 variant=both-cold round=3 stages run=catchup window=256 heartbeat_ms=50 scan reads=17587 read_ms=0.02/0.02/0.08
t=1792172935 variant=both-cold round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=67045 waited=67021 wait_ms=0.03/0.05/0.11 wait_total_ms=2187 by_groups_ms=72 by_range_ms=5 by_reads_ms=0 unattributed_ms=2110 ranges=611 range_ms=0.05/0.06/0.11 range_records_mean=1024
t=1792172935 variant=both-cold round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15773 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=32936 positions_in_flight_mean=61 commit_samples=6234869 commit_p50_ms=1.055 commit_p99_ms=1.493 commit_p999_ms=3.327 commit_max_ms=290.906
t=1792172935 variant=both-cold round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1057770
t=1792172935 variant=both-cold round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65554 waited=65554 wait_ms=0.04/0.04/0.06 wait_total_ms=2222 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2221 ranges=0 range_ms=- range_records_mean=0
t=1792172935 variant=both-cold round=3 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17855 still_tick_share=0.005 bytes_in_flight_mean=7822 bytes_in_flight_max=1053952 positions_in_flight_mean=61 commit_samples=6241821 commit_p50_ms=1.066 commit_p99_ms=1.636 commit_p999_ms=3.155 commit_max_ms=11.788
t=1792172935 variant=both-cold round=3 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1080997
t=1792172935 variant=both-cold round=3 stages run=catchup window=1024 heartbeat_ms=50 scan reads=4296 read_ms=0.03/0.05/0.14
t=1792172935 variant=both-cold round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65755 waited=65747 wait_ms=0.04/0.04/0.08 wait_total_ms=2253 by_groups_ms=31 by_range_ms=10 by_reads_ms=0 unattributed_ms=2212 ranges=607 range_ms=0.06/0.07/0.28 range_records_mean=1024
t=1792172935 variant=both-cold round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=16051 still_tick_share=0.000 bytes_in_flight_mean=9057 bytes_in_flight_max=49404 positions_in_flight_mean=71 commit_samples=6332591 commit_p50_ms=1.051 commit_p99_ms=1.767 commit_p999_ms=3.347 commit_max_ms=30.318
t=1792172935 variant=both-cold round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=1098314
t=1792172935 variant=both-cold round=3 stages run=control window=1024 heartbeat_ms=50 scan reads=2 read_ms=16.42/16.42/16.42
t=1792172935 variant=both-cold round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65560 waited=65560 wait_ms=0.04/0.04/0.06 wait_total_ms=2192 by_groups_ms=1 by_range_ms=0 by_reads_ms=0 unattributed_ms=2191 ranges=0 range_ms=- range_records_mean=0
t=1792173077 variant=both-warm round=3 catchup window=256 heartbeat_ms=50 lag_first=470978 lag_rate_under_load=-24763 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17891 still_tick_share=0.018 bytes_in_flight_mean=8234 bytes_in_flight_max=205850 positions_in_flight_mean=65 commit_samples=6340220 commit_p50_ms=1.053 commit_p99_ms=1.593 commit_p999_ms=3.057 commit_max_ms=17.613
t=1792173077 variant=both-warm round=3 stages run=catchup window=256 heartbeat_ms=50 stamps_dropped=1253186
t=1792173077 variant=both-warm round=3 stages run=catchup window=256 heartbeat_ms=50 scan reads=17524 read_ms=0.02/0.02/0.07
t=1792173077 variant=both-warm round=3 stages run=catchup window=256 heartbeat_ms=50 blocking peer=lagging appends=66988 waited=66974 wait_ms=0.03/0.04/0.10 wait_total_ms=2083 by_groups_ms=54 by_range_ms=0 by_reads_ms=0 unattributed_ms=2029 ranges=611 range_ms=0.07/0.07/0.14 range_records_mean=1022
t=1792173077 variant=both-warm round=3 control window=256 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15944 still_tick_share=0.000 bytes_in_flight_mean=7822 bytes_in_flight_max=24702 positions_in_flight_mean=61 commit_samples=6316154 commit_p50_ms=1.055 commit_p99_ms=1.679 commit_p999_ms=3.142 commit_max_ms=21.636
t=1792173077 variant=both-warm round=3 stages run=control window=256 heartbeat_ms=50 stamps_dropped=1091227
t=1792173077 variant=both-warm round=3 stages run=control window=256 heartbeat_ms=50 blocking peer=lagging appends=65573 waited=65573 wait_ms=0.04/0.04/0.07 wait_total_ms=2177 by_groups_ms=9 by_range_ms=0 by_reads_ms=0 unattributed_ms=2168 ranges=0 range_ms=- range_records_mean=0
t=1792173077 variant=both-warm round=3 catchup window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=17685 still_tick_share=0.018 bytes_in_flight_mean=8506 bytes_in_flight_max=1053952 positions_in_flight_mean=68 commit_samples=6258373 commit_p50_ms=1.063 commit_p99_ms=1.599 commit_p999_ms=3.673 commit_max_ms=14.687
t=1792173077 variant=both-warm round=3 stages run=catchup window=1024 heartbeat_ms=50 stamps_dropped=1078658
t=1792173077 variant=both-warm round=3 stages run=catchup window=1024 heartbeat_ms=50 scan reads=4068 read_ms=0.03/0.05/0.13
t=1792173077 variant=both-warm round=3 stages run=catchup window=1024 heartbeat_ms=50 blocking peer=lagging appends=65670 waited=65670 wait_ms=0.03/0.04/0.08 wait_total_ms=2225 by_groups_ms=21 by_range_ms=0 by_reads_ms=0 unattributed_ms=2204 ranges=615 range_ms=0.07/0.08/0.09 range_records_mean=1022
t=1792173077 variant=both-warm round=3 control window=1024 heartbeat_ms=50 lag_first=0 lag_rate_under_load=0 lag_at_stop=0 recovery_rate=caught_up positions_per_tick=15320 still_tick_share=0.000 bytes_in_flight_mean=9057 bytes_in_flight_max=16468 positions_in_flight_mean=71 commit_samples=6019738 commit_p50_ms=1.069 commit_p99_ms=2.400 commit_p999_ms=9.580 commit_max_ms=52.816
t=1792173077 variant=both-warm round=3 stages run=control window=1024 heartbeat_ms=50 stamps_dropped=967169
t=1792173077 variant=both-warm round=3 stages run=control window=1024 heartbeat_ms=50 blocking peer=lagging appends=65596 waited=65594 wait_ms=0.04/0.04/0.07 wait_total_ms=2229 by_groups_ms=3 by_range_ms=0 by_reads_ms=0 unattributed_ms=2226 ranges=0 range_ms=- range_records_mean=0
```

Both cold and both warm agree: read p50 0.02–0.03 ms, catch-up commit p99 1.6–3.6 ms, the replica's append wait 0.04–0.05 ms, caught up under load in every trial. Base cold: reads 10–15 ms, commit p99 70–101 ms. Base cold reads 10–15 ms, no slower than base warm in D-142's run (13.8–16.6 ms) and this run has no base-warm arm, so it does not show cold reads costing more here: the eviction is one pass before the restart, base rescans its segment and warms it again after its first read, and the VM's disk is itself cached by macOS. With the cursor each record is read from the disk once. At window 1 024 the `both` trials had mostly caught up by the first sample (`lag_first=0`), so they compare nothing; window 256 is the comparison. The disk here is the VM's, whose image macOS may still cache: a physical cold disk is not represented. Not accepted; direction only.
