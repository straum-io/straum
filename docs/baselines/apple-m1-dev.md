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
