# Benchmarks

> **Partly history.** The harness and workload matrix below were built for the old broker (`e2e` load generators), removed at step 8 (D-111). The measurement protocol — baseline, alternating rounds, medians, Linux only — still applies; the new path has `node/benches/cap.rs` and `storage/benches/`, and no Linux baseline yet (D-84 A, D-94 C).

Target: about 1,000,000 small messages/sec on modern server hardware for the
in-memory broker benchmark. A target, not a production guarantee.

Process rules are in CLAUDE.md §11. This file is the protocol.

## 1. Levels

Keep them separate; a fast lower level proves nothing about the next one.

```text
codec           mqtt/benches           decode, encode
durability      storage/benches        checksum, batch construction, commit cycle
component       domain, broker         topic match, router, queue
broker          broker/benches         publish → route → dispatch
in-memory e2e   runtime                full path without sockets
network e2e     e2e/benches            real TCP, real client
```

A microbenchmark with no I/O, no contention and no scheduling is never evidence
for production throughput. Report which level a number comes from.

## 2. Harness

Micro benchmarks use `divan` (stable Rust, `harness = false`) and count
allocations with its profiler:

```toml
# crates/mqtt/Cargo.toml
[dev-dependencies]
divan = "0.1"

[[bench]]
name = "decode"
harness = false
```

```rust
#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

#[divan::bench(args = [64, 256, 1024, 4096, 16384])]
fn publish_decode(bencher: divan::Bencher, payload: usize) {
    let input = packet_with_payload(payload);        // setup, not measured
    bencher
        .with_inputs(|| input.clone())
        .bench_values(|mut buf| decode(&mut buf));   // result is consumed
}
```

Network benchmarks in `e2e` are `harness = false` binaries.

**Open-loop load generation.** A client that sends the next message only after
the previous reply arrives cannot observe latency above its own think time
(coordinated omission): the p99 it reports is the p99 of a system that slowed
the client down. The generator sends at a fixed target rate, records the
*intended* send time, and reports latency against it. Rates that the broker
cannot sustain must show up as growing latency, not as a lower measured rate.

Latencies go into a `Vec<u64>` (nanoseconds) allocated before the run, sorted
afterwards for p50/p95/p99. Warm-up is excluded from the recorded samples.

## 3. Required benchmarks

```text
mqtt:    mqtt_connect_decode  mqtt_publish_decode  mqtt_subscribe_decode
         mqtt_publish_encode  mqtt_ack_encode
domain:  exact_match  single_level_wildcard  multi_level_wildcard
broker:  route with 1 / 10 / 100 / 1000 subscribers, many_subscriptions
         route_wildcard  publish_route
runtime: enqueue  dequeue  enqueue_dequeue  full_queue   (the session queue)
         publish_consume  publish_consume_at_least_once  (in-memory e2e:
         decode → mapping → routing → encode, no sockets and no channels)
e2e:     mqtt_connect  mqtt_publish  mqtt_publish_subscribe  mqtt_publish_consume
storage: checksum  batch_construction
         commit: idle / low / moderate / high / burst, reporting records per
         sync, bytes per sync, sync p50 and commit latency p50/p95/p99
         (docs/storage.md §93, §94)
         crash survival: acknowledged QoS 1 messages still present after
         SIGKILL, for an offline durable session (scripts/crash-survival.sh,
         §6 — the one comparison valid off a Linux host)
```

## 4. Workload matrix

Dimensions: payload size, subscriber count, subscription count, topic length,
message rate, connection count, queue depth, thread count.

The full cross product is too large to run per change, so it is tiered:

| Tier | When | Scope |
|---|---|---|
| quick | every hot-path change | payload 64 B and 1 KiB × subscribers 1 and 100, single thread |
| neighbors | before accepting an optimization | every dimension the change touches, both directions |
| full | before a release, or when a structural decision depends on it | payload 64 B…16 KiB × subscribers 1/10/100/1000 × threads 1/2/4/8/16 |

"Neighbors" is not optional. A routing change re-runs exact match, single-level
wildcard, multi-level wildcard, small and large subscription sets, one and many
subscribers. An optimization that helps one cell and hurts the general case is
a regression.

Never claim an implementation is faster without naming the workload it is
faster under.

## 5. Build and environment configuration

Benchmarks run in the release configuration frozen in the root `Cargo.toml`
(`lto = "thin"`, `codegen-units = 1`, `panic = "unwind"`, system allocator).
`bench` inherits `release`; only debug info is added.

No `-C target-cpu=native` by default: benchmarks use the same flags as the
production build. `RUSTFLAGS` is recorded in every report.

Each of these is a benchmark variable in its own right, changed alone and
measured alone: rustc version, opt-level, LTO, codegen-units, target-cpu, panic
strategy, allocator, thread count, tokio flavor.

Never compare debug against release, one thread against sixteen, different
payloads, different compilers, or different machines and call the difference an
optimization.

## 6. Where numbers come from

| Benchmark | macOS | Linux container on macOS | Linux host / VM |
|---|---|---|---|
| codec, topic, router, queue | direction only | direction only | reference numbers |
| in-memory e2e | direction only | not used | reference numbers |
| network e2e | not used | not used | reference numbers |
| crash survival | valid | valid | valid |

Apple Silicon moves threads between performance and efficiency cores and offers
no portable pinning, so run-to-run spread on macOS is large. Docker Desktop adds
a VM with virtualized timing and network.

**Crash survival is the exception, and the reason is the unit.**
`scripts/crash-survival.sh` reports how many acknowledged QoS 1 messages a
broker still has after `SIGKILL` — an integer, and no amount of virtualised
timing changes how many messages are on a disk. It is the one comparison
against other brokers that can be taken here and quoted.

It runs in rounds for a reason that is not noise. A broker that writes the
record before acknowledging answers the same every round; one that saves on
a timer answers whatever the timer was doing when the kill landed.

Measured 2026-09-24, 800 QoS 1 messages for an offline durable session,
three rounds each: straum kept 800/800 every round, with the session and the
retained message. Mosquitto (`persistence true`, `autosave_interval 1`) kept
0/800 every round and lost the session with them — and then kept 800/800 in
both rounds with `STRAUM_BENCH_SETTLE=3`. Its database measured 47 bytes
immediately after the workload and 88 KB two seconds later.

That pair of runs is the result, not either one alone. Mosquitto is neither
broken nor misconfigured here: given the time, it restores everything. What
it does not have is an acknowledgement that means the record is on disk, so
a crash that does not wait for the timer takes the window with it. Quote
both rows or the comparison is unfair.

On the Linux benchmark host, before a measurement:

```bash
# pin the broker and the generator to separate physical cores
taskset -c 2,3 ./target/release/straum-server
taskset -c 8,9 ./loadgen

# keep frequency stable for the duration
sudo cpupower frequency-set -g performance

# record what could not be controlled
uname -a; lscpu | head -20; rustc -V
```

Record CPU model, core count, kernel, governor, SMT on/off, whether the machine
was otherwise idle, and the sysctls from `docs/linux-tuning.md`.

## 7. Measurement protocol

1. Build baseline and candidate with identical configuration.
2. Warm up; discard the warm-up samples.
3. Run **alternating** rounds: `base, cand, base, cand, base, cand, base, cand,
   base, cand` — at least 5 of each. Sequential blocks hide drift.
4. Compare medians. Never the best run, never the maximum throughput.
5. Report the spread (min/max or IQR) next to the median.
6. If the spread exceeds the effect, the result is inconclusive: fix the
   environment and repeat. Do not report it as an improvement.
7. Noise floor **(initial)**: below 3% on a pinned quiet Linux host, below 10%
   on a development Mac.
8. Unexpected results are investigated, not discarded. A candidate that is
   faster for no explainable reason is usually measuring less work.

## 8. What to report

```text
Performance change:

Baseline:
- benchmark:            broker/publish_route
- workload:             64 B payload, 1 publisher, 100 subscribers, 1 thread
- result:               1.18 M msg/s (median of 5), min 1.15 max 1.21
- allocations/op:       2
- syscalls/msg:         n/a (in-memory)

Candidate:
- benchmark:            same
- workload:             same
- result:               1.31 M msg/s (median of 5), min 1.29 max 1.33
- allocations/op:       0
- syscalls/msg:         n/a

Comparison:
- throughput:           +11.0%
- latency p50/p95/p99:  ...
- allocations:          −2 per publish
- memory:               peak RSS unchanged

Environment:
- machine, CPU, cores used, SMT, governor
- kernel, rustc, profile, RUSTFLAGS
- sysctls relevant to the run

Validation:
- repeated measurements: 5 alternating rounds
- neighboring benchmarks: exact/wildcard match, 1/10/1000 subscribers — no regression
- regression checks:      codec benchmarks unchanged

Conclusion:
- measured effect:       ...
- remaining bottleneck:  ...
```

Only claim what was measured. "Performance improved significantly" is not a
result. If nothing meaningful changed, report that.

## 9. Baselines and regression protection

Accepted numbers live in `docs/baselines/<machine-id>.md`, one file per machine,
each entry carrying the environment block above and the commit hash. Baselines
from different machines are never compared.

CI runners are too noisy to gate on; benchmarks are not a CI gate. Instead, the
baseline file is refreshed on the benchmark host at release points, and a later
regression is investigated against it.

This sequence must never pass unexplained:

```text
baseline → optimized → refactor → back to baseline
```

## 10. Benchmark integrity

Benchmark code is production-quality code and is reviewed as such.

* Setup, data generation, cloning of inputs and logging stay outside the
  measured section, consistently for baseline and candidate.
* Inputs and results go through `black_box`; the measured operation must not be
  removable by the compiler. Do not add artificial work to defeat the optimizer;
  keep the benchmark close to the real operation.
* One name, one workload. Do not change payloads, thread counts or subscriber
  counts between comparisons.
* Do not select favorable runs or drop inconvenient ones without saying why.
* Do not delete or weaken a benchmark because it shows a regression.
* Do not report a maximum as a typical value, or claim causality without a
  controlled comparison.
* Contradictory or noisy results are reported as such and investigated.

## 11. Allocations and memory

Track per operation: allocation count, bytes allocated, temporary allocations,
deallocations, buffer growth. `divan`'s `AllocProfiler` reports these for micro
benchmarks; for the e2e path, use the queued-bytes gauges plus peak RSS.

```bash
/usr/bin/time -v <cmd>   # Linux: max RSS, context switches, page faults
/usr/bin/time -l <cmd>   # macOS
```

A throughput win that adds allocations is a trade-off and is reported as one.
Preallocation is preferred over growth, but large preallocated buffers are
measured against `STRAUM_MAX_CONNECTIONS`: per-connection cost times connection
count is the number that matters.

## 12. Syscalls

For network benchmarks report messages/sec, packets/sec, syscalls/sec and
bytes/sec separately. High message throughput with one syscall per message is a
finding, not a success.

```bash
strace -c -f -p $(pidof straum-server)     # Linux only, run outside the measured section
perf stat -e 'syscalls:sys_enter_*' -p $(pidof straum-server)
```

## 13. Tracing during benchmarks

Default level only. If tracing is needed to investigate a result, run a separate
profiling configuration and do not compare its numbers against benchmark
numbers. Instrumentation must not become the benchmark; when instrumentation
changes, verify once that default-level tracing does not move the result.
