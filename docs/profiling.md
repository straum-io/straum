# Profiling

Profiling answers a concrete question. Pick the question first, then the tool.

```text
Where does CPU time go?              sampling profiler
Which function dominates?            sampling profiler, flamegraph
Are allocations significant?         allocation profiler
Is the code memory-bound?            hardware counters (cache misses, stalls)
Is synchronization dominating?       off-CPU / futex profiling
Are syscalls dominating?             syscall counting
Why is the generated code slow?      assembly, LLVM IR
```

Do not add speculative optimizations before a profile identifies the bottleneck
(rules.md §11).

## Tools by platform

| Question | Linux host | macOS (development) |
|---|---|---|
| CPU sampling | `perf record -g`, `samply record` | `samply record`, Instruments (Time Profiler) |
| Flamegraph | `cargo flamegraph` (uses perf) | `samply` web view |
| Hardware counters | `perf stat -e cache-misses,branch-misses,stalled-cycles-backend` | Instruments (CPU Counters); no perf |
| Allocations | `divan` AllocProfiler, `heaptrack` | `divan` AllocProfiler, Instruments (Allocations) |
| Off-CPU / locks | `perf sched`, `perf record -e sched:sched_switch` | limited |
| Syscalls | `strace -c -f`, `perf trace` | `dtruss` (needs SIP considerations) |
| Assembly | `cargo asm`, `--emit=asm`, godbolt | same |

`perf` and `cargo flamegraph` are Linux-host tools. They do not work usefully
through Docker Desktop on macOS: the container shares a virtualized kernel, and
`perf_event_open` is blocked by default (`--cap-add SYS_ADMIN` plus
`perf_event_paranoid` on the VM, and the numbers are still not trustworthy).
Profile on a Linux machine.

`samply` works on both and needs no root, which makes it the default tool while
iterating on macOS.

## Sampling

```bash
# Linux
perf record -g --call-graph dwarf ./target/release/deps/router-<hash> --bench
perf report --sort symbol

# both platforms
samply record ./target/release/deps/router-<hash> --bench
```

`[profile.bench] debug = "line-tables-only"` keeps symbols without changing
codegen, so bench binaries are profilable as built.

## Generated code

When source-level reasoning runs out, read the output. Useful signals:

```text
missed inlining          unnecessary call in the inner loop
redundant loads/stores    value reloaded across a call
unexpected branches       a check that should have been hoisted
bounds checks             index that the compiler could not prove in range
copies and conversions    a memcpy or widening that the source does not show
```

```bash
cargo asm mqtt::decode::publish
RUSTFLAGS="--emit=asm" cargo bench --no-run
```

Prefer changes that let the compiler generate better code (restructured loops,
slices with proven lengths, `#[inline]` where measured) over hand-written
assembly or unsafe. Do not remove a bounds check with `get_unchecked` because it
appears in the listing; the decoder's lint set exists to keep those checks
(rules.md §3). Removing one requires the unsafe procedure in §14.

## Interpreting a profile

* A flat profile with no dominant frame usually means memory traffic or
  scheduling, not one slow function. Check counters and off-CPU time.
* Time in the allocator means the hot path allocates; go to §11 priority order
  step 2 before anything else.
* Time in `futex`/`__lll_lock_wait` means contention; ownership or sharding, not
  a faster lock.
* Time in syscalls means missing batching.
* If the profile disagrees with the benchmark result, trust neither until the
  disagreement is explained.
