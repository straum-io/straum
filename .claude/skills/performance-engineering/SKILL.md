---
name: performance-engineering
description: Evidence-driven performance engineering for Rust systems code. Use for optimization, throughput, latency, CPU, memory, allocation, hot-path, benchmark, algorithm and data-structure decisions.
---

# Performance Engineering Skill

## Objective

Find the actual limiting factor and improve the system without sacrificing
correctness, determinism, bounded memory or architectural invariants.

Never interpret "optimize this" as "rewrite this code to look simpler".

Optimization means measurable improvement under a representative workload.

---

## Required operating mode

Before modifying performance-sensitive code:

1. Locate the hot path.
2. Identify the workload.
3. Find existing benchmarks.
4. Establish the baseline.
5. Identify the limiting resource.
6. Generate competing implementation strategies.
7. Select the strategy using workload characteristics.
8. Implement the smallest necessary change.
9. Verify correctness.
10. Benchmark against baseline.
11. Inspect regressions.
12. Keep the change only if the evidence supports it.

---

# Algorithm selection

For every non-trivial algorithm, explicitly evaluate:

```
asymptotic complexity
constant factors
expected N
operation frequency
read/write ratio
ordering requirements
duplicate behavior
mutation frequency
cache locality
memory overhead
allocation behavior
concurrency
contention
failure behavior
```

Do not select algorithms from textbook complexity alone.

The actual workload dominates theoretical preference.

---

# Data-structure selection

When choosing a structure, evaluate the actual access pattern.

Consider at minimum when relevant:

```
Vec
sorted Vec + binary search
VecDeque
HashMap
BTreeMap
slab/indexed storage
arena
intrusive structures
fixed-capacity structures
ring buffers
arrays
custom structures
```

Do not add a custom data structure unless the workload justifies it.

For small N, benchmark contiguous structures before introducing trees or hashes.

For large N, consider cache behavior and allocation patterns.

For ordered data, consider whether sorting once is cheaper than maintaining
an ordered tree.

For frequently scanned data, prioritize locality.

For shared mutable data, consider whether ownership partitioning can eliminate
synchronization entirely.

---

# Memory layout

When performance is memory-bound, inspect:

```
object size
alignment
padding
pointer count
indirection
allocation count
cache-line locality
hot/cold fields
```

Prefer compact representations when they reduce memory traffic.

Do not pack structures blindly.

A smaller structure is not automatically faster if it creates extra decoding,
branches or indirections.

---

# Allocation analysis

For every allocation on a hot path ask:

```
Is it necessary?
Can ownership be transferred?
Can an existing buffer be reused?
Can capacity be reserved once?
Can multiple allocations become one?
Can the representation be borrowed?
Would avoiding the allocation increase retention or fragmentation?
```

Do not optimize allocations that are irrelevant to the measured bottleneck.

---

# Copy analysis

Track data movement explicitly.

For important paths write down:

```
source
  ↓
copy?
  ↓
destination
  ↓
copy?
  ↓
I/O
```

Distinguish:

```
payload copy
metadata copy
buffer growth
framing
checksum pass
serialization
deserialization
```

A memcpy may be cheaper than a synchronization or allocation.

Do not remove copies blindly.

---

# Concurrency analysis

For concurrent code identify:

```
producer
consumer
queue
ownership
synchronization
wakeup
backpressure
capacity
```

Ask whether batching or ownership transfer can eliminate synchronization.

Do not assume:

```
lock-free > locked
async > sync
more workers > fewer workers
```

Benchmark the actual workload.

---

# CPU analysis

Only after algorithm/data-structure/memory behavior is understood, inspect:

```
branch behavior
hashing
checksums
parsing
encoding
atomics
reference counting
virtual dispatch
bounds checks
repeated length calculation
redundant validation
```

Do not perform instruction-level optimization without evidence.

---

# I/O analysis

Separate:

```
syscall cost
storage latency
network latency
serialization
copying
queueing
```

For storage workloads measure:

```
write duration
sync duration
batch size
messages/sync
bytes/sync
```

For network workloads measure:

```
application bytes
wire bytes
writes
reads
batching
queue depth
end-to-end latency
```

---

# Benchmark discipline

Never compare two implementations using different workloads.

Keep constant:

```
payload
concurrency
duration
configuration
machine
build profile
storage
topology
```

Only change the variable being tested.

Run enough repetitions to identify noise.

Investigate anomalies before changing code.

---

# Optimization acceptance

An optimization is accepted only if:

```
correctness remains intact
relevant tests pass
benchmark is representative
performance improvement is reproducible
no unacceptable memory regression exists
no unacceptable latency regression exists
architecture remains coherent
```

If results are inconclusive, keep the simpler implementation.

---

# Important rule

If the agent cannot explain why an optimization should work mechanically,
it must not implement it merely because the code looks inefficient.

If it can explain the mechanism but cannot measure the expected benefit,
label the change as a hypothesis and benchmark it.

Never call a hypothesis an optimization result.
