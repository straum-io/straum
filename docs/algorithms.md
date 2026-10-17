# Algorithm and Data Structure Selection Protocol

This document defines how algorithms and data structures are selected and changed in Straum.

The objective is not minimum code, minimum abstraction, or theoretical elegance.

The objective is the lowest measured cost for the actual workload while preserving correctness, bounded memory, predictable latency, concurrency properties, and architectural invariants.

---

## 1. Core Rule

Never replace an algorithm or data structure because another one appears faster in theory.

Every non-trivial replacement must answer:

1. What operation is currently expensive?
2. What is its actual frequency?
3. What are the actual input cardinalities?
4. What are the read/write/mutation ratios?
5. Is ordering required?
6. Is stable iteration required?
7. What are the ownership and lifetime constraints?
8. Is the operation concurrent?
9. Where does contention occur?
10. How many allocations occur?
11. How many bytes are moved or copied?
12. What is the expected cache/locality behavior?
13. What is the memory cost per element?
14. What happens at the minimum and maximum expected cardinality?
15. What happens under pathological but valid input?
16. Which existing invariants depend on the representation?
17. What benchmark proves the replacement is better?

If these questions cannot be answered, the representation is not ready to be changed.

---

## 2. Workload Before Representation

The workload is the primary input to algorithm selection.

Before choosing a structure, record the relevant workload:

```text
N                 element count
Q                 operation count
R/W               read/write ratio
I                 insertion rate
D                 deletion rate
L                 lookup rate
S                 scan/iteration rate
O                 ordering requirement
C                 concurrency level
B                 burst behavior
P                 payload size distribution
K                 key size distribution
M                 memory budget
H                 hot/cold access distribution
```

Do not invent precise values when they are unknown.

Use measured values when available.

If values are unknown but materially affect the choice, create a benchmark that measures the range.

---

## 3. Candidate Generation

For a meaningful data-structure decision, consider the plausible candidates before selecting one.

Typical candidates include:

```text
Vec
VecDeque
SmallVec
ArrayVec
HashMap
HashSet
BTreeMap
BTreeSet
IndexMap
sorted Vec + binary_search
flat arrays
slab
generational arena
arena + index
ring buffer
bounded queue
unbounded queue
single-producer/single-consumer queue
multi-producer queue
intrusive representation
bitmap
bitset
integer-indexed table
direct array indexing
copy-on-write representation
immutable snapshot
sharded structure
per-thread structure
lock-free structure
```

This is not an instruction to use every structure.

It is an instruction not to stop at the first plausible one.

The final candidate set should normally contain at least two viable representations when the choice materially affects performance.

---

## 4. Complexity Is Necessary but Insufficient

Record asymptotic complexity:

```text
lookup
insert
delete
iteration
range query
minimum/maximum
ordering
resize
```

But do not treat Big-O as the decision.

Also evaluate:

```text
allocation count
allocation size
pointer chasing
cache locality
branch predictability
memory overhead
copy volume
move volume
key hashing
comparison cost
indirection
locking
atomic operations
contention
false sharing
syscalls
wakeups
serialization
```

A theoretically worse algorithm may be faster at the actual cardinality.

Example:

```text
O(N) scan over a contiguous Vec
```

may beat:

```text
O(1) hash lookup
```

for small N because the scan has excellent locality and the hash table performs hashing, indirection and unpredictable memory accesses.

Do not generalize this example.

Measure the actual workload.

---

## 5. Cardinality Is a First-Class Input

Never select a structure without considering cardinality.

Evaluate at least:

```text
N = 0
N = 1
N = 4
N = 16
N = 64
N = 256
N = 1K
N = 10K
N = 100K
```

Only use values relevant to the actual system.

The purpose is to detect representation crossovers.

For example:

```text
small N → Vec scan
large N → indexed/hash/tree structure
```

may be valid.

Do not introduce a dynamic representation switch unless measurement demonstrates that the additional complexity is worthwhile.

---

## 6. Memory Is Part of Performance

For every candidate estimate:

```text
bytes per element
fixed overhead
capacity overhead
allocation count
temporary allocation
metadata
pointer overhead
alignment overhead
fragmentation
```

The comparison must consider:

```text
total memory = structure overhead + element storage + temporary working memory
```

Never report throughput without considering memory when the structure is persistent or multiplied by connection/session/topic count.

For per-connection structures, explicitly calculate:

```text
cost_per_connection × number_of_connections
```

For per-message structures:

```text
cost_per_message × messages_in_flight
```

A 20-byte improvement that occurs once is not equivalent to a 20-byte improvement multiplied by one million objects.

---

## 7. Ownership Before Synchronization

Before introducing:

```text
Arc
Mutex
RwLock
Atomic*
DashMap
concurrent collections
channels
```

first determine whether ownership can be transferred or partitioned.

Preferred order:

```text
exclusive ownership
    ↓
message passing / ownership transfer
    ↓
immutable shared data
    ↓
atomic state
    ↓
sharded synchronization
    ↓
fine-grained locking
    ↓
global locking
```

This is a heuristic, not a universal rule.

If a lower level is measurably faster for the real workload, use it.

The author must explain the contention and ownership reason for the choice.

---

## 8. Sequential Access Must Be Considered Explicitly

When data is normally processed sequentially, evaluate contiguous representations first:

```text
Vec
byte buffers
flat arrays
packed records
index-based representations
```

before pointer-heavy structures.

Sequential memory access frequently benefits from:

```text
cache locality
hardware prefetching
fewer allocations
fewer indirections
lower metadata overhead
```

Do not assume this wins.

Benchmark it.

---

## 9. Hot Path Representation

For hot-path structures, inspect the generated representation.

Ask:

```text
How many loads?
How many stores?
How many branches?
How many pointer dereferences?
How many atomics?
How many hash operations?
How many allocations?
How many copies?
```

The source-level simplicity of:

```rust
map.get(&key)
```

does not establish that the generated path is cheap.

Likewise, manually optimized code is not automatically better.

When a change is close enough to matter, inspect:

```text
cargo asm
objdump
llvm IR
perf
```

or equivalent tooling available in the environment.

Do this only after higher-level profiling identifies the path as relevant.

---

## 10. Algorithm Selection Matrix

Before replacing a meaningful algorithm, create a short comparison:

| Candidate   | Complexity | Allocations | Locality | Memory | Sync | Ordering | Expected workload |
| ----------- | ---------- | ----------: | -------- | -----: | ---- | -------- | ----------------- |
| Current     | ...        |         ... | ...      |    ... | ...  | ...      | ...               |
| Candidate A | ...        |         ... | ...      |    ... | ...  | ...      | ...               |
| Candidate B | ...        |         ... | ...      |    ... | ...  | ...      | ...               |

Then benchmark the candidates.

Do not select the candidate from the table alone.

The table records reasoning.

The benchmark establishes the result.

---

## 11. Microbenchmark Requirements

A microbenchmark must isolate the operation being compared.

It must:

* use identical input data;
* use identical workload distribution;
* exclude setup from the measured region;
* prevent compiler elimination;
* measure enough iterations;
* include relevant cardinalities;
* include realistic key/value sizes;
* include cold and hot cases when relevant;
* report allocations when relevant.

Use `black_box`.

Never add artificial work solely to defeat optimization.

---

## 12. End-to-End Validation

A microbenchmark is insufficient when the structure participates in:

```text
routing
storage
replication
networking
session management
durability
```

After a microbenchmark demonstrates a difference, run the affected end-to-end benchmark.

A structure that improves a local operation by 20% but increases cache pressure or allocations in the real broker is not an optimization.

---

## 13. Benchmark Hierarchy

Use this order:

```text
1. microbenchmark
2. affected component benchmark
3. neighboring benchmarks
4. end-to-end benchmark
5. sustained workload
6. memory/RSS measurement
7. allocation measurement
8. CPU/profile validation
```

Not every change requires every level.

The deeper the architectural impact, the more levels are required.

---

## 14. Small-N and Large-N Crossovers

If two representations have different performance characteristics, search for the crossover point.

Example:

```text
Vec scan:
    faster N <= X

HashMap:
    faster N > X
```

Do not introduce runtime switching unless:

1. X occurs inside the real workload;
2. the crossover is stable;
3. switching cost is understood;
4. memory overhead is justified;
5. the added complexity is benchmarked.

Otherwise use the representation that is best for the dominant workload.

---

## 15. Adaptive Algorithms

Adaptive algorithms are allowed only when the workload genuinely varies.

An adaptive algorithm must have:

```text
observable signal
bounded state
stable adaptation
bounded memory
bounded CPU overhead
clear fallback
```

Never adapt on noisy single-operation measurements.

Prefer:

```text
EWMA
periodic sampling
bounded windows
hysteresis
```

over reacting to every event.

Straum's adaptive storage batching is the canonical example.

The storage device provides the pacing signal; the algorithm must not invent an artificial cadence.

---

## 16. Zero-Copy Is Not a Goal by Itself

Do not introduce zero-copy mechanisms merely because they sound optimal.

Compare:

```text
copy cost
ownership complexity
lifetime complexity
cache locality
reference counting
fragmentation
syscall requirements
network I/O requirements
```

A single predictable copy may be faster and safer than several layers of reference-counted buffers.

The existing replication WAL path must preserve its important property:

```text
bytes written to WAL
=
bytes supplied to replication
```

unless a benchmark demonstrates a superior representation without weakening correctness.

---

## 17. Unsafe

Do not introduce `unsafe` for algorithmic optimization unless:

1. the current safe implementation is proven to be a bottleneck;
2. alternatives have been benchmarked;
3. the unsafe representation has a precise invariant;
4. correctness tests exist;
5. generated-code evidence shows why it is necessary;
6. the measured improvement survives the relevant benchmark matrix.

Unsafe is an implementation technique, not an optimization strategy.

---

## 18. Concurrency

For concurrent structures measure:

```text
contention
atomic operations
cache-line bouncing
wakeups
queue depth
scheduler interaction
producer count
consumer count
```

Do not assume lock-free means faster.

Do not assume fewer locks means faster.

Do not assume more sharding means faster.

Benchmark:

```text
1 producer
2 producers
4 producers
8 producers
16 producers
```

where applicable.

---

## 19. Pathological Inputs

Every candidate must remain correct for:

```text
empty
single element
maximum element count
duplicate keys
maximum key size
maximum payload size
rapid insertion/deletion
burst traffic
high contention
slow consumer
slow storage
reconnect
restart
```

The exact set depends on the structure.

Performance optimization must never silently convert bounded behavior into unbounded behavior.

---

## 20. Decision Record

For every architectural algorithm/data-structure replacement record:

```text
Decision:
Current representation:
Candidate representation:

Workload:
Cardinality:
Read/write ratio:
Concurrency:
Memory constraints:

Current complexity:
Candidate complexity:

Current allocation behavior:
Candidate allocation behavior:

Current locality:
Candidate locality:

Benchmark:
Environment:
Rounds:

Result:
Throughput:
Latency:
Allocations:
Allocated bytes:
RSS:
CPU:

Neighboring results:

Rejected alternatives:
Reason:

Remaining bottleneck:
```

The decision record is part of the engineering result.

---

## 21. Rejection Is a Valid Result

If the benchmark shows:

```text
candidate <= baseline
```

keep the baseline.

Do not change the code merely because the candidate is theoretically cleaner.

A failed optimization is useful evidence.

Keep the benchmark when it protects against regression.

---

## 22. Anti-Patterns

The author must not optimize by:

```text
replacing every HashMap with a faster-looking structure
rewriting Vec into unsafe arrays without measurement
adding SIMD before removing allocations
adding lock-free structures without contention evidence
adding caching without measuring cache hit rate
adding batching without measuring latency impact
adding threads because the CPU is not saturated
removing abstractions because they "look expensive"
using unsafe because "Rust is slower"
using target-cpu=native to make a benchmark win
changing several variables in one benchmark
selecting the fastest run
discarding inconvenient benchmark results
optimizing only the benchmark that improved
```

---

## 23. Final Selection Rule

The selected algorithm or data structure must minimize the relevant cost function for the actual workload:

```text
performance =
    useful work
    / CPU cost
```

subject to:

```text
correctness
bounded memory
bounded queues
required ordering
required durability
required latency
required concurrency semantics
platform constraints
maintainability
```

No single metric dominates universally.

A throughput improvement that breaks durability is a regression.

A latency improvement that multiplies memory by ten is a trade-off.

A memory reduction that increases CPU cost by 50% is a trade-off.

The author must report the trade-off rather than calling it an optimization.

---

## 24. Required Process

Before changing an important algorithm or data structure:

```text
inspect
→ identify workload
→ identify invariants
→ identify actual bottleneck
→ enumerate plausible candidates
→ eliminate candidates violating invariants
→ benchmark remaining candidates
→ inspect memory/allocation behavior
→ validate neighboring workloads
→ select based on evidence
→ document the decision
```

Never:

```text
guess
→ rewrite
→ benchmark once
→ declare victory
```

The first sequence is the required engineering process.
