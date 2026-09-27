---
name: algorithm-selection
description: Choose or replace a data structure - lookup, routing, queue, index, buffer, session or replication state - from the measured workload rather than from complexity or familiarity. Covers Vec against HashMap and BTreeMap, small-N crossovers, ordering, ownership before synchronization, and the benchmark that settles it. Use before adding or replacing any structure on a path that matters.
---

# Algorithm and data structure selection

Do not begin with the data structure. Begin with the workload.

Mandatory whenever a change affects lookup structures, routing structures,
queues, indexes, storage structures, replication state, session state, topic
matching, buffering, batching, scheduling, concurrency, memory ownership or
serialization layout.

The full protocol is `docs/algorithms.md`; the constitution's §4, §5 and §6
are what make it binding. This skill is the working order.

## 1. Characterize the workload

Before touching code, know:

```text
the operation, exactly
call frequency
N, and how large N can become
key and payload sizes
read/write ratio
lookup pattern
iteration pattern
ordering requirements
mutation frequency
lifetime
concurrency
memory constraints
expected worst case
```

If these are unknown, read the code, the configuration, the benchmarks and
the tests until they are. A structure chosen against an imagined workload is
a guess with a data structure attached.

## 2. Establish the current cost

Use, in this order: an existing benchmark, a profiler, counters, allocation
measurements, syscall measurements where relevant.

If no evidence exists, build the smallest measurement that produces some.

## 3. Inspect the current representation

Determine what it actually does per operation:

```text
allocations          copies              pointer indirections
hashing              comparisons         atomics
locks                cache locality      resizing
metadata overhead
```

Do not infer any of these from how the source looks.

## 4. Generate candidates

At least two viable ones, unless only one satisfies the invariants:

```text
Vec                  sorted Vec          VecDeque
HashMap              BTreeMap            indexed Vec
ring buffer          slab                arena
fixed-capacity array sharded             immutable snapshot
ownership transfer   custom structure
```

The standard library type is not automatically right. A custom structure
needs evidence that nothing standard fits.

## 5. Evaluate each candidate

| Property | Question |
| --- | --- |
| Lookup | What is the actual operation? |
| Insert | How often? |
| Delete | How often? |
| Iterate | How often? |
| Order | Is deterministic order required? |
| N | How large can it become? |
| Memory | Bytes per element? |
| Allocation | How often? |
| Locality | Sequential or pointer-heavy? |
| Concurrency | Shared or partitioned? |
| Contention | Where? |
| Worst case | What happens under adversarial load? |
| Failure | What state survives an error? |

### Small collections

When N is small or bounded, consider `Vec`, a sorted `Vec` and a linear scan
**before** `HashMap` or `BTreeMap`: contiguous memory and simple iteration
can dominate asymptotic complexity. Not a rule — benchmark when the path
matters.

### Large collections

When N is large, weigh cache locality, allocation overhead, hash cost, tree
node overhead, pointer indirection and footprint. A theoretically optimal
algorithm with poor locality loses to a simpler one.

### Ordering

If deterministic ordering is required, decide where it is established:
continuously, at insertion, during batch processing, or only at read time.
Maintaining order continuously can cost more than sorting a batch once.
Choose from the actual operation frequency.

### Ownership before synchronization

Before reaching for `Arc`, `Mutex` or `RwLock`, ask whether ownership can be
transferred, state partitioned, the structure owned by one task, immutable
data shared, or ownership carried by the message. Removing shared mutable
state is usually a larger win than replacing one collection with another.

## 6. Eliminate what violates an invariant

Reject any candidate that breaks ordering, durability, memory bounds,
concurrency, protocol semantics, recovery semantics or replication
semantics. Record why it was rejected — a rejected candidate with a reason
is a result.

## 7. Benchmark

Identical workloads. Between candidates do not change the compiler, the
workload, the payload, the thread count or the environment.

Measure throughput, latency, allocations, allocated bytes, memory, and CPU
where available.

Benchmark what the structure is actually used for. If a map exists mostly to
be iterated, benchmarking lookup answers the wrong question.

## 8. Find the crossover

If a candidate's standing changes with N, measure enough cardinalities to
locate where. Do not add adaptive selection unless the crossover falls
inside the real workload.

## 9. Validate end to end

Run the affected benchmark, its neighbours, and the relevant end-to-end
benchmark. A structure that wins in isolation and loses in the system has
lost.

## 10. Inspect generated code

Only when the measured difference is small or the path is extremely hot. Use
it to explain a result, never to manufacture one.

## 11. Decide

Accept only on a meaningful improvement under the real workload with no
unacceptable regression. Otherwise keep what is there — that is a result,
not a failure.

Choose one design. Do not leave several implementations in production
because they were considered.

## Required output

Before implementing a significant replacement:

```text
CURRENT
CANDIDATES
INVARIANTS
WORKLOAD
MEASUREMENTS
DECISION
EXPECTED TRADE-OFF
VALIDATION PLAN
```

After implementing it:

```text
BASELINE
CANDIDATE
NEIGHBORS
MEMORY
ALLOCATIONS
CORRECTNESS
FINAL DECISION
```

Never call a change "faster" without a measured workload and a comparison.
