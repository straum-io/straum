# Straum Engineering Constitution

> **Precedence.** `docs/architecture/unified-stream.md` decides where it and
> this constitution differ (CLAUDE.md, "Sources, in order of precedence";
> level 4 of that order, D-35). Superseded or narrowed, in particular:
>
> - §1: the contractual list ("durable acknowledgement semantics",
>   "memory / written / fsynced durability distinction") and the rule
>   "existing architecture is authoritative". The unified stream replaces the
>   architecture (ADR-0001) and the acknowledgement semantics (ADR-0005:
>   PUBACK and delivery only after commit by a majority); the durability
>   distinction remains for a single node and the current leader/follower,
>   while a cluster commits by majority only (ADR-0006, D-36). §1's
>   requirement to name each invariant and show it is preserved or
>   intentionally replaced still applies to every change that follows.
> - §13: "Do not introduce quorum into local fsynced semantics" and "Do not
>   make MQTT PUBACK wait for replication merely because replication exists".
>   In the target a PUBACK waits for commit by a majority (§2.3, ADR-0005).
> - §20 and §28 "Unsafe": the full policy is unified-stream §4.1 and ADR-0002.
>
> Everything else here stands.

## 0. Mission

Straum is a high-performance Rust systems project.

The primary engineering objective is:

> Maximize correctness, throughput, latency characteristics, memory efficiency,
> failure safety and operational predictability without introducing complexity
> that does not buy measurable value.

The project is not optimized for shortest implementation.

It is not optimized for minimum line count.

It is not optimized for "clean-looking" abstractions.

It is optimized for a correct systems architecture whose performance
characteristics are understood and measured.

The agent must behave as a performance engineer and systems architect, not as
a code completion engine.

---

# 1. Existing architecture is authoritative

Before changing architecture, inspect:

```
CLAUDE.md
docs/
crates/
tests/
benches/
configuration
existing invariants
existing benchmark results
```

Never replace an existing architectural invariant merely because another design
is more familiar.

Existing semantics are contractual unless the task explicitly changes them.

For Straum this currently includes, among other things:

* synchronous broker domain logic;
* separation between broker and blocking WAL I/O;
* adaptive group commit;
* WAL global ordering;
* durable offset;
* durable acknowledgement semantics;
* memory / written / fsynced durability distinction;
* replication consuming committed WAL batches;
* replication not introducing a second batching policy;
* follower durability reports;
* no quorum unless explicitly introduced as a later architectural layer;
* MQTT PUBACK must not accidentally become dependent on asynchronous follower
  state;
* bounded memory;
* crash recovery correctness;
* deterministic replay where required;
* storage and replication format compatibility rules.

When modifying an existing invariant, identify the invariant explicitly and
prove that the new design preserves or intentionally replaces it.

---

# 2. No speculative optimization

Never optimize because code "looks slow".

Never optimize because an alternative data structure is theoretically faster.

Never optimize because another project uses it.

Never optimize because an operation is O(n).

Never optimize because an operation allocates.

First establish:

```
1. where the cost occurs;
2. how large the cost is;
3. under which workload it matters;
4. what constraint actually limits the system;
5. whether the proposed replacement improves that constraint.
```

A performance change without measurement is an architectural hypothesis,
not an optimization.

---

# 3. Performance hierarchy

When evaluating an optimization, reason in this order:

```
algorithmic complexity
    ↓
data structure
    ↓
memory layout
    ↓
allocation/copy behavior
    ↓
synchronization
    ↓
syscall / I/O behavior
    ↓
CPU instructions
    ↓
micro-optimizations
```

Do not optimize level 7 while level 1 or 2 is wrong.

A better algorithm or memory layout normally dominates instruction-level
micro-optimization.

---

# 4. Algorithm and data-structure selection

The agent must actively evaluate algorithm and data-structure choices.

Do not automatically use:

```
HashMap
BTreeMap
Vec
VecDeque
LinkedList
Arc
Mutex
RwLock
DashMap
channels
atomics
async tasks
```

merely because they are familiar.

For every non-trivial hot-path structure, consider:

```
access pattern
mutation pattern
ordering requirements
cardinality
key distribution
lifetime
ownership
cache locality
allocation behavior
contention
memory overhead
false sharing
branch predictability
iteration characteristics
concurrency requirements
```

Prefer the structure that matches the actual workload.

Examples:

* contiguous storage is preferred when iteration dominates;
* sorted Vec may beat a tree or hash table for small collections;
* HashMap is not automatically preferable for lookup;
* BTreeMap may be preferable when ordering and locality matter;
* an index may be better than duplicating payloads;
* generational/index-based storage may be preferable to pointer-heavy graphs;
* batching may eliminate synchronization entirely;
* immutable data shared by reference may be preferable to repeated cloning;
* a single owner may be preferable to Arc<Mutex<T>>;
* a bounded queue is preferable to an unbounded queue when backpressure is a
  system invariant.

These are examples, not rules.

The agent must select based on the actual access pattern and measured workload.

---

# 5. Data structure decision protocol

When introducing or replacing a meaningful data structure, produce an internal
comparison before implementation.

Evaluate at least:

```
candidate A
candidate B
candidate C
```

when there are materially different alternatives.

Compare:

```
asymptotic complexity
constant factors
memory overhead
allocation count
cache locality
branch behavior
ordering
concurrency
contention
implementation complexity
expected workload fit
benchmarkability
```

Do not introduce three implementations into production.

Choose one.

If the difference is uncertain, benchmark a representative micro-workload.

The benchmark must represent the actual access pattern rather than an isolated
synthetic operation that favors one candidate.

---

# 6. Complexity is necessary but insufficient

Do not treat Big-O as a performance verdict.

For example:

```
O(1) hash lookup
```

may be slower than:

```
O(log n) binary search
```

for a small contiguous collection.

Likewise:

```
O(n)
```

may be preferable to a tree or hash structure when n is permanently small and
iteration dominates.

Consider:

```
n
operation frequency
memory locality
allocation
branch predictability
cache footprint
contention
```

before selecting a structure.

---

# 7. Rust ownership and allocation policy

Prefer ownership designs that minimize:

```
clone()
to_vec()
Vec allocation
String allocation
temporary buffers
reference-count manipulation
synchronization
```

Do not eliminate a clone merely because it exists.

First determine whether it is on a hot path and whether removing it improves
measured performance.

Do not use unsafe merely to eliminate a copy.

Unsafe requires a demonstrated performance reason and a smaller, explicit
safety boundary.

---

# 8. Zero-copy policy

"Zero-copy" is a goal, not a religion.

A copy is acceptable when it:

```
simplifies ownership;
removes synchronization;
reduces lifetime complexity;
improves cache locality;
avoids fragmentation;
enables contiguous I/O;
or is cheaper than maintaining a more complex zero-copy representation.
```

Every proposed zero-copy optimization must consider total cost:

```
memory retention
fragmentation
reference counting
cache behavior
lifetime constraints
allocator behavior
```

Do not trade a cheap memcpy for a complex ownership system without evidence.

---

# 9. Concurrency

Concurrency must be introduced because the workload requires it, not because
"parallelism is faster".

Prefer:

```
ownership
batching
message passing
single-writer designs
immutable state
```

over shared mutable state when practical.

Treat these as potentially expensive:

```
Arc
Mutex
RwLock
atomic contention
task spawning
wakeups
channels
cross-thread queues
```

Do not assume lock-free means faster.

Measure.

Avoid creating one asynchronous task per message or record unless measurement
shows that this architecture is appropriate.

---

# 10. Single-writer principle

When ordering and mutation are naturally sequential, prefer a single
authoritative writer.

Straum's WAL is an example.

Do not introduce multiple writers merely to increase apparent parallelism.

If parallelism is proposed, prove:

```
what was previously serialized;
why it is the bottleneck;
how ordering remains deterministic;
how contention changes;
how memory behavior changes;
how failure semantics change.
```

---

# 11. Storage invariants

The storage design is governed by docs/storage.md.

The following are hard invariants:

* WAL order is commit order.
* Durable acknowledgement never exceeds the durable boundary.
* Durable boundary advances monotonically.
* Records accumulated during a durability operation are eligible for the next
  batch.
* Idle operation does not wait for an artificial timer.
* WAL remains globally ordered.
* failed durability does not generate successful fsynced acknowledgement.
* recovery never infers durability from file length alone.
* compaction cannot rewind sequence or correlation high-water marks.
* replication cannot silently invalidate WAL identity/order.
* crash recovery remains correct after every storage-format change.

Do not weaken these invariants for benchmark results.

---

# 12. Adaptive group commit

The storage device is the pacing signal.

Do not introduce:

```
fixed flush timers
fixed message-count flushes
fixed latency-based flush cadence
```

as the primary commit mechanism.

The normal cycle is:

```
receive
  ↓
batch
  ↓
write
  ↓
sync_data()
  ↓
consume what accumulated
  ↓
repeat
```

Batch size is an emergent property of:

```
arrival rate × storage latency
```

Batch limits are safety boundaries, not primary flush triggers.

---

# 13. Replication

Replication consumes WAL commit batches.

It must not create an independent batching policy.

The fundamental path is:

```
WAL commit
    ↓
committed batch
    ↓
replication
    ↓
follower WAL
    ↓
follower durability
    ↓
report
```

Do not re-encode WAL records unnecessarily.

Do not introduce quorum into local fsynced semantics unless the task explicitly
changes durability semantics.

Do not make MQTT PUBACK wait for replication merely because replication exists.

---

# 14. Backpressure

Every queue must have a reason to exist and a defined bound.

For every queue identify:

```
producer
consumer
capacity
ownership
full behavior
failure behavior
memory cost
backpressure propagation
```

Never add an unbounded queue to hide a throughput mismatch.

A queue is not a solution to backpressure.

It is a bounded shock absorber.

---

# 15. Memory is a first-class performance metric

For every component that scales with:

```
connections
messages
topics
sessions
retained messages
replication backlog
```

determine:

```
per-item memory
fixed overhead
growth function
upper bound
allocator effects
```

Prefer measuring:

```
bytes/item
```

over guessing from source code.

Never claim "bounded memory" when only one component is bounded while another
component grows proportionally to the same workload.

---

# 16. Benchmarks are evidence

A benchmark must state:

```
workload
environment
configuration
warmup
duration
concurrency
payload size
storage mode
network topology
```

Do not compare numbers from different environments as if they were directly
equivalent.

Do not optimize against a single lucky run.

Measure:

```
throughput
p50
p95
p99
CPU
RSS
allocations
copies
batching
queue depth
```

where relevant.

---

# 17. Performance regression protocol

Before changing a hot path:

```
establish baseline
```

After changing it:

```
run identical workload
```

Compare:

```
throughput
latency
CPU
memory
allocations
correctness
```

A change that improves throughput but significantly worsens latency or memory
must not be described simply as "faster".

---

# 18. Profiling before micro-optimization

If performance is unexplained, profile.

Preferred progression:

```
benchmark
    ↓
counters
    ↓
tracing/instrumentation
    ↓
CPU profile
    ↓
allocation/heap profile
    ↓
code change
    ↓
benchmark again
```

Do not guess.

If a benchmark produces an anomalous result, investigate the anomaly before
optimizing around it.

---

# 19. Compiler and Rust optimization

Use the compiler as an optimization tool.

Check:

```
release profile
LTO
codegen units
panic strategy
target CPU
debug assertions
overflow checks
```

Do not change these globally merely to improve one benchmark.

Any compiler-level optimization must be evaluated against:

```
portability
binary size
compile time
runtime performance
```

---

# 20. Unsafe code

Unsafe is permitted only when:

```
correctness can be stated as explicit invariants;
the unsafe boundary is minimal;
safe Rust cannot achieve the required result reasonably;
the performance benefit is measured.
```

Never use unsafe as a substitute for choosing the correct data structure.

Never use unsafe to silence the borrow checker.

---

# 21. Tests

Tests verify behavior.

Tests do not define the implementation.

Never implement a special case merely to satisfy a test.

Prefer tests that verify invariants rather than implementation details.

For concurrency and distributed behavior:

```
synchronize through protocol/state transitions
```

rather than:

```
sleep()
```

Timing-based tests are not acceptable evidence of correctness when a
deterministic synchronization point exists.

---

# 22. Benchmark-driven architectural changes

For major architectural changes, establish:

```
baseline
hypothesis
candidate
benchmark
result
decision
```

Document the decision if it materially affects architecture.

Do not keep multiple competing implementations merely because they were
considered.

---

# 23. Agent workflow

For every non-trivial task:

## Phase A — Understand

Inspect the relevant architecture, invariants, tests and benchmarks.

Do not edit immediately.

## Phase B — Establish the constraint

Identify what actually limits the system:

```
CPU
memory
allocation
lock contention
queue contention
syscall
storage
network
algorithmic complexity
```

## Phase C — Generate candidates

Consider multiple algorithm/data-structure designs where meaningful.

## Phase D — Select

Choose the design with the best total system characteristics, not merely the
best Big-O complexity.

## Phase E — Implement minimally

Change only what is necessary.

## Phase F — Verify correctness

Run targeted tests first.

Then run the complete test suite.

## Phase G — Measure

Run the relevant benchmark against the previous baseline.

## Phase H — Audit

Check:

```
allocations
copies
synchronization
memory growth
failure behavior
cache-sensitive structures
hot-path regressions
```

## Phase I — Report

State:

```
what changed
why it changed
what was measured
what improved
what regressed
what remains uncertain
```

Never report an unmeasured optimization as a performance improvement.

---

# 24. Existing project lesson

Previous work has repeatedly demonstrated that apparently obvious changes can
be wrong.

Examples include:

* using a maximum correlation as a durable watermark;
* allowing correlation allocation before queue admission;
* allowing compaction to rewind sequence state;
* assuming TCP acknowledgement implies follower durability;
* treating a slow acknowledgement as equivalent to a slow reader;
* assuming a configured read buffer represents total per-connection memory;
* assuming a single throughput anomaly has an obvious cause.

Therefore:

> Treat every apparently obvious systems optimization as a hypothesis until
> its mechanism and measurement are established.

---

# 25. Anti-patterns

Do not:

* rewrite large modules without a measured reason;
* introduce abstractions for hypothetical requirements;
* replace a structure solely because another structure is theoretically faster;
* add caches without measuring cache effectiveness;
* add concurrency without measuring contention;
* add unsafe without a benchmark;
* add timers to compensate for incorrect batching;
* use sleep in correctness tests;
* hide queue growth with larger buffers;
* claim bounded memory from one bounded component;
* claim network performance from loopback;
* claim durability from write() without sync_data();
* claim replication correctness from a happy-path test;
* claim optimization from a single benchmark number;
* optimize around unexplained anomalies.

---

# 26. Definition of "optimized"

A component is considered optimized only when:

```
correctness is established;
architecture is understood;
algorithm/data structure is appropriate;
hot path is identified;
unnecessary allocation is removed where useful;
unnecessary copies are removed where useful;
synchronization is justified;
memory growth is understood;
relevant benchmarks exist;
regressions are checked;
failure behavior is tested.
```

"Compiles faster" or "looks cleaner" is not optimization.

---

# 27. Default decision rule

When two implementations are both correct:

Prefer the one with:

```
fewer synchronization points
fewer allocations
fewer copies
better locality
bounded memory
simpler ownership
fewer syscalls
fewer wakeups
deterministic behavior
measurable performance evidence
```

provided that it does not violate architecture or correctness invariants.

Do not optimize one metric in isolation.

Optimize the system.

---

# 28. Performance and algorithm enforcement

The repository contains two normative documents:

1. `CONSTITUTION.md` — highest-level project engineering rules.
2. `docs/algorithms.md` — algorithm and data-structure selection procedure.

These rules override convenience, implementation speed, minimum diff size and
stylistic preference.

## Mandatory order

For performance-sensitive work:

```text
understand
→ measure
→ profile
→ identify bottleneck
→ identify invariants
→ evaluate algorithm/data-structure alternatives
→ benchmark
→ implement one change
→ benchmark again
→ validate neighbors
→ inspect memory/allocation behavior
→ review
```

Never:

```text
guess
→ rewrite
→ benchmark once
→ declare optimized
```

## Algorithm selection

A data structure or algorithm must not be changed merely because:

* it has better Big-O complexity;
* it looks more idiomatic;
* an LLM considers it faster;
* another project uses it;
* it has fewer lines;
* it uses fewer abstractions.

Use `docs/algorithms.md`.

The workload determines the representation.

## Performance evidence

No performance claim without:

* workload;
* baseline;
* candidate;
* repeated measurements;
* relevant environment;
* variance;
* neighboring benchmarks when applicable;
* allocation/memory impact.

## Anomalies

An anomalous benchmark result blocks optimization.

Required investigation order:

```text
repeat benchmark
→ verify workload
→ inspect counters
→ inspect tracing
→ CPU profile
→ allocation profile
→ inspect generated code
→ form hypothesis
→ change code
```

Do not optimize around unexplained measurements.

## One change

One meaningful performance variable per experiment.

Do not combine:

* algorithm changes;
* data structure changes;
* compiler flags;
* allocator changes;
* thread-count changes;
* workload changes

in one experiment.

## Generated code

Source-level elegance is not performance evidence.

When a hot path is under investigation, inspect generated code only after
profiling establishes that it matters.

## Unsafe

Unsafe is not an optimization strategy.

It requires measured evidence that safe code is the bottleneck and that the
unsafe implementation materially improves the relevant workload.

## Memory

Memory is a first-class performance metric.

Always consider:

```text
per-object cost × object count
```

A per-connection optimization must be evaluated at realistic connection
counts.

## Review

After a non-trivial performance change:

1. run correctness tests;
2. run benchmarks;
3. run neighboring workloads;
4. inspect allocation/memory behavior;
5. inspect the final diff;
6. document the measured result.

A failed optimization is a valid result and should not be hidden.

---

# 29. Memory accounting is an invariant

A memory limit is not valid merely because one allocation is bounded.

If several allocations constitute one logical resource, the bound must
account for the complete resource, or state explicitly which components it
excludes.

Configuration names must describe the resource they actually control.

A global memory budget must be a real upper bound under the stated
assumptions, not an estimate derived from one per-connection allocation.

RSS measurements may validate a model. They never substitute for one.

Before changing memory limits, buffer sizes, allocation strategy or
configuration semantics:

```text
1. identify every allocation in the resource;
2. establish ownership and lifetime;
3. derive the bound;
4. test the bound;
5. only then optimize.
```

Never optimize one memory component in isolation when its cost is coupled to
another.

---

# 30. Who may declare durability

```text
Storage declares local durability.
Replication observes durability.
Only the replication quorum coordinator may translate peer durability
reports into a quorum durability watermark.
The broker consumes the resulting Event::Durable without knowing
whether it represents local fsync or quorum durability.
```

A peer's report means one thing: that peer has written and synced
everything through a position. Whether that position is enough for a quorum
is a different question with a different owner, and answering it inside the
peer protocol would let the wire format decide the runtime's durability
semantics.

A quorum watermark is monotonic. An acknowledgement cannot be taken back, so
a position once declared durable stays declared even when the replica that
helped establish it goes away.

Losing a quorum is not a poisoned log. A partition, a peer restarting and a
follower that is merely slow all stop new acknowledgements and none of them
is terminal; a poisoned log is this node's storage being unusable. The two
must not share a failure mode.
