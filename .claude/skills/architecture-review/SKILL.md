---
name: architecture-review
description: Review Rust systems architecture before significant changes, especially storage, networking, concurrency, replication and performance-sensitive paths.
---

# Architecture Review

Before changing architecture, establish the current system model.

Identify:

```
ownership boundaries
execution boundaries
I/O boundaries
synchronization boundaries
durability boundaries
failure boundaries
backpressure boundaries
```

---

# Do not optimize locally when the bottleneck is architectural

Examples:

If the bottleneck is:

```
fsync latency
```

do not optimize:

```
HashMap lookup
```

If the bottleneck is:

```
lock contention
```

do not optimize:

```
serialization
```

If the bottleneck is:

```
allocation
```

do not redesign:

```
network protocol
```

If the bottleneck is:

```
memory retention
```

do not merely increase queue capacity.

---

# Boundary analysis

For every major path identify:

```
producer
consumer
queue
owner
synchronization
persistence
acknowledgment
```

Determine where state becomes authoritative.

For Straum, examples include:

```
WAL writer
durable offset
commit batch
follower durable report
broker durable watermark
```

Do not duplicate authority.

---

# Architectural change threshold

Before introducing:

```
new crate
new queue
new thread
new async task
new synchronization primitive
new persistence layer
new cache
new abstraction
```

explain what measurable problem it solves.

Prefer modifying the existing boundary when it already provides the required
semantics.

---

# Architecture before implementation

If a proposed change affects multiple crates, first produce:

```
current path
proposed path
changed invariants
preserved invariants
failure modes
performance hypothesis
```

Then implement.
