---
name: rust-hotpath
description: Review and optimize performance-sensitive Rust code, ownership, allocation, async execution, memory layout and synchronization.
---

# Rust Hot Path Review

Treat Rust abstractions as potential runtime costs only when they actually
produce runtime work.

Inspect:

```
allocation
cloning
copying
synchronization
atomics
Arc
Mutex
channel operations
task creation
wakeups
dynamic dispatch
parsing
hashing
bounds checks
buffer growth
```

Do not optimize abstractions merely because they look high-level.

---

# Ownership

Prefer:

```
ownership transfer
borrowing
slices
Bytes sharing
buffer reuse
```

when they reduce real copies or allocations.

Do not introduce complicated lifetimes solely to eliminate an insignificant
copy.

---

# Async

Check:

```
task count
wakeups
channel operations
executor scheduling
cancellation
backpressure
```

Do not create one task per message unless measured evidence supports it.

Prefer batching when work naturally arrives in batches.

---

# Channels

For every channel inspect:

```
capacity
message size
ownership
producer count
consumer count
blocking behavior
full behavior
```

Avoid using a channel as an accidental unbounded memory store.

---

# Atomics

For each atomic:

```
identify contention
identify ordering requirements
identify update frequency
determine whether batching can reduce updates
```

Do not replace an atomic with a mutex or vice versa without measurement.

---

# Copy elimination

Before eliminating a copy, determine:

```
where the source buffer lives
how long it must live
who owns it
whether reference counting is introduced
whether memory retention increases
```

A zero-copy design that retains a 10 MB buffer for one 10-byte message may
be worse than a small copy.

---

# Unsafe

Unsafe is permitted only when:

```
the invariant is explicit;
the unsafe region is minimal;
safe alternatives were considered;
benchmark evidence demonstrates meaningful benefit.
```

Keep unsafe isolated and test its invariants aggressively.
