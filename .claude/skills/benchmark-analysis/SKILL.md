---
name: benchmark-analysis
description: Analyze Rust benchmark results, identify anomalies, distinguish measurement artifacts from real regressions and prevent optimization based on unexplained data.
---

# Benchmark Analysis

A benchmark result is evidence, not a conclusion.

## First classify the result

Determine whether the result is:

```
stable
noisy
anomalous
environment-dependent
configuration-dependent
workload-dependent
```

---

# Anomaly protocol

If one result differs materially from otherwise comparable results:

Do NOT optimize for it.

First:

```
reproduce
isolate
instrument
identify the stage responsible
compare against control
```

Construct competing hypotheses.

Example:

```
storage latency
scheduler
queue contention
TCP backpressure
allocation
benchmark harness
CPU frequency
filesystem
process interference
synchronization
```

Eliminate hypotheses with measurements.

---

# Required comparison

For a pipeline:

```
A → B → C → D → E
```

measure boundaries:

```
A→B
B→C
C→D
D→E
```

The goal is to identify where throughput or latency disappears.

Do not infer the bottleneck from the final number.

---

# Reporting

Separate:

```
measured fact
interpretation
hypothesis
unresolved question
```

Never turn an unresolved hypothesis into documentation as if it were fact.

---

# Optimization after anomaly

Only optimize after the responsible stage is identified.

Then:

```
baseline
change
identical benchmark
comparison
```

If the anomaly disappears without the code change, treat the original result
as measurement noise or environment-specific behavior.
