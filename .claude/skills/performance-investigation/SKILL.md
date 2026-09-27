---
name: performance-investigation
description: Work through an anomalous or unexplained performance measurement in a fixed order - reproduce, verify the workload, collect counters, profile - before changing any code. Use whenever a benchmark result cannot be explained.
---

# Performance Investigation Order

1. reproduce
2. verify same workload
3. verify same binary/profile
4. verify same machine
5. verify same environment
6. collect counters
7. syscall profile
8. CPU profile
9. allocation profile
10. compare flamegraphs
11. identify first divergent subsystem
12. formulate hypothesis
13. create minimal experiment
14. confirm/refute
15. only then modify implementation