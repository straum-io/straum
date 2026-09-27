# Performance Baseline

## Replication

Environment:
Apple M1
macOS
localhost TCP
two nodes

### Current measured baseline

64 B:
local       14,528 msg/s
replicated   7,500 msg/s

1 KiB:
local       12,702 msg/s
replicated   7,621 msg/s

64 KiB:
local       16,899 msg/s
replicated  14,575 msg/s

### Resolved anomaly

The 667 msg/s figure was a benchmark-harness defect: that section timed the
client teardown along with the publish loop. The real number for the same
scenario is 11,300 msg/s at 4.4 ms per commit, which is the fsync.

Higher than the 6,323 an ordinary follower gives, because this one keeps no
log. See docs/replication.md §124.

### Connection memory

16 KiB transport buffers:

~53.4 kB (52.2 KiB) / idle connection

4 KiB:

~32.8 kB (32.1 KiB) / idle connection

Measured:
2500
5000
8860
15000 connections

Observed approximately linear growth in this range.

This is an empirical observation, not a validated extrapolation to 100k/1M.