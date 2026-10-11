# Performance Baseline

> **History.** This document describes the old leader/follower broker, removed at step 8 on 2026-10-11 (D-111): the crates `broker`, `protocol`, `mqtt`, `replication`, `runtime` and `e2e` are gone, and its code is in git history (commit ffeda02 and earlier). What runs now is the cluster path of ADR-0008, documented in the ADR, `docs/configuration.md` and `docs/testing.md`. Kept as a record of what was built, measured and decided.

## Replication

Environment:
Apple M1
macOS
localhost TCP
two nodes

### Current measured baseline

D-6 B, 2026-10-02, `e2e/benches/durability.rs`, one publisher, QoS 1, a
window of 100 in flight, timed from the first publish to the last PUBACK,
medians of 5 alternating rounds. Full table, method and spread in
docs/baselines/apple-m1-dev.md.

```text
              64 B          1 KiB         64 KiB
memory     305 792 msg/s   275 843       59 273
fsynced     11 972 msg/s    11 377        4 270     p50 PUBACK 7.9 / 8.4 / 13.1 ms
quorum       5 192 msg/s     4 285        2 514     p50 PUBACK 17.4 / 23.1 / 35.5 ms
```

Admission (D-7) costs 24–33 ns per durable publish, no allocation, measured
uncontended (`runtime/benches/admission.rs`) — about 0.1% of what a
`fsynced` publish costs here with one publisher, and not resolvable end to
end on this machine. Contention between many publishers is not measured.

### `replicate.rs`: local and asynchronous replica, re-measured (D-11)

`fsynced` on the leader; `replicated` is an asynchronous follower — a PUBACK
does not wait for it. Timed to the last PUBACK, medians of 5, 2026-10-02.
Full table, latencies and ranges in docs/baselines/apple-m1-dev.md.

```text
64 B     local 11,843   replicated 6,478
1 KiB    local 11,700   replicated 6,155
64 KiB   local  3,702   replicated 1,860
```

Superseded — the figures that stood here until 2026-10-02. That harness
stopped the clock when the last publish was *queued in the client*, not
when it was acknowledged, and its latency columns were the time to queue:
its 64 KiB row showed a 0.01 ms acknowledgement under `fsynced`, where the
sync alone takes ~5 ms. Long runs were barely affected; these short ones
were. Kept for the record only:

```text
64 B     local 14,528   replicated  7,500
1 KiB    local 12,702   replicated  7,621
64 KiB   local 16,899   replicated 14,575
```

### Resolved anomaly

The 667 msg/s figure was a benchmark-harness defect: that section timed the
client teardown along with the publish loop. The real number for the same
scenario is 11,300 msg/s at 4.4 ms per commit, which is the fsync.

That 11,300 came from the same `replicate.rs` loop and had the flaw described
above. Re-measured to the last PUBACK it is 11,443 msg/s (median of 5): over
20 000 messages the queue tail the old clock missed was too small to matter,
so the figure held.

Higher than the 6,323 an ordinary follower gives (6,293 re-measured to
the last PUBACK), because this one keeps no log. See docs/replication.md §124.

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