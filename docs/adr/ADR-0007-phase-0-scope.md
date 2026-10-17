# ADR-0007: Phase 0 scope, and how D-28 maps onto it

- Status: accepted
- Date: 2026-10-04
- Amended by: ADR-0008 (D-48 C: the broker and replication are rewritten for gateway/sequencer now, ahead of the phase order)
- Decided by: Stanislav (D-37: A)
- Deviations from: docs/architecture/unified-stream.md §6 (phase 0
  invariant 5, in part) and from accepted decision D-28 (stage 3 moves)

## Context

Phase 0 of the unified stream is: gap analysis and ADRs, an updated
rules.md, a simulation harness for the existing leader/follower replication
with invariants 1, 3 and 5, a versioned WAL record format with a properties
section, and an MQTT codec model with properties and reason codes. D-28
planned four stages for the simulation: (1) a disk boundary and a simulated
disk — done; (2) the leader's link decisions in `replication::Link` — done;
(3) admission accounting as a synchronous function; (4) a `sim` crate with
an event loop, an oracle (I1–I10) and the scenarios. Invariant 5 — "`lsn` is
monotonic, `prev_hash` forms an unbroken chain" — needs `prev_hash`, which
the document introduces in phase 1.

## Decision

Phase 0 consists of three tasks, each with its own review and closeout:

1. **The harness** — D-28 stage 4: a `sim` crate (publish = false) with an
   event loop over simulated time, the simulated disk, a network model at the
   connection level, an oracle and a seed-driven scenario generator. The
   oracle checks unified-stream invariants 1 and 3, the "`lsn` is monotonic"
   half of invariant 5, and I1–I10 of D-28. Same seed, same trace hash.
   It also takes in S4 at byte level and S9, deferred from stage 2 (D-34);
   until it does, the TCP matrix covers them.
2. **The WAL record format v4** — versioned records, and a message record
   with an extensible properties section (ADR-0003). `STRAUMW3` logs are
   handled by version, as `STRAUMW2` logs were.
3. **The MQTT 5 codec model** — properties, reason codes and reason strings
   in the sans-IO `mqtt` crate's packet model, with 3.1.1 mapped onto the
   same model. Whether `protocol`'s contract (and so the broker) carries
   reason codes in this phase is decided when task 3 is planned.

## Deviations, recorded

- Invariant 5 is checked in part: `lsn` monotonic from phase 0, the
  `prev_hash` chain from phase 1, when the format carries it.
- D-28 stage 3 (admission accounting as a synchronous function) moves to
  phase 3. That phase moves admission to the gateway, and refactoring the
  current path first would be done twice.

## Consequences

- The WAL format changes twice: v4 in phase 0, `(slot, lsn, prev_hash)` in
  phase 1. Versioning makes the second a version bump, not a migration of an
  unversioned format.
- D-28 is complete when task 1 of phase 0 is. Amended by ADR-0008: D-28 stays open for the old path — links C0
  and C5 of its chain are unchecked until step 3 (new path) or step 8 (the old
  path removed or kept as the single-node mode).

## Status of task 1 (2026-10-04)

The `sim` crate is the harness. Its checks have names of their own, mapped
here to unified-stream §5 and to D-28's I1–I10; the crate header carries the
same table.

```text
check            covers
U1               unified 5.1, over the addressable region (narrowed, below);
                 D-28 I3 for `fsynced` on the leader
U3               unified 5.3; D-28 I6, the prefix half
U5-lsn           unified 5.5 without prev_hash; D-28 I7 without correlations
H-quorum-copy    D-28 I4, the "copies" half
H-watermark      D-28 I5, in part (no PUBACK to withdraw in the harness)
H-ship-durable   D-28 I6, the first half
H-ahead          harness's own (a follower ahead of a leader that lost nothing)
H-reported       harness's own; serves D-28 I4 (a follower's report survives
                 its own power loss)
H-floor          D-28 I10, the first two parts
C1–C4            links of the D-28 chain, by reading the code
                 (crates/sim/tests/chain.rs); C3 as "a single caller" only
```

Not checked yet: D-28 I1, I2, I9, the PUBACK side of I3, I8 as D-28 defines
it, correlations, "within R is served", and scenario S10 (sessions and
retained) — these need the broker in the harness (D-44); and links C0 and C5
of the chain, which D-28 requires checked (C0 by a runtime test, since a count
cannot show it) — D-46.

## Deviations of task 1, recorded

- **U1 is narrowed** to the addressable region of the leader's log: a
  rewrite drops consumed records by design, below the retention floor. Unified
  §5.1 says "никогда не теряется"; whether a consumed, compacted record counts
  as lost is put to Stanislav with D-45.
- **The harness copies runtime glue** where the runtime still holds it,
  contrary to D-28 §7 ("Симуляция гоняет production-код, а не его модель"):
  the write-stall timer (so S9 is S9 as modelled by the harness, not the
  runtime's `tokio::time::timeout`), the live queue's drop-when-full policy,
  catch-up assembly into frames, the retention rule per mode
  (`runtime::wal::Retention` is crate-private), and the follower's
  commit-when-the-link-ends. Each is listed in D-45 with the option of moving
  it into a sans-IO crate the runtime and the harness both call.
- **Fault model.** `SimDisk` keeps the operating system's layer after a failed
  sync and loses it only on a power loss. On Linux a failed fsync may mark
  dirty pages clean; the harness does not model that.

## Published numbers (task 1)

Per step, by weight: commit 22.0%, delivery to the follower 28.0%, delivery
to the leader 19.0%, follower sync 13.0%, catch-up 12.0%, connect 3.0%,
rewrite 1.0%, stop reading 0.3%, cut 0.2%, leader crash 0.2%, leader power
loss 0.1%, follower crash 0.2%, follower power loss 0.1%, fresh copy 0.4%
(only while refused), injected sync failure 0.2%, nothing 0.3%. Each step
advances simulated time by 1–20 ms; the write window is 1 KiB, the stall
limit 300 ms, a power loss keeps a torn tail of 0–63 bytes.

Result on 2026-10-04: on the first sweep, 1 000 seeds × 400 steps per mode,
every check held except `H-reported` with the D-43 signature (22 seeds under
`fsynced`, 30 under `quorum`): a log that called durable what the operating
system held after a process crash. D-43 is fixed (the log syncs what it
recovers at open); since then 2 000 of 2 000 hold, with nothing tolerated,
and seed 3 is kept as the regression. Reproduce:
`STRAUM_SIM_SEEDS=1000 cargo test --release -p sim`.
