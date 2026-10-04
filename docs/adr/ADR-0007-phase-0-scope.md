# ADR-0007: Phase 0 scope, and how D-28 maps onto it

- Status: accepted
- Date: 2026-10-04
- Decided by: Stanislav (D-37: A)
- Deviations from: docs/architecture/unified-stream.md §6 (phase 0
  invariant 5, in part) and from accepted decision D-28 (stage 3 moves)

## Context

Phase 0 of the unified stream is: gap analysis and ADRs, an updated
CLAUDE.md, a simulation harness for the existing leader/follower replication
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
- D-28 is complete when task 1 of phase 0 is.
