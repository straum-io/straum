# ADR-0001: Unified Stream

- Status: accepted
- Date: 2026-10-04
- Decided by: Stanislav
- Supersedes: the cluster plan (docs/cluster.md), the roadmap order of
  docs/VISION.md V5, and "leader/follower as the target" in
  docs/replication.md

## Context

Straum runs as one node, or as one leader shipping its WAL to one static
follower. Durability levels `memory`, `written`, `fsynced` and `quorum`
decide when a PUBACK goes; `quorum` waits for the follower's durable report.
There is no election, no fencing and no failover; a lost leader is an
operator's problem. The earlier cluster plan (docs/cluster.md) designed
session ownership across autonomous nodes and was never built.

Work on that model found defects that live between the nodes rather than
inside either: a follower holding records the leader later lost and reused
under the same numbers (D-33), a catch-up that shipped records not yet
durable on the leader (D-22). They are what a model without an identity of
history and without fencing produces.

## Decision

Straum adopts the architecture in `docs/architecture/unified-stream.md`:
the cluster is one mechanism; the log is the only source of truth; a node is
a deterministic function of the committed log and its input events;
gateways and one sequencer per slot; commit by majority, PUBACK and delivery
only after commit; fencing by `(slot, lsn, prev_hash)`; graceful and
ungraceful handoff; MQTT 5 from the start; sans-IO cores driven by a
runtime; correctness by deterministic simulation, performance by
measurement on Linux.

The document takes precedence over CLAUDE.md, which takes precedence over
ADRs, which take precedence over everything else.

Separate records cover the decisions this one rests on:

- ADR-0002: the unsafe policy and how a crate is exempted.
- ADR-0003: MQTT 5 from the first day.
- ADR-0004: sans-IO cores, the runtime as a driver.
- ADR-0005: commit before PUBACK.

## Consequences

- Development follows the phases of unified-stream §6, starting with
  phase 0.
- What the code does today is documented in
  `docs/architecture/current-implementation.md` and stays true until each
  phase changes it. Documents that describe the old target carry a
  "Superseded by docs/architecture/unified-stream.md §…" line rather than
  being deleted.
- The WAL record format and the replication protocol will change (phase 0:
  versioned records with properties; phase 1: `(slot, lsn, prev_hash)`).
  The current `STRAUMW3` format and its tests remain until then.
- The durability levels remain the modes of a single node and of the current
  leader/follower; a cluster commits by majority only (ADR-0006, D-36).
- The deterministic simulation begun under D-28 is the phase-0 harness;
  ADR-0007 records how D-28's remaining stages map onto phase 0 (D-37).
- Decisions still pending on the old model (D-33, D-34) are re-read against
  this one before they are taken.
