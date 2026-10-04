# ADR-0006: Durability levels on a single node, majority commit in a cluster

- Status: accepted
- Date: 2026-10-04
- Decided by: Stanislav (D-36: A)
- Refines: ADR-0005
- Deviation from: docs/architecture/unified-stream.md §1, §2.3 and §5
  invariant 4, for a single node and the current leader/follower

## Context

`STRAUM_DURABILITY` has four levels: `memory`, `written`, `fsynced`,
`quorum`. A PUBACK waits for its level; delivery to subscribers happens at
routing, at every level. The unified stream (§2.3, ADR-0005) says commit is a
durable acknowledgement from a majority, and that a PUBACK and a delivery
come only after commit. It says nothing about a single node, and nothing
about whether the levels disappear.

## Decision

- **Cluster mode** — from phase 1, three or more voting nodes — has one
  semantics: commit by a majority, a PUBACK and a delivery only after commit.
  `memory`, `written` and `fsynced` are refused in cluster mode at startup, as
  a configuration error that names the variable. A cluster without an
  explicit mode is a configuration error too.
- **A single node and the current leader/follower** keep the four levels as
  they are until phase 3. Their PUBACK follows the chain C0–C5 recorded in
  D-28; their delivery stays at routing.
- Delivery after commit is introduced in cluster mode, in phase 3
  (gateway/sequencer). It is not introduced on a single node.

## Deviation, recorded

Unified-stream §2.3 and §5 invariant 4: "PUBACK и доставка выполняются
ТОЛЬКО после commit", commit being a majority. On a single node and on the
current leader/follower under `memory`, `written` or `fsynced`, a PUBACK
follows only this node's level, never a majority, and delivery precedes it.

Unified-stream §1: "every observable effect is the application of a committed
record". On a single node, a delivery is observable before the record is
durable (at every level), and under `memory` there is no record at all. This
is kept deliberately for single-node users and is not allowed in cluster
mode.

## Consequences

- Phase 1 adds the cluster-mode configuration and its startup checks.
- The simulation oracle checks invariant 4 of unified-stream §5 ("PUBACK and
  delivery never precede commit") in cluster mode; for a single node and the
  current leader/follower it checks the PUBACK half through C0–C5 and I1–I10
  of D-28.
- D-33 (`written` with a follower) stays a leader/follower question until
  phase 1; in cluster mode `written` cannot occur.
- Documentation separates the two modes wherever durability is described.

## Proposed, for the start of phase 1 (D-42)

Not decided; recorded so that phase 1 starts from a worked answer, which is
then put to Stanislav as a card.

- Cluster mode is three or more voting nodes, and a witness (unified-stream
  §2.5) votes: two data nodes and a witness are a cluster.
- Commit is a majority of the voters that hold data among the voters, so a
  witness can complete a promise but never stands in for a durable copy.
- A single node remains a shipped mode after phase 3, with its levels.
