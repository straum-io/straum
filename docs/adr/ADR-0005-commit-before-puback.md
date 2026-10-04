# ADR-0005: Commit before PUBACK

- Status: accepted
- Date: 2026-10-04
- Decided by: Stanislav
- Source: docs/architecture/unified-stream.md §2.3, §5 (invariant 4)

## Context

Today a PUBACK waits for durability at the configured level: routing only
(`memory`), the operating system (`written`), the local disk (`fsynced`), or
the follower's durable report as well (`quorum`). Delivery to subscribers
happens at routing, before any of these — at every level. The chain that
keeps a PUBACK from leaving early is recorded as C0–C5 in D-28.

## Decision

In the target model:

- commit is a durable acknowledgement from a majority (two of three nodes),
  and it is a prefix;
- a PUBACK is sent, and a message is delivered to subscribers, only after
  the record that carries it is committed — never before, so nothing
  delivered can be forked or rolled back;
- retained messages and other reads are served from committed state, or
  under a lease, which is the one place physical time may appear, with an
  explicit bound on drift;
- the gateway that holds the client's connection sends the PUBACK when it
  sees the commit.

## Consequences

- Delivery moves behind commit in cluster mode (ADR-0006). That is phase 3
  work (gateway/sequencer), with its own simulation scenarios; a single node
  keeps delivery at routing.
- The single-node durability levels (`memory`, `written`, `fsynced`) remain
  for a single node; a cluster commits by majority only (ADR-0006, D-36).
- The invariant "PUBACK and delivery never precede commit" (unified-stream §5,
  invariant 4) joins the simulation's oracle; the current oracle (I1–I10 of
  D-28) checks PUBACK only.
