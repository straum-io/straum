# ADR-0003: MQTT 5 from the first day

- Status: accepted
- Date: 2026-10-04
- Decided by: Stanislav
- Source: docs/architecture/unified-stream.md §2.7, §6
- Supersedes: "Out of scope: MQTT 5, QoS 2, shared subscriptions" in the
  former CLAUDE.md §6 (now docs/architecture/current-implementation.md) and
  docs/VISION.md V4 where it ruled MQTT 5 out

## Context

The broker implements MQTT 3.1.1. Its codec, its internal command model
(`protocol::Command`, `Outbound`, `Response`) and its WAL record bodies have
no place for properties, reason codes or reason strings. Adding MQTT 5 later
would change the codec model, the broker contract and — most expensively —
the on-disk record format.

## Decision

MQTT 5 is part of the architecture now, even where features land later:

- the sans-IO MQTT crate is versioned (3.1.1 and 5) over one internal packet
  model in which properties, reason codes and reason strings are first-class;
  broker errors map to reason codes;
- the WAL record format is versioned, and a message record carries an
  extensible properties section from the start (user properties, content
  type, response topic, correlation data, payload format indicator, message
  expiry);
- time-based protocol behaviour (session expiry, message expiry, will delay)
  is decided through the log by the sequencer, never autonomously by a node;
- subscriptions are stored with their options (No Local, Retain As Published,
  Retain Handling, Subscription Identifier);
- session takeover goes through the log; Assigned Client Identifiers are
  unique in the cluster and assigned deterministically;
- per-connection state (Topic Alias, Receive Maximum, Maximum Packet Size,
  keep alive) is local to the gateway and not replicated; inflight QoS 1 is;
- enhanced authentication is a separate sans-IO state machine;
- QoS 2 and shared subscriptions are implemented in phase 6, but record types
  are reserved for them now.

## Consequences

- Phase 0 delivers the codec model and the versioned WAL record with a
  properties section; the current `STRAUMW3` record bodies are migrated or
  refused by version, as `STRAUMW2` was.
- `protocol` gains properties and reason codes in its contract; the broker
  stays protocol-neutral, with no MQTT vocabulary.
- The MQTT 3.1.1 behaviour table in current-implementation.md remains the
  description of 3.1.1 handling.
