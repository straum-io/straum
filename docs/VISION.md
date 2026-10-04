# Straum Vision

> **Superseded in part by docs/architecture/unified-stream.md** (ADR-0001):
> V1 positioning by §1; V4's exclusion of MQTT 5 and QoS 2 by §2.7 and §6
> (ADR-0003: MQTT 5 from the first day, QoS 2 and shared subscriptions in
> phase 6); V5's roadmap order by §6. V3's promises stand where they agree
> with the unified stream; "acknowledged ⇒ durable at the declared level"
> becomes "PUBACK only after commit" in the target (ADR-0005).

Status: **DRAFT.** Written from the product analysis of 2026-09-28.
Every item marked `(unconfirmed)` is a proposal until Stanislav confirms it.
The vision-guardian treats unconfirmed items as soft: a conflict with one is
reported as a note, never as a DRIFT verdict.

This file answers one question for every change: **does it move Straum
towards what it is for?** `CONSTITUTION.md` says how work is done;
`CLAUDE.md` says how the system is built; this file says why it exists and
what it deliberately is not.

Keep it under two screens. It is loaded by the guardian on every closeout.

---

## V1. Positioning

An MQTT broker whose acknowledgement means exactly what it says, and that
claim is verified rather than asserted.

A PUBACK at level `written`, `fsynced` or `quorum` is a durability statement
that survives the failures that level names. Proving that is the product.

## V2. Who it is for (unconfirmed)

* IIoT and telemetry where a lost message costs money: metering, billing,
  SCADA events, audit trails.
* Edge-to-cloud gateways that need a small, predictable binary.
* Teams that need a replicated, highly available broker under a permissive
  licence and do not want a JVM or BEAM runtime.

## V3. Core promises, in priority order

When two promises conflict, the higher one wins.

1. **Acknowledged ⇒ durable at the declared level.** No early
   acknowledgement, no silent downgrade of a durability level.
2. **Memory is bounded** under any input, per connection, per session and
   globally, by budgets that bound what they claim to.
3. **Operational predictability.** Explicit backpressure, explicit failure
   modes, configuration errors fail at startup, metrics an operator can act on.
4. **Performance per CPU cycle,** measured and reported with its workload.
5. **Protocol coverage** sufficient for the users in V2.

## V4. Non-goals

A change that moves towards one of these is drift unless a decision in
`docs/decisions/log.md` explicitly accepted it.

* Winning raw msg/s benchmarks at the expense of V3.1–V3.3.
* A rule engine, data-integration connectors, or a management UI inside the
  broker process.
* Protocols other than MQTT (MQTT-SN, CoAP, LwM2M, STOMP).
* Becoming a general streaming platform (retention-as-storage, consumer
  groups, Kafka semantics).
* QoS 2 before there is a concrete user requirement.
* Speculative abstraction: traits, plugin systems or extension points without
  two real implementations.

## V5. Roadmap priority (unconfirmed)

Work that serves an earlier item is preferred over work that serves a later
one, unless it unblocks the earlier item.

1. Deterministic simulation of broker, WAL and replication with fault
   injection; a published verification of V3.1.
2. Automatic failover with fencing.
3. MQTT 5 core: reason codes, session and message expiry, shared
   subscriptions, receive maximum.
4. A reproducible comparison against other brokers on a quiet Linux host.
5. Operability: Helm chart, runbooks for every failure in the failure matrix.

## V6. Licence (unconfirmed)

Permissive (Apache 2.0 or MIT). Clustering and replication are never gated
behind a commercial licence.

## V7. Drift signals

The guardian looks for these in particular:

* A feature from V4 appearing without an accepted decision.
* Work on a later V5 item while an earlier one is blocked by nothing.
* A durability, ordering or memory guarantee weakened "temporarily".
* A performance claim without a workload and a measurement.
* A task that grew beyond what was asked (scope creep), however useful.
* Re-proposing an option that `docs/decisions/log.md` already rejected,
  without new evidence.
