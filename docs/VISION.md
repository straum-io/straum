# Straum Vision — what still stands

The architecture is `docs/architecture/unified-stream.md`. This file keeps
the parts of the original vision it did not supersede (ADR-0001): V1
positioning and V5 roadmap order are superseded and live only in
`docs/archive/VISION.md`; V4 is amended by ADR-0003.

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


In the target, promise 1 reads "PUBACK only after commit" (ADR-0005).

## V4. Non-goals

A change that moves towards one of these is drift unless a decision in
`docs/decisions/log.md` explicitly accepted it.

* Winning raw msg/s benchmarks at the expense of V3.1–V3.3.
* A rule engine, data-integration connectors, or a management UI inside the
  broker process.
* Protocols other than MQTT (MQTT-SN, CoAP, LwM2M, STOMP).
* Becoming a general streaming platform (retention-as-storage, consumer
  groups, Kafka semantics).
* QoS 2 and shared subscriptions before phase 6 (ADR-0003; MQTT 5 itself is in scope from the start).
* Speculative abstraction: traits, plugin systems or extension points without
  two real implementations.


## V6. Licence (unconfirmed)

Permissive (Apache 2.0 or MIT). Clustering and replication are never gated
behind a commercial licence.

## V7. Drift signals

The guardian looks for these in particular:

* A feature from V4 appearing without an accepted decision.
* Work on a later ADR-0008 step while an earlier one is blocked by nothing.
* A durability, ordering or memory guarantee weakened "temporarily".
* A performance claim without a workload and a measurement.
* A task that grew beyond what was asked (scope creep), however useful.
* Re-proposing an option that `docs/decisions/log.md` already rejected,
  without new evidence.
