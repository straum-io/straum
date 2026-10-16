# Pending decisions

Decision cards waiting for the user. Written only by the task-closeout
skill. When the user answers `D-<n>: <choice>`, the card moves to
`log.md` with the answer and is removed from here.

---


## D-144. Step 7c: the design of persistent sessions, before any code

**Context.**
- **D-143 A:** 7c starts with a design. It must define:
  - a global limit on persisted sessions, with refusal at the limit, never unbounded memory or disk;
  - recovery from a failed admission, a restart and a partial write.

  No implementation until this is agreed.
- **What exists:**
  - `apply` keys every session by `Origin {node, session}`: one connection at one gateway. There is no client id in the replicated state.
  - A session ends with `SessionGone`, or with its node's `NodeReset`.
  - CONNECT never reaches the log: CONNACK always says `session_present = false`.
  - One live session per client id holds only within a gateway (`SessionTakenOver`).
  - The gateway refuses at CONNECT a 3.1.1 CleanSession=0, an MQTT 5 session expiry other than 0, and a will (D-138).
  - Client ids are at most 256 bytes (`codec::Limits::max_client_id_bytes`).
  - Bounds already global in `apply`: subscriptions (`max_subscriptions`), and queued inflight plus backlog bytes (`max_total_queued_bytes`).
- **Kinds:** `Session` (2), `Connect` (7) and `Disconnect` (8) are not written today; `apply` refuses them (`NotYet`). D-145 corrects the contract that lists them as written. Using them here is the separate decision D-143 named, and straum-cli is told first (done 2026-10-16).
- **unified-stream:**
  - §1: session state, inflight QoS 1 and takeover are effects of committed records.
  - §2: takeover through a log record, also for 3.1.1.
  - Invariant 7: at most one live session per client id in the cluster.
  - Time only through the log: expiry needs `SessionExpired`, out of scope here.

**Vision.** unified-stream §1, §2, invariant 7; CLAUDE.md §7 (bounded per session and globally), §8 (overflow policy), §12 (explicit lifecycle); D-138 (sessions first; wills and expiry stay refused); D-143 A's conditions; D-141 (contract changes told to straum-cli first).

### A. Client-id sessions through `Connect` and `Disconnect`, a count bound, and refusal at it — recommended
- **State:** `apply` gains `persisted: BTreeMap<ClientId, Persisted>`. Each entry holds subscriptions, the inflight and backlog it already keeps per `Origin`, its next delivery id, and `attached: Option<Origin>`. A clean session stays keyed by `Origin` as now.
- **Records:**
  - `Connect {origin, client_id, clean_start, persist}`: proposed by the gateway on CONNECT, before any CONNACK.
  - `Disconnect {origin}`: on a clean or broken disconnect of a persistent session.
  - `SessionGone` and `NodeReset` stay. For a persisted session they detach rather than drop.
- **Applying `Connect`, the same decision on every node:**
  1. A live session elsewhere with that client id is told `Close {SessionTakenOver}`. This is invariant 7 cluster-wide: the log orders two simultaneous connects, and the later one wins.
  2. `clean_start` drops any persisted state for the client id.
  3. `persist` with an existing entry attaches it: the effect is `Resumed {to, session_present: true, inflight}`, and the gateway resends the inflight messages with DUP in delivery order.
  4. `persist` with no entry creates one, **only if** `persisted.len() < max_persisted_sessions`. At the limit it is `Refused {to, reason: QuotaExceeded}`: CONNACK 0x97 in MQTT 5, 0x03 Server Unavailable in 3.1.1. No state is created, nothing is evicted, and the client may retry. Resuming an existing session never counts against the limit.
  5. Not `persist`: a clean session as today, plus the takeover in step 1.
- **The bound:** `max_persisted_sessions` is a new `apply::Limits` field. Its default comes from a measurement of what one entry costs; 10 000 is a placeholder until then. It is in the configuration digest, as every `apply` limit is.

  Per entry, bytes are already bounded: the client id is at most 256 bytes; subscriptions count against `max_subscriptions`; queued bytes count against `max_total_queued_bytes`. So the count bound makes the whole persisted set bounded, which goes into CLAUDE.md §7.
- **While detached:**
  - deliveries go to the backlog under the existing per-session bounds, and under a budget of their own for all detached sessions together: `max_detached_queued_bytes`, a new `apply::Limits` field inside `max_total_queued_bytes`. Abandoned sessions then can't fill the shared queued budget and get live QoS 1 subscribers closed by its overflow rule (`apply`, `deliver`);
  - an at-most-once message without room is dropped and counted, as now;
  - an at-least-once message without room drops the persisted session (`SessionDropped`, counted). It doesn't block the cluster or grow without bound. This is §8's existing rule for a full backlog (`apply`, `deliver`: a full backlog closes the session), applied to a detached session.
  - Why this loss is acceptable when C's eviction is not: the client can see it — its next CONNACK says `session_present = false` — and it follows from the session's own backlog overflowing, not from another client arriving.
- **Admission failure:**
  - `Connect` refused by `apply` (the limit): committed, so every node agrees; CONNACK refuses; nothing is held.
  - `Connect` not committed (`commit_timeout`, a full outbox, a sequencer change): the gateway closes the connection without a CONNACK, as for any proposal today. The client reconnects; nothing was applied.
  - `Connect` committed after the gateway gave up on the connection: the session is attached to a dead `Origin`. The gateway sees `Resumed`/`Attached` for a connection it no longer has and proposes `Disconnect {origin}`. Until that commits, deliveries go to the backlog, which is bounded. Tested in the gateway simulation.
  - A stale `Connect` and a newer live session: log order is not connect order, since a proposal can be held across a slot change (D-109 B). If the client gave up on gateway G1 and connected through G2, and G2's `Connect` commits first, G1's late `Connect` takes the session over from G2. G1 then sees no such connection and proposes `Disconnect`; the client on G2 is closed (`SessionTakenOver`) and reconnects. Accepted as a liveness cost, bounded by `commit_timeout` (the longest a proposal waits). No state is lost: the session moves, nothing is dropped. It gets a gateway-simulation scenario. A fence (a `Connect` that carries the attachment it expects to replace) is left for later, if the scenario shows the cost matters.
- **Every CONNECT waits for a commit**, clean ones too: without a quorum, a CONNECT gets no CONNACK and its connection is closed after `commit_timeout`, as a publish already is. Unified-stream §2 asks for this: takeover goes through the log, also for 3.1.1.
- **"Live" in invariant 7** is attachment in the committed state: one attached origin per client id. A gateway closes the old connection when it applies the takeover. Until then, that connection's proposals carry an origin no longer attached: `apply` refuses them, no PUBACK, and its deliveries go to the attached origin only, so nothing is delivered twice.
- **Restart:**
  - Persisted sessions are in the replicated state, so they survive any node's restart: snapshots carry `persisted`, and the snapshot format version is bumped.
  - A restarted gateway's `NodeReset` detaches every session attached to its old origins. Their clients reconnect to any node.
  - A restarted node that applies its log ends with the same `persisted` as every other node, checked by the state digest as now.
- **Partial write:**
  - A torn or failed write of a batch holding `Connect` either committed on a majority, so it applies everywhere, or it didn't, so it is nowhere.
  - The writing node stops on a log failure (`Event::Failed`) and its `NodeReset` on restart detaches its sessions.
  - No CONNACK is sent before commit, so no client is told "session present" about state that could vanish.
- **Out of scope, still refused:**
  - wills;
  - MQTT 5 session expiry other than 0 and 0xFFFFFFFF ("never"), which needs time through the log;
  - QoS 2;
  - removing a detached session except by a clean-start reconnect or the at-least-once overflow rule. An operator removal waits for the admin API (D-141 C).
- **Cost:** L over several tasks: `apply` (state, records, snapshot, digest), `gateway` (CONNACK after commit, resend, takeover across gateways), simulation scenarios (takeover race, restart while detached, commit after give-up, the limit), node and server tests, and a stable-contract update with straum-cli (`Connect`/`Disconnect` leave the reserved block).
- **Risks:**
  - CONNACK now waits one commit, about one round of the commit path.
  - With no expiry, a persistent session nobody reconnects to holds its slot for ever. At the limit, new persistent sessions are refused until someone frees space with a clean start. That is visible (a counter and the refusals), not silent.

### B. As A, but no persistent session until expiry exists
- **What we do:** reverses D-138 (5)'s order ("persistent sessions first; wills and session expiry refused until implemented"), so it needs that decision amended. Then: first `SessionExpired` through the log (time decided by the sequencer, unified-stream §2), then A with every persisted session carrying an expiry, so abandoned sessions free their slots.
- **Cost:** L + M. Expiry is the harder half: timers through the log, a sequencer change during expiry (unified-stream's open scenario).
- **Risk:** delays the first usable persistent session behind the hardest part of 7c.

**Discarded.**
- Persisted sessions kept per gateway, outside the log: violates unified-stream §1 and invariant 7.
- Evicting the session detached longest at the limit, instead of refusing: D-143 A asks for refusal at the limit, and eviction loses state a client was told was kept.

**Open for you, decided by choosing A:**
- a persistent session nobody reconnects to holds its slot under `max_persisted_sessions` until expiry exists;
- clean sessions pay one commit at CONNECT;
- a stale `Connect` may move a session away from a newer connection once, as above.

**Recommendation.** A. It bounds the persisted set by count, and by bytes through the existing bounds. It refuses at the limit, and every failure path follows from "nothing is acknowledged before it is committed". B if abandoned sessions filling the limit is unacceptable even for a first release; then expiry comes first.

**Answer:** `D-144: A` · `D-144: B` · `D-144: discuss`

## D-145. Correct the stable-kinds block: Session, Connect and Disconnect are not written

**Context.**
- D-141's `stable-kinds` block in `docs/configuration.md` lists `Session = 2`, `Connect = 7` and `Disconnect = 8` as kinds that are written. Nothing writes them: `apply` refuses each as `NotYet` (`crates/apply/tests/state.rs`).
- My mistake when the block was made. The test that pins it checks names and numbers, not whether a kind is written.
- straum-cli's checks read the block, and the straum-cli session has been told (2026-10-16).
- D-144 A would make two of them written.

**Vision.** D-141 (the contract changes only by a recorded decision, the CLI told first); CLAUDE.md "do not change public APIs silently".

### A. Move the three to `reserved-kinds` now — recommended
- **What we do:** the three move to the reserved block, keeping their numbers. The block's text says "written" only for what `apply` writes. The journal test pins a further rule: every stable kind other than `BatchHeader`, `SlotStart` and `SlotEnd` is one `apply::Proposal::kind()` returns.

  When D-144's records land, they move back to stable by that change, with straum-cli told.
- **Cost:** S; one commit; straum-cli updates its matrix.

### B. Leave them stable and let D-144 make them true
- **What we do:** nothing now; if D-144 A is chosen, `Connect` and `Disconnect` become written; `Session` would need a role or a correction then.
- **Risk:** the contract states something false until then, and the CLI may treat those kinds as supported.

**Recommendation.** A: the contract should say what is true today, and the new pinning rule stops this error recurring.

**Answer:** `D-145: A` · `D-145: B` · `D-145: discuss`
