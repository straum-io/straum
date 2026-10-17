# Pending decisions

Decision cards waiting for the user. Written only by the task-closeout
skill. When the user answers `D-<n>: <choice>`, the card moves to
`log.md` with the answer and is removed from here.

---


## D-146. Are D-144's four definitions agreed, and is 7c authorised?

**Context.** D-144 A withheld implementation until four things were defined. They are in ADR-0008, "Step 7c design (D-144 A)". A first draft was revised after the guardian review: its fence did not survive an entry's removal, and re-proposing a stale `Connect` brought back the late takeover.
1. **Atomicity:**
   - each record is one deterministic transition in `apply`;
   - one origin's records reach the log in proposal order, at most once;
   - `Disconnect(O)` acts only if `O` is attached;
   - records from an origin that is not attached are refused and never acknowledged;
   - invariant A: the attachment is the origin of the last `Connect` that passed the fence, unless that origin was since detached or its session deleted.
2. **The order:**
   - a `Connect` wins only if its proposer had applied every earlier change to that client id: `seen >=` its last change (`connected_at`, a tombstone, or the global floor), and `seen` within `W` positions;
   - any other `Connect` is refused at once, with no re-proposal;
   - tombstones are bounded by `max_connect_tombstones`; the floor rises when they overflow;
   - invariant B: no `Connect` changes a client id's state if a change to it committed after its `seen`.
3. **Overflow:**
   - attaching never overflows, since the detached budget is inside the total;
   - at most once: that message alone is dropped;
   - at least once, or a detachment the budget cannot take: the session is deleted whole, leaving a tombstone;
   - CONNACK has four distinct outcomes: stale (0x88 / 0x03), resumed (`session_present = 1`), new (`= 0`), at the limit (0x97 / 0x03).
4. **Recovery:** the commit rule and the takeover rule, both tested; the cluster simulation's invariant 1; limits stated, including D-57 A's lost replica until 7b.

**Open for you, decided by choosing A or B:**
- **Invariant 7:** "at most one live session per client id" (unified-stream §5) is met over committed state — one attached origin. A lagging gateway may hold a taken-over connection open until it applies the takeover, but nothing that connection proposes is committed in its name.
- **The winner:** when an older CONNECT's `Connect` commits after a newer one, the newer always wins. The older is refused, and its client is told to reconnect.
- **The fence's cost:** a legitimate reconnect through a gateway that has not yet applied that client id's last change, or whose `Connect` commits more than `W` positions after `seen`, is refused once, and the client reconnects. `W` and `max_connect_tombstones` are sized in stage 2.

**Vision.** D-144 A (implementation only once the four are agreed); unified-stream §1, §2, invariant 7; rules.md §7 (the tombstones bounded), §8, §12; D-138 (5); D-141 (straum-cli told before `Connect`/`Disconnect` leave the reserved block).

### A. Agree the revised four; authorise stages 1–2, then decide again — recommended
- **What we do:**
  1. `apply`: the client-id state, `Connect`/`Disconnect`, the fence (tombstones, floor, `W`), the limit, the detached budget, the refusal of records from detached origins, the snapshot. Rule tests, plus invariants A and B as property tests.
  2. The simulation: the nine scenarios in the ADR, with invariants A and B checked after every record; the stale-refusal rate measured to size `W` and `max_connect_tombstones`.

  Then a card with what the simulation found, before the gateway changes. The gateway's refusal at CONNECT stays until then, so nothing is user-visible.
- **Cost:** M now.

### B. Agree the revised four; authorise all of 7c in stages
- **What we do:** A's stages, then `gateway` (CONNACK from the applied `Connect`, stale refusal, resend of inflight on resume, `Disconnect` for a closed connection), then `node`/`server` end to end and the contract update with straum-cli. One task per stage, each closed out and merged.
- **Cost:** L.
- **Risk:** the fence's sizing is decided before the simulation has measured it.

### C. A simpler fence: the global floor only
- **What we do:** as A, but no tombstones. A `Connect` with no entry passes only if `seen >=` the lsn of the last removal of any entry.
- **Cost:** less code, one u64.
- **Risk:** every clean disconnect raises the floor, so under normal churn most connects through a gateway even slightly behind are refused once. Likely unusable at load; unmeasured.

**Discarded.**
- Re-proposing a stale `Connect` transparently: it re-arms an old CONNECT packet and can take a session over from a newer connection, which D-144 (2) excludes.
- Unbounded tombstones: violates rules.md §7.

**Recommendation.** A. The fence is sound on paper and bounded; stages 1–2 let the simulation show invariants A and B hold and size it, before any client depends on it. B if you'd rather not take a decision between stages. C only if stage 2 shows tombstones aren't worth their memory.

**Answer:** `D-146: A` · `D-146: B` · `D-146: C` · `D-146: discuss`

## D-147. Correct the opening of unified-stream.md: is leader/follower replication still "implemented"?

**Context.** `docs/architecture/unified-stream.md:3` says "durable leader/follower replication implemented". D-111 (2026-10-11) removed the old leader/follower path completely; the server now runs only the experimental gateway/sequencer cluster path (ADR-0008, D-102 A). Found by `grep -n 'leader/follower' docs/architecture/unified-stream.md` during the move to straum-io.
**Why now.** The opening is level-1/3 text (the "Sources" order): a reader, or a reviewer checking against it, is told a removed path exists. Constitution §14.1 forbids presenting stale current state as fact.
**Vision.** unified-stream §1 (the log is the only source of truth), §7 (the owner decides the document's text); ADR-0008; D-111; D-102 A.

### A. Replace the clause with the current state — recommended
- **What we do:** in `docs/architecture/unified-stream.md:3`, replace "durable leader/follower replication implemented" with "a gateway/sequencer cluster that commits by majority (ADR-0008), experimental and not production-ready (D-102 A); the old leader/follower path was removed at step 8 (D-111)". Nothing else in the document changes.
- **Result:** `grep -n 'replication implemented' docs/architecture/unified-stream.md` finds nothing; the opening matches `docs/architecture/current-implementation.md`.
- **Cost:** S: one sentence.
- **Risks:** the step-0 line (`unified-stream.md:121`) still describes the old harness → it is a historical plan step, left as is.
- **Invariants:** none; text only.
- **Contracts:** none.
- **Cross-repo:** none.
- **Reversibility:** easy.
- **Check:** the grep above; read the opening against ADR-0008 and D-111.
- **Fact / hypothesis:** fact: the clause and D-111.

### B. Delete the clause without a replacement
- **What we do:** drop "durable leader/follower replication implemented" from line 3.
- **Result:** no false statement; the opening no longer says what replicates the log.
- **Cost:** S.
- **Risks:** the opening under-describes the system → `current-implementation.md` carries it.
- **Invariants:** none.
- **Contracts:** none.
- **Cross-repo:** none.
- **Reversibility:** easy.
- **Check:** as A.
- **Fact / hypothesis:** fact.

### C. Do nothing / defer
- **When this is right:** if unified-stream.md is to be rewritten after 7c, and the line goes with it.
- **Cost of waiting:** the highest-ranked document states something false about the code.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | full | partial | none |
| Risk to invariants | none | none | none |
| Cost | S | S | none |
| Reversibility | easy | easy | easy |

**Recommendation.** A, because the opening is what readers take as the summary of the system. B if the owner wants the opening to describe direction only, not state.
**What would change it.** A decision to bring back a leader/follower mode.
**Rejected.**
- Correct it inside the migration PR without a card — violates unified-stream §7 (the owner decides this document's text; M-8 A).

**Answer:** `D-147: A` · `D-147: B, but …` · `D-147: discuss` · `D-147: defer`
