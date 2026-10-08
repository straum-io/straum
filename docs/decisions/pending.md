# Pending decisions

Decision cards waiting for the user. Written only by the task-closeout
skill. When the user answers `D-<n>: <choice>`, the card moves to
`log.md` with the answer and is removed from here.

---




## D-70. The next step after step 5

**Context.** Steps 1–5 of ADR-0008 are done, but only in tests.

What the evidence covers:
- `cluster::Voter` passes 200 000 seeds of the takeover simulation, with invariants 1, 2, 3, 5 and 6.
- The node takes a slot over on real sockets and files.

What it does not cover:
- The simulations that compact (`sim/tests/cluster.rs`) and that run gateways (`apply.rs`, `gateway.rs`) still use a fixed owner.
- Takeover under compaction, and gateways forwarding to an owner that changes, are covered only by the node's loopback tests.

ADR-0008 lists step 6 (graceful handoff) next.
**Why now.** Step 6, and step 7 even more so, build on takeover. unified-stream §5 asks for "compaction during catch-up" and session takeover across gateways during a change of sequencer, and neither has run in simulation yet.
**Vision.** unified-stream §1 (correctness is shown by deterministic simulation), §5; ADR-0008 step plan.

### A. Harden step 5 in simulation first — recommended
- **What we do:**
  - `sim/tests/cluster.rs` runs three `Voter`s instead of a fixed owner, with sequencer crashes, partitions and compaction during catch-up and during takeover (invariants 1, 2, 3, 5 and 6, and the D-57 A floor checks);
  - `sim/tests/apply.rs` and `gateway.rs` run on `Voter`s, so gateways forward to an owner that changes; invariant 4 and per-session order must hold across takeovers;
  - nothing else. The step-4 test hooks (`Status.digest`, the effects clone) stay for a later card.
- **Result (done when):** all three simulations pass 1000 seeds with takeovers counted, at least one per hundred steps, and compactions during a takeover counted.
- **Cost:** M.
- **Risks:** a new protocol bug, which is the point. It is fixed with its seed, as in step 5.
- **Invariants:** none change; more of them are checked across takeovers.
- **Reversibility:** easy (tests only).
- **Check:** `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim`.
- **Fact / hypothesis:** It is a fact that these paths are untested in simulation. It is a hypothesis that bugs remain there; step 5's simulation found eight.

### B. Step 6: graceful handoff
- **What we do:**
  - `SlotEnd(k, last_lsn, hash)` is written by a sequencer that fences itself; its successor writes `SlotStart(k+1)` without a promise round;
  - planned rotation is optional, off by default (§2.4);
  - the takeover simulation gains crashes before, during and after `SlotEnd`.
- **Result:** maintenance without a timeout.
- **Cost:** M.
- **Risks:** it builds on paths A would have checked first. The open risks above stay open.
- **Invariants:** 1, 2, 3 and 6.
- **Reversibility:** a new record kind is used (reserved since step 1).
- **Check:** `cargo test -p sim --test takeover`, plus new scenarios.
- **Fact / hypothesis:** fact.

### C. Defer
- **When this is right:** if the old path needs work first.
- **Cost of waiting:** two paths to maintain for longer.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | §1, §5 scenarios not yet run | next in the ADR order | none |
| Risk to invariants | finds, does not add | adds on unchecked ground | none |
| Cost | M | M | none |
| Reversibility | easy | record kind in use | — |

**Recommendation.** A. Step 5's simulation found eight protocol bugs in paths that looked finished. The paths still outside simulation, compaction and gateways under takeover, are the ones every later step uses. Choose B if a planned failover for maintenance is needed soon.
**What would change it.** A requirement for planned failover before anything else.
**Rejected.**
- Step 7 before step 6: it reverses ADR-0008's order (unified-stream phase 3 before phase 2) with no concrete reason, while step 6 is unblocked.

**Answer:** `D-70: A` · `D-70: B, but …` · `D-70: discuss` · `D-70: defer`

## D-71. When the sequencer ships before its own sync (D-58)

**Context.** D-58 A: "sync, then ship, until step 5; step 5 … only then lifts the restriction". Its rejected option B was "sync-then-ship permanently".

Step 5 made it possible: a restarted node never leads its old slot again, so a sequencer that lost an unsynced tail cannot reuse its lsns. But step 5 did not take it. The sequencer still ships only what is locally durable (ADR-0008 step 5, "Restart").
**Why now.** Without a decision, the code stays on D-58's rejected option B by default. Shipping in parallel saves one local sync per batch on the commit latency path.
**Vision.** unified-stream §2.3 (commit = majority durable), §7 (no performance claim without a Linux measurement); D-58.

### A. Defer it to unified-stream phase 5 (Linux performance), amending D-58 A — recommended
- **What we do:** D-58 A is amended in log.md and ADR-0008. The parallel ship becomes a phase-5 candidate, taken only if a Linux measurement shows the commit latency gain. Today's behaviour stays.
- **Result:** the deferral is recorded, and no rejected option is applied silently.
- **Cost:** S (documentation).
- **Risks:** none now.
- **Invariants:** none.
- **Reversibility:** easy.
- **Check:** log.md and ADR-0008 agree.
- **Fact / hypothesis:** It is a hypothesis that the gain matters; unmeasured.

### B. Take it now
- **What we do:**
  - the sequencer ships a batch as soon as it is handed to the log; its own durability counts toward commit only after its sync;
  - the takeover simulation gains a power loss of the sequencer after shipping but before syncing;
  - a container measurement, which is preliminary.
- **Result:** one sync off the commit path, in principle.
- **Cost:** M.
- **Risks:** a sequencer that loses an unsynced tail its replicas hold. Takeover makes this safe in theory; the simulation would show it.
- **Invariants:** 1 and 3 under that power loss.
- **Reversibility:** easy behind a setting.
- **Check:** takeover sim at 200 000 seeds, plus the new fault.
- **Fact / hypothesis:** It is a hypothesis that it is faster; §7 forbids claiming so without Linux.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §7: measure first | §2.3 as designed |
| Risk to invariants | none | new fault to prove |
| Cost | S | M |
| Reversibility | easy | easy |

**Recommendation.** A. The gain is a latency claim that only a Linux host can make (§7), and phase 5 is where those measurements are taken. Choose B if commit latency on the new path becomes the bottleneck before then.
**What would change it.** A Linux measurement showing the local sync dominating commit latency.
**Rejected.**
- Sync-then-ship forever without a decision: this is D-58's rejected option B.

**Answer:** `D-71: A` · `D-71: B` · `D-71: discuss` · `D-71: defer`
