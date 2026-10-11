# Pending decisions

Decision cards waiting for the user. Written only by the task-closeout
skill. When the user answers `D-<n>: <choice>`, the card moves to
`log.md` with the answer and is removed from here.

---

## D-109. A publish made while the slot changes hands is dropped

**Context.** The end-to-end test (D-106) finds it every run: the first QoS 1 publish after the operator's handoff gets no PUBACK.
- **What the node does:** a proposal reaching a node while no one sequences, or forwarded to a sequencer that has just stepped down, is dropped, not held (ADR-0008, "Not yet in step 4"). Nothing uncommitted is ever acknowledged.
- **What the client sees:** a silence of up to `commit_timeout` (30 s), after which the gateway closes the connection; an at-least-once client then reconnects and resends.
- **In the test:** the resend is acknowledged on the second attempt.

It is safe, but every handoff — planned, as an operator's command — costs publishers up to 30 s.
**Vision.** unified-stream §2.4 (planned handoff); CLAUDE.md §8 (today: "the proposal is dropped, never acknowledged; `commit_timeout` closes its publisher"); §7 (bounded memory); D-102 A.

### A. Pass the queue on at the handoff — recommended
- **What we do:** the outgoing sequencer forwards the proposals it still holds, in order, to the successor with the handoff — the successor is known then, from the schedule. A proposal reaching a node while no successor is known yet is dropped as today. The drop at a planned handoff is fixed at its source, in the handoff protocol.
- **Requirement:** held proposals stay within the gateway's existing budgets (`max_pending`, `max_total_pending_bytes`, §7); one older than `commit_timeout` is dropped as today.
- **Duplicates:** a proposal forwarded once and re-forwarded may commit twice. For a QoS 1 publish that is a legitimate duplicate — at-least-once allows it, and the client's own resend already can — so no dedup key is added; lifecycle proposals are already idempotent.
- **Cost:** M. The handoff path and the node loop; the end-to-end test then asserts the first publish after a handoff is acknowledged without a resend.
- **Check:** that test; the takeover, MQTT and apply simulations.

### B. Hold proposals at the node across a slot change, bounded
- **What we do:** each node keeps what it could not hand to a sequencer, in order, within the same budgets, and re-forwards it once a later slot's sequencer is known. Covers takeovers by silence too, not only planned handoffs.
- **Duplicates:** as in A.
- **Cost:** M.
- **Risks:** a buffer per node, bounded by the gateway's budgets.

### C. Close the publisher's connection at once when its proposal is dropped
- **What we do:** the gateway learns of the drop and closes the connection with a reason, so the client reconnects and resends immediately, not after 30 s.
- **Cost:** S.
- **Risks:** a burst of reconnects at each handoff.

### D. Keep it; document it
- **Cost:** none.
- **Risks:** up to 30 s of silence per publisher at every handoff.

**Recommendation.** A; C as a cheap first step if A waits.

**Answer:** `D-109: A` · `D-109: B` · `D-109: C` · `D-109: D` · `D-109: discuss`

## D-110. A design for credentials and TLS on the new path

**Context.** D-107 asks for the bounds of acceptable use and a design.
- **The bounds, now in place:** loopback by default; any address beyond it refused at startup unless `STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1`; an isolated, trusted network only.
- **What already exists:**
  - the old path has client credentials (`auth`: password file, ACL) and client TLS (`transport`, rustls);
  - the new path has neither;
  - its links are plain TCP whose HELLO carries a version and a configuration digest — integrity of intent, not authentication.
**Vision.** CLAUDE.md scope (TLS optional), §2 (`auth`, `transport`); D-102 A; unified-stream §1.

### A. Clients first, then mutual TLS between nodes — recommended
- **What we do:**
  1. **Clients:** `node` checks CONNECT credentials with the `auth` crate (its password file and ACL format) and serves TLS through `transport`. The ACL is checked only at the gateway's ingress — CONNECT, PUBLISH, SUBSCRIBE — never at apply or delivery, so nodes with different ACL files still apply the same log the same way; a refusal filters input before it reaches the log and decides nothing in shared state (§1: a node is a function of the committed log and its input).
  2. **Links:** mutual TLS with a cluster CA; each node's certificate names its id, checked against the HELLO.
  3. The unauthenticated-network flag then applies only to what still lacks either.
- **Cost:** M for 1, M–L for 2.
- **Risks:**
  - certificate handling for every node;
  - TLS on links costs throughput, unmeasured until Linux.

### B. Clients as A; links authenticated by a cluster secret
- **What we do:** HELLO becomes a challenge and response under a shared key (HMAC), with no encryption on links.
- **Cost:** M; a new dependency for HMAC (§2 asks for a reason).
- **Risks:** link traffic stays readable; one secret for the whole cluster.

### C. Clients as A; links left to the network (WireGuard, IPsec, a private VLAN)
- **What we do:** document that links require a protected network, and keep the flag for them.
- **Cost:** S beyond A.
- **Risks:** the broker depends on an operator getting the network right.

**Recommendation.** A, in that order: clients first, since that is what an untrusted network reaches first.

**Answer:** `D-110: A` · `D-110: B` · `D-110: C` · `D-110: discuss`

## D-112. What to rebuild on the new path after step 8

**Context.** Step 8 (D-111) removed, with the old broker:
- a single-node mode — the new path needs three voters;
- metrics;
- kernel probes at startup;
- client TLS and credentials (D-110);
- ADR-0006's durability levels.

Your order after D-106–D-108 is the rest of 7a, then measurements on a Linux host.
**Vision.** unified-stream §1 (one mechanism; the log decides), §6 phases; D-102 A; D-111.

### A. Metrics before Linux measurements, the rest after — recommended
- **What we do:**
  - export `node::Status` and `gateway::Load` (commit, applied, slot, leading, pending, dropped, takeovers) in the Prometheus text format, written by hand as before;
  - opt-in address, loopback by default (D-107).
  - Linux measurements need them to say what happened.
- **Cost:** S–M.

### B. A cluster of one first
- **What we do:** allow one voter (commit = the local sync), as step 8 planned; a single host can run Straum again.
- **Cost:** M; the startup rule "at least three voters" becomes "one, or an odd number ≥ 3", with the simulation covering one.
- **Risks:** it reopens D-104 A.

### C. Neither before 7a's remaining parts (7b state copy, 7c sessions)
- **Cost:** none now.
- **Risks:** Linux measurements without metrics see only what the benchmarks print.

**Recommendation.** A.

**Answer:** `D-112: A` · `D-112: B` · `D-112: C` · `D-112: discuss`

## D-113. Closing D-108: the unstable node start

**Context.** One run in the Linux container failed two node tests at a node's start; the run kept no error text.
- **Since then:** 24 container runs and every host run passed. The tests now report `Node::start`'s error.
- **One candidate removed:** the bind-and-release port race in the test helper. Each test binary now takes ports from a block of its own, numbered by a counter.
- **The cause is not established.**
**Vision.** D-108 (diagnose; never mask by retries); CLAUDE.md §17.

### A. Keep it open with a long container loop that keeps the logs — recommended
- **What we do:** 100 runs of the node tests in the container, keeping the full output of any failure.
- **Outcome:** a failure with its error decides the fix; none, and D-108 closes as not reproduced after the helper fix.
- **Cost:** S, about 90 minutes of machine time.

### B. Close it now as not reproduced after the helper fix
- **Cost:** none.
- **Risks:** a cause in node startup, if there is one, waits for the next failure. Its error text will now be kept.

**Recommendation.** A.

**Answer:** `D-113: A` · `D-113: B` · `D-113: discuss`

## D-114. The architecture document after step 8

**Context.** `docs/architecture/unified-stream.md` (level 1, yours) still describes the old path as existing:
- line 5: durable leader/follower replication "is implemented";
- §6, phase 0: a harness "for the existing leader/follower replication".

Both the replication and the harness were deleted at step 8 (D-111). ADR-0008 records the deviation, but the level-1 text now contradicts the tree.
**Vision.** CLAUDE.md "Sources, in order of precedence" (level 1 decides; the lower is corrected); D-111.

### A. Amend unified-stream.md to say what was removed and when — recommended
- **What we do:** you edit it, or approve the edit: the intro and §6 phase 0 say the old path existed until step 8 (D-111), and point to ADR-0008.
- **Cost:** XS.

### B. Leave it; ADR-0008's record suffices
- **Risks:** the highest-ranked document disagrees with the code.

**Recommendation.** A.

**Answer:** `D-114: A` · `D-114: B` · `D-114: discuss`
