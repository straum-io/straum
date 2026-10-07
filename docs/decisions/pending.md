# Pending decisions

Decision cards waiting for the user. Written only by the task-closeout
skill. When the user answers `D-<n>: <choice>`, the card moves to
`log.md` with the answer and is removed from here.

---



## D-67. How the new path keeps its log bounded on disk before state copy exists

**Context.** `node` never removes a segment (`crates/node/src/wal.rs`), so disk use grows with the log (CLAUDE.md §7; D-57 A rejected "unbounded disk"). It cannot simply call `Sequencer::compaction_floor` and `Segments::remove_through`. A node rebuilds `apply::State` from lsn 1 at every start (`crates/node/src/lib.rs`, the `Range` reads), and a removed segment would leave it nothing to rebuild from. Recovery memory is already bounded: the log is now verified record by record (`wal::start`).
**Why now.** This is a §7 invariant gap in step 4, which the vision review found. Every later step builds on this log.
**Vision.** unified-stream §1 (the state is a deterministic function of the committed log), §3 (segmented WAL, compaction); D-57 A; D-60 A (state copy at 7b); CLAUDE.md §7.

### A. A hard cap on the log until 7b — recommended
- **What we do:**
  - add `NodeConfig.max_log_bytes`, read against `Segments::sizes`;
  - at the cap minus a reserve, the sequencer refuses publishes and subscribes, and gateways close those publishers through `commit_timeout`;
  - lifecycle records (`Ack`, `SessionGone`, `NodeReset`) still commit into the reserve;
  - at the cap itself, everything is refused, with one `error` log line and a `Status.full` flag;
  - one test fills a small cap and checks that nothing grows past it, that lifecycle records commit in the reserve, and that restart rebuilds the state.
- **Result:** disk is bounded by `max_log_bytes` plus one segment. A full cluster stops taking publishes instead of growing.
- **Cost:** S.
- **Risks:** once the reserve is used up too, a full log is terminal until its data is wiped. Inflight is never released, and a restarted gateway's `NodeReset` never commits, so it accepts no connections. This is acceptable only while cluster mode is test-only (until step 7), and it is visible through `Status.full`.
- **Invariants:** CLAUDE.md §7 holds. Invariants 1, 3 and 5 are untouched because nothing is removed.
- **Reversibility:** easy. Step 7b replaces it with compaction plus snapshots.
- **Check:** `cargo test -p node`, with a new test `a_full_log_refuses_and_still_restarts`.
- **Fact / hypothesis:** fact.

### B. A local snapshot of the applied state now
- **What we do:**
  - `apply::State` gains `encode`/`decode` at a committed lsn;
  - the WAL thread writes the snapshot atomically before `remove_through(min(compaction_floor(bytes_floor), snapshot_lsn))`;
  - restart loads the snapshot, then the log after it;
  - pins on catch-up reads, as in `sim/tests/cluster.rs`.
- **Result:** D-57 A retention on the real disk, and restarts work after compaction.
- **Cost:** M–L: a snapshot format, its tests, and a `sim` scenario.
- **Risks:** this revises D-60 A, which put state copy after step 7 because the snapshot format changes again there (sessions, wills, expiry). The mitigation is a format version that refuses old snapshots.
- **Invariants:** 1 and 5 depend on the snapshot matching the log; checked by digest equality in `sim`.
- **Reversibility:** changes an on-disk format.
- **Check:** `cargo test -p node`, `cargo test -p apply`, and a new `sim` restart-after-compaction scenario.
- **Fact / hypothesis:** It is a hypothesis that the format survives step 7 without breaking changes.

### C. Defer to 7b, recorded as a known gap
- **When this is right:** if cluster mode stays test-only until 7b and no test runs long enough to matter.
- **Cost of waiting:** an unbounded-disk path exists in the code, against §7, recorded in ADR-0008.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | §7 holds; D-60 order kept | D-57 A in full; D-60 order broken | §7 gap stays |
| Risk to invariants | none | snapshot ↔ log | none now |
| Cost | S | M–L | none |
| Reversibility | easy | on-disk format | — |

**Recommendation.** A. It is the smallest change that makes §7 hold, and it keeps D-60 A's reason for doing state copy after step 7. Choose B if a long-running cluster test (a soak or a load generator) is wanted before step 7.
**What would change it.** A need to run the new path for longer than a full log allows.
**Rejected.**
- Compacting without a snapshot: a restarted node could not rebuild its state, against unified-stream §1.

**Answer:** `D-67: A` · `D-67: B, but …` · `D-67: discuss` · `D-67: defer`

## D-68. Where step 4's "configuration and its startup checks" go

**Context.** ADR-0008 step 4 lists "configuration and its startup checks (ADR-0006)". `node::NodeConfig` exists, but only tests build it. Nothing reads `STRAUM_*`, and the server refuses cluster mode until step 7 (ADR-0008, "Cluster mode is test-only until step 7"). The one startup check done is the `apply` limits digest at every link.
**Why now.** Step 4 cannot be called done while this is open and unrecorded.
**Vision.** ADR-0006 (cluster mode, majority commit only); ADR-0008 step plan; CLAUDE.md §8 (every limit has an env var).

### A. Move it to step 7, recorded in ADR-0008 — recommended
- **What we do:** ADR-0008's step plan moves "configuration and its startup checks" from step 4 to step 7, which is where the server stops refusing cluster mode. docs/configuration.md says so (it already does).
- **Result:** step 4 is done apart from D-67, and the env names are chosen once, when they become reachable.
- **Cost:** S (documentation).
- **Risks:** settings later in steps 5–6 change `NodeConfig`. That is cheaper before the settings are public.
- **Invariants:** none.
- **Reversibility:** easy.
- **Check:** ADR-0008 and docs/configuration.md agree.
- **Fact / hypothesis:** fact.

### B. Do it now
- **What we do:**
  - the `STRAUM_CLUSTER_*` variables (`NODE`, `PEERS`, `LISTEN`, `DATA`) and the `apply`/gateway limits are parsed into `NodeConfig` by a `node::config` module;
  - `server` gains an edge to `node` (a new row in the CLAUDE.md §2 table), but `runtime` does not, so the old path stays uncoupled (D-49 A);
  - startup checks for an odd voter count, this node in the voter list, unique addresses, and limits within the validation ranges;
  - the server still refuses to start in cluster mode.
- **Result:** configuration code and tests exist, but nothing reaches them.
- **Cost:** M.
- **Risks:** names and checks rewritten by steps 5–7.
- **Invariants:** none.
- **Reversibility:** changes the public configuration surface.
- **Check:** unit tests of the parsing and the checks.
- **Fact / hypothesis:** fact.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | matches "test-only until step 7" | configuration that nothing uses |
| Risk to invariants | none | none |
| Cost | S | M |
| Reversibility | easy | public surface |

**Recommendation.** A. Configuration that cannot be reached is configuration that gets written twice. Choose B if the server is to start a cluster before step 7, which the ADR forbids today.
**What would change it.** A decision to run the new path outside tests before step 7.
**Rejected.** none.

**Answer:** `D-68: A` · `D-68: B, but …` · `D-68: discuss` · `D-68: defer`

## D-69. The next step after D-67 and D-68

**Context.** With D-67 and D-68 settled, step 4 is done and test-only. Step 5 (ungraceful takeover) is next in the plan. Smaller step-4 items are listed in ADR-0008 "Not yet in step 4":
- proposals are dropped when the sequencer's queue is full;
- `auth` is not wired;
- metrics are not exported;
- `Status.digest` is on the loop;
- there is no Linux `storage/commit` baseline with the batch hash.

**Why now.** A sequencer that stops stops the cluster. Until step 5 there is no fencing.
**Vision.** unified-stream §1 (a change of sequencer is a committed record), §5 invariants 2 and 6; ADR-0008 step plan; D-58 A.

### A. Step 5: ungraceful takeover — recommended
- **What we do:**
  - durable promises, with fsync before the reply;
  - the slot table and log truncation in `cluster`;
  - a sequencer restart becomes a takeover, after which the sequencer may ship a batch in parallel with its sync (D-58 A);
  - `sim/tests/cluster.rs` gains partitions and sequencer loss, and checks invariants 2 and 6;
  - `node` gains a test that stops the sequencer and checks that another node takes the slot.

  Truncation runs on the log as D-67 leaves it.
- **Result:** the cluster commits again after its sequencer dies. In `sim`, no committed record is lost or changed across takeovers, over 1000 seeds.
- **Cost:** L.
- **Risks:** split brain under partition. The `sim` checks for invariants 2 and 6 catch it at every step.
- **Invariants:** 2 and 6 are introduced. 1, 3 and 5 must hold through truncation.
- **Reversibility:** a protocol change, plus record kinds for the slot table. It is reversible while the path is test-only.
- **Check:**
  - `cargo test -p sim --test cluster`;
  - `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim`;
  - `cargo test -p node`.
- **Fact / hypothesis:** It is a fact that there is no takeover today. The size is an estimate.

### B. The step-4 leftovers first
- **What we do:**
  - hold proposals the sequencer refuses, instead of dropping them, in a bounded queue that stops reading links and clients when it is full;
  - wire `auth` into CONNECT;
  - export the `node` metrics (CLAUDE.md §15);
  - take the test hooks off the loop: `Status.digest`, and the per-record effects clone, which is already skipped when no test listens;
  - add an e2e load generator over `node`, with a container baseline (preliminary).
- **Result:** the new path checks credentials, can be observed, has a preliminary end-to-end number, and drops nothing at the sequencer.
- **Cost:** M.
- **Risks:** low. Container numbers are preliminary (§11).
- **Invariants:** CLAUDE.md §8 "nothing dropped" is restored for proposals.
- **Reversibility:** easy.
- **Check:**
  - `cargo test -p node`, with new tests for wrong credentials and a full sequencer queue;
  - a baseline section in `docs/baselines/`.
- **Fact / hypothesis:** It is a fact that these items are missing. It is a hypothesis that an early end-to-end number guides steps 5–7.

### C. Defer
- **When this is right:** if the old path needs work first.
- **Cost of waiting:** two paths are maintained side by side for longer.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | next in the plan; removes the single point of failure | closes gaps, nothing new in the protocol | none |
| Risk to invariants | high, caught by `sim` | low | none |
| Cost | L | M | none |
| Reversibility | protocol, test-only | easy | — |

**Recommendation.** A. B's items are not observable while cluster mode is test-only, and steps 5–7 change the gateway lifecycle that `auth` and metrics hook into. Choose B if you want an end-to-end number from the new path before its protocol grows.
**What would change it.** A container measurement could show the node loop or the batch hash costing more than the old path on the same workload. Then B's baseline comes first.
**Rejected.**
- Step 7 before step 5: no fencing before step 5 means effects outside the log (§1, ADR-0008).

**Answer:** `D-69: A` · `D-69: B, but …` · `D-69: discuss` · `D-69: defer`
