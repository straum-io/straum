# Decision cards, full text

Appended when a card leaves pending.md (decision-options skill). D-55 and
earlier: cards-to-D-55.md.

````markdown
## D-51. Что станет со старым путём на шаге 8

**Контекст.** ADR-0006 (D-36 A): одиночный узел сохраняет уровни `memory`…`fsynced`, кластер — только commit большинством. ADR-0008 ставит вопрос на шаге 8. **Вариант A отменяет принятые решения:** D-36 A («уровни остаются для одиночного узла») и черновик D-42 A («одиночный узел остаётся поставляемым режимом и после фазы 3, с уровнями»); в D-42 даже отсеян вариант «исключить одиночный узел — противоречит D-36 A». Нового факта нет — есть новое направление D-48 C (переписать под единый поток), и только оно. Отдельно: сегодняшние пользователи пары лидер/фолловер с `quorum` (два узла) в новом пути получают кластер из двух узлов с данными плюс witness (§2.5) — без witness безопасного перехода нет.
**Почему сейчас.** Не срочно — нужно к шагу 8; от ответа зависит, сколько старого кода поддерживать по пути.
**Связь с видением.** unified-stream §1, ADR-0006.

### A. Одиночный узел — кластер из одного на новом пути; старый путь удалить — рекомендую
- **Что делаем:** на шаге 8 одиночный узел работает как кластер из одного (commit = локальный fsync, доставка после commit); `memory` и `written` уходят; ADR-0006 и решения D-36 A, D-42 пересматриваются; пара лидер/фолловер с `quorum` до появления witness (фаза 4 §6) остаётся на старом пути — шаг 8 удаляет старый путь только после неё.
- **Результат:** один путь, §1 без исключений.
- **Цена:** M на шаге 8.
- **Риски:** доставка на одиночном узле медленнее на один sync (~4 мс на Mac, предварительно) → меняется поведение для пользователей `memory`.
- **Инварианты:** инвариант 4 везде.
- **Обратимость:** трудно.
- **Проверка:** вся матрица и бенчмарки durability.
- **Факт / гипотеза:** стоимость — оценка по Mac.

### B. Оставить старый путь как одиночный режим
- **Что делаем:** как в ADR-0006.
- **Результат:** одиночные пользователи ничего не теряют.
- **Цена:** поддержка двух путей.
- **Риски:** два брокера в одном продукте надолго.
- **Инварианты:** отступление ADR-0006 остаётся.
- **Обратимость:** легко.
- **Проверка:** —.
- **Факт / гипотеза:** —.

**Сравнение**

| | A | B |
|---|---|---|
| Соответствие видению | §1 полностью | отступление ADR-0006 |
| Риск для инвариантов | нет | нет |
| Цена | M | поддержка двух путей |
| Обратимость | трудно | легко |

**Рекомендация.** A — иначе переписывание оставит старую модель жить рядом.
**Что изменит рекомендацию.** Пользователь, которому нужна доставка без sync на одном узле, → B.
**Отсеяно.** —

**Ответ:** `D-51: A` · `D-51: B`

---

## D-53. What follows ADR-0008 step 1

Step 1 is done but not wired: `journal` (record v4 + batch chain) is tested,
and nothing writes it yet. ADR-0008 step 2 bundles two independent pieces: a
segmented WAL with segment refcounts in `storage`, and the sans-IO
sequencer/replica for three nodes. All options keep: the old path untouched
(D-49 A); cluster mode reachable only from `sim` and tests, refused by the
server configuration (ADR-0008, "test-only until step 7"); D-47 ("lost" under
compaction) re-decided as a card inside step 2 (ADR-0008 Consequences).
Commits are separate and wait for the user (D-52 withdrawn, D-55 A).

### A. Step 2 as written, in one piece
- **What we do:** segmented WAL with refcount + sequencer and replica (star, majority commit, fixed slot owner, writing `journal` v4 batches) + 3-node `sim` checking invariants 1, 3, 5 and compaction during catch-up.
- **Result:** a 3-node commit path in simulation at the end of one step.
- **Cost:** L.
- **Risks:** one large diff against unified-stream §7 "small verifiable steps"; a defect in segments surfaces through the 3-node sim, where it is harder to isolate.
- **Invariants:** 1, 3, 5.
- **Reversibility:** medium (new code beside the old path).
- **Check:** `sim` 3 nodes, 1000-seed sweep; `storage/commit` before/after (Linux for accepted numbers; macOS preliminary).
- **Fact / hypothesis:** the size is an estimate.

### B. Split step 2 into 2a and 2b — recommended
- **What we do:** 2a: segmented WAL with refcount in `storage`, `sim` scenario "compaction never truncates a segment being read", the existing single-node and leader/follower tests unchanged, `storage/commit` unchanged within noise. 2b: sequencer + replica + 3-node `sim` on top of 2a.
- **Result:** each half verifiable alone; 2a has no dependency on 2b.
- **Cost:** M + M.
- **Risks:** 2a's segment API may need a change once 2b uses it.
- **Invariants:** 2a: 5 (one log) and bounded memory; 2b: 1, 3, 5.
- **Reversibility:** easy per half.
- **Check:** as A, per half.
- **Fact / hypothesis:** —.

### C. Record the `storage/commit` Linux baseline first, then B
- **What we do:** run `storage/commit` on a quiet Linux host now, while the commit path is unchanged, and record it in `docs/baselines/`; then B.
- **Result:** a clean "before" for D-50's check (the hash enters the hot path at 2b).
- **Cost:** S, plus access to a Linux host.
- **Risks:** none in code; blocked if no Linux host is available (then the baseline is the macOS one, marked preliminary).
- **Invariants:** —.
- **Reversibility:** —.
- **Check:** 5 alternating rounds, medians.
- **Fact / hypothesis:** —.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | §7 weaker | §7 | §7, §11 |
| Risk to invariants | medium | low | low |
| Cost | L | M+M | S + B |
| Reversibility | medium | easy | easy |

**Recommendation.** B; C instead if a Linux host is available now, because a "before" taken after 2a is no longer clean.
**What would change it.** If the segment API cannot be designed without the replica's read pattern, A.
**Rejected.** Wire `journal` v4 into the existing single-node path before step 2: contradicts D-49 A (old path stays until step 8) and puts the hash on the old hot path without a baseline.

**Answer:** `D-53: A` · `D-53: B` · `D-53: C`

---

## D-54. Where `prev_hash` lives: per batch or per record

Unified-stream §2.2 says every WAL record carries `(slot, lsn, prev_hash)`.
Step 1 puts `(slot, lsn)` on every record and `prev_hash` once per commit
batch, in the header (ADR-0008 Progress, "Interpretation").

### A. Per batch, as built — recommended
- **What we do:** confirm; record it as an interpretation of §2.2 in ADR-0008 (already written there).
- **Result:** 16 bytes per batch; a record is bound by its batch's hash.
- **Cost:** 0.
- **Risks:** a reader that wants to verify one record alone needs its whole batch.
- **Invariants:** 5 — any changed byte is refused (property test).
- **Reversibility:** a format change later (record version 5).
- **Check:** `a_changed_byte_breaks_the_chain`.
- **Fact / hypothesis:** fact (tested).

### B. Per record
- **What we do:** each record carries the hash of the previous record.
- **Result:** literal §2.2; per-record verification.
- **Cost:** M; +16 bytes per record, one hash per record on the commit path.
- **Risks:** hot-path cost without a detection gain over A.
- **Invariants:** 5.
- **Reversibility:** format change.
- **Check:** same tests plus `storage/commit`.
- **Fact / hypothesis:** the cost is a hypothesis until measured.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §2.2 by interpretation | §2.2 literally |
| Risk to invariants | none | none |
| Cost | 0 | M |
| Reversibility | format version | format version |

**Recommendation.** A.
**What would change it.** A requirement to verify single records outside their batch.
**Rejected.** —

**Answer:** `D-54: A` · `D-54: B`

---

````

````markdown
## D-56. Correction to D-54: what unified-stream §2.2 says about `prev_hash`

**Context.** The D-54 card called option B, a hash per record, "literally §2.2". §2.2 says two things: "every WAL record carries `(slot, lsn, prev_hash)`" *and* "`prev_hash` is computed over the group-commit batch, not per message". B satisfies the first and contradicts the second. A third reading satisfies both, and the card did not offer it: every record carries the hash of the *previous batch*. B is built as chosen (`crates/journal`, pinned format hash).
**Why now.** The format is not written by the running server yet; after step 4 a change means record version 5.
**Vision.** unified-stream §2.2; CLAUDE.md "every deviation from unified-stream.md is recorded in an ADR".

### A. Every record carries the previous batch's hash — recommended
- **What we do:** `journal`: the record header keeps its `prev_hash` field, but it holds the hash of the previous *batch*; the batch hash covers all the batch's record bytes; `Verifier` checks every record against the previous batch's hash. Tests and the pinned value updated; `sim` unchanged in shape.
- **Result:** both sentences of §2.2 hold, with no deviation to record.
- **Cost:** S (one crate, its tests, ADR-0008 text).
- **Risks:** a record alone is bound only to the history before its batch; within the batch, the batch hash binds it → covered by `a_changed_byte_breaks_the_chain`.
- **Invariants:** 5.
- **Reversibility:** easy now.
- **Check:** `cargo test -p journal`, `sim` journal and cluster sweeps.
- **Fact / hypothesis:** fact (the text of §2.2).

### B. Keep the hash per record, recorded as a deviation from §2.2
- **What we do:** ADR-0008 records that `prev_hash` is computed per record, contrary to §2.2's "per batch, not per message", and why.
- **Result:** as built.
- **Cost:** S (documentation).
- **Risks:** one xxh3 finalisation per record on the commit path, cost unmeasured until step 4.
- **Invariants:** 5.
- **Reversibility:** a format version after step 4.
- **Check:** `storage/commit` at step 4.
- **Fact / hypothesis:** the cost is a hypothesis.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §2.2 whole | deviation from §2.2 |
| Risk to invariants | none | none |
| Cost | S | S |
| Reversibility | easy | format version later |

**Recommendation.** A: it is what §2.2 says in full, and my card should have offered it.
**What would change it.** A need to verify one record without its batch → B.
**Rejected.** Per batch, header only (the original A of D-54): contradicts "every record carries prev_hash".

**Answer:** `D-56: A` · `D-56: B`

---

## D-57. What the log keeps for a replica that falls behind (re-reads the withdrawn D-47)

**Context.** A replica whose next record has been compacted away cannot be caught up from the log; it needs a copy of the state, which no step builds yet. In step 2 the sequencer may compact up to its commit point, and a catch-up that stalls for a heartbeat period gives up its pin; the 1000-seed sweep lost 1 306 replicas this way. With both replicas lost, the cluster stops committing (checked).
**Why now.** It decides the sequencer's compaction floor before step 4 makes it real, and whether "lost" is an outcome or a defect (unified-stream §5.1, invariant 1 "never lost"). The step-2 simulation already uses one definition: a node that compacted a record after it was committed counts as holding it (`crates/sim/tests/cluster.rs`, invariant 1). A and B keep that check; another definition changes it.
**Vision.** unified-stream §3 (refcount), §5 (invariant 1, compaction during catch-up); CLAUDE.md §7 (bounded memory and disk).

### A. Keep for the slowest live replica, up to a byte bound — recommended
- **What we do:** floor = min(commit, slowest known replica's durable), but never more than `STRAUM_WAL_RETAIN_BYTES` behind the end; a replica beyond that is lost and counted (as D-18 did on the old path). A compacted record below every replica's position is consumed, not lost (U1 above the floor).
- **Result:** a briefly slow or restarted replica is caught up; disk stays bounded.
- **Cost:** M (sequencer exposes the floor, sim policy, a test per path, docs/configuration.md).
- **Risks:** a long partition still loses a replica → `replication_followers_lost` and an error log.
- **Invariants:** 1 (as above the floor), memory/disk bound.
- **Reversibility:** easy.
- **Check:** sim: no replica lost while within the bound; lost only beyond it.
- **Fact / hypothesis:** —.

### B. As built: compact to the commit point, lost replicas wait for state copies
- **What we do:** nothing now. This is today's state: the 1000-seed sweep lost 1 306 replicas that were only briefly behind, and with both replicas lost the cluster stops committing until a replica is restored from a copy of the state.
- **Result:** the log never holds more than the uncommitted tail; a lost replica needs a state copy, which ADR-0008 has no step for yet — choosing B means adding one (a snapshot step before step 4).
- **Cost:** 0 now; a new step later.
- **Risks:** availability: a short partition can cost a replica.
- **Invariants:** 1 (as above the floor).
- **Reversibility:** easy.
- **Check:** the existing sim.
- **Fact / hypothesis:** the loss count is from the sweep, whose compaction policy is random up to the commit point.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §3, §5, §7 | §3, §5 |
| Risk to invariants | none | none; availability |
| Cost | M | 0 now, a snapshot step later |
| Reversibility | easy | easy |

**Recommendation.** A.
**What would change it.** A snapshot step scheduled before step 4 → B.
**Rejected.** Never compact past a replica — a dead replica holds the disk forever (CLAUDE.md §7).

**Answer:** `D-57: A` · `D-57: B`

---

````

````markdown
## D-58. When the sequencer may ship a batch: after its own sync, or in parallel

**Context.** Step 2 ships only what is durable on the sequencer, so commit latency is two syncs in a row (sequencer, then a replica) instead of the slower of two in parallel. Shipping before the local sync is safe only if a restarted sequencer first learns the majority's tail and takes the slot over (§2.4 ungraceful), or it can overwrite records a majority committed.
**Why now.** It shapes step 5 and the latency that step 4 will first measure.
**Vision.** unified-stream §2.3, §2.4; ADR-0005.

### A. Keep sync-then-ship until step 5, then ship in parallel behind the takeover — recommended
- **What we do:** nothing now; step 5 makes a sequencer restart a new slot with promises, and only then lifts the restriction, with `sim` checking invariants 1, 2, 6.
- **Result:** correct now, faster after step 5.
- **Cost:** part of step 5.
- **Risks:** step 4 numbers show the serial latency → recorded as such.
- **Invariants:** 1.
- **Reversibility:** easy.
- **Check:** step 4 e2e latency, step 5 sim.
- **Fact / hypothesis:** the gain (about one sync) is a hypothesis until step 4.

### B. Keep sync-then-ship permanently
- **What we do:** never ship before the local sync.
- **Result:** simpler takeover; one extra sync of latency per commit forever.
- **Cost:** 0.
- **Risks:** latency.
- **Invariants:** 1, as now.
- **Reversibility:** easy: step 5 can still lift it.
- **Check:** step 4 e2e latency.
- **Fact / hypothesis:** the cost (about one sync per commit) is a hypothesis until step 4.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §2.3, §2.4 | §2.3 |
| Risk to invariants | none | none |
| Cost | in step 5 | 0 |
| Reversibility | easy | easy |

**Recommendation.** A.
**What would change it.** Step 4 measurements showing the second sync is not on the critical path → B.
**Rejected.** Ship in parallel now: a sequencer restarting after a power loss could overwrite committed records (invariant 1) until step 5 exists.

**Answer:** `D-58: A` · `D-58: B`

---

## D-59. The next step

**Context.** ADR-0008 steps 1 and 2 are done in simulation. Step 3 is the MQTT 5 codec model and the protocol contract, then the broker as the apply function of committed records and the sans-IO gateway (proposals, PUBACK on commit, delivery after commit, a bound on proposals awaiting commit in messages and bytes). Under D-49 A all of it goes into **new crates beside** `mqtt`, `protocol` and `broker`; the old path is not touched.
**Why now.** Nothing applies committed records yet; invariant 4 waits for step 3. D-56 A and D-57 A are built; the record format and the retention rule are settled.
**Vision.** unified-stream §2.1, §2.3, §2.7; ADR-0003, ADR-0005; D-49 A.

### A. Step 3 split: 3a codec model, 3b apply function and gateway — recommended
- **What we do:** 3a: a new crate (`codec`) with one internal packet model for 3.1.1 and 5 — properties, reason codes, reason strings — and the new contract types it hands to the gateway; proptests and a fuzz target; `mqtt`/`protocol` untouched. 3b: new crates `gateway` (sans-IO: sessions, proposals, PUBACK on commit, delivery after commit, the bounds) and the apply function over `cluster` commits; `sim`: invariant 4, and invariant 7 within one gateway.
- **Result:** each half verifiable on its own; the old path and its tests unchanged.
- **Cost:** L + L.
- **Risks:** two codecs live side by side until step 8 → both under the conformance tests; the new one gains them in 3a.
- **Invariants:** 3a: malformed input never panics, no allocation from an unchecked length; 3b: 4, 7 (one gateway), bounded memory.
- **Reversibility:** easy (new crates).
- **Check:** codec proptests and fuzzing; `sim` scenarios for 3b; e2e of the old path unchanged.
- **Fact / hypothesis:** —.

### B. Step 3 in one piece, same new crates
- **What we do:** the codec, the contract, the apply function and the gateway in one change.
- **Result:** as A at the end.
- **Cost:** 2L at once.
- **Risks:** a fault in the codec surfaces through the gateway simulation, harder to isolate.
- **Invariants:** as A.
- **Reversibility:** easy (new crates).
- **Check:** as A.
- **Fact / hypothesis:** —.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §7 small steps | §7 weaker |
| Risk to invariants | low | medium |
| Cost | L+L | 2L |
| Reversibility | easy | easy |

**Recommendation.** A.
**What would change it.** —.
**Rejected.** Changing `mqtt`/`protocol` in place for MQTT 5 — contradicts D-49 A. Starting step 4 before step 3 — the driver would carry a broker that applies nothing.

**Answer:** `D-59: A` · `D-59: B`

---

## D-60. How a lost replica comes back

**Context.** Under D-57 A a replica that falls further behind than the byte bound is lost: the log no longer holds what it needs next, and nothing in ADR-0008 can give it a copy of the state. The 1000-seed sweep lost 1 180 replicas this way (bound 5 000 bytes in the simulation). With two of three lost, the cluster stops committing (checked). A lost replica still counts as a voter: with one lost, one more failure stops commit.
**Why now.** Step 4 makes clusters real; without a way back, an operator's only remedy for a lost replica is undocumented.
**Vision.** unified-stream §2.5, §3, §6 (phase 4: metadata and membership); CLAUDE.md §7.

### A. A state-copy step in ADR-0008, after step 7 — recommended
- **What we do:** add a step "state copy". Once the floor has passed a replica, the segments it needs are gone, so the copy is a snapshot of the *applied* state at a committed lsn — a deterministic function of the committed log (§1) — plus the log after that lsn; the replica installs both and resumes with `Chain::resume` at that lsn. After step 7, not step 3: step 3 applies publishes and subscriptions only, while sessions, expiry and wills enter the log at step 7; a snapshot format fixed earlier would change again.
- **Result:** a lost replica rejoins without operator file surgery.
- **Cost:** L.
- **Risks:** copying while compaction runs → the copy pins the segments it reads (the refcount already exists).
- **Invariants:** 1, 3, 5 on the copied log.
- **Reversibility:** easy (new step).
- **Check:** sim: a lost replica comes back and the cluster recommits with it.
- **Fact / hypothesis:** —.

### B. Operator procedure only, until phase 4
- **What we do:** document "stop the replica, delete its log, copy the sequencer's segment directory, start"; test it in e2e at step 4.
- **Result:** a way back, by hand.
- **Cost:** S.
- **Risks:** copying a live directory can tear the last segment → recovery cuts the torn batch (tested).
- **Invariants:** as A, if the copy is a prefix.
- **Reversibility:** easy.
- **Check:** an e2e at step 4.
- **Fact / hypothesis:** that recovery handles a copied live directory is a hypothesis until tested.

### C. Nothing until phase 4
- **What we do:** nothing now; a lost replica stays lost and is replaced through the metadata log when membership changes are built (§2.5, §6 phase 4).
- **Result:** no cost now.
- **Cost:** 0 now.
- **Risks:** until phase 4, a lost replica is a voter that never votes: one more failure stops commit.
- **Invariants:** none touched; availability only.
- **Reversibility:** easy.
- **Check:** —.
- **Fact / hypothesis:** —.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | §3, §6 | §7 operability, manual | §6 order |
| Risk to invariants | none | none if prefix | none |
| Cost | L | S | 0 |
| Reversibility | easy | easy | easy |

**Recommendation.** A, with B documented at step 4 as the interim way.
**What would change it.** A requirement that clusters run unattended before step 7 → A earlier.
**Rejected.** Never compacting past a replica — rejected in D-57.

**Answer:** `D-60: A` · `D-60: B` · `D-60: C`
````

````markdown
## D-61. Revise D-59 A: move the contract types from 3a to 3b?

**Context.** D-59 A, as accepted, put in 3a "a new codec crate … and the new contract types it hands to the gateway". I built the codec and left the contract types out — a reversal of part of an accepted decision, made inside the task; it needs your ruling. ADR-0008 records 3a as done except these types. The protocol-neutral types between the gateway and the apply function — what a proposal and a committed effect look like, with no MQTT vocabulary (unified-stream §2.1, the old `protocol` crate's role) — were not written: they have no user before 3b.
**Why now.** 3b starts from it.
**Vision.** unified-stream §2.1, §2.7; ADR-0003 ("the broker stays protocol-neutral").

### A. Revise D-59 A: define them in 3b, with the gateway that uses them — recommended
- **What we do:** 3b writes the gateway, the apply function and, between them, the types: proposals as `journal` entries, committed effects as deliveries and acknowledgements with reason codes. Shaped by their first two users at once.
- **Result:** no type without a user (CLAUDE.md §14).
- **Cost:** part of 3b.
- **Risks:** 3b is larger → it stays one reviewable change, or is split again at the card.
- **Invariants:** —.
- **Reversibility:** easy.
- **Check:** 3b's simulation.
- **Fact / hypothesis:** —.

### B. Keep D-59 A: define them now, as 3a's remainder
- **What we do:** a contract module before the gateway exists, as D-59 A says.
- **Result:** the card's text, literally.
- **Cost:** S now, likely reshaped in 3b.
- **Risks:** speculative types (CLAUDE.md §14: start concrete).
- **Invariants:** —.
- **Reversibility:** easy.
- **Check:** compiles; nothing uses it yet.
- **Fact / hypothesis:** that 3b would reshape them is a hypothesis.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §14 concrete first | D-59 A as accepted; types before their first user (§14) |
| Risk to invariants | none | none |
| Cost | in 3b | S now |
| Reversibility | easy | easy |

**Recommendation.** A.
**What would change it.** A second consumer of the contract before 3b → B.
**Rejected.** —

**Answer:** `D-61: A` · `D-61: B`

---

## D-62. The next step

**Context.** Steps 1, 2 and 3a are done. 3b is the gateway (sessions within one gateway, proposals, PUBACK on commit, delivery after commit, the bounds in messages and bytes) and the broker as the apply function of committed records, with `sim` checking invariant 4 and one live session per client id within a gateway. The codec's cost against `mqtt` is unmeasured.
**Why now.** Nothing applies committed records yet.
**Vision.** unified-stream §2.1, §2.3, §5 (invariant 4); ADR-0005.

### A. Step 3b now
- **What we do:** new crates `gateway` (sans-IO) and the apply function, over `codec`, `journal` and `cluster`; `sim` drives MQTT clients through the codec against three nodes.
- **Result:** a publish goes client → gateway → sequencer → majority → apply → PUBACK and delivery, in simulation.
- **Cost:** L.
- **Risks:** a large step → its simulation scenarios land with it.
- **Invariants:** 4; 7 within one gateway; bounded memory.
- **Reversibility:** easy (new crates).
- **Check:** `sim` sweep; unit tests per rule.
- **Fact / hypothesis:** —.

### B. First an encode/decode benchmark of `codec` against `mqtt`
- **What we do:** divan benches on the same 3.1.1 packets, allocation counts; macOS preliminary, Linux for numbers.
- **Result:** a baseline before anything is built on the codec.
- **Cost:** S.
- **Risks:** none in code.
- **Invariants:** —.
- **Reversibility:** —.
- **Check:** `cargo bench -p codec`.
- **Fact / hypothesis:** whether the scratch buffers matter is the hypothesis it tests.

### C. Split 3b: first the apply function and the contract, then the gateway — recommended with D-61 A
- **What we do:** 3b-1: the apply function and the protocol-neutral contract over committed `journal` records, in `sim` against three nodes, no MQTT — invariant 4 with abstract publishers and subscribers. 3b-2: the gateway over `codec`, sessions within one gateway, the bounds in messages and bytes, invariant 7 within a gateway.
- **Result:** the commit-to-effect path is checked before MQTT is on top of it; each half reviewable alone.
- **Cost:** M + M.
- **Risks:** 3b-1's contract may need reshaping once the gateway uses it → it is the first user, so less than defining it with no user.
- **Invariants:** 3b-1: 4; 3b-2: 4, 7 (one gateway), bounded memory.
- **Reversibility:** easy (new crates).
- **Check:** `sim` sweeps per half.
- **Fact / hypothesis:** —.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | §6 order | §11 measure first | §7 small steps |
| Risk to invariants | none | none | none |
| Cost | L | S | M+M |
| Reversibility | easy | — | easy |

**Recommendation.** C if D-61 is answered A (it makes 3b larger); otherwise A. B is needed before step 4 puts the codec on the hot path, not before 3b.
**What would change it.** A plan to optimise the codec's shape during 3b → B first.
**Rejected.** —

**Answer:** `D-62: A` · `D-62: B` · `D-62: C`
````

````markdown
## D-63. The next step

**Context.** Steps 1, 2, 3a and 3b-1 are done. 3b-2 is the sans-IO gateway over `codec`: MQTT connections in, proposals to the sequencer, `apply` effects back out as CONNACK/PUBACK/SUBACK/UNSUBACK and deliveries; one live session per client id within a gateway; bounds on proposals awaiting commit per publisher and in total, in messages and in bytes. The codec's cost against `mqtt` is still unmeasured.
**Why now.** Until 3b-2 no MQTT client reaches the new path.
**Vision.** unified-stream §2.1, §2.3; ADR-0003, ADR-0005; CLAUDE.md §7, §8.

### A. Step 3b-2 now — recommended
- **What we do:** new crate `gateway` (sans-IO): per-connection protocol state on `codec` (keep-alive deadlines from a passed-in instant, topic aliases, receive maximum), proposals with bounds, effects to packets. Session close goes through the log: when a connection ends, the gateway proposes a `SessionGone` record (kind 3, reserved), and `apply` drops that origin's subscriptions and inflight state, so closing a session removes them on every node (CLAUDE.md §7). `apply` bounds its own state from the log, so every node refuses the same way: subscriptions per origin and retained messages in count and bytes; a subscribe or retained publish beyond a bound is applied as a refusal (SUBACK failure, publish delivered but not retained, as the old path does). `sim` drives MQTT 3.1.1 and 5 clients through `codec` against three nodes: invariant 4 at the PUBACK and delivery packets, invariant 7 within a gateway, session close, the bounds.
- **Result:** an MQTT publish goes through the new path end to end in simulation.
- **Cost:** L.
- **Risks:** the size of the step → the D-64 answer decides whether inflight state is in it.
- **Invariants:** 4, 7 (one gateway), bounded memory, malformed input never panics.
- **Reversibility:** easy (new crate).
- **Check:** unit tests per rule, `sim` sweep, the codec fuzz target.
- **Fact / hypothesis:** —.

### B. First the codec benchmark against `mqtt`
- **What we do:** divan benches on the same 3.1.1 packets, allocation counts.
- **Result:** a baseline before the gateway is built on the codec.
- **Cost:** S.
- **Risks:** none in code.
- **Invariants:** —.
- **Reversibility:** —.
- **Check:** `cargo bench -p codec`.
- **Fact / hypothesis:** whether the scratch buffers matter is what it tests.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §6 order | §11 measure first |
| Risk to invariants | none | none |
| Cost | L | S |
| Reversibility | easy | — |

**Recommendation.** A; B before step 4, which puts the codec on the hot path.
**What would change it.** A plan to change the codec's shape in 3b-2 → B first.
**Rejected.** —

**Answer:** `D-63: A` · `D-63: B`

---

## D-64. Where QoS 1 inflight state lives in 3b-2

**Context.** A QoS 1 delivery stays unacknowledged until the subscriber's PUBACK. ADR-0003 says "inflight QoS 1 is replicated", and unified-stream §1 lists it among the state that is the application of committed records. ADR-0008 put sessions in the log at step 7, but did not say where inflight state lives before that. 3b-2 needs an answer to deliver QoS 1 at all.
**Why now.** It decides 3b-2's size and whether it deviates from ADR-0003.
**Vision.** unified-stream §1; ADR-0003; ADR-0005.

### A. Through the log now: Ack records applied by `apply` — recommended
- **What we do:** the gateway proposes an `Ack` record (kind 6, reserved) for each subscriber PUBACK; `apply` keeps inflight per `Origin` (delivery lsn → message), adds on delivery, removes on committed Ack. Bounds, decided in `apply` from the log so every node agrees: at most `STRAUM_MAX_INFLIGHT` inflight per origin, further at-least-once deliveries wait in a per-origin backlog bounded in messages and bytes, and a delivery that finds the backlog full is applied as a `Close(SlowConsumer)` effect for that origin — the old path's overflow rule (CLAUDE.md §8), now the effect of a committed record. A closed origin's inflight and backlog go with its `SessionGone` record (D-63 A). A gateway that takes over a session after step 7 resends from that state.
- **Result:** no inflight state outside the log; nothing to migrate at step 7.
- **Cost:** M on top of 3b-2.
- **Risks:** one log record per subscriber acknowledgement → log volume grows with fan-out; measured at step 4.
- **Invariants:** §1 whole; bounded memory.
- **Reversibility:** a record kind in the format, kept.
- **Check:** `sim`: a QoS 1 delivery stays inflight on every node until its Ack commits; reference model extended.
- **Fact / hypothesis:** the log-volume cost is a hypothesis until step 4.

### B. Local to the gateway until step 7, recorded as a deviation
- **What we do:** the gateway tracks inflight itself; ADR-0008 records the deviation from ADR-0003 until step 7 moves it into the log.
- **Result:** smaller 3b-2.
- **Cost:** S now, M at step 7.
- **Risks:** a gateway restart loses inflight state — acceptable only while cluster mode is test-only (until step 7).
- **Invariants:** §1 deviated from, by ADR.
- **Reversibility:** easy.
- **Check:** `sim` within one gateway.
- **Fact / hypothesis:** —.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §1, ADR-0003 | deviation until step 7 |
| Risk to invariants | none | inflight lost on gateway restart |
| Cost | M | S now, M later |
| Reversibility | format kind kept | easy |

**Recommendation.** A: the log is the only source of truth, and inflight state is named in ADR-0003.
**What would change it.** A measured log-volume cost that rules out an Ack record per delivery → B, and a batched-ack design at step 7.
**Rejected.** —

**Answer:** `D-64: A` · `D-64: B`
````

````markdown
## D-65. Carry MQTT 5 message properties through the log

**Context.** A v5 publisher's user properties, content type, response topic, correlation data and payload format indicator are decoded by `codec` and then dropped: `apply::Proposal::Publish` has no field for them, so subscribers never see them. ADR-0003 asks for "an extensible properties section" in the message record from the start, and `journal` records have one, unused.
**Why now.** Request/response over MQTT 5 (response topic, correlation data) does not work on the new path until it is done; the record format is cheapest to settle before step 4.
**Vision.** ADR-0003; unified-stream §2.7.

### A. Pass them through, opaque to `apply` — recommended
- **What we do:** the gateway writes a publish's message properties into the record's properties section (`journal::encode_property`, MQTT 5 ids, as `journal` already defines); `apply` carries the bytes into `Effect::Deliver` without reading them (fan-out shares them); the gateway turns them back into `codec` properties for v5 subscribers and drops them for 3.1.1. Message expiry is carried but not enforced: expiry is decided through the log by the sequencer at step 7.
- **Result:** v5 request/response works end to end; `apply` stays protocol-neutral.
- **Cost:** M (gateway both ways, `apply` field, tests, sim check that properties arrive unchanged).
- **Risks:** properties count toward the bounds → payload byte bounds include them.
- **Invariants:** none changed; fan-out without copies.
- **Reversibility:** easy.
- **Check:** gateway rule test; MQTT simulation compares properties sent and received.
- **Fact / hypothesis:** —.

### B. Defer to phase 6 (MQTT 5 extensions)
- **What we do:** nothing; record the gap in ADR-0008 as it is now.
- **Result:** smaller step 4.
- **Cost:** 0 now.
- **Risks:** v5 clients that rely on properties misbehave on the new path.
- **Invariants:** —.
- **Reversibility:** easy.
- **Check:** —.
- **Fact / hypothesis:** —.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | ADR-0003 | ADR-0003 deferred |
| Risk to invariants | none | none |
| Cost | M | 0 |
| Reversibility | easy | easy |

**Recommendation.** A.
**What would change it.** A decision to keep step 4 minimal → B, done right after.
**Rejected.** —

**Answer:** `D-65: A` · `D-65: B`

---

## D-66. The next step

**Context.** Steps 1–3 are done in simulation: log format, three-node replication, codec, apply function, gateway. Nothing runs in the server yet. Step 4 is the runtime driver: inter-node links over TCP, forwarding (FIFO, nothing across a restart — the two transport requirements the simulations found), the WAL on disk, configuration and its startup checks, e2e tests; cluster mode stays refused by the server's configuration until step 7. None of `codec`, `apply` or `gateway` has been measured.
**Why now.** The rest of the plan needs a cluster that runs.
**Vision.** unified-stream §4, §6; ADR-0004, ADR-0006, ADR-0008; CLAUDE.md §11.

### A. Step 4, starting with baselines — recommended
- **What we do:** every node's `apply` limits compared at link setup, a mismatch refusing the link; first the benchmarks the new code needs before it is on a hot path: `codec` decode/encode against `mqtt` on the same 3.1.1 packets, `apply` per publish and per fan-out, `gateway` per packet — allocations counted, macOS preliminary, Linux numbers recorded in `docs/baselines/`. Then the driver: tokio tasks for links, sequencer, replica, gateway and WAL writer; e2e tests with `rumqttc` against a three-node cluster on loopback.
- **Result:** a cluster that runs, measured from its first commit.
- **Cost:** L.
- **Risks:** large → split at the card into 4a (links and replication on disk) and 4b (gateway over TCP).
- **Invariants:** all, now outside the simulation.
- **Reversibility:** easy (cluster mode refused by configuration).
- **Check:** benchmarks, e2e, the simulations unchanged.
- **Fact / hypothesis:** —.

### B. Step 5 first (takeover with promises), still in simulation
- **What we do:** slot changes, promises, truncation by the slot table; invariants 2 and 6 in `sim`.
- **Result:** failover proven before anything runs.
- **Cost:** L.
- **Risks:** more design without a running system to check it against.
- **Invariants:** 2, 6.
- **Reversibility:** easy.
- **Check:** `sim` sweeps.
- **Fact / hypothesis:** —.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | §6 order, §11 measure | §5 simulation first |
| Risk to invariants | low | none |
| Cost | L | L |
| Reversibility | easy | easy |

**Recommendation.** A, with D-65 A folded in first if chosen (it changes the record format).
**What would change it.** A requirement for failover before anything runs → B.
**Rejected.** Wiring the new path into the server's default configuration before step 7: cluster mode would have effects outside the log (ADR-0008).

**Answer:** `D-66: A` · `D-66: B`
````

````markdown
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
````

````markdown
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
````

````markdown
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
````

````markdown
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
````

````markdown
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
````

````markdown
## D-75. What keeps the old replication path, and how it leaves

**Context.** The graph that graphify built (`graphify-out/`) showed `codec::Sender` bridging the runtime, node, gateway and old replication, and a cycle between `e2e/benches/durability.rs` and `runtime`. Both are name-resolution artifacts.
- `mpsc`, `watch`, `oneshot` and `broadcast` senders in `runtime`, `node` and `storage` were matched to the only symbol named `Sender`, which is codec's two-variant enum.
- `pub use domain::Durability` in `runtime/src/lib.rs` was matched to the file `durability.rs`.

No new-path crate depends on the old one (`scripts/check-boundaries.sh`); `e2e` uses `runtime` only as a dev-dependency, through its public re-exports. The old replication (about 5 800 lines: `crates/replication`, `runtime/src/replication.rs`, the leader/follower simulation, `replication_matrix.rs`, `replicate.rs`) stays because ADR-0008 step 8 and D-51 A keep it until a single node runs on the new path and a witness exists for the two-node pair.
**Options offered.** A, follow ADR-0008 as planned; B, drop the two-node `quorum` mode now (reverses D-51 A); C, fix the analysis tooling: ignore `graphify-out/`, and record that graphify resolves symbols by name.

**Answer:** `D-75: A + C's .gitignore`, with conditions.
````

````markdown
## D-74. The next step

**Context.** Step 5 runs in every simulation (D-70 A), with D-72 and D-73 open.

Still waiting:
- step 6, graceful handoff with `SlotEnd`;
- the step-4 test hooks that D-70 A left for "a later card" (`Status.digest` hashes the whole state on every change; the effects broadcast clones every applied record when a test listens);
- auth wiring;
- metrics.
**Why now.** D-70 A's reason for putting step 6 second, "it builds on paths not yet simulated", no longer applies: takeover under compaction and gateways now runs in simulation. D-72 and D-73 are still open.
**Vision.** ADR-0008 step plan; unified-stream §2.4 (graceful handoff); CLAUDE.md §10 (nothing accidental on the hot path).

### A. D-73 first, then step 6 — recommended
- **What we do:**
  - D-73 as decided;
  - then step 6: `SlotEnd(k, last_lsn, hash)` from a fencing sequencer, and the successor writes `SlotStart(k+1)` without a promise round;
  - planned rotation, off by default;
  - the takeover simulation gains crashes before, during and after `SlotEnd`.
- **Result:** maintenance without a timeout, on a slot table that rotation cannot leave incomplete.
- **Cost:** the chosen D-73 option (S–M for D-73 A) plus M for step 6.
- **Risks:** rotation multiplies slots, so it depends on D-73.
- **Invariants:** 1, 2, 3 and 6.
- **Reversibility:** a record kind reserved since step 1.
- **Check:** `cargo test -p sim --test takeover`, new scenarios, all sweeps.
- **Fact / hypothesis:** fact.

### B. The step-4 hooks and leftovers first
- **What we do:**
  - take `Status.digest` and the effects clone off the node loop, computing them on demand;
  - hold refused proposals instead of dropping them;
  - wire `auth` into CONNECT;
  - export the `node` metrics;
  - add an end-to-end load generator with a container baseline (preliminary).
- **Result:** the new path is measurable and observable.
- **Cost:** M.
- **Risks:** low.
- **Invariants:** CLAUDE.md §8, "nothing dropped", restored for proposals.
- **Reversibility:** easy.
- **Check:** `cargo test -p node`; a baseline in `docs/baselines/`.
- **Fact / hypothesis:** It is a hypothesis that early numbers guide steps 6–7.

### C. Defer
- **When this is right:** if the old path needs work first, or D-72 and D-73 should settle before anything else.
- **Cost of waiting:** two paths maintained side by side for longer, and the D-57 A deviation stays open.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | ADR order, gap closed first | hot-path hygiene | none |
| Risk to invariants | new handoff path, simulated | low | none |
| Cost | D-73 + M | M | none |
| Reversibility | record kind | easy | — |

**Recommendation.** A. D-73 is a small fix to a known gap that rotation would widen, and step 6 is next in the plan. Choose B if you want end-to-end numbers before the protocol grows further.
**What would change it.** A need for a measured baseline of the new path before more protocol.
**Rejected.**
- Step 7 before step 6: it reverses the ADR-0008 order with no reason (D-70).

**Answer:** `D-74: A` · `D-74: B` · `D-74: discuss` · `D-74: defer`
````

````markdown
## D-73. Repairing "unplaceable" replicas (the open D-57 A deviation)

**Context.** After compaction, a node rebuilds its slot table from the records that are left, and it no longer knows where older slots began. A replica whose log holds records past the kept start, in a slot the sequencer no longer knows, cannot be placed. Placing it would take a guess, and a guess cut committed copies before (cluster simulation, seed 9046). So it is lost inside the byte bound, which deviates from D-57 A: 488 of 941 losses in the cluster sweep. The simulation exempts these and counts them (crates/sim/tests/cluster.rs, the `unplaceable` check). Liveness only.
**Why now.** Step 6 (planned rotation) adds slots and makes this more frequent. Until it is fixed, D-57 A's "lost only past the byte bound" does not hold.
**Vision.** unified-stream §1 (no state outside the log), §3 (compaction must not lose a replica that is only behind); D-57 A; D-67 B.

### A. The slot table travels in the snapshot — recommended
- **What we do:**
  - `apply::State`'s snapshot (D-67 B: versioned, checksummed, refused when damaged, taken at a committed lsn) gains the node's slot table up to that lsn, which is derived only from committed `SlotStart` records;
  - restart merges it with the table rebuilt from the records after the snapshot. Compaction already stops at the snapshot, so the merged table is complete;
  - the snapshot format version goes to 2, and version 1 is refused;
  - the cluster simulation's `unplaceable` exemption is removed.
- **Result:** no unplaceable losses, and D-57 A checked in full, with no new file. A 10 000-seed sweep shows 0 unplaceable losses.
- **Cost:** S–M: a snapshot field, its version, recovery, and tests.
- **Risks:** a table that diverges from the log. Ruled out by construction: it is written from committed records only, at the snapshot's lsn, and checked against the log after it at restart.
- **Invariants:** D-57 A is restored; §1 holds on the same footing as the D-67 B snapshot.
- **Reversibility:** a snapshot format version.
- **Check:**
  - `cargo test -p apply` (round-trip with the table);
  - `STRAUM_SIM_SEEDS=10000` for cluster and apply with the exemption removed.
- **Fact / hypothesis:** fact.

### B. A slot table file, derived from committed `SlotStart`s
- **What we do:**
  - the node appends `(slot, first lsn)` to a small versioned file when a `SlotStart` commits;
  - the file is refused like the D-67 B snapshot if its version or checksum is wrong;
  - recovery reads it before the log;
  - the simulation's exemption is removed.
- **Result:** as A, without keeping segments.
- **Cost:** M: a new on-disk format, its recovery and tests, recorded in ADR-0008.
- **Risks:** one more file to keep consistent, though it is derived from committed records like the snapshot. A covers the same ground without it.
- **Invariants:** D-57 A is restored.
- **Reversibility:** an on-disk format.
- **Check:** as A, plus a file-damage test.
- **Fact / hypothesis:** fact.

### C. The sequencer answers with the slot boundaries it knows
- **What we do:** the repair messages carry the sequencer's slot table near the meet point, so the replica places itself.
- **Result:** fixes placement only when the sequencer still knows the boundary. A boundary that was compacted away stays unknown.
- **Cost:** M (a wire change).
- **Risks:** an incomplete fix.
- **Invariants:** D-57 A is restored only partly.
- **Reversibility:** a wire format.
- **Check:** the sweep's unplaceable count.
- **Fact / hypothesis:** It is a hypothesis that it covers most cases; it does not cover all.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Fit with vision | in the snapshot, like the state | a derived file | partial |
| Risk to invariants | none | one more file | none |
| Cost | S–M | M | M |
| Reversibility | snapshot version | format | wire |

**Recommendation.** A. The snapshot already is the log-derived record that compaction relies on, so the slot table belongs with it, and no new file is needed. Choose B if snapshots are to stay pure application state.
**What would change it.** A requirement that the snapshot hold only `apply`'s state.
**Rejected.**
- Compaction keeps each slot's first segment: segments go only from the front, so keeping one keeps every later one, and under a stable sequencer the log would never compact (CLAUDE.md §7; D-57's rejected "never compacting past a replica").
- Keeping the exemption permanently: it contradicts D-57 A without a decision.

**Answer:** `D-73: A` · `D-73: B` · `D-73: C` · `D-73: discuss` · `D-73: defer`
````

````markdown
## D-76. Old-path e2e test that fails in the Linux container

**Context.** `e2e/tests/replication_matrix.rs::a_follower_that_stops_reading_is_cut_off_and_caught_up` fails 2 of 3 isolated runs in `scripts/linux.sh` with "a follower that stopped reading was never cut off". It passes on macOS.
- Nothing under e2e, runtime, replication or storage/src changed in step 6, so the failure is not caused by it. Whether it fails at BASE ffeda02 has not been run.
- Separately, one full container run had one unnamed failure; two later full runs were clean (477 passed).
- The test covers the CLAUDE.md §8 row "Follower stops reading for `STRAUM_WRITE_STALL_TIMEOUT_MS`". D-75 freezes the old path to bug fixes.
**Vision.** CLAUDE.md §8: every path in the table has a test. §13: Linux is production.

### A. Find out, then fix — recommended
- **What we do:**
  1. Run the test 20 times in the container at BASE and at HEAD.
  2. Decide whether it is a test-sizing bug or a real stall-detection bug. Linux socket-buffer autotuning can absorb the fixed publish volume.
  3. If it is sizing: publish until `replication_errors` moves, with a deadline, or shrink the follower's receive buffer in the test.
  4. If it is real: fix the stall detection, as a D-75 bug fix.
  5. Run the full container suite until any other failure is named.
- **Cost:** S, or M if the bug is real.
- **Risks:** a real bug in the old path's stall detection.
- **Invariants:** §8 table row.
- **Check:** 20/20 container runs.
- **Fact / hypothesis:** the autotuning cause is a hypothesis.

### C. Defer
Container validation stays intermittently red until step 8 removes the old path.

**Rejected.**
- Ignoring it on Linux: it would remove the only test of that §8 row on the production platform.

**Answer:** `D-76: A` · `D-76: C` · `D-76: discuss`
````

````markdown
## D-77. The next step after step 6

**Context.** Step 6 is done, test-only. Still open:
- D-72, the takeover-rate criterion, which keeps its own card;
- D-73, unplaceable replicas: rotation runs on that open D-57 A deviation, 92 unplaceable against 89 without rotation over 2 000 cluster seeds;
- D-75's removal preconditions;
- step-6 gaps: a successor that restarts between learning the commit and promising forgets `SlotEnd` and takes over by silence, and there is no operator `hand_off` command.
**Vision.** ADR-0008 step plan; D-75; unified-stream §5.

### A. D-73 A, then step 7 — recommended
- **What we do:** the slot table goes into snapshot version 2 (D-73 A), which restores the full D-57 A check. Step 7 comes after it, as its own card: sessions, expiry and will through the log, config, server wiring with the operator `hand_off` command, and the frame version.
- **Cost:** S–M now.
- **Risks:** low.
- **Invariants:** D-57 A, back to fully checked.
- **Check:** the cluster sweep without the exemption.

### B. Step 7 directly
- **What we do:** the new path reaches the server sooner, and D-73 waits.
- **Cost:** L.
- **Risks:** rotation and takeovers keep running on the D-57 A deviation meanwhile (measured: 92 against 89).
- **Check:** node and e2e tests.

### C. Port D-28 C1–C4 to new-path simulation invariants now
- **What we do:** the first D-75 precondition. It does not depend on step 7.
- **Cost:** M.
- **Note:** the durability benchmark, the second precondition, needs server wiring and belongs after step 7.
- **Check:** the new invariants in the sims, and a mutation that breaks each one.

### D. Close the step-6 gaps first
- **What we do:**
  - persist a pending handoff, so a successor that restarts after learning the commit still takes over without a promise round;
  - count handoffs completed against handoffs started in the node's status.
- **Cost:** S.
- **Risks:** low. The gap costs liveness only, since the takeover by silence covers it.

**Recommendation.** A. It is the dependency D-74 named, and it is small. Choose C if removing the old path is the priority.

**Answer:** `D-77: A` · `D-77: B` · `D-77: C` · `D-77: D` · `D-77: discuss`
````

````markdown
## D-79. The slot table past 4096 slot changes (what is left of the D-57 A deviation)

**Context.** D-73 A puts the committed slot table in `apply::State` and its snapshot, and caps it at `apply::MAX_SLOTS` = 4096 entries, dropping the oldest. Past 4096 slot changes, a replica whose log ends in a dropped slot is lost inside the byte bound again. That is still a D-57 A deviation, liveness only.
- Rotation every few heartbeats reaches the cap within hours.
- No simulation reaches it.
- `apply` cannot prune by the compaction floor: it does not see the floor, and its state is compared across nodes by digest.
**Vision.** CLAUDE.md §7 (memory bounded); D-57 A (lost only past the byte bound).

### A. Prune by the floor, outside `apply` — recommended
- **Supersedes:** D-73 A's "in the `apply` snapshot" part. The new evidence is this card's remainder. D-73 A's rule stays: the table is built only from committed records.
- **What we do:**
  - `apply::State` stops keeping the table: `State::slots()` goes, and so does the `slots` term in `State::digest`. That changes the digest `Status` reports, which no released node depends on. HELLO compares the limits digest, not the state digest;
  - the node keeps the table and writes it beside the snapshot, in the snapshot file, after the state bytes (file version 2);
  - each compaction prunes the entries that end below the new floor, since a replica that needs them needs records that are already gone and is lost anyway;
  - its size is then bounded by the slot changes inside the retained log.
- **Cost:** S–M, a reshuffle of D-73 A's code.
- **Risks:** low.
- **Invariants:** D-57 A, closed with no count condition.
- **Check:**
  - a cluster-sim rotation mode that crosses thousands of slots;
  - a node test that restarts after compacting past a slot change.

### B. Keep the count bound and document it
- **What we do:** raise `MAX_SLOTS` (for example to 65 536, 1 MiB) and record the remainder as an accepted deviation in ADR-0008.
- **Cost:** XS.
- **Risks:** rotation at short intervals still crosses the bound eventually.

**Rejected.**
- No bound at all: it breaks CLAUDE.md §7.

**Answer:** `D-79: A` · `D-79: B` · `D-79: discuss`
````

````markdown
## D-78. Step 7 of ADR-0008

**Context.** Steps 1–6 are done, test-only. Still open:
- D-79, the slot-table remainder;
- D-72, the takeover-rate criterion.

Step 7 per ADR-0008:
- sessions, and takeover across gateways;
- expiry and will through the log (the sequencer writes `SessionExpired` and `WillPublished`);
- config and startup checks (ADR-0006, D-68 A);
- server wiring.

It also needs an operator `hand_off` command and a frame version, or a HELLO refusal, for the changed ACK and FETCHED layouts. ADR-0008:125 keeps cluster mode test-only until sessions and wills are in the log.
**Vision.** unified-stream §1, §6 phase 3; ADR-0008:125; D-75.

### A. Split: 7a wiring, then 7c sessions — recommended
"7b" is already state copy (D-60 A).
- **7a:**
  - `server` can start a node on the new path behind a switch marked experimental;
  - the old path stays the default (D-75);
  - config and startup checks, the frame version in HELLO, operator `hand_off`;
  - on the new path the switch refuses wills, persistent sessions and expiry, rather than handling them locally (§7);
  - ADR-0008:125 is updated in the same change.
- **7c:** sessions, expiry and will through the log; the refusals then go.
- **Gate:** D-72 is resolved (accepted with its floors or raised) before 7a makes the switch reachable.
- **Cost:** M + L.
- **Risks:** an experimental mode in the server with a narrower MQTT surface.
- **Check:** node and e2e tests; the sims for 7c.

### B. Step 7 whole, as ADR-0008 words it
- **What we do:** the same refusal statement applies if the wiring lands before sessions.
- **Cost:** L–XL, one long task.

### C. Port D-28 C1–C4 to new-path simulation invariants first
- **What we do:** D-75 condition 2, independent of step 7.
- **Cost:** M.

**Recommendation.** A, after D-79. D-72 is the gate on making 7a reachable.

**Answer:** `D-78: A` · `D-78: B` · `D-78: C` · `D-78: discuss`
````

````markdown
## D-72. D-70 A's takeover-rate criterion

**Context.** D-70 A's done criterion asks for "at least one takeover per hundred steps".

Measured over 10 000 seeds each:
- cluster simulation: 45 533 takeovers in 15 M steps, 0.30 per hundred;
- apply simulation: about 0.24;
- MQTT simulation: about 0.26.

Biasing crashes and partitions toward the sequencer raised the cluster simulation only to 0.32. Faults are a few percent of all events, so the rate follows how often faults happen at all.
**Why now.** Step 5 is recorded in ADR-0008 as "criterion not met" until this is decided.
**Vision.** unified-stream §1 (correctness is shown by simulation), §5 (takeover scenarios); D-70 A.

### A. Accept the measured rate, amending D-70 A — recommended
- **What we do:** amend D-70 A in log.md to read "takeovers counted, about 0.25–0.3 per hundred steps; over a 10 000-seed sweep at least 40 000 in the cluster simulation, 30 000 in the apply simulation and 35 000 slot takeovers in the MQTT simulation", and update ADR-0008's criterion line. No code changes.
- **Result:** the criterion matches what the runs show. A 10 000-seed sweep already holds tens of thousands of takeovers, 14 042 cuts, and 677 compactions during a takeover.
- **Cost:** S (documentation).
- **Risks:** takeovers are a smaller share of each run than the card intended. They are deeply tested by the takeover simulation (100 000 seeds, about 690 000 takeovers).
- **Invariants:** none.
- **Reversibility:** easy.
- **Check:** log.md and ADR-0008 agree.
- **Fact / hypothesis:** It is a fact that the measured rates are as above.

### B. Raise it to one per hundred
- **What we do:** a fault mode per seed (one seed in three crash-heavy, with sequencer crashes and partitions about 4× as frequent), in all three simulations; assert the rate in the summary.
- **Result:** the criterion is met on the fault-heavy seeds and on average.
- **Cost:** M.
- **Risks:** fewer commits, deliveries and compactions per run. The cluster simulation already lost about 27% of its commits to a smaller bias. Default runs may need more seeds.
- **Invariants:** none.
- **Reversibility:** easy.
- **Check:** `STRAUM_SIM_SEEDS=10000`, all three simulations, with a rate assertion.
- **Fact / hypothesis:** It is a hypothesis that 4× is enough.

**Comparison**

| | A | B |
|---|---|---|
| Fit with vision | evidence already in the sweep | the card's number met |
| Risk to invariants | none | none |
| Cost | S | M |
| Reversibility | easy | easy |

**Recommendation.** A. The criterion was a proxy for "takeover under compaction and gateways is exercised". The sweeps show that is the case in tens of thousands of takeovers, and B buys the number by checking less of everything else. Choose B if you want the per-run rate itself as a gate.
**What would change it.** A bug found later that only fault-heavy runs would have caught.
**Rejected.** none.

**Answer:** `D-72: A` · `D-72: B` · `D-72: discuss` · `D-72: defer`
````

````markdown
## D-80. The takeover-rate criterion after D-72 B was measured

**Context.** D-72 B, sequencer faults about 4× as frequent in one seed in three, did not reach one takeover per hundred steps. In the cluster simulation over 1 000 seeds:
- fault-heavy seeds reached 0.40, 0.40 and 0.42 at 4, 8 and 16 faults per hundred events, against 0.36 without;
- making time pass faster lowered the rate (0.20, then 0.09), because heartbeats displace the deliveries and syncs a takeover needs.

A takeover needs about a hundred events or more: silence, a durable promise, a majority's answers, a catch-up, then `SlotStart` made durable and committed. The rate per step is therefore bounded by protocol round trips, not by how often faults happen. The mode was reverted; the simulation now prints its rate (0.34). Option A is D-72 A, which you did not choose; it is offered again because B was measured.
**Why now.** D-78 A gates step 7a on this: the server switch becomes reachable only once the criterion is settled.
**Vision.** unified-stream §1 (correctness shown by simulation), §5 (the takeover scenarios); D-70 A.

### A. Accept the measured rate as the criterion — recommended
- **What we do:** amend D-70 A to "takeovers counted; over a 10 000-seed sweep at least 40 000 in the cluster simulation, 30 000 in the apply simulation and 35 000 in the MQTT simulation", as D-72 A had it. Each simulation asserts its floor scaled to the seeds run, and ADR-0008's "criterion not met" goes.
- **Result:** the gate checks what the runs actually do. Re-measured on the current code (2026-10-09, 10 000 seeds): cluster 51 272, apply 36 139, MQTT 39 059. The takeover simulation adds 91 743 per 20 000 seeds (the step-6 sweep).
- **Cost:** S, a floor assertion in three simulations plus docs.
- **Risks:** none new.
- **Invariants:** none; it is a coverage gate.
- **Reversibility:** easy.
- **Check:** the three 10 000-seed sweeps with their floors.
- **Fact / hypothesis:** fact (the counts above).

### B. Count per fault cycle instead of per step
- **What we do:** in one seed in three, a "cycle" crashes or cuts off the sequencer, then runs events until a new slot commits or a bound is reached. The criterion becomes "a takeover follows at least 9 in 10 sequencer faults within the bound".
- **Result:** it measures the property directly, as liveness after a fault.
- **Cost:** M.
- **Risks:** those seeds interleave less while the cycle runs.
- **Invariants:** none; liveness only, with no time-based safety.
- **Reversibility:** easy.
- **Check:** a rate assertion per cycle.
- **Fact / hypothesis:** It is a hypothesis that 9 in 10 holds. Partitions and dead successors legitimately delay a takeover.

### C. Drop the per-step rate and keep the deviation documented
- **What we do:** D-70 A loses its rate criterion. ADR-0008 keeps the measured counts, and the takeover simulation stays the main evidence. Nothing is asserted.
- **Result:** no gate. 7a goes ahead on the evidence as it stands.
- **Cost:** XS.
- **Risks:** a regression that removes takeovers from the three simulations would go unnoticed.

**Comparison**

| | A | B | C |
|---|---|---|---|
| Gate | floors per sweep | liveness per fault | none |
| Cost | S | M | XS |
| Risk | none new | fewer interleavings in those seeds | silent regression |

**Recommendation.** A. The per-step number cannot be reached without changing what a step is, and every takeover scenario §5 lists is already exercised tens of thousands of times. Choose B if you want liveness after a fault as a gate of its own.
**Rejected.**
- Raising the share of heartbeats: measured to lower the rate.

**Answer:** `D-80: A` · `D-80: B` · `D-80: discuss`
````

````markdown
## D-81. Takeover liveness after a sequencer fault

**Context.** D-80 B was measured in the cluster simulation, 1 000 seeds: share of fault cycles in which a later slot commits within the bound.

| Bound (events) | Other faults running | Faults paused during the cycle |
|---|---|---|
| 600 | 4% | 50% |
| 2 000 | 9% | 75% |
| 6 000 | 20% | 86% |

9 in 10 is not met, so it was not turned on as a gate (log: D-80 Outcome).
- In quiet failed cycles every node is up, and slots keep advancing while nothing commits.
- The longest log holds an uncommitted tail of hundreds of records, and a candidate fetches it one batch (`max_batch` = 4) per round trip. Whether that is the cause is not established.
- Keeping followers from counting silence while a candidate catches up did not help (53%).
- About 14% of quiet cycles fail even at 6 000 events.

"Faults paused" is my reading of D-80 B; the card did not say which other faults run during a cycle. Neither unified-stream §5 nor ADR-0008 defines a bound for liveness after a fault, so which bound counts is your choice.
**Why now.** D-78 A gates step 7a on D-72/D-80, and how long failover takes is what a cluster user sees first.
**Vision.** unified-stream §1 (liveness through the log; time only triggers), §5; D-70 A.

### A. Find and fix why a takeover stalls, then gate it — recommended
- **What we do:**
  1. confirm the rates on 10 000 seeds;
  2. trace the quiet failed cycles to a cause, starting with the unexplained 14%;
  3. make catch-up fetch up to a byte bound per round trip instead of one batch (a configured limit, CLAUDE.md §8);
  4. gate the cycle rate at 9 in 10 within 2 000 events, on the cycles where a majority stays up. The near-unbounded uncommitted tail stays in the seeds that run it today: capping it would pass the gate by testing less.
- **Cost:** M–L.
- **Risks:** catch-up is on the takeover path, so every sweep reruns.
- **Invariants:** 1, 2, 3, 6 are unchanged; this is liveness only.
- **Reversibility:** the wire change is pre-release.
- **Check:** the cycle measurement at 10 000 seeds, all sweeps, the node takeover test.
- **Fact / hypothesis:** It is a hypothesis that batch-at-a-time catch-up explains most of it.

### B. Gate at the measured level, investigate later
- **What we do:**
  - confirm on 10 000 seeds, then assert a regression floor on cycles where a majority stays up, set below the 10 000-seed rate by its seed-to-seed spread;
  - this amends D-80's 9 in 10 and is recorded as a deviation in ADR-0008; a card follows to investigate.
- **Cost:** S.
- **Risks:** it records a weak property as the standard.

### C. Count floors, no liveness gate
- **What we do:** assert takeover counts per sweep. This is D-80 A, which you turned down in D-72 and D-80, offered again because D-80 B was measured short.
- **Cost:** S.
- **Risks:** a failover that is slow or stalls stays invisible.

**Comparison**

| | A | B | C |
|---|---|---|---|
| What is gated | liveness, after fixing it | today's liveness | coverage only |
| Cost | M–L | S | S |
| Risk | the takeover path changes | weak standard | blind to stalls |

**Recommendation.** A. The measurement found a weakness, not a test artefact. Choose B to unblock 7a sooner, with the weakness on record.

**Answer:** `D-81: A` · `D-81: B` · `D-81: C` · `D-81: discuss`
````

````markdown
## D-82. Failover with no bound on the uncommitted tail

**Context.** D-81 A's gate, a later slot committing within 2 000 events after 9 in 10 sequencer faults, holds at 93.5% only with both seed groups pooled:
- with `max_uncommitted` bounded to 4–16, 98.4%;
- with no bound (1 000 000), 87.7%, below 9 in 10. This is recorded as a deviation in ADR-0008.

Failover correlates with the uncommitted tail a new sequencer must move. The mechanism behind the remaining unbounded-group failures is not traced.
**Why now.** D-78 A gates step 7a on this criterion, and a server with no tail bound would fail over slower than the gate says.
**Vision.** unified-stream §1 (liveness through the log), §5; D-81 A.

### A. Bound the tail by default and gate per group — recommended
- **What we do:**
  - give `max_uncommitted` a default chosen for failover as well as memory, measured as the largest tail that keeps 9 in 10;
  - gate each group on its own;
  - keep the unbounded seeds for coverage, gated at 9 in 10 as well once they run under the new default. Seeds still unbounded for coverage are gated only at their measured level; for that group alone, this amends D-81 A and brings back D-81 B.
- **Cost:** S–M.
- **Risks:** a tighter bound makes proposals wait sooner under a slow majority.
- **Invariants:** none.
- **Reversibility:** a default.
- **Check:** a sweep over tail bounds, and the gate per group.
- **Fact / hypothesis:** It is a hypothesis that one default serves both memory and failover.

### B. Trace the unbounded failures first
- **What we do:** find what holds the remaining 12.3%, then decide between A and a protocol change.
- **Cost:** M.
- **Risks:** it may end in A anyway.

### C. Accept the pooled gate
- **What we do:** record that the unbounded group is gated only as part of the pool.
- **Cost:** XS.
- **Risks:** a regression in long-tail failover hides behind the bounded group.

**Recommendation.** A. The measurement says the bound is the lever, and a default is needed for step 7a anyway.

**Answer:** `D-82: A` · `D-82: B` · `D-82: C` · `D-82: discuss`
````

````markdown
## D-83. A default for `read_bytes`

**Context.** `NodeConfig.read_bytes`, the bytes one catch-up or fetch read moves per round trip (D-81 A), has no default; the node tests use 64 KiB. It is clamped to half of `max_frame`, and a batch larger than that alone still goes out whole. Step 7a brings configuration from the environment and needs a default.
**Vision.** CLAUDE.md §8 (every limit has a default and an owner); docs/configuration.md.

### A. 64 KiB default with an env var, in 7a — recommended
- **What we do:** `STRAUM_READ_BYTES`, default 64 KiB, checked at startup against `max_frame` and the largest batch.
- **Cost:** S, within 7a.
- **Risks:** low.
- **Check:** startup refuses a value above `max_frame / 2`. That replaces today's silent clamp (`crates/node/src/lib.rs`), a change of behaviour. It also closes the remaining gap: a single batch larger than the frame.

### B. Derive it from `max_frame`
- **What we do:** half of `max_frame`, no setting of its own.
- **Cost:** XS.
- **Risks:** catch-up and frame size become coupled for no measured reason.

**Recommendation.** A.

**Answer:** `D-83: A` · `D-83: B` · `D-83: discuss`
````

````markdown
## D-84. The default for `max_uncommitted`: 64 or 96, and what it costs

**Context.** D-82 A asked for "the largest tail measured to keep 9 in 10".

| Tail (records) | Faults followed by a commit within 2 000 events (10 000 seeds) |
|---|---|
| 64 | 94.1% |
| 96 | 91.1% |
| 128 | 89.1% |
| 256 and above | 87.7%, the same as no bound |

The largest that keeps 9 in 10 is therefore 96. I shipped 64 instead, for margin, because 96 failed a default-run sample (28 of 32 cycles). That departs from the decision as worded (log: D-82 Outcome).
- Both numbers are in records and simulation events, not time on a host.
- A tail of 64 also caps what a sequencer has in flight: about 64 records per commit round trip. The node tests run with 4096.
- `node` uses the default only once step 7a reads configuration.

The gate for the group with no bound is 85%, the measured 87.1% less its spread.
**Why now.** Step 7a makes this constant the server's default.
**Vision.** CLAUDE.md §11 (no limit chosen without a measurement on Linux); unified-stream §1 (predictability before peak speed). unified-stream does not say whether a liveness criterion measured in simulation may set a production limit on its own; your answer here decides it.

### A. Measure on Linux, then choose by a stated target — recommended
- **What we do:** three nodes on a Linux host.
  - Measure publish throughput and failover time (kill the sequencer, time to the next commit) at `max_uncommitted` 64, 96, 128, 256, 1024 and 4096.
  - Choose the default by this target: 9 in 10 failovers commit within a stated time, proposed as 1 s, at the highest throughput that meets it.
  - Record both curves in docs/baselines/; 64 stays in the simulation's default group until then.
- **Cost:** M, plus one Linux host for about an hour.
- **Risks:** none to correctness.
- **Check:** a baseline file with both curves and the chosen point.
- **Fact / hypothesis:** It is a hypothesis that 64 costs throughput on a real link.

### B. Take 64 into 7a as is
- **What we do:** the simulation's number with margin becomes the default; revisit it if it limits throughput.
- **Cost:** XS.
- **Risks:** a pipeline depth chosen by simulation events.

### C. 96, the decision as worded
- **What we do:** the largest that passes (91.1%) becomes the default; the default run keeps skipping the gate.
- **Cost:** XS.
- **Risks:** a thin margin, 1.1 points over 9 in 10.

### D. 4096, as the node tests use
- **What we do:** keep throughput. This reverses D-82 A and amends D-81 A for the shipped configuration: the shipped default would sit where the simulation measures 87.7%, outside the per-group gate.
- **Cost:** XS.
- **Risks:** shipped failover is below the criterion.

**Recommendation.** A. A host is the only place both costs show, and the choice is a trade between them.

**Answer:** `D-84: A` · `D-84: B` · `D-84: C` · `D-84: D` · `D-84: discuss`
````

````markdown
## D-85. What bounds failover time: a replica that falls behind

**Context.** D-84 A's benchmark (`crates/node/benches/cap.rs`) ran only in the Linux container, so its numbers are preliminary (docs/baselines/apple-m1-dev.md). The sequencer was stopped while the load ran, with an orderly shutdown; the lag is sampled just before the stop.

| Cap | Failovers within 1 s | Largest survivor lag before the stop (records) |
|---|---|---|
| 64 | 6 of 10 | 58 175 |
| 96 | 2 of 10 | 80 275 |
| 128 | 7 of 10 | 26 910 |
| 256 | 8 of 10 | 15 015 |
| 1024 | 0 of 10 | 231 985 |
| 4096 | 10 of 10 | 64 |

A new slot commits only once the new majority holds the whole log. While the old sequencer lived, the slower replica was outside the majority that committed, and nothing kept it close. `max_uncommitted` bounds the tail past the commit point, not that lag. In the stuck states the new sequencer had written its `SlotStart` and was waiting for the other survivor. The simulation never showed this, because its replicas don't fall that far behind.

Throughput also collapses at caps of 1024 and 4096 (16 k and 7 k against about 300 k), for a reason not found. unified-stream does not say whether a replica outside the committing majority may fall behind without bound.

Update 2026-10-09, after the D-85 failover fixes (stale read answers, read routing, progress keepalives, patience, older-slot fetches; ADR-0008). In the same container: every failover commits again (max 8.6 s), and throughput rose to about 350–450 k at caps 64–256. But failovers within 1 s are 2, 7, 2, 5, 9 and 10 of 10 for caps 64, 96, 128, 256, 1024 and 4096, and the misses still go with a survivor 30 000–255 000 records behind. Eventual liveness is fixed; bounded failover time is not, and B is its lever. The collapse at caps of 1024 and up remains.
**Why now.** D-84 A's target, 9 in 10 failovers within 1 s, may be unreachable by the cap alone, and step 7a is waiting on a default.
**Vision.** unified-stream §1 (predictability before peak speed; time never decides), §2.3 (commit by majority); CLAUDE.md §11.

### A. Repeat on a Linux host, then decide — recommended
- **What we do:** run `cargo bench -p node --bench cap` on a quiet Linux host and record both curves and the lag. If the lag pattern holds, the next card is a lag bound (B). If it doesn't, the cap alone is chosen as D-84 A says.
- **Cost:** about an hour of host time, plus S.
- **Risks:** none.
- **Fact / hypothesis:** It is a hypothesis that the container's pattern holds on a host.

### B. Bound replica lag
- **What we do:** the sequencer stops making batches while a replica it would need for a future majority is more than N records behind. A replica that is lost or needs a state copy (D-57 A, 7b) is excluded: counting it would stop batching for good, which would break liveness. N is a record count, never a time (§1), and comes from a Linux measurement, not this table. This changes the ADR-0008 sequencer protocol and needs an ADR entry. It is not runnable on today's preliminary evidence.
- **Cost:** M, a protocol change with sweeps.
- **Risks:** one slow disk slows the cluster. It trades throughput for failover time.
- **Check:** the cap benchmark's lag column stays under N on the host; the simulations still pass.

### C. Faster catch-up for the new majority partner
- **What we do:** the new sequencer ships the lagging survivor's gap in larger reads before its `SlotStart` commits.
- **Cost:** S–M, tied to `read_bytes` (D-83 A, step 7a).
- **Risks:** it helps only if read or window size is the bottleneck, which nothing measured shows yet.
- **Check:** failover time against lag on the host, before and after.

### D. Investigate the throughput collapse at caps of 1024 and up
- **What we do:** profile the 1024 and 4096 runs (on the host if possible) to find why throughput drops about 20×. It decides the throughput half of D-84 A independently of lag.
- **Cost:** S–M.
- **Risks:** none.

### E. Do nothing now
- **What we do:** keep 64 as the simulation's default group only, with no node default, and go on with step 7a; revisit when a host is available.
- **Cost:** XS.
- **Risks:** step 7a ships with whatever `max_uncommitted` its configuration is given.

**Recommendation.** A, then D on the same host. Two of the three effects are unexplained and come from a container; a host settles which is real before a protocol change.

**Answer:** `D-85: A` · `D-85: B` · `D-85: C` · `D-85: D` · `D-85: E` · `D-85: discuss`
````

````markdown
## D-86. The cap on takeover patience

**Context.** D-85's fix for takeovers taking turns for ever doubles a node's wait for silence after each takeover it starts that does not end in a commit (`patience`). The cap is 64×, back to 1 once the commit point moves. Two consequences, recorded in ADR-0008's liveness contract:
- eventual progress is guaranteed only while a takeover's round trips fit within `takeover_ticks × 64` heartbeats;
- after a run of failed takeovers, a dead sequencer may be passed over only after up to `takeover_ticks × 64` heartbeats per turn. At 50 ms and 3 ticks that is 9.6 s, until a commit resets it.

Neither is measured on a host. The simulation needs the doubling: without it, seed 643 churns through 3520 slots, and 3000 seeds take 2826 s instead of 8.
**Vision.** unified-stream §1 (time only triggers; predictability before peak speed).

### A. Keep 64, measure on the Linux host with D-85 A — recommended
- **What we do:** add to the cap benchmark a run of repeated failovers (kill the new sequencer each time), and record the worst failover time with patience grown. Revisit 64 if it exceeds the target.
- **Cost:** S, with D-85 A's host hour.
- **Risks:** none now.

### B. Derive the cap from a measured worst round trip
- **What we do:** cap = the measured worst takeover time on the host ÷ (`takeover_ticks` × heartbeat), with margin; make it a setting.
- **Cost:** S–M after the host run.
- **Risks:** one more setting to choose.

### C. Grow by a fixed step instead of doubling
- **What we do:** add one `takeover_ticks` per failed takeover, up to a cap.
- **Cost:** S plus sweeps.
- **Risks:** slower to outgrow a long round trip; how many failed takeovers that costs is unmeasured.

**Recommendation.** A.

**Answer:** `D-86: A` · `D-86: B` · `D-86: C` · `D-86: discuss`
````

````markdown
## D-87. Fault cycles that need a state copy (2.8% of cycles)

**Context.** The cluster simulation classifies 504 of 18 438 fault cycles (10 000 seeds) as "needs a state copy". A replica was lost past the byte bound, which is 1000 bytes in this simulation, and no majority able to catch up remained. These cycles are outside the liveness guarantee until step 7b (state copy) exists. They are reported, not counted as recovered. The guard against hiding a stall behind a loss (`LOSS_GRACE`) is described in ADR-0008.
**Vision.** D-57 A; ADR-0008 step 7b (D-60 A); D-85.

### A. Rerun the liveness sweep with a realistic retention bound — recommended
- **What we do:** a seed group with `RETAIN_BYTES` at a production-like multiple of the uncommitted tail, to show how many cycles need a state copy once the bound is not tiny.
- **Cost:** S.
- **Risks:** fewer losses means less coverage of the loss paths in that group; the 1000-byte groups stay as they are.

### B. Bring step 7b forward
- **What we do:** build state copy before 7a/7c, so these cycles become recoverable and are gated too.
- **Cost:** M–L.
- **Risks:** it reorders ADR-0008 (D-78 A).

### C. Leave it classified
- **What we do:** nothing until 7b comes in its turn.
- **Cost:** XS.
- **Risks:** 2.8% of fault cycles stay outside the claim.

**Recommendation.** A. It tells whether the 2.8% is the simulation's tiny bound or a real share.

**Answer:** `D-87: A` · `D-87: B` · `D-87: C` · `D-87: discuss`
````

````markdown
## D-88. Patience grown during a partition slows recovery after it

**Context.** D-86 A asked whether the 64× patience turns repeated failed takeovers into long unavailability once the network recovers. In the cluster simulation over 3000 seeds it does, in the tail. Rounds (heartbeats) from healing to the first commit, by the highest patience among the nodes at that moment:

| Patience | Runs | p50 | max |
|---|---|---|---|
| 1–8× | 2 926 | 2–8 | 66–188 |
| 16× | 39 | 17 | 300 |
| 32× | 3 | 383 | 389 |
| 64× | 32 | 2 | 1 790 |

1790 heartbeats is about 90 s at 50 ms. Patience doubles after every takeover a node starts by silence that does not commit, including takeovers that fail because no majority is reachable at all (a partition). The doubling was meant for takeovers that reach a majority but are too slow to finish.
**Why now.** D-86 A treats the 64× wait as a known risk; this shows it applies after every long partition.
**Vision.** unified-stream §1 (time only triggers; predictability before peak speed); D-85, D-86.

### A. Grow patience only for a takeover that got a majority's promises — recommended
- **What we do:** the doubling happens when a takeover reaches the catch-up stage (`decide`, a majority promised), not when it starts. A takeover that never hears from a majority leaves patience as it was.
- **Cost:** S plus all sweeps and the regression seeds (4 and 643 must still recover).
- **Risks:** a takeover that loses its majority right after `decide` still doubles; partitions that flap at that point are untested.
- **Check:** the healing table above, with the 16–64× tail gone; every regression seed passes; the cluster sweep (10 000 seeds), the takeover sweep (20 000), and the apply and MQTT sweeps (10 000).
- **Fact / hypothesis:** It is a hypothesis that the livelock seeds all reach `decide`.

### B. Reset patience when the node hears from the slot's owner or a majority
- **What we do:** any append, or promises from a majority, resets patience to 1, as a commit does.
- **Cost:** S.
- **Risks:** a livelock between nodes that do hear each other could reset patience before it outgrows the round trip.

### C. Keep it, measure on the Linux host (D-86 A as is)
- **Cost:** XS now.
- **Risks:** up to about 90 s to recover after a long partition, in the tail.

### D. Lower the cap (D-86 B or C, with this data)
- **What we do:** for example 8×: the tail above is all at 16× and higher, where at most 188 rounds were seen.
- **Cost:** S plus sweeps.
- **Risks:** a cap too low brings back takers-in-turn livelocks whose round trips need more (seeds 4 and 643 are the check).

**Recommendation.** A; D if A's hypothesis fails.

**Answer:** `D-88: A` · `D-88: B` · `D-88: C` · `D-88: D` · `D-88: discuss`
````

````markdown
## D-89. The replica-lag invariants for D-85 B

**Context.** D-85 B is the direction, to be implemented only once what it bounds is defined and it is shown that flow control cannot block majority progress. ADR-0008 now has a draft ("D-85 B: bounding replica lag"):
- the bounded measure is the **lag** `C − D_r`, the commit point minus a replica's durable position;
- **L1:** safety is untouched;
- **L2:** commit of records already appended is never held; only new batches may wait, and only for *attached* replicas. A replica whose lag stays above `L` for `T` heartbeats detaches, so new appends wait at most `T` heartbeats;
- **L3:** an attached replica's catch-up is bounded by `L` records;
- **L4:** with one survivor detached and the other failing, failover is bounded only by retention and 7b;
- proof obligations (a)–(c).

Your condition was "never block commit for an available majority". L2 reads it as: commit is never held, and new appends may wait up to `T` heartbeats. That reading is yours to confirm.
**Vision.** D-85 (conditions); unified-stream §1, §2.3; D-57 A.

### A1. Accept L1–L4 and write proof (a) as an argument — recommended first
- **What we do:** confirm L2's reading; write in ADR-0008 the argument that the hold rule names only attached replicas, and that detachment is a pure function of lag and heartbeats, so no available majority waits more than `T` for an append. No code.
- **Cost:** S.

### A2. Implement, with simulation proof (b) — only after A1
- **What we do:** attach/detach and the hold rule, then two properties in the cluster simulation: no stall, and no available majority waiting more than `T` heartbeats for a batch. Then (c) on Linux sets `L` and `T`.
- **Cost:** M, plus the host.
- **Risks:** throughput limited by the slowest attached replica; L2's bound is the guard.

### B. Revise the draft first
- **What we do:** for example bound `E − D_r`, or detach on catch-up room `D_r − F` rather than on time.
- **Cost:** S.

### C. Wait for the Linux measurement before any design
- **Cost:** XS now.
- **Risks:** the design stays open.

**Recommendation.** A1, then A2.

**Answer:** `D-89: A1` · `D-89: A1+A2` · `D-89: B` · `D-89: C` · `D-89: discuss`
````

````markdown
## D-90. Cycles still progressing at the cap ("slow")

**Context.** The cluster simulation's eventual-liveness check stops a cycle at 500 000 events. One cycle in the 10 000-seed sweep (seed 8956) is still getting further there. It is in the group with no uncommitted bound: its sequencer appends faster than the only replica it can commit with catches up. It commits at about 1 300 000 events (an ignored test reruns it with `STRAUM_SIM_STALL_CAP=5000000`, about two minutes). The test treats such a cycle as "slow", allowed only in that group and only for the known seed: any other fails the sweep. That allowance was decided inside the test; it is yours to confirm.
**Vision.** D-85 (eventual liveness); D-82 A (`max_uncommitted` bounds failover).

### A. Allow slow cycles only without an uncommitted bound, listed by seed — as now, recommended
- **What we do:** keep the allowance; every new slow seed fails the sweep until it is shown to commit with the cap raised and added to the list.
- **Cost:** none.

### B. No allowance: a raised cap in the sweep
- **What we do:** the 10 000-seed sweep runs with a 5 000 000-event cap, so seed 8956 must commit within it.
- **Cost:** S. The sweep takes a few minutes longer.

### C. Drop the uncapped group's appends during cycles
- **What we do:** the simulation stops proposing to a sequencer that cannot commit.
- **Risks:** it tests less of what an uncapped tail does.

**Recommendation.** A. Choose B if a slow cycle should never pass a gate.

**Answer:** `D-90: A` · `D-90: B` · `D-90: C` · `D-90: discuss`
````

````markdown
## D-91. What retention bound is realistic for the new path

**Context.** D-87 A's rerun used 100 000 bytes, which I chose: about 1400 of this simulation's records. It found no state-copy cycles. The old path's documented default is 64 MiB (`STRAUM_WAL_RETAIN_BYTES`); `node` has no default for `retain_bytes`. The figure matters for how many failovers end in a state copy, and it comes to the server with step 7a's configuration.
**Vision.** D-57 A; D-87 A; configuration.md.

### A. Default 64 MiB, like the old path, in 7a — recommended
- **What we do:** set the node default to 64 MiB, and keep the simulation's 100 000 bytes as "realistic" there (records are tiny).
- **Cost:** XS now, S in 7a.

### B. Derive it from `max_uncommitted` × a record size × a factor
- **Cost:** S.
- **Risks:** a derived default nobody can read off.

**Recommendation.** A.

**Answer:** `D-91: A` · `D-91: B` · `D-91: discuss`
````

````markdown
## D-92. The promise round no longer gets a longer wait (a consequence of D-88 A)

**Context.** Under D-88 A patience doubles only after a takeover a majority promised. A takeover whose promise round itself is slower than the wait is passed over and the wait does not grow, so the liveness contract in ADR-0008 now asks that the promise round fit `takeover_ticks` heartbeats once in a while (the remaining round trips still get up to 64×). In 10 000 seeds nothing stalls and seeds 4 and 643 recover, but the group with the default bound (64) is at 90.4% within 2 000 events against the 90% gate (91.0% before D-88 A): a 0.4-point margin.
**Why now.** The contract is narrower than before; that is yours to accept.
**Vision.** unified-stream §1 (time only triggers); D-85 (liveness contract); D-88 A.

### A. Accept the narrower contract as written — recommended
- **What we do:** nothing; the contract states it, the sweeps check it.
- **Cost:** none.
- **Risks:** a network whose promise round always exceeds `takeover_ticks` never takes over; the 0.4-point margin on group 64 may go under the gate with other changes.
- **Check:** every sweep keeps "no stall" and the 90% gate.

### B. Also double after a takeover that heard a promise besides its own that was not enough for a majority
- **What we do:** evidence of slow round trips, not of a partition, also grows patience.
- **Cost:** S.
- **Risks:** with three voters it never applies — one other promise already is a majority — so it changes nothing in the simulation or a three-node cluster; it only matters at five voters or more.

### C. Reset patience on hearing a majority (D-88 B)
- **What we do:** revive D-88 B: grow at every takeover, reset when a majority is heard.
- **Cost:** S.
- **Risks:** only with new evidence that A stalls or falls under the gate; D-88 B was not chosen.

**Recommendation.** A. C if a sweep shows A stalling or under 90%.

**Answer:** `D-92: A` · `D-92: B` · `D-92: C` · `D-92: discuss`
````

````markdown
## D-93. Flow control is rarely exercised in the simulation

**Context.** With flow control on in half of each bounded seed group, 10 000 seeds produce 64 held heartbeats in 21 stretches between batches. The rules are covered by unit tests, each failing without its rule, and the simulation's check catches a broken R2 (seed 57), but holds and detachments under faults are rare.
**Vision.** D-89 A2; unified-stream §7 (correctness by deterministic simulation).

### A. Add a "slow replica" fault to the cluster simulation — recommended
- **What we do:** a replica whose writes and syncs lag for a while, so attached replicas fall behind by more than `L` often; the same checks (no stall, held fewer than `T` heartbeats per batch, every safety invariant).
- **Cost:** S–M, plus a sweep.
- **Risks:** it shifts the seed schedules; the gates are rechecked.

### B. Leave it
- **Cost:** none.
- **Risks:** interactions between detachment and takeovers stay rare in the simulation.

**Recommendation.** A, before `lag_bound` is turned on anywhere.

**Answer:** `D-93: A` · `D-93: B` · `D-93: discuss`
````

````markdown
## D-94. A Linux host for step 6 (D-84 A; D-89: `L`, `T`, the cap)

**Context.** Step 6 of your order — Linux benchmarks and production settings — needs a quiet Linux host; none is available here. The cap bench (`node/benches/cap.rs`) is ready; flow control can be measured off and on once `node` sets `lag_bound`.
**Vision.** CLAUDE.md §11, §13 (accepted numbers come from a quiet Linux host); D-84 A.

### A. You provide a quiet Linux host (ssh); I run the cap bench with flow control off and on — recommended
- **Cost:** M, your host time.

### B. A cloud VM
- **Risks:** noisier; its numbers are preliminary and cannot set `L`, `T` or the cap.

### C. Defer
- **Risks:** no production settings, no bounded-failover claim.

**Recommendation.** A.

**Answer:** `D-94: A` · `D-94: B` · `D-94: C` · `D-94: discuss`
````

````markdown
## D-95. A short test of the takeover-during-catch-up race

**Context.** D-90 B asked for a short regression test of the mechanism behind the long cycles. `without_an_uncommitted_bound_a_new_slot_must_first_cover_the_whole_tail` shows one half: a new slot commits only once a replica holds everything to its first record. The other half — a takeover during the catch-up moves that first record further on, while the sequencer keeps appending — is shown only by the sweeps and the ignored test.
**Vision.** D-90 B (conditions).

### A. A `Voter`-level test of the race — recommended
- **What we do:** a sequencer with a long uncommitted tail, a replica catching up, a takeover by the next owner before it finishes; assert the new slot's `SlotStart` lies past the old target and commit waits for it.
- **Cost:** S–M.

### B. Pin the shortest simulation seed that shows it
- **What we do:** seed 2212 (about 210 000 events), release only, ignored.
- **Cost:** XS.
- **Risks:** not short; schedules shift with any change.

**Recommendation.** A.

**Answer:** `D-95: A` · `D-95: B` · `D-95: discuss`
````

````markdown
## D-96. Step 7a while there is no Linux host

**Context.** Flow control is built and off by default (`lag_bound: None`); `max_uncommitted` and `retain_bytes` have starting defaults (64, 64 MiB) that are not measured optima. Step 7a (configuration from the environment) can proceed with them or wait.
**Vision.** ADR-0008 step plan; D-84 A, D-91 A.

### A. Proceed to 7a with flow control off; `L`, `T` and the cap chosen once measured — recommended
- **Cost:** none now.
- **Risks:** 7a's defaults are provisional, as documented.

### B. Wait for the Linux host before 7a
- **Risks:** the rewrite stalls on hardware.

**Recommendation.** A.

**Answer:** `D-96: A` · `D-96: B` · `D-96: discuss`
````

````markdown
## D-97. One sweep that did not finish within an hour

**Context.** One 10 000-seed run of the cluster simulation with flow control on did not finish within an hour and was stopped by its time limit. A 50 000 000-cap sweep was running beside it. Every later run of the same and later code finished in 197–219 s, and per-seed progress showed no seed hanging. The cause is not identified. Under D-90 B an unexplained non-termination should not be left open.
**Vision.** D-90 B; D-85.

### A. Reproduce or exclude it — recommended
- **What we do:** rerun the configuration of that run (flow control on, `records: seed % 8`, before the R4 count fix) alone and beside a 50M sweep, with per-seed wall-clock and event counts, until it reproduces or contention is shown.
- **Cost:** S, about an hour of machine time.

### B. Record it as contention
- **Risks:** a real hang in that code version would go unexamined (that version's R4 counted per hold, since fixed).

**Recommendation.** A.

**Answer:** `D-97: A` · `D-97: B` · `D-97: discuss`
````

````markdown
## D-98. A replica with a slow disk starts takeovers it cannot finish

**Context.** Found by D-93's scenario (ADR-0008, "A takeover finding"). The trigger is a replica whose promise of the newer slot is not durable yet:
- it treats that slot's appends as a reason to promise, not as a heartbeat (`Voter::on_append` resets silence only when following);
- on a slow disk it therefore counts silence while the sequencer is talking to it;
- it starts takeovers of its own that it cannot finish, and its patience stays at 1, because no majority ever answers them (D-88 A);
- when one of its promises finally lands, it can depose a healthy sequencer.

With 95 of 100 writes deferred, takeovers roughly tripled (145–200 a run against 35–53 with no slow disk). The count was the same with flow control off and under the old patience rule, so neither caused it. Safety holds throughout; availability and throughput suffer.
**Why now.** A slow disk on one replica is an ordinary production fault, and this happens with flow control off — so also in 7a as D-96 configures it. Whether 7a waits for it is D-101.
**Vision.** unified-stream §1 (time only triggers; predictability before peak speed); D-85 liveness contract; D-88 A.

### A. Count an append from the owner of a newer slot as being heard — recommended
- **What we do:** in `on_append`, an append from the owner of a slot above the one promised resets silence while the promise of that slot is being made durable. The node is then neither silent nor ignoring the sequencer it is about to follow.
- **Cost:** S. A unit test, the slow-replica scenario at `takeover_ticks` 3, and every sweep.
- **Risks:** silence is reset by a sequencer this node has not promised yet; safety does not depend on silence (time only triggers).
- **Check:** the slow-replica scenario at the base timing (3 heartbeats, rotation on), compared with the same runs without a slow disk: takeovers per run within that baseline (35–53 in the measurement, against 145–200 now), the commit point moving after each detach, and no takeover started by the slow replica itself. Not zero takeovers: at this timing the simulation takes over about every 300 events with no fault (D-99).
- **Fact / hypothesis:** It is a hypothesis that this alone removes the churn.

### B. A pre-vote: a takeover asks first whether a majority still hears the sequencer
- **What we do:** a takeover starts only if a majority reports silence too. Silence still only triggers; the promises still decide (§1).
- **Cost:** M; a new message and round trip.
- **Risks:** a longer failover; a new liveness argument to write.

### C. Leave it; document that a slow disk can depose a healthy sequencer
- **Cost:** none.
- **Risks:** churn in production on one slow disk; 7a then ships with a known availability fault, which D-96 asks to state rather than present as ready.

**Recommendation.** A. B if A's check does not bring takeovers back to the baseline.

**Answer:** `D-98: A` · `D-98: B` · `D-98: C` · `D-98: discuss`
````

````markdown
## D-99. The simulation's base timing takes over every ~300 events with no fault

**Context.** With `takeover_ticks` 3 and the simulation's random delivery, a slot is taken over about every 300 events even in calm periods: several times in a 3 000-event window (D-93's scenario, ADR-0008). Every new slot starts with all replicas detached (R1), so flow control rarely runs long enough to act. The slow-replica test therefore uses 30 heartbeats and no rotation. The main sweep keeps 3: that churn is part of what it checks.
**Vision.** unified-stream §7 (correctness by deterministic simulation); D-89 A2.

### A. Keep 3 in the main sweep; give D-93's scenario its own steady setting (as now) — recommended
- **Cost:** none.
- **Risks:** flow control under heavy churn is covered only by the main sweep, where it rarely acts.

### B. Add a seed group with a longer `takeover_ticks`
- **What we do:** for example 10 heartbeats, so flow control and long slots meet faults more often.
- **Cost:** S. The group shifts the seed schedules; the gates are re-checked.

**Recommendation.** A; B if D-98 A lands and the sweep should cover it at a realistic timing.

**Answer:** `D-99: A` · `D-99: B` · `D-99: discuss`
````

````markdown
## D-100. A detached replica may never re-attach under sustained load

**Context.** In D-93's scenario, after the slow disk recovers and under the same load, the detached replica is caught up from the log at about the rate the sequencer appends:
- it closed the gap in only 74 of 800 windows;
- its lag held at up to 550 records (p50 295);
- R2 attaches only at lag 0, so it stays detached until the load drops, and keeps no bounded failover meanwhile (condition 5);
- with a slower catch-up it would fall behind until retention passes it and it needs a state copy, the D-85 concern.

L2 forbids holding appends for a detached replica, so flow control cannot close the gap.
**Vision.** D-85 (bounded lag: backpressure or an explicit move to a state copy, never at the majority's cost); D-89 A1 (R2, Lemma 2); D-57 A.

### A. Attach at lag ≤ `L − U` instead of 0 (`U` = `max_uncommitted + max_batch`, the floor `L` is already raised to) — recommended once flow control is to be turned on
- **What we do:** generalise Lemma 2. A replica attached at lag ≤ `L − U` cannot pass `L` before the next batch, since within a hold its lag grows by less than `E − C < U`. A larger `L` widens the re-attach window. R4 then detaches those above `L − U`. A replica trailing within `L − U` is counted again, and its failover bound restored.
- **Cost:** S, plus the proof's update in ADR-0008 and the slow-replica scenario with a re-attach under load.
- **Risks:** a larger attached lag bound, `L + U`, which is what condition 5's bound already says.
- **Note:** it changes R2, part of D-89 A1's accepted proof, so it re-opens D-89 A1 (Lemma 2 and conditions 2 and 6), gated by the cluster and takeover sweeps. Irrelevant while `lag_bound` is `None` (D-96).
- **Check:** the scenario's "gap closed under load" turns into "re-attached under load" in most windows.

### B. Catch a detached replica up faster
- **What we do:** a larger read for a replica that is detached, so its catch-up outruns the appends.
- **Cost:** S–M; read sizes are D-81 A and D-83 A's.
- **Risks:** more I/O on the sequencer while it serves; the size comes from Linux.

### C. Defer until flow control is to be turned on — recommended now
- **What we do:** nothing now. Under D-96 flow control is off in 7a, so attachment does not run there; decide when `lag_bound` is first set.
- **Cost:** none.

### D. Accept it: a detached replica rejoins when the load allows, or moves to a state copy past retention
- **Cost:** none.
- **Risks:** under sustained load, bounded failover is held by the other replica alone.

**Recommendation.** C now; A when flow control is to be turned on, then B measured on Linux if the gap still does not close.

**Answer:** `D-100: A` · `D-100: B` · `D-100: C` · `D-100: D` · `D-100: discuss`
````

````markdown
## D-101. Does 7a wait for D-98?

**Context.** D-96 lets 7a continue once D-92, D-93, D-95 and D-97 are closed. D-93 is closed with the slow-replica test at 30 heartbeats and no rotation:
- every window: the slow replica detached at `T` by the settled sequencer, the commit point moving after that detach, the replica never lost, rejoining when the load pauses.

At the base timing (3 heartbeats) majority progress is not shown, because a slow disk deposes healthy sequencers (D-98). That happens with flow control on or off, so it applies to 7a as configured (`lag_bound: None`). `node` has no default `takeover_ticks` yet; 3 is the simulation's.
**Vision.** D-96 (conditions); unified-stream §1 (predictability before peak speed).

### A. 7a waits for D-98 A, gated by its check — recommended
- **What we do:** D-98 first; 7a once D-98 A's check holds at the base timing: takeovers per run within the no-slow-disk baseline, the commit point moving after each detach, no takeover started by the slow replica.
- **Cost:** S before 7a.
- **Risks:** 7a later by that much.

### B. 7a waits for D-98 A only; the base-timing measurement lands with it
- **What we do:** the fix gates 7a; the slow-replica scenario at the base timing is added as a reported comparison against the baseline, not as a pass condition, since the simulation's own churn (D-99) may keep any strict condition out of reach.
- **Risks:** the fix's effect is reported, not enforced.

### C. 7a in parallel; D-98 tracked as a known availability fault
- **What we do:** 7a with `lag_bound: None`; the server documents that one slow disk can cause takeover churn until D-98 lands.
- **Risks:** an experimental server with a known liveness fault under an ordinary disk problem.

**Recommendation.** A.

**Answer:** `D-101: A` · `D-101: B` · `D-101: C` · `D-101: discuss`
````

````markdown
## D-102. D-101 reassessed: start 7a now?

**Context.** D-101 A held 7a until D-98's check passes at the standard timing. It now does (ADR-0008, "D-98 A"):
- **Takeovers:** those started by silence with a slow disk are back at the fast-disk level (−6% over 100 seeds, +3% over 20; +47% before).
- **The slowed replica:** wins fewer takeovers than on a fast disk.
- **Commit:** it moved after every detach.
- **Durability:** nothing is appended, acknowledged or committed on the strength of a sign of life.
- **A dead sequencer:** is still taken over.
- **Every sweep:** passes.

One residue: the slowed replica still *starts* more takeovers than on a fast disk (2 492 against 1 520). A takeover it began legitimately in the base churn stays pending on its slow promise, and it moves on to later slots without finishing any; it wins none of them. An append from the current slot's owner does not reset silence while this node is itself taking over.
One reading to confirm: condition 2 ("the number of takeovers returns to the level") is met for the cluster total; the takeovers the slowed replica itself starts are still +64%. The ADR reads it as the total.
**Vision.** D-101 A; D-96 (7a with `lag_bound: None`, flow control off, unverified settings not presented as production-ready).

### A. Start 7a with flow control off — recommended
- **What we do:** step 7a as D-96 configures it, still labelled not production-ready. The D-98 gate (`a_slow_disk_adds_no_takeovers_at_the_standard_timing`) stays ignored in the debug run and is run explicitly in release with the sweeps.
- **Cost:** the step itself.
- **Risks:** the residue above, measured as harmless in the simulation only.

### B. First also reset silence when the current slot's owner is heard during a takeover of this node's own
- **What we do:** stop the futile moves on to later slots; the pending takeover itself still completes when its promise is durable.
- **Accepted when:** the D-98 gate still passes; the slowed replica's own starts fall towards the fast-disk level; a dead sequencer is still taken over while this node's own takeover is pending (a unit test like the D-98 ones).
- **Cost:** S.
- **Risks:** it changes when silence counts during this node's own takeover, the liveness-critical path (D-85, D-88 patience).

### C. Wait for the Linux host (D-94)
- **Risks:** 7a stalls on hardware; nothing here depends on it.

**Recommendation.** A; B only if a host measurement shows the residue costing anything.

**Answer:** `D-102: A` · `D-102: B` · `D-102: C` · `D-102: discuss`
````

````markdown
## D-103. How the server reaches the new path (7a)

**Context.** 7a lets `server` start a node on the new path behind an experimental switch (D-78 A, D-102 A).
- **Where the new path lives:** in `node` (links, sequencer or replica, the log on disk, the gateway over TCP; `publish = false`). It depends on `apply`, `cluster`, `gateway`, `journal`, `storage` and `transport`.
- **What the boundaries allow:** `server` may depend on `runtime` only (CLAUDE.md §2, enforced by `scripts/check-boundaries.sh`). `runtime` is the old path's driver.

Wiring the switch therefore changes the dependency table, which is a decision, not a detail (CLAUDE.md §17: no silent changes). D-78 A's precondition holds: D-72 was settled by D-80 B (liveness per fault cycle, not a rate), made concrete by D-81 A and D-82 A.
**Vision.** CLAUDE.md §2 (enforced boundaries); D-49 A (the new crates beside the old path until step 8); D-75 (old path frozen).

### A. `server` depends on `node` directly — recommended
- **What we do:**
  - the table gains `server → node`, and `check-boundaries.sh` follows;
  - `server`'s config parses the switch and builds a `node::NodeConfig`;
  - `runtime` and the frozen old path stay untouched.
- **Cost:** S for the dependency and the table; the configuration is D-104.
- **Risks:**
  - `server` links two drivers until step 8;
  - `node` stays `publish = false`, which is fine for a binary.
- **Check:** `check-boundaries.sh`; a server test that starts in each mode.

### B. A new binary for the new path
- **What we do:** a separate `straum-node` binary beside `straum-server` until step 8, then folded into the server; no dependency change for `server`.
- **Benefit:** the experimental path is not reachable from the production binary at all, which fits D-102 A's "not production-ready".
- **Cost:** M (a second binary: its config, signals, packaging).
- **Risks:**
  - two binaries to operate;
  - step 8 must merge them again.

**Recommendation.** A. B if the experimental path should not be reachable from the production binary at all.

**Rejected.** `runtime` depends on `node` and offers the switch: it adds a feature to the frozen old path's crate (D-75 allows bug fixes only).

**Answer:** `D-103: A` · `D-103: B` · `D-103: discuss`
````

````markdown
## D-104. The new path's configuration surface and defaults (7a)

**Context.** `node::NodeConfig` is built by tests today. 7a reads it from the environment with startup checks (ADR-0006, D-68 A). Decided already:
- `read_bytes` 64 KiB, checked against `max_frame` and the largest batch (D-83 A);
- `retain_bytes` 64 MiB (D-91 A);
- `max_uncommitted` 64 (D-82 A, D-84);
- `lag_bound` off and not configurable in 7a (D-102 A).

Undecided:
- **The switch and membership:** how a node learns its id, its peers' link addresses and the first slot's owner.
- **The timing:**
  - `heartbeat` and `takeover_ticks` have no measured defaults;
  - the tests and the simulation use 50 ms and 3, and the simulation shows 3 heartbeats churns under random delivery (D-99);
  - none of it is measured on a real network (D-94 C).
**Vision.** ADR-0006 (startup checks); CLAUDE.md §8 (every limit with a default and an owner in configuration.md); D-102 A (5: no unmeasured production claims).

### A. Explicit env, timing defaults marked experimental — recommended
- **What we do:**
  - the switch is `STRAUM_EXPERIMENTAL_CLUSTER=1`, plus names of its own, so none takes on an old-path meaning (`STRAUM_WAL_RETAIN_BYTES` and `STRAUM_WAL_PATH` keep theirs):
    - `STRAUM_CLUSTER_NODE_ID`;
    - `STRAUM_CLUSTER_PEERS` (`id=host:port,…`, own id included);
    - `STRAUM_CLUSTER_LISTEN`;
    - `STRAUM_CLUSTER_DATA_DIR`;
    - `STRAUM_CLUSTER_RETAIN_BYTES` (64 MiB) and D-83 A's `STRAUM_READ_BYTES` (64 KiB);
  - timing: `STRAUM_CLUSTER_HEARTBEAT_MS` default 50 and `STRAUM_CLUSTER_TAKEOVER_TICKS` default 3. These are placeholders — the values the tests use — not recommendations; no cap and no failover claim before Linux (D-102 A, condition 5);
  - startup refuses an id not in the peers, `read_bytes` over the frame, and a data directory holding another cluster's promise;
  - part of this choice, not decided before: refuse fewer than three voters, and an even count (a majority of an even count tolerates no more failures than one fewer node);
  - the switch refuses wills, persistent sessions and expiry at CONNECT, rather than handling them locally (D-78 A).
- **Cost:** M (parsing, checks, their tests, configuration.md).
- **Risks:** the 3-heartbeat default churns in the simulation; on a real network that is unmeasured.

### B. No timing defaults: both required
- **What we do:** as A, but `STRAUM_HEARTBEAT_MS` and `STRAUM_TAKEOVER_TICKS` must be set, or the node refuses to start.
- **Cost:** as A.
- **Risks:** none new. Every operator picks a value with no measurement to guide it.

### C. A more conservative timing default
- **What we do:** as A, with `takeover_ticks` 10 (about 500 ms of silence at 50 ms).
- **Cost:** as A.
- **Risks:** a slower failover; still unmeasured, and the simulation's main sweep keeps 3 (D-99 A).

**Recommendation.** A. B if no number should look like a recommendation before Linux.

**Answer:** `D-104: A` · `D-104: B` · `D-104: C` · `D-104: discuss`
````

````markdown
## D-105. How an operator hands the slot over (7a)

**Context.**
- **What exists:** `cluster::Voter::hand_off` (step 6, `SlotEnd` and the successor schedule) is reachable only from tests. `node::Node` has no command for it.
- **What D-78 A asks:** an operator `hand_off` in 7a.
- **Precedent:** the server already handles SIGTERM and Ctrl-C (`#[cfg(unix)]` in `main.rs`), and has a hand-written HTTP listener for metrics (`STRAUM_METRICS_ADDR`).
**Vision.** unified-stream §2.4 (planned handoff); CLAUDE.md §13 (platform code); ADR-0008 step 6.

### A. A signal: SIGUSR1 asks the node to hand its slot over — recommended
- **What we do:**
  - `node::Node::hand_off()`, called on SIGUSR1;
  - on a node that is not sequencing it is logged and ignored;
  - unix only, as SIGTERM is.
- **Cost:** S.
- **Risks:**
  - no reply to the operator except the log and the status;
  - Windows is not a target.

### B. An HTTP request on the metrics listener
- **What we do:** `POST /hand-off` on `STRAUM_METRICS_ADDR`, answered with the outcome.
- **Cost:** S–M.
- **Risks:** the metrics listener becomes able to change state. It has no authentication today, so it needs a separate switch or address.

### C. Not in 7a: rotation (`rotate_every`) only
- **What we do:** leave the operator command for later; `STRAUM_CLUSTER_ROTATE_EVERY` exposes step 6's optional rotation. This amends D-78 A, which lists the command as part of 7a, and is recorded as an amendment.
- **Cost:** XS.
- **Risks:** D-78 A asked for the command; no way to move the sequencer off a host on demand.

**Recommendation.** A.

**Answer:** `D-105: A` · `D-105: B` · `D-105: C` · `D-105: discuss`
````

````markdown
## D-106. An automated end-to-end test of the server in cluster mode

**Context.** 7a is wired and checked piece by piece:
- `node` tests drive `Node` directly;
- the server's configuration is unit-tested;
- a manual smoke run on loopback passed: three server processes, a QoS 1 publish across nodes, an operator handoff, refused CONNECTs, clean shutdown (ADR-0008, "Step 7a").

Nothing automated starts the server binary in this mode. D-102 A says 7a is not complete until the new server path passes its correctness checks. The rules allow the MQTT client `rumqttc` only in the e2e and node tests (CLAUDE.md §2).
**Vision.** D-102 A (transition criterion); CLAUDE.md §2 (dev-dependencies), §17.

### A. A server integration test with a hand-written MQTT client — recommended
- **What we do:**
  - a test in `crates/server/tests/` starts three `straum-server` processes (`CARGO_BIN_EXE_straum-server`) in cluster mode on loopback;
  - it speaks MQTT 3.1.1 with a few hand-encoded packets (CONNECT, SUBSCRIBE, PUBLISH QoS 1), as the smoke run did;
  - it checks: delivery across nodes; a handoff by SIGUSR1 while delivery goes on; a will and a CleanSession=0 refused; a restart over the same data; SIGTERM.
- **Cost:** M.
- **Risks:** a hand-written client covers only what it encodes; process and port handling in tests.
- **Check:** in every `cargo test`, unix only, as SIGUSR1 is.

### B. The same, with `rumqttc` as a dev-dependency of `server`
- **What we do:** as A, with the full client already used by `node`'s tests.
- **Cost:** M; a change to §2's list of allowed dev-dependencies.
- **Risks:** a second crate pulls the client in.

### C. Defer to after 7c
- **What we do:** keep the manual smoke run until sessions and wills are in the log, then test once.
- **Cost:** none now.
- **Risks:** 7a's wiring stays unguarded against regressions until then. Under D-102 A, 7a stays incomplete until such a test exists: C defers the step's completion too.

**Recommendation.** A.

**Answer:** `D-106: A` · `D-106: B` · `D-106: C` · `D-106: discuss`
````

````markdown
## D-107. Credentials and TLS on the new path

**Context.** In the experimental cluster mode:
- MQTT clients connect without credentials or TLS: `node` has no `auth`, and the server refuses `STRAUM_TLS_*` and `STRAUM_AUTH_*` there rather than ignore them;
- the links between nodes are plain TCP, protected only by the frame version and the configuration digest, which are not authentication.

Anyone who can reach the ports can publish, subscribe, or join the link protocol with a guessed digest.
**Vision.** CLAUDE.md scope (TLS optional); D-102 A (experimental, not production-ready); ADR-0008 "Not yet in step 4" (credentials not checked).

### A. Client credentials and TLS before 7a counts as complete — recommended
- **Amends D-102 A:** it adds a completion criterion D-78 A and D-102 A do not contain.
- **What we do:**
  - `node` checks CONNECT credentials with the `auth` crate (password file, ACL) as the broker does;
  - TLS for clients over `transport`, which already has it;
  - links between nodes stay plain, documented as trusted-network only.
- **Cost:** M.
- **Risks:** `node` gains `auth`. An ACL refusal at the gateway decides nothing in shared state: it filters input before it reaches the log (§1).

### B. A and authenticated links (mutual TLS between nodes)
- **Cost:** L.
- **Risks:** certificate handling for every node.

### C. Loopback by default until credentials exist
- **What we do:** `STRAUM_CLUSTER_MQTT_LISTEN` defaults to `127.0.0.1:1883` instead of every interface; reaching it from elsewhere needs an explicit address. A change of default, so a decision.
- **Cost:** XS.
- **Risks:** a multi-host trial must set the address, knowingly unauthenticated.

### D. Keep it experimental and trusted-network only until step 8
- **What we do:** document it, and log a warning at startup (as now).
- **Cost:** XS.
- **Risks:** an unauthenticated `0.0.0.0:1883` stays the default.

**Recommendation.** C now (XS), then A; B before any production claim.

**Answer:** `D-107: A` · `D-107: B` · `D-107: C` · `D-107: D` · `D-107: discuss`
````

````markdown
## D-108. Node tests that pick ports by binding and releasing them

**Context.** One validation run in the Linux container failed two node tests that pass everywhere else: `a_node_with_other_limits_is_refused_and_the_others_go_on` and `a_restarted_sequencer_resumes_where_its_log_ends`, both at a node's start. Three reruns in the container and every host run passed.
- **The suspected cause, not proven:** the tests' `free_port()` binds port 0, reads the number and releases it before the node binds it, so a test running in parallel can take the port first. Two more cluster tests in that file make it likelier.
- **The cause is not confirmed:** the failing run kept no error text.
**Vision.** CLAUDE.md §17 (failure paths tested; no flaky gate).

### A. Bind first, then tell the peers
- **What we do:** each node binds port 0 itself (`Node::start` already reports `listen`); the test starts the nodes, then gives each its peers' real addresses.
- **Cost:** S–M; `NodeConfig.peers` must then be set after binding, so a small change to how tests start a cluster.
- **Risks:** none for production; the server takes fixed ports.

### B. Keep the error, then decide — recommended
- **What we do:** make the tests print `Node::start`'s error instead of `unwrap`, run the node test binary in the container in a loop (for example 20 times) to try to reproduce it, and look at the error.
- **Cost:** XS.
- **Risks:** waiting for a rare failure.

**Recommendation.** B now (XS), A if the error shows a port in use.

**Answer:** `D-108: A` · `D-108: B` · `D-108: discuss`
````

````markdown
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

````

````markdown
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

````

````markdown
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

````

````markdown
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

````

````markdown
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
````


````markdown
## D-115. The losses the outbox still has, inside one slot

**Context.** D-109 B closed the loss at a slot change. One loss remains, inside a slot:
- **When:** a proposal is refused by a full sequencer queue (`max_queue`), or lost in the tail of a broken link, and a later proposal of the same node then commits in the same slot.
- **What the node does:** sending it again would put it behind the later one, so it is given up and logged ("proposals lost before a later one of this node committed"). Its publisher is closed by `commit_timeout` (30 s), as before D-109.
- **What never happens:** a duplicate or a reorder. Only a loss, which at-least-once covers through the client's resend.
- **Unknown:** how often this happens under load. Nothing counts it yet (D-112).
**Vision.** CLAUDE.md §7 (ordering), §8; D-109 B's condition ("no republishing without well-defined deduplication"); D-112 A.

### A. Count it first, decide on the number — recommended
- **What we do:** the metrics of D-112 count these losses (`proposals_given_up`) next to the queue and link drops. If the Linux runs show them under a realistic load, this card comes back with the number.
- **Cost:** XS within D-112.
- **Risks:** until then — D-112 comes after D-113 and D-114 — a publisher can wait 30 s after such a loss.

### B. Acknowledged forwarding: the sequencer confirms what it queued
- **What we do:** every forwarded proposal carries a per-node sequence number. The sequencer acknowledges receipt, or reports a refusal, over the link. The origin then resends a refused one at once, within the slot, before anything after it. That needs a dedup window at the sequencer per origin, and a wire change (`WIRE_VERSION` 2).
- **Cost:** M–L. The protocol, the wire, the simulation, and a new invariant to prove: no reorder across a refusal.
- **Limits:** the sequencer's dedup window is volatile and lives per slot (a fresh `Sequencer` at every takeover or handoff), so the outbox's slot-closure rule stays the only dedup across slots.
- **Check:** a takeover-simulation scenario with refusals (`max_queue` small) and link loss, asserting each node's proposals commit in order, with no gap followed by a reordered resend.
- **Risks:** new protocol state beside the log; it decides nothing durable, but it is a second place where order is kept.

### C. Close the publisher at once on a give-up
- **What we do:** when the outbox gives an entry up, the node tells the gateway, which closes that publisher's connection with a reason; the client reconnects and resends at once instead of after 30 s. No protocol change. You weighed the same mechanism as D-109 C, for slot changes.
- **Cost:** S. A gateway call keyed by the proposal's origin and request.
- **Risks:** a reconnect burst when a queue overflows under load — exactly when load is highest.

### D. Leave it as it is
- **Cost:** none.
- **Risks:** as in A, without the number.

**Recommendation.** A. C if the number shows these losses and a 30 s wait is unacceptable; B only if reconnects are unacceptable too.

**Answer:** `D-115: A` · `D-115: B` · `D-115: C` · `D-115: D` · `D-115: discuss`

````

````markdown
## D-116. A failing node test leaves its nodes running

**Context.** Seen once while checking D-109 by mutation. Two node tests failed after 31 s, but the test process took about 7 more minutes to exit.
- **Why:** `Node` does not stop when dropped (its documented contract). A test that panics never reaches `Cluster::shutdown`, and its nodes keep running until the runtime is torn down.
- **Scope:** a passing run is unaffected. A failing one is slow to report, and in the container loop of D-113 it would stretch each failing run.
**Vision.** CLAUDE.md §17 (failure paths tested); D-113 (diagnostics kept, nothing retried).

### A. Stop the nodes when the test `Cluster` is dropped — recommended
- **What we do:** a `Drop` on the test helper `Cluster` sends each node its stop, without awaiting. Test code only; `Node`'s contract stays as it is.
- **Cost:** XS.
- **Risks:** none in production code.

### B. Leave it
- **Cost:** none.
- **Risks:** a failing run in D-113's loop takes minutes longer; no information is lost.

**Recommendation.** A, before D-113's 100 runs.

**Answer:** `D-116: A` · `D-116: B` · `D-116: discuss`

````

````markdown
## D-117. The outbox is not in any deterministic simulation

**Context.** D-109 B's ordering, dedup and fencing rules live in the tokio driver (`node::Loop::pump`, `node/src/outbox.rs`).
- **What checks it now:** unit tests, two loopback tests (a handoff, and a sequencer stopped), and a mutation check.
- **What does not:** no simulation runs it. Partitions, message loss and a fenced owner that is still alive with forwards in its queue (unified-stream §5, {A}|{B,C}) are untested for it.
- **Rule broken:** unified-stream §1 and §7 ask that correctness be shown by deterministic simulation, with scenarios in every step. This is recorded as a deviation in ADR-0008.
**Vision.** unified-stream §1, §5, §7; ADR-0007; D-109 B.

### A. Move the outbox into `gateway`, drive it in the gateway and takeover simulations
- **What we do:** the gateway already owns its proposals' identity (origin, request) and sees every committed effect. It would hold the outbox and decide what to send, given the slot and owner the node tells it, and whether that slot has a committed record. `node` would only carry out the sends. `sim/tests/gateway.rs` and the takeover simulation then add these scenarios:
  - a handoff, and a takeover by silence;
  - a fenced owner still alive with forwards queued;
  - link loss and refusals.
  
  Each scenario asserts commit-once, per-session order, and no older-slot commit after a newer one.
- **Cost:** M. The gateway's interface gains the slot/owner input; the node's pump moves over.
- **Risks:** the gateway's tests change shape; the simulation needs a per-node forwarding model it does not have yet.

### B. Move the outbox into `cluster`, beside `Sequencer`, driven by the takeover simulation — recommended
- **What we do:** `cluster` already knows the slot, the owner and the committed slot table. The outbox would live there and decide what to send. The existing takeover simulation already has partitions, loss and a live fenced owner ({A}|{B,C}), so it drives the outbox with no new forwarding model. Each scenario asserts commit-once, per-origin order, and no older-slot commit after a newer one.
- **Cost:** M. `cluster` learns which records a node proposed (opaque bytes; identity by equality, as now).
- **Risks:** `cluster` grows a client-facing concern; per-session order then rests on per-origin order, as it does now.

### C. Keep it in `node`; model it in the takeover simulation as a copy
- **What we do:** the takeover simulation gets its own outbox model with the same rules.
- **Cost:** S–M.
- **Risks:** two implementations that can drift; the simulation then checks the model, not the code.

### D. Keep the deviation as recorded
- **Cost:** none.
- **Risks:** the rules that keep order and prevent duplicates across slot changes are shown only on loopback, under no partition. D-109 then stays open against its own condition (fencing of a live owner unverified).

**Recommendation.** B: it reuses the simulation that already has the faults, without a new model. A if the outbox should stay with the proposals' owner. Before D-112's measurements, which will exercise it under load.

**Answer:** `D-117: A` · `D-117: B` · `D-117: C` · `D-117: D` · `D-117: discuss`

````

````markdown
## D-118. A takeover by silence seen in the end-to-end test under load

**Context.** One gate run of the D-106 test failed: when SIGUSR1 came, node 0 was no longer the sequencer ("this node is not the slot's sequencer").
- **When it happened:** the run shared the machine with the full workspace validation and the Linux container.
- **Likely cause, unconfirmed:** node 1, just restarted over its data, took the slot over by silence, which the protocol allows when heartbeats are late.
- **What the test assumes:** node 0 still sequences at that point.
- **The other runs:** five passed before it, and three after.
- **Diagnostics:** only node 0's log was kept (in the hook's report): node 0 started at 16:08:38.5 and refused the handoff at 16:08:43.5. Node 1's log was not kept.
- **What I did:** I briefly relaxed the test to accept either node, then reverted it: you made this test a regression barrier, and a relaxation needs your decision.
**Vision.** D-106 (mandatory regression barrier); D-113 (diagnose; no retries); unified-stream §1 (time triggers, never decides).

### A. Treat it as a D-113 lead: keep every node's log on failure, then decide — recommended
- **What we do:** the server test copies all three nodes' logs aside when it fails. D-113's 100 container runs include the server test, under the same parallel load, and the same loop runs the test at a6986ea, before D-109 B, to tell a regression from a takeover that was already there. A recurrence with node 1's log shows whether it was a takeover by silence, and why.
- **Cost:** XS, plus machine time inside D-113.
- **Outcome:** a confirmed takeover by silence comes back to you as a card (widening the barrier is your decision); anything else is a defect to fix.

### B. Accept that either running node may sequence there
- **What we do:** the test asks both nodes for the handoff and checks that one hands over and the other refuses, as I had briefly done.
- **Cost:** XS.
- **Risks:** it widens what the barrier accepts before the cause is known.

### C. Leave the test as it is
- **Risks:** a flaky barrier under load, with nothing kept to explain it.

**Recommendation.** A.

**Answer:** `D-118: A` · `D-118: B` · `D-118: C` · `D-118: discuss`

````

````markdown
## D-119. Is D-109 closed?

**Context.** D-112 A is ordered "after D-109 is closed". D-109 B's conditions, and where they stand:
- **No proposal republished without dedup and confirmation of the result:** met. Confirmation is the identical committed record; a resend happens only after the slot it went to is closed.
- **Order, fencing and commit rules unchanged:** met on loopback. The fencing check is structural: committed slots never go backwards. A fenced owner that is still alive is not tested (D-117).
- **No loss of an accepted proposal:**
  - met for proposals in flight at a handoff or a failure;
  - not met for in-slot give-ups after a full queue or a lost link tail (D-115). The publisher is never acknowledged; it is closed at `commit_timeout`.
- **Within the existing limits:** a count bound, `max_queue` = 1 024 entries per node. That is the sequencer's own bound, without a byte budget of its own: up to 256 MiB at worst.

**Vision.** D-109 B; D-112 A; unified-stream §1, §7; CLAUDE.md §7.

### A. Closed, with D-115 and D-117 open — recommended
- **What we do:** D-109 counts as closed. "Accepted" means acknowledged to a client: none is lost, and none was acknowledged before its commit. The next steps are D-113, then D-114, with D-117 before D-112's measurements.
- **Risks:** the live-fenced-owner case stays unverified until D-117.

### B. Open until D-117 is done
- **What we do:** D-117 next, before D-113.
- **Cost:** M first.
- **Risks:** the order you set (D-113, D-114 next) shifts.

### C. Open until D-115 and D-117 are done, and the outbox has a byte bound
- **What we do:** D-117 next; then an outbox byte budget (the gateway's `max_total_pending_bytes` plus a fixed share for lifecycle proposals); then D-115 once counted.
- **Cost:** M–L before D-113.

**Recommendation.** A.

**Answer:** `D-119: A` · `D-119: B` · `D-119: C` · `D-119: discuss`
````


````markdown
## D-120. Proposals stranded in an open slot, and the resend burst after a slot change

**Context.** The takeover simulation now runs the real outbox (D-117 B) and found two gaps. Neither is a safety failure: no duplicate, no reorder, nothing committed after fencing. Both are liveness gaps.
- **Stranded:** a node's forward is lost — on the link, refused by the sequencer's full or sealed queue, or dropped by a node that does not lead its slot — and nothing of that node commits after it while the slot stays open. Nothing shows it lost, and resending it into the same slot could commit it twice. So it waits: until a later proposal of the same node commits (then it is given up and counted), or the slot changes (then it is sent again). Its publisher is closed after `commit_timeout` (30 s); the client's resend unblocks it.
  - In 2 000 seeds: 16 741 stranded of 1 187 814 proposals. Every one had a lost forward; any other wait fails the run.
- **Resend burst:** after a slot change every node sends its whole outbox again at once. The sequencer's queue (`max_queue`, 1 024, fixed in 7a) is as large as one node's outbox (also 1 024), so three full outboxes overflow it, and the overflow is lost as above.
- **What is counted now, and what is not:**
  - counted: refusals at a full outbox (`Status::refused`), give-ups (`Status::given_up`), forwards a full or sealed sequencer queue refused (`Voter::forwards_refused`, not yet in `Status`);
  - not counted: forwards lost on a link, or dropped by a node not leading their slot. D-112's metrics must cover these paths (D-115 A).
**Vision.** D-109 B, D-115 A (measure first; a forwarding acknowledgement is not a durable commit), D-119 A (the outbox bound becomes a real resource bound; explicit backpressure or fail-fast, never silent loss); CLAUDE.md §7, §8.

### A. Cap each node's outbox at `max_queue / voters` — recommended
- **What we do:** each node holds at most 341 proposals with three voters, so every outbox together fits the sequencer's queue and a resend burst cannot overflow it. The sequencer's bound and memory stay as they are. A node at its cap refuses new proposals, counted (`Status::refused`), and its publisher waits as at any refusal: explicit backpressure.
- **Strandings:** counted in D-112 (the oldest sent entry's age in heartbeats). The stranding itself is decided on that number.
- **Check:** the takeover simulation, with its assertions unchanged; refused forwards at the sequencer should fall to those from a sealed queue.
- **Cost:** S. A fixed 7a limit changes (`docs/configuration.md`); a behaviour change by CLAUDE.md §8.
- **Risks:** a third of today's per-node admission, at a byte cost still unbounded per entry (up to 341 × 256 KiB per node).

### B. Grow the sequencer's queue to voters × `max_queue`
- **What we do:** the sequencer's queue takes every outbox at once.
- **Cost:** S. A fixed 7a limit and the configuration digest change.
- **Risks:** a count-only bound tripled: a worst case of 3 072 × 256 KiB = 768 MiB at the sequencer, against D-119 A's priority on bounding memory.

### C. Reopen D-115: the sequencer acknowledges receipt of each forward
- **What we do:** you deferred this in D-115 A until D-112 measures the losses. The new evidence is the 16 741 strandings in the simulation; the decision should still wait for D-112's numbers from a real workload. Each forward would carry a per-node number, and the sequencer would report it queued or refused. A refused one is sent again at once, in order. A receipt is not a commit: confirmation stays the committed record.
- **Cost:** M–L; a wire change, new protocol state, simulation scenarios.

### D. Leave both as recorded
- **Risks:** the 30 s wait on loss stays, and every slot change under load overflows the sequencer's queue.

**Rejected.**
- *Give up a sent entry after K heartbeats and close its publisher at once:* this revives D-109 C and D-115 C, which you rejected, without new evidence. Without the immediate close it only frees outbox room. It would also redefine "given up" as "may still commit".

**Recommendation.** A. C once D-112's numbers show strandings matter on a real workload.

**Answer:** `D-120: A` · `D-120: B` · `D-120: C` · `D-120: D` · `D-120: discuss`
````

````markdown
## D-121. Storage and transport tests leave their directories behind

**Context.** Found while checking D-116. `$TMPDIR` holds about 5 400 `straum-*` directories, about 32 MB:
- **`storage` tests:** about 4 800 of them (`straum-retention-*` and `straum-commit-*`), one or more per test case, left on passing runs too. Some come from proptest cases, which multiplies them.
- **`transport` tests:** 379 empty `straum-tls-*`.
- **The node and server tests:** they now clean up after themselves (D-116).

Nothing breaks: names carry the process and thread ids. But the directories grow with every validation run, and in D-113's loops with every run.
**Vision.** CLAUDE.md §17 (failure paths tested); D-116 A (cleanup on success and failure).

### A. Each test removes its directory in a guard dropped at its end — recommended
- **What we do:** the storage and transport test helpers return a guard that removes the directory on drop, success or failure. Same shape as D-116. Test code only.
- **Cost:** S.
- **Risks:** none in production code; a failing storage test no longer leaves its files to inspect. Its assertion message is the diagnostic, as today in the node tests.

### B. Leave it
- **Risks:** about 30 MB more per few hundred runs, in a directory macOS clears only on reboot.

**Recommendation.** A.

**Answer:** `D-121: A` · `D-121: B` · `D-121: discuss`

````

````markdown
## D-122. A restarted node takes a live sequencer's slot over, because the sequencer redials slowly

**Context.** D-118's failure recurred during a gate run while D-113's loops loaded the machine. This time every node's log was kept (D-118 A).
- **What the logs show:** node 1 restarted at 21:21:21.644 and was the sequencer 0.66 s later. Node 0 had been sequencing and was alive throughout. So node 1 took the slot over by silence. The end-to-end test, which requires node 0 to sequence there, failed.
- **Likely mechanism, from the code, not yet measured:**
  - While a peer is down, a node redials it with a backoff doubling from 50 ms to 1 s (`node/src/link.rs`, `BACKOFF_MAX`).
  - Node 1 was down for about 4 s, so node 0's next dial could be up to 1 s away when node 1 came back.
  - A restarted node that hears nothing from the slot's owner for `takeover_ticks` heartbeats (3 × 50 ms) starts a takeover.
  - The sequencer's silence toward a restarted peer can therefore outlast the takeover window. Under load the window is crossed more often.
- **Not a D-109 regression:** the link code predates D-109, and D-113's loop of the server test at a6986ea fails the same way — node 0 no longer the sequencer when SIGUSR1 comes (5 of 100 at a6986ea, 6 of 100 on the current tree, D-113 A).
- **Is it unsafe?** No. The takeover follows the protocol, with fencing and the committed prefix intact. But a needless takeover after every restart costs a slot change and its stall, and it breaks the barrier test.
**Vision.** unified-stream §1 (time only triggers a transition); D-118 A (keep the assertion, find the cause); D-85 liveness contract.

### A. Cap the redial backoff below the takeover window — recommended
- **What we do:** a link's redial backoff never exceeds one heartbeat. A peer that comes back hears the sequencer within one heartbeat, inside the window of `takeover_ticks`. A redial costs one failed `connect` per heartbeat per down peer, to loopback or the LAN.
- **Check:**
  - a node test that stops a replica for longer than the old 1 s cap and restarts it, asserting the slot stays with its owner;
  - the D-106 test under the D-113 loop.
- **Cost:** S.
- **Risks:** more connect attempts toward a dead peer, one per heartbeat.

### B. A restarted node waits before counting silence
- **What we do:** after a start, a node counts silence only after it has heard the owner once, or after `takeover_ticks × MAX_PATIENCE`.
- **Cost:** S–M; touches `cluster::Voter`, with a simulation scenario.
- **Risks:** a cluster restarting with its sequencer dead waits longer for a takeover.

**Rejected.**
- *Accept it and relax the barrier:* you rejected that in D-118 A.

**Recommendation.** A; B as well if the loop still shows takeovers after A.

**Answer:** `D-122: A` · `D-122: B` · `D-122: discuss`
````


````markdown
## D-123. Keep-alives under outbox backpressure

**Context.** D-120 A made a full outbox backpressure: while a node's outbox holds its share, the node reads none of its MQTT clients.
- **What that includes:** subscribers' PINGREQs and PUBACKs, from clients that publish nothing.
- **What can follow:** the gateway closes a client after 1.5 × its keep-alive with no packet received. A client idle through a long full spell can be closed for silence the node caused.
- **How long a full spell lasts:** as long as commits stall — for example, no majority. In that case `commit_timeout` (30 s) closes the publishers anyway.
- **Unmeasured:** how often a spell outlasts a keep-alive. D-112 can count reads withheld and how long.
**Vision.** D-120 A ("failure or backpressure, never silent loss"); CLAUDE.md §8; MQTT 3.1.1 §3.1.2.10 and MQTT 5 §3.1.2.10 (keep-alive).

### A. Measure first, in D-112 — recommended
- **What we do:** D-112 counts spells of withheld reads and their length. This card comes back with the number if spells approach a keep-alive.
- **Cost:** XS within D-112.
- **Risks:** until then, an idle client can be closed during a long stall. It reconnects; nothing it was promised is lost.

### B. Pause only reads that would propose
- **What we do:** keep reading every client, and hold every packet that would propose — PUBLISH, SUBSCRIBE, UNSUBSCRIBE and PUBACK, since a PUBACK is itself a log record (`Proposal::Ack`) — until the outbox has room. Only PINGREQ and DISCONNECT pass.
- **Cost:** M. The gateway holds at most one decoded packet per connection while it waits, counted in the connection-buffer budget.
- **Risks:** a subscriber that acknowledges deliveries is held as a publisher is; only idle clients are helped.

### C. Suspend keep-alive expiry while reads are withheld
- **What we do:** the gateway does not count a client's silence while the node is not reading it. MQTT 3.1.1 and 5 §3.1.2.10 require a disconnect after 1.5 × keep-alive with no packet *received*; this reads "received" as "read by the gateway". A clock only triggers the close, it decides nothing in shared state (§1).
- **Cost:** S.
- **Risks:** a client that really died is noticed later, after the spell.

**Recommendation.** A. C if D-112 shows spells near a keep-alive.

**Answer:** `D-123: A` · `D-123: B` · `D-123: C` · `D-123: discuss`
````


````markdown
## D-124. The Linux run: the cost of measuring first, then the measurements

**Context.** D-112 ends with Linux measurements, with the safety and liveness checks as gates. The metrics added cost to the hot path, and it is unmeasured:
- an `Instant::now()` per own proposal, per batch append, per durable report and per pass of the node loop;
- two outbox reads per applied record;
- three counter increments in the gateway, per publish, PUBACK and delivery.

`Measured` always runs, even with no metrics listener, so the same build cannot be compared with metrics off. No Linux host has been available so far (D-84); container numbers are preliminary.
**Vision.** CLAUDE.md §11 (baseline, one variable, ≥ 5 alternating rounds, medians; macOS and containers show direction only), §17 (hot-path changes carry a performance report); D-112 ("no measurement overhead that distorts the result"); D-84 A.

### A. Overhead against 071db00 first, then the gated measurements — recommended
- **What we do:**
  1. On a quiet Linux host, run `node/benches/cap.rs` at 071db00 and at this change, alternating, at least 5 rounds each. Compare medians of throughput and failover time, which is what it measures; the noise floor is 3 %. `cap.rs` proposes through a node handle, not over MQTT, so the gateway's counters are not on its path: an MQTT workload (publishers and subscribers through the server, PUBACK latency at the client) goes into the same comparison.
  2. If the overhead is under the floor, take the D-112 measurements with the metrics on. Gates before each run: the takeover simulation, the node tests, the e2e test, and your D-123 criterion — the load handoff test green, fencing verified, and no unexplained proposal loss. A failure there becomes a go/no-go card, not a number.
  3. If it is above, the cost is found by profile before anything else is measured.
- **Cost:** M, plus an MQTT load generator for step 1 and access to a Linux host.
- **Risks:** without a host, only container numbers, marked preliminary.

### B. Make measuring switchable first, then compare on one build
- **What we do:** `Measured` is skipped when the node serves no metrics and nobody asks for them. The same binary is then run with metrics on and off.
- **Cost:** S, plus A's runs.
- **Risks:** a branch per event to switch it, and a node that measures nothing unless asked: a stall seen only after the fact cannot be explained.

**Rejected.**
- *Measure with the metrics on, without the overhead check:* D-112 forbids measurement overhead that distorts the result, and CLAUDE.md §11 asks for a baseline first.

**Recommendation.** A. B only if A shows an overhead above the floor that cannot be removed.

**Answer:** `D-124: A` · `D-124: B` · `D-124: discuss`

````

````markdown
## D-125. Stage 4 incomplete: handoffs are not timed, and fencing is not told from handover

**Context.** D-112's stage 4 asks for the reason, duration and result of a role transfer, and for fencing. Covered:
- takeovers by silence: started, completed (timed), abandoned;
- successions by `SlotEnd`: counted;
- slots led;
- times stepped down.

Missing:
- a handoff's duration, from the old sequencer sealing its slot until the successor leads;
- `stepped_down` does not separate a handover from fencing by a newer slot.
**Vision.** D-112 stage 4; unified-stream §2.4.

### A. Complete it now
- **What we do:**
  - the old sequencer times from `hand_off` until it steps down after `SlotEnd` commits;
  - the successor times from its succession until it leads;
  - `stepped_down` splits into `handed_over` and `fenced`, decided by whether the slot's `SlotEnd` was committed;
  - a voter test for each.
- **Cost:** S–M; small changes in `cluster::Voter` and the node.

### B. After D-124's first Linux run — recommended
- **What we do:** as A, scheduled after the overhead check. That run needs commit, outbox and replication; a handoff's duration matters once failover is measured.
- **Cost:** as A, later.
- **Risks:** the first Linux run cannot attribute a handoff's cost to the handoff.

### C. Leave it as recorded
- **Risks:** stage 4 stays partial.

**Recommendation.** B. A if the first Linux run includes failover.

**Answer:** `D-125: A` · `D-125: B` · `D-125: C` · `D-125: discuss`

````

````markdown
## D-126. A metrics listener over the network, or in-process only

**Context.** D-112 asked for metrics fit for the Linux benchmark. I also added an HTTP listener (`STRAUM_CLUSTER_METRICS_LISTEN`):
- off by default;
- loopback unless an unauthenticated network is acknowledged (D-107);
- at most 4 scrapes at once, with 2 s timeouts, and a test.

It is a new public surface you did not ask for. The benchmark harness can read `Node::metrics` in-process instead.
**Vision.** D-112; D-107 (exposure); CLAUDE.md §15 (metrics nobody can read are metrics nobody acts on); "do not do work the task did not ask for".

### A. Keep the listener as built — recommended
- **What we do:** keep it. A three-process run (the server, the e2e test, a Linux host) has no other way to read the metrics.
- **Cost:** none now.
- **Risks:** one more listener to secure when D-110 brings credentials and TLS.

### B. Remove it; in-process only
- **What we do:** drop the listener and the setting; the benchmark reads `Node::metrics`.
- **Cost:** S.
- **Risks:** a running server cannot be observed until D-110.

**Recommendation.** A.

**Answer:** `D-126: A` · `D-126: B` · `D-126: discuss`
````


````markdown
## D-127. A quiet Linux host for D-124

**Context.** D-124 A needs its numbers from a quiet Linux host. The harness is ready:
- `scripts/overhead.sh` alternates a baseline and a candidate tree;
- it runs `cap` (throughput, failover) and the new `server/benches/puback.rs` (MQTT throughput, publish→PUBACK p50 and p99, server CPU per PUBACK).

Its first run, in the container on the development Mac, put every median within 3.3 %, inside that machine's 10 % noise floor. That result is preliminary (`docs/baselines/apple-m1-dev.md`). No Linux host has been available (D-84). The accepted result and the gated D-112 measurements wait for one.
- **Not covered yet:** the container run compared metric collection only. The benches run with no metrics listener, so nothing was scraped during the rounds. The cost of scraping — the listener on, polled every few seconds — is part of D-124's step 4 on the same host.
**Vision.** CLAUDE.md §11 (accepted numbers from a quiet Linux host, never a container on macOS); D-124 A; D-84 A.

### A. A bare-metal Linux machine: one you have, or rented for a few hours — recommended
- **What we do:**
  - first, a machine you already own or can borrow (a spare box or a home server) costs nothing; otherwise rent one;
  - a dedicated machine with no VM neighbours (Hetzner AX, OVH, Equinix Metal or similar), with the CPU and frequency recorded and cores pinned as `docs/benchmarks.md` §6 describes;
  - first measure the host's own noise: the same build run against itself, its spread below the 3 % floor, with the governor and pinning recorded;
  - then `scripts/overhead.sh` (it exists; 5 rounds), then the D-112 measurements under the D-123 gates, with a scraper polling the metrics listener;
  - record the results in `docs/baselines/<machine>.md`.
- **Cost:** nothing on your own machine, or a few hours of rental in your account. The script exists; you run it, or give me access.
- **Risks:** none to the code; the host must be quiet, which a rented bare-metal machine usually is.

### B. A cloud VM (a dedicated-CPU instance)
- **What we do:** as A, on a VM with dedicated vCPUs.
- **Cost:** lower and quicker to get.
- **What it settles:** CLAUDE.md §11 asks for a quiet Linux host, and a VM is not one. Its numbers would stay preliminary — better than the Mac's, and they do not close D-124.
- **Risks:** virtualised time and noisy neighbours on the host.

### C. Keep the container result as the only one for now
- **Risks:** D-124 stays open; no claim about the cost of measuring or about throughput can be made.

**Recommendation.** A.

**Answer:** `D-127: A` · `D-127: B` · `D-127: C` · `D-127: discuss`
````


````markdown
## D-128. Handoff and takeover in the Linux run's diagnostics

**Context.** D-127 step 4 asks for handoff and takeover to be measured separately, not mixed with fencing.
- **What the runbook does now:** its diagnostic run is 60 s of steady load. It injects no handoff and no takeover, so its counters show only the startup quirk: the first owner starting slot 1 counts as a takeover.
- **Where failover appears at all:** only in `cap`'s failover p50 — a stopped sequencer, measured inside one process.
- **Planned handoffs:** never exercised.
- **Server-side timing of handoffs:** D-125, after the first Linux run (D-125 B).
**Vision.** D-127 A step 4; D-125 B (handoff, takeover and fencing are different events); D-112 stage 4.

### A. Inject both into the diagnostic run now; time them from the client — recommended
- **What we do:** `puback` gains `STRAUM_BENCH_EVENTS=handoff,kill`. Midway through the run it sends SIGUSR1 to the sequencer (a planned handoff). Later it kills the sequencer with SIGKILL (a takeover) and restarts that node. For each event it reports, apart:
  - the longest gap between PUBACKs around the event;
  - the PUBACKs lost to it: always 0 for a handoff, and for a kill only those of connections to the killed node;
  - the takeover and succession counters before and after.
  
  No server change; the server-side split is still D-125.
- **Cost:** S–M.
- **Risks:** client-side timing only, which includes the clients' own reconnects.

### B. Leave it with D-125 B, after the first Linux run
- **What we do:** the first Linux run is steady load only; handoff and takeover come with D-125's server-side timing.
- **Risks:** the first run cannot say what a handoff or a takeover costs a client.

**Recommendation.** A. It is client-side, needs no server change, and gives step 4 what it asks for in the first run.

**Answer:** `D-128: A` · `D-128: B` · `D-128: discuss`

````

````markdown
## D-129. When is a Linux host's noise low enough?

**Context.** You close D-124 only when the host's noise is low enough to read the result. The runbook measures that noise: this tree against itself, every median with its spread. But no number says what "low enough" is.
- CLAUDE.md §11 treats differences below 3 % on a pinned, quiet Linux host as noise.
- The noise run is what shows whether a given host is that quiet.
**Vision.** CLAUDE.md §11 (noise floor, initial; "if variance is larger than the effect, fix the environment"); D-127 A; D-124.

### A. 3 % for both — recommended
- **The rule:** a host is accepted when, in the noise run, the two copies' medians differ by less than 3 % on every quantity, and each copy's spread (max − min over min) is below 3 %. Otherwise the run stops after step 1, and steadier conditions or more rounds come first.
- **What it settles:** an overhead within 3 % is "within this host's noise". It is never "zero".

### B. The noise run itself sets the floor
- **The rule:** whatever spread the noise run shows becomes the floor for that host. Overhead below it is noise.
- **Risks:** a noisy host passes with a wide floor that hides a real cost.

**Recommendation.** A. It is §11's floor, and B lets a noisy host pass.

**Answer:** `D-129: A` · `D-129: B` · `D-129: discuss`
````


````markdown
## D-130. Failover time cannot pass D-129's 3 % gate as measured

**Context.** D-129 A gates a host on every quantity: its two copies' medians within 3 %, each copy's spread across rounds below 3 %.
- **Why failover cannot pass:** `cap` measures its failover p50 from 5 failovers per round, and the distribution is bimodal. Most failovers take about 300 ms, some about 1 975 ms. In the container run the spread was over 500 %.
- **Why this is not just the Mac's noise:** a failover that misses its first takeover waits a further round of `takeover_ticks × patience`, by design (D-85, D-88). A quiet Linux host will likely still show two modes, and the median of 5 can land in either.
- **Consequence:** as decided, every host fails the noise gate on this quantity alone, and the accepted run never starts.
**Vision.** D-129 A; D-124 A (failover time among the quantities); CLAUDE.md §11 ("a latency is a gauge of the latest value… the average of a bimodal latency is a number that never happened").

### A. Report failover as a distribution, and gate on the others — recommended
- **What we do:**
  - this amends D-129 A's "on every quantity": the gate drops failover time, and `log.md` records the change;
  - `cap` runs more failovers per round (50 instead of 5) and reports the share within 1 s (D-84 A's own criterion) and p50 and p90 apart;
  - the D-129 gate applies to throughput, PUBACK latency and CPU per PUBACK;
  - failover is compared baseline against candidate by its share within 1 s and its two modes, with their spread reported, not gated.
- **Cost:** S; the run takes longer, about 50 × 0.3–2 s per round.
- **Risks:** failover is not held to 3 %. A real regression in it shows as a shift in the share or the modes, not in a median.

### B. Keep it gated, with many more failovers
- **What we do:** enough failovers per round (200 or more) that the p50 settles within 3 %.
- **Cost:** M in time: hours per run.
- **Risks:** a bimodal median may never settle within 3 %, however many failovers.

### C. Drop failover from D-124's overhead comparison
- **What we do:** metrics add no work to the takeover path that differs from the steady path, so failover is left to D-125's measurements.
- **Risks:** it narrows what you set in D-124 ("throughput, publication-to-PUBACK latency, and failover time").

**Recommendation.** A.

**Answer:** `D-130: A` · `D-130: B` · `D-130: C` · `D-130: discuss`

````

````markdown
## D-131. A restarted node cannot catch up under load

**Context.** Found by D-128's event run, in the container, preliminary. A sequencer is killed under about 80 000 publishes/s and started again 2 s later.
- **What happens:**
  - The node rejoins as a replica and must catch up before its gateway serves clients: it refuses them until its own reset record is applied.
  - In 5 runs of 6 its clients gave up after 30 s. Nothing was lost or duplicated; their in-flight publishes stayed unacknowledged.
  - With the load stopped, its commit moved 62 143, 156 865 and 68 499 positions in 10 s. That is a catch-up of about 6 000–16 000 positions a second, far below the write rate.
- **Likely mechanism, from the code, not measured:**
  - catch-up moves at most `read_bytes` (64 KiB) per round (D-81 A);
  - if a round is paced by the heartbeat (50 ms) rather than by the replica's reply, that is about 1.3 MB/s, roughly 13 000 records a second — which matches.
- **Already known in part:** D-85 noted that a replica outside the committing majority falls behind under load, and flow control on replica lag (D-89) is off by default.
- **Not a safety issue:** the committed log and fencing hold. A node that falls further behind than `retain_bytes` is lost and needs a state copy (step 7b).
**Vision.** unified-stream §1 (predictability before peak speed); D-81 A, D-83 A (`read_bytes`), D-85, D-89 (flow control), step 7b; D-127 A step 4.

### A. Measure first: confirm the mechanism by `read_bytes` — recommended
- **What we do:** run the event scenario at `STRAUM_READ_BYTES` = 64 KiB, 512 KiB and 8 MiB (half a frame, the most allowed), and read the catch-up rate from the replica's commit over time. It runs inside the Linux diagnostics step (D-127 A step 4). Run in the container first, the numbers are preliminary, as everything there is. `puback` needs one passthrough setting for the server's `STRAUM_READ_BYTES`.
  - If the rate scales with `read_bytes`, the round is paced by the heartbeat. The fix is then B.
  - If it does not, the cost is elsewhere (applying, the disk), and a profile on Linux comes first.
- **Cost:** S; settings already exist, no code change.

### B. Pipeline the catch-up: the next read on the replica's reply, not on the heartbeat
- **What we do:** in `cluster::Sequencer`, a replica's progress report triggers the next read at once, up to the replication window. The heartbeat stays the resend timer only. This is a sans-IO change, with takeover-simulation scenarios for a replica far behind.
- **Cost:** M.
- **Risks:** more reads at once on the sequencer's disk; bounded by the window.
- **Order:** after the accepted D-124 run. It changes the code that run compares (071db00 against this tree), and CLAUDE.md §11 allows one variable at a time.

### C. Turn flow control on (D-89)
- **What we do:** the sequencer holds new batches while an attached replica is more than `L` behind, for at most `T` heartbeats.
- **Risks:** it slows every client to let one replica catch up; values not measured. It does not help a node that was down and is not attached.

### D. Defer: record it and change nothing until D-124's accepted run
- **What we do:** keep the finding in ADR-0008. The mechanism comes from a container only, and step 7b's state copy is the planned path for a replica too far behind (unified-stream §6 order).
- **Risks:** a node restarted under sustained load stays unavailable to its clients until the load drops.
- **Needs your view first:** may a node behind refuse its clients indefinitely under load before 7b, or does "predictability before peak speed" (unified-stream §1) require a bound on catch-up time? If the latter, D is not enough.

**Recommendation.** A, inside the Linux diagnostics. B if A shows the heartbeat paces it, after the accepted D-124 run.

**Answer:** `D-131: A` · `D-131: B` · `D-131: C` · `D-131: D` · `D-131: discuss`
````


````markdown
## D-132. The catch-up contract: making "no progress" diagnosable

**Context.** D-131 set a contract for a lagging replica before the state copy (step 7b). Checked against the code:
- **Holds — no early service:** a restarted node refuses every CONNECT at once with `ServerUnavailable` until its own reset record is applied. A client gets an explicit refusal, never an unbounded wait at the broker; any wait is the client's own retry loop.
- **Holds — bounded waits for clients of a node that is behind:** a node that is serving but behind acknowledges nothing before its record is applied locally, and `commit_timeout` (30 s) closes a publisher whose publish does not commit. The wait is bounded and the close explicit.
- **Holds — TCP reachability is not the right to serve:** serving requires the node's own committed reset to be applied.
- **Missing — a diagnosable state when there is no progress:** a replica that is behind cannot tell how far, because the commit point it learns is capped at its own log's end. Nothing reports a catch-up that has stalled. The D-128 runs showed a restarted node behind by 48 000–146 000 positions, visible only from outside, by comparing nodes.
**Vision.** D-131 (the contract); unified-stream §1 (predictability before peak speed; time triggers, never decides); D-112 (metrics); D-89 (lag is measured at the sequencer).

### A. Report it from both sides, decide nothing — recommended
- **What we do:**
  - **The sequencer**, which knows each replica's durable position, exports per replica how far it is behind and how long since that position last moved (`straum_replica_lag`, and a new `straum_replica_stalled_seconds`).
  - **The replica** exports the sequencer's commit point as heard, uncapped, and how long since its own log's end last moved.
  
  A stall is then a state an operator, and the Linux run, can see, with a threshold for alerting. Nothing in the protocol acts on it.
- **Cost:** S–M; `cluster` exposes the heard commit and the last-progress times; `node::metrics` exports them, with tests.
- **Risks:** none to safety; two more gauges.

### B. As A, and close the stalled replica's clients after a bound
- **What we do:** after N seconds with no progress, the stalled node closes its client connections with a reason, so they reconnect elsewhere.
- **Risks:** you asked for no hard timeout yet: a temporary overload could become a cycle of closes and reconnects. The bound is unmeasured.
- **Needs from you first:** the "established period" without progress, N seconds. A needs it as an alert threshold, B as its bound.

### C. Do nothing until the accepted D-124 and Linux run
- **Risks:** until then a stalled catch-up is visible only by comparing nodes from outside — what D-128's runs had to do — and the alert threshold, the "established period" without progress, stays undefined.

**Recommendation.** A. It makes the contract's missing point observable without a timeout that decides anything.

**Answer:** `D-132: A` · `D-132: B` · `D-132: C` · `D-132: discuss`

````

````markdown
## D-133. What limits catch-up — and D-131 B

**Context.** D-131 A's first measurement, in the container, one run per size, preliminary:
- **Under load:** a restarted node's lag shrank at every read size, by about 2 400–4 300 positions a second net.
- **After the load:** it caught up at 7 400, 4 500 and 7 100 positions a second, for 64 KiB, 512 KiB and 4 MiB.
- **The running nodes' PUBACK:** p50 1.15, 1.03 and 0.96 ms; p99 25.7, 27.6 and 17.0 ms. Commit latency per size was not read.
- **So:** the node catches up, slowly — about a minute under this load, longer than its clients' 30 s.

Read size did not show as the limiter in that one run per size. But 512 KiB was 40 % slower than both ends, so one run cannot establish a non-effect. Your step 4 asks for a decision on D-131 B, the next read on the replica's reply. Another suspect, from the code and not measured: the replication window of 256 positions in flight beyond the replica's durable report. At about 28 round trips a second, that is about 7 000 a second.
**Vision.** D-131 A (measure one variable at a time; no flow control at the same time); CLAUDE.md §11 (repeated measurement over one good number); D-124 (code under its comparison unchanged until its run).

### A. Repeat D-131 A, 3 rounds per size, before anything else — recommended
- **What we do:** the same kill scenario at 64 KiB, 512 KiB and 4 MiB, alternating, 3 rounds each, with the medians and spreads of the lag's rate and the recovery rate, and the running nodes' commit latency and PUBACK. Container first, preliminary.
- **Outcome:**
  - if the rates stay within each other's spread, read size is out and B comes next;
  - if they differ, read size matters and D-131 B is back in question.
- **Cost:** S; about 15 minutes in the container.

### B. Measure the replication window
- **What we do:** a different harness from `puback.rs`: in-process, as `node/benches/cap.rs` runs, where `window` is a `NodeConfig` field, so no server setting is added. A sequencer is stopped with `shutdown`, not killed, under load from node handles (no MQTT), then restarted. At a window of 256, 1 024 and 4 096, it gives the restarted node's lag under load, its recovery after, and the running nodes' commit latency. It gives no PUBACK. Flow control stays off.
- **Cost:** S; a bench beside `cap.rs`, no change to the server or to the code under D-124's comparison.

### C. Profile a catching-up replica on Linux
- **What we do:** `perf` on the restarted node during catch-up, to see where its time goes.
- **Cost:** M; needs the Linux host (D-127).

### D. D-131 B, the next read on the replica's reply: not now
- **What we do:** decide B only once A and B above have shown what limits catch-up. Even then, only after the accepted D-124 run, since it changes code under that comparison.
- **Risks:** none; restarted nodes stay about a minute behind under load meanwhile.

**Recommendation.** A, then B. D for D-131 B.

**Answer:** `D-133: A` · `D-133: B` · `D-133: C` · `D-133: D` · `D-133: discuss`

````

````markdown
## D-134. When does failover's p90 call for more failovers?

**Context.** You set D-130 so that if p90 changes substantially between rounds, the count of 50 failovers per round goes up. The runbook prints p90's spread between rounds, `(max − min) / median`, in `scripts/noise.awk`, marked "not gated". But nothing says what "substantially" is, so the trigger cannot be checked mechanically.
**Vision.** D-130 A; D-129 A (spreads defined the same way); CLAUDE.md §11.

### A. A p90 spread above 10 % doubles the count — recommended
- **What we do:** `noise.awk` flags failover p90 when its spread between rounds is above 10 %, and the runbook reruns step 2 with 100 failovers, then 200, until the flag clears or 200 is reached. Each count is reported.
- **Why 10 %:** a tail estimate from 50 events moves more than a median does; 3 % would rarely clear, and 10 % is the Mac's noise floor in CLAUDE.md §11.
- **Cost:** S; the run grows to about 200 × 1–2 s per round at worst.

### B. 3 %, as the other quantities
- **Risks:** with two modes, p90 may sit on either side of the mode boundary; it may never clear, and the run grows to its cap each time.

### C. No automatic trigger; the operator reads the spread
- **Risks:** a judgement call in every run, against D-129's mechanical rule.

**Recommendation.** A.

**Answer:** `D-134: A` · `D-134: B` · `D-134: C` · `D-134: discuss`
````


````markdown
## D-135. The replication window limits catch-up

**Context.** D-133's measurements, all preliminary (container):
- **Read size** (A, through the server, 3 rounds per size): no effect on catch-up. During catch-up the sequencer used a full core, while the replica was nearly idle.
- **Replication window** (B, in-process, 3 rounds per window):
  - 256, 7a's fixed value: a restarted replica recovers at about 6 400 positions a second and loses about 2 050 a second of its lag under load;
  - 1 024: about 30 000 a second after the load, 25 000 under it;
  - 4 096: it caught up under load within 20 s.
  
  The rounds agree within a few percent. The sequencer's commit latency showed no change, at coarse bucket bounds.
- **What the window is:** `cluster::Config::window`, the positions a sequencer sends a replica beyond what that replica has reported durable. It is part of the configuration digest. In 7a it is fixed at 256 (`node::experimental`).
- **Its memory cost, against what already bounds it:**
  - in positions, the window has no byte budget of its own;
  - what a link holds is already bounded per peer by `link_queue`, 1 024 frames of at most 8 MiB, whatever the window;
  - a larger window fills that bound sooner under catch-up, rather than adding a new unbounded amount.
- **Not explained:**
  - 4× the window gave about 12× the rate.
  - D-133 also deferred D-131 B "until the heartbeat and window hypotheses are tested". The heartbeat hypothesis is still untested: whether the sequencer sends the next window only at a heartbeat rather than on the replica's reply. That alone could explain the superlinear rate.
**Vision.** D-133 A→B, D-131 (one variable at a time; D-131 B deferred until the heartbeat and window hypotheses are tested; code under D-124's comparison unchanged before its run); CLAUDE.md §7, §8 (changing a default is a behaviour change), §11.

### A. Explain the mechanism and measure the cost, before any change — recommended
- **What we do:** in the same in-process mode, at windows 256, 1 024 and 4 096:
  - **Heartbeat pacing:** the replica's position against heartbeat ticks, and against heartbeat lengths of 25, 50 and 100 ms. If the rate scales with heartbeats per second, the window is sent once per heartbeat; that explains the 12×, and it is what D-131 B would fix.
  - **Cost:** bytes in flight per link (frames queued and sent but not acknowledged), and the sequencer's commit latency from raw samples, not buckets.
  
  Then a card with the mechanism and its numbers.
- **Cost:** S; benches only, no change to `cluster` or to the code under D-124.

### B. A separate, larger window for catch-up only
- **What we do:** a replica behind by more than the window is sent at a catch-up window, up to a byte budget — for example 64 MiB per peer, owned by `cluster`, as a new `NodeConfig` field. It is fixed in 7a beside the other values and added to CLAUDE.md §7's bounded-memory list and to `docs/configuration.md`. The steady-state window stays at 256, so a healthy replica's in-flight amount does not change. A sans-IO change with takeover-simulation scenarios.
- **Preconditions:**
  - A's finding: if heartbeat pacing explains the rate, D-131 B may make B unnecessary;
  - the accepted D-124 run, since B changes `cluster` code under that comparison.
- **Open for you:** whether a catch-up window is local policy on the sequencer, or part of the configuration digest like `window`. The digest decides whether nodes with different values may join one cluster.
- **Cost:** M.

### C. Raise the fixed window to 4 096
- **What we do:** one constant in `node::experimental`; the configuration digest changes.
- **Risks:**
  - a healthy replica's in-flight amount grows too, within the existing `link_queue` bound;
  - it changes code and the digest under D-124's comparison, so it also waits for that run;
  - it is chosen without the mechanism known.

### D. Nothing until the D-124 Linux run
- **What we do:** every option that changes code waits for that run anyway, and these numbers come from a container. Keep the findings as recorded.
- **Risks:** restarted replicas recover at about 6 400 positions a second under 7a's window meanwhile.

**Recommendation.** A. Then B or D-131 B, whichever A's mechanism points to, after the accepted D-124 run.

**Answer:** `D-135: A` · `D-135: B` · `D-135: C` · `D-135: D` · `D-135: discuss`
````

````markdown
## D-136. What sets the ~40 ms round trip that bounds catch-up

**Context.** D-135 A, preliminary (container, 27 in-process trials):
- **Supported:**
  - At windows 256 and 1 024 the heartbeat does not pace catch-up. Positions per heartbeat scale with its length; the rate per second does not. Still heartbeats are 0.2–7 %, so D-131 B would not change it there.
  - Catch-up fits window ÷ an effective round trip of about 40 ms. At 256: about 6 400/s, observed 5 600–7 300. At 1 024: about 25 600/s, observed about 30 000 in the runs that had not drained; the others' figure is a lower bound.
  - The "12×" of D-133 B is these rates less the write rate (about 4 500/s), not a superlinear window.
  - Bytes in flight at most: 0.13–0.45 MB at 256, 0.6–1.8 MB at 1 024, 2.7–6.9 MB at 4 096.
- **Not established:**
  - where the 40 ms comes from. It sets the rate at every window, so it decides between a larger window (D-135 B or C) and removing the delay itself;
  - the window's cost in commit latency: p99 is about 100 ms at every window, with no control trial without a lagging replica;
  - heartbeat pacing at 4 096: the 100 ms runs did not catch up, the 25 and 50 ms runs did.
- **Open:** the sequencer's commit p50 is bimodal. It is about 1 ms in 19 trials and 55–85 ms in 8: every trial at 25 ms with windows 256 and 1 024, and two at 50 ms.

**Vision.** D-135 A (close it only once the mechanism is investigated and the cost measured; the window stays at 256 until then); D-124 open; D-137 (whether D-124's candidate includes the measuring); CLAUDE.md §11 (find the stage; do not infer the bottleneck from the final number).

### A. Split the round trip into its stages, with a control, in the same mode — recommended
- **What we do:** in `cap`'s catch-up mode, time each boundary per Append to the lagging replica. The samples are matched by the batch's last position over one in-process monotonic clock, with no change to `cluster::wire`:
  1. sequencer send → frame read by the replica;
  2. read → appended;
  3. appended → durable (the replica's group commit and fsync);
  4. durable report sent → read by the sequencer → next window shipped.

  Raw samples, quantiles per window. Plus a control row at each window and heartbeat with no replica restarted, so the commit tail and the bimodal p50 can be attributed or not.

  Then D-135 closes with the stage named, and a card for its fix:
  - the replica's fsync → a catch-up window with a byte budget (D-135 B);
  - the report's path or the sequencer loop → that path;
  - the sequencer's CPU at 4 096 → a profile on Linux.
- **Where it lives:** the timing adds per-Append work to the `node` loop that D-124 compares; that waits on D-137's answer. Under D-137 B it goes behind the same off-by-default feature.
- **Cost:** S. No change to `cluster`.
- **Risk:** container fsync timing may differ from Linux, so the dominant stage could move on a real disk.

### B. Amend D-135 A's closing condition: close it now, decide D-135 B after D-124
- **What we do:** record the mechanism as "bounded by window ÷ round trip, the round trip's source unknown". This changes D-135 A, which closes only once the mechanism is investigated. After the accepted D-124 run, build the catch-up window with a byte budget (D-135 B).
- **Risk:** if the 40 ms is a removable delay (for example, the report waiting for the next group commit), a larger window treats the symptom and adds memory in flight — up to about 7 MB per replica measured at 4 096.
- **Cost:** none now; M later.

### C. Measure the round trip only on the Linux host, after D-124's overhead comparison
- **What we do:** add A's stage timing and the control to `scripts/linux-run.sh`, as a step after the overhead comparison (D-127's order). Nothing is measured in the container.
- **Risk:** blocked for as long as D-124 is; D-135 stays open meanwhile.
- **Cost:** S, plus the host.

**Discarded.** Raise the window now (D-135 C): D-135 A keeps the window unchanged until the evidence exists.

**Recommendation.** A. It names the stage at S cost, and the container numbers mark which stage to recheck on Linux. B becomes right if A shows the replica's fsync dominates; C, if container fsync timing proves unrepresentative.

**Answer:** `D-136: A` · `D-136: B` · `D-136: C` · `D-136: discuss`
````

````markdown
## D-137. Does D-124's candidate include the D-132 and D-135 instrumentation?

**Context.**
- D-124 compares a build with metrics against one without; D-131 says code under that comparison does not change before the run.
- D-132 and D-135 A asked for diagnostics, and they now sit in the `node` loop that comparison covers:
  - per heartbeat: `Progress`, `Voter::heard_commit`, the replica table, link queue depth;
  - per Append sent: a sum of the records' lengths and a push onto `Flight`, a queue bounded at 4 096 entries per peer.
- Their cost is not measured. The run that would measure it is D-124's own, still waiting for a quiet Linux host.

**Vision.** D-124 A, D-131 (no change to the compared code before the run), D-127 (no rewriting of metrics); CLAUDE.md §10 (every addition to the hot path needs a reason), §11.

### A. The candidate is the tree as it is, all instrumentation included — recommended
- **What we do:** D-124 measures the cost of the measuring that will ship, D-132 and D-135 included. Record this in ADR-0008 as an amendment to D-131's freeze. No code change. It also decides that D-124's result is no longer comparable to the D-112-only candidate named in D-127 step (2).
- **Risk:** if the overhead exceeds the noise, the run does not say which part costs it. Then a split run (B's feature) is the follow-up.
- **Cost:** none.

### B. The per-send part behind an off-by-default cargo feature of `node`
- **What we do:** `Flight` and the per-send byte count compile only with a feature (`catchup-trace`), which `cap` enables. D-124's candidate is the per-heartbeat metrics only, and the per-send cost is a separate later comparison.
- **Risk:** a second build configuration — a separate benchmark variable (§11) — and the series `straum_replica_bytes_in_flight` would be missing from production builds.
- **Cost:** S.

### C. Revert the per-send instrumentation before the run
- **What we do:** keep the per-heartbeat series; remove `Flight` and the per-send count from the node loop. D-135 A's in-flight numbers stay recorded, but cannot be reproduced from this tree.
- **Risk:** loses what D-136 A builds on.
- **Cost:** S.

**Recommendation.** A. D-124 asks what measuring costs, and its candidate should be what is measured in production. B if the D-124 run shows an overhead above the noise.

**Answer:** `D-137: A` · `D-137: B` · `D-137: C` · `D-137: discuss`
````

````markdown
## D-141. Where the CLI and console go in the order, and what their first slice reads

**Context.**
- D-140 puts `straum-cli` (Go) and `straum-console` (full stack) in the next steps (`docs/ecosystem.md`). Both repositories are empty.
- The core has no administrative API and no machine-readable contract. Its outside surfaces:
  - MQTT;
  - the metrics endpoint (D-112): Prometheus text, read-only, off by default, plain HTTP without credentials, loopback unless D-107's opt-in;
  - a planned handoff on SIGUSR1, local to the host.

  The Prometheus endpoint, with Grafana, stays the initial monitoring path (D-138 (7), second half).
- Already in the core's queue:
  - D-139 (the catch-up read; D-135 open);
  - 7c, persistent sessions (D-138: first among features);
  - D-110, TLS client authentication (D-138).
- The brief's Stage 2: the smallest slice that uses existing core capabilities and can be tested end to end, with no speculative infrastructure.
- **Open, decided by the option chosen:**
  - whether Prometheus text may serve as a client contract at all — D-112 made it for measurement;
  - whether an admin API is a core surface that needs an addition to unified-stream.md §2 or §6 through an ADR. §6 has no phase for tools outside the cluster.

**Vision.** D-140, D-138 (security before network exposure; the committed-log boundary; the core authoritative); D-112 (metrics are read by people and tools, nothing read back into a decision); CLAUDE.md §15 (no framework) and "Rules of work"; the brief §3, §9 (no invented protocol; the smallest contract addition).

### A. A CLI-only first slice on the metrics endpoint — recommended
- **What we do:**
  - `straum-cli status` (Go) reads the metrics endpoint of each configured node. It shows each node's role, slot, commit, applied, log end, durable, own lag and outbox. Each value names the node it came from and how old it is; a node that doesn't answer is shown as unknown, never as healthy. Exit codes and a stable JSON output for scripts.
  - The console waits for an admin API (C): without one it could only re-show what Grafana already shows.
- **Core change, docs and test only:** a small stable set is named in `docs/configuration.md` and pinned by a test in `node`: `straum_node`, `straum_slot`, `straum_leading`, `straum_serving`, `straum_commit`, `straum_applied`, `straum_log_end`, `straum_log_durable`, `straum_lag`, `straum_outbox_entries`, `straum_outbox_capacity`. Everything else is marked unstable, including the catch-up diagnostics D-139 is still changing.
- **Order:** in parallel with D-139 and 7c, in the other repository; the core part touches neither.
- **Security:** the endpoint stays loopback by default; the CLI runs beside the nodes. Nothing is opened wider.
- **Cost:** S in the core; S–M in `straum-cli` (Go module, tests, CI).
- **Risk:** Prometheus text is a monitoring format. When an admin API arrives (C), `status` moves onto it and keeps the stable set's meaning.

### B. The same slice in both the CLI and the console
- **What we do:** A, plus a console (frontend and backend, stack chosen at its start) with a status page over the same stable set, its backend the only reader of the endpoints.
- **Cost:** S in the core; M across the two repositories.
- **Risk:** the console duplicates Grafana until an admin API exists, and a stack gets chosen for a product that has no operations yet.

### C. An admin API ADR in the core first, then both clients on it
- **What we do:**
  - An ADR adds an administrative surface to unified-stream.md (§2/§6): a versioned read API (status, topology) with a machine-readable contract, its format chosen in the ADR.
  - Authenticated from the start: it waits for D-110, or stays loopback-only until then.
  - The handoff becomes its first operation, as a command whose effect goes through the log as now.
  - Then the clients, generated from the contract.
- **Cost:** M–L in the core before any client exists.
- **Risk:** it competes with 7c and D-110 for core work.

### D. After 7c and D-110
- **What we do:** keep the core order (D-139, 7c, D-110), then C directly on an authenticated surface.
- **Risk:** no client for the longest time.

**Discarded.**
- A client reading the log or the data directory: the brief §4 and the committed-log boundary forbid it.
- An unauthenticated admin API reachable off loopback: D-138, D-107.

**Recommendation.** A: one repository, read-only, existing surfaces only, beside the core work. C comes once D-110 lands, for the first operation, and brings the console. B if a console is wanted before any operation exists; D if no client work should run beside the core.

**Answer:** `D-141: A` · `D-141: B` · `D-141: C` · `D-141: D` · `D-141: discuss`
````

````markdown
## D-139. The catch-up read: confirm why it costs 14–25 ms, then remove the cost

**Context.** D-136 A, preliminary (container):
- **Measured:**
  - Catch-up is one read from the log at a time.
  - Each read costs 14–25 ms (`read_K`), with no queueing and no wait for the report.
  - Each read brings 96 positions at window 256, about 430 at 1 024.
- **Read from the code, not measured:** `Segments::read` replays the segment from its start whatever the read needs, up to `segment_bytes` (64 MiB, also in 7a). D-135 stays open until that is confirmed.
- **The cost:**
  - commit p99 52–170 ms during catch-up, against 1.5–2.4 ms in the control;
  - recovery after the load: 3 891–6 563/s at 256; at 1 024, 27 040/s where it didn't drain (lower bounds elsewhere).

  At 256, a lag of several hundred thousand positions outlasts the clients' 30 s `commit_timeout` (D-133).
- **Not explained:** the replica's own log thread waits 10–20 ms at p90 (`append_queue_W`) during catch-up only.
- fsync costs about 0 in the container.

**Vision.** CLAUDE.md §7 (bounded under any input), §10 (append and replicate on the hot path), §11 (baseline, one change, Linux for accepted numbers); D-138 (container results may drive exploratory work; truncation and compaction get a full guardian review); D-57 A (retention).

**Common first step, for A–C** (S; it closes D-135 or reopens the analysis):
- stamp `read_K` against the read's offset in its segment;
- stamp what the replica's log thread does while an append waits (`Range` read-backs for apply).

If `read_K` grows with the offset, the cause is confirmed and the option below follows; if not, a new card.

### A. The common step only; the fix chosen by a new card — recommended
- **What we do:** the two stamps above, D-136's catch-up mode with control, three rounds. Then D-135 closes with the cause confirmed or not, and a new card offers B, C or another fix on that evidence.
- **Why first:** D-135 is open on exactly this question; B and C each commit to a fix before the cause is confirmed.
- **Cost:** S; diagnostics only.

### B. A read cursor per replica
- **What we do:** `storage::Segments` gets a read from a file offset that returns where it stopped (`read_from(key, offset)`); `node::wal` keeps, per replica being caught up, the segment and file offset where its previous read stopped. The next read from the following position starts there. It is dropped on `Truncated`, `Compacted` or a segment change, which fall back to the scan; a read from any other position scans as now.
- **Why it fits:** catch-up reads are sequential, one outstanding per replica.
- **Memory:** O(replicas), no per-batch structure; bounded under any input.
- **Tests:** in `storage`/`node`, a read through the cursor equals the scan's after appends, a truncation, a compaction and a segment roll; the cluster simulation unchanged.
- **Measure:** D-136's catch-up mode with control, before and after. `read_K` should follow what a read returns.
- **Cost:** S–M. Touches the read path near truncation and compaction: a full guardian review (D-138).

### C. A sparse offset index per segment
- **What we do:** `storage::Segments` keeps the offset of the first batch at or after every 64 KiB of each retained segment: a fixed bound of `segment_bytes / 64 KiB` entries, 1 024 per 64 MiB segment, at most about 16 KiB each, however small the batches. A read seeks to the entry at or below its start and scans from there.
- **Invalidation:** rebuilt on open; cut on `Truncated`; dropped with a segment on `Compacted`.
- **Serves:** any read, not only sequential ones (a new replica, a takeover's catch-up), and `Range` read-backs.
- **Cost:** M. A second representation of each segment that truncation and compaction must keep exact; full guardian review and simulation.

### D. Accept it as a known limitation of 7a for now
- **What we do:** record in ADR-0008 and `docs/configuration.md`, as preliminary container figures:
  - recovery at window 256 is 3 891–6 563 positions/s;
  - commit p99 is 52–170 ms during catch-up, against 1.5–2.4 ms without;
  - a restarted replica can stay behind longer than the clients' 30 s `commit_timeout` (D-133).

  D-135 is recorded as the stage measured, its cause inferred and accepted as a limitation (D-138). Feature work goes first.
- **Risk:** every restart or long partition pays this, and 7c's persistent sessions add load on the same log.

**Discarded.**
- Stopping the scan once the read is complete (S): a read still scans everything before its start; B removes that for the sequential case it would help, fully.
- Raising the window (D-135 C): the window acts only through what fits in one read.
- A separate reader thread: it addresses the commit tail only if the reads cause it, which isn't established. It returns as a card if the tail outlives A.

**Recommendation.** A. B is the likely fix if A confirms the scan, being the smallest change that matches how catch-up reads (sequentially), bounded by replicas. C if A shows other reads (`Range`, a takeover's catch-up) paying the same scan. D if feature work is to go first.

**Answer:** `D-139: A` · `D-139: B` · `D-139: C` · `D-139: D` · `D-139: discuss`
````

````markdown
## D-142. Stop replaying whole segments for catch-up reads and read-backs

**Context.** D-139 A, preliminary (container, segments in the page cache):
- **A catch-up read (sequencer, `wal::read_batches`):**
  - the part it keeps takes 0.01–0.04 ms;
  - reaching its start: 5–17 ms over 16–62 MB, which depends on the read's offset in the segment;
  - going on to the segment's end after it is complete: 5–15 ms over 20–53 MB, pure waste.

  That is 55–63 MB a read at 3.4–4.7 GB/s, and 14–25 ms in all. Catch-up runs one read at a time; recovery at window 256 was 5 237–7 122 positions/s. The commit p99 was 83–131 ms during catch-up, against 1.4–2.4 ms in the control (D-139 A's rows).
- **What bounds the rate:** the sequencer's read and the replica's wait on its own read-backs (one per read, p90 13–16 ms) are both serial. The run doesn't separate which dominates; A measures that after step 1.
- **A read-back for apply (any node, `wal::read_range`):** the same replay, 55–61 MB for 1 024 records, 17–21 ms. During catch-up it holds the replica's log thread for 99 % of its appends' waiting (p90 13–16 ms).
- **What `storage` offers:**
  - `Segments::read(key, visit)` replays a segment from its start, and the visitor cannot stop it;
  - `scan` already has a stop hook (`ready`), but only for the header;
  - `cluster::ReadBatches::done()` says when a read needs nothing more.
- **Not measured:** a cold disk, where the scanned bytes are disk reads, not memory, would cost more.

**Vision.** CLAUDE.md §7 (bounded under any input), §10 (append and replicate on the hot path), §11 (baseline recorded — D-139 A; one change at a time; Linux for accepted numbers); D-138 (container results drive exploratory work; storage, truncation and compaction get a full guardian review); D-57 A (retention).

### A. Early stop, then a cursor — two changes, measured apart — recommended
- **Step 1 — early stop (S):** discarded in D-139's card ("a read still scans everything before its start"); it returns on new evidence, since the part after the read is measured at 5–15 ms a read.
  - `storage`'s replay takes a visitor that can stop the walk;
  - `read_batches` stops once `ReadBatches::done()`;
  - `read_range` stops once it has `MAX_RANGE` records or passed `upto`.

  Expected: the "after" part goes, 5–15 ms a read. `wal::Scan` stays, so the next run shows what is left; it is counted on every read (two adds and a branch a record, two clock reads a read — against a checksum a record and a 14–25 ms read), and is dropped once step 2's measurement is recorded unless it is still needed.
- **Step 2 — a cursor (S–M):**
  - `storage::Segments` reads from a file offset and returns where it stopped (`read_from(key, offset)`);
  - `node::wal` keeps one cursor per replica being caught up and one for the apply read-back, each `(segment, offset, next position)`;
  - a read continuing at the next position starts there; a truncation, a compaction or another position drops the cursor, which falls back to the scan.
  - the cursor is a position only and holds no open file; truncation and compaction run on the same log thread and drop it before a segment changes or goes, so it never pins a segment (unified-stream §3, "compaction must not truncate a segment someone is reading").

  Expected: the "before" part goes too, so a sequential read costs about what it brings.
- **Memory:** O(replicas + 1), bounded under any input.
- **Tests:**
  - in `storage`: a read from an offset, or stopped early, equals the full scan's;
  - in `node`: through a truncation, a compaction and a segment roll;
  - the cluster simulation unchanged.
- **Measure after each step:** D-139's mode with controls, against D-139 A as the baseline. That covers `read_K`, the read cycle, the catch-up rate, the commit p99 against the control, and the replica's waits.
- **Cost:** S + S–M. Full guardian review (storage, truncation).
- **Why now, against D-138's order (feature work after D-136):** a restarted replica can stay behind longer than the clients' 30 s `commit_timeout` (D-133), and every restart pays it. 7c's persistent sessions would add to that load.

### B. A sparse offset index per segment
- **What we do:** `storage::Segments` keeps the offset of the first batch at or after every 64 KiB of each retained segment: at most 1 024 entries per 64 MiB segment, rebuilt on open, cut on truncation, dropped with the segment. Reads seek to the entry below their start and stop early.
- **Serves:** any read, sequential or not — a takeover's catch-up, a restart's read-back.
- **Cost:** M; a second representation truncation and compaction must keep exact.

### C. Early stop only
- **What we do:** A's step 1 alone.
- **Expected:** reads and read-backs lose their "after" part, roughly 40–60 % of each. The "before" part, which grows with the offset in the segment, stays — and so, in part, does the replica's wait on read-backs, since `read_range` still scans from the segment's start. Revives the early stop D-139's card discarded, on the measured after-part.
- **Cost:** S.

### D. Accept it for now
- **What we do:** record these numbers in `docs/configuration.md` as a known limitation of 7a, and go on with 7c.
- **Risk:** every restart pays a replica's read-backs and the sequencer's catch-up reads at 14–25 ms each, and the commit tail during catch-up.

**Discarded.**
- A separate reader thread: it would move the scan off the commit path without removing it. It returns only if the tail outlives A.
- A smaller `segment_bytes`: it shortens every scan but multiplies files and compaction work, and it is a default change that needs Linux numbers (D-138).

**Recommendation.** A. Step 1 is small and removes measured waste on its own; step 2 removes the rest for the sequential reads that catch-up and apply do. B if non-sequential reads (takeover, restart) turn out to matter, measured the same way.

**Answer:** `D-142: A` · `D-142: B` · `D-142: C` · `D-142: D` · `D-142: discuss`
````

````markdown
## D-143. After D-142: what next

**Context.** D-142 A is in, preliminary (container):
- with the early stop and the cursor together, a catch-up read takes 0.02–0.04 ms against 13.8–16.6 ms (medians, five rounds);
- at window 256, catch-up no longer costs commits: p99 1.8 ms and about 311 000 commits/s (medians), against controls of 306 000–312 000 (at 1 024 the replica had nothing left to catch up by the first sample, so its figures compare nothing);
- a restarted replica catches up under load.
- in cap's failover mode, 10 of 10 failovers commit within 1 s against 4 of 10 before: the survivors no longer lag (about 80 000 positions before).
- The figures are from D-142's protocol run (five rounds, windows 256–4 096, the failover mode); see ADR-0008.

Not measured: a cold disk, the sync on a real disk, a Linux host. D-138's order puts 7c (persistent sessions) first among features. D-124, the accepted Linux comparison, still has no host. The CLI's first slice, D-141, runs in its own repository.

**Vision.** D-138 (order; Linux gates accepted claims and defaults, not features); D-124, D-127; CLAUDE.md §11.

### A. Go on to 7c, persistent sessions — recommended
- **What we do:** step 7c of ADR-0008 — sessions that survive a disconnect, through the log, with the gateway's refusal at CONNECT lifted for them only. Wills and expiry stay refused (D-138). With expiry refused, a 3.1.1 session with CleanSession=0 would persist indefinitely, so the design card must name the global bound on persisted sessions and their bytes (CLAUDE.md §7, "per session, and globally") and what happens when it is reached. A card for its design first: the record kinds it writes come out of the reserved block (`SessionExpired` and others) only by a decision, and the straum-cli session is told first (D-141).
- **Cost:** L, over several tasks.

### B. Measure the catch-up path on a cold disk first
- **What we do:** drop the page cache between the stop and the restart (on Linux, `/proc/sys/vm/drop_caches`, which needs privileges the container may lack — checked first) and rerun D-142's comparison, base against both.
- **Cost:** S if the container allows it; otherwise it waits for a Linux host.
- **Risk:** the container's virtual disk may not represent a real one anyway.

### C. The D-124 Linux run first
- **What we do:** the accepted overhead comparison on bare metal (D-127), which also gives the first non-container numbers for D-142.
- **Precondition:** a quiet host, which isn't available to me.

**Recommendation.** A. D-142's gain is large enough that a cold disk is unlikely to reverse it. B is cheap if the container permits dropping caches, and can run beside A. C whenever a host exists.

**Answer:** `D-143: A` · `D-143: B` · `D-143: C` · `D-143: discuss`
````

````markdown
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
````

````markdown
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
````
