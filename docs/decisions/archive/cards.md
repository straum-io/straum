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
