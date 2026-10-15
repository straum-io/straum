# Pending decisions

Decision cards waiting for the user. Written only by the task-closeout
skill. When the user answers `D-<n>: <choice>`, the card moves to
`log.md` with the answer and is removed from here.

---


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
