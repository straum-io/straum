# Pending decisions

Decision cards waiting for the user. Written only by the task-closeout
skill. When the user answers `D-<n>: <choice>`, the card moves to
`log.md` with the answer and is removed from here.

---


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
