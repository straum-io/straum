# Pending decisions

Decision cards waiting for the user. Written only by the task-closeout
skill. When the user answers `D-<n>: <choice>`, the card moves to
`log.md` with the answer and is removed from here.

---


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
