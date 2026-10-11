# D-97: the cluster sweep that did not finish (2026-10-10)

Kept per D-97 A: the parameters of the run, the code it ran, the evidence
of its cause, and the reproduction.

## The run

- Started 2026-10-10 03:26 (local, +0300), as a background task with a
  one-hour limit; stopped by that limit at about 04:26 with no output after
  "running 1 test".
- Command, from the repository root:
  `STRAUM_SIM_SEEDS=10000 cargo test --release -p sim --test cluster three_voters -- --nocapture`
  (no other `STRAUM_SIM_*` variable set).
- Code: the uncommitted tree of that moment, between two closeouts, so it
  has no commit. It differed from the code of this record's date in:
  - `sim`: flow control on in half of each bounded group,
    `LagBound { records: seed % 8, ticks: 2 + seed % 7 }`, with the hold
    check counting heartbeats in a row;
  - `cluster::Sequencer`, rule R4 counted per hold, and a hold did not
    need a queued proposal. The diff below rebuilds it from the current
    `crates/cluster/src/lib.rs`, with the slot-start comment, the `counts`
    query and the idle check removed.
- Rebuilt in a scratch copy for the reproduction. SHA-256 (first 16 hex
  digits) of its files: `cluster/src/lib.rs` 71a00649bc150261,
  `cluster/src/voter.rs` 9a4f735b97fb68ee, `sim/tests/cluster.rs`
  1e606b6ce7bfcbaf (with per-seed timing added).

```diff
--- current/crates/cluster/src/lib.rs	2026-10-10 17:11:21
+++ that-run/crates/cluster/src/lib.rs	2026-10-10 06:44:00
@@ -631,11 +631,6 @@
         let Some(bound) = self.lag_bound() else {
             return false;
         };
-        // Held only when something waits: an idle sequencer detaches no
-        // one, and a sealed slot's last batches are never held (R3).
-        if self.sealed || self.queue.is_empty() {
-            return false;
-        }
         self.peers
             .iter()
             .any(|peer| peer.attached && self.commit.saturating_sub(peer.durable) > bound.records)
@@ -646,11 +641,6 @@
         self.peers.iter().filter(|peer| peer.attached).count()
     }
 
-    /// Whether flow control counts replica `id` now (D-89).
-    pub fn counts(&self, id: NodeId) -> bool {
-        self.peers.iter().any(|peer| peer.id == id && peer.attached)
-    }
-
     fn lag_bound(&self) -> Option<LagBound> {
         let floor = self
             .config
@@ -704,7 +694,7 @@
         }
         // R3: a sealed slot's last batches are never held — `SlotEnd`
         // ends a handoff.
-        if self.holding_back() {
+        if !self.sealed && self.holding_back() {
             return Ok(());
         }
         if self.queue.is_empty() && self.sealed && !self.ended {
@@ -766,8 +756,6 @@
             self.chain.batch(self.config.slot, &entries, &mut records)?;
         }
         self.queue.drain(..take);
-        // R4 counts held heartbeats since the last batch.
-        self.holding = 0;
         if self.slots.last().map(|&(slot, _)| slot) != Some(self.config.slot) {
             self.slots.push((self.config.slot, first));
         }
@@ -1049,7 +1037,10 @@
                     for peer in &mut self.peers {
                         peer.attached &= peer.durable >= commit;
                     }
+                    self.holding = 0;
                 }
+            } else {
+                self.holding = 0;
             }
         }
         for peer in &mut self.peers {
```

## The cause: the machine slept

From `pmset -g log`:

```text
2026-10-10 03:26:56 +0300 Sleep     Entering Sleep state due to 'Idle Sleep':TCPKeepAlive=active Using Batt (Charge:91%) 322 secs
2026-10-10 03:32:18 +0300 DarkWake  DarkWake from Deep Idle [CDNP] : due to smc.sysState.Wake(0x70070000) wifibt SMC.OutboxNotEmpty/ Using BATT
2026-10-10 03:32:24 +0300 Sleep     Entering Sleep state due to 'Maintenance Sleep':TCPKeepAlive=active Using Batt (Charge:91%) 3604 secs
2026-10-10 04:32:28 +0300 DarkWake  DarkWake from Deep Idle [CDNP] : due to smc.sysState.Wake(0x70070000) wifibt SMC.OutboxNotEmpty/ Using BATT
```

The run started at 03:26, the machine slept from 03:26:56 to 04:32 with a
6-second maintenance wake, and the task's limit fell inside the sleep. The
next run, started at 04:33, finished in 189 s.

## Reproduction of that code

| Run | Wall time | Peak RSS | Voluntary context switches | Slowest seed |
|---|---|---|---|---|
| alone | 201 s | 171 MB | 1 | 1 831, 33 s |
| two copies at once, a third sweep beside them | 378 s, 382 s | 174 MB | — | 1 831, 78 s |

All three runs gave identical per-seed outcomes (committed records,
takeovers, recovered cycles) for all 10 000 seeds: the simulation is
deterministic and no seed hangs. No disk I/O (the disks are simulated); one
voluntary context switch, so no waiting on a lock; memory flat. CPU
contention slows a run by under 2×, not by the more than 18× an hour would
need. Long runs are now wrapped in `caffeinate -i`.
