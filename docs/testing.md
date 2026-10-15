# Testing

> **Simulation target: docs/architecture/unified-stream.md §5** — the harness
> with controlled clock, network and disk, its failure list and its eight
> invariants. The old leader/follower simulation and the D-28 oracle went with
> the old broker at step 8 (D-111); C1–C4 were not carried over.

## Running

```bash
cargo test --workspace --tests                 # everything, benches excluded
cargo test --workspace --all-targets --no-run # and they still compile
cargo test -p codec                    # one crate
cargo test -p codec connect            # one test, or any name filter
cargo test -p cluster --test voter     # one integration test file
cargo test -p server --test cluster -- --nocapture   # the end-to-end test
STRAUM_SIM_SEEDS=10000 cargo test --release -p sim --test cluster   # a sweep
cargo test --release -p sim --test cluster -- --ignored a_slow_disk    # the D-98 gate
```

## Layers

| Layer | Where | What |
|---|---|---|
| Unit | `#[cfg(test)]` in each module | pure logic, state transitions, error paths |
| Crate integration | `crates/<crate>/tests/` | public API of one crate |
| Property | proptest in domain, codec, journal, apply | round-trips and invariants |
| Fuzz | `fuzz/` (nightly) | decoder never panics, bounded allocation |
| End-to-end | `crates/server/tests/cluster.rs`, `crates/node/tests/` | real processes or nodes over TCP, an independent client |
| Crash recovery | `crates/storage/tests/` | every way a WAL tail can be cut, flipped or lied about (docs/storage.md §92) |
| Simulated disk | `crates/storage/tests/simulated.rs` | the production log on `storage::simulated::SimDisk`: power loss at every torn length after acknowledged commits, process crash, injected write and sync failures, and a rewrite failing at each of its writes and syncs with no commit after it; deterministic, no real file or clock (D-28 stage 1, scenarios S1–S3). A failed directory sync after a rewrite poisons the log (D-31); a new log survives a power loss (D-30) |
| Log record v4 | `crates/journal/tests/format.rs` | record v4 round-trip and malformed input; the batch chain refuses a changed byte, a dropped record, a reorder, a foreign history, a slot going back and a record outside a batch; a torn batch is a tail; xxh3-128 against its reference vector and the batch bytes pinned (ADR-0008 step 1) |
| Simulated chain | `crates/sim/tests/journal.rs` | v4 batches through `Wal<SimDisk>` with random power loss and torn tails: recovery verifies as one chain, every synced batch comes back with the same hash, a torn batch only at the tail; 50 seeds by default, `STRAUM_SIM_SEEDS` for more (unified-stream §5 invariants 1 and 5, single log) |
| Segmented log | `crates/storage/tests/segments.rs` | segments start at batch boundaries and recover in order; removal takes whole segments from the front, stops at a pin, keeps the active and the last sealed one; a torn batch is cut; a power loss never leaves a hole between segments (ADR-0008 step 2) |
| Sequencer and replica | `crates/cluster/tests/protocol.rs` | one rule per test: nothing shipped before it is durable on the sequencer, commit needs a majority and only grows, a replica takes only the owner's next whole batch with an unbroken chain, catch-up from the log and loss below it, the window, a full queue refused, only the heartbeat resends; replicas count toward commit only once their logs end in the sequencer's slot (ADR-0008 step 5); a stale read answer neither clears the read outstanding nor asks again (D-85); flow control holds new batches for an attached replica more than `L` behind for at most `T` heartbeats between two batches — counted across holds — then detaches it, keeps it detached while behind, and never holds the commit of what is appended (D-89 A2); without an uncommitted bound a new slot must first cover the whole tail (the mechanism of the long cycles, D-90 B) |
| Three-node simulation | `crates/sim/tests/cluster.rs` | three `cluster::Voter`s on simulated disks, sequencers dying and the next in the schedule taking over (D-70 A): drops, reordering, crashes, power losses with torn writes, partitions, compaction during catch-up, alternating with calm periods; invariants 1, 3, 5, storage keeping its word, the refcount, every compaction floor within the retention rule and a replica lost only below the byte floor (D-57 A); after healing, a new record committed and every replica able to catch up committed through it; one seed in three with an uncommitted bound of 4–16; one seed in four rotates the sequencer every 4 heartbeats (step 6). 100 seeds × 1500 events by default (retention bound 1000 bytes; pins during compaction need that many seeds); every loss is checked against D-57 A, with no exemption: a node's committed slot table is durable before each compaction, as the snapshot carries it, and every table is pruned by the floor compaction reached (D-73 A, D-79 A); every seed runs fault cycles — the sequencer crashes or is cut off, nobody else is — and every cycle with a majority able to catch up must commit again (eventual liveness: a stall is no progress for 20 000 events or none by 5 000 000 — no allowance for slow cycles, D-90 B; cycles that lose such a majority within 5 000 events of their last progress are classified as needing a state copy, 7b; healing likewise), asserted in every run (D-85); `every_seed_that_stalled_recovers` reruns each seed that ever stalled, and 8956; a classified cycle must owe its state copy to the byte bound, and must commit once the node cut off comes back if that restores an able majority; every catch-up of at least 2 000 records must cost no more than 24 events a record per replica caught up (twice the typical 12, measured 10.8–13.0; D-90 B), and the ignored `a_tail_outrunning_its_catch_up_commits_eventually` runs the three cycles that outlasted the old cap to their commit (about 100 s in release); flow control on replica lag runs in half of each bounded group, and a sequencer may hold new batches fewer than `T` heartbeats while its slot and end stay unchanged (D-89 A2); `a_realistic_retention_bound_needs_no_state_copy` runs 100 seeds at 100 000 bytes (`STRAUM_SIM_RETAIN_BYTES` sets it for a sweep); healing reports rounds to the first commit by seed group and the patience nodes had built up (D-86 A, D-87 A, D-88); `STRAUM_SIM_TRACE=1` prints the trace a stall is diagnosed from; a later slot committing within 2 000 events is reported in three groups by `seed % 3`: uncommitted bound 4–16, none, and `DEFAULT_MAX_UNCOMMITTED` (64); asserted per group from 300 cycles (9 in 10 for the bounded groups, 85% for none), so the 10 000-seed sweep carries the gate (D-81 A, D-82 A); `STRAUM_SIM_STEPS=<n>` lengthens runs (1 000 000 steps cross about 2 000 slot changes; a manual sweep, not part of the default run), the ignored `a_slow_disk_adds_no_takeovers_at_the_standard_timing` (9 s in release) compares the slow-disk scenario at the standard timing with the same runs on a fast disk: takeovers by silence within 10%, the slowed replica winning no more, commit after every detach (the D-98 A gate); `a_slow_replica_is_detached_at_t_and_rejoins_while_the_majority_commits` slows one replica's disk (99 of 100 writes deferred, promises with them) in a settled cluster, with no rotation and 30 heartbeats before a takeover, and requires in every window: the slow replica detached at `T` by the settled sequencer, the commit point moving after that detach, the commit point moving, no takeover, the replica never lost, and — with proposals paused — the replica caught up and counted again; it reports how far a detached replica trails under continuing load (5 seeds by default, `STRAUM_SIM_SLOW_SEEDS=200` for the sweep; D-93 A); `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim --test cluster` for the sweep |
| MQTT 3.1.1 and 5 codec | `crates/codec/tests/roundtrip.rs`, `crates/codec/tests/rules.rs` | every packet either side may send, in either version, survives encode and decode (2000 cases), split anywhere it decodes whole, arbitrary bytes never panic; one test per rule: properties a packet may not carry, duplicates, out-of-range values, user-property bound, topic alias, 3.1.1 mappings (CONNACK codes, SUBACK 0x80, no PUBACK reason), MQTT 5 short forms, version-only fields, oversized packets refused from the header, subscription options, side-specific DISCONNECT fields, QoS 2 decoded to be refused (ADR-0008 step 3a) |
| Apply function | `crates/apply/tests/state.rs` | one rule per test: one delivery per matching session at the lesser guarantee, the publisher acknowledged after its deliveries, retained messages kept, replaced, removed and delivered on subscribe, unsubscribe, order and exactly-once application, unknown and malformed records stop the node, subscription options, inflight and backlog with acknowledgements, slow-consumer close, the global backlog bound, session close and node reset, subscription and retained bounds, nothing overtaking what waits; a proposal survives its record, and the same log gives the same state and effects with inflight within its bound (properties); a snapshot taken anywhere plus the rest of the log gives the whole log's state and effects, and a damaged, short, other-limits or other-version snapshot is refused (D-67 B) (ADR-0008 steps 3b-1, 3b-2) |
| Apply simulation | `crates/sim/tests/apply.rs` | three `cluster::Voter`s plus `apply` (takeovers and cuts counted, invariant 2 checked; D-70 A), clients at every gateway, forwarded proposals over a lossy reordering network, crashes and power losses; at every effect: invariant 4 (committed and durable on a majority), equality with an independent reference model including inflight, backlog and bounds, the same state on every node at every lsn; no session left once every session ended; nodes take snapshots at random durable points, compact their logs below them, and restart from snapshot and suffix (D-67 B). 30 seeds × 1500 events by default, `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim --test apply` for the sweep |
| Gateway | `crates/gateway/tests/rules.rs` | one rule per test, MQTT bytes through `codec`, commit on demand: a will, a 3.1.1 CleanSession=0 and an MQTT 5 session expiry are refused at CONNECT, an MQTT 5 CleanStart=0 without expiry accepted (step 7a); connections wait for the gateway's reset, PUBACK and SUBACK only after commit, QoS 1 acknowledged through the log and retried until seen, takeover, keep-alive, topic aliases, the client's receive maximum, QoS 2 refused, a connection at its bound not read, a proposal that does not commit closes its connection, 3.1.1 empty id with CleanSession=0 refused (ADR-0008 step 3b-2) |
| MQTT simulation | `crates/sim/tests/gateway.rs` | MQTT 3.1.1 and 5 clients through `codec` against three nodes (`cluster::Voter`, `apply`, `gateway`; gateways forward to a changing owner, D-70 A), takeovers, goodbyes, dropped sockets, crashes and power losses, a mostly ordered network; invariant 4 at every packet, invariant 7 within a gateway, per-session order per topic, no session left after every client has gone. `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim --test gateway` for the sweep |
| Voter | `crates/cluster/tests/voter.rs` | one rule per test: an append from a newer slot's owner resets silence but appends, acknowledges and commits nothing before the promise is durable, and a sequencer heard once and then silent is still taken over, also when it dies while the promise is pending (D-98 A); a takeover during a catch-up builds on the longest log of the promising majority, keeps every committed record in place on every log, commits only once the replica holds `SlotStart`, and the old sequencer's late batch is refused by both (D-95 A); a takeover a majority promised that does not commit doubles the wait for the next, one no majority answered leaves it as it was (D-88 A); a read answer reaches the sequencer past an older fetch from the same replica; a fetch for an older slot than promised is not served; a handoff waits a bounded while for its successor to hold `SlotEnd` (D-85); a promise answered only once durable; nothing of a lower slot taken or acknowledged after it; the first owner starts its slot with `SlotStart`; a candidate catches up from the longest log of a majority; a follower cuts only where the slots prove the logs differ (ADR-0008 step 5); a handoff ends the slot with `SlotEnd` and the successor starts the next without a promise round, only a committed `SlotEnd` is taken over, only a sequencer with another owner next may hand off (step 6); compaction prunes the slot table to the slot holding the floor, and a restart merges the snapshot's table with the log's (D-79 A) |
| Takeover simulation | `crates/sim/tests/takeover.rs` | three voters on in-memory logs, every node proposing through its outbox (invariant 7, D-117 B: each proposal commits at most once, in its node's order, never after it was given up; at the end each is committed, given up, refused, orphaned, or stranded with a lost forward; seed 14165 pins the slot tag on forwards): planned handoffs at random (crashes before, during and after `SlotEnd`; `SlotEnd`s never committed), crashes, power losses, promises made durable by nodes that die before answering, partitions, loss and reordering, cuts and reads a crash can fall between; invariants 1, 2, 2a (a `SlotEnd` closes its slot), 3, 5, 6 after every step; `SlotEnd`s never committed and crashes over a pending one are counted and required, convergence after healing, then a handoff in the healed cluster that must complete without a promise round (step 6). 30 seeds by default; `STRAUM_SIM_SEEDS=200000 cargo test --release -p sim --test takeover` for the sweep (about 70 s); `STRAUM_SIM_TRACE=1` prints the protocol's events; `a_handoff_waits_for_a_successor_catching_up` reruns seed 4905 on every `cargo test` (D-85) |
| Link framing | `crates/cluster/tests/wire.rs` | every frame round-trips, split anywhere it decodes whole; an oversized or malformed frame is refused (ADR-0008 step 4) |
| Cap benchmark | `crates/node/benches/cap.rs` | three nodes on loopback: with `STRAUM_BENCH_MODE=catchup`, a replica stopped 2 s and restarted under load, its own lag read every second, at each replication window of `STRAUM_BENCH_WINDOWS` and heartbeat of `STRAUM_BENCH_HEARTBEATS` (D-133 B, D-135 A): the lag's rate under load and its recovery after, positions per heartbeat and the share of heartbeats a lagging replica did not move, the bytes and positions in flight to it, and the sequencer's commit latency quantiles from raw samples; each trial paired with a control without a restarted replica, and per trial a `stages` row per replica and path: every append followed through its stages to the durable report covering it (the read from the log, the link, the replica's log thread, the report), waiting told from work, with the read cycle and positions per read (D-136 A); `STRAUM_BENCH_TRACE_DIR` writes the raw stamps as CSV; otherwise publishes per second in a closed loop, failovers committing within 1 s — reported as a distribution (D-130): the share within 1 s, p50 and p90, and the slow mode apart from the fast one (slots the cluster moved on, takeovers abandoned, the survivors' lag at the stop) (the sequencer stopped in order while the load runs), and the largest survivor lag sampled just before the stop, per `max_uncommitted` (D-84 A). `cargo bench -p node --bench cap`; `STRAUM_BENCH_CAPS`, `_SECONDS`, `_ROUNDS`, `_FAILOVERS`, `_WINDOW`, `_HEARTBEAT_MS`, `_TAKEOVER_TICKS`. Accepted numbers only from a quiet Linux host |
| MQTT benchmark | `crates/server/benches/puback.rs` | three `straum-server` processes on loopback; closed-loop QoS 1 publishers and QoS 0 subscribers over MQTT 3.1.1: PUBACKs per second, publish-to-PUBACK p50 and p99, deliveries per second, and the servers' CPU time per 1 000 PUBACKs (D-124). `cargo bench -p server --bench puback`; `STRAUM_BENCH_PUBLISHERS`, `_SUBSCRIBERS`, `_WINDOW`, `_PAYLOAD`, `_WARMUP`, `_SECONDS`, `_SERVER` (another build's binary), `_EVENTS` (`handoff,kill`: a planned handoff and a killed sequencer, spread over the run; each reported apart — the longest wait for a PUBACK for its node's clients and for the others' — with every publish accounted: late, retransmitted, unacknowledged, duplicate acknowledgements, publishers that gave up reconnecting after 30 s; each node's position at the end and 10 s later; D-128). The categories overlap: `acknowledged` includes late and retransmitted publishes; `retransmitted` counts resends, not publishes; a client that gave up has no gap to measure, so `max_gap_ms_its_node` leaves it out and only `its_clients_gave_up` shows it; a resend whose original also committed is a duplicate in the log, which this client cannot see, `_READ_BYTES` (passed to the servers as `STRAUM_READ_BYTES`; D-131 A), after a kill a per-second `catchup` line with the restarted node's commit against the cluster's and its lag (D-131 A), `_SCRAPE_MS` (a metrics listener on each node polled at that interval, and a per-node diagnostic line at the end: quantiles of accepted→applied, append→durable and append→commit, the outbox, withheld reads, takeovers and successions). Accepted numbers only from a quiet Linux host |
| Node driver | `crates/node/tests/cluster.rs` | a lagging replica visible from both sides — its lag, rate and progress age at the sequencer and at itself, then caught up and serving (D-132); metrics updated from accepted to committed, served over HTTP, and an outbox overflow with the delay placed at the quorum (D-112); the metrics listener: a burst of silent connections bounded and closed, a scrape still answered (`node/src/metrics.rs`, D-126 A); a replica down longer than the old 1 s redial cap restarts and the slot stays with its live owner (D-122 A); a planned handoff and a sequencer failure between two halves of 200 proposals per node: each commits once, in its node's order, and no older slot commits after a newer one (D-109 B); an operator handoff moves the slot while every node keeps serving, and a node not sequencing refuses it (D-105 A); a data directory of another cluster is refused at start (D-104 A); a link from another frame version is refused at its HELLO, one from this version kept (step 7a); three nodes on loopback with logs on disk: one log committed and applied everywhere, a replica and the sequencer restarted from their logs, a node with other `apply` limits refused by both peers, and `rumqttc` clients — a QoS 1 subscriber on one node receives in order what publishers on the sequencer and on a replica sent, each PUBACK after commit; nodes that compacted their logs restart from their snapshots and converge (D-67 B), also after rotating past slot changes (D-79 A); the sequencer stops, another node takes a new slot over and commits, the old one rejoins as a follower (step 5); with takeovers by silence out of reach, rotation hands the slot over at least three times while publishes commit (step 6) (ADR-0008 step 4) |
| Node configuration | `crates/node/tests/config.rs` | the experimental profile passes its checks with flow control off and the decided defaults; every startup rule refused — voters fewer than three or even, listed twice, this node missing, heartbeat or takeover out of range, `read_bytes` above half the frame or zero; `Node::start` refuses a broken configuration (step 7a, D-104 A) |
| Server configuration | `crates/server/src/config.rs` (unit tests) | every documented variable parsed, typos and malformed or out-of-range values refused; the experimental switch selects the cluster path, a missing identity or data directory, an old-path setting, and invalid voters, timing and read sizes refused in that mode (step 7a, D-104 A); loopback by default and any address beyond it refused unless acknowledged (D-107) |
| Server, cluster mode end to end | `crates/server/tests/cluster.rs` | three real `straum-server` processes on loopback and a minimal MQTT 3.1.1 client: QoS 1 publish acknowledged and delivered across nodes; will and CleanSession=0 refused; with the sequencer alive and both replicas stopped no PUBACK and no delivery, and the same publish acknowledged and delivered once a replica restarts over its data; SIGUSR1 handoff, and the first publish after it acknowledged without a resend (D-109 B); a node refused over another cluster's data and its links refused from fresh data; clean rejoin and stops (D-106; unix; about 7 s) |
| Platform | `#[cfg(target_os = "linux")]` tests, run via `scripts/linux.sh` | kernel probes, Linux-only socket options |

## MQTT codec

Every supported packet type, encode and decode:
CONNECT, CONNACK, PUBLISH, PUBACK, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK,
PINGREQ, PINGRESP, DISCONNECT.

Failure and edge cases:

* empty payload, maximum allowed payload, payload one byte over the limit
* truncated packet at every field boundary
* invalid packet type, invalid fixed-header flags
* Remaining Length: 1–4 bytes, 5th continuation byte, value above
  `codec::Limits::max_packet_bytes`
* invalid UTF-8, U+0000 in strings, wildcards in a PUBLISH topic
* packet identifier 0
* several packets in one buffer
* one packet split at every possible byte offset
* the decoder never consumes zero bytes while reporting progress

## MQTT conformance

The codec and the gateway carry the normative rules (`codec/tests/rules.rs`,
`gateway/tests/rules.rs`). Documented deviations of the new path: an empty
ClientId with CleanSession = 0 is refused with CONNACK 0x02; a will, a
persistent session and a session expiry are refused at CONNECT until step 7c
(D-78 A).

## Property tests

* `decode(encode(p)) == p` for every valid packet
* Remaining Length encode/decode for the full value range
* topic filter matching against a naive reference implementation
* apply: the set of receiving sessions equals the reference; each session
  receives at most one delivery per publish

## Fuzzing

`fuzz/` is created by `cargo fuzz init` and has its own `[workspace]`, so it is
outside the main workspace. Requires nightly; works on macOS and Linux.

Targets:

```text
topic_filter      arbitrary bytes → TopicFilter / Topic construction and matching
codec_stream      arbitrary bytes, either side, either version, split anywhere →
                  the new codec (ADR-0008 step 3a); whole equals streamed, and
                  whatever decodes re-encodes to the same packet
```

Run with memory limits so uncontrolled allocation is a finding:

```bash
cargo +nightly fuzz run codec_stream -- -rss_limit_mb=512 -malloc_limit_mb=64 -max_total_time=60
```

On macOS `libfuzzer-sys` may fail with `'cassert' file not found`: the Command
Line Tools are missing `usr/include/c++/v1`. Point the build at the SDK copy
rather than reinstalling them:

```bash
export CXXFLAGS="-isystem $(xcrun --show-sdk-path)/usr/include/c++/v1"
```

Every crash becomes a regression test in `codec` (or `domain`) before the fix.
The old codec's targets went with it at step 8 (D-111).

## End-to-end

`crates/server/tests/cluster.rs` starts three real `straum-server`
processes on loopback and speaks MQTT 3.1.1 to them with a minimal client of
its own (D-106 A): delivery across nodes, the operator's handoff, no PUBACK
without a majority, refused configurations, restarts. `crates/node/tests/`
drives nodes in-process and uses `rumqttc` (`default-features = false`) as
an independent client: our own codec as the client would hide misreadings
of the specification. Memory boundedness under slow consumers was checked by
the old `e2e` suite; the new path's gateway bounds are unit-tested
(`gateway/tests/rules.rs`), not yet end to end.

## Platform tests

Parsing of `/proc` content and `auto` resolution are portable functions tested
with fixture strings on every platform. Only the tests that read real `/proc`
files or set Linux-only socket options are `#[cfg(target_os = "linux")]`:

```bash
scripts/linux.sh test -p transport
```
