# Testing

> **Simulation target: docs/architecture/unified-stream.md §5** — the harness
> with controlled clock, network and disk, its failure list and its eight
> invariants. The oracle described for D-28 (I1–I10, chain C0–C5) is the
> current one for the leader/follower replication as built (phase 0).

## Running

```bash
cargo test --workspace --tests                 # everything, benches excluded
cargo test --workspace --all-targets --no-run # and they still compile
cargo test -p mqtt                     # one crate
cargo test -p mqtt decode_connect      # one test, or any name filter
cargo test -p mqtt --test mqtt_connect # one integration test file
cargo test -p e2e -- --nocapture --test-threads=1
```

## Layers

| Layer | Where | What |
|---|---|---|
| Unit | `#[cfg(test)]` in each module | pure logic, state transitions, error paths |
| Crate integration | `crates/<crate>/tests/` | public API of one crate |
| Property | proptest in core, mqtt, broker | round-trips and invariants |
| Fuzz | `fuzz/` (nightly) | decoder never panics, bounded allocation |
| End-to-end | `crates/e2e/tests/` | real TCP, independent client |
| Crash recovery | `crates/storage/tests/` | every way a WAL tail can be cut, flipped or lied about (docs/storage.md §92) |
| Simulated disk | `crates/storage/tests/simulated.rs` | the production log on `storage::simulated::SimDisk`: power loss at every torn length after acknowledged commits, process crash, injected write and sync failures, and a rewrite failing at each of its writes and syncs with no commit after it; deterministic, no real file or clock (D-28 stage 1, scenarios S1–S3). A failed directory sync after a rewrite poisons the log (D-31); a new log survives a power loss (D-30) |
| Simulated link | `crates/replication/tests/link.rs`, `crates/runtime/tests/simulated_link.rs` | the leader's `Link` under random interleavings of commits, live batches, drops, catch-ups, reports and reconnects (property test: nothing shipped beyond the leader's durable sequence, no hole, watermark monotonic and never ahead of a copy); a leader and a follower on two simulated disks through power loss under `quorum`, `fsynced` and `written` (D-28 stage 2) |
| Deterministic simulation | `crates/sim` | a leader and a follower on simulated disks driven by the production log, `Link`, `Redial`, `Follower` and frame codec, over a connection that delivers bytes in arbitrary pieces (S4) with a receive window and a write-stall limit modelled in the harness (S9 as modelled, not the runtime's timer); crashes, power losses with torn tails, injected sync failures, rewrites under both retention rules, refusals and fresh copies. Checks after every step, with their mapping to unified-stream §5 and D-28, are in ADR-0007 ("Status of task 1"), as are the event probabilities and the copies of runtime glue the harness makes. Links C1–C4 of the D-28 chain are checked by reading the code (`crates/sim/tests/chain.rs`); C0 and C5 are not (D-46). 30 seeds × 400 steps per mode by default; `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim` for the wide sweep; `STRAUM_SIM_SEED=<n>` to reproduce one. Phase 0 task 1 |
| Log record v4 | `crates/journal/tests/format.rs` | record v4 round-trip and malformed input; the batch chain refuses a changed byte, a dropped record, a reorder, a foreign history, a slot going back and a record outside a batch; a torn batch is a tail; xxh3-128 against its reference vector and the batch bytes pinned (ADR-0008 step 1) |
| Simulated chain | `crates/sim/tests/journal.rs` | v4 batches through `Wal<SimDisk>` with random power loss and torn tails: recovery verifies as one chain, every synced batch comes back with the same hash, a torn batch only at the tail; 50 seeds by default, `STRAUM_SIM_SEEDS` for more (unified-stream §5 invariants 1 and 5, single log) |
| Segmented log | `crates/storage/tests/segments.rs` | segments start at batch boundaries and recover in order; removal takes whole segments from the front, stops at a pin, keeps the active and the last sealed one; a torn batch is cut; a power loss never leaves a hole between segments (ADR-0008 step 2) |
| Sequencer and replica | `crates/cluster/tests/protocol.rs` | one rule per test: nothing shipped before it is durable on the sequencer, commit needs a majority and only grows, a replica takes only the owner's next whole batch with an unbroken chain, catch-up from the log and loss below it, the window, a full queue refused, only the heartbeat resends |
| Three-node simulation | `crates/sim/tests/cluster.rs` | sequencer and two replicas on simulated disks: drops, reordering, crashes, power losses with torn writes, partitions, compaction during catch-up, alternating with calm periods; invariants 1, 3, 5, storage keeping its word, the refcount, every compaction floor within the retention rule and a replica lost only below the byte floor (D-57 A); every live replica complete after healing; one seed in three with an uncommitted bound of 4–16. 30 seeds × 1500 events by default, `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim --test cluster` for the sweep |
| MQTT 3.1.1 and 5 codec | `crates/codec/tests/roundtrip.rs`, `crates/codec/tests/rules.rs` | every packet either side may send, in either version, survives encode and decode (2000 cases), split anywhere it decodes whole, arbitrary bytes never panic; one test per rule: properties a packet may not carry, duplicates, out-of-range values, user-property bound, topic alias, 3.1.1 mappings (CONNACK codes, SUBACK 0x80, no PUBACK reason), MQTT 5 short forms, version-only fields, oversized packets refused from the header, subscription options, side-specific DISCONNECT fields, QoS 2 decoded to be refused (ADR-0008 step 3a) |
| Apply function | `crates/apply/tests/state.rs` | one rule per test: one delivery per matching session at the lesser guarantee, the publisher acknowledged after its deliveries, retained messages kept, replaced, removed and delivered on subscribe, unsubscribe, order and exactly-once application, unknown and malformed records stop the node, subscription options, inflight and backlog with acknowledgements, slow-consumer close, the global backlog bound, session close and node reset, subscription and retained bounds, nothing overtaking what waits; a proposal survives its record, and the same log gives the same state and effects with inflight within its bound (properties) (ADR-0008 steps 3b-1, 3b-2) |
| Apply simulation | `crates/sim/tests/apply.rs` | three nodes, `cluster` plus `apply`, clients at every gateway, forwarded proposals over a lossy reordering network, crashes and power losses; at every effect: invariant 4 (committed and durable on a majority), equality with an independent reference model including inflight, backlog and bounds, the same state on every node at every lsn; no session left once every session ended. 30 seeds × 1500 events by default, `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim --test apply` for the sweep |
| Gateway | `crates/gateway/tests/rules.rs` | one rule per test, MQTT bytes through `codec`, commit on demand: connections wait for the gateway's reset, PUBACK and SUBACK only after commit, QoS 1 acknowledged through the log and retried until seen, takeover, keep-alive, topic aliases, the client's receive maximum, QoS 2 refused, a connection at its bound not read, a proposal that does not commit closes its connection, 3.1.1 empty id with CleanSession=0 refused (ADR-0008 step 3b-2) |
| MQTT simulation | `crates/sim/tests/gateway.rs` | MQTT 3.1.1 and 5 clients through `codec` against three nodes (`cluster`, `apply`, `gateway`), takeovers, goodbyes, dropped sockets, crashes and power losses, a mostly ordered network; invariant 4 at every packet, invariant 7 within a gateway, per-session order per topic, no session left after every client has gone. `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim --test gateway` for the sweep |
| Link framing | `crates/cluster/tests/wire.rs` | every frame round-trips, split anywhere it decodes whole; an oversized or malformed frame is refused (ADR-0008 step 4) |
| Node driver | `crates/node/tests/cluster.rs` | three nodes on loopback with logs on disk: one log committed and applied everywhere, a replica and the sequencer restarted from their logs, a node with other `apply` limits refused by both peers, and `rumqttc` clients — a QoS 1 subscriber on one node receives in order what publishers on the sequencer and on a replica sent, each PUBACK after commit (ADR-0008 step 4) |
| Platform | `#[cfg(target_os = "linux")]` tests, run via `scripts/linux.sh` | kernel probes, Linux-only socket options |

Direct broker unit tests do not replace e2e tests.

## MQTT codec

Every supported packet type, encode and decode:
CONNECT, CONNACK, PUBLISH, PUBACK, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK,
PINGREQ, PINGRESP, DISCONNECT.

Failure and edge cases:

* empty payload, maximum allowed payload, payload one byte over the limit
* truncated packet at every field boundary
* invalid packet type, invalid fixed-header flags
* Remaining Length: 1–4 bytes, 5th continuation byte, value above
  `STRAUM_MAX_PACKET_BYTES`
* invalid UTF-8, U+0000 in strings, wildcards in a PUBLISH topic
* packet identifier 0
* several packets in one buffer
* one packet split at every possible byte offset
* the decoder never consumes zero bytes while reporting progress

## MQTT conformance

Tests for normative statements are named after the statement ID, for example
`mqtt_3_1_2_1_protocol_name`. Each decision in CLAUDE.md §6 has a test,
including the documented deviations — which are now one: an empty ClientId
with CleanSession = 0 is refused with CONNACK 0x02 rather than given a
kept session nothing can name again.

Will publishing, retained messages and persistent sessions were deviations
when this was written and are built; §6 is the list that decides, and this
paragraph follows it rather than repeating it.

## Property tests

* `decode(encode(p)) == p` for every valid packet
* Remaining Length encode/decode for the full value range
* topic filter matching against a naive reference implementation
* routing: the set of receiving sessions equals the reference; each session
  receives at most one delivery per publish

## Fuzzing

`fuzz/` is created by `cargo fuzz init` and has its own `[workspace]`, so it is
outside the main workspace. Requires nightly; works on macOS and Linux.

Targets:

```text
decode_packet     arbitrary bytes → decoder
decode_stream     arbitrary bytes split at arbitrary points; result must equal
                  decoding the unsplit input
topic_filter      arbitrary bytes → TopicFilter / Topic construction and matching
codec_stream      arbitrary bytes, either side, either version, split anywhere →
                  the new codec (ADR-0008 step 3a); whole equals streamed, and
                  whatever decodes re-encodes to the same packet
```

Run with memory limits so uncontrolled allocation is a finding:

```bash
cargo +nightly fuzz run decode_packet -- -rss_limit_mb=512 -malloc_limit_mb=64 -max_total_time=60
```

On macOS `libfuzzer-sys` may fail with `'cassert' file not found`: the Command
Line Tools are missing `usr/include/c++/v1`. Point the build at the SDK copy
rather than reinstalling them:

```bash
export CXXFLAGS="-isystem $(xcrun --show-sdk-path)/usr/include/c++/v1"
```

Every crash becomes a regression test in `mqtt` before the fix.

## End-to-end

`e2e` starts the server in-process on `127.0.0.1:0` and talks to it with
`rumqttc` (`default-features = false`, no TLS). Using our own codec as the
client would hide misreadings of the specification.

```text
crates/e2e/tests/
├── mqtt_connect.rs
├── mqtt_publish.rs
├── mqtt_subscribe.rs
├── mqtt_qos1.rs
├── slow_consumer.rs
└── broker_lifecycle.rs
```

`slow_consumer.rs` checks memory boundedness: subscribers that never read,
a publisher that floods; the `queued_bytes` gauge stays within
`STRAUM_SESSION_QUEUE_BYTES` and `STRAUM_TOTAL_QUEUED_BYTES`, and sessions are
closed or messages dropped exactly as CLAUDE.md §8 states. Use metrics, not OS
RSS, so the test is portable.

## Platform tests

Parsing of `/proc` content and `auto` resolution are portable functions tested
with fixture strings on every platform. Only the tests that read real `/proc`
files or set Linux-only socket options are `#[cfg(target_os = "linux")]`:

```bash
scripts/linux.sh test -p transport
```
