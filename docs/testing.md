# Testing

## Running

```bash
cargo test --workspace --all-targets          # everything
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
