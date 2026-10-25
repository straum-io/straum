# Straum protocol conformance

Language-independent checks of the protocol in `docs/protocol.md`. A client
SDK in any language uses them to verify its encoder, decoder, partitioner
and client behavior against the same contract as the Rust crates.

| Directory | What it checks |
|---|---|
| `frames/` | encoding and decoding: golden frames of every request and response |
| `partitioner/` | the default partitioner of docs/protocol.md section 7 |

## Frames

`frames/index.json` lists every fixture by name. For each name:

- `<name>.bin`: one complete frame (length, type, correlation id, body).
- `<name>.json`: its description:

```json
{
  "name": "produce_keyed",
  "note": "what the fixture covers",
  "direction": "request",
  "type_code": 6,
  "correlation_id": "11",
  "frame_bytes": 111,
  "message": { "type": "Produce", "body": { "topic": "orders", "...": "..." } }
}
```

JSON conventions: field names as in docs/protocol.md; `u8`, `u16` and
`u32` are numbers; `u64` values (seqs, ids, timestamps, correlation ids)
are decimal strings; bytes (keys, header names and values, bodies, auth
payloads) are lowercase hex; an absent key is `null`; error codes are their
names (`"UnknownTopic"`), or a number for a code version 0 does not define.

An SDK passes when, for every fixture, decoding `<name>.bin` gives the
fields of `message`, and encoding `message` with `correlation_id` gives
exactly `<name>.bin`. The fixtures include boundary values (largest
integers, empty strings and arrays, a present empty key, an 80-byte topic
name) and messages at and just over the broker's default limits; the ones
over a limit are valid frames that a broker with default limits refuses
with `MessageTooLarge`.

The fixtures are generated from `crates/straum-proto`
(`tests/fixtures.rs`); that test fails if the committed files are out of
date. To regenerate after a protocol change:

```bash
STRAUM_UPDATE_FIXTURES=1 cargo test -p straum-proto --test fixtures
```

## Partitioner

`partitioner/vectors.json` lists keys (hex) with their `crc32c` and, in
`partitions`, the partition for each partition count from 1 to 16
(element `i` for count `i + 1`). An SDK passes when its default
partitioner gives the same partition for every key and count.
