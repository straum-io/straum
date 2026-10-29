# Straum protocol conformance

Language-independent checks of the protocol in `docs/protocol.md`. A client
SDK in any language uses them to verify its encoder, decoder, partitioner
and client behavior against the same contract as the Rust crates.

| Directory | What it checks |
|---|---|
| `frames/` | encoding and decoding: golden frames of every request and response |
| `partitioner/` | the default partitioner of docs/protocol.md section 7 |
| `scenarios/` | a broker's behavior, played against a live broker |
| `client-scenarios/` | a client's behavior (section 6), played against a scripted fake server |

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
STRAUM_UPDATE_FIXTURES=1 cargo test -p straum-proto --test fixtures committed_fixtures_are_up_to_date
```

## Partitioner

`partitioner/vectors.json` lists keys (hex) with their `crc32c` and, in
`partitions`, the partition for each partition count from 1 to 16
(element `i` for count `i + 1`). An SDK passes when its default
partitioner gives the same partition for every key and count.

## Broker scenarios

Each `scenarios/<name>.json` is one scenario, run against a fresh broker:

```json
{
  "name": "fetch_wait",
  "description": "what it checks",
  "broker": { "max_fetch_waiters": 1 },
  "steps": [ { "connect": "a" }, { "send": "a", "id": "h", "request": { "type": "Hello", "body": {} } } ]
}
```

`broker` holds settings for the broker (keys of
`crates/straum-broker/README.md`); the runner adds a new data directory,
`--listen 127.0.0.1:0`, `--cluster-id conformance` and `--node-id 1`, and
reads the address from the broker's `straum-server: listening on <address>`
line on stderr. Steps run in order:

| Step | Meaning |
|---|---|
| `{"connect": "a"}` | open connection `a` |
| `{"send": "a", "id": "x", "request": {...}}` | send a request (JSON as in the frame descriptions) with the next correlation id of `a` (1, 2, …), or `correlation_id` if given; sends never block the script |
| `{"send_raw": "a", "id": "x", "correlation_id": "9", "hex": "..."}` | send bytes as they are; `correlation_id` is the one the response will carry |
| `{"expect": "a", "id": "x", "response": {...}, "within_ms": 5000, "not_before_ms": 0}` | the response to send `x`, in any order with others, within `within_ms` of now and at least `not_before_ms` after the send |
| `{"expect_closed": "a", "within_ms": 5000}` | the broker closes `a` |
| `{"close": "a"}` | close `a` |
| `{"sleep_ms": 100}` | wait |
| `{"repeat": 3, "steps": [...]}` | the steps, 3 times; ids get `#0`, `#1`, … |

A response matches if every field the expected value names matches;
omitted fields are not checked. Arrays must have the same length and match
element by element. `"*"` matches anything; `{"$le": n}` and `{"$ge": n}`
compare integers (numbers or decimal strings); `{"$len": n}` checks an
array's length.

## Client scenarios

Each `client-scenarios/<name>.json` scripts a server and states what the
client under test does and what it must return:

```json
{
  "name": "not_leader_then_metadata",
  "description": "what it checks",
  "client": { "op": "produce", "topic": "t", "partition": 0, "producer_id": "77", "producer_seq": "5", "messages": [] },
  "server": [ { "accept": true }, { "expect": { "type": "Hello" }, "reply": { "type": "HelloOk", "body": {} } } ],
  "result": { "ok": { "base_seq": "11" } }
}
```

- `client`: the operation (`produce` or `fetch` with the fields of the
  request) made through the SDK's normal API, after connecting to the fake
  server; `max_attempts`, if present, is the SDK's retry limit.
- `server`: steps the fake server plays in order: `{"accept": true}` waits
  for a new connection; `{"expect": <request>, "reply": <response>}` reads
  the next request, matches it (as above) and answers it with its
  correlation id; `"not_before_ms"` requires the request to come at least
  that long after the previous reply; `"close": true` closes the
  connection, after a reply if there is one. In replies, `"{{host}}"` and
  `"{{port}}"` are the fake server's own address.
- `result`: `{"ok": <response body>}` (matched as above) or `{"error":
  <code name, or the number of an unknown code>}`.

An SDK passes a scenario when the fake server played every step, received
nothing more within 300 ms, and the operation's outcome matches `result`.
`straum-fake-server <scenario.json>` plays one script from any language: it
prints `straum-fake-server: listening on <host:port>`, and exits 0 if the
client did exactly what the script expects, 1 otherwise.

## Runners

The Rust runner uses straum-client:

```bash
cargo build --release -p straum-broker -p straum-conformance
target/release/straum-conformance broker target/release/straum-server   # scenarios/
target/release/straum-conformance client                                # client-scenarios/
```

Both are also cargo tests (`crates/straum-broker/tests/conformance.rs`,
`crates/straum-conformance/tests/client_scenarios.rs`).

A runner in another language:

1. Frames: decode every `frames/*.bin` and compare with its `.json`; encode
   the `.json` message and compare with the `.bin`.
2. Partitioner: compare the SDK's partitioner with `partitioner/vectors.json`.
3. Broker scenarios: build `straum-server` (crate `straum-broker`), start
   it per scenario as above, play the steps with the SDK's connection layer
   (raw requests, pipelining, correlation ids), and match responses with the
   rules above.
4. Client scenarios: start `straum-fake-server <scenario.json>`, read its
   address, run the `client` operation with the SDK pointed at it, compare
   the outcome with `result`, and require the fake server to exit 0.
