# Straum protocol

The binary protocol between Straum clients and a Straum broker, over TCP.
Sections 1–9 are version 0. Section 10 is version 1, a draft: no broker
serves it yet, and its golden frames do not exist yet.

Version 0 is served by a single-node broker (`crates/straum-broker`); its
fields already describe partitions, leaders, leader epochs and replicas, so
a clustered broker can serve the same version. Fields that version 0 does
not use are marked **reserved**: a v0 client sends the value given, and a
v0 broker either ignores the field or checks the value, as stated.

The contract is this document, the golden frames in `conformance/frames/`,
the partitioner vectors in `conformance/partitioner/`, and the scenarios in
`conformance/scenarios/` (broker behavior) and
`conformance/client-scenarios/` (client behavior). Code:
`crates/straum-proto` (encoding and decoding, no I/O).

## 1. Encoding

All integers are little-endian and unsigned unless stated.

| Type | Encoding |
|---|---|
| `u8`, `u16`, `u32`, `u64` | fixed width |
| `string` | `len u16`, then `len` bytes of UTF-8 |
| `bytes` | `len u32`, then `len` bytes |
| `array<T>` | `count u32`, then `count` elements |

A decoder rejects (`MalformedRequest`, or a client-side decode error) a
body that ends early, a string that is not UTF-8, a length or count larger
than the bytes left, an enum or flag value this version does not define,
and trailing bytes after the last field. Decoding never trusts a count to
allocate: elements are read one at a time from the bytes present.

## 2. Frames

```text
offset 0   length u32           bytes after this field: 10 + body length
offset 4   type u16             message type (section 4)
offset 6   correlation_id u64   chosen by the client; echoed in the response
offset 14  body                 length − 10 bytes
```

- A frame's total size is `4 + length`. The broker's limit is
  `max_frame_bytes` (configuration, default 8 MiB), also reported in
  `HelloOk`. A frame over it gets `FrameTooLarge` with the frame's
  correlation id, and the broker closes the connection without reading
  the body. A `length` below 10 gets `MalformedRequest` with correlation
  id 0, then the connection is closed.
- **Pipelining.** A client may send further requests before earlier ones
  are answered. Responses carry the request's correlation id and may arrive
  in any order: a waiting Fetch does not hold back the responses behind it.
  The broker does not interpret correlation ids; reusing one while a
  request with it is outstanding makes the responses ambiguous to the
  client.
- An unknown message type gets `Unsupported` (the connection stays open).
  A body that does not decode gets `MalformedRequest` (the connection stays
  open; the frame was read whole).

## 3. Connection lifecycle

1. The client connects and sends `Hello`. Until a `Hello` succeeds, every
   request but `Ping` gets `Unauthenticated`. A second `Hello` on the same
   connection gets `Unsupported`.
2. `HelloOk` fixes the protocol version for the connection.
3. Either side may close the connection at any time. Requests whose
   response was not received have an unknown outcome (section 8).

### Versions and compatibility

`Hello` carries the client's lowest and highest supported versions; the
broker picks the highest version both support, or answers
`UnsupportedVersion` with its own range in the error body. A v0 broker
supports exactly version 0.

Rules for later versions:

- A version's layouts never change. A new version may add message types,
  append fields at the end of a body, add error codes, and define values of
  reserved fields or flags; it may not reorder, remove or reinterpret.
- A broker supports a contiguous range of versions; a client offers one.
- An error code a client does not know is treated as not retryable.
- A message type the broker does not know gets `Unsupported`, never a
  closed connection.

## 4. Message types

| Type | Request | Success response (type) |
|---|---|---|
| `0x0001` | `Hello` | `HelloOk` (`0x8001`) |
| `0x0002` | `Ping` | `Pong` (`0x8002`) |
| `0x0003` | `Metadata` | `MetadataOk` (`0x8003`) |
| `0x0004` | `CreateTopic` | `CreateTopicOk` (`0x8004`) |
| `0x0005` | `DeleteTopic` | `DeleteTopicOk` (`0x8005`) |
| `0x0006` | `Produce` | `ProduceOk` (`0x8006`) |
| `0x0007` | `Fetch` | `FetchOk` (`0x8007`) |
| `0x0008` | `ListTopics` | `ListTopicsOk` (`0x8008`) |
| `0x0009` | `DescribeTopic` | `DescribeTopicOk` (`0x8009`) |
| `0x000a` | `BrokerStats` | `BrokerStatsOk` (`0x800a`) |
| `0xffff` | — | `Error`, the response to any request that failed |

A success response's type is the request's type with bit 15 set. Types
`0x8000`–`0xfffe` are never requests.

`ListTopics`, `DescribeTopic` and `BrokerStats` are the admin requests the
console uses; there is no separate HTTP API.

### Topic names

1 to 80 bytes of `A–Z a–z 0–9 . _ -`; anything else is `MalformedRequest`.
Names starting with `__` are reserved for the broker's internal topics: a
request that creates, produces to or fetches from one gets `Forbidden`.

### 4.1 Hello

| Field | Type | |
|---|---|---|
| `min_version` | `u16` | |
| `max_version` | `u16` | `min_version ≤ max_version`, else `MalformedRequest` |
| `client_name` | `string` | free text, for the broker's diagnostics |
| `auth_mechanism` | `string` | v0 supports only `none`; anything else is `Unauthenticated` |
| `auth_payload` | `bytes` | reserved: empty for `none` |

`HelloOk`:

| Field | Type | |
|---|---|---|
| `version` | `u16` | the version used on this connection |
| `cluster_id` | `string` | |
| `node_id` | `u32` | the broker answering |
| `max_frame_bytes` | `u32` | the broker's frame limit (section 2) |
| `max_message_bytes` | `u32` | the broker's body limit per message |

### 4.2 Ping

`Ping { nonce u64 }` → `Pong { nonce u64 }` with the same nonce. Allowed
before `Hello`, for health checks.

### 4.3 Metadata

| Field | Type | |
|---|---|---|
| `topics` | `array<string>` | at most 1000; more is `MalformedRequest`. Empty: no topics, only the cluster and its nodes |

`MetadataOk`:

| Field | Type | |
|---|---|---|
| `cluster_id` | `string` | |
| `controller_node_id` | `u32` | reserved: v0 sends its own node id |
| `nodes` | `array<Node>` | every node of the cluster; v0: itself |
| `topics` | `array<TopicMetadata>` | in request order |

`Node`: `node_id u32`, `host string`, `port u16`, `rack string` (reserved:
empty).

`TopicMetadata`: `name string`, `error u16` (0, or `UnknownTopic` with no
partitions), `partitions array<PartitionMetadata>`.

`PartitionMetadata`:

| Field | Type | |
|---|---|---|
| `partition` | `u32` | |
| `leader_node_id` | `u32` | |
| `leader_epoch` | `u32` | v0: 1 |
| `replicas` | `array<u32>` | v0: the leader only |
| `in_sync_replicas` | `array<u32>` | reserved: v0 sends `replicas` |
| `start_seq` | `u64` | the first seq still readable |
| `next_seq` | `u64` | the seq the next message gets; the last message's seq is `next_seq − 1`, none if `next_seq = start_seq` |

### 4.4 CreateTopic

| Field | Type | |
|---|---|---|
| `name` | `string` | |
| `partitions` | `u32` | v0: 1; 0 is `MalformedRequest`, more is `Unsupported` |
| `replication_factor` | `u16` | reserved: v0 accepts 1; 0 is `MalformedRequest`, more is `Unsupported` |
| `retention_ms` | `u64` | 0: keep everything. Recorded; v0 does not release by age |

`CreateTopicOk`: empty body. An existing name gets `TopicExists`.

### 4.5 DeleteTopic

`DeleteTopic { name string }` → `DeleteTopicOk` (empty body). v0 answers
`Unsupported` to every DeleteTopic.

### 4.6 Message

The unit of `Produce` and `Fetch`.

| Field | Type | |
|---|---|---|
| `flags` | `u8` | bit 0: key present; bit 1: headers present; bits 2–7 reserved, must be 0 |
| `key` | `u16` length, bytes | only if bit 0 is set |
| `headers` | `u16` count, then per header: `name` (`u16` length, bytes), `value` (`u32` length, bytes) | only if bit 1 is set; count ≥ 1 |
| `body` | `bytes` | |

A message without key and headers costs 1 byte (`flags = 0`) on top of the
body. A present but empty key (`flags` bit 0, length 0) is distinct from no
key. Header names and values are bytes; names need not be unique, and their
order is kept.

The broker's limits (configuration, defaults in brackets), each checked
before any write and answered with `MessageTooLarge`:

| Limit | Default |
|---|---|
| `max_message_bytes`: body | 1 MiB |
| `max_key_bytes`: key | 1 KiB |
| `max_headers`: headers per message | 64 |
| `max_header_name_bytes`: one header name | 256 B |
| `max_header_bytes`: all names and values of one message | 16 KiB |

**Deduplication.** The broker's store keeps each distinct body once, by the
BLAKE3 hash of the body alone. Key and headers belong to the publication:
the same body published with different keys or headers is stored once and
fetched with each publication's own key and headers. Every publication gets
its own seq; deduplication is invisible to clients except in `BrokerStats`.

### 4.7 Produce

| Field | Type | |
|---|---|---|
| `topic` | `string` | |
| `partition` | `u32` | |
| `producer_id` | `u64` | reserved: v0 does not check it; clients send a stable id per producer |
| `producer_seq` | `u64` | reserved: v0 does not check it; clients send it per request, unchanged on a retry |
| `ack_level` | `u8` | 0 `written`, 1 `durable`, 2 `replicated`; v0 supports only 1, the others get `UnsupportedAckLevel`, other values `MalformedRequest` |
| `expected_leader_epoch` | `u32` | 0: any; otherwise the request is refused with `FencedLeaderEpoch` unless it equals the partition's leader epoch |
| `timeout_ms` | `u32` | reserved: v0 ignores it (the bound for `replicated` acks) |
| `messages` | `array<Message>` | at least one |

`ProduceOk`:

| Field | Type | |
|---|---|---|
| `base_seq` | `u64` | seq of the first message; the request's messages have `base_seq … base_seq + count − 1`, in order, with no other message between them |
| `count` | `u32` | |
| `leader_epoch` | `u32` | |
| `timestamp_ms` | `u64` | the time the broker assigned to the first message |

**Durability.** With `durable`, `ProduceOk` is sent only after the
messages and their WAL records are on disk (`fdatasync`) on the leader. The
broker commits produce requests of all connections together in groups; a
request never spans two groups.

**Timestamps.** The broker assigns each message's timestamp (milliseconds
since the Unix epoch) when its group commits, and records it in the WAL;
within a partition, timestamps never decrease. Clients cannot set them.

### 4.8 Fetch

| Field | Type | |
|---|---|---|
| `topic` | `string` | |
| `partition` | `u32` | |
| `from_seq` | `u64` | |
| `max_bytes` | `u32` | of the returned `FetchedMessage`s as encoded; capped by the broker's `max_fetch_bytes` |
| `max_wait_ms` | `u32` | capped by the broker's `max_fetch_wait_ms` |
| `expected_leader_epoch` | `u32` | as in Produce |

The response holds the messages with seq ≥ max(`from_seq`, `start_seq`),
in seq order, while their total encoded size (each `FetchedMessage` as
below: `seq`, `timestamp_ms`, `flags`, the key and headers with their
lengths, the body with its length; 21 bytes for an empty body without key
or headers) stays within `max_bytes`, and at most the broker's
`max_fetch_messages` (default 10 000) of them; the first such message is
returned even if it alone is larger. If there is none, the
broker waits up to `max_wait_ms` for one to be committed and answers then
(possibly with no messages). A `from_seq` past `next_seq` waits the same
way. Clients keep their own position (`next` = last seq + 1).

`FetchOk`:

| Field | Type | |
|---|---|---|
| `leader_epoch` | `u32` | |
| `start_seq` | `u64` | as in Metadata; a `from_seq` below it skipped released messages |
| `next_seq` | `u64` | |
| `messages` | `array<FetchedMessage>` | |

`FetchedMessage`: `seq u64`, `timestamp_ms u64`, then the fields of a
Message (section 4.6).

### 4.9 ListTopics

Topics by name (bytewise order), one page per request.

| Field | Type | |
|---|---|---|
| `cursor` | `string` | empty: the first page; otherwise the `next_cursor` of the previous page |
| `limit` | `u32` | the most topics to return; 0 is `MalformedRequest`, more than 1000 is taken as 1000 |

`ListTopicsOk`:

| Field | Type | |
|---|---|---|
| `topics` | `array<TopicSummary>` | the topics whose names sort after `cursor`, at most `limit` |
| `next_cursor` | `string` | empty if no topic follows this page; otherwise pass it as the next request's `cursor` |

`TopicSummary`: `name string`, `partitions u32`.

The broker keeps no state between pages. Clients pass `next_cursor` back
unchanged; a non-empty cursor the broker did not return gives unspecified
results.
A topic created between two pages may or may not appear on a later page.
In v0 a cursor is the name of the last topic of its page; this is not part
of the contract.

### 4.10 DescribeTopic

`DescribeTopic { name string }` names one topic; for several, send one
request each. `DescribeTopicOk`:

| Field | Type | |
|---|---|---|
| `name` | `string` | |
| `replication_factor` | `u16` | |
| `retention_ms` | `u64` | |
| `partitions` | `array<PartitionDescription>` | |

`PartitionDescription`: `partition u32`, `start_seq u64`, `next_seq u64`,
`message_count u64` (live messages: `next_seq − start_seq`), `index_bytes
u64` (bytes of the partition's own index on disk; bodies are shared across
partitions through deduplication and are not attributed to one).

### 4.11 BrokerStats

Empty request body. `BrokerStatsOk`, counters since the broker started.
The store counters (`fsyncs` to `index_bytes`) include the broker's own
records, such as one per CreateTopic:

| Field | Type | |
|---|---|---|
| `node_id` | `u32` | |
| `open_connections` | `u32` | |
| `pending_bytes` | `u64` | bytes of produce requests read and not yet answered |
| `pending_bytes_peak` | `u64` | the highest `pending_bytes` since start |
| `max_pending_bytes` | `u64` | the configured bound |
| `fetch_waiters` | `u32` | fetches waiting for data now |
| `produce_requests` | `u64` | |
| `groups_committed` | `u64` | store commits of produce groups |
| `fsyncs` | `u64` | of the store |
| `messages_accepted` | `u64` | |
| `messages_deduplicated` | `u64` | publications whose body was already stored |
| `body_bytes_received` | `u64` | |
| `body_bytes_new` | `u64` | bodies stored as new objects |
| `wal_bytes` | `u64` | |
| `segment_bytes` | `u64` | |
| `index_bytes` | `u64` | written to partition indexes |

## 5. Errors

`Error` (type `0xffff`):

| Field | Type | |
|---|---|---|
| `code` | `u16` | below |
| `message` | `string` | for people; not to be parsed |
| `leader_node_id` | `u32` | `NotLeader`, `FencedLeaderEpoch`: the current leader; otherwise 0 |
| `leader_epoch` | `u32` | `NotLeader`, `FencedLeaderEpoch`: its epoch; otherwise 0 |
| `retry_after_ms` | `u32` | `Overloaded`: a suggested wait, 0 for none; otherwise 0 |
| `min_version` | `u16` | `UnsupportedVersion`: the broker's range; otherwise 0 |
| `max_version` | `u16` | as above |

| Code | Name | v0 sends it when | Retryable |
|---|---|---|---|
| 1 | `UnknownTopic` | the topic does not exist (in Metadata: per topic) | no |
| 2 | `UnknownPartition` | the partition is not one of the topic's | no |
| 3 | `TopicExists` | CreateTopic of an existing name | no |
| 4 | `MessageTooLarge` | a message exceeds a limit of section 4.6, or a Produce's messages exceed what one store commit can hold | no |
| 5 | `FrameTooLarge` | the frame exceeds `max_frame_bytes`; the connection is closed | no |
| 6 | `Overloaded` | a Fetch would wait while `max_fetch_waiters` fetches already wait | after backing off |
| 7 | `UnsupportedAckLevel` | `ack_level` 0 or 2 | no |
| 8 | `UnsupportedVersion` | no common version in Hello | no |
| 9 | `Unsupported` | an unknown message type, a second Hello, DeleteTopic, CreateTopic with more than one partition or replica | no |
| 10 | `Unauthenticated` | a request before Hello, or an auth mechanism other than `none` | no |
| 11 | `Forbidden` | a reserved topic name (`__…`) | no |
| 12 | `MalformedRequest` | a body that does not decode, or a field value outside its definition | no |
| 13 | `NotLeader` | never in v0 | after refreshing Metadata |
| 14 | `FencedLeaderEpoch` | `expected_leader_epoch` is neither 0 nor the partition's leader epoch | after refreshing Metadata |
| 15 | `StorageError` | the store failed to read or write; after a failed write the broker refuses every write until restarted | no |

Code 0 is never sent.

## 6. Normative client behavior

A client SDK implements these rules; `conformance/client-scenarios/` checks
them against a scripted server.

1. **Leader changes.** On `NotLeader` or `FencedLeaderEpoch`, the client
   refreshes `Metadata` for the topic (on any connected node), connects to
   the partition's leader as listed there, and retries the same request
   with the same `producer_id` and `producer_seq`, with
   `expected_leader_epoch` set to the epoch Metadata reported. It gives up
   after a configured number of attempts (default 5).
2. **Overloaded.** The client waits, then retries the same request: at
   least `retry_after_ms` if it is non-zero, otherwise an exponential
   backoff from 10 ms to at most 1 s, and at most the configured number of
   attempts.
3. **Connection lost** before the response: the client reconnects (Hello
   again) and retries a Produce with the same `producer_id` and
   `producer_seq`; a Fetch is retried from the same `from_seq`. Without
   idempotent produce (a later version), a retried Produce may store its
   messages twice.
4. **Not retryable:** every other error code, and any unknown code. The
   client returns the error to its caller.
5. The client never reuses a correlation id of an outstanding request.

## 7. Default partitioner

Every SDK that picks partitions by key uses this function, so that the same
key lands in the same partition whatever the client's language:

```text
partition(key, partition_count) = crc32c(key) mod partition_count
```

`crc32c` is CRC-32/ISCSI (Castagnoli polynomial `0x1EDC6F41`, reflected,
initial value and final XOR `0xFFFFFFFF`; `crc32c("123456789") =
0xE3069283`), over the key bytes as sent, as an unsigned 32-bit value.
`mod` is the unsigned remainder. An empty key (present, length 0) hashes to
0 and goes to partition 0.

Why CRC32C: it is fully specified by a standard (RFC 3720), it is in Go's
standard library (`hash/crc32` with the Castagnoli table, hardware
accelerated on amd64 and arm64) and in most languages' standard or common
libraries, it is already a dependency of the store, and every output bit
depends on every input bit, so the remainder spreads keys across any
partition count. Measured on 100 000 keys of each of three kinds
(`user-<n>`, 8-byte little-endian counters, random 16 bytes), the largest
χ² per degree of freedom over 2 to 16 partitions was 2.54 for CRC32C and
2.64 for FNV-1a 64 (about 1 is uniform; both within noise). FNV-1a was not
taken: its low bits depend only on the low bits of the input bytes, and it
has no standard of its own.

Messages without a key: the client chooses; the reference client sends
them to partitions in turn (round robin per producer). This is not
normative.

Test vectors: `conformance/partitioner/vectors.json`.

## 8. Delivery guarantees in v0

- Order: per partition, by seq. A Produce's messages are contiguous.
- An acknowledged message (`ProduceOk` at `durable`) survives a broker
  crash and is fetched with the same seq, timestamp, key, headers and body.
- A Produce whose response was lost may or may not have been committed;
  retrying it may store its messages twice (section 6, rule 3).
- Consumers keep their own positions; the broker stores no offsets.

## 9. Reserved for clustering and security

| Field | v0 | v1 (section 10) |
|---|---|---|
| `Hello.auth_mechanism`, `auth_payload` | empty payload (`none` only) | used: SCRAM-SHA-256 (10.7) |
| `MetadataOk.controller_node_id` | the answering node | as v0 |
| `Node.rack` | empty | as v0 |
| `PartitionMetadata.in_sync_replicas` | equals `replicas` | as v0 |
| `CreateTopic.replication_factor` | 1 | as v0 |
| `Produce.producer_id`, `producer_seq` | not checked | used when `producer_id` ≠ 0 (10.14) |
| `Produce.ack_level` `written`, `replicated` | `UnsupportedAckLevel` | as v0 |
| `Produce.timeout_ms` | ignored | as v0 |
| `expected_leader_epoch` | checked against the constant epoch 1 | as v0 |
| `Error.leader_node_id`, `leader_epoch` | set for `FencedLeaderEpoch` | as v0 |
| `NotLeader` | never sent | as v0 |
| `Message.flags` bits 2–7 | 0 | as v0 |
| `DeleteTopic` | `Unsupported` | as v0 |

## 10. Version 1

Version 1 is version 0 plus TLS, SCRAM-SHA-256 authentication, ACLs and
their admin requests, idempotent produce, Fetch by timestamp range, and
responses that can gain fields without a new version. Decisions: ADR-0015
(D-244 A to D-249 A, D-258 C, D-259 A). Everything sections 1–9 say holds
in version 1 unless this section says otherwise.

### 10.1 Negotiation

- A version 1 broker serves versions 0 and 1: `min_version` 0,
  `max_version` 1 in an `UnsupportedVersion` error. It picks the highest
  version both sides support (section 3).
- `Hello` has the same layout in every version, since the broker decodes
  it before a version exists (section 4.1; its fields' v1 meaning: 10.5).
- A connection that negotiates version 0 behaves as sections 1–9 say,
  with two exceptions that come from the broker's settings, not from the
  version: the frame bound before authentication (10.4), and a broker
  with `auth = scram` refuses every version 0 Hello, since version 0 has
  only `none` (10.5).
- A client that sends credentials offers `min_version` 1, so a version 0
  broker answers `UnsupportedVersion` instead of accepting `none`.
- Every error that answers a `Hello` has exactly the seven fields of
  section 5, with nothing appended, whatever versions the Hello offered:
  the client decodes it before a version is fixed.

### 10.2 Encoding

Version 1 uses the types of section 1, and one more:

| Type | Encoding |
|---|---|
| `uvarint` | an unsigned integer up to 2^32 − 1 in 1 to 5 bytes, 7 bits per byte, the low group first; the high bit is set on every byte but the last |

**Requests** are decoded as in section 1: a body that ends early, a field
outside its definition, and trailing bytes are `MalformedRequest`. A new
request field needs a new version.

**Responses**, `HelloOk` and `Error` included, can gain fields without a
new version:

- A response body has no length of its own: the frame bounds it
  (section 2). A decoder reads the fields it knows, in order, and ignores
  every byte after the last of them.
- Every struct that is an element of an array in a response (an **item**)
  starts with `len uvarint`, the number of bytes of the item after `len`.
  A decoder reads the fields it knows and skips to the item's end.
  Items in version 1: `Node`, `TopicMetadata`, `PartitionMetadata`,
  `TopicSummary`, `PartitionDescription`, `FetchedMessage`,
  `PrincipalSummary`, and `AclEntry` in `ListAclsOk`. Array elements that
  are integers or strings (`replicas`, `in_sync_replicas`) and message
  headers have no `len`: they cannot gain fields.
- Fields are only appended, at the end of a body or of an item. A field
  is never removed or reordered and its meaning never changes; a change
  that cannot follow this rule needs a new version.
- The fields version 1 defines are required: a body or item that ends
  before one of them is malformed. A field appended later is optional: if
  a body or item ends before it, it reads as its default (0, empty
  string, empty bytes, empty array), and this document gives that default
  with the field. A field is present whole or not at all; a body or item
  that ends inside a field is malformed.
- A decoder rejects a `len` longer than 5 bytes, a value above 2^32 − 1, a
  `len` not in its shortest form (a last byte of 0 after the first), and
  a `len` larger than the bytes left in the enclosing body or item.
  Skipping never reads past the enclosing length and never allocates
  from a count (section 1).

Example: an item of 21 bytes, then a decoder that knows 13 of them.

```text
0x15                    len = 21
<13 bytes it decodes>
<8 bytes it skips>
```

The `len` of an item is part of its encoded size wherever a size is
counted (Fetch `max_bytes`, 10.15). Its cost per item, the overhead of
version 1 over version 0 (an estimate from the layouts; the measurement
on the conformance fixtures comes with the golden frames):

| Item size after `len` | `len` bytes |
|---|---|
| up to 127 | 1 |
| 128 – 16 383 | 2 |
| 16 384 – 2 097 151 | 3 |
| 2 097 152 – 268 435 455 | 4 |
| larger | 5 |

### 10.3 Message types

The types of section 4, and:

| Type | Request | Success response (type) | Section |
|---|---|---|---|
| `0x000b` | `AuthContinue` | `AuthContinueOk` (`0x800b`) | 10.8 |
| `0x000c` | `WhoAmI` | `WhoAmIOk` (`0x800c`) | 10.9 |
| `0x000d` | `PutPrincipal` | `PutPrincipalOk` (`0x800d`) | 10.10 |
| `0x000e` | `DeletePrincipal` | `DeletePrincipalOk` (`0x800e`) | 10.10 |
| `0x000f` | `ListPrincipals` | `ListPrincipalsOk` (`0x800f`) | 10.10 |
| `0x0010` | `AddAcl` | `AddAclOk` (`0x8010`) | 10.11 |
| `0x0011` | `RemoveAcl` | `RemoveAclOk` (`0x8011`) | 10.11 |
| `0x0012` | `ListAcls` | `ListAclsOk` (`0x8012`) | 10.11 |
| `0x0013` | `InitProducer` | `InitProducerOk` (`0x8013`) | 10.14 |

On a version 0 connection these types are unknown and get `Unsupported`.

Layouts that differ from version 0: `HelloOk` (10.5), `MetadataOk`,
`ListTopicsOk`, `DescribeTopicOk`, `BrokerStatsOk` (10.13), `Produce`
(10.14), `Fetch` and `FetchOk` (10.15), and `Error` (10.16, the same
bytes until a field is appended). The other requests and responses of
section 4 keep their version 0 layout; as responses, they follow the
rules of 10.2.

### 10.4 TLS and the connection lifecycle

**TLS** (ADR-0015 §2). A broker configured with a certificate
(`tls_cert_file`, `tls_key_file`) speaks TLS only on its listener: the
client starts the TLS handshake right after the TCP connection. There is
no plaintext negotiation and no upgrade on an open connection. Inside
TLS, frames are as in section 2, for both versions.

- TLS 1.3; TLS 1.2 (ECDHE with AEAD suites only) when the broker sets
  `tls_min_version = 1.2`. No session resumption, no tickets, no early
  data: every connection does a full handshake.
- ALPN `straum`. A client that offers ALPN without `straum` fails the
  handshake; one that offers no ALPN is accepted. Clients offer `straum`.
- Client certificates (`tls_client_auth`): `none`; `optional`, a
  certificate is verified if sent; `required`, a certificate that
  verifies against `tls_client_ca_file` is a condition of the handshake.
  A certificate is never a substitute for authentication in version 1.
- A failed handshake (certificate, ALPN, version) closes the connection
  without a frame.
- Which listen address needs TLS and `auth = scram` is a broker setting
  (ADR-0015 §1): every address but loopback needs both. A client never
  sends a password to an address other than loopback without TLS.

**States of a connection:**

| State | Served | Any other request |
|---|---|---|
| before `Hello` | `Ping`, `Hello` | `Unauthenticated`; the connection stays open |
| authenticating: `HelloOk` sent with `auth_complete = 0` | `Ping`, `AuthContinue` | `Unauthenticated`; open. A second `Hello` gets `Unsupported` |
| authenticated: `auth_complete = 1` | every request, each checked against the principal's ACLs (10.12) | — |

Bounds before authentication (ADR-0015 §2, §3.4, §3.5; broker settings
with their defaults):

- From TCP accept to `auth_complete = 1`, the TLS handshake included, at
  most `auth_timeout_ms` (10 000); then the broker closes the connection
  without a frame.
- Until `auth_complete = 1` a frame is at most `max_preauth_frame_bytes`
  (16 384) in total size (`4 + length`), on every version: a larger one
  gets `FrameTooLarge` and the connection is closed. `max_frame_bytes`
  (section 2, reported in `HelloOk`) applies from `auth_complete = 1`.
- A connection over `max_unauthenticated_connections` (64) or
  `max_unauthenticated_per_source` (16, per IPv4 address or IPv6 /64) is
  closed at accept, without a frame.

**Authentication failure.** Every cause (unknown principal, wrong proof,
expired credential, malformed SCRAM message, `none` on a broker with
`auth = scram`, an `AuthContinue` once authenticated) gets
`Error{Unauthenticated, "authentication failed"}`, the same message for
every cause, and the connection is closed after it is written. The broker
may delay this answer, up to `auth_failure_delay_max_ms` (5 000), when a
source fails repeatedly (ADR-0015 §3.4).

### 10.5 Hello and HelloOk

`Hello` (section 4.1), version 1 meaning:

| Field | Version 1 |
|---|---|
| `auth_mechanism` | `none` or `SCRAM-SHA-256`, compared bytewise |
| `auth_payload` | `none`: empty, otherwise `MalformedRequest`. `SCRAM-SHA-256`: the client-first-message (10.7) |

The broker's answer:

| Negotiated version, `auth_mechanism` | Broker `auth = none` | Broker `auth = scram` |
|---|---|---|
| 1, `none` | `HelloOk`, `auth_complete = 1` | authentication failure (10.4) |
| 1, `SCRAM-SHA-256` | `Unsupported`; open | `HelloOk` with the server-first-message, `auth_complete = 0`; or authentication failure |
| 1, any other | `Unsupported`; open | `Unsupported`; open |
| 0, `none` | version 0 `HelloOk` | `Unauthenticated`, then the connection is closed |
| 0, any other | `Unauthenticated` (section 4.1); open | `Unauthenticated`, then the connection is closed |

After `Unsupported` or `UnsupportedVersion` the connection stays before
`Hello`, and the client may send another `Hello`.

`HelloOk`, version 1:

| Field | Type | |
|---|---|---|
| `version` | `u16` | the version used on this connection |
| `cluster_id` | `string` | |
| `node_id` | `u32` | |
| `max_frame_bytes` | `u32` | the frame limit once authenticated |
| `max_message_bytes` | `u32` | |
| `auth_payload` | `bytes` | `none`: empty. SCRAM: the server-first-message |
| `auth_complete` | `u8` | 1: authenticated; 0: the client sends `AuthContinue`. Other values are malformed |

These seven fields are fixed for every version from 1 on: a later
version's `HelloOk` starts with them, in this layout, and appends its own
fields after them. The first five are version 0's `HelloOk`, so a client
that offered several versions reads `version` and decodes the rest
accordingly. `HelloOk` reveals the version, cluster id, node id and
limits before authentication; none is a secret.

Overhead over version 0: 5 bytes and the payload (estimate: about 90 bytes
for SCRAM's server-first-message).

### 10.6 Authentication sequence

```text
client                                    broker
  TCP, TLS handshake
  Hello{min 1, max 1, name,           ─►
        "SCRAM-SHA-256", client-first}
                                      ◄─  HelloOk{…, server-first, auth_complete 0}
  AuthContinue{client-final}          ─►
                                      ◄─  AuthContinueOk{server-final, auth_complete 1}
  requests
```

Two round trips, SCRAM's minimum. `none` completes in `HelloOk`.

### 10.7 SCRAM-SHA-256

RFC 5802 with SHA-256 (RFC 7677). Messages are ASCII. The subset the
broker accepts (RFC 5802 §7 grammar):

```text
client-first-message  = gs2-header client-first-bare
gs2-header            = ("n" / "y") ",,"
client-first-bare     = "n=" name ",r=" c-nonce ["," extensions]
server-first-message  = "r=" c-nonce s-nonce ",s=" salt ",i=" iterations
client-final-message  = "c=" cbind ",r=" c-nonce s-nonce ["," extensions] ",p=" proof
server-final-message  = "v=" server-signature
```

| Part | Rule |
|---|---|
| `gs2-header` | `n,,` (no channel binding) or `y,,` (the client supports it and believes the server does not). `p=…` and an authorization identity (`a=…`) are failures |
| `name` | a principal name: 1–64 bytes of `A–Z a–z 0–9 . _ @ -`. These never need SASLprep or saslname escapes; any other byte is a failure |
| `c-nonce` | 16–255 bytes, each `0x21`–`0x7e` except `,` |
| `s-nonce` | the broker's: 24 random bytes in base64, 32 characters |
| `salt` | base64 of the credential's 16-byte salt |
| `iterations` | decimal, the credential's PBKDF2 iteration count |
| `cbind` | base64 of the `gs2-header` as sent: `biws` for `n,,`, `eSws` for `y,,` |
| `proof`, `server-signature` | base64 of 32 bytes |
| `extensions` | attributes other than these; ignored, but part of `AuthMessage`. An `m=` attribute before `n=` is a failure (RFC 5802 §5.1) |

Base64 is RFC 4648 §4, with padding. A first attribute out of this order,
a missing attribute, or a nonce in the client-final-message that is not
exactly `c-nonce s-nonce` is a failure.

Computation (RFC 5802 §3), `H` = SHA-256, `HMAC` = HMAC-SHA-256:

```text
SaltedPassword  = PBKDF2-HMAC-SHA-256(password, salt, iterations, 32 bytes)
ClientKey       = HMAC(SaltedPassword, "Client Key")
StoredKey       = H(ClientKey)
ServerKey       = HMAC(SaltedPassword, "Server Key")
AuthMessage     = client-first-bare "," server-first-message "," client-final-message-without-proof
ClientSignature = HMAC(StoredKey, AuthMessage)
ClientProof     = ClientKey XOR ClientSignature
ServerSignature = HMAC(ServerKey, AuthMessage)
```

- The broker holds `salt`, `iterations`, `StoredKey` and `ServerKey`
  only; it never receives a password (10.10). It accepts the proof when
  `H(ClientProof XOR ClientSignature)` equals `StoredKey`, compared in
  constant time.
- Passwords are their UTF-8 bytes, without SASLprep: an ASCII password
  gives RFC 5802's result, a non-ASCII password may not match another
  SCRAM implementation.
- **Iterations.** Every credential's count equals the broker's
  `auth_scram_iterations` when it is written (default 8 192, 4 096 to
  1 000 000; 4 096 is RFC 7677's minimum). A credential written before a
  change of the setting keeps its count until it is replaced. Clients
  learn the current count from `WhoAmIOk`.
- **Unknown principal.** The broker answers as for a known one: the salt
  is HMAC-SHA-256(`auth_secret`, name) truncated to 16 bytes, the count
  is `auth_scram_iterations`, and the proof is checked against a dummy
  key, so the failure comes at the same step with the same work. After a
  change of `auth_scram_iterations`, a credential that still has the old
  count is distinguishable from an unknown name.
- **Expiry.** A credential past its `expires_ms` (broker clock at login)
  fails at the proof check, as a wrong password does. An expiry does not
  close connections already authenticated.
- **Channel binding**: none in version 1 (`n,,`; `y,,` accepted).
  SCRAM-SHA-256-PLUS with `tls-exporter` (RFC 9266) is a later addition;
  once the broker offers it, `y,,` becomes a failure.
- **Client checks.** The client aborts and closes the connection unless
  the server nonce starts with its `c-nonce` and is longer, `iterations`
  is at least 4 096, and `ServerSignature` equals the one it computes.

### 10.8 AuthContinue

`AuthContinue`: `auth_payload bytes`, the next mechanism message (SCRAM:
the client-final-message).

`AuthContinueOk`:

| Field | Type | |
|---|---|---|
| `auth_payload` | `bytes` | SCRAM: the server-final-message |
| `auth_complete` | `u8` | SCRAM: 1. Other values than 0 and 1 are malformed |

- Served only while authenticating (10.4). SCRAM takes exactly one.
- Before `Hello` it gets `Unauthenticated` like any request. Once
  authenticated, or after a `none` login, it is an authentication
  failure (10.4).
- Requests a client pipelines behind `AuthContinue` are read while the
  connection is still authenticating and get `Unauthenticated`; a client
  waits for `AuthContinueOk` before it sends any but `Ping`.

### 10.9 WhoAmI

Empty request body. Any authenticated connection. `WhoAmIOk`:

| Field | Type | |
|---|---|---|
| `principal` | `string` | the authenticated principal; empty after `none` |
| `scram_iterations` | `u32` | the broker's `auth_scram_iterations` |

### 10.10 Principals

A principal is a name with one credential. Principals and ACL entries
are records of the broker's WAL (ADR-0015 §5): every change below is
durable before its answer, and its record names the principal that made
it. These requests need cluster `admin` (10.12), except `PutPrincipal` on
oneself. With `auth = none`, `PutPrincipal`, `DeletePrincipal`, `AddAcl`
and `RemoveAcl` get `Unsupported`.

**PutPrincipal** creates a principal or replaces its credential. The
client derives the credential (10.7) with a random 16-byte salt; the
password never leaves it.

| Field | Type | |
|---|---|---|
| `name` | `string` | a principal name (10.7) |
| `mechanism` | `u8` | 1: SCRAM-SHA-256 |
| `iterations` | `u32` | must equal `auth_scram_iterations` (`WhoAmIOk`) |
| `salt` | `bytes` | exactly 16 bytes |
| `stored_key` | `bytes` | exactly 32 bytes |
| `server_key` | `bytes` | exactly 32 bytes |
| `expires_ms` | `u64` | ms since the Unix epoch after which logins fail; 0: never |

`PutPrincipalOk`: `created u8`, 1 for a new principal, 0 for a replaced
credential.

- `MalformedRequest`: an invalid name, a `mechanism` other than 1, an
  `iterations` other than the setting, a salt or key of another length.
- `LimitExceeded`: a new name while `max_principals` exist.
- `Forbidden`: without cluster `admin` on another principal's name; on
  one's own name, a `mechanism` or `expires_ms` that differs from the
  stored one (a principal cannot lift an expiry an admin set).
- Replacing a credential stops reading from every open connection of that
  principal, answers the requests already read, and closes them; the
  connection that sent the `PutPrincipal` is one of them when a principal
  replaces its own credential.

**DeletePrincipal**: `name string` → `DeletePrincipalOk`, empty. The
principal's ACL entries go with it, and its open connections are closed
as above. An unknown name gets `UnknownPrincipal`.

**ListPrincipals**: principals by name, bytewise order, one page per
request, paged as `ListTopics` (section 4.9).

| Field | Type | |
|---|---|---|
| `cursor` | `string` | empty: the first page; otherwise the previous `next_cursor` |
| `limit` | `u32` | 0 is `MalformedRequest`; more than 1000 is taken as 1000 |

`ListPrincipalsOk`: `principals array<PrincipalSummary>`, `next_cursor
string` (empty after the last page).

`PrincipalSummary` (item):

| Field | Type | |
|---|---|---|
| `name` | `string` | |
| `mechanism` | `u8` | 1: SCRAM-SHA-256 |
| `iterations` | `u32` | |
| `expires_ms` | `u64` | |
| `updated_ms` | `u64` | timestamp of the record that set this credential |

No salt or key is ever returned. Overhead over a fixed layout: 1 byte per
principal (an item is at most 87 bytes).

### 10.11 ACL entries

`AclEntry`:

| Field | Type | |
|---|---|---|
| `principal` | `string` | a principal name |
| `effect` | `u8` | 0 allow, 1 deny |
| `resource_type` | `u8` | 1 topic, 2 topic prefix, 3 cluster |
| `resource_name` | `string` | topic: a topic name. Prefix: 0–80 bytes of topic-name characters; empty matches every topic. Cluster: empty |
| `operations` | `u16` | bits: 0 produce, 1 fetch, 2 create, 3 describe, 4 admin. Topic and prefix: bits 0–3; cluster: bits 3 and 4; at least one bit |

Anything else is `MalformedRequest`. An entry is identified by all five
fields.

**AddAcl**: the `AclEntry` fields as the body → `AddAclOk`, empty.
Adding an entry that exists changes nothing and succeeds.
`UnknownPrincipal` if the principal does not exist; `LimitExceeded` at
`max_acl_entries`, or for a prefix entry at the principal's
`max_acl_prefixes_per_principal`.

**RemoveAcl**: the `AclEntry` fields → `RemoveAclOk`, empty. Removing an
entry that does not exist changes nothing and succeeds. A client can
retry either after a lost connection.

**ListAcls**:

| Field | Type | |
|---|---|---|
| `principal` | `string` | empty: every principal's entries; a name that does not exist gives an empty last page |
| `cursor` | `string` | as in `ListPrincipals` |
| `limit` | `u32` | as in `ListPrincipals` |

`ListAclsOk`: `entries array<AclEntry>` (items), `next_cursor string`.
Order: by principal, then `resource_type`, `resource_name`, `effect` and
`operations`. The cursor is opaque, as in section 4.9. Overhead: 1 byte
per entry up to 127 bytes, 2 above (an entry is at most 152 bytes).

An ACL change applies from the next request on every open connection. A
Fetch admitted before the change keeps waiting and answers with what it
finds.

### 10.12 Authorization

With `auth = scram`, a request is allowed only if an allow entry of the
principal matches it and no deny entry does. With `auth = none` (loopback
only, ADR-0015 §1) every request is allowed, except the four writes of
10.10.

| Request | Resource | Operation |
|---|---|---|
| `Ping`, `Hello`, `AuthContinue` | — | none |
| `WhoAmI`; `Metadata` with no topics | — | any authenticated principal |
| `Metadata`, per topic | topic | `describe`; a refused topic gets `Forbidden` in its `TopicMetadata.error`, without partitions |
| `CreateTopic` | topic | `create` |
| `Produce` | topic | `produce` |
| `Fetch` | topic | `fetch` |
| `DescribeTopic` | topic | `describe` |
| `ListTopics` | each topic | `describe`; topics not allowed are left out (10.13) |
| `BrokerStats` | cluster | `describe` |
| `PutPrincipal` on another name, `DeletePrincipal`, `ListPrincipals`, `AddAcl`, `RemoveAcl`, `ListAcls` | cluster | `admin` |
| `PutPrincipal` on one's own name | — | none (10.10) |
| `InitProducer` | — | an allow entry with `produce` on some topic or prefix (10.14) |
| `DeleteTopic` | — | `Unsupported`, as in version 0 |

- `produce` or `fetch` on a topic implies `describe` of it.
- A topic entry matches its exact name; a prefix entry matches every
  topic name that starts with it. A matching deny wins over any allow; no
  match is a deny.
- The check runs after the topic name is validated and before the topic
  is looked up: a principal without the operation gets `Forbidden`
  whether the topic exists or not. Names starting with `__` stay
  `Forbidden` for everyone.
- Cluster `admin` includes adding entries for oneself: it is full access.

### 10.13 Changed responses of version 0 requests

**MetadataOk.** Layout of section 4.3; `Node`, `TopicMetadata` and
`PartitionMetadata` are items. `TopicMetadata.error` may also be
`Forbidden` (10.12). Overhead: one `len` per node, topic and partition,
1 byte for an item up to 127 bytes and 2 above. A `PartitionMetadata` is
36 + 8 × replicas bytes (1 byte up to 11 replicas); a `TopicMetadata`
includes its partitions, so a topic with one partition and a long name
takes 2.

**ListTopicsOk.** Layout of section 4.9; `TopicSummary` is an item (1
byte; at most 86 bytes). With ACLs, the broker examines at most
`list_topics_scan_factor` × `limit` names per request (default 10) and
leaves out the topics the principal may not describe: a page may hold
fewer than `limit` topics, none included, with a non-empty `next_cursor`.
An empty `next_cursor` still means no topic follows.

**DescribeTopicOk.** Layout of section 4.10; `PartitionDescription` is an
item (1 byte; 36 bytes).

**BrokerStatsOk.** The fields of section 4.11, then:

| Field | Type | |
|---|---|---|
| `auth_successes` | `u64` | logins completed since start |
| `auth_failures` | `u64` | authentication failures (10.4) since start |
| `forbidden` | `u64` | requests and Metadata topics refused with `Forbidden` by an ACL since start |
| `unauthenticated_connections` | `u32` | connections open now that have not completed authentication |
| `quarantined_files` | `u64` | files in the store's checkpoint quarantine directories now, until an operator removes them (D-258 C, ADR-0014 §3) |

No counter is per principal or per address. Overhead: 36 bytes per
response.

### 10.14 Idempotent produce

A producer that sets `producer_id` has each Produce request stored at
most once per partition, retries included, while the broker holds its
state.

**InitProducer** obtains a producer id and epoch, or fences an earlier
holder of an id.

| Field | Type | |
|---|---|---|
| `producer_id` | `u64` | 0: issue a new id. Otherwise an id this principal holds |
| `producer_epoch` | `u32` | ignored for id 0; otherwise the epoch the client holds |

`InitProducerOk`:

| Field | Type | |
|---|---|---|
| `producer_id` | `u64` | never 0, never issued twice |
| `producer_epoch` | `u32` | |

- `producer_id` 0: a new id with epoch 0, bound to the requesting
  principal (with `auth = none`, to no principal).
- An id this principal holds, with its current epoch: the epoch becomes
  epoch + 1 and the id's sequence state on every partition is dropped;
  requests with the old epoch get `ProducerFenced` from then on. At epoch
  2^32 − 1 the broker issues a new id with epoch 0 instead, and the old id
  is fenced.
- An epoch below the current one: `ProducerFenced`. An epoch above it, an
  id never issued, an id the broker no longer holds (below), or an id of
  another principal: `UnknownProducer`.
- The answer is sent after the new state is durable.

**Produce**, version 1: the fields of section 4.7, then:

| Field | Type | |
|---|---|---|
| `producer_epoch` | `u32` | the epoch from `InitProducerOk`; checked only when `producer_id` ≠ 0 |

With `producer_id` = 0 the request is not idempotent and
`producer_seq` and `producer_epoch` are not checked, as in version 0.
With `producer_id` ≠ 0, `producer_seq` numbers the producer's Produce
requests to one partition in one epoch: 0 for the first, then one more
per request (not per message). The broker checks a request after the
checks of section 4.7 and before the write:

| Condition | Answer |
|---|---|
| id not held by the broker, or of another principal; epoch above the current | `UnknownProducer` |
| epoch below the current | `ProducerFenced` |
| no request accepted on this partition in this epoch, and `producer_seq` ≠ 0 | `UnknownProducer` |
| `producer_seq` = last accepted + 1 (or 0 for the first) | written; `ProduceOk` |
| `producer_seq` among the last `producer_window` accepted | not written; the `ProduceOk` the original got (same `base_seq`, `count`, `leader_epoch`, `timestamp_ms`) |
| `producer_seq` accepted earlier, but older than the window | `DuplicateSequence`: committed, its `ProduceOk` no longer held |
| `producer_seq` above last accepted + 1 | `OutOfOrderSequence`; not written |

- "Accepted" includes requests admitted into a group that has not
  committed yet, from any connection. A retry that arrives while its
  original is still waiting for the commit gets the original's answer
  when it comes, `ProduceOk` or error.
- A request answered with an error changes no producer state and does
  not use its `producer_seq`: the next request reuses that number.
- The broker does not compare the messages of a retry with the
  original's: the client must resend the same messages with the same
  `producer_seq`.

**State and its bounds.** For each producer id the broker holds its
principal, its epoch, and per partition written to in this epoch the last
`producer_window` accepted `producer_seq` values with their `ProduceOk`.
The state is recorded in the WAL with the messages it describes, in the
same commit, so it survives a restart and replay rebuilds it. Broker
settings, each with a hard range in code (`broker-rules.md` §3):

| Setting | Default | Range | Bounds |
|---|---|---|---|
| `producer_window` | 5 | 1–16 | accepted requests remembered per producer and partition |
| `max_producers` | 10 000 | 100–1 000 000 | producer ids held |
| `max_producer_states` | 100 000 | 1 000–1 000 000 | (producer, partition) states held, all producers together |

When an `InitProducer` or an accepted Produce would exceed a bound, the
broker drops the whole state of the producer id used least recently (by
WAL order, so replay drops the same ones); that id then gets
`UnknownProducer`. Memory, estimated from the layouts: about 64 bytes per
id and 60 + 28 × `producer_window` bytes per state, about 20 MB at the
defaults and about 0.6 GB at the upper ends.

Overhead: 4 bytes per Produce request; nothing per message.

### 10.15 Fetch by timestamp range

**Fetch**, version 1: the fields of section 4.8, then:

| Field | Type | |
|---|---|---|
| `from_timestamp_ms` | `u64` | 0: no lower bound |
| `to_timestamp_ms` | `u64` | exclusive; 0: no upper bound. Non-zero and ≤ `from_timestamp_ms` is `MalformedRequest` |

The response holds the messages that version 0 would return (section
4.8) whose `timestamp_ms` is ≥ `from_timestamp_ms` and <
`to_timestamp_ms`. Timestamps never decrease within a partition (section
4.7), so the messages of a range have consecutive seqs: the broker starts
at the first seq ≥ max(`from_seq`, `start_seq`) whose timestamp is ≥
`from_timestamp_ms`, found by a binary search over the partition's
index, and stops before the first message at or after `to_timestamp_ms`.
To read a range, a client sends `from_seq` 0 with both timestamps, then
continues from the last seq + 1 with the same timestamps.

`FetchOk`, version 1:

| Field | Type | |
|---|---|---|
| `leader_epoch` | `u32` | |
| `start_seq` | `u64` | |
| `next_seq` | `u64` | |
| `messages` | `array<FetchedMessage>` | `FetchedMessage` is an item |
| `range_complete` | `u8` | 1: a message with a timestamp ≥ `to_timestamp_ms` exists at or after the position, so no later message is in the range; 0 otherwise, and always with `to_timestamp_ms` = 0 |

- The broker waits up to `max_wait_ms` as in version 0 when the response
  would hold no message and `range_complete` would be 0; with
  `range_complete` = 1 it answers at once.
- `range_complete` is 1 only once a message past the range exists:
  timestamps are the broker's clock at commit, raised to stay
  non-decreasing, so without such a message a later commit may still fall
  in the range.
- Timestamps are the broker's (section 4.7), not the producer's.

Overhead of `FetchedMessage` over version 0 (estimate, 10.2): one `len` per message, 1 byte
up to 127 bytes of item (a body up to 106 bytes without key or headers),
2 bytes up to 16 383, 3 bytes up to 2 097 151, which covers the default
1 MiB `max_message_bytes`. The 21 bytes of an empty message (section 4.8)
become 22. A 100-byte body: 121 → 122 bytes (+0.8 %); 1 KiB: 1 045 →
1 047 (+0.2 %). Per request: 16 bytes; per response: 1 byte.

### 10.16 Errors

`Error` has the layout of section 5, follows the rules of 10.2 and may
gain fields; version 1 appends none, so its bytes equal version 0's. An
error answering `Hello` never carries appended fields (10.1).

Codes added in version 1, none retryable:

| Code | Name | Sent when |
|---|---|---|
| 16 | `UnknownPrincipal` | `DeletePrincipal` or `AddAcl` names a principal that does not exist |
| 17 | `LimitExceeded` | a write of 10.10 or 10.11 would exceed `max_principals`, `max_acl_entries` or `max_acl_prefixes_per_principal` |
| 18 | `OutOfOrderSequence` | a `producer_seq` above the last accepted + 1 (10.14) |
| 19 | `DuplicateSequence` | a `producer_seq` accepted earlier and older than the window |
| 20 | `ProducerFenced` | an epoch below the producer id's current epoch |
| 21 | `UnknownProducer` | an id the broker does not hold for this principal, an epoch it never issued, or a partition state it does not hold |

Codes with more uses in version 1:

| Code | Name | Also sent when |
|---|---|---|
| 5 | `FrameTooLarge` | a frame over `max_preauth_frame_bytes` before authentication completes |
| 9 | `Unsupported` | a mechanism the broker does not offer (version 0 answers `Unauthenticated`); a principal or ACL write with `auth = none` |
| 10 | `Unauthenticated` | a request while authenticating; every authentication failure (10.4) |
| 11 | `Forbidden` | a request the principal's ACLs do not allow (10.12); a principal's own `PutPrincipal` that changes `mechanism` or `expires_ms` |
| 12 | `MalformedRequest` | a decoding error of 10.2; a field outside its definition in this section |

### 10.17 Normative client behavior, version 1

Section 6 applies, with:

1. **Credentials.** With credentials configured, the client offers
   `min_version` 1 and never falls back to `none`. A wrong
   `ServerSignature` fails the connection.
2. **Not retried:** `Unauthenticated`, `Forbidden`, `Unsupported`,
   `UnknownPrincipal`, `LimitExceeded`. A Produce refused with
   `Forbidden` is not resent.
3. **Reconnect.** A client that reconnects authenticates again; if that
   fails, the request fails with `Unauthenticated` and is not resent.
4. **Idempotent produce.** The client calls `InitProducer` once per
   producer and keeps one `producer_seq` per partition, from 0. A retry
   (section 6, rules 1–3) resends the same messages with the same
   `producer_id`, `producer_epoch` and `producer_seq`. It keeps at most
   `producer_window` requests in flight per partition; the default 5 is
   the most it can assume. On errors:
   - `DuplicateSequence`: the request was stored; its seqs are unknown.
   - `OutOfOrderSequence`: not stored. It follows a request that failed;
     the client resends it with the next number not used, after that
     failure is handled.
   - `ProducerFenced`: another holder of the id took over; the producer
     stops and returns the error.
   - `UnknownProducer`: the client calls `InitProducer` with id 0 and
     starts each partition at 0. Requests whose outcome was unknown at
     that point may be stored twice; the client reports them so.

### 10.18 Delivery guarantees, version 1

Section 8 holds, with: a Produce with `producer_id` ≠ 0 whose response
was lost is stored once when it is retried with the same `producer_id`,
`producer_epoch` and `producer_seq`, across broker restarts, while the
broker holds the producer's state (10.14). Without `producer_id`, or
after `UnknownProducer`, a retry may store its messages twice.
