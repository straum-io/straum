# Straum protocol, version 0

The binary protocol between Straum clients and a Straum broker, over TCP.
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
| `topics` | `array<string>` | empty: every topic |

`MetadataOk`:

| Field | Type | |
|---|---|---|
| `cluster_id` | `string` | |
| `controller_node_id` | `u32` | reserved: v0 sends its own node id |
| `nodes` | `array<Node>` | every node of the cluster; v0: itself |
| `topics` | `array<TopicMetadata>` | in request order; all topics by name when the request was empty |

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
| `max_bytes` | `u32` | of keys, header names and values, and bodies; capped by the broker's `max_fetch_bytes` |
| `max_wait_ms` | `u32` | capped by the broker's `max_fetch_wait_ms` |
| `expected_leader_epoch` | `u32` | as in Produce |

The response holds the messages with seq ≥ max(`from_seq`, `start_seq`),
in seq order, while their total size stays within `max_bytes`; the first
such message is returned even if it alone is larger. If there is none, the
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

Empty request body. `ListTopicsOk`: `topics array<TopicSummary>` by name;
`TopicSummary`: `name string`, `partitions u32`.

### 4.10 DescribeTopic

`DescribeTopic { name string }` → `DescribeTopicOk`:

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

| Field | v0 |
|---|---|
| `Hello.auth_payload` | empty (`none` only) |
| `MetadataOk.controller_node_id` | the answering node |
| `Node.rack` | empty |
| `PartitionMetadata.in_sync_replicas` | equals `replicas` |
| `CreateTopic.replication_factor` | 1 |
| `Produce.producer_id`, `producer_seq` | not checked |
| `Produce.ack_level` `written`, `replicated` | `UnsupportedAckLevel` |
| `Produce.timeout_ms` | ignored |
| `expected_leader_epoch` | checked against the constant epoch 1 |
| `Error.leader_node_id`, `leader_epoch` | set for `FencedLeaderEpoch` |
| `NotLeader` | never sent |
| `Message.flags` bits 2–7 | 0 |
| `DeleteTopic` | `Unsupported` |
