# Configuration

> **`STRAUM_DURABILITY` and the target.** The levels below are the current
> single-node and leader/follower modes. In the target, a PUBACK and a
> delivery follow commit by a majority (docs/architecture/unified-stream.md
> §2.3, ADR-0005); the levels remain for a single node and the current
> leader/follower, and a cluster allows only commit by majority (ADR-0006).

All configuration comes from `STRAUM_*` environment variables. No config file,
no serde.

## Rules

* Parsed only in `server` into typed structs (`TransportConfig`,
  `MqttLimits`, `BrokerLimits`, `RuntimeConfig`). Lower crates never read the
  environment; they receive typed values.
* An unknown `STRAUM_*` variable is a startup error. Typos must not silently
  fall back to defaults.
* A malformed or out-of-range value is a startup error, never a fallback.
* Units are in the name: `_BYTES`, `_MS`, `_SECS`. Booleans are `true`/`false`.
* `auto` is resolved at startup, on Linux from kernel parameters
  (see `linux-tuning.md`), on macOS from the fallback column.
* The effective configuration is logged once at `info`, each value with its
  source: `env`, `default` or `kernel`.
* Changing a default is a behavior change: update this file, tests, and the
  change report.

Defaults below are **initial**. They are adjusted by benchmarks, not intuition.

## Process

| Variable | Default | Notes |
|---|---|---|
| `STRAUM_LISTEN_ADDR` | `0.0.0.0:1883` | |
| `STRAUM_LISTENERS` | `1` | `>1` binds several sockets with `SO_REUSEPORT`; the kernel balances accepts. Linux only; on macOS forced to 1 with a warning (BSD `SO_REUSEPORT` does not balance). |
| `STRAUM_WORKER_THREADS` | `auto` | Tokio worker threads; `auto` = number of CPUs. |
| `STRAUM_LOG_LEVEL` | `info` | `error`, `warn`, `info`, `debug`, `trace`. |
| `STRAUM_SHUTDOWN_GRACE_MS` | `5000` | Writer drain time during shutdown. |

## Transport (owner: transport)

| Variable | Default | Linux `auto` | macOS fallback | Notes |
|---|---|---|---|---|
| `STRAUM_BACKLOG` | `auto` | `net.core.somaxconn` | `1024` (kernel clamps to `kern.ipc.somaxconn`) | Warns if an explicit value exceeds `somaxconn`. |
| `STRAUM_MAX_CONNECTIONS` | `auto` | `RLIMIT_NOFILE` soft − 64 | `1024` | 64 descriptors reserved for listeners, logs, runtime. |
| `STRAUM_TCP_NODELAY` | `true` | | | MQTT packets are small; Nagle adds latency. |
| `STRAUM_TLS_CERT_PATH` | empty | | | PEM certificate chain. Empty means the listener speaks plaintext. Setting one of the two TLS paths without the other is a startup error. |
| `STRAUM_TLS_KEY_PATH` | empty | | | PEM private key for the chain above. |
| `STRAUM_AUTH_PASSWORD_FILE` | empty | | | `user:<argon2 hash>` per line. Empty means no user has a password, which with anonymous access allowed is an open broker. |
| `STRAUM_AUTH_ACL_FILE` | empty | | | Per-user topic rules (see below). Empty means every authenticated user may do anything. |
| `STRAUM_AUTH_ALLOW_ANONYMOUS` | `true` | | | Whether a client without a user name may connect. The default is `true` so an unconfigured broker behaves as it always did; it warns loudly at startup. |
| `STRAUM_SO_RCVBUF_BYTES` | `0` | | | `0` = do not set. Setting it disables Linux receive autotuning for the socket; the kernel doubles the value and clamps to `net.core.rmem_max`. |
| `STRAUM_SO_SNDBUF_BYTES` | `0` | | | Same as above, `net.core.wmem_max`. |
| `STRAUM_TCP_KEEPALIVE_SECS` | `300` | | | Idle time before TCP keepalive probes. Detects dead peers for clients with MQTT keep-alive 0. `0` disables. |
| `STRAUM_TCP_USER_TIMEOUT_MS` | `0` | | ignored, warning | Linux only. Max time unacknowledged data may stay in flight before the kernel drops the connection. `0` = kernel default. |
| `STRAUM_READ_BUFFER_BYTES` | `16384` | | | Steady-state read buffer per connection. It grows for a larger packet and is returned to this size once that packet is out of the way. Sizes the read buffer and nothing else; until 2026-09-27 it silently sized the write buffer too (docs/memory.md). |
| `STRAUM_WRITE_BUFFER_BYTES` | `16384` | | | Steady-state buffer outbound packets are encoded into, per connection. It grows for a large batch and is returned to this size once the batch is written. Independent of the read buffer: they are different resources. |
| `STRAUM_TOTAL_CONNECTION_BUFFER_BYTES` | `1073741824` | | | Global budget for connection transport buffers, read and write together. A connection that would push the total over it is refused the memory before either buffer is allocated; per-connection limits times the connection count are a product, not a bound. It bounds these two buffers and nothing else — packet-id tables, session state, task futures and channel metadata are separate and are not claimed by it (docs/memory.md). Renamed from `STRAUM_TOTAL_READ_BYTES`, which is now an unknown variable and therefore a startup error. |
| `STRAUM_WRITE_STALL_TIMEOUT_MS` | `30000` | | | Close if the writer makes no progress this long. The same limit applies to each frame a leader sends its follower: a follower that stops reading has its link ended, counted in `replication_errors` and redialled (D-27). |

## MQTT (owner: mqtt)

| Variable | Default | Notes |
|---|---|---|
| `STRAUM_MAX_PACKET_BYTES` | `1048576` | Checked against Remaining Length before any allocation. Protocol maximum is 268,435,455 + header. |
| `STRAUM_MAX_TOPIC_BYTES` | `1024` | Protocol maximum is 65,535. |
| `STRAUM_MAX_TOPIC_LEVELS` | `32` | Also bounds routing depth. |
| `STRAUM_MAX_CLIENT_ID_BYTES` | `256` | 3.1.1 requires accepting 1–23 bytes; more is allowed. |
| `STRAUM_MAX_FILTERS_PER_SUBSCRIBE` | `64` | Per SUBSCRIBE/UNSUBSCRIBE packet. |
| `STRAUM_CONNECT_TIMEOUT_MS` | `10000` | Time from accept to a valid CONNECT. |
| `STRAUM_PAYLOAD_COPY_THRESHOLD_BYTES` | `512` | Payloads up to this size are copied at ingress instead of sliced (CLAUDE.md §9). |

### New path: `codec` (ADR-0008 step 3a; not yet configurable)

The `codec` crate takes the limits above in its own `codec::Limits`, with the
same defaults, plus one of its own. None is read from the environment until
the server runs the new path (step 7, D-68 A); then each gets a variable here.

| Limit | Default | Notes |
|---|---|---|
| `max_user_properties` | `32` | User Properties in one MQTT 5 property section; more is refused before the next is kept (Implementation specific error, 0x83). |

### New path: `apply` and `gateway` (ADR-0008 step 3b-2; not yet configurable)

Bounds the apply function decides from the log. Every node must run with the
same values: they are part of what the log means. None is read from the
environment until the server runs the new path (step 7, D-68 A).

| Limit | Default | Notes |
|---|---|---|
| `max_subscriptions_per_session` | `1024` | Beyond: SUBACK failure (Quota exceeded in MQTT 5). |
| `max_subscriptions` | `1000000` | All sessions together. |
| `max_retained` | `10000` | Beyond: delivered, not kept (as the old path). |
| `max_retained_bytes` | `67108864` | Retained payload bytes. |
| `max_inflight` | `256` | At-least-once deliveries unacknowledged per session (as `STRAUM_MAX_INFLIGHT`). |
| `max_backlog` | `16384` | Waiting behind a full window, per session (as `STRAUM_SESSION_BACKLOG_MESSAGES`). |
| `max_backlog_bytes` | `8388608` | Per session. Beyond: at-most-once dropped, at-least-once closes the session as a slow consumer. |
| `max_total_queued_bytes` | `1073741824` | Inflight and backlog payload bytes of all sessions together (the budget of `STRAUM_TOTAL_BACKLOG_BYTES`, now counting inflight too). At-least-once beyond: backlog, then a slow-consumer close. |

Gateway settings, per node:

| Setting | Notes |
|---|---|
| `first_session` | Kept durable and growing across restarts by the driver: sessions are never renumbered. |
| `max_connections` | Beyond: the socket is closed at accept. |
| `connect_timeout` | As `STRAUM_CONNECT_TIMEOUT_MS`. |
| `max_pending`, `max_pending_bytes` | Proposals of one connection awaiting commit; at the bound the connection is not read. |
| `max_total_pending_bytes` | All connections. |
| `commit_timeout` | A proposal not committed within it closes its connection. |
| `retry_interval` | First interval for `Ack`, `SessionGone`, `NodeReset`; doubles to 32×. |
| `receive_maximum`, `topic_alias_maximum` | Advertised to MQTT 5 clients. |
| `max_waiting`, `max_waiting_bytes`, `max_total_waiting_bytes` | At-most-once deliveries held behind a QoS 1 delivery waiting for the client's receive maximum; beyond, dropped and counted. |
| `max_retries` | Lifecycle proposals awaiting their effect; at the bound new connections are refused. |

### New path: `node` (ADR-0008 step 4; test-only, not yet configurable)

`node::NodeConfig`, per node. None is read from the environment until step 7
lets the server start in cluster mode.

| Setting | Notes |
|---|---|
| `core` | `cluster::Config`: id, voters, the first slot's owner, replication window, queue, batch, `max_uncommitted`, `takeover_ticks` (heartbeats of silence before the next node in the schedule takes the slot over; step 5). |
| `data` (step 5) | Also holds `promise`: the highest slot promised, synced before any answer. |
| `listen`, `peers` | Where other nodes connect to this one; where to reach each of them. One TCP connection per direction. |
| `data` | The segmented log, the `snapshot` file (the applied state at one lsn, D-67 B) and the `incarnation` file (the gateway's `first_session`, made durable before use). |
| `retain_bytes` | D-57 A: the log keeps no more than this behind a replica that is only slow; one further behind is lost until state copy (step 7b). |
| `snapshot_every` | Applied records between snapshots; segments wholly below the last durable snapshot and the retention floor are removed after it is written. |
| `segments` | `storage::SegmentLimits`. |
| `limits` | `apply::Limits`, the table above. A link whose HELLO carries another digest of them is refused. |
| `heartbeat` | Sequencer tick and gateway tick. |
| `max_frame` | Largest link frame read; beyond, the link closes. |
| `link_queue` | Frames queued per peer; beyond, dropped (the protocol resends). |
| `mqtt.listen`, `mqtt.gateway` | MQTT listener and the gateway settings above. |
| `mqtt.write_queue` | Packets queued per client; a client that lets it fill is closed. |

## WAL (owner: storage)

| Variable | Default | Notes |
|---|---|---|
| `STRAUM_WAL_PATH` | `straum.wal` | Log file. Recovery cuts an incomplete or corrupt tail on open. |
| `STRAUM_WAL_QUEUE` | `65536` | Records queued from the broker loop to the WAL writer. Full means the submitter waits; nothing is acknowledged early to make room. |
| `STRAUM_WAL_COMPACT_BYTES` | `67108864` | Rewrite the log, dropping records nothing needs any more (docs/storage.md §104): at startup once the file is at least this large, and beside the writer once this many bytes were appended since the last rewrite. Appended rather than file size, so a log that has to stay large is not rewritten after every commit (D-18). `0` never rewrites it. The rewrite runs on the writer thread and commits wait for it. |
| `STRAUM_WAL_RETAIN_BYTES` | `67108864` **(initial)** | With a follower configured and durability below `quorum`: the last this many bytes of the log stay readable by sequence across a rewrite, so a follower that far behind is caught up rather than lost (docs/replication.md §118, D-18). One further behind is counted in `replication_followers_lost` and needs a fresh copy of the log. Ignored without a follower; under `quorum` the follower's own position decides instead (D-20). `0` keeps nothing. Each rewrite copies up to this much again, so write amplification is about (compact + retain + live set) / compact. |
| `STRAUM_WAL_MAX_RECORD_BYTES` | `1048576` | Largest payload one record may carry. Matches `STRAUM_MAX_PACKET_BYTES`. |
| `STRAUM_WAL_MIN_BATCH_BYTES` | `65536` | Floor for the adaptive batch bound. |
| `STRAUM_WAL_MAX_BATCH_BYTES` | `4194304` | Upper safety boundary for one batch — not a size to wait for (docs/storage.md §77). |
| `STRAUM_WAL_INITIAL_BATCH_BYTES` | `1048576` | Where adaptation starts, before any storage latency is known: the §78 estimate of 1 GB/s × 1 ms. |
| `STRAUM_DURABILITY` | `memory` | `memory`, `written`, `fsynced` or `quorum` (docs/storage.md §80). `quorum` is `fsynced` plus waiting for `STRAUM_REQUIRED_COPIES` nodes (docs/replication.md §126); the numbers below are for the other three. `memory` never writes to the log. `written` writes but does not sync: the acknowledgement waits for `write(2)`, so a process crash keeps the message and a machine or storage failure may not. `fsynced` waits for the storage device. Measured here, 1 publisher and 1 subscriber unthrottled at QoS 1: 183,912 / 180,997 / 11,313 msg/s — writing costs almost nothing, syncing costs 16x. |
| `STRAUM_MAX_AWAITING_QUORUM` | `65536` | Commits durable on this node and waiting for a quorum before their publishes are acknowledged. Only used with `quorum`. Reaching it stops the broker reading the log's reports, so the log writer waits, the log queue fills and the publisher is held at its TCP window; the pairs are never dropped, because a lost one is a correlation no watermark can release (docs/replication.md §128). Counted in commits: it is not the bound on acknowledgements, see the next row. |
| `STRAUM_MAX_PENDING_DURABLE` | `65536` | **(initial)** PUBACKs held back until their publish is durable at the configured level, counted in publishes (24 bytes each, 1.5 MiB at the default). Used with `written`, `fsynced` and `quorum`. Enforced as permits taken before a publish that will be acknowledged goes to the log: at the limit that publisher waits at its own reader and is held at its TCP window, and no other client waits — subscriptions, QoS 0, acknowledgements and disconnects keep moving. Nothing is dropped, and no record past the limit reaches the log (D-7, docs/memory.md §9). |
| `STRAUM_REQUIRED_COPIES` | `2` | | | Durable copies a `quorum` publish waits for, **this node included**: 2 is a leader and one follower. Only read when `STRAUM_DURABILITY=quorum`; a value above the nodes configured is refused at startup, because a quorum that cannot be formed acknowledges nothing. |
| `STRAUM_METRICS_ADDR` | unset | | | Where to serve the counters, in the Prometheus text exposition format, on a port of their own. Unset: they stay in the process. Every path answers the same thing and there is no authentication, so it belongs on an interface the operator controls. One scrape is served at a time; scrapes are seconds apart and a task per connection would be a way for anyone who can reach the port to make tasks. |
| `STRAUM_REPLICATION_LISTEN` | unset | Address to accept a leader's replication stream on. Unset or empty: this node is not a follower. Plaintext, and a different port from the MQTT listener (docs/replication.md §119). |
| `STRAUM_REPLICATION_FOLLOWER` | unset | Address of the one follower to ship committed batches to. Unset or empty: this node is not a leader. One static follower only; nothing here elects anybody or discovers anybody. |

## Broker (owner: broker)

| Variable | Default | Notes |
|---|---|---|
| `STRAUM_MAX_SUBSCRIPTIONS_PER_SESSION` | `1024` | |
| `STRAUM_MAX_INFLIGHT` | `256` | Unacknowledged at-least-once deliveries per session. Must be < 65,535. At-least-once throughput per session is this window divided by the acknowledgement round trip, so it is a rate ceiling as much as a memory bound: at 32 a subscriber could not sustain 20,000/s and was closed as a slow consumer. |
| `STRAUM_SESSION_BACKLOG_MESSAGES` | `16384` | At-least-once deliveries waiting for inflight window, per session. This is the burst a session may fall behind by before it is closed; 1024 was under a second of it at moderate rates. |
| `STRAUM_SESSION_BACKLOG_BYTES` | `8388608` | Payload bytes those deliveries may hold. Without it the message count is not a memory bound at all — the messages can be a megabyte each (CLAUDE.md §7). Whichever ceiling is reached first refuses the message. |
| `STRAUM_TOTAL_BACKLOG_BYTES` | `1073741824` | Global budget across every session backlog, the counterpart of `STRAUM_TOTAL_QUEUED_BYTES` for the broker side. Per-session caps times the session count is a product, not a bound (CLAUDE.md §8). |
| `STRAUM_MAX_OFFLINE_SESSIONS` | `4096` | Sessions kept for clients that asked to be resumed and are not connected. `0` turns persistent sessions off: a session is then dropped whatever CleanSession said. |
| `STRAUM_MAX_RETAINED_MESSAGES` | `10000` | Topics that may hold a retained message. `0` disables retention. |
| `STRAUM_MAX_RETAINED_BYTES` | `67108864` | Payload bytes held across all retained messages. |

## Runtime (owner: runtime; policy in broker)

| Variable | Default | Notes |
|---|---|---|
| `STRAUM_SESSION_QUEUE_MESSAGES` | `1024` | Capacity of broker → writer channel per session. |
| `STRAUM_SESSION_QUEUE_BYTES` | `8388608` | Payload bytes queued per session. |
| `STRAUM_TOTAL_QUEUED_BYTES` | `1073741824` | Global budget across all sessions. Without it, per-session caps × max connections is not a real bound. |
| `STRAUM_BROKER_QUEUE` | `65536` | Commands reader → broker. |
| `STRAUM_BROKER_BATCH` | `256` | Commands handled per broker loop wakeup. |
| `STRAUM_WRITER_BATCH` | `64` | Outbound items encoded per socket write. |

## Access rules

The password file is one `user:hash` per line; `#` starts a comment. The
hash is an argon2 PHC string, so each line carries its own cost
parameters and they can be changed per user without a code change.

The ACL file names a user, then what that user may do:

```text
user alice
publish   sensors/+/temp
subscribe sensors/#

user bob
readwrite shared/bus

anonymous
subscribe public/#
```

A user that appears in the file may do only what its rules allow; a user
that does not appear may do anything. A rule for a user with no password
is a startup error, because it is a typo, not a permission.

A subscription is allowed when a rule covers everything the filter could
match: `subscribe sensors/#` does not grant `#`, because the wider filter
would deliver topics no rule mentions.

An unauthorised publish is dropped and still acknowledged: 3.1.1 has no
way to tell a publisher it was refused, and withholding PUBACK would only
stall it. The `unauthorized_publishes` counter is how an operator sees it.

## Validation ranges

Each variable has a minimum and maximum enforced at startup. Examples:

```text
STRAUM_MAX_PACKET_BYTES      2 ..= 268435460
STRAUM_MAX_INFLIGHT          1 ..= 65534
STRAUM_READ_BUFFER_BYTES     1024 ..= STRAUM_MAX_PACKET_BYTES
STRAUM_WRITE_BUFFER_BYTES    1024 ..= STRAUM_MAX_PACKET_BYTES
STRAUM_SESSION_QUEUE_BYTES   >= STRAUM_MAX_PACKET_BYTES
STRAUM_TOTAL_CONNECTION_BUFFER_BYTES >= STRAUM_MAX_PACKET_BYTES
STRAUM_WAL_MAX_RECORD_BYTES  >= STRAUM_MAX_PACKET_BYTES
STRAUM_WAL_MAX_BATCH_BYTES   >= STRAUM_WAL_MIN_BATCH_BYTES
STRAUM_WAL_INITIAL_BATCH_BYTES  within [min_batch, max_batch]
```

Cross-field constraints are validated too; the error names both variables.
