# Configuration

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
| `STRAUM_READ_BUFFER_BYTES` | `16384` | | | Steady-state read buffer per connection. It grows for a larger packet and is returned to this size once that packet is out of the way. |
| `STRAUM_TOTAL_READ_BYTES` | `1073741824` | | | Global budget for read buffers, the ingress counterpart of `STRAUM_TOTAL_QUEUED_BYTES`. A connection that would push the total over it is refused the memory and closed; per-connection limits times the connection count are a product, not a bound. |
| `STRAUM_WRITE_STALL_TIMEOUT_MS` | `30000` | | | Close if the writer makes no progress this long. |

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

## WAL (owner: storage)

| Variable | Default | Notes |
|---|---|---|
| `STRAUM_WAL_PATH` | `straum.wal` | Log file. Recovery cuts an incomplete or corrupt tail on open. |
| `STRAUM_WAL_QUEUE` | `65536` | Records queued from the broker loop to the WAL writer. Full means the submitter waits; nothing is acknowledged early to make room. |
| `STRAUM_WAL_COMPACT_BYTES` | `67108864` | Rewrite the log once it is at least this large, dropping records nothing needs any more (docs/storage.md §104). `0` never rewrites it. Checked at startup and again after every commit, so a broker that never restarts compacts too; the rewrite runs on the writer thread and commits wait for it. |
| `STRAUM_WAL_MAX_RECORD_BYTES` | `1048576` | Largest payload one record may carry. Matches `STRAUM_MAX_PACKET_BYTES`. |
| `STRAUM_WAL_MIN_BATCH_BYTES` | `65536` | Floor for the adaptive batch bound. |
| `STRAUM_WAL_MAX_BATCH_BYTES` | `4194304` | Upper safety boundary for one batch — not a size to wait for (docs/storage.md §77). |
| `STRAUM_WAL_INITIAL_BATCH_BYTES` | `1048576` | Where adaptation starts, before any storage latency is known: the §78 estimate of 1 GB/s × 1 ms. |
| `STRAUM_DURABILITY` | `memory` | `memory`, `written` or `fsynced` (docs/storage.md §80). `memory` never writes to the log. `written` writes but does not sync: the acknowledgement waits for `write(2)`, so a process crash keeps the message and a machine or storage failure may not. `fsynced` waits for the storage device. Measured here, 1 publisher and 1 subscriber unthrottled at QoS 1: 183,912 / 180,997 / 11,313 msg/s — writing costs almost nothing, syncing costs 16x. |
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
STRAUM_SESSION_QUEUE_BYTES   >= STRAUM_MAX_PACKET_BYTES
STRAUM_TOTAL_READ_BYTES      >= STRAUM_MAX_PACKET_BYTES
STRAUM_WAL_MAX_RECORD_BYTES  >= STRAUM_MAX_PACKET_BYTES
STRAUM_WAL_MAX_BATCH_BYTES   >= STRAUM_WAL_MIN_BATCH_BYTES
STRAUM_WAL_INITIAL_BATCH_BYTES  within [min_batch, max_batch]
```

Cross-field constraints are validated too; the error names both variables.
