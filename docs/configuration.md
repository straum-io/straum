# Configuration

The server runs one path since step 8 (D-111): a node of the cluster path,
experimental and **not production-ready** (D-102 A). All of its
configuration comes from `STRAUM_*` environment variables, read only in
`server`; no config file, no serde. Lower crates never read the
environment; they receive typed values.

## Rules

* An unknown `STRAUM_*` variable is a startup error — a typo, or a setting
  of the removed broker, must not fall back to a default or be ignored.
* A malformed or out-of-range value is a startup error, never a fallback.
* Nothing is corrected silently: a configuration that breaks a rule is
  refused before anything opens (D-104 A).
* Units are in the name: `_BYTES`, `_MS`. Switches are `0`/`1`.
* The effective configuration is logged once at `info`, each value with its
  source: `env` or `default`.
* Changing a default is a behavior change: update this file, tests, and the
  change report.

Defaults below are **initial** placeholders: they are adjusted by Linux
measurements, not intuition, and none is measured yet.

## The server

Without `STRAUM_EXPERIMENTAL_CLUSTER=1` the server refuses to start: the
only path is experimental and **not production-ready** — flow control off,
client TLS and credentials only when configured (below), none between nodes, timing defaults placeholders, nothing measured on
Linux (D-102 A, D-111). Only the variables below, `STRAUM_LOG_LEVEL`
(default `info`) and `STRAUM_WORKER_THREADS` (`auto`: the CPUs) are read;
any other `STRAUM_*` variable is refused at startup (D-104 A).

| Variable | Default | Notes |
|---|---|---|
| `STRAUM_EXPERIMENTAL_CLUSTER` | unset | Must be `1`: the operator acknowledges the path is experimental. Unset or `0`: refused, as anything else. |
| `STRAUM_CLUSTER_NODE_ID` | required | This node's id, 0–255. |
| `STRAUM_CLUSTER_PEERS` | required | Every voter, this node included: `id=address,…` (socket addresses). Odd, at least three; no id twice. |
| `STRAUM_CLUSTER_LISTEN` | this node's address in the peers | Where its links listen. |
| `STRAUM_CLUSTER_MQTT_LISTEN` | `127.0.0.1:1883` | Where MQTT clients connect. Loopback by default (D-107). It may bind beyond loopback without the next-but-one setting only when `STRAUM_TLS_*` and a password file are set and anonymous clients are not let in (D-164 A); otherwise credentials, if any, would cross the network in clear. |
| `STRAUM_CLUSTER_METRICS_LISTEN` | off | Where the node serves its metrics over plain HTTP, in the Prometheus text format (D-112): every request gets the current snapshot. No credentials; loopback unless the next setting is `1`. A diagnostic interface, not a monitoring system (D-126 A): at most 4 scrapes at once — more wait unaccepted — and each read and write bounded by 2 s. |
| `STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK` | `0` | `1` lets the MQTT listener, the link listener, the peers and the metrics listener use addresses beyond loopback — links, peers and metrics unencrypted and unauthenticated; MQTT clients encrypted only under `STRAUM_TLS_*`, and without it their passwords cross the network in clear: for an isolated, trusted network only. Without it any such address is refused at startup, except the MQTT listener when TLS and a password file with no anonymous clients secure it (D-164 A). The experimental switch is no security mechanism (D-107). |
| `STRAUM_CLUSTER_DATA_DIR` | required | The log, the promise, snapshots, and the `cluster` file that pins the directory to its cluster's configuration digest: a node started over another cluster's data is refused. |
| `STRAUM_CLUSTER_HEARTBEAT_MS` | `50` | 1–10 000. A placeholder, not a recommendation (D-104 A). |
| `STRAUM_CLUSTER_TAKEOVER_TICKS` | `3` | Heartbeats of silence before a takeover, 1–1 000. A placeholder; the simulation shows 3 churns under random delivery (D-99). |
| `STRAUM_CLUSTER_RETAIN_BYTES` | `67108864` | The log kept behind a slow replica (D-57 A, D-91 A); a starting value, not a measured optimum. |
| `STRAUM_READ_BYTES` | `65536` | One catch-up or fetch read (D-83 A); at most half the 8 MiB frame — 4 MiB — refused above. |
| `STRAUM_WAL_PREALLOCATE_BYTES` | `0` | Zeros written this far ahead of each log segment's end (ADR-0009, D-170 A), 0–1 GiB, so a sync flushes data only and does not also commit the file's new length through the filesystem's journal: on the measured ext4 SATA host (preliminary) it raised PUBACK throughput about 2.6×. **Off by default** until a host that passes D-129 confirms that; whether the setting stays, and its meaning, are subject to that run (ADR-0009). Recovery is unchanged (a zero frame ends the log, as any torn tail); a sealed segment is trimmed to its log; readers stop at the written end. At most this much disk per open segment beyond its log; one extension costs one write of this size. |
| `STRAUM_AUTH_PASSWORD_FILE` | off | MQTT credentials (D-110 A, D-158 A): one `user:<hash>` line per user, `#` comments and blank lines ignored; the hash is argon2, as `straum-server hash-password` prints it for a password given on stdin. Set: every CONNECT is checked, off the node loop, before its CONNACK; a wrong or unknown user gets "bad user name or password" (3.1.1: return code 4), an anonymous client "not authorized" (5) unless `STRAUM_AUTH_ALLOW_ANONYMOUS` lets it in. Read at startup, where unreadable or malformed refuses the start, and again on SIGHUP (below). |
| `STRAUM_AUTH_ACL_FILE` | off | Topic rules: `user <name>` or `anonymous`, then lines `publish <filter>`, `subscribe <filter>` or `readwrite <filter>`. Read at startup and again on SIGHUP (below). The file grants only what it lists (D-159 A): a user it names may do what its rules allow, one it does not name nothing, and anonymous clients only what an `anonymous` section lists. Without the file every user, and every anonymous client let in, may do anything. A correct file under which clients can connect but nobody, anonymous clients included, may do anything starts, with a warning in the log (D-160 A, D-161 A; an ACL file alone that grants anonymous clients nothing included); a file that cannot be read or parsed refuses the start, never falling back to a looser policy. A refused SUBSCRIBE filter gets "not authorized" (3.1.1: 0x80) and is not proposed; a refused PUBLISH is never proposed: MQTT 5 answers a QoS 1 one "not authorized" (or closes the connection, "not authorized", while earlier QoS 1 publishes still wait for their PUBACKs, which a refusal may not overtake) and drops a QoS 0 one; 3.1.1 closes the connection. A subscription is allowed only where a rule covers all it can match. |
| `STRAUM_AUTH_ALLOW_ANONYMOUS` | `1` without a password file, `0` with one | Whether a client without a user name is let in (restricted by the ACL file's `anonymous` section). A configuration under which no client could connect is refused at startup: no user, and anonymous clients refused. One where clients connect but may do nothing starts with a warning. |
| `STRAUM_TLS_CERT_PATH` | off | A PEM certificate chain for the MQTT listener (D-164 A). With it and the next one, the listener speaks TLS only, never plaintext beside it. Both or neither; read at startup, where a file that cannot be read, holds no certificate or a key of another certificate refuses the start, and again on SIGHUP (below). A TLS handshake must finish within the gateway's connect timeout (10 s), and at most 1 024 run at once: a connection past that is closed at once. TLS state per connection (rustls's buffers and session) is not charged to the read-buffer budget; it is bounded by count, at most 1 024 handshakes and 1 024 open connections, not measured in bytes. At most 16 connections from one source address wait at once, for a handshake or a CONNECT (D-165 A); past that the source's next one is closed at once, so one address or one IPv6 /64 cannot take every slot: a source is an IPv4 address, or an IPv6 /64, since a host commonly holds a whole one (D-168 A); an IPv4-mapped address counts as its IPv4 address. Hosts sharing one /64 share the 16: so do every link-local client (fe80::/64, whatever its interface), and, behind a NAT64 that shows IPv4 clients as 64:ff9b::/96, every IPv4 client. Clients behind one NAT share those 16; many sources together, or a host routed a wider prefix, still can fill the 1 024, which is a firewall's job. Each refusal before the gateway is counted, with or without TLS, outside the stable metrics (D-167 A): `straum_mqtt_refused_no_permit_total`, `straum_mqtt_refused_source_full_total`, and `straum_mqtt_handshake_failed_total` (a connection whose setup, read buffers or the TLS handshake, failed or did not finish in time). Clients only: links between nodes are not encrypted (node-to-node TLS comes later, D-138 (6)). |
| `STRAUM_TLS_KEY_PATH` | off | The certificate's private key, PEM. As above. |

Credentials checks (D-158 A) run off the node loop, at most one per CPU at once, each argon2 check taking about 19 MiB while it runs; a check waiting for its turn holds one CONNECT's user name and password and is cancelled when its connection goes, so the waiting ones are at most the gateway's 1 024 connections: at worst 1 024 × the 256 KiB packet bound, 256 MiB. Hashes must use `straum-server hash-password`'s parameters (argon2id, m=19456, t=2, p=1): a refusal costs one hash with them, so a stored hash made with others would let a known user name be told from an unknown one by its time.

Fixed in 7a, not settings: `max_uncommitted` 64 (D-82 A), batches of 16
records, MQTT packets up to 256 KiB, link frames up to 8 MiB, window 256,
snapshots every 4 096 applied records, segments of 64 MiB, a queue of
1 024 proposals (`max_queue`) at the sequencer; each node's outbox of its own
proposals awaiting commit holds its share, `max_queue / voters` rounded
down and never 0 — 341 with three voters (D-109 B, D-120 A): up to 341
times a packet of 256 KiB, about 85 MiB, per node at worst, a count without
a byte budget of its own (D-119 A); while it is full the node reads none
of its MQTT clients. A queue below one proposal per voter is refused at startup
(`ConfigError::Queue`), so the shares always fit it. A link to a down peer is redialled at least once per
heartbeat (D-122 A), and at once when the peer comes back as a new process:
its HELLO carries its incarnation (D-151 A). And the gateway's and `apply`'s limits at the values in `node::experimental`. Every node of one
cluster must agree on the voters, the first slot, the timing and these
bounds: they go into the configuration digest each link's HELLO carries, and
a link from a node that differs is refused. Wills, persistent sessions and
session expiry are refused at CONNECT until step 7c. SIGHUP reads the TLS and credential files again (D-166 A), the ones named at startup: every file is read and checked by the start's rules first, and only if all pass are they used, for new connections; open connections keep what they were admitted with, and a credentials check already started (running, or waiting for its turn) finishes with the old files. If any fails, the log says why and the files in use stay, never a looser policy. A reload never turns TLS or credentials on or off (the settings are not read again); `systemctl reload straum` sends it. SIGUSR1 asks the node to
hand its slot over (a protocol handoff, D-105 A); SIGTERM or Ctrl-C stops it,
with exit code 0. A node that stops by itself — its log fails, a committed
record does not apply, a panic — logs why, and the process exits non-zero so
a supervisor restarts it (D-152 A).

### Stable metrics (D-141 A)

Clients outside the cluster — `straum-cli` first (D-140) — may rely on
these series of `STRAUM_CLUSTER_METRICS_LISTEN` and on nothing else there.
Each is a gauge without labels, one sample per node; its name, type and
meaning (its HELP text) change only by a decision recorded here, and
`node`'s test `the_stable_metrics_are_rendered_as_gauges` reads this block
and fails if the renderer and it disagree. Every other series is unstable:
diagnostics that may be renamed, re-labelled or removed with any change —
the catch-up series of D-132–D-139 among them.

<!-- stable-metrics:begin -->
```text
straum_node
straum_slot
straum_leading
straum_serving
straum_commit
straum_applied
straum_log_end
straum_log_durable
straum_lag
straum_outbox_entries
straum_outbox_capacity
```
<!-- stable-metrics:end -->

What they mean, as rendered: `straum_node` this node's id; `straum_slot`
the highest slot it promised; `straum_leading` 1 while it sequences its
slot; `straum_serving` 1 once its gateway takes clients; `straum_commit`
the commit point it knows; `straum_applied` the last position applied
here; `straum_log_end` and `straum_log_durable` its log's last position
and the position durable through; `straum_lag` positions its log is
behind the commit point it heard; `straum_outbox_entries` and
`straum_outbox_capacity` its own proposals awaiting commit and their
bound. A value is this node's view at the scrape — not the cluster's, and
not a health verdict.

### Stable names (D-141 A)

Three more lists clients may rely on, by the same rule as the metrics —
`straum-cli`'s coverage check reads these blocks, not the source. Each is
pinned by a test in the crate that owns it, which reads the block from
this file.

The settings the server reads (`server`'s
`the_stable_settings_are_the_ones_read`), each described in the table
above:

<!-- stable-settings:begin -->
```text
STRAUM_EXPERIMENTAL_CLUSTER
STRAUM_LOG_LEVEL
STRAUM_WORKER_THREADS
STRAUM_CLUSTER_NODE_ID
STRAUM_CLUSTER_PEERS
STRAUM_CLUSTER_DATA_DIR
STRAUM_CLUSTER_LISTEN
STRAUM_CLUSTER_MQTT_LISTEN
STRAUM_CLUSTER_METRICS_LISTEN
STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK
STRAUM_CLUSTER_HEARTBEAT_MS
STRAUM_CLUSTER_TAKEOVER_TICKS
STRAUM_CLUSTER_RETAIN_BYTES
STRAUM_READ_BYTES
STRAUM_WAL_PREALLOCATE_BYTES
STRAUM_AUTH_PASSWORD_FILE
STRAUM_AUTH_ACL_FILE
STRAUM_AUTH_ALLOW_ANONYMOUS
STRAUM_TLS_CERT_PATH
STRAUM_TLS_KEY_PATH
```
<!-- stable-settings:end -->

The journal's record kinds that are written and applied today, and their
numbers (`journal`'s `the_stable_kinds_are_the_ones_decoded`, and
`apply`'s `the_stable_kinds_are_written_and_applied_and_the_reserved_are_refused`,
which fails if a stable kind is not written or a reserved one is applied):

<!-- stable-kinds:begin -->
```text
BatchHeader = 0
Publish = 1
SessionGone = 3
Subscribe = 4
Unsubscribe = 5
Ack = 6
NodeReset = 9
SlotStart = 24
SlotEnd = 25
```
<!-- stable-kinds:end -->

Reserved kinds (unified-stream §2.7): their numbers are held — no other
kind will take them — but nothing writes them yet, `apply` refuses each,
and their names and meaning stay free until 7c or phase 6 builds them. Not
stable. `Session`, `Connect` and `Disconnect` were listed as stable by
mistake until D-145 A moved them here.

<!-- reserved-kinds:begin -->
```text
Session = 2
Connect = 7
Disconnect = 8
SessionExpired = 16
MessageExpired = 17
WillPublished = 18
QosTwoState = 32
SharedAssignment = 33
```
<!-- reserved-kinds:end -->

The MQTT reason codes the gateway sends by name — in CONNACK, SUBACK and
DISCONNECT, refusals among them (`gateway`'s
`the_stable_reason_codes_are_the_ones_sent`). That check is textual: the
`ReasonCode::` names in `gateway/src/lib.rs`, which today is where the
gateway names every reason it sends. A malformed packet closes the
connection with the reason `codec` gives it; those are not in this list.

<!-- stable-reasons:begin -->
```text
Success
GrantedQos1
BadAuthenticationMethod
BadUserNameOrPassword
ClientIdentifierNotValid
ImplementationSpecificError
KeepAliveTimeout
NotAuthorized
ProtocolError
QosNotSupported
QuotaExceeded
ReceiveMaximumExceeded
ServerBusy
ServerUnavailable
SessionTakenOver
SharedSubscriptionsNotSupported
SubscriptionIdentifiersNotSupported
TopicAliasInvalid
```
<!-- stable-reasons:end -->

## Internal settings

Not read from the environment: fixed in 7a (`node::experimental`), or set by tests and simulations.

### `codec` (ADR-0008 step 3a)

`codec::Limits`; the experimental profile lowers `max_packet_bytes` to 256 KiB,
so a batch of the largest packets fits one link frame.

| Limit | Default | Notes |
|---|---|---|
| `max_packet_bytes` | `1048576` | Whole packet, fixed header included; checked against Remaining Length before any allocation. |
| `max_topic_bytes` | `1024` | |
| `max_topic_levels` | `32` | |
| `max_client_id_bytes` | `256` | 3.1.1 requires accepting 1–23 bytes; more is allowed. |
| `max_filters_per_subscribe` | `64` | Per SUBSCRIBE/UNSUBSCRIBE packet. |
| `payload_copy_threshold_bytes` | `512` | Payloads up to this size are copied at ingress instead of sliced: one copy per publish, not per subscriber, so a small payload does not pin a read-buffer chunk. |
| `max_user_properties` | `32` | User Properties in one MQTT 5 property section; more is refused before the next is kept (Implementation specific error, 0x83). |

### `apply` and `gateway` (ADR-0008 step 3b-2)

Bounds the apply function decides from the log. Every node must run with the
same values: they are part of what the log means. None is read from the
environment until the server runs the new path (step 7, D-68 A).

| Limit | Default | Notes |
|---|---|---|
| `max_subscriptions_per_session` | `1024` | Beyond: SUBACK failure (Quota exceeded in MQTT 5). |
| `max_subscriptions` | `1000000` | All sessions together. |
| `max_retained` | `10000` | Beyond: delivered, not kept. |
| `max_retained_bytes` | `67108864` | Retained payload bytes. |
| `max_inflight` | `256` | At-least-once deliveries unacknowledged per session. |
| `max_backlog` | `16384` | Waiting behind a full window, per session. |
| `max_backlog_bytes` | `8388608` | Per session. Beyond: at-most-once dropped, at-least-once closes the session as a slow consumer. |
| `max_total_queued_bytes` | `1073741824` | Inflight and backlog payload bytes of all sessions together. At-least-once beyond: backlog, then a slow-consumer close. |

Gateway settings, per node:

| Setting | Notes |
|---|---|
| `first_session` | Kept durable and growing across restarts by the driver: sessions are never renumbered. |
| `max_connections` | Beyond: the socket is closed at accept. |
| `node::MAX_WAITING_PER_SOURCE` (16, fixed) | Connections from one source (an IPv4 address, or an IPv6 /64, D-168 A) waiting at once for a TLS handshake or a CONNECT, plaintext and TLS alike (D-165 A); beyond: the next one is closed at accept. |
| `connect_timeout` | Time from accept to a valid CONNECT. |
| `max_pending`, `max_pending_bytes` | Proposals of one connection awaiting commit; at the bound the connection is not read. |
| `max_total_pending_bytes` | All connections. |
| `commit_timeout` | A proposal not committed within it closes its connection. |
| `retry_interval` | First interval for `Ack`, `SessionGone`, `NodeReset`; doubles to 32×. |
| `receive_maximum`, `topic_alias_maximum` | Advertised to MQTT 5 clients. |
| `max_waiting`, `max_waiting_bytes`, `max_total_waiting_bytes` | At-most-once deliveries held behind a QoS 1 delivery waiting for the client's receive maximum; beyond, dropped and counted. |
| `max_retries` | Lifecycle proposals awaiting their effect; at the bound new connections are refused. |

### `node` (ADR-0008 step 4)

`node::NodeConfig`, per node. The server builds it from the variables above
in the experimental cluster mode (step 7a); tests build it directly.

| Setting | Notes |
|---|---|
| `core` | `cluster::Config`: id, voters, the first slot's owner, replication window, queue, batch, `max_uncommitted` (lsns appended past the commit point; it also bounds how long a takeover takes, since a new sequencer moves that tail first — `cluster::DEFAULT_MAX_UNCOMMITTED` = 64, measured in simulation, used once step 7a reads configuration, throughput cost unmeasured, D-82 A, D-84), `takeover_ticks` (heartbeats of silence before the next node in the schedule takes the slot over; step 5), `lag_bound` (`Option<LagBound>`, default `None`: flow control on replica lag, ADR-0008 R1–R4 — while an attached replica is more than `records` behind the commit point no new batch is made, for at most `ticks` heartbeats, after which that replica is detached; `records` is raised to `max_uncommitted + max_batch`; never holds commit; values come from the Linux measurement, D-89). |
| `data` (step 5) | Also holds `promise`: the highest slot promised, synced before any answer. |
| `listen`, `peers` | Where other nodes connect to this one; where to reach each of them. One TCP connection per direction. |
| `data` | The segmented log, the `snapshot` file (the committed slot table, pruned by compaction, and the applied state at one lsn: `[version=2][slots]{…}[state]`; D-67 B, D-73 A, D-79 A) and the `incarnation` file (the gateway's `first_session`, made durable before use). |
| `retain_bytes` | D-57 A: the log keeps no more than this behind a replica that is only slow; one further behind is lost until state copy (step 7b). `node::DEFAULT_RETAIN_BYTES` = 64 MiB, the removed broker's default; read from `STRAUM_CLUSTER_RETAIN_BYTES`; a starting value, not a measured optimum (D-91 A). |
| `snapshot_every` | Applied records between snapshots; segments wholly below the last durable snapshot and the retention floor are removed after it is written. |
| `read_bytes` | `usize`, no default: bytes one read of the log hands back at most (or one batch, if that alone is larger) — what a sequencer's catch-up or a candidate's fetch moves per round trip, within the replication window and the sequencer's durable log (D-81 A). Clamped to half of `max_frame`, which carries it. The node tests use 64 KiB. |
| `rotate_every` | `Option<u32>`, default `None`: leading, hand the slot over to the next owner by `SlotEnd` every this many heartbeats (ADR-0008 step 6; unified-stream §2.4 planned rotation). Off by default: a sequencer is stable. |
| `segments` | `storage::SegmentLimits`. |
| `limits` | `apply::Limits`, the table above. A link whose HELLO carries another digest of them is refused. |
| `heartbeat` | Sequencer tick and gateway tick. |
| `max_frame` | Largest link frame read; beyond, the link closes. |
| `link_queue` | Frames queued per peer; beyond, dropped (the protocol resends). |
| `mqtt.listen`, `mqtt.gateway` | MQTT listener and the gateway settings above. |
| `mqtt.write_queue` | Packets queued per client; a client that lets it fill is closed. |

## Access rules (D-110 A, D-158 A, D-159 A)

The formats of `STRAUM_AUTH_PASSWORD_FILE` and `STRAUM_AUTH_ACL_FILE`, read by the `auth` crate. What the settings do is in the table above.

The password file is one `user:hash` per line; `#` starts a comment. The
hash is an argon2 PHC string, as `straum-server hash-password` prints it. A
PHC string carries its own cost parameters, but use that command's: a
refusal costs one hash with them, so a stored hash made with others lets a
known user name be told from an unknown one by its time (D-158 A).

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
