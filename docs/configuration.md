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
timing defaults placeholders, failover and performance not confirmed by a
Linux measurement (D-102 A, D-111). It serves no clients: MQTT was removed
(D-188 A) and Straum's own protocol is not built yet. Only the variables below, `STRAUM_LOG_LEVEL`
(default `info`) and `STRAUM_WORKER_THREADS` (`auto`: the CPUs) are read;
any other `STRAUM_*` variable is refused at startup (D-104 A).

| Variable | Default | Notes |
|---|---|---|
| `STRAUM_EXPERIMENTAL_CLUSTER` | unset | Must be `1`: the operator acknowledges the path is experimental. Unset or `0`: refused, as anything else. |
| `STRAUM_CLUSTER_NODE_ID` | required | This node's id, 0–255. |
| `STRAUM_CLUSTER_PEERS` | required | Every voter, this node included: `id=address,…` (socket addresses). Odd, at least three; no id twice. |
| `STRAUM_CLUSTER_LISTEN` | this node's address in the peers | Where its links listen. It and the peers may be beyond loopback without the unauthenticated-network setting when the three `STRAUM_CLUSTER_TLS_*` are set (ADR-0010). |
| `STRAUM_CLUSTER_METRICS_LISTEN` | off | Where the node serves its metrics over plain HTTP, in the Prometheus text format (D-112): every request gets the current snapshot. No credentials; loopback unless the next setting is `1`. A diagnostic interface, not a monitoring system (D-126 A): at most 4 scrapes at once — more wait unaccepted — and each read and write bounded by 2 s. |
| `STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK` | `0` | `1` lets the link listener, the peers and the metrics listener use addresses beyond loopback — metrics always unencrypted and unauthenticated, links and peers so unless `STRAUM_CLUSTER_TLS_*` are set: for an isolated, trusted network only. Without it any such address is refused at startup, except the link listener and the peers when node TLS is set (ADR-0010). The experimental switch is no security mechanism (D-107). |
| `STRAUM_CLUSTER_DATA_DIR` | required | The log, the promise, snapshots, and the `cluster` file that pins the directory to its cluster's configuration digest: a node started over another cluster's data is refused. |
| `STRAUM_CLUSTER_HEARTBEAT_MS` | `50` | 1–10 000. A placeholder, not a recommendation (D-104 A). |
| `STRAUM_CLUSTER_TAKEOVER_TICKS` | `3` | Heartbeats of silence before a takeover, 1–1 000. A placeholder; the simulation shows 3 churns under random delivery (D-99). With node TLS at least 4, or the start is refused (D-173 A): a stalled handshake to a restarted peer can take a heartbeat on top of a backoff, the handshake and the wait for the next heartbeat. |
| `STRAUM_CLUSTER_RETAIN_BYTES` | `67108864` | The log kept behind a slow replica (D-57 A, D-91 A); a starting value, not a measured optimum. |
| `STRAUM_READ_BYTES` | `65536` | One catch-up or fetch read (D-83 A); at most half the 8 MiB frame — 4 MiB — refused above. |
| `STRAUM_WAL_PREALLOCATE_BYTES` | `8388608` (8 MiB) | Zeros written this far ahead of each log segment's end (ADR-0009, D-170 A, D-178 A), 0–1 GiB, so a sync flushes data only and does not also commit the file's new length through the filesystem's journal. Measured on one host that passes D-129 (an i5-10600, ext4 on a SATA disk, 2026-10-22), through the MQTT path since removed: steady PUBACKs/s from about 1 800 to about 4 500 with it, journal commits per flush from 0.5 to 0.01. The pre-zeroed rounds of that run varied 1–7 %, and it is one host and one filesystem: no precise gain is claimed for others. Not measured on XFS, Btrfs or ZFS. On a copy-on-write filesystem (Btrfs, ZFS) no benefit is expected (not measured), only the cost of writing the zeros: set `0` there, which turns it off. Recovery is unchanged (a zero frame ends the log, as any torn tail); a sealed segment is trimmed to its log; readers stop at the written end. At most this much disk per open segment beyond its log; one extension costs one write of this size. |
| `STRAUM_CLUSTER_TLS_CERT_PATH` | off | This node's certificate chain for its links, PEM (ADR-0010, D-171 A). With the next two, every link is TLS 1.3, with no session resumption and no early data, and both ends present a certificate the cluster CA signed. All three or none. The certificate must name this node exactly once (below), or the start is refused. A node with them and one without cannot link: turning node TLS on or off means stopping every node, then starting every node with the new settings. |
| `STRAUM_CLUSTER_TLS_KEY_PATH` | off | Its private key, PEM. As above. |
| `STRAUM_CLUSTER_TLS_CA_PATH` | off | The cluster CA, PEM: the only certificates a peer's chain may lead to. Use a CA for this cluster alone: its key is the root of every node's identity. As above. |

Fixed in 7a, not settings: `max_uncommitted` 64 (D-82 A), batches of 16
records, records (body and properties) up to 256 KiB, link frames up to 8 MiB, window 256,
snapshots every 4 096 applied records, segments of 64 MiB, a queue of
1 024 proposals (`max_queue`) at the sequencer; each node's outbox of its own
proposals awaiting commit holds its share, `max_queue / voters` rounded
down and never 0 — 341 with three voters (D-109 B, D-120 A): up to 341
times a record of 256 KiB, about 85 MiB, per node at worst, a count without
a byte budget of its own (D-119 A); while it is full the node refuses new
proposals and counts them. A queue below one proposal per voter is refused at startup
(`ConfigError::Queue`), so the shares always fit it. A link to a down peer is redialled at least once per
heartbeat (D-122 A), and at once when the peer comes back as a new process:
its HELLO carries its incarnation (D-151 A). Every node of one
cluster must agree on the voters, the first slot, the timing and these
bounds: they go into the configuration digest each link's HELLO carries, and
a link from a node that differs is refused. SIGHUP reads the node TLS files again (D-166 A), the ones named at startup: every file is read and checked by the start's rules first, and only if all pass are they used. If any fails, the log says why and the files in use stay. After they are installed, every open link, dialled or accepted, is checked again with the new CA, its peer's node name and the current time, and one that fails, or cannot be checked, is closed (D-172 C); a renewal under the same CA closes none. A reload never turns TLS on or off (the settings are not read again); `systemctl reload straum` sends it. SIGUSR1 asks the node to
hand its slot over (a protocol handoff, D-105 A); SIGTERM or Ctrl-C stops it,
with exit code 0. A node that stops by itself — its log fails, a committed
record does not decode, a panic — logs why, and the process exits non-zero so
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
slot; `straum_commit`
the commit point it knows; `straum_applied` the last position applied
here; `straum_log_end` and `straum_log_durable` its log's last position
and the position durable through; `straum_lag` positions its log is
behind the commit point it heard; `straum_outbox_entries` and
`straum_outbox_capacity` its own proposals awaiting commit and their
bound. `straum_serving` was removed with the gateway (D-188 A). A value
is this node's view at the scrape — not the cluster's, and not a health
verdict.

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
STRAUM_CLUSTER_METRICS_LISTEN
STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK
STRAUM_CLUSTER_HEARTBEAT_MS
STRAUM_CLUSTER_TAKEOVER_TICKS
STRAUM_CLUSTER_RETAIN_BYTES
STRAUM_READ_BYTES
STRAUM_WAL_PREALLOCATE_BYTES
STRAUM_CLUSTER_TLS_CERT_PATH
STRAUM_CLUSTER_TLS_KEY_PATH
STRAUM_CLUSTER_TLS_CA_PATH
```
<!-- stable-settings:end -->

The journal's record kinds, version 5, and their numbers (`journal`'s
`the_stable_kinds_are_the_ones_decoded`). Version 5 (D-188 A) dropped the
MQTT kinds; a record of version 4 is refused by its version byte.

<!-- stable-kinds:begin -->
```text
BatchHeader = 0
Data = 1
SlotStart = 24
SlotEnd = 25
```
<!-- stable-kinds:end -->

Reserved kinds: none since D-188 A. The block stays so the readers of these
blocks keep finding it.

<!-- reserved-kinds:begin -->
```text
```
<!-- reserved-kinds:end -->

Reason codes: none since D-188 A removed the MQTT gateway that sent them.
The block stays empty for the same reason.

<!-- stable-reasons:begin -->
```text
```
<!-- stable-reasons:end -->

## Internal settings

Not read from the environment: fixed in 7a (`node::experimental`), or set by tests and simulations.

### `node` (ADR-0008 step 4)

`node::NodeConfig`, per node. The server builds it from the variables above
in the experimental cluster mode (step 7a); tests build it directly.

| Setting | Notes |
|---|---|
| `core` | `cluster::Config`: id, voters, the first slot's owner, replication window, queue, batch, `max_uncommitted` (lsns appended past the commit point; it also bounds how long a takeover takes, since a new sequencer moves that tail first — `cluster::DEFAULT_MAX_UNCOMMITTED` = 64, measured in simulation, used once step 7a reads configuration, throughput cost unmeasured, D-82 A, D-84), `takeover_ticks` (heartbeats of silence before the next node in the schedule takes the slot over; step 5), `lag_bound` (`Option<LagBound>`, default `None`: flow control on replica lag, ADR-0008 R1–R4 — while an attached replica is more than `records` behind the commit point no new batch is made, for at most `ticks` heartbeats, after which that replica is detached; `records` is raised to `max_uncommitted + max_batch`; never holds commit; values come from the Linux measurement, D-89). |
| `data` (step 5) | Also holds `promise`: the highest slot promised, synced before any answer. |
| `listen`, `peers` | Where other nodes connect to this one; where to reach each of them. One TCP connection per direction. |
| `data` | The segmented log, the `snapshot` file (the committed slot table, pruned by compaction, the applied position and its digest: `[version=3][slots]{…}[applied][digest]`; D-67 B, D-73 A, D-79 A, D-188 A) and the `incarnation` file (this process's life, made durable before use; it is in every HELLO and in the origin property of each own proposal). |
| `retain_bytes` | D-57 A: the log keeps no more than this behind a replica that is only slow; one further behind is lost until state copy (step 7b). `node::DEFAULT_RETAIN_BYTES` = 64 MiB, the removed broker's default; read from `STRAUM_CLUSTER_RETAIN_BYTES`; a starting value, not a measured optimum (D-91 A). |
| `snapshot_every` | Applied records between snapshots; segments wholly below the last durable snapshot and the retention floor are removed after it is written. |
| `read_bytes` | `usize`, no default: bytes one read of the log hands back at most (or one batch, if that alone is larger) — what a sequencer's catch-up or a candidate's fetch moves per round trip, within the replication window and the sequencer's durable log (D-81 A). Clamped to half of `max_frame`, which carries it. The node tests use 64 KiB. |
| `rotate_every` | `Option<u32>`, default `None`: leading, hand the slot over to the next owner by `SlotEnd` every this many heartbeats (ADR-0008 step 6; unified-stream §2.4 planned rotation). Off by default: a sequencer is stable. |
| `segments` | `storage::SegmentLimits`. |
| `max_record` | The largest body and properties of one record, the node's origin property included (`journal::ORIGIN_PROPERTY`: node, incarnation, sequence, on every own proposal so the outbox tells its records from another node's); a larger proposal is refused and counted. In the configuration digest. A batch of `max_batch` such records must fit `max_frame`, or the start is refused. |
| `heartbeat` | Sequencer tick. |
| `max_frame` | Largest link frame read; beyond, the link closes. |
| `link_queue` | Frames queued per peer; beyond, dropped (the protocol resends). |
## Node certificates (ADR-0010, D-171 A, D-172 C)

Each node's certificate names it with a subjectAltName dNSName
`<id>.node.straum`: its `STRAUM_CLUSTER_NODE_ID` in decimal, no leading
zeros, as `2.node.straum` for node 2. The name is checked exactly:

- Of the certificate's dNSNames, those under `node.straum` (compared in
  lower case, wildcards and ids such as `02` included) must be exactly one,
  and it must be this node's. A wildcard is refused, and so is a
  certificate naming two nodes. The Subject CN is not read.
- extendedKeyUsage includes both serverAuth and clientAuth, or is absent:
  every node both dials and accepts.
- A node dialling another closes the link unless the peer's certificate
  names the node it dialled, before it sends HELLO. A node accepting a link
  closes it unless the dialler's certificate names the node in its HELLO,
  before the HELLO has any effect. A HELLO naming the accepting node itself
  is refused: two hosts hold one node's key.
- TLS proves which node is speaking and nothing else: HELLO keeps its
  checks, and the cluster protocol and the committed log keep their rules.
  No authority comes from an address.

With node TLS, a link's connect and handshake must finish within one
heartbeat; a stalled attempt is tried again at once. With or without TLS, a
connection to a link port that has not sent a HELLO it may send within two
heartbeats of its accept is closed, and at most twice as many as there are
peers wait at once; one more is closed at once. Without TLS a waiting
connection holds its read and write buffers from the transport's budget, so at most twice the peers times those buffers; with TLS
it holds none until its handshake is done. Refusals before a HELLO names a
node (a handshake that fails or stalls) are logged at debug level with
the source address, since anyone who reaches the port can cause them; too
many waiting is one warning per burst; refusals of a named node are
errors. A flood of such
connections can keep links from forming, which costs availability, not
safety: shield the link port with a firewall.

One way to issue the files, with `openssl`:

```sh
# Once per cluster: the CA. Keep ca.key off the nodes.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout ca.key -out ca.pem -days 3650 -subj "/CN=straum cluster CA" \
  -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign

# Per node, here node 2.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout node-2.key -out node-2.csr -subj "/CN=node 2"
openssl x509 -req -in node-2.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out node-2.pem -days 365 -extfile <(printf '%s\n' \
  'subjectAltName=DNS:2.node.straum' \
  'extendedKeyUsage=serverAuth,clientAuth' \
  'keyUsage=critical,digitalSignature')
```

Node 2 then runs with `STRAUM_CLUSTER_TLS_CERT_PATH=node-2.pem`,
`STRAUM_CLUSTER_TLS_KEY_PATH=node-2.key`, `STRAUM_CLUSTER_TLS_CA_PATH=ca.pem`
and `STRAUM_CLUSTER_TAKEOVER_TICKS=4` or more.

To renew, issue a new certificate from the same CA, replace the files and
send SIGHUP: no link closes. To remove a node, for example after its key
leaked, give every other node files of a new CA (or new certificates of a
CA that never signed the removed node), then SIGHUP each: its links to the
removed node, both ways, close and stay closed. There is no revocation list.
A peer certificate that expires keeps its link until the next reload checks
it.
