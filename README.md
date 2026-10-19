# Straum

A clustered MQTT broker in Rust that acknowledges a publish only after a
majority of its nodes has committed it to one replicated log.

> **Status: experimental.** Not production-ready: no TLS or credentials,
> flow control off, timing defaults are placeholders, and persistent
> sessions, wills and session expiry are refused at CONNECT for now.

## Why

A PUBACK from a single-node broker means one machine has your message. In
Straum it means the message is in the log of a majority of the cluster, so
a three-node cluster keeps every acknowledged message when one node dies.

## The idea

A Straum cluster is one mechanism, not a set of nodes that happen to talk
to each other.

- **One log decides.** Everything a client can observe, from a PUBACK to a
  delivery, a retained message or a change of sequencer, is the result of
  applying a committed record. No node changes shared state any other way.
- **Nodes are deterministic.** A node is a function of the committed log
  and its input events: two nodes with the same log prefix hold the same
  state.
- **Predictable over fast.** Who takes over next is known in advance,
  handing the sequencer role over is routine, and each failure has one
  defined reaction.
- **Clocks don't decide.** Timeouts only start a transition; the log and
  the majority decide how it ends.
- **Proof before claims.** Correctness is shown by deterministic
  simulation, performance only by measurements on Linux. No "faster"
  without numbers.

The goal it has to prove: a replicated MQTT 3.1.1 and 5 broker where a
PUBACK means a majority committed the message, a planned leader change
keeps client connections open, simulation checks correctness, and any
`unsafe` code is isolated, justified and verified.

Under the hood the protocol is a variant of Viewstamped Replication and
Multi-Paxos with a deterministic owner per epoch, plus a few ideas from
Solana's leader schedule. It is not a new consensus algorithm, and
consensus only comes in when an owner fails.

**Who it's for:** telemetry and industrial systems where a lost message
costs money (metering, billing, SCADA events, audit trails), edge-to-cloud
gateways that want a small, predictable binary, and teams that need a
replicated broker under a permissive license without a JVM or BEAM
runtime.

**When goals conflict, this order wins:** an acknowledged message is
committed; memory stays bounded; operations stay predictable (explicit
backpressure and failure modes, configuration errors at startup);
performance per CPU cycle, measured; then protocol coverage.

**What it won't become:** a benchmark winner at the cost of those
guarantees, a rule engine or integration hub, a broker with a management
UI built in (that is [straum-console](https://github.com/straum-io/straum-console),
outside the broker process), or a server for protocols other than MQTT.

## What it does

- **MQTT 3.1.1 and MQTT 5** over TCP, from one packet model with
  properties and reason codes. QoS 0 and 1, retained messages, MQTT 5
  subscription options. QoS 2 and shared subscriptions are not supported
  yet.
- **Majority commit.** A gateway forwards each publish to the sequencer,
  which orders it in the log; the PUBACK goes out once a majority holds
  the record.
- **Failover and handoff.** A silent sequencer is taken over by another
  node; an operator can hand the role over on purpose with `SIGUSR1`.
- **Bounded memory.** Every queue and buffer has a limit and a stated
  behavior when it is full, so a slow client cannot grow the broker
  without bound.
- **Deterministic simulation.** Three-node clusters run under a simulated
  network and disk with the safety invariants checked throughout,
  next to real three-node tests on loopback.
- **Metrics.** Each node can serve Prometheus text; the stable metric and
  setting names are pinned by tests.

## Quick start

You need Rust 1.85 or newer. Build the server:

```sh
cargo build --release -p server --bin straum-server
```

Start three nodes on one machine (there is no single-node mode):

```sh
export STRAUM_EXPERIMENTAL_CLUSTER=1
export STRAUM_CLUSTER_PEERS=0=127.0.0.1:7000,1=127.0.0.1:7001,2=127.0.0.1:7002
for i in 0 1 2; do
  STRAUM_CLUSTER_NODE_ID=$i \
  STRAUM_CLUSTER_DATA_DIR=/tmp/straum/n$i \
  STRAUM_CLUSTER_MQTT_LISTEN=127.0.0.1:188$((3 + i)) \
  STRAUM_CLUSTER_METRICS_LISTEN=127.0.0.1:910$i \
  target/release/straum-server &
done
```

Subscribe on one node, publish to another. Any MQTT client works; with
[straum-cli](https://github.com/straum-io/straum-cli):

```sh
straum-cli sub --broker 127.0.0.1:1884 -t 'sensors/+' -v &
straum-cli pub --broker 127.0.0.1:1883 -t sensors/a -m 21.5
# sensors/a 21.5

straum-cli status --node 127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102
```

Everything binds to loopback. Any other address is refused unless you set
`STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1`, and then only on a network you
trust: nothing is authenticated or encrypted yet.

## Run with Docker

Build the image (about 40 MB, a distroless base, runs as a non-root user):

```sh
docker build -t straum .
```

Three nodes on their own network. Containers need addresses beyond
loopback, so this sets `STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1`: keep that
network private, since nothing is authenticated or encrypted yet.

```sh
docker network create --subnet 10.89.42.0/24 straum
PEERS=0=10.89.42.10:7000,1=10.89.42.11:7000,2=10.89.42.12:7000
for i in 0 1 2; do
  docker run -d --name straum-$i --network straum --ip 10.89.42.1$i \
    -v straum-$i:/var/lib/straum \
    -p 188$((3 + i)):1883 -p 910$i:9100 \
    -e STRAUM_EXPERIMENTAL_CLUSTER=1 \
    -e STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1 \
    -e STRAUM_CLUSTER_NODE_ID=$i \
    -e STRAUM_CLUSTER_PEERS=$PEERS \
    -e STRAUM_CLUSTER_LISTEN=0.0.0.0:7000 \
    -e STRAUM_CLUSTER_MQTT_LISTEN=0.0.0.0:1883 \
    -e STRAUM_CLUSTER_METRICS_LISTEN=0.0.0.0:9100 \
    straum
done
```

The ports map as in the quick start (MQTT on 1883–1885, metrics on
9100–9102), so the same `straum-cli` commands work. Each node keeps its log
in its volume and picks up where it left off after a restart. Peers are
given as IP addresses: the node does not resolve host names.

Release versions are published to Docker Hub by
[`.github/workflows/release.yml`](.github/workflows/release.yml) for
`linux/amd64` and `linux/arm64`, with provenance and an SBOM attached.

## Install the Debian package

On Ubuntu 22.04, 24.04 and 26.04, add the PPA and let apt keep Straum up
to date (amd64 and arm64):

```sh
sudo add-apt-repository ppa:@PPA@
sudo apt install straum
```

Otherwise, each release on [GitHub](https://github.com/straum-io/straum/releases) has
`.deb` packages for amd64 and arm64 (Debian 12, Ubuntu 22.04 and newer),
with `SHA256SUMS` and build provenance. The package installs
`/usr/bin/straum-server`, a `straum` systemd service and its settings file;
it does not start anything, and neither does the PPA package. Then:

```sh
sudo apt install ./straum_<version>_<arch>.deb
sudoedit /etc/default/straum          # node id, peers, listen addresses
sudo systemctl enable --now straum
```

The service runs as a dynamic user with its data in `/var/lib/straum` and
a tight sandbox (`systemd-analyze security` rates it 1.4). Run it on hosts
or VMs; in containers use the Docker image, since the sandbox needs
namespaces a container usually cannot create.

## Run on Kubernetes

[`deploy/kubernetes`](deploy/kubernetes) is a Kustomize package: a
three-replica StatefulSet with a volume per node, one Service per node for
the links, a `straum-mqtt` Service for clients, a headless Service for
per-pod metrics, and a NetworkPolicy that keeps the links to Straum pods.
It needs Kubernetes 1.28 or newer (each pod takes its node id from the
`apps.kubernetes.io/pod-index` label).

```sh
kubectl apply -k deploy/kubernetes
kubectl rollout status statefulset/straum
kubectl port-forward svc/straum-mqtt 1883:1883
```

Before applying, set two things:

- **The image**, in `kustomization.yaml` (`images:`).
- **Three free ClusterIPs** in `peers.yaml`, inside your service CIDR, and
  the same three in `STRAUM_CLUSTER_PEERS` in `statefulset.yaml`. The node
  takes socket addresses, not host names, so each peer gets a Service with a
  fixed IP that survives pod restarts. The defaults fit the common
  `10.96.0.0/12`.

The pods run as a non-root user with a read-only root filesystem.
Kubernetes service links are off, because the server refuses unknown
`STRAUM_*` variables.

One limitation to know before you rely on it: a pod that restarts can stay
behind and not serve clients
([#13](https://github.com/straum-io/straum/issues/13)); the other two keep
the cluster available.

## Compared with others

Need a broker in production today? Use Mosquitto on a single node, or a
clustered broker such as EMQX, VerneMQ or HiveMQ. Straum is for people who
want an MQTT cluster whose behavior follows from one log, and want to
follow or help with getting there.

## More

- [docs/configuration.md](docs/configuration.md): every setting, the
  metrics, and the names clients may rely on.
- [straum-cli](https://github.com/straum-io/straum-cli): a terminal client
  for publish, subscribe and cluster status.
- [Issues](https://github.com/straum-io/straum/issues): what is being
  worked on.
- The architecture, ADRs and decision log live in the private
  `straum-io/straum-dev-plan` repository, under
  `plugins/straum-engineering/repos/straum/`.

Apache License 2.0.
