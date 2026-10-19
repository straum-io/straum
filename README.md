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
