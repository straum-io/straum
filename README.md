# Straum

[![CI](https://github.com/straum-io/straum/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/straum-io/straum/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/straum-io/straum?include_prereleases&sort=semver)](https://github.com/straum-io/straum/releases)
[![Docker image](https://img.shields.io/docker/v/straumio/straum?sort=semver&label=docker)](https://hub.docker.com/r/straumio/straum)
[![PPA](https://img.shields.io/badge/apt-ppa%3Astraum%2Fppa-E95420)](https://launchpad.net/~straum/+archive/ubuntu/ppa)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
![Status: experimental](https://img.shields.io/badge/status-experimental-orange)

A replicated log: a scheduled sequencer orders records, and a record is
committed once a majority of the nodes holds it on disk.

**Website and docs:** [straum-io.github.io](https://straum-io.github.io/)

> **Status: experimental.** Straum has no client protocol right now. It was
> an MQTT broker until MQTT was removed, and its own protocol is being
> designed. What runs today is the cluster: nodes, links, the log,
> failover and handoff, metrics. Not production-ready: flow control is off
> and timing defaults are placeholders.

## Why

Most replicated systems pick a leader by election and find out who leads
after the fact. In Straum the order of sequencers is a schedule known in
advance: handing the role over is a routine, planned step, and a failed
sequencer is replaced by the next one in line. The committed log is the
only thing that decides; clocks only start a transition.

## What it does

- **Majority commit.** The sequencer of the current slot orders proposals
  into the log; a record is committed once a majority has it durably.
- **Scheduled leadership.** A silent sequencer is taken over by the next
  node in the schedule, fenced by durable promises; an operator hands the
  role over with `SIGUSR1`.
- **A segmented log on disk** with group commit, snapshots and compaction,
  and a chain hash over every batch, so two diverged logs disagree from the
  next batch on.
- **Bounded memory.** Every queue has a limit and a stated behavior when
  it is full.
- **Deterministic simulation.** Three-node clusters under a simulated
  network and disk, with the safety invariants checked after every step,
  next to real three-node tests on loopback.
- **TLS between nodes**, each node's certificate proving which node it is,
  and **Prometheus metrics** whose stable names are pinned by tests.

## Quick start

You need Rust 1.85 or newer. Build the server and start three nodes on one
machine:

```sh
cargo build --release -p server --bin straum-server
export STRAUM_EXPERIMENTAL_CLUSTER=1
export STRAUM_CLUSTER_PEERS=0=127.0.0.1:7000,1=127.0.0.1:7001,2=127.0.0.1:7002
for i in 0 1 2; do
  STRAUM_CLUSTER_NODE_ID=$i \
  STRAUM_CLUSTER_DATA_DIR=/tmp/straum/n$i \
  STRAUM_CLUSTER_METRICS_LISTEN=127.0.0.1:910$i \
  target/release/straum-server &
done
```

Watch them commit and hand over:

```sh
curl -s 127.0.0.1:9100 | grep -E '^straum_(slot|leading|commit) '
kill -USR1 %1        # node 0 hands its slot to node 1
curl -s 127.0.0.1:9101 | grep -E '^straum_(slot|leading|commit) '
```

Everything binds to loopback. Any other address is refused unless you set
`STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1`, on a network you trust, or, for
the links between nodes, configure TLS between nodes
(`STRAUM_CLUSTER_TLS_*`). The metrics endpoint never leaves loopback without
the first. Every setting is in [docs/configuration.md](docs/configuration.md).

## Run it elsewhere

- **Docker:** `docker build -t straum .` gives a distroless image that runs
  as a non-root user. Containers need addresses beyond loopback, so set
  `STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1` on a private network or
  configure node TLS. Releases are published to Docker Hub for
  `linux/amd64` and `linux/arm64`.
- **Debian and Ubuntu:** `sudo add-apt-repository ppa:straum/ppa && sudo apt
  install straum`, or a `.deb` from the
  [releases](https://github.com/straum-io/straum/releases). The package
  installs a `straum` systemd service and `/etc/default/straum`; it starts
  nothing until you set the node id and peers there.
- **Kubernetes:** [`deploy/kubernetes`](deploy/kubernetes) is a Kustomize
  package for a three-replica StatefulSet. Set the image and three free
  ClusterIPs (`peers.yaml`) first; the node takes socket addresses, not host
  names.

## Compared with others

Need a replicated log in production today? Use Kafka, Redpanda or an etcd
cluster. Straum is for people who want to follow, or help with, a log
whose leadership is a schedule rather than an election, checked by
deterministic simulation.

## More

- [docs/configuration.md](docs/configuration.md): every setting, the
  metrics, and the names other tools may rely on.
- [straum-cli](https://github.com/straum-io/straum-cli): `status` reads
  every node's metrics.
- [Issues](https://github.com/straum-io/straum/issues): what is being
  worked on.
- The architecture, ADRs and decision log live in the private
  `straum-io/straum-dev-plan` repository, under
  `plugins/straum-engineering/repos/straum/`.

Apache License 2.0.
