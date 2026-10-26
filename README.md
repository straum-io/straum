# Straum

[![CI](https://github.com/straum-io/straum/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/straum-io/straum/actions/workflows/ci.yml)
![Status: rebuilding](https://img.shields.io/badge/status-rebuilding-lightgrey)

Straum is being rebuilt from scratch. This repository holds a single-node
broker, its protocol and conformance suite, a storage engine, and the build
and release setup:

- `crates/straum-broker`: the broker, version 0: one node, one shard, the
  binary protocol of [docs/protocol.md](docs/protocol.md) over TCP
  ([README](crates/straum-broker/README.md)).
- `crates/straum-proto`: the protocol's encoding, no I/O.
  `crates/straum-client`: the reference client (blocking, `std::net`).
- `conformance/`: the language-independent conformance suite (golden
  frames, partitioner vectors, broker and client scenarios;
  [README](conformance/README.md)), run by `crates/straum-conformance`,
  which also holds the scripted fake server for client SDKs.
- `crates/straum-store`: a content-addressed log storage engine on compio
  (one shard, local; [README](crates/straum-store/README.md)).
  `crates/straum-store-bench` is its disk benchmark, `tools/dedup-survey` a
  duplicate survey over log files.
- `crates/straum-broker-bench` and `scripts/bench/broker-e2e.sh`: the
  broker's end-to-end benchmark.
- `.github/`: CI (`ci.yml`), the release pipeline (`release.yml`), the PR
  body check (`pr-contract.yml`), the Issue and PR templates.
- `Cargo.toml`, `clippy.toml`: the workspace settings (members under
  `crates/` and `tools/`, lints, release profile).
- `Dockerfile`, `debian/`, `deploy/kubernetes/`, `scripts/ppa.sh`: the
  image, the Debian and PPA packaging and the Kubernetes package. They
  build the `straum-server` binary and work again once a crate provides it.
- `scripts/linux.sh`: runs cargo in a Linux container on macOS.
- `scripts/container/`: runs cargo in a Linux container with io_uring
  enabled, on a dedicated Podman machine.

The release pipeline runs only on version tags.

The previous code, a replicated log with scheduled leadership, is kept
under the tag
[`archive/main-2026-10-23`](https://github.com/straum-io/straum/tree/archive/main-2026-10-23)
and in the releases `v0.1.0-rc.1` to `v0.1.0-rc.3`.

Apache License 2.0.
