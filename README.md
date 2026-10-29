# Straum

[![CI](https://github.com/straum-io/straum/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/straum-io/straum/actions/workflows/ci.yml)
![Status: experimental](https://img.shields.io/badge/status-experimental-orange)

A durable event log for one server: a single binary with the data model of
a partitioned log. Messages are acknowledged only after their group commit
is written and `fdatasync`ed, stored content-addressed with exact
deduplication, and recovered by strict rules that never repair data
silently.

> **Status: experimental, version 0.** One node, one partition per topic,
> loopback only (no authentication yet), formats not stable. Not for
> production. What is and is not guaranteed today:
> [docs/guarantees.md](docs/guarantees.md).

## Quick start

Start a broker on loopback (the binary is `straum-server`; `cargo run`
builds it as `target/release/straum-server`):

```bash
git clone https://github.com/straum-io/straum && cd straum
cargo run --release -p straum-broker -- \
    --data-dir /tmp/straum-data --cluster-id dev --node-id 1 --listen 127.0.0.1:7171
```

In another terminal, with the
[CLI](https://github.com/straum-io/straum-cli)
([v0.1.0](https://github.com/straum-io/straum-cli/releases/tag/v0.1.0)):

```bash
straum-cli ping
straum-cli topic create orders
straum-cli produce orders 'first order' 'second order' --key customer-7
straum-cli consume orders
```

## What it does

- **Durable acknowledgements.** A Produce is answered after its group
  commit is written and `fdatasync`ed. Produce requests from all
  connections are committed together, without a timer, so a busy broker
  commits larger groups.
- **Exact deduplication.** Message bodies are stored once by their BLAKE3
  content id; every publication keeps its own sequence number, key and
  headers. Deduplication is invisible to clients.
- **Strict recovery.** After a crash, recovery changes only what the
  documented rules allow (for example a torn last WAL frame or a derived
  index; the full list is in [docs/guarantees.md](docs/guarantees.md)) and
  reports every change; anything else is an error, not a silent repair.
  Crash tests and a LazyFS power-loss harness run in CI on every PR.
- **Bounded requests.** Backpressure per connection and across
  connections, and limits on requests and responses, bound the memory
  a client can make the broker hold. Limits that still grow with stored
  data are listed in [docs/guarantees.md](docs/guarantees.md).

## Ecosystem

| Project | |
|---|---|
| [straum-cli](https://github.com/straum-io/straum-cli) | command-line client, [v0.1.0](https://github.com/straum-io/straum-cli/releases/tag/v0.1.0) |
| [straum-go-sdk](https://github.com/straum-io/straum-go-sdk) | Go client library, v0.1.0 |
| [straum-console](https://github.com/straum-io/straum-console) | web console, in design |

Clients in other languages can be built against the
[protocol](docs/protocol.md) and checked with the language-independent
[conformance suite](conformance/README.md).

## Documentation

- [docs/guarantees.md](docs/guarantees.md): what an acknowledgement means,
  what survives a crash, recovery, ordering, deduplication, current limits.
- [docs/protocol.md](docs/protocol.md): the binary protocol, version 0.
- [crates/straum-broker](crates/straum-broker/README.md): running and
  configuring the broker.
- [crates/straum-store](crates/straum-store/README.md): the storage engine
  and its on-disk format.

## Development

Rust (see `rust-version` in `Cargo.toml`). Linux is the target (io_uring);
macOS works for development with the polling driver.

```bash
scripts/validate.sh quick   # fmt, clippy, the quick tests
scripts/validate.sh push    # adds the long tests, and the io_uring container (scripts/container/README.md)
```

## Layout

- `crates/`: the broker, the protocol encoding, the reference client, the
  conformance runner, the storage engine, benchmarks.
- `conformance/`: fixtures and scenarios.
- `tools/`: the duplicate survey and the power-loss writer and verifier.
- `scripts/`: validation, the Linux container, benchmarks, the power-loss
  harness.
- `docs/`: guarantees and protocol.
- `.github/`: CI, the release pipeline (runs only on version tags), the
  PR body check, the Issue and PR templates.
- `Dockerfile`, `debian/`: the image and the Debian package of
  `straum-server` (loopback only, so the image runs with `--network host`;
  see the Dockerfile). `deploy/kubernetes/`: not yet working with the
  current broker.

The previous code, a replicated log with scheduled leadership, is kept
under the tag
[`archive/main-2026-10-23`](https://github.com/straum-io/straum/tree/archive/main-2026-10-23)
and in the releases `v0.1.0-rc.1` to `v0.1.0-rc.3`.

Apache License 2.0.
