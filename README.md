# Straum

[![CI](https://github.com/straum-io/straum/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/straum-io/straum/actions/workflows/ci.yml)
![Status: rebuilding](https://img.shields.io/badge/status-rebuilding-lightgrey)

Straum is being rebuilt from scratch. This repository holds a storage engine
and the build and release setup the new code will use:

- `crates/straum-store`: a content-addressed log storage engine on compio
  (one shard, local; [README](crates/straum-store/README.md)).
  `crates/straum-store-bench` is its disk benchmark, `tools/dedup-survey` a
  duplicate survey over log files, `tools/block-survey` a block compression
  survey.
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
- `scripts/bench/format7-vs-8.sh`: the storage engine's format 7 against
  its compressed-block format on a Linux machine
  ([how to run it](crates/straum-store/README.md#format-7-against-format-8-on-a-linux-machine)).

The release pipeline runs only on version tags.

The previous code, a replicated log with scheduled leadership, is kept
under the tag
[`archive/main-2026-10-23`](https://github.com/straum-io/straum/tree/archive/main-2026-10-23)
and in the releases `v0.1.0-rc.1` to `v0.1.0-rc.3`.

Apache License 2.0.
