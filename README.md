# Straum

[![CI](https://github.com/straum-io/straum/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/straum-io/straum/actions/workflows/ci.yml)
![Status: rebuilding](https://img.shields.io/badge/status-rebuilding-lightgrey)

Straum is being rebuilt from scratch. This repository holds no code right
now, only the build and release setup the new code will use:

- `.github/`: CI (`ci.yml`), the release pipeline (`release.yml`), the PR
  body check (`pr-contract.yml`), the Issue and PR templates.
- `Cargo.toml`, `clippy.toml`: the workspace settings (members under
  `crates/`, lints, release profile).
- `Dockerfile`, `debian/`, `deploy/kubernetes/`, `scripts/ppa.sh`: the
  image, the Debian and PPA packaging and the Kubernetes package. They
  build the `straum-server` binary and work again once a crate provides it.
- `scripts/linux.sh`: runs cargo in a Linux container on macOS.

While `crates/` is empty, CI skips its cargo steps, and the release
pipeline runs only on version tags.

The previous code, a replicated log with scheduled leadership, is kept
under the tag
[`archive/main-2026-10-23`](https://github.com/straum-io/straum/tree/archive/main-2026-10-23)
and in the releases `v0.1.0-rc.1` to `v0.1.0-rc.3`.

Apache License 2.0.
