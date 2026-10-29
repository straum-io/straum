#!/usr/bin/env sh
# Run a cargo command inside a Linux container.
#
# Used on macOS to exercise cfg(target_os = "linux") code paths. Linux hosts
# can run cargo directly. Needs podman or docker; set STRAUM_CONTAINER_ENGINE
# to pick one explicitly.
#
#   scripts/linux.sh                          # cargo test --workspace --tests
#   scripts/linux.sh test -p transport
#   STRAUM_CONTAINER_ARGS="--sysctl net.core.somaxconn=8192" scripts/linux.sh test
#
# Build artifacts go to a named volume, not ./target: macOS and Linux artifacts
# must never share a target directory.
set -eu

IMAGE="${STRAUM_LINUX_IMAGE:-rust:1.95-bookworm}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

ENGINE="${STRAUM_CONTAINER_ENGINE:-}"
if [ -z "$ENGINE" ]; then
    for candidate in podman docker; do
        if command -v "$candidate" >/dev/null 2>&1; then
            ENGINE="$candidate"
            break
        fi
    done
fi
if [ -z "$ENGINE" ]; then
    echo "scripts/linux.sh: neither podman nor docker found" >&2
    exit 127
fi

if [ "$#" -eq 0 ]; then
    # --tests rather than --all-targets: it skips the benchmarks entirely,
    # where --all-targets builds them all and runs divan's short pass
    # (D-4).
    set -- test --workspace --tests
fi

# STRAUM_CONTAINER_ARGS is split on whitespace on purpose.
# shellcheck disable=SC2086
exec "$ENGINE" run --rm \
    -v "$ROOT":/src -w /src \
    -v straum-cargo-registry:/usr/local/cargo/registry \
    -v straum-target-linux:/src/target \
    ${STRAUM_CONTAINER_ARGS:-} \
    "$IMAGE" \
    sh -c 'rustup component add clippy rustfmt >/dev/null 2>&1 || true; exec cargo "$@"' cargo "$@"
