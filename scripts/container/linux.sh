#!/usr/bin/env sh
# shellcheck disable=SC2317 # on_signal and restore run from traps
# Run a cargo command inside a Linux container on the dedicated Podman
# machine `straum-test`, which allows io_uring (seccomp profile and SELinux
# module next to this script; setup in scripts/container/README.md). Used on
# macOS to run the storage engine's tests on io_uring. Results from a
# container on macOS are functional checks only, never performance evidence.
#
#   scripts/container/linux.sh                    # cargo test --workspace
#   scripts/container/linux.sh run -p straum-store --example uring_probe
#   scripts/container/linux.sh --switch-machine test --workspace
#   STORE_CONTAINER_ARGS="-v /host/dir:/bench" scripts/container/linux.sh run ...
#   scripts/container/linux.sh -- scripts/bench/format7-vs-8.sh --quick --out /bench/out
#
# After `--`, the rest is run as a command instead of cargo.
#
# Podman on macOS runs one machine at a time. If another machine is
# running, the script refuses and stops nothing, unless --switch-machine is
# the first argument: then it stops that machine, starts `straum-test`,
# runs the command, stops `straum-test` and starts the previous machine
# again, also when the command fails or the script is interrupted. If no
# machine was running, `straum-test` is started for the command and stopped
# after it. The command's exit status is returned.
#
# The configuration of machines other than `straum-test` is never changed.
#
# Build artifacts go to a named volume, not ./target: macOS and Linux
# artifacts must never share a target directory.
set -eu

MACHINE="${CONTAINER_CONNECTION:-straum-test}"
# rust:1-bookworm, rustc 1.99.1, pinned by digest.
IMAGE="${STORE_LINUX_IMAGE:-docker.io/library/rust@sha256:8a2a102d089fbd151137b9473c01b1ed4519a64fb9c032a03609b815701615c8}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

SWITCH=0
if [ "${1:-}" = "--switch-machine" ]; then
    SWITCH=1
    shift
fi

if ! command -v podman >/dev/null 2>&1; then
    echo "scripts/container/linux.sh: podman not found" >&2
    exit 127
fi
if ! podman machine inspect "$MACHINE" >/dev/null 2>&1; then
    echo "scripts/container/linux.sh: Podman machine '$MACHINE' does not exist." >&2
    echo "Create it as described in scripts/container/README.md." >&2
    exit 1
fi

# Running machines other than ours. `podman machine list` marks the default
# machine with a trailing '*'.
other=""
target_running=0
for line in $(podman machine list --format '{{.Name}}|{{.Running}}'); do
    name="${line%%|*}"
    name="${name%\*}"
    running="${line##*|}"
    [ "$running" = "true" ] || continue
    if [ "$name" = "$MACHINE" ]; then
        target_running=1
    else
        other="$name"
    fi
done

if [ -n "$other" ] && [ "$SWITCH" -eq 0 ]; then
    echo "scripts/container/linux.sh: Podman machine '$other' is running; only one machine can run at a time." >&2
    echo "Nothing was stopped. To let this script stop '$other', run '$MACHINE' for the command" >&2
    echo "and start '$other' again afterwards, pass --switch-machine as the first argument:" >&2
    echo "  scripts/container/linux.sh --switch-machine ${*:-test --workspace}" >&2
    exit 2
fi

started=0
child=""
NAME="straum-container-$$"
# A signal interrupts `wait` at once (a foreground command would delay the
# trap until it exits). The container is stopped by name: cargo runs as its
# PID 1, which ignores signals it has no handler for, so a forwarded signal
# would not end it.
on_signal() {
    if [ -n "$child" ]; then
        podman --connection "$MACHINE" stop --time 5 "$NAME" >/dev/null 2>&1 || true
        wait "$child" 2>/dev/null || true
    fi
    exit "$1"
}
trap 'on_signal 130' INT
trap 'on_signal 143' TERM

restore() {
    status=$?
    trap - EXIT
    if [ "$started" -eq 1 ]; then
        echo "scripts/container/linux.sh: stopping '$MACHINE'" >&2
        podman machine stop "$MACHINE" >/dev/null || status=1
    fi
    if [ -n "$other" ]; then
        echo "scripts/container/linux.sh: starting '$other' again" >&2
        podman machine start "$other" >/dev/null || status=1
    fi
    exit "$status"
}

if [ "$target_running" -eq 0 ]; then
    trap restore EXIT
    if [ -n "$other" ]; then
        echo "scripts/container/linux.sh: stopping '$other' (--switch-machine)" >&2
        podman machine stop "$other" >/dev/null
    fi
    echo "scripts/container/linux.sh: starting '$MACHINE'" >&2
    started=1
    podman machine start "$MACHINE" >/dev/null
fi

if [ "$#" -eq 0 ]; then
    set -- test --workspace
fi

# STORE_CONTAINER_ARGS is split on whitespace on purpose.
# shellcheck disable=SC2086
podman --connection "$MACHINE" run --rm --name "$NAME" \
    -v "$ROOT":/src -w /src \
    -v straum-container-registry:/usr/local/cargo/registry \
    -v straum-container-target:/src/target \
    ${STORE_CONTAINER_ARGS:-} \
    "$IMAGE" \
    sh -c 'rustup component add clippy rustfmt >/dev/null 2>&1 || true
        if [ "${1:-}" = "--" ]; then shift; exec "$@"; fi
        exec cargo "$@"' cargo "$@" &
child=$!
status=0
wait "$child" || status=$?
child=""
exit "$status"
