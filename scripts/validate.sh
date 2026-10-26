#!/usr/bin/env bash
# Validation at three levels; success is judged by exit status only.
#
#   scripts/validate.sh quick   # before every commit: macOS, long tests skipped
#   scripts/validate.sh push    # before every push: macOS and the io_uring
#                               # container, long tests at 50 crash iterations
#   scripts/validate.sh pr      # before a PR: as push, 200 crash iterations
#
# Long tests are `#[ignore = "long: ..."]`; push and pr run them with
# `-- --include-ignored`. The macOS and container sequences run at the same
# time, each with its own log under target/validate/, and the script prints
# both exit statuses. The container runs through
# `scripts/container/linux.sh --session`: `straum-test` stays running
# afterwards (stop it with `scripts/container/linux.sh --end-session`).
# STRAUM_STORE_CRASH_ITERATIONS and STRAUM_BROKER_CRASH_ITERATIONS, if set,
# override the level's iteration count.
set -uo pipefail

LEVEL="${1:-}"
case "$LEVEL" in
    quick) ;;
    push) ITERATIONS=50 ;;
    pr) ITERATIONS=200 ;;
    *)
        echo "usage: scripts/validate.sh quick|push|pr" >&2
        exit 2
        ;;
esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
LOGS="$ROOT/target/validate"
mkdir -p "$LOGS"

# Runs each command, logging it and its exit status; returns 1 if any failed.
steps() {
    local log="$1" status=0
    shift
    : >"$log"
    for cmd in "$@"; do
        echo "+ $cmd" >>"$log"
        bash -c "$cmd" >>"$log" 2>&1
        local s=$?
        echo "exit=$s :: $cmd" | tee -a "$log"
        [ "$s" -eq 0 ] || status=1
    done
    return "$status"
}

if [ "$LEVEL" = quick ]; then
    steps "$LOGS/quick-macos.log" \
        "cargo fmt --check" \
        "cargo clippy --workspace --all-targets -- -D warnings" \
        "cargo test --workspace"
    status=$?
    echo "macos: exit $status (log: $LOGS/quick-macos.log)"
    exit "$status"
fi

export STRAUM_STORE_CRASH_ITERATIONS="${STRAUM_STORE_CRASH_ITERATIONS:-$ITERATIONS}"
export STRAUM_BROKER_CRASH_ITERATIONS="${STRAUM_BROKER_CRASH_ITERATIONS:-$ITERATIONS}"

steps "$LOGS/$LEVEL-macos.log" \
    "cargo fmt --check" \
    "cargo clippy --workspace --all-targets -- -D warnings" \
    "cargo clippy --workspace --all-targets --all-features -- -D warnings" \
    "cargo test --workspace -- --include-ignored" &
macos=$!

# One container for the three steps; the variables go in with -e.
container_log="$LOGS/$LEVEL-container.log"
# shellcheck disable=SC2016 # the script expands inside the container
(
    STORE_CONTAINER_ARGS="${STORE_CONTAINER_ARGS:-} -e STRAUM_STORE_CRASH_ITERATIONS -e STRAUM_BROKER_CRASH_ITERATIONS" \
        scripts/container/linux.sh --session -- sh -c '
            status=0
            for cmd in "cargo fmt --check" \
                "cargo clippy --workspace --all-targets -- -D warnings" \
                "cargo test --workspace -- --include-ignored"; do
                echo "+ $cmd"
                $cmd
                s=$?
                echo "exit=$s :: container $cmd"
                [ "$s" -eq 0 ] || status=1
            done
            exit $status' >"$container_log" 2>&1
    s=$?
    grep -E '^exit=' "$container_log"
    if ! grep -q 'linux_runtime_uses_io_uring \.\.\. ok' "$container_log"; then
        echo "container: linux_runtime_uses_io_uring did not pass"
        s=1
    fi
    exit "$s"
) &
container=$!

wait "$macos"
macos_status=$?
wait "$container"
container_status=$?
echo "macos: exit $macos_status (log: $LOGS/$LEVEL-macos.log)"
echo "container: exit $container_status (log: $container_log)"
[ "$macos_status" -eq 0 ] && [ "$container_status" -eq 0 ]
