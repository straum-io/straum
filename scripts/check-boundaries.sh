#!/usr/bin/env sh
# Fail if a crate depends, directly or transitively, on something its layer
# forbids. Dev-dependencies are not checked.
# One path since step 8 (D-111): the old broker's crates are gone.
set -eu

cd "$(dirname "$0")/.."
fail=0

check() {
    crate="$1"
    shift
    deps="$(cargo tree -p "$crate" -e normal,build --prefix none --format '{p}' \
        | awk '{print $1}' | sort -u)"
    for forbidden in "$@"; do
        if printf '%s\n' "$deps" | grep -qx "$forbidden"; then
            echo "boundary violation: $crate depends on $forbidden" >&2
            fail=1
        fi
    done
}

check transport   storage journal cluster node server sim
check storage     tokio transport journal cluster node server sim
check journal     tokio transport storage cluster node server sim
check cluster     tokio transport storage node server sim
check node        server sim
check sim         tokio transport node server

if [ "$fail" -eq 0 ]; then
    echo "crate boundaries: ok"
fi
exit "$fail"
