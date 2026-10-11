#!/usr/bin/env sh
# Fail if a crate depends, directly or transitively, on something its layer
# forbids. Dev-dependencies are not checked. Source of truth: CLAUDE.md §2.
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

check domain      tokio auth transport storage journal cluster codec apply gateway node server sim
check auth        tokio transport storage journal cluster codec apply gateway node server sim
check transport   auth storage journal cluster codec apply gateway node server sim
check storage     tokio auth transport journal cluster codec apply gateway node server sim
# `journal` owns record bodies v4 and the batch chain: sans-IO, what it
# encodes is opaque to every other layer.
check journal     tokio domain auth transport storage cluster codec apply gateway node server sim
# `cluster` is the sequencer and the replica, sans-IO: no tokio, no storage,
# no sockets; it knows only the record format.
check cluster     tokio domain auth transport storage codec apply gateway node server sim
# `codec` is MQTT 3.1.1 and 5 bytes and nothing else: no tokio, no socket;
# it knows topics and client ids from `domain`.
check codec       tokio auth transport storage journal cluster apply gateway node server sim
# `apply` is the apply function over committed records: no tokio, no MQTT,
# no storage; it reads `journal` records and knows topics from `domain`.
check apply       tokio auth codec transport storage cluster gateway node server sim
# `gateway` is the MQTT side of a node, sans-IO: no tokio, no sockets, no
# storage, no cluster; it speaks `codec` and the `apply` contract.
check gateway     tokio auth transport storage cluster node server sim
# `node` is the driver: tokio and sockets, the cores, the log on disk.
check node        server sim
# `sim` drives the cores with simulated time, network and disk (ADR-0007):
# no tokio, no sockets, no driver.
check sim         tokio transport node server

if [ "$fail" -eq 0 ]; then
    echo "crate boundaries: ok"
fi
exit "$fail"
