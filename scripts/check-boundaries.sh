#!/usr/bin/env sh
# Fail if a crate depends, directly or transitively, on something its layer
# forbids. Dev-dependencies are not checked. Source of truth: CLAUDE.md §2.
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

check domain      tokio auth protocol mqtt transport storage replication broker runtime server
check auth        tokio protocol mqtt transport storage replication broker runtime server
check protocol    tokio mqtt transport storage replication broker runtime server
# `auth` is not forbidden to mqtt and broker: it arrives through `protocol`,
# which carries `Permissions` in its Connect. Forbidding it there would
# forbid the contract itself.
check mqtt        tokio transport storage replication broker runtime server
check transport   auth protocol mqtt storage replication broker runtime server
check storage     tokio auth protocol mqtt transport broker runtime server replication
# `replication` is the wire format only: no tokio, no sockets, and no
# workspace crate at all. What it frames is opaque bytes.
check replication tokio domain auth protocol mqtt transport storage broker runtime server
check broker      tokio mqtt transport storage replication runtime server
check runtime     server
# `sim` drives the cores the runtime drives, with simulated time, network and
# disk (ADR-0007): no tokio, no runtime, no sockets.
# `journal` owns record bodies v4 and the batch chain: sans-IO, what it
# encodes is opaque to every other layer.
check journal     tokio domain auth protocol mqtt transport storage replication broker runtime server sim
# `cluster` is the sequencer and the replica, sans-IO: no tokio, no storage,
# no sockets; it knows only the record format.
check cluster     tokio domain auth protocol mqtt transport storage replication broker runtime server sim
# `codec` is MQTT 3.1.1 and 5 bytes and nothing else: no tokio, no socket, no
# broker; it knows topics and client ids from `domain`.
check codec       tokio auth protocol mqtt transport storage replication journal cluster broker runtime server sim
# `apply` is the apply function over committed records: no tokio, no MQTT,
# no storage; it reads `journal` records and knows topics from `domain`.
check apply       tokio auth protocol mqtt codec transport storage replication cluster broker runtime server sim
# `gateway` is the MQTT side of a node, sans-IO: no tokio, no sockets, no
# storage, no cluster; it speaks `codec` and the `apply` contract.
check gateway     tokio auth protocol mqtt transport storage replication cluster broker runtime server sim
# `node` is the new path's driver (ADR-0008 step 4): tokio and sockets,
# but none of the old path's crates.
check node        protocol mqtt replication broker runtime server sim
check sim         tokio runtime transport server broker mqtt protocol node

if [ "$fail" -eq 0 ]; then
    echo "crate boundaries: ok"
fi
exit "$fail"
