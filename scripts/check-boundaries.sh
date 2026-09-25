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

if [ "$fail" -eq 0 ]; then
    echo "crate boundaries: ok"
fi
exit "$fail"
