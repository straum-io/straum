#!/bin/sh
# scripts/release/archive.sh VERSION TARGET BINARY OUTDIR
#
# Packs BINARY with LICENSE and README.md into
# OUTDIR/straum-server-VERSION-TARGET.tar.gz. First runs BINARY with an
# unknown setting, which must exit 2 (the binary starts and parses its
# arguments); NO_SMOKE=1 skips that, for a binary this host cannot run.
set -eu
version="${1:?usage: archive.sh VERSION TARGET BINARY OUTDIR}"
target="${2:?}"
bin="${3:?}"
out="${4:?}"
name="straum-server-$version-$target"

if [ "${NO_SMOKE:-}" != 1 ]; then
    status=0
    "$bin" --no-such-setting x 2>/dev/null || status=$?
    [ "$status" -eq 2 ] || { echo "archive: $bin --no-such-setting exited $status, expected 2" >&2; exit 1; }
fi

stage="$(mktemp -d)"
trap 'rm -rf -- "${stage:?}"' EXIT
mkdir -- "$stage/$name"
cp -- "$bin" "$stage/$name/straum-server"
cp -- LICENSE README.md "$stage/$name/"
mkdir -p -- "$out"
tar -czf "$out/$name.tar.gz" -C "$stage" "$name"
echo "archive: $out/$name.tar.gz"
