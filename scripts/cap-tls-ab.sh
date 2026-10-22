#!/usr/bin/env sh
# `cap` with TLS between nodes off (A) and on (B) (ADR-0010, straum#70),
# on the disk being measured, the order flipped each round. TLS needs 4
# takeover ticks (D-173 A), so both sides run with 4. The logs go under
# target/ on this disk, never the system temp directory: on a host where
# that is tmpfs or has a quota, the logs would not fit or not be measured
# (2026-10-22: every round stopped with EDQUOT in /tmp).
#
#   scripts/cap-tls-ab.sh [rounds] [out]   # 4 by default; about 25 min
#
# Run by scripts/gated-run.sh after the gate, or alone on a host that
# passed it. Writes cap-tls-0.txt, cap-tls-1.txt and cap-tls-summary.txt
# into `out` (target/cap-tls-ab/<time> by default).
set -eu
cd "$(dirname "$0")/.."
ROUNDS=${1:-4}
OUT=${2:-target/cap-tls-ab/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
TMPDIR=$PWD/target/cap-tls-ab/tmp
export TMPDIR
rm -rf "$TMPDIR"
mkdir -p "$TMPDIR"
fs=$(findmnt -n -o FSTYPE -T "$TMPDIR" 2>/dev/null || stat -f -c %T "$TMPDIR")
if [ "$fs" = tmpfs ] || [ "$fs" = ramfs ]; then
    echo "$0: $TMPDIR is on $fs; nothing here would be measured" >&2
    exit 2
fi
echo "logs in $TMPDIR ($fs); free: $(df -h "$TMPDIR" | awk 'NR == 2 { print $4 }')" | tee "$OUT/cap-tls-host.txt"

BENCH=$(cargo bench -p node --bench cap --no-run 2>&1 | sed -n 's/.*Executable .*(\(.*\))$/\1/p' | tail -1)
if [ -z "$BENCH" ] || [ ! -x "$BENCH" ]; then
    echo "$0: the cap bench did not build" >&2
    exit 4
fi
round=1
while [ "$round" -le "$ROUNDS" ]; do
    if [ $((round % 2)) -eq 1 ]; then order="0 1"; else order="1 0"; fi
    for tls in $order; do
        echo "--- round $round" >> "$OUT/cap-tls-$tls.txt"
        env STRAUM_BENCH_CAPS=64 STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_FAILOVERS=10 \
            STRAUM_BENCH_ROUNDS=1 STRAUM_BENCH_TAKEOVER_TICKS=4 STRAUM_BENCH_TLS="$tls" \
            "$BENCH" >> "$OUT/cap-tls-$tls.txt" 2>&1 || echo "round $round FAILED" >> "$OUT/cap-tls-$tls.txt"
        # A failed round's logs must not crowd out the next one.
        rm -rf "$TMPDIR"
        mkdir -p "$TMPDIR"
    done
    echo "cap round $round done (load $(cut -d' ' -f1-3 /proc/loadavg))"
    round=$((round + 1))
done
rm -rf "$TMPDIR"
for tls in 0 1; do
    echo "tls=$tls"
    grep -E "^64 |^cap_stats|FAILED" "$OUT/cap-tls-$tls.txt" || echo "no result"
done > "$OUT/cap-tls-summary.txt"
cat "$OUT/cap-tls-summary.txt"
