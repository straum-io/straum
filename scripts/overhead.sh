#!/usr/bin/env sh
# The cost of measuring (D-124): two source trees, a baseline and a
# candidate, built in release and measured in alternating rounds — the
# order flipped each round — with medians at the end (CLAUDE.md §11).
#
# Each round runs, for each tree:
#   cap     node/benches/cap.rs at cap 64: publishes/s through a node
#           handle, and failover time (that tree's own bench);
#   puback  server/benches/puback.rs from THIS tree against that tree's
#           straum-server: PUBACKs/s, p50/p99 publish-to-PUBACK, and the
#           servers' CPU time per 1 000 PUBACKs.
#
#   git worktree add --detach .bench/base 071db00
#   git worktree add --detach .bench/head HEAD     # or use the tree itself
#   scripts/overhead.sh .bench/base . 5
#
# Accepted numbers come from a quiet Linux host. Anything else is marked
# preliminary; on a macOS container only CPU time per PUBACK says much.
set -eu

A=$1
B=$2
ROUNDS=${3:-5}
HERE=$(cd "$(dirname "$0")/.." && pwd)
OUT=${STRAUM_OVERHEAD_OUT:-$HERE/target/overhead}
mkdir -p "$OUT"

executable() {
    sed -n 's/.*Executable .*(\(.*\))$/\1/p' | tail -1
}

build() {
    (
        cd "$1"
        cargo build --release -p server --bin straum-server >&2
        cargo bench -p node --bench cap --no-run 2>&1 | executable
    )
}

echo "building $A" >&2
CAP_A=$(cd "$A" && pwd)/$(build "$A")
echo "building $B" >&2
CAP_B=$(cd "$B" && pwd)/$(build "$B")
PUBACK=$HERE/$(cd "$HERE" && cargo bench -p server --bench puback --no-run 2>&1 | executable)
SERVER_A=$(cd "$A" && pwd)/target/release/straum-server
SERVER_B=$(cd "$B" && pwd)/target/release/straum-server

run() { # label cap-binary server-binary round
    # 50 failovers: failover is a distribution (D-130) — its share within
    # 1 s, p50 and p90 (p90 only from builds that print it), and the slow
    # mode's diagnosis lines.
    STRAUM_BENCH_CAPS=64 STRAUM_BENCH_ROUNDS=3 STRAUM_BENCH_FAILOVERS=${STRAUM_OVERHEAD_FAILOVERS:-50} "$2" \
        | awk -v l="$1" -v r="$4" '
            $1 == "64" {
                p90 = "-"; for (i = 1; i <= NF; i++) if ($i ~ /^p90=/) { p90 = substr($i, 5) }
                print l, r, "cap", "publishes_per_s=" $2, "failover_within_1s=" ($5 > 0 ? $3 / $5 : 0), "failover_p50_ms=" $6, "failover_p90_ms=" p90
            }
            $1 == "failover" { print l, r, $0 }'
    STRAUM_BENCH_SERVER=$3 "$PUBACK" | sed "s/^puback/$1 $4 puback/"
}

: > "$OUT/rounds"
for round in $(seq 1 "$ROUNDS"); do
    if [ $((round % 2)) -eq 1 ]; then
        run base "$CAP_A" "$SERVER_A" "$round" | tee -a "$OUT/rounds"
        run head "$CAP_B" "$SERVER_B" "$round" | tee -a "$OUT/rounds"
    else
        run head "$CAP_B" "$SERVER_B" "$round" | tee -a "$OUT/rounds"
        run base "$CAP_A" "$SERVER_A" "$round" | tee -a "$OUT/rounds"
    fi
done

echo "medians over $ROUNDS rounds, with the spread between rounds (min, max):"
for label in base head; do
    for key in publishes_per_s failover_within_1s failover_p50_ms failover_p90_ms pubacks_per_s p50_ms p99_ms cpu_ms_per_1k; do
        grep "^$label " "$OUT/rounds" | tr ' ' '\n' | sed -n "s/^$key=//p" | sort -n \
            | awk -v l="$label" -v k="$key" '{ v[NR] = $1 } END { if (NR) print l, k, "median", v[int((NR + 1) / 2)], "min", v[1], "max", v[NR] }'
    done
done
