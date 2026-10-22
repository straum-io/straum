#!/usr/bin/env sh
# The cost of measuring (D-124): two source trees, a baseline and a
# candidate, built in release and measured in alternating rounds — the
# order flipped each round — with medians at the end.
#
# Each round runs, for each tree, node/benches/cap.rs at cap 64: records/s
# through a node handle, and failover time (that tree's own bench). The
# MQTT `puback` bench went with MQTT (D-188 A).
#
#   git worktree add --detach .bench/base 071db00
#   git worktree add --detach .bench/head HEAD     # or use the tree itself
#   scripts/overhead.sh .bench/base . 5
#
# Accepted numbers come from a quiet Linux host. Anything else is marked
# preliminary.
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
        cargo bench -p node --bench cap --no-run 2>&1 | executable
    )
}

echo "building $A" >&2
CAP_A=$(cd "$A" && pwd)/$(build "$A")
echo "building $B" >&2
CAP_B=$(cd "$B" && pwd)/$(build "$B")
# D-178 A made zeros ahead of each segment the default. Both trees run
# without them, so the comparison measures what it is for: a tree that
# knows the setting is given 0, an older one has none and would refuse
# the unknown variable (D-104 A).
zeros() {
    if grep -q STRAUM_WAL_PREALLOCATE_BYTES "$1/crates/server/src/config.rs" 2>/dev/null; then
        echo 0
    fi
}
ZEROS_A=$(zeros "$A")
ZEROS_B=$(zeros "$B")

run() { # label cap-binary round zeros
    if [ -n "$4" ]; then
        STRAUM_BENCH_PREALLOCATE_BYTES=$4
        export STRAUM_BENCH_PREALLOCATE_BYTES
    else
        unset STRAUM_BENCH_PREALLOCATE_BYTES
    fi
    # 50 failovers: failover is a distribution (D-130) — its share within
    # 1 s, p50 and p90 (p90 only from builds that print it), and the slow
    # mode's diagnosis lines.
    STRAUM_BENCH_CAPS=64 STRAUM_BENCH_ROUNDS=3 STRAUM_BENCH_FAILOVERS=${STRAUM_OVERHEAD_FAILOVERS:-50} "$2" \
        | awk -v l="$1" -v r="$3" '
            $1 == "64" {
                p90 = "-"; for (i = 1; i <= NF; i++) if ($i ~ /^p90=/) { p90 = substr($i, 5) }
                print l, r, "cap", "publishes_per_s=" $2, "failover_within_1s=" ($5 > 0 ? $3 / $5 : 0), "failover_p50_ms=" $6, "failover_p90_ms=" p90
            }
            $1 == "failover" { print l, r, $0 }'
}

: > "$OUT/rounds"
for round in $(seq 1 "$ROUNDS"); do
    if [ $((round % 2)) -eq 1 ]; then
        run base "$CAP_A" "$round" "$ZEROS_A" | tee -a "$OUT/rounds"
        run head "$CAP_B" "$round" "$ZEROS_B" | tee -a "$OUT/rounds"
    else
        run head "$CAP_B" "$round" "$ZEROS_B" | tee -a "$OUT/rounds"
        run base "$CAP_A" "$round" "$ZEROS_A" | tee -a "$OUT/rounds"
    fi
done

echo "medians over $ROUNDS rounds, with the spread between rounds (min, max):"
for label in base head; do
    for key in publishes_per_s failover_within_1s failover_p50_ms failover_p90_ms; do
        grep "^$label " "$OUT/rounds" | tr ' ' '\n' | sed -n "s/^$key=//p" | sort -n \
            | awk -v l="$label" -v k="$key" '{ v[NR] = $1 } END { if (NR) print l, k, "median", v[int((NR + 1) / 2)], "min", v[1], "max", v[NR] }'
    done
done
