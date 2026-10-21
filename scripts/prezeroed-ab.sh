#!/usr/bin/env sh
# D-162 A, a prototype: today's WAL (A) against zeros written ahead of each
# segment's log (B), on the disk being measured. Rounds alternate A B B A,
# so a disk that is faster in its first minutes (2026-10-21: round 1's
# flushes took 0.76 ms, the rest 2.7 ms) does not favour either side.
#
#   scripts/prezeroed-ab.sh [rounds]     # 4 by default; about 1 h 20 min
#
# Each round runs puback's steady and heavy loads and cap at 64, with the
# disk's flushes and journal commits counted. Writes target/prezeroed-ab/
# <time>/ with one file per side and stage, a summary from scripts/rates.awk,
# and run.log. Preliminary unless the host passes D-129.
set -eu
cd "$(dirname "$0")/.."
if [ "$(uname -s)" != Linux ]; then
    echo "$0: this is for a Linux host" >&2
    exit 2
fi
ROUNDS=${1:-4}
AHEAD=8388608
OUT=target/prezeroed-ab/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"
TMPDIR=$PWD/target/prezeroed-ab/tmp
export TMPDIR
mkdir -p "$TMPDIR"
fs=$(findmnt -n -o FSTYPE -T "$TMPDIR" 2>/dev/null || stat -f -c %T "$TMPDIR")
if [ "$fs" = tmpfs ] || [ "$fs" = ramfs ]; then
    echo "$0: $TMPDIR is on $fs; nothing here would be measured" >&2
    exit 2
fi
LOGDEV=$(lsblk -no KNAME "$(findmnt -n -o SOURCE -T "$TMPDIR" | sed 's/\[.*//')" 2>/dev/null | head -1 || true)
DISK=$(lsblk -rs -no NAME,TYPE "/dev/${LOGDEV:-none}" 2>/dev/null | awk '$2 == "disk" { print $1; exit }' || true)
JOURNAL=$(ls /proc/fs/jbd2 2>/dev/null | grep "^${LOGDEV:-none}-" | head -1 || true)
{
    echo "commit: $(git rev-parse HEAD) dirty: $(git status --porcelain | wc -l)"
    echo "disk: ${DISK:-none} journal: ${JOURNAL:-none} fs: $fs"
    echo "governors: $(sort /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | uniq -c | tr -s ' ' | tr '\n' ';')"
    echo "loadavg: $(cat /proc/loadavg)"
} | tee "$OUT/run.log"

cargo build --release -p server --bin straum-server >/dev/null 2>&1
PUBACK=$(cargo bench -p server --bench puback --no-run 2>&1 | sed -n 's/.*Executable .*(\(.*\))$/\1/p' | tail -1)
CAP=$(cargo bench -p node --bench cap --no-run 2>&1 | sed -n 's/.*Executable .*(\(.*\))$/\1/p' | tail -1)

# side <A|B> <round>: one round of every stage on one side.
side() {
    ahead=0
    if [ "$1" = B ]; then ahead=$AHEAD; fi
    for stage in steady heavy; do
        extra=""
        if [ "$stage" = heavy ]; then extra="STRAUM_BENCH_PUBLISHERS=24 STRAUM_BENCH_WINDOW=32"; fi
        echo "--- round $2" >> "$OUT/rates-$stage-$1.txt"
        # shellcheck disable=SC2086
        env STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_POSITIONS_MS=100 STRAUM_BENCH_DISK="$DISK" \
            STRAUM_BENCH_JOURNAL="$JOURNAL" STRAUM_BENCH_PREALLOCATE_BYTES=$ahead $extra \
            "$PUBACK" >> "$OUT/rates-$stage-$1.txt" 2>&1 || echo "round $2 FAILED" >> "$OUT/rates-$stage-$1.txt"
    done
    echo "--- round $2" >> "$OUT/cap-$1.txt"
    env STRAUM_BENCH_CAPS=64 STRAUM_BENCH_ROUNDS=3 STRAUM_BENCH_FAILOVERS=10 \
        STRAUM_BENCH_PREALLOCATE_BYTES=$ahead "$CAP" >> "$OUT/cap-$1.txt" 2>&1 \
        || echo "round $2 FAILED" >> "$OUT/cap-$1.txt"
    echo "round $2 side $1 done (load $(cut -d' ' -f1-3 /proc/loadavg))" | tee -a "$OUT/run.log"
}

round=1
while [ "$round" -le "$ROUNDS" ]; do
    if [ $((round % 2)) -eq 1 ]; then side A $round; side B $round; else side B $round; side A $round; fi
    round=$((round + 1))
done

for side in A B; do
    for stage in steady heavy; do
        awk -v stage="$stage-$side" -f scripts/rates.awk "$OUT/rates-$stage-$side.txt"
    done
    grep -h "^cap_stats" "$OUT/cap-$side.txt" | sed "s/^/  $side /"
done > "$OUT/summary.txt" 2>&1
rm -rf "$TMPDIR"
echo "== summary: $OUT/summary.txt" | tee -a "$OUT/run.log"
tar czf "$OUT.tar.gz" -C target/prezeroed-ab "$(basename "$OUT")"
echo "send back: $OUT.tar.gz" | tee -a "$OUT/run.log"
sha256sum "$OUT.tar.gz" | tee -a "$OUT/run.log"
