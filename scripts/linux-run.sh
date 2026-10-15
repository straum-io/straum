#!/usr/bin/env sh
# The accepted Linux run of D-124, in D-127 A's order. Run from the
# repository on the bare-metal Linux host itself (not in a container):
#
#   scripts/linux-run.sh [rounds]        # 5 by default
#
# 0. The host as it is: CPUs, governor, load, cgroup limits — written down,
#    and a warning if anything else is running.
# 1. Noise: this tree against itself, alternating. Its spread is what any
#    difference below has to beat; if it is near the effect sought, find
#    steadier conditions, or more rounds — never read small differences of
#    medians. D-129 A gates it: every quantity's two copies within
#    3 % and each spread below 3 % (scripts/noise.awk), or the run stops.
# 2. Overhead: 071db00 (no metrics) against this tree (D-112's metrics),
#    alternating (scripts/overhead.sh).
# 3. Scraping: this tree, scraper off against scraper on (every 1 s),
#    alternating, the same load.
# 4. Diagnostics: one longer run with the scraper on; per node, where the
#    delay forms — durable here, the quorum, the outbox, withheld reads —
#    and takeovers apart from successions and fencing. A planned handoff and
#    a killed sequencer are injected; each reports, apart, the longest wait
#    for a PUBACK — its own node's clients and the others' — and the run
#    counts late, retransmitted, unacknowledged and duplicated (D-128).
# 5. Catch-up by read size (D-131 A): a killed sequencer at 64 KiB, 512 KiB
#    and 4 MiB (the most a frame allows); per size, the restarted node's
#    lag every second under load, its recovery after the load stops, and the
#    running nodes' commit latency and PUBACK. Flow control stays off.
#
# Everything lands in target/linux-run/<time>/. Not a universal benchmark
# of Straum: one host, one workload (D-127).
set -eu

ROUNDS=${1:-5}
cd "$(dirname "$0")/.."
OUT=target/linux-run/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"

echo "0. host -> $OUT/host.txt"
{
    uname -a
    echo "cpus: $(nproc)"
    lscpu 2>/dev/null || true
    echo "governors:"
    cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c || true
    echo "cgroup cpu.max: $(cat /sys/fs/cgroup/cpu.max 2>/dev/null || echo none)"
    echo "loadavg: $(cat /proc/loadavg)"
    free -m 2>/dev/null || true
} > "$OUT/host.txt" 2>&1
load=$(cut -d' ' -f1 /proc/loadavg)
if awk -v l="$load" 'BEGIN { exit !(l > 0.5) }'; then
    echo "warning: load average $load before the run; numbers may not be accepted" | tee -a "$OUT/host.txt"
fi

[ -d .bench/base ] || git worktree add --detach .bench/base 071db00

echo "1. noise: this tree against itself"
STRAUM_OVERHEAD_OUT=$OUT/noise scripts/overhead.sh . . "$ROUNDS" | tee "$OUT/noise.txt"
# D-129 A: the host is accepted only if every quantity's two copies differ
# by at most 3 % and each copy's spread is below 3 %; otherwise stop here.
if awk -v gate=0.03 -f scripts/noise.awk "$OUT/noise/rounds" > "$OUT/noise-verdict.txt"; then
    cat "$OUT/noise-verdict.txt"
else
    cat "$OUT/noise-verdict.txt"
    echo "the host's noise is too high to read a 3 % effect: steadier conditions, or more rounds, then again (D-129)"
    exit 3
fi
if [ -n "${STRAUM_STOP_AFTER_NOISE:-}" ]; then
    echo "stopped after the noise run: read noise.txt first (D-128)"
    exit 0
fi

echo "2. overhead: 071db00 against this tree"
STRAUM_OVERHEAD_OUT=$OUT/overhead scripts/overhead.sh .bench/base . "$ROUNDS" | tee "$OUT/overhead.txt"
# Each difference against the smallest effect the noise run lets this host
# detect, per quantity (D-129): within it, not proof of zero overhead.
awk -v noisefile="$OUT/noise-verdict.txt" -f scripts/noise.awk "$OUT/overhead/rounds" | tee "$OUT/overhead-verdict.txt"
# D-134 A: failover p90 spread above 10 % between rounds, in either build,
# means 50 failovers were too few: again with 200, share, p50 and p90 shown.
if awk '$1 == "failover_p90_ms" { for (i = 5; i <= 6; i++) { s = $i; sub(/%$/, "", s); if (s + 0 > 10) found = 1 } } END { exit !found }' "$OUT/overhead-verdict.txt"; then
    echo "failover p90 spread above 10 %: again with 200 failovers (D-134)"
    STRAUM_OVERHEAD_FAILOVERS=200 STRAUM_OVERHEAD_OUT=$OUT/overhead-200 scripts/overhead.sh .bench/base . "$ROUNDS" | tee "$OUT/overhead-200.txt"
    awk -v noisefile="$OUT/noise-verdict.txt" -f scripts/noise.awk "$OUT/overhead-200/rounds" | tee "$OUT/overhead-200-verdict.txt"
fi

echo "3. scraping: off against on, every 1 s"
PUBACK=$(cargo bench -p server --bench puback --no-run 2>&1 | sed -n 's/.*Executable .*(\(.*\))$/\1/p' | tail -1)
: > "$OUT/scrape.txt"
for round in $(seq 1 "$ROUNDS"); do
    for every in $( [ $((round % 2)) -eq 1 ] && echo "0 1000" || echo "1000 0"); do
        STRAUM_BENCH_SCRAPE_MS=$every "$PUBACK" | grep '^puback' | sed "s/^/$round /" | tee -a "$OUT/scrape.txt"
    done
done
for every in 0 1000; do
    for key in pubacks_per_s p50_ms p99_ms cpu_ms_per_1k; do
        grep "scrape_ms=$every " "$OUT/scrape.txt" | tr ' ' '\n' | sed -n "s/^$key=//p" | sort -n \
            | awk -v e="$every" -v k="$key" '{ v[NR] = $1 } END { if (NR) print "scrape_ms=" e, k, "median", v[int((NR + 1) / 2)], "min", v[1], "max", v[NR] }'
    done
done | tee -a "$OUT/scrape.txt"

echo "4. diagnostics: 60 s with the scraper on, a handoff at 20 s and a killed sequencer at 40 s"
STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_SCRAPE_MS=1000 STRAUM_BENCH_EVENTS=handoff,kill "$PUBACK" | tee "$OUT/diagnostics.txt"

echo "5. catch-up by read size (D-131 A, D-133 A): a killed sequencer, 60 s, 3 rounds of 64 KiB, 512 KiB, 4 MiB, the order turned each round"
for round in 1 2 3; do
    case $round in 1) order="65536 524288 4194304" ;; 2) order="524288 4194304 65536" ;; *) order="4194304 65536 524288" ;; esac
    for bytes in $order; do
        STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_SCRAPE_MS=1000 STRAUM_BENCH_EVENTS=kill STRAUM_BENCH_READ_BYTES=$bytes "$PUBACK" \
            | sed "s/^/rb=$bytes round=$round /" | tee -a "$OUT/catchup.txt"
    done
done
awk -f scripts/catchup.awk "$OUT/catchup.txt" | tee "$OUT/catchup-medians.txt"

echo "done: $OUT"
