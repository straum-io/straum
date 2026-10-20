#!/usr/bin/env sh
# Everything Straum needs checked and measured on a real Linux host, in one
# run, packed into one archive to send back. Run it from a full clone (not
# --depth 1: the full mode builds an older commit) on the bare-metal host,
# as a normal user, with nothing else running:
#
#   scripts/linux-host.sh            # quick: about an hour
#   scripts/linux-host.sh full       # quick, then the D-124 run (hours)
#
# Stages, each written to target/linux-host/<time>/<stage>.txt; a failure is
# recorded and the run goes on, except a failed build:
#
#   host        the machine as it is: kernel, CPUs, governor, memory, disk,
#               toolchain, the commit, whether it runs in a VM or container
#   build       the tests and the release server
#   tests       cargo test --workspace --tests (Linux-only paths included),
#               and the crate boundaries
#   sim-*       the simulation sweeps of docs/testing.md, in release
#   bench-*     the micro benchmarks (divan) and the group-commit benchmark
#   rates-*     what sizes the step 7c fence (straum#23, D-153 A), 3 rounds
#               each through three real server processes:
#                 steady   positions per second, apply lag, commit latency
#                 heavy    the same at four times the load
#                 events   a planned handoff and a killed sequencer: the
#                          hold across a slot change, and the lag meanwhile
#                 churn-N  N clients connecting and disconnecting: the
#                          session-removal rate, under the steady load
#   d124        full only: scripts/linux-run.sh, the D-124 run
#
# At the end: target/linux-host/straum-linux-<time>.tar.gz, and its SHA-256.
# Send that file back. Nothing here needs root, changes the system, or sends
# anything anywhere.
set -eu

MODE=${1:-quick}
case $MODE in quick | full) ;; *) echo "usage: $0 [quick|full]" >&2; exit 2 ;; esac
cd "$(dirname "$0")/.."
if [ "$(uname -s)" != Linux ]; then
    echo "$0: this is for a Linux host; on macOS use scripts/linux.sh for tests only" >&2
    exit 2
fi
STAMP=$(date +%Y%m%d-%H%M%S)
OUT=target/linux-host/$STAMP
mkdir -p "$OUT"
LOG=$OUT/run.log
ROUNDS=3

note() { echo "$*" | tee -a "$LOG"; }

# run <stage> <command...>: the command's output into <stage>.txt, its
# outcome and duration into run.log.
run() {
    stage=$1
    shift
    note "== $stage: $*"
    started=$(date +%s)
    if "$@" > "$OUT/$stage.txt" 2>&1; then status=ok; else status="FAILED (exit $?)"; fi
    note "   $status in $(($(date +%s) - started)) s"
}

note "straum linux-host run, mode $MODE, into $OUT"

{
    echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit: $(git rev-parse HEAD)"
    echo "dirty: $(git status --porcelain | wc -l) files"
    uname -a
    cat /etc/os-release 2>/dev/null | grep -E '^(PRETTY_NAME|VERSION_ID)=' || true
    echo "virtualization: $(systemd-detect-virt 2>/dev/null || echo unknown)"
    [ -f /.dockerenv ] && echo "container: /.dockerenv present"
    echo "cpus: $(nproc)"
    lscpu 2>/dev/null || true
    echo "governors:"
    cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c || echo "  none readable"
    echo "cgroup cpu.max: $(cat /sys/fs/cgroup/cpu.max 2>/dev/null || echo none)"
    echo "loadavg: $(cat /proc/loadavg)"
    free -m 2>/dev/null || true
    echo "disk under target/:"
    df -h target 2>/dev/null || true
    findmnt -T target -o SOURCE,FSTYPE,OPTIONS 2>/dev/null || true
    lsblk -d -o NAME,ROTA,SIZE,MODEL 2>/dev/null || true
    for s in net.core.somaxconn net.ipv4.tcp_max_syn_backlog vm.dirty_ratio vm.dirty_background_ratio; do
        echo "$s = $(sysctl -n "$s" 2>/dev/null || echo -)"
    done
    rustc -V
    cargo -V
} > "$OUT/host.txt" 2>&1
note "== host: $OUT/host.txt"
load=$(cut -d' ' -f1 /proc/loadavg)
if awk -v l="$load" 'BEGIN { exit !(l > 0.5) }'; then
    note "   warning: load average $load before the run; the rates may not be accepted"
fi

run build sh -c 'cargo test --workspace --all-targets --no-run && cargo build --release -p server --bin straum-server'
if [ "$status" != ok ]; then
    note "the build failed: see $OUT/build.txt"
    exit 1
fi

run tests sh -c 'cargo test --workspace --tests && scripts/check-boundaries.sh'

for sim in takeover:20000 cluster:1000 apply:1000 gateway:1000 connect:1000; do
    name=${sim%%:*}
    run "sim-$name" env STRAUM_SIM_SEEDS="${sim#*:}" cargo test --release -p sim --test "$name" -- --nocapture
done

for bench in apply:apply auth:verify codec:codec domain:topic gateway:gateway \
    storage:record storage:scan transport:read_buffer storage:commit; do
    run "bench-${bench#*:}" cargo bench -p "${bench%%:*}" --bench "${bench#*:}"
done

PUBACK=$(cargo bench -p server --bench puback --no-run 2>&1 | sed -n 's/.*Executable .*(\(.*\))$/\1/p' | tail -1)
# rates <label> <settings...>: ROUNDS runs of puback with the positions
# sampled every 100 ms; fails if any round did.
rates() {
    shift
    failed=0
    for round in $(seq 1 "$ROUNDS"); do
        echo "--- round $round"
        if ! env STRAUM_BENCH_POSITIONS_MS=100 "$@" "$PUBACK"; then
            echo "round $round FAILED"
            failed=1
        fi
    done
    return "$failed"
}
run rates-steady rates steady STRAUM_BENCH_SECONDS=60
run rates-heavy rates heavy STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_PUBLISHERS=24 STRAUM_BENCH_WINDOW=32
run rates-events rates events STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_EVENTS=handoff,kill
for clients in 8 64; do
    run "rates-churn-$clients" rates churn STRAUM_BENCH_SECONDS=30 STRAUM_BENCH_CHURN="$clients"
done

# The lines the rates are read from, every stage and round together.
grep -E '^(--- round|round .* FAILED|puback |positions |apply_lag |churn |event |diag )' "$OUT"/rates-*.txt > "$OUT/rates-summary.txt" || true
note "== rates-summary: $OUT/rates-summary.txt"

if [ "$MODE" = full ]; then
    before=$(ls -d target/linux-run/* 2>/dev/null | tail -1 || true)
    run d124 scripts/linux-run.sh 5
    after=$(ls -d target/linux-run/* 2>/dev/null | tail -1 || true)
    if [ -n "$after" ] && [ "$after" != "$before" ]; then
        cp -r "$after" "$OUT/d124-run"
    fi
fi

ARCHIVE=target/linux-host/straum-linux-$STAMP.tar.gz
tar czf "$ARCHIVE" -C target/linux-host "$STAMP"
note "== done: $(grep -c FAILED "$LOG" || true) stage(s) failed"
note "send back: $ARCHIVE"
sha256sum "$ARCHIVE" | tee -a "$LOG"
