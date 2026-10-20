#!/usr/bin/env sh
# Everything Straum needs checked and measured on a real Linux host, in one
# run, packed into one archive to send back.
#
# Prerequisites (Debian or Ubuntu shown):
#   sudo apt install build-essential git curl      # a C compiler for ring
#   curl https://sh.rustup.rs -sSf | sh             # Rust 1.85 or newer
#   git clone https://github.com/straum-io/straum   # a full clone, not --depth 1
# A bare-metal host, quiet (nothing else running), its disk the one being
# measured: the run refuses a target/ on tmpfs. Network access only for
# cargo to fetch crates. No root; nothing is changed on the system and
# nothing is sent anywhere.
#
#   cd straum && scripts/linux-host.sh           # quick: a few hours
#   scripts/linux-host.sh full                   # quick, then the D-124 run
#
# Stages, each written to target/linux-host/<time>/<stage>.txt, with its
# outcome, duration and load average in run.log. A failure is recorded and
# the run goes on, except a failed build or a tmpfs disk.
#
#   host        the machine: kernel, CPUs, governor, memory, the disk the
#               logs go to, toolchain, the commit, VM or container
#   build       the tests and the release server
#   tests       cargo test --workspace --tests (Linux-only paths included),
#               and the crate boundaries
#   sim-*       the simulation sweeps of docs/testing.md, in release
#   bench-*     the micro benchmarks (divan) and the group-commit benchmark
#   rates-*     what sizes the step 7c fence (straum#23, D-153 A), through
#               three real server processes, 5 rounds each:
#                 steady       positions per second, apply lag, PUBACK latency
#                 heavy        the same at four times the load
#                 events       10 planned handoffs and 10 killed sequencers a
#                              round: the hold across a slot change as a
#                              distribution (D-130 A, D-134 A)
#                 churn-100    sessions removed at 100/s under the steady load
#                 churn-1000   the same at 1 000/s
#                 saturation   64 unpaced churners: the most the cluster takes
#   d124        full only: scripts/linux-run.sh, the D-124 run
#
# summary-rates.txt has each quantity's median, min, max and spread across
# rounds, and the hold's p50, p90, max and share within 1 s (scripts/rates.awk).
# At the end: target/linux-host/straum-linux-<time>.tar.gz and its SHA-256.
# Send that file back. It holds the host's name (uname -a) and nothing else
# about the machine beyond what is listed above.
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
ROUNDS=5

# The benchmarks write their logs under the system temp directory: here,
# on the disk being measured, never tmpfs, where a sync costs nothing.
TMPDIR=$PWD/target/linux-host/tmp
export TMPDIR
mkdir -p "$TMPDIR"
fs=$(findmnt -n -o FSTYPE -T "$TMPDIR" 2>/dev/null || stat -f -c %T "$TMPDIR")
if [ "$fs" = tmpfs ] || [ "$fs" = ramfs ]; then
    echo "$0: $TMPDIR is on $fs; the commit and rate figures would be meaningless. Clone onto a real disk." >&2
    exit 2
fi

note() { echo "$*" | tee -a "$LOG"; }

# run <stage> <command...>: the command's output into <stage>.txt, its
# outcome, duration and the load average before it into run.log.
run() {
    stage=$1
    shift
    note "== $stage: $* (load $(cut -d' ' -f1-3 /proc/loadavg))"
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
    grep -E '^(PRETTY_NAME|VERSION_ID)=' /etc/os-release 2>/dev/null || true
    echo "virtualization: $(systemd-detect-virt 2>/dev/null || echo unknown)"
    if [ -f /.dockerenv ]; then echo "container: /.dockerenv present"; fi
    echo "cpus: $(nproc)"
    lscpu 2>/dev/null || true
    echo "governors:"
    cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c || echo "  none readable"
    echo "cgroup cpu.max: $(cat /sys/fs/cgroup/cpu.max 2>/dev/null || echo none)"
    echo "loadavg: $(cat /proc/loadavg)"
    free -m 2>/dev/null || true
    echo "the disk the logs go to ($TMPDIR):"
    df -h "$TMPDIR" 2>/dev/null || true
    findmnt -T "$TMPDIR" -o SOURCE,FSTYPE,OPTIONS 2>/dev/null || true
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
if [ -z "$PUBACK" ] || [ ! -x "$PUBACK" ]; then
    note "the puback benchmark did not build: the rates are skipped"
else
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
    events=$(printf 'handoff,kill,%.0s' $(seq 1 10))
    run rates-steady rates steady STRAUM_BENCH_SECONDS=60
    run rates-heavy rates heavy STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_PUBLISHERS=24 STRAUM_BENCH_WINDOW=32
    run rates-events rates events STRAUM_BENCH_SECONDS=300 STRAUM_BENCH_EVENTS="${events%,}"
    run rates-churn-100 rates churn STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_CHURN=8 STRAUM_BENCH_CHURN_RATE=100
    run rates-churn-1000 rates churn STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_CHURN=32 STRAUM_BENCH_CHURN_RATE=1000
    run rates-saturation rates saturation STRAUM_BENCH_SECONDS=30 STRAUM_BENCH_CHURN=64
    for file in "$OUT"/rates-*.txt; do
        stage=${file##*/rates-}
        awk -v stage="${stage%.txt}" -f scripts/rates.awk "$file"
    done > "$OUT/summary-rates.txt" 2>&1
    note "== summary-rates: $OUT/summary-rates.txt"
fi

if [ "$MODE" = full ]; then
    before=$(ls -d target/linux-run/* 2>/dev/null | tail -1 || true)
    run d124 scripts/linux-run.sh 5
    after=$(ls -d target/linux-run/* 2>/dev/null | tail -1 || true)
    if [ -n "$after" ] && [ "$after" != "$before" ]; then
        cp -r "$after" "$OUT/d124-run"
    fi
fi

rm -rf "$TMPDIR"
ARCHIVE=target/linux-host/straum-linux-$STAMP.tar.gz
tar czf "$ARCHIVE" -C target/linux-host "$STAMP"
note "== done: $(grep -c FAILED "$LOG" || true) stage(s) failed"
note "send back: $ARCHIVE"
sha256sum "$ARCHIVE" | tee -a "$LOG"
