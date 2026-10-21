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
#                 events       10 planned handoffs and 10 killed sequencers in
#                              each 6-minute round, 30 minutes in all (D-154 A):
#                              the hold across a slot change as a
#                              distribution (D-130 A, D-134 A)
#                 churn-100    sessions removed at 100/s under the steady load
#                 churn-1000   the same at 1 000/s
#                 saturation   64 unpaced churners: the most the cluster takes
#   cap-64      node/benches/cap.rs at max_uncommitted 64 (D-155 A): commit
#               latency, uncommitted work, RSS, failover and catch-up, 5
#               rounds and 50 failovers, the three logs on the measured disk
#   cap-64-disks  the same, each node's log on its own disk: only with
#               STRAUM_HOST_DATA_DIRS=/a,/b,/c on three separate physical
#               disks; refused when two share one
#   d124        full only: scripts/linux-run.sh, the D-124 run
#
# summary-rates.txt has each quantity's median, min, max and spread across
# rounds, and the hold's p50, p90, max and share within 1 s (scripts/rates.awk);
# fence-size.txt the step 7c W and tombstone queue from it (scripts/fence.awk,
# D-154 A), preliminary until the host passes D-129.
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

# D-157 A: the physical disk under TMPDIR, as /proc/diskstats names it (the
# kernel counts flushes on the whole disk, never on a partition), and the
# ext4 journal of the filesystem there, under /proc/fs/jbd2 (named after
# the partition or dm volume), for puback to count flushes and journal
# commits over each measured window. A volume over several disks counts
# the first.
LOGDEV=$(lsblk -no KNAME "$(findmnt -n -o SOURCE -T "$TMPDIR" 2>/dev/null | sed 's/\[.*//')" 2>/dev/null | head -1 || true)
DISK=$(lsblk -rs -no NAME,TYPE "/dev/${LOGDEV:-none}" 2>/dev/null | awk '$2 == "disk" { print $1; exit }' || true)
JOURNAL=$(ls /proc/fs/jbd2 2>/dev/null | grep "^${LOGDEV:-none}-" | head -1 || true)
note "   disk counted: ${DISK:-none}, journal: ${JOURNAL:-none}"

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
    # A volatile write cache is what makes a flush cost anything.
    for c in /sys/block/*/queue/write_cache; do
        [ -r "$c" ] && echo "write_cache $(echo "$c" | cut -d/ -f4): $(cat "$c")"
    done
    echo "disk counted: ${DISK:-none}, journal: ${JOURNAL:-none}"
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
            if ! env STRAUM_BENCH_POSITIONS_MS=100 STRAUM_BENCH_DISK="$DISK" STRAUM_BENCH_JOURNAL="$JOURNAL" "$@" "$PUBACK"; then
                echo "round $round FAILED"
                failed=1
            fi
        done
        return "$failed"
    }
    events=$(printf 'handoff,kill,%.0s' $(seq 1 10))
    run rates-steady rates steady STRAUM_BENCH_SECONDS=60
    run rates-heavy rates heavy STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_PUBLISHERS=24 STRAUM_BENCH_WINDOW=32
    run rates-events rates events STRAUM_BENCH_SECONDS=360 STRAUM_BENCH_EVENTS="${events%,}"
    run rates-churn-100 rates churn STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_CHURN=8 STRAUM_BENCH_CHURN_RATE=100
    run rates-churn-1000 rates churn STRAUM_BENCH_SECONDS=60 STRAUM_BENCH_CHURN=32 STRAUM_BENCH_CHURN_RATE=1000
    run rates-saturation rates saturation STRAUM_BENCH_SECONDS=30 STRAUM_BENCH_CHURN=64
    for file in "$OUT"/rates-*.txt; do
        stage=${file##*/rates-}
        awk -v stage="${stage%.txt}" -f scripts/rates.awk "$file"
    done > "$OUT/summary-rates.txt" 2>&1
    note "== summary-rates: $OUT/summary-rates.txt"
    awk -f scripts/fence.awk "$OUT/summary-rates.txt" > "$OUT/fence-size.txt" 2>&1 || true
    note "== fence-size (preliminary): $OUT/fence-size.txt"
fi

cap() { env STRAUM_BENCH_CAPS=64 STRAUM_BENCH_ROUNDS=5 STRAUM_BENCH_FAILOVERS=50 "$@" cargo bench -p node --bench cap; }
run cap-64 cap
# The physical disks under a directory, one per line: every TYPE=disk
# device below its mount (through LVM, device mapper or RAID); nothing when
# that cannot be resolved (tmpfs, a network or overlay mount).
disks_of() {
    source=$(findmnt -n -o SOURCE -T "$1" 2>/dev/null) || return 0
    source=${source%%\[*}
    lsblk -rs -no NAME,TYPE "$source" 2>/dev/null | awk '$2 == "disk" { print $1 }' | sort -u
}
if [ -n "${STRAUM_HOST_DATA_DIRS:-}" ]; then
    # Refused unless there are three directories, each on a disk that was
    # resolved, and no disk under two of them.
    all=""
    refused=""
    count=0
    for d in $(echo "$STRAUM_HOST_DATA_DIRS" | tr ',' ' '); do
        count=$((count + 1))
        mkdir -p "$d"
        found=$(disks_of "$d")
        note "   data dir $d on disk(s): $(echo ${found:-unresolved} | tr "\n" " ")"
        if [ -z "$found" ]; then refused="$d on no resolved disk"; fi
        all="$all
$found"
    done
    if [ "$count" -ne 3 ]; then refused="$count directories, not 3"; fi
    shared=$(echo "$all" | grep -v '^$' | sort | uniq -d | tr '\n' ' ')
    if [ -n "$shared" ]; then refused="disk(s) $shared under more than one directory"; fi
    if [ -n "$refused" ]; then
        note "   cap-64-disks refused: $refused; the control needs three directories on three separate physical disks"
    else
        run cap-64-disks cap STRAUM_BENCH_DATA_DIRS="$STRAUM_HOST_DATA_DIRS"
    fi
else
    note "   cap-64-disks skipped: STRAUM_HOST_DATA_DIRS not set (the separate-disk control, D-155 A)"
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
