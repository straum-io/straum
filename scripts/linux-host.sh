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
#   cd straum && scripts/linux-host.sh           # a few hours
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
#   cap-64      node/benches/cap.rs at max_uncommitted 64 (D-155 A): commit
#               latency, uncommitted work, RSS, failover and catch-up, 5
#               rounds and 50 failovers, the three logs on the measured disk
#   cap-64-disks  the same, each node's log on its own disk: only with
#               STRAUM_HOST_DATA_DIRS=/a,/b,/c on three separate physical
#               disks; refused when two share one
#
# The MQTT stages (the `puback` rates, the step 7c fence sizing and the
# D-124 run) went with MQTT (D-188 A).
# At the end: target/linux-host/straum-linux-<time>.tar.gz and its SHA-256.
# Send that file back. It holds the host's name (uname -a) and nothing else
# about the machine beyond what is listed above.
set -eu

cd "$(dirname "$0")/.."
if [ "$(uname -s)" != Linux ]; then
    echo "$0: this is for a Linux host; on macOS use scripts/linux.sh for tests only" >&2
    exit 2
fi
STAMP=$(date +%Y%m%d-%H%M%S)
OUT=target/linux-host/$STAMP
mkdir -p "$OUT"
LOG=$OUT/run.log

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

note "straum linux-host run into $OUT"

# D-157 A: the physical disk under TMPDIR, as /proc/diskstats names it (the
# kernel counts flushes on the whole disk, never on a partition), and the
# ext4 journal of the filesystem there, under /proc/fs/jbd2 (named after
# the partition or dm volume), recorded with the run. A volume over several
# disks counts the first.
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

for sim in takeover:20000 cluster:1000 journal:1000; do
    name=${sim%%:*}
    run "sim-$name" env STRAUM_SIM_SEEDS="${sim#*:}" cargo test --release -p sim --test "$name" -- --nocapture
done

for bench in storage:record storage:scan transport:read_buffer storage:commit; do
    run "bench-${bench#*:}" cargo bench -p "${bench%%:*}" --bench "${bench#*:}"
done

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

rm -rf "$TMPDIR"
ARCHIVE=target/linux-host/straum-linux-$STAMP.tar.gz
tar czf "$ARCHIVE" -C target/linux-host "$STAMP"
note "== done: $(grep -c FAILED "$LOG" || true) stage(s) failed"
note "send back: $ARCHIVE"
sha256sum "$ARCHIVE" | tee -a "$LOG"
