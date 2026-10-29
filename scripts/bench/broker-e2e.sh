#!/usr/bin/env bash
# End-to-end benchmark of the broker on one Linux machine, over loopback:
# message size x producers x messages per Produce, durable acks. See
# "Benchmark" in crates/straum-broker/README.md.
#
#   scripts/bench/broker-e2e.sh --out <dir> [--quick] [--keep]
#
# --out   a new or empty directory on the disk to measure; everything the
#         script writes goes there, the broker's data directories included
# --quick a minimal matrix, a smoke test of the script; not a benchmark
# --keep  keep the worktree and build under <dir>/work
#
# The broker and the load generator run on the same machine and share its
# CPUs; the results include that. The script never uses sudo, never
# installs anything and never changes a system setting. It deletes only
# <dir>/work (unless --keep) and the git worktree entry it created. It stops
# at the first failed step; its exit status is 0 only if every step
# succeeded.
set -uo pipefail

# A run is marked noisy when the 1-minute load average at its start is
# above what the benchmark itself explains: one per CPU, one per producer
# thread of the largest run (the average lags behind the previous run),
# and 2 for the broker's thread and its fsync waits. The real quietness
# check is LOAD_START_THRESHOLD before the first run.
LOAD_SLACK=2
# Load average above which the machine is not quiet before the first run,
# after the build; the script waits up to LOAD_SETTLE_SECONDS for it.
LOAD_START_THRESHOLD=0.5
LOAD_SETTLE_SECONDS=300
SAMPLE_SECONDS=10
# Disk allowance for the release build (target directory).
BUILD_BYTES=$((2 * 1024 * 1024 * 1024))
# Largest run: bodies of one run, plus metadata per message. The engine's
# examples/meta_bytes measures at most 73.7 bytes of WAL and index per
# message; 128 bounds it.
MAX_BYTES=$((1024 * 1024 * 1024))
OVERHEAD_PER_MSG=128

usage() {
    echo "usage: scripts/bench/broker-e2e.sh --out <dir> [--quick] [--keep]" >&2
    exit 2
}

OUT="" QUICK=0 KEEP=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --out) [ "$#" -ge 2 ] || usage; OUT="$2"; shift 2 ;;
        --quick) QUICK=1; shift ;;
        --keep) KEEP=1; shift ;;
        *) usage ;;
    esac
done
[ -n "$OUT" ] || usage

if [ "$QUICK" -eq 1 ]; then
    SIZES="100 1024" PRODUCERS="1 4" PER_PRODUCE="1 64" REPS=2 SECONDS_PER_RUN=1
else
    SIZES="100 1024 10240" PRODUCERS="1 4 16" PER_PRODUCE="1 64" REPS=5 SECONDS_PER_RUN=10
fi

die() {
    echo "broker-e2e: $*" >&2
    [ -n "${LOG:-}" ] && echo "STOPPED: $*" >>"$LOG"
    exit 1
}

# --- Output directory and logging -------------------------------------------

[ "$(uname -s)" = Linux ] || die "Linux only (this is $(uname -s))"
if [ -e "$OUT" ]; then
    [ -d "$OUT" ] || die "--out $OUT exists and is not a directory"
    [ -z "$(ls -A "$OUT")" ] || die "--out $OUT is not empty"
fi
mkdir -p "$OUT" || die "cannot create $OUT"
OUT="$(cd "$OUT" && pwd)"
[ "$OUT" != / ] || die "--out must not be /"
WORK="$OUT/work"
LOG="$OUT/commands.log"
ENV_MD="$OUT/env.md"
QUIET="$OUT/quiet.csv"
RESULTS="$OUT/e2e.csv"
CPU_CSV="$OUT/broker-cpu.csv"
: >"$LOG"
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
STEPS=0
SAMPLER=""
BROKER_PID=""
WORKTREE=""

# Runs one step: the command line and its output go to commands.log, then
# its exit status. A failed step stops the script.
step() {
    STEPS=$((STEPS + 1))
    printf '[%s] + %s\n' "$(date -u +%FT%TZ)" "$*" >>"$LOG"
    "$@" </dev/null >>"$LOG" 2>&1
    local status=$?
    printf '[%s] exit=%d\n' "$(date -u +%FT%TZ)" "$status" >>"$LOG"
    [ "$status" -eq 0 ] || die "step failed (exit $status): $*; see $LOG"
}

cleanup() {
    local status=$?
    [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null
    if [ -n "$BROKER_PID" ]; then
        kill -TERM "$BROKER_PID" 2>/dev/null
        wait "$BROKER_PID" 2>/dev/null
    fi
    if [ "$KEEP" -eq 0 ] && [ -d "$WORK" ]; then
        [ -n "$WORKTREE" ] && git -C "$REPO" worktree remove --force "$WORKTREE" >>"$LOG" 2>&1
        case "$WORK" in
            "$OUT"/work) rm -rf "$WORK" ;;
        esac
        git -C "$REPO" worktree prune >>"$LOG" 2>&1
        echo "cleanup: removed $WORK" >>"$LOG"
    fi
    if [ "$status" -eq 0 ]; then
        echo "broker-e2e: OK: $STEPS steps, results in $OUT" | tee -a "$LOG"
    else
        echo "broker-e2e: FAILED (exit $status): see $LOG" | tee -a "$LOG" >&2
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# --- Preconditions -----------------------------------------------------------

for tool in git findmnt lsblk df awk sort getconf nproc; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool not found; install it and rerun"
done
git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1 || die "$REPO is not a git checkout"

uring="$(cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || echo absent)"
case "$uring" in
    0 | absent) ;;
    *) die "io_uring is disabled or restricted (kernel.io_uring_disabled = $uring)" ;;
esac

if ! command -v cargo >/dev/null 2>&1 || ! command -v rustc >/dev/null 2>&1; then
    die "rustc and cargo not found. Install Rust with rustup, then rerun:
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
fi
need="$(sed -n 's/^rust-version = "\(.*\)"/\1/p' "$REPO/Cargo.toml")"
have="$(rustc --version | awk '{print $2}')"
[ -n "$need" ] || die "no rust-version in $REPO/Cargo.toml"
[ "$(printf '%s\n%s\n' "$need" "$have" | sort -V | head -1)" = "$need" ] ||
    die "rustc $have is older than rust-version $need; update with: rustup update stable"

read -r fstype src_dev majmin <<<"$(findmnt -n -o FSTYPE,SOURCE,MAJ:MIN -T "$OUT")" ||
    die "findmnt cannot resolve $OUT"
src_dev="${src_dev%%[[]*}"
case "$fstype" in
    tmpfs | ramfs | overlay | aufs | squashfs | nfs* | cifs | smb* | 9p | virtiofs | fuse* | ceph | glusterfs)
        die "$OUT is on $fstype, not a local block device filesystem" ;;
esac
case "$src_dev" in
    /dev/*) ;;
    *) die "$OUT is on $src_dev ($fstype), not a block device" ;;
esac
# sysfs knows every block device, also where /dev has no node for it (a
# container); a btrfs mount reports an anonymous device number instead.
if [ -e "/sys/dev/block/$majmin" ]; then
    blockdev="$(basename "$(readlink -f "/sys/dev/block/$majmin")")"
elif [ -b "$src_dev" ]; then
    blockdev="$(basename "$src_dev")"
else
    die "$src_dev ($OUT) is not a block device"
fi

max_run=0
for size in $SIZES; do
    run=$((MAX_BYTES + MAX_BYTES * OVERHEAD_PER_MSG / size))
    [ "$run" -gt "$max_run" ] && max_run=$run
done
planned=$((max_run + BUILD_BYTES))
avail=$(($(df -Pk "$OUT" | awk 'NR == 2 {print $4}') * 1024))
[ "$avail" -ge $((2 * planned)) ] ||
    die "not enough free space on $OUT: $avail bytes, needs twice the planned $planned"

cpus="$(nproc)"
max_producers=0
for p in $PRODUCERS; do [ "$p" -gt "$max_producers" ] && max_producers=$p; done
LOAD_THRESHOLD=$((cpus + max_producers + LOAD_SLACK))
load_now() { awk '{print $1}' /proc/loadavg; }
above() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a > b) }'; }

# --- Environment record ------------------------------------------------------

COMMIT="$(git -C "$REPO" rev-parse HEAD)"
dirty="$(git -C "$REPO" status --porcelain --untracked-files=no | wc -l)"
script_commit="$(git -C "$REPO" log -1 --format=%H -- scripts/bench/broker-e2e.sh)"
governors="$(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c | awk '{printf "%s%s x%s", sep, $2, $1; sep = ", "}')"
virt="$(systemd-detect-virt 2>/dev/null || true)"
clk_tck="$(getconf CLK_TCK)"
NOISY=0
cpu="$(awk -F: '/^model name/ {sub(/^ /, "", $2); print $2; exit}' /proc/cpuinfo)"
[ -n "$cpu" ] || cpu="$(lscpu 2>/dev/null | awk -F: '/^(Vendor ID|Model name)/ {gsub(/^ +/, "", $2); printf "%s%s", sep, $2; sep = " "}')"

{
    echo "# Environment"
    echo
    echo "- Started: $(date -u +%FT%TZ)"
    echo "- Matrix: $([ "$QUICK" -eq 1 ] && echo "quick (smoke test, not a benchmark)" || echo full)"
    echo "- Commit: \`$COMMIT\` (tracked files changed: $dirty)"
    echo "- Script: last changed in \`$script_commit\`"
    echo "- CPU: ${cpu:-unknown}"
    echo "- Cores: $cpus online; $(lscpu 2>/dev/null | awk -F: '/^(Socket|Core|Thread)/ {gsub(/^ +/, "", $2); printf "%s%s: %s", sep, $1, $2; sep = "; "}')"
    echo "- RAM: $(awk '/^MemTotal/ {print $2, $3}' /proc/meminfo)"
    echo "- Virtualization: ${virt:-not detected (systemd-detect-virt missing)}"
    echo "- Kernel: $(uname -srvm)"
    echo "- Distribution: $(
        # shellcheck source=/dev/null
        . /etc/os-release 2>/dev/null && echo "$PRETTY_NAME"
    )"
    echo "- CPU frequency governor: ${governors:-not available}"
    echo "- io_uring_disabled: $uring"
    echo "- CLK_TCK: $clk_tck"
    echo "- rustc: $(rustc --version)"
    echo "- cargo: $(cargo --version)"
    echo "- Load average before: $(cat /proc/loadavg)"
    echo
    echo "## Disk and filesystem of --out (block device $blockdev)"
    echo
    echo '```text'
    findmnt -T "$OUT" -o TARGET,SOURCE,FSTYPE,OPTIONS
    echo
    lsblk -o NAME,MODEL,SIZE,ROTA,TRAN,TYPE,SCHED,MOUNTPOINT
    echo '```'
} >"$ENV_MD"

echo "time,load1,load5,load15,mhz_min,mhz_mean,mhz_max" >"$QUIET"
(
    while :; do
        # cpufreq in kHz where the kernel has it, else /proc/cpuinfo in MHz.
        if ls /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq >/dev/null 2>&1; then
            mhz="$(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq | awk '{v = $1 / 1000; s += v; n++; if (n == 1 || v < min) min = v; if (v > max) max = v} END {printf "%.0f,%.0f,%.0f", min, s / n, max}')"
        else
            mhz="$(awk -F: '/^cpu MHz/ {v = $2 + 0; s += v; n++; if (n == 1 || v < min) min = v; if (v > max) max = v} END {if (n) printf "%.0f,%.0f,%.0f", min, s / n, max; else printf ",,"}' /proc/cpuinfo)"
        fi
        echo "$(date -u +%FT%TZ),$(awk '{print $1 "," $2 "," $3}' /proc/loadavg),$mhz"
        sleep "$SAMPLE_SECONDS"
    done
) >>"$QUIET" &
SAMPLER=$!

# --- Build -------------------------------------------------------------------

mkdir -p "$WORK"
WORKTREE="$WORK/src"
step git -C "$REPO" worktree add --detach "$WORKTREE" "$COMMIT"
{
    echo
    echo "## Build profile"
    echo
    echo '```toml'
    awk '/^\[profile\./ {on = 1; print; next} /^\[/ {on = 0} on && NF' "$WORKTREE/Cargo.toml"
    echo '```'
} >>"$ENV_MD"
step env CARGO_TARGET_DIR="$WORK/target" cargo build --manifest-path "$WORKTREE/Cargo.toml" \
    --release --locked -p straum-broker -p straum-broker-bench
BROKER="$WORK/target/release/straum-server"
BENCH="$WORK/target/release/straum-broker-bench"

# --- Broker per run ----------------------------------------------------------

# Starts a broker on a fresh data directory on the measured disk; sets
# BROKER_PID and BROKER_ADDR, and checks the io_uring driver.
start_broker() {
    local data="$WORK/data" err="$WORK/broker.err"
    rm -rf "$data"
    "$BROKER" --data-dir "$data" --listen 127.0.0.1:0 --cluster-id bench --node-id 1 \
        </dev/null >/dev/null 2>"$err" &
    BROKER_PID=$!
    for _ in $(seq 1 100); do
        BROKER_ADDR="$(sed -n 's/^straum-server: listening on //p' "$err")"
        [ -n "$BROKER_ADDR" ] && break
        kill -0 "$BROKER_PID" 2>/dev/null || break
        sleep 0.1
    done
    cat "$err" >>"$LOG"
    [ -n "$BROKER_ADDR" ] || die "the broker did not start; see $LOG"
    grep -q '^straum-server: compio driver IoUring$' "$err" ||
        die "the broker does not use io_uring: $(grep 'compio driver' "$err")"
}

stop_broker() {
    kill -TERM "$BROKER_PID" 2>/dev/null
    wait "$BROKER_PID"
    local status=$?
    echo "broker $BROKER_PID stopped, exit $status" >>"$LOG"
    BROKER_PID=""
    rm -rf "$WORK/data"
    [ "$status" -eq 0 ] || die "the broker exited with $status"
}

# User plus system CPU of a process in nanoseconds (fields 14 and 15 of
# /proc/<pid>/stat, clock ticks).
cpu_ns() {
    awk -v tck="$clk_tck" '{sub(/.*\) /, ""); printf "%.0f", ($12 + $13) * 1e9 / tck}' "/proc/$1/stat"
}

# --- Runs --------------------------------------------------------------------

waited=0
while above "$(load_now)" "$LOAD_START_THRESHOLD" && [ "$waited" -lt "$LOAD_SETTLE_SECONDS" ]; do
    sleep 10
    waited=$((waited + 10))
done
if above "$(load_now)" "$LOAD_START_THRESHOLD"; then
    NOISY=1
    echo "broker-e2e: warning: load average $(load_now) above $LOAD_START_THRESHOLD before the first run" >&2
fi
{
    echo "- Load average at the first run: $(cat /proc/loadavg) (after waiting $waited s)"
    echo "- Sizes: $SIZES; producers: $PRODUCERS; messages per Produce: $PER_PRODUCE; repetitions: $REPS"
    echo "- Per run: $SECONDS_PER_RUN s or $MAX_BYTES bytes of bodies, at least 100 requests; a fresh broker and data directory per run"
    echo "- Noisy: 1-minute load average above $LOAD_THRESHOLD (CPUs + largest producer count + $LOAD_SLACK) at a run start, or above $LOAD_START_THRESHOLD before the first"
} >>"$ENV_MD"

echo "label,msg_size,producers,per_produce,rep,broker_cpu_ns,broker_cpu_ns_per_msg" >"$CPU_CSV"
HEADER=1
NOISY_RUNS=0
total=0
for _ in $SIZES; do for _ in $PRODUCERS; do for _ in $PER_PRODUCE; do total=$((total + REPS)); done; done; done
done_runs=0
for rep in $(seq 1 "$REPS"); do
    for size in $SIZES; do
        for producers in $PRODUCERS; do
            for per in $PER_PRODUCE; do
                done_runs=$((done_runs + 1))
                echo "broker-e2e: [$done_runs/$total] size $size producers $producers per $per rep $rep" >&2
                load="$(load_now)"
                if above "$load" "$LOAD_THRESHOLD"; then
                    NOISY=1
                    NOISY_RUNS=$((NOISY_RUNS + 1))
                    echo "broker-e2e: warning: load average $load above $LOAD_THRESHOLD" >&2
                fi
                start_broker
                header=--no-header
                [ "$HEADER" -eq 1 ] && header="" && HEADER=0
                before="$(cpu_ns "$BROKER_PID")"
                # shellcheck disable=SC2086 # $header is empty or one flag
                step "$BENCH" --addr "$BROKER_ADDR" --out "$RESULTS" $header --label e2e \
                    --sizes "$size" --producers "$producers" --per-produce "$per" \
                    --reps 1 --rep-offset $((rep - 1)) --seconds "$SECONDS_PER_RUN" --max-bytes "$MAX_BYTES"
                after="$(cpu_ns "$BROKER_PID")"
                messages="$(tail -1 "$RESULTS" | awk -F, '{print $7}')"
                echo "e2e,$size,$producers,$per,$rep,$((after - before)),$(awk -v c=$((after - before)) -v m="$messages" 'BEGIN {printf "%.0f", (m > 0 ? c / m : 0)}')" >>"$CPU_CSV"
                stop_broker
            done
        done
    done
done

# --- Medians -----------------------------------------------------------------

# Median of every numeric column per (label, msg_size, producers,
# per_produce), and the spread of msgs/s: (max - min) / median.
medians() {
    awk -F, -v keys=4 -v rate="msgs_per_s" '
        function med(list,   all, a, m, n, i, j, t) {
            m = split(list, all, " ")
            n = 0
            for (i = 1; i <= m; i++) if (all[i] ~ /^-?[0-9.]+$/) a[++n] = all[i] + 0
            if (n == 0) return "NaN"
            for (i = 2; i <= n; i++) for (j = i; j > 1 && a[j - 1] > a[j]; j--) { t = a[j]; a[j] = a[j - 1]; a[j - 1] = t }
            return sprintf("%.1f", n % 2 ? a[(n + 1) / 2] : (a[n / 2] + a[n / 2 + 1]) / 2)
        }
        NR == 1 {
            for (i = 1; i <= NF; i++) { name[i] = $i; if ($i == rate) rc = i; if ($i == "rep") repc = i }
            nf = NF
            line = ""
            for (i = 1; i <= nf; i++) if (i != repc) line = line (line == "" ? "" : ",") name[i]
            print line ",runs,min_msgs_per_s,max_msgs_per_s,spread_pct"
            next
        }
        {
            k = ""
            for (i = 1; i <= keys; i++) k = k "," $i
            if (!(k in runs)) order[++n] = k
            runs[k]++
            for (i = 1; i <= nf; i++) vals[k, i] = vals[k, i] " " $i
        }
        END {
            for (o = 1; o <= n; o++) {
                k = order[o]
                line = ""
                for (i = 1; i <= nf; i++) {
                    if (i == repc) continue
                    split(vals[k, i], first, " ")
                    v = (i <= keys) ? first[1] : med(vals[k, i])
                    line = line (line == "" ? "" : ",") v
                }
                split(vals[k, rc], r, " "); lo = r[1]; hi = r[1]
                for (j in r) { if (r[j] + 0 < lo + 0) lo = r[j]; if (r[j] + 0 > hi + 0) hi = r[j] }
                m = med(vals[k, rc]) + 0
                printf "%s,%d,%.1f,%.1f,%.1f\n", line, runs[k], lo, hi, (m > 0 ? (hi - lo) / m * 100 : 0)
            }
        }' "$1" >"$2"
}
step medians "$RESULTS" "$OUT/e2e-median.csv"

rows=$(($(wc -l <"$RESULTS") - 1))
kill "$SAMPLER" 2>/dev/null
SAMPLER=""
{
    echo
    echo "## Run"
    echo
    echo "- Finished: $(date -u +%FT%TZ)"
    echo "- Load average after: $(cat /proc/loadavg)"
    echo "- Rows: $rows runs (e2e.csv), medians in e2e-median.csv, broker CPU per run in broker-cpu.csv"
    echo "- Load and CPU frequency every $SAMPLE_SECONDS s: quiet.csv"
    if [ "$NOISY" -eq 1 ]; then
        echo "- **Noisy run**: load average above the thresholds ($NOISY_RUNS run starts above $LOAD_THRESHOLD); see quiet.csv"
    else
        echo "- Quiet: load average stayed within the thresholds"
    fi
} >>"$ENV_MD"
echo "broker-e2e: $rows runs, noisy: $([ "$NOISY" -eq 1 ] && echo yes || echo no)" | tee -a "$LOG"
