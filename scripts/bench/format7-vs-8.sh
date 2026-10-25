#!/usr/bin/env bash
# The storage engine's WAL format 7 against its compressed-block format, on
# one Linux machine, from a fresh clone. See "Benchmark: format 7 against
# format 8" in crates/straum-store/README.md.
#
#   scripts/bench/format7-vs-8.sh --out <dir> [--quick] [--keep]
#
# --out   a new or empty directory on the disk to measure; everything the
#         script writes goes there
# --quick a minimal matrix, a smoke test of the script; not a benchmark
# --keep  keep the worktrees, builds and datasets under <dir>/work
#
# "format7" is commit FORMAT7_COMMIT (WAL version 7, objects stored
# uncompressed). "format8" is the commit this script runs from (compressed
# blocks, introduced as WAL version 8; the header gained the dictionary
# training threshold in version 9). Both are built in git worktrees under
# <dir>/work with the release profile of their own Cargo.toml, which must
# be identical. The benchmark binary is the one of the format8 commit,
# built against each engine; against format 7 it is built without its
# `blocks` feature.
#
# The script never uses sudo, never installs anything and never changes a
# system setting. It deletes only <dir>/work (unless --keep) and the git
# worktree entries it created. It stops at the first failed step; its exit
# status is 0 only if every step succeeded.
set -uo pipefail

FORMAT7_COMMIT=7d298a243662516ae404e85c124fdb8693de0694
# 1-minute load average above which a run is marked noisy. The benchmark
# itself raises it: tasks waiting in fsync and io_uring workers count. In
# the straum-test container smoke test (4 cores) it reached 1.82.
LOAD_THRESHOLD=3
# Load average above which the machine is not quiet before the first run,
# after the builds and downloads; the script waits up to LOAD_SETTLE_SECONDS
# for it to fall below.
LOAD_START_THRESHOLD=0.5
LOAD_SETTLE_SECONDS=300
SAMPLE_SECONDS=10
# Disk allowance for the two release builds (target directories). The
# smoke test in the straum-test container (aarch64) used 372 MB for both.
BUILD_BYTES=$((2 * 1024 * 1024 * 1024))

# Loghub (https://github.com/logpai/loghub), Zenodo record 8196385, v8.
ZENODO=https://zenodo.org/records/8196385/files
ARCHIVES="OpenStack.tar.gz:87c98c5ed03262e05cdb7a6f3717033df76d88fda0f7d2db23bd9fa4200f1879:5394691
SSH.tar.gz:296610a35773f58b7c295e5407fc5ccdf48beb3c544fc14a6309addf904a2712:4596666
BGL.zip:d67fd82a711aea0157a9b83175892c6ee60e384a2ddf5bc51f39118453816da8:57489019"
LOGS="OpenStack/openstack_abnormal.log:7ab718fbb2f2b804893955cecf37ad9533cfa0efaf5b03009474d5bb019b6af4:5436122
OpenStack/openstack_normal1.log:4e4d47347bdae198056bb3b0a8a755e1cb0100d6c6a30bbd4058684234769199:15499595
OpenStack/openstack_normal2.log:3c51741dcea1fb0731d1f86588b12587707e7abea01e725ee16c9441b8b68db7:40506365
SSH/SSH.log:89eee9e36c182722f9fadc6e5617a4818ffc66109e7e222b8f9e8b78fc3aa2f3:73417506
BGL/BGL.log:666130b15ef44eb32fd02bd053e6c6e007c37696b5e7e8b9d8e45b729876a5d2:743185031"

usage() {
    echo "usage: scripts/bench/format7-vs-8.sh --out <dir> [--quick] [--keep]" >&2
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
    SIZES="100 1024" BATCHES="1 64" REPS=2
    MIN_BATCHES=20 MAX_BATCHES=20
    READS=200 READ_SECONDS=1
    DATASETS="ssh"
else
    SIZES="100 1024 10240" BATCHES="1 8 64 512" REPS=5
    MIN_BATCHES=100 MAX_BATCHES=1000
    READS=10000 READ_SECONDS=5
    DATASETS="openstack ssh bgl"
fi
MAX_BYTES=$((1024 * 1024 * 1024))
MODES="sequential parallel inline"
SEED=1

die() {
    echo "format7-vs-8: $*" >&2
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
WRITES="$OUT/writes.csv"
READS_CSV="$OUT/reads.csv"
: >"$LOG"
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
STEPS=0
SAMPLER=""
WORKTREES=()

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
    if [ "$KEEP" -eq 0 ] && [ -d "$WORK" ]; then
        for wt in ${WORKTREES[@]+"${WORKTREES[@]}"}; do
            git -C "$REPO" worktree remove --force "$wt" >>"$LOG" 2>&1
        done
        case "$WORK" in
            "$OUT"/work) rm -rf "$WORK" ;;
        esac
        git -C "$REPO" worktree prune >>"$LOG" 2>&1
        echo "cleanup: removed $WORK" >>"$LOG"
    fi
    if [ "$status" -eq 0 ]; then
        echo "format7-vs-8: OK: $STEPS steps, results in $OUT" | tee -a "$LOG"
    else
        echo "format7-vs-8: FAILED (exit $status): see $LOG" | tee -a "$LOG" >&2
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# --- Preconditions -----------------------------------------------------------

for tool in git curl tar unzip sha256sum findmnt lsblk df awk sort getconf nproc; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool not found; install it and rerun"
done
git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1 || die "$REPO is not a git checkout"
git -C "$REPO" cat-file -e "$FORMAT7_COMMIT^{commit}" 2>/dev/null ||
    die "commit $FORMAT7_COMMIT not in this clone; run: git -C $REPO fetch origin import-straum-store"

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
    for batch in $BATCHES; do
        per=$((size * batch))
        n=$(((MAX_BYTES + per - 1) / per))
        [ "$n" -gt "$MAX_BATCHES" ] && n=$MAX_BATCHES
        [ "$n" -lt "$MIN_BATCHES" ] && n=$MIN_BATCHES
        run=$((n * batch * (size + 512)))
        [ "$run" -gt "$max_run" ] && max_run=$run
    done
done
data=0
while IFS=: read -r _ _ bytes; do data=$((data + bytes)); done <<<"$ARCHIVES"
while IFS=: read -r _ _ bytes; do data=$((data + bytes)); done <<<"$LOGS"
planned=$((data + max_run + BUILD_BYTES))
avail=$(($(df -Pk "$OUT" | awk 'NR == 2 {print $4}') * 1024))
[ "$avail" -ge $((2 * planned)) ] ||
    die "not enough free space on $OUT: $avail bytes, needs twice the planned $planned"

load_now() { awk '{print $1}' /proc/loadavg; }
above() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a > b) }'; }

# --- Environment record ------------------------------------------------------

FORMAT8_COMMIT="$(git -C "$REPO" rev-parse HEAD)"
dirty="$(git -C "$REPO" status --porcelain --untracked-files=no | wc -l)"
script_commit="$(git -C "$REPO" log -1 --format=%H -- scripts/bench/format7-vs-8.sh)"
governors="$(cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c | awk '{printf "%s%s x%s", sep, $2, $1; sep = ", "}')"
virt="$(systemd-detect-virt 2>/dev/null || true)"
clk_tck="$(getconf CLK_TCK)"
load_before="$(cat /proc/loadavg)"
NOISY=0

{
    echo "# Environment"
    echo
    echo "- Started: $(date -u +%FT%TZ)"
    echo "- Matrix: $([ "$QUICK" -eq 1 ] && echo "quick (smoke test, not a benchmark)" || echo full)"
    echo "- Commits: format7 \`$FORMAT7_COMMIT\`; format8 \`$FORMAT8_COMMIT\` (tracked files changed: $dirty)"
    echo "- Script: last changed in \`$script_commit\`, run from \`$FORMAT8_COMMIT\`"
    cpu="$(awk -F: '/^model name/ {sub(/^ /, "", $2); print $2; exit}' /proc/cpuinfo)"
    [ -n "$cpu" ] || cpu="$(lscpu 2>/dev/null | awk -F: '/^(Vendor ID|Model name)/ {gsub(/^ +/, "", $2); printf "%s%s", sep, $2; sep = " "}')"
    echo "- CPU: ${cpu:-unknown}"
    echo "- Cores: $(nproc) online; $(lscpu 2>/dev/null | awk -F: '/^(Socket|Core|Thread)/ {gsub(/^ +/, "", $2); printf "%s%s: %s", sep, $1, $2; sep = "; "}')"
    echo "- RAM: $(awk '/^MemTotal/ {print $2, $3}' /proc/meminfo)"
    echo "- Virtualization: ${virt:-not detected (systemd-detect-virt missing)}"
    echo "- Kernel: $(uname -srvm)"
    # shellcheck source=/dev/null
    echo "- Distribution: $(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME")"
    echo "- CPU frequency governor: ${governors:-not available}"
    echo "- io_uring_disabled: $uring"
    echo "- CLK_TCK: $clk_tck"
    echo "- rustc: $(rustc --version)"
    echo "- cargo: $(cargo --version)"
    echo "- Load average before: $load_before"
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

# --- Builds ------------------------------------------------------------------

mkdir -p "$WORK"
SRC7="$WORK/src-format7" SRC8="$WORK/src-format8"
step git -C "$REPO" worktree add --detach "$SRC7" "$FORMAT7_COMMIT"
WORKTREES+=("$SRC7")
step git -C "$REPO" worktree add --detach "$SRC8" "$FORMAT8_COMMIT"
WORKTREES+=("$SRC8")
step git -C "$SRC7" checkout "$FORMAT8_COMMIT" -- crates/straum-store-bench
profile() { awk '/^\[profile\./ {on = 1; print; next} /^\[/ {on = 0} on && NF' "$1/Cargo.toml"; }
[ "$(profile "$SRC7")" = "$(profile "$SRC8")" ] ||
    die "the release profiles of the two commits differ"
{
    echo
    echo "## Build profile (identical in both commits)"
    echo
    echo '```toml'
    profile "$SRC8"
    echo '```'
} >>"$ENV_MD"
step env CARGO_TARGET_DIR="$WORK/target-format7" cargo build --manifest-path "$SRC7/Cargo.toml" \
    --release --locked -p straum-store-bench --no-default-features
step env CARGO_TARGET_DIR="$WORK/target-format8" cargo build --manifest-path "$SRC8/Cargo.toml" \
    --release --locked -p straum-store-bench
BIN7="$WORK/target-format7/release/straum-store-bench"
BIN8="$WORK/target-format8/release/straum-store-bench"
for bin in "$BIN7" "$BIN8"; do
    driver="$("$bin" --dir "$WORK/probe" --mode raw --sizes 8 --batches 1 --reps 1 --min-batches 1 --max-batches 1 2>&1 >/dev/null | sed -n 's/^compio driver: //p')"
    echo "$bin: compio driver $driver" >>"$LOG"
    [ "$driver" = IoUring ] || die "$bin does not use io_uring (driver: ${driver:-none})"
done
rm -rf "$WORK/probe"
echo "- compio driver: IoUring (both builds)" >>"$ENV_MD"

# --- Datasets ----------------------------------------------------------------

# concat <out> <file>...: OpenStack is three files, read in this order.
concat() {
    local out="$1"
    shift
    cat "$@" >"$out"
}
DATA="$WORK/data"
mkdir -p "$WORK/downloads" "$DATA"
PAYLOADS=""
if [ -n "$DATASETS" ]; then
    while IFS=: read -r name sum _; do
        case "$name" in
            OpenStack*) dataset=openstack ;;
            SSH*) dataset=ssh ;;
            *) dataset=bgl ;;
        esac
        case " $DATASETS " in *" $dataset "*) ;; *) continue ;; esac
        step curl -sSL --fail --retry 3 -o "$WORK/downloads/$name" "$ZENODO/$name?download=1"
        echo "$sum  $WORK/downloads/$name" >"$WORK/downloads/$name.sha256"
        step sha256sum -c "$WORK/downloads/$name.sha256"
        dir="$DATA/${name%%.*}"
        mkdir -p "$dir"
        case "$name" in
            *.zip) step unzip -q "$WORK/downloads/$name" -d "$dir" ;;
            *) step tar -xzf "$WORK/downloads/$name" -C "$dir" ;;
        esac
    done <<<"$ARCHIVES"
    while IFS=: read -r file sum _; do
        [ -e "$DATA/$file" ] || continue
        echo "$sum  $DATA/$file" >"$DATA/$(basename "$file").sha256"
        step sha256sum -c "$DATA/$(basename "$file").sha256"
    done <<<"$LOGS"
    for dataset in $DATASETS; do
        case "$dataset" in
            openstack)
                step concat "$DATA/openstack.log" "$DATA/OpenStack/openstack_abnormal.log" \
                    "$DATA/OpenStack/openstack_normal1.log" "$DATA/OpenStack/openstack_normal2.log"
                PAYLOADS="$PAYLOADS file:$DATA/openstack.log" ;;
            ssh) PAYLOADS="$PAYLOADS file:$DATA/SSH/SSH.log" ;;
            bgl) PAYLOADS="$PAYLOADS file:$DATA/BGL/BGL.log" ;;
        esac
    done
fi
PAYLOADS="$PAYLOADS random"
{
    echo "- Payloads: ${PAYLOADS# } (random: seed $SEED); dataset SHA-256 values checked"
    echo "- Sizes: $SIZES; batches: $BATCHES; write modes: $MODES; repetitions: $REPS"
    echo "- Per run: up to $MAX_BATCHES batches or $MAX_BYTES payload bytes, at least $MIN_BATCHES batches"
    echo "- Reads per workload: up to $READS or $READ_SECONDS s; format8 with the default block cache and with it off"
    echo "- Noisy: 1-minute load average above $LOAD_THRESHOLD at a run start, or above $LOAD_START_THRESHOLD before the first"
} >>"$ENV_MD"

# --- Runs --------------------------------------------------------------------

# The builds and downloads load the machine; let the load average settle.
waited=0
while above "$(load_now)" "$LOAD_START_THRESHOLD" && [ "$waited" -lt "$LOAD_SETTLE_SECONDS" ]; do
    sleep 10
    waited=$((waited + 10))
done
if above "$(load_now)" "$LOAD_START_THRESHOLD"; then
    NOISY=1
    echo "format7-vs-8: warning: load average $(load_now) above $LOAD_START_THRESHOLD before the first run" >&2
fi
echo "- Load average at the first run: $(cat /proc/loadavg) (after waiting $waited s)" >>"$ENV_MD"

BENCH_DIR="$WORK/bench"
HEADER=1
NOISY_RUNS=0
bench() {
    local label="$1" bin="$2" mode="$3" payload="$4" size="$5" batch="$6" rep="$7" cache="$8"
    local load
    load="$(load_now)"
    if above "$load" "$LOAD_THRESHOLD"; then
        NOISY=1
        NOISY_RUNS=$((NOISY_RUNS + 1))
        echo "format7-vs-8: warning: load average $load above $LOAD_THRESHOLD ($label $mode $size $batch rep $rep)" >&2
    fi
    local header=--no-header
    [ "$HEADER" -eq 1 ] && header="" && HEADER=0
    # shellcheck disable=SC2086 # $header is empty or one flag
    step "$bin" --dir "$BENCH_DIR" --out "$WRITES" --reads-out "$READS_CSV" $header \
        --label "$label" --mode "$mode" --payload "$payload" --seed "$SEED" \
        --sizes "$size" --batches "$batch" --reps 1 --rep-offset $((rep - 1)) \
        --min-batches "$MIN_BATCHES" --max-batches "$MAX_BATCHES" --max-bytes "$MAX_BYTES" \
        --reads "$READS" --read-seconds "$READ_SECONDS" --read-cache "$cache" --clk-tck "$clk_tck"
}

total=0
for _ in $PAYLOADS; do for _ in $SIZES; do for _ in $BATCHES; do total=$((total + REPS)); done; done; done
done_configs=0
for payload in $PAYLOADS; do
    for size in $SIZES; do
        for batch in $BATCHES; do
            for rep in $(seq 1 "$REPS"); do
                done_configs=$((done_configs + 1))
                echo "format7-vs-8: [$done_configs/$total] ${payload##*/} size $size batch $batch rep $rep" >&2
                bench baseline "$BIN8" raw "$payload" "$size" "$batch" "$rep" default
                bench baseline "$BIN8" hashcopy "$payload" "$size" "$batch" "$rep" default
                for mode in $MODES; do
                    # Alternate which format runs first.
                    if [ $((rep % 2)) -eq 1 ]; then
                        bench format7 "$BIN7" "$mode" "$payload" "$size" "$batch" "$rep" default
                        bench format8 "$BIN8" "$mode" "$payload" "$size" "$batch" "$rep" default,off
                    else
                        bench format8 "$BIN8" "$mode" "$payload" "$size" "$batch" "$rep" default,off
                        bench format7 "$BIN7" "$mode" "$payload" "$size" "$batch" "$rep" default
                    fi
                done
            done
        done
    done
done

# --- Medians -----------------------------------------------------------------

# Median of every numeric column per key, and the spread of msgs_per_s
# (min, max, (max - min) / median). $1 = number of key columns before
# `rep`, $2 = input, $3 = the rate column, $4 = output. The key also holds
# the `workload` and `cache` columns of reads.csv.
medians() {
    awk -F, -v OFS=, -v keys="$1" -v rate="$3" '
        # Median of the numeric values in a space-separated list ("NaN"
        # and empty values left out); "NaN" if there are none.
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
            for (i = 1; i <= nf; i++) if (i != repc && name[i] !~ /^(rep_min_msgs_per_s|rep_max_msgs_per_s|spread_pct)$/) line = line (line == "" ? "" : ",") name[i]
            print line ",runs,min_msgs_per_s,max_msgs_per_s,spread_pct"
            next
        }
        {
            k = ""
            for (i = 1; i <= nf; i++) if (i <= keys || name[i] ~ /^(workload|cache)$/) k = k "," $i
            if (!(k in runs)) order[++n] = k
            runs[k]++
            for (i = 1; i <= nf; i++) vals[k, i] = vals[k, i] " " $i
        }
        END {
            for (o = 1; o <= n; o++) {
                k = order[o]
                line = ""
                for (i = 1; i <= nf; i++) {
                    if (i == repc || name[i] ~ /^(rep_min_msgs_per_s|rep_max_msgs_per_s|spread_pct)$/) continue
                    split(vals[k, i], first, " ")
                    v = (i <= keys || name[i] ~ /^(workload|cache)$/) ? first[1] : med(vals[k, i])
                    line = line (line == "" ? "" : ",") v
                }
                split(vals[k, rc], r, " "); lo = r[1]; hi = r[1]
                for (j in r) { if (r[j] + 0 < lo + 0) lo = r[j]; if (r[j] + 0 > hi + 0) hi = r[j] }
                m = med(vals[k, rc]) + 0
                printf "%s,%d,%.1f,%.1f,%.1f\n", line, runs[k], lo, hi, (m > 0 ? (hi - lo) / m * 100 : 0)
            }
        }' "$2" >"$4"
}
step medians 5 "$WRITES" msgs_per_s "$OUT/writes-median.csv"
step medians 5 "$READS_CSV" msgs_per_s "$OUT/reads-median.csv"

write_rows=$(($(wc -l <"$WRITES") - 1))
read_rows=$(($(wc -l <"$READS_CSV") - 1))
kill "$SAMPLER" 2>/dev/null
SAMPLER=""
{
    echo
    echo "## Run"
    echo
    echo "- Finished: $(date -u +%FT%TZ)"
    echo "- Load average after: $(cat /proc/loadavg)"
    echo "- Rows: $write_rows write runs (writes.csv), $read_rows read workloads (reads.csv); medians in writes-median.csv and reads-median.csv"
    echo "- Load and CPU frequency every $SAMPLE_SECONDS s: quiet.csv"
    if [ "$NOISY" -eq 1 ]; then
        echo "- **Noisy run**: load average above the thresholds ($NOISY_RUNS run starts above $LOAD_THRESHOLD); see quiet.csv"
    else
        echo "- Quiet: load average stayed within the thresholds"
    fi
} >>"$ENV_MD"
echo "format7-vs-8: $write_rows write runs, $read_rows read workloads, noisy: $([ "$NOISY" -eq 1 ] && echo yes || echo no)" | tee -a "$LOG"
