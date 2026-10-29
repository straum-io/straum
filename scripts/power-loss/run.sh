#!/usr/bin/env bash
# Power-loss harness: the store and the broker on a LazyFS mount, a
# simulated power loss at a random moment, then recovery checked against
# docs/guarantees.md. Linux only. Installs nothing, never uses sudo and
# changes no system setting; everything it creates, mounts and deletes is
# under --out. Usage and what each scenario checks: README.md next to this
# script.
#
#   scripts/power-loss/run.sh --out DIR [--quick] [--iterations N] [--seed N]
#       [--scenarios store,broker,compaction,dictionary]
#       [--modes sequential,parallel,inline] [--lazyfs DIR] [--keep]
#
# Exit status: 0 only if every iteration of every scenario passed; 1 if any
# failed; 2 if a precondition is missing.
set -uo pipefail

LAZYFS_PIN=045a0b3a1126725e693934e29d3ba15e08cc39ec # LazyFS 0.3.1
LAZYFS_TAG=0.3.1
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

OUT=""
ITERATIONS=200
QUICK=0
SEED=24301
SCENARIOS=store,broker,compaction,dictionary
MODES=sequential,parallel,inline
LAZYFS="${LAZYFS_DIR:-$HOME/lazyfs}"
KEEP=0
while [ $# -gt 0 ]; do
    case "$1" in
        --out) OUT="$2"; shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --quick) QUICK=1; shift ;;
        --seed) SEED="$2"; shift 2 ;;
        --scenarios) SCENARIOS="$2"; shift 2 ;;
        --modes) MODES="$2"; shift 2 ;;
        --lazyfs) LAZYFS="$2"; shift 2 ;;
        --keep) KEEP=1; shift ;;
        -h | --help) sed -n '2,15p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1 (see --help)" >&2; exit 2 ;;
    esac
done
[ "$QUICK" -eq 1 ] && ITERATIONS=5
if [ -z "$OUT" ]; then
    echo "--out DIR is required: every file of the run, the LazyFS root and the mounts go there" >&2
    exit 2
fi
case "$ITERATIONS$SEED" in *[!0-9]*) echo "--iterations and --seed take numbers" >&2; exit 2 ;; esac
[ "$ITERATIONS" -gt 0 ] || { echo "--iterations must be at least 1" >&2; exit 2; }

# ---------------------------------------------------------------- preconditions

missing=0
need() { # need <check command> <message>
    if ! eval "$1" >/dev/null 2>&1; then
        echo "MISSING: $2" >&2
        missing=1
    fi
}
need '[ "$(uname -s)" = Linux ]' "Linux (LazyFS is a FUSE file system; macOS and containers without /dev/fuse cannot run it)"
need '[ -c /dev/fuse ] && [ -r /dev/fuse ] && [ -w /dev/fuse ]' "/dev/fuse readable and writable by this user (package fuse3; the fuse kernel module)"
need 'command -v fusermount3' "fusermount3 (package fuse3)"
need 'command -v cargo' "cargo (https://rustup.rs)"
for tool in findmnt mkfifo timeout awk git; do
    need "command -v $tool" "$tool (packages util-linux, coreutils, gawk, git)"
done
need '[ -x "$LAZYFS/lazyfs/build/lazyfs" ]' "LazyFS built at $LAZYFS/lazyfs/build/lazyfs"
need '[ "$(git -C "$LAZYFS" rev-parse HEAD)" = "$LAZYFS_PIN" ]' "LazyFS checkout at $LAZYFS on commit $LAZYFS_PIN (tag $LAZYFS_TAG)"
if [ "$missing" -eq 1 ]; then
    cat >&2 <<EOF

Install on Debian or Ubuntu (run these yourself; this script never does):

    sudo apt install fuse3 libfuse3-dev g++ cmake git pkg-config
    git clone https://github.com/dsrhaslab/lazyfs.git $LAZYFS
    git -C $LAZYFS checkout $LAZYFS_PIN    # tag $LAZYFS_TAG
    (cd $LAZYFS/libs/libpcache && ./build.sh)
    (cd $LAZYFS/lazyfs && ./build.sh)

Another location: --lazyfs DIR or LAZYFS_DIR=DIR. No /etc/fuse.conf change
is needed: the harness mounts without allow_other, as the user running it.
EOF
    exit 2
fi

mkdir -p "$OUT" || exit 2
OUT="$(cd "$OUT" && pwd)"
LAZYFS_BIN="$LAZYFS/lazyfs/build/lazyfs"
# The store keeps every segment file open: raise the soft limit to the hard
# one for this process only.
ulimit -n "$(ulimit -Hn)" 2>/dev/null || true

echo "building straum-power-loss and straum-server (release)"
if ! (cd "$ROOT" && cargo build --release -p straum-power-loss -p straum-broker) >"$OUT/build.log" 2>&1; then
    echo "build failed: $OUT/build.log" >&2
    exit 2
fi
BIN="$ROOT/target/release/straum-power-loss"
BROKER="$ROOT/target/release/straum-server"

# ---------------------------------------------------------------- environment

{
    echo "date_utc: $(date -u +%FT%TZ)"
    echo "command: $0 --out $OUT --iterations $ITERATIONS --seed $SEED --scenarios $SCENARIOS --modes $MODES"
    # STRAUM_COMMIT: for a tree whose git metadata is not reachable (a
    # worktree mounted into a container).
    if commit="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null)"; then
        echo "straum_commit: $commit ($(git -C "$ROOT" status --porcelain | wc -l | tr -d ' ') uncommitted changes)"
    else
        echo "straum_commit: ${STRAUM_COMMIT:-unknown} (uncommitted changes: unknown)"
    fi
    echo "binaries_sha256: $(sha256sum "$BIN" "$BROKER" 2>/dev/null | awk '{printf "%s %s; ", $1, $2}')"
    echo "lazyfs: $LAZYFS_TAG, commit $(git -C "$LAZYFS" rev-parse HEAD), $LAZYFS_BIN"
    echo "lazyfs_options: single-threaded (-s), cache simple 1gb, apply_eviction=false"
    echo "fuse: $(fusermount3 --version 2>&1 | head -1); libfuse3 $(pkg-config --modversion fuse3 2>/dev/null || echo unknown)"
    echo "kernel: $(uname -srvm)"
    echo "os: $(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME")"
    echo "cpu: $(lscpu 2>/dev/null | sed -n 's/^Model name: *//p' | head -1) x $(nproc)"
    echo "memory: $(awk '/MemTotal/ {print $2, $3}' /proc/meminfo)"
    echo "fs_under_lazyfs: $(findmnt -n -o SOURCE,FSTYPE,OPTIONS --target "$OUT")"
    echo "rustc: $(rustc -V)"
    echo "open_files_limit: $(ulimit -n)"
} >"$OUT/env.txt"
cat "$OUT/env.txt"

# ---------------------------------------------------------------- LazyFS control

LAZYFS_PID=""
MOUNTED=""

# mount_lazyfs <run dir> <log file>
mount_lazyfs() {
    local run="$1" log="$2"
    cat >"$run/lazyfs.toml" <<EOF
[faults]
fifo_path="$run/faults.fifo"
fifo_path_completed="$run/done.fifo"
[cache]
apply_eviction=false
[cache.simple]
custom_size="1gb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile="$log"
EOF
    [ -p "$run/done.fifo" ] || mkfifo "$run/done.fifo"
    # LazyFS opens the notifier FIFO for writing and blocks until a reader
    # exists: hold it open read-write on fd 7 for the whole mount.
    exec 7<>"$run/done.fifo"
    "$LAZYFS_BIN" "$run/mnt" --config-path "$run/lazyfs.toml" \
        -o modules=subdir -o "subdir=$run/root" -s -f >>"$log" 2>&1 &
    LAZYFS_PID=$!
    MOUNTED="$run/mnt"
    for _ in $(seq 100); do
        findmnt -n "$run/mnt" >/dev/null 2>&1 && [ -p "$run/faults.fifo" ] && return 0
        kill -0 "$LAZYFS_PID" 2>/dev/null || break
        sleep 0.1
    done
    echo "LazyFS did not mount $run/mnt (log: $log)" >&2
    return 1
}

# wait_gone <pid> <tenths of a second>: 0 if the process exited in time.
wait_gone() {
    for _ in $(seq "$2"); do
        kill -0 "$1" 2>/dev/null || return 0
        sleep 0.1
    done
    return 1
}

unmount_lazyfs() {
    [ -n "$MOUNTED" ] || return 0
    fusermount3 -u "$MOUNTED" 2>/dev/null || fusermount3 -uz "$MOUNTED" 2>/dev/null
    if ! wait_gone "$LAZYFS_PID" 100; then
        kill -KILL "$LAZYFS_PID" 2>/dev/null
        fusermount3 -uz "$MOUNTED" 2>/dev/null
    fi
    wait "$LAZYFS_PID" 2>/dev/null
    exec 7<&-
    MOUNTED=""
}

WRITERS=""
cleanup() {
    # shellcheck disable=SC2086 # a list of pids
    [ -n "$WRITERS" ] && kill -KILL $WRITERS 2>/dev/null
    unmount_lazyfs
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# wait_line <file> <pattern> <seconds> <pid>: the pattern appeared in the
# file while the process was alive.
wait_line() {
    for _ in $(seq $(($3 * 10))); do
        grep -q "$2" "$1" 2>/dev/null && return 0
        kill -0 "$4" 2>/dev/null || { grep -q "$2" "$1" 2>/dev/null; return; }
        sleep 0.1
    done
    return 1
}

# inject <run> <kind> <command>: the power loss. Sets OUTCOME.
inject() {
    local run="$1" kind="$2" cmd="$3" line
    # shellcheck disable=SC2086
    if [ "$kind" = clear ]; then
        # Freeze the writers so nothing runs between the loss of the cache
        # and their end, as in a power cut.
        kill -STOP $WRITERS 2>/dev/null
        printf '%s\n' "$cmd" >"$run/faults.fifo"
        if read -r -t 60 -u 7 line && [ "$line" = "finished::clear-cache" ]; then
            OUTCOME=triggered
        else
            OUTCOME=no-confirmation
        fi
        kill -KILL $WRITERS 2>/dev/null
        wait $WRITERS 2>/dev/null
        WRITERS=""
        unmount_lazyfs
        return
    fi
    # crash and torn: LazyFS kills itself at the matching operation and its
    # cache, the unsynced data, is gone.
    [ -n "$cmd" ] && printf '%s\n' "$cmd" >"$run/faults.fifo"
    if wait_gone "$LAZYFS_PID" "$4"; then
        OUTCOME=triggered
    else
        # The operation did not come: a power loss now, by killing LazyFS.
        OUTCOME=fallback
        kill -KILL "$LAZYFS_PID" 2>/dev/null
    fi
    kill -KILL $WRITERS 2>/dev/null
    wait $WRITERS 2>/dev/null
    WRITERS=""
    unmount_lazyfs
}

# ---------------------------------------------------------------- scenarios

FAILED=0
: >"$OUT/iterations.log"
: >"$OUT/summary.rows"

# verify <scenario> <mode> <run> <dir for the log>: prints the RESULT line.
verify() {
    local scenario="$1" mode="$2" run="$3" dir="$4"
    if [ "$scenario" = broker ]; then
        timeout 3600 "$BIN" broker-verify --broker "$BROKER" --data-dir "$run/mnt/data" \
            --mode "$mode" --logs "$run" --stderr "$dir/verify-broker.log" >"$dir/verify.log" 2>&1
    else
        timeout 3600 "$BIN" store-verify --dir "$run/mnt/store" --mode "$mode" \
            --logs "$run" >"$dir/verify.log" 2>&1
    fi
    local s=$?
    grep '^RESULT' "$dir/verify.log" || echo "RESULT FAIL verifier exited $s without a result"
    return "$s"
}

# write_phase <scenario> <mode> <run> <iter dir> <delay ms> <seed> <kind> <cmd> <arm_at> <i>
# Runs in this shell, not a subshell: it changes WRITERS and the mount.
# On a harness error it sets ERR and returns 1.
write_phase() {
    local scenario="$1" mode="$2" run="$3" it="$4" delay="$5" wseed="$6" kind="$7" cmd="$8" arm="$9" i="${10}"
    local ready
    case "$scenario" in
        store)
            "$BIN" store-write --dir "$run/mnt/store" --mode "$mode" --seed "$wseed" \
                >"$it/writer.log" 2>"$it/writer.err" &
            WRITERS="$!"; ready="$it/writer.log"
            ;;
        compaction)
            "$BIN" store-write --dir "$run/mnt/store" --mode "$mode" --seed "$wseed" \
                --compact-every 1 --arm-at-compaction "$arm" --fifo "$run/faults.fifo" \
                --fault "$cmd" >"$it/writer.log" 2>"$it/writer.err" &
            WRITERS="$!"; ready="$it/writer.log"
            ;;
        broker)
            mkdir -p "$run/mnt/data"
            "$BROKER" --data-dir "$run/mnt/data" --listen 127.0.0.1:0 --cluster-id powerloss \
                --node-id 1 --store-write-mode "$mode" --store-segment-max-bytes 65536 \
                >/dev/null 2>"$it/broker.log" &
            WRITERS="$!"
            if ! wait_line "$it/broker.log" 'listening on' 120 "$WRITERS"; then
                ERR="broker did not start (see $it)"; return 1
            fi
            # Recovery ran in the verifier's broker: nothing is left to do.
            if ! grep -q 'store recovery: 0 action(s)' "$it/broker.log"; then
                ERR="second broker start changed the store again (see $it/broker.log)"; return 1
            fi
            local addr
            addr="$(sed -n 's/^straum-server: listening on //p' "$it/broker.log")"
            "$BIN" broker-produce --addr "$addr" --base $((i * 2000000)) --seed "$wseed" \
                >"$it/producer.log" 2>"$it/producer.err" &
            WRITERS="$WRITERS $!"; ready="$it/producer.log"
            ;;
    esac
    if ! wait_line "$ready" '^READY END' 300 "${WRITERS##* }"; then
        ERR="writer did not get ready (see $it)"; return 1
    fi
    local pid
    for pid in $WRITERS; do
        kill -0 "$pid" 2>/dev/null || { ERR="a writer exited before the fault (see $it)"; return 1; }
    done
    if [ "$scenario" = compaction ]; then
        # The writer arms the fault itself, just before a compaction.
        inject "$run" "$kind" "" 600
    else
        sleep "$(awk -v d="$delay" 'BEGIN {printf "%.3f", d / 1000}')"
        inject "$run" "$kind" "$cmd" 100
    fi
}

run_scenario() {
    local scenario="$1" mode="$2"
    local name="$scenario-$mode" run="$OUT/$scenario-$mode"
    if [ "$scenario" = dictionary ]; then
        if grep -q dict_train_bytes "$ROOT/crates/straum-store/src/store.rs"; then
            echo "$name: FAIL the store trains dictionaries but this harness does not drive it yet (README.md, Dictionary)"
            echo "$name 0 0 1 - - - - - - harness-missing" >>"$OUT/summary.rows"
            FAILED=1
        else
            echo "$name: not applicable: this store has no dictionary training"
            echo "$name 0 0 0 - - - - - - not-applicable" >>"$OUT/summary.rows"
        fi
        return
    fi
    local store_root="$run/root/store"
    [ "$scenario" = broker ] && store_root="$run/root/data/store"
    if [ -e "$run" ]; then
        echo "$run exists: use a new --out" >&2
        FAILED=1
        return
    fi
    mkdir -p "$run/root" "$run/mnt" "$run/initial"
    if ! "$BIN" plan --scenario "$scenario" --seed "$SEED" --iterations "$ITERATIONS" \
        --store-root "$store_root" >"$run/plan.out" ||
        [ "$(grep -vc '^RESULT' "$run/plan.out")" -ne "$ITERATIONS" ]; then
        echo "$name: plan failed ($run/plan.out)"
        FAILED=1
        return
    fi
    grep -v '^RESULT' "$run/plan.out" >"$run/plan.txt"

    local pass=0 fail=0 clear=0 crash=0 torn=0 fallback=0 incompaction=0 result="" status=ok
    mount_lazyfs "$run" "$run/initial/lazyfs.log" || { FAILED=1; return; }
    if ! result="$(verify "$scenario" "$mode" "$run" "$run/initial")"; then
        echo "$name initial verify: $result"
        status=failed
    fi
    while [ "$status" = ok ] && read -r -u 3 i delay wseed kind cmd arm; do
        local it
        it="$run/iter-$(printf '%06d' "$i")"
        mkdir -p "$it"
        if [ "${#cmd}" -ge 250 ]; then
            echo "fault command longer than LazyFS reads (255 bytes): use a shorter --out" >&2
            status=failed; FAILED=1; break
        fi
        local where=""
        ERR=""
        if ! write_phase "$scenario" "$mode" "$run" "$it" "$delay" "$wseed" "$kind" "$cmd" "$arm" "$i"; then
            # shellcheck disable=SC2086
            [ -n "$WRITERS" ] && kill -KILL $WRITERS 2>/dev/null
            WRITERS=""
            unmount_lazyfs
            OUTCOME=harness-error
        fi
        case "$kind" in clear) clear=$((clear + 1)) ;; crash) crash=$((crash + 1)) ;; torn) torn=$((torn + 1)) ;; esac
        [ "$OUTCOME" = fallback ] && fallback=$((fallback + 1))
        if [ "$scenario" = compaction ]; then
            where="outside-compaction"
            grep ' END$' "$it/writer.log" | tail -1 | grep -q '^COMPACT-START' && where="in-compaction"
            [ "$where" = in-compaction ] && incompaction=$((incompaction + 1))
        fi
        if [ -n "$ERR" ]; then
            result="RESULT FAIL $ERR"
        else
            # The files as the power loss left them, before recovery changes
            # them: the reproduction if this iteration fails.
            rm -rf -- "${run:?}/pre-recovery"
            cp -a "$run/root" "$run/pre-recovery"
            mount_lazyfs "$run" "$it/lazyfs.log" || { result="RESULT FAIL remount"; }
            [ -n "$MOUNTED" ] && result="$(verify "$scenario" "$mode" "$run" "$it")"
        fi
        local verdict=PASS
        case "$result" in "RESULT ok"*) pass=$((pass + 1)) ;; *) verdict=FAIL; fail=$((fail + 1)); status=failed ;; esac
        printf '%s %s iter=%s fault=%s outcome=%s delay_ms=%s seed=%s %s%s %s cmd=%s\n' \
            "$verdict" "$name" "$i" "$kind" "$OUTCOME" "$delay" "$wseed" \
            "${where:+where=}" "$where" "${result#RESULT }" "$cmd" | tee -a "$OUT/iterations.log"
    done 3<"$run/plan.txt"
    unmount_lazyfs

    # Recovery actions seen over all verifications of this scenario.
    local actions
    actions="$(cat "$run"/*/verify.log 2>/dev/null | awk '/^RECOVERY/ {split($2, a, /[ {]/); n[a[1]]++}
        END {for (k in n) printf "%s%s:%d", (s++ ? "," : ""), k, n[k]}')"
    local acked
    acked="$(grep -h '^RESULT ok' "$run"/*/verify.log 2>/dev/null | tail -1 | sed -n 's/.*acked=\([0-9]*\).*/\1/p')"
    echo "$name $ITERATIONS $pass $fail $clear $crash $torn $fallback $incompaction ${acked:-0} ${actions:--}" >>"$OUT/summary.rows"
    if [ "$fail" -gt 0 ] || [ "$status" != ok ] || [ "$pass" -ne "$ITERATIONS" ]; then
        FAILED=1
        echo "$name: FAILED, data kept in $run (pre-recovery/: the files after the failing fault)"
    elif [ "$KEEP" -eq 0 ]; then
        rm -rf -- "${run:?}/root" "${run:?}/pre-recovery"
    fi
}

START=$(date +%s)
for scenario in ${SCENARIOS//,/ }; do
    case "$scenario" in
        store | broker | compaction | dictionary) ;;
        *) echo "unknown scenario $scenario" >&2; exit 2 ;;
    esac
    for mode in ${MODES//,/ }; do
        case "$mode" in
            sequential | parallel | inline) ;;
            *) echo "unknown mode $mode" >&2; exit 2 ;;
        esac
        echo "== $scenario $mode: $ITERATIONS iterations, seed $SEED"
        run_scenario "$scenario" "$mode"
    done
done

{
    printf '%-22s %6s %5s %5s %6s %6s %5s %9s %14s %9s  %s\n' scenario planned pass fail clear crash torn fallback in-compaction acked "recovery actions"
    while read -r name planned pass fail clear crash torn fallback inc acked actions; do
        printf '%-22s %6s %5s %5s %6s %6s %5s %9s %14s %9s  %s\n' "$name" "$planned" "$pass" "$fail" "$clear" "$crash" "$torn" "$fallback" "$inc" "$acked" "$actions"
    done <"$OUT/summary.rows"
    echo
    echo "seed $SEED, $ITERATIONS iterations per scenario, $(($(date +%s) - START)) s"
    if [ "$FAILED" -eq 0 ]; then echo "result: PASS"; else echo "result: FAIL (iterations.log)"; fi
} | tee "$OUT/summary.txt"
exit "$FAILED"
