#!/usr/bin/env sh
# D-174 A: the D-129 noise gate once more, with the load recorded, and only
# if the host passes it, the measurements that wait on it: the pre-zeroed
# WAL A/B (D-170 A, scripts/prezeroed-ab.sh) and `cap` with TLS between
# nodes off and on (ADR-0010, straum#70). A host that fails stops here: no
# measurement runs, and the next step is finding why, not another try.
#
#   scripts/gated-run.sh       # gate ~1 h; if it passes, ~2 h more
#
# Run on the Linux host itself, on main, with no desktop session and every
# CPU on the `performance` governor. Writes target/gated-run/<time>/ and,
# however it ends, one archive of it to send back. Exit 3: the host is not
# accepted. Exit 4: a tool failed, which says nothing about the host.
set -eu
cd "$(dirname "$0")/.."
if [ "$(uname -s)" != Linux ]; then
    echo "$0: this is for a Linux host" >&2
    exit 2
fi
OUT=target/gated-run/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"

# The load every 30 s for the whole run, so a slow round can be matched to
# what else ran.
(
    while :; do
        echo "$(date +%T) $(cat /proc/loadavg)"
        sleep 30
    done
) > "$OUT/load.txt" &
LOAD=$!

# However the run ends: stop the load record, archive what there is, and
# say what to send back.
finish() {
    kill "$LOAD" 2>/dev/null || true
    tar czf "$OUT.tar.gz" -C target/gated-run "$(basename "$OUT")"
    echo "send back: $OUT.tar.gz"
    sha256sum "$OUT.tar.gz"
}
trap finish EXIT
trap 'exit 130' INT TERM

# tool_failed <what>: a step failed for a reason that is not the host's.
tool_failed() {
    echo "== $1 failed: a tool failure, not a gate result (see $OUT)" | tee -a "$OUT/verdict.txt"
    exit 4
}

{
    echo "commit: $(git rev-parse HEAD) dirty: $(git status --porcelain | wc -l)"
    echo "governors: $(sort /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | uniq -c | tr -s ' ' | tr '\n' ';')"
    echo "no_turbo: $(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || echo n/a)"
    echo "loadavg: $(cat /proc/loadavg)"
} | tee "$OUT/host.txt"

echo "== 1. the noise gate (D-129, D-174 A)"
STRAUM_OVERHEAD_OUT=$OUT/noise scripts/overhead.sh . . 5 > "$OUT/noise.txt" 2>&1 ||
    tool_failed "the noise run (scripts/overhead.sh)"
# noise.awk exits 3 for a host that fails the gate; anything else non-zero
# is a fault of the run, not a verdict.
gate=0
awk -v gate=0.03 -f scripts/noise.awk "$OUT/noise/rounds" > "$OUT/noise-verdict.txt" || gate=$?
cat "$OUT/noise-verdict.txt"
if grep -q "TOO FEW ROUNDS" "$OUT/noise-verdict.txt"; then
    tool_failed "a noise round (rounds missing)"
fi
case $gate in
    0) echo "== the host is accepted (D-129)" | tee "$OUT/verdict.txt" ;;
    3)
        echo "== the host is not accepted again: stopping (D-174 A); nothing else runs" | tee "$OUT/verdict.txt"
        exit 3
        ;;
    *) tool_failed "noise.awk (exit $gate)" ;;
esac

echo "== 2. pre-zeroed WAL segments, A/B (D-170 A)"
scripts/prezeroed-ab.sh > "$OUT/prezeroed-ab.txt" 2>&1 || tool_failed "scripts/prezeroed-ab.sh"
archive=$(sed -n 's/^send back: //p' "$OUT/prezeroed-ab.txt" | tail -1)
[ -n "$archive" ] && [ -f "$archive" ] || tool_failed "finding the prezeroed-ab archive"
cp "$archive" "$OUT/"

echo "== 3. cap, TLS between nodes off and on (ADR-0010)"
scripts/cap-tls-ab.sh 4 "$OUT" || tool_failed "scripts/cap-tls-ab.sh"
