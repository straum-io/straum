#!/usr/bin/env sh
# scripts/rates.awk and scripts/fence.awk on a fixture with known answers
# (straum#29): run by CI.
set -eu
cd "$(dirname "$0")/.."
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT

cat > "$dir/rates-steady.txt" <<'EOF'
--- round 1
puback publishers=6 subscribers=3 window=16 payload=64 scrape_ms=0 scrapes=0 pubacks_per_s=1800 p50_ms=50 p99_ms=80 deliveries_per_s=5400 cpu_ms_per_1k=40
positions samples=600 every_ms=100 records_per_s=2000
apply_lag node=0 samples=600 p50=0 p99=40 max=50 p50_ms=0 p99_ms=20 max_ms=25
serving_lag node=0 samples=600 p50=0 p99=40 max=50 time_p99_ms=20 time_max_ms=25
serving_lag node=1 samples=600 p50=0 p99=45 max=60 time_p99_ms=20 time_max_ms=30
--- round 2
puback publishers=6 subscribers=3 window=16 payload=64 scrape_ms=0 scrapes=0 pubacks_per_s=1900 p50_ms=50 p99_ms=90 deliveries_per_s=5700 cpu_ms_per_1k=40
positions samples=600 every_ms=100 records_per_s=2100
serving_lag node=0 samples=600 p50=0 p99=50 max=70 time_p99_ms=25 time_max_ms=35
disk device=sda seconds=60.0 writes_per_s=900 flushes_per_s=600 flush_ms_avg=1.500 records_per_s=2100 record_writes_per_flush=10.5 journal_commits_per_s=590 journal_commits_per_flush=0.98
EOF
cat > "$dir/rates-events.txt" <<'EOF'
--- round 1
puback publishers=6 subscribers=3 window=16 payload=64 scrape_ms=0 scrapes=0 pubacks_per_s=1700 p50_ms=50 p99_ms=85 deliveries_per_s=5100 cpu_ms_per_1k=-
positions samples=3000 every_ms=100 records_per_s=1900
serving_lag node=0 samples=2900 p50=0 p99=900 max=1150 time_p99_ms=2800 time_max_ms=3100
serving_again node=0 after_ms=2900 lag_at_serving=1150
serving_again node=1 after_ms=- lag_at_serving=-
event kind=kill node=0 at_s=4.0 max_gap_ms_other_nodes=200.0 max_gap_ms_its_node=3000.0 its_clients_gave_up=0
event kind=handoff node=1 at_s=8.0 max_gap_ms_other_nodes=40.0 max_gap_ms_its_node=- its_clients_gave_up=0
EOF
cat > "$dir/rates-saturation.txt" <<'EOF'
--- round 1
puback publishers=6 subscribers=3 window=16 payload=64 scrape_ms=0 scrapes=0 pubacks_per_s=25 p50_ms=500 p99_ms=26000 deliveries_per_s=75 cpu_ms_per_1k=40
positions samples=300 every_ms=100 records_per_s=5000
churn clients=64 rate=0 sessions_ended_per_s=1600 closed_total=48000
EOF

for file in "$dir"/rates-*.txt; do
    stage=${file##*/rates-}
    awk -v stage="${stage%.txt}" -f scripts/rates.awk "$file"
done > "$dir/summary.txt"
awk -f scripts/fence.awk "$dir/summary.txt" > "$dir/fence.txt"

fail=0
expect() {
    if ! grep -qF -- "$2" "$dir/$1"; then
        echo "missing in $1: $2" >&2
        fail=1
    fi
}
expect summary.txt "serving_lag_max median=70 min=60 max=70"
expect summary.txt "serving_lag_time_max_ms median=35 min=30 max=35"
expect summary.txt "record_writes_per_flush median=10.5 min=10.5 max=10.5"
expect summary.txt "journal_commits_per_flush median=0.98"
expect summary.txt "serving_again n=2 missing=1 p50_ms=2900 p90_ms=2900 max_ms=2900 lag_p50=1150 lag_max=1150"
# The queue takes its lag time from the time lag, not from W/R.
# The saturation stage's p99 and rate are left out of W; its removals are not.
expect fence.txt "inputs L_g=1150 T_g_s=3.100 C_s=0.090 R=2100 H_s=0.200 removals_per_s=1600"
expect fence.txt "W raw=7036 (L_g 1150 + C*R 189 + H*R 420, x4) pow2=8192"
expect fence.txt "queue raw=21696 (removals 1600/s x T 3.390 s, x4) pow2=32768 pruning_bound=6242 above_pruning_bound"
expect fence.txt "PRELIMINARY"

# Without serving_lag, fence.awk refuses rather than sizing from apply lag.
grep -v serving_lag "$dir/summary.txt" > "$dir/old.txt"
if awk -f scripts/fence.awk "$dir/old.txt" > "$dir/old-fence.txt"; then
    echo "fence.awk sized W without serving_lag" >&2
    fail=1
fi
expect old-fence.txt "not computed: no L T"

if [ "$fail" = 0 ]; then echo "test-fence: ok"; else cat "$dir/summary.txt" "$dir/fence.txt" >&2; fi
exit "$fail"
