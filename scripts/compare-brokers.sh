#!/usr/bin/env sh
# Run the same load against several MQTT brokers and print what each did.
#
# Every broker runs in its own container on one network, and one generator
# binary talks to all of them, so the numbers differ because the brokers do.
#
#   scripts/compare-brokers.sh                 # default workload
#   STRAUM_BENCH_RATE=50000 scripts/compare-brokers.sh
#   STRAUM_BENCH_ONLY=straum scripts/compare-brokers.sh
#
# Read the caveat it prints. Inside a VM on macOS these are not reference
# numbers (docs/benchmarks.md §6); they are a same-conditions comparison and
# nothing more.
set -eu

cd "$(dirname "$0")/.."

ENGINE="${STRAUM_CONTAINER_ENGINE:-}"
if [ -z "$ENGINE" ]; then
    for candidate in podman docker; do
        if command -v "$candidate" >/dev/null 2>&1; then
            ENGINE="$candidate"
            break
        fi
    done
fi
if [ -z "$ENGINE" ]; then
    echo "compare-brokers: neither podman nor docker found" >&2
    exit 127
fi

RUST_IMAGE="${STRAUM_LINUX_IMAGE:-rust:1.85-bookworm}"
MOSQUITTO_IMAGE="${STRAUM_MOSQUITTO_IMAGE:-docker.io/library/eclipse-mosquitto:2}"
NANOMQ_IMAGE="${STRAUM_NANOMQ_IMAGE:-docker.io/emqx/nanomq:latest}"
NETWORK=straum-bench
ROOT="$(pwd)"

PUBLISHERS="${STRAUM_BENCH_PUBLISHERS:-4}"
SUBSCRIBERS="${STRAUM_BENCH_SUBSCRIBERS:-4}"
RATE="${STRAUM_BENCH_RATE:-20000}"
PAYLOAD="${STRAUM_BENCH_PAYLOAD:-64}"
QOS="${STRAUM_BENCH_QOS:-0}"
SECONDS_TO_RUN="${STRAUM_BENCH_SECONDS:-10}"
CONNECTS="${STRAUM_BENCH_CONNECTS:-500}"
# One round proves nothing here: the spread between rounds is larger than
# most differences between brokers (docs/benchmarks.md §7).
ROUNDS="${STRAUM_BENCH_ROUNDS:-5}"
ONLY="${STRAUM_BENCH_ONLY:-straum mosquitto nanomq}"

run_in_rust() {
    "$ENGINE" run --rm --network "$NETWORK" \
        -v "$ROOT":/src -w /src \
        -v straum-cargo-registry:/usr/local/cargo/registry \
        -v straum-target-linux:/src/target \
        "$@"
}

build() {
    echo "building straum-server and the generator for linux..."
    run_in_rust "$RUST_IMAGE" \
        sh -c 'cargo build --release --bin straum-server >/dev/null && \
               cargo build --release -p e2e --bench loadgen >/dev/null && \
               ls -t target/release/deps/loadgen-* | grep -v "\.d$" | head -1 > /src/target/loadgen-path'
}

start_broker() {
    name="$1"
    case "$name" in
        straum)
            "$ENGINE" run -d --rm --name straum-bench-straum --network "$NETWORK" \
                -v "$ROOT":/src -w /src \
                -v straum-cargo-registry:/usr/local/cargo/registry \
                -v straum-target-linux:/src/target \
                -e STRAUM_LISTEN_ADDR=0.0.0.0:1883 \
                -e STRAUM_LOG_LEVEL=warn \
                -e STRAUM_WORKER_THREADS="${STRAUM_BENCH_WORKERS:-auto}" \
                -v straum-bench-straum-data:/state \
                -e STRAUM_DURABILITY="${STRAUM_BENCH_DURABILITY:-memory}" \
                -e STRAUM_WAL_PATH=/state/straum.wal \
                "$RUST_IMAGE" ./target/release/straum-server >/dev/null
            ;;
        mosquitto)
            "$ENGINE" run -d --rm --name straum-bench-mosquitto --network "$NETWORK" \
                "$MOSQUITTO_IMAGE" \
                sh -c 'printf "listener 1883\nallow_anonymous true\nmax_queued_messages 100000\n" > /m.conf; exec mosquitto -c /m.conf' >/dev/null
            ;;
        nanomq)
            "$ENGINE" run -d --rm --name straum-bench-nanomq --network "$NETWORK" \
                "$NANOMQ_IMAGE" >/dev/null
            ;;
    esac
}

stop_broker() {
    "$ENGINE" rm -f "straum-bench-$1" >/dev/null 2>&1 || true
}

loadgen() {
    label="$1"
    shift
    # /dev/tcp is a bash feature and the image's sh is dash.
    run_in_rust "$RUST_IMAGE" bash -c \
        "\$(cat /src/target/loadgen-path) --address straum-bench-$label:1883 --label $label $*"
}

"$ENGINE" network create "$NETWORK" >/dev/null 2>&1 || true
build

echo
echo "workload: $PUBLISHERS publishers, $SUBSCRIBERS subscribers, rate $RATE/s,"
echo "          payload $PAYLOAD B, QoS $QOS, $SECONDS_TO_RUN s; then $CONNECTS connects"
echo
printf '%-12s %10s %10s %11s %11s %9s %9s %9s %8s\n' \
    broker published received "pub/s" "recv/s" "p50 ms" "p95 ms" "p99 ms" payload
printf '%.0s-' 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 \
    31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 \
    61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 \
    91 92 93 94 95 96 97 98
echo

for round in $(seq 1 "$ROUNDS"); do
for broker in $ONLY; do
    stop_broker "$broker"
    start_broker "$broker"
    # Wait for the port rather than guessing how long a broker takes to bind.
    # /dev/tcp is a bash feature and the image's sh is dash.
    run_in_rust "$RUST_IMAGE" bash -c \
        "for i in \$(seq 1 60); do if (exec 3<>/dev/tcp/straum-bench-$broker/1883) 2>/dev/null; then exit 0; fi; sleep 0.5; done; exit 1" \
        || { echo "$broker never started listening"; stop_broker "$broker"; continue; }

    printf "%-7s " "round $round"
    loadgen "$broker" --publishers "$PUBLISHERS" --subscribers "$SUBSCRIBERS" \
        --rate "$RATE" --payload "$PAYLOAD" --qos "$QOS" --seconds "$SECONDS_TO_RUN"
    stop_broker "$broker"
done
done

echo
echo "connect storm: $CONNECTS clients connecting at once"
echo
printf '%-12s %10s %10s %11s %11s %9s %9s %9s %8s\n' \
    broker attempted connected "conn/s" "-" "p50 ms" "p95 ms" "p99 ms" "-"

for round in $(seq 1 "$ROUNDS"); do
for broker in $ONLY; do
    stop_broker "$broker"
    start_broker "$broker"
    # /dev/tcp is a bash feature and the image's sh is dash.
    run_in_rust "$RUST_IMAGE" bash -c \
        "for i in \$(seq 1 60); do if (exec 3<>/dev/tcp/straum-bench-$broker/1883) 2>/dev/null; then exit 0; fi; sleep 0.5; done; exit 1" \
        || { stop_broker "$broker"; continue; }

    printf "%-7s " "round $round"
    loadgen "$broker" --connects --publishers "$CONNECTS"
    stop_broker "$broker"
done
done

"$ENGINE" network rm "$NETWORK" >/dev/null 2>&1 || true

cat <<'NOTE'

Compare medians across rounds, never a single row: in this environment the
spread between rounds is larger than most differences between brokers, and
one round will happily show a broker as nine times worse than it is
(docs/benchmarks.md §7).

These numbers were taken inside containers. On a macOS host that means a
virtualised network and virtualised timing, which docs/benchmarks.md §6 rules
out as reference numbers. What they are good for is comparing brokers under
identical conditions on one machine; what they cannot tell you is what any of
these brokers does on real hardware.
NOTE
