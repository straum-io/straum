#!/usr/bin/env sh
# Kill each broker and count what it still has.
#
# This is the one comparison that is valid on a development Mac. It reports
# integers — messages promised, messages kept — and a virtualised network
# cannot move an integer. Latency comparisons here cannot say anything
# (docs/benchmarks.md §6, §7); this can.
#
#   scripts/crash-survival.sh
#   STRAUM_BENCH_MESSAGES=500 scripts/crash-survival.sh
#   STRAUM_BENCH_ONLY=straum scripts/crash-survival.sh
#
# Each broker is configured with its own durability story, not a handicap:
# the script prints what each one was told to do. A broker that keeps less
# is not being sabotaged, it is answering the question.
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
    echo "crash-survival: neither podman nor docker found" >&2
    exit 127
fi

RUST_IMAGE="${STRAUM_LINUX_IMAGE:-rust:1.85-bookworm}"
MOSQUITTO_IMAGE="${STRAUM_MOSQUITTO_IMAGE:-docker.io/library/eclipse-mosquitto:2}"
NANOMQ_IMAGE="${STRAUM_NANOMQ_IMAGE:-docker.io/emqx/nanomq:latest}"
NETWORK=straum-crash
ROOT="$(pwd)"

# Below the default session backlog of 1024 on purpose: above it the answer
# stops being about durability and becomes about that bound, which the
# second run below measures separately.
MESSAGES="${STRAUM_BENCH_MESSAGES:-800}"
GRACE="${STRAUM_BENCH_GRACE:-15}"
# One round decides nothing for a broker whose durability is periodic: kill
# it just after its save and it keeps everything, just before and it keeps
# nothing. Rounds are what separate "durable" from "lucky".
ROUNDS="${STRAUM_BENCH_ROUNDS:-3}"
# Seconds to wait after arming before killing. Zero is the honest default —
# a crash does not wait for anybody's autosave timer. Raise it to show how
# much a periodic saver needs to catch up.
SETTLE="${STRAUM_BENCH_SETTLE:-0}"
ONLY="${STRAUM_BENCH_ONLY:-straum mosquitto nanomq}"

run_in_rust() {
    "$ENGINE" run --rm --network "$NETWORK" \
        -v "$ROOT":/src -w /src \
        -v straum-cargo-registry:/usr/local/cargo/registry \
        -v straum-target-linux:/src/target \
        "$@"
}

build() {
    echo "building straum-server and the survival probe for linux..."
    run_in_rust "$RUST_IMAGE" \
        sh -c 'cargo build --release --bin straum-server >/dev/null && \
               cargo build --release -p e2e --bench survive >/dev/null && \
               ls -t target/release/deps/survive-* | grep -v "\.d$" | head -1 > /src/target/survive-path'
}

# Each broker keeps its state in its own named volume, so killing the
# container does not take the state with it.
start_broker() {
    name="$1"
    case "$name" in
        straum)
            "$ENGINE" run -d --name straum-crash-straum --network "$NETWORK" \
                -v "$ROOT":/src -w /src \
                -v straum-cargo-registry:/usr/local/cargo/registry \
                -v straum-target-linux:/src/target \
                -v straum-crash-straum-data:/state \
                -e STRAUM_LISTEN_ADDR=0.0.0.0:1883 \
                -e STRAUM_LOG_LEVEL=warn \
                -e STRAUM_DURABILITY=fsynced \
                -e STRAUM_WAL_PATH=/state/straum.wal \
                "$RUST_IMAGE" ./target/release/straum-server >/dev/null
            ;;
        mosquitto)
            "$ENGINE" run -d --name straum-crash-mosquitto --network "$NETWORK" \
                -v straum-crash-mosquitto-data:/mosquitto/data \
                "$MOSQUITTO_IMAGE" \
                sh -c 'printf "listener 1883\nallow_anonymous true\npersistence true\npersistence_location /mosquitto/data/\nautosave_interval 1\nmax_queued_messages 100000\n" > /m.conf; exec mosquitto -c /m.conf' >/dev/null
            ;;
        nanomq)
            "$ENGINE" run -d --name straum-crash-nanomq --network "$NETWORK" \
                -v straum-crash-nanomq-data:/state \
                "$NANOMQ_IMAGE" >/dev/null
            ;;
    esac
}

describe() {
    case "$1" in
        straum)    echo "STRAUM_DURABILITY=fsynced, log on a volume" ;;
        mosquitto) echo "persistence true, autosave_interval 1 (its shortest)" ;;
        nanomq)    echo "default configuration (no message persistence)" ;;
    esac
}

stop_broker() {
    "$ENGINE" rm -f "straum-crash-$1" >/dev/null 2>&1 || true
}

wipe_state() {
    stop_broker "$1"
    "$ENGINE" volume rm "straum-crash-$1-data" >/dev/null 2>&1 || true
}

wait_for_port() {
    # /dev/tcp is a bash feature and the image's sh is dash.
    run_in_rust "$RUST_IMAGE" bash -c \
        "for i in \$(seq 1 60); do if (exec 3<>/dev/tcp/straum-crash-$1/1883) 2>/dev/null; then exit 0; fi; sleep 0.5; done; exit 1"
}

probe() {
    broker="$1"
    shift
    run_in_rust "$RUST_IMAGE" bash -c \
        "\$(cat /src/target/survive-path) --address straum-crash-$broker:1883 $*"
}

"$ENGINE" network create "$NETWORK" >/dev/null 2>&1 || true
build

echo
echo "$MESSAGES QoS 1 messages published for an offline durable session,"
echo "then the broker is killed with SIGKILL and started again."
echo
printf '%-12s %10s %10s %10s %9s %10s %10s\n' \
    broker promised kept lost "lost %" session retained
printf '%.0s-' 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 \
    31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 \
    61 62 63 64 65 66 67 68 69 70 71 72 73 74 75
echo

for round in $(seq 1 "$ROUNDS"); do
for broker in $ONLY; do
    wipe_state "$broker"
    start_broker "$broker"
    wait_for_port "$broker" || { echo "$broker never started listening"; continue; }

    # Only acknowledged publishes count: the rest were in flight and the
    # broker never claimed them.
    promised=$(probe "$broker" --arm --messages "$MESSAGES" --grace "$GRACE" | tail -1)
    [ "$SETTLE" = "0" ] || sleep "$SETTLE"

    # SIGKILL, not a shutdown. A broker that only survives a clean stop has
    # not answered the question.
    "$ENGINE" kill --signal KILL "straum-crash-$broker" >/dev/null 2>&1 || true
    "$ENGINE" rm -f "straum-crash-$broker" >/dev/null 2>&1 || true

    start_broker "$broker"
    wait_for_port "$broker" || { echo "$broker did not come back up"; continue; }
    printf 'round %-2s ' "$round"
    probe "$broker" --verify --messages "$promised" --grace "$GRACE" --label "$broker"
    stop_broker "$broker"
done
done

echo
for broker in $ONLY; do
    printf '%-12s %s\n' "$broker" "$(describe "$broker")"
done
"$ENGINE" network rm "$NETWORK" >/dev/null 2>&1 || true

cat <<'NOTE'

These are counts, not timings, which is why they are worth taking here at
all: a container on macOS cannot change how many messages a broker still
has (docs/benchmarks.md §6).

"promised" is what the broker acknowledged at QoS 1 before it was killed —
its own claim, in its own protocol. "kept" is how many of those it handed
over after coming back. "session" is whether it remembered the durable
subscriber at all; a broker that answers no there lost the subscription as
well as the messages.

Read the rounds, not one row. What separates the brokers here is not how
much they keep on a good round but whether the number moves at all. A
broker that acknowledges when the record is on disk gives the same answer
every time. A broker that saves on a timer gives whatever the timer
happened to be doing: mosquitto's database was 47 bytes immediately after
this workload and 88 KB two seconds later, so killing it a moment earlier
or later is the difference between keeping everything and keeping nothing.
Set STRAUM_BENCH_SETTLE to give a periodic saver that time and watch its
numbers change — which is the point, because a crash will not.

Straum's own ceiling is STRAUM_SESSION_BACKLOG_MESSAGES (1024 by default):
a session cannot be owed more than that, and beyond it messages are refused
and counted in backlog_refused. Raise the message count above that limit
and straum starts losing too — that is a bound, not a bug, but it is the
number to raise for a deployment that needs a longer offline window.
NOTE
