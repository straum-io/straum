# Linux Tuning

Production hosts run Linux. This document covers what the broker checks and
what the operator sets.

## Who sets what

| Layer | Examples | Set by | How |
|---|---|---|---|
| Kernel | `net.core.somaxconn`, `net.core.rmem_max` | operator | `/etc/sysctl.d/*.conf`, container `--sysctl` |
| Process limits | open files (`RLIMIT_NOFILE`) | operator | systemd `LimitNOFILE`, `ulimit`, container `--ulimit` |
| Socket options | backlog, `TCP_NODELAY`, buffers, keepalive | broker | `STRAUM_*` env vars |

The broker never writes sysctls or limits. It reads them, logs them, derives
`auto` values from them, and warns about conflicts.

## Startup checks

Read from `/proc/sys/...` and `/proc/self/limits`, logged once at `info`.

| Check | Source | Result |
|---|---|---|
| `STRAUM_BACKLOG` > `net.core.somaxconn` | `/proc/sys/net/core/somaxconn` | warn: the kernel silently clamps the backlog |
| `STRAUM_BACKLOG` > `net.ipv4.tcp_max_syn_backlog` | `/proc/sys/net/ipv4/tcp_max_syn_backlog` | info: SYN queue may overflow first in connection bursts |
| `STRAUM_SO_RCVBUF_BYTES` > `net.core.rmem_max` | `/proc/sys/net/core/rmem_max` | warn: clamped |
| `STRAUM_SO_SNDBUF_BYTES` > `net.core.wmem_max` | `/proc/sys/net/core/wmem_max` | warn: clamped |
| `STRAUM_MAX_CONNECTIONS` + 64 > open files soft limit | `/proc/self/limits` | warn: accepts will fail with `EMFILE` |

`auto` resolution:

```text
STRAUM_BACKLOG=auto          → net.core.somaxconn
STRAUM_MAX_CONNECTIONS=auto  → open files soft limit − 64
```

A probe that fails (unreadable file, container restrictions) logs a warning and
falls back to the macOS default from `configuration.md`. It never aborts startup.

## Recommended kernel parameters

Starting points for a broker host. Verify with the checks at the end of this
document and with benchmarks; they are not universal values.

`/etc/sysctl.d/90-straum.conf`:

```ini
# Accept queue. The kernel clamps listen() backlog to this.
net.core.somaxconn = 8192

# Half-open (SYN_RECV) queue for connection bursts, e.g. mass reconnects.
net.ipv4.tcp_max_syn_backlog = 8192

# Upper bounds for SO_RCVBUF/SO_SNDBUF and for autotuning.
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216

# min / default / max per socket. Only max is raised; see below.
net.ipv4.tcp_rmem = 4096 131072 16777216
net.ipv4.tcp_wmem = 4096 16384 16777216

# Packets queued between the NIC driver and the stack under load.
net.core.netdev_max_backlog = 16384

# Ceilings for per-process and system-wide file descriptors.
fs.nr_open = 1048576
fs.file-max = 2097152
```

Apply: `sudo sysctl --system`.

### Socket memory and many connections

MQTT workloads usually mean many mostly idle connections with small messages.
Socket buffer memory is per connection:

```text
100,000 connections × 128 KiB default receive buffer ≈ 12 GiB worst case
```

Keep the **default** (middle) values of `tcp_rmem`/`tcp_wmem` modest and raise
only the **max**, so autotuning can grow buffers for the few high-throughput
connections. Do not set `STRAUM_SO_RCVBUF_BYTES`/`STRAUM_SO_SNDBUF_BYTES`
without a measurement: a fixed value disables autotuning. Watch
`net.ipv4.tcp_mem` (system-wide TCP memory, in pages) when connection counts
are high.

## File descriptors

One connection is one descriptor. systemd:

```ini
[Service]
LimitNOFILE=1048576
Environment=STRAUM_MAX_CONNECTIONS=auto
```

Container: `--ulimit nofile=1048576:1048576`.

## Containers

Network-namespaced parameters (for example `net.core.somaxconn`,
`net.ipv4.tcp_max_syn_backlog`) are set per container:

```bash
docker run --sysctl net.core.somaxconn=8192 --ulimit nofile=1048576:1048576 ...
```

In Kubernetes, `securityContext.sysctls`; parameters Kubernetes treats as unsafe
must be allowlisted on the kubelet.

Some `net.core` parameters (for example `rmem_max`/`wmem_max` on many kernels)
are host-global and cannot be set from a container. Check with `sysctl` inside
the container; the broker's startup log shows what it actually sees.

## Load generator host

Not the broker host. Many outgoing connections to one address need:

```ini
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
```

One source IP gives at most ~64k connections to one destination address and
port. Use several source addresses or several broker ports beyond that.

## Verification

```bash
# Listening socket: Recv-Q = current accept queue, Send-Q = backlog in effect.
ss -lnt 'sport = :1883'

# Accept queue overflows. Growing values mean backlog or accept rate is too low.
nstat -az TcpExtListenOverflows TcpExtListenDrops

# Effective descriptor limit of the running broker.
grep 'open files' /proc/$(pidof straum-server)/limits

# Per-socket memory and TCP state.
ss -tmi 'sport = :1883' | head
```

## Not tuned yet

Busy polling, IRQ and CPU affinity, congestion control selection, `io_uring`,
huge pages. Each needs a benchmark showing the problem first.

## macOS (development only)

* `kern.ipc.somaxconn` defaults to 128; the backlog is clamped to it.
* Interactive shells default to `ulimit -n 256`. For local load tests:
  `ulimit -n 10240` in the shell that runs the broker and the load generator.
* The broker skips kernel probes on macOS. Numbers measured on macOS are not
  used as production numbers.
