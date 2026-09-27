//! Letting an operator read the counters.
//!
//! Metrics that never leave the process are metrics nobody acts on. This
//! serves them over HTTP in the Prometheus text exposition format, which is
//! a few lines of `name value` and needs no library — adding an HTTP
//! framework to print thirty integers would be the wrong trade
//! (CLAUDE.md §2).
//!
//! Off unless `STRAUM_METRICS_ADDR` is set. It is a separate port from the
//! MQTT listener and it answers anyone who can reach it, so it belongs on
//! an interface the operator controls.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::metrics::{Metrics, Snapshot};

/// Largest request this reads before answering. It does not parse the
/// request — every path gets the same answer — but it has to drain it, and
/// a peer that never finishes sending one must not grow a buffer here.
const MAX_REQUEST_BYTES: usize = 4096;

/// Serves the counters until shutdown.
pub(crate) async fn serve(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        // Checked every time round: `changed()` fires once, and waiting for
        // a second change after shutdown waits forever.
        if *shutdown.borrow() {
            return;
        }
        let accepted = tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            accepted = listener.accept() => accepted,
        };
        let Ok((stream, _)) = accepted else {
            continue;
        };
        // One scrape at a time, on purpose. Scrapes are seconds apart and
        // serialising them costs nothing; a task per connection would be a
        // way for anybody who can reach the port to make tasks.
        answer(stream, &metrics.snapshot()).await;
    }
}

async fn answer(mut stream: TcpStream, seen: &Snapshot) {
    let mut request = [0u8; MAX_REQUEST_BYTES];
    let mut filled = 0;
    // Drained, not parsed: a client that gets its request refused before it
    // finished sending sees a broken pipe instead of an answer.
    while filled < MAX_REQUEST_BYTES {
        let Ok(read) = stream.read(&mut request[filled..]).await else {
            return;
        };
        if read == 0 {
            break;
        }
        filled += read;
        if request[..filled].windows(4).any(|end| end == b"\r\n\r\n") {
            break;
        }
    }

    let body = expose(seen);
    let head = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain; version=0.0.4\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(body.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// The Prometheus text exposition format.
///
/// Counters and gauges are distinguished because the distinction is what a
/// reader needs: a counter is meaningful as a rate, a gauge is meaningful
/// as a value. CLAUDE.md §15 is the list this follows.
fn expose(seen: &Snapshot) -> String {
    let mut out = String::with_capacity(4096);

    let counters: [(&str, u64, &str); 19] = [
        (
            "connections_accepted",
            seen.connections_accepted,
            "Connections accepted since start.",
        ),
        (
            "protocol_errors",
            seen.protocol_errors,
            "Connections closed for a protocol violation.",
        ),
        (
            "transport_errors",
            seen.transport_errors,
            "Connections closed by a socket error or a stalled write.",
        ),
        ("bytes_in", seen.bytes_in, "Bytes read from clients."),
        ("bytes_out", seen.bytes_out, "Bytes written to clients."),
        (
            "keepalive_closes",
            seen.keepalive_closes,
            "Connections closed for missing their keep-alive.",
        ),
        (
            "authentication_failures",
            seen.authentication_failures,
            "CONNECTs refused for bad or missing credentials.",
        ),
        (
            "slow_consumer_closes",
            seen.slow_consumer_closes,
            "Sessions closed for falling too far behind.",
        ),
        (
            "backlog_refused",
            seen.backlog_refused,
            "At-least-once deliveries a session backlog had no room for.",
        ),
        (
            "session_records_dropped",
            seen.session_records_dropped,
            "Session records the log queue could not take; each one is a session that will not come back.",
        ),
        ("wal_commits", seen.wal_commits, "Completed commit cycles."),
        (
            "wal_records_committed",
            seen.wal_records_committed,
            "Records made durable.",
        ),
        (
            "wal_bytes_committed",
            seen.wal_bytes_committed,
            "Bytes made durable.",
        ),
        (
            "wal_commit_failures",
            seen.wal_commit_failures,
            "Durability operations that failed. Nothing is acknowledged after one.",
        ),
        (
            "wal_compactions",
            seen.wal_compactions,
            "Times the log was rewritten.",
        ),
        (
            "replication_connections",
            seen.replication_connections,
            "Replication links established.",
        ),
        (
            "replication_errors",
            seen.replication_errors,
            "Replication links that ended in an error.",
        ),
        (
            "replication_shipments_dropped",
            seen.replication_shipments_dropped,
            "Committed batches the live queue had no room for. Not lost; read back from the log.",
        ),
        (
            "replication_catchups",
            seen.replication_catchups,
            "Times the leader read its own log to fill a gap.",
        ),
    ];
    let gauges: [(&str, u64, &str); 10] = [
        (
            "wal_sync_nanos",
            seen.wal_sync_nanos,
            "Duration of the most recent durability operation. Latest rather than an average: the average of a bimodal latency is a number that never happened.",
        ),
        (
            "replication_queued",
            seen.replication_queued,
            "Committed batches waiting in the live queue.",
        ),
        (
            "active_sessions",
            seen.active_sessions,
            "Sessions the broker is holding.",
        ),
        (
            "queued_messages",
            seen.queued_messages as u64,
            "Outbound items queued across all sessions.",
        ),
        (
            "queued_bytes",
            seen.queued_bytes as u64,
            "Payload bytes queued across all sessions.",
        ),
        (
            "backlog_bytes",
            seen.backlog_bytes,
            "Payload bytes held for sessions that are behind.",
        ),
        (
            "wal_durable_sequence",
            seen.wal_durable_sequence,
            "Highest WAL sequence this node has made durable.",
        ),
        (
            "replication_connected",
            seen.replication_connected,
            "1 while a replication link is up.",
        ),
        (
            "replication_follower_sequence",
            seen.replication_follower_sequence,
            "Highest WAL sequence the follower has reported durable.",
        ),
        (
            "replication_in_flight",
            seen.replication_in_flight,
            "Sequences sent to the follower and not yet confirmed.",
        ),
    ];

    for (name, value, help) in counters {
        write_metric(&mut out, name, "counter", help, value);
    }
    for (name, value, help) in gauges {
        write_metric(&mut out, name, "gauge", help, value);
    }
    out
}

fn write_metric(out: &mut String, name: &str, kind: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP straum_{name} {help}\n# TYPE straum_{name} {kind}\nstraum_{name} {value}\n"
    ));
}

/// Binds the metrics port. Synchronous, because `Runtime::start` is, and
/// the same `from_std` path the connection listener already uses.
pub(crate) fn bind(addr: SocketAddr) -> Result<TcpListener, std::io::Error> {
    let socket = std::net::TcpListener::bind(addr)?;
    socket.set_nonblocking(true)?;
    TcpListener::from_std(socket)
}
