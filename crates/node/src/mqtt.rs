//! MQTT connections: sockets in, bytes to the node loop, bytes back out.
//! Each connection is a reader task and a writer task; nothing here knows
//! MQTT — the gateway in the node loop does.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use transport::Listener;

/// The node loop's hold on one connection. Dropping it closes the socket:
/// the writer's queue closes, and with it the connection's tasks.
pub struct Conn {
    pub writer: mpsc::Sender<Bytes>,
    /// One per read the reader may make: the gateway's backpressure.
    pub credit: mpsc::Sender<()>,
    reader: tokio::task::AbortHandle,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

pub enum Event {
    Opened {
        conn: u64,
        handle: Conn,
        /// Counted in [`Waiting`] until admitted or gone.
        source: IpAddr,
    },
    Bytes {
        conn: u64,
        bytes: Vec<u8>,
    },
    Closed {
        conn: u64,
    },
    /// A credentials check finished (D-158 A).
    Authenticated {
        conn: u64,
        verdict: gateway::Verdict,
    },
}

static NEXT: AtomicU64 = AtomicU64::new(1);

/// Connections not yet admitted, per source (D-165 A; a source is an IPv4
/// address or an IPv6 /64, D-168 A): from the
/// accept until the gateway admits the CONNECT, or the connection goes. At
/// most `max` per source wait at once; a connection past that is closed at
/// once, so one source cannot take every slot. The map holds only sources
/// with a connection waiting: bounded by the waiting connections
/// (handshakes, connections queued for the node loop's `Opened`, and the
/// gateway's unadmitted ones), at most 16 per source.
#[derive(Debug)]
pub struct Waiting {
    max: usize,
    counts: Mutex<HashMap<IpAddr, usize>>,
    /// Refusals before the gateway (D-167 A): counted from the accept
    /// tasks, so atomics, not the node loop's plain counts.
    no_permit: AtomicU64,
    source_full: AtomicU64,
    handshake_failed: AtomicU64,
}

impl Waiting {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            counts: Mutex::new(HashMap::new()),
            no_permit: AtomicU64::new(0),
            source_full: AtomicU64::new(0),
            handshake_failed: AtomicU64::new(0),
        }
    }

    /// `false`: `source` already has `max` waiting.
    pub fn enter(&self, source: IpAddr) -> bool {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = counts.entry(unit(source)).or_insert(0);
        if *count >= self.max {
            self.source_full.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        *count += 1;
        true
    }

    /// What was refused before the gateway saw it (D-167 A).
    pub fn refused(&self) -> crate::metrics::Refused {
        crate::metrics::Refused {
            no_permit: self.no_permit.load(Ordering::Relaxed),
            source_full: self.source_full.load(Ordering::Relaxed),
            handshake_failed: self.handshake_failed.load(Ordering::Relaxed),
        }
    }

    pub fn leave(&self, source: IpAddr) {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = counts.get_mut(&unit(source)) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&unit(source));
            }
        }
    }

    #[cfg(test)]
    fn sources(&self) -> usize {
        self.counts.lock().map_or(0, |c| c.len())
    }
}

/// What one host controls, the key the cap counts by (D-168 A): an IPv4
/// address, or an IPv6 /64 (a host commonly holds a whole one); an
/// IPv4-mapped IPv6 address counts as its IPv4 address.
fn unit(source: IpAddr) -> IpAddr {
    match source {
        IpAddr::V4(_) => source,
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => {
                let [a, b, c, d, ..] = v6.segments();
                IpAddr::V6(Ipv6Addr::new(a, b, c, d, 0, 0, 0, 0))
            }
        },
    }
}

/// Bounds on connections before the gateway sees them (D-164 A). A TLS
/// handshake happens there, so a client that never finishes one would
/// otherwise hold a task and a socket for good: each must finish `within`,
/// and at most `at_once` run together; past that, a new connection is
/// closed at once.
#[derive(Debug, Clone, Copy)]
pub struct Handshakes {
    pub within: Duration,
    pub at_once: usize,
}

pub async fn listen(
    listener: Listener,
    write_queue: usize,
    handshakes: Handshakes,
    waiting: Arc<Waiting>,
    events: mpsc::Sender<Event>,
) {
    let permits = Arc::new(tokio::sync::Semaphore::new(handshakes.at_once.max(1)));
    loop {
        let Ok(accepted) = listener.accept().await else {
            continue;
        };
        let source = accepted.peer().ip();
        if !waiting.enter(source) {
            drop(accepted);
            continue;
        }
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            waiting.no_permit.fetch_add(1, Ordering::Relaxed);
            waiting.leave(source);
            drop(accepted);
            continue;
        };
        let events = events.clone();
        let waiting = Arc::clone(&waiting);
        tokio::spawn(async move {
            let established = tokio::time::timeout(handshakes.within, accepted.establish()).await;
            drop(permit);
            let Ok(Ok(connection)) = established else {
                waiting.handshake_failed.fetch_add(1, Ordering::Relaxed);
                waiting.leave(source);
                return;
            };
            let conn = NEXT.fetch_add(1, Ordering::Relaxed);
            let (writer_tx, mut writer_rx) = mpsc::channel::<Bytes>(write_queue);
            let (credit_tx, mut credit_rx) = mpsc::channel::<()>(1);
            let mut reader = connection.reader;
            let mut writer = connection.writer;
            let reader_events = events.clone();
            let reading = tokio::spawn(async move {
                // Reads only with a credit from the node loop.
                while credit_rx.recv().await.is_some() {
                    match reader.fill().await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let bytes = reader.buf_mut().split().to_vec();
                            reader.reclaim();
                            if reader_events
                                .send(Event::Bytes { conn, bytes })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
                let _ = reader_events.send(Event::Closed { conn }).await;
            });
            let handle = Conn {
                writer: writer_tx,
                credit: credit_tx,
                reader: reading.abort_handle(),
            };
            if events
                .send(Event::Opened {
                    conn,
                    handle,
                    source,
                })
                .await
                .is_err()
            {
                waiting.leave(source);
                return;
            }
            // Writes until the node loop drops the connection.
            // One write for everything queued; the buffer is reused.
            let mut bytes = BytesMut::new();
            while let Some(first) = writer_rx.recv().await {
                bytes.clear();
                bytes.extend_from_slice(&first);
                while let Ok(more) = writer_rx.try_recv() {
                    bytes.extend_from_slice(&more);
                }
                if writer.write_all(&bytes).await.is_err() {
                    break;
                }
            }
            let _ = writer.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// D-165 A: one source is capped, another is not affected, and the map
    /// keeps nothing for a source with nothing waiting.
    #[test]
    fn one_source_cannot_take_every_waiting_slot() {
        let waiting = Waiting::new(16);
        let a = IpAddr::from([10, 0, 0, 1]);
        let b = IpAddr::from([10, 0, 0, 2]);
        for _ in 0..16 {
            assert!(waiting.enter(a));
        }
        assert!(!waiting.enter(a), "the 17th from one source");
        assert_eq!(waiting.refused().source_full, 1, "and counted (D-167 A)");
        assert!(waiting.enter(b), "another source is still served");
        waiting.leave(a);
        assert!(waiting.enter(a), "a slot freed is taken again");
        for _ in 0..16 {
            waiting.leave(a);
        }
        waiting.leave(b);
        assert_eq!(waiting.sources(), 0);
        waiting.leave(b);
        assert_eq!(waiting.sources(), 0, "leaving twice is harmless");
    }

    /// D-168 A: an IPv6 /64 is one source; another /64 is another; an
    /// IPv4-mapped address is its IPv4 address.
    #[test]
    fn an_ipv6_prefix_is_one_source() {
        let waiting = Waiting::new(2);
        let one: IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let same_prefix: IpAddr = "2001:db8:1:2:ffff::7".parse().unwrap();
        let other_prefix: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert!(waiting.enter(one));
        assert!(waiting.enter(same_prefix));
        assert!(!waiting.enter(one), "two in one /64 fill it");
        assert!(waiting.enter(other_prefix), "another /64 is served");
        waiting.leave(same_prefix);
        assert!(
            waiting.enter(one),
            "a slot left under one address frees the /64"
        );

        let v4: IpAddr = "192.0.2.1".parse().unwrap();
        let mapped: IpAddr = "::ffff:192.0.2.1".parse().unwrap();
        assert!(waiting.enter(v4));
        assert!(waiting.enter(mapped));
        assert!(
            !waiting.enter(v4),
            "a mapped address counts as its IPv4 one"
        );
    }
}

/// D-167 A: each refusal before the gateway is counted where it happens.
#[cfg(test)]
mod refusals {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::io::Read;
    use std::time::Instant;

    use super::*;
    use transport::{BufferBudget, Tls, TransportConfig};

    /// A TLS listener with room for one handshake, finished within 300 ms.
    fn listener() -> (Listener, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "straum-node-refusals-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
        std::fs::write(&cert, issued.cert.pem()).unwrap();
        std::fs::write(&key, issued.signing_key.serialize_pem()).unwrap();
        let tls = Tls::from_pem_files(&cert, &key).unwrap();
        let config = TransportConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..TransportConfig::default()
        };
        let budget = Arc::new(BufferBudget::new(1 << 20));
        (Listener::bind(config, budget, Some(tls)).unwrap(), dir)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refusal_for_no_permit_and_a_handshake_that_times_out_are_counted() {
        let (listener, dir) = listener();
        let address = listener.local_addr().unwrap();
        let waiting = Arc::new(Waiting::new(16));
        let (events, _keep) = mpsc::channel(16);
        let accepting = tokio::spawn(listen(
            listener,
            16,
            Handshakes {
                within: Duration::from_millis(300),
                at_once: 1,
            },
            Arc::clone(&waiting),
            events,
        ));
        // One idle socket holds the only handshake slot.
        let idle = std::net::TcpStream::connect(address).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        // The next is closed at once, for want of a permit.
        let started = Instant::now();
        let mut second = std::net::TcpStream::connect(address).unwrap();
        second
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let read = tokio::task::spawn_blocking(move || second.read(&mut [0u8; 1]))
            .await
            .unwrap();
        assert!(matches!(read, Ok(0)), "closed: {read:?}");
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "at once, before the timeout"
        );
        assert_eq!(waiting.refused().no_permit, 1);
        // The idle one's handshake times out and is counted.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(waiting.refused().handshake_failed, 1);
        assert_eq!(waiting.refused().source_full, 0);
        drop(idle);
        accepting.abort();
        let _ = std::fs::remove_dir_all(dir);
    }
}
