//! The replication failure matrix, over real sockets (docs/replication.md §121).
//!
//! Nothing here waits for a duration to prove anything. The synchronisation
//! point is the protocol itself: a follower sends a durability report only
//! after its own `sync_data` returned, so *receiving that report* is the
//! observable transition, and the absence of one is proved against the WAL
//! file rather than against a clock.
//!
//! The determinism comes from the test speaking the wire protocol itself
//! rather than from hooks inside the broker. Driving one side by hand is
//! what makes "close the socket after exactly this many bytes" and "deliver
//! three frames in one write" ordinary rather than racy, and it costs the
//! production path nothing.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use replication::{Limits, Message};
use runtime::{
    Durability, ReplicationSettings, Runtime, RuntimeConfig, TransportConfig, WalSettings,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Everything in this file finishes in milliseconds when it works. The
/// deadline exists so a broken run fails with a message instead of hanging;
/// it is never how a test decides that something happened.
const DEADLINE: Duration = Duration::from_secs(20);

fn scratch(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "straum-matrix-{name}-{}-{:?}.wal",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&path);
    path
}

fn any_port() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn base(path: &Path) -> RuntimeConfig {
    RuntimeConfig {
        transport: TransportConfig {
            listen: any_port(),
            ..TransportConfig::default()
        },
        durability: Durability::Fsynced,
        wal: WalSettings {
            path: path.to_path_buf(),
            ..WalSettings::default()
        },
        ..RuntimeConfig::default()
    }
}

/// A node that only receives a log.
fn follower_node(path: &Path) -> RuntimeConfig {
    RuntimeConfig {
        replication: ReplicationSettings {
            listen: Some(any_port()),
            ..ReplicationSettings::default()
        },
        ..base(path)
    }
}

fn leader_node(path: &Path, ships_to: SocketAddr) -> RuntimeConfig {
    RuntimeConfig {
        replication: ReplicationSettings {
            follower: Some(ships_to),
            ..ReplicationSettings::default()
        },
        ..base(path)
    }
}

/// The framed records a WAL holds, after its header. What byte-preserving
/// replication has to reproduce exactly.
fn body(path: &Path) -> Vec<u8> {
    let bytes = std::fs::read(path).unwrap();
    bytes[storage::HEADER_BYTES..].to_vec()
}

fn records(path: &Path) -> u64 {
    storage::replay(path, storage::WalLimits::default(), &mut |_| {})
        .unwrap()
        .records
}

/// One framed WAL record, as a leader's log would hold it.
fn record(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    storage::encode_for_benchmarks(payload, &mut out);
    out
}

/// A batch of `count` records, starting the payloads at `from`.
fn batch(from: usize, count: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for index in from..from + count {
        out.extend_from_slice(&record(format!("record-{index}").as_bytes()));
    }
    out
}

fn frame(previous: u64, last: u64, batch: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    replication::encode_records_header(previous, last, batch, &mut out);
    out.extend_from_slice(batch);
    out
}

/// The test playing leader against a real follower node.
struct AsLeader {
    stream: TcpStream,
    buffered: BytesMut,
}

impl AsLeader {
    /// Connects and reads the follower's opening report, which is what it
    /// says it already holds.
    async fn connect(to: SocketAddr) -> (Self, u64) {
        let stream = tokio::time::timeout(DEADLINE, TcpStream::connect(to))
            .await
            .expect("the follower never accepted")
            .unwrap();
        let mut link = Self {
            stream,
            buffered: BytesMut::new(),
        };
        let reported = link.report().await.expect("no opening report");
        (link, reported)
    }

    async fn write(&mut self, bytes: &[u8]) {
        tokio::time::timeout(DEADLINE, self.stream.write_all(bytes))
            .await
            .expect("the write never completed")
            .unwrap();
    }

    /// Sends a frame one byte per write, so the follower sees it split in
    /// every possible place.
    async fn write_dribbled(&mut self, bytes: &[u8]) {
        for at in 0..bytes.len() {
            self.write(&bytes[at..at + 1]).await;
        }
    }

    /// Blocks until the follower reports, or the connection ends.
    ///
    /// This is the synchronisation point the whole file rests on: the report
    /// is sent after `sync_data` returned and at no other time.
    async fn report(&mut self) -> Option<u64> {
        loop {
            match replication::decode(&self.buffered, Limits::default()).unwrap() {
                Some((Message::Durable(sequence), consumed)) => {
                    let _ = self.buffered.split_to(consumed);
                    return Some(sequence);
                }
                Some((Message::Records(_), _)) => panic!("a follower sent records"),
                None => {}
            }
            let read = tokio::time::timeout(DEADLINE, self.stream.read_buf(&mut self.buffered))
                .await
                .expect("the follower went quiet")
                .unwrap();
            if read == 0 {
                return None;
            }
        }
    }

    /// Ends the connection the way a crash does, without a goodbye.
    fn kill(self) {
        drop(self.stream);
    }
}

/// The test playing follower against a real leader node.
struct AsFollower {
    stream: TcpStream,
    buffered: BytesMut,
}

impl AsFollower {
    /// Binds, and hands back the address a leader should be pointed at.
    async fn listen() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind(any_port()).await.unwrap();
        let at = listener.local_addr().unwrap();
        (listener, at)
    }

    /// Accepts the leader and claims to hold everything through `durable`.
    async fn accept(listener: &TcpListener, durable: u64) -> Self {
        let (stream, _) = tokio::time::timeout(DEADLINE, listener.accept())
            .await
            .expect("the leader never connected")
            .unwrap();
        let mut link = Self {
            stream,
            buffered: BytesMut::new(),
        };
        let mut out = Vec::new();
        replication::encode_durable(durable, &mut out);
        link.stream.write_all(&out).await.unwrap();
        link
    }

    /// The next frame the leader sends, as its range and its bytes.
    async fn frame(&mut self) -> Option<(u64, u64, Vec<u8>)> {
        loop {
            let taken = match replication::decode(&self.buffered, Limits::default()).unwrap() {
                Some((Message::Records(held), consumed)) => {
                    Some((held.previous, held.last, held.batch.to_vec(), consumed))
                }
                Some((Message::Durable(_), _)) => panic!("a leader sent a report"),
                None => None,
            };
            if let Some((previous, last, batch, consumed)) = taken {
                let _ = self.buffered.split_to(consumed);
                return Some((previous, last, batch));
            }
            let read = tokio::time::timeout(DEADLINE, self.stream.read_buf(&mut self.buffered))
                .await
                .expect("the leader sent nothing")
                .unwrap();
            if read == 0 {
                return None;
            }
        }
    }

    /// Says this side made everything through `sequence` durable.
    async fn report(&mut self, sequence: u64) {
        let mut out = Vec::new();
        replication::encode_durable(sequence, &mut out);
        self.stream.write_all(&out).await.unwrap();
    }

    fn kill(self) {
        drop(self.stream);
    }
}

/// Publishes `count` QoS 1 messages and returns once they are acknowledged.
async fn publish(broker: &Runtime, name: &str, count: usize) {
    publish_sized(broker, name, count, 16).await;
}

/// The same, with a payload large enough to fill a socket buffer.
async fn publish_sized(broker: &Runtime, name: &str, count: usize, payload: usize) {
    use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};
    let addr = broker.local_addr();
    let mut options = MqttOptions::new(name, addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(5));
    options.set_max_packet_size(payload + 1024, payload + 1024);
    let (client, mut eventloop) = AsyncClient::new(options, 64);

    // The requests go out from a second task because the event loop has to
    // be polled for them to leave at all: `publish` only queues, and the
    // queue is 64 deep.
    let sending = client.clone();
    let body = vec![b'x'; payload];
    let publisher = tokio::spawn(async move {
        for _ in 0..count {
            sending
                .publish("matrix/topic", QoS::AtLeastOnce, false, body.clone())
                .await
                .unwrap();
        }
    });

    // The acknowledgement is the synchronisation point. A PUBACK is the
    // broker's own statement that it is done with the publish at the
    // configured durability level, which is what every caller here is
    // waiting to be true — and it is a protocol event, not a duration.
    let mut acked = 0usize;
    while acked < count {
        if let Event::Incoming(Packet::PubAck(_)) = eventloop.poll().await.unwrap() {
            acked = acked.saturating_add(1);
        }
    }
    publisher.await.unwrap();

    // Then say goodbye and stop at the moment it is on the wire. What this
    // deliberately does not do is wait for the event loop to notice the
    // closed socket: it only notices when its keep-alive timer fires, so
    // that cost five seconds per call and proved nothing. Measured on this
    // machine: the broker drops the session 218 µs after `disconnect()`,
    // and the event loop task ended 5.004 s after it.
    client.disconnect().await.unwrap();
    while !matches!(
        eventloop.poll().await,
        Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(_)
    ) {}
}

// ---------------------------------------------------------------- follower
// The test is the leader. It decides what bytes go out, where they are cut,
// and when the socket dies.

/// 2. Two empty logs agree without anything being sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_follower_reports_nothing_and_stays_empty() {
    let log = scratch("empty");
    let node = Runtime::start(follower_node(&log)).unwrap();
    let (link, reported) = AsLeader::connect(node.replication_addr().unwrap()).await;

    assert_eq!(reported, 0, "an empty log has allocated nothing");
    link.kill();
    node.shutdown().await;

    assert_eq!(records(&log), 0);
    let _ = std::fs::remove_file(&log);
}

/// 13. A frame the follower already holds is written once, not twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_frame_is_acknowledged_and_not_written_again() {
    let log = scratch("duplicate");
    let node = Runtime::start(follower_node(&log)).unwrap();
    let (mut link, _) = AsLeader::connect(node.replication_addr().unwrap()).await;

    let bytes = batch(0, 2);
    link.write(&frame(0, 2, &bytes)).await;
    assert_eq!(link.report().await, Some(2));

    // The leader did not hear the first answer and sends it again.
    link.write(&frame(0, 2, &bytes)).await;
    assert_eq!(link.report().await, Some(2), "the position moved");

    link.kill();
    node.shutdown().await;

    assert_eq!(records(&log), 2, "the batch was appended twice");
    assert_eq!(body(&log), bytes);
    let _ = std::fs::remove_file(&log);
}

/// 14. One byte per write: every split point in one test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_frame_split_across_every_byte_boundary_still_arrives_whole() {
    let log = scratch("dribble");
    let node = Runtime::start(follower_node(&log)).unwrap();
    let (mut link, _) = AsLeader::connect(node.replication_addr().unwrap()).await;

    let bytes = batch(0, 3);
    link.write_dribbled(&frame(0, 3, &bytes)).await;
    assert_eq!(link.report().await, Some(3));

    link.kill();
    node.shutdown().await;
    assert_eq!(
        body(&log),
        bytes,
        "the reassembled frame is not the original"
    );
    let _ = std::fs::remove_file(&log);
}

/// 7, 8, 14. A frame that never finished arriving is not a frame, so nothing
/// was written and nothing is durable. One test for three rows because the
/// three name the same instant: the connection dies with an incomplete frame
/// in the buffer, which is before any write could have happened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_frame_cut_off_by_the_connection_never_reaches_the_log() {
    let log = scratch("partial");
    let node = Runtime::start(follower_node(&log)).unwrap();
    let at = node.replication_addr().unwrap();

    let first = batch(0, 2);
    let (mut link, _) = AsLeader::connect(at).await;
    link.write(&frame(0, 2, &first)).await;
    assert_eq!(link.report().await, Some(2));

    // Half of the next one, then the socket dies.
    let second = batch(2, 2);
    let whole = frame(2, 4, &second);
    link.write(&whole[..whole.len() / 2]).await;
    link.kill();

    // Reconnecting, the follower says where it really is. Not four.
    let (link, reported) = AsLeader::connect(at).await;
    assert_eq!(reported, 2, "a half-delivered frame moved the boundary");
    link.kill();
    node.shutdown().await;

    assert_eq!(records(&log), 2);
    assert_eq!(body(&log), first, "a fragment reached the log");
    let _ = std::fs::remove_file(&log);
}

/// 15. Three frames in one write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_frames_in_one_write_are_all_taken_in_order() {
    let log = scratch("coalesced");
    let node = Runtime::start(follower_node(&log)).unwrap();
    let (mut link, _) = AsLeader::connect(node.replication_addr().unwrap()).await;

    let (one, two, three) = (batch(0, 1), batch(1, 1), batch(2, 1));
    let mut all = frame(0, 1, &one);
    all.extend_from_slice(&frame(1, 2, &two));
    all.extend_from_slice(&frame(2, 3, &three));
    link.write(&all).await;

    // The reports may be coalesced into one commit, so wait for the last.
    let mut reached = 0;
    while reached < 3 {
        reached = link.report().await.expect("the link ended early");
    }

    link.kill();
    node.shutdown().await;

    let mut expected = one;
    expected.extend_from_slice(&two);
    expected.extend_from_slice(&three);
    assert_eq!(body(&log), expected, "the order or the bytes changed");
    let _ = std::fs::remove_file(&log);
}

/// 16. Corruption is refused and the log does not move. The bytes chosen
/// are the kind, the range, the checksum itself and a byte of the batch —
/// everything except the length, which is the one field that has to be acted
/// on before the checksum can be computed and has its own test below.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupted_frame_ends_the_link_and_changes_nothing() {
    for at in [1usize, 9, 24, 30] {
        let log = scratch(&format!("corrupt-{at}"));
        let node = Runtime::start(follower_node(&log)).unwrap();
        let (mut link, _) = AsLeader::connect(node.replication_addr().unwrap()).await;

        let bytes = batch(0, 4);
        let mut damaged = frame(0, 4, &bytes);
        damaged[at] ^= 0x01;
        link.write(&damaged).await;

        // The follower refuses it and ends the connection rather than
        // writing something it cannot vouch for.
        assert_eq!(link.report().await, None, "byte {at} was accepted");

        node.shutdown().await;
        assert_eq!(records(&log), 0, "byte {at} reached the log");
        let _ = std::fs::remove_file(&log);
    }
}

/// 17, 18. A frame that starts past where the follower is leaves a hole, and
/// a prefix with a hole is not a prefix. One test: out of order and a gap are
/// the same refusal, which is the point of the range rule.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_frame_that_skips_ahead_does_not_advance_anything() {
    let log = scratch("gap");
    let node = Runtime::start(follower_node(&log)).unwrap();
    let at = node.replication_addr().unwrap();
    let (mut link, _) = AsLeader::connect(at).await;

    let first = batch(0, 2);
    link.write(&frame(0, 2, &first)).await;
    assert_eq!(link.report().await, Some(2));

    // Record 3 is missing: this frame claims to start at 3.
    let ahead = batch(3, 2);
    link.write(&frame(3, 5, &ahead)).await;
    assert_eq!(link.report().await, None, "a gap was accepted");

    let (link, reported) = AsLeader::connect(at).await;
    assert_eq!(reported, 2, "the boundary moved past the hole");
    link.kill();
    node.shutdown().await;

    assert_eq!(records(&log), 2);
    assert_eq!(body(&log), first);
    let _ = std::fs::remove_file(&log);
}

/// 19. A follower that is stopped and started again knows where it is from
/// its own log, and nothing else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_follower_recovers_its_boundary_from_its_own_log() {
    let log = scratch("restart");
    let node = Runtime::start(follower_node(&log)).unwrap();
    let (mut link, _) = AsLeader::connect(node.replication_addr().unwrap()).await;

    let first = batch(0, 5);
    link.write(&frame(0, 5, &first)).await;
    assert_eq!(link.report().await, Some(5));
    link.kill();
    node.shutdown().await;

    // A different process would be a different test harness; what matters
    // here is that nothing in memory carried over.
    let node = Runtime::start(follower_node(&log)).unwrap();
    let at = node.replication_addr().unwrap();
    let (mut link, reported) = AsLeader::connect(at).await;
    assert_eq!(reported, 5, "the follower forgot what it had synced");

    // Only the suffix, and it lands after what was already there.
    let second = batch(5, 3);
    link.write(&frame(5, 8, &second)).await;
    assert_eq!(link.report().await, Some(8));
    link.kill();
    node.shutdown().await;

    let mut expected = first;
    expected.extend_from_slice(&second);
    assert_eq!(records(&log), 8);
    assert_eq!(body(&log), expected);
    let _ = std::fs::remove_file(&log);
}

/// 16. A corrupted length is the one corruption a receiver cannot tell from
/// a frame still in flight, so it waits for bytes that will never come
/// (docs/replication.md §114). What matters is that it commits nothing while
/// it waits — asserted against the log and the next handshake, not against a
/// clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupted_length_commits_nothing_while_it_waits() {
    let log = scratch("corrupt-length");
    let node = Runtime::start(follower_node(&log)).unwrap();
    let at = node.replication_addr().unwrap();
    let (mut link, _) = AsLeader::connect(at).await;

    let mut damaged = frame(0, 4, &batch(0, 4));
    // Byte 18 is inside the length: the frame now claims more than it has.
    damaged[18] ^= 0x01;
    link.write(&damaged).await;
    link.kill();

    let (link, reported) = AsLeader::connect(at).await;
    assert_eq!(
        reported, 0,
        "a frame that never completed moved the boundary"
    );
    link.kill();
    node.shutdown().await;

    assert_eq!(records(&log), 0);
    let _ = std::fs::remove_file(&log);
}

// ------------------------------------------------------------------ leader
// The test is the follower. It decides what position to claim, whether to
// acknowledge at all, and when the socket dies.

/// Waits for the leader to have heard a report. An observable transition,
/// with a deadline only so a broken run fails instead of hanging.
async fn heard(node: &Runtime, want: u64) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().replication_follower_sequence < want {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the leader heard {}, not {want}",
            node.metrics().replication_follower_sequence
        );
        tokio::task::yield_now().await;
    }
}

/// Everything the leader sent on one connection, collected until it stops.
async fn collect(peer: &mut AsFollower, from: u64, until: u64) -> Vec<u8> {
    let mut seen = Vec::new();
    let mut at = from;
    while at < until {
        let Some((previous, last, batch)) = peer.frame().await else {
            break;
        };
        assert_eq!(previous, at, "the leader left a hole");
        seen.extend_from_slice(&batch);
        at = last;
    }
    seen
}

/// 3. A follower that has nothing is sent everything, read back out of the
/// leader's log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_is_behind_is_caught_up_from_the_log() {
    let log = scratch("catchup");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();

    // Published before anybody is listening, so this is catch-up and not
    // the live path.
    publish(&node, "publisher", 20).await;

    let mut peer = AsFollower::accept(&listener, 0).await;
    let sent = collect(&mut peer, 0, 20).await;
    peer.report(20).await;

    // What arrived is what the leader's log holds, byte for byte.
    assert_eq!(sent, body(&log));
    heard(&node, 20).await;

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 4. A follower that is level with the leader is sent nothing at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_is_level_is_sent_nothing() {
    let log = scratch("level");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();
    publish(&node, "publisher", 6).await;

    let mut peer = AsFollower::accept(&listener, 6).await;

    // Anything the leader thinks is missing would arrive now. What arrives
    // is the next live batch, so publish one and check it starts at 6 —
    // proof that nothing before it was resent.
    publish(&node, "again", 2).await;
    let (previous, last, _) = peer.frame().await.expect("the live batch never came");
    assert_eq!(
        previous, 6,
        "the leader resent what the follower already had"
    );
    assert!(last > 6);

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 5. A follower claiming more than this log ever wrote is refused, not
/// reset and not believed (docs/replication.md §117).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_is_ahead_is_refused() {
    let log = scratch("ahead");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();
    publish(&node, "publisher", 3).await;

    let mut peer = AsFollower::accept(&listener, 9_000).await;

    // The link ends rather than the leader inventing a history.
    assert!(peer.frame().await.is_none(), "the leader kept talking");
    assert_eq!(
        node.metrics().replication_follower_sequence,
        0,
        "a claim the leader could not honour moved its watermark"
    );

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 11, 12. The leader never hears the report. On reconnect the follower says
/// what it actually holds and the leader believes that instead of its own
/// memory — so nothing is sent twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_report_that_never_arrived_is_not_a_reason_to_resend() {
    let log = scratch("unreported");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();
    publish(&node, "publisher", 10).await;

    // Everything arrives, and this side deliberately acknowledges nothing.
    let mut peer = AsFollower::accept(&listener, 0).await;
    let first = collect(&mut peer, 0, 10).await;
    assert_eq!(first, body(&log));
    assert_eq!(
        node.metrics().replication_follower_sequence,
        0,
        "the leader counted delivery as durability"
    );
    peer.kill();

    // Reconnecting, this side says it did make all ten durable.
    let mut peer = AsFollower::accept(&listener, 10).await;
    publish(&node, "again", 4).await;
    let (previous, _, _) = peer.frame().await.expect("nothing followed");
    assert_eq!(
        previous, 10,
        "the leader resent what it was told was durable"
    );

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 20. A leader that is stopped and started again reconciles from what its
/// log persisted, not from what it used to think.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_leader_reconciles_from_its_log() {
    let log = scratch("leader-restart");
    let (listener, at) = AsFollower::listen().await;

    let node = Runtime::start(leader_node(&log, at)).unwrap();
    publish(&node, "publisher", 8).await;
    let mut peer = AsFollower::accept(&listener, 0).await;
    let first = collect(&mut peer, 0, 8).await;
    peer.report(8).await;
    heard(&node, 8).await;
    peer.kill();
    node.shutdown().await;

    // Same log, new process state. The follower claims what it has.
    let node = Runtime::start(leader_node(&log, at)).unwrap();
    publish(&node, "after", 5).await;
    let mut peer = AsFollower::accept(&listener, 8).await;
    let second = collect(&mut peer, 8, 13).await;

    assert_eq!(second.len(), body(&log).len() - first.len());
    let mut whole = first;
    whole.extend_from_slice(&second);
    assert_eq!(whole, body(&log), "the suffix did not continue the prefix");

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 21. A follower that never answers must cost the leader a bounded amount
/// and change nothing a client sees.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_never_answers_is_bounded_and_invisible() {
    let log = scratch("absent");
    // Nothing is ever bound here, so every connection attempt is refused.
    let at = SocketAddr::from(([127, 0, 0, 1], 1));
    let node = Runtime::start(leader_node(&log, at)).unwrap();

    // The publishes are acknowledged as always: replication is not in the
    // acknowledgement path in this phase.
    publish(&node, "publisher", 200).await;

    let seen = node.metrics();
    assert_eq!(seen.replication_connected, 0);
    assert_eq!(seen.replication_follower_sequence, 0);
    assert_eq!(records(&log), 200, "the local log was affected");

    // The exact statement of boundedness: every batch the log committed
    // either fitted in the queue, whose capacity is fixed, or was counted as
    // dropped. Nothing was buffered anywhere else, and nothing grew with the
    // producer. What was dropped is in the log and the catch-up path reads
    // it back (docs/replication.md §119).
    let capacity = ReplicationSettings::default().queue as u64;
    assert!(
        seen.replication_shipments_dropped.saturating_add(capacity) >= seen.wal_commits,
        "{} commits, {} dropped, a queue of {capacity}: something else is holding batches",
        seen.wal_commits,
        seen.replication_shipments_dropped
    );

    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 22. A follower that reads but never acknowledges. The leader keeps
/// committing locally, the queue stays at its bound, and once the follower
/// starts answering it ends up level.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_follower_is_bounded_and_eventually_level() {
    let log = scratch("slow");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();

    let mut peer = AsFollower::accept(&listener, 0).await;
    publish(&node, "publisher", 400).await;

    // This side reads everything and says nothing, which is what a follower
    // whose disk is slower than the leader's looks like from here.
    let mut at_sequence = 0;
    while at_sequence < 400 {
        let Some((previous, last, _)) = peer.frame().await else {
            break;
        };
        assert_eq!(previous, at_sequence, "the leader left a hole");
        at_sequence = last;
    }
    assert_eq!(at_sequence, 400);
    assert_eq!(
        node.metrics().replication_follower_sequence,
        0,
        "unacknowledged frames advanced the leader's watermark"
    );

    // And now it answers.
    peer.report(400).await;
    heard(&node, 400).await;

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 23. The records a follower asks for are gone. It is not reset, not
/// discarded and not told it is synchronised: the link fails with a reason
/// somebody has to act on (docs/replication.md §130).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_past_the_end_of_the_history_is_refused_explicitly() {
    let log = scratch("toofar");
    let (listener, at) = AsFollower::listen().await;
    let mut config = leader_node(&log, at);
    // Rewrite the log at the first opportunity, retaining no bytes for the
    // follower: it is further behind than the retention budget from the
    // first record on, so the records really do go (D-18).
    config.wal.compact_bytes = 1;
    config.wal.retain_bytes = 0;
    let node = Runtime::start(config).unwrap();

    // Clean sessions, so nothing is owed these publishes and compaction
    // drops every one of them.
    publish(&node, "publisher", 30).await;
    // Waited on the file, not on the compaction counter. The first rewrite
    // fires while publishes are still arriving, so "one compaction has
    // happened" does not mean "the log is empty" — it only looked that way
    // while this helper took five seconds to return.
    //
    // Both are waited for, not one waited for and the other asserted: the
    // rewrite replaces the file and only then counts itself, so a reader
    // that sees the empty file can still see the counter at zero. Under load
    // that window was hit three runs in eight.
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while records(&log) != 0 || node.metrics().wal_compactions == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the records were still there"
        );
        tokio::task::yield_now().await;
    }

    // A follower that wants them from the beginning cannot have them.
    let mut peer = AsFollower::accept(&listener, 0).await;
    assert!(peer.frame().await.is_none(), "the leader sent something");

    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().replication_errors == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no error was raised"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(
        node.metrics().replication_follower_sequence,
        0,
        "a follower that could not be served was counted as served"
    );

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 21. The leader starting first. It cannot connect, retries, and the link
/// comes up when the follower does — with no task left waiting on anything
/// once both stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_that_starts_first_waits_for_the_follower() {
    let leader_log = scratch("order-leader");
    let follower_log = scratch("order-follower");

    // Bound, then dropped: the port is very likely still free, and if it is
    // not the leader simply keeps retrying until the follower binds it.
    let at = {
        let (listener, at) = AsFollower::listen().await;
        drop(listener);
        at
    };

    let primary = Runtime::start(leader_node(&leader_log, at)).unwrap();
    publish(&primary, "publisher", 10).await;

    let mut standby = follower_node(&follower_log);
    standby.replication.listen = Some(at);
    let standby = Runtime::start(standby).unwrap();

    heard(&primary, 10).await;

    primary.shutdown().await;
    standby.shutdown().await;
    assert_eq!(body(&follower_log), body(&leader_log));
    let _ = std::fs::remove_file(&leader_log);
    let _ = std::fs::remove_file(&follower_log);
}

/// Real backpressure: a follower that stops reading, so the socket buffers
/// fill, the leader's write blocks, and the bounded queue behind it reaches
/// its bound (docs/replication.md §123).
///
/// A follower that is merely slow to acknowledge still drains its socket and
/// never puts pressure anywhere. This one reads nothing at all after the
/// handshake, which is the only way to reach the queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_stops_reading_fills_the_queue_and_still_loses_nothing() {
    let log = scratch("backpressure");
    let (listener, at) = AsFollower::listen().await;
    let mut config = leader_node(&log, at);
    // The producer has to outrun the stalled consumer, and two fsyncs per
    // batch would let the consumer keep up by accident. What is under test
    // is the queue, not the disk.
    config.durability = Durability::Written;
    let node = Runtime::start(config).unwrap();

    // Accept, say where we are, and then read nothing ever again.
    let mut peer = AsFollower::accept(&listener, 0).await;

    let messages = 3_000;
    let payload = 16 * 1024;
    publish_sized(&node, "publisher", messages, payload).await;

    let stalled = node.metrics();
    let capacity = ReplicationSettings::default().queue as u64;

    // The queue reached its bound. Past it, batches were refused rather than
    // buffered — which is the property, and the earlier tests never got far
    // enough to show it.
    assert!(
        stalled.replication_shipments_dropped > 0,
        "{} commits and a queue of {capacity}, yet nothing was refused: \
         the socket absorbed everything and this tested nothing",
        stalled.wal_commits
    );
    assert_eq!(
        stalled.replication_queued, capacity,
        "the queue is not sitting at its bound"
    );

    // And it is not growing with the producer. What was accepted and is no
    // longer queued went into the kernel's socket buffer, which is not this
    // process's memory; if the queue were unbounded this difference would be
    // in the thousands rather than a handful over its capacity.
    let accepted = stalled
        .wal_commits
        .saturating_sub(stalled.replication_shipments_dropped);
    assert!(
        accepted <= capacity * 2,
        "{} of {} commits were accepted with a queue of {capacity}: it grew",
        accepted,
        stalled.wal_commits
    );

    // And none of it reached the client: local durability is untouched.
    assert_eq!(records(&log), messages as u64);
    assert_eq!(stalled.replication_follower_sequence, 0);

    // Now start reading. Everything the queue dropped has to come back out
    // of the log, in order and without a hole.
    let mut seen: Vec<u8> = Vec::new();
    let mut reached = 0u64;
    while reached < messages as u64 {
        let Some((previous, last, batch)) = peer.frame().await else {
            panic!("the leader stopped at {reached} of {messages}");
        };
        assert_eq!(previous, reached, "a gap opened at {reached}");
        seen.extend_from_slice(&batch);
        reached = last;
        peer.report(reached).await;
    }

    heard(&node, messages as u64).await;
    assert_eq!(
        seen,
        body(&log),
        "what came back is not what the leader's log holds"
    );
    assert!(
        node.metrics().replication_catchups > 0,
        "the gap closed without the log being read back"
    );

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

// ------------------------------------------------------------------ quorum
// The level exists to make a PUBACK wait for somebody else's disk. These
// two tests are the claim: one that it arrives when the follower reports,
// one that it does not when the follower stays silent.

fn quorum_leader(path: &Path, ships_to: SocketAddr) -> RuntimeConfig {
    RuntimeConfig {
        durability: Durability::Quorum,
        replication: ReplicationSettings {
            follower: Some(ships_to),
            required_copies: 2,
            ..ReplicationSettings::default()
        },
        ..base(path)
    }
}

/// Publishes in the background and counts the PUBACKs as they arrive.
///
/// The counter is shared rather than returned at the end, so a test can
/// read it at a moment it chose rather than after waiting a while and
/// hoping (CONSTITUTION.md §21).
fn publishing(broker: &Runtime, count: usize) -> Arc<AtomicUsize> {
    publishing_as(broker, "quorum", count)
}

fn publishing_as(broker: &Runtime, name: &str, count: usize) -> Arc<AtomicUsize> {
    let addr = broker.local_addr();
    let name = name.to_owned();
    let acked = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&acked);
    tokio::spawn(async move {
        let mut options = rumqttc::MqttOptions::new(name, addr.ip().to_string(), addr.port());
        options.set_keep_alive(Duration::from_secs(30));
        let (client, mut eventloop) = rumqttc::AsyncClient::new(options, 64);
        let driving = tokio::spawn(async move {
            while let Ok(event) = eventloop.poll().await {
                if matches!(
                    event,
                    rumqttc::Event::Incoming(rumqttc::Incoming::PubAck(_))
                ) {
                    counting.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        for _ in 0..count {
            if client
                .publish(
                    "quorum/topic",
                    rumqttc::QoS::AtLeastOnce,
                    false,
                    vec![b'x'; 32],
                )
                .await
                .is_err()
            {
                break;
            }
        }
        // Held open: the test decides when it has seen enough.
        let _ = driving.await;
    });
    acked
}

/// Waits for a count to reach `want`, with a deadline only so a broken run
/// fails instead of hanging.
async fn acked(counter: &AtomicUsize, want: usize) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while counter.load(Ordering::Relaxed) < want {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} of {want} publishes were acknowledged",
            counter.load(Ordering::Relaxed)
        );
        tokio::task::yield_now().await;
    }
}

/// A follower that reports: the publishes are acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quorum_publish_is_acknowledged_once_the_follower_has_it() {
    let log = scratch("quorum-yes");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(quorum_leader(&log, at)).unwrap();

    let mut peer = AsFollower::accept(&listener, 0).await;
    let counter = publishing(&node, 20);

    // Take what the leader sends and report it durable, as a real follower
    // does after its own sync_data.
    let mut reached = 0u64;
    while reached < 20 {
        let Some((_, last, _)) = peer.frame().await else {
            break;
        };
        reached = last;
        peer.report(reached).await;
    }
    heard(&node, 20).await;
    acked(&counter, 20).await;

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// **The test that makes the level mean something.** The follower takes
/// everything and acknowledges nothing. The leader's own log is synced, so
/// `fsynced` would have acknowledged every one of these; `quorum` must not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quorum_publish_is_not_acknowledged_while_the_follower_is_silent() {
    let log = scratch("quorum-no");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(quorum_leader(&log, at)).unwrap();

    let mut peer = AsFollower::accept(&listener, 0).await;
    let counter = publishing(&node, 20);

    // Drain the socket so nothing is blocked on backpressure, and report
    // nothing at all.
    let mut taken = 0;
    while taken < 20 {
        let Some((_, last, _)) = peer.frame().await else {
            break;
        };
        taken = last as usize;
    }

    // A deterministic moment, not a pause: a record is committed and
    // synced locally *before* its batch is shipped, so having received the
    // frame covering all twenty means `fsynced` would already have
    // acknowledged every one of them.
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "a publish was acknowledged on the leader's own disk alone"
    );
    // The records really are on the leader: this is not a broker that
    // failed to write, it is one that wrote and did not promise.
    assert_eq!(records(&log), 20);
    assert_eq!(node.metrics().replication_follower_sequence, 0);

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// Waits for this node's own log to have made `want` records durable. The
/// moment `fsynced` would have answered.
async fn locally_durable(node: &Runtime, want: u64) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().wal_durable_sequence < want {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the log reached {}, not {want}",
            node.metrics().wal_durable_sequence
        );
        tokio::task::yield_now().await;
    }
}

/// The whole cycle: in sync, the peer disappears, no new quorum, the peer
/// returns, catch-up, in sync again — and nothing acknowledged along the
/// way is ever taken back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_a_quorum_stops_new_acknowledgements_and_withdraws_none() {
    let log = scratch("quorum-cycle");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(quorum_leader(&log, at)).unwrap();

    // ── in sync ───────────────────────────────────────────────────────
    let mut peer = AsFollower::accept(&listener, 0).await;
    let first = publishing_as(&node, "before", 10);
    let mut reached = 0u64;
    while reached < 10 {
        let Some((_, last, _)) = peer.frame().await else {
            break;
        };
        reached = last;
        peer.report(reached).await;
    }
    heard(&node, 10).await;
    acked(&first, 10).await;

    // ── the peer disappears ───────────────────────────────────────────
    peer.kill();

    let second = publishing_as(&node, "during", 10);
    // The leader commits and syncs these on its own disk. At that moment
    // `fsynced` would have acknowledged all ten.
    locally_durable(&node, 20).await;
    assert_eq!(
        second.load(Ordering::Relaxed),
        0,
        "a publish was acknowledged with no quorum to acknowledge it"
    );

    // The quorum is lost, and that is not a broken log: the broker kept
    // writing, and nothing failed.
    let hurt = node.metrics();
    assert_eq!(
        hurt.wal_commit_failures, 0,
        "a lost quorum poisoned the log"
    );
    assert_eq!(records(&log), 20, "the broker stopped writing");

    // And what was already promised stays promised.
    assert_eq!(first.load(Ordering::Relaxed), 10, "a PUBACK was withdrawn");

    // ── the peer returns ──────────────────────────────────────────────
    let mut peer = AsFollower::accept(&listener, 10).await;
    let mut reached = 10u64;
    while reached < 20 {
        let Some((previous, last, _)) = peer.frame().await else {
            break;
        };
        // Catch-up, continuing exactly where this side said it was.
        assert_eq!(
            previous, reached,
            "the leader left a hole after reconnecting"
        );
        reached = last;
        peer.report(reached).await;
    }
    heard(&node, 20).await;

    // ── in sync again ─────────────────────────────────────────────────
    acked(&second, 10).await;
    assert_eq!(
        first.load(Ordering::Relaxed),
        10,
        "recovering acknowledged something twice"
    );
    assert!(
        node.metrics().replication_catchups > 0,
        "nothing was read back"
    );

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// Acknowledged, then the peer vanishes, then it comes back. The point is
/// narrow and worth its own test: durability once declared is not a claim
/// that can be revisited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acknowledgement_survives_the_replica_that_established_it() {
    let log = scratch("quorum-irrevocable");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(quorum_leader(&log, at)).unwrap();

    let mut peer = AsFollower::accept(&listener, 0).await;
    let counter = publishing_as(&node, "promised", 6);
    let mut reached = 0u64;
    while reached < 6 {
        let Some((_, last, _)) = peer.frame().await else {
            break;
        };
        reached = last;
        peer.report(reached).await;
    }
    heard(&node, 6).await;
    acked(&counter, 6).await;
    let promised = node.metrics().replication_follower_sequence;
    assert_eq!(promised, 6);

    peer.kill();
    // The replica that made it durable is gone. The statement is not.
    assert_eq!(node.metrics().replication_follower_sequence, 6);
    assert_eq!(counter.load(Ordering::Relaxed), 6);

    // It comes back claiming less than it had — a replica rebuilt from an
    // older copy. The leader resends, and the position it already declared
    // does not move down to meet it.
    let mut peer = AsFollower::accept(&listener, 3).await;
    let Some((previous, _, _)) = peer.frame().await else {
        panic!("the leader sent nothing to a follower that was behind");
    };
    assert_eq!(
        previous, 3,
        "the leader did not resend from where it was told"
    );
    assert_eq!(
        counter.load(Ordering::Relaxed),
        6,
        "an acknowledgement was withdrawn when the replica came back smaller"
    );

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

// ---------------------------------------------------------------------------
// The retention floor (docs/replication.md §130).
//
// Row 23 above compacts a log down to nothing, which is the easy half: with
// no records left, any arithmetic refuses. The half that broke is a rewrite
// with survivors. Compaction keeps a filtered set, not a prefix, so a leader
// that labelled the records it still holds by their position in the file
// would ship frames whose ranges name records the bytes are not — and the
// follower would accept them, because nothing in the frame disagrees.
// ---------------------------------------------------------------------------

/// A log of ten records rewritten down to the ones written as 1, 5 and 9.
///
/// Its floor is 11: everything still in the file is below it. Returns how
/// many bytes of records the survivors occupy, which is where a frame
/// serving this log from the floor has to start.
fn compacted_log(path: &Path) -> usize {
    let mut wal = storage::Wal::open(path, storage::WalLimits::default())
        .unwrap()
        .0;
    for index in 0..10 {
        wal.stage(format!("old-{index}").as_bytes()).unwrap();
    }
    assert_eq!(wal.commit().unwrap().0.durable_sequence, 10);

    let mut seen = 0u64;
    wal.compact(0, storage::Retain::NONE, &mut |_| {
        seen += 1;
        matches!(seen, 1 | 5 | 9)
    })
    .unwrap();
    drop(wal);

    assert_eq!(records(path), 3, "the fixture is not the case under test");
    assert_eq!(storage::allocated(path).unwrap().retention_floor(), 11);
    body(path).len()
}

/// A follower reporting `holds` gets no frames and an error on the leader.
async fn refused_at(name: &str, holds: u64) {
    let log = scratch(name);
    compacted_log(&log);

    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();
    // The leader's live position only exists once it has committed
    // something, so give it one record: 11, the first it can still name.
    publish(&node, "publisher", 1).await;

    let mut peer = AsFollower::accept(&listener, holds).await;

    // Serving the survivors as 1, 5, 9 — or, as the arithmetic that broke
    // would have had it, as 8, 9, 10 — is what this asserts did not happen.
    assert!(
        peer.frame().await.is_none(),
        "the leader served a range it cannot name, asked for {}",
        holds + 1
    );

    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().replication_errors == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no error was raised"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(
        node.metrics().replication_follower_sequence,
        0,
        "a follower that could not be served was counted as served"
    );
    // Under `fsynced` nothing waits for this follower, so its refusal stops
    // no acknowledgement and is not reported as one (D-8).
    assert_eq!(node.metrics().quorum_unavailable, 0);

    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 24. A follower asks for a sequence the log kept the bytes of but not the
/// name of. Below the floor there is no answer, only a refusal.
///
/// Every position below the floor, not just the first: 1, 5 and 9 are the
/// sequences the surviving records were written under, and 10 is the last
/// one the log allocated before the rewrite. A reader that picked records
/// out of the file by position would answer all four.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_below_the_retention_floor_is_refused() {
    for holds in [0, 4, 8, 9] {
        refused_at(&format!("floor-refused-{holds}"), holds).await;
    }
}

/// 25. The same log, and a follower that asks for exactly the floor. That is
/// a position the log can still name, so it is served — and the range says
/// 10, not the survivor count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_at_the_retention_floor_is_served() {
    let log = scratch("floor-served");
    let survivors = compacted_log(&log);

    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();
    publish(&node, "publisher", 3).await;

    // Holds everything the rewrite accounted for, so it asks for 11.
    let mut peer = AsFollower::accept(&listener, 10).await;

    let (previous, last, sent) = peer.frame().await.expect("nothing was served");
    assert_eq!(previous, 10, "the range did not start at the floor");
    assert!(last > previous, "the frame covers nothing");

    // And the bytes are the ones appended after the rewrite, not the three
    // the rewrite kept.
    let appended = &body(&log)[survivors..];
    assert_eq!(
        sent,
        appended[..sent.len()],
        "the leader shipped records from below the floor"
    );

    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 26. A refused follower is tried again, and once an operator has given it
/// a fresh copy of the log it is served — without restarting the leader
/// (D-23). Until D-18 and D-20 the leader stopped for good here (D-2): a
/// retrying leader held the log for a follower it could not serve. Nothing
/// holds the log for a refused follower any more, so a retry costs a
/// connection and one header read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_follower_is_served_once_it_has_a_fresh_copy() {
    let log = scratch("floor-retry");
    compacted_log(&log);

    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();
    publish(&node, "publisher", 1).await;

    // Below the floor, twice: refused both times, and dialled again after
    // the first.
    for refusals in 1..=2 {
        let mut peer = AsFollower::accept(&listener, 0).await;
        assert!(
            peer.frame().await.is_none(),
            "the leader served a lost range"
        );
        let deadline = tokio::time::Instant::now() + DEADLINE;
        while node.metrics().replication_errors < refusals {
            assert!(tokio::time::Instant::now() < deadline, "no refusal");
            tokio::task::yield_now().await;
        }
    }

    // The operator's fresh copy holds everything the rewrite accounted for:
    // 10, so it asks for the floor and is served from it.
    let mut peer = AsFollower::accept(&listener, 10).await;
    let (previous, _, _) = peer.frame().await.expect("the leader did not come back");
    assert_eq!(previous, 10);
    assert_eq!(node.metrics().replication_errors, 2);

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 27. A leader restarted on a log whose two identity spaces have drifted
/// apart publishes the WAL sequence, not the correlation high-water.
///
/// The spaces are independent (docs/storage.md, location-vs-identity) and
/// coincide only in a run where every record is a publish and nothing has
/// been rewritten. Here they are made to differ by 692, so a leader that
/// continued the wrong one is not off by a rounding error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_leader_publishes_its_sequence_not_its_correlation() {
    let log = scratch("two-spaces");
    {
        let mut wal = storage::Wal::open(&log, storage::WalLimits::default())
            .unwrap()
            .0;
        for index in 0..5 {
            wal.stage(format!("old-{index}").as_bytes()).unwrap();
        }
        wal.commit().unwrap();
        // The rewrite writes a correlation high-water of its own choosing;
        // the sequence carries on from 5 either way.
        wal.compact(700, storage::Retain::NONE, &mut |_| true)
            .unwrap();
        for index in 0..3 {
            wal.stage(format!("more-{index}").as_bytes()).unwrap();
        }
        assert_eq!(wal.commit().unwrap().0.durable_sequence, 8);
    }
    assert_eq!(storage::allocated(&log).unwrap().correlation, 700);

    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();

    // The follower holds everything: 8 sequences, not 700 correlations.
    let mut peer = AsFollower::accept(&listener, 8).await;
    publish(&node, "publisher", 1).await;

    // A leader that thought it held 700 would see this follower as 692
    // behind and read its own log to fill a gap that is not there.
    let (previous, last, _) = peer.frame().await.expect("nothing was shipped");
    assert_eq!(
        (previous, last),
        (8, 9),
        "the live batch did not continue the log's own sequence"
    );
    assert_eq!(
        node.metrics().replication_catchups,
        0,
        "the leader caught a follower up that was already in sync"
    );

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

// ---------------------------------------------------------------------------
// The pending-acknowledgement bound (C8, D-7, docs/memory.md §9).
//
// The broker holds one entry per publish it has routed and not yet
// acknowledged. `STRAUM_MAX_AWAITING_QUORUM` counts commits, and a commit
// holds any number of publishes, so it never bounded this. The limit is
// counted in publishes and taken as a permit before a publish that will be
// acknowledged goes to the log: the publisher waits at its own reader, and
// nobody else waits at all.
// ---------------------------------------------------------------------------

async fn pending_reaches(node: &Runtime, want: u64) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().pending_acknowledgements != want {
        assert!(
            tokio::time::Instant::now() < deadline,
            "pending acknowledgements at {}, not {want}",
            node.metrics().pending_acknowledgements
        );
        tokio::task::yield_now().await;
    }
}

/// 28. The bound is the configured number of publishes, it holds before the
/// log rather than after it, and nothing is lost to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_acknowledgements_stop_at_the_limit_and_lose_nothing() {
    let log = scratch("pending-bound");
    let (listener, at) = AsFollower::listen().await;
    let mut config = quorum_leader(&log, at);
    config.max_pending_durable = 20;
    let node = Runtime::start(config).unwrap();

    let mut peer = AsFollower::accept(&listener, 0).await;
    let counter = publishing(&node, 100);

    pending_reaches(&node, 20).await;
    locally_durable(&node, 20).await;
    // Twenty in the log, not a hundred: the twenty-first publisher waits for
    // a permit before its record is written, so the log holds exactly what
    // the broker may acknowledge. Nothing can add a twenty-first record
    // until a permit comes back, and none does without a quorum.
    assert_eq!(
        records(&log),
        20,
        "the bound let records past it into the log"
    );
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "a publish was acknowledged without a quorum"
    );

    // The quorum arrives, a batch at a time: each confirmation returns
    // permits, the next publishes are admitted and shipped, and every one
    // of the hundred is acknowledged in the end.
    // Stops at what was shipped, not at what was acknowledged: the last
    // acknowledgements arrive after the last report, and a loop that waited
    // on them would ask for a frame that is never coming.
    let mut reported = 0;
    while reported < 100 {
        let (_, last, _) = peer.frame().await.expect("the leader stopped shipping");
        if last > reported {
            reported = last;
            peer.report(reported).await;
        }
    }
    acked(&counter, 100).await;
    pending_reaches(&node, 0).await;
    assert_eq!(records(&log), 100);

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 29. Under `fsynced` the reports that release acknowledgements arrive on
/// their own channel, so a bound of two cannot stop them, and the permits
/// come back as fast as the disk confirms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tiny_pending_bound_does_not_wedge_fsynced() {
    let log = scratch("pending-tiny");
    let mut config = base(&log);
    config.max_pending_durable = 2;
    let node = Runtime::start(config).unwrap();

    // `publish` waits for every PUBACK.
    publish(&node, "publisher", 200).await;
    pending_reaches(&node, 0).await;

    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 30. A bound that holds forever — a quorum nobody will confirm — must
/// still let the broker stop. The publishers are parked waiting for a permit
/// that will not come; shutdown ends them like any other wait (§16).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_bound_does_not_stop_shutdown() {
    let log = scratch("pending-shutdown");
    let (listener, at) = AsFollower::listen().await;
    let mut config = quorum_leader(&log, at);
    config.max_pending_durable = 5;
    let node = Runtime::start(config).unwrap();

    let peer = AsFollower::accept(&listener, 0).await;
    let _counter = publishing(&node, 50);
    pending_reaches(&node, 5).await;

    // With the default grace of five seconds. Shutdown closes the permits
    // first, so the parked publisher ends at once instead of being
    // cancelled when the grace runs out (D-10).
    assert_eq!(
        RuntimeConfig::default().shutdown_grace,
        Duration::from_secs(5),
        "the row assumes the default grace"
    );
    let started = std::time::Instant::now();
    assert!(
        tokio::time::timeout(DEADLINE, node.shutdown())
            .await
            .is_ok(),
        "shutdown waited for publishers parked on the bound"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "shutdown took {:?}: it waited out the grace period for a parked publisher",
        started.elapsed()
    );
    peer.kill();
    let _ = std::fs::remove_file(&log);
}

/// 31. The bound holds publishers, not the broker. While it is full and a
/// quorum is not coming, another client can still subscribe, and a QoS 0
/// message still reaches it — before D-7 the whole command loop stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_bound_does_not_stop_other_clients() {
    use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};

    let log = scratch("pending-others");
    let (listener, at) = AsFollower::listen().await;
    let mut config = quorum_leader(&log, at);
    config.max_pending_durable = 5;
    let node = Runtime::start(config).unwrap();

    let peer = AsFollower::accept(&listener, 0).await;
    let _counter = publishing(&node, 50);
    pending_reaches(&node, 5).await;

    let addr = node.local_addr();
    let (subscriber, mut inbox) = AsyncClient::new(
        MqttOptions::new("bystander", addr.ip().to_string(), addr.port()),
        16,
    );
    subscriber
        .subscribe("free/topic", QoS::AtMostOnce)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + DEADLINE;
    loop {
        let event = tokio::time::timeout_at(deadline, inbox.poll())
            .await
            .expect("the subscription was never answered")
            .expect("another client could not even connect while the bound was full");
        if let Event::Incoming(Incoming::SubAck(_)) = event {
            break;
        }
    }

    let (talker, mut talker_loop) = AsyncClient::new(
        MqttOptions::new("talker", addr.ip().to_string(), addr.port()),
        16,
    );
    talker
        .publish("free/topic", QoS::AtMostOnce, false, "still moving")
        .await
        .unwrap();
    let talking = tokio::spawn(async move { while talker_loop.poll().await.is_ok() {} });

    loop {
        let event = tokio::time::timeout_at(deadline, inbox.poll())
            .await
            .expect("a QoS 0 message never arrived while the bound was full")
            .unwrap();
        if let Event::Incoming(Incoming::Publish(message)) = event {
            assert_eq!(message.payload.as_ref(), b"still moving");
            break;
        }
    }
    // And the bound is still exactly where it was.
    assert_eq!(node.metrics().pending_acknowledgements, 5);

    talking.abort();
    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 32. A quorum that can no longer form is said once, as a state, and
/// changes nothing else (D-8): no PUBACK, the log is not poisoned, the
/// publishes are written and wait. Once the follower has a fresh copy it is
/// served again, the state clears, and what waited is acknowledged (D-23).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreachable_quorum_is_reported_and_withholds_acknowledgements() {
    let log = scratch("quorum-gone");
    compacted_log(&log);

    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(quorum_leader(&log, at)).unwrap();
    assert_eq!(node.metrics().quorum_unavailable, 0);

    // Below the floor: refused.
    AsFollower::accept(&listener, 0).await.kill();
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().quorum_unavailable == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "an unreachable quorum was never reported"
        );
        tokio::task::yield_now().await;
    }

    // Publishes still reach this node's log — the log is fine — and none
    // is acknowledged, because `quorum` promised more than this node alone.
    let counter = publishing(&node, 3);
    locally_durable(&node, 13).await;
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "acknowledged without a quorum"
    );
    assert_eq!(
        node.metrics().wal_commit_failures,
        0,
        "the log was treated as failed"
    );
    assert_eq!(node.metrics().quorum_unavailable, 1);

    // The operator's fresh copy holds through 10. The leader is still
    // dialling, serves it from the floor, and the quorum forms.
    let mut peer = AsFollower::accept(&listener, 10).await;
    let mut reached = 10u64;
    while reached < 13 {
        let Some((previous, last, _)) = peer.frame().await else {
            panic!("the leader ended the link");
        };
        assert_eq!(previous, reached, "the leader left a hole");
        reached = last;
        peer.report(reached).await;
    }
    acked(&counter, 3).await;
    assert_eq!(node.metrics().quorum_unavailable, 0);

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

// ---------------------------------------------------------------------------
// Retention (D-18, D-19, D-20, docs/replication.md §130).
//
// A rewrite keeps a suffix of the log readable by the numbers it was written
// under. Without a follower it keeps none; with one that acknowledgements do
// not wait for, the last `retain_bytes`; under `quorum`, everything past the
// follower's own reports.
// ---------------------------------------------------------------------------

/// A leader that rewrites its log after every commit, keeping `retain_bytes`
/// addressable for its follower.
fn retaining(path: &Path, ships_to: SocketAddr, retain_bytes: u64) -> RuntimeConfig {
    let mut config = leader_node(path, ships_to);
    config.wal.compact_bytes = 1;
    config.wal.retain_bytes = retain_bytes;
    config
}

/// Waits for the leader to have rewritten its log at least `want` times.
async fn compacted(node: &Runtime, want: u64) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().wal_compactions < want {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the log was rewritten {} times, not {want}",
            node.metrics().wal_compactions
        );
        tokio::task::yield_now().await;
    }
}

/// 33. A follower that is behind by less than the retained bytes is caught
/// up after the log was rewritten — from the same numbers, with the same
/// bytes. Without retention every one of these publishes is owed to nobody
/// and the rewrite would have dropped them, as row 23 shows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_within_the_retained_bytes_is_caught_up_after_a_rewrite() {
    let log = scratch("retain-within");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(retaining(&log, at, 1 << 20)).unwrap();

    publish(&node, "publisher", 20).await;
    compacted(&node, 1).await;
    assert_eq!(node.metrics().wal_retention_floor, 1);

    let mut peer = AsFollower::accept(&listener, 0).await;
    let sent = collect(&mut peer, 0, 20).await;
    peer.report(20).await;
    assert_eq!(sent, body(&log), "the suffix was not served as written");
    heard(&node, 20).await;
    assert_eq!(node.metrics().replication_followers_lost, 0);

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 34. A follower the leader has heard from falls further behind than the
/// retained bytes. That is said once, when a rewrite passes it — not at
/// every rewrite after — and when it comes back it is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_beyond_the_retained_bytes_is_reported_and_refused() {
    let log = scratch("retain-beyond");
    let (listener, at) = AsFollower::listen().await;
    // Room for a few records of this size, not for twenty.
    let node = Runtime::start(retaining(&log, at, 256)).unwrap();

    // The leader hears the follower is at 0, then the follower is gone.
    let peer = AsFollower::accept(&listener, 0).await;
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().replication_connected == 0 {
        assert!(tokio::time::Instant::now() < deadline, "no link came up");
        tokio::task::yield_now().await;
    }
    peer.kill();

    publish(&node, "publisher", 20).await;
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().replication_followers_lost == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the floor passed the follower and nobody said so"
        );
        tokio::task::yield_now().await;
    }
    assert!(node.metrics().wal_retention_floor > 1);

    // More rewrites, the follower no further along: still one.
    let before = node.metrics().wal_compactions;
    publish(&node, "more", 5).await;
    compacted(&node, before + 1).await;
    assert_eq!(node.metrics().replication_followers_lost, 1);

    let mut peer = AsFollower::accept(&listener, 0).await;
    assert!(
        peer.frame().await.is_none(),
        "the leader served a lost range"
    );
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().replication_errors == 0 {
        assert!(tokio::time::Instant::now() < deadline, "no refusal");
        tokio::task::yield_now().await;
    }

    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 35. The leader restarts while a follower is behind by less than the
/// retained bytes. The rewrite at startup keeps what the follower needs, so
/// it is caught up exactly as if the leader had kept running (D-19). The
/// startup rewrite used to check the file size alone and drop it all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_leader_still_serves_a_follower_within_the_retained_bytes() {
    let log = scratch("retain-restart");
    let (_absent, at) = AsFollower::listen().await;
    let node = Runtime::start(retaining(&log, at, 1 << 20)).unwrap();
    publish(&node, "publisher", 20).await;
    node.shutdown().await;

    // A listener of its own: the first one may still hold the old leader's
    // dead connection in its backlog.
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(retaining(&log, at, 1 << 20)).unwrap();
    assert_eq!(node.metrics().wal_retention_floor, 1);

    let mut peer = AsFollower::accept(&listener, 0).await;
    let sent = collect(&mut peer, 0, 20).await;
    peer.report(20).await;
    assert_eq!(sent, body(&log));
    heard(&node, 20).await;

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 36. Without a follower nothing is retained, and the log stays within
/// the live set plus the threshold plus one batch — however much is
/// written through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_follower_the_log_stays_within_the_threshold() {
    let log = scratch("retain-none");
    let mut config = base(&log);
    let threshold = 4096u64;
    config.wal.compact_bytes = threshold;
    // Ignored without a follower; set so the row fails if it is not.
    config.wal.retain_bytes = 1 << 20;
    let node = Runtime::start(config).unwrap();

    // About five times the threshold, owed to nobody.
    publish(&node, "publisher", 400).await;
    compacted(&node, 2).await;
    let size = std::fs::metadata(&log).unwrap().len();
    // One batch is at most the client's window of 64 publishes.
    let batch = 64 * u64::try_from(record(&[b'x'; 64]).len()).unwrap();
    assert!(
        size <= storage::HEADER_BYTES as u64 + threshold + batch,
        "the log is {size} bytes"
    );
    assert_eq!(node.metrics().replication_followers_lost, 0);

    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 37. Under `quorum` the follower is away while more than `retain_bytes`
/// is written and the log is rewritten again and again. It comes back, is
/// caught up from where it said it was, and the quorum resumes — nobody
/// re-initialised anything (D-20). The byte budget plays no part.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quorum_follower_away_for_longer_than_the_budget_resumes_on_its_own() {
    let log = scratch("quorum-outage");
    let (listener, at) = AsFollower::listen().await;
    let mut config = quorum_leader(&log, at);
    config.wal.compact_bytes = 1;
    config.wal.retain_bytes = 1;
    let node = Runtime::start(config).unwrap();

    let mut peer = AsFollower::accept(&listener, 0).await;
    let first = publishing_as(&node, "before", 5);
    let mut reached = 0u64;
    while reached < 5 {
        let Some((_, last, _)) = peer.frame().await else {
            break;
        };
        reached = last;
        peer.report(reached).await;
    }
    acked(&first, 5).await;
    peer.kill();

    let before = node.metrics().wal_compactions;
    let second = publishing_as(&node, "during", 20);
    locally_durable(&node, 25).await;
    compacted(&node, before + 1).await;
    assert_eq!(second.load(Ordering::Relaxed), 0);
    assert_eq!(node.metrics().wal_retention_floor, 6);

    let mut peer = AsFollower::accept(&listener, 5).await;
    let mut reached = 5u64;
    while reached < 25 {
        let Some((previous, last, _)) = peer.frame().await else {
            break;
        };
        assert_eq!(previous, reached, "the leader left a hole");
        reached = last;
        peer.report(reached).await;
    }
    acked(&second, 20).await;
    let seen = node.metrics();
    assert_eq!(seen.quorum_unavailable, 0);
    assert_eq!(seen.replication_followers_lost, 0);

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 38. Under `quorum` the leader restarts while the follower is behind. The
/// startup rewrite keeps everything still addressable until the follower
/// has spoken, so it is caught up and the quorum forms again (D-19, D-20).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_quorum_leader_still_serves_its_follower() {
    let log = scratch("quorum-restart");
    let (listener, at) = AsFollower::listen().await;
    let mut config = quorum_leader(&log, at);
    config.wal.compact_bytes = 1;
    let node = Runtime::start(config.clone()).unwrap();

    let mut peer = AsFollower::accept(&listener, 0).await;
    let first = publishing_as(&node, "before", 5);
    let mut reached = 0u64;
    while reached < 5 {
        let Some((_, last, _)) = peer.frame().await else {
            break;
        };
        reached = last;
        peer.report(reached).await;
    }
    acked(&first, 5).await;
    peer.kill();

    // Written while the follower is away, never acknowledged.
    let _unacknowledged = publishing_as(&node, "during", 5);
    locally_durable(&node, 10).await;
    node.shutdown().await;

    let (listener, at) = AsFollower::listen().await;
    config.replication.follower = Some(at);
    let node = Runtime::start(config).unwrap();
    assert!(node.metrics().wal_retention_floor <= 6);

    // Caught up to what the log held at the restart first, and only then
    // anything new, one publish at a time. Not for this row's sake: catch-up
    // reads the file to its end and can ship a batch the leader has written
    // but not yet synced, and the quorum then waits for a report that never
    // comes. A defect of its own, found by this row in its first form.
    let mut peer = AsFollower::accept(&listener, 5).await;
    let mut reached = 5u64;
    while reached < 10 {
        let Some((previous, last, _)) = peer.frame().await else {
            break;
        };
        assert_eq!(previous, reached, "the leader left a hole");
        reached = last;
        peer.report(reached).await;
    }
    assert_eq!(
        reached, 10,
        "the restarted leader did not serve the follower"
    );
    for name in ["after-1", "after-2"] {
        let after = publishing_as(&node, name, 1);
        let Some((previous, last, _)) = peer.frame().await else {
            panic!("the leader stopped shipping");
        };
        assert_eq!(previous, reached, "the leader left a hole");
        reached = last;
        peer.report(reached).await;
        acked(&after, 1).await;
    }
    assert_eq!(node.metrics().quorum_unavailable, 0);

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 39. A follower is caught up while the last publishes are being written.
/// Catch-up reads the file, which holds a batch as soon as it is written and
/// before it is synced. Shipping from there let the follower report past
/// this node's own durable position; the watermark took the smaller of the
/// two, and with no later report it never moved again, so the last
/// publishes were never acknowledged (D-22). Found by row 38 in its first
/// form, which failed this way five runs in six. Catch-up now stops at what
/// this node has made durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_last_publishes_during_a_catch_up_are_acknowledged() {
    // The race is a matter of timing, so it is run several times: without
    // the bound, three runs in six of five rounds each failed here.
    for round in 0..10 {
        let log = scratch(&format!("quorum-catch-up-race-{round}"));
        let (listener, at) = AsFollower::listen().await;
        let mut config = quorum_leader(&log, at);
        let node = Runtime::start(config.clone()).unwrap();

        let mut peer = AsFollower::accept(&listener, 0).await;
        let first = publishing_as(&node, "before", 5);
        let mut reached = 0u64;
        while reached < 5 {
            let Some((_, last, _)) = peer.frame().await else {
                break;
            };
            reached = last;
            peer.report(reached).await;
        }
        acked(&first, 5).await;
        peer.kill();
        let _unacknowledged = publishing_as(&node, "away", 5);
        locally_durable(&node, 10).await;
        node.shutdown().await;

        // Restarted, the follower returns at 5, and two publishes race the
        // catch-up of the five it is missing. Nothing comes after them.
        let (listener, at) = AsFollower::listen().await;
        config.replication.follower = Some(at);
        let node = Runtime::start(config).unwrap();
        let mut peer = AsFollower::accept(&listener, 5).await;
        let racing = publishing_as(&node, "racing", 2);
        let acknowledged = acked(&racing, 2);
        tokio::pin!(acknowledged);
        let mut reached = 5u64;
        loop {
            tokio::select! {
                () = &mut acknowledged => break,
                frame = peer.frame() => {
                    let Some((previous, last, _)) = frame else {
                        panic!("the leader ended the link");
                    };
                    assert_eq!(previous, reached, "the leader left a hole");
                    reached = last;
                    peer.report(reached).await;
                }
            }
        }

        peer.kill();
        node.shutdown().await;
        let _ = std::fs::remove_file(&log);
    }
}

/// 40. A follower whose kernel accepted the connection and that never says
/// anything. Nothing is owed to it before its handshake, so shutting the
/// leader down does not wait for one (D-21). It used to wait for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_follower_does_not_hold_the_shutdown() {
    let log = scratch("silent-follower");
    // Bound and never accepted: the kernel completes the connection into
    // the backlog, and the leader waits for a handshake that never comes.
    let (_silent, at) = AsFollower::listen().await;
    let node = Runtime::start(leader_node(&log, at)).unwrap();
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().replication_connections == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the leader never connected"
        );
        tokio::task::yield_now().await;
    }

    tokio::time::timeout(DEADLINE, node.shutdown())
        .await
        .expect("the shutdown waited for a follower that never spoke");
    let _ = std::fs::remove_file(&log);
}

/// A leader whose writes to the follower stall after `stall`, publishing
/// fast enough to fill the socket buffers between them.
fn stalling_leader(path: &Path, ships_to: SocketAddr, stall: Duration) -> RuntimeConfig {
    let mut config = leader_node(path, ships_to);
    // The producer has to outrun the follower, which an fsync per batch
    // would not; the disk is not what is under test.
    config.durability = Durability::Written;
    config.write_stall_timeout = stall;
    // No rewrites: filling the socket can write past the threshold, and a
    // follower at 0 would then be refused for a reason that is not this one.
    config.wal.compact_bytes = 0;
    config
}

/// Publishes 64 KiB messages until `done` holds, with a deadline so a broken
/// run fails instead of hanging.
async fn publish_until(node: &Runtime, done: impl Fn(&Runtime) -> bool, what: &str) {
    let deadline = tokio::time::Instant::now() + DEADLINE;
    let mut round = 0;
    while !done(node) {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        publish_sized(node, &format!("filler-{round}"), 16, 64 * 1024).await;
        round += 1;
    }
}

/// 41. A follower that holds the connection and stops reading. A frame that
/// cannot leave within the write-stall limit ends the link, which is
/// counted and redialled, and the follower is caught up from the log once
/// it reads again (D-27). Until then it held the replication task for as
/// long as it pleased, and under `quorum` every acknowledgement with it,
/// without a signal anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_stops_reading_is_cut_off_and_caught_up() {
    let log = scratch("stalled-follower");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(stalling_leader(&log, at, Duration::from_millis(300))).unwrap();

    // Accept, say where we are, and then read nothing.
    let stalled = AsFollower::accept(&listener, 0).await;
    publish_until(
        &node,
        |node| node.metrics().replication_errors > 0,
        "a follower that stopped reading was never cut off",
    )
    .await;

    // The leader dials again; this time the follower reads, and gets
    // everything from the beginning out of the log.
    drop(stalled);
    let mut peer = AsFollower::accept(&listener, 0).await;
    let (previous, _, _) = peer
        .frame()
        .await
        .expect("nothing was sent after the stall");
    assert_eq!(
        previous, 0,
        "the leader did not catch up from where it was told"
    );

    peer.kill();
    node.shutdown().await;
    let _ = std::fs::remove_file(&log);
}

/// 42. The same follower, and a shutdown while its socket is full. The
/// stream does not watch the shutdown signal — it ships what was committed
/// (§120) — so what ends it is the write-stall limit (D-26, D-27).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_stops_reading_does_not_hold_the_shutdown() {
    let log = scratch("stalled-shutdown");
    let (listener, at) = AsFollower::listen().await;
    let node = Runtime::start(stalling_leader(&log, at, Duration::from_millis(300))).unwrap();

    let _stalled = AsFollower::accept(&listener, 0).await;
    // Enough that the queue is full, so the socket behind it is too.
    publish_until(
        &node,
        |node| node.metrics().replication_shipments_dropped > 0,
        "the follower's socket never filled",
    )
    .await;

    tokio::time::timeout(DEADLINE, node.shutdown())
        .await
        .expect("the shutdown waited for a follower that stopped reading");
    let _ = std::fs::remove_file(&log);
}
