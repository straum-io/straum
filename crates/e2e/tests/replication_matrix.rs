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
    use rumqttc::{AsyncClient, MqttOptions, QoS};
    let addr = broker.local_addr();
    let mut options = MqttOptions::new(name, addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(5));
    options.set_max_packet_size(payload + 1024, payload + 1024);
    let (client, mut eventloop) = AsyncClient::new(options, 64);

    let driving = tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });
    let body = vec![b'x'; payload];
    for _ in 0..count {
        client
            .publish("matrix/topic", QoS::AtLeastOnce, false, body.clone())
            .await
            .unwrap();
    }
    client.disconnect().await.unwrap();
    let _ = driving.await;
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
/// somebody has to act on (docs/replication.md §118).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_past_the_end_of_the_history_is_refused_explicitly() {
    let log = scratch("toofar");
    let (listener, at) = AsFollower::listen().await;
    let mut config = leader_node(&log, at);
    // Rewrite the log at the first opportunity. Nothing has ever connected,
    // so §118's hold does not apply and the records really do go.
    config.wal.compact_bytes = 1;
    let node = Runtime::start(config).unwrap();

    // Clean sessions, so nothing is owed these publishes and compaction
    // drops every one of them.
    publish(&node, "publisher", 30).await;
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while node.metrics().wal_compactions == 0 {
        assert!(tokio::time::Instant::now() < deadline, "nothing compacted");
        tokio::task::yield_now().await;
    }
    assert_eq!(records(&log), 0, "the records were still there");

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
