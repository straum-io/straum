//! What framing a commit batch costs, and how that cost scales.
//!
//! The architectural claim in docs/replication.md §122 is that replication
//! allocates **per batch and never per record**. That was established by
//! reading the code; this measures it. A count that grows with the record
//! count would mean the claim is wrong, whatever the throughput says.

use replication::{Accepted, Follower, Limits, Message, Peer, Records};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// A batch of `records` framed WAL records, shaped like the ones a log holds:
/// a length, a checksum and a body.
fn batch(records: usize, payload: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..records {
        let body = vec![b'x'; payload];
        out.extend_from_slice(&u32::try_from(body.len()).unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&body);
    }
    out
}

/// The leader's side of one frame: a header into a buffer it already owns.
///
/// The record count is varied while the batch is the same shape, so the
/// allocation count either stays flat or the claim is false.
#[divan::bench(args = [1, 16, 256])]
fn encode_header(bencher: divan::Bencher, records: usize) {
    let batch = batch(records, 64);
    let mut out = Vec::with_capacity(replication::RECORDS_HEADER_BYTES);

    bencher
        .counter(divan::counter::BytesCount::new(batch.len()))
        .bench_local(|| {
            out.clear();
            replication::encode_records_header(
                0,
                records as u64,
                divan::black_box(&batch),
                &mut out,
            );
            divan::black_box(out.len())
        });
}

/// The follower's side: the batch is borrowed out of the read buffer, so
/// this should allocate nothing at all whatever the batch holds.
#[divan::bench(args = [1, 16, 256])]
fn decode_frame(bencher: divan::Bencher, records: usize) {
    let batch = batch(records, 64);
    let mut frame = Vec::new();
    replication::encode_records_header(0, records as u64, &batch, &mut frame);
    frame.extend_from_slice(&batch);

    bencher
        .counter(divan::counter::BytesCount::new(frame.len()))
        .bench_local(|| divan::black_box(replication::decode(&frame, Limits::default())));
}

/// The state machine deciding what to do with a frame. Ranges, not messages
/// (§115), so this must not scale with the records inside the range.
#[divan::bench(args = [1, 256, 65536])]
fn follower_accept(bencher: divan::Bencher, span: u64) {
    bencher.bench_local(|| {
        let mut follower = Follower::resuming_from(0);
        let taken = follower.accept(Records {
            previous: 0,
            last: span,
            batch: b"opaque",
        });
        debug_assert!(matches!(taken, Ok(Accepted::Append { .. })));
        follower.on_durable(span);
        divan::black_box(follower.durable())
    });
}

/// The leader recording what a peer confirmed.
#[divan::bench]
fn peer_report(bencher: divan::Bencher) {
    bencher.bench_local(|| {
        let mut peer = Peer::starting_at(0);
        peer.on_sent(1_000_000);
        for at in (1..=64u64).map(|step| step * 1000) {
            let _ = peer.on_durable(at);
        }
        divan::black_box(peer.durable())
    });
}

/// Reading a whole frame back out, header and body, the way a follower does.
#[divan::bench(args = [1, 256])]
fn round_trip(bencher: divan::Bencher, records: usize) {
    let batch = batch(records, 64);
    let mut frame = Vec::new();
    replication::encode_records_header(0, records as u64, &batch, &mut frame);
    frame.extend_from_slice(&batch);

    bencher
        .counter(divan::counter::BytesCount::new(frame.len()))
        .bench_local(|| {
            let held = replication::decode(&frame, Limits::default());
            match held {
                Ok(Some((Message::Records(records), consumed))) => {
                    divan::black_box((records.batch.len(), consumed))
                }
                _ => panic!("the frame did not decode"),
            }
        });
}
