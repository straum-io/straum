//! CPU side of the WAL: checksum and framing. A batching algorithm that saves
//! syncs but burns CPU per byte is not an improvement (docs/storage.md §97).

use bytes::Bytes;
use storage::{WalLimits, WalRecord};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// Throughput of the checksum, the one per-byte cost of the write path.
#[divan::bench(args = [64, 1024, 65536, 1048576])]
fn checksum(bencher: divan::Bencher, size: usize) {
    let payload = vec![b'x'; size];
    bencher
        .counter(divan::counter::BytesCount::new(size))
        .bench(|| storage::checksum_for_benchmarks(divan::black_box(&payload)));
}

/// Building one batch out of records: no payload copy beyond the single one
/// into the write buffer.
#[divan::bench(args = [1, 16, 256])]
fn batch_construction(bencher: divan::Bencher, records: usize) {
    let payload = Bytes::from(vec![b'x'; 256]);
    let limits = WalLimits::default();

    bencher
        .with_inputs(|| {
            // The writer reuses one buffer; allocating it inside the
            // measured section would measure the allocator instead.
            let buffer = Vec::with_capacity(limits.initial_batch_bytes);
            let records: Vec<WalRecord> = (0..records)
                .map(|index| WalRecord::local(index as u64, payload.clone()))
                .collect();
            (records, buffer)
        })
        .bench_values(|(records, mut buffer)| {
            buffer.clear();
            for record in &records {
                storage::encode_for_benchmarks(&record.payload, &mut buffer);
            }
            divan::black_box(buffer.len())
        });
}

/// The same records taken in already framed, as a follower takes a
/// replication batch.
///
/// The claim in docs/replication.md §114 is that nothing is encoded twice:
/// the bytes go in as they arrived and the framing is only walked to count
/// the records. Against `batch_construction` above, which encodes the same
/// records from their payloads, this is the difference between re-framing a
/// batch and appending it.
#[divan::bench(args = [1, 16, 256])]
fn framed_append(bencher: divan::Bencher, records: usize) {
    let payload = vec![b'x'; 256];
    let limits = WalLimits::default();

    // One batch, framed once, outside the measured section.
    let mut run = Vec::new();
    for _ in 0..records {
        storage::encode_for_benchmarks(&payload, &mut run);
    }

    bencher
        .with_inputs(|| Vec::with_capacity(limits.initial_batch_bytes))
        .bench_values(|mut buffer: Vec<u8>| {
            buffer.clear();
            let counted = storage::count_for_benchmarks(&run, limits.max_record_bytes);
            buffer.extend_from_slice(&run);
            divan::black_box((counted, buffer.len()))
        });
}
