//! Two policies for a connection's read buffer, measured the way the reader
//! actually uses it: fill, hand the packet off as `Bytes`, repeat.
//!
//!   keep     — the buffer keeps whatever capacity a large packet forced.
//!   reclaim  — capacity above the steady state is returned once it drains.

use std::sync::Arc;

use transport::{BufferBudget, ConnectionBuffer};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const TARGET: usize = 16 * 1024;

fn buffer() -> ConnectionBuffer {
    let budget = Arc::new(BufferBudget::new(64 * 1024 * 1024));
    match ConnectionBuffer::new(TARGET, budget) {
        Ok(buffer) => buffer,
        Err(error) => panic!("budget too small for the benchmark: {error}"),
    }
}

/// One read-loop iteration: bytes arrive, one packet is handed off.
fn cycle(buffer: &mut ConnectionBuffer, packet: &[u8], reclaim: bool) {
    if let Err(error) = buffer.reserve(packet.len()) {
        panic!("budget exhausted: {error}");
    }
    buffer.buf_mut().extend_from_slice(packet);
    let handed_off = buffer.buf_mut().split_to(packet.len()).freeze();
    divan::black_box(&handed_off);
    drop(handed_off);

    if reclaim {
        buffer.reclaim();
    }
}

/// Steady traffic of small packets: what does the policy cost when nothing
/// unusual ever happens?
#[divan::bench(args = [false, true])]
fn steady_small(bencher: divan::Bencher, reclaim: bool) {
    let packet = vec![b'x'; 64];
    bencher.with_inputs(buffer).bench_values(|mut buffer| {
        for _ in 0..16 {
            cycle(&mut buffer, &packet, reclaim);
        }
        buffer
    });
}

/// A connection that always sends large packets: reclaiming after each one
/// would mean growing again for the next.
#[divan::bench(args = [false, true])]
fn steady_large(bencher: divan::Bencher, reclaim: bool) {
    let packet = vec![b'x'; 256 * 1024];
    bencher.with_inputs(buffer).bench_values(|mut buffer| {
        for _ in 0..4 {
            cycle(&mut buffer, &packet, reclaim);
        }
        buffer
    });
}

/// The shape that matters for a million connections: one large packet, then
/// the connection goes back to being small and mostly idle.
#[divan::bench(args = [false, true])]
fn burst_then_small(bencher: divan::Bencher, reclaim: bool) {
    let large = vec![b'x'; 1024 * 1024];
    let small = vec![b'x'; 64];
    bencher.with_inputs(buffer).bench_values(|mut buffer| {
        cycle(&mut buffer, &large, reclaim);
        for _ in 0..16 {
            cycle(&mut buffer, &small, reclaim);
        }
        buffer
    });
}

/// What a flush pays when the buffer is already at its steady size, which
/// is every flush a connection makes until something forces it to grow.
///
/// `reclaim` settles the charge against the budget, and the budget is one
/// counter shared by every connection in the process. When nothing changed
/// there is nothing to settle, so what this measures is whether the common
/// case still touches that counter (CONSTITUTION.md §27: fewer
/// synchronization points).
#[divan::bench(args = [1, 64])]
fn flush_at_steady_size(bencher: divan::Bencher, flushes: usize) {
    bencher.with_inputs(buffer).bench_values(|mut buffer| {
        for _ in 0..flushes {
            // A flush writes what was encoded, empties the buffer and
            // returns whatever capacity a large batch forced.
            buffer.buf_mut().clear();
            buffer.reclaim();
        }
        buffer
    });
}

/// The same, from many threads at once: one shared counter, and this is
/// where touching it for nothing shows up.
#[divan::bench(threads = [1, 8])]
fn flush_contended(bencher: divan::Bencher) {
    bencher.with_inputs(buffer).bench_values(|mut buffer| {
        for _ in 0..64 {
            buffer.buf_mut().clear();
            buffer.reclaim();
        }
        buffer
    });
}
