//! Ingress memory: what one connection holds, and what the budget refuses.
//!
//! The numbers here are the ones the policy was chosen on; they are asserted
//! so a later change has to argue with a failing test.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use transport::{BufferBudget, BufferError, ConnectionBuffer};

const TARGET: usize = 16 * 1024;
const LARGE: usize = 1024 * 1024;

fn budget(limit: usize) -> Arc<BufferBudget> {
    Arc::new(BufferBudget::new(limit))
}

/// One read-loop iteration: bytes arrive, a packet is handed to the decoder.
fn cycle(buffer: &mut ConnectionBuffer, size: usize, reclaim: bool) {
    buffer.reserve(size).unwrap();
    buffer.buf_mut().extend_from_slice(&vec![b'x'; size]);
    let packet = buffer.buf_mut().split_to(size).freeze();
    drop(packet);
    if reclaim {
        buffer.reclaim();
    }
}

fn after_a_burst(reclaim: bool) -> (usize, usize) {
    let budget = budget(64 * 1024 * 1024);
    let mut buffer = ConnectionBuffer::new(TARGET, Arc::clone(&budget)).unwrap();

    cycle(&mut buffer, LARGE, reclaim);
    for _ in 0..16 {
        cycle(&mut buffer, 64, reclaim);
    }
    (buffer.capacity(), budget.used())
}

#[test]
fn a_connection_that_calmed_down_holds_its_steady_state_again() {
    let (capacity, charged) = after_a_burst(true);

    assert!(capacity <= TARGET, "{capacity} B still held");
    assert!(charged <= TARGET, "{charged} B still charged");
}

/// The measurement the policy was chosen on: without reclaiming, one large
/// packet fixes the connection's footprint at its peak for good.
#[test]
fn without_reclaiming_a_single_large_packet_is_held_forever() {
    let (capacity, charged) = after_a_burst(false);

    assert!(capacity >= LARGE / 2, "{capacity} B");
    assert!(
        charged >= 64 * TARGET,
        "{charged} B — 64× the steady state is the difference the policy buys"
    );
}

#[test]
fn the_budget_refuses_before_it_allocates() {
    let budget = budget(TARGET + 1024);
    let mut buffer = ConnectionBuffer::new(TARGET, Arc::clone(&budget)).unwrap();
    let before = buffer.capacity();

    assert_eq!(buffer.reserve(LARGE), Err(BufferError::BudgetExhausted));
    assert_eq!(buffer.capacity(), before, "memory was taken anyway");
    assert_eq!(budget.used(), buffer.charged(), "the refusal was charged");

    // What fits is still served afterwards: a refusal is not a broken buffer.
    buffer.reserve(512).unwrap();
    buffer.buf_mut().extend_from_slice(&[b'x'; 512]);
    assert_eq!(buffer.buf_mut().len(), 512);
}

#[test]
fn a_connection_cannot_start_without_room_for_its_buffer() {
    let budget = budget(TARGET - 1);
    assert_eq!(
        ConnectionBuffer::new(TARGET, Arc::clone(&budget)).map(|_| ()),
        Err(BufferError::BudgetExhausted)
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn a_closed_connection_returns_everything_it_held() {
    let budget = budget(64 * 1024 * 1024);
    {
        let mut buffer = ConnectionBuffer::new(TARGET, Arc::clone(&budget)).unwrap();
        cycle(&mut buffer, LARGE, false);
        assert!(budget.used() > TARGET);
    }
    assert_eq!(budget.used(), 0, "the charge outlived the connection");
}

#[test]
fn the_charge_never_understates_what_is_held() {
    let budget = budget(64 * 1024 * 1024);
    let mut buffer = ConnectionBuffer::new(TARGET, Arc::clone(&budget)).unwrap();

    for size in [64, 4096, 200_000, 64, LARGE, 64] {
        cycle(&mut buffer, size, true);
        assert!(
            buffer.charged() >= buffer.capacity(),
            "charged {} B for {} B held",
            buffer.charged(),
            buffer.capacity()
        );
        assert_eq!(budget.used(), buffer.charged());
    }
}

#[test]
fn connections_share_one_budget() {
    let budget = budget(3 * TARGET);
    let first = ConnectionBuffer::new(TARGET, Arc::clone(&budget)).unwrap();
    let second = ConnectionBuffer::new(TARGET, Arc::clone(&budget)).unwrap();
    let third = ConnectionBuffer::new(TARGET, Arc::clone(&budget)).unwrap();
    assert_eq!(budget.used(), 3 * TARGET);

    // The fourth connection is refused: the limit is global, not per socket.
    assert!(ConnectionBuffer::new(TARGET, Arc::clone(&budget)).is_err());

    drop(third);
    let _fourth = ConnectionBuffer::new(TARGET, Arc::clone(&budget)).unwrap();
    drop((first, second));
}
