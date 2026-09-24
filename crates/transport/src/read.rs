//! Ingress memory: what a connection is allowed to hold while it waits for a
//! packet to arrive, and what all connections together are allowed to hold.
//!
//! Per-connection limits alone are not a bound: a limit times the connection
//! count is a product, not a budget. The same reasoning CLAUDE.md §8 applies
//! to queued egress bytes applies to the read side.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BytesMut;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    #[error("the global read budget is exhausted")]
    BudgetExhausted,
}

/// Bytes held in read buffers across all connections.
///
/// Counters are `Relaxed`: they order nothing but themselves (§14).
#[derive(Debug)]
pub struct ReadBudget {
    used: AtomicUsize,
    limit: usize,
}

impl ReadBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit,
        }
    }

    /// Takes `bytes` from the budget, or leaves it untouched and fails.
    pub fn try_reserve(&self, bytes: usize) -> Result<(), ReadError> {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = used.checked_add(bytes) else {
                return Err(ReadError::BudgetExhausted);
            };
            if next > self.limit {
                return Err(ReadError::BudgetExhausted);
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Ok(()),
                Err(current) => used = current,
            }
        }
    }

    pub fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// One connection's read buffer, charged to the global budget.
///
/// The charge follows the buffer's capacity, so releasing it is not something
/// a caller can forget: it happens when the buffer is dropped.
#[derive(Debug)]
pub struct ReadBuffer {
    buf: BytesMut,
    /// Capacity to fall back to once a large packet is out of the way.
    target: usize,
    charged: usize,
    budget: Arc<ReadBudget>,
}

impl ReadBuffer {
    /// `target` is the steady-state capacity (`STRAUM_READ_BUFFER_BYTES`).
    pub fn new(target: usize, budget: Arc<ReadBudget>) -> Result<Self, ReadError> {
        budget.try_reserve(target)?;
        Ok(Self {
            buf: BytesMut::with_capacity(target),
            target,
            charged: target,
            budget,
        })
    }

    /// Makes room for `additional` bytes, charging the growth.
    pub fn reserve(&mut self, additional: usize) -> Result<(), ReadError> {
        let wanted = self
            .buf
            .len()
            .checked_add(additional)
            .ok_or(ReadError::BudgetExhausted)?;
        if wanted <= self.buf.capacity() {
            return Ok(());
        }

        // Asked for before it is allocated: over the budget means the
        // connection never gets the memory, not that it gets it and is
        // closed afterwards.
        let requested = wanted.saturating_sub(self.buf.capacity());
        self.budget.try_reserve(requested)?;
        self.charged = self.charged.saturating_add(requested);

        self.buf.reserve(additional);
        self.settle();
        Ok(())
    }

    /// Returns capacity above the steady state once the buffer has drained.
    pub fn reclaim(&mut self) {
        if !self.buf.is_empty() || self.buf.capacity() <= self.target {
            return;
        }
        self.buf = BytesMut::with_capacity(self.target);
        self.settle();
    }

    pub fn buf_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub fn charged(&self) -> usize {
        self.charged
    }

    /// Keeps the charge equal to the capacity actually held: a budget
    /// that does not match reality is not a bound.
    fn settle(&mut self) {
        let capacity = self.buf.capacity();
        if capacity > self.charged {
            // Allocator slack, already taken: account it, do not hide it.
            self.budget.try_reserve(capacity - self.charged).ok();
        } else {
            self.budget.release(self.charged - capacity);
        }
        self.charged = capacity;
    }
}

impl Drop for ReadBuffer {
    fn drop(&mut self) {
        self.budget.release(self.charged);
    }
}
