//! Single-threaded synchronization for tasks of one compio runtime: a FIFO
//! async mutex, an event with a generation counter, and a one-shot reply.
//! Everything here is `!Send` and assumes one thread.

use std::cell::{Cell, RefCell, RefMut};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// An async mutex over a `RefCell`: the `RefMut` is taken only by the
/// holder of the lock, so it never panics, and it may be held across
/// `.await`. Waiters are woken in arrival order.
pub struct LocalMutex<T> {
    value: RefCell<T>,
    locked: Cell<bool>,
    waiters: RefCell<VecDeque<Waker>>,
}

impl<T> LocalMutex<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: RefCell::new(value),
            locked: Cell::new(false),
            waiters: RefCell::new(VecDeque::new()),
        }
    }

    pub fn lock(&self) -> impl Future<Output = Guard<'_, T>> {
        std::future::poll_fn(move |cx| {
            if self.locked.get() {
                self.waiters.borrow_mut().push_back(cx.waker().clone());
                return Poll::Pending;
            }
            self.locked.set(true);
            Poll::Ready(Guard {
                mutex: self,
                value: Some(self.value.borrow_mut()),
            })
        })
    }
}

pub struct Guard<'a, T> {
    mutex: &'a LocalMutex<T>,
    value: Option<RefMut<'a, T>>,
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value
            .as_deref()
            .unwrap_or_else(|| unreachable!("taken only in drop"))
    }
}

impl<T> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.value
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!("taken only in drop"))
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        drop(self.value.take());
        self.mutex.locked.set(false);
        // Every waiter is woken, in arrival order: the executor polls them in
        // that order, the first takes the lock and the others queue again.
        // Waking only one would stall if that waiter was dropped meanwhile.
        let woken: Vec<Waker> = self.mutex.waiters.borrow_mut().drain(..).collect();
        for w in woken {
            w.wake();
        }
    }
}

/// A generation counter with waiters: `wait_change(since)` completes once
/// `notify` has run after `since` was read. A waiter that is dropped (for
/// example by a timeout) removes itself, so idle waiters never accumulate.
#[derive(Default)]
pub struct Event {
    generation: Cell<u64>,
    next_key: Cell<u64>,
    waiters: RefCell<BTreeMap<u64, Waker>>,
}

impl Event {
    pub fn generation(&self) -> u64 {
        self.generation.get()
    }

    pub fn notify(&self) {
        self.generation.set(self.generation.get().wrapping_add(1));
        let woken = std::mem::take(&mut *self.waiters.borrow_mut());
        for w in woken.into_values() {
            w.wake();
        }
    }

    pub fn wait_change(&self, since: u64) -> Waiter<'_> {
        let key = self.next_key.get();
        self.next_key.set(key.wrapping_add(1));
        Waiter {
            event: self,
            since,
            key,
        }
    }

    /// Waiters registered now (for tests and statistics).
    pub fn waiters(&self) -> usize {
        self.waiters.borrow().len()
    }
}

pub struct Waiter<'a> {
    event: &'a Event,
    since: u64,
    key: u64,
}

impl Future for Waiter<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.event.generation.get() != self.since {
            self.event.waiters.borrow_mut().remove(&self.key);
            return Poll::Ready(());
        }
        self.event
            .waiters
            .borrow_mut()
            .insert(self.key, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        self.event.waiters.borrow_mut().remove(&self.key);
    }
}

/// A one-shot value from one task to another.
pub struct Oneshot<T> {
    slot: Rc<RefCell<(Option<T>, Option<Waker>)>>,
}

impl<T> Clone for Oneshot<T> {
    fn clone(&self) -> Self {
        Self {
            slot: Rc::clone(&self.slot),
        }
    }
}

impl<T> Oneshot<T> {
    pub fn new() -> Self {
        Self {
            slot: Rc::new(RefCell::new((None, None))),
        }
    }

    pub fn send(&self, value: T) {
        let waker = {
            let mut s = self.slot.borrow_mut();
            s.0 = Some(value);
            s.1.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    pub async fn recv(self) -> T {
        std::future::poll_fn(|cx| {
            let mut s = self.slot.borrow_mut();
            match s.0.take() {
                Some(v) => Poll::Ready(v),
                None => {
                    s.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await
    }
}

impl<T> Default for Oneshot<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_waiters_remove_themselves() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let e = Event::default();
            for _ in 0..100 {
                let g = e.generation();
                let r =
                    compio::time::timeout(std::time::Duration::from_millis(1), e.wait_change(g))
                        .await;
                assert!(r.is_err(), "no notify, so it times out");
            }
            assert_eq!(e.waiters(), 0, "timed-out waiters are gone");
            let g = e.generation();
            e.notify();
            e.wait_change(g).await;
        });
    }

    #[test]
    fn mutex_serializes_holders_across_awaits() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let m = Rc::new(LocalMutex::new(Vec::new()));
            let mut handles = Vec::new();
            for i in 0..4 {
                let m = Rc::clone(&m);
                handles.push(compio::runtime::spawn(async move {
                    let mut g = m.lock().await;
                    g.push(i);
                    compio::time::sleep(std::time::Duration::from_millis(2)).await;
                    g.push(i);
                }));
            }
            for h in handles {
                let _ = h.await;
            }
            let v = m.lock().await.clone();
            assert!(v.chunks(2).all(|c| c[0] == c[1]), "no interleaving: {v:?}");
        });
    }

    #[test]
    fn oneshot_delivers_before_or_after_the_wait() {
        let rt = compio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let a = Oneshot::new();
            a.send(1);
            assert_eq!(a.recv().await, 1);
            let b = Oneshot::new();
            let s = b.clone();
            compio::runtime::spawn(async move { s.send(2) }).detach();
            assert_eq!(b.recv().await, 2);
        });
    }
}
