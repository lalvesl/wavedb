//! One mirror at a time into a client's cache.
//!
//! A mirror is a collection operation — read the chain and the trees, plan,
//! apply — and two of them planned from the same reads would each write a
//! tree node built without the other's entry: the record stays at its
//! anchor and silently leaves the index (`concurrent_node_clobber.rs` in
//! `wavedb-core`). A store that never suspends cannot interleave them; the
//! client's stores both suspend — IndexedDB on every request, the native
//! cache on the channel to its carrier (`cache::native`) — and a watch's
//! mirror and a write's can run side by side on one executor. So every
//! mirror takes the cache's turn first.
//!
//! One thread uses it, as one thread uses the `Db`: a flag and the wakers
//! of whoever waits, no lock. Releasing wakes every waiter and the first to
//! run takes the turn — so a waiter that gave up never strands the rest.

use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::task::{Poll, Waker};

/// Whether a mirror is running on the cache, and who waits for it.
#[derive(Debug, Default)]
pub struct Turn {
    taken: Cell<bool>,
    waiting: RefCell<Vec<Waker>>,
}

impl Turn {
    /// Wait for the turn and take it; it is given back when the returned
    /// [`Taken`] drops.
    pub async fn take(&self) -> Taken<'_> {
        poll_fn(|cx| {
            if self.taken.replace(true) {
                self.waiting.borrow_mut().push(cx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        Taken(self)
    }
}

/// The turn, held.
#[derive(Debug)]
pub struct Taken<'a>(&'a Turn);

impl Drop for Taken<'_> {
    fn drop(&mut self) {
        self.0.taken.set(false);
        let waiting = std::mem::take(&mut *self.0.waiting.borrow_mut());
        for waker in waiting {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use futures::FutureExt;

    use super::Turn;

    /// Pending once, waking itself — where an executor can interleave.
    async fn yield_once() {
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                std::task::Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await;
    }

    /// Two holders never overlap: the second waits until the first lets go,
    /// even across a suspension.
    #[test]
    fn a_second_mirror_waits_for_the_first() {
        let turn = Turn::default();
        let log = RefCell::new(Vec::new());
        let run = |name: &'static str| {
            let (turn, log) = (&turn, &log);
            async move {
                let _held = turn.take().await;
                log.borrow_mut().push((name, "in"));
                yield_once().await;
                log.borrow_mut().push((name, "out"));
            }
        };
        futures::executor::block_on(futures::future::join(run("a"), run("b")));
        assert_eq!(
            *log.borrow(),
            [("a", "in"), ("a", "out"), ("b", "in"), ("b", "out")]
        );
    }

    /// A waiter that gives up strands nobody: the turn is free once its
    /// holder lets go.
    #[test]
    fn an_abandoned_wait_strands_no_one() {
        let turn = Turn::default();
        let held = turn.take().now_or_never().expect("free");
        assert!(turn.take().now_or_never().is_none(), "taken");
        drop(held);
        assert!(turn.take().now_or_never().is_some(), "free again");
    }
}
