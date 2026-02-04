//! Waiting on a storage shard ([RFC 0073] §3): a round reserves its window
//! and then builds it, and between the two its task is parked — the
//! worker's executor runs every other task meanwhile.
//!
//! - **[`Gate`]**: a round plans from the directories and publishes into
//!   them, so nothing else may publish while it waits for its window — a
//!   job restoring an evicted bucket, say, whose change the round would
//!   then overwrite with the copy it planned from. One round at a time per
//!   store, in the order they asked; every other task runs on.
//! - **[`park_on`]**: the synchronous callers — a close, a test, a bench
//!   phase — run the same rounds to the end on the calling thread, parked
//!   while the storage shard answers. A store with its own files never
//!   waits at all.
//!
//! Single-threaded like the store: a flag and the waiting wakers in cells,
//! no lock, no atomic. The one `Arc` is the waker's, which every waker is.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards-WIP.md

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;

/// One round at a time (see the module docs).
#[derive(Debug, Default)]
pub struct Gate {
    held: Cell<bool>,
    waiting: RefCell<VecDeque<Waker>>,
}

/// A round through the [`Gate`]; the next one goes when it is dropped.
#[derive(Debug)]
pub struct Through<'a>(&'a Gate);

impl Gate {
    /// Wait for the gate, and hold it.
    pub async fn enter(&self) -> Through<'_> {
        poll_fn(|cx| {
            if self.held.get() {
                self.waiting.borrow_mut().push_back(cx.waker().clone());
                return Poll::Pending;
            }
            self.held.set(true);
            Poll::Ready(())
        })
        .await;
        Through(self)
    }
}

impl Drop for Through<'_> {
    fn drop(&mut self) {
        self.0.held.set(false);
        // Every waiter polls again; the first to run takes the gate and the
        // others queue anew.
        let waiting = std::mem::take(&mut *self.0.waiting.borrow_mut());
        for waker in waiting {
            waker.wake();
        }
    }
}

/// Wakes a parked thread.
struct Unpark(Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Run `step` to the end on the calling thread, parked whenever it waits
/// (see the module docs). Never from inside a task of the store's own
/// thread that another of its tasks may be holding the [`Gate`] against.
pub fn park_on<F: Future>(step: F) -> F::Output {
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut step = pin!(step);
    loop {
        if let Poll::Ready(out) = step.as_mut().poll(&mut cx) {
            return out;
        }
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use futures::FutureExt;

    use super::{Gate, park_on};

    /// The second round waits for the first, and goes once it is through.
    #[test]
    fn rounds_go_through_one_at_a_time() {
        let gate = Gate::default();
        let order = RefCell::new(Vec::new());
        let first = gate.enter().now_or_never().expect("free");
        let mut second = Box::pin(async {
            let _through = gate.enter().await;
            order.borrow_mut().push(2);
        });
        assert!((&mut second).now_or_never().is_none(), "held");
        order.borrow_mut().push(1);
        drop(first);
        park_on(second);
        assert_eq!(*order.borrow(), [1, 2]);
    }

    /// A step that waits on another thread is woken by it.
    #[test]
    fn a_parked_step_is_woken_from_another_thread() {
        let (tx, rx) = futures::channel::oneshot::channel();
        let sender = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            tx.send(7).unwrap();
        });
        assert_eq!(park_on(rx).unwrap(), 7);
        sender.join().unwrap();
    }
}
