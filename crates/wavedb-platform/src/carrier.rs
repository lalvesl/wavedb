//! Carrier threads: one OS thread, one Embassy executor ([RFC 0066] §4).
//!
//! A carrier runs tasks from **static pools**, the Embassy way: each task
//! function declares how many of it may exist at once, and a full pool is an
//! explicit refusal rather than an allocation. Nothing is scheduled across
//! carriers; what crosses between them is data on a channel and a `Waker`.
//!
//! ## When a carrier ends
//!
//! Embassy has no way to abort a task, and a task the executor stops polling
//! would keep its pool slot forever. So a carrier ends only when **every**
//! task on it has finished, counted by [`Live`] tokens: the root task gets
//! the first, each task it spawns carries a clone, and the thread returns
//! once the last one drops. Stopping a carrier is therefore cooperative —
//! its tasks watch a stop signal of their own and return.
//!
//! [RFC 0066]: ../../../rfcs/0066-embassy-executor-and-async-io-WIP.md

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::channel::oneshot;

/// The executor crate, re-exported so code generated elsewhere (the
/// `#[task]` attribute and its `embassy_executor = …` path) names it through
/// this crate rather than depending on it directly.
pub use embassy_executor as rt;
pub use embassy_executor::{SpawnError, SpawnToken, Spawner};

use crate::error::Result;

/// Keeps its carrier alive. Hold one for the whole life of a task, and clone
/// it into every task you spawn on the same carrier.
///
/// Not `Send`: a token counts tasks on the thread that made it.
#[derive(Debug)]
pub struct Live(Rc<Cell<usize>>);

impl Clone for Live {
    fn clone(&self) -> Self {
        self.0.set(self.0.get() + 1);
        Self(Rc::clone(&self.0))
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

/// Where a task runs: its carrier's [`Spawner`], and a [`Live`] token
/// keeping that carrier up. Every task takes one and passes clones on.
#[derive(Clone)]
pub struct Here {
    spawner: Spawner,
    live: Live,
}

impl Here {
    /// Spawn `task` — the result of calling a `#[task]` function, which has
    /// already taken its own clone of this `Here` — on this carrier.
    ///
    /// # Errors
    /// [`SpawnError`] when the task's pool is full. The arguments the task
    /// was given, its `Here` included, are dropped with it.
    pub fn spawn<S>(
        &self,
        task: core::result::Result<SpawnToken<S>, SpawnError>,
    ) -> core::result::Result<(), SpawnError> {
        self.spawner.spawn(task?);
        Ok(())
    }
}

impl std::fmt::Debug for Here {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Here")
            .field("live", &self.live)
            .finish_non_exhaustive()
    }
}

/// Resolves once a carrier's last task has ended and its thread returned.
///
/// Await it from anywhere — any executor, or none (`block_on`): it is a
/// plain one-shot channel.
#[derive(Debug)]
pub struct Finished(oneshot::Receiver<()>);

impl Future for Finished {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // A dropped sender is the thread unwinding: it is over either way.
        Pin::new(&mut self.0).poll(cx).map(|_| ())
    }
}

/// Start a carrier thread named `name` and run `init` on it, with the first
/// [`Here`], to spawn the root task(s).
///
/// `init` runs **on the new thread**, so the tasks it starts may own
/// non-`Send` state; only `init` itself has to cross. If it spawns nothing
/// (it drops its `Here`), the thread returns at once.
///
/// The executor is leaked deliberately — a few hundred bytes per carrier,
/// because Embassy's executor must be `'static` to hand out wakers.
///
/// # Errors
/// The OS refusing to start the thread ([`Error::Io`]).
///
/// [`Error::Io`]: crate::Error::Io
pub fn spawn<F>(name: &str, init: F) -> Result<Finished>
where
    F: FnOnce(Here) + Send + 'static,
{
    let (done, finished) = oneshot::channel();
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let executor = Box::leak(Box::new(rt::Executor::new()));
            let count = Rc::new(Cell::new(1));
            let first = Live(Rc::clone(&count));
            executor.run_until(
                |spawner| {
                    init(Here {
                        spawner,
                        live: first,
                    });
                },
                || count.get() == 0,
            );
            let _ = done.send(());
        })?;
    Ok(Finished(finished))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Here, spawn};

    // One task per test: a static pool is shared by every test in the binary,
    // and the tests run in parallel.
    #[embassy_executor::task]
    async fn nap(here: Here) {
        crate::time::sleep(Duration::from_millis(20)).await;
        drop(here); // held to here: the carrier waits for it
    }

    /// Pool of two, so the third concurrent spawn is refused.
    #[embassy_executor::task(pool_size = 2)]
    async fn crowded_nap(here: Here) {
        crate::time::sleep(Duration::from_millis(20)).await;
        drop(here);
    }

    /// The carrier outlives its root: it ends when the tasks the root
    /// spawned have ended too, not before.
    #[test]
    fn a_carrier_ends_when_its_last_task_does() {
        let started = std::time::Instant::now();
        let finished = spawn("carrier-test", move |here| {
            here.spawn(nap(here.clone())).expect("a free slot");
        })
        .expect("carrier thread");
        futures::executor::block_on(finished);
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    /// A full pool refuses the spawn, and a refused task's `Here` does not
    /// keep the carrier alive.
    #[test]
    fn a_full_pool_refuses_and_releases_its_token() {
        let (tx, rx) = std::sync::mpsc::channel();
        let finished = spawn("carrier-test", move |here| {
            let admitted = (0..3)
                .filter(|_| here.spawn(crowded_nap(here.clone())).is_ok())
                .count();
            let _ = tx.send(admitted);
        })
        .expect("carrier thread");
        futures::executor::block_on(finished);
        assert_eq!(rx.recv().ok(), Some(2), "pool_size 2 admits two");
    }
}
