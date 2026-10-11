//! One shard's worker thread — where an operation actually executes.
//!
//! A worker owns a thread, a current-thread runtime, a [`LocalSet`], and its
//! own [`ShardStore`] (and therefore its own cache). It receives [`Job`]s and
//! answers them; nothing else about it crosses a thread boundary, which is why
//! everything it holds may be `Rc`.
//!
//! ## Why each job is its own local task
//!
//! Serving jobs one after another in the receive loop would make a shard's
//! concurrency exactly one, and the operations that must not interleave are
//! already excluded by [`OwnerLocks`] — so the serialisation would buy nothing
//! and cost everything: one operation waiting on the disk actor would idle the
//! whole shard. Spawning each job locally lets a shard have as many operations
//! in flight as it has distinct owners, which is the model this is for.
//!
//! ## What crosses, and what does not
//!
//! Into the worker: a [`Request`] and a `oneshot` for its [`Answer`] — plain
//! data. Out of it: a [`Mutation`] per committed write, handed to the accept
//! thread's publisher through the [`Publish`](crate::subscribe::Publish) seam,
//! because the WebSocket subscription table lives there and must not be
//! touched from here. Shared with everything: the [`OwnerLocks`] table, which
//! is the *only* structure that has to be one for the node rather than one per
//! thread — see [`super::lock`].
//!
//! [`LocalSet`]: tokio::task::LocalSet

use std::rc::Rc;
use std::sync::Arc;

use async_channel::{Receiver, Sender};
use futures::channel::oneshot;
use wavedb_core::expose::Exposure;
use wavedb_net::frame::Request;

use super::{DiskHandle, OwnerLocks, ShardStore};
use crate::dispatch::{self, Answer};
use crate::feed::MutationFeed;
use crate::subscribe::NotifyStore;

/// Jobs queued on one shard before its sender waits.
///
/// Bounded for the reason the disk queue is: an unbounded queue turns a client
/// outrunning a shard into unbounded memory, and a bound turns it into
/// backpressure that reaches the socket.
pub const JOB_DEPTH: usize = 512;

/// One request routed to the shard that owns it, plus where its answer goes.
pub struct Job {
    /// The decoded request — gates and all still to run on the shard.
    pub request: Request,
    /// The routing caller's return channel. Dropped without a send only if the
    /// worker is torn down mid-flight, which the router reports as a refusal.
    pub answer: oneshot::Sender<Answer>,
}

/// The store a worker serves through: its own cache in front of the disk
/// actor, wrapped so committed mutations reach the accept thread's publisher.
type WorkerStore = NotifyStore<ShardStore, MutationFeed>;

/// Start a worker and return the channel that reaches it.
///
/// `registry` is `Copy` so each job takes its own copy rather than sharing
/// one — a schema registry is a zero-sized marker, so this is free.
///
/// # Errors
/// The worker's thread failing to spawn.
pub fn start<E>(
    registry: E,
    disk: DiskHandle,
    locks: Arc<OwnerLocks>,
    publisher: MutationFeed,
    secret: [u8; 32],
) -> wavedb_platform::Result<Sender<Job>>
where
    E: Exposure + Copy + Send + 'static,
{
    let (jobs, rx) = async_channel::bounded(JOB_DEPTH);
    wavedb_platform::task::spawn_detached("wavedb-shard", move || {
        // Built here, on the worker's own thread: `ShardStore` holds an `Rc`
        // and must never be constructed anywhere it could be moved from.
        let store = Rc::new(NotifyStore::new(
            Rc::new(ShardStore::new(disk)),
            publisher,
        ));
        run(registry, store, locks, secret, rx)
    })?;
    Ok(jobs)
}

/// Receive and serve until the router's last sender drops.
async fn run<E>(
    registry: E,
    store: Rc<WorkerStore>,
    locks: Arc<OwnerLocks>,
    secret: [u8; 32],
    jobs: Receiver<Job>,
) where
    E: Exposure + Copy + 'static,
{
    while let Ok(Job { request, answer }) = jobs.recv().await {
        let store = Rc::clone(&store);
        let locks = Arc::clone(&locks);
        wavedb_platform::task::spawn_local(async move {
            let served =
                dispatch::handle(&registry, &*store, &locks, &secret, request)
                    .await;
            // A caller that gave up between routing and here: the answer has
            // nowhere to go, and that is not this shard's problem.
            let _ = answer.send(served);
        });
    }
}

/// What a job carries has to cross a thread; what a worker holds must not.
///
/// The second half is enforced structurally rather than asserted — `Rc` in
/// `WorkerStore` makes it so, and an assertion here could only restate it.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<Job>();
    assert_send::<Sender<Job>>();
};
