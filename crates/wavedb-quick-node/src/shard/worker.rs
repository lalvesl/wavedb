//! One shard's worker thread — where an operation actually executes.
//!
//! A worker owns a carrier thread (one Embassy executor, RFC 0066) and its own
//! [`ShardStore`] (and therefore its own cache). It receives [`Job`]s and
//! answers them; nothing else about it crosses a thread boundary, which is why
//! everything it holds may be `Rc`.
//!
//! ## Why each job is its own task
//!
//! Serving jobs one after another in the receive loop would make a shard's
//! concurrency exactly one, and the operations that must not interleave are
//! already excluded by [`OwnerLocks`] — so the serialisation would buy nothing
//! and cost everything: one operation waiting on the disk actor would idle the
//! whole shard. Spawning each job lets a shard have as many operations in
//! flight as it has distinct owners, which is the model this is for.
//!
//! The job pool is static and shared by every shard
//! ([`MAX_JOBS`](crate::__rt::MAX_JOBS)). When it is full the shard runs the
//! job **itself**, in its receive loop: the job is not lost, and the shard
//! stops receiving until it is done, so the backlog stays in the bounded
//! queue in front of it. That is why a job travels in a [`JobSlot`] — a
//! refused spawn drops its arguments, and the slot is how the shard gets the
//! job back.
//!
//! ## What crosses, and what does not
//!
//! Into the worker: [`Work`] — a POST's [`Request`], or a WebSocket session's
//! `Call` with the caller it bound at `Hello` — and a `oneshot` for its
//! [`Answer`]. Plain data. Out of it: a [`Mutation`](wavedb_core::notify::Mutation) per
//! committed write, handed to the accept thread's publisher through the
//! [`Publish`](crate::subscribe::Publish) seam, because the WebSocket
//! subscription table lives there and must not be touched from here. The
//! [`OwnerLocks`] table is the shard's own, like its cache — see
//! [`super::lock`].

use std::cell::RefCell;
use std::rc::Rc;

use async_channel::{Receiver, Sender};
use futures::channel::oneshot;
use wavedb_core::expose::Caller;
use wavedb_net::frame::{CommandFrame, Request};
use wavedb_platform::carrier::{self, Here};

use super::{DiskHandle, OwnerLocks, ShardStore};
use crate::__rt::NodeTasks;
use crate::dispatch::{self, Answer};
use crate::feed::MutationFeed;
use crate::subscribe::NotifyStore;

/// Jobs queued on one shard before its sender waits.
///
/// Bounded for the reason the disk queue is: an unbounded queue turns a client
/// outrunning a shard into unbounded memory, and a bound turns it into
/// backpressure that reaches the socket.
pub const JOB_DEPTH: usize = 512;

/// What a shard is asked to run.
pub enum Work {
    /// A POST: the decoded request, gates and all still to run on the shard.
    Request(Request),
    /// A WebSocket session's command. Gate 1 already ran, once, at the
    /// session's `Hello`, and `caller` is what it resolved to; the shard runs
    /// gates 2–3 only.
    Call { caller: Caller, frame: CommandFrame },
}

/// One piece of work routed to the shard that owns it, plus where its answer
/// goes.
pub struct Job {
    pub work: Work,
    /// The routing caller's return channel. Dropped without a send only if the
    /// worker is torn down mid-flight, which the router reports as a refusal.
    pub answer: oneshot::Sender<Answer>,
}

/// A job on its way into a task, recoverable if the task never starts.
pub type JobSlot = Rc<RefCell<Option<Job>>>;

/// The store a worker serves through: its own cache in front of the disk
/// actor, wrapped so committed mutations reach the accept thread's publisher.
type WorkerStore = NotifyStore<ShardStore, MutationFeed>;

/// What a shard's root task is started with. Built on the routing thread and
/// moved onto the shard's carrier, so every field is `Send`.
pub struct ShardCtx<E> {
    registry: E,
    disk: DiskHandle,
    publisher: MutationFeed,
    secret: [u8; 32],
    jobs: Receiver<Job>,
}

/// One shard's serving state, shared by its job tasks.
pub struct Worker<E> {
    registry: E,
    store: Rc<WorkerStore>,
    locks: OwnerLocks,
    secret: [u8; 32],
}

/// Start a worker and return the channel that reaches it.
///
/// A shard whose root task cannot be placed ([`MAX_SHARDS`] already running)
/// drops its queue at once, so every request routed to it is refused as
/// "shard stopped" — a fault the caller sees, not a hang.
///
/// [`MAX_SHARDS`]: crate::__rt::MAX_SHARDS
///
/// # Errors
/// The worker's thread failing to spawn.
pub fn start<E: NodeTasks>(
    registry: E,
    disk: DiskHandle,
    publisher: MutationFeed,
    secret: [u8; 32],
) -> wavedb_platform::Result<Sender<Job>> {
    let (jobs, rx) = async_channel::bounded(JOB_DEPTH);
    let ctx = ShardCtx {
        registry,
        disk,
        publisher,
        secret,
        jobs: rx,
    };
    // The carrier ends by itself once the router's senders are gone and the
    // last job has answered; nobody needs to wait for it.
    drop(carrier::spawn("wavedb-shard", move |here| {
        let _ = E::spawn_shard(ctx, here);
    })?);
    Ok(jobs)
}

/// A shard's root: receive and serve until the router's last sender drops.
pub async fn shard<E: NodeTasks>(ctx: ShardCtx<E>, here: Here) {
    let ShardCtx {
        registry,
        disk,
        publisher,
        secret,
        jobs,
    } = ctx;
    // Built here, on the shard's own thread: `ShardStore` holds an `Rc` and
    // must never be constructed anywhere it could be moved from.
    let worker = Rc::new(Worker {
        registry,
        store: Rc::new(NotifyStore::new(
            Rc::new(ShardStore::new(disk)),
            publisher,
        )),
        locks: OwnerLocks::new(),
        secret,
    });
    while let Ok(job) = jobs.recv().await {
        let slot: JobSlot = Rc::new(RefCell::new(Some(job)));
        let spawned =
            E::spawn_job(Rc::clone(&slot), Rc::clone(&worker), here.clone());
        // Refused: the pool is full. The job comes back out of its slot and
        // runs here, holding the queue until it is done.
        let refused = spawned.is_err().then(|| slot.borrow_mut().take());
        if let Some(Some(job)) = refused {
            serve(&worker, job).await;
        }
    }
}

/// One job's task.
pub async fn job<E: NodeTasks>(
    slot: JobSlot,
    worker: Rc<Worker<E>>,
    here: Here,
) {
    let taken = slot.borrow_mut().take();
    if let Some(job) = taken {
        serve(&worker, job).await;
    }
    drop(here); // held to the end: the carrier waits for this job
}

async fn serve<E: NodeTasks>(worker: &Worker<E>, Job { work, answer }: Job) {
    let (registry, store, locks) =
        (&worker.registry, &*worker.store, &worker.locks);
    let served = match work {
        Work::Request(request) => {
            dispatch::handle(registry, store, locks, &worker.secret, request)
                .await
        }
        Work::Call { caller, frame } => dispatch::Answer {
            response: dispatch::call(registry, store, locks, caller, frame)
                .await,
            sync: None,
        },
    };
    // A caller that gave up between routing and here: the answer has
    // nowhere to go, and that is not this shard's problem.
    let _ = answer.send(served);
}

/// What a job carries has to cross a thread; what a worker holds must not.
///
/// The second half is enforced structurally rather than asserted — `Rc` in
/// `WorkerStore` makes it so, and an assertion here could only restate it.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<Job>();
    assert_send::<Sender<Job>>();
    assert_send::<ShardCtx<()>>();
};
