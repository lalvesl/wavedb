//! One worker thread — where an operation actually executes, on the data
//! shards it owns ([RFC 0067] step 4).
//!
//! A worker owns a carrier thread (one Embassy executor, RFC 0066) and the
//! `PageStore`s of its contiguous range of data shards ([`Owned`]). It
//! receives [`Job`]s and runs them as plain calls on the owner's store —
//! nothing about an operation crosses a thread once it is here — and its
//! maintenance runs beside them on the same thread, in bounded steps
//! ([`super::background`]).
//!
//! ## Why each job is its own task
//!
//! Serving jobs one after another in the receive loop would make a worker's
//! concurrency exactly one, and the operations that must not interleave are
//! already excluded by [`OwnerLocks`] — so the serialisation would buy
//! nothing and cost everything: one job waiting on an owner's lock would idle
//! the whole worker. Spawning each job lets a worker have as many operations
//! in flight as it has distinct owners.
//!
//! The job pool is static and shared by every worker
//! ([`MAX_JOBS`](crate::__rt::MAX_JOBS)). When it is full the worker runs the
//! job **itself**, in its receive loop: the job is not lost, and the worker
//! stops receiving until it is done, so the backlog stays in the bounded
//! queue in front of it. That is why a job travels in a [`JobSlot`] — a
//! refused spawn drops its arguments, and the slot is how the worker gets the
//! job back.
//!
//! ## What crosses, and what does not
//!
//! Into the worker: its stores once, at start ([`ShardCtx`]), then a [`Job`]
//! — a client command with the caller gate 1 resolved at the edge, or a
//! `#[server]` body's [`NodeOp`] — and a `oneshot` for its answer. Out of it:
//! a [`Mutation`](wavedb_core::notify::Mutation) per committed write, handed
//! to the accept thread's publisher, and at the end the outcome of closing
//! its stores.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use async_channel::{Receiver, Sender};
use futures::channel::oneshot;
use wavedb_core::expose::Caller;
use wavedb_core::{NodeOp, NodeReply, U48};
use wavedb_net::frame::{CommandFrame, NodeError, Response};
use wavedb_platform::carrier::{self, Here};
use wavedb_storage::{PageStore, StorageResult};

use super::background::{Background, background};
use super::owned::Owned;
use super::{Owner, OwnerKey, OwnerLocks};
use crate::__rt::NodeTasks;
use crate::dispatch;
use crate::feed::MutationFeed;
use crate::subscribe::NotifyStore;
use crate::upkeep::Maintenance;

/// Jobs queued on one worker before its sender waits.
///
/// Bounded for the reason every queue here is: an unbounded queue turns a
/// client outrunning a worker into unbounded memory, and a bound turns it into
/// backpressure that reaches the socket.
pub const JOB_DEPTH: usize = 512;

/// One piece of work routed to the worker that owns it, plus where its answer
/// goes. An answer channel dropped without a send — the worker torn down
/// mid-flight — is reported by the router as a refusal.
pub enum Job {
    /// A client command. Gate 1 ran at the edge and `caller` is what it
    /// resolved; the worker runs gates 2–3 under the owner's lock.
    Call {
        caller: Caller,
        frame: CommandFrame,
        answer: oneshot::Sender<Response>,
    },
    /// One operation of a `#[server]` body, under its owner's lock.
    Node {
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
        answer: oneshot::Sender<wavedb_core::Result<NodeReply>>,
    },
}

/// A job on its way into a task, recoverable if the task never starts.
pub type JobSlot = Rc<RefCell<Option<Job>>>;

/// The store one operation runs on: its owner's data shard, wrapped so
/// committed mutations reach the accept thread's publisher.
type OpStore<'a> = NotifyStore<&'a PageStore, &'a MutationFeed>;

/// Where a worker reports how closing its stores went.
pub type Closed = oneshot::Sender<StorageResult<()>>;

/// What a worker's root task is started with. Built on the routing thread and
/// moved onto the worker's carrier, so every field is `Send` — the stores
/// included: they move in once and never leave.
pub struct ShardCtx<E> {
    registry: E,
    /// The first data shard of the range, and the range's stores in order.
    first: usize,
    stores: Vec<PageStore>,
    publisher: MutationFeed,
    /// Data shards in the database — where an owner's records live.
    shards: usize,
    policy: Maintenance,
    jobs: Receiver<Job>,
    closed: Closed,
}

/// One worker's serving state, shared by its job tasks.
pub struct Worker<E> {
    registry: E,
    owned: Rc<Owned>,
    publisher: MutationFeed,
    shards: usize,
    locks: OwnerLocks,
}

impl<E> Worker<E> {
    /// The store an operation on `owner` runs on.
    fn store(&self, owner: Owner) -> wavedb_core::Result<OpStore<'_>> {
        let store = self.owned.store(owner.shard(self.shards))?;
        Ok(NotifyStore::new(store, &self.publisher))
    }
}

/// What starting a worker hands back: the queue that reaches it, and where
/// it will say how closing its stores went.
pub struct Started {
    pub jobs: Sender<Job>,
    pub closed: oneshot::Receiver<StorageResult<()>>,
}

/// Start a worker over data shards `first ..` (`stores`, in order) and return
/// the channel that reaches it.
///
/// A worker whose root task cannot be placed ([`MAX_SHARDS`] already running)
/// drops its queue and its stores at once: every request routed to it is
/// refused as "shard stopped", and its close is reported as never having
/// run — a fault the caller sees, not a hang.
///
/// [`MAX_SHARDS`]: crate::__rt::MAX_SHARDS
///
/// # Errors
/// The worker's thread failing to spawn.
pub fn start<E: NodeTasks>(
    registry: E,
    first: usize,
    stores: Vec<PageStore>,
    publisher: MutationFeed,
    shards: usize,
    policy: Maintenance,
) -> wavedb_platform::Result<Started> {
    let (jobs, rx) = async_channel::bounded(JOB_DEPTH);
    let (closed, on_close) = oneshot::channel();
    let ctx = ShardCtx {
        registry,
        first,
        stores,
        publisher,
        shards,
        policy,
        jobs: rx,
        closed,
    };
    // The carrier ends by itself once the router's senders are gone, the
    // last job has answered and the stores are closed; `closed` says when.
    drop(carrier::spawn("wavedb-shard", move |here| {
        let _ = E::spawn_shard(ctx, here);
    })?);
    Ok(Started {
        jobs,
        closed: on_close,
    })
}

/// A worker's root: receive and serve until the router's last sender drops,
/// then close the stores.
pub async fn shard<E: NodeTasks>(ctx: ShardCtx<E>, here: Here) {
    let ShardCtx {
        registry,
        first,
        stores,
        publisher,
        shards,
        policy,
        jobs,
        closed,
    } = ctx;
    let owned = Rc::new(Owned::new(first, stores));
    // BG ends when this closes; refused (its pool is full), the worker serves
    // without background maintenance and still closes its stores cleanly.
    let (halt, halted) = async_channel::bounded::<()>(1);
    let _ = here.spawn(background(
        Background {
            owned: Rc::clone(&owned),
            jobs: jobs.clone(),
            policy,
            partitions: shards,
            halt: halted,
        },
        here.clone(),
    ));
    let worker = Rc::new(Worker {
        registry,
        owned,
        publisher,
        shards,
        locks: OwnerLocks::new(),
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
    drop(halt);
    // Every job and BG hold a share of the stores; the last of them has to
    // be gone before they can be closed and dropped.
    let worker = sole(worker).await;
    let owned = sole(worker.owned).await;
    let _ = closed.send(owned.close());
    drop(here);
}

/// Wait for every worker to report closing its stores; the first fault wins,
/// and a worker that never reported is one.
///
/// # Errors
/// The first worker's fault.
pub async fn closed_all(
    closing: Vec<oneshot::Receiver<StorageResult<()>>>,
) -> StorageResult<()> {
    let mut first = Ok(());
    for worker in closing {
        let outcome =
            worker
                .await
                .unwrap_or(Err(wavedb_storage::StorageError::Corrupt(
                    "a worker stopped before closing its stores",
                )));
        if first.is_ok() {
            first = outcome;
        }
    }
    first
}

/// Wait until `rc` is the only reference left, and take its value.
async fn sole<T>(mut rc: Rc<T>) -> T {
    loop {
        match Rc::try_unwrap(rc) {
            Ok(value) => return value,
            Err(shared) => {
                rc = shared;
                wavedb_platform::time::sleep(Duration::from_millis(1)).await;
            }
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
    drop(worker);
    drop(here); // held to the end: the carrier waits for this job
}

async fn serve<E: NodeTasks>(worker: &Worker<E>, job: Job) {
    let (registry, locks) = (&worker.registry, &worker.locks);
    let _busy = worker.owned.busy();
    // A caller that gave up between routing and here: the answer has
    // nowhere to go, and that is not this worker's problem.
    match job {
        Job::Call {
            caller,
            frame,
            answer,
        } => {
            let owner = dispatch::owner_of(registry, caller.tenant, &frame);
            let response = match worker.store(owner) {
                Ok(store) => {
                    dispatch::call(
                        registry, &store, locks, caller, owner, frame,
                    )
                    .await
                }
                Err(e) => {
                    Response::Err(NodeError::from_core(frame.struct_hash, &e))
                }
            };
            let _ = answer.send(response);
        }
        Job::Node {
            tenant,
            struct_hash,
            op,
            answer,
        } => {
            let owner = op.owner(tenant, struct_hash);
            let reply = match worker.store(owner) {
                Ok(store) => {
                    let held =
                        locks.acquire(OwnerKey::new(tenant, owner)).await;
                    let reply = registry
                        .run_node(&store, tenant, struct_hash, op)
                        .await;
                    drop(held);
                    reply
                }
                Err(e) => Err(e),
            };
            let _ = answer.send(reply);
        }
    }
}

/// What a job carries has to cross a thread; what a worker holds must not.
///
/// The second half is enforced structurally rather than asserted — `Rc` in
/// `Worker` makes it so, and an assertion here could only restate it.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<Job>();
    assert_send::<Sender<Job>>();
    assert_send::<ShardCtx<()>>();
};
