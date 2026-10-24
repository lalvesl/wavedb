//! One worker thread — where an operation actually executes, on the data
//! shards it owns ([RFC 0067] steps 4–5).
//!
//! A worker owns a carrier thread (one Embassy executor, RFC 0066), the
//! `PageStore`s of its contiguous range of data shards ([`Owned`]) and the
//! subscriptions to the topics those shards hold. It receives [`Job`]s and
//! runs them as plain calls on the owner's store — nothing about an
//! operation crosses a thread once it is here. Beside the jobs, on the same
//! carrier: its maintenance, in bounded steps ([`super::background`]), and
//! its accept loop and connections ([`crate::serve`]).
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
//! A subscription is not a job: registering one is a table insert, done in
//! the receive loop itself, so it is in place before any job received after
//! it runs.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use futures::channel::oneshot;
use wavedb_core::expose::Caller;
use wavedb_core::{NodeOp, NodeReply, U48};
use wavedb_net::frame::{CommandFrame, NodeError, Response};
use wavedb_net::ws::Topic;
use wavedb_platform::carrier::Here;
use wavedb_storage::PageStore;

use super::background::{Background, background};
use super::lease::{self, Lease};
use super::owned::{Busy, Owned};
use super::spawn::ShardCtx;
use super::{Owner, OwnerKey, OwnerLocks};
use crate::__rt::NodeTasks;
use crate::dispatch;
use crate::subscribe::{NotifyStore, Outbox, SubTable};

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
    /// Publish `topic`'s mutations into `outbox` from now on; `done` says
    /// it is registered.
    Watch {
        tenant: U48,
        topic: Topic,
        outbox: Outbox,
        done: oneshot::Sender<()>,
    },
    /// Stop publishing `topic` into `outbox`.
    Unwatch {
        tenant: U48,
        topic: Topic,
        outbox: Outbox,
    },
    /// Hold `owner` for a `#[server]` body until the [`Lease`] it is
    /// answered with drops (`super::lease`).
    Lease {
        tenant: U48,
        owner: Owner,
        answer: oneshot::Sender<Lease>,
    },
}

/// A job on its way into a task, recoverable if the task never starts.
pub type JobSlot = Rc<RefCell<Option<Job>>>;

/// The store one operation runs on: its owner's data shard, wrapped so
/// committed mutations reach the topic's subscribers.
type OpStore<'a> = NotifyStore<'a, &'a PageStore>;

/// One worker's serving state, shared by its job tasks.
pub struct Worker<E> {
    registry: E,
    owned: Rc<Owned>,
    /// Subscriptions to the topics this worker's shards hold.
    subs: RefCell<SubTable>,
    shards: usize,
    locks: OwnerLocks,
}

impl<E> Worker<E> {
    /// The store an operation on `owner` runs on.
    fn store(&self, owner: Owner) -> wavedb_core::Result<OpStore<'_>> {
        let store = self.owned.store(owner.shard(self.shards))?;
        Ok(NotifyStore::new(store, &self.subs))
    }

    /// This worker's owner locks.
    pub(super) const fn locks(&self) -> &OwnerLocks {
        &self.locks
    }

    /// Count an operation as executing until the guard drops.
    pub(super) fn busy(&self) -> Busy<'_> {
        self.owned.busy()
    }

    /// Apply a subscription job here and now; any other job comes back.
    fn subscription(&self, job: Job) -> Option<Job> {
        match job {
            Job::Watch {
                tenant,
                topic,
                outbox,
                done,
            } => {
                self.subs.borrow_mut().subscribe(tenant, topic, outbox);
                let _ = done.send(());
                None
            }
            Job::Unwatch {
                tenant,
                topic,
                outbox,
            } => {
                self.subs.borrow_mut().unsubscribe(tenant, topic, &outbox);
                None
            }
            job => Some(job),
        }
    }
}

/// A worker's root: start its accept loop and its maintenance, receive and
/// serve until every router is gone, then close the stores.
pub async fn shard<E: NodeTasks>(ctx: ShardCtx<E>, here: Here) {
    let ShardCtx {
        registry,
        first,
        stores,
        shards,
        policy,
        jobs,
        closed,
        accept,
    } = ctx;
    // This worker's share of ingress. Its router is one of the senders that
    // keep every queue open; the loop drops it when serving stops.
    if let Some(accept) = accept {
        let fault = accept.fault.clone();
        if E::spawn_accept(accept, here.clone()).is_err() {
            let _ = fault.try_send(crate::ServerError::Io(
                std::io::Error::other("no free accept slot"),
            ));
        }
    }
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
        subs: RefCell::new(SubTable::default()),
        shards,
        locks: OwnerLocks::new(),
    });
    while let Ok(job) = jobs.recv().await {
        let Some(job) = worker.subscription(job) else {
            continue;
        };
        let slot: JobSlot = Rc::new(RefCell::new(Some(job)));
        let spawned =
            E::spawn_job(Rc::clone(&slot), Rc::clone(&worker), here.clone());
        // Refused: the pool is full. The job comes back out of its slot and
        // runs here, holding the queue until it is done — except a lease,
        // which would hold the queue for as long as its body holds the
        // owner: it is refused instead, its answer dropped.
        let refused = spawned.is_err().then(|| slot.borrow_mut().take());
        match refused {
            None | Some(None | Some(Job::Lease { .. })) => {}
            Some(Some(job)) => serve(&worker, job).await,
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
    // A caller that gave up between routing and here: the answer has
    // nowhere to go, and that is not this worker's problem.
    match job {
        Job::Call {
            caller,
            frame,
            answer,
        } => {
            let _busy = worker.busy();
            let registry = &worker.registry;
            let owner = dispatch::owner_of(registry, caller.tenant, &frame);
            let response = match worker.store(owner) {
                Ok(store) => {
                    dispatch::call(
                        registry,
                        &store,
                        &worker.locks,
                        caller,
                        owner,
                        frame,
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
            let _busy = worker.busy();
            let owner = op.owner(tenant, struct_hash);
            let held = worker.locks.acquire(OwnerKey::new(tenant, owner)).await;
            let reply = worker.run_node(owner, tenant, struct_hash, op).await;
            drop(held);
            let _ = answer.send(reply);
        }
        // Busy only while an operation runs under it, not while it is held.
        Job::Lease {
            tenant,
            owner,
            answer,
        } => lease::hold(worker, tenant, owner, answer).await,
        // Applied in the receive loop; a refused spawn never carries one.
        job @ (Job::Watch { .. } | Job::Unwatch { .. }) => {
            let _ = worker.subscription(job);
        }
    }
}

impl<E: NodeTasks> Worker<E> {
    /// Run a node operation on `owner`'s store, whoever holds its lock.
    pub(super) async fn run_node(
        &self,
        owner: Owner,
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> wavedb_core::Result<NodeReply> {
        let store = self.store(owner)?;
        self.registry
            .run_node(&store, tenant, struct_hash, op)
            .await
    }
}

/// What a job carries has to cross a thread; what a worker holds must not.
///
/// The second half is enforced structurally rather than asserted — `Rc` in
/// `Worker` makes it so, and an assertion here could only restate it.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<Job>();
};
