//! Leases on the owner's thread ([RFC 0067] §5).
//!
//! A `#[server]` body runs at the edge, often on another thread than the
//! owners it touches. To hold one across its awaits it sends a
//! [`Job::Lease`](super::Job::Lease) to the owner's worker, where a job task
//! takes the owner's lock — waiting in the owner's chain like any operation
//! — and answers with a queue into itself. Until that queue closes, the task
//! keeps the lock and runs the operations it receives, one after another;
//! the body's operations on the owner travel that queue instead of the
//! worker's, so they never wait behind the lock their own body holds.
//!
//! Releasing is dropping the [`Lease`]: the queue closes, the task finishes
//! the operation in hand and lets the owner go. Nothing is sent, so it
//! cannot fail — and a body dropped mid-await (its connection closed, the
//! node halting) releases the same way.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

use async_channel::Sender;
use futures::channel::oneshot;
use wavedb_core::{NodeOp, NodeReply, U48};

use super::{Owner, OwnerKey, Worker};
use crate::__rt::NodeTasks;

/// One operation run under a lease, and where its answer goes.
pub struct Leased {
    pub struct_hash: u64,
    pub op: NodeOp,
    pub answer: oneshot::Sender<wavedb_core::Result<NodeReply>>,
}

/// An owner held for a body: the queue into the task holding it. Clones
/// share the hold; it ends when the last one drops.
#[derive(Clone)]
pub struct Lease {
    ops: Sender<Leased>,
}

impl Lease {
    /// Run `op` on the held owner.
    ///
    /// # Errors
    /// The operation's own; a backend fault when the holding task is gone.
    pub async fn run(
        &self,
        struct_hash: u64,
        op: NodeOp,
    ) -> wavedb_core::Result<NodeReply> {
        let (answer, wait) = oneshot::channel();
        let leased = Leased {
            struct_hash,
            op,
            answer,
        };
        let gone = || wavedb_core::Error::Backend("lease: holder gone".into());
        self.ops.send(leased).await.map_err(|_| gone())?;
        wait.await.unwrap_or_else(|_| Err(gone()))
    }
}

/// The lease task's body: take `owner`'s lock, hand out the queue, run what
/// arrives until it closes.
pub async fn hold<E: NodeTasks>(
    worker: &Worker<E>,
    tenant: U48,
    owner: Owner,
    answer: oneshot::Sender<Lease>,
) {
    let held = worker.locks().acquire(OwnerKey::new(tenant, owner)).await;
    // One operation at a time: a body's concurrent operations on its owner
    // wait their turn here, as they would on the lock.
    let (ops, inbox) = async_channel::bounded::<Leased>(1);
    // The body gave up while waiting: release at once.
    if answer.send(Lease { ops }).is_err() {
        return;
    }
    while let Ok(Leased {
        struct_hash,
        op,
        answer,
    }) = inbox.recv().await
    {
        let _busy = worker.busy();
        let reply = if op.owner(tenant, struct_hash) == owner {
            worker.run_node(owner, tenant, struct_hash, op).await
        } else {
            Err(wavedb_core::Error::LockOrder(
                "an operation sent under a lease on another owner",
            ))
        };
        let _ = answer.send(reply);
    }
    drop(held);
}

/// What crosses: the request for a lease, and the lease itself.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<Lease>();
    assert_send::<Leased>();
};
