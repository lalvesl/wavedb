//! Ingress routing — where a request stops being the accept thread's problem.
//!
//! The accept loop reads a socket, decodes a [`Request`], and hands it to the
//! shard that owns it. It executes nothing itself: no gate, no engine call, no
//! `Store` of its own. That is the point of the split — accepting is I/O, and
//! serving is work, and they should not share a thread.
//!
//! ## The routing key is the brake's key, and it has to be
//!
//! [`OwnerLocks`](super::OwnerLocks) excludes two operations on one owner from
//! interleaving, and today its key is `(tenant, STRUCT_HASH)` because `Get`,
//! `Update` and `Remove` do not carry a Pivot on the wire (`CONCURRENCY_BRAKE.md`
//! spells out why a *mixed* granularity would be worse than a coarse one).
//! Routing uses the **same** key, via
//! [`Owner::Unique`](super::Owner::Unique) — including for collection
//! commands, whose Pivot-grained [`Owner::Collection`] arm is right for the
//! target and wrong for today. Route one way and brake the other and two
//! operations on one owner can be picked up by two shards holding two
//! different locks, which is the silent index loss with routing in front of
//! it. Both narrow to the Pivot together or neither does.
//!
//! ## The cost that is here on purpose
//!
//! Routing needs the tenant, the tenant comes from the token, so the token is
//! verified here **and again** by gate 1 on the shard. One extra HMAC per
//! request. Threading the resolved [`Caller`](wavedb_core::expose::Caller)
//! through the [`Job`] would remove it, at the price of a `handle` that trusts
//! its caller to have run gate 1 — a seam worth having only once it is
//! measured, and it is not.

use std::sync::Arc;

use async_channel::Sender;
use futures::channel::oneshot;
use wavedb_net::frame::{NodeError, NodeErrorKind, Request, Response};

use super::worker::{self, Job};
use super::{DiskHandle, Owner, OwnerLocks};
use crate::__rt::NodeTasks;
use crate::dispatch::{self, Answer};
use crate::feed::MutationFeed;

/// The fan-out onto the shard workers.
///
/// Cheap to clone (a `Vec` of channel senders and a key) and `Send`, so every
/// connection task can hold one.
#[derive(Clone)]
pub struct Router {
    workers: Vec<Sender<Job>>,
    secret: [u8; 32],
}

impl Router {
    /// Start `count` workers over one disk actor and return the router onto
    /// them.
    ///
    /// `locks` is shared by every worker — one table for the node, which is
    /// what makes the brake a brake once operations run on several threads.
    /// `publisher` is likewise one channel: every worker's committed mutations
    /// converge on the accept thread that owns the subscription table.
    ///
    /// # Errors
    /// A worker thread failing to spawn.
    pub fn start<E>(
        registry: E,
        disk: &DiskHandle,
        locks: &Arc<OwnerLocks>,
        publisher: &MutationFeed,
        secret: [u8; 32],
        count: usize,
    ) -> wavedb_platform::Result<Self>
    where
        E: NodeTasks,
    {
        let workers = (0..count.max(1))
            .map(|_| {
                worker::start(
                    registry,
                    disk.clone(),
                    Arc::clone(locks),
                    publisher.clone(),
                    secret,
                )
            })
            .collect::<wavedb_platform::Result<Vec<_>>>()?;
        Ok(Self { workers, secret })
    }

    /// How many shards this router fans out to.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.workers.len()
    }

    /// Route one request to its owner's shard and wait for the answer.
    ///
    /// Never returns a transport error: a worker that is gone becomes a
    /// [`Response::Err`], because the caller upstream has a socket to answer
    /// and needs bytes for it either way.
    pub async fn dispatch(&self, request: Request) -> Answer {
        let struct_hash = request.frame.struct_hash;
        let Some(worker) = self.workers.get(self.shard_for(&request)) else {
            // Unreachable: `start` refuses an empty set and `shard_for` folds
            // onto the length. Said rather than indexed, because a routing
            // function has no business panicking on a request path.
            return refused(struct_hash, "no shard");
        };
        let (answer, wait) = oneshot::channel();
        if worker.send(Job { request, answer }).await.is_err() {
            return refused(struct_hash, "shard stopped");
        }
        wait.await
            .unwrap_or_else(|_| refused(struct_hash, "shard dropped the job"))
    }

    /// The shard owning `request` — see the module note on why this is the
    /// type key rather than the Pivot.
    ///
    /// A request whose identity does not verify routes to shard 0. It is about
    /// to be refused by gate 1 wherever it lands, so the only thing that
    /// matters is that resolving it costs nothing extra.
    fn shard_for(&self, request: &Request) -> usize {
        let Ok(caller) = dispatch::identify(&request.auth, &self.secret) else {
            return 0;
        };
        Owner::Unique {
            tenant: caller.tenant,
            struct_hash: request.frame.struct_hash,
        }
        .shard(self.workers.len())
    }
}

/// The node could not reach a shard. A backend fault, not a refusal of the
/// request: nothing about the request was wrong.
fn refused(struct_hash: u64, why: &str) -> Answer {
    Answer {
        response: Response::Err(NodeError {
            kind: NodeErrorKind::Backend,
            struct_hash,
            message: format!("shard router: {why}"),
        }),
        sync: None,
    }
}

/// Every connection task holds one, and connection tasks are not all on one
/// thread once the accept loop stops being the only executor.
const _: fn() = || {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Router>();
};

#[cfg(test)]
mod tests {
    use super::Router;
    use wavedb_core::U48;
    use wavedb_core::expose::Command;
    use wavedb_net::frame::{Auth, CommandFrame, Request};

    fn request(tenant: u32, struct_hash: u64) -> Request {
        Request {
            auth: Auth::Anonymous {
                tenant: U48::from(tenant),
            },
            frame: CommandFrame {
                struct_hash,
                command: Command::Get,
                payload: Vec::new(),
            },
            sync: Vec::new(),
        }
    }

    /// A router with no live workers: the senders are dropped immediately, so
    /// every dispatch takes the "shard stopped" path. Enough to pin routing,
    /// which is the pure part.
    fn router(count: usize) -> Router {
        let workers = (0..count).map(|_| async_channel::bounded(1).0).collect();
        Router {
            workers,
            secret: [0u8; 32],
        }
    }

    /// The property the brake depends on: one owner, one shard, always.
    #[test]
    fn one_owner_always_routes_to_one_shard() {
        let router = router(8);
        let first = router.shard_for(&request(7, 0xDEAD_BEEF));
        for _ in 0..100 {
            assert_eq!(router.shard_for(&request(7, 0xDEAD_BEEF)), first);
        }
    }

    /// Tenants and types both move the route — the same property
    /// `super::super::route` asserts, restated at the seam that consumes it,
    /// because this is where getting it wrong concentrates a node's load.
    #[test]
    fn tenants_and_types_spread_across_shards() {
        let router = router(8);
        let by_tenant: std::collections::HashSet<usize> = (0..64u32)
            .map(|t| router.shard_for(&request(t, 9)))
            .collect();
        assert!(by_tenant.len() > 4, "the tenant barely moved the route");
        let by_type: std::collections::HashSet<usize> = (0..64u64)
            .map(|h| router.shard_for(&request(3, h)))
            .collect();
        assert!(by_type.len() > 4, "the type barely moved the route");
    }

    #[test]
    fn one_shard_owns_every_request() {
        let router = router(1);
        for t in 0..32u32 {
            assert_eq!(router.shard_for(&request(t, u64::from(t))), 0);
        }
    }

    /// A dead shard is a backend fault carrying the request's own hash — the
    /// client has to be able to attribute it.
    #[tokio::test]
    async fn a_stopped_shard_answers_rather_than_hangs() {
        let answer = router(2).dispatch(request(1, 0xABCD)).await;
        let wavedb_net::frame::Response::Err(err) = answer.response else {
            panic!("a stopped shard must refuse");
        };
        assert_eq!(err.kind, wavedb_net::frame::NodeErrorKind::Backend);
        assert_eq!(err.struct_hash, 0xABCD);
    }
}
