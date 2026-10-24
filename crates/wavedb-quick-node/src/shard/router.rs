//! Routing — where each piece of work goes, and how it gets there.
//!
//! A connection reads a socket, decodes a [`Request`] — or a WebSocket
//! session's `Call` — and runs the edge ([`crate::edge`]): gate 1, then every
//! operation at its owner. Every worker holds a router onto every worker,
//! its own included: an operation it owns goes into its own queue, the same
//! way an operation another worker owns goes into that one's (RFC 0067 §6).
//! Accepting and coordinating are I/O; executing is work, and it belongs to
//! the worker that owns the data.
//!
//! ## The routing key is the brake's key, and it has to be
//!
//! [`OwnerLocks`](super::OwnerLocks) excludes two operations on one owner from
//! interleaving — one table per worker, which is enough only because of this
//! routing. Both use the same [`Owner`]: a collection command's Pivot, read
//! off the front of its payload, or a Unique type under the caller's tenant
//! ([`Owner::of_command`]); a node operation's owner comes from the operation
//! itself ([`NodeOp::owner`]). The registry says how a hash is addressed
//! ([`Exposure::addressing`]), which is why the router holds it. Route one way
//! and brake another and two operations on one owner could be picked up by
//! two workers holding two different locks — the silent index loss, with
//! routing in front of it.
//!
//! A subscription is placed by the same rule: a topic is an owner, so its
//! subscribers are registered with the worker its mutations commit on
//! ([`Router::watch`]).
//!
//! A `#[server]` function has no owner: it runs at the edge, and each
//! operation it performs is routed here on its own ([`Route`]).
//!
//! ## Gate 1 runs once
//!
//! A POST's token is verified at the edge, and the [`Caller`] it resolves
//! travels with the work, exactly as a WebSocket session's does from its
//! `Hello`. A worker runs gates 2–3 only.

use async_channel::Sender;
use futures::channel::oneshot;
use wavedb_core::expose::{Caller, Exposure};

use wavedb_core::{NodeOp, NodeReply, Route, U48};
use wavedb_net::frame::{
    CommandFrame, NodeError, NodeErrorKind, Request, Response,
};
use wavedb_net::ws::Topic;
use wavedb_storage::PageStore;

use super::Owner;
use super::lease::Lease;
use super::spawn::{self, Closing};
use super::worker::Job;
use crate::__rt::NodeTasks;
use crate::dispatch::Answer;
use crate::edge::{self, Exec};
use crate::subscribe::Outbox;
use crate::upkeep::Maintenance;

/// The fan-out onto the workers.
///
/// Cheap to clone (the registry is a zero-sized value, then a `Vec` of channel
/// senders and a key) and `Send`, so every connection task can hold one.
#[derive(Clone)]
pub struct Router<E> {
    registry: E,
    workers: Vec<Sender<Job>>,
    /// Data shards in the database; each worker serves a contiguous range.
    shards: usize,
    secret: [u8; 32],
}

impl<E> Router<E> {
    pub(super) const fn new(
        registry: E,
        workers: Vec<Sender<Job>>,
        shards: usize,
        secret: [u8; 32],
    ) -> Self {
        Self {
            registry,
            workers,
            shards,
            secret,
        }
    }

    /// Gate 1's key, for a WebSocket session's `Hello`.
    pub(crate) const fn secret(&self) -> &[u8; 32] {
        &self.secret
    }
}

impl<E: NodeTasks> Router<E> {
    /// Start `threads` workers over the database's data shards (`stores`, in
    /// shard order), with no ingress of their own, and return the router onto
    /// them with one receiver per worker for the outcome of closing its
    /// stores ([`spawn::start`]). The workers stop once every clone of the
    /// router is dropped.
    ///
    /// # Errors
    /// A worker thread failing to spawn.
    pub fn start(
        registry: E,
        stores: Vec<PageStore>,
        secret: [u8; 32],
        threads: usize,
        policy: Maintenance,
    ) -> wavedb_platform::Result<(Self, Vec<Closing>)> {
        spawn::start(registry, stores, secret, threads, policy, None)
    }
}

impl<E: Exposure> Router<E> {
    /// How many workers this router fans out to.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.workers.len()
    }

    /// Serve one request: gate 1, then its work at its owners.
    ///
    /// Never returns a transport error: a worker that is gone becomes a
    /// [`Response::Err`], because the caller upstream has a socket to answer
    /// and needs bytes for it either way.
    pub async fn dispatch(&self, request: Request) -> Answer {
        edge::handle(&self.registry, self, &self.secret, request).await
    }

    /// Serve one WebSocket `Call`. `caller` is what the session's gate 1
    /// resolved at `Hello`.
    ///
    /// Never returns a transport error, as [`dispatch`](Self::dispatch).
    pub async fn call(&self, caller: Caller, frame: CommandFrame) -> Response {
        edge::command(&self.registry, self, caller, frame).await
    }

    /// Have `topic`'s owner publish its mutations into `outbox`, and return
    /// once it is registered there — a mutation committed after this returns
    /// reaches the outbox. A worker that is gone registers nothing; the
    /// session then hears nothing, and its client catches up by cursor.
    pub async fn watch(&self, tenant: U48, topic: Topic, outbox: Outbox) {
        let (done, registered) = oneshot::channel();
        let job = Job::Watch {
            tenant,
            topic,
            outbox,
            done,
        };
        if self.post(topic_owner(tenant, topic), job).await.is_ok() {
            let _ = registered.await;
        }
    }

    /// Stop `topic`'s owner publishing into `outbox`.
    pub async fn unwatch(&self, tenant: U48, topic: Topic, outbox: Outbox) {
        let job = Job::Unwatch {
            tenant,
            topic,
            outbox,
        };
        let _ = self.post(topic_owner(tenant, topic), job).await;
    }

    /// Queue `job` on the worker serving `owner`'s data shard.
    async fn post(&self, owner: Owner, job: Job) -> Result<(), &'static str> {
        // Unreachable: `start` refuses an empty set and the range arithmetic
        // stays below the length. Said rather than indexed, because a routing
        // function has no business panicking on a request path.
        let worker =
            self.workers.get(self.worker_of(owner)).ok_or("no shard")?;
        worker.send(job).await.map_err(|_| "shard stopped")
    }

    /// The worker serving `owner`'s data shard.
    fn worker_of(&self, owner: Owner) -> usize {
        owner.shard(self.shards) * self.workers.len() / self.shards
    }

    /// The owner of `frame` reached under `tenant` — the lock's key, so one
    /// owner is always on one worker, however its work arrived.
    fn owner(&self, tenant: U48, frame: &CommandFrame) -> Owner {
        Owner::of_command(
            self.registry.addressing(frame.struct_hash),
            tenant,
            frame.struct_hash,
            frame.command,
            &frame.payload,
        )
    }
}

/// The owner whose commits are `topic`'s mutations: the collection's Pivot,
/// or the Unique type under the tenant.
const fn topic_owner(tenant: U48, topic: Topic) -> Owner {
    match topic.pivot {
        Some(pivot) => Owner::Collection(pivot),
        None => Owner::Unique {
            tenant,
            struct_hash: topic.struct_hash,
        },
    }
}

impl<E: Exposure> Exec for Router<E> {
    async fn owned(&self, caller: Caller, frame: CommandFrame) -> Response {
        let owner = self.owner(caller.tenant, &frame);
        let h = frame.struct_hash;
        let (answer, wait) = oneshot::channel();
        let job = Job::Call {
            caller,
            frame,
            answer,
        };
        if let Err(why) = self.post(owner, job).await {
            return refused(h, why);
        }
        wait.await
            .unwrap_or_else(|_| refused(h, "shard dropped the job"))
    }
}

impl<E: Exposure> Route for Router<E> {
    type Lease = Lease;

    async fn run(
        &self,
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> wavedb_core::Result<NodeReply> {
        let owner = op.owner(tenant, struct_hash);
        let (answer, wait) = oneshot::channel();
        let job = Job::Node {
            tenant,
            struct_hash,
            op,
            answer,
        };
        self.post(owner, job).await.map_err(unreachable)?;
        wait.await
            .unwrap_or_else(|_| Err(unreachable("shard dropped the job")))
    }

    /// A lease job on the owner's worker; resolves once the owner is held
    /// there. A worker whose job pool is full refuses rather than hold its
    /// queue for the length of a body.
    async fn lease(
        &self,
        tenant: U48,
        owner: Owner,
    ) -> wavedb_core::Result<Lease> {
        let (answer, held) = oneshot::channel();
        let job = Job::Lease {
            tenant,
            owner,
            answer,
        };
        self.post(owner, job).await.map_err(unreachable)?;
        held.await
            .map_err(|_| unreachable("shard refused the lease"))
    }

    async fn run_leased(
        &self,
        lease: &Lease,
        _tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> wavedb_core::Result<NodeReply> {
        lease.run(struct_hash, op).await
    }
}

/// The router could not reach a worker, for a node operation.
fn unreachable(why: &str) -> wavedb_core::Error {
    wavedb_core::Error::Backend(format!("shard router: {why}"))
}

/// The node could not reach a shard. A backend fault, not a refusal of the
/// request: nothing about the request was wrong.
fn refused(struct_hash: u64, why: &str) -> Response {
    Response::Err(NodeError {
        kind: NodeErrorKind::Backend,
        struct_hash,
        message: format!("shard router: {why}"),
    })
}

#[cfg(test)]
mod tests {
    use super::Router;
    use wavedb_core::expose::{Addressing, Command, Exposure};
    use wavedb_core::wire::to_wire;
    use wavedb_core::{Error, Id, LocalId, U48};
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
    fn router(count: usize) -> Router<Registry> {
        let workers = (0..count).map(|_| async_channel::bounded(1).0).collect();
        Router {
            registry: Registry,
            shards: 64,
            workers,
            secret: [0u8; 32],
        }
    }

    /// Serves one collection type, [`COLLECTION`]; every other hash is a
    /// Unique type.
    #[derive(Clone, Copy)]
    struct Registry;

    const COLLECTION: u64 = 0xC011;

    impl Exposure for Registry {
        fn knows(&self, _: u64) -> bool {
            true
        }

        fn decode_check(
            &self,
            struct_hash: u64,
            _: &[u8],
        ) -> wavedb_core::Result<()> {
            Err(Error::UnknownStructHash(struct_hash))
        }

        fn addressing(&self, struct_hash: u64) -> Option<Addressing> {
            Some(if struct_hash == COLLECTION {
                Addressing::Collection
            } else {
                Addressing::Unique
            })
        }
    }

    fn shard_for(router: &Router<Registry>, request: &Request) -> usize {
        let tenant = match request.auth {
            Auth::Anonymous { tenant } => tenant,
            Auth::Token(_) => U48::MAX,
        };
        router.worker_of(router.owner(tenant, &request.frame))
    }

    fn on_pivot(command: Command, pivot: u64, payload: &[u8]) -> Request {
        let mut r = request(7, COLLECTION);
        r.frame.command = command;
        let mut bytes = to_wire(&LocalId::new(pivot, false, 9));
        bytes.extend_from_slice(payload);
        r.frame.payload = bytes;
        r
    }

    /// Every command on one collection lands on one shard, and two
    /// collections of one type do not have to share one.
    #[test]
    fn a_collection_routes_by_its_pivot() {
        let router = router(64);
        let id = to_wire(&Id::new(5, U48::from(7u32), false, 9));
        let shards: std::collections::HashSet<usize> = [
            on_pivot(Command::Insert, 11, &[]),
            on_pivot(Command::Get, 11, &id),
            on_pivot(Command::Update, 11, &id),
            on_pivot(Command::Remove, 11, &id),
            on_pivot(Command::All, 11, &[]),
        ]
        .iter()
        .map(|r| shard_for(&router, r))
        .collect();
        assert_eq!(shards.len(), 1, "one collection, one shard");

        let spread: std::collections::HashSet<usize> = (0..64u64)
            .map(|p| shard_for(&router, &on_pivot(Command::All, p, &[])))
            .collect();
        assert!(spread.len() > 16, "pivots of one type barely spread");
    }

    /// The property the brake depends on: one owner, one shard, always.
    #[test]
    fn one_owner_always_routes_to_one_shard() {
        let router = router(8);
        let first = shard_for(&router, &request(7, 0xDEAD_BEEF));
        for _ in 0..100 {
            assert_eq!(shard_for(&router, &request(7, 0xDEAD_BEEF)), first);
        }
    }

    /// Tenants and types both move the route — the same property
    /// `super::super::route` asserts, restated at the seam that consumes it,
    /// because this is where getting it wrong concentrates a node's load.
    #[test]
    fn tenants_and_types_spread_across_shards() {
        let router = router(8);
        let by_tenant: std::collections::HashSet<usize> = (0..64u32)
            .map(|t| shard_for(&router, &request(t, 9)))
            .collect();
        assert!(by_tenant.len() > 4, "the tenant barely moved the route");
        let by_type: std::collections::HashSet<usize> = (0..64u64)
            .map(|h| shard_for(&router, &request(3, h)))
            .collect();
        assert!(by_type.len() > 4, "the type barely moved the route");
    }

    #[test]
    fn one_shard_owns_every_request() {
        let router = router(1);
        for t in 0..32u32 {
            assert_eq!(shard_for(&router, &request(t, u64::from(t))), 0);
        }
    }

    /// A dead shard is a backend fault carrying the request's own hash — the
    /// client has to be able to attribute it.
    #[test]
    fn a_stopped_shard_answers_rather_than_hangs() {
        wavedb_platform::carrier::block_on(async {
            let answer = router(2).dispatch(request(1, 0xABCD)).await;
            let wavedb_net::frame::Response::Err(err) = answer.response else {
                panic!("a stopped shard must refuse");
            };
            assert_eq!(err.kind, wavedb_net::frame::NodeErrorKind::Backend);
            assert_eq!(err.struct_hash, 0xABCD);
        });
    }
}
