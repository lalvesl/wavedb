//! Where a `#[server]` body's operations run ([`Route`]), and the route over
//! one store in hand ([`LocalRoute`]) — RFC 0067 steps 2 and 5.

use std::future::Future;

use crate::error::Result;
use crate::expose::Exposure;
use crate::node_op::{NodeOp, NodeReply};
use crate::placement::Owner;
use crate::store::Store;
use crate::u48::U48;

/// Where a server body's operations run.
///
/// The node routes each one to its owner's shard and runs it under the
/// owner's lock; [`LocalRoute`] runs it on a store in hand, for tests and
/// in-process use. Generic, never `dyn`: a body is monomorphised per route.
///
/// A route also grants **leases** (RFC 0067 §5): an owner held across a
/// body's awaits, its operations run under the lease rather than each taking
/// the lock on its own. Bodies reach them through
/// [`RouteHandle::lock`](crate::RouteHandle::lock), which keeps the order
/// that makes them safe.
pub trait Route {
    /// An owner held. Dropping the last clone releases it, and that cannot
    /// fail.
    type Lease: Clone;

    /// Run `op` on `struct_hash` under `tenant`, wherever its owner lives.
    fn run(
        &self,
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> impl Future<Output = Result<NodeReply>>;

    /// Hold `owner`, reached under `tenant`, until the lease is dropped;
    /// resolves once it is held.
    fn lease(
        &self,
        tenant: U48,
        owner: Owner,
    ) -> impl Future<Output = Result<Self::Lease>>;

    /// Run `op` under `lease` — on the owner the lease holds, without
    /// taking its lock again.
    fn run_leased(
        &self,
        lease: &Self::Lease,
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> impl Future<Output = Result<NodeReply>>;
}

/// A [`Route`] over one store: every owner is here.
#[derive(Debug)]
pub struct LocalRoute<'a, S, E> {
    store: &'a S,
    registry: &'a E,
}

impl<'a, S: Store, E: Exposure> LocalRoute<'a, S, E> {
    /// Run operations on `store`, typed through `registry`.
    #[must_use]
    pub const fn new(store: &'a S, registry: &'a E) -> Self {
        Self { store, registry }
    }
}

/// A store in hand has no lock table: whoever holds the route is the only
/// caller, so a lease is nothing and a leased operation is a plain one.
impl<S: Store, E: Exposure> Route for LocalRoute<'_, S, E> {
    type Lease = ();

    async fn run(
        &self,
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> Result<NodeReply> {
        self.registry
            .run_node(self.store, tenant, struct_hash, op)
            .await
    }

    fn lease(
        &self,
        _tenant: U48,
        _owner: Owner,
    ) -> impl Future<Output = Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn run_leased(
        &self,
        (): &(),
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> Result<NodeReply> {
        self.run(tenant, struct_hash, op).await
    }
}
