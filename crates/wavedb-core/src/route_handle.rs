//! [`RouteHandle`] — a [`DbHandle`] whose operations each run at their owner
//! (RFC 0067 step 2).
//!
//! Each typed operation becomes a [`NodeOp`] handed to a [`Route`], and the
//! [`NodeReply`] comes back decoded. On a node the route is the shard router,
//! so most operations hop to another thread and run there under their owner's
//! lock; over one store it is a [`LocalRoute`](crate::LocalRoute). Each
//! operation is atomic on its own; nothing spans two of them — unless the
//! body [`lock`](RouteHandle::lock)s their owners first ([`crate::lease`]).
//!
//! It is what a `#[server]` body runs against (the client crate's `ServerDb`
//! wraps it) and what node-side code outside a request uses, such as seeding
//! before serving. Walk-shaped operations come back whole and are yielded as
//! a stream.

use futures::Stream;

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::fuzzy::{Fuzzy, Scored};
use crate::handle::DbHandle;
use crate::id::Id;
use crate::index::Bound;
use crate::lease::{Leases, Locked, Reach};
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::node_op::{NodeOp, NodeReply};
use crate::placement::Owner;
use crate::route::Route;
use crate::route_reply::{
    buffered, decode, done, mismatch, value, values, versions,
};
use crate::traits::{NonUniqueStruct, UniqueStruct};
use crate::u48::U48;
use crate::wire::to_wire;

/// A borrowed [`Route`] bound to a tenant, and — for a `#[server]` body —
/// the owners that body holds.
#[derive(Debug)]
pub struct RouteHandle<'a, R: Route> {
    route: &'a R,
    tenant: U48,
    /// `None`: a handle that cannot hold owners (seeding, tools).
    leases: Option<&'a Leases<R::Lease>>,
}

// Manual impls: a derive would demand `R: Clone`, but only the references
// are copied.
impl<R: Route> Clone for RouteHandle<'_, R> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<R: Route> Copy for RouteHandle<'_, R> {}

impl<'a, R: Route> RouteHandle<'a, R> {
    /// Operations on `route`, as `tenant`. Each takes its owner's lock for
    /// itself; this handle cannot [`lock`](Self::lock).
    #[must_use]
    pub const fn new(route: &'a R, tenant: U48) -> Self {
        Self {
            route,
            tenant,
            leases: None,
        }
    }

    /// Operations on `route`, as `tenant`, able to hold owners across awaits
    /// in `leases` — one table per `#[server]` body invocation.
    #[must_use]
    pub const fn leasing(
        route: &'a R,
        tenant: U48,
        leases: &'a Leases<R::Lease>,
    ) -> Self {
        Self {
            route,
            tenant,
            leases: Some(leases),
        }
    }

    /// The route operations go through.
    #[must_use]
    pub const fn route(&self) -> &'a R {
        self.route
    }

    /// The owners this handle's body holds, if it can hold any.
    #[must_use]
    pub const fn leases(&self) -> Option<&'a Leases<R::Lease>> {
        self.leases
    }

    /// Hold every owner in `owners` (reached under this handle's tenant)
    /// until the returned [`Locked`] drops. Operations on them run under
    /// their leases meanwhile; operations past them are refused
    /// ([`crate::lease`] has the rules).
    ///
    /// The owners are taken in [`Owner`]'s order whatever order they are
    /// given in, so two bodies asking for overlapping sets cannot each hold
    /// a part.
    ///
    /// # Errors
    /// [`Error::LockOrder`] when a lock is already held, or this handle
    /// cannot hold owners; the route's fault taking a lease (the owners
    /// taken so far are released).
    pub async fn lock(&self, owners: &[Owner]) -> Result<Locked<'a, R::Lease>> {
        let leases = self.leases.ok_or(Error::LockOrder(
            "this handle cannot hold owners: only a #[server] body's can",
        ))?;
        let locked = leases.begin()?;
        let mut wanted = owners.to_vec();
        wanted.sort_unstable();
        wanted.dedup();
        for owner in wanted {
            let lease = self.route.lease(self.tenant, owner).await?;
            leases.hold(self.tenant, owner, lease);
        }
        Ok(locked)
    }

    /// Run `op` on `struct_hash`'s owner: under its lease when the body
    /// holds it, plainly when the body holds nothing, refused when the body
    /// holds a lock that does not cover it.
    async fn run(&self, struct_hash: u64, op: NodeOp) -> Result<NodeReply> {
        let Some(leases) = self.leases else {
            return self.route.run(self.tenant, struct_hash, op).await;
        };
        let owner = op.owner(self.tenant, struct_hash);
        let creates = matches!(op, NodeOp::CreatePivot(_));
        match leases.reach(self.tenant, owner, creates) {
            Reach::Plain => self.route.run(self.tenant, struct_hash, op).await,
            Reach::Leased(lease) => {
                self.route
                    .run_leased(&lease, self.tenant, struct_hash, op)
                    .await
            }
            Reach::Created => {
                let reply =
                    self.route.run(self.tenant, struct_hash, op).await?;
                if creates {
                    leases.created(self.tenant, owner);
                }
                Ok(reply)
            }
            Reach::Refused => Err(Error::LockOrder(
                "an owner outside the held lock: take every owner in one lock",
            )),
        }
    }
}

impl<R: Route> DbHandle for RouteHandle<'_, R> {
    type Error = crate::Error;

    fn tenant(&self) -> U48 {
        self.tenant
    }

    fn as_tenant(&self, tenant: U48) -> Self {
        Self {
            route: self.route,
            tenant,
            leases: self.leases,
        }
    }

    async fn get_unique<T: UniqueStruct>(&self) -> Result<Option<T>> {
        value(self.run(T::STRUCT_HASH, NodeOp::GetUnique).await?)
    }

    async fn save_unique<T: UniqueStruct>(&self, value: &T) -> Result<()> {
        let op = NodeOp::SaveUnique(to_wire(value));
        done(&self.run(T::STRUCT_HASH, op).await?)
    }

    fn unique_history<T: UniqueStruct + 'static>(
        &self,
    ) -> impl Stream<Item = Result<(Metadata, T)>> {
        buffered(async move {
            versions(self.run(T::STRUCT_HASH, NodeOp::UniqueHistory).await?)
        })
    }

    async fn create_pivot<T: NonUniqueStruct>(&self) -> Result<LocalId> {
        // Minted here, created where the id places it: the owner of a new
        // collection is known before the collection exists.
        let pivot = Collection::<T>::mint_pivot(self.tenant);
        done(&self.run(T::STRUCT_HASH, NodeOp::CreatePivot(pivot)).await?)?;
        Ok(pivot)
    }

    async fn insert<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
    ) -> Result<Id> {
        let body = to_wire(value);
        match self
            .run(T::STRUCT_HASH, NodeOp::Insert { pivot, body })
            .await?
        {
            NodeReply::Inserted(id) => Ok(id),
            _ => Err(mismatch()),
        }
    }

    async fn get_record<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<Option<T>> {
        value(
            self.run(T::STRUCT_HASH, NodeOp::GetRecord { pivot, id })
                .await?,
        )
    }

    async fn update<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
    ) -> Result<()> {
        let body = to_wire(value);
        let op = NodeOp::Update { pivot, id, body };
        done(&self.run(T::STRUCT_HASH, op).await?)
    }

    async fn remove<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<bool> {
        match self
            .run(T::STRUCT_HASH, NodeOp::Remove { pivot, id })
            .await?
        {
            NodeReply::Removed(removed) => Ok(removed),
            _ => Err(mismatch()),
        }
    }

    fn all<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
    ) -> impl Stream<Item = Result<T>> {
        buffered(async move {
            values(self.run(T::STRUCT_HASH, NodeOp::All { pivot }).await?)
        })
    }

    fn listed<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<T>> {
        let op = NodeOp::Listed {
            pivot,
            index,
            offset,
            limit: None,
        };
        buffered(async move { values(self.run(T::STRUCT_HASH, op).await?) })
    }

    fn listed_page<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
        limit: u32,
    ) -> impl Stream<Item = Result<T>> {
        let op = NodeOp::Listed {
            pivot,
            index,
            offset,
            limit: Some(limit),
        };
        buffered(async move { values(self.run(T::STRUCT_HASH, op).await?) })
    }

    async fn list_len<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
    ) -> Result<u64> {
        let op = NodeOp::ListLen { pivot, index };
        match self.run(T::STRUCT_HASH, op).await? {
            NodeReply::Count(len) => Ok(len),
            _ => Err(mismatch()),
        }
    }

    async fn fuzzy_search<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        query: &str,
        mode: Fuzzy,
        limit: usize,
    ) -> Result<Vec<Scored<(Id, T)>>> {
        let op = NodeOp::Fuzzy {
            pivot,
            index,
            query: query.to_owned(),
            mode,
            limit,
        };
        let NodeReply::Scored(hits) = self.run(T::STRUCT_HASH, op).await?
        else {
            return Err(mismatch());
        };
        hits.into_iter()
            .map(|hit| {
                Ok(Scored {
                    item: (hit.item.0, decode(&hit.item.1)?),
                    score: hit.score,
                    rank: hit.rank,
                })
            })
            .collect()
    }

    fn search_by<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<T>> {
        let op = NodeOp::SearchBy {
            pivot,
            index,
            bound,
        };
        buffered(async move { values(self.run(T::STRUCT_HASH, op).await?) })
    }

    fn record_history<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> impl Stream<Item = Result<(Metadata, T)>> {
        let op = NodeOp::RecordHistory { pivot, id };
        buffered(async move { versions(self.run(T::STRUCT_HASH, op).await?) })
    }
}
