//! [`RouteHandle`] — a [`DbHandle`] whose operations each run at their owner
//! (RFC 0067 step 2).
//!
//! Each typed operation becomes a [`NodeOp`] handed to a [`Route`], and the
//! [`NodeReply`] comes back decoded. On a node the route is the shard router,
//! so most operations hop to another thread and run there under their owner's
//! lock; over one store it is a [`LocalRoute`](crate::LocalRoute). Each
//! operation is atomic on its own; nothing spans two of them.
//!
//! It is what a `#[server]` body runs against (the client crate's `ServerDb`
//! wraps it) and what node-side code outside a request uses, such as seeding
//! before serving. Walk-shaped operations come back whole and are yielded as
//! a stream.

use futures::Stream;

use crate::collection::Collection;
use crate::error::Result;
use crate::fuzzy::{Fuzzy, Scored};
use crate::handle::DbHandle;
use crate::id::Id;
use crate::index::Bound;
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::node_op::{NodeOp, NodeReply, Route};
use crate::route_reply::{
    buffered, decode, done, mismatch, value, values, versions,
};
use crate::traits::{NonUniqueStruct, UniqueStruct};
use crate::u48::U48;
use crate::wire::to_wire;

/// A borrowed [`Route`] bound to a tenant.
#[derive(Debug)]
pub struct RouteHandle<'a, R> {
    route: &'a R,
    tenant: U48,
}

// Manual impls: a derive would demand `R: Clone`, but only the reference is
// copied.
impl<R> Clone for RouteHandle<'_, R> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<R> Copy for RouteHandle<'_, R> {}

impl<'a, R: Route> RouteHandle<'a, R> {
    /// Operations on `route`, as `tenant`.
    #[must_use]
    pub const fn new(route: &'a R, tenant: U48) -> Self {
        Self { route, tenant }
    }

    /// The route operations go through.
    #[must_use]
    pub const fn route(&self) -> &'a R {
        self.route
    }

    /// Run `op` on `struct_hash`'s owner.
    async fn run(&self, struct_hash: u64, op: NodeOp) -> Result<NodeReply> {
        self.route.run(self.tenant, struct_hash, op).await
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
