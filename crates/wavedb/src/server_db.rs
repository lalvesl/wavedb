//! `ServerDb` — the node-side execution context a `#[server]` function body
//! runs against.
//!
//! It implements [`DbHandle`], so the same generated spelling a client uses
//! — `T::get(db)`, `T::collection(pivot)`, `col.insert(db, v)` — resolves
//! inside a server body. The `#[server]` macro retypes a body's `db: &Db`
//! parameter to `db: &ServerDb<R>`, so one body source drives both sides.
//!
//! A body **coordinates; it does not execute**. Each operation belongs to an
//! owner — a collection's Pivot, or a Unique type under the tenant — and runs
//! where that owner lives and under its lock (RFC 0067): the context hands it
//! to its [`Route`] as a [`NodeOp`] and decodes the [`NodeReply`]. On a node
//! the route is the shard router, so most operations hop to another thread;
//! in process it is a [`LocalRoute`](wavedb_core::LocalRoute) over one store.
//! Each operation is atomic on its own; nothing spans two of them.
//!
//! Walk-shaped operations come back whole and are yielded as a stream, like
//! the client's.

use futures::Stream;
use wavedb_core::fuzzy::{Fuzzy, Scored};
use wavedb_core::wire::to_wire;
use wavedb_core::{
    Bound, Collection, DbHandle, Id, LocalId, Metadata, NodeOp, NodeReply,
    NonUniqueStruct, Route, U48, UniqueStruct,
};

use crate::error::{Error, Result};
use crate::server_reply::{
    buffered, decode, done, mismatch, value, values, versions,
};

/// A node-side handle: a borrowed [`Route`] plus the bound identity. Cheap
/// to re-scope with [`as_tenant`](DbHandle::as_tenant).
pub struct ServerDb<'a, R> {
    route: &'a R,
    tenant: U48,
    user: U48,
}

impl<'a, R: Route> ServerDb<'a, R> {
    /// Wrap a route + tenant as an execution context (`user = tenant` — the
    /// engine-local/B2C identity).
    pub const fn new(route: &'a R, tenant: U48) -> Self {
        Self {
            route,
            tenant,
            user: tenant,
        }
    }

    /// The context a verified request executes as — what the generated
    /// `#[server]` dispatch builds from gate 1's [`Caller`].
    ///
    /// [`Caller`]: wavedb_core::Caller
    pub const fn for_caller(route: &'a R, caller: wavedb_core::Caller) -> Self {
        Self {
            route,
            tenant: caller.tenant,
            user: caller.user,
        }
    }

    /// The verified user this context executes as (`U48::MAX` = the
    /// unauthenticated tier — only reachable inside `#[server(public)]`).
    #[must_use]
    pub const fn user(&self) -> U48 {
        self.user
    }

    /// Re-scope to an explicit `(user, tenant)` — the node's impersonation
    /// seam, and the counterpart to [`as_tenant`](DbHandle::as_tenant).
    ///
    /// `as_tenant` moves the tenant and **keeps the caller's user**, which is
    /// what acting *on that caller's behalf* means. This one replaces both,
    /// which is what a node acting as its own authority wants: records a
    /// `#[server(public)]` body writes into a space the caller does not own
    /// should be stamped with the identity that owns them, not with whoever
    /// happened to call the function — inside a public body that user is
    /// `U48::MAX`, the anonymous tier, which would otherwise be recorded as
    /// the author in `Metadata`.
    ///
    /// There is no client equivalent, by construction: the node is the
    /// authority over identity, so this is deliberately absent from
    /// [`DbHandle`] and reachable only server-side.
    #[must_use]
    pub const fn as_identity(&self, user: U48, tenant: U48) -> Self {
        Self {
            route: self.route,
            tenant,
            user,
        }
    }

    /// Run `op` on `T`'s owner.
    async fn run(&self, struct_hash: u64, op: NodeOp) -> Result<NodeReply> {
        Ok(self.route.run(self.tenant, struct_hash, op).await?)
    }
}

impl<R: Route> DbHandle for ServerDb<'_, R> {
    type Error = Error;

    fn tenant(&self) -> U48 {
        self.tenant
    }

    fn as_tenant(&self, tenant: U48) -> Self {
        Self {
            route: self.route,
            tenant,
            user: self.user,
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
