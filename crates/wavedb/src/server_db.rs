//! `ServerDb` — the node-side execution context a `#[server]` function body
//! runs against.
//!
//! It implements [`DbHandle`], so the same generated spelling a client uses
//! — `T::get(db)`, `T::collection(pivot)`, `col.insert(db, v)` — resolves
//! inside a server body. The `#[server]` macro retypes a body's `db: &Db`
//! parameter to `db: &ServerDb<R>`, so one body source drives both sides.
//!
//! A body **coordinates; it does not execute** (RFC 0067): every operation
//! goes through core's [`RouteHandle`] to its owner — on a node, usually on
//! another thread — and the error is re-wrapped into the client-facing
//! [`Error`](crate::Error) so a body's `?` and the typed helpers
//! (`Error::not_found`, …) compose. Each operation is atomic on its own; a
//! body that needs several to be one step [`lock`](ServerDb::lock)s their
//! owners first (RFC 0067 §5).

use futures::{Stream, TryStreamExt};
use wavedb_core::fuzzy::{Fuzzy, Scored};
use wavedb_core::placement::Owner;
use wavedb_core::{
    Bound, DbHandle, Id, Leases, LocalId, Locked, Metadata, NonUniqueStruct,
    Route, RouteHandle, U48, UniqueStruct,
};

use crate::error::{Error, Result};

/// A node-side handle: a borrowed [`Route`] plus the bound identity, and
/// the owners the body holds. Cheap to re-scope with
/// [`as_tenant`](DbHandle::as_tenant).
pub struct ServerDb<'a, R: Route> {
    local: RouteHandle<'a, R>,
    user: U48,
}

impl<'a, R: Route> ServerDb<'a, R> {
    /// Wrap a route + tenant as an execution context (`user = tenant` — the
    /// engine-local/B2C identity). `leases` is the body's table of held
    /// owners: one per invocation.
    pub const fn new(
        route: &'a R,
        tenant: U48,
        leases: &'a Leases<R::Lease>,
    ) -> Self {
        Self {
            local: RouteHandle::leasing(route, tenant, leases),
            user: tenant,
        }
    }

    /// The context a verified request executes as — what the generated
    /// `#[server]` dispatch builds from gate 1's [`Caller`].
    ///
    /// [`Caller`]: wavedb_core::Caller
    pub const fn for_caller(
        route: &'a R,
        caller: wavedb_core::Caller,
        leases: &'a Leases<R::Lease>,
    ) -> Self {
        Self {
            local: RouteHandle::leasing(route, caller.tenant, leases),
            user: caller.user,
        }
    }

    /// Hold `owners` across this body's awaits until the returned guard
    /// drops: operations on them run under their leases, so nothing else
    /// runs on them in between. Take **every** owner the body needs in one
    /// call — a second lock, or an operation outside the held owners, is
    /// refused (`wavedb_core::lease` has the rules) — except a collection
    /// the body creates while holding the lock.
    ///
    /// # Errors
    /// [`Error`] carrying `LockOrder` when a lock is already held; the
    /// node's fault taking a lease.
    pub async fn lock(&self, owners: &[Owner]) -> Result<Locked<'a, R::Lease>> {
        Ok(self.local.lock(owners).await?)
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
    pub fn as_identity(&self, user: U48, tenant: U48) -> Self {
        Self {
            local: self.local.as_tenant(tenant),
            user,
        }
    }
}

impl<R: Route> DbHandle for ServerDb<'_, R> {
    type Error = Error;

    fn tenant(&self) -> U48 {
        self.local.tenant()
    }

    fn as_tenant(&self, tenant: U48) -> Self {
        Self {
            user: self.user,
            local: self.local.as_tenant(tenant),
        }
    }

    async fn get_unique<T: UniqueStruct>(&self) -> Result<Option<T>> {
        Ok(self.local.get_unique().await?)
    }

    async fn save_unique<T: UniqueStruct>(&self, value: &T) -> Result<()> {
        Ok(self.local.save_unique(value).await?)
    }

    async fn save_unique_durable<T: UniqueStruct>(
        &self,
        value: &T,
    ) -> Result<()> {
        Ok(self.local.save_unique_durable(value).await?)
    }

    fn unique_history<T: UniqueStruct + 'static>(
        &self,
    ) -> impl Stream<Item = Result<(Metadata, T)>> {
        self.local.unique_history().map_err(Error::from)
    }

    async fn create_pivot<T: NonUniqueStruct>(&self) -> Result<LocalId> {
        Ok(self.local.create_pivot::<T>().await?)
    }

    async fn create_pivot_durable<T: NonUniqueStruct>(
        &self,
    ) -> Result<LocalId> {
        Ok(self.local.create_pivot_durable::<T>().await?)
    }

    async fn insert<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
    ) -> Result<Id> {
        Ok(self.local.insert(pivot, value).await?)
    }

    async fn insert_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
    ) -> Result<Id> {
        Ok(self.local.insert_durable(pivot, value).await?)
    }

    async fn get_record<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<Option<T>> {
        Ok(self.local.get_record(pivot, id).await?)
    }

    async fn update<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
    ) -> Result<()> {
        Ok(self.local.update(pivot, id, value).await?)
    }

    async fn update_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
    ) -> Result<()> {
        Ok(self.local.update_durable(pivot, id, value).await?)
    }

    async fn remove<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<bool> {
        Ok(self.local.remove::<T>(pivot, id).await?)
    }

    async fn remove_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<bool> {
        Ok(self.local.remove_durable::<T>(pivot, id).await?)
    }

    fn all<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
    ) -> impl Stream<Item = Result<T>> {
        self.local.all(pivot).map_err(Error::from)
    }

    fn listed<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<T>> {
        self.local.listed(pivot, index, offset).map_err(Error::from)
    }

    fn listed_page<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
        limit: u32,
    ) -> impl Stream<Item = Result<T>> {
        self.local
            .listed_page(pivot, index, offset, limit)
            .map_err(Error::from)
    }

    async fn list_len<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
    ) -> Result<u64> {
        self.local
            .list_len::<T>(pivot, index)
            .await
            .map_err(Error::from)
    }

    async fn fuzzy_search<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        query: &str,
        mode: Fuzzy,
        limit: usize,
    ) -> Result<Vec<Scored<(Id, T)>>> {
        self.local
            .fuzzy_search::<T>(pivot, index, query, mode, limit)
            .await
            .map_err(Error::from)
    }

    fn search_by<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<T>> {
        self.local
            .search_by(pivot, index, bound)
            .map_err(Error::from)
    }

    fn record_history<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> impl Stream<Item = Result<(Metadata, T)>> {
        self.local.record_history(pivot, id).map_err(Error::from)
    }
}
