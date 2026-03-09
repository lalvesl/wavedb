//! [`DbHandle`] for the client [`Db`]: every typed op becomes a command
//! frame to the node.
//!
//! This is what makes the generated `T::get(&db)` / `col.insert(&db, v)`
//! spelling work against the network — the same call sites that resolve
//! against a `LocalHandle` or a `ServerDb`.
//!
//! When the handle carries a local cache (`Db::open`), ops thread through
//! [`crate::client_cache`]: node first, acknowledged results mirrored
//! best-effort, reads served locally only on a transport fault the cache
//! can actually answer.
//!
//! Walk-shaped ops are **streamed**: the node writes one frame per record
//! and the client decodes each as it arrives — a caller can stop early
//! without the node's whole answer in memory client-side. Three ops have
//! **no wire command yet** and refuse with the node's uniform answer,
//! [`UnknownStructHash`](wavedb_core::Error::UnknownStructHash):
//! `create_pivot` (collections bootstrap inside `#[server]` bodies),
//! `search_by`, and `record_history`.
//!
//! The declared-list reads (`listed` / `listed_page` / `list_len`) used to be
//! on that list and no longer are: a list page is bounded by the caller's
//! `limit`, so it needed none of the streaming work `search_by` waits on. See
//! [`crate::client_lists`].

use futures::{Stream, StreamExt, TryStreamExt};
use wavedb_core::expose::Command;
use wavedb_core::wire::{WaveWire, Wire, from_wire, to_wire};
use wavedb_core::{
    Bound, DbHandle, Id, LocalId, Metadata, NonUniqueStruct, U48, UniqueStruct,
};

use crate::client_cache as cache;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::reply;

/// Run a walk-shaped command and decode each streamed item frame as a `V`.
// `Db` is re-exported; `pub(crate)` keeps this seam crate-internal.
#[allow(clippy::redundant_pub_crate)]
pub(crate) fn streamed<'a, V: WaveWire + 'a>(
    db: &'a Db,
    struct_hash: u64,
    command: Command,
    payload: Vec<u8>,
) -> impl Stream<Item = Result<V>> + 'a {
    futures::stream::once(db.command_stream(struct_hash, command, payload))
        .map(|res| match res {
            Ok(items) => items.left_stream(),
            Err(e) => {
                futures::stream::once(std::future::ready(Err(e))).right_stream()
            }
        })
        .flatten()
        .and_then(|bytes| async move {
            from_wire::<V>(&bytes)
                .map_err(|e| Error::Core(wavedb_core::Error::from(e)))
        })
}

/// `create_pivot`'s answer: a collection is bootstrapped node-side, so the
/// wire has no command for it and the node's uniform refusal stands in.
fn refuse_pivot<T: NonUniqueStruct>() -> std::future::Ready<Result<LocalId>> {
    std::future::ready(Err(Error::Core(wavedb_core::Error::UnknownStructHash(
        <T::Pivot as wavedb_core::Pivot>::STRUCT_HASH,
    ))))
}

/// The uniform refusal for an op the wire cannot carry yet — the same answer
/// the node gives an unlisted hash.
fn refuse<T>(struct_hash: u64) -> impl Stream<Item = Result<T>> {
    futures::stream::once(std::future::ready(Err(Error::Core(
        wavedb_core::Error::UnknownStructHash(struct_hash),
    ))))
}

impl DbHandle for Db {
    type Error = Error;

    fn tenant(&self) -> U48 {
        // The inherent accessor (method resolution prefers it at call sites;
        // the trait impl delegates so both agree).
        Self::tenant(self)
    }

    fn as_tenant(&self, tenant: U48) -> Self {
        Self::as_tenant(self, tenant)
    }

    async fn get_unique<T: UniqueStruct>(&self) -> Result<Option<Wire<T>>> {
        match self.command(T::STRUCT_HASH, Command::Get, Vec::new()).await {
            Ok(r) => {
                let value: Option<Wire<T>> = reply::wire(r)?;
                if let Some(w) = &value {
                    cache::mirror_unique_wire(self, w).await?;
                }
                Ok(value)
            }
            Err(err) if cache::is_transport(&err) => {
                cache::unique_fallback(self, err).await
            }
            Err(err) => Err(err),
        }
    }

    async fn save_unique<T: UniqueStruct>(&self, value: &T) -> Result<()> {
        self.save_unique_with(value, false).await
    }

    async fn save_unique_durable<T: UniqueStruct>(
        &self,
        value: &T,
    ) -> Result<()> {
        self.save_unique_with(value, true).await
    }

    fn unique_history<T: UniqueStruct + 'static>(
        &self,
    ) -> impl Stream<Item = Result<(Metadata, T)>> {
        // Each item frame carries one `(Metadata, T)` version pair.
        streamed(self, T::STRUCT_HASH, Command::History, Vec::new())
    }

    fn create_pivot<T: NonUniqueStruct>(
        &self,
    ) -> impl Future<Output = Result<LocalId>> {
        // Not wire-reachable: a collection is bootstrapped node-side (inside
        // a `#[server]` body via `ServerDb`), never by a raw client command.
        refuse_pivot::<T>()
    }

    fn create_pivot_durable<T: NonUniqueStruct>(
        &self,
    ) -> impl Future<Output = Result<LocalId>> {
        refuse_pivot::<T>()
    }

    async fn insert<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
    ) -> Result<Id> {
        self.insert_with(pivot, value, false).await
    }

    async fn insert_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
    ) -> Result<Id> {
        self.insert_with(pivot, value, true).await
    }

    async fn get_record<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<Option<Wire<T>>> {
        // No back-fill on the read: a `Get` also resolves *removed* records
        // (history stays navigable), and adopting one would resurrect it
        // into the cached living set. Walks and writes warm the cache.
        match self
            .command(T::STRUCT_HASH, Command::Get, to_wire(&(pivot, id)))
            .await
        {
            Ok(r) => reply::wire(r),
            Err(err) if cache::is_transport(&err) => {
                cache::record_fallback(self, pivot, id, err).await
            }
            Err(err) => Err(err),
        }
    }

    async fn update<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
    ) -> Result<()> {
        self.update_with(pivot, id, value, false).await
    }

    async fn update_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
    ) -> Result<()> {
        self.update_with(pivot, id, value, true).await
    }

    async fn remove<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<bool> {
        self.remove_with::<T>(pivot, id, false).await
    }

    async fn remove_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<bool> {
        self.remove_with::<T>(pivot, id, true).await
    }

    fn all<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
    ) -> impl Stream<Item = Result<T>> {
        // Each `All` frame carries the wire pair `(Id, T)` — the node-minted
        // identity rides along so the walk mirrors into the local cache; the
        // typed surface yields values only. Transport fault + adopted local
        // pivot = the warm walk.
        cache::cached_all::<T>(self, pivot)
    }

    fn search_by<T: NonUniqueStruct + 'static>(
        &self,
        _pivot: LocalId,
        _index: usize,
        _bound: Bound,
    ) -> impl Stream<Item = Result<T>> {
        // No wire command yet — secondary lookups over the transport land
        // with the streaming frames.
        refuse(T::STRUCT_HASH)
    }

    fn listed_where<T: NonUniqueStruct + 'static>(
        &self,
        _pivot: LocalId,
        _index: usize,
        _bound: Bound,
    ) -> impl Stream<Item = Result<T>> {
        // A bounded scan has no limit to page by, so it waits on the
        // streaming frames with `search_by`.
        refuse(T::STRUCT_HASH)
    }

    fn listed<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<T>> {
        // Unlike `search_by`, this one does not wait on streaming frames: the
        // wire `Listed` is bounded by a limit, so the unbounded reader is a
        // chunk loop over bounded exchanges (see `client_lists`).
        crate::client_lists::cached_listed::<T>(self, pivot, index, offset)
    }

    fn listed_page<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
        limit: u32,
    ) -> impl Stream<Item = Result<T>> {
        crate::client_lists::cached_listed_page::<T>(
            self, pivot, index, offset, limit,
        )
    }

    async fn list_len<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
    ) -> Result<u64> {
        crate::client_lists::cached_list_len::<T>(self, pivot, index).await
    }

    fn fuzzy_search<T: NonUniqueStruct + 'static>(
        &self,
        _pivot: LocalId,
        _index: usize,
        _query: &str,
        _mode: wavedb_core::fuzzy::Fuzzy,
        _limit: usize,
    ) -> impl Future<
        Output = Result<Vec<wavedb_core::fuzzy::Scored<(wavedb_core::Id, T)>>>,
    > {
        // No wire command yet (RFC 0056 leaves it open). Unlike `listed`,
        // this one has a natural home without one: the todo-app shape —
        // every struct storage-only, the whole API `#[server]` functions —
        // reaches it node-side, where a search can also decide what a hit
        // should even look like to the app.
        std::future::ready(Err(Error::Core(
            wavedb_core::Error::UnknownStructHash(T::STRUCT_HASH),
        )))
    }

    fn record_history<T: NonUniqueStruct + 'static>(
        &self,
        _pivot: LocalId,
        _id: Id,
    ) -> impl Stream<Item = Result<(Metadata, T)>> {
        // No wire command yet — the NonUnique timeline walk is node-side
        // only until a `History`-by-id frame exists.
        refuse(T::STRUCT_HASH)
    }
}
