//! [`DbHandle`] — the one execution-context seam typed generated code runs
//! over.
//!
//! Its `Store`-backed implementation is [`LocalHandle`](crate::LocalHandle),
//! next door in [`local_handle`](crate::local_handle).
//!
//! The `#[wavedb]` macro emits `T::get(&db)` / `value.save(&db)` /
//! `T::collection(&db, pivot)` methods generic over this trait, so the same
//! call sites resolve against every context: a [`LocalHandle`](crate::LocalHandle)
//! driving a [`Store`](crate::Store) directly (engine tests, in-process embedding), the client `Db`
//! sending command frames, and the node-side `ServerDb` a `#[server]` body
//! runs against.
//!
//! Every write has a `_durable` twin ([RFC 0072] §12): the same operation,
//! resolving only once it is durable — on the node that answers it, or in
//! the store a local context drives. The plain write resolves once it is
//! committed, durable at that store's next settle (a 200 ms tick on a node);
//! a durable one also makes durable everything committed before it on the
//! same store — on a node, the same shard, which is why a body that writes
//! two owners makes the write it relies on durable on each of them.
//!
//! [RFC 0072]: https://github.com/wavedb/wavedb/blob/main/rfcs/0072-the-settle-is-the-durability-point.md
//!
//! Two deliberate signature choices:
//!
//! - **`type Error: From<Error>`** — the client's error is richer than
//!   core's (node refusals, transport faults), so each context brings its
//!   own; core errors convert in.
//! - **Walk-shaped ops return `impl Stream`** even where an implementation
//!   buffers today (the M4 client collects a `Reply::Values` and wraps it in
//!   an iterator stream) — when streaming frames land, only implementations
//!   change, never the generated call sites.

use std::future::Future;

use futures::Stream;

use crate::error::Error;
use crate::fuzzy::{Fuzzy, Scored};
use crate::id::Id;
use crate::index::Bound;
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::traits::{NonUniqueStruct, UniqueStruct};
use crate::u48::U48;
use crate::wire::Wire;

/// An execution context bound to one tenant: somewhere typed operations can
/// run — locally against a [`Store`](crate::Store), or remotely over a
/// transport.
///
/// The tenant is bound **once, in the handle** — the partition key is
/// structural, never restated per call. Collection ops address their
/// collection by the `Pivot` record's [`LocalId`] (the generated typed
/// wrappers pass it from a `{Name}PivotId`).
pub trait DbHandle: Sized {
    /// This context's error. Core faults convert in, and so do wire faults —
    /// a `wire.view()?` over what a read returned (RFC 0076), through
    /// [`Error::Wire`]; a context may add its own layers (node refusal,
    /// transport) on top.
    type Error: From<Error> + From<crate::wire::Error>;

    /// The tenant this handle is bound to.
    fn tenant(&self) -> U48;

    /// The same context scoped to a different tenant — the server-side
    /// cross-tenant seam (a `register`-style function bootstrapping a new
    /// tenant's records). Not a privilege escalation by itself: enforcement
    /// is the node's job (M8), not the handle's.
    #[must_use]
    fn as_tenant(&self, tenant: U48) -> Self;

    /// Fetch this tenant's `Unique` record from its anchor, as a [`Wire`]
    /// over its body: `.view()` reads it, `.try_into()` owns it (RFC 0076).
    /// `None` = never saved.
    ///
    /// # Errors
    /// A backend/transport failure or a head fault; the body is checked
    /// when viewed.
    async fn get_unique<T: UniqueStruct>(
        &self,
    ) -> Result<Option<Wire<T>>, Self::Error>;

    /// Save (insert-or-overwrite) this tenant's `Unique` record at its
    /// anchor. Save **is** the upsert; the superseded version is archived on
    /// the modification chain.
    ///
    /// # Errors
    /// A backend/transport failure.
    async fn save_unique<T: UniqueStruct>(
        &self,
        value: &T,
    ) -> Result<(), Self::Error>;

    /// [`save_unique`](Self::save_unique), resolving once the save is
    /// durable.
    ///
    /// # Errors
    /// As [`save_unique`](Self::save_unique), or a failed settle.
    async fn save_unique_durable<T: UniqueStruct>(
        &self,
        value: &T,
    ) -> Result<(), Self::Error>;

    /// Stream this tenant's `Unique` record versions **newest-first** (the
    /// live record, then each archive along the modification chain). Empty
    /// when never saved.
    ///
    /// The `'static` bound on the walk-shaped ops is free: `WaveWire` values
    /// are always owned (decode never borrows), so every `#[wavedb]` type is
    /// `'static` — and it unties the yielded items from the handle borrow.
    fn unique_history<T: UniqueStruct + 'static>(
        &self,
    ) -> impl Stream<Item = Result<(Metadata, T), Self::Error>>;

    /// Create a new, empty collection of `T` under this tenant — explicit,
    /// never automatic. The caller stores the returned root (via the typed
    /// `{Name}PivotId`) in an owning record.
    ///
    /// # Errors
    /// A backend/transport failure.
    async fn create_pivot<T: NonUniqueStruct>(
        &self,
    ) -> Result<LocalId, Self::Error>;

    /// [`create_pivot`](Self::create_pivot), resolving once the new
    /// collection is durable — before a record naming it is written durably
    /// elsewhere (on a node, its holder usually lives on another shard).
    ///
    /// # Errors
    /// As [`create_pivot`](Self::create_pivot), or a failed settle.
    async fn create_pivot_durable<T: NonUniqueStruct>(
        &self,
    ) -> Result<LocalId, Self::Error>;

    /// Insert `value` into the collection at `pivot`, returning its stable
    /// identity [`Id`] (the anchor references point at — it never changes).
    ///
    /// # Errors
    /// A backend/transport failure, or a stale/foreign `pivot`.
    async fn insert<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
    ) -> Result<Id, Self::Error>;

    /// [`insert`](Self::insert), resolving once the record is durable.
    ///
    /// # Errors
    /// As [`insert`](Self::insert), or a failed settle.
    async fn insert_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
    ) -> Result<Id, Self::Error>;

    /// Fetch the record at `id`, as a [`Wire`] over its body (RFC 0076).
    /// `None` = no such record. A removed record still resolves (history
    /// stays navigable); `pivot` scopes the typed wrapper, remote contexts
    /// may not need it.
    ///
    /// # Errors
    /// A backend/transport failure or a head fault (including an `id`
    /// resolving to a different type's record); the body is checked when
    /// viewed.
    async fn get_record<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<Option<Wire<T>>, Self::Error>;

    /// Update the record at `id` to `value` — same identity, new bytes; the
    /// superseded version is archived, changed secondary indexes re-key.
    ///
    /// # Errors
    /// A backend/transport failure, or a missing record.
    async fn update<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
    ) -> Result<(), Self::Error>;

    /// [`update`](Self::update), resolving once the new version is durable.
    ///
    /// # Errors
    /// As [`update`](Self::update), or a failed settle.
    async fn update_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
    ) -> Result<(), Self::Error>;

    /// Move the record at `id` from the living set to the dead tree; returns
    /// whether it was living. Bytes are kept — nothing is erased.
    ///
    /// # Errors
    /// A backend/transport failure.
    async fn remove<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<bool, Self::Error>;

    /// [`remove`](Self::remove), resolving once the removal is durable.
    ///
    /// # Errors
    /// As [`remove`](Self::remove), or a failed settle.
    async fn remove_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<bool, Self::Error>;

    /// Stream every living record of the collection at `pivot`, in insertion
    /// (`CREATED_AT`) order.
    fn all<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
    ) -> impl Stream<Item = Result<T, Self::Error>>;

    /// Stream the living records secondary index `index` selects under
    /// `bound`, ordered by the indexed field. The generated `by_<field>`
    /// wrappers call this with the field's exact
    /// [`IndexKey`](crate::index::IndexKey) encoding.
    fn search_by<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<T, Self::Error>>;

    /// Stream the living records declared list `index` holds under `bound`,
    /// in the list's order, records inline (RFC 0075). A list's generated
    /// `by_<field>` lookups call this.
    fn listed_where<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<T, Self::Error>>;

    /// Stream every living record in declared list `index`'s order, entered at
    /// global `offset` ([RFC 0051]). `offset = 0` is the whole list.
    ///
    /// The generated `listed_by_<fields>` wrappers call this with the
    /// declaration's compile-time index.
    ///
    /// [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
    fn listed<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<T, Self::Error>>;

    /// One **bounded** page of declared list `index`: at most `limit` records
    /// from global `offset`.
    ///
    /// This is what a pager rendering "rows 50…75 of M" wants, and over the
    /// wire it is one exchange — where [`listed`](Self::listed), being
    /// unbounded, has to page internally and would fetch far more than the
    /// window asked for.
    fn listed_page<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
        limit: u32,
    ) -> impl Stream<Item = Result<T, Self::Error>>;

    /// How many living records declared list `index` holds — the pager's
    /// "of M", one read cold.
    fn list_len<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
    ) -> impl Future<Output = Result<u64, Self::Error>>;

    /// Records whose fuzzy index `index` matches `query` under `mode`, best
    /// first, at most `limit` ([RFC 0056]).
    ///
    /// The one read that is **buffered and ranked** rather than streamed: a
    /// best-first order cannot be known until the last candidate is scored.
    ///
    /// [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md
    fn fuzzy_search<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        query: &str,
        mode: Fuzzy,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Scored<(Id, T)>>, Self::Error>>;

    /// Stream the record at `id`'s versions **newest-first** along the
    /// modification chain (the live version, then each archive).
    fn record_history<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> impl Stream<Item = Result<(Metadata, T), Self::Error>>;
}
