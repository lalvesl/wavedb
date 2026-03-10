//! [`CollectionHandle`] — the typed collection surface application code
//! drives, generic over its execution context.
//!
//! `#[wavedb(NonUnique)]` emits `T::collection(pivot_id)` returning one of
//! these; every method re-takes the [`DbHandle`] so the same handle value
//! works against a `LocalHandle`, the client `Db`, or a `ServerDb`:
//!
//! ```text
//! let todos = Todo::create_pivot(&db).await?;      // once, explicit
//! let col   = Todo::collection(todos);             // cheap, Copy
//! let id    = col.insert(&db, &Todo { .. }).await?;
//! col.all(&db)                                     // Stream<Result<Todo>>
//! ```
//!
//! The engine-facing [`Collection`](crate::Collection) (which takes a
//! [`Store`](crate::Store) directly) stays the internal layer this surface
//! resolves to on local contexts.

use std::marker::PhantomData;

use futures::Stream;

use crate::fuzzy::{Fuzzy, Scored};
use crate::handle::DbHandle;
use crate::id::Id;
use crate::index::Bound;
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::traits::NonUniqueStruct;
use crate::wire::Wire;

/// The typed handle into one NonUnique collection, addressed by its `Pivot`
/// record's [`LocalId`]. Holds no context — pass the [`DbHandle`] per call.
#[derive(Debug)]
pub struct CollectionHandle<T: NonUniqueStruct> {
    pivot: LocalId,
    _record: PhantomData<fn() -> T>,
}

// Manual impls: a derive would demand `T: Clone`/`T: Copy`, but the handle
// holds only the pivot id.
impl<T: NonUniqueStruct> Clone for CollectionHandle<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: NonUniqueStruct> Copy for CollectionHandle<T> {}

impl<T: NonUniqueStruct> CollectionHandle<T> {
    /// The handle for the collection whose `Pivot` record sits at `pivot`.
    #[must_use]
    pub const fn at(pivot: LocalId) -> Self {
        Self {
            pivot,
            _record: PhantomData,
        }
    }

    /// The `LocalId` of this collection's `Pivot` record.
    #[must_use]
    pub const fn pivot(&self) -> LocalId {
        self.pivot
    }

    /// Insert `value`, returning its stable identity [`Id`].
    ///
    /// # Errors
    /// A backend/transport failure, or a stale/foreign pivot.
    pub async fn insert<D: DbHandle>(
        &self,
        db: &D,
        value: &T,
    ) -> Result<Id, D::Error> {
        db.insert(self.pivot, value).await
    }

    /// [`insert`](Self::insert), resolving once the record is durable.
    ///
    /// # Errors
    /// As [`insert`](Self::insert), or a failed settle.
    pub async fn insert_durable<D: DbHandle>(
        &self,
        db: &D,
        value: &T,
    ) -> Result<Id, D::Error> {
        db.insert_durable(self.pivot, value).await
    }

    /// Fetch the record at `id`, as a [`Wire`] over its body: `.view()` reads
    /// it, `.try_into()` owns it (RFC 0076). `None` = no such record; a
    /// removed record still resolves (history stays navigable).
    ///
    /// # Errors
    /// A backend/transport failure or a head fault; the body is checked
    /// when viewed.
    pub async fn get<D: DbHandle>(
        &self,
        db: &D,
        id: Id,
    ) -> Result<Option<Wire<T>>, D::Error> {
        db.get_record(self.pivot, id).await
    }

    /// Update the record at `id` to `value` — same identity, new bytes; the
    /// superseded version is archived, changed secondary indexes re-key.
    ///
    /// # Errors
    /// A backend/transport failure, or a missing record.
    pub async fn save<D: DbHandle>(
        &self,
        db: &D,
        id: Id,
        value: &T,
    ) -> Result<(), D::Error> {
        db.update(self.pivot, id, value).await
    }

    /// [`save`](Self::save), resolving once the new version is durable.
    ///
    /// # Errors
    /// As [`save`](Self::save), or a failed settle.
    pub async fn save_durable<D: DbHandle>(
        &self,
        db: &D,
        id: Id,
        value: &T,
    ) -> Result<(), D::Error> {
        db.update_durable(self.pivot, id, value).await
    }

    /// Move the record at `id` to the dead tree; returns whether it was
    /// living. Bytes are kept.
    ///
    /// # Errors
    /// A backend/transport failure.
    pub async fn remove<D: DbHandle>(
        &self,
        db: &D,
        id: Id,
    ) -> Result<bool, D::Error> {
        db.remove::<T>(self.pivot, id).await
    }

    /// [`remove`](Self::remove), resolving once the removal is durable.
    ///
    /// # Errors
    /// As [`remove`](Self::remove), or a failed settle.
    pub async fn remove_durable<D: DbHandle>(
        &self,
        db: &D,
        id: Id,
    ) -> Result<bool, D::Error> {
        db.remove_durable::<T>(self.pivot, id).await
    }

    // The stream-returning reads capture `db`'s lifetime but deliberately
    // NOT the receiver's (`use<..>`): the handle is `Copy` and only its
    // `pivot` value feeds the stream, so `T::collection(p).all(db)` on a
    // temporary handle must work.

    /// Stream every living record, most recently written first.
    pub fn all<'d, D: DbHandle>(
        &self,
        db: &'d D,
    ) -> impl Stream<Item = Result<Wire<T>, D::Error>> + use<'d, D, T>
    where
        T: 'static,
    {
        db.all(self.pivot)
    }

    /// Stream the living records secondary index `index` selects under
    /// `bound`, ordered by the indexed field. The generated `by_<field>`
    /// wrappers call this with the field's exact encoding.
    pub fn search_by<'d, D: DbHandle>(
        &self,
        db: &'d D,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<Wire<T>, D::Error>> + use<'d, D, T>
    where
        T: 'static,
    {
        db.search_by(self.pivot, index, bound)
    }

    /// Stream the living records declared list `index` holds under `bound`,
    /// in its order and inline (RFC 0075). The generated `by_<field>` and
    /// `by_<field>_range` of a list call this.
    pub fn listed_where<'d, D: DbHandle>(
        &self,
        db: &'d D,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<Wire<T>, D::Error>> + use<'d, D, T>
    where
        T: 'static,
    {
        db.listed_where(self.pivot, index, bound)
    }

    /// Stream every living record in declared list `index`'s order — one read
    /// per segment with the records **inline** — where `all` walks pointers and
    /// resolves each record at its anchor. The generated `listed_by_<fields>` wrappers
    /// call this with the declaration's index.
    pub fn listed<'d, D: DbHandle>(
        &self,
        db: &'d D,
        index: usize,
    ) -> impl Stream<Item = Result<Wire<T>, D::Error>> + use<'d, D, T>
    where
        T: 'static,
    {
        db.listed(self.pivot, index, 0)
    }

    /// [`listed`](Self::listed) entered at global `offset` — one sparse-index
    /// descent rather than a walk. The stream runs on to the end of the list.
    pub fn listed_at<'d, D: DbHandle>(
        &self,
        db: &'d D,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<Wire<T>, D::Error>> + use<'d, D, T>
    where
        T: 'static,
    {
        db.listed(self.pivot, index, offset)
    }

    /// One **bounded** page of declared list `index` — at most `limit`
    /// records from `offset`, in one exchange.
    ///
    /// The pager's read: `listed_at(..).take(n)` would ask the node for far
    /// more than the window renders, because the unbounded reader pages at its
    /// own chunk size. This asks for exactly the window.
    pub fn listed_page<'d, D: DbHandle>(
        &self,
        db: &'d D,
        index: usize,
        offset: u64,
        limit: u32,
    ) -> impl Stream<Item = Result<Wire<T>, D::Error>> + use<'d, D, T>
    where
        T: 'static,
    {
        db.listed_page(self.pivot, index, offset, limit)
    }

    /// How many living records declared list `index` holds — the pager's
    /// "of M".
    ///
    /// # Errors
    /// A backend/transport failure, or an undeclared list index.
    pub async fn list_len<D: DbHandle>(
        &self,
        db: &D,
        index: usize,
    ) -> Result<u64, D::Error>
    where
        T: 'static,
    {
        db.list_len::<T>(self.pivot, index).await
    }

    /// Records whose fuzzy index `index` matches `query`, best first, at most
    /// `limit` of them ([RFC 0056]).
    ///
    /// **Buffered and ranked**, unlike every other read here: a best-first
    /// order is not known until the last candidate is scored. The generated
    /// `fuzzy_<field>` wrapper calls this with the declaration's index.
    ///
    /// [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md
    ///
    /// # Errors
    /// A backend/transport failure, or an undeclared fuzzy index.
    pub async fn fuzzy<D: DbHandle>(
        &self,
        db: &D,
        index: usize,
        query: &str,
        mode: Fuzzy,
        limit: usize,
    ) -> Result<Vec<Scored<(Id, Wire<T>)>>, D::Error>
    where
        T: 'static,
    {
        db.fuzzy_search(self.pivot, index, query, mode, limit).await
    }

    /// Stream the record at `id`'s versions **newest-first** along the
    /// modification chain. Saving never destroys old bytes — this walks them.
    pub fn history<'d, D: DbHandle>(
        &self,
        db: &'d D,
        id: Id,
    ) -> impl Stream<Item = Result<(Metadata, Wire<T>), D::Error>> + use<'d, D, T>
    where
        T: 'static,
    {
        db.record_history(self.pivot, id)
    }
}
