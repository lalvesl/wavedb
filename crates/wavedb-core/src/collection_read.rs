//! [`Collection`]'s reading half — `get`, `history`, `search`, `search_by` and
//! `all`.
//!
//! Split from [`collection`](crate::collection) for the file budget, along the
//! layer seam the write side already uses (`collection_write` /
//! `collection_remove`).
//!
//! Every read here resolves records **by address**: the index yields anchors and
//! each one is fetched with `get_of`. That is one page read and one dictionary
//! decompression per record, which is exactly the cost RFC 0050's chain exists
//! to collapse — so this module is where phase 6 lands, descending the chain's
//! sparse index and streaming whole segments instead.
//!
//! [`get`](Collection::get) already needs no index at all: a record's anchor is
//! a computed address, and it resolves for removed records too, which is what
//! keeps history navigable.

use futures::{Stream, TryStreamExt};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::id::Id;
use crate::index::{Bound, Pivot, SecKey};
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::record::{decode_record, history_stream};
use crate::store::Store;
use crate::traits::NonUniqueStruct;

/// What an anchor holds — the answer a membership tree used to give, read
/// off the record itself (RFC 0050).
// `pub` inside a private module, as `MovedRoots` is: `pub(crate)` here is what
// clippy's `redundant_pub_crate` objects to, and the module gates the reach.
pub enum Anchor<T> {
    /// Nothing was ever written here.
    Vacant,
    /// A living record, with the metadata that says so.
    Living(Metadata, T),
    /// A record whose [`Metadata`] records that it left the living set. Its
    /// bytes are still here — removal never destroys them — so history stays
    /// navigable and a keyed anchor can be revived onto them.
    Dead,
}

impl<T: NonUniqueStruct> Collection<T> {
    /// Read the anchor at `id` and classify it.
    ///
    /// **One** store read answers what a `current` descent plus a follow-up
    /// fetch used to: liveness is a property of the record, so the read that
    /// would merely *confirm* the tree's verdict is the read that decides it.
    /// The callers that need the bytes anyway — a mirror comparing them —
    /// get them from the same read.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure or a decode fault.
    pub(crate) async fn anchor<S: Store>(
        &self,
        store: &S,
        id: Id,
    ) -> Result<Anchor<T>> {
        let Some(bytes) = store.get_of(T::STRUCT_HASH, id).await? else {
            return Ok(Anchor::Vacant);
        };
        let (meta, value) = decode_record::<T>(T::STRUCT_HASH, &bytes)?;
        Ok(if meta.is_live() {
            Anchor::Living(meta, value)
        } else {
            Anchor::Dead
        })
    }
}

impl<T: NonUniqueStruct> Collection<T> {
    /// Fetch and decode the record at `id`. `None` = no such record. Resolves
    /// by direct address — a removed (dead-indexed) record still resolves,
    /// which is what keeps history navigable.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure or a decode fault (including an `id`
    /// that resolves to a different type's record).
    pub async fn get<S: Store>(&self, store: &S, id: Id) -> Result<Option<T>> {
        match store.get_of(T::STRUCT_HASH, id).await? {
            Some(bytes) => Ok(Some(decode_record(T::STRUCT_HASH, &bytes)?.1)),
            None => Ok(None),
        }
    }

    /// Stream the record at `id`'s versions **newest-first**: the live
    /// version, then each archived one along the modification chain that
    /// [`save`](Self::save) maintains. Saving never destroys old bytes — this
    /// is the walk over them.
    // The read methods take `self` by value (the handle is `Copy`): they
    // return streams, and a borrowed receiver would tie the opaque type to a
    // temporary when called as `T::collection(..).all(store)`.
    pub fn history<'a, S: Store>(
        self,
        store: &'a S,
        id: Id,
    ) -> impl Stream<Item = Result<(Metadata, T)>> + 'a
    where
        T: 'a,
    {
        history_stream::<T, S>(
            store,
            T::STRUCT_HASH,
            T::SHAPE,
            id,
            self.tenant(),
        )
    }

    /// Stream the living records secondary index `index` selects under
    /// `bound` (a bound over the index's [`IndexKey`](crate::index::IndexKey)
    /// encoding — `Exact` for one value, `Prefix`/`Range` for scans),
    /// resolved two-phase: the chain yields anchors, each one is fetched.
    /// Ordered by the indexed field. The generated `by_<field>` wrappers call
    /// this with the field's exact encoding.
    ///
    /// One sparse-index descent to the bound's least key, then the chain in
    /// order while the bound holds ([RFC 0075]): a range read touches the
    /// segments it returns from, and one more at most.
    ///
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains-WIP.md
    pub fn search_by<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<(Id, T)>> + 'a
    where
        T: 'a,
    {
        let handle = self;
        let tenant = self.tenant();
        futures::stream::once(async move {
            let pivot = handle.load_pivot(store).await?;
            let roots = *pivot
                .secondaries()
                .get(index)
                .ok_or(Error::SecondaryIndexOutOfRange(index))?;
            let from = SecKey {
                field: bound.least().to_vec(),
                rec: LocalId::default(),
            };
            let keys = handle.sec_chain(roots).scan_from(store, from);
            Ok::<_, Error>(keys.try_take_while(move |(key, ())| {
                futures::future::ready(Ok(bound.contains(&key.field)))
            }))
        })
        .try_flatten()
        .map_ok(move |(key, ())| key.rec.to_id(tenant))
        .and_then(move |id| async move {
            let bytes = store
                .get_of(T::STRUCT_HASH, id)
                .await?
                .ok_or(Error::RecordMissing(id))?;
            Ok((id, decode_record::<T>(T::STRUCT_HASH, &bytes)?.1))
        })
    }

    /// Stream every living record, **most recently changed first**.
    ///
    /// This walks the record chain (RFC 0050) back from its tail. The chain
    /// holds **pointers** (RFC 0054 — a record is stored once, at its anchor),
    /// so one read per segment gives membership and order and each record is
    /// then resolved at its address. A collection that wants a listing without
    /// that per-record read declares a `#[wavedb::list]`, which is the
    /// structure that holds records inline.
    ///
    /// ## The order changed
    ///
    /// This used to stream in **insertion** order (`CREATED_AT` ascending). The
    /// chain is ordered by each record's live version's *authoring* instant, so
    /// the order is now "most recently written first" — which is what a listing
    /// usually wants, and what makes this same structure serve a reconnect
    /// catch-up.
    ///
    /// Insertion order is no longer available at all: it lived in the
    /// `CREATED_AT` B+tree, which phase 5c retired along with `Collection::
    /// search`. It comes back as a **declared** ordering under RFC 0051 when a
    /// real caller needs it, rather than as a tree every collection pays for.
    pub fn all<'a, S: Store>(
        self,
        store: &'a S,
    ) -> impl Stream<Item = Result<(Id, T)>> + 'a
    where
        T: 'a,
    {
        let handle = self;
        futures::stream::once(async move {
            let pivot = handle.load_pivot(store).await?;
            let chain = handle.recency_chain(&pivot);
            let tenant = handle.tenant();
            Ok::<_, Error>(
                futures::stream::try_unfold(
                    Some(chain.tail()),
                    move |cursor| async move {
                        let Some(id) = cursor else {
                            return Ok::<_, Error>(None);
                        };
                        let seg = chain.segment(store, id).await?;
                        // The chain holds pointers, not records: one segment
                        // read gives the membership and the order, and each
                        // record is then resolved at its anchor. Reversed: a
                        // segment holds its keys ascending, the walk runs
                        // descending.
                        let mut page = Vec::with_capacity(seg.len());
                        for (key, ()) in seg.entries() {
                            let rec = key.rec.to_id(tenant);
                            let bytes = store
                                .get_of(T::STRUCT_HASH, rec)
                                .await?
                                .ok_or(Error::RecordMissing(rec))?;
                            let (_, value) =
                                decode_record::<T>(T::STRUCT_HASH, &bytes)?;
                            page.push(Ok((rec, value)));
                        }
                        page.reverse();
                        Ok(Some((futures::stream::iter(page), seg.prev())))
                    },
                )
                .try_flatten(),
            )
        })
        .try_flatten()
    }
}
