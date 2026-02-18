//! [`Collection`]'s mutating half — `insert` / `save` / `remove` — each one
//! atomic [`Store::apply`] batch: the record write, every touched chain
//! segment and index node (recency, removals, lists, secondaries, fuzzy, via
//! the chains' `plan_*` planners), and the rewritten `Pivot` when a root moved.
//!
//! Every instant minted here (an insert's id, a save's version instant, a
//! removal's log key) lands strictly above the collection's **instant
//! floor** (see [`crate::collection_recency`]), so the recency and dead
//! logs only ever grow at their tails.

use crate::collection::Collection;
use crate::collection_reindex::Reindex;
use crate::error::Result;
use crate::id::Id;
use crate::index::{Chain, ChainRoots, LogRoots, Pivot, Roots};
use crate::local_id::LocalId;
use crate::metadata::{Metadata, Succession};
use crate::notify::{Mutation, MutationKind};
use crate::record::{
    Overlay, encode_record, mint_floored_id, plan_chained_save,
};
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;
use crate::wire::to_wire;

/// Everything one mutation may have moved, gathered so the `Pivot` is compared
/// and rewritten **exactly once** — a second write built from the pre-batch
/// pivot would drop whatever the first one carried.
pub struct MovedRoots<'a> {
    /// The secondary chains, in declaration order (RFC 0075).
    pub secondaries: &'a [Chain<()>],
    /// The record chain's endpoints and index root (RFC 0050).
    pub recency: ChainRoots,
    /// The removal log's endpoints.
    pub removals: LogRoots,
    /// The declared lists' chains, in declaration order (RFC 0051).
    pub lists: &'a [Chain<Vec<u8>>],
    /// The fuzzy posting chains, in declaration order (RFC 0056).
    pub fuzzy: &'a [Chain<()>],
}

impl<T: NonUniqueStruct> Collection<T> {
    /// When any root moved — a chain's endpoint, at its first split —
    /// append the `Pivot` rewrite carrying them all.
    ///
    /// A chain moves an endpoint at most once in its life (its first split) and
    /// its index root never moves, so a collection with no declared secondary
    /// index compares and writes nothing here after that one split. What used
    /// to make this fire constantly — `current`, `recency`, `dead` — is gone
    /// (RFC 0050 phase 5c).
    pub(crate) fn push_root_moves(
        &self,
        batch: &mut Vec<Write>,
        pivot: &T::Pivot,
        moved: &MovedRoots<'_>,
    ) {
        let sec_roots: Vec<ChainRoots> =
            moved.secondaries.iter().map(Chain::roots).collect();
        let list_roots: Vec<ChainRoots> =
            moved.lists.iter().map(Chain::roots).collect();
        let fuzzy_roots: Vec<ChainRoots> =
            moved.fuzzy.iter().map(Chain::roots).collect();
        if sec_roots.as_slice() != pivot.secondaries()
            || moved.recency != pivot.recency()
            || moved.removals != pivot.removals()
            || list_roots.as_slice() != pivot.lists()
            || fuzzy_roots.as_slice() != pivot.fuzzy()
        {
            let rewritten = pivot.replace_roots(Roots {
                secondaries: &sec_roots,
                recency: moved.recency,
                removals: moved.removals,
                lists: &list_roots,
                fuzzy: &fuzzy_roots,
            });
            batch.push(self.pivot_rewrite(&rewritten));
        }
    }

    /// Insert `value` as a new record: mints its timestamp-keyed [`Id`] (the
    /// stable identity for the record's whole life), writes the record,
    /// indexes it in `current`, the recency log, and every secondary tree,
    /// and rewrites the `Pivot` if any root moved — one atomic batch.
    /// Returns the minted `Id`.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure, [`Error::PivotMissing`](crate::Error::PivotMissing)
    /// on a stale handle, or a decode fault on a corrupt pivot.
    pub async fn insert<S: Store>(&self, store: &S, value: &T) -> Result<Id> {
        let pivot = self.load_pivot(store).await?;
        // A `#[wavedb::key(...)]` type's identity is its key fields —
        // insert IS the upsert at the content-derived anchor (see
        // `collection_keyed`).
        if let Some(id) = self.keyed_anchor(value) {
            self.upsert_keyed(store, &pivot, id, value).await?;
            return Ok(id);
        }
        let floor = self.instant_floor(store, &pivot).await?;
        let id = mint_floored_id(self.tenant(), T::STRUCT_HASH, floor);
        self.insert_at(store, &pivot, id, value).await?;
        Ok(id)
    }

    /// [`insert`](Self::insert)'s body with the identity supplied instead of
    /// minted — the seam [`adopt`](Self::adopt) writes a **node-minted** `Id`
    /// through when mirroring a record into a local cache store.
    pub(crate) async fn insert_at<S: Store>(
        &self,
        store: &S,
        pivot: &T::Pivot,
        id: Id,
        value: &T,
    ) -> Result<()> {
        // First version: no chain yet; the pivot back-link is stamped here
        // (the future handle-less `record.save(&db)` reaches roots through it)
        // and the writer identity is the tenant until node auth exists (M8).
        // The authoring instant is the id's own key — the key IS when that
        // version was authored.
        let meta = Metadata {
            succession: Succession::CreatedAt(id.key()),
            pivot_id: Some(self.pivot()),
            user: self.user(),
            ..Metadata::default()
        };
        self.insert_with_meta(store, pivot, id, meta, value).await
    }

    /// The indexed first-version write, with the [`Metadata`] supplied —
    /// [`adopt_with`](Self::adopt_with) passes the **node's** verbatim, so a
    /// mirror carries authoritative chain data.
    pub(crate) async fn insert_with_meta<S: Store>(
        &self,
        store: &S,
        pivot: &T::Pivot,
        id: Id,
        meta: Metadata,
        value: &T,
    ) -> Result<()> {
        // A first version is live by definition; its instant keys the
        // recency entry.
        let Succession::CreatedAt(instant) = meta.succession else {
            return Err(crate::Error::ChainCorrupt(id));
        };
        let mut records = self.recency_chain(pivot);
        let mut secs = self.sec_chains(pivot);
        let mut lists = self.list_chains(pivot);
        let mut fuzzy = self.fuzzy_chains(pivot);
        let envelope = encode_record(T::STRUCT_HASH, &meta, value);
        let key = Self::instant_key(instant, LocalId::from_id(id));
        // The chain carries the record **inline**, keyed by the instant its
        // live version was authored. That one entry is the membership set and
        // the modification log at once (RFC 0050).
        let mut batch = vec![Write::Put(id, envelope.clone())];
        batch.extend(records.plan_insert(store, key, ()).await?);
        // …and one more copy per declared list, sorted by its own property
        // (RFC 0051).
        self.plan_list_inserts(
            store, &mut batch, &mut lists, id, &envelope, value,
        )
        .await?;
        for (i, chain) in secs.iter_mut().enumerate() {
            let key = Self::sec_key(value, i, id);
            batch.extend(chain.plan_insert(store, key, ()).await?);
        }
        // …and `L + n - 1` postings per fuzzy declaration (RFC 0056).
        let mut view = Overlay::new(store);
        self.plan_fuzzy_inserts(&mut view, &mut batch, &mut fuzzy, id, value)
            .await?;
        self.push_root_moves(
            &mut batch,
            pivot,
            &MovedRoots {
                secondaries: &secs,
                recency: records.roots(),
                removals: pivot.removals(),
                lists: &lists,
                fuzzy: &fuzzy,
            },
        );
        store.apply(&batch).await?;
        store.note_mutation(|| Mutation {
            struct_hash: T::STRUCT_HASH,
            tenant: self.tenant(),
            pivot: Some(self.pivot()),
            id,
            kind: MutationKind::Saved,
            meta: Some(meta),
            body: to_wire(value),
        });
        Ok(())
    }

    /// Overwrite the record at `id` with `value` — the NonUnique *update*.
    /// The superseded version is **archived** (its bytes move to a fresh id)
    /// and the modification chain linked, so the timeline stays walkable via
    /// [`history`](Collection::history). The `Id` (and so the record's place
    /// in `current`) is unchanged — the primary tree never reindexes; each
    /// **secondary** tree re-keys when its field values changed (old key out,
    /// new key in), and the recency entry re-keys to the new version's
    /// instant. Everything commits as one atomic batch.
    ///
    /// A save to a never-inserted `id` writes an unindexed, chainless first
    /// version (Unique-style upsert semantics) — but a secondary-indexed
    /// type's fields would then be invisible to `by_` lookups, so its save
    /// requires an existing record.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure or a decode fault,
    /// [`Error::PivotMissing`] on a stale handle, or
    /// [`Error::RecordMissing`] when a secondary-indexed `id` was never
    /// inserted.
    ///
    /// [`Error::PivotMissing`]: crate::Error::PivotMissing
    /// [`Error::RecordMissing`]: crate::Error::RecordMissing
    pub async fn save<S: Store>(
        &self,
        store: &S,
        id: Id,
        value: &T,
    ) -> Result<()> {
        self.save_planned(store, id, value, None).await
    }

    /// [`save`](Self::save) with the node's [`Metadata`] written verbatim —
    /// [`adopt_with`](Self::adopt_with)'s update half.
    pub(crate) async fn save_with_meta<S: Store>(
        &self,
        store: &S,
        id: Id,
        meta: Metadata,
        value: &T,
    ) -> Result<()> {
        self.save_planned(store, id, value, Some(meta)).await
    }

    pub(crate) async fn save_planned<S: Store>(
        &self,
        store: &S,
        id: Id,
        value: &T,
        imposed: Option<Metadata>,
    ) -> Result<()> {
        // A keyed value may never be saved under a foreign anchor — the
        // key fields ARE the identity ("renaming" = remove + insert).
        if let Some(anchor) = self.keyed_anchor(value)
            && anchor != id
        {
            return Err(crate::Error::KeyMismatch(id));
        }
        let pivot = self.load_pivot(store).await?;
        let floor = self.instant_floor(store, &pivot).await?;
        let plan = crate::record::SavePlan {
            hash: T::STRUCT_HASH,
            shape: T::SHAPE,
            live_id: id,
            tenant: self.tenant(),
            user: self.user(),
            pivot_id: Some(self.pivot()),
            imposed,
            floor,
            revives: false,
        };
        let (mut batch, old, live_meta) =
            plan_chained_save::<T, S>(store, &plan, value).await?;
        let mut records = self.recency_chain(&pivot);
        let mut secs = self.sec_chains(&pivot);
        let mut lists = self.list_chains(&pivot);
        let mut fuzzy = self.fuzzy_chains(&pivot);
        // Removals and inserts mutate the same structures in one batch: each
        // plan reads through the overlay of the pending node writes.
        let mut view = Overlay::new(store);
        if let Some((old_instant, old_value)) = &old {
            let mut moving = Reindex {
                records: &mut records,
                lists: &mut lists,
                secs: &mut secs,
                fuzzy: &mut fuzzy,
            };
            self.plan_reindex(
                &mut view,
                &mut batch,
                &mut moving,
                (id, *old_instant),
                &live_meta,
                (old_value, value),
            )
            .await?;
        } else if T::NUM_SECONDARIES > 0 {
            return Err(crate::Error::RecordMissing(id));
        }
        self.push_root_moves(
            &mut batch,
            &pivot,
            &MovedRoots {
                secondaries: &secs,
                recency: records.roots(),
                removals: pivot.removals(),
                lists: &lists,
                fuzzy: &fuzzy,
            },
        );
        store.apply(&batch).await?;
        store.note_mutation(|| Mutation {
            struct_hash: T::STRUCT_HASH,
            tenant: self.tenant(),
            pivot: Some(self.pivot()),
            id,
            kind: MutationKind::Saved,
            meta: Some(live_meta),
            body: to_wire(value),
        });
        Ok(())
    }
}
