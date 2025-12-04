//! [`Collection`]'s mutating half — `insert` / `save` / `remove` — each one
//! atomic [`Store::apply`] batch: the record write and one entry per chain it
//! touches (recency, removals, lists, secondaries, fuzzy), each a key of its
//! lane (RFC 0078 §7). No chain write rewrites the `Pivot`.
//!
//! Every instant minted here (an insert's id, a save's version instant, a
//! removal's log key) lands strictly above the collection's **instant
//! floor** (see [`crate::collection_recency`]), so the recency and dead
//! logs only ever grow at their tails.

use crate::collection::Collection;
use crate::collection_reindex::Reindex;
use crate::error::Result;
use crate::id::Id;
use crate::local_id::LocalId;
use crate::metadata::{Metadata, Succession};
use crate::notify::{Mutation, MutationKind};
use crate::record::{
    Overlay, encode_record, mint_floored_id, plan_chained_save,
};
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;
use crate::wire::to_wire;

impl<T: NonUniqueStruct> Collection<T> {
    /// Insert `value` as a new record: mints its timestamp-keyed [`Id`] (the
    /// stable identity for the record's whole life), writes the record, and
    /// files it in the recency chain, every declared list and every
    /// secondary — one atomic batch. Returns the minted `Id`.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure, [`Error::PivotMissing`](crate::Error::PivotMissing)
    /// on a stale handle, or a decode fault on a corrupt pivot.
    pub async fn insert<S: Store>(&self, store: &S, value: &T) -> Result<Id> {
        self.check_pivot(store).await?;
        // A `#[wavedb::key(...)]` type's identity is its key fields —
        // insert IS the upsert at the content-derived anchor (see
        // `collection_keyed`).
        if let Some(id) = self.keyed_anchor(value) {
            self.upsert_keyed(store, id, value).await?;
            return Ok(id);
        }
        let floor = self.instant_floor(store).await?;
        let id = mint_floored_id(self.tenant(), T::STRUCT_HASH, floor);
        self.insert_at(store, id, value).await?;
        Ok(id)
    }

    /// [`insert`](Self::insert)'s body with the identity supplied instead of
    /// minted — the seam [`adopt`](Self::adopt) writes a **node-minted** `Id`
    /// through when mirroring a record into a local cache store.
    pub(crate) async fn insert_at<S: Store>(
        &self,
        store: &S,
        id: Id,
        value: &T,
    ) -> Result<()> {
        // First version: no chain yet; the pivot back-link is stamped here
        // and the writer identity is the tenant until node auth exists (M8).
        // The authoring instant is the id's own key — the key IS when that
        // version was authored.
        let meta = Metadata {
            succession: Succession::CreatedAt(id.key()),
            pivot_id: Some(self.pivot()),
            user: self.user(),
            ..Metadata::default()
        };
        self.insert_with_meta(store, id, meta, value).await
    }

    /// The indexed first-version write, with the [`Metadata`] supplied —
    /// [`adopt_with`](Self::adopt_with) passes the **node's** verbatim, so a
    /// mirror carries authoritative chain data.
    pub(crate) async fn insert_with_meta<S: Store>(
        &self,
        store: &S,
        id: Id,
        meta: Metadata,
        value: &T,
    ) -> Result<()> {
        // A first version is live by definition; its instant keys the
        // recency entry.
        let Succession::CreatedAt(instant) = meta.succession else {
            return Err(crate::Error::ChainCorrupt(id));
        };
        let envelope = encode_record(T::STRUCT_HASH, &meta, value);
        let key = Self::instant_key(instant, LocalId::from_id(id));
        let mut batch = vec![Write::Put(id, envelope.clone())];
        // One recency entry: the membership set and the modification log at
        // once (RFC 0050)…
        batch.push(self.recency_chain().insert(&key, &()));
        // …one more copy per declared list, sorted by its own property
        // (RFC 0051)…
        self.plan_list_inserts(&mut batch, id, &envelope, value);
        for (i, chain) in self.sec_chains().iter().enumerate() {
            batch.push(chain.insert(&Self::sec_key(value, i, id), &()));
        }
        // …and `L + n - 1` postings per fuzzy declaration (RFC 0056).
        self.plan_fuzzy_inserts(&mut batch, id, value);
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
    /// [`history`](Collection::history). The `Id` is unchanged; each
    /// **secondary** re-keys when its field values changed (old key out,
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

    /// The body of [`save`](Self::save), with `imposed` metadata when a
    /// mirror writes the node's verbatim.
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
        self.check_pivot(store).await?;
        let floor = self.instant_floor(store).await?;
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
        // The moves are gated on reads of the chains; each reads through the
        // overlay of what the batch already writes.
        let mut view = Overlay::new(store);
        view.stage(&batch);
        if let Some((old_instant, old_value)) = &old {
            let moving = Reindex {
                records: self.recency_chain(),
                lists: self.list_chains(),
                secs: self.sec_chains(),
                fuzzy: self.fuzzy_chains(),
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
