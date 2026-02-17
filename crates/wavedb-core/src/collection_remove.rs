//! [`Collection`]'s removal half — `remove`, the one op that writes the `dead`
//! log and the one that takes a record out of the living set.
//!
//! Split from [`collection_write`](crate::collection_write) for the file budget,
//! along the same seam the index layer already uses (`tree_insert` /
//! `tree_delete`, `chain` / `chain_remove`): the shrinking side carries its own
//! policy and its own invariants.
//!
//! **Bytes are never destroyed.** A removal de-indexes the record and logs the
//! event; the record itself stays at its anchor, so `Collection::get` and a
//! history walk keep resolving it by address. What changes on the record is one
//! flag — [`Metadata::removed`] — because liveness is a property of the anchor
//! rather than of a membership tree (RFC 0050).
//!
//! [`Metadata::removed`]: crate::Metadata::removed

use crate::collection::Collection;
use crate::collection_read::Anchor;
use crate::collection_write::MovedRoots;
use crate::error::Result;
use crate::id::Id;
use crate::local_id::LocalId;
use crate::metadata::{Metadata, Succession};
use crate::notify::{Mutation, MutationKind};
use crate::record::{encode_record, mint_instant};
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;

impl<T: NonUniqueStruct> Collection<T> {
    /// Remove the record at `id` from the living set: de-indexes it from
    /// `current`, the recency log, **and every secondary tree**, and logs it
    /// in `dead` keyed by a freshly minted removal instant — one atomic
    /// batch. The record **bytes stay** (history stays navigable); only
    /// `remove` ever writes the `dead` tree. Returns whether the record was
    /// in `current`.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure, [`Error::PivotMissing`](crate::Error::PivotMissing)
    /// on a stale handle, [`Error::RecordMissing`](crate::Error::RecordMissing) if a
    /// living record's bytes are gone (index out of sync), or a decode fault on
    /// a corrupt pivot.
    pub async fn remove<S: Store>(&self, store: &S, id: Id) -> Result<bool> {
        let pivot = self.load_pivot(store).await?;
        let mut secs = self.sec_chains(&pivot);

        // Liveness reads off the anchor (RFC 0050), and the same read hands
        // back what the removal needs: the live instant keying the record's
        // chain entry, and the field values its secondary keys were built
        // from. A vacant or already-dead anchor is not in the living set.
        let Anchor::Living(meta, value) = self.anchor(store, id).await? else {
            return Ok(false);
        };
        let mut batch = Vec::new();
        let floor = self.instant_floor(store, &pivot).await?;
        let removed_at = mint_instant(floor);
        let anchor = LocalId::from_id(id);
        // Liveness is a property of the anchor, not of a membership tree
        // (RFC 0050): the record's bytes stay exactly where they are and simply
        // record that they left the living set, so the question is answered by
        // the read that fetches the record.
        batch.push(Write::Put(
            id,
            encode_record(
                T::STRUCT_HASH,
                &Metadata {
                    removed: true,
                    ..meta.clone()
                },
                &value,
            ),
        ));
        // The chains: the record leaves the living one — which is *why* the
        // removal log can stay skinny, since the anchor becomes the only home
        // for its bytes — and an instant-keyed reference joins the removal log.
        let mut records = self.recency_chain(&pivot);
        let mut removals = self.dead_log(&pivot);
        let mut lists = self.list_chains(&pivot);
        if let Succession::CreatedAt(instant) = meta.succession
            && let Some(writes) = records
                .plan_remove(store, &Self::instant_key(instant, anchor))
                .await?
        {
            batch.extend(writes);
        }
        // Every declared list holds only living records, so a removal takes the
        // record out of all of them (RFC 0051).
        self.plan_list_removes(store, &mut batch, &mut lists, id, &value)
            .await?;
        batch.extend(
            removals
                .plan_insert(store, Self::instant_key(removed_at, anchor), ())
                .await?,
        );
        for (i, chain) in secs.iter_mut().enumerate() {
            let key = Self::sec_key(&value, i, id);
            if let Some(writes) = chain.plan_remove(store, &key).await? {
                batch.extend(writes);
            }
        }
        // A fuzzy index holds only living records too — every posting goes.
        let mut fuzzy = self.fuzzy_trees(&pivot);
        let mut view = crate::overlay::Overlay::new(store);
        self.plan_fuzzy_removes(&mut view, &mut batch, &mut fuzzy, id, &value)
            .await?;
        self.push_root_moves(
            &mut batch,
            &pivot,
            &MovedRoots {
                secondaries: &secs,
                recency: records.roots(),
                removals: removals.log_roots(),
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
            kind: MutationKind::Removed(removed_at),
            meta: None,
            body: Vec::new(),
        });
        Ok(true)
    }
}
