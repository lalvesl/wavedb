//! [`Collection`]'s removal half — `remove`, the one op that writes the `dead`
//! log and the one that takes a record out of the living set.
//!
//! Split from [`collection_write`](crate::collection_write) for the file
//! budget: the shrinking side carries its own policy and its own invariants.
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
use crate::error::Result;
use crate::id::Id;
use crate::local_id::LocalId;
use crate::metadata::{Metadata, Succession};
use crate::notify::{Mutation, MutationKind};
use crate::record::{encode_record, mint_instant};
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;

impl<T: NonUniqueStruct> Collection<T> {
    /// Remove the record at `id` from the living set: takes it out of the
    /// recency chain, every declared list **and every secondary**, and logs
    /// it in the removal log keyed by a freshly minted removal instant — one
    /// atomic batch. The record **bytes stay** (history stays navigable);
    /// only `remove` ever writes the removal log. Returns whether the record
    /// was living.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure, [`Error::PivotMissing`](crate::Error::PivotMissing)
    /// on a stale handle, [`Error::RecordMissing`](crate::Error::RecordMissing) if a
    /// living record's bytes are gone (index out of sync), or a decode fault on
    /// a corrupt pivot.
    pub async fn remove<S: Store>(&self, store: &S, id: Id) -> Result<bool> {
        self.check_pivot(store).await?;
        let Anchor::Living(meta, value) = self.anchor(store, id).await? else {
            return Ok(false);
        };
        let floor = self.instant_floor(store).await?;
        let removed_at = mint_instant(floor);
        let anchor = LocalId::from_id(id);
        let mut batch = vec![Write::Put(
            id,
            encode_record(
                T::STRUCT_HASH,
                &Metadata {
                    removed: true,
                    ..meta.clone()
                },
                &value,
            ),
        )];
        if let Succession::CreatedAt(instant) = meta.succession {
            let key = Self::instant_key(instant, anchor);
            batch.push(self.recency_chain().remove(&key));
        }
        self.plan_list_removes(&mut batch, id, &value);
        let logged = Self::instant_key(removed_at, anchor);
        batch.push(self.dead_log().insert(&logged, &()));
        for (i, chain) in self.sec_chains().iter().enumerate() {
            batch.push(chain.remove(&Self::sec_key(&value, i, id)));
        }
        self.plan_fuzzy_removes(&mut batch, id, &value);
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
