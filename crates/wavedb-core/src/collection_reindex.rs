//! What a **save** has to move, once the record itself is written: every
//! derived structure that was keyed off the old version.
//!
//! Split from [`collection_write`](crate::collection_write) for the file
//! budget, along the seam the save already had — `save_planned` decides *that*
//! a version was superseded, this decides what that costs each structure.
//!
//! The four kinds answer the same question differently, and the differences
//! are the whole design:
//!
//! | structure | moves when | writes |
//! |---|---|---|
//! | the recency chain | **always** — its key is the authoring instant | one entry |
//! | a declared list | always, even if its order is unchanged ([RFC 0051]) | the whole record |
//! | a secondary index | only if its own fields changed | one key |
//! | a fuzzy index | only the grams that changed ([RFC 0056]) | one key each |
//!
//! A list is the outlier, and not by accident: it is the only one that
//! *duplicates the record*, so any other field moving makes its copy stale.
//! The other three store keys and anchors, so an unchanged input means there is
//! genuinely nothing to say.
//!
//! [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
//! [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md

use crate::collection::Collection;
use crate::error::Result;
use crate::id::Id;
use crate::index::Chain;
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::overlay::Overlay;
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;

/// Every structure a save re-keys, borrowed together so the planners can be
/// handed one thing instead of four.
pub struct Reindex<'a> {
    /// The built-in recency chain.
    pub records: &'a mut Chain<()>,
    /// The declared lists (RFC 0051).
    pub lists: &'a mut [Chain<Vec<u8>>],
    /// The `#[wavedb::pivot(...)]` secondary chains (RFC 0075).
    pub secs: &'a mut [Chain<()>],
    /// The `#[wavedb::fuzzy]` posting chains (RFC 0056).
    pub fuzzy: &'a mut [Chain<()>],
}

impl<T: NonUniqueStruct> Collection<T> {
    /// Re-key every derived structure for a record whose live version just
    /// changed from `old_value` to `value`.
    ///
    /// `at` is the record's `(anchor, previous authoring instant)` — the chain
    /// entry to vacate.
    pub(crate) async fn plan_reindex<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        batch: &mut Vec<Write>,
        moving: &mut Reindex<'_>,
        at: (Id, u64),
        live_meta: &Metadata,
        values: (&T, &T),
    ) -> Result<()> {
        let (id, old_instant) = at;
        let (old_value, value) = values;
        // A save **relocates** the record in the chain: its key is the live
        // version's authoring instant, and that instant just changed. Out of
        // the old position, in at the new one — which is the movement that
        // *is* the modification log (RFC 0050).
        let moved = self
            .plan_chain_move(
                view,
                batch,
                moving.records,
                Self::instant_key(old_instant, LocalId::from_id(id)),
                live_meta,
                value,
            )
            .await?;
        // Only a record the built-in chain holds belongs in a declared list —
        // the same liveness gate, applied once (RFC 0051).
        if let Some(envelope) = moved {
            self.plan_list_moves(
                view,
                batch,
                moving.lists,
                id,
                &envelope,
                (old_value, value),
            )
            .await?;
        }
        self.plan_secondary_rekeys(
            view,
            batch,
            moving.secs,
            id,
            (old_value, value),
        )
        .await?;
        // Only the grams that actually moved — an unchanged indexed field
        // writes nothing at all here (RFC 0056).
        self.plan_fuzzy_moves(view, batch, moving.fuzzy, id, (old_value, value))
            .await
    }

    /// Re-key every secondary index whose fields the save changed.
    ///
    /// An index whose fields are untouched is skipped outright — the same
    /// shape as [`plan_fuzzy_moves`](Collection::plan_fuzzy_moves)'s rule, and
    /// for the same reason: a secondary stores a key and an anchor, never the
    /// record, so nothing about it can go stale when another field moves.
    async fn plan_secondary_rekeys<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        batch: &mut Vec<Write>,
        secs: &mut [Chain<()>],
        id: Id,
        values: (&T, &T),
    ) -> Result<()> {
        let (old_value, value) = values;
        for (i, chain) in secs.iter_mut().enumerate() {
            let old_key = Self::sec_key(old_value, i, id);
            let new_key = Self::sec_key(value, i, id);
            if old_key == new_key {
                continue;
            }
            if let Some(writes) = chain.plan_remove(&*view, &old_key).await? {
                view.stage(&writes);
                batch.extend(writes);
            }
            let writes = chain.plan_insert(&*view, new_key, ()).await?;
            view.stage(&writes);
            batch.extend(writes);
        }
        Ok(())
    }
}
