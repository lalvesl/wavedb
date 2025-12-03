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

/// Every structure a save re-keys, gathered so the planners can be handed
/// one thing instead of four.
pub struct Reindex {
    /// The built-in recency chain.
    pub records: Chain<()>,
    /// The declared lists (RFC 0051).
    pub lists: Vec<Chain<Vec<u8>>>,
    /// The `#[wavedb::pivot(...)]` secondary chains (RFC 0075).
    pub secs: Vec<Chain<()>>,
    /// The `#[wavedb::fuzzy]` posting chains (RFC 0056).
    pub fuzzy: Vec<Chain<()>>,
}

impl<T: NonUniqueStruct> Collection<T> {
    /// Re-key every derived structure for a record whose live version just
    /// changed from `old_value` to `value`.
    ///
    /// `at` is the record's `(anchor, previous authoring instant)` — the chain
    /// entry to vacate. The recency chain's verdict gates the lists: a record
    /// it does not hold must not enter any other chain.
    pub(crate) async fn plan_reindex<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        batch: &mut Vec<Write>,
        moving: &Reindex,
        at: (Id, u64),
        live_meta: &Metadata,
        values: (&T, &T),
    ) -> Result<()> {
        let (id, old_instant) = at;
        let (old_value, value) = values;
        let moved = self
            .plan_chain_move(
                view,
                batch,
                &moving.records,
                Self::instant_key(old_instant, LocalId::from_id(id)),
                live_meta,
                value,
            )
            .await?;
        if let Some(envelope) = moved {
            Self::plan_list_moves(
                batch,
                &moving.lists,
                id,
                &envelope,
                (old_value, value),
            );
        }
        Self::plan_secondary_rekeys(
            batch,
            &moving.secs,
            id,
            (old_value, value),
        );
        Self::plan_fuzzy_moves(batch, &moving.fuzzy, id, (old_value, value));
        Ok(())
    }

    /// Re-key every secondary index whose fields the save changed.
    ///
    /// An index whose fields are untouched is skipped outright: a secondary
    /// stores a key and an anchor, never the record, so nothing about it can
    /// go stale when another field moves.
    fn plan_secondary_rekeys(
        batch: &mut Vec<Write>,
        secs: &[Chain<()>],
        id: Id,
        values: (&T, &T),
    ) {
        let (old_value, value) = values;
        for (i, chain) in secs.iter().enumerate() {
            let old_key = Self::sec_key(old_value, i, id);
            let new_key = Self::sec_key(value, i, id);
            if old_key != new_key {
                batch.push(chain.remove(&old_key));
                batch.push(chain.insert(&new_key, &()));
            }
        }
    }
}
