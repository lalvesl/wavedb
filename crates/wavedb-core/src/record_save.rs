//! Planning a chained save ([`crate::record`]'s version chain): the
//! superseded version archived at its derived slot, the new live version
//! written at the anchor, and the guard on what was planned from — as the
//! writes of one atomic batch. Split from [`crate::record`] for the file
//! budget.

use crate::error::{Error, Result};
use crate::id::Id;
use crate::local_id::LocalId;
use crate::metadata::{Metadata, Succession};
use crate::record::{
    archive_id, encode_record, encode_record_raw, mint_instant, split_record,
};
use crate::store::{Store, Write};
use crate::traits::Shape;
use crate::u48::U48;
use crate::wire::{WaveWire, from_wire};

/// One chained save's addressing and authorship, bundled for
/// [`plan_chained_save`].
pub struct SavePlan {
    /// The saved type's `STRUCT_HASH`.
    pub hash: u64,
    /// Its shape — picks the archive namespace's flipped `FLAG`.
    pub shape: Shape,
    /// The shape's anchor the live record sits at.
    pub live_id: Id,
    pub tenant: U48,
    /// Stamped as `Metadata.user`.
    pub user: U48,
    /// The pivot back-link a **first** version is stamped with (a later
    /// save carries the existing one forward).
    pub pivot_id: Option<LocalId>,
    /// A node-authoritative [`Metadata`] to write **verbatim** instead of
    /// authoring one — the cache-mirror adopt path, so a mirrored record
    /// carries the node's chain data and the mirror's archives land at the
    /// node's own derived slots. `None` = author a fresh version here.
    pub imposed: Option<Metadata>,
    /// The collection's persisted instant watermark (its recency/dead
    /// maxima) an authored version must land strictly above — `0` for a
    /// `Unique` (no collection; its chain guard alone keeps it walkable).
    pub floor: u64,
    /// Whether this write deliberately makes a **non-living** anchor live
    /// again, clearing [`Metadata::removed`].
    ///
    /// `false` for an ordinary save, which carries the removal forward: writing
    /// to a removed anchor must not silently resurrect it, or the record's own
    /// metadata would claim to be alive while every index still says otherwise.
    /// `true` only on the path that re-indexes it too — a `#[wavedb::key]`
    /// upsert at a dead anchor, or a mirror adopting that revival.
    pub revives: bool,
}

/// A chained save planned: its writes, the superseded version's authoring
/// instant and value (`None` on a first save), and the new live version's
/// [`Metadata`].
pub type Planned<V> = (Vec<Write>, Option<(u64, V)>, Metadata);

/// Plan a chained save of `value` at the shape's anchor: archive the
/// superseded version (when one exists) at its **derived slot**, stamp
/// its forward link, and write the new live record — all as `Write`s for
/// one atomic batch. Returns the writes, the superseded version's
/// authoring instant and decoded value (`None` on a first save) for the
/// caller's own needs (secondary and recency re-keying), and the new live
/// version's [`Metadata`] (what `note_mutation` carries to watchers).
///
/// The batch opens with a [`Write::Expect`] guard: two concurrent saves of
/// one anchor would derive the **same** archive slot, and the loser's
/// commit would overwrite history — the guard refuses it as a typed
/// [`Error::Conflict`] instead (the caller re-plans against the new live
/// version).
///
/// # Errors
/// Propagates a [`Store`] failure or a decode fault on the existing
/// record; [`Error::ChainCorrupt`] when the record at the anchor carries
/// an archive's `Next` (or an imposed metadata does).
pub async fn plan_chained_save<V: WaveWire, S: Store>(
    store: &S,
    plan: &SavePlan,
    value: &V,
) -> Result<Planned<V>> {
    let current = store.get_of(plan.hash, plan.live_id).await?;
    plan_chained_save_over(plan, current, value)
}

/// [`plan_chained_save`] over the anchor's bytes as the caller already
/// read them — one read of the anchor per write, the guard comparing
/// against exactly what was planned from.
///
/// # Errors
/// As [`plan_chained_save`], but for the store's.
pub fn plan_chained_save_over<V: WaveWire>(
    plan: &SavePlan,
    current: Option<Vec<u8>>,
    value: &V,
) -> Result<Planned<V>> {
    let Some(old_bytes) = current else {
        // First version: nothing to archive. The guard still rides along —
        // a concurrent first save must conflict, not silently supersede.
        let meta = plan.imposed.clone().unwrap_or_else(|| Metadata {
            succession: Succession::CreatedAt(mint_instant(plan.floor)),
            pivot_id: plan.pivot_id,
            user: plan.user,
            ..Metadata::default()
        });
        let writes = vec![
            Write::Expect(plan.hash, plan.live_id, None),
            Write::Put(plan.live_id, encode_record(plan.hash, &meta, value)),
        ];
        return Ok((writes, None, meta));
    };
    let (old_meta, old_body) = split_record(plan.hash, &old_bytes)?;
    let Succession::CreatedAt(authored) = old_meta.succession else {
        return Err(Error::ChainCorrupt(plan.live_id));
    };

    // The new version's instant: imposed metadata rules verbatim (the node
    // authored it); an authored-here save stays monotone within the chain
    // (equal instants would collapse two archive slots into one) and above
    // the collection floor, even against a stalled or rewound clock.
    let live_meta = plan.imposed.clone().unwrap_or_else(|| Metadata {
        previous: Some(authored),
        succession: Succession::CreatedAt(mint_instant(
            authored.max(plan.floor),
        )),
        pivot_id: old_meta.pivot_id,
        user: plan.user,
        device_created: 0,
        permission: old_meta.permission.clone(),
        // A removal carries forward unless this write is the one that
        // re-indexes the anchor into the living set.
        removed: !plan.revives && old_meta.removed,
    });
    let Succession::CreatedAt(instant) = live_meta.succession else {
        return Err(Error::ChainCorrupt(plan.live_id));
    };

    let mut writes = Vec::with_capacity(3);
    // Archive the superseded version byte-for-byte at its derived slot,
    // its forward link stamped with the successor's instant — written once,
    // correct forever (the successor's own archival derives the same slot
    // from the same value). A mirror whose local state does not precede the
    // imposed instant skips the archive: its stale bytes are not a version
    // the node's chain knows.
    if authored < instant {
        let slot = archive_id(plan.hash, plan.shape, authored, plan.tenant);
        let archived = Metadata {
            succession: Succession::Next(instant),
            ..old_meta
        };
        writes.push(Write::Put(
            slot,
            encode_record_raw(plan.hash, &archived, old_body),
        ));
    }
    writes.push(Write::Put(
        plan.live_id,
        encode_record(plan.hash, &live_meta, value),
    ));
    let old_value = from_wire::<V>(old_body)?;
    writes.insert(0, Write::Expect(plan.hash, plan.live_id, Some(old_bytes)));
    Ok((writes, Some((authored, old_value)), live_meta))
}
