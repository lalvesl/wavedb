//! The spare room of a header ([RFC 0072] §5): what is left of its 32 KiB
//! after the addresses its batch changed restates old ones — on disk and in
//! the object store alike — and the horizon that restating moves.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point-WIP.md

use std::collections::BTreeMap;

use crate::block::BlockDescriptor;
use crate::header::{ENTRY_BYTES, ITEM_BYTES, NO_FILE, Rotation, TypeEntry};

/// A type's directory as a header names it, and restates from. Every
/// registered type has one, a type never written with no slots: it is
/// named too, in its turn.
pub struct TypeView<'a> {
    pub(crate) struct_hash: u64,
    pub(crate) slots: &'a [BlockDescriptor],
    /// Its dictionary's descriptor, if it has one.
    pub(crate) dict: Option<u64>,
}

/// Spend `spare` bytes of the header at `at` restating addresses into
/// `entries`: the type restated longest ago first — one never restated
/// before any, the smaller directory first on a tie, so the empty ones take
/// an entry and no item and the first header names them all — from its
/// rotating cursor, then the next, until the room runs out or every type had
/// its turn. Returns the bytes left.
///
/// By restatement, not by mention: a collection's `Pivot` and its B+tree
/// nodes keep their ids, so the same few buckets of a type change in every
/// batch while the rest hold still. Ordered by mention, that type would
/// never be restated, and its quiet buckets would be known only from ever
/// older headers.
pub fn restate(
    entries: &mut Vec<TypeEntry>,
    mut spare: usize,
    views: &[TypeView<'_>],
    rotations: &mut BTreeMap<u64, Rotation>,
    at: u64,
) -> usize {
    let mut order: Vec<&TypeView<'_>> = views.iter().collect();
    order.sort_by_key(|v| {
        let shifted = rotations.get(&v.struct_hash).and_then(|r| r.shifted);
        (shifted.map_or(0, |at| at.saturating_add(1)), v.slots.len())
    });
    for view in order {
        let buckets = view.slots.len();
        let present = entries
            .iter()
            .position(|e| e.struct_hash == view.struct_hash);
        let entry = if present.is_some() { 0 } else { ENTRY_BYTES };
        let least = entry + if buckets > 0 { ITEM_BYTES } else { 0 };
        if least == 0 || spare < least {
            continue;
        }
        let n = ((spare - entry) / ITEM_BYTES).min(buckets);
        let rotation = rotations.entry(view.struct_hash).or_default();
        let idx = present.unwrap_or_else(|| {
            entries.push(TypeEntry {
                struct_hash: view.struct_hash,
                buckets: buckets as u32,
                last: rotation.last,
                dict: view.dict,
                changed: Vec::new(),
            });
            entries.len() - 1
        });
        if buckets > 0 {
            if rotation.began.is_none() {
                rotation.began = Some(at);
            }
            for k in 0..n {
                let bucket = (rotation.shift + k) % buckets;
                entries[idx]
                    .changed
                    .push((bucket as u32, view.slots[bucket].raw()));
            }
            if rotation.shift + n >= buckets {
                // A whole rotation: every bucket named since it began.
                rotation.covered = rotation.began;
                rotation.began = Some(at);
            }
            rotation.shift = (rotation.shift + n) % buckets;
        }
        rotation.shifted = Some(at);
        spare -= entry + n * ITEM_BYTES;
    }
    spare
}

/// The lowest header a reconstruction from the newest one may still read
/// for these types: for a type with addresses, where its last whole
/// rotation began — every bucket is named at or after it — and for an empty
/// type the newest header naming it. A type with no whole rotation since
/// the store opened holds it at `floor`, the lowest header the open read.
pub fn horizon(
    views: &[TypeView<'_>],
    rotations: &BTreeMap<u64, Rotation>,
    floor: u64,
) -> u64 {
    views
        .iter()
        .map(|v| {
            let rotation = rotations.get(&v.struct_hash);
            if v.slots.is_empty() {
                rotation
                    .map(|r| r.last)
                    .filter(|&last| last != NO_FILE)
                    .unwrap_or(floor)
            } else {
                rotation.and_then(|r| r.covered).unwrap_or(floor)
            }
        })
        .min()
        .unwrap_or(floor)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{TypeView, horizon, restate};
    use crate::block::BlockDescriptor;
    use crate::header::{Rotation, TypeEntry};

    fn dir(n: usize) -> Vec<BlockDescriptor> {
        (0..n)
            .map(|b| BlockDescriptor::new(b as u64 + 1, 1))
            .collect()
    }

    fn view(hash: u64, slots: &[BlockDescriptor]) -> TypeView<'_> {
        TypeView {
            struct_hash: hash,
            slots,
            dict: None,
        }
    }

    /// A rotation covers a type once its cursor has come round: the horizon
    /// is then where that rotation began, and rises with the next one.
    #[test]
    fn the_horizon_rises_as_rotations_complete() {
        let (a, b) = (dir(100), dir(10));
        let views = [view(1, &a), view(2, &b)];
        let mut rotations: BTreeMap<u64, Rotation> = BTreeMap::new();
        let room = 12 * 40; // forty items a header, about
        for at in 0..3 {
            restate(
                &mut Vec::<TypeEntry>::new(),
                room,
                &views,
                &mut rotations,
                at,
            );
        }
        assert_eq!(horizon(&views, &rotations, 0), 0, "type 1 not round yet");
        for at in 3..12 {
            restate(&mut Vec::new(), room, &views, &mut rotations, at);
        }
        let h = horizon(&views, &rotations, 0);
        assert!(h > 0, "both came round at least once: {h}");
        assert!(rotations[&2].covered.is_some());
    }
}
