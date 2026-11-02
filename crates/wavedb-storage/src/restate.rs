//! The spare room of a header ([RFC 0072] §5, [RFC 0078] §6): what is left
//! of its 32 KiB after the ranges its batch changed restates old ones,
//! whole — on disk and in the object store alike — and the horizon that
//! restating moves.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::collections::BTreeMap;

use crate::header::{ENTRY_BYTES, NO_FILE, Rotation, TypeEntry};
use crate::pages::Pages;

/// A type's index as a header names it, and restates from. Every
/// registered type has one, a type never written with no index: it is
/// named too, in its turn.
pub struct TypeView<'a> {
    pub(crate) struct_hash: u64,
    pub(crate) index: Option<&'a Pages>,
    /// Its dictionary's descriptor, if it has one.
    pub(crate) dict: Option<u64>,
}

impl TypeView<'_> {
    /// Ranges its index holds.
    pub(crate) fn ranges(&self) -> usize {
        self.index.map_or(0, Pages::range_count)
    }
}

/// Spend `spare` bytes of the header at `at` restating ranges into
/// `entries`: the type restated longest ago first — one never restated
/// before any, the smaller index first on a tie, so the empty ones take an
/// entry and no record and the first header names them all — from its
/// rotating cursor, a range at a time, whole, until the room runs out or
/// every type had its turn. Returns the bytes left.
///
/// By restatement, not by mention: a collection's `Pivot` keeps its id, so
/// the same few pages of a type change in every batch while the rest hold
/// still. Ordered by mention, that type would never be restated, and its
/// quiet ranges would be known only from ever older headers.
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
        (shifted.map_or(0, |at| at.saturating_add(1)), v.ranges())
    });
    for view in order {
        let present = entries
            .iter()
            .position(|e| e.struct_hash == view.struct_hash);
        let entry = if present.is_some() { 0 } else { ENTRY_BYTES };
        let rotation = rotations.entry(view.struct_hash).or_default();
        let (named, used) = view.index.map_or((Vec::new(), 0), |index| {
            rotate(index, rotation, spare.saturating_sub(entry), at)
        });
        let worth =
            present.is_some() || !named.is_empty() || view.ranges() == 0;
        if !worth || spare < entry + used {
            continue;
        }
        let idx = present.unwrap_or_else(|| {
            entries.push(TypeEntry {
                struct_hash: view.struct_hash,
                ranges: view.ranges() as u32,
                last: rotation.last,
                dict: view.dict,
                changed: Vec::new(),
            });
            entries.len() - 1
        });
        entries[idx].changed.extend(named);
        rotation.shifted = Some(at);
        spare -= entry + used;
    }
    spare
}

/// Restate `index`'s ranges from `rotation`'s cursor, whole, as many as
/// `room` bytes hold — at most one pass. A pass coming round marks the
/// type covered from where it began.
fn rotate(
    index: &Pages,
    rotation: &mut Rotation,
    room: usize,
    at: u64,
) -> (Vec<crate::ordered::RangeRecord>, usize) {
    let (mut named, mut used) = (Vec::new(), 0);
    let len = index.range_count();
    if len == 0 {
        return (named, used);
    }
    if rotation.began.is_none() {
        rotation.began = Some(at);
    }
    let mut steps = 0;
    while steps < len {
        let Some(range) = index.range_after(rotation.cursor.as_deref()) else {
            // A whole rotation: every range named since it began.
            rotation.covered = rotation.began;
            rotation.began = Some(at);
            rotation.cursor = None;
            continue;
        };
        let (start, record) = index.whole_record(range);
        let cost = record.wire_len();
        if used + cost > room {
            break;
        }
        used += cost;
        named.push(record);
        rotation.cursor = Some(start);
        steps += 1;
    }
    if index.range_after(rotation.cursor.as_deref()).is_none() {
        // The last range named: the pass has come round.
        rotation.covered = rotation.began;
        rotation.began = Some(at);
        rotation.cursor = None;
    }
    (named, used)
}

/// The lowest header a reconstruction from the newest one may still read
/// for these types: for a type with ranges, where its last whole rotation
/// began — every range is named at or after it — and for an empty type the
/// newest header naming it. A type with no whole rotation since the store
/// opened holds it at `floor`, the lowest header the open read.
pub fn horizon(
    views: &[TypeView<'_>],
    rotations: &BTreeMap<u64, Rotation>,
    floor: u64,
) -> u64 {
    views
        .iter()
        .map(|v| {
            let rotation = rotations.get(&v.struct_hash);
            if v.ranges() == 0 {
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
    use crate::header::{Rotation, TypeEntry};
    use crate::ordered::{Cut, CutOutcome, Found, Placed, RangeRecord};
    use crate::pages::{Pages, disk_pages};

    /// An index of `n` ranges, one page each: every page cut off the one
    /// before, at an address no range of it reaches.
    fn index(n: u64) -> Pages {
        let key = |i: u64| (i << 56).to_be_bytes();
        let page = |address| Placed {
            address,
            blocks: 1,
            meta: 0,
        };
        let mut pages = disk_pages();
        pages.place(&key(0), page(0)).unwrap();
        for i in 1..n {
            let Found::Page { at, .. } = pages.find(&key(i - 1)) else {
                panic!("the page before");
            };
            let (left, right) = (key(i - 1), key(i));
            let cut = Cut {
                first: &left,
                left_last: &left,
                right_first: &right,
                last: &right,
            };
            let outcome = pages.cut(at, &cut, page(i * 100_000));
            assert!(matches!(outcome, CutOutcome::Cut(_)));
        }
        assert_eq!(pages.range_count() as u64, n);
        pages
    }

    fn view(hash: u64, index: &Pages) -> TypeView<'_> {
        TypeView {
            struct_hash: hash,
            index: Some(index),
            dict: None,
        }
    }

    /// A rotation covers a type once its cursor has come round: the horizon
    /// is then where that rotation began, and rises with the next one.
    #[test]
    fn the_horizon_rises_as_rotations_complete() {
        let (a, b) = (index(60), index(4));
        let views = [view(1, &a), view(2, &b)];
        let mut rotations: BTreeMap<u64, Rotation> = BTreeMap::new();
        let room = 400; // a few ranges a header
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
        for at in 3..80 {
            restate(&mut Vec::new(), room, &views, &mut rotations, at);
        }
        let h = horizon(&views, &rotations, 0);
        assert!(h > 0, "both came round at least once: {h}");
        assert!(rotations[&2].covered.is_some());
    }

    /// One header with room for all names every range once, whole, and
    /// comes round.
    #[test]
    fn a_rotation_names_every_range_whole() {
        let pages = index(5);
        let views = [view(1, &pages)];
        let mut rotations = BTreeMap::new();
        let mut entries = Vec::new();
        restate(&mut entries, 32 * 1024, &views, &mut rotations, 0);
        assert_eq!(entries[0].changed.len(), 5);
        assert!(
            entries[0]
                .changed
                .iter()
                .all(|r| matches!(r, RangeRecord::Whole(_)))
        );
        assert_eq!(entries[0].ranges, 5);
        assert!(rotations[&1].covered.is_some(), "one header, whole round");
    }
}
