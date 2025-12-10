//! Folding two neighbouring ranges into one ([RFC 0078] §4, §12): no page
//! moves, only the index — a page given a range of its own because its
//! range could not address it is folded into the range beside it when one
//! sub-header can name both. Pages a round writes lie side by side, so the
//! two halves of a cut, and neighbours in key order rewritten together,
//! fold back as they are isolated.
//!
//! A fold keeps every entry's start, so every key finds the page it found
//! before; quanta between the two ranges that no entry covered become a
//! hole.
//!
//! [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::cmp::Ordering;

use super::access::MAX_RANGE_ENTRIES;
use super::head::{QUANTA, Shape, SubHeader, q_of};
use super::index::{Entries, PageIndex, Range, Slot};

impl<M: Copy + Default> PageIndex<M> {
    /// Fold `at`'s range with the ranges on either side where it can;
    /// where `at`'s entry is now.
    pub(super) fn fold_around(&mut self, at: Slot) -> Slot {
        self.fold(at.range);
        let left = at.range.checked_sub(1);
        left.and_then(|left| self.fold(left))
            .map_or(at, |offset| Slot {
                range: at.range - 1,
                entry: offset + at.entry,
            })
    }

    /// Fold range `i + 1` into range `i`, if one sub-header can name both;
    /// where `i + 1`'s first entry is now.
    fn fold(&mut self, i: usize) -> Option<usize> {
        let (range, offset) = self.folded(i)?;
        // Named before they change: the second range is gone by its start.
        self.note_whole(i);
        self.note_whole(i + 1);
        self.ranges[i] = range;
        self.ranges.remove(i + 1);
        Some(offset)
    }

    /// Ranges `i` and `i + 1` as one, and where the second's entries start
    /// in it: the same prefix, the finer of the two quantizations, based at
    /// the lowest page, every page addressable from there — and reaching as
    /// far as the second range did, or to the range after it.
    fn folded(&self, i: usize) -> Option<(Range<M>, usize)> {
        let (a, b) = (self.ranges.get(i)?, self.ranges.get(i + 1)?);
        let (ha, hb) = (&a.head, &b.head);
        let (sa, sb) = (ha.shape, hb.shape);
        // One more for a hole between them.
        let len = a.entries.len() + b.entries.len() + 1;
        if ha.prefix != hb.prefix
            || sa.address_bits() != sb.address_bits()
            || sa.address_factor() != sb.address_factor()
            || len > MAX_RANGE_ENTRIES
        {
            return None;
        }
        let shape = Shape::new(
            sa.key_shift().min(sb.key_shift()),
            sa.address_bits(),
            sa.address_factor(),
            sa.size_factor().max(sb.size_factor()),
        )?;
        let base = [a, b]
            .iter()
            .flat_map(|r| r.entries.pages.iter().map(|&e| r.head.decode(e)))
            .flatten()
            .map(|(address, _)| address)
            .min()
            .unwrap_or(ha.base_block);
        let head = SubHeader {
            base_block: base,
            first_key: ha.first_key,
            prefix: ha.prefix.clone(),
            shape,
        };
        if !self.reaches(i, &head) {
            return None;
        }
        let mut out = Entries {
            pages: Vec::with_capacity(len),
            meta: Vec::with_capacity(len),
            counts: self.counted.then(|| Vec::with_capacity(len)),
        };
        moved(&head, a, &mut out)?;
        // Quanta past the first range's end and short of the second's start
        // held no key: a hole, not the first range's last page.
        if let Some(end) = ha.quantum_start(QUANTA)
            && end < hb.first_key
        {
            let q = requantized(&head, end)?;
            push(&mut out, head.hole(q), M::default(), 0);
        }
        let offset = out.len();
        moved(&head, b, &mut out)?;
        let ascending = out.pages.windows(2).all(|w| q_of(w[0]) < q_of(w[1]));
        ascending.then_some((Range { head, entries: out }, offset))
    }

    /// Whether `head`, folding ranges `i` and `i + 1`, still holds every
    /// key the second one did: its quanta reach as far, or past the start
    /// of the range after it.
    fn reaches(&self, i: usize, head: &SubHeader) -> bool {
        let Some(end) = head.quantum_start(QUANTA) else {
            return true; // to the end of keys
        };
        let second = &self.ranges[i + 1].head;
        if second.quantum_start(QUANTA).is_some_and(|e| e <= end) {
            return true;
        }
        let end = SubHeader {
            first_key: end,
            ..head.clone()
        }
        .start();
        self.ranges
            .get(i + 2)
            .is_some_and(|next| next.head.start_cmp(&end) != Ordering::Greater)
    }
}

/// `range`'s entries, under `head`, appended to `out`.
fn moved<M: Copy>(
    head: &SubHeader,
    range: &Range<M>,
    out: &mut Entries<M>,
) -> Option<()> {
    for (i, &entry) in range.entries.pages.iter().enumerate() {
        let q =
            requantized(head, range.head.quantum_start(q_of(entry).into())?)?;
        let encoded = match range.head.decode(entry) {
            Some((address, blocks)) => head.encode(q, address, blocks)?,
            None => head.hole(q),
        };
        let count = range.entries.count(i).unwrap_or(0);
        push(out, encoded, range.entries.meta[i], count);
    }
    Some(())
}

/// The quantum of `head` starting at `slice`, if one does.
fn requantized(head: &SubHeader, slice: u64) -> Option<u16> {
    let offset = slice.checked_sub(head.first_key)?;
    let shift = head.shape.key_shift();
    if offset & ((1u64 << shift) - 1) != 0 {
        return None;
    }
    u16::try_from(offset >> shift).ok()
}

fn push<M>(out: &mut Entries<M>, entry: u32, meta: M, count: u8) {
    out.pages.push(entry);
    out.meta.push(meta);
    if let Some(counts) = &mut out.counts {
        counts.push(count);
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Cut, CutOutcome, Found, PageIndex, Placed, Shape};

    fn page(address: u64) -> Placed<u8> {
        Placed {
            address,
            blocks: 1,
            meta: 0,
        }
    }

    /// A key in quantum `n` of the coarsest quantization.
    fn key(n: u8) -> [u8; 8] {
        (u64::from(n) << 48).to_be_bytes()
    }

    /// Three pages of one range, side by side, a quantum each.
    fn three() -> PageIndex<u8> {
        let mut ix = PageIndex::new(Shape::new(48, 4, 1, 1).unwrap());
        let mut at = ix.place(&key(0), page(10)).unwrap();
        for n in 1..3 {
            let cut = Cut {
                first: &key(n - 1),
                left_last: &key(n - 1),
                right_first: &key(n),
                last: &key(n),
            };
            let CutOutcome::Cut(right) =
                ix.cut(at, &cut, page(10 + u64::from(n)))
            else {
                panic!("distinct quanta cut");
            };
            at = right;
        }
        assert_eq!(ix.sub_headers().count(), 1);
        ix
    }

    fn rewrite(ix: &mut PageIndex<u8>, n: u8, address: u64) {
        let Found::Page { at, .. } = ix.find(&key(n)) else {
            panic!("a page");
        };
        ix.retarget(at, page(address)).unwrap();
    }

    fn address_of(ix: &PageIndex<u8>, n: u8) -> u64 {
        match ix.find(&key(n)) {
            Found::Page { page, .. } => page.address,
            other => panic!("lost: {other:?}"),
        }
    }

    #[test]
    fn pages_rewritten_side_by_side_fold_into_one_range() {
        let mut ix = three();
        // Out of the window — fifteen units is the hole marker — and side
        // by side.
        rewrite(&mut ix, 1, 900);
        assert_eq!(ix.sub_headers().count(), 3, "isolated between two");
        rewrite(&mut ix, 2, 901);
        assert_eq!(ix.sub_headers().count(), 2, "the second folds in");
        let bases: Vec<u64> = ix.sub_headers().map(|h| h.base_block).collect();
        assert_eq!(bases, [10, 900]);
        assert_eq!([0, 1, 2].map(|n| address_of(&ix, n)), [10, 900, 901]);
    }

    #[test]
    fn pages_far_apart_stay_in_ranges_of_their_own() {
        let mut ix = three();
        rewrite(&mut ix, 1, 900);
        rewrite(&mut ix, 2, 2000);
        assert_eq!(ix.sub_headers().count(), 3);
        assert_eq!([0, 1, 2].map(|n| address_of(&ix, n)), [10, 900, 2000]);
    }

    #[test]
    fn a_fold_is_recorded_whole_and_the_range_it_took_gone() {
        let mut ix = three();
        ix.drain(usize::MAX);
        rewrite(&mut ix, 1, 900);
        rewrite(&mut ix, 2, 901);
        let (records, _) = ix.drain(usize::MAX);
        let gone = records
            .iter()
            .filter(|r| matches!(r, super::super::RangeRecord::Gone { .. }))
            .count();
        assert_eq!(gone, 1, "{records:?}");
    }
}
