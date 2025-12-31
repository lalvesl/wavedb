//! Folding two neighbouring ranges into one ([RFC 0078] §4, §12): no page
//! moves, only the index — a page given a range of its own because its
//! range could not address it is folded into the range beside it when one
//! sub-header can name both. Pages a round writes lie side by side, so the
//! two halves of a cut, and neighbours in key order rewritten together,
//! fold back as they are isolated.
//!
//! A fold keeps every entry's quantum, so every key finds the page it
//! found before: two ranges fold only under one prefix, which quantizes
//! them alike.
//!
//! [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use super::access::MAX_RANGE_ENTRIES;
use super::head::{Shape, SubHeader, q_of};
use super::index::{Entries, PageIndex, Range, Slot};

impl<M: Copy + Default> PageIndex<M> {
    /// Fold `at`'s range with the ranges on either side for as long as it
    /// can — a regroup's pages arrive one by one, and the piece of an old
    /// range left between two of them may hold only a hole; where `at`'s
    /// entry is now.
    pub(super) fn fold_around(&mut self, at: Slot) -> Slot {
        while self.fold(at.range).is_some() {}
        let mut at = at;
        while let Some(left) = at.range.checked_sub(1)
            && let Some(offset) = self.fold(left)
        {
            at = Slot {
                range: left,
                entry: offset + at.entry,
            };
        }
        at
    }

    /// Runs of neighbouring ranges worth rewriting side by side, so the
    /// fold makes each one range ([RFC 0078] §12): consecutive ranges of one
    /// prefix and quantization, each holding fewer than [`SCATTERED`]
    /// pages, as many as one window can address — its blocks, at most
    /// [`MAX_RANGE_ENTRIES`] entries and `most` pages — and at least two.
    /// Each run named by its pages' keys, in key order.
    ///
    /// [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md
    #[must_use]
    pub fn scattered(&self, most: usize) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut run = Stretch::default();
        for (i, r) in self.ranges.iter().enumerate() {
            let pages: Vec<(Vec<u8>, u64)> = (0..r.entries.len())
                .filter_map(|entry| {
                    let (_, blocks) = r.head.decode(r.entries.pages[entry])?;
                    Some((self.key_of(Slot { range: i, entry })?, blocks))
                })
                .collect();
            if pages.len() >= SCATTERED {
                run.close(&mut out);
                continue;
            }
            let blocks: u64 = pages.iter().map(|p| p.1).sum();
            let joins = run.head.is_some_and(|h| {
                let (a, b) = (h.shape, r.head.shape);
                h.prefix == r.head.prefix
                    && h.prefix_bits == r.head.prefix_bits
                    && a.address_bits() == b.address_bits()
                    && a.address_factor() == b.address_factor()
                    && run.blocks + blocks <= a.window()
                    && run.entries + r.entries.len() <= MAX_RANGE_ENTRIES
                    && run.keys.len() + pages.len() <= most
            });
            if !joins {
                run.close(&mut out);
            }
            run.head = Some(&r.head);
            run.ranges += 1;
            run.blocks += blocks;
            run.entries += r.entries.len();
            run.keys.extend(pages.into_iter().map(|p| p.0));
        }
        run.close(&mut out);
        out
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
    /// in it: the same prefix — so the same quanta — based at the lowest
    /// page, the larger size unit, every page addressable from there.
    ///
    /// The first range's last entry covered every quantum up to the
    /// second's start, and the folded range reaches as far as the second
    /// did — to the end of their prefix, or the range after — so every key
    /// finds the entry it found before.
    fn folded(&self, i: usize) -> Option<(Range<M>, usize)> {
        let (a, b) = (self.ranges.get(i)?, self.ranges.get(i + 1)?);
        let (ha, hb) = (&a.head, &b.head);
        let (sa, sb) = (ha.shape, hb.shape);
        let len = a.entries.len() + b.entries.len();
        if ha.prefix_bits != hb.prefix_bits
            || ha.prefix != hb.prefix
            || sa.address_bits() != sb.address_bits()
            || sa.address_factor() != sb.address_factor()
            || len > MAX_RANGE_ENTRIES
        {
            return None;
        }
        let shape = Shape::new(
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
            shape,
            ..ha.clone()
        };
        let mut out = Entries {
            pages: Vec::with_capacity(len),
            meta: Vec::with_capacity(len),
            counts: self.counted.then(|| Vec::with_capacity(len)),
        };
        moved(&head, a, &mut out)?;
        let offset = out.len();
        moved(&head, b, &mut out)?;
        let ascending = out.pages.windows(2).all(|w| q_of(w[0]) < q_of(w[1]));
        ascending.then_some((Range { head, entries: out }, offset))
    }
}

/// Pages a range holds from which it is not scattered: its sub-header
/// costs about what eight entries do.
pub const SCATTERED: usize = 8;

/// A run of ranges [`scattered`](PageIndex::scattered) is gathering.
#[derive(Default)]
struct Stretch<'a> {
    head: Option<&'a SubHeader>,
    ranges: usize,
    blocks: u64,
    entries: usize,
    keys: Vec<Vec<u8>>,
}

impl Stretch<'_> {
    /// Hand the run over if it folds two ranges or more, and start afresh.
    fn close(&mut self, out: &mut Vec<Vec<Vec<u8>>>) {
        let run = std::mem::take(self);
        if run.ranges >= 2 && !run.keys.is_empty() {
            out.push(run.keys);
        }
    }
}

/// `range`'s entries, under `head`, appended to `out`.
fn moved<M: Copy>(
    head: &SubHeader,
    range: &Range<M>,
    out: &mut Entries<M>,
) -> Option<()> {
    for (i, &entry) in range.entries.pages.iter().enumerate() {
        let q = q_of(entry);
        let encoded = match range.head.decode(entry) {
            Some((address, blocks)) => head.encode(q, address, blocks)?,
            None => head.hole(q),
        };
        let count = range.entries.count(i).unwrap_or(0);
        push(out, encoded, range.entries.meta[i], count);
    }
    Some(())
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
        let mut ix = PageIndex::new(Shape::new(4, 1, 1).unwrap());
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

    #[test]
    fn scattered_ranges_are_named_as_one_run_in_key_order() {
        let mut ix = three();
        rewrite(&mut ix, 1, 900);
        rewrite(&mut ix, 2, 2000);
        assert_eq!(ix.sub_headers().count(), 3);
        let runs = ix.scattered(128);
        // A page is named by its quantum's first key, the prefix and 16 bits.
        let want: Vec<Vec<u8>> = (0..3).map(|n| key(n)[..2].to_vec()).collect();
        assert_eq!(runs, std::slice::from_ref(&want));
        // A round moves two pages at most: the run stops short.
        assert_eq!(ix.scattered(2), [want[..2].to_vec()]);
    }

    #[test]
    fn moved_side_by_side_a_run_folds_into_one_range() {
        let mut ix = three();
        rewrite(&mut ix, 1, 900);
        rewrite(&mut ix, 2, 2000);
        for (n, address) in [(0, 5000), (1, 5001), (2, 5002)] {
            rewrite(&mut ix, n, address);
        }
        assert_eq!(ix.sub_headers().count(), 1);
        assert_eq!([0, 1, 2].map(|n| address_of(&ix, n)), [5000, 5001, 5002]);
        assert_eq!(ix.scattered(128), Vec::<Vec<Vec<u8>>>::new());
    }
}
