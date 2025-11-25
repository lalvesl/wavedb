//! Reshaping ranges: a page cut in two, a quantum refined, a page given a
//! range of its own ([RFC 0078] §4). All of it in RAM, in the round that
//! writes the pages.
//!
//! Every reshape keeps one rule: a range boundary is an entry boundary, so a
//! page is named by exactly one entry. Quanta a reshape leaves with no page
//! of their own become **holes** — they hold no key, which is why the page
//! that covered them could give them up.
//!
//! [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::cmp::Ordering;

use super::head::{MAX_SHIFT, QUANTA, SubHeader, q_of, with_q};
use super::index::{Entries, PageIndex, Placed, Range, Slot};
use super::key::cmp_padded;

/// The keys around a cut of one page: its first and last, and the two the
/// cut falls between.
#[derive(Debug, Clone, Copy)]
pub struct Cut<'a> {
    pub first: &'a [u8],
    pub left_last: &'a [u8],
    pub right_first: &'a [u8],
    pub last: &'a [u8],
}

/// What became of a cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutOutcome {
    /// The right half's page has its entry, here.
    Cut(Slot),
    /// The two keys share a quantum, but the page's keys do not all: cut at
    /// a quantum boundary between them instead
    /// ([`PageIndex::quantum_of`] finds one). The page is at this slot now —
    /// a refinement may have moved it before the cut gave up.
    Elsewhere(Slot),
    /// The two keys are one key to the index (equal padded with zeros), or
    /// a key lies outside the page's range.
    Inseparable,
}

impl<M: Copy + Default> PageIndex<M> {
    /// Cut `at`'s page between `keys.left_last` and `keys.right_first`, the
    /// right half going to `right`. The cut lands on the quantum boundary
    /// below `right_first`; when the whole page lies in one quantum, the
    /// quantum is refined — finer, then deeper — until the two keys part.
    pub fn cut(
        &mut self,
        at: Slot,
        keys: &Cut<'_>,
        right: Placed<M>,
    ) -> CutOutcome {
        if cmp_padded(keys.left_last, keys.right_first) != Ordering::Less {
            return CutOutcome::Inseparable;
        }
        let mut at = at;
        loop {
            let quanta =
                [keys.first, keys.left_last, keys.right_first, keys.last]
                    .map(|k| self.quantum_of(at, k));
            let [Some(first), Some(left), Some(right_q), Some(last)] = quanta
            else {
                return CutOutcome::Inseparable;
            };
            if left < right_q {
                return self
                    .insert_after(at, right_q, right)
                    .map_or(CutOutcome::Inseparable, CutOutcome::Cut);
            }
            if first != left || right_q != last {
                return CutOutcome::Elsewhere(at);
            }
            match self.refine(at, left) {
                Some(moved) => at = moved,
                None => return CutOutcome::Inseparable,
            }
        }
    }

    /// A new entry for `page` right after `at`, starting at quantum `q`.
    fn insert_after(
        &mut self,
        at: Slot,
        q: u16,
        page: Placed<M>,
    ) -> Option<Slot> {
        let r = self.ranges.get_mut(at.range)?;
        let slot = Slot {
            range: at.range,
            entry: at.entry + 1,
        };
        if let Some(entry) = r.head.encode(q, page.address, page.blocks) {
            r.entries.insert(slot.entry, entry, page.meta);
            self.note_whole(at.range);
            return Some(slot);
        }
        let hole = r.head.hole(q);
        r.entries.insert(slot.entry, hole, M::default());
        self.isolate(slot, page)
    }

    /// Give quantum `q` of `at`'s range a range of its own holding `at`'s
    /// page alone — the page's keys all lie in `q`. The new range quantizes
    /// 16 bits finer, or, at a shift of 0, starts 8 bytes deeper. The page's
    /// other quanta become holes on either side. Returns the page's slot.
    fn refine(&mut self, at: Slot, q: u16) -> Option<Slot> {
        let Range { head, entries } = self.ranges.get(at.range)?;
        let shift = head.shape.key_shift();
        let start = head.quantum_start(u32::from(q))?;
        let middle_head = if shift > 0 {
            SubHeader {
                first_key: start,
                prefix: head.prefix.clone(),
                shape: head.shape.with_shift(shift.saturating_sub(16)),
                ..head.clone()
            }
        } else {
            let mut prefix = head.prefix.to_vec();
            prefix.extend_from_slice(&start.to_be_bytes());
            SubHeader {
                first_key: 0,
                prefix: prefix.into(),
                shape: head.shape.with_shift(MAX_SHIFT),
                ..head.clone()
            }
        };
        let page = entries.pages[at.entry];
        let middle = Range {
            head: middle_head,
            entries: Entries::one(with_q(page, 0), entries.meta[at.entry]),
        };
        let own = u32::from(q_of(page));
        let end = entries.end_of(at.entry);
        let mut left = entries.head(at.entry);
        if own < u32::from(q) {
            left.insert(left.len(), head.hole(q_of(page)), M::default());
        }
        let after = u32::from(q) + 1;
        let right = self.right_of(at, after, end);
        Some(self.splice(at.range, left, middle, right))
    }

    /// Give `at` — a page or a hole — a range of its own pointing at `page`,
    /// based at the page's address and with a size unit large enough for it.
    /// The range covers the entry's quanta; the entries on either side keep
    /// the old range's shape and base. Returns the page's slot.
    pub(super) fn isolate(
        &mut self,
        at: Slot,
        page: Placed<M>,
    ) -> Option<Slot> {
        let Range { head, entries } = self.ranges.get(at.range)?;
        let own = u32::from(q_of(*entries.pages.get(at.entry)?));
        let end = entries.end_of(at.entry);
        let middle_head = SubHeader {
            base_block: page.address,
            first_key: head.quantum_start(own)?,
            prefix: head.prefix.clone(),
            shape: head.shape.sized_for(page.blocks)?,
        };
        let entry = middle_head.encode(0, page.address, page.blocks)?;
        let middle = Range {
            head: middle_head,
            entries: Entries::one(entry, page.meta),
        };
        let left = entries.head(at.entry);
        let right = self.right_of(at, end, end);
        Some(self.splice(at.range, left, middle, right))
    }

    /// The part of `at`'s range from quantum `from` on: a hole up to `end`
    /// (where `at`'s entry stopped covering) when `from` is short of it,
    /// then the entries after `at`, moved down by `from`. `None` when that
    /// leaves nothing, or `from` is past the end of keys.
    ///
    /// A part holding only a hole is kept: a key arriving there later fills
    /// it with this range's quantization, where a range opened afresh could
    /// only guess one.
    ///
    /// A range's quanta run on past the next range's start — a key belongs
    /// to the last range starting at or before it — so a part holding only
    /// a hole is dropped when it would start where the next range already
    /// does: its quanta are that range's.
    fn right_of(&self, at: Slot, from: u32, end: u32) -> Option<Range<M>> {
        let Range { head, entries } = &self.ranges[at.range];
        let tail = at.entry + 1 < entries.len();
        if from >= QUANTA || (from >= end && !tail) {
            return None;
        }
        let right_head = SubHeader {
            first_key: head.quantum_start(from)?,
            ..head.clone()
        };
        let shadowed = self.ranges.get(at.range + 1).is_some_and(|next| {
            next.head.start_cmp(&right_head.start()) != Ordering::Greater
        });
        if !tail && shadowed {
            return None;
        }
        let mut right = entries.tail(at.entry + 1, from);
        if from < end {
            right.insert(0, right_head.hole(0), M::default());
        }
        Some(Range {
            head: right_head,
            entries: right,
        })
    }

    /// Replace range `i` with `left` (unless empty), `middle` and `right`;
    /// the slot of `middle`'s one entry.
    fn splice(
        &mut self,
        i: usize,
        left: Entries<M>,
        middle: Range<M>,
        right: Option<Range<M>>,
    ) -> Slot {
        let head = self.ranges[i].head.clone();
        let mut pieces = Vec::with_capacity(3);
        if left.len() > 0 {
            pieces.push(Range {
                head,
                entries: left,
            });
        }
        let range = i + pieces.len();
        pieces.push(middle);
        pieces.extend(right);
        let made = pieces.len();
        self.ranges.splice(i..=i, pieces);
        for piece in i..i + made {
            self.note_whole(piece);
        }
        Slot { range, entry: 0 }
    }
}
