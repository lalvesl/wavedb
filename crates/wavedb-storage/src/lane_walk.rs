//! One walk of an ordered lane ([RFC 0078] §7) — split from
//! [`lane_read`](crate::lane_read), which starts and finishes it, for the
//! file budget.
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::cmp::Ordering;

use wavedb_core::store::{Entry, Span};

use crate::error::StorageResult;
use crate::ordered::key::cmp_padded;
use crate::ordered::{Found, Slot};
use crate::page_store::PageStore;
use crate::pages::{EVICTED, Pages, coverage, run_of, with_page};
use crate::record_cache::Lookup;

/// The bounds `[lo, hi)`, `None` being past every key.
pub fn bounds<'a>(
    lo: &'a [u8],
    hi: Option<&'a [u8]>,
) -> (std::ops::Bound<&'a [u8]>, std::ops::Bound<&'a [u8]>) {
    (
        std::ops::Bound::Included(lo),
        hi.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
    )
}

/// `a < b`, padded as the index compares keys; `None` is past every key.
pub fn below(a: &[u8], b: Option<&[u8]>) -> bool {
    b.is_none_or(|b| cmp_padded(a, b) == Ordering::Less)
}

/// The lesser of two upper bounds, `None` being past every key.
fn min_hi<'a>(a: Option<&'a [u8]>, b: Option<&'a [u8]>) -> Option<&'a [u8]> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if below(a, Some(b)) { a } else { b }),
        (a, None) => a,
        (None, b) => b,
    }
}

/// One walk's state.
pub struct Walk<'a> {
    pub store: &'a PageStore,
    pub idx: usize,
    pub index: Option<&'a Pages>,
    pub span: &'a Span,
    /// Entries still to pass before any is held.
    pub skip: u64,
    /// Entries passed so far.
    pub skipped: u64,
    pub out: Vec<Entry>,
}

impl Walk<'_> {
    /// Ascending, from `span.lo`. The key of an evicted page met.
    pub fn forth(&mut self, need: usize) -> StorageResult<Option<Vec<u8>>> {
        let hi = self.span.hi.clone();
        let mut cursor = self.span.lo.clone();
        let mut at = self.index.and_then(|ix| ix.slot_from(&cursor));
        while self.out.len() < need && below(&cursor, hi.as_deref()) {
            let Some((slot, start, next)) = self.cover(at) else {
                self.merged(None, &cursor, hi.as_deref(), false, false)?;
                break;
            };
            if below(&cursor, Some(&start)) {
                let gap = min_hi(Some(&start), hi.as_deref());
                self.merged(None, &cursor, gap, false, false)?;
                cursor = start;
                continue;
            }
            let end = min_hi(next.as_deref(), hi.as_deref());
            let whole = cursor == start && end == next.as_deref();
            if let Some(key) =
                self.merged(Some(slot), &cursor, end, false, whole)?
            {
                return Ok(Some(key));
            }
            match end {
                Some(end) => cursor = end.to_vec(),
                None => break,
            }
            at = self.index.and_then(|ix| ix.next(slot));
        }
        Ok(None)
    }

    /// Descending, from `span.hi`. The key of an evicted page met.
    pub fn back(&mut self, need: usize) -> StorageResult<Option<Vec<u8>>> {
        let lo = self.span.lo.clone();
        let mut cursor = self.span.hi.clone();
        let mut at = self.index.and_then(|ix| {
            cursor.as_deref().map_or_else(
                || ix.last(),
                |hi| match ix.slot_from(hi) {
                    Some(s)
                        if ix
                            .key_of(s)
                            .is_some_and(|k| below(&k, Some(hi))) =>
                    {
                        Some(s)
                    }
                    Some(s) => ix.prev(s),
                    None => ix.last(),
                },
            )
        });
        while self.out.len() < need && below(&lo, cursor.as_deref()) {
            let Some((slot, start, next)) = self.cover(at) else {
                self.merged(None, &lo, cursor.as_deref(), true, false)?;
                break;
            };
            // Keys past this entry's coverage, below the cursor: no page.
            if let Some(next) = next.as_deref()
                && below(next, cursor.as_deref())
            {
                self.merged(None, next, cursor.as_deref(), true, false)?;
                cursor = Some(next.to_vec());
                continue;
            }
            let whole = !below(&start, Some(&lo)) && cursor == next;
            let from = if below(&start, Some(&lo)) {
                lo.clone()
            } else {
                start
            };
            if let Some(key) =
                self.merged(Some(slot), &from, cursor.as_deref(), true, whole)?
            {
                return Ok(Some(key));
            }
            if !below(&lo, Some(&from)) {
                break;
            }
            cursor = Some(from);
            at = self.index.and_then(|ix| ix.prev(slot));
        }
        Ok(None)
    }

    /// `at` with its coverage `[start, next)`.
    fn cover(
        &self,
        at: Option<Slot>,
    ) -> Option<(Slot, Vec<u8>, Option<Vec<u8>>)> {
        let slot = at?;
        let (start, next) = coverage(self.index?, slot)?;
        Some((slot, start, next))
    }

    /// How many entries `[lo, hi)` — `slot`'s whole coverage — holds, when
    /// that is known without reading its page: a hole's none, or a counted
    /// page's count, and the cache holding nothing there unsettled.
    fn known(
        &self,
        slot: Option<Slot>,
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Option<u64> {
        let (slot, index) = slot.zip(self.index)?;
        let n = match index.at(slot)? {
            Found::Page { .. } => index.count_at(slot)?,
            Found::Hole(_) => 0,
            Found::Uncovered => return None,
        };
        let cache = self.store.cache.borrow();
        cache.settled_in(self.idx, bounds(lo, hi)).then_some(n)
    }

    /// Take the entries in `[lo, hi)` — `slot`'s page's, when it has one,
    /// with the cache's over them — in the walk's order, passing what is
    /// still to skip; when the range is the page's `whole` coverage and its
    /// count is [`known`](Self::known) within the skip, without reading it.
    /// The key of `slot` when its page is evicted, and nothing taken.
    fn merged(
        &mut self,
        slot: Option<Slot>,
        lo: &[u8],
        hi: Option<&[u8]>,
        rev: bool,
        whole: bool,
    ) -> StorageResult<Option<Vec<u8>>> {
        if whole
            && self.skip > 0
            && let Some(n) = self.known(slot, lo, hi)
            && n <= self.skip
        {
            self.skip -= n;
            self.skipped += n;
            return Ok(None);
        }
        let inside = |key: &[u8]| !below(key, Some(lo)) && below(key, hi);
        let placed = slot.zip(self.index).and_then(|(slot, index)| match index
            .at(
            slot,
        )? {
            Found::Page { page, .. } => Some((slot, index, page)),
            Found::Hole(_) | Found::Uncovered => None,
        });
        let page: Vec<Entry> = match placed {
            Some((slot, index, placed)) if placed.meta & EVICTED != 0 => {
                return Ok(index.key_of(slot));
            }
            Some((_, _, placed)) => {
                let run = run_of(placed);
                self.store.touch(self.idx, run.start);
                let dict = self.store.types[self.idx].dictionary().borrow();
                with_page(&self.store.file, run, &dict, |p| {
                    p.iter()
                        .filter(|(k, _)| inside(k))
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .collect()
                })?
            }
            None => Vec::new(),
        };
        let cache = self.store.cache.borrow();
        let over = cache
            .ordered(self.idx, bounds(lo, hi))
            .filter(|(k, _)| inside(k));
        let mut merged = merge(page, over);
        if rev {
            merged.reverse();
        }
        let pass = usize::try_from(self.skip)
            .unwrap_or(usize::MAX)
            .min(merged.len());
        self.skip -= pass as u64;
        self.skipped += pass as u64;
        self.out.extend(merged.into_iter().skip(pass));
        Ok(None)
    }
}

/// `page`'s entries with the cache's `over` them, both ascending: a hit
/// replaces or adds, a removal drops.
fn merge<'a>(
    page: Vec<Entry>,
    over: impl Iterator<Item = (&'a [u8], Lookup)>,
) -> Vec<Entry> {
    let mut out = Vec::with_capacity(page.len());
    let mut page = page.into_iter().peekable();
    for (key, lookup) in over {
        // Not cached after all: the page is the truth for it.
        if lookup == Lookup::Miss {
            continue;
        }
        while let Some((k, _)) = page.peek()
            && k.as_slice() < key
        {
            out.extend(page.next());
        }
        if page.peek().is_some_and(|(k, _)| k.as_slice() == key) {
            page.next();
        }
        if let Lookup::Hit(value) = lookup {
            out.push((key.to_vec(), value));
        }
    }
    out.extend(page);
    out
}
