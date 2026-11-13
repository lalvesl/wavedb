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
                self.merged(None, &cursor, hi.as_deref(), false, false, need)?;
                break;
            };
            if below(&cursor, Some(&start)) {
                let gap = min_hi(Some(&start), hi.as_deref());
                self.merged(None, &cursor, gap, false, false, need)?;
                cursor = start;
                continue;
            }
            let end = min_hi(next.as_deref(), hi.as_deref());
            let whole = cursor == start && end == next.as_deref();
            if let Some(key) =
                self.merged(Some(slot), &cursor, end, false, whole, need)?
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
                self.merged(None, &lo, cursor.as_deref(), true, false, need)?;
                break;
            };
            // Keys past this entry's coverage, below the cursor: no page.
            if let Some(next) = next.as_deref()
                && below(next, cursor.as_deref())
            {
                self.merged(None, next, cursor.as_deref(), true, false, need)?;
                cursor = Some(next.to_vec());
                continue;
            }
            let whole = !below(&start, Some(&lo)) && cursor == next;
            let from = if below(&start, Some(&lo)) {
                lo.clone()
            } else {
                start
            };
            if let Some(key) = self.merged(
                Some(slot),
                &from,
                cursor.as_deref(),
                true,
                whole,
                need,
            )? {
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
    /// count is [`known`](Self::known) within the skip, without reading it
    /// — until the walk holds `need`.
    /// The key of `slot` when its page is evicted, and nothing taken.
    fn merged(
        &mut self,
        slot: Option<Slot>,
        lo: &[u8],
        hi: Option<&[u8]>,
        rev: bool,
        whole: bool,
        need: usize,
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
        let placed = slot.zip(self.index).and_then(|(slot, index)| match index
            .at(
            slot,
        )? {
            Found::Page { page, .. } => Some((slot, index, page)),
            Found::Hole(_) | Found::Uncovered => None,
        });
        match placed {
            Some((slot, index, placed)) if placed.meta & EVICTED != 0 => {
                return Ok(index.key_of(slot));
            }
            Some((_, _, placed)) => {
                let store = self.store;
                let run = run_of(placed);
                store.touch(self.idx, run.start);
                let slot = &store.types[self.idx];
                with_page(&store.file, run, slot.struct_hash(), |p| {
                    self.take(p.iter().collect(), lo, hi, rev, need);
                })?;
            }
            None => self.take(Vec::new(), lo, hi, rev, need),
        }
        Ok(None)
    }

    /// `page`'s entries in `[lo, hi)` with the cache's over them, in the
    /// walk's order, passing what is still to skip and holding until `need`
    /// are: nothing past that is looked up or copied — a walk for the
    /// lane's last entry meets one, not the whole unsettled range.
    fn take(
        &mut self,
        page: Vec<(&[u8], &[u8])>,
        lo: &[u8],
        hi: Option<&[u8]>,
        rev: bool,
        need: usize,
    ) {
        let inside = |key: &[u8]| !below(key, Some(lo)) && below(key, hi);
        let store = self.store;
        let cache = store.cache.borrow();
        let over = cache
            .ordered(self.idx, bounds(lo, hi))
            .filter(|(k, _)| inside(k));
        let page = page.into_iter().filter(|(k, _)| inside(k));
        if rev {
            self.hold(
                Merge::new(page.rev(), over.rev(), Ordering::Greater),
                need,
            );
        } else {
            self.hold(Merge::new(page, over, Ordering::Less), need);
        }
    }

    /// Pass what is still to skip of `entries`, then hold them until `need`.
    fn hold<'e>(
        &mut self,
        mut entries: impl Iterator<Item = (&'e [u8], Value<'e>)>,
        need: usize,
    ) {
        while self.skip > 0 || self.out.len() < need {
            let Some((key, value)) = entries.next() else {
                return;
            };
            if self.skip > 0 {
                self.skip -= 1;
                self.skipped += 1;
                continue;
            }
            let value = match value {
                Value::Page(bytes) => bytes.to_vec(),
                Value::Cached(bytes) => bytes,
            };
            self.out.push((key.to_vec(), value));
        }
    }
}

/// An entry's bytes, as the merge found them.
enum Value<'a> {
    /// Its page's, borrowed.
    Page(&'a [u8]),
    /// The cache's newest.
    Cached(Vec<u8>),
}

/// A page's entries with the cache's over them, both in the walk's order:
/// a hit replaces or adds, a removal drops — lazily, one entry a step.
struct Merge<'a, P, O>
where
    P: Iterator<Item = (&'a [u8], &'a [u8])>,
    O: Iterator<Item = (&'a [u8], Lookup)>,
{
    page: std::iter::Peekable<P>,
    over: O,
    /// How a key ahead of another compares to it: `Less` ascending.
    ahead: Ordering,
    /// The cache's next key, not yet passed.
    held: Option<(&'a [u8], Lookup)>,
}

impl<'a, P, O> Merge<'a, P, O>
where
    P: Iterator<Item = (&'a [u8], &'a [u8])>,
    O: Iterator<Item = (&'a [u8], Lookup)>,
{
    fn new(page: P, over: O, ahead: Ordering) -> Self {
        Self {
            page: page.peekable(),
            over,
            ahead,
            held: None,
        }
    }
}

impl<'a, P, O> Iterator for Merge<'a, P, O>
where
    P: Iterator<Item = (&'a [u8], &'a [u8])>,
    O: Iterator<Item = (&'a [u8], Lookup)>,
{
    type Item = (&'a [u8], Value<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.held.is_none() {
                // Not cached after all: the page is the truth for it.
                self.held =
                    self.over.by_ref().find(|(_, l)| *l != Lookup::Miss);
            }
            let Some(&(key, _)) = self.held.as_ref() else {
                return self.page.next().map(|(k, v)| (k, Value::Page(v)));
            };
            match self.page.peek().map(|&(k, _)| k.cmp(key)) {
                Some(order) if order == self.ahead => {
                    return self.page.next().map(|(k, v)| (k, Value::Page(v)));
                }
                Some(Ordering::Equal) => {
                    self.page.next();
                }
                _ => {}
            }
            if let Some((key, Lookup::Hit(bytes))) = self.held.take() {
                return Some((key, Value::Cached(bytes)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{Merge, Value};
    use crate::record_cache::Lookup;

    fn run(rev: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
        let page: Vec<(&[u8], &[u8])> =
            vec![(b"a", b"1"), (b"b", b"1"), (b"c", b"1"), (b"e", b"1")];
        let over: Vec<(&[u8], Lookup)> = vec![
            (b"b", Lookup::Removed),
            (b"c", Lookup::Hit(b"2".to_vec())),
            (b"d", Lookup::Hit(b"2".to_vec())),
            (b"e", Lookup::Miss),
        ];
        let take = |(k, v): (&[u8], Value<'_>)| {
            let v = match v {
                Value::Page(b) => b.to_vec(),
                Value::Cached(b) => b,
            };
            (k.to_vec(), v)
        };
        if rev {
            let it = page.into_iter().rev();
            Merge::new(it, over.into_iter().rev(), Ordering::Greater)
                .map(take)
                .collect()
        } else {
            Merge::new(page.into_iter(), over.into_iter(), Ordering::Less)
                .map(take)
                .collect()
        }
    }

    fn entry(k: &[u8], v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (k.to_vec(), v.to_vec())
    }

    #[test]
    fn the_cache_replaces_adds_and_removes_over_the_page_either_way() {
        let forth = vec![
            entry(b"a", b"1"),
            entry(b"c", b"2"),
            entry(b"d", b"2"),
            entry(b"e", b"1"),
        ];
        assert_eq!(run(false), forth);
        let mut back = forth;
        back.reverse();
        assert_eq!(run(true), back);
    }
}
