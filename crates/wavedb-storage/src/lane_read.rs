//! Reading an ordered lane ([RFC 0078] §7) — the entries of a span, in
//! either order, from the lane's pages with the record cache's keys merged
//! over them.
//!
//! The pages are walked from the span's edge, one entry's coverage at a
//! time. Coverages are contiguous — an entry covers up to where the next one
//! starts — so the keys between two pages' records are only ever in the
//! cache, which the cache's ordered keys of the lane answer. A walk stops
//! once it holds what the span skips and returns, so a read near an edge
//! reads the pages there and no others.
//!
//! An evicted page is brought back first, as a write would: the walk stops
//! at it, the page is restored, and the walk starts over.
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

/// What one synchronous walk came to.
pub enum Walked {
    /// The span's entries, skipped and limited.
    Done(Vec<Entry>),
    /// It met the evicted page of slot `idx` that `key` finds.
    Evicted { idx: usize, key: Vec<u8> },
}

/// `a < b`, padded as the index compares keys; `None` is past every key.
fn below(a: &[u8], b: Option<&[u8]>) -> bool {
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

impl PageStore {
    /// The synchronous half of a scan of the ordered lane `lane`: the
    /// record cache and the pages on disk — all of it, on a disk-only
    /// store, so its scan is ready when it is called.
    ///
    /// # Errors
    /// A page read or decode fault.
    pub(crate) fn scan_now(
        &self,
        lane: u64,
        span: &Span,
    ) -> StorageResult<Walked> {
        let Some(idx) = self.slot_index(lane) else {
            return Ok(Walked::Done(Vec::new()));
        };
        if !below(&span.lo, span.hi.as_deref()) {
            return Ok(Walked::Done(Vec::new()));
        }
        let skip = usize::try_from(span.skip).unwrap_or(usize::MAX);
        Ok(
            match self.walk(idx, span, skip.saturating_add(span.limit))? {
                Walked::Done(entries) => Walked::Done(
                    entries.into_iter().skip(skip).take(span.limit).collect(),
                ),
                evicted @ Walked::Evicted { .. } => evicted,
            },
        )
    }

    /// Finish a scan [`scan_now`](Self::scan_now) began: each evicted page
    /// it meets restored, and the walk made again.
    ///
    /// # Errors
    /// As [`scan_now`](Self::scan_now); an object-store fault restoring a
    /// page.
    pub(crate) async fn scan_rest(
        &self,
        lane: u64,
        span: &Span,
        mut now: StorageResult<Walked>,
    ) -> StorageResult<Vec<Entry>> {
        loop {
            match now? {
                Walked::Done(entries) => return Ok(entries),
                Walked::Evicted { idx, key } => {
                    self.restore(idx, &key).await?;
                }
            }
            now = self.scan_now(lane, span);
        }
    }

    /// The span reading every key in `[lo, hi)`, which a count walks.
    pub(crate) fn counted(lo: &[u8], hi: Option<&[u8]>) -> Span {
        Span {
            lo: lo.to_vec(),
            hi: hi.map(<[u8]>::to_vec),
            rev: false,
            skip: 0,
            limit: usize::MAX,
        }
    }

    /// Walk slot `idx`'s pages along `span` until `need` entries are held.
    fn walk(
        &self,
        idx: usize,
        span: &Span,
        need: usize,
    ) -> StorageResult<Walked> {
        let index = self.types[idx].directory().borrow();
        let mut walk = Walk {
            store: self,
            idx,
            index: index.as_ref(),
            span,
            out: Vec::new(),
        };
        let evicted = if span.rev {
            walk.back(need)?
        } else {
            walk.forth(need)?
        };
        Ok(match evicted {
            Some(key) => Walked::Evicted { idx, key },
            None => Walked::Done(walk.out),
        })
    }
}

/// One walk's state.
struct Walk<'a> {
    store: &'a PageStore,
    idx: usize,
    index: Option<&'a Pages>,
    span: &'a Span,
    out: Vec<Entry>,
}

impl Walk<'_> {
    /// Ascending, from `span.lo`. The key of an evicted page met.
    fn forth(&mut self, need: usize) -> StorageResult<Option<Vec<u8>>> {
        let hi = self.span.hi.clone();
        let mut cursor = self.span.lo.clone();
        let mut at = self.index.and_then(|ix| ix.slot_from(&cursor));
        while self.out.len() < need && below(&cursor, hi.as_deref()) {
            let Some((slot, start, next)) = self.cover(at) else {
                self.merged(None, &cursor, hi.as_deref(), false)?;
                break;
            };
            if below(&cursor, Some(&start)) {
                let gap = min_hi(Some(&start), hi.as_deref());
                self.merged(None, &cursor, gap, false)?;
                cursor = start;
                continue;
            }
            let end = min_hi(next.as_deref(), hi.as_deref());
            if let Some(key) = self.merged(Some(slot), &cursor, end, false)? {
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
    fn back(&mut self, need: usize) -> StorageResult<Option<Vec<u8>>> {
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
                self.merged(None, &lo, cursor.as_deref(), true)?;
                break;
            };
            // Keys past this entry's coverage, below the cursor: no page.
            if let Some(next) = next.as_deref()
                && below(next, cursor.as_deref())
            {
                self.merged(None, next, cursor.as_deref(), true)?;
                cursor = Some(next.to_vec());
                continue;
            }
            let from = if below(&start, Some(&lo)) {
                lo.clone()
            } else {
                start
            };
            if let Some(key) =
                self.merged(Some(slot), &from, cursor.as_deref(), true)?
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

    /// Append the entries in `[lo, hi)` — `slot`'s page's, when it has one,
    /// with the cache's over them — in the walk's order. The key of `slot`
    /// when its page is evicted, and nothing appended.
    fn merged(
        &mut self,
        slot: Option<Slot>,
        lo: &[u8],
        hi: Option<&[u8]>,
        rev: bool,
    ) -> StorageResult<Option<Vec<u8>>> {
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
        let bounds = (
            std::ops::Bound::Included(lo),
            hi.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
        );
        let over = cache.ordered(self.idx, bounds).filter(|(k, _)| inside(k));
        let mut merged = merge(page, over);
        if rev {
            merged.reverse();
        }
        self.out.extend(merged);
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
