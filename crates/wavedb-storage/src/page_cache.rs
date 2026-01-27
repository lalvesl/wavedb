//! The page cache ([RFC 0044]) — settled page images held in RAM.
//!
//! ## What is cached, and why that key
//!
//! The key is the **run** — the block extent an image occupies — and the value
//! is its bytes exactly as they sit on disk, **still compressed**. Both
//! choices fall out of the engine rather than being tuning:
//!
//! - Page writes are copy-on-write: a rewritten page is placed in a *new* run
//!   and the descriptor is repointed ([RFC 0041]). A run's bytes are therefore
//!   immutable for as long as that run is allocated, so a cache keyed by run
//!   **never goes stale while its run is allocated**. An entry leaves through
//!   [`invalidate`](PageCache::invalidate) at the two points where that stops
//!   holding: when a round supersedes its run (no directory names it any
//!   more, so its image is dead weight), and when a write lands on a
//!   recycled extent.
//! - Compressed is the form the disk holds and the form a reader must
//!   decompress from anyway, so storing it uncompressed would cache *fewer*
//!   pages for the same RAM and save nothing on the read path.
//!
//! ## Lent, not handed out
//!
//! A hit is served by **lending** the image to the reader for as long as it
//! decodes ([`with`](PageCache::with)): no copy of the page, and no shared
//! pointer either — a reference count would have to be atomic for the store
//! to stay `Send`, and the store is used from one thread, so there is nothing
//! for one to protect. The reader decodes into its own buffers and lets go.
//!
//! ## Not cached
//!
//! Only page reads go through here. The dictionary image, the headers and the
//! defragmenter's relocation reads all want owned, mutable
//! buffers and are read straight from the file — caching them would hold bytes
//! that are read once.
//!
//! [RFC 0044]: ../../../rfcs/0044-page-cache-PLANNED-LOW.md
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};

use crate::block::Run;

/// Bytes of page images held by default.
///
/// Deliberately modest: this is a *second* copy of data the OS page cache may
/// also hold, and under a container memory limit both are charged to the same
/// budget. A node that wants more should say so rather than inherit a number
/// sized for someone else's machine.
pub const DEFAULT_BUDGET_BYTES: usize = 64 << 20;

struct Entry {
    /// Blocks the run spans, so an overlap test needs only the map.
    blocks: u64,
    /// Which insertion this is, so the queue can tell it from an earlier
    /// image of the same run.
    stamp: u64,
    bytes: Box<[u8]>,
}

#[derive(Default)]
struct Inner {
    /// Keyed by the run's first block. Runs are disjoint allocations, so the
    /// map's ordering makes "which entries overlap [start, end)?" a range
    /// query — which a `HashMap` could only answer by a full scan, and
    /// invalidation happens on every window write.
    pages: BTreeMap<u64, Entry>,
    /// Insertion order, for eviction, as `(start, stamp)`. An entry that
    /// left early (invalidated, or replaced by a newer image of its run)
    /// stays queued and is skipped when it comes up — its stamp no longer
    /// matches — and [`Inner::tidy`] drops such leftovers once they
    /// outnumber the live entries, so the queue stays proportional to what
    /// is held.
    fifo: VecDeque<(u64, u64)>,
    stamp: u64,
    bytes: usize,
}

/// Page images by run. Every method takes `&self`.
pub struct PageCache {
    inner: RefCell<Inner>,
    budget: usize,
}

impl PageCache {
    /// A cache holding at most `budget` bytes of images (`0` disables it).
    #[must_use]
    pub fn new(budget: usize) -> Self {
        Self {
            inner: RefCell::new(Inner::default()),
            budget,
        }
    }

    /// Lend `run`'s image to `read`, if held; `read` comes back unused on a
    /// miss. `read` must not touch the cache — it runs while the image is
    /// borrowed from it.
    pub fn with<R, F: FnOnce(&[u8]) -> R>(
        &self,
        run: Run,
        read: F,
    ) -> Result<R, F> {
        let inner = self.inner.borrow();
        match inner.pages.get(&run.start) {
            Some(e) if e.blocks == run.count => Ok(read(&e.bytes)),
            _ => Err(read),
        }
    }

    /// Hold `run`'s image, evicting oldest-first to stay inside the budget.
    ///
    /// A single image larger than the whole budget is simply not held: caching
    /// it would evict everything else to serve one page.
    // The guard deliberately spans insert *and* eviction: between the two the
    // cache is over budget and its byte count disagrees with its contents, and
    // no reader may observe that. Releasing early — the lint's suggestion —
    // would make the over-budget window visible.
    #[allow(clippy::significant_drop_tightening)]
    pub fn insert(&self, run: Run, bytes: Box<[u8]>) {
        if self.budget == 0 || bytes.len() > self.budget {
            return;
        }
        let mut inner = self.inner.borrow_mut();
        inner.drop_at(run.start);
        inner.stamp += 1;
        let stamp = inner.stamp;
        inner.bytes += bytes.len();
        inner.pages.insert(
            run.start,
            Entry {
                blocks: run.count,
                stamp,
                bytes,
            },
        );
        inner.fifo.push_back((run.start, stamp));
        while inner.bytes > self.budget {
            let Some((oldest, stamp)) = inner.fifo.pop_front() else {
                break; // nothing left to give back
            };
            if inner.pages.get(&oldest).is_some_and(|e| e.stamp == stamp) {
                inner.drop_at(oldest);
            }
        }
        inner.tidy();
    }

    /// Forget every image overlapping `run`.
    ///
    /// Called wherever a run stops being what the cache remembers: a round
    /// superseding it, and a write into it. Overlap rather than exact match,
    /// because one window write covers many cached runs.
    pub fn invalidate(&self, run: Run) {
        let mut inner = self.inner.borrow_mut();
        let end = run.end();
        let doomed: Vec<u64> = inner
            .pages
            .range(..end)
            .rev()
            .take_while(|(start, e)| **start + e.blocks > run.start)
            .map(|(start, _)| *start)
            .collect();
        for start in doomed {
            inner.drop_at(start);
        }
        inner.tidy();
    }

    /// The ceiling this cache was built with — what
    /// [`bytes`](Self::bytes) is kept under.
    #[must_use]
    pub const fn budget(&self) -> usize {
        self.budget
    }

    /// Bytes of images held.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.inner.borrow_mut().bytes
    }

    /// Images held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.borrow_mut().pages.len()
    }

    /// Whether the cache holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop everything (a reopen, or a test wanting a cold read).
    pub fn clear(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.pages.clear();
        inner.fifo.clear();
        inner.bytes = 0;
    }
}

impl Inner {
    /// Remove the entry starting at `start`, if any, keeping `bytes` true.
    fn drop_at(&mut self, start: u64) {
        if let Some(old) = self.pages.remove(&start) {
            self.bytes -= old.bytes.len();
        }
    }

    /// Drop the queue entries whose image has already left, once they
    /// outnumber the live ones — amortised O(1) per insert or invalidation.
    fn tidy(&mut self) {
        if self.fifo.len() <= 2 * self.pages.len() + 64 {
            return;
        }
        let pages = &self.pages;
        self.fifo.retain(|(start, stamp)| {
            pages.get(start).is_some_and(|e| e.stamp == *stamp)
        });
    }
}

/// Its size, never its contents. A derived `Debug` on a cache of page images
/// would dump the database into whatever printed the `BlockFile` holding it.
impl std::fmt::Debug for PageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.borrow_mut();
        f.debug_struct("PageCache")
            .field("pages", &inner.pages.len())
            .field("bytes", &inner.bytes)
            .field("budget", &self.budget)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_BUDGET_BYTES, PageCache};
    use crate::block::Run;
    fn image(len: usize, fill: u8) -> Box<[u8]> {
        vec![fill; len].into()
    }

    fn copy(cache: &PageCache, run: Run) -> Option<Vec<u8>> {
        cache.with(run, <[u8]>::to_vec).ok()
    }

    #[test]
    fn a_held_page_comes_back_by_its_run() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        let run = Run::new(8, 2);
        cache.insert(run, image(100, 7));
        assert_eq!(copy(&cache, run).as_deref(), Some(&[7u8; 100][..]));
        assert_eq!(cache.bytes(), 100);
    }

    /// The same start with a different length is a different extent, so it
    /// must miss rather than hand back the wrong number of bytes.
    #[test]
    fn a_run_of_another_length_is_a_miss() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        cache.insert(Run::new(8, 2), image(100, 7));
        assert!(copy(&cache, Run::new(8, 3)).is_none());
    }

    /// One window write covers many cached runs, so invalidation is by
    /// overlap. A cache that only matched exact runs would keep serving the
    /// bytes a recycled extent no longer holds.
    #[test]
    fn invalidating_a_window_forgets_every_page_inside_it() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        cache.insert(Run::new(10, 2), image(50, 1)); // 10..12
        cache.insert(Run::new(12, 1), image(50, 2)); // 12..13
        cache.insert(Run::new(13, 4), image(50, 3)); // 13..17
        cache.insert(Run::new(40, 1), image(50, 4)); // outside
        assert_eq!(cache.len(), 4);

        cache.invalidate(Run::new(11, 3)); // 11..14 — clips all three
        assert!(
            copy(&cache, Run::new(10, 2)).is_none(),
            "overlaps at its tail"
        );
        assert!(copy(&cache, Run::new(12, 1)).is_none(), "wholly inside");
        assert!(
            copy(&cache, Run::new(13, 4)).is_none(),
            "overlaps at its head"
        );
        assert!(copy(&cache, Run::new(40, 1)).is_some(), "untouched");
        assert_eq!(cache.bytes(), 50, "byte count follows the evictions");
    }

    #[test]
    fn a_run_ending_where_another_begins_does_not_overlap() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        cache.insert(Run::new(10, 2), image(50, 1)); // 10..12
        cache.invalidate(Run::new(12, 2)); // 12..14 — adjacent, not overlapping
        assert!(copy(&cache, Run::new(10, 2)).is_some());
    }

    #[test]
    fn the_budget_evicts_oldest_first() {
        let cache = PageCache::new(250);
        for n in 0..5u64 {
            cache.insert(Run::new(n * 2, 1), image(100, n as u8));
        }
        assert!(cache.bytes() <= 250, "held {} bytes", cache.bytes());
        assert!(copy(&cache, Run::new(0, 1)).is_none(), "oldest goes first");
        assert!(copy(&cache, Run::new(8, 1)).is_some(), "newest stays");
    }

    /// An image larger than the whole budget is not held: caching it would
    /// evict everything to serve one page.
    #[test]
    fn an_oversized_image_is_not_held() {
        let cache = PageCache::new(100);
        cache.insert(Run::new(0, 64), image(1000, 1));
        assert!(cache.is_empty());
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn a_zero_budget_holds_nothing() {
        let cache = PageCache::new(0);
        cache.insert(Run::new(0, 1), image(10, 1));
        assert!(cache.is_empty());
    }

    /// Re-inserting the same run must not double-count its bytes.
    #[test]
    fn replacing_an_entry_keeps_the_byte_count_true() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        cache.insert(Run::new(4, 1), image(100, 1));
        cache.insert(Run::new(4, 1), image(30, 2));
        assert_eq!(cache.bytes(), 30);
        assert_eq!(cache.len(), 1);
        assert_eq!(copy(&cache, Run::new(4, 1)).unwrap().len(), 30);
    }

    /// A run invalidated and held again is queued twice; the stale queue
    /// entry must not evict the newer image when it comes up first.
    #[test]
    fn a_reinserted_run_is_not_evicted_by_its_old_queue_entry() {
        let cache = PageCache::new(300);
        cache.insert(Run::new(0, 1), image(100, 1));
        cache.insert(Run::new(2, 1), image(100, 2));
        cache.invalidate(Run::new(0, 1));
        cache.insert(Run::new(0, 1), image(100, 3)); // queued again
        cache.insert(Run::new(4, 1), image(100, 4));
        cache.insert(Run::new(6, 1), image(100, 5)); // one must go
        assert!(
            copy(&cache, Run::new(2, 1)).is_none(),
            "the oldest live image is the one that goes"
        );
        assert!(copy(&cache, Run::new(0, 1)).is_some(), "younger, kept");
        assert_eq!(cache.bytes(), 300);
    }

    /// Invalidations under the budget never pop the queue, so without tidying
    /// it would grow by one per insert for ever.
    #[test]
    fn the_queue_stays_proportional_to_what_is_held() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        for n in 0..10_000u64 {
            cache.insert(Run::new(n, 1), image(8, 1));
            cache.invalidate(Run::new(n, 1));
        }
        assert!(cache.is_empty());
        assert!(cache.inner.borrow().fifo.len() <= 64, "queue leaked");
    }

    /// Serving a page lends the held image itself, not a copy of it.
    #[test]
    fn serving_a_page_lends_rather_than_copies() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        let run = Run::new(2, 1);
        cache.insert(run, image(64, 9));
        let a = cache.with(run, <[u8]>::as_ptr).ok();
        let b = cache.with(run, <[u8]>::as_ptr).ok();
        assert!(a.is_some() && a == b, "each read copied the page");
    }
}
