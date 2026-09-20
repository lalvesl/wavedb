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
//!   **never needs invalidating on a page rewrite**. The only events that can
//!   falsify an entry are a run being written again after being recycled by
//!   the allocator, and the defragmenter relocating one — both of which go
//!   through [`invalidate`](PageCache::invalidate).
//! - Compressed is the form the disk holds and the form a reader must
//!   decompress from anyway, so storing it uncompressed would cache *fewer*
//!   pages for the same RAM and save nothing on the read path.
//!
//! ## Cheap handing-out
//!
//! Entries are `Arc<[u8]>`, so serving a cached page is a refcount bump rather
//! than a copy of the image — the "pointer copy" a checkpoint wants when it
//! hands pages to a writer, available from `std` without a bespoke map.
//!
//! ## Not cached
//!
//! Only page reads go through here. The dictionary image, the addressing log's
//! chunks and the defragmenter's relocation reads all want owned, mutable
//! buffers and are read straight from the file — caching them would hold bytes
//! that are read once.
//!
//! [RFC 0044]: ../../../rfcs/0044-page-cache-PLANNED-LOW.md
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;

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
    bytes: Arc<[u8]>,
}

#[derive(Default)]
struct Inner {
    /// Keyed by the run's first block. Runs are disjoint allocations, so the
    /// map's ordering makes "which entries overlap [start, end)?" a range
    /// query — which a `HashMap` could only answer by a full scan, and
    /// invalidation happens on every window write.
    pages: BTreeMap<u64, Entry>,
    /// Insertion order, for eviction. Keys removed by invalidation stay here
    /// and are skipped when they come up — cheaper than keeping the two
    /// structures exactly in step for a queue that is drained in order.
    fifo: VecDeque<u64>,
    bytes: usize,
}

/// Page images by run. Cheap to share; every method takes `&self`.
pub struct PageCache {
    inner: Mutex<Inner>,
    budget: usize,
}

impl PageCache {
    /// A cache holding at most `budget` bytes of images (`0` disables it).
    #[must_use]
    pub fn new(budget: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            budget,
        }
    }

    /// `run`'s image, if held.
    #[must_use]
    pub fn get(&self, run: Run) -> Option<Arc<[u8]>> {
        let inner = self.inner.lock();
        inner
            .pages
            .get(&run.start)
            .filter(|e| e.blocks == run.count)
            .map(|e| Arc::clone(&e.bytes))
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
    pub fn insert(&self, run: Run, bytes: &Arc<[u8]>) {
        if self.budget == 0 || bytes.len() > self.budget {
            return;
        }
        let mut inner = self.inner.lock();
        inner.drop_at(run.start);
        inner.bytes += bytes.len();
        inner.pages.insert(
            run.start,
            Entry {
                blocks: run.count,
                bytes: Arc::clone(bytes),
            },
        );
        inner.fifo.push_back(run.start);
        while inner.bytes > self.budget {
            let Some(oldest) = inner.fifo.pop_front() else {
                break; // nothing left to give back
            };
            inner.drop_at(oldest);
        }
    }

    /// Forget every image overlapping `run`.
    ///
    /// Called wherever a run's bytes stop being what the cache remembers: a
    /// write into it, and the allocator handing it out again. Overlap rather
    /// than exact match, because one window write covers many cached runs.
    pub fn invalidate(&self, run: Run) {
        let mut inner = self.inner.lock();
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
        self.inner.lock().bytes
    }

    /// Images held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().pages.len()
    }

    /// Whether the cache holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop everything (a reopen, or a test wanting a cold read).
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
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
}

/// Its size, never its contents. A derived `Debug` on a cache of page images
/// would dump the database into whatever printed the `BlockFile` holding it.
impl std::fmt::Debug for PageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
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
    use std::sync::Arc;

    fn image(len: usize, fill: u8) -> Arc<[u8]> {
        vec![fill; len].into()
    }

    #[test]
    fn a_held_page_comes_back_by_its_run() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        let run = Run::new(8, 2);
        cache.insert(run, &image(100, 7));
        assert_eq!(cache.get(run).as_deref(), Some(&[7u8; 100][..]));
        assert_eq!(cache.bytes(), 100);
    }

    /// The same start with a different length is a different extent, so it
    /// must miss rather than hand back the wrong number of bytes.
    #[test]
    fn a_run_of_another_length_is_a_miss() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        cache.insert(Run::new(8, 2), &image(100, 7));
        assert!(cache.get(Run::new(8, 3)).is_none());
    }

    /// One window write covers many cached runs, so invalidation is by
    /// overlap. A cache that only matched exact runs would keep serving the
    /// bytes a recycled extent no longer holds.
    #[test]
    fn invalidating_a_window_forgets_every_page_inside_it() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        cache.insert(Run::new(10, 2), &image(50, 1)); // 10..12
        cache.insert(Run::new(12, 1), &image(50, 2)); // 12..13
        cache.insert(Run::new(13, 4), &image(50, 3)); // 13..17
        cache.insert(Run::new(40, 1), &image(50, 4)); // outside
        assert_eq!(cache.len(), 4);

        cache.invalidate(Run::new(11, 3)); // 11..14 — clips all three
        assert!(cache.get(Run::new(10, 2)).is_none(), "overlaps at its tail");
        assert!(cache.get(Run::new(12, 1)).is_none(), "wholly inside");
        assert!(cache.get(Run::new(13, 4)).is_none(), "overlaps at its head");
        assert!(cache.get(Run::new(40, 1)).is_some(), "untouched");
        assert_eq!(cache.bytes(), 50, "byte count follows the evictions");
    }

    #[test]
    fn a_run_ending_where_another_begins_does_not_overlap() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        cache.insert(Run::new(10, 2), &image(50, 1)); // 10..12
        cache.invalidate(Run::new(12, 2)); // 12..14 — adjacent, not overlapping
        assert!(cache.get(Run::new(10, 2)).is_some());
    }

    #[test]
    fn the_budget_evicts_oldest_first() {
        let cache = PageCache::new(250);
        for n in 0..5u64 {
            cache.insert(Run::new(n * 2, 1), &image(100, n as u8));
        }
        assert!(cache.bytes() <= 250, "held {} bytes", cache.bytes());
        assert!(cache.get(Run::new(0, 1)).is_none(), "oldest goes first");
        assert!(cache.get(Run::new(8, 1)).is_some(), "newest stays");
    }

    /// An image larger than the whole budget is not held: caching it would
    /// evict everything to serve one page.
    #[test]
    fn an_oversized_image_is_not_held() {
        let cache = PageCache::new(100);
        cache.insert(Run::new(0, 64), &image(1000, 1));
        assert!(cache.is_empty());
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn a_zero_budget_holds_nothing() {
        let cache = PageCache::new(0);
        cache.insert(Run::new(0, 1), &image(10, 1));
        assert!(cache.is_empty());
    }

    /// Re-inserting the same run must not double-count its bytes.
    #[test]
    fn replacing_an_entry_keeps_the_byte_count_true() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        cache.insert(Run::new(4, 1), &image(100, 1));
        cache.insert(Run::new(4, 1), &image(30, 2));
        assert_eq!(cache.bytes(), 30);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(Run::new(4, 1)).unwrap().len(), 30);
    }

    /// Handing out a page is a refcount bump, not a copy of the image.
    #[test]
    fn serving_a_page_shares_rather_than_copies() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        let run = Run::new(2, 1);
        cache.insert(run, &image(64, 9));
        let a = cache.get(run).unwrap();
        let b = cache.get(run).unwrap();
        assert!(Arc::ptr_eq(&a, &b), "each read copied the page");
    }
}
