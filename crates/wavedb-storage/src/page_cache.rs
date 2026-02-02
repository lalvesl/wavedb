//! The page cache ([RFC 0044]) — settled pages held in RAM, **decoded**.
//!
//! ## What is cached, and why that key
//!
//! The key is the **run** — the extent an image occupies, on disk or in the
//! object store (two address spaces: [`Lane`]) — and the value is the page
//! it holds, decompressed and decoded into a [`SlotPage`] once, when it came
//! from disk or the object store. The key falls out of the engine rather
//! than being tuning:
//!
//! - Page writes are copy-on-write: a rewritten page is placed in a *new* run
//!   and the descriptor is repointed ([RFC 0041]). A run's bytes are therefore
//!   immutable for as long as that run is allocated, so a cache keyed by run
//!   **never goes stale while its run is allocated**. An entry leaves through
//!   [`invalidate`](PageCache::invalidate) at the two points where that stops
//!   holding: when a round supersedes its run (no directory names it any
//!   more, so its image is dead weight), and when a write lands on a
//!   recycled extent.
//! - An object store's descriptor names bytes an upload put there, in a file
//!   PUT once and never again, so a remote entry never goes stale at all; it
//!   leaves by the budget.
//!
//! ## Decoded, on purpose
//!
//! Held compressed, every hit would pay the zstd decompression and the
//! decode again. Held decoded, a hit costs a lookup: CPU per hit is traded
//! for RAM per page — the same budget holds fewer pages — and the budget
//! counts what a page takes decoded ([`SlotPage::footprint`]). Object mode's
//! RAM backing is the store's "disk" and keeps the stored images; this
//! cache over it holds them decoded, as over a disk.
//!
//! - **A settle takes its pages out** ([`take`](PageCache::take)) instead of
//!   decoding them again: the round rewrites each one into a new run,
//!   superseding the old anyway.
//! - **A round holds the pages it writes**, decoded, under their new runs
//!   (`crate::checkpoint`): the page just settled is the one read next.
//!
//! ## Lent, not handed out
//!
//! A hit is served by **lending** the page to the reader
//! ([`with`](PageCache::with)): no copy of the page, and no shared pointer
//! either — a reference count would have to be atomic for the store to stay
//! `Send`, and the store is used from one thread, so there is nothing for
//! one to protect. The reader copies out what it needs and lets go.
//!
//! ## Not cached
//!
//! Only pages go through here. The dictionary image, the headers and the
//! defragmenter's relocation reads want the stored bytes and are read
//! straight from the file — caching them would hold bytes that are read once.
//!
//! [RFC 0044]: ../../../rfcs/0044-page-cache.md
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};

use crate::block::Run;
use crate::block_file::BlockFile;
use crate::error::StorageResult;
use crate::page::SlotPage;

/// Bytes of decoded pages held by default.
///
/// Deliberately modest: this is a *second* copy of data the OS page cache may
/// also hold, and under a container memory limit both are charged to the same
/// budget. A node that wants more should say so rather than inherit a number
/// sized for someone else's machine.
pub const DEFAULT_BUDGET_BYTES: usize = 64 << 20;

/// Which address space a run is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Lane {
    /// The data files' blocks.
    Disk,
    /// The object store's.
    Remote,
}

/// A run in its lane.
type Key = (Lane, u64);

struct Entry {
    /// Blocks the run spans, so an overlap test needs only the map.
    blocks: u64,
    /// Which insertion this is, so the queue can tell it from an earlier
    /// image of the same run.
    stamp: u64,
    /// What the page is charged against the budget.
    bytes: usize,
    page: SlotPage,
}

#[derive(Default)]
struct Inner {
    /// Keyed by the run's first block. Runs are disjoint allocations, so the
    /// map's ordering makes "which entries overlap [start, end)?" a range
    /// query — which a `HashMap` could only answer by a full scan, and
    /// invalidation happens on every window write.
    pages: BTreeMap<Key, Entry>,
    /// Insertion order, for eviction, as `(start, stamp)`. An entry that
    /// left early (invalidated, or replaced by a newer image of its run)
    /// stays queued and is skipped when it comes up — its stamp no longer
    /// matches — and [`Inner::tidy`] drops such leftovers once they
    /// outnumber the live entries, so the queue stays proportional to what
    /// is held.
    fifo: VecDeque<(Key, u64)>,
    stamp: u64,
    bytes: usize,
}

/// Decoded pages by run. Every method takes `&self`.
pub struct PageCache {
    inner: RefCell<Inner>,
    budget: usize,
}

impl PageCache {
    /// A cache holding at most `budget` bytes of pages (`0` disables it).
    #[must_use]
    pub fn new(budget: usize) -> Self {
        Self {
            inner: RefCell::new(Inner::default()),
            budget,
        }
    }

    /// Lend `run`'s page to `read`, if held; `read` comes back unused on a
    /// miss. `read` must not touch the cache — it runs while the page is
    /// borrowed from it.
    pub fn with<R, F: FnOnce(&SlotPage) -> R>(
        &self,
        lane: Lane,
        run: Run,
        read: F,
    ) -> Result<R, F> {
        let inner = self.inner.borrow();
        match inner.pages.get(&(lane, run.start)) {
            Some(e) if e.blocks == run.count => Ok(read(&e.page)),
            _ => Err(read),
        }
    }

    /// Take `run`'s page out, if held — for a caller about to rewrite it
    /// elsewhere, which leaves the run dead anyway.
    pub fn take(&self, lane: Lane, run: Run) -> Option<SlotPage> {
        let mut inner = self.inner.borrow_mut();
        let held = inner
            .pages
            .get(&(lane, run.start))
            .is_some_and(|e| e.blocks == run.count);
        let entry = held.then(|| inner.remove((lane, run.start))).flatten();
        inner.tidy();
        entry.map(|e| e.page)
    }

    /// Hold `run`'s page, evicting oldest-first to stay inside the budget.
    ///
    /// A single page larger than the whole budget is simply not held: caching
    /// it would evict everything else to serve one page.
    // The guard deliberately spans insert *and* eviction: between the two the
    // cache is over budget and its byte count disagrees with its contents, and
    // no reader may observe that. Releasing early — the lint's suggestion —
    // would make the over-budget window visible.
    #[allow(clippy::significant_drop_tightening)]
    pub fn insert(&self, lane: Lane, run: Run, page: SlotPage) {
        let bytes = page.footprint();
        if self.budget == 0 || bytes > self.budget {
            return;
        }
        let key = (lane, run.start);
        let mut inner = self.inner.borrow_mut();
        inner.drop_at(key);
        inner.stamp += 1;
        let stamp = inner.stamp;
        inner.bytes += bytes;
        inner.pages.insert(
            key,
            Entry {
                blocks: run.count,
                stamp,
                bytes,
                page,
            },
        );
        inner.fifo.push_back((key, stamp));
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

    /// Forget every page overlapping the disk's `run`.
    ///
    /// Called wherever a run stops being what the cache remembers: a round
    /// superseding it, and a write into it. Overlap rather than exact match,
    /// because one window write covers many cached runs.
    pub fn invalidate(&self, run: Run) {
        let mut inner = self.inner.borrow_mut();
        let end = run.end();
        let doomed: Vec<Key> = inner
            .pages
            .range((Lane::Disk, 0)..(Lane::Disk, end))
            .rev()
            .take_while(|((_, start), e)| *start + e.blocks > run.start)
            .map(|(key, _)| *key)
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

    /// Bytes of pages held, as charged.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.inner.borrow_mut().bytes
    }

    /// Pages held.
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
    /// Remove the entry at `key`, if any, keeping `bytes` true.
    fn remove(&mut self, key: Key) -> Option<Entry> {
        let old = self.pages.remove(&key)?;
        self.bytes -= old.bytes;
        Some(old)
    }

    /// Drop the entry at `key`, if any.
    fn drop_at(&mut self, key: Key) {
        self.remove(key);
    }

    /// Drop the queue entries whose image has already left, once they
    /// outnumber the live ones — amortised O(1) per insert or invalidation.
    fn tidy(&mut self) {
        if self.fifo.len() <= 2 * self.pages.len() + 64 {
            return;
        }
        let pages = &self.pages;
        self.fifo.retain(|(key, stamp)| {
            pages.get(key).is_some_and(|e| e.stamp == *stamp)
        });
    }
}

/// The page cache on the data files' read path.
impl BlockFile {
    /// The page at `run`, lent to `read` — held by the page cache, or read,
    /// decoded by `decode` and then held, decoded (RFC 0044): a hit costs no
    /// read and no decompression. `read` must not reach the cache itself.
    /// Pages only: the dictionary image, the headers and compaction's
    /// buffers are read once and want the stored bytes
    /// ([`read_run`](BlockFile::read_run)).
    ///
    /// # Errors
    /// The same faults as [`read_run`](BlockFile::read_run), and `decode`'s. A
    /// cache hit cannot fault at all, which is the point.
    pub fn with_page<R>(
        &self,
        run: Run,
        decode: impl FnOnce(&[u8]) -> StorageResult<SlotPage>,
        read: impl FnOnce(&SlotPage) -> R,
    ) -> StorageResult<R> {
        let read = match self.cache.with(Lane::Disk, run, read) {
            Ok(done) => return Ok(done),
            Err(read) => read,
        };
        let page = decode(&self.read_run(run)?)?;
        let done = read(&page);
        self.cache.insert(Lane::Disk, run, page);
        Ok(done)
    }

    /// The page at `run`, owned — taken out of the page cache, or read and
    /// decoded — for a settle about to rewrite it into a new run, which
    /// leaves this one dead anyway.
    ///
    /// # Errors
    /// As [`with_page`](Self::with_page).
    pub fn take_page(
        &self,
        run: Run,
        decode: impl FnOnce(&[u8]) -> StorageResult<SlotPage>,
    ) -> StorageResult<SlotPage> {
        match self.cache.take(Lane::Disk, run) {
            Some(page) => Ok(page),
            None => decode(&self.read_run(run)?),
        }
    }

    /// The page cache backing [`with_page`](Self::with_page).
    #[must_use]
    pub const fn page_cache(&self) -> &PageCache {
        &self.cache
    }
}

/// Its size, never its contents. A derived `Debug` on a cache of pages
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
    use wavedb_core::{Id, U48};

    use super::{DEFAULT_BUDGET_BYTES, Lane, PageCache};
    use crate::block::Run;
    use crate::page::SlotPage;

    /// A page of one record of `len` bytes of `fill`.
    fn page(len: usize, fill: u8) -> SlotPage {
        let mut page = SlotPage::new(7);
        page.upsert(Id::new(1, U48::from(1u32), false, 1), vec![fill; len]);
        page
    }

    /// What a one-record page of `len` bytes is charged.
    fn cost(len: usize) -> usize {
        page(len, 0).footprint()
    }

    fn insert(cache: &PageCache, run: Run, len: usize, fill: u8) {
        cache.insert(Lane::Disk, run, page(len, fill));
    }

    fn copy(cache: &PageCache, run: Run) -> Option<SlotPage> {
        cache.with(Lane::Disk, run, SlotPage::clone).ok()
    }

    #[test]
    fn a_held_page_comes_back_by_its_run() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        let run = Run::new(8, 2);
        insert(&cache, run, 100, 7);
        assert_eq!(copy(&cache, run), Some(page(100, 7)));
        assert_eq!(cache.bytes(), cost(100));
    }

    /// The same start with a different length is a different extent, so it
    /// must miss rather than hand back another page.
    #[test]
    fn a_run_of_another_length_is_a_miss() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        insert(&cache, Run::new(8, 2), 100, 7);
        assert!(copy(&cache, Run::new(8, 3)).is_none());
    }

    /// The disk and the object store are different address spaces: the
    /// same run in each is a different page.
    #[test]
    fn the_lanes_do_not_share_runs() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        let run = Run::new(8, 2);
        insert(&cache, run, 10, 1);
        cache.insert(Lane::Remote, run, page(10, 2));
        assert_eq!(copy(&cache, run), Some(page(10, 1)));
        assert_eq!(
            cache.with(Lane::Remote, run, SlotPage::clone).ok(),
            Some(page(10, 2))
        );
        cache.invalidate(run);
        assert!(
            copy(&cache, run).is_none(),
            "a disk write forgets the disk's"
        );
        assert!(
            cache.with(Lane::Remote, run, |_| ()).is_ok(),
            "and never the object store's"
        );
    }

    /// One window write covers many cached runs, so invalidation is by
    /// overlap. A cache that only matched exact runs would keep serving the
    /// pages a recycled extent no longer holds.
    #[test]
    fn invalidating_a_window_forgets_every_page_inside_it() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        insert(&cache, Run::new(10, 2), 50, 1); // 10..12
        insert(&cache, Run::new(12, 1), 50, 2); // 12..13
        insert(&cache, Run::new(13, 4), 50, 3); // 13..17
        insert(&cache, Run::new(40, 1), 50, 4); // outside
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
        assert_eq!(cache.bytes(), cost(50), "byte count follows");
    }

    #[test]
    fn a_run_ending_where_another_begins_does_not_overlap() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        insert(&cache, Run::new(10, 2), 50, 1); // 10..12
        cache.invalidate(Run::new(12, 2)); // 12..14 — adjacent
        assert!(copy(&cache, Run::new(10, 2)).is_some());
    }

    #[test]
    fn the_budget_evicts_oldest_first() {
        let cache = PageCache::new(cost(100) * 5 / 2);
        for n in 0..5u64 {
            insert(&cache, Run::new(n * 2, 1), 100, n as u8);
        }
        assert!(cache.bytes() <= cache.budget(), "held {}", cache.bytes());
        assert!(copy(&cache, Run::new(0, 1)).is_none(), "oldest goes first");
        assert!(copy(&cache, Run::new(8, 1)).is_some(), "newest stays");
    }

    /// A page larger than the whole budget is not held: caching it would
    /// evict everything to serve one page.
    #[test]
    fn an_oversized_page_is_not_held() {
        let cache = PageCache::new(100);
        insert(&cache, Run::new(0, 64), 1000, 1);
        assert!(cache.is_empty());
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn a_zero_budget_holds_nothing() {
        let cache = PageCache::new(0);
        insert(&cache, Run::new(0, 1), 10, 1);
        assert!(cache.is_empty());
    }

    /// Re-inserting the same run must not double-count its bytes.
    #[test]
    fn replacing_an_entry_keeps_the_byte_count_true() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        insert(&cache, Run::new(4, 1), 100, 1);
        insert(&cache, Run::new(4, 1), 30, 2);
        assert_eq!(cache.bytes(), cost(30));
        assert_eq!(cache.len(), 1);
        assert_eq!(copy(&cache, Run::new(4, 1)), Some(page(30, 2)));
    }

    /// Taking a page out hands it over and forgets it; another length is not
    /// it.
    #[test]
    fn a_page_taken_out_is_no_longer_held() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        let run = Run::new(4, 1);
        insert(&cache, run, 30, 2);
        assert!(cache.take(Lane::Disk, Run::new(4, 2)).is_none());
        assert_eq!(cache.take(Lane::Disk, run), Some(page(30, 2)));
        assert!(cache.is_empty());
        assert_eq!(cache.bytes(), 0);
        assert!(cache.take(Lane::Disk, run).is_none());
    }

    /// A run invalidated and held again is queued twice; the stale queue
    /// entry must not evict the newer page when it comes up first.
    #[test]
    fn a_reinserted_run_is_not_evicted_by_its_old_queue_entry() {
        let cache = PageCache::new(cost(100) * 3);
        insert(&cache, Run::new(0, 1), 100, 1);
        insert(&cache, Run::new(2, 1), 100, 2);
        cache.invalidate(Run::new(0, 1));
        insert(&cache, Run::new(0, 1), 100, 3); // queued again
        insert(&cache, Run::new(4, 1), 100, 4);
        insert(&cache, Run::new(6, 1), 100, 5); // one must go
        assert!(
            copy(&cache, Run::new(2, 1)).is_none(),
            "the oldest live page is the one that goes"
        );
        assert!(copy(&cache, Run::new(0, 1)).is_some(), "younger, kept");
        assert_eq!(cache.bytes(), cost(100) * 3);
    }

    /// Invalidations under the budget never pop the queue, so without tidying
    /// it would grow by one per insert for ever.
    #[test]
    fn the_queue_stays_proportional_to_what_is_held() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        for n in 0..10_000u64 {
            insert(&cache, Run::new(n, 1), 8, 1);
            cache.invalidate(Run::new(n, 1));
        }
        assert!(cache.is_empty());
        assert!(cache.inner.borrow().fifo.len() <= 64, "queue leaked");
    }

    /// Serving a page lends the held page itself, not a copy of it.
    #[test]
    fn serving_a_page_lends_rather_than_copies() {
        let cache = PageCache::new(DEFAULT_BUDGET_BYTES);
        let run = Run::new(2, 1);
        insert(&cache, run, 64, 9);
        let ptr = |p: &SlotPage| std::ptr::from_ref(p) as usize;
        let a = cache.with(Lane::Disk, run, ptr).ok();
        let b = cache.with(Lane::Disk, run, ptr).ok();
        assert!(a.is_some() && a == b, "each read copied the page");
    }
}
