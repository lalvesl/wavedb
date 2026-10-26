//! The settle drain — feeding rounds of the types' logs to the checkpoint
//! writer, plus the cache-eviction policy ([RFC 0068]).
//!
//! The work itself lives in [`crate::plan`] (page images, grouped per bucket)
//! and [`crate::checkpoint`] (one window, one write, then the descriptor
//! swap). This module owns only the log side: what a round is, when it
//! retries, and when a settled chunk may leave the cache.
//!
//! [RFC 0068]: ../../../rfcs/0068-record-cache-as-a-log-WIP.md

use crate::error::StorageResult;
use crate::page_store::PageStore;

/// What one [`settle_step`](PageStore::settle_step) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// A round was settled. There may be more — including writes that landed
    /// while this one ran.
    Settled,
    /// Every log was observed settled. Nothing to do until another write
    /// lands.
    Done,
}

impl PageStore {
    /// Settle everything committed: step until every type's log is settled
    /// to its end (writes landing while a round settles are picked up by the
    /// next round).
    ///
    /// # Errors
    /// A page write fault. Nothing acked is at risk — the journal still
    /// holds every unsettled batch, and the failed round's settle cursors
    /// did not move, so a later drain (or the reopen replay) retries it.
    pub fn drain(&self) -> StorageResult<()> {
        while self.settle_step(usize::MAX)? == Progress::Settled {}
        Ok(())
    }

    /// **One** round, then return — the interruptible form of
    /// [`drain`](Self::drain) ([RFC 0063] part 3).
    ///
    /// The caller drives it, and between two calls it may do anything: serve a
    /// read, take a write, stop. That boundary is the engine's only legal
    /// yield point.
    ///
    /// A round copies the live entries from each log's settle cursor out of
    /// the cache, builds and writes their pages with nothing borrowed, and
    /// only then moves the cursors past what it copied. So at every point a
    /// caller can observe, everything before a cursor is in the pages — the
    /// one fact eviction relies on. A write landing during the round appends
    /// a newer copy after the cursor; the next round writes it. One round at
    /// a time comes free: the store is used from one thread, and a step runs
    /// to its end without suspending, so no older version can land after a
    /// newer one.
    ///
    /// ## Why a `step` and not an `async fn`
    ///
    /// The engine contains no await point, so an `async fn drain` would be a
    /// future that never yields — the machinery of suspension with nothing to
    /// suspend on. The driver that makes a step useful (a node worker's BG
    /// task, between its jobs) already exists.
    ///
    /// ## `budget`
    ///
    /// The most log entries a round may scan, so one burst does not become
    /// one uninterruptible round. Superseded copies count — they are what the
    /// cursor walks over — and are skipped: a record saved a thousand times
    /// is written once, at its newest. `usize::MAX` means "no bound", which
    /// is what [`drain`](Self::drain) asks for.
    ///
    /// # Errors
    /// A page write fault. The cursors stay where they were, so a later
    /// step — or the reopen replay — retries the round.
    ///
    /// [RFC 0063]: ../../../rfcs/0063-engine-yield-map-and-interruptible-engine-PLANNED.md
    pub fn settle_step(&self, budget: usize) -> StorageResult<Progress> {
        let mut round = self.cache.borrow().round(budget);
        if round.is_empty() {
            return Ok(Progress::Done);
        }
        self.settle(std::mem::take(&mut round.work))?;
        self.cache.borrow_mut().advance(&round);
        Ok(Progress::Settled)
    }

    /// Whether any committed batch still awaits its page write.
    #[must_use]
    pub fn has_pending(&self) -> bool {
        self.cache.borrow().has_pending()
    }

    /// Bytes of committed journal frames — the checkpoint policy's input.
    #[must_use]
    pub fn journal_len(&self) -> u64 {
        self.journal.borrow().len_bytes()
    }

    /// Drop settled chunks of the types' logs, oldest first, until the
    /// record cache — its index and its logs, measured as allocated — holds
    /// at most `budget_bytes`, or nothing settled is left to drop. What is
    /// not settled yet is never dropped: its page is stale.
    pub fn evict_settled(&self, budget_bytes: usize) {
        self.cache.borrow_mut().evict(budget_bytes);
    }

    /// The record-cache budget this store was opened with
    /// ([`StoreOptions::record_cache_bytes`](crate::StoreOptions::record_cache_bytes)).
    #[must_use]
    pub const fn record_cache_bytes(&self) -> usize {
        self.record_cache_bytes
    }

    /// [`evict_settled`](Self::evict_settled) down to the budget the store was
    /// opened with.
    ///
    /// The engine never calls this itself — eviction stays the caller's
    /// decision, so a store does no work on a schedule of its own inside a
    /// window someone else is timing. What the store supplies is the number,
    /// so a caller stops having to remember one.
    pub fn evict_to_budget(&self) {
        self.evict_settled(self.record_cache_bytes);
    }
}
