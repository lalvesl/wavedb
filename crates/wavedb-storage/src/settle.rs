//! The record cache's side of a settle ([RFC 0068]): what is still to
//! settle, and when a settled chunk may leave the cache.
//!
//! The rounds themselves are the cycle's ([`crate::cycle`]); the work lives
//! in [`crate::plan`] (page images, grouped per bucket) and
//! [`crate::checkpoint`] (one window, one write, then the descriptor swap).
//!
//! [RFC 0068]: ../../../rfcs/0068-record-cache-as-a-log-WIP.md

use crate::page_store::PageStore;

/// What one [`cycle_step`](PageStore::cycle_step) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// A round of the open batch was settled; it owes more, or its end is
    /// next.
    Settled,
    /// The batch ended — or none was open.
    Done,
}

impl PageStore {
    /// Whether any committed write still awaits its page write.
    #[must_use]
    pub fn has_pending(&self) -> bool {
        self.cache.borrow().has_pending()
    }

    /// Bytes of committed writes not settled yet — what a node's
    /// maintenance valve weighs.
    #[must_use]
    pub fn unsettled_bytes(&self) -> u64 {
        self.cache.borrow().unsettled_bytes()
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

    /// Whether committed writes not settled yet have passed the store's
    /// [`pressure_percent`](crate::StoreOptions::pressure_percent) share of
    /// its record-cache budget — the moment to settle, then
    /// [`evict_to_budget`](Self::evict_to_budget), rather than wait for
    /// the next tick.
    ///
    /// The store never acts on it, as it never evicts on its own: a caller
    /// asks after a write (a read adds nothing unsettled) and decides. It
    /// sums one subtraction per type, so asking after every write is cheap.
    #[must_use]
    pub fn under_pressure(&self) -> bool {
        self.unsettled_bytes() > self.pressure_bytes
    }
}
