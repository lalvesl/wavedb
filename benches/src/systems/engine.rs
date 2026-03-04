//! Which engine seam a WaveDB row measures — the single/multi-thread axis.
//!
//! Both rows run the *same* workload through the *same* index layer. The only
//! difference is what sits under `Store`:
//!
//! - [`Engine::Direct`] — the `PageStore` in this thread. One thread does
//!   everything, and a `get` that hits the cache is a function call.
//! - [`Engine::Sharded`] — the RFC 0067 step 4 shape: the database is N
//!   data shards, one per consumer, and each consumer owns its shard's
//!   `PageStore` on its own thread, as a node worker does. Owners are placed
//!   by `% N` — the bench's stand-in for the node's owner hash, with the
//!   property that matters: one owner's records are on one thread. On
//!   `shop` the owner is a user; on `micro` each consumer owns a `Thing`
//!   collection of its own, and row `n` is consumer `n % N`'s (RFC 0065 §3,
//!   revised 2026-10-18).
//!
//! No `dyn`: the seam is a trait bound and each row monomorphises to its own
//! concrete engine, the same rule the workspace holds itself to.

use wavedb_core::Store;
use wavedb_storage::{PageStore, Progress};

/// Settle what is committed **without waiting for it to be durable**: open
/// a batch and step it to its end, its barrier asked of the helper (or the
/// storage thread), the answer read on a later call — a node's
/// `settle_step`, on a thread with nothing else to run. No batch opens
/// while the previous one is not yet durable, so batches never pile up
/// behind the disk.
///
/// # Errors
/// A settle or an earlier barrier failed.
pub fn settle_unwaited(store: &PageStore) -> Result<(), String> {
    let settle = |e| format!("settle: {e}");
    if store.begin_cycle().map_err(settle)? {
        while store.cycle_step(usize::MAX).map_err(settle)? == Progress::Settled
        {
        }
    }
    Ok(())
}

/// The axis. Named for what differs — where the engine lives — rather than for
/// a thread count, because the thread count is a consequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// `PageStore` here, in this thread.
    Direct,
    /// N data shards, each owned by one consumer thread.
    Sharded,
}

impl Engine {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Direct => "single",
            Self::Sharded => "sharded",
        }
    }

    /// How the row's `mode` setting reads in the results file.
    #[must_use]
    pub const fn mode(self) -> &'static str {
        match self {
            Self::Direct => "PageStore (in-process, one thread)",
            Self::Sharded => "one PageStore per consumer thread (RFC 0067)",
        }
    }
}

/// What the adapter needs of an engine beyond `Store`: the maintenance the
/// benchmark drives itself, so a phase measures a steady state rather than
/// work postponed into it.
///
/// Every method is the *waiting* form. A hint would be wrong here for the
/// reason a node's `run_with_shutdown` waits for its workers: a dropped
/// settle makes the footprint that follows it a measurement of nothing.
pub trait Engineish {
    type Store: Store;

    /// The `Store` the index layer runs against.
    fn store(&self) -> &Self::Store;

    /// Settle everything committed into a durable batch (RFC 0072).
    fn drain(&self) -> Result<(), String>;

    /// Settle without waiting for durability ([`settle_unwaited`]).
    fn settle(&self) -> Result<(), String>;

    /// Drop settled cache entries down to `budget`.
    fn evict(&self, budget: usize);

    /// Drop settled cache entries down to the budget the store was opened
    /// with — on a sharded database, this data shard's share of it.
    fn evict_to_budget(&self);

    /// Whether what is not settled has passed the store's pressure line
    /// (`StoreOptions::pressure_percent` of its record budget).
    fn under_pressure(&self) -> bool;

    /// Compact to the end: the live pages of the emptiest data files carried
    /// forward, what they leave cut off and deleted (RFC 0071).
    fn compact(&self) -> Result<(), String>;

    /// Close the storage probe's read counters at a phase boundary,
    /// naming the phase that just ended (the `probe` feature; a no-op
    /// otherwise).
    fn probe_mark(&self, _phase: &str) {}

    /// Is a committed batch still waiting for its page write?
    fn has_pending(&self) -> bool;

    /// Release the engine. Required, not tidy: the directory's lock has to be
    /// free before the next phase opens the same directory.
    fn close(self);
}

impl Engineish for PageStore {
    type Store = Self;

    fn store(&self) -> &Self {
        self
    }

    fn drain(&self) -> Result<(), String> {
        Self::drain(self).map_err(|e| format!("drain: {e}"))
    }

    fn settle(&self) -> Result<(), String> {
        settle_unwaited(self)
    }

    fn evict(&self, budget: usize) {
        self.evict_settled(budget);
    }

    fn evict_to_budget(&self) {
        Self::evict_to_budget(self);
    }

    fn under_pressure(&self) -> bool {
        Self::under_pressure(self)
    }

    fn compact(&self) -> Result<(), String> {
        Self::compact(self)
            .map(|_| ())
            .map_err(|e| format!("compact: {e}"))
    }

    fn has_pending(&self) -> bool {
        Self::has_pending(self)
    }

    #[cfg(feature = "probe")]
    fn probe_mark(&self, phase: &str) {
        Self::probe_mark(self, phase);
    }

    fn close(self) {
        drop(self);
    }
}
