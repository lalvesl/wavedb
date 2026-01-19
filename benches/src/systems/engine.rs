//! Which engine seam a WaveDB row measures — the single/multi-thread axis.
//!
//! Both rows run the *same* workload through the *same* index layer. The only
//! difference is what sits under `Store`:
//!
//! - [`Engine::Direct`] — the `PageStore` in this thread. One thread does
//!   everything, and a `get` that hits the cache is a function call.
//! - [`Engine::Sharded`] — the RFC 0067 step 4 shape, on `shop` only: the
//!   database is N data shards, one per consumer, and each consumer owns its
//!   shard's `PageStore` on its own thread, as a node worker does. Users are
//!   placed by `u % N` — the bench's stand-in for the node's owner hash,
//!   with the property that matters: one user's records are on one thread.
//!
//! `micro` has no sharded row: it is one collection, so one owner, so one
//! thread however many exist (RFC 0065 §3).
//!
//! No `dyn`: the seam is a trait bound and each row monomorphises to its own
//! concrete engine, the same rule the workspace holds itself to.

use wavedb_core::Store;
use wavedb_storage::PageStore;

/// The axis. Named for what differs — where the engine lives — rather than for
/// a thread count, because the thread count is a consequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// `PageStore` here, in this thread.
    Direct,
    /// N data shards, each owned by one consumer thread (`shop` only).
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

    /// Settle the pending queue into pages, to completion.
    fn drain(&self) -> Result<(), String>;

    /// Settle, sync the data files, frame the journal.
    fn checkpoint(&self) -> Result<(), String>;

    /// Drop settled cache entries down to `budget`.
    fn evict(&self, budget: usize);

    /// Compact to the end: the live pages of the emptiest data files carried
    /// forward, what they leave cut off and deleted (RFC 0071).
    fn compact(&self) -> Result<(), String>;

    /// Committed journal bytes — what the adapter's maintenance threshold
    /// weighs.
    fn journal_len(&self) -> u64;

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

    fn checkpoint(&self) -> Result<(), String> {
        self.commit_journal()
            .map_err(|e| format!("checkpoint: {e}"))
    }

    fn evict(&self, budget: usize) {
        self.evict_settled(budget);
    }

    fn compact(&self) -> Result<(), String> {
        Self::compact(self)
            .map(|_| ())
            .map_err(|e| format!("compact: {e}"))
    }

    fn journal_len(&self) -> u64 {
        Self::journal_len(self)
    }

    fn has_pending(&self) -> bool {
        Self::has_pending(self)
    }

    fn close(self) {
        drop(self);
    }
}
