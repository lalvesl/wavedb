//! Which engine seam a WaveDB row measures — the single/multi-thread axis.
//!
//! Both rows run the *same* workload through the *same* index layer. The only
//! difference is what sits under `Store`:
//!
//! - [`Engine::Direct`] — the `PageStore` in this thread. One thread does
//!   everything, and a `get` that hits the cache is a function call.
//! - [`Engine::Sharded`] — the RFC 0064 shape: the engine is owned by a **disk
//!   actor** on its own thread, and this thread reaches it through a
//!   `ShardStore` (its own cache) by message. Two threads, and every cache miss
//!   is a channel round trip and a thread wake-up.
//!
//! ## What the pair can and cannot show
//!
//! It measures **the cost of the boundary**, not the benefit of parallelism.
//! Three separate reasons, and each alone is enough:
//!
//! 1. the benchmark issues one operation at a time (`block_on` per op, each
//!    timed alone), so a second shard would never have a second operation;
//! 2. the brake serialises per owner — one collection, or one Unique type
//!    under one tenant — so this workload's one collection is **one owner**
//!    however many shards exist, even under a concurrent client;
//! 3. there is only one shard here anyway: the per-shard worker threads belong
//!    to the node's `Router`, and a benchmark driving `Store` directly builds
//!    none. This thread is the shard.
//!
//! So the axis is honestly named "what does the actor cost". The concurrency it
//! exists to enable needs a concurrent client over many collections before it
//! can be measured at all.
//!
//! No `dyn`: the seam is a trait bound and each row monomorphises to its own
//! concrete engine, the same rule the workspace holds itself to.

use std::rc::Rc;

use futures::executor::block_on;
use wavedb_core::Store;
use wavedb_quick_node::shard::{DiskRequest, Maintenance, ShardStore, Shards};
use wavedb_storage::PageStore;

/// The axis. Named for what differs — where the engine lives — rather than for
/// a thread count, because the thread count is a consequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// `PageStore` here, in this thread.
    Direct,
    /// Behind the disk actor, reached by `ShardStore` (RFC 0064).
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
            Self::Sharded => "ShardStore → disk actor (RFC 0064)",
        }
    }
}

/// What the adapter needs of an engine beyond `Store`: the maintenance the
/// benchmark drives itself, so a phase measures a steady state rather than
/// work postponed into it.
///
/// Every method is the *waiting* form. A hint would be wrong here for the
/// reason `DiskHandle::maintain` exists: a dropped settle makes the footprint
/// that follows it a measurement of nothing.
pub trait Engineish {
    type Store: Store;

    /// The `Store` the index layer runs against.
    fn store(&self) -> &Self::Store;

    /// Settle the pending queue into pages, to completion.
    fn drain(&self) -> Result<(), String>;

    /// Settle, sync `data.bin`, frame the journal.
    fn checkpoint(&self) -> Result<(), String>;

    /// Drop settled cache entries down to `budget`.
    fn evict(&self, budget: usize);

    /// Relocate stranded live pages so free space coalesces.
    fn defragment(&self, blocks: u64) -> Result<(), String>;

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

    fn defragment(&self, blocks: u64) -> Result<(), String> {
        Self::defragment(self, blocks)
            .map(|_| ())
            .map_err(|e| format!("defragment: {e}"))
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

/// The engine behind its actor, plus this thread's shard.
pub struct Sharded {
    shards: Shards,
    store: Rc<ShardStore>,
}

/// Bytes of records this thread's shard caches — `ShardStore`'s **shipped
/// default**, deliberately.
///
/// An earlier version halved it twice, to leave room in the 500 MB cage beside
/// the engine's own write cache. That produced a real number for a
/// configuration nobody runs, and a badly misleading one: at 100 000 records
/// the working set is ~40 MB, so a 16 MiB budget overflowed constantly — and
/// `ShardStore` bounds itself by **clearing the whole cache**, not by evicting
/// (its own comment says so, and defers the eviction order to RFC 0044). Hot
/// reads collapsed from 1 269 000/s to 96 000/s, which reads like "the actor
/// costs 13×" and is actually "the cache was flushed repeatedly".
///
/// Both numbers are real and the pair is the finding: the round trip costs
/// roughly an order of magnitude over an in-process hit, so **everything
/// depends on the shard's hit rate**, and the hit rate currently has no
/// eviction policy behind it. The row measures the shipped budget; the note
/// carries the other point.
const SHARD_CACHE_BYTES: usize = wavedb_quick_node::shard::CACHE_BYTES;

impl Sharded {
    /// Hand `store` to a disk actor and take one caching shard onto it.
    ///
    /// One caching shard, and that is a correctness condition rather than a
    /// choice: `ShardStore` remembers absence, which holds only while a record
    /// is reached by exactly one holder.
    ///
    /// The count is 1 because it is the truth rather than a setting: this
    /// thread *is* the shard — it holds the `ShardStore` and runs the index
    /// layer — and `Shards::start` spawns only the disk actor. The per-shard
    /// worker threads are the node's `Router`'s, and a benchmark driving
    /// `Store` directly has no requests to route. Passing `N` here would spawn
    /// nothing and describe a configuration that is not running.
    pub fn new(store: PageStore) -> Result<Self, String> {
        let shards = Shards::start(store, 1)
            .map_err(|e| format!("start shards: {e}"))?;
        let store = Rc::new(ShardStore::with_budget(
            shards.handle(),
            SHARD_CACHE_BYTES,
        ));
        Ok(Self { shards, store })
    }

    /// Ask the actor for `work` and wait for it.
    fn ask(&self, work: Maintenance, what: &str) -> Result<(), String> {
        block_on(self.shards.handle().maintain(work))
            .map_err(|e| format!("{what}: {e}"))
    }
}

impl Engineish for Sharded {
    type Store = ShardStore;

    fn store(&self) -> &ShardStore {
        &self.store
    }

    fn drain(&self) -> Result<(), String> {
        self.ask(Maintenance::Settle, "drain")
    }

    fn checkpoint(&self) -> Result<(), String> {
        self.ask(Maintenance::Checkpoint, "checkpoint")
    }

    fn evict(&self, budget: usize) {
        // The engine's cache, behind the actor. This shard's own cache is
        // bounded by `SHARD_CACHE_BYTES` and evicts itself.
        let _ = self.ask(
            Maintenance::Evict {
                budget_bytes: budget,
            },
            "evict",
        );
    }

    fn defragment(&self, blocks: u64) -> Result<(), String> {
        self.ask(
            Maintenance::Defragment {
                budget_blocks: blocks,
            },
            "defragment",
        )
    }

    fn journal_len(&self) -> u64 {
        block_on(self.shards.handle().stats()).map_or(0, |s| s.journal_bytes)
    }

    fn has_pending(&self) -> bool {
        // An unreachable actor is reported as "nothing pending" so a quiesce
        // loop ends rather than spinning; the caller's next real request will
        // surface the fault with a cause attached.
        block_on(self.shards.handle().stats()).is_ok_and(|s| s.pending)
    }

    fn close(self) {
        // Not a drop: the actor must *finish* — settle, checkpoint, force the
        // retirement barrier — and release the directory's lock before the
        // next phase opens it. Dropping the handles would end the actor
        // eventually, and "eventually" reopens as `EngineBusy`.
        let (answer, wait) = futures::channel::oneshot::channel();
        let handle = self.shards.handle();
        drop(self.store);
        if block_on(handle.send(DiskRequest::Shutdown { answer })).is_ok() {
            let _ = block_on(wait);
        }
        drop(handle);
        drop(self.shards);
    }
}
