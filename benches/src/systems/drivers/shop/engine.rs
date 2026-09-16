//! The engine seam the shop's WaveDB rows are generic over.
//!
//! Two implementations, one per row of the single/multi axis, and no `dyn`:
//! each row monomorphises to its own concrete engine, the same rule the
//! workspace holds itself to.
//!
//! It is deliberately smaller than
//! [`Engineish`](crate::systems::engine::Engineish), and the missing method is
//! the interesting one. There is no `close`: with three consumers over one
//! disk actor, the first driver to shut it down would stop the actor the other
//! two are still using, so closing is the **row's** job
//! ([`row::shop`](crate::row::shop)).

use std::rc::Rc;
use std::sync::{Arc, Mutex};

use futures::executor::block_on;
use wavedb_core::Store;
use wavedb_quick_node::shard::{ShardStore, Shards};
use wavedb_storage::PageStore;

/// What a consumer's engine must offer. Deliberately smaller than
/// [`Engineish`](crate::systems::engine::Engineish): closing is the **row's**
/// job here, because with three consumers over one disk actor the first
/// `close` would otherwise stop the actor the other two are still using.
pub trait ShopEngine: Sized {
    /// Built once by the row, shared across consumer threads.
    type Shared: Send + Sync;
    /// This consumer's own store — non-`Send`, built on its own thread.
    type Store: Store;

    /// How many consumers this seam can honour, given what was asked.
    fn consumers(asked: usize) -> usize;

    /// Take the row's one `PageStore` into whatever the seam shares.
    ///
    /// # Errors
    /// The disk actor's thread failing to spawn.
    fn share(
        store: PageStore,
        consumers: usize,
    ) -> Result<Self::Shared, String>;

    /// Build consumer `shard`'s engine, on that consumer's thread.
    ///
    /// # Errors
    /// A second consumer asking a single-consumer seam for the store.
    fn build(shared: &Self::Shared, shard: usize) -> Result<Self, String>;

    fn store(&self) -> &Self::Store;
    fn journal_len(&self) -> u64;

    /// # Errors
    /// A checkpoint that failed.
    fn checkpoint(&self) -> Result<(), String>;
    fn evict(&self, budget: usize);
}

/// The direct seam: the engine in the consumer's own thread, one consumer.
impl ShopEngine for PageStore {
    type Shared = Mutex<Option<Self>>;
    type Store = Self;

    /// One, always. There is exactly one `PageStore` per process, and it
    /// cannot be in two threads at once.
    fn consumers(_asked: usize) -> usize {
        1
    }

    fn share(store: Self, _consumers: usize) -> Result<Self::Shared, String> {
        Ok(Mutex::new(Some(store)))
    }

    fn build(shared: &Self::Shared, shard: usize) -> Result<Self, String> {
        shared
            .lock()
            .map_err(|_| "the store mutex was poisoned".to_string())?
            .take()
            .ok_or_else(|| {
                format!(
                    "consumer {shard} asked for a second store: the direct \
                     seam is one `PageStore` in one thread"
                )
            })
    }

    fn store(&self) -> &Self {
        self
    }

    fn journal_len(&self) -> u64 {
        Self::journal_len(self)
    }

    fn checkpoint(&self) -> Result<(), String> {
        self.commit_journal()
            .map_err(|e| format!("checkpoint: {e}"))
    }

    fn evict(&self, budget: usize) {
        self.evict_settled(budget);
    }
}

/// The sharded seam: one disk actor owning the engine, N caching shards over
/// it — one per consumer, each on its own thread.
pub struct ShopShard {
    store: Rc<ShardStore>,
    disk: wavedb_quick_node::shard::DiskHandle,
}

impl ShopEngine for ShopShard {
    /// `Arc` because every consumer needs the handle **and** the row needs to
    /// outlive them to shut the actor down.
    type Shared = Arc<Shards>;
    type Store = ShardStore;

    /// Whatever was asked. This is the seam the consumer count exists for.
    fn consumers(asked: usize) -> usize {
        asked.max(1)
    }

    fn share(
        store: PageStore,
        consumers: usize,
    ) -> Result<Self::Shared, String> {
        // `count` is the routing width the node would use. It is passed
        // through rather than fixed at 1 (which is what the `micro` row does)
        // because here the consumers really are shards: each holds its own
        // cache over a disjoint set of tenants.
        Shards::start(store, consumers)
            .map(Arc::new)
            .map_err(|e| format!("start shards: {e}"))
    }

    fn build(shared: &Self::Shared, _shard: usize) -> Result<Self, String> {
        let disk = shared.handle();
        Ok(Self {
            // Its own cache, which is the whole point: a cacheless store
            // would make every read a round trip and measure the channel.
            store: Rc::new(ShardStore::with_budget(
                disk.clone(),
                wavedb_quick_node::shard::CACHE_BYTES,
            )),
            disk,
        })
    }

    fn store(&self) -> &ShardStore {
        &self.store
    }

    fn journal_len(&self) -> u64 {
        block_on(self.disk.stats()).map_or(0, |s| s.journal_bytes)
    }

    fn checkpoint(&self) -> Result<(), String> {
        block_on(
            self.disk
                .maintain(wavedb_quick_node::shard::Maintenance::Checkpoint),
        )
        .map_err(|e| format!("checkpoint: {e}"))
    }

    fn evict(&self, budget: usize) {
        let _ = block_on(self.disk.maintain(
            wavedb_quick_node::shard::Maintenance::Evict {
                budget_bytes: budget,
            },
        ));
    }
}
