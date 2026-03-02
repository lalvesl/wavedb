//! The engine seam the shop's WaveDB rows are generic over.
//!
//! Two implementations, one per row of the single/multi axis, and no `dyn`:
//! each row monomorphises to its own concrete engine, the same rule the
//! workspace holds itself to.
//!
//! Both hand each consumer a `PageStore` of its own — the database is opened
//! as one data shard per consumer — and the difference is only how many:
//!
//! - **direct** (`wavedb/single`): one shard, one consumer;
//! - **sharded** (`wavedb/multi`): N shards, N consumers, each owning its
//!   shard's store on its own thread — the shape a node worker has since
//!   RFC 0067 step 4. Users are placed by `u % N`, so a user's records are on
//!   one thread, which is the property the node's owner hash provides.
//!
//! There is no `close`: the stores go down with the drivers that own them,
//! and the row decides when the footprint is taken.

use std::sync::Mutex;

use wavedb_core::Store;
use wavedb_storage::PageStore;

/// What a consumer's engine must offer.
pub trait ShopEngine: Sized {
    /// Built once by the row, shared across consumer threads.
    type Shared: Send + Sync;
    /// This consumer's own store.
    type Store: Store;

    /// How many consumers — and therefore data shards — this seam runs,
    /// given what was asked.
    fn consumers(asked: usize) -> usize;

    /// Take the row's data shards (one per consumer, in order) into whatever
    /// the seam shares.
    ///
    /// # Errors
    /// A shard count that is not the consumer count.
    fn share(
        stores: Vec<PageStore>,
        consumers: usize,
    ) -> Result<Self::Shared, String>;

    /// Build consumer `shard`'s engine, on that consumer's thread.
    ///
    /// # Errors
    /// A consumer asking for a shard that is gone or not there.
    fn build(shared: &Self::Shared, shard: usize) -> Result<Self, String>;

    fn store(&self) -> &Self::Store;

    /// Settle everything committed into a durable batch (RFC 0072).
    ///
    /// # Errors
    /// A settle that failed.
    fn drain(&self) -> Result<(), String>;
    fn evict(&self, budget: usize);
    /// Drop settled cache entries down to the budget the store was opened
    /// with — on a sharded database, this data shard's share of it.
    fn evict_to_budget(&self);
    /// Whether what is not settled has passed the store's pressure line
    /// (`StoreOptions::pressure_percent` of its record budget).
    fn under_pressure(&self) -> bool;
}

/// One data shard per consumer: consumer `i` takes store `i`, once.
type Slots = Mutex<Vec<Option<PageStore>>>;

fn slots(stores: Vec<PageStore>, consumers: usize) -> Result<Slots, String> {
    if stores.len() != consumers {
        return Err(format!(
            "{} data shards for {consumers} consumers: each consumer owns \
             exactly one",
            stores.len()
        ));
    }
    Ok(Mutex::new(stores.into_iter().map(Some).collect()))
}

fn take(shared: &Slots, shard: usize) -> Result<PageStore, String> {
    shared
        .lock()
        .map_err(|_| "the store mutex was poisoned".to_string())?
        .get_mut(shard)
        .and_then(Option::take)
        .ok_or_else(|| {
            format!("consumer {shard} asked for a shard that is not its own")
        })
}

/// The direct seam: one shard, one consumer, the engine in its thread.
impl ShopEngine for PageStore {
    type Shared = Slots;
    type Store = Self;

    /// One, always.
    fn consumers(_asked: usize) -> usize {
        1
    }

    fn share(
        stores: Vec<Self>,
        consumers: usize,
    ) -> Result<Self::Shared, String> {
        slots(stores, consumers)
    }

    fn build(shared: &Self::Shared, shard: usize) -> Result<Self, String> {
        take(shared, shard)
    }

    fn store(&self) -> &Self {
        self
    }

    fn drain(&self) -> Result<(), String> {
        Self::drain(self).map_err(|e| format!("settle: {e}"))
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
}

/// The sharded seam: N shards, each owned by one consumer thread.
pub struct ShopShard(PageStore);

impl ShopEngine for ShopShard {
    type Shared = Slots;
    type Store = PageStore;

    /// Whatever was asked. This is the seam the consumer count exists for.
    fn consumers(asked: usize) -> usize {
        asked.max(1)
    }

    fn share(
        stores: Vec<PageStore>,
        consumers: usize,
    ) -> Result<Self::Shared, String> {
        slots(stores, consumers)
    }

    fn build(shared: &Self::Shared, shard: usize) -> Result<Self, String> {
        take(shared, shard).map(Self)
    }

    fn store(&self) -> &PageStore {
        &self.0
    }

    fn drain(&self) -> Result<(), String> {
        self.0.drain().map_err(|e| format!("settle: {e}"))
    }

    fn evict(&self, budget: usize) {
        self.0.evict_settled(budget);
    }

    fn evict_to_budget(&self) {
        self.0.evict_to_budget();
    }

    fn under_pressure(&self) -> bool {
        self.0.under_pressure()
    }
}
