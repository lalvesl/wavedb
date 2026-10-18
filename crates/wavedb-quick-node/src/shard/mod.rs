//! Pivot-owned shards ([RFC 0064]) — the concurrency model, first slice.
//!
//! ```text
//!   route by pivot ──> shard i   (owns its cache; runs Collection/BpTree)
//!                        │
//!                        │  DiskRequest  (the only Send surface)
//!                        ▼
//!                   disk actor   (sole owner of the PageStore)
//! ```
//!
//! **What is built here:** the ownership boundary. One disk actor owns the
//! engine, shards reach it only by message, each shard keeps its own cache,
//! and `shard_of(pivot)` decides which shard an operation belongs to. Nothing
//! above `Store` changes.
//!
//! **What is not, and why.** RFC 0064's target splits the disk owner into a
//! journal actor and a page actor, and moves `StructStorage`'s per-type cache
//! into the shards. Both wait on the same open question — the settle path
//! straddles the boundary, reading shard-side caches to write page-side
//! dictionaries — so this slice draws the boundary where it can be drawn
//! correctly and leaves that seam alone.
//!
//! The shard count `N` is [`wavedb_platform::cpu::available`] by default: the
//! parallelism this process may actually use, which is the affinity mask and
//! the container quota rather than the machine's core count.
//!
//! [RFC 0064]: ../../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md

mod disk;
mod disk_work;
mod lock;
mod msg;
pub mod priority;
mod route;
mod router;
mod seed;
mod store;
mod waiters;
mod worker;

pub use disk::{
    DiskHandle, MAINTENANCE_DEPTH, QUEUE_DEPTH, start as start_disk,
};
pub use lock::{Acquire, Held, OwnerKey, OwnerLocks};
pub use msg::{DiskRequest, EngineStats, Maintenance};
pub use priority::Priority;
pub use route::{Owner, shard_of};
pub use router::Router;
pub use seed::Seeder;
pub use store::{CACHE_BYTES, ShardStore, ShardView};
pub use waiters::MAX_WAITERS;
pub use worker::{JOB_DEPTH, Job, JobSlot, ShardCtx, Worker, job, shard};

use std::rc::Rc;

use wavedb_storage::PageStore;

/// A running database: its data shards behind one disk actor, and the
/// worker threads that serve them.
///
/// A worker's own `ShardStore` is built **on that worker's thread**, because
/// it is non-`Send` and must not be constructed anywhere it could be moved
/// from; this holds only the handle it is built from.
pub struct Shards {
    disk: DiskHandle,
    /// Data shards (`P`, from the `MANIFEST`).
    partitions: usize,
    /// Worker threads, each serving a contiguous range of the data shards.
    threads: usize,
}

impl Shards {
    /// Take ownership of the database's shard `stores` into a disk actor and
    /// prepare `threads` workers over them.
    ///
    /// `threads` is clamped to `1..=stores.len()`: zero workers is not a
    /// degraded mode, it is a process that serves nothing, and a worker
    /// without a shard of its own would serve nothing either.
    ///
    /// # Errors
    /// The disk actor's thread failing to spawn.
    pub fn start(
        stores: Vec<PageStore>,
        threads: usize,
    ) -> wavedb_platform::Result<Self> {
        let partitions = stores.len().max(1);
        Ok(Self {
            disk: disk::start(stores)?,
            partitions,
            threads: threads.clamp(1, partitions),
        })
    }

    /// Workers sized to the parallelism this process actually has.
    ///
    /// # Errors
    /// The disk actor's thread failing to spawn.
    pub fn start_sized(
        stores: Vec<PageStore>,
    ) -> wavedb_platform::Result<Self> {
        Self::start(stores, wavedb_platform::cpu::available())
    }

    /// How many worker threads serve the shards.
    #[must_use]
    pub const fn threads(&self) -> usize {
        self.threads
    }

    /// How many data shards the database has.
    #[must_use]
    pub const fn shards(&self) -> usize {
        self.partitions
    }

    /// The data shard holding the collection `pivot`.
    #[must_use]
    pub fn owner(&self, pivot: wavedb_core::LocalId) -> usize {
        shard_of(pivot, self.partitions)
    }

    /// A **cacheless** store onto the disk actor, for code that is not a
    /// worker: node-side seeding before serving, and tests. Reach a data
    /// shard through [`ShardStore::at`].
    ///
    /// Cacheless is not a tuning choice, it is the correctness condition.
    /// [`ShardStore`]'s cache remembers absence, and that is sound only while
    /// exactly one holder reaches a given record — a second cache would keep
    /// serving a `None` after a shard's insert filled it. So the shards cache
    /// and nobody else does; every other holder forwards straight through.
    #[must_use]
    pub fn store(&self) -> Rc<ShardStore> {
        Rc::new(ShardStore::with_budget(self.disk.clone(), 0))
    }

    /// A handle for a thread that will build its own store later.
    #[must_use]
    pub fn handle(&self) -> DiskHandle {
        self.disk.clone()
    }

    /// Ask for low-priority maintenance. Returns immediately — the caller is
    /// by definition not waiting, which is what makes it low priority.
    pub fn hint(&self, work: Maintenance) {
        self.disk.hint(work);
    }
}
