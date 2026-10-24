//! Thread per core: each worker owns its data shards ([RFC 0067]).
//!
//! ```text
//!   edge (accept thread) ── route by owner ──> worker w  (owns shards p with
//!                                               │          p · T / P = w)
//!                                               ├─ jobs: operations on its
//!                                               │  owners, as calls on the
//!                                               │  owner's PageStore
//!                                               └─ BG: settle / checkpoint
//!                                                  steps, upkeep policy
//! ```
//!
//! A database is `P` data shards, each a whole `PageStore` (`MANIFEST` +
//! `shards/xx`); an owner — a collection's Pivot, or a Unique type under a
//! tenant — lives in shard `seahash(owner) % P`. `T` workers each own a
//! contiguous range of the shards, **stores included**: an operation reaches
//! its engine as a function call on the thread that owns it, and nothing but
//! the [`Job`] that brought it crosses a thread. The single disk actor this
//! replaced is gone (RFC 0067 step 4).
//!
//! What crosses: a [`Job`] in (plain data — a command frame or a
//! [`NodeOp`](wavedb_core::NodeOp)), its answer back, committed mutations to
//! the accept thread's publisher, and at shutdown each worker's report of
//! closing its stores.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

mod background;
mod lock;
mod owned;
pub mod priority;
mod route;
mod router;
mod seed;
mod waiters;
mod worker;

pub use background::{Background, UPKEEP_PERIOD, background};
pub use lock::{Acquire, Held, OwnerKey, OwnerLocks};
pub use owned::{Owned, SETTLE_BUDGET_IDS};
pub use priority::Priority;
pub use route::{Owner, shard_of};
pub use router::Router;
pub use seed::Seeder;
pub use waiters::MAX_WAITERS;
pub use worker::{
    Closed, JOB_DEPTH, Job, JobSlot, ShardCtx, Started, Worker, closed_all,
    job, shard,
};
