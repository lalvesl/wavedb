//! Thread per core: each worker owns its data shards and serves its own
//! connections ([RFC 0067]).
//!
//! ```text
//!   listener w (SO_REUSEPORT) ──> worker w  (owns shards p with p · T / P = w)
//!                                   │
//!                                   ├─ EDGE: its connections — gate 1,
//!                                   │  `#[server]` bodies; every operation
//!                                   │  routed by owner, to this worker or
//!                                   │  another
//!                                   ├─ jobs: operations on its owners, as
//!                                   │  calls on the owner's PageStore, and
//!                                   │  the subscriptions to its topics
//!                                   └─ BG: settle / checkpoint steps,
//!                                      upkeep policy
//! ```
//!
//! A database is `P` data shards, each a whole `PageStore` (`MANIFEST` +
//! `shards/xx`); an owner — a collection's Pivot, or a Unique type under a
//! tenant — lives in shard `seahash(owner) % P`. `T` workers each own a
//! contiguous range of the shards, **stores included**: an operation reaches
//! its engine as a function call on the thread that owns it, and nothing but
//! the [`Job`] that brought it crosses a thread.
//!
//! What crosses: a [`Job`] in (plain data — a command frame, a
//! [`NodeOp`](wavedb_core::NodeOp), or a session's outbox to subscribe), its
//! answer back, a committed mutation's event into a subscriber's outbox, and
//! at shutdown each worker's report of closing its stores.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

mod background;
mod lease;
mod lock;
mod object_policy;
mod owned;
pub mod priority;
mod route;
mod router;
mod seed;
mod spawn;
mod waiters;
mod worker;

pub use background::{Background, UPKEEP_PERIOD, background};
pub use lease::{Lease, Leased};
pub use lock::{Acquire, Held, OwnerKey, OwnerLocks};
pub use owned::{Owned, SETTLE_BUDGET_IDS};
pub use priority::Priority;
pub use route::{Owner, shard_of};
pub use router::Router;
pub use seed::Seeder;
pub use spawn::{Closed, Closing, JOB_DEPTH, ShardCtx, closed_all, start};
pub use waiters::MAX_WAITERS;
pub use worker::{Job, JobSlot, Worker, job, shard};
