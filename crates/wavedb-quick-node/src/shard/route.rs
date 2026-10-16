//! Which shard owns an operation: `wavedb_core::placement` ([RFC 0067] §1),
//! named here where the node routes by it.
//!
//! One function decides both the worker an operation runs on and — once the
//! shards are separate stores — the store that holds it, so the two can never
//! disagree. It lives in `wavedb-core` because it is an on-disk address, frozen
//! by golden vectors there.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

pub use wavedb_core::placement::{Owner, shard_of};
