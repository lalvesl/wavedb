//! What the WaveDB rows still need from the RFC 0060 adapter: how a store
//! is opened, and the notes a row carries.
//!
//! Everything that used to measure here now lives in
//! [`drivers::wavedb`](crate::systems::drivers::wavedb) and
//! [`row::embedded`](crate::row::embedded). Three constraints from the old
//! file survive as the reason `open` looks the way it does:
//!
//! - **One store per process.** Every open is scoped so the `EngineClaim`
//!   drops before the next one; the cold-read phase depends on it.
//! - **The typed path only.** Reads go through `CollectionHandle::get`, which
//!   routes by `STRUCT_HASH`. `Store::get` is an untyped fallback that probes
//!   every slot — a path no generated code takes.
//! - **Steady state.** The settle queue is drained and the journal
//!   checkpointed before a footprint, so the number is not just work
//!   postponed.

use std::path::Path;

use wavedb_storage::PageStore;

use super::Durability;
use super::engine::Engine;
use crate::schema::Thing;

pub fn notes(seeded: bool, engine: Engine) -> Vec<String> {
    let mut notes = vec![
        "Every collection op is one apply batch. The durable row takes one \
         fsync per batch; the relaxed row takes one per elapsed window \
         (RFC 0061), the counterpart of the others' relaxed knobs."
            .into(),
        "Retains every superseded version; the other systems retain none. Read \
         the update row beside the footprint, never alone."
            .into(),
        "Barrier count is not recorded: PageStore exposes no public IoCounts \
         accessor and RFC 0060 forbids changing a shipped crate."
            .into(),
    ];
    if seeded {
        notes.push(
            "Seeded run: no insert phase (the insert benchmark IS the fill), \
             and no read_hot phase — the per-type cache is a write cache that \
             reads never populate, so on a store nobody has just written to, \
             hot and cold are the same measurement. RFC 0044 is that gap."
                .into(),
        );
    }
    if engine == Engine::Sharded {
        notes.push(
            "Sharded row: the engine is owned by a disk actor on its own \
             thread and reached by message through a ShardStore. It is the \
             single/multi-thread axis, and it measures the cost of that \
             boundary — NOT parallelism. This benchmark issues one operation \
             at a time, so only one shard ever has work; and the brake keys \
             on (tenant, STRUCT_HASH), so one type under one tenant would be \
             one owner even under a concurrent client."
                .into(),
        );
        notes.push(
            "read_cold is NOT comparable to the direct row. ShardStore \
             memoises on read; the engine's per-type cache is a write cache \
             that reads never populate. So the sharded row has a read cache \
             the direct row does not — the gap RFC 0044 names, filled here as \
             a side effect of the shard owning its own cache. A faster \
             read_cold on this row is that difference, not a faster read path."
                .into(),
        );
        notes.push(
            "Measured cost of a ShardStore MISS: the same row run with a \
             16 MiB shard budget against a ~40 MB working set reported \
             read_hot at 96 175/s where the direct row reported 1 269 423/s. \
             The round trip is roughly an order of magnitude over an \
             in-process hit, so the row's result is governed by the shard's \
             hit rate — and ShardStore bounds itself by clearing the whole \
             cache rather than evicting (RFC 0044 is that gap). This row uses \
             the shipped 64 MiB default, which holds this working set."
                .into(),
        );
    }
    notes
}

/// Opened at the row's own durability (RFC 0061). Unlike the shop workload
/// there is no untimed preload to exempt here — the fill **is** the `insert`
/// phase — so every open in this adapter takes the row's window.
pub fn open(dir: &Path, d: Durability) -> Result<PageStore, String> {
    PageStore::open_with(
        dir,
        &Thing::storage_entries(),
        wavedb_storage::StoreOptions {
            relax_window: match d {
                Durability::Durable => std::time::Duration::ZERO,
                Durability::Relaxed => crate::RELAXED_WINDOW,
            },
        },
    )
    .map_err(|e| format!("open: {e}"))
}
