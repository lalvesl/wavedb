//! What the WaveDB rows still need from the RFC 0060 adapter: how a store
//! is opened, and the notes a row carries.
//!
//! Everything that used to measure here now lives in
//! [`drivers::wavedb`](crate::systems::drivers::wavedb) and
//! [`row::embedded`](crate::row::embedded). Three constraints from the old
//! file survive as the reason `open` looks the way it does:
//!
//! - **One store per directory.** Every open is scoped so the directory's
//!   lock drops before the next open of it; the cold-read phase depends on it.
//! - **The typed path only.** Reads go through `CollectionHandle::get`, which
//!   routes by `STRUCT_HASH`. `Store::get` is an untyped fallback that probes
//!   every slot — a path no generated code takes.
//! - **Steady state.** The settle queue is drained and the journal
//!   checkpointed before a footprint, so the number is not just work
//!   postponed.

use std::path::Path;

use wavedb_storage::PageStore;

use crate::schema::Thing;

pub fn notes(seeded: bool) -> Vec<String> {
    let mut notes = vec![
        "Every collection op is one apply batch. The durable row writes \
         through the `_durable` operations, one fsync per batch; the relaxed \
         row through the standard ones, one fsync per elapsed window \
         (RFC 0072), the counterpart of the others' relaxed knobs."
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
    notes
}

/// The store. Its durability is not an option of it any more (RFC 0072):
/// the row's driver makes every write durable, or settles on a window
/// (`drivers::wavedb`).
pub fn open(dir: &Path) -> Result<PageStore, String> {
    PageStore::open(dir, &Thing::storage_entries())
        .map_err(|e| format!("open: {e}"))
}

/// The multi row's database: `shards` data shards, one per consumer, all
/// writing through **one** storage shard (RFC 0073) — the shop's shape, and
/// a node's in miniature.
pub fn open_shards(
    dir: &Path,
    shards: usize,
) -> Result<Vec<PageStore>, String> {
    let options = wavedb_storage::StoreOptions {
        storage_shards: 1,
        ..wavedb_storage::StoreOptions::default()
    };
    wavedb_storage::open_shards(dir, &Thing::storage_entries(), options, shards)
        .map_err(|e| format!("open: {e}"))
}
