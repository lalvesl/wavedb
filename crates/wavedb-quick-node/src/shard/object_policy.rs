//! The object store's share of a worker's upkeep policy (RFC 0070): when a
//! store's disk sheds its coldest pages, and when it uploads. Split from
//! [`super::owned`], which applies them.

use wavedb_storage::{BLOCK_SIZE, PageStore};

use crate::upkeep::Maintenance;

/// An object-backed store's disk is a cache bounded by the compaction
/// ceiling (RFC 0070 §7): past its share, the coldest uploaded pages leave
/// it, down to nine tenths of the share, and the ceiling trigger then
/// compacts the room they left out of the file.
///
/// In object mode the "disk" is RAM, and its ceiling the policy's
/// [`object_ram_bytes`](Maintenance::object_ram_bytes).
pub fn evict_past_ceiling(
    store: &PageStore,
    policy: &Maintenance,
    partitions: usize,
) {
    if !store.is_object_backed() {
        return;
    }
    let ceiling = if store.is_object_mode() {
        Some(policy.object_ram_bytes)
    } else {
        policy.compaction.above_bytes
    };
    let Some(ceiling) = ceiling else {
        return;
    };
    let share = ceiling / u64::try_from(partitions.max(1)).unwrap_or(u64::MAX);
    let share_blocks = share / BLOCK_SIZE as u64;
    let space = store.space();
    let live = space.file_blocks - space.free_blocks - space.deferred_blocks;
    if live <= share_blocks {
        return;
    }
    if let Err(e) = store.evict_cold(share_blocks / 10 * 9) {
        eprintln!("worker: eviction failed: {e}");
    }
}

/// Whether `store` should upload now: it holds back the policy's bytes, or
/// its oldest write no upload carried has waited the policy's interval.
pub fn upload_due(store: &PageStore, policy: &Maintenance) -> bool {
    store.pending_upload_bytes() >= policy.upload_after_bytes.max(1)
        || policy
            .upload_interval
            .zip(store.unuploaded_for())
            .is_some_and(|(interval, waited)| waited >= interval)
}
