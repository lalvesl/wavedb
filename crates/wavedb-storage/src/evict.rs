//! Eviction from disk ([RFC 0070] §7, phase 4) — in an object-backed store
//! the disk is a cache, and a page the object store holds whole may leave it.
//!
//! - **What leaves**: the coldest pages whose flag is clear — the object
//!   store has everything in them. Cold is [`Heat`]: a stamp per disk bucket,
//!   moved on every page read and every page a settle round writes. A
//!   resident type's pages never leave (§10): what navigation reads stays,
//!   so only the record bytes it leads to are ever fetched.
//! - **What it leaves behind**: [`BlockDescriptor::EVICTED`], and a run
//!   freed like any superseded one: a hole once a batch whose headers name
//!   the eviction is durable, so no reconstruction ever names a cut run.
//! - **Coming back**: a read of an evicted bucket goes to the object store
//!   and marks the bucket wanted; a write restores it before it commits, so a
//!   settle round never meets an evicted bucket and a page on disk is always
//!   complete. A restored page is the object store's copy filtered to the
//!   bucket — exactly what the object store has, so its flag is clear.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::collections::BTreeSet;

use futures::future::try_join_all;
use wavedb_core::{Id, Write};

use crate::block::BlockDescriptor;
use crate::error::{StorageError, StorageResult};
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::plan::SlotPlan;
use crate::upload_plan::sharing;

/// How recently each disk bucket was used: a stamp per bucket from a clock
/// that ticks on every use, so eviction takes the least recently used first.
/// RAM only — after a reopen every bucket is equally cold.
#[derive(Debug, Default)]
pub struct Heat {
    clock: u32,
    /// Slot index → bucket → the clock at its last use.
    stamps: Vec<Vec<u32>>,
}

impl Heat {
    /// Bucket `bucket` of slot `idx` was just used.
    pub fn touch(&mut self, idx: usize, bucket: usize) {
        if self.clock == u32::MAX {
            // Halve everything: the order survives, the clock gets room.
            for stamp in self.stamps.iter_mut().flatten() {
                *stamp /= 2;
            }
            self.clock /= 2;
        }
        self.clock += 1;
        if self.stamps.len() <= idx {
            self.stamps.resize_with(idx + 1, Vec::new);
        }
        let slot = &mut self.stamps[idx];
        if slot.len() <= bucket {
            slot.resize(bucket + 1, 0);
        }
        slot[bucket] = self.clock;
    }

    /// When bucket `bucket` of slot `idx` was last used; `0` for never.
    #[must_use]
    pub fn stamp(&self, idx: usize, bucket: usize) -> u32 {
        self.stamps
            .get(idx)
            .and_then(|s| s.get(bucket))
            .copied()
            .unwrap_or(0)
    }
}

impl PageStore {
    /// Note a use of a disk bucket — nothing for a disk-only store.
    pub(crate) fn touch(&self, idx: usize, bucket: usize) {
        if let Some(object) = &self.object {
            object.heat.borrow_mut().touch(idx, bucket);
        }
    }

    /// Note an evicted bucket worth having on disk again.
    pub(crate) fn want(&self, idx: usize, bucket: usize) {
        if let Some(object) = &self.object {
            object.wanted.borrow_mut().insert((idx, bucket));
        }
    }

    /// Evict the coldest uploaded pages until the live blocks are at most
    /// `target_blocks` — what compaction then gives back to the filesystem.
    /// Returns the blocks evicted; `0` for a disk-only store.
    ///
    /// Everything committed is settled first, so no unsettled record routes
    /// to a bucket about to leave the disk.
    ///
    /// # Errors
    /// The settle's faults; nothing is evicted then.
    pub fn evict_cold(&self, target_blocks: u64) -> StorageResult<u64> {
        let Some(object) = &self.object else {
            return Ok(0);
        };
        if self.live_blocks() <= target_blocks {
            return Ok(0);
        }
        self.drain()?;
        let mut live = self.live_blocks();
        let mut cold = Vec::new();
        {
            let heat = object.heat.borrow();
            // Navigation stays (§10): a walk never waits on the object store.
            let evictable = self
                .types
                .iter()
                .enumerate()
                .filter(|(_, s)| !s.is_resident());
            for (idx, slot) in evictable {
                let dir = slot.directory().borrow();
                let Some(dir) = dir.as_ref() else { continue };
                for (bucket, desc) in dir.slots().iter().enumerate() {
                    if desc.is_allocated() && !desc.pending_upload() {
                        cold.push((heat.stamp(idx, bucket), idx, bucket));
                    }
                }
            }
        }
        cold.sort_unstable();
        let mut evicted = 0;
        let mut unrecorded = self.unrecorded.borrow_mut();
        let mut alloc = self.alloc.borrow_mut();
        for (_, idx, bucket) in cold {
            if live <= target_blocks {
                break;
            }
            let mut dir = self.types[idx].directory().borrow_mut();
            let Some(dir) = dir.as_mut() else { continue };
            let run = dir.descriptor(bucket).run();
            dir.set_descriptor(bucket, BlockDescriptor::EVICTED);
            self.file.page_cache().invalidate(run);
            self.retire_run(&mut alloc, run);
            unrecorded.slots.entry(idx).or_default().disk.insert(bucket);
            live = live.saturating_sub(run.count);
            evicted += run.count;
        }
        if !self.has_disk() {
            unrecorded.slots.clear(); // no header to record it in
        }
        Ok(evicted)
    }

    /// Blocks in use: the file, less what is free or held back.
    fn live_blocks(&self) -> u64 {
        let space = self.space();
        space.file_blocks - space.free_blocks - space.deferred_blocks
    }

    /// The evicted buckets `batch` writes to, by slot index.
    pub(crate) fn evicted_in(&self, batch: &[Write]) -> Vec<(usize, usize)> {
        let mut out = BTreeSet::new();
        if self.object.is_none() {
            return Vec::new();
        }
        for w in batch {
            let (hash, id) = match w {
                Write::Put(id, bytes) => match crate::apply::head_of(bytes) {
                    Some(hash) => (hash, *id),
                    None => continue,
                },
                Write::Remove(hash, id) | Write::Expect(hash, id, _) => {
                    (*hash, *id)
                }
            };
            let Some(idx) = self.slot_index(hash) else {
                continue;
            };
            let dir = self.types[idx].directory().borrow();
            let Some(dir) = dir.as_ref() else { continue };
            let bucket = dir.bucket_of(id.raw());
            if dir.descriptor(bucket).is_evicted() {
                out.insert((idx, bucket));
            }
        }
        out.into_iter().collect()
    }

    /// Bring evicted bucket `bucket` of slot `idx` back to disk: the object
    /// store's pages that share records with it, filtered to its own. A
    /// bucket some other caller restored meanwhile is left as it is.
    ///
    /// # Errors
    /// An object-store or page fault; the bucket stays evicted.
    pub(crate) async fn restore(
        &self,
        idx: usize,
        bucket: usize,
    ) -> StorageResult<()> {
        let slot = &self.types[idx];
        let shared: Vec<BlockDescriptor> = {
            let disk = slot.directory().borrow();
            let Some(disk) = disk.as_ref() else {
                return Ok(());
            };
            if !disk.descriptor(bucket).is_evicted() {
                return Ok(());
            }
            let remote = slot.remote_directory().borrow();
            let remote = remote.as_ref().ok_or(StorageError::Corrupt(
                "an evicted bucket with no remote directory",
            ))?;
            sharing(disk.len() as u64, bucket as u64, remote.len() as u64)
                .into_iter()
                .map(|s| remote.descriptor(s))
                .filter(|d| d.is_allocated())
                .collect()
        };
        let images = try_join_all(
            shared
                .into_iter()
                .map(|d| self.with_remote_page(idx, d, SlotPage::clone)),
        )
        .await?;
        let Some(disk) = slot.directory().borrow().clone() else {
            return Ok(());
        };
        if !disk.descriptor(bucket).is_evicted() {
            return Ok(());
        }
        let mut page = SlotPage::new(slot.struct_hash());
        for image in images {
            for (id, bytes) in image.into_entries() {
                if disk.bucket_of(id.raw()) == bucket {
                    page.upsert(id, bytes);
                }
            }
        }
        let image = {
            let dict = slot.dictionary().borrow();
            page.to_bytes(dict.dictionary(), dict.enabled())?
        };
        let mut plan = SlotPlan::verbatim(idx, disk);
        plan.pages.insert(bucket, image);
        plan.decoded.insert(bucket, page);
        self.place_restored(plan)?;
        self.touch(idx, bucket);
        if let Some(object) = &self.object {
            object.wanted.borrow_mut().remove(&(idx, bucket));
        }
        Ok(())
    }

    /// Restore up to `limit` of the wanted buckets — evicted ones a read
    /// had to fetch, or one holding back its type's next split. Returns how
    /// many came back.
    ///
    /// # Errors
    /// The first restore's fault; the rest stay wanted.
    pub async fn restore_wanted(&self, limit: usize) -> StorageResult<usize> {
        let Some(object) = &self.object else {
            return Ok(0);
        };
        let wanted: Vec<(usize, usize)> =
            object.wanted.borrow().iter().take(limit).copied().collect();
        for &(idx, bucket) in &wanted {
            self.restore(idx, bucket).await?;
            object.wanted.borrow_mut().remove(&(idx, bucket));
        }
        Ok(wanted.len())
    }

    /// `id`'s bytes from the object store, for a read whose disk bucket was
    /// evicted — which marks the bucket wanted back.
    pub(crate) async fn read_evicted(
        &self,
        idx: usize,
        bucket: usize,
        id: Id,
    ) -> StorageResult<Option<Vec<u8>>> {
        self.want(idx, bucket);
        self.read_remote(idx, id).await
    }

    /// Whether `id`'s disk bucket is on disk (not evicted) — a diagnostic.
    #[must_use]
    pub fn is_resident(&self, struct_hash: u64, id: Id) -> bool {
        self.slot_of(struct_hash).is_none_or(|slot| {
            slot.directory().borrow().as_ref().is_none_or(|dir| {
                !dir.descriptor(dir.bucket_of(id.raw())).is_evicted()
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Heat;

    #[test]
    fn the_least_recently_used_bucket_has_the_lowest_stamp() {
        let mut heat = Heat::default();
        heat.touch(0, 3);
        heat.touch(1, 0);
        heat.touch(0, 1);
        assert!(heat.stamp(0, 3) < heat.stamp(1, 0));
        assert!(heat.stamp(1, 0) < heat.stamp(0, 1));
        assert_eq!(heat.stamp(0, 2), 0, "never used");
        assert_eq!(heat.stamp(5, 9), 0);
    }

    /// The clock running out halves every stamp: the order survives, though
    /// two neighbours may come to tie.
    #[test]
    fn a_full_clock_halves_and_keeps_the_order() {
        let mut heat = Heat {
            clock: u32::MAX - 1,
            stamps: vec![vec![10, u32::MAX - 1]],
        };
        heat.touch(0, 2);
        heat.touch(0, 0);
        let (old, mid, new) =
            (heat.stamp(0, 1), heat.stamp(0, 2), heat.stamp(0, 0));
        assert!(old <= mid && mid < new, "{old} {mid} {new}");
        assert!(new < u32::MAX / 2 + 2);
    }
}
