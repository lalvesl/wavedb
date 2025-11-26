//! Eviction from disk ([RFC 0070] §7, phase 4) — in an object-backed store
//! the disk is a cache, and a page the object store holds whole may leave it.
//!
//! - **What leaves**: the coldest pages whose [`PENDING`] flag is clear —
//!   the object store has everything in them. Cold is [`Heat`]: a stamp per
//!   disk page, moved on every page read and every page a settle round
//!   writes. A resident type's pages never leave (§10): what navigation
//!   reads stays, so only the record bytes it leads to are ever fetched.
//! - **What it leaves behind**: the page's entry, flagged [`EVICTED`] — its
//!   keys still route to it — and a run freed like any superseded one: a
//!   hole once a batch whose headers name the eviction is durable, so no
//!   reconstruction ever names a cut run.
//! - **Coming back**: a read of an evicted page goes to the object store and
//!   marks the page wanted; a write restores it before it commits, so a
//!   settle round never meets an evicted page and a page on disk is always
//!   complete. A restored page is the object store's records within the
//!   page's coverage (RFC 0078) — exactly what the object store has, so its
//!   flag is clear.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::collections::{BTreeMap, BTreeSet};

use futures::future::try_join_all;
use wavedb_core::{Id, Write};

use crate::error::{StorageError, StorageResult};
use crate::ordered::Found;
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::pages::{
    EVICTED, Located, PENDING, coverage, entries_between, key_of_id, locate,
    run_of, within,
};
use crate::plan::{PlannedPage, SlotPlan, image_of};

/// How recently each disk page was used: a stamp per page — by slot and
/// first block — from a clock that ticks on every use, so eviction takes
/// the least recently used first. RAM only — after a reopen every page is
/// equally cold.
#[derive(Debug, Default)]
pub struct Heat {
    clock: u32,
    stamps: BTreeMap<(usize, u64), u32>,
}

impl Heat {
    /// The page of slot `idx` at block `start` was just used.
    pub fn touch(&mut self, idx: usize, start: u64) {
        if self.clock == u32::MAX {
            // Halve everything: the order survives, the clock gets room.
            for stamp in self.stamps.values_mut() {
                *stamp /= 2;
            }
            self.clock /= 2;
        }
        self.clock += 1;
        self.stamps.insert((idx, start), self.clock);
    }

    /// When the page of slot `idx` at block `start` was last used; `0` for
    /// never.
    #[must_use]
    pub fn stamp(&self, idx: usize, start: u64) -> u32 {
        self.stamps.get(&(idx, start)).copied().unwrap_or(0)
    }

    /// The page at `start` left its run: its stamp goes with it.
    pub fn forget(&mut self, idx: usize, start: u64) {
        self.stamps.remove(&(idx, start));
    }
}

impl PageStore {
    /// Note a use of the disk page of slot `idx` at block `start` — nothing
    /// for a disk-only store.
    pub(crate) fn touch(&self, idx: usize, start: u64) {
        if let Some(object) = &self.object {
            object.heat.borrow_mut().touch(idx, start);
        }
    }

    /// Note an evicted page — named by a key of its — worth having on disk
    /// again.
    pub(crate) fn want(&self, idx: usize, key: Vec<u8>) {
        if let Some(object) = &self.object {
            object.wanted.borrow_mut().insert((idx, key));
        }
    }

    /// Evict the coldest uploaded pages until the live blocks are at most
    /// `target_blocks` — what compaction then gives back to the filesystem.
    /// Returns the blocks evicted; `0` for a disk-only store.
    ///
    /// Everything committed is settled first, so no unsettled record routes
    /// to a page about to leave the disk.
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
                let index = slot.directory().borrow();
                let Some(index) = index.as_ref() else {
                    continue;
                };
                for (at, page) in index.pages() {
                    if page.meta & (PENDING | EVICTED) == 0
                        && let Some(key) = index.key_of(at)
                    {
                        let stamp = heat.stamp(idx, page.address);
                        cold.push((stamp, idx, key));
                    }
                }
            }
        }
        cold.sort_unstable();
        let mut evicted = 0;
        for (_, idx, key) in cold {
            if live <= target_blocks {
                break;
            }
            let mut index = self.types[idx].directory().borrow_mut();
            let Some(index) = index.as_mut() else {
                continue;
            };
            let Found::Page { at, page } = index.find(&key) else {
                continue;
            };
            let run = run_of(page);
            index.set_meta(at, page.meta | EVICTED);
            self.file.page_cache().invalidate(run);
            object.heat.borrow_mut().forget(idx, run.start);
            self.retire_run(run);
            live = live.saturating_sub(run.count);
            evicted += run.count;
        }
        if !self.has_disk() {
            // No header to record it in.
            for slot in &self.types {
                if let Some(index) = slot.directory().borrow_mut().as_mut() {
                    let _ = index.drain(usize::MAX);
                }
            }
        }
        Ok(evicted)
    }

    /// Blocks in use: the file, less what is free or held back.
    fn live_blocks(&self) -> u64 {
        let space = self.space();
        space.file_blocks - space.free_blocks - space.deferred_blocks
    }

    /// The evicted pages `batch` writes to, by slot index and a key of
    /// theirs.
    pub(crate) fn evicted_in(&self, batch: &[Write]) -> Vec<(usize, Vec<u8>)> {
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
            let index = self.types[idx].directory().borrow();
            let Some(index) = index.as_ref() else {
                continue;
            };
            if let Located::Page { at, flags, .. } = locate(index, id)
                && flags & EVICTED != 0
                && let Some(key) = index.key_of(at)
            {
                out.insert((idx, key));
            }
        }
        out.into_iter().collect()
    }

    /// Bring the evicted page of slot `idx` that `key` finds back to disk:
    /// the object store's records within its coverage. A page some other
    /// caller restored meanwhile is left as it is.
    ///
    /// # Errors
    /// An object-store or page fault; the page stays evicted.
    pub(crate) async fn restore(
        &self,
        idx: usize,
        key: &[u8],
    ) -> StorageResult<()> {
        let slot = &self.types[idx];
        let Some((lo, hi, runs)) = self.evicted_cover(idx, key)? else {
            return Ok(());
        };
        let images = try_join_all(
            runs.into_iter()
                .map(|run| self.with_remote_page(idx, run, SlotPage::clone)),
        )
        .await?;
        // Through the gate from the copy of the index to its publishing: a
        // settle round waiting for its window would overwrite it.
        let _through = self.gate.enter().await;
        let Some(index) = slot.directory().borrow().clone() else {
            return Ok(());
        };
        let Found::Page { page, .. } = index.find(key) else {
            return Ok(());
        };
        if page.meta & EVICTED == 0 {
            return Ok(());
        }
        let mut restored = SlotPage::new(slot.struct_hash());
        for image in images {
            for (id, bytes) in image.iter() {
                if within(&key_of_id(id), &lo, hi.as_deref()) {
                    restored.upsert(id, bytes.to_vec());
                }
            }
        }
        let image = image_of(&mut restored, &slot.dictionary().borrow())?;
        let mut plan = SlotPlan::verbatim(idx, index);
        plan.pages.push(PlannedPage {
            key: lo,
            image,
            decoded: Some(restored),
            flags: 0,
        });
        self.place_restored(plan).await?;
        if let Some(object) = &self.object {
            object.wanted.borrow_mut().remove(&(idx, key.to_vec()));
        }
        Ok(())
    }

    /// The coverage of the evicted page of slot `idx` that `key` finds, and
    /// the runs of the remote pages meeting it; `None` when the page is not
    /// evicted (any more).
    #[allow(clippy::type_complexity)]
    fn evicted_cover(
        &self,
        idx: usize,
        key: &[u8],
    ) -> StorageResult<Option<(Vec<u8>, Option<Vec<u8>>, Vec<crate::block::Run>)>>
    {
        let slot = &self.types[idx];
        let disk = slot.directory().borrow();
        let Some(disk) = disk.as_ref() else {
            return Ok(None);
        };
        let Found::Page { at, page } = disk.find(key) else {
            return Ok(None);
        };
        if page.meta & EVICTED == 0 {
            return Ok(None);
        }
        let (lo, hi) = coverage(disk, at)
            .ok_or(StorageError::Corrupt("an evicted page without a key"))?;
        let remote = slot.remote_directory().borrow();
        let remote = remote.as_ref().ok_or(StorageError::Corrupt(
            "an evicted page with no remote index",
        ))?;
        let runs = entries_between(remote, &lo, hi.as_deref())
            .into_iter()
            .filter_map(|(_, page)| page.map(run_of))
            .collect();
        Ok(Some((lo, hi, runs)))
    }

    /// Restore up to `limit` of the wanted pages — evicted ones a read had
    /// to fetch. Returns how many came back.
    ///
    /// # Errors
    /// The first restore's fault; the rest stay wanted.
    pub async fn restore_wanted(&self, limit: usize) -> StorageResult<usize> {
        let Some(object) = &self.object else {
            return Ok(0);
        };
        let wanted: Vec<(usize, Vec<u8>)> =
            object.wanted.borrow().iter().take(limit).cloned().collect();
        for (idx, key) in &wanted {
            self.restore(*idx, key).await?;
            object.wanted.borrow_mut().remove(&(*idx, key.clone()));
        }
        Ok(wanted.len())
    }

    /// `id`'s bytes from the object store, for a read whose disk page was
    /// evicted — which marks the page wanted back.
    pub(crate) async fn read_evicted(
        &self,
        idx: usize,
        key: Vec<u8>,
        id: Id,
    ) -> StorageResult<Option<Vec<u8>>> {
        self.want(idx, key);
        self.read_remote(idx, id).await
    }

    /// Whether `id`'s disk page is on disk (not evicted) — a diagnostic.
    #[must_use]
    pub fn is_resident(&self, struct_hash: u64, id: Id) -> bool {
        self.slot_of(struct_hash).is_none_or(|slot| {
            slot.directory().borrow().as_ref().is_none_or(|index| {
                !matches!(
                    locate(index, id),
                    Located::Page { flags, .. } if flags & EVICTED != 0
                )
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Heat;

    #[test]
    fn the_least_recently_used_page_has_the_lowest_stamp() {
        let mut heat = Heat::default();
        heat.touch(0, 3);
        heat.touch(1, 0);
        heat.touch(0, 1);
        assert!(heat.stamp(0, 3) < heat.stamp(1, 0));
        assert!(heat.stamp(1, 0) < heat.stamp(0, 1));
        assert_eq!(heat.stamp(0, 2), 0, "never used");
        assert_eq!(heat.stamp(5, 9), 0);
        heat.forget(0, 1);
        assert_eq!(heat.stamp(0, 1), 0, "its run went");
    }

    /// The clock running out halves every stamp: the order survives, though
    /// two neighbours may come to tie.
    #[test]
    fn a_full_clock_halves_and_keeps_the_order() {
        let mut heat = Heat {
            clock: u32::MAX - 1,
            stamps: [((0, 0), 10), ((0, 1), u32::MAX - 1)].into(),
        };
        heat.touch(0, 2);
        heat.touch(0, 0);
        let (old, mid, new) =
            (heat.stamp(0, 1), heat.stamp(0, 2), heat.stamp(0, 0));
        assert!(old <= mid && mid < new, "{old} {mid} {new}");
        assert!(new < u32::MAX / 2 + 2);
    }
}
