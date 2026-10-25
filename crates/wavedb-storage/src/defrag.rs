//! Free-space defragmentation ([RFC 0042]) — relocating isolated live pages so
//! free extents coalesce into windows a whole checkpoint can land in.
//!
//! Copy-on-write page rewrites free the old run and allocate elsewhere, so
//! `data.bin` accumulates holes whose *total* size stays ample while the
//! *largest contiguous* one shrinks. Once no hole fits a checkpoint's window
//! ([RFC 0041]) every checkpoint grows the tail and the file grows
//! monotonically over mostly-free space.
//!
//! The defragmenter fixes the shape, not the size: it picks live page runs
//! whose neighbours are already free — moving one merges the gap on its left,
//! the run itself, and the gap on its right into a single extent — copies them
//! **forward to the tail** (best-fit would cheerfully drop a page back beside
//! the hole it was vacating), and repoints their descriptors. The bytes are
//! moved verbatim: a page is position-independent, so nothing is decoded,
//! recompressed, or re-planned.
//!
//! Safety falls out of what the checkpoint already guarantees. The relocation
//! rides the same window write, the vacated runs are freed through the
//! allocator's protected set — so a run the last durable `Commit` still names
//! is only released after the next one — and the directory is merely marked
//! dirty, leaving the last committed delta pointing at the old, still-intact
//! locations until a checkpoint rewrites it.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md
//! [RFC 0042]: ../../../rfcs/0042-free-space-defragmentation.md

use std::collections::BTreeMap;

use crate::block::Run;
use crate::error::StorageResult;
use crate::page_store::PageStore;
use crate::plan::SlotPlan;

/// What one defragmentation pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DefragReport {
    /// Pages relocated.
    pub moved_pages: usize,
    /// Blocks copied (the pass's IO cost, in 4 KiB units).
    pub moved_blocks: u64,
    /// Largest free extent before and after, in blocks.
    pub largest_before: u64,
    pub largest_after: u64,
}

/// A page worth relocating, and what moving it buys.
struct Candidate {
    slot: usize,
    bucket: usize,
    run: Run,
    /// Blocks that coalesce into one extent once this run moves away.
    coalesced: u64,
}

impl PageStore {
    /// Blocks in the largest free extent — "would the next checkpoint's window
    /// fit a hole?", the gauge a defrag policy triggers on.
    #[must_use]
    pub fn largest_free_extent(&self) -> u64 {
        self.alloc.borrow_mut().largest_free_extent()
    }

    /// Relocate up to `budget_blocks` worth of isolated pages, coalescing the
    /// space they vacate. Returns what the pass moved.
    ///
    /// Pages are chosen by how much free space their departure merges per
    /// block copied, so a small page stranded between two large holes goes
    /// first. A pass that finds nothing worth moving is free.
    ///
    /// # Errors
    /// Propagates read/write faults. A fault leaves the descriptors untouched:
    /// nothing is repointed until the window write returns.
    pub fn defragment(
        &self,
        budget_blocks: u64,
    ) -> StorageResult<DefragReport> {
        let largest_before = self.largest_free_extent();
        let mut chosen = self.candidates();
        // Best value first: merged blocks per block copied.
        chosen.sort_by(|a, b| {
            (b.coalesced * a.run.count).cmp(&(a.coalesced * b.run.count))
        });

        let mut plans: BTreeMap<usize, SlotPlan> = BTreeMap::new();
        let mut moved_blocks = 0;
        let mut moved_pages = 0;
        for candidate in chosen {
            if moved_blocks + candidate.run.count > budget_blocks {
                break;
            }
            let Some(bytes) = self.page_bytes(candidate.run)? else {
                continue; // an unreadable length prefix: leave it alone
            };
            let plan = match plans.entry(candidate.slot) {
                std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::btree_map::Entry::Vacant(v) => {
                    let Some(dir) = self.types[candidate.slot]
                        .directory()
                        .borrow_mut()
                        .clone()
                    else {
                        continue;
                    };
                    v.insert(SlotPlan::verbatim(candidate.slot, dir))
                }
            };
            plan.pages.insert(candidate.bucket, bytes);
            moved_blocks += candidate.run.count;
            moved_pages += 1;
        }

        if !plans.is_empty() {
            self.relocate(plans.into_values().collect())?;
        }
        Ok(DefragReport {
            moved_pages,
            moved_blocks,
            largest_before,
            largest_after: self.largest_free_extent(),
        })
    }

    /// Every live page run that has free space adjacent to it, scored by how
    /// much would coalesce if it moved.
    // Both guards span their loops by design: the allocator's free map and a
    // slot's directory must be read as one consistent picture, or a candidate
    // could be scored against neighbours that no longer exist.
    #[allow(clippy::significant_drop_tightening)]
    fn candidates(&self) -> Vec<Candidate> {
        let alloc = self.alloc.borrow_mut();
        let end_of_file = alloc.total_blocks();
        let mut out = Vec::new();
        for (slot, storage) in self.types.iter().enumerate() {
            let dir = storage.directory().borrow_mut();
            let Some(dir) = dir.as_ref() else { continue };
            for (bucket, desc) in dir.slots().iter().enumerate() {
                if !desc.is_allocated() {
                    continue;
                }
                let run = desc.run();
                let (before, after) = alloc.free_neighbours(run);
                let coalesced = before + run.count + after;
                // Two guards against churn. A run must merge at least twice
                // what copying it costs — otherwise it is only being shifted
                // sideways into the hole next door…
                if coalesced < run.count.saturating_mul(2) {
                    continue;
                }
                // …and a run backing onto the file's trailing free space would
                // be "moved" to almost exactly where it already is, gaining
                // nothing and re-dirtying its page every pass.
                if run.end() + after == end_of_file {
                    continue;
                }
                out.push(Candidate {
                    slot,
                    bucket,
                    run,
                    coalesced,
                });
            }
        }
        out
    }

    /// A page's stored bytes, trimmed to its own length prefix so the copy
    /// carries no padding.
    fn page_bytes(&self, run: Run) -> StorageResult<Option<Vec<u8>>> {
        let mut buf = self.file.read_run(run)?;
        let Some(prefix) = buf.get(..4).and_then(|s| s.try_into().ok()) else {
            return Ok(None);
        };
        let len = 4 + u32::from_le_bytes(prefix) as usize;
        if len > buf.len() {
            return Ok(None);
        }
        buf.truncate(len);
        Ok(Some(buf))
    }
}

#[cfg(test)]
mod tests {
    use crate::block::Run;

    #[test]
    fn moving_an_isolated_run_merges_its_neighbours() {
        // The allocator's view is what selection reads: a live run with free
        // space on both sides is the cheapest thing to move.
        let mut alloc = crate::block::BlockAllocator::new();
        let left = alloc.alloc(4); // [0,4)
        let live = alloc.alloc(1); // [4,5)  ← the straggler
        let right = alloc.alloc(4); // [5,9)
        let _keep = alloc.alloc(1); // [9,10) keeps the tail busy
        alloc.free(left);
        alloc.free(right);

        assert_eq!(alloc.free_neighbours(live), (4, 4));
        assert_eq!(alloc.largest_free_extent(), 4, "two 4-block holes");
        // Vacating it coalesces 4 + 1 + 4.
        alloc.free(live);
        assert_eq!(alloc.largest_free_extent(), 9);
        assert_eq!(alloc.free_extent_count(), 1);
    }

    #[test]
    fn tail_allocation_never_lands_in_a_hole() {
        let mut alloc = crate::block::BlockAllocator::new();
        let hole = alloc.alloc(8); // [0,8)
        let _live = alloc.alloc(1); // [8,9)
        alloc.free(hole);
        // Best-fit would reuse [0,8) — the tail strategy must not.
        let run = alloc.alloc_tail(2);
        assert_eq!(run, Run::new(9, 2));
        assert_eq!(alloc.largest_free_extent(), 8, "the hole is untouched");
    }

    #[test]
    fn a_packed_run_is_not_a_candidate() {
        let mut alloc = crate::block::BlockAllocator::new();
        let _a = alloc.alloc(2); // [0,2)
        let live = alloc.alloc(2); // [2,4) — live neighbours on both sides
        let _b = alloc.alloc(2); // [4,6)
        assert_eq!(alloc.free_neighbours(live), (0, 0));
    }
}
