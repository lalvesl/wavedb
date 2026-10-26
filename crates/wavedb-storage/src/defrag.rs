//! Compaction's moves ([RFC 0069]) — which runs go down, into which hole,
//! one round (one window write) at a time. The phases around them, and the
//! public surface, are [`crate::compact`]'s.
//!
//! [RFC 0069]: ../../../rfcs/0069-compaction-into-the-holes.md

use std::collections::BTreeMap;

use crate::block::{BlockAllocator, Run};
use crate::edit;
use crate::error::StorageResult;
use crate::page_store::PageStore;
use crate::plan::{SlotPlan, blocks_of};

/// Images one round moves at most, so its delta chunk stays inside the one
/// block the round keeps for it (a moved page costs it 12 bytes).
const ROUND_IMAGES: usize = 128;

/// A run worth moving down.
#[derive(Clone, Copy)]
struct Movable {
    slot: usize,
    /// The page's bucket; `None` for the type's dictionary.
    bucket: Option<usize>,
    run: Run,
    /// Blocks its image takes once moved.
    blocks: u64,
}

impl PageStore {
    /// One pass: rounds until about `budget` blocks moved. `finished` once no
    /// run can move down at all.
    pub(crate) fn shift(
        &self,
        budget: u64,
    ) -> StorageResult<(usize, u64, bool)> {
        let mut movable = self.movable();
        let (mut images, mut blocks) = (0, 0);
        loop {
            if self.meta.borrow().wants_snapshot() {
                self.rewrite_addressing()?;
            }
            let found = next_round(&self.alloc.borrow(), &movable);
            let Some((limit, mut picked)) = found else {
                return Ok((images, blocks, true));
            };
            // The budget bounds the pass, not progress: a pass moves at least
            // one image, however large.
            let mut taken = blocks;
            picked.retain(|&i| {
                let fits = taken == 0 || taken + movable[i].blocks <= budget;
                taken += if fits { movable[i].blocks } else { 0 };
                fits
            });
            if picked.is_empty() {
                return Ok((images, blocks, false));
            }
            let mut loaded = Vec::with_capacity(picked.len());
            for &i in &picked {
                if let Some(bytes) = self.image_of(movable[i])? {
                    loaded.push((movable[i], bytes));
                }
            }
            if !self.move_round(&mut loaded, limit)? {
                return Ok((images, blocks, true)); // even one would not fit
            }
            images += loaded.len();
            blocks += loaded.iter().map(|(m, _)| m.blocks).sum::<u64>();
            for i in picked.into_iter().rev() {
                movable.remove(i);
            }
        }
    }

    /// Write `loaded` below `limit` as one round, carrying one image fewer
    /// each time the round (with its delta) outgrows the hole. `false` when
    /// not even one fits; `loaded` is then empty.
    fn move_round(
        &self,
        loaded: &mut Vec<(Movable, Vec<u8>)>,
        limit: u64,
    ) -> StorageResult<bool> {
        while !loaded.is_empty() {
            let mut plans: BTreeMap<usize, SlotPlan> = BTreeMap::new();
            for (m, bytes) in loaded.iter() {
                let Some(dir) = self.types[m.slot].directory().borrow().clone()
                else {
                    continue;
                };
                let plan = plans
                    .entry(m.slot)
                    .or_insert_with(|| SlotPlan::verbatim(m.slot, dir));
                match m.bucket {
                    Some(bucket) => {
                        plan.pages.insert(bucket, bytes.clone());
                    }
                    None => plan.dict = Some(bytes.clone()),
                }
            }
            if self.relocate(plans.into_values().collect(), limit)? {
                return Ok(true);
            }
            loaded.pop();
        }
        Ok(false)
    }

    /// Every page and dictionary run, highest first.
    fn movable(&self) -> Vec<Movable> {
        let mut out = Vec::new();
        for (slot, ts) in self.types.iter().enumerate() {
            let dir = ts.directory().borrow();
            let Some(dir) = dir.as_ref() else { continue };
            for (bucket, desc) in dir.slots().iter().enumerate() {
                if desc.is_allocated() {
                    out.push(Movable {
                        slot,
                        bucket: Some(bucket),
                        run: desc.run(),
                        blocks: desc.run().count,
                    });
                }
            }
            let dict = ts.dictionary().borrow();
            if dict.descriptor().is_allocated() {
                out.push(Movable {
                    slot,
                    bucket: None,
                    run: dict.descriptor().run(),
                    blocks: blocks_of(&dict.image()),
                });
            }
        }
        out.sort_by_key(|m| std::cmp::Reverse(m.run.start));
        out
    }

    /// The bytes a move writes: a page as stored, trimmed to its own length
    /// prefix; a dictionary as it stands (its newest image — growth is
    /// append-only, so every page's prefix of it is unchanged).
    fn image_of(&self, m: Movable) -> StorageResult<Option<Vec<u8>>> {
        if m.bucket.is_none() {
            let dict = self.types[m.slot].dictionary().borrow().image();
            return Ok(Some(dict));
        }
        let mut buf = self.file.read_run(m.run)?;
        let Some(prefix) = buf.get(..4).and_then(|s| s.try_into().ok()) else {
            return Ok(None);
        };
        let len = 4 + u32::from_le_bytes(prefix) as usize;
        if len > buf.len() {
            return Ok(None); // an unreadable length prefix: leave it alone
        }
        buf.truncate(len);
        Ok(Some(buf))
    }

    /// Whether a fresh snapshot would land below the addressing run that
    /// sits highest now.
    pub(crate) fn snapshot_would_drop(&self) -> bool {
        let meta = self.meta.borrow();
        let Some(top) = meta.runs().iter().map(|r| r.start).max() else {
            return false;
        };
        let full = edit::chunk_of(self, &[], true, &BTreeMap::new(), 0);
        let need = blocks_of(&edit::encode(&full));
        self.alloc.borrow().lowest_hole(need, top).is_some()
    }
}

/// The next round: the lowest hole some of the highest runs fit in, and
/// which of them (indices into `movable`, ascending). The hole keeps one
/// block for the round's delta, so it needs two at least.
fn next_round(
    alloc: &BlockAllocator,
    movable: &[Movable],
) -> Option<(u64, Vec<usize>)> {
    let top = movable.first()?.run.start;
    for hole in alloc.holes_below(top).filter(|h| h.count >= 2) {
        let mut room = hole.count - 1;
        let mut picked = Vec::new();
        for (i, m) in movable.iter().enumerate() {
            if m.run.start < hole.end() || room == 0 {
                break; // the rest lie below the hole
            }
            if m.blocks <= room {
                picked.push(i);
                room -= m.blocks;
                if picked.len() == ROUND_IMAGES {
                    break;
                }
            }
        }
        if !picked.is_empty() {
            return Some((hole.end(), picked));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{Movable, next_round};
    use crate::block::{BlockAllocator, Run};

    fn page(start: u64, count: u64) -> Movable {
        Movable {
            slot: 0,
            bucket: Some(start as usize),
            run: Run::new(start, count),
            blocks: count,
        }
    }

    /// A move only ever goes down: first-fit by address, never past the limit.
    #[test]
    fn a_run_is_placed_in_the_lowest_hole_below_it() {
        let mut alloc = BlockAllocator::new();
        let low = alloc.alloc(4); // [0,4)
        let _a = alloc.alloc(1); // [4,5)
        let high = alloc.alloc(8); // [5,13)
        let _b = alloc.alloc(1); // [13,14)
        alloc.free(low);
        alloc.free(high);
        // Best-fit would take [0,4) for 3 blocks too, but for 6 it would take
        // [5,13); below 5 there is no room for 6.
        assert_eq!(alloc.alloc_below(6, 5), None);
        assert_eq!(alloc.alloc_below(3, 14), Some(Run::new(0, 3)));
        assert_eq!(alloc.alloc_below(3, 14), Some(Run::new(5, 3)));
        assert_eq!(alloc.alloc_lowest(20), Run::new(14, 20), "none fits");
    }

    /// The highest runs go first, into the lowest hole, keeping a block for
    /// the round's delta.
    #[test]
    fn a_round_fills_the_lowest_hole_from_the_top() {
        let mut alloc = BlockAllocator::new();
        let hole = alloc.alloc(4); // [0,4) — room for 3 + the delta
        let _rest = alloc.alloc(16); // [4,20)
        alloc.free(hole);
        let movable = [page(18, 2), page(15, 2), page(12, 1), page(2, 1)];
        let (limit, picked) = next_round(&alloc, &movable).unwrap();
        assert_eq!(limit, 4);
        assert_eq!(picked, vec![0, 2], "18 (2 blocks) + 12 (1 block)");
    }

    /// Nothing moves when every hole lies above the runs, or is too small
    /// for a run and its delta.
    #[test]
    fn a_packed_file_has_no_round() {
        let mut alloc = BlockAllocator::new();
        let one = alloc.alloc(1); // [0,1) — a one-block hole fits no round
        let _rest = alloc.alloc(9); // [1,10)
        alloc.free(one);
        assert!(next_round(&alloc, &[page(5, 1)]).is_none());
        assert!(next_round(&alloc, &[]).is_none());
    }
}
