//! Compaction's moves ([RFC 0071] §7) — which data file to empty, and its
//! runs carried forward into the active file, highest first, one round (one
//! window write) at a time. The phases around them, and the public surface,
//! are [`crate::compact`]'s.
//!
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

use std::collections::BTreeMap;

use crate::block::Run;
use crate::error::{StorageError, StorageResult};
use crate::page_store::PageStore;
use crate::plan::{SlotPlan, blocks_of};

/// Images one round moves at most, so its header names them all (a moved
/// page costs it 12 bytes).
const ROUND_IMAGES: usize = 128;

/// A run to carry forward.
#[derive(Clone, Copy)]
struct Movable {
    slot: usize,
    /// The page's bucket; `None` for the type's dictionary.
    bucket: Option<usize>,
    run: Run,
}

/// A file worth emptying, and what it still holds.
struct Victim {
    start: u64,
    end: u64,
    live: u64,
}

impl PageStore {
    /// One pass: rounds until about `budget` blocks moved. `finished` once no
    /// file is worth emptying any more.
    pub(crate) async fn shift(
        &self,
        budget: u64,
    ) -> StorageResult<(usize, u64, bool)> {
        let (mut images, mut blocks) = (0, 0);
        loop {
            // Through the gate from the reading of the runs to their
            // publishing: a round settled meanwhile would be overwritten by
            // the stale copy.
            let _through = self.gate.enter().await;
            let Some(victim) = self.victim() else {
                return Ok((images, blocks, true));
            };
            if self.alloc.borrow().active() == Some(victim.start) {
                // Emptying the file new runs go to: they go elsewhere first.
                self.alloc.borrow_mut().roll();
            }
            let movable = self.movable_in(&victim);
            if movable.is_empty() {
                // Nothing left to carry: the sealing frees what it held.
                return Ok((images, blocks, true));
            }
            // The budget bounds the pass, not progress: a pass moves at least
            // one image, however large.
            let mut loaded = Vec::new();
            for m in movable.into_iter().take(ROUND_IMAGES) {
                if blocks > 0 && blocks + m.run.count > budget {
                    break;
                }
                let bytes = self.image_of(m)?;
                blocks += blocks_of(&bytes);
                loaded.push((m, bytes));
            }
            if loaded.is_empty() {
                return Ok((images, blocks, false)); // the next would not fit
            }
            images += loaded.len();
            self.move_round(loaded).await?;
            if blocks >= budget {
                return Ok((images, blocks, false));
            }
        }
    }

    /// Write `loaded` into the active file as one round, repointing each.
    async fn move_round(
        &self,
        loaded: Vec<(Movable, Vec<u8>)>,
    ) -> StorageResult<()> {
        let mut plans: BTreeMap<usize, SlotPlan> = BTreeMap::new();
        for (m, bytes) in loaded {
            let Some(dir) = self.types[m.slot].directory().borrow().clone()
            else {
                continue;
            };
            let plan = plans
                .entry(m.slot)
                .or_insert_with(|| SlotPlan::verbatim(m.slot, dir));
            match m.bucket {
                Some(bucket) => {
                    plan.pages.insert(bucket, bytes);
                }
                None => plan.dict = Some(bytes),
            }
        }
        self.relocate(plans.into_values().collect()).await
    }

    /// The data file to empty next: of those at least
    /// [`StoreOptions::victim_free_percent`](crate::StoreOptions) free or held
    /// back, still holding a page or a dictionary, and holding no header a
    /// reconstruction may read — a header cannot move — the one with the
    /// fewest live blocks, the oldest on a tie.
    fn victim(&self) -> Option<Victim> {
        self.candidates(true)
            .into_iter()
            .min_by_key(|v| (v.live, v.start))
    }

    /// Close the active file if it is worth emptying, before a compaction
    /// restates every address: the restatement's headers then land in a new
    /// file, and this one can be emptied too — or a store smaller than one
    /// file would never shrink.
    pub(crate) fn roll_if_worth_emptying(&self) {
        let active = self.alloc.borrow().active();
        if active.is_some()
            && self
                .candidates(false)
                .iter()
                .any(|v| Some(v.start) == active)
        {
            self.alloc.borrow_mut().roll();
        }
    }

    /// The files worth emptying — skipping those holding a live header, if
    /// `headers` says so.
    fn candidates(&self, headers: bool) -> Vec<Victim> {
        let percent = u64::from(self.victim_free_percent);
        let alloc = self.alloc.borrow();
        let mut held: Vec<u64> =
            self.movable_runs().map(|m| m.run.start).collect();
        held.sort_unstable();
        let chain = self.chain.borrow();
        alloc
            .files()
            .into_iter()
            .filter(|f| {
                let size = f.end - f.start;
                let holds = held.partition_point(|&s| s < f.start)
                    < held.partition_point(|&s| s < f.end);
                let header = headers
                    && chain.live.range(f.start..f.end).next().is_some();
                holds
                    && !header
                    && (f.free + f.deferred) * 100 >= size * percent
            })
            .map(|f| Victim {
                start: f.start,
                end: f.end,
                live: f.end - f.start - f.free - f.deferred,
            })
            .collect()
    }

    /// The pages and dictionaries in `victim`, highest first.
    fn movable_in(&self, victim: &Victim) -> Vec<Movable> {
        let mut out: Vec<Movable> = self
            .movable_runs()
            .filter(|m| (victim.start..victim.end).contains(&m.run.start))
            .collect();
        out.sort_by_key(|m| std::cmp::Reverse(m.run.start));
        out
    }

    /// Every page and dictionary run on disk.
    fn movable_runs(&self) -> impl Iterator<Item = Movable> + '_ {
        self.types.iter().enumerate().flat_map(|(slot, ts)| {
            let mut out = Vec::new();
            if let Some(dir) = ts.directory().borrow().as_ref() {
                for (bucket, desc) in dir.slots().iter().enumerate() {
                    if desc.is_allocated() {
                        out.push(Movable {
                            slot,
                            bucket: Some(bucket),
                            run: desc.run(),
                        });
                    }
                }
            }
            let dict = ts.dictionary().borrow().descriptor();
            if dict.is_allocated() {
                out.push(Movable {
                    slot,
                    bucket: None,
                    run: dict.run(),
                });
            }
            out
        })
    }

    /// The bytes a move writes: a page as stored, trimmed to its own length
    /// prefix; a dictionary as it stands (its newest image — growth is
    /// append-only, so every page's prefix of it is unchanged).
    fn image_of(&self, m: Movable) -> StorageResult<Vec<u8>> {
        if m.bucket.is_none() {
            return Ok(self.types[m.slot].dictionary().borrow().image());
        }
        let mut buf = self.file.read_run(m.run)?;
        let len = buf
            .get(..4)
            .and_then(|s| s.try_into().ok())
            .map(|prefix| 4 + u32::from_le_bytes(prefix) as usize)
            .filter(|&len| len <= buf.len())
            .ok_or(StorageError::Corrupt("a page's length prefix"))?;
        buf.truncate(len);
        Ok(buf)
    }
}
