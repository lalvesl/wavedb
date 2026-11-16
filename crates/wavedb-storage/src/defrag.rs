//! Compaction's moves ([RFC 0071] §7) — which data file to empty, and its
//! runs carried forward into the active file, highest first, one round (one
//! window write) at a time. The phases around them, and the public surface,
//! are [`crate::compact`]'s.
//!
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

use std::collections::BTreeMap;

use crate::block::Run;
use crate::error::{StorageError, StorageResult};
use crate::ordered::key::cmp_padded;
use crate::page_store::PageStore;
use crate::pages::{EVICTED, run_of, take_page};
use crate::plan::{PlannedPage, SlotPlan, blocks_for};
use crate::shape::weight;

/// Images one round moves at most, so its header has room for them.
pub const ROUND_IMAGES: usize = 128;

/// A run to carry forward.
#[derive(Clone)]
pub struct Movable {
    pub slot: usize,
    /// A key that finds the page.
    pub key: Vec<u8>,
    /// Keys of the other entries naming it, if any: a page a span left
    /// named either side (`crate::partial`) moves once, under both.
    pub also: Vec<Vec<u8>>,
    /// Its flags.
    pub flags: u8,
    pub run: Run,
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
            if let Some(alloc) = self.own()
                && alloc.borrow().active() == Some(victim.start)
            {
                // Emptying the file new runs go to: they go elsewhere first.
                alloc.borrow_mut().roll();
            }
            let movable = self.movable_in(&victim);
            if movable.is_empty() {
                // The active file, closed with nothing to carry: the sealing
                // cuts it. Through a storage thread, nothing left to carry.
                if self.own().is_some() {
                    continue;
                }
                return Ok((images, blocks, true));
            }
            // The budget bounds the pass, not progress: a pass moves at least
            // one image, however large.
            let mut loaded = Vec::new();
            for m in movable.into_iter().take(ROUND_IMAGES) {
                if blocks > 0 && blocks + m.run.count > budget {
                    break;
                }
                let bytes = self.image_of(&m)?;
                blocks += blocks_for(bytes.len());
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

    /// Write `loaded` into the active file as one round, repointing each —
    /// in key order, so pages that are neighbours in their lane land side
    /// by side and their ranges fold (RFC 0078 §4).
    pub(crate) async fn move_round(
        &self,
        mut loaded: Vec<(Movable, Vec<u8>)>,
    ) -> StorageResult<()> {
        loaded.sort_by(|(a, _), (b, _)| {
            a.slot.cmp(&b.slot).then_with(|| cmp_padded(&a.key, &b.key))
        });
        let mut plans: BTreeMap<usize, SlotPlan> = BTreeMap::new();
        for (m, bytes) in loaded {
            let Some(index) = self.types[m.slot].directory().borrow().clone()
            else {
                continue;
            };
            let plan = plans
                .entry(m.slot)
                .or_insert_with(|| SlotPlan::verbatim(m.slot, index));
            // The original goes with its last name (`crate::shares`).
            plan.retired
                .extend(std::iter::repeat_n(m.run, 1 + m.also.len()));
            plan.pages.push(PlannedPage {
                key: m.key,
                image: bytes,
                decoded: None,
                flags: m.flags,
                also: m.also,
            });
            // The copy carries the flag; the original takes its weight with
            // it.
            plan.unpended += weight(m.flags, m.run.count);
        }
        self.relocate(plans.into_values().collect()).await
    }

    /// The data file to empty next: of those at least
    /// [`StoreOptions::victim_free_percent`](crate::StoreOptions) free or held
    /// back, still holding a page, and holding no header a
    /// reconstruction may read — a header cannot move — the one worth most
    /// by the log-structured cleaner's measure ([`worth`]): free space, and
    /// age, which an address is, since writing only appends (RFC 0081 §7).
    /// An old file's live pages are cold — moved once, they stay put — and
    /// a young one may still empty by itself.
    ///
    /// Through a storage thread, the evacuation's files instead (RFC 0073
    /// §7): the next one this data shard still holds a run in.
    fn victim(&self) -> Option<Victim> {
        if self.own().is_none() {
            let chain = self.chain.borrow();
            return self
                .victims
                .borrow()
                .iter()
                .map(|&(start, end)| Victim {
                    start,
                    end,
                    live: 0,
                })
                .find(|v| {
                    chain.live.range(v.start..v.end).next().is_none()
                        && !self.movable_in(v).is_empty()
                });
        }
        let newest = self.own()?.borrow().files().last().map_or(0, |f| f.end);
        self.candidates(true)
            .into_iter()
            .max_by_key(|v| (worth(v, newest), std::cmp::Reverse(v.start)))
    }

    /// Close the active file if it is worth emptying, before a compaction
    /// restates every address: the restatement's headers then land in a new
    /// file, and this one can be emptied too — or a store smaller than one
    /// file would never shrink. Whether any file is worth emptying.
    pub(crate) fn roll_if_worth_emptying(&self) -> bool {
        let Some(alloc) = self.own() else {
            return false;
        };
        let active = alloc.borrow().active();
        let worth = self.candidates(false);
        if active.is_some() && worth.iter().any(|v| Some(v.start) == active) {
            alloc.borrow_mut().roll();
        }
        !worth.is_empty()
    }

    /// The files worth emptying: mostly holes, and holding a run to move,
    /// or (unless `headers` skips those) a live header, which a restatement
    /// outlives — or the active file, whose holes no reclaim cuts until it
    /// closes: a file whose pages all left for the object store holds
    /// nothing else.
    fn candidates(&self, headers: bool) -> Vec<Victim> {
        let percent = u64::from(self.victim_free_percent);
        let Some(alloc) = self.own() else {
            return Vec::new();
        };
        let alloc = alloc.borrow();
        let active = alloc.active();
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
                let header = chain.live.range(f.start..f.end).next().is_some();
                let open = active == Some(f.start);
                let worth = if headers {
                    (holds || open) && !header
                } else {
                    holds || header || open
                };
                worth && (f.free + f.deferred) * 100 >= size * percent
            })
            .map(|f| Victim {
                start: f.start,
                end: f.end,
                live: f.end - f.start - f.free - f.deferred,
            })
            .collect()
    }

    /// The pages in `victim`, highest first.
    fn movable_in(&self, victim: &Victim) -> Vec<Movable> {
        let mut out: Vec<Movable> = self
            .movable_runs()
            .filter(|m| (victim.start..victim.end).contains(&m.run.start))
            .collect();
        out.sort_by_key(|m| std::cmp::Reverse(m.run.start));
        out
    }

    /// Every page run on disk, once per type naming it.
    fn movable_runs(&self) -> impl Iterator<Item = Movable> + '_ {
        self.types.iter().enumerate().flat_map(|(slot, ts)| {
            let mut out: Vec<Movable> = Vec::new();
            let mut seen: BTreeMap<u64, usize> = BTreeMap::new();
            if let Some(index) = ts.directory().borrow().as_ref() {
                for (at, page) in index.pages() {
                    if page.meta & EVICTED == 0
                        && let Some(key) = index.key_of(at)
                    {
                        named(&mut out, &mut seen, slot, key, page);
                    }
                }
            }
            out
        })
    }

    /// The bytes a move writes: a page compressed again at the cleanup's
    /// level (RFC 0081 §9) — its records as stored, none clipped — or, for a
    /// type that does not compress, as stored, trimmed to its own length
    /// prefix.
    pub(crate) fn image_of(&self, m: &Movable) -> StorageResult<Vec<u8>> {
        let slot = &self.types[m.slot];
        if slot.compresses() {
            let mut page = take_page(&self.file, m.run, slot.struct_hash())?;
            return page.encode(slot.compression().thorough());
        }
        let run = self.file.read_run(m.run)?;
        // A pack's section moves as a page of its own: every slot naming
        // the pack carries its own out, and the pack goes with the last.
        let hash = self.types[m.slot].struct_hash();
        let mut buf = crate::pack::page_image(&run, hash)?.into_owned();
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

/// `page`, found by `key`, onto `out`: a run already there — named either
/// side of a span — takes the key as another name. `seen` says where each
/// run is in `out`.
pub fn named(
    out: &mut Vec<Movable>,
    seen: &mut BTreeMap<u64, usize>,
    slot: usize,
    key: Vec<u8>,
    page: crate::ordered::Placed<u8>,
) {
    let run = run_of(page);
    if let Some(&i) = seen.get(&run.start) {
        out[i].also.push(key);
        return;
    }
    seen.insert(run.start, out.len());
    out.push(Movable {
        slot,
        key,
        also: Vec::new(),
        flags: page.meta,
        run,
    });
}

/// What emptying `v` is worth beside what it costs, as the log-structured
/// filesystem's cleaner weighs a segment: `(1 − u) · age / (1 + u)`, `u`
/// the share of its blocks still live and its age the blocks written since
/// it ended, `newest` the end of the newest file. Scaled to integers.
fn worth(v: &Victim, newest: u64) -> u128 {
    let size = u128::from(v.end - v.start);
    let live = u128::from(v.live);
    let age = u128::from(newest.saturating_sub(v.end)) + 1;
    (size - live.min(size)) * age * 1_000 / (size + live).max(1)
}

#[cfg(test)]
mod tests {
    use super::{Victim, worth};

    const fn file(start: u64, live: u64) -> Victim {
        Victim {
            start,
            end: start + 100,
            live,
        }
    }

    /// Emptier is worth more at one age, and older at one emptiness — so an
    /// old file a little fuller can go before a young one.
    #[test]
    fn free_space_and_age_both_count() {
        let newest = 1_000;
        assert!(worth(&file(0, 20), newest) > worth(&file(0, 40), newest));
        assert!(worth(&file(0, 40), newest) > worth(&file(800, 40), newest));
        assert!(worth(&file(0, 45), newest) > worth(&file(800, 30), newest));
        assert_eq!(worth(&file(0, 100), newest), 0, "nothing to give back");
    }
}
