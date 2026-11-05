//! The regroup's merge ([RFC 0078] §12): neighbouring pages that removals
//! left small, rewritten as one.
//!
//! A page is only ever cut, and a removal shrinks it in place, so a lane
//! whose records come and go is left with pages far under target — more
//! entries, more reads, worse compression. A compaction merges them: runs
//! of neighbouring pages of one range and one collection that together
//! fill at most half a page. Half, so the two halves of a cut, which a
//! write grows past target again, are not merged back only to be cut once
//! more.
//!
//! A merge is a settle round over what it already holds: the entries of
//! the run's later pages go, their quanta handed to the first page's
//! (`PageIndex::remove`), and their records are shaped into it
//! (`crate::shape`) — read, rebuilt, counted and encoded as any round's
//! page, at the end of the active file. Their runs are freed when the
//! compaction seals.
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use crate::cut::collection;
use crate::error::{StorageError, StorageResult};
use crate::ordered::{Found, Placed};
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::pages::{EVICTED, Pages, clip, read_page, run_of, take_page};
use crate::plan::{SlotPlan, image_of};
use crate::record_cache::Records;
use crate::shape::{Rules, shape, weight};

/// Runs a merge round takes at most.
const ROUND_RUNS: usize = 64;

impl PageStore {
    /// Queue every type's runs of small neighbouring pages for the passes
    /// to merge.
    pub(crate) fn plan_merges(&self) {
        let mut queue = self.regroup.borrow_mut();
        queue.clear();
        if !self.has_disk() {
            return; // object mode: its pages are rewritten by upload
        }
        let most = self.target_blocks_per_bucket / 2;
        for (slot, ts) in self.types.iter().enumerate() {
            if let Some(index) = ts.directory().borrow().as_ref() {
                let runs = small_runs(index, most);
                queue.extend(runs.into_iter().map(|keys| (slot, keys)));
            }
        }
    }

    /// One pass: queued runs merged, a round per type's batch of them,
    /// until about `budget` blocks are written. `finished` once the queue
    /// is empty.
    pub(crate) async fn merge_pass(
        &self,
        budget: u64,
    ) -> StorageResult<(usize, u64, bool)> {
        let (mut images, mut blocks) = (0, 0);
        loop {
            // As a settle round: nothing else plans over the index meanwhile.
            let _through = self.gate.enter().await;
            let batch = {
                let mut queue = self.regroup.borrow_mut();
                let Some(&(slot, _)) = queue.front() else {
                    return Ok((images, blocks, true));
                };
                let mut batch = Vec::new();
                while batch.len() < ROUND_RUNS
                    && queue.front().is_some_and(|q| q.0 == slot)
                {
                    batch.extend(queue.pop_front().map(|q| q.1));
                }
                (slot, batch)
            };
            let Some(plan) = self.plan_merge(batch.0, &batch.1)? else {
                continue;
            };
            images += plan.pages.len();
            blocks += plan
                .pages
                .iter()
                .map(|p| crate::plan::blocks_for(p.image.len()))
                .sum::<u64>();
            self.place_plans(vec![plan]).await?;
            if blocks >= budget {
                return Ok((images, blocks, false));
            }
        }
    }

    /// Slot `idx`'s round merging `runs`, each named by its pages' keys —
    /// the runs whose pages are still small neighbours of one collection;
    /// `None` when none is.
    #[allow(clippy::significant_drop_tightening)] // the dictionary binds the round
    fn plan_merge(
        &self,
        idx: usize,
        runs: &[Vec<Vec<u8>>],
    ) -> StorageResult<Option<SlotPlan>> {
        let slot = &self.types[idx];
        let Some(mut index) = slot.directory().borrow().clone() else {
            return Ok(None);
        };
        let dict = slot.dictionary().borrow();
        let (mut retired, mut unpended) = (Vec::new(), 0);
        let mut records: Records = Vec::new();
        for keys in runs {
            let Some(pages) =
                self.still_small(&index, keys, slot.struct_hash(), &dict)?
            else {
                continue;
            };
            // The later pages' entries go, their quanta to the first's;
            // their records are shaped into it.
            for (key, page) in keys.iter().zip(&pages).skip(1) {
                let Found::Page { at, page: placed } = index.find(key) else {
                    return Err(StorageError::Corrupt(
                        "a merged page lost its entry",
                    ));
                };
                index
                    .remove(at)
                    .ok_or(StorageError::Corrupt("a merged entry refused"))?;
                retired.push(run_of(placed));
                unpended += weight(placed.meta, placed.blocks);
                records.extend(
                    page.iter().map(|(k, v)| (k.to_vec(), Some(v.to_vec()))),
                );
            }
        }
        if records.is_empty() {
            return Ok(None);
        }
        records.sort_by(|a, b| a.0.cmp(&b.0));
        let rules = Rules {
            target_blocks: self.target_blocks_per_bucket,
            keep_holes: self.object.is_some(),
            partial: false,
        };
        let shaped = shape(
            &mut index,
            records,
            &rules,
            slot.struct_hash(),
            |page| {
                take_page(&self.file, run_of(page), slot.struct_hash(), &dict)
            },
            |page| image_of(page, &dict),
        )?;
        retired.extend(shaped.retired);
        Ok(Some(SlotPlan {
            idx,
            index,
            pages: shaped.pages,
            dict: None,
            retired,
            named: shaped.named,
            unpended: unpended + shaped.unpended,
            punched: shaped.punched,
        }))
    }

    /// The pages `keys` name, read, if they are still one per key, side by
    /// side in one range, on disk, and of one collection.
    fn still_small(
        &self,
        index: &Pages,
        keys: &[Vec<u8>],
        hash: u64,
        dict: &crate::dictionary::DictState,
    ) -> StorageResult<Option<Vec<SlotPage>>> {
        let mut found: Vec<(crate::ordered::Slot, Placed<u8>)> = Vec::new();
        for key in keys {
            let Found::Page { at, page } = index.find(key) else {
                return Ok(None);
            };
            let beside = found.last().is_none_or(|(prev, _)| {
                prev.range == at.range && prev.entry + 1 == at.entry
            });
            if page.meta & EVICTED != 0 || !beside {
                return Ok(None);
            }
            found.push((at, page));
        }
        let mut pages = Vec::with_capacity(found.len());
        for &(at, page) in &found {
            let read = read_page(&self.file, run_of(page), hash, dict)?;
            pages.push(clip(read, index, at)?);
        }
        let ends: Vec<Option<&[u8]>> = pages
            .iter()
            .flat_map(|p| [p.iter().next(), p.iter().last()])
            .flatten()
            .map(|(key, _)| collection(key))
            .collect();
        let one = ends.windows(2).all(|w| w[0] == w[1]);
        Ok(one.then_some(pages))
    }
}

/// Runs of neighbouring pages in one range, two or more, together at most
/// `most` blocks; each named by its pages' keys.
fn small_runs(index: &Pages, most: u64) -> Vec<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    let mut run: Vec<Vec<u8>> = Vec::new();
    // A run named by several entries of the group — a page around a span
    // rewritten apart (`crate::partial`) — is read and rewritten once.
    let mut seen = std::collections::BTreeSet::new();
    let (mut blocks, mut last) = (0, None);
    for (at, page) in index.pages() {
        let beside = last.is_some_and(|(range, entry)| {
            range == at.range && entry + 1 == at.entry
        });
        let more = if seen.contains(&page.address) {
            0
        } else {
            page.blocks
        };
        if !beside || blocks + more > most || page.meta & EVICTED != 0 {
            if run.len() >= 2 {
                out.push(std::mem::take(&mut run));
            }
            run.clear();
            seen.clear();
            blocks = 0;
        }
        last = Some((at.range, at.entry));
        if page.blocks <= most && page.meta & EVICTED == 0 {
            if seen.insert(page.address) {
                blocks += page.blocks;
            }
            run.extend(index.key_of(at));
        }
    }
    if run.len() >= 2 {
        out.push(run);
    }
    out
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use wavedb_core::{Id, Store, U48, Write};

    use crate::page_store::PageStore;
    use crate::struct_storage::StructStorage;

    const SH: u64 = 0x5EED_0078_0008_0002;
    static SLOT: StructStorage = StructStorage::new(SH);

    const RECORDS: u64 = 3_000;

    fn id(k: u64) -> Id {
        Id::new(k, U48::from(1u32), false, (SH & 0x7FFF) as u16)
    }

    /// ~1 KiB that does not compress away.
    fn record(k: u64) -> Vec<u8> {
        let mut bytes = SH.to_le_bytes().to_vec();
        let mut x = k.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        for _ in 0..128 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            bytes.extend_from_slice(&x.to_le_bytes());
        }
        bytes
    }

    fn kept(k: u64) -> bool {
        k.is_multiple_of(10)
    }

    fn assert_records(s: &PageStore) {
        s.evict_settled(0);
        block_on(async {
            for k in 0..RECORDS {
                let want = kept(k).then(|| record(k));
                assert_eq!(s.get_of(SH, id(k)).await.unwrap(), want, "{k}");
            }
        });
    }

    /// Nine records in ten removed leave every page a tenth full; a
    /// compaction merges them back to a few, and every record left reads
    /// from them — through a reopen too.
    #[test]
    fn a_compaction_merges_the_pages_removals_left_small() {
        let d = tempfile::tempdir().unwrap();
        let s = PageStore::open(d.path(), &[&SLOT]).unwrap();
        block_on(async {
            for k in 0..RECORDS {
                s.apply(&[Write::Put(id(k), record(k))]).await.unwrap();
            }
            s.drain().unwrap();
            for k in (0..RECORDS).filter(|&k| !kept(k)) {
                s.apply(&[Write::Remove(SH, id(k))]).await.unwrap();
            }
            s.drain().unwrap();
        });
        let before = s.bucket_count(SH);
        s.compact().unwrap();
        let after = s.bucket_count(SH);
        assert!(after * 3 < before, "{before} pages → {after}");
        assert_records(&s);

        drop(s);
        let s = PageStore::open(d.path(), &[&SLOT]).unwrap();
        assert_eq!(s.bucket_count(SH), after);
        assert_records(&s);
    }
}
