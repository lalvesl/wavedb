//! The regroup ([RFC 0078] §12, phase 8): a phase of compaction that
//! rewrites the pages of scattered ranges side by side.
//!
//! Data files only append, so a page rewritten long after its neighbours
//! lands past its range's window and gets a range of its own; a lane of
//! scattered rewrites drifts toward a range per page. The fold (§4) merges
//! back what already shares a window. The regroup brings the rest
//! together: it finds runs of neighbouring ranges of a few pages each
//! ([`PageIndex::scattered`](crate::ordered::PageIndex::scattered)) and
//! moves each run's pages verbatim, in key order, into one window — the
//! compaction's own move, so the counts and flags go with the pages, and
//! the runs they leave are freed when the compaction seals. As each page is
//! repointed the fold makes the run one range.
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use crate::defrag::{Movable, ROUND_IMAGES};
use crate::error::StorageResult;
use crate::ordered::Found;
use crate::page_store::PageStore;
use crate::pages::{EVICTED, run_of};
use crate::plan::blocks_for;

impl PageStore {
    /// Queue every type's scattered runs for the passes to move: once per
    /// compaction, so a run whose fold did not take is not moved again.
    pub(crate) fn plan_regroup(&self) {
        let mut queue = self.regroup.borrow_mut();
        queue.clear();
        if !self.has_disk() {
            return; // object mode: no page is on a disk to move
        }
        for (slot, ts) in self.types.iter().enumerate() {
            if let Some(index) = ts.directory().borrow().as_ref() {
                let runs = index.scattered(ROUND_IMAGES);
                queue.extend(runs.into_iter().map(|keys| (slot, keys)));
            }
        }
    }

    /// One pass: queued runs moved, a round each, until about `budget`
    /// blocks. `finished` once the queue is empty.
    pub(crate) async fn regroup_pass(
        &self,
        budget: u64,
    ) -> StorageResult<(usize, u64, bool)> {
        let (mut images, mut blocks) = (0, 0);
        loop {
            // As a compaction's move: a round settled between the reading
            // of the pages and their publishing would be overwritten.
            let _through = self.gate.enter().await;
            let Some((slot, keys)) = self.regroup.borrow_mut().pop_front()
            else {
                return Ok((images, blocks, true));
            };
            let loaded = self.gathered(slot, &keys)?;
            if loaded.is_empty() {
                continue;
            }
            images += loaded.len();
            blocks += loaded.iter().map(|l| blocks_for(l.1.len())).sum::<u64>();
            self.move_round(loaded).await?;
            if blocks >= budget {
                return Ok((images, blocks, false));
            }
        }
    }

    /// The pages `keys` find now, and their bytes — each once: writes since
    /// the run was queued may have rewritten, merged or evicted some.
    fn gathered(
        &self,
        slot: usize,
        keys: &[Vec<u8>],
    ) -> StorageResult<Vec<(Movable, Vec<u8>)>> {
        let mut movable: Vec<Movable> = Vec::with_capacity(keys.len());
        if let Some(index) = self.types[slot].directory().borrow().as_ref() {
            for key in keys {
                let Found::Page { page, .. } = index.find(key) else {
                    continue;
                };
                let run = run_of(page);
                if page.meta & EVICTED != 0
                    || movable.iter().any(|m| m.run.start == run.start)
                {
                    continue;
                }
                movable.push(Movable {
                    slot,
                    page: Some((key.clone(), page.meta)),
                    run,
                });
            }
        }
        movable
            .into_iter()
            .map(|m| {
                let bytes = self.image_of(&m)?;
                Ok((m, bytes))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use wavedb_core::{Id, Store, U48, Write};

    use crate::ordered::Found;
    use crate::page_store::PageStore;
    use crate::pages::key_of_id;
    use crate::struct_storage::StructStorage;

    const SH: u64 = 0x5EED_0078_0008_0001;
    static SLOT: StructStorage = StructStorage::new(SH);

    /// Records a page each: 20 KiB that do not compress, in pages cut at
    /// 32 KiB.
    const PAGES: u64 = 64;

    /// A key in a quantum of its own.
    fn id(k: u64) -> Id {
        Id::new(k << 48, U48::from(1u32), false, (SH & 0x7FFF) as u16)
    }

    fn record(k: u64, version: u64) -> Vec<u8> {
        let mut bytes = SH.to_le_bytes().to_vec();
        let mut x = k.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version ^ 1;
        for _ in 0..2560 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            bytes.extend_from_slice(&x.to_le_bytes());
        }
        bytes
    }

    fn ranges(s: &PageStore) -> usize {
        s.types[0]
            .directory()
            .borrow()
            .as_ref()
            .map_or(0, crate::ordered::PageIndex::range_count)
    }

    /// The range each of the first pages' keys is found in.
    fn ranges_of_keys(s: &PageStore) -> Vec<usize> {
        let index = s.types[0].directory().borrow();
        let index = index.as_ref().unwrap();
        (0..PAGES)
            .map(|k| match index.find(&key_of_id(id(k))) {
                Found::Page { at, .. } => at.range,
                other => panic!("key {k} lost: {other:?}"),
            })
            .collect()
    }

    fn assert_records(s: &PageStore) {
        s.evict_settled(0);
        block_on(async {
            for k in 0..PAGES {
                let want = record(k, k % 2);
                let got = s.get_of(SH, id(k)).await.unwrap();
                assert_eq!(got, Some(want), "record {k}");
            }
        });
    }

    /// Every other page rewritten past its range's window: each is
    /// isolated, and the old pages between keep the rewritten ones from
    /// folding. The compaction's regroup moves them side by side, and they
    /// come out one range — through a reopen too.
    #[test]
    fn a_compaction_regroups_scattered_ranges_into_one() {
        let d = tempfile::tempdir().unwrap();
        let s = PageStore::open(d.path(), &[&SLOT]).unwrap();
        block_on(async {
            for k in 0..PAGES {
                s.apply(&[Write::Put(id(k), record(k, 1))]).await.unwrap();
            }
            s.drain().unwrap();
            // 18 MiB further on: past a 16 MiB window.
            for k in 40_000..40_900 {
                s.apply(&[Write::Put(id(k), record(k, 0))]).await.unwrap();
            }
            s.drain().unwrap();
            for k in (0..PAGES).step_by(2) {
                s.apply(&[Write::Put(id(k), record(k, 0))]).await.unwrap();
            }
            s.drain().unwrap();
        });
        let scattered = ranges_of_keys(&s);
        assert!(
            scattered.windows(2).filter(|w| w[0] != w[1]).count() > 32,
            "{scattered:?}"
        );
        let before = ranges(&s);

        s.compact().unwrap();
        let regrouped = ranges_of_keys(&s);
        assert!(
            regrouped.iter().all(|&r| r == regrouped[0]),
            "{regrouped:?}"
        );
        let after = ranges(&s);
        assert!(after + 32 < before, "{before} → {after}");
        assert_records(&s);

        drop(s);
        let s = PageStore::open(d.path(), &[&SLOT]).unwrap();
        assert_eq!(ranges(&s), after);
        assert_eq!(ranges_of_keys(&s), regrouped);
        assert_records(&s);
    }
}
