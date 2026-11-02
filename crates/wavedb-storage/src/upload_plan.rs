//! An upload's middle ([RFC 0070] §5, [RFC 0078]): which remote pages meet
//! the flagged disk pages, and those pages rebuilt — every record within a
//! flagged page's coverage replaced by the flagged page's own — then cut
//! where they run over target.
//!
//! The disk and the object store each have an ordered index of their own,
//! over the same keys. A flagged disk entry is the whole truth about the
//! keys it covers, `[lo, hi)`: the remote pages meeting that span are
//! exactly the ones that can hold a record of it, so a record the disk no
//! longer has is dropped from them, and every one it has goes in.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::collections::{BTreeMap, BTreeSet};

use wavedb_core::Id;

use crate::block::Run;
use crate::error::{StorageError, StorageResult};
use crate::ordered::{Journal, Placed};
use crate::page::SlotPage;
use crate::page_store::PageStore;
use futures::stream::{self, StreamExt, TryStreamExt};

use crate::pages::{
    PENDING, Pages, coverage, entries_between, key_of_id, read_page,
    remote_pages, run_of, within,
};
use crate::plan::{PlannedPage, image_of};
use crate::shape::weight;
use crate::shape::{Rules, Shaped, shape};
use crate::upload::{IN_FLIGHT, UPLOAD_INPUT_BLOCKS};

/// Blocks a remote page should hold (128 KiB): a target, as on disk — a
/// page over it is cut in the upload that rewrites it.
pub const REMOTE_TARGET_BLOCKS: u64 = 32;

/// One flagged disk entry an upload carries.
pub struct Flagged {
    /// The keys it covers, `[lo, hi)`; `lo` finds it again.
    pub(crate) lo: Vec<u8>,
    pub(crate) hi: Option<Vec<u8>>,
    /// The entry as taken: its page (`None` for a hole) and its flags — its
    /// flag is cleared only if it is still so once the files are up.
    pub(crate) taken: Option<Placed<u8>>,
    pub(crate) flags: u8,
    /// Its records; none for a hole.
    pub(crate) page: SlotPage,
}

/// One type's share of an upload, from the snapshot to the encoded pages.
pub struct SlotUpload {
    /// Index into [`PageStore::types`].
    pub(crate) idx: usize,
    pub(crate) flagged: Vec<Flagged>,
    /// The remote index being rebuilt — the upload's own copy.
    pub(crate) remote: Pages,
    /// Remote pages fetched, by their run.
    pub(crate) fetched: BTreeMap<Run, SlotPage>,
    /// Remote pages carried forward as they are, out of files a compaction
    /// empties (`crate::remote_compact`): a key finding each, and its run.
    pub(crate) moved: Vec<(Vec<u8>, Run)>,
    /// Whether the dictionary is carried forward too.
    pub(crate) move_dict: bool,
    /// The pages it writes, once rebuilt.
    pub(crate) shaped: Shaped,
    /// What of the remote index it changed, for the disk's headers.
    pub(crate) changes: Journal,
}

impl SlotUpload {
    /// The remote runs this upload reads: those meeting a flagged coverage,
    /// and those carried forward.
    pub(crate) fn affected(&self) -> BTreeSet<Run> {
        let meeting = self.flagged.iter().flat_map(|f| {
            entries_between(&self.remote, &f.lo, f.hi.as_deref())
                .into_iter()
                .filter_map(|(_, page)| page.map(run_of))
        });
        meeting
            .chain(self.moved.iter().map(|&(_, run)| run))
            .collect()
    }

    /// The records the remote pages take: every one a flagged page holds,
    /// and the removal of every one the fetched pages hold within a flagged
    /// coverage that the page does not — in key order.
    fn records(&self) -> Vec<(Id, Option<Vec<u8>>)> {
        let mut out: BTreeMap<u128, Option<Vec<u8>>> = BTreeMap::new();
        for page in self.fetched.values() {
            for (id, _) in page.iter() {
                let key = key_of_id(id);
                let covered = self
                    .flagged
                    .iter()
                    .any(|f| within(&key, &f.lo, f.hi.as_deref()));
                if covered {
                    out.insert(id.raw(), None);
                }
            }
        }
        for f in &self.flagged {
            for (id, bytes) in f.page.iter() {
                out.insert(id.raw(), Some(bytes.to_vec()));
            }
        }
        out.into_iter()
            .map(|(raw, bytes)| (Id::from_raw(raw), bytes))
            .collect()
    }
}

impl PageStore {
    /// Step 1: every type's flagged disk entries, up to the input bound, and
    /// a copy of each type's remote index — and whether that took every
    /// flagged entry, the bound leaving none behind (a cut). The remote
    /// pages a compaction carries forward come along, up to their own bound.
    pub(crate) fn take_flagged(
        &self,
    ) -> StorageResult<(Vec<SlotUpload>, bool)> {
        let (mut taken, mut cut) = (0, true);
        let mut moving = crate::remote_compact::MOVE_BLOCKS;
        let mut work = Vec::new();
        for (idx, slot) in self.types.iter().enumerate() {
            let (moved, move_dict) = self.moves_of(idx, &mut moving);
            let mut flagged = Vec::new();
            if let Some(disk) = slot.directory().borrow().as_ref() {
                let dict = slot.dictionary().borrow();
                for (at, page) in disk.entries_all() {
                    let flags = page.map_or_else(
                        || disk.meta_at(at).unwrap_or(0),
                        |p| p.meta,
                    );
                    if flags & PENDING == 0 {
                        continue;
                    }
                    if taken >= UPLOAD_INPUT_BLOCKS {
                        cut = false;
                        continue;
                    }
                    let (lo, hi) = coverage(disk, at).ok_or(
                        StorageError::Corrupt("a flagged entry without a key"),
                    )?;
                    let records = match page {
                        Some(page) => {
                            read_page(&self.file, run_of(page), &dict)?
                        }
                        None => SlotPage::new(slot.struct_hash()),
                    };
                    taken += weight(flags, page.map_or(0, |p| p.blocks));
                    flagged.push(Flagged {
                        lo,
                        hi,
                        taken: page,
                        flags,
                        page: records,
                    });
                }
            }
            if flagged.is_empty() && moved.is_empty() && !move_dict {
                continue;
            }
            let remote = slot.remote_directory().borrow().clone();
            work.push(SlotUpload {
                idx,
                flagged,
                remote: remote.unwrap_or_else(remote_pages),
                fetched: BTreeMap::new(),
                moved,
                move_dict,
                shaped: Shaped::default(),
                changes: Journal::new(),
            });
        }
        Ok((work, cut))
    }

    /// Step 2: fetch every remote page the flagged entries meet,
    /// [`IN_FLIGHT`] at a time.
    pub(crate) async fn fetch_shared(
        &self,
        work: &mut [SlotUpload],
    ) -> StorageResult<()> {
        let mut wanted = Vec::new();
        for (w, slot) in work.iter().enumerate() {
            for run in slot.affected() {
                wanted.push((w, slot.idx, run));
            }
        }
        let fetched: Vec<(usize, crate::block::Run, SlotPage)> =
            stream::iter(wanted)
                .map(|(w, idx, run)| async move {
                    let page = self
                        .with_remote_page(idx, run, SlotPage::clone)
                        .await?;
                    StorageResult::Ok((w, run, page))
                })
                .buffer_unordered(IN_FLIGHT)
                .try_collect()
                .await?;
        for (w, run, page) in fetched {
            work[w].fetched.insert(run, page);
        }
        Ok(())
    }
    /// Rebuild `work`'s remote pages (see the module docs), and carry the
    /// moved ones forward re-encoded. The pages to write land in
    /// `work.shaped`.
    ///
    /// # Errors
    /// A page this upload needs that was not fetched, or an encoding fault.
    pub(crate) fn rebuild_remote(
        &self,
        work: &mut SlotUpload,
    ) -> StorageResult<()> {
        let records = work.records();
        let slot = &self.types[work.idx];
        let dict = slot.dictionary().borrow();
        let rules = Rules {
            target_blocks: REMOTE_TARGET_BLOCKS,
            keep_holes: false,
        };
        let fetched = &work.fetched;
        let mut shaped = shape(
            &mut work.remote,
            records,
            &rules,
            slot.struct_hash(),
            |page| {
                fetched.get(&run_of(page)).cloned().ok_or(
                    StorageError::Corrupt("an upload met an unfetched page"),
                )
            },
            |page| image_of(page, &dict),
        )?;
        let rewritten: BTreeSet<Run> = shaped.retired.iter().copied().collect();
        for (key, run) in &work.moved {
            if rewritten.contains(run) {
                continue;
            }
            let mut page = fetched
                .get(run)
                .cloned()
                .ok_or(StorageError::Corrupt("a moved page was not fetched"))?;
            shaped.pages.push(PlannedPage {
                key: key.clone(),
                image: image_of(&mut page, &dict)?,
                decoded: Some(page),
                flags: 0,
            });
            shaped.retired.push(*run);
        }
        work.shaped = shaped;
        Ok(())
    }

    /// Decode an uploaded image of slot `idx`.
    pub(crate) fn decode_remote(
        &self,
        idx: usize,
        image: &[u8],
    ) -> StorageResult<SlotPage> {
        let dict = self.types[idx].dictionary().borrow();
        SlotPage::from_bytes(image, dict.dictionary())
    }
}
