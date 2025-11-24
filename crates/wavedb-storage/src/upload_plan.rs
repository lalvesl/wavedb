//! An upload's middle ([RFC 0070] §5): which remote pages share records with
//! the flagged disk pages, and those pages rebuilt — the old one, minus every
//! record of a flagged disk bucket, plus the flagged pages' records — then
//! split while over target ([RFC 0074]).
//!
//! The disk and the object store each have an extendible hash of their
//! own. A page's **depth** is how many low hash bits its records share, so
//! disk bucket `d` and a remote page can hold the same record exactly when
//! the remote page has an entry `j ≡ d (mod 2^min(depth(d), D))`, `D` the
//! remote directory's depth — arithmetic, no lookup.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0074]: ../../../rfcs/0074-extendible-hashing.md

use std::collections::{BTreeMap, BTreeSet};

use wavedb_core::Id;

use crate::block::BlockDescriptor;
use crate::directory::Directory;
use crate::error::{StorageError, StorageResult};
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::plan::image_of;
use crate::split::split_staged;

/// Blocks a remote page should hold (128 KiB): a target, as on disk — a
/// page over it splits in the upload that rewrites it.
pub const REMOTE_TARGET_BLOCKS: u64 = 32;

/// The buckets of `remote` that can share records with bucket `bucket` of
/// `disk`: those with an entry agreeing with it on every bit both depths
/// route by.
pub fn sharing(
    disk: &Directory,
    bucket: usize,
    remote: &Directory,
) -> BTreeSet<usize> {
    let bits = disk.descriptor(bucket).depth().min(remote.depth());
    let step = 1usize << bits;
    (bucket & (step - 1)..remote.len())
        .step_by(step)
        .map(|entry| remote.bucket_at(entry))
        .collect()
}

/// One type's share of an upload, from the snapshot to the encoded pages.
pub struct SlotUpload {
    /// Index into [`PageStore::types`].
    pub(crate) idx: usize,
    /// The disk directory as it stood when the upload began.
    pub(crate) disk: Directory,
    /// Every flagged disk bucket taken, with its descriptor then.
    pub(crate) flagged: Vec<(usize, BlockDescriptor)>,
    /// Those buckets' pages, in the same order.
    pub(crate) pages: Vec<SlotPage>,
    /// The remote directory being rebuilt — the upload's own copy.
    pub(crate) remote: Directory,
    /// Remote bucket → its rebuilt page.
    pub(crate) staged: BTreeMap<usize, SlotPage>,
    /// Remote buckets carried forward as they are, out of files a
    /// compaction empties (`crate::remote_compact`).
    pub(crate) moved: BTreeSet<usize>,
    /// Whether the dictionary is carried forward too.
    pub(crate) move_dict: bool,
}

impl SlotUpload {
    /// The remote buckets this upload rewrites: those sharing records with a
    /// flagged disk page, and those carried forward.
    pub(crate) fn affected(&self) -> BTreeSet<usize> {
        self.flagged
            .iter()
            .flat_map(|&(d, _)| sharing(&self.disk, d, &self.remote))
            .chain(self.moved.iter().copied())
            .collect()
    }

    /// Rebuild the staged pages: each holds its old records (fetched into
    /// `staged` beforehand) minus those of a flagged disk bucket, plus the
    /// flagged pages' records that route to it.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] if a record routes to a remote page the
    /// congruence did not name — the arithmetic above is wrong, and writing
    /// on would drop that page's other records.
    pub(crate) fn rebuild(&mut self) -> StorageResult<()> {
        let flagged: BTreeSet<usize> =
            self.flagged.iter().map(|&(d, _)| d).collect();
        for page in self.staged.values_mut() {
            let stale: Vec<Id> = page
                .iter()
                .map(|(id, _)| id)
                .filter(|id| flagged.contains(&self.disk.bucket_of(id.raw())))
                .collect();
            for id in stale {
                page.remove(id);
            }
        }
        for page in std::mem::take(&mut self.pages) {
            for (id, bytes) in page.iter() {
                let bucket = self.remote.bucket_of(id.raw());
                self.staged
                    .get_mut(&bucket)
                    .ok_or(StorageError::Corrupt("upload congruence"))?
                    .upsert(id, bytes.to_vec());
            }
        }
        Ok(())
    }
}

impl PageStore {
    /// Encode one remote page with its type's dictionary; an empty page is
    /// no bytes at all — its bucket becomes vacant.
    pub(crate) fn encode_remote(
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
