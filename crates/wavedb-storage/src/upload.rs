//! The upload ([RFC 0070] §5): the disk pages the object store has not
//! received, reassembled record by record into its pages and shipped in
//! files.
//!
//! 1. **Settle, then take** — with no await between: every committed batch
//!    goes into the pages, then the flagged pages are taken, up to
//!    [`UPLOAD_INPUT_BLOCKS`] — each the whole truth about its disk bucket,
//!    removals included. A take that leaves nothing flagged behind is a
//!    **cut** (`Header::end_of_batch`): with the uploads before it, it holds
//!    every batch up to here and no batch in part, and only a cut is ever
//!    recovered (`crate::recover`).
//! 2. **Fetch** the remote pages that share records with them, all at once.
//! 3. **Rebuild** those pages, **split** where the turn's bucket runs over,
//!    **encode**, and **pack** them into files (`crate::upload_file`).
//! 4. **PUT** the files, each only if its key is absent, then **publish**:
//!    the remote directories, and the flags of the pages whose descriptor
//!    did not move meanwhile, cleared.
//!
//! Nothing is borrowed across an await: the snapshot copies what it needs
//! out of the cells, and the cells are touched again only to publish. A
//! settle round may run between two awaits — it writes newer, flagged pages,
//! which the next upload carries. Publishing records nothing on disk: the
//! next window carries it, whatever else it holds (RFC 0070 §5). A crash
//! before then uploads those pages again, past the files the lost upload
//! left — a file number is never written twice (`step_over_leftovers`), and
//! neither is the number of a file whose PUT failed. So a file already
//! there is another writer's: the store is fenced
//! ([`StorageError::Fenced`]) rather than overwrite it.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::cell::Cell;
use std::collections::BTreeMap;

use futures::stream::{self, StreamExt, TryStreamExt};

use crate::block::BLOCK_SIZE;
use crate::block::BlockDescriptor;
use crate::directory::Directory;
use crate::error::{StorageError, StorageResult};
use crate::header::Chain;
use crate::header::NO_FILE;
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::remote::weight;
use crate::roots::ROOTS_BLOCKS;
use crate::upload_file::{
    Packed, Placed, SlotImages, TypeView, finish, layout, rebase,
};
use crate::upload_plan::SlotUpload;
use crate::upload_publish::Carried;

/// Blocks of flagged disk pages one upload takes (8 MiB) — a pacing bound,
/// so the files it builds stay a few, and a backlog drains over several.
pub const UPLOAD_INPUT_BLOCKS: u64 = 2048;

/// Object-store requests one upload keeps in flight.
pub const IN_FLIGHT: usize = 32;

/// What one [`upload`](PageStore::upload) shipped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Uploaded {
    /// Files written.
    pub files: usize,
    /// Disk pages whose flag was cleared.
    pub pages: usize,
    /// Bytes PUT.
    pub bytes: u64,
    /// Remote pages carried forward out of files compaction empties
    /// ([`compact_remote`](PageStore::compact_remote)).
    pub moved: usize,
}

/// Clears the one-upload-at-a-time mark, however the upload ends.
pub struct Release<'a>(pub &'a Cell<bool>);

impl Drop for Release<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

impl PageStore {
    /// Upload up to 8 MiB of the disk pages the object store
    /// has not received (RFC 0070 §5). Returns what it shipped — nothing
    /// when nothing is pending, or another upload is under way.
    ///
    /// The worker serves other operations while this awaits the object
    /// store; nothing it reads can see a half-published upload, since
    /// publishing happens between two awaits.
    ///
    /// # Errors
    /// [`StorageError::NotObjectBacked`]
    /// for a disk-only store; [`StorageError::Fenced`] once another writer's
    /// file was met; an object-store or page fault. Nothing is published
    /// then: the flags stay set and a later upload retries.
    pub async fn upload(&self) -> StorageResult<Uploaded> {
        let object = self.object()?;
        if object.uploading.replace(true) {
            return Ok(Uploaded::default());
        }
        let _one = Release(&object.uploading);
        self.upload_turn().await
    }

    /// [`upload`](Self::upload) with the turn already held — by it, or by a
    /// compaction ([`compact_remote`](Self::compact_remote)).
    pub(crate) async fn upload_turn(&self) -> StorageResult<Uploaded> {
        let object = self.object()?;
        if object.fenced.get() {
            return Err(StorageError::Fenced);
        }
        // Every committed batch into the pages first, and the take right
        // after it, with no await between: what goes up is then every batch
        // up to here, whole (`cut`), when the take leaves nothing behind.
        self.drain()?;
        let (work, cut) = self.take_flagged()?;
        // Taken at a cut: the oldest write waiting is carried — back if the
        // upload fails.
        let carried = if cut {
            object.oldest_write.take()
        } else {
            None
        };
        let moved = work.iter().map(|w| w.moved.len()).sum();
        let shipped = self.ship(work, cut).await;
        if shipped.is_err()
            && let Some(at) = carried
        {
            object.oldest_write.set(Some(at));
        }
        shipped.map(|done| Uploaded { moved, ..done })
    }

    /// Everything after the take: fetch, rebuild, pack, PUT, publish.
    async fn ship(
        &self,
        mut work: Vec<SlotUpload>,
        cut: bool,
    ) -> StorageResult<Uploaded> {
        let object = self.object()?;
        if work.is_empty() {
            return Ok(Uploaded::default());
        }
        self.fetch_shared(&mut work).await?;
        let mut slots = Vec::with_capacity(work.len());
        // The dictionary length each carried copy holds.
        let mut dicts = Vec::with_capacity(work.len());
        for w in &mut work {
            w.rebuild()?;
            let pages = self.split_remote(w).await?;
            let slot = &self.types[w.idx];
            let dict = slot.dictionary().borrow();
            let len = dict.dictionary().len();
            let carried = dict.enabled()
                && (len > slot.uploaded_dict().get() || w.move_dict);
            dicts.push(carried.then_some(len));
            slots.push(SlotImages {
                struct_hash: slot.struct_hash(),
                pages,
                dict: carried.then(|| dict.image()),
            });
        }
        // Through a storage shard, the files are laid out from block 0 and
        // moved to the sections it reserves (RFC 0073 §8).
        let link = object.link.as_ref();
        let end = link.map_or_else(|| object.chain.borrow().end, |_| 0);
        let mut packed = layout(end, &slots);
        packed.cut = cut;
        let mut chain = object.chain.borrow().clone();
        if let Some(link) = link {
            let sizes = packed
                .files
                .iter()
                .map(|f| {
                    f.bytes.len().div_ceil(BLOCK_SIZE) as u64 - ROOTS_BLOCKS
                })
                .collect();
            let reserved = link.reserve(sizes).await?;
            rebase(&mut packed, &reserved.bases);
            chain.roots = (reserved.roots != NO_FILE).then_some(reserved.roots);
            chain.start = chain.start.max(reserved.start);
            chain.end = reserved.end;
        }
        let mut placed_dicts = vec![None; work.len()];
        for placed in &packed.placed {
            match *placed {
                Placed::Page { slot, bucket, desc } => {
                    work[slot].remote.set_descriptor(bucket, desc);
                }
                Placed::Dict { slot, desc } => placed_dicts[slot] = Some(desc),
            }
        }
        let headers =
            self.write_headers(&mut packed, &work, &placed_dicts, &mut chain);
        let delivered = match (headers, link) {
            (Err(e), Some(link)) => {
                link.abandon(packed.files.iter().map(|f| f.base).collect());
                Err(e)
            }
            (Err(e), None) => Err(e),
            (Ok(()), None) => self.put_files(&packed).await.inspect_err(|_| {
                // Any of the files may have landed: their numbers are never
                // written again (`crate::recover`), so the next upload starts
                // past them.
                object.chain.borrow_mut().end = packed.end;
            }),
            (Ok(()), Some(link)) => {
                let sections = packed
                    .files
                    .iter()
                    .map(|f| (f.base, f.bytes.clone()))
                    .collect();
                let hurry = (cut, object.hurry.get());
                link.write(self.position as usize, sections, hurry).await
            }
        };
        delivered?;
        let carried = Carried {
            lens: dicts,
            descs: placed_dicts,
        };
        Ok(self.publish_upload(work, &slots, &carried, &packed, chain))
    }

    /// Write every file's header ([`finish`]) over a view of every
    /// registered type as the object store holds it once this upload is in
    /// — the work's new directories, the others as they stand, a type never
    /// uploaded with none.
    fn write_headers(
        &self,
        packed: &mut Packed,
        work: &[SlotUpload],
        placed_dicts: &[Option<BlockDescriptor>],
        chain: &mut Chain,
    ) -> StorageResult<()> {
        let guards: Vec<_> = self
            .types
            .iter()
            .map(|slot| slot.remote_directory().borrow())
            .collect();
        let mut views = Vec::new();
        for (idx, slot) in self.types.iter().enumerate() {
            let at = work.iter().position(|w| w.idx == idx);
            let slots = match (at, guards[idx].as_ref()) {
                (Some(w), _) => work[w].remote.slots(),
                (None, Some(dir)) => dir.slots(),
                (None, None) => &[],
            };
            let dict = at
                .and_then(|w| placed_dicts[w])
                .unwrap_or_else(|| slot.remote_dict().get());
            views.push(TypeView {
                struct_hash: slot.struct_hash(),
                slots,
                dict: dict.is_allocated().then_some(dict.raw()),
            });
        }
        finish(packed, &views, chain, self.position)
    }

    /// Step 1: every type's flagged disk pages, up to the input bound, and a
    /// copy of each type's remote directory — and whether that took every
    /// flagged page, the bound leaving none behind (a cut). The remote pages
    /// a compaction carries forward come along, up to their own bound.
    fn take_flagged(&self) -> StorageResult<(Vec<SlotUpload>, bool)> {
        let (mut taken, mut cut) = (0, true);
        let mut moving = crate::remote_compact::MOVE_BLOCKS;
        let mut work = Vec::new();
        for (idx, slot) in self.types.iter().enumerate() {
            let (moved, move_dict) = self.moves_of(idx, &mut moving);
            let disk = match slot.directory().borrow().clone() {
                Some(disk) => disk,
                None if moved.is_empty() && !move_dict => continue,
                None => Directory::new(self.seed),
            };
            let (mut flagged, mut pages) = (Vec::new(), Vec::new());
            let dict = slot.dictionary().borrow();
            for (bucket, &desc) in disk.slots().iter().enumerate() {
                if !desc.pending_upload() {
                    continue;
                }
                if taken >= UPLOAD_INPUT_BLOCKS {
                    cut = false;
                    continue;
                }
                let hash = slot.struct_hash();
                pages.push(disk.read_page(hash, &self.file, bucket, &dict)?);
                flagged.push((bucket, desc));
                taken += weight(desc);
            }
            if flagged.is_empty() && moved.is_empty() && !move_dict {
                continue;
            }
            let remote = slot.remote_directory().borrow().clone();
            work.push(SlotUpload {
                idx,
                disk,
                flagged,
                pages,
                remote: remote.unwrap_or_else(|| Directory::new(self.seed)),
                staged: BTreeMap::new(),
                moved,
                move_dict,
            });
        }
        Ok((work, cut))
    }

    /// Step 2: stage every remote page the flagged ones share records with —
    /// fetched, [`IN_FLIGHT`] at a time, or empty where the bucket is vacant.
    async fn fetch_shared(&self, work: &mut [SlotUpload]) -> StorageResult<()> {
        let mut wanted = Vec::new();
        for (w, slot) in work.iter_mut().enumerate() {
            let hash = self.types[slot.idx].struct_hash();
            for bucket in slot.affected() {
                let desc = slot.remote.descriptor(bucket);
                if desc.is_allocated() {
                    wanted.push((w, slot.idx, bucket, desc));
                } else {
                    slot.staged.insert(bucket, SlotPage::new(hash));
                }
            }
        }
        let fetched: Vec<(usize, usize, SlotPage)> = stream::iter(wanted)
            .map(|(w, idx, bucket, desc)| async move {
                let page =
                    self.with_remote_page(idx, desc, SlotPage::clone).await?;
                StorageResult::Ok((w, bucket, page))
            })
            .buffer_unordered(IN_FLIGHT)
            .try_collect()
            .await?;
        for (w, bucket, page) in fetched {
            work[w].staged.insert(bucket, page);
        }
        Ok(())
    }
}
