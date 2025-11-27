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

use crate::block::{BLOCK_SIZE, BlockDescriptor};
use crate::error::{StorageError, StorageResult};
use crate::header::{Chain, NO_FILE};
use crate::ordered::{Found, Placed as Entry};
use crate::page_store::PageStore;
use crate::roots::ROOTS_BLOCKS;
use crate::shape::blocks_for;
use crate::upload_file::{
    Named, Packed, Placed, SlotImages, finish, layout, rebase,
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
        // upload fails, or its headers could not name all of it.
        let carried = if cut {
            object.oldest_write.take()
        } else {
            None
        };
        let moved = work.iter().map(|w| w.moved.len()).sum();
        let shipped = self.ship(work, cut).await;
        let whole = matches!(shipped, Ok((_, true)));
        if !whole && let Some(at) = carried {
            object.oldest_write.set(Some(at));
        }
        shipped.map(|(done, _)| Uploaded { moved, ..done })
    }

    /// Everything after the take: fetch, rebuild, pack, PUT, publish —
    /// and whether it ended a batch.
    async fn ship(
        &self,
        mut work: Vec<SlotUpload>,
        cut: bool,
    ) -> StorageResult<(Uploaded, bool)> {
        let object = self.object()?;
        if work.is_empty() {
            return Ok((Uploaded::default(), false));
        }
        self.fetch_shared(&mut work).await?;
        let mut slots = Vec::with_capacity(work.len());
        // The dictionary length each carried copy holds.
        let mut dicts = Vec::with_capacity(work.len());
        for w in &mut work {
            self.rebuild_remote(w)?;
            let slot = &self.types[w.idx];
            let dict = slot.dictionary().borrow();
            let len = dict.dictionary().len();
            let carried = dict.enabled()
                && (len > slot.uploaded_dict().get() || w.move_dict);
            dicts.push(carried.then_some(len));
            let pages = w
                .shaped
                .pages
                .iter()
                .map(|p| {
                    let blocks = w.remote.canonical(blocks_for(p.image.len()));
                    (p.image.clone(), blocks)
                })
                .collect();
            slots.push(SlotImages {
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
        let placed_dicts = install_remote(&mut work, &packed)?;
        let headers = self.write_headers(
            &mut packed,
            &mut work,
            &placed_dicts,
            &mut chain,
        );
        let ended = headers.as_ref().is_ok_and(|&named| named && cut);
        let delivered = match (headers, link) {
            (Err(e), Some(link)) => {
                link.abandon(packed.files.iter().map(|f| f.base).collect());
                Err(e)
            }
            (Err(e), None) => Err(e),
            (Ok(_), None) => self.put_files(&packed).await.inspect_err(|_| {
                // Any of the files may have landed: their numbers are never
                // written again (`crate::recover`), so the next upload starts
                // past them.
                object.chain.borrow_mut().end = packed.end;
            }),
            (Ok(_), Some(link)) => {
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
        let done = self.publish_upload(work, &carried, &packed, chain);
        Ok((done, ended))
    }

    /// Write every file's header ([`finish`]) over every registered type's
    /// index in the object store as this upload leaves it — the work's, the
    /// others as they stand, a type never uploaded with none. Whether the
    /// headers named everything.
    fn write_headers(
        &self,
        packed: &mut Packed,
        work: &mut [SlotUpload],
        placed_dicts: &[Option<BlockDescriptor>],
        chain: &mut Chain,
    ) -> StorageResult<bool> {
        let mut guards: Vec<_> = self
            .types
            .iter()
            .map(|slot| slot.remote_directory().borrow_mut())
            .collect();
        let mut by_idx: BTreeMap<
            usize,
            (&mut SlotUpload, Option<BlockDescriptor>),
        > = work
            .iter_mut()
            .zip(placed_dicts)
            .map(|(w, &d)| (w.idx, (w, d)))
            .collect();
        let mut named = Vec::with_capacity(self.types.len());
        for ((idx, slot), guard) in
            self.types.iter().enumerate().zip(&mut guards)
        {
            let (index, desc, moved) = match by_idx.remove(&idx) {
                Some((w, placed)) => (
                    Some(&mut w.remote),
                    placed.unwrap_or_else(|| slot.remote_dict().get()),
                    placed.is_some(),
                ),
                None => (guard.as_mut(), slot.remote_dict().get(), false),
            };
            named.push(Named {
                struct_hash: slot.struct_hash(),
                index,
                dict: desc.is_allocated().then_some(desc.raw()),
                dict_moved: moved,
            });
        }
        finish(packed, &mut named, chain, self.position)
    }
}

/// Point every placed page's entry in the work's remote index at its run,
/// and keep each copy of what changed for the disk's headers. The
/// dictionary each work item's upload placed, if it placed one.
fn install_remote(
    work: &mut [SlotUpload],
    packed: &Packed,
) -> StorageResult<Vec<Option<BlockDescriptor>>> {
    let mut dicts = vec![None; work.len()];
    for placed in &packed.placed {
        match *placed {
            Placed::Page { slot, page, run } => {
                let w = &mut work[slot];
                let key = &w.shaped.pages[page].key;
                let at = match w.remote.find(key) {
                    Found::Page { at, .. } | Found::Hole(at) => at,
                    Found::Uncovered => {
                        return Err(StorageError::Corrupt(
                            "an uploaded page lost its entry",
                        ));
                    }
                };
                let entry = Entry {
                    address: run.start,
                    blocks: run.count,
                    meta: 0,
                };
                w.remote
                    .retarget(at, entry)
                    .ok_or(StorageError::Corrupt("a run no range can name"))?;
            }
            Placed::Dict { slot, desc } => dicts[slot] = Some(desc),
        }
    }
    for w in work.iter_mut() {
        w.remote.split_long();
        w.changes = w.remote.journal().clone();
    }
    Ok(dicts)
}
