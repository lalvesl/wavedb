//! The upload ([RFC 0070] §5): the disk pages the object store has not
//! received, reassembled record by record into its pages and shipped in
//! files.
//!
//! 1. **Take** the flagged disk pages, up to [`UPLOAD_INPUT_BLOCKS`] — each
//!    is the whole truth about its disk bucket, removals included.
//! 2. **Fetch** the remote pages that share records with them, all at once.
//! 3. **Rebuild** those pages, **split** where the turn's bucket runs over,
//!    **encode**, and **pack** them into files (`crate::upload_file`).
//! 4. **PUT** the files, then **publish**: the remote directories, and the
//!    flags of the pages whose descriptor did not move meanwhile, cleared.
//!
//! Nothing is borrowed across an await: the snapshot copies what it needs
//! out of the cells, and the cells are touched again only to publish. A
//! settle round may run between two awaits — it writes newer, flagged pages,
//! which the next upload carries. Publishing records nothing on disk: the
//! next window carries it, whatever else it holds (RFC 0070 §5). A crash
//! before then uploads those pages again, past the files the lost upload
//! left — a file number is never written twice (`step_over_leftovers`).
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

use std::cell::Cell;
use std::collections::BTreeMap;

use futures::stream::{self, StreamExt, TryStreamExt};
use wavedb_platform::s3::When;

use crate::block::BlockDescriptor;
use crate::directory::Directory;
use crate::error::StorageResult;
use crate::file_header::Chain;
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::remote::weight;
use crate::upload_file::{
    Packed, Placed, SlotImages, TypeView, finish, layout,
};
use crate::upload_plan::SlotUpload;

/// Blocks of flagged disk pages one upload takes (8 MiB) — a pacing bound,
/// so the files it builds stay a few, and a backlog drains over several.
pub const UPLOAD_INPUT_BLOCKS: u64 = 2048;

/// Object-store requests one upload keeps in flight.
const IN_FLIGHT: usize = 32;

/// What one [`upload`](PageStore::upload) shipped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Uploaded {
    /// Files written.
    pub files: usize,
    /// Disk pages whose flag was cleared.
    pub pages: usize,
    /// Bytes PUT.
    pub bytes: u64,
}

/// Clears the one-upload-at-a-time mark, however the upload ends.
struct Release<'a>(&'a Cell<bool>);

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
    /// [`StorageError::NotObjectBacked`](crate::StorageError::NotObjectBacked)
    /// for a disk-only store; an object-store or page fault. Nothing is
    /// published then: the flags stay set and a later upload retries.
    pub async fn upload(&self) -> StorageResult<Uploaded> {
        let object = self.object()?;
        if object.uploading.replace(true) {
            return Ok(Uploaded::default());
        }
        let _one = Release(&object.uploading);
        let mut work = self.take_flagged()?;
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
            let carried = dict.enabled() && len > slot.uploaded_dict().get();
            dicts.push(carried.then_some(len));
            slots.push(SlotImages {
                struct_hash: slot.struct_hash(),
                pages,
                dict: carried.then(|| dict.image()),
            });
        }
        let types = self
            .types
            .iter()
            .enumerate()
            .filter(|(idx, slot)| {
                work.iter().any(|w| w.idx == *idx)
                    || slot.remote_directory().borrow().is_some()
            })
            .count();
        let mut packed = layout(object.chain.borrow().end, &slots, types);
        let mut placed_dicts = vec![None; work.len()];
        for placed in &packed.placed {
            match *placed {
                Placed::Page { slot, bucket, desc } => {
                    work[slot].remote.set_descriptor(bucket, desc);
                }
                Placed::Dict { slot, desc } => placed_dicts[slot] = Some(desc),
            }
        }
        let mut chain = object.chain.borrow().clone();
        self.write_headers(&mut packed, &work, &placed_dicts, &mut chain)?;
        let remote = &object.remote;
        stream::iter(&packed.files)
            .map(|file| {
                let key = remote.file_key(file.number);
                async move {
                    remote.bucket().put(&key, &file.bytes, When::Always).await
                }
            })
            .buffer_unordered(IN_FLIGHT)
            .try_for_each(|_| async { Ok(()) })
            .await?;
        let carried = Carried {
            lens: dicts,
            descs: placed_dicts,
        };
        Ok(self.publish_upload(work, &slots, &carried, &packed, chain))
    }

    /// Write every file's header ([`finish`]): each names every type the
    /// object store holds once this upload is in — the work's new
    /// directories, the others as they stand.
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
                (None, None) => continue,
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
        finish(packed, &views, chain, self.seed)
    }

    /// Step 1: every type's flagged disk pages, up to the input bound, and a
    /// copy of each type's remote directory.
    fn take_flagged(&self) -> StorageResult<Vec<SlotUpload>> {
        let mut taken = 0;
        let mut work = Vec::new();
        for (idx, slot) in self.types.iter().enumerate() {
            let Some(disk) = slot.directory().borrow().clone() else {
                continue;
            };
            let (mut flagged, mut pages) = (Vec::new(), Vec::new());
            let dict = slot.dictionary().borrow();
            for (bucket, &desc) in disk.slots().iter().enumerate() {
                if !desc.pending_upload() || taken >= UPLOAD_INPUT_BLOCKS {
                    continue;
                }
                let hash = slot.struct_hash();
                pages.push(disk.read_page(hash, &self.file, bucket, &dict)?);
                flagged.push((bucket, desc));
                taken += weight(desc);
            }
            if flagged.is_empty() {
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
            });
        }
        Ok(work)
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
                    wanted.push((w, bucket, desc));
                } else {
                    slot.staged.insert(bucket, SlotPage::new(hash));
                }
            }
        }
        let fetched: Vec<(usize, usize, Vec<u8>)> = stream::iter(wanted)
            .map(|(w, bucket, desc)| async move {
                let image = self.fetch_image(desc).await?;
                StorageResult::Ok((w, bucket, image))
            })
            .buffer_unordered(IN_FLIGHT)
            .try_collect()
            .await?;
        for (w, bucket, image) in fetched {
            let page = self.decode_remote(work[w].idx, &image)?;
            work[w].staged.insert(bucket, page);
        }
        Ok(())
    }

    /// Step 4, after every PUT: adopt the remote directories and clear the
    /// flag of each page still at the descriptor it was taken at — one a
    /// settle round rewrote meanwhile is newer than what went up, and keeps
    /// its flag. What changed waits in `unrecorded` for the next window.
    fn publish_upload(
        &self,
        work: Vec<SlotUpload>,
        slots: &[SlotImages],
        carried: &Carried,
        packed: &Packed,
        chain: Chain,
    ) -> Uploaded {
        let Some(object) = &self.object else {
            return Uploaded::default();
        };
        let mut done = Uploaded {
            files: packed.files.len(),
            pages: 0,
            bytes: packed.files.iter().map(|f| f.bytes.len() as u64).sum(),
        };
        let mut unrecorded = object.unrecorded.borrow_mut();
        for (w, upload) in work.into_iter().enumerate() {
            let slot = &self.types[upload.idx];
            let touched = unrecorded.slots.entry(upload.idx).or_default();
            touched.remote.extend(slots[w].pages.keys());
            *slot.remote_directory().borrow_mut() = Some(upload.remote);
            if let (Some(len), Some(desc)) = (carried.lens[w], carried.descs[w])
            {
                slot.uploaded_dict().set(len);
                slot.remote_dict().set(desc);
            }
            let mut live = slot.directory().borrow_mut();
            let Some(dir) = live.as_mut() else { continue };
            for (bucket, desc) in upload.flagged {
                if dir.descriptor(bucket) != desc {
                    continue;
                }
                let sent = desc.with_pending_upload(false);
                dir.set_descriptor(bucket, sent);
                object.retally(desc, sent);
                touched.disk.insert(bucket);
                done.pages += 1;
            }
        }
        *object.chain.borrow_mut() = chain;
        done
    }
}

/// The dictionaries an upload carried, per work item: the length of each
/// copy and where it landed.
struct Carried {
    lens: Vec<Option<usize>>,
    descs: Vec<Option<BlockDescriptor>>,
}
