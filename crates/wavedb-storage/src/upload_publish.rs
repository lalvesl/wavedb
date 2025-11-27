//! The upload's last step ([RFC 0070] §5): the files go up, each only if
//! its key is absent, and only then is anything published. Split from
//! [`crate::upload`], which takes, fetches, rebuilds and packs.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use futures::stream::{self, StreamExt, TryStreamExt};
use wavedb_platform::s3::{Put, When};

use crate::block::BlockDescriptor;
use crate::error::{StorageError, StorageResult};
use crate::header::Chain;
use crate::ordered::Found;
use crate::page_store::PageStore;
use crate::pages::PENDING;
use crate::shape::weight;
use crate::upload::{IN_FLIGHT, Uploaded};
use crate::upload_file::Packed;
use crate::upload_plan::SlotUpload;

impl PageStore {
    /// PUT every file, [`IN_FLIGHT`] at a time, each only if its key is
    /// absent: a number is written once, so a file already there is another
    /// writer's, and this store is fenced ([`StorageError::Fenced`]).
    pub(crate) async fn put_files(&self, packed: &Packed) -> StorageResult<()> {
        let object = self.object()?;
        let remote = &object.remote;
        stream::iter(&packed.files)
            .map(|file| {
                let key = remote.file_key(file.number);
                async move {
                    match remote
                        .bucket()
                        .put(&key, &file.bytes, When::Absent)
                        .await?
                    {
                        Put::Stored { .. } => Ok(()),
                        Put::Refused => Err(StorageError::Fenced),
                    }
                }
            })
            .buffer_unordered(IN_FLIGHT)
            .try_for_each(|()| async { Ok(()) })
            .await
            .inspect_err(|e| {
                if matches!(e, StorageError::Fenced) {
                    object.fenced.set(true);
                }
            })
    }

    /// Step 4, after every PUT: adopt the remote indexes — what they changed
    /// noted for the disk's headers — and clear the flag of each disk entry
    /// still as it was taken; one a settle round rewrote meanwhile is newer
    /// than what went up, and keeps its flag.
    pub(crate) fn publish_upload(
        &self,
        work: Vec<SlotUpload>,
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
            moved: 0,
        };
        let mut unrecorded = self.unrecorded.borrow_mut();
        for (w, upload) in work.into_iter().enumerate() {
            let slot = &self.types[upload.idx];
            if self.has_disk() {
                slot.remote_journal().borrow_mut().merge(&upload.changes);
            }
            *slot.remote_directory().borrow_mut() = Some(upload.remote);
            if let (Some(len), Some(desc)) = (carried.lens[w], carried.descs[w])
            {
                slot.uploaded_dict().set(len);
                slot.remote_dict().set(desc);
                unrecorded.slots.entry(upload.idx).or_default().remote_dict =
                    true;
            }
            let mut live = slot.directory().borrow_mut();
            let Some(disk) = live.as_mut() else { continue };
            for f in upload.flagged {
                let now = match disk.find(&f.lo) {
                    Found::Page { at, page } => (at, Some(page)),
                    Found::Hole(at) => (at, None),
                    Found::Uncovered => continue,
                };
                let (at, page) = now;
                let flags = page
                    .map_or_else(|| disk.meta_at(at).unwrap_or(0), |p| p.meta);
                if page != f.taken || flags != f.flags {
                    continue;
                }
                let blocks = page.map_or(0, |p| p.blocks);
                disk.set_meta(at, flags & !PENDING);
                object.adjust_pending(0, weight(flags, blocks));
                done.pages += 1;
            }
        }
        if !self.has_disk() {
            // Object mode keeps no header to record it in.
            unrecorded.slots.clear();
            for slot in &self.types {
                if let Some(disk) = slot.directory().borrow_mut().as_mut() {
                    let _ = disk.drain(usize::MAX);
                }
            }
        }
        *object.chain.borrow_mut() = chain;
        done
    }
}

/// The dictionaries an upload carried, per work item: the length of each
/// copy and where it landed.
pub struct Carried {
    pub lens: Vec<Option<usize>>,
    pub descs: Vec<Option<BlockDescriptor>>,
}
