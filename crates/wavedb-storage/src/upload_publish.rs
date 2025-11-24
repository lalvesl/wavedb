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
use crate::page_store::PageStore;
use crate::upload::{IN_FLIGHT, Uploaded};
use crate::upload_file::{Packed, SlotImages};
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

    /// Step 4, after every PUT: adopt the remote directories and clear the
    /// flag of each page still at the descriptor it was taken at — one a
    /// settle round rewrote meanwhile is newer than what went up, and keeps
    /// its flag. What changed waits, unrecorded, for the next header.
    pub(crate) fn publish_upload(
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
            moved: 0,
        };
        let mut unrecorded = self.unrecorded.borrow_mut();
        for (w, upload) in work.into_iter().enumerate() {
            let slot = &self.types[upload.idx];
            let touched = unrecorded.slots.entry(upload.idx).or_default();
            touched.remote.extend(slots[w].pages.keys());
            *slot.remote_directory().borrow_mut() = Some(upload.remote);
            if let (Some(len), Some(desc)) = (carried.lens[w], carried.descs[w])
            {
                slot.uploaded_dict().set(len);
                slot.remote_dict().set(desc);
                touched.remote_dict = true;
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
        if !self.has_disk() {
            // Object mode keeps no header to record it in.
            unrecorded.slots.clear();
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
