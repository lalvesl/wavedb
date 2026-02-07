//! Converting a database between disk-only and object-backed ([RFC 0070]
//! §9) — offline, with the database closed; the node then opens it in its
//! new mode.
//!
//! Between disk + object and object mode there is nothing to convert: the
//! files are the same, and a close and an open is the whole of it. What
//! changes the stored mode is moving the pages:
//!
//! - **To the object store** ([`Backend::ObjectStore`]): the prefix must
//!   hold no database. Its `MANIFEST` goes up first, then every page on disk
//!   is flagged "not uploaded yet" and a durable batch names the flags, and the
//!   local `MANIFEST` says object-backed last. The next open with the object
//!   store uploads it all, as it uploads any backlog.
//! - **To the disk** ([`Backend::Disk`]): opened object-backed — recovered
//!   from the object store when there is no disk yet — every evicted page is
//!   brought back, the flags cleared, a durable batch names it, and the local
//!   `MANIFEST` says disk-only last. The object store's files stay where
//!   they are, as they were; nothing reads them any more.
//!
//! Each step is safe to stop after: the stored mode changes only at the
//! last, and running the conversion again finishes it.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::path::Path;

use futures::executor::block_on;

use crate::block::BlockDescriptor;
use crate::error::{StorageError, StorageResult};
use crate::manifest;
use crate::page_store::{PageStore, StoreOptions};
use crate::recover::newest_file;
use crate::shard_set::open_shards_with;
use crate::struct_storage::StructStorage;

/// Where a database's pages live — what [`convert`] turns it into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// The disk alone.
    Disk,
    /// An object store, the disk caching it (or, opened in object mode,
    /// no disk at all).
    ObjectStore,
}

/// Convert the database at `dir` to keep its pages in `to`.
///
/// `options.remote` is the object store to upload to, or to bring the pages
/// back from (see the module docs). Converting a database already in `to`
/// does nothing.
///
/// # Errors
/// [`StorageError::NotObjectBacked`] without `options.remote`;
/// [`StorageError::NoDatabase`] when there is nothing to convert;
/// [`StorageError::ObjectStoreInUse`] when converting to an object store
/// whose prefix holds another database; the opens', reads' and writes'
/// faults — after any of which running it again carries on.
pub fn convert(
    dir: impl AsRef<Path>,
    types: &[&'static StructStorage],
    options: StoreOptions,
    to: Backend,
) -> StorageResult<()> {
    let dir = dir.as_ref();
    if options.remote.is_none() {
        return Err(StorageError::NotObjectBacked);
    }
    match to {
        Backend::ObjectStore => to_object_store(dir, types, options),
        Backend::Disk => to_disk(dir, types, options),
    }
}

fn to_object_store(
    dir: &Path,
    types: &[&'static StructStorage],
    mut options: StoreOptions,
) -> StorageResult<()> {
    let remote = options.remote.take().ok_or(StorageError::NotObjectBacked)?;
    let facts = manifest::read_local(dir)?.ok_or(StorageError::NoDatabase)?;
    if facts.object_backed() {
        return Ok(());
    }
    let target = facts.with_object_backed(true);
    for n in 0..facts.storage_shards() {
        if block_on(newest_file(&remote.storage(n)))?.is_some() {
            return Err(StorageError::ObjectStoreInUse);
        }
    }
    if manifest::write_remote(&remote, target)? != target {
        return Err(StorageError::ObjectStoreInUse);
    }
    // `options` holds no object store any more: the database opens disk-only.
    for store in open_shards_with(dir, types, options, facts.shards(), 1)? {
        store.flag_every_page()?;
    }
    manifest::write_local(dir, &target)
}

fn to_disk(
    dir: &Path,
    types: &[&'static StructStorage],
    options: StoreOptions,
) -> StorageResult<()> {
    let remote = options
        .remote
        .clone()
        .ok_or(StorageError::NotObjectBacked)?;
    let local = manifest::read_local(dir)?;
    if local.is_some_and(|facts| !facts.object_backed()) {
        return Ok(());
    }
    let Some(facts) = local.or(manifest::read_remote(&remote)?) else {
        return Err(StorageError::NoDatabase);
    };
    for store in open_shards_with(dir, types, options, facts.shards(), 1)? {
        block_on(store.restore_every_page())?;
        store.clear_every_flag()?;
    }
    manifest::write_local(dir, &facts.with_object_backed(false))
}

impl PageStore {
    /// Flag every page on disk "not uploaded yet", and make it durable: a
    /// batch whose headers name every bucket names the flags.
    fn flag_every_page(&self) -> StorageResult<()> {
        // Everything committed into the pages first: a settle round after
        // this would write pages without the flag.
        self.drain()?;
        self.set_every_flag(true);
        self.record_all();
        self.drain()
    }

    /// Clear every page's flag, and make it durable as
    /// [`flag_every_page`](Self::flag_every_page) does.
    fn clear_every_flag(&self) -> StorageResult<()> {
        self.drain()?;
        self.set_every_flag(false);
        self.record_all();
        self.drain()
    }

    fn set_every_flag(&self, pending: bool) {
        for slot in &self.types {
            let mut dir = slot.directory().borrow_mut();
            let Some(dir) = dir.as_mut() else { continue };
            for bucket in 0..dir.len() {
                let desc = dir.descriptor(bucket);
                if desc.is_allocated() {
                    dir.set_descriptor(
                        bucket,
                        desc.with_pending_upload(pending),
                    );
                }
            }
        }
    }

    /// Bring every evicted page back from the object store.
    async fn restore_every_page(&self) -> StorageResult<()> {
        for idx in 0..self.types.len() {
            let evicted: Vec<usize> = self.types[idx]
                .directory()
                .borrow()
                .iter()
                .flat_map(|dir| dir.slots().iter().enumerate())
                .filter(|(_, desc)| desc.is_evicted())
                .map(|(bucket, _)| bucket)
                .collect();
            for bucket in evicted {
                self.restore(idx, bucket).await?;
            }
        }
        Ok(())
    }

    /// A disk-only store keeps no remote directory: a database converted to
    /// the disk drops the ones its headers still name, and the next headers
    /// leave them out.
    pub(crate) fn forget_remote(&self) {
        for slot in &self.types {
            *slot.remote_directory().borrow_mut() = None;
            slot.remote_dict().set(BlockDescriptor::EMPTY);
        }
    }
}
