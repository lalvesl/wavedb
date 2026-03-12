//! Object mode ([RFC 0070] §9) — a store with no disk at all.
//!
//! Its pages live in the object store, and what it holds besides — the
//! pages not uploaded yet and those it caches — is laid out in RAM exactly
//! as `data.bin` would lay it out (`crate::backing`). There is no journal:
//! **a write is durable once an upload has carried it.** The store uploads
//! when it holds a file's worth of pages or when its oldest write has
//! waited the caller's interval ([`unuploaded_for`](PageStore::unuploaded_for)),
//! so a crash loses at most that interval's writes, and a close that uploads
//! everything first ([`upload_all`](PageStore::upload_all)) loses nothing.
//!
//! No addressing log either: nothing ever reopens from RAM. Every open is a
//! recovery from the files (`crate::recover`), which take only a **cut** —
//! an upload carrying every batch up to a point, whole — so what a crash
//! leaves is always the database as it stood at some acknowledged batch.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

use std::task::Poll;
use std::time::{Duration, Instant};

use crate::block::BlockAllocator;
use crate::block_file::{BlockFile, RESERVED_BLOCKS, random_seed};
use crate::error::{StorageError, StorageResult};
use crate::page_store::{PageStore, StoreOptions, slots_of};
use crate::struct_storage::StructStorage;

impl PageStore {
    /// Open the database kept in `options.remote`, with no disk: recovered
    /// from its files, every page left in the object store until a read or
    /// a write wants it, and created empty when the prefix holds none.
    ///
    /// One writer per prefix is the caller's to arrange — there is no lock.
    /// A second writer is not silent, though: every file is created only if
    /// absent, so whichever store meets the other's file first is fenced
    /// ([`StorageError::Fenced`]) instead of overwriting it.
    ///
    /// # Errors
    /// [`StorageError::NotObjectBacked`] without `options.remote`; the
    /// recovery's faults.
    pub fn open_object(
        types: &[&'static StructStorage],
        options: StoreOptions,
    ) -> StorageResult<Self> {
        let remote = options
            .remote
            .clone()
            .ok_or(StorageError::NotObjectBacked)?;
        let found =
            futures::executor::block_on(crate::recover::recover(&remote))?;
        let seed = found.as_ref().and_then(|f| f.seed);
        let file = BlockFile::in_memory(seed.unwrap_or_else(random_seed));
        let mut alloc = BlockAllocator::new();
        alloc.alloc(RESERVED_BLOCKS);
        let store = Self::assemble(
            file,
            None,
            slots_of(types),
            (alloc, crate::meta_log::MetaLog::default()),
            options,
        );
        if let Some(found) = found {
            store.adopt(found)?;
        }
        Ok(store)
    }

    /// Whether this store has no disk (object mode).
    #[must_use]
    pub const fn is_object_mode(&self) -> bool {
        self.disk.is_none()
    }

    /// Note a committed write the object store has not received: the
    /// upload interval runs from the oldest one.
    pub(crate) fn note_unuploaded(&self) {
        if let Some(object) = &self.object
            && object.oldest_write.get().is_none()
        {
            object.oldest_write.set(Some(Instant::now()));
        }
    }

    /// How long the oldest write no upload has carried yet has waited —
    /// what an upload interval is compared with. `None` when every write is
    /// in the object store, and for a disk-only store.
    #[must_use]
    pub fn unuploaded_for(&self) -> Option<Duration> {
        self.object
            .as_ref()
            .and_then(|o| o.oldest_write.get())
            .map(|at| at.elapsed())
    }

    /// Whether another writer's file was met ([`StorageError::Fenced`]).
    #[must_use]
    pub fn is_fenced(&self) -> bool {
        self.object.as_ref().is_some_and(|o| o.fenced.get())
    }

    /// Upload until every committed write is in the object store — what a
    /// close does first, so that an object-mode store closed in order loses
    /// nothing.
    ///
    /// # Errors
    /// [`StorageError::NotObjectBacked`] for a disk-only store; the first
    /// upload's fault.
    pub async fn upload_all(&self) -> StorageResult<()> {
        self.object()?;
        loop {
            self.upload().await?;
            if self.unuploaded_for().is_none() {
                return Ok(());
            }
            // Another upload may hold the turn: let it run.
            let mut yielded = false;
            futures::future::poll_fn(|cx| {
                if yielded {
                    return Poll::Ready(());
                }
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            })
            .await;
        }
    }
}
