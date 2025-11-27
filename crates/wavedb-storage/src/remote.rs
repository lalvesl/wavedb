//! The object store as one store sees it ([RFC 0070]): where its objects
//! are, how a remote descriptor becomes a key and a byte range, and the
//! state an object-backed store keeps beside its disk.
//!
//! ## Addressing is arithmetic
//!
//! Remote descriptors count 4 KiB blocks, as disk ones do. A file is
//! [`FILE_BLOCKS`] blocks (8 MiB) of address space in which pages *start*;
//! the last page of a file may run past it (§3), so a page's range always
//! lies inside the one object its start names:
//!
//! ```text
//! file   = start / 2048           → key  <prefix>1seg/<!file:016x>
//! offset = (start % 2048) · 4096  → range offset .. offset + count · 4096
//! ```
//!
//! Each file opens with a [`HEADER_BYTES`] header (its addressing delta,
//! `crate::upload_file`), so a file's first page starts at block
//! [`HEADER_BLOCKS`] of it.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::ops::Range;
use std::time::Instant;

use wavedb_platform::s3::Bucket;

use crate::block::{BLOCK_SIZE, Run};
use crate::error::{StorageError, StorageResult};
use crate::page_store::PageStore;
use crate::shape::weight;

/// Blocks of address space a file starts pages in (8 MiB).
pub const FILE_BLOCKS: u64 = 2048;

/// Bytes of every file's header — always this many, whatever it holds.
pub const HEADER_BYTES: usize = 32 * 1024;

/// Blocks the header takes at the head of each file.
pub const HEADER_BLOCKS: u64 = (HEADER_BYTES / BLOCK_SIZE) as u64;

/// Where an object-backed store keeps its objects: a bucket, and the key
/// prefix every one of its objects is under.
#[derive(Debug, Clone)]
pub struct Remote {
    bucket: Bucket,
    prefix: String,
}

impl Remote {
    /// The objects under `prefix` in `bucket`. The prefix is used verbatim,
    /// so it is empty or ends in `/`.
    #[must_use]
    pub fn new(bucket: Bucket, prefix: impl Into<String>) -> Self {
        Self {
            bucket,
            prefix: prefix.into(),
        }
    }

    /// The same bucket, under storage shard `n`'s prefix (`…storage/<nn>/`)
    /// — its chain of files, as its data files are on disk (RFC 0073 §8).
    #[must_use]
    pub fn storage(&self, n: usize) -> Self {
        Self::new(
            self.bucket.clone(),
            format!("{}storage/{n:02x}/", self.prefix),
        )
    }

    /// The bucket.
    #[must_use]
    pub const fn bucket(&self) -> &Bucket {
        &self.bucket
    }

    /// The key prefix every object is under.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The key of file `file`: its number complemented, so a listing — in
    /// key order — names the newest file first (§4).
    #[must_use]
    pub fn file_key(&self, file: u64) -> String {
        format!("{}1seg/{:016x}", self.prefix, !file)
    }
}

/// The file a remote run starts in.
#[must_use]
pub const fn file_of(start: u64) -> u64 {
    start / FILE_BLOCKS
}

/// The bytes of its file a remote run names.
#[must_use]
pub const fn byte_range(run: Run) -> Range<u64> {
    let offset = (run.start % FILE_BLOCKS) * BLOCK_SIZE as u64;
    offset..offset + run.count * BLOCK_SIZE as u64
}

/// What an object-backed store carries beside its disk.
#[derive(Debug)]
pub(crate) struct ObjectState {
    pub(crate) remote: Remote,
    /// Where the chain of uploaded files stands: the remote end (writing is
    /// always there), the newest file and its header's hash, and per type
    /// the newest file with entries for it (RFC 0070 §6).
    pub(crate) chain: RefCell<crate::header::Chain>,
    /// Blocks of disk pages the object store has not received yet (a
    /// pending hole counts one) — what the upload trigger reads.
    pub(crate) pending_blocks: Cell<u64>,
    /// One upload at a time.
    pub(crate) uploading: Cell<bool>,
    /// How recently each disk page was read or written — what eviction
    /// orders by (RFC 0070 §7).
    pub(crate) heat: RefCell<crate::evict::Heat>,
    /// Evicted pages worth having on disk again, by slot and a key of
    /// theirs: read from the object store (`restore_wanted`).
    pub(crate) wanted: RefCell<BTreeSet<(usize, Vec<u8>)>>,
    /// When the oldest write no cut upload carries was committed — what
    /// the upload interval is measured against (RFC 0070 §9).
    pub(crate) oldest_write: Cell<Option<Instant>>,
    /// Another writer's file was met: this store takes no more writes
    /// ([`StorageError::Fenced`]).
    pub(crate) fenced: Cell<bool>,
    /// The files a compaction is emptying: uploads carry their pages
    /// forward meanwhile (`crate::remote_compact`).
    pub(crate) moving: RefCell<BTreeSet<u64>>,
    /// Files deleted above the chain's oldest live one — not deleted
    /// again. RAM only: after a reopen they are deleted once more.
    pub(crate) deleted: RefCell<BTreeSet<u64>>,
    /// The files the disk's reconstruction roots name — the newest batch's,
    /// and the one before while the newest is not durable yet. Not
    /// deleted.
    pub(crate) rooted: RefCell<[BTreeSet<u64>; 2]>,
    /// The storage shard's remote thread, when its data shards share its
    /// chain of files (RFC 0073 §8); `None`: this store's own.
    pub(crate) link: Option<crate::remote_link::RemoteLink>,
    /// Somebody waits for the upload under way: its files go at once.
    pub(crate) hurry: Cell<bool>,
    /// Through a storage shard: the files its last compaction round said to
    /// carry this store's pages out of.
    pub(crate) victims: RefCell<BTreeSet<u64>>,
}

impl ObjectState {
    pub(crate) fn new(remote: Remote) -> Self {
        Self {
            remote,
            chain: RefCell::default(),
            pending_blocks: Cell::new(0),
            uploading: Cell::new(false),
            heat: RefCell::default(),
            wanted: RefCell::default(),
            oldest_write: Cell::new(None),
            fenced: Cell::new(false),
            moving: RefCell::default(),
            deleted: RefCell::default(),
            rooted: RefCell::default(),
            link: None,
            hurry: Cell::new(false),
            victims: RefCell::default(),
        }
    }

    /// Account for pending blocks added and taken away
    /// ([`weight`](crate::shape::weight)).
    pub(crate) fn adjust_pending(&self, added: u64, taken: u64) {
        let n = self.pending_blocks.get() + added;
        self.pending_blocks.set(n.saturating_sub(taken));
    }
}

impl PageStore {
    /// The object store's side of this store; [`StorageError::NotObjectBacked`]
    /// for a disk-only one.
    pub(crate) fn object(&self) -> StorageResult<&ObjectState> {
        self.object.as_ref().ok_or(StorageError::NotObjectBacked)
    }

    /// Whether this store's pages live in an object store (RFC 0070).
    #[must_use]
    pub const fn is_object_backed(&self) -> bool {
        self.object.is_some()
    }

    /// Bytes of disk pages the object store has not received yet — what an
    /// upload would carry; `0` for a disk-only store.
    #[must_use]
    pub fn pending_upload_bytes(&self) -> u64 {
        self.object
            .as_ref()
            .map_or(0, |o| o.pending_blocks.get() * BLOCK_SIZE as u64)
    }

    /// An object-backed store's open: tally what is still to upload and,
    /// over a disk that had a database, step past every file the object
    /// store holds (`step_over_leftovers`).
    ///
    /// # Errors
    /// Object-store faults.
    pub(crate) fn prepare_object(&self, reopened: bool) -> StorageResult<()> {
        self.tally_pending();
        self.root_remote();
        // A storage shard's remote thread steps over them for its data
        // shards (RFC 0073 §8).
        let linked = self.object.as_ref().is_some_and(|o| o.link.is_some());
        if reopened && !linked {
            self.step_over_leftovers()?;
        }
        Ok(())
    }

    /// Count the pending blocks the restored directories hold — once, at
    /// open; every change after it is tallied where it happens.
    fn tally_pending(&self) {
        let Some(object) = &self.object else { return };
        let mut blocks = 0;
        for slot in &self.types {
            if let Some(index) = slot.directory().borrow().as_ref() {
                blocks += index
                    .entries_all()
                    .map(|(at, page)| {
                        let meta = page
                            .map(|p| p.meta)
                            .or_else(|| index.meta_at(at))
                            .unwrap_or(0);
                        weight(meta, page.map_or(0, |p| p.blocks))
                    })
                    .sum::<u64>();
            }
        }
        object.pending_blocks.set(blocks);
        // What waited before the reopen waits on from now.
        if blocks > 0 {
            object.oldest_write.set(Some(Instant::now()));
        }
    }

    /// One uploaded image, fetched by its remote run.
    ///
    /// # Errors
    /// [`StorageError::Remote`] when the request fails;
    /// [`StorageError::Corrupt`] when the object or the range is missing —
    /// a descriptor names bytes an upload put there.
    pub(crate) async fn fetch_image(&self, run: Run) -> StorageResult<Vec<u8>> {
        self.object()?.remote.fetch(run).await
    }
}

impl Remote {
    /// The image a remote run names — what
    /// [`PageStore::fetch_image`] reads, usable before there is a store.
    ///
    /// # Errors
    /// As [`PageStore::fetch_image`].
    pub(crate) async fn fetch(&self, run: Run) -> StorageResult<Vec<u8>> {
        let remote = self;
        let range = byte_range(run);
        let want = range.end - range.start;
        let image = remote
            .bucket()
            .get(&remote.file_key(file_of(run.start)), Some(range))
            .await?
            .ok_or(StorageError::Corrupt("an uploaded file is missing"))?;
        if image.len() as u64 != want {
            return Err(StorageError::Corrupt("an uploaded page is cut short"));
        }
        Ok(image)
    }
}

#[cfg(test)]
mod tests {
    use super::{FILE_BLOCKS, HEADER_BLOCKS, byte_range, file_of};
    use crate::block::Run;
    use crate::pages::PENDING;
    use crate::shape::weight;

    /// A page names its file by its start and its bytes by arithmetic — a
    /// page running past the file's 8 MiB still reads from that one file.
    #[test]
    fn a_descriptor_is_a_file_and_a_range() {
        let first = Run::new(HEADER_BLOCKS, 3);
        assert_eq!(file_of(first.start), 0);
        assert_eq!(byte_range(first), 32_768..32_768 + 3 * 4096);

        let late = Run::new(5 * FILE_BLOCKS + 2047, 4096);
        assert_eq!(file_of(late.start), 5);
        let range = byte_range(late);
        assert_eq!(range.start, 2047 * 4096);
        assert_eq!(range.end - range.start, 4096 * 4096, "16 MiB, one object");
    }

    #[test]
    fn a_pending_hole_still_weighs() {
        assert_eq!(weight(0, 8), 0, "uploaded");
        assert_eq!(weight(PENDING, 8), 8);
        assert_eq!(weight(PENDING, 0), 1);
    }
}
