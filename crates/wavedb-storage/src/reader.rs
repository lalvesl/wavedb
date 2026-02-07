//! A data shard's read handles on its storage shard's data files
//! ([RFC 0073] §2): the files belong to the storage thread, which writes,
//! locks, rolls, cuts and deletes them; a data shard sharing it only reads
//! — its own descriptors, `pread`, nothing shared.
//!
//! - **Files** are opened read-only and unlocked (the storage thread holds
//!   the locks), the ones there at open by listing the directory, a later
//!   one when a reservation names it ([`note_file`](BlockFile::note_file))
//!   — opened on its first read, once the storage thread has written it.
//!   Their lengths are the storage thread's to know: a read past an end is
//!   an [`OutOfBounds`](StorageError::OutOfBounds) the read itself reports.
//! - **Landing windows.** A round publishes its pages as soon as it hands
//!   its window to the storage thread, which writes it at its next tick
//!   (§4): until then the window's bytes are read from here, a copy kept
//!   ([`hold`](BlockFile::hold)) until the batch carrying it is durable
//!   ([`landed`](BlockFile::landed)) — by then they are in the file.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;

use crate::backing::Backing;
use crate::block::{BLOCK_SIZE, Run};
use crate::block_file::BlockFile;
use crate::data_file::{DataFile, name_of, start_of};
use crate::error::{StorageError, StorageResult};
use crate::page_cache::PageCache;

/// What a reading [`BlockFile`] keeps besides its handles.
#[derive(Debug, Default)]
pub struct Shared {
    /// Files a reservation named, opened on their first read.
    noted: RefCell<BTreeSet<u64>>,
    /// Windows handed to the storage thread, by first block: the batch
    /// they belong to, and their bytes.
    landing: RefCell<BTreeMap<u64, (u64, Vec<u8>)>>,
}

impl BlockFile {
    /// Read handles on the data files in `dir`, which a storage thread
    /// writes (see the module docs).
    ///
    /// # Errors
    /// [`StorageError::Io`] on a filesystem failure.
    pub fn reader(
        dir: &Path,
        seed: [u64; 4],
        object_backed: bool,
        page_cache_bytes: usize,
    ) -> StorageResult<Self> {
        let mut file = Self::open_with(
            dir,
            seed,
            object_backed,
            PageCache::new(page_cache_bytes),
        );
        file.shared = Some(Shared::default());
        for entry in std::fs::read_dir(dir)? {
            if let Some(start) = entry?.file_name().to_str().and_then(start_of)
            {
                file.note_file(start);
            }
        }
        Ok(file)
    }

    /// Whether these are read handles on a storage thread's files.
    #[must_use]
    pub const fn is_reader(&self) -> bool {
        self.shared.is_some()
    }

    /// The data file starting at `start` exists, or is about to: a
    /// reservation landed in it.
    pub fn note_file(&self, start: u64) {
        if let Some(shared) = &self.shared {
            shared.noted.borrow_mut().insert(start);
        }
    }

    /// The data file starting at `start` is gone: its handle is closed —
    /// on Linux the space returns only once the last one is.
    pub fn forget_file(&self, start: u64) {
        if let Some(shared) = &self.shared {
            shared.noted.borrow_mut().remove(&start);
            self.files.borrow_mut().remove(&start);
        }
    }

    /// Keep `window`'s bytes, handed to the storage thread for batch
    /// `batch`, readable until that batch is durable.
    pub fn hold(&self, window: Run, batch: u64, bytes: Vec<u8>) {
        if let Some(shared) = &self.shared {
            shared
                .landing
                .borrow_mut()
                .insert(window.start, (batch, bytes));
        }
    }

    /// Batches up to `batch` are durable: their windows are in the files.
    pub fn landed(&self, batch: u64) {
        if let Some(shared) = &self.shared {
            shared.landing.borrow_mut().retain(|_, (b, _)| *b > batch);
        }
    }

    /// `run`'s bytes from a window still landing, if one holds it.
    pub(crate) fn landing(&self, run: Run) -> Option<Vec<u8>> {
        let shared = self.shared.as_ref()?;
        let landing = shared.landing.borrow();
        let (&start, (_, bytes)) = landing.range(..=run.start).next_back()?;
        let from = ((run.start - start) * BLOCK_SIZE as u64) as usize;
        let to = from + run.byte_len() as usize;
        let mut out = bytes.get(from..bytes.len().min(to))?.to_vec();
        // A window's last image may end short of its block.
        out.resize(run.byte_len() as usize, 0);
        (from < bytes.len()).then_some(out)
    }

    /// Open the noted file holding block `at`, if it is not open yet.
    ///
    /// # Errors
    /// [`StorageError::Io`] if it cannot be opened.
    pub(crate) fn open_noted(&self, at: u64) -> StorageResult<()> {
        let Some(shared) = &self.shared else {
            return Ok(());
        };
        let Some(&start) = shared.noted.borrow().range(..=at).next_back()
        else {
            return Ok(());
        };
        if self.files.borrow().contains_key(&start) {
            return Ok(());
        }
        let Some(dir) = &self.dir else {
            return Ok(());
        };
        let handle = File::open(dir.join(name_of(start)))?;
        self.files
            .borrow_mut()
            .insert(start, DataFile::new(Backing::File(handle), u64::MAX));
        Ok(())
    }
}

/// A read that ran past what the storage thread has written.
pub fn short_read(e: std::io::Error, need: u64) -> StorageError {
    if e.kind() == std::io::ErrorKind::UnexpectedEof {
        StorageError::OutOfBounds { need, have: 0 }
    } else {
        e.into()
    }
}
