//! One data file ([RFC 0071]) — its handle, its name, and the calls that
//! create, cut and delete one; split from [`crate::block_file`] for the file
//! budget.
//!
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::backing::{Backing, Memory};
use crate::block::{BLOCK_SIZE, Run};
use crate::block_file::BlockFile;
use crate::error::StorageResult;

/// One data file: its bytes and its length.
#[derive(Debug)]
pub struct DataFile {
    pub backing: Backing,
    /// Bytes, as last written or cut — kept here so a read costs no
    /// `stat`.
    pub len: Cell<u64>,
}

impl BlockFile {
    /// Cut the data file starting at `file` back to `end` (a block address),
    /// deleting it when that is its own start, and forget what the cache
    /// held of the blocks cut. No barrier: a cut a crash loses leaves a file
    /// longer than its last run, which a reopen reads as free tail, and a
    /// deletion it loses a file no run names, which a reopen deletes.
    ///
    /// # Errors
    /// [`StorageError::Io`](crate::StorageError::Io) if the resize or the unlink fails; the file is
    /// then merely longer than the allocator says.
    pub fn cut(&self, file: u64, end: u64) -> StorageResult<()> {
        let mut files = self.files.borrow_mut();
        let Some(data) = files.get(&file) else {
            return Ok(());
        };
        let was = file + data.len.get().div_ceil(BLOCK_SIZE as u64);
        self.cache
            .invalidate(Run::new(end, was.saturating_sub(end)));
        if end > file {
            let len = (end - file) * BLOCK_SIZE as u64;
            data.backing.set_len(len)?;
            data.len.set(len);
            return Ok(());
        }
        files.remove(&file);
        self.dirty.borrow_mut().remove(&file);
        if let Some(dir) = &self.dir {
            std::fs::remove_file(dir.join(name_of(file)))?;
        }
        Ok(())
    }

    /// Create the data file starting at `file`: locked, and named in the
    /// directory durably before any run in it can be.
    pub fn create(&self, file: u64) -> StorageResult<()> {
        let backing = match &self.dir {
            Some(dir) => {
                let handle = open_locked(&dir.join(name_of(file)), true)?;
                File::open(dir)?.sync_all()?;
                Backing::File(handle)
            }
            None => Backing::Memory(Memory::default()),
        };
        self.files
            .borrow_mut()
            .insert(file, DataFile::new(backing, 0));
        Ok(())
    }
}

impl DataFile {
    pub const fn new(backing: Backing, len: u64) -> Self {
        Self {
            backing,
            len: Cell::new(len),
        }
    }
}

/// The name of the data file starting at block `start`.
#[must_use]
pub fn name_of(start: u64) -> String {
    format!("{start:016x}")
}

/// The first block a directory entry named `name` starts at, if it is a
/// data file's name.
pub fn start_of(name: &str) -> Option<u64> {
    let hex = name.len() == 16
        && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    hex.then(|| u64::from_str_radix(name, 16).ok()).flatten()
}

/// Open `path` read-write and lock it — creating it, when `create`, and
/// refusing one already there.
pub fn open_locked(path: &Path, create: bool) -> StorageResult<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(create)
        .open(path)?;
    file.try_lock()?; // one open store per directory, in any process
    Ok(file)
}
