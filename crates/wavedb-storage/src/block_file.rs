//! [`BlockFile`] — a shard's **data files** ([RFC 0071]), one address space
//! of 4 KiB blocks cut into files each named by the block it starts at.
//!
//! The block layer ([`crate::block`]) decides *which* runs of blocks exist
//! and in which file ([`BlockAllocator`](crate::BlockAllocator)); this module
//! is what actually puts bytes there. It owns the file handles, found by
//! **listing** the shard's directory: a name of exactly 16 hex digits is a
//! data file starting at that block, anything else is not one. A run at
//! block `s` lives in the file with the greatest start `≤ s`, at byte
//! `(s − start) · 4096` — one `BTreeMap` lookup, no arithmetic on a size, so
//! the size a file grows to can change between runs.
//!
//! A data file is blocks and nothing else: the database's fixed facts — the
//! seed, the mode — are its `MANIFEST`'s ([`crate::manifest`]).
//!
//! ## One open store per directory
//!
//! Every data file is opened read-write and **locked** (`try_lock`, an
//! advisory `flock`), and a file created later is locked as it is created:
//! a second store over the same directory meets the first file locked and
//! is [`StorageError::EngineBusy`]. There is no lock file.
//!
//! ## Positioned I/O
//!
//! Reads and writes take `&self` and use positioned syscalls (`pread`/`pwrite`),
//! so no caller fights over a seek cursor. The engine is native-only (the
//! browser uses IndexedDB, never this file), so the Unix
//! [`FileExt`](std::os::unix::fs::FileExt) is the portable-enough surface here.
//!
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

use std::cell::{Cell, RefCell};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet};

use std::hash::{BuildHasher, Hasher};
use std::path::{Path, PathBuf};

use crate::backing::Backing;
use crate::block::{BLOCK_SIZE, Run};
use crate::data_file::{DataFile, open_locked, start_of};
use crate::error::{StorageError, StorageResult};
use crate::page_cache::PageCache;

/// A shard's data files (see the module docs), on disk or in RAM.
#[derive(Debug)]
pub struct BlockFile {
    /// The shard's directory; `None` in RAM (object mode, RFC 0070 §9).
    pub(crate) dir: Option<PathBuf>,
    /// The data files, by first block.
    pub(crate) files: RefCell<BTreeMap<u64, DataFile>>,
    /// Files written since the last [`sync`](Self::sync).
    pub(crate) dirty: RefCell<BTreeSet<u64>>,
    seed: [u64; 4],
    object_backed: bool,
    io: IoCounts,
    /// Settled page images by run ([RFC 0044]).
    ///
    /// It lives here because the cache's key **is** this type's addressing
    /// unit, so every event that can falsify an entry — a write over blocks,
    /// a file cut or deleted — already passes through this type.
    ///
    /// [RFC 0044]: ../../../rfcs/0044-page-cache.md
    pub(crate) cache: PageCache,
}

/// Disk operations issued on one [`BlockFile`] since it was opened.
///
/// The accounting behind RFC 0041's and RFC 0072's claims: a settle round is
/// **one** write, a batch one sync per file written, and reads are one per
/// touched page.
#[derive(Debug, Default)]
pub struct IoCounts {
    reads: Cell<u64>,
    writes: Cell<u64>,
    syncs: Cell<u64>,
}

impl IoCounts {
    /// `(reads, writes, syncs)` so far.
    #[must_use]
    pub const fn snapshot(&self) -> (u64, u64, u64) {
        (self.reads.get(), self.writes.get(), self.syncs.get())
    }

    fn bump(cell: &Cell<u64>) {
        cell.set(cell.get() + 1);
    }

    fn add(cell: &Cell<u64>, n: u64) {
        cell.set(cell.get() + n);
    }
}

impl BlockFile {
    /// Open the data files in `dir`, creating the directory if needed, each
    /// locked. `seed` and `object_backed` are the database's (its
    /// `MANIFEST`'s); the page cache gets `page_cache_bytes` (`0` disables
    /// it).
    ///
    /// # Errors
    /// [`StorageError::EngineBusy`] if another store holds a file;
    /// [`StorageError::Io`] on a filesystem failure.
    pub fn open(
        dir: &Path,
        seed: [u64; 4],
        object_backed: bool,
        page_cache_bytes: usize,
    ) -> StorageResult<Self> {
        std::fs::create_dir_all(dir)?;
        let mut files = BTreeMap::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let Some(start) = entry.file_name().to_str().and_then(start_of)
            else {
                continue;
            };
            let file = open_locked(&entry.path(), false)?;
            let len = file.metadata()?.len();
            files.insert(start, DataFile::new(Backing::File(file), len));
        }
        Ok(Self {
            dir: Some(dir.to_path_buf()),
            files: RefCell::new(files),
            dirty: RefCell::default(),
            seed,
            object_backed,
            io: IoCounts::default(),
            cache: PageCache::new(page_cache_bytes),
        })
    }

    /// Data files in RAM, for a store with no disk (RFC 0070 §9). No page
    /// cache: the pages are in RAM already.
    #[must_use]
    pub fn in_memory(seed: [u64; 4]) -> Self {
        Self {
            dir: None,
            files: RefCell::default(),
            dirty: RefCell::default(),
            seed,
            object_backed: true,
            io: IoCounts::default(),
            cache: PageCache::new(0),
        }
    }

    /// Whether the blocks are in RAM rather than in files.
    #[must_use]
    pub const fn is_in_memory(&self) -> bool {
        self.dir.is_none()
    }

    /// The database's hash seed.
    #[must_use]
    pub const fn seed(&self) -> [u64; 4] {
        self.seed
    }

    /// Whether this database's pages live in an object store (RFC 0070).
    #[must_use]
    pub const fn object_backed(&self) -> bool {
        self.object_backed
    }

    /// Every data file as `(first block, end)` — whole blocks, a partial
    /// tail rounded down.
    #[must_use]
    pub fn layout(&self) -> Vec<(u64, u64)> {
        self.files
            .borrow()
            .iter()
            .map(|(&start, f)| (start, start + f.len.get() / BLOCK_SIZE as u64))
            .collect()
    }

    /// Whether any data file exists.
    #[must_use]
    pub fn has_files(&self) -> bool {
        !self.files.borrow().is_empty()
    }

    /// Read a run's bytes (`run.byte_len()` of them).
    ///
    /// # Errors
    /// [`StorageError::OutOfBounds`] if the run extends past its file, or no
    /// file holds it; [`StorageError::Io`] on a read fault.
    pub fn read_run(&self, run: Run) -> StorageResult<Vec<u8>> {
        let files = self.files.borrow();
        let Some((&start, file)) = files.range(..=run.start).next_back() else {
            return Err(StorageError::OutOfBounds {
                need: run.end() * BLOCK_SIZE as u64,
                have: 0,
            });
        };
        let offset = (run.start - start) * BLOCK_SIZE as u64;
        let need = offset + run.byte_len();
        let have = file.len.get();
        if need > have {
            return Err(StorageError::OutOfBounds { need, have });
        }
        let mut buf = vec![0u8; run.byte_len() as usize];
        file.backing.read_exact_at(&mut buf, offset)?;
        IoCounts::bump(&self.io.reads);
        Ok(buf)
    }

    /// Write `bytes` to the start of `run`, which lies in the data file
    /// starting at block `file` — created (and locked) here if it does not
    /// exist yet. `bytes` must fit in the run; the rest of the run is left as
    /// it is (a page records its own length). The file grows to hold the run.
    ///
    /// # Errors
    /// [`StorageError::RunOverflow`] if `bytes` is larger than the run;
    /// [`StorageError::Io`] on a create or write fault.
    pub fn write_run(
        &self,
        file: u64,
        run: Run,
        bytes: &[u8],
    ) -> StorageResult<()> {
        let cap = run.byte_len();
        if bytes.len() as u64 > cap || run.start < file {
            return Err(StorageError::RunOverflow {
                got: bytes.len() as u64,
                cap,
            });
        }
        if !self.files.borrow().contains_key(&file) {
            self.create(file)?;
        }
        let files = self.files.borrow();
        let Some(data) = files.get(&file) else {
            return Err(StorageError::Corrupt("a data file vanished"));
        };
        let offset = (run.start - file) * BLOCK_SIZE as u64;
        let end = offset + cap;
        if data.len.get() < end {
            data.backing.set_len(end)?;
            data.len.set(end);
        }
        // Blocks are written once and never again until the file is cut,
        // which drops what the cache held of them — but a write is the one
        // place that can say so for certain.
        self.cache.invalidate(run);
        data.backing.write_all_at(bytes, offset)?;
        self.dirty.borrow_mut().insert(file);
        IoCounts::bump(&self.io.writes);
        Ok(())
    }

    /// Flush every file written since the last sync to stable storage
    /// (`fsync`) — the active file, or two across a rollover.
    ///
    /// # Errors
    /// [`StorageError::Io`] if a sync fails; the files not synced yet stay
    /// owed to the next call.
    pub fn sync(&self) -> StorageResult<()> {
        let files = self.files.borrow();
        let mut dirty = self.dirty.borrow_mut();
        while let Some(&file) = dirty.first() {
            if let Some(data) = files.get(&file)
                && !self.is_in_memory()
            {
                data.backing.sync_all()?;
                IoCounts::bump(&self.io.syncs);
            }
            dirty.remove(&file);
        }
        Ok(())
    }

    /// Duplicates of the descriptors of every file written since the last
    /// sync, for a barrier taken on another thread (`crate::barrier`) — and
    /// the files no longer owed to [`sync`](Self::sync), the barrier now
    /// owing them. Nothing in RAM: it needs no barrier.
    ///
    /// # Errors
    /// [`StorageError::Io`] if a descriptor cannot be duplicated; the files
    /// stay owed then.
    pub(crate) fn take_dirty(&self) -> StorageResult<Vec<std::fs::File>> {
        let files = self.files.borrow();
        let mut dirty = self.dirty.borrow_mut();
        let mut out = Vec::new();
        for file in dirty.iter() {
            if let Some(DataFile {
                backing: Backing::File(handle),
                ..
            }) = files.get(file)
            {
                out.push(handle.try_clone()?);
            }
        }
        IoCounts::add(&self.io.syncs, out.len() as u64);
        dirty.clear();
        Ok(out)
    }

    /// Disk operations issued on these files since they were opened.
    #[must_use]
    pub const fn io(&self) -> &IoCounts {
        &self.io
    }
}

/// A per-database random `[u64; 4]` seed, drawn from the OS via the stdlib's
/// randomly-keyed [`RandomState`] (no extra crate dependency). Each lane uses an
/// independently constructed hasher, so the four words are independent.
pub(crate) fn random_seed() -> [u64; 4] {
    let mut seed = [0u64; 4];
    for (i, lane) in seed.iter_mut().enumerate() {
        let mut h = RandomState::new().build_hasher();
        h.write_u64(i as u64);
        *lane = h.finish();
    }
    seed
}

#[cfg(test)]
mod tests {
    use wavedb_core::{Id, U48};

    use super::BlockFile;
    use crate::block::{BLOCK_SIZE, Run};
    use crate::data_file::{name_of, start_of};
    use crate::error::StorageError;
    use crate::page::SlotPage;
    use crate::page_cache::Lane;

    fn open(dir: &std::path::Path, cache: usize) -> BlockFile {
        BlockFile::open(dir, [1, 2, 3, 4], false, cache).unwrap()
    }

    fn id() -> Id {
        Id::new(1, U48::from(1u32), false, 1)
    }

    /// A "page" of the stored bytes as one record — a decode the test can
    /// check without a dictionary, in the shape a page read takes.
    #[allow(clippy::unnecessary_wraps)]
    fn as_page(bytes: &[u8]) -> crate::error::StorageResult<SlotPage> {
        let mut page = SlotPage::new(1);
        page.upsert(id(), bytes.to_vec());
        Ok(page)
    }

    /// A run written then read through the cached path, so the entry exists.
    fn cache_one(bf: &BlockFile, start: u64) {
        let run = Run::new(start, 1);
        let byte = u8::try_from(start % 251).unwrap();
        bf.write_run(0, run, &vec![byte; run.byte_len() as usize])
            .unwrap();
        bf.with_page(run, as_page, |_| ()).unwrap();
    }

    /// The caller's budget is kept by **evicting**, not by refusing to cache:
    /// a ninth page arrives and the oldest leaves, so the newest read is
    /// always the one still held.
    #[test]
    fn the_page_cache_holds_only_what_the_caller_budgeted() {
        let d = tempfile::tempdir().unwrap();
        let budget = 2 * as_page(&[0; BLOCK_SIZE]).unwrap().footprint();
        let bf = open(d.path(), budget);
        for start in 1..=8u64 {
            cache_one(&bf, start);
        }
        assert_eq!(bf.page_cache().budget(), budget);
        assert!(bf.page_cache().bytes() <= budget);
        assert!(
            bf.page_cache()
                .with(Lane::Disk, Run::new(8, 1), |_| ())
                .is_ok(),
            "the newest read must be the one kept"
        );
        assert!(
            bf.page_cache()
                .with(Lane::Disk, Run::new(1, 1), |_| ())
                .is_err(),
            "the oldest must have been evicted"
        );
    }

    /// Zero disables it, and a disabled cache must still serve correct bytes
    /// — `with_page` is the read path, not an optimisation beside it.
    #[test]
    fn a_zero_budget_disables_the_cache_without_changing_reads() {
        let d = tempfile::tempdir().unwrap();
        let bf = open(d.path(), 0);
        let run = Run::new(1, 2);
        let payload = vec![0xA5; run.byte_len() as usize];
        bf.write_run(0, run, &payload).unwrap();
        let read =
            bf.with_page(run, as_page, |p| p.get(id()).map(<[u8]>::to_vec));
        assert_eq!(read.unwrap(), Some(payload));
        assert!(bf.page_cache().is_empty(), "zero must hold nothing");
    }

    /// A run is read from the file the listing places it in, at its offset
    /// there — and a reopen lists the same files.
    #[test]
    fn runs_live_in_the_file_their_address_names() {
        let d = tempfile::tempdir().unwrap();
        let (low, high) = (Run::new(2, 1), Run::new(0x30, 2));
        {
            let bf = open(d.path(), 0);
            bf.write_run(0, low, &[7; 10]).unwrap();
            bf.write_run(0x2e, high, &[9; 10]).unwrap();
            assert_eq!(bf.layout(), vec![(0, 3), (0x2e, 0x32)]);
        }
        assert!(d.path().join(name_of(0x2e)).exists());
        let bf = open(d.path(), 0);
        assert_eq!(bf.layout(), vec![(0, 3), (0x2e, 0x32)]);
        assert_eq!(bf.read_run(low).unwrap()[..10], [7; 10]);
        assert_eq!(bf.read_run(high).unwrap()[..10], [9; 10]);
        let len = std::fs::metadata(d.path().join(name_of(0x2e)))
            .unwrap()
            .len();
        assert_eq!(len, 4 * BLOCK_SIZE as u64, "block 0x2e is its first");
    }

    /// Only a name of exactly 16 lowercase hex digits is a data file.
    #[test]
    fn other_names_are_not_data_files() {
        assert_eq!(start_of("00000000000000ff"), Some(0xff));
        for name in ["MANIFEST", "journal_1.log", "ff", "00000000000000FF"] {
            assert_eq!(start_of(name), None, "{name}");
        }
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("journal_1.log"), b"x").unwrap();
        assert!(!open(d.path(), 0).has_files());
    }

    #[test]
    fn a_cut_shortens_a_file_and_a_cut_to_its_start_deletes_it() {
        let d = tempfile::tempdir().unwrap();
        let bf = open(d.path(), 0);
        bf.write_run(4, Run::new(4, 4), &[1]).unwrap();
        bf.cut(4, 6).unwrap();
        assert_eq!(bf.layout(), vec![(4, 6)]);
        let err = bf.read_run(Run::new(6, 1)).unwrap_err();
        assert!(matches!(err, StorageError::OutOfBounds { .. }), "{err:?}");
        bf.cut(4, 4).unwrap();
        assert!(!bf.has_files());
        assert!(!d.path().join(name_of(4)).exists());
    }

    #[test]
    fn short_write_leaves_run_tail_untouched() {
        let d = tempfile::tempdir().unwrap();
        let bf = open(d.path(), 0);
        let run = Run::new(1, 1);
        bf.write_run(0, run, &[0xAB, 0xCD]).unwrap();
        let back = bf.read_run(run).unwrap();
        assert_eq!(&back[..2], &[0xAB, 0xCD]);
        assert_eq!(back.len(), BLOCK_SIZE);
    }

    #[test]
    fn read_past_end_is_out_of_bounds() {
        let d = tempfile::tempdir().unwrap();
        let bf = open(d.path(), 0);
        let err = bf.read_run(Run::new(10, 1)).unwrap_err();
        assert!(matches!(err, StorageError::OutOfBounds { .. }));
        bf.write_run(0, Run::new(0, 1), &[1]).unwrap();
        let err = bf.read_run(Run::new(0, 2)).unwrap_err();
        assert!(matches!(err, StorageError::OutOfBounds { .. }));
    }

    #[test]
    fn oversized_write_is_run_overflow() {
        let d = tempfile::tempdir().unwrap();
        let bf = open(d.path(), 0);
        let big = vec![0u8; BLOCK_SIZE + 1];
        let err = bf.write_run(0, Run::new(1, 1), &big).unwrap_err();
        assert!(matches!(err, StorageError::RunOverflow { .. }));
    }

    /// A second store over the directory meets the first's lock — on a file
    /// it found, and on one it created.
    #[test]
    fn a_second_open_is_engine_busy() {
        let d = tempfile::tempdir().unwrap();
        let bf = open(d.path(), 0);
        bf.write_run(9, Run::new(9, 1), &[1]).unwrap(); // created while open
        let err = BlockFile::open(d.path(), [0; 4], false, 0).unwrap_err();
        assert!(matches!(err, StorageError::EngineBusy), "{err:?}");
        drop(bf);
        let _found = open(d.path(), 0);
        let err = BlockFile::open(d.path(), [0; 4], false, 0).unwrap_err();
        assert!(matches!(err, StorageError::EngineBusy), "{err:?}");
    }

    #[test]
    fn ram_files_work_alike() {
        let bf = BlockFile::in_memory([1; 4]);
        bf.write_run(0, Run::new(0, 2), &[5; 5000]).unwrap();
        assert_eq!(bf.read_run(Run::new(1, 1)).unwrap()[..904], [5; 904]);
        bf.cut(0, 0).unwrap();
        assert!(!bf.has_files());
    }
}
