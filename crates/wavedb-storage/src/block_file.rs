//! [`BlockFile`] — `data.bin` as a block-addressed file.
//!
//! The block layer ([`crate::block`]) decides *which* runs of blocks exist; this
//! module is what actually puts bytes on disk. It owns the file handle, the
//! **superblock** in block 0 (magic + format version + the per-database hash
//! seed), and positioned run-granular reads/writes.
//!
//! ## Block 0 is the superblock
//!
//! Block 0 is reserved for the superblock and is **never** handed out by the
//! allocator — see [`RESERVED_BLOCKS`]. It persists the per-database random
//! `[u64; 4]` seed that [`crate::directory::hash_of`] routes every record with, so
//! a `data.bin` reopened (or rebuilt by journal replay on another machine) hashes
//! every `Id` into the same bucket.
//!
//! ## Positioned I/O
//!
//! Reads and writes take `&self` and use positioned syscalls (`pread`/`pwrite`),
//! so concurrent callers don't fight over a shared seek cursor. The engine is
//! native-only (the browser uses IndexedDB, never this file), so the Unix
//! [`FileExt`] is the portable-enough surface here.

use std::collections::hash_map::RandomState;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use wavedb_core::wire::{
    CRC_PREFIX_LEN, WaveWire, from_wire_checked, to_wire_checked,
};

use crate::block::{BLOCK_SIZE, Run};
use crate::error::{StorageError, StorageResult};
use crate::page_cache::{self, PageCache};

/// Magic at the head of every WaveDB `data.bin` superblock.
const MAGIC: &[u8; 8] = b"WAVEDBIN";

/// On-disk format version.
///
/// **Pinned at 1 while WaveDB is pre-release**: the format changes freely with
/// no bumps, no history, and no migrations — an old `data.bin` is simply not
/// supported (delete it; the journal replay rebuilds pages). Versioning starts
/// mattering at the first release.
const FORMAT_VERSION: u32 = 1;

/// Blocks reserved at the head of the file for engine metadata (the superblock).
/// The allocator must never hand these out.
pub const RESERVED_BLOCKS: u64 = 1;

// Superblock byte layout within block 0, the rest zero-padded:
//   [0..8)  magic
//   [8..)   to_wire_checked(SuperblockBody) = [ crc32 (u32 LE) ][ wire bytes ]
const OFF_BODY: usize = 8;

/// Everything the superblock persists besides the magic. Stack-only wire value
/// (fixed size), so its checked encoding has a compile-time length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, WaveWire)]
struct SuperblockBody {
    /// On-disk format version — inside the crc-protected body.
    version: u32,
    /// The per-database hash seed every record is routed with.
    seed: [u64; 4],
}

/// Byte length of the checked-encoded [`SuperblockBody`] within block 0.
const BODY_CHECKED_LEN: usize =
    CRC_PREFIX_LEN + <SuperblockBody as WaveWire>::STACK_SIZE;

/// `data.bin` opened as an array of fixed [`BLOCK_SIZE`]-byte blocks.
#[derive(Debug)]
pub struct BlockFile {
    file: File,
    seed: [u64; 4],
    io: IoCounts,
    /// Settled page images by run ([RFC 0044]).
    ///
    /// It lives here because the cache's key **is** this type's addressing
    /// unit, so every event that can falsify an entry — a write into an extent
    /// the allocator recycled, the defragmenter relocating one — already
    /// passes through [`write_run`](BlockFile::write_run) or is one call away
    /// from it. Putting it a layer up would mean re-deriving "which cached
    /// bytes did that write invalidate?" from something that no longer knows.
    ///
    /// [RFC 0044]: ../../../rfcs/0044-page-cache-PLANNED-LOW.md
    cache: PageCache,
}

/// Disk operations issued on one [`BlockFile`] since it was opened.
///
/// The accounting behind RFC 0041's claim: a settle round is **one** write,
/// a checkpoint one write plus one sync, and reads are one per touched page.
#[derive(Debug, Default)]
pub struct IoCounts {
    reads: AtomicU64,
    writes: AtomicU64,
    syncs: AtomicU64,
}

impl IoCounts {
    /// `(reads, writes, syncs)` so far.
    #[must_use]
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.reads.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
            self.syncs.load(Ordering::Relaxed),
        )
    }
}

impl BlockFile {
    /// Open `path`, creating and initialising it if it does not yet exist.
    ///
    /// A fresh file gets a superblock with a freshly generated random seed; an
    /// existing file's superblock is validated (magic + version) and its seed
    /// loaded. Either way the returned handle's [`seed`](Self::seed) is the one
    /// records must be routed with.
    ///
    /// The page cache gets [`page_cache::DEFAULT_BUDGET_BYTES`];
    /// [`open_with`](Self::open_with) chooses it.
    ///
    /// # Errors
    /// [`StorageError::Io`] on a filesystem failure, [`StorageError::BadMagic`] /
    /// [`StorageError::BadVersion`] if an existing file is not a compatible
    /// `data.bin`.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_with(path, page_cache::DEFAULT_BUDGET_BYTES)
    }

    /// [`open`](Self::open), with the page cache's budget named by the caller
    /// (RFC 0065 §6). `0` disables the cache; a fill wants gigabytes and a
    /// measured row wants the default, and neither number belongs hardcoded
    /// where the file is opened.
    ///
    /// # Errors
    /// The same faults as [`open`](Self::open).
    pub fn open_with(
        path: impl AsRef<Path>,
        page_cache_bytes: usize,
    ) -> StorageResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let len = file.metadata()?.len();
        if len == 0 {
            let seed = random_seed();
            let bf = Self {
                file,
                seed,
                io: IoCounts::default(),
                cache: PageCache::new(page_cache_bytes),
            };
            bf.write_superblock()?;
            bf.file.sync_all()?;
            Ok(bf)
        } else {
            let body = read_and_check_superblock(&file)?;
            Ok(Self {
                file,
                seed: body.seed,
                io: IoCounts::default(),
                cache: PageCache::new(page_cache_bytes),
            })
        }
    }

    /// The per-database hash seed read from (or written to) the superblock.
    #[must_use]
    pub const fn seed(&self) -> [u64; 4] {
        self.seed
    }

    /// Current file length in whole blocks (rounding any partial tail down).
    ///
    /// # Errors
    /// [`StorageError::Io`] if the file's metadata can't be read.
    pub fn len_blocks(&self) -> StorageResult<u64> {
        Ok(self.file.metadata()?.len() / BLOCK_SIZE as u64)
    }

    /// Read a run's bytes (`run.byte_len()` of them) out of `data.bin`.
    ///
    /// # Errors
    /// [`StorageError::OutOfBounds`] if the run extends past the file;
    /// [`StorageError::Io`] on a read fault.
    pub fn read_run(&self, run: Run) -> StorageResult<Vec<u8>> {
        let have = self.file.metadata()?.len();
        let need = run.end() * BLOCK_SIZE as u64;
        if need > have {
            return Err(StorageError::OutOfBounds { need, have });
        }
        let mut buf = vec![0u8; run.byte_len() as usize];
        self.file.read_exact_at(&mut buf, run.byte_offset())?;
        self.io.reads.fetch_add(1, Ordering::Relaxed);
        Ok(buf)
    }

    /// Write `bytes` to the start of `run`. `bytes` must fit in the run; any
    /// remaining tail of the run is left untouched (a page records its own
    /// length, so trailing bytes are irrelevant). The file grows if the run
    /// reaches past the current end.
    ///
    /// # Errors
    /// [`StorageError::RunOverflow`] if `bytes` is larger than the run;
    /// [`StorageError::Io`] on a write fault.
    pub fn write_run(&self, run: Run, bytes: &[u8]) -> StorageResult<()> {
        let cap = run.byte_len();
        if bytes.len() as u64 > cap {
            return Err(StorageError::RunOverflow {
                got: bytes.len() as u64,
                cap,
            });
        }
        self.ensure_len(run.end() * BLOCK_SIZE as u64)?;
        // Before the write, not after: a concurrent reader must never be able
        // to take a cached image that the bytes underneath have already
        // stopped matching. Cached runs are immutable while allocated, so this
        // fires only where that stops being true — a recycled extent, and the
        // defragmenter's relocation.
        self.cache.invalidate(run);
        self.file.write_all_at(bytes, run.byte_offset())?;
        self.io.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// [`read_run`](Self::read_run) through the page cache.
    ///
    /// Returns a shared image rather than an owned buffer: a hit is a refcount
    /// bump instead of a copy of the page. Use this for anything read
    /// repeatedly and consumed read-only — which is pages, and only pages.
    /// The dictionary image, the addressing log's chunks and the
    /// defragmenter's relocation buffers want owned bytes and are read once,
    /// so they keep using [`read_run`](Self::read_run).
    ///
    /// # Errors
    /// The same faults as [`read_run`](Self::read_run). A cache hit cannot
    /// fault at all, which is the point.
    pub fn read_run_shared(&self, run: Run) -> StorageResult<Arc<[u8]>> {
        if let Some(hit) = self.cache.get(run) {
            return Ok(hit);
        }
        let bytes: Arc<[u8]> = self.read_run(run)?.into();
        self.cache.insert(run, &bytes);
        Ok(bytes)
    }

    /// The page cache backing [`read_run_shared`](Self::read_run_shared).
    #[must_use]
    pub const fn page_cache(&self) -> &PageCache {
        &self.cache
    }

    /// Flush all buffered data and metadata to stable storage (`fsync`).
    ///
    /// # Errors
    /// [`StorageError::Io`] if the sync fails.
    pub fn sync(&self) -> StorageResult<()> {
        self.file.sync_all()?;
        self.io.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Disk operations issued on this file since it was opened.
    #[must_use]
    pub const fn io(&self) -> &IoCounts {
        &self.io
    }

    /// Truncate the file to exactly `blocks` blocks, discarding everything past
    /// them. Used to reset `data.bin` to the superblock before a journal-replay
    /// rebuild of the pages. `blocks` must be `>= RESERVED_BLOCKS`.
    ///
    /// # Errors
    /// [`StorageError::Io`] if the resize fails.
    pub fn truncate_to_blocks(&self, blocks: u64) -> StorageResult<()> {
        debug_assert!(blocks >= RESERVED_BLOCKS, "would drop the superblock");
        self.file.set_len(blocks * BLOCK_SIZE as u64)?;
        Ok(())
    }

    /// Grow the file to at least `min_len` bytes (never shrinks).
    fn ensure_len(&self, min_len: u64) -> StorageResult<()> {
        if self.file.metadata()?.len() < min_len {
            self.file.set_len(min_len)?;
        }
        Ok(())
    }

    /// Serialise the superblock into block 0 and persist it.
    fn write_superblock(&self) -> StorageResult<()> {
        let mut block = vec![0u8; BLOCK_SIZE];
        block[..MAGIC.len()].copy_from_slice(MAGIC);
        let body = to_wire_checked(&SuperblockBody {
            version: FORMAT_VERSION,
            seed: self.seed,
        });
        block[OFF_BODY..OFF_BODY + BODY_CHECKED_LEN].copy_from_slice(&body);
        self.ensure_len(BLOCK_SIZE as u64)?;
        self.file.write_all_at(&block, 0)?;
        Ok(())
    }
}

/// Read block 0, verify magic + crc + version, and return the stored body.
fn read_and_check_superblock(file: &File) -> StorageResult<SuperblockBody> {
    let have = file.metadata()?.len();
    if have < BLOCK_SIZE as u64 {
        return Err(StorageError::OutOfBounds {
            need: BLOCK_SIZE as u64,
            have,
        });
    }
    let mut block = vec![0u8; BLOCK_SIZE];
    file.read_exact_at(&mut block, 0)?;

    if &block[..MAGIC.len()] != MAGIC {
        return Err(StorageError::BadMagic);
    }
    let body: SuperblockBody =
        from_wire_checked(&block[OFF_BODY..OFF_BODY + BODY_CHECKED_LEN])
            .map_err(|_| StorageError::Corrupt("superblock"))?;
    if body.version != FORMAT_VERSION {
        return Err(StorageError::BadVersion(body.version));
    }
    Ok(body)
}

/// A per-database random `[u64; 4]` seed, drawn from the OS via the stdlib's
/// randomly-keyed [`RandomState`] (no extra crate dependency). Each lane uses an
/// independently constructed hasher, so the four words are independent.
fn random_seed() -> [u64; 4] {
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
    use super::{
        BlockFile, FORMAT_VERSION, MAGIC, OFF_BODY, SuperblockBody, page_cache,
    };
    use crate::block::{BLOCK_SIZE, Run};
    use crate::error::StorageError;
    use std::os::unix::fs::FileExt;
    use wavedb_core::wire::to_wire_checked;

    fn temp_path() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");
        (dir, path)
    }

    /// A run written then read through the cached path, so the entry exists.
    fn cache_one(bf: &BlockFile, start: u64) {
        let run = Run::new(start, 1);
        let byte = u8::try_from(start % 251).unwrap();
        bf.write_run(run, &vec![byte; run.byte_len() as usize])
            .unwrap();
        bf.read_run_shared(run).unwrap();
    }

    /// The caller's budget is kept by **evicting**, not by refusing to cache:
    /// a ninth page arrives and the oldest leaves, so the newest read is
    /// always the one still held.
    #[test]
    fn the_page_cache_holds_only_what_the_caller_budgeted() {
        let (_d, path) = temp_path();
        let budget = 2 * BLOCK_SIZE;
        let bf = BlockFile::open_with(&path, budget).unwrap();
        for start in 1..=8u64 {
            cache_one(&bf, start);
        }
        assert_eq!(bf.page_cache().budget(), budget);
        assert!(
            bf.page_cache().bytes() <= budget,
            "held {} bytes against a {budget}-byte budget",
            bf.page_cache().bytes()
        );
        assert!(
            bf.page_cache().get(Run::new(8, 1)).is_some(),
            "the newest read must be the one kept"
        );
        assert!(
            bf.page_cache().get(Run::new(1, 1)).is_none(),
            "the oldest must have been evicted"
        );
    }

    /// Zero disables it, and a disabled cache must still serve correct bytes
    /// — `read_run_shared` is the read path, not an optimisation beside it.
    #[test]
    fn a_zero_budget_disables_the_cache_without_changing_reads() {
        let (_d, path) = temp_path();
        let bf = BlockFile::open_with(&path, 0).unwrap();
        let run = Run::new(1, 2);
        let payload = vec![0xA5; run.byte_len() as usize];
        bf.write_run(run, &payload).unwrap();
        assert_eq!(&*bf.read_run_shared(run).unwrap(), &payload[..]);
        assert!(bf.page_cache().is_empty(), "zero must hold nothing");
    }

    /// [`BlockFile::open`] is [`open_with`] at the documented default, so the
    /// callers that never asked keep the cache they have always had.
    #[test]
    fn open_is_open_with_at_the_default_budget() {
        let (_d, path) = temp_path();
        let bf = BlockFile::open(&path).unwrap();
        assert_eq!(bf.page_cache().budget(), page_cache::DEFAULT_BUDGET_BYTES);
    }

    #[test]
    fn fresh_file_gets_superblock_and_nonzero_seed() {
        let (_d, path) = temp_path();
        let bf = BlockFile::open(&path).unwrap();
        assert_ne!(bf.seed(), [0; 4], "seed must be randomised");
        // Block 0 exists on disk.
        assert_eq!(bf.len_blocks().unwrap(), 1);
    }

    #[test]
    fn seed_persists_across_reopen() {
        let (_d, path) = temp_path();
        let seed = {
            let bf = BlockFile::open(&path).unwrap();
            bf.seed()
        };
        let reopened = BlockFile::open(&path).unwrap();
        assert_eq!(reopened.seed(), seed, "seed must survive reopen");
    }

    #[test]
    fn run_write_then_read_roundtrips() {
        let (_d, path) = temp_path();
        let bf = BlockFile::open(&path).unwrap();
        // Block 0 is the superblock; write user data at block 1.
        let run = Run::new(1, 2);
        let mut payload = vec![0u8; run.byte_len() as usize];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        bf.write_run(run, &payload).unwrap();
        assert_eq!(bf.read_run(run).unwrap(), payload);
    }

    #[test]
    fn write_grows_the_file() {
        let (_d, path) = temp_path();
        let bf = BlockFile::open(&path).unwrap();
        assert_eq!(bf.len_blocks().unwrap(), 1);
        bf.write_run(Run::new(5, 1), &[1, 2, 3]).unwrap();
        assert_eq!(bf.len_blocks().unwrap(), 6, "file grew to hold block 5");
    }

    #[test]
    fn short_write_leaves_run_tail_untouched() {
        let (_d, path) = temp_path();
        let bf = BlockFile::open(&path).unwrap();
        let run = Run::new(1, 1);
        bf.write_run(run, &[0xAB, 0xCD]).unwrap(); // only first 2 bytes
        let back = bf.read_run(run).unwrap();
        assert_eq!(&back[..2], &[0xAB, 0xCD]);
        assert_eq!(back.len(), BLOCK_SIZE);
    }

    #[test]
    fn read_past_end_is_out_of_bounds() {
        let (_d, path) = temp_path();
        let bf = BlockFile::open(&path).unwrap();
        let err = bf.read_run(Run::new(10, 1)).unwrap_err();
        assert!(matches!(err, StorageError::OutOfBounds { .. }));
    }

    #[test]
    fn oversized_write_is_run_overflow() {
        let (_d, path) = temp_path();
        let bf = BlockFile::open(&path).unwrap();
        let big = vec![0u8; BLOCK_SIZE + 1];
        let err = bf.write_run(Run::new(1, 1), &big).unwrap_err();
        assert!(matches!(err, StorageError::RunOverflow { .. }));
    }

    #[test]
    fn bad_magic_is_rejected() {
        let (_d, path) = temp_path();
        {
            let f = std::fs::File::create(&path).unwrap();
            f.set_len(BLOCK_SIZE as u64).unwrap();
            f.write_all_at(b"NOTWAVE!", 0).unwrap();
        }
        assert!(matches!(
            BlockFile::open(&path).unwrap_err(),
            StorageError::BadMagic
        ));
    }

    #[test]
    fn wrong_version_is_rejected() {
        let (_d, path) = temp_path();
        {
            // A well-formed (crc-valid) superblock claiming a future version.
            let f = std::fs::File::create(&path).unwrap();
            f.set_len(BLOCK_SIZE as u64).unwrap();
            f.write_all_at(MAGIC, 0).unwrap();
            let body = to_wire_checked(&SuperblockBody {
                version: FORMAT_VERSION + 1,
                seed: [1, 2, 3, 4],
            });
            f.write_all_at(&body, OFF_BODY as u64).unwrap();
        }
        assert!(matches!(
            BlockFile::open(&path).unwrap_err(),
            StorageError::BadVersion(v) if v == FORMAT_VERSION + 1
        ));
    }

    #[test]
    fn corrupt_superblock_body_is_rejected() {
        let (_d, path) = temp_path();
        {
            let bf = BlockFile::open(&path).unwrap();
            drop(bf);
        }
        // Flip a byte inside the crc-protected body (the seed area).
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let at = (OFF_BODY + 8) as u64;
            let mut b = [0u8];
            f.read_exact_at(&mut b, at).unwrap();
            b[0] ^= 0xFF;
            f.write_all_at(&b, at).unwrap();
        }
        assert!(matches!(
            BlockFile::open(&path).unwrap_err(),
            StorageError::Corrupt("superblock")
        ));
    }
}
