//! Block 0 of `data.bin` — the **superblock**: the magic, the format version,
//! the per-database hash seed and the storage mode, written once when the
//! file is created and checked on every open. Split from
//! [`crate::block_file`], which owns the file handle and calls these two.

use std::fs::File;
use std::os::unix::fs::FileExt;

use wavedb_core::wire::{
    CRC_PREFIX_LEN, WaveWire, from_wire_checked, to_wire_checked,
};

use crate::block::BLOCK_SIZE;
use crate::error::{StorageError, StorageResult};

/// Magic at the head of every WaveDB `data.bin` superblock.
pub const MAGIC: &[u8; 8] = b"WAVEDBIN";

/// On-disk format version.
///
/// **Pinned at 1 while WaveDB is pre-release**: the format changes freely with
/// no bumps, no history, and no migrations — an old `data.bin` is simply not
/// supported (delete it; the journal replay rebuilds pages). Versioning starts
/// mattering at the first release.
pub const FORMAT_VERSION: u32 = 1;

// Superblock byte layout within block 0, the rest zero-padded:
//   [0..8)  magic
//   [8..)   to_wire_checked(SuperblockBody) = [ crc32 (u32 LE) ][ wire bytes ]
pub const OFF_BODY: usize = 8;

/// Everything the superblock persists besides the magic. Stack-only wire value
/// (fixed size), so its checked encoding has a compile-time length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, WaveWire)]
pub struct SuperblockBody {
    /// On-disk format version — inside the crc-protected body.
    pub(crate) version: u32,
    /// The per-database hash seed every record is routed with.
    pub(crate) seed: [u64; 4],
    /// Whether the pages live in an object store, the disk caching them
    /// (RFC 0070) — fixed when the file is created, like the seed.
    pub(crate) object_backed: bool,
}

/// Byte length of the checked-encoded [`SuperblockBody`] within block 0.
pub const BODY_CHECKED_LEN: usize =
    CRC_PREFIX_LEN + <SuperblockBody as WaveWire>::STACK_SIZE;

/// Block 0's bytes for `body`, the rest zero-padded.
pub fn block_of(body: &SuperblockBody) -> Vec<u8> {
    let mut block = vec![0u8; BLOCK_SIZE];
    block[..MAGIC.len()].copy_from_slice(MAGIC);
    block[OFF_BODY..OFF_BODY + BODY_CHECKED_LEN]
        .copy_from_slice(&to_wire_checked(body));
    block
}

/// Read block 0, verify magic + crc + version, and return the stored body.
pub fn read(file: &File) -> StorageResult<SuperblockBody> {
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
