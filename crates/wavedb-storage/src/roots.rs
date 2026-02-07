//! The roots block ([RFC 0073] §5) — one block ending every storage write,
//! naming each data shard's root: the newest header ending one of its
//! batches.
//!
//! A storage shard's data files hold the windows of every data shard that
//! writes through it, interleaved, so the newest header in a file is only
//! the newest of one data shard: the others' roots lie further back, a data
//! shard idle for hours far back. The roots block names all of them, by
//! position — `roots[i]` is the `i`-th data shard's, [`NO_FILE`] for one
//! that never ended a batch — so a reconstruction finds every root in the
//! newest block and follows nothing to get there. `prev` is the roots block
//! before: when the storage write this one ends was cut by a crash, the one
//! before it is the root, durable — only the newest write can be cut.
//!
//! The same envelope as a header (`[len u32][crc][wire]`), with a magic of
//! its own first, so a block holding one is never taken for the other. 256
//! data shards' roots are 2 KiB: a roots block always fits its one block.
//!
//! The object store's files open with one too (§8): there it also says
//! where the storage shard's chain of files stands — the oldest file not
//! deleted, and the remote end once the file is in.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use wavedb_core::wire::{WaveWire, from_wire_checked, to_wire_checked};

use crate::block::BLOCK_SIZE;
use crate::error::{StorageError, StorageResult};
use crate::header::{Header, NO_FILE};

/// Blocks a roots block takes.
pub const ROOTS_BLOCKS: u64 = 1;

/// What every roots block's wire opens with: "WDBROOTS".
const MAGIC: u64 = u64::from_le_bytes(*b"WDBROOTS");

/// A storage shard's roots (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct Roots {
    magic: u64,
    /// Where this block is: a copy of it elsewhere is not it.
    pub at: u64,
    /// The roots block of the storage write before ([`NO_FILE`] for the
    /// first).
    pub prev: u64,
    /// Per data shard, by its position in the storage shard: the newest
    /// header ending one of its batches.
    pub by_shard: Vec<u64>,
    /// In the object store: the oldest file not deleted when this one was
    /// written — it may lag behind the truth, never run ahead of it — and
    /// the remote end once this file is in. Zero on disk.
    pub start: u64,
    pub end: u64,
}

impl Roots {
    /// The roots block at `at`, after `prev`, naming `roots`.
    #[must_use]
    pub const fn new(at: u64, prev: u64, by_shard: Vec<u64>) -> Self {
        Self {
            magic: MAGIC,
            at,
            prev,
            by_shard,
            start: 0,
            end: 0,
        }
    }

    /// The same, heading an object store's file: with where its chain of
    /// files stands.
    #[must_use]
    pub const fn with_chain(mut self, start: u64, end: u64) -> Self {
        self.start = start;
        self.end = end;
        self
    }

    /// The data shard at `position`'s root, if it ever ended a batch.
    #[must_use]
    pub fn root(&self, position: usize) -> Option<u64> {
        self.by_shard
            .get(position)
            .copied()
            .filter(|&at| at != NO_FILE)
    }

    /// `[len u32 LE][crc32][wire]`, padded to its block.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] when the roots outgrow their block — more
    /// data shards than a database may have.
    pub fn encode(&self) -> StorageResult<Vec<u8>> {
        let payload = to_wire_checked(self);
        let mut out = Vec::with_capacity(BLOCK_SIZE);
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        if out.len() > BLOCK_SIZE {
            return Err(StorageError::Corrupt("roots outgrew their block"));
        }
        out.resize(BLOCK_SIZE, 0);
        Ok(out)
    }

    /// The roots block `bytes` hold, if they hold one.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let len = bytes
            .get(..4)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_le_bytes)? as usize;
        let envelope = bytes.get(4..4 + len)?;
        from_wire_checked::<Self>(envelope)
            .ok()
            .filter(|r| r.magic == MAGIC)
    }
}

/// What a reconstruction finds at a block: a header, or a roots block —
/// each one only where it says it is.
#[derive(Debug)]
pub enum Mark {
    Header(Header),
    Roots(Roots),
}

impl Mark {
    /// The mark `bytes`, read at block `at`, hold.
    #[must_use]
    pub fn at(bytes: &[u8], at: u64) -> Option<Self> {
        if let Some(roots) = Roots::decode(bytes) {
            return (roots.at == at).then_some(Self::Roots(roots));
        }
        Header::decode(bytes)
            .ok()
            .filter(|h| h.at == at)
            .map(Self::Header)
    }

    /// The roots block this mark leads to: itself, or the newest one
    /// written before the header.
    #[must_use]
    pub const fn roots_at(&self) -> u64 {
        match self {
            Self::Header(h) => h.roots,
            Self::Roots(r) => r.at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Mark, Roots};
    use crate::block::BLOCK_SIZE;
    use crate::header::NO_FILE;

    #[test]
    fn a_roots_block_roundtrips_in_one_block() {
        let roots = Roots::new(40, 12, vec![8, NO_FILE, 30]);
        let bytes = roots.encode().unwrap();
        assert_eq!(bytes.len(), BLOCK_SIZE);
        assert_eq!(Roots::decode(&bytes), Some(roots.clone()));
        assert_eq!(roots.root(0), Some(8));
        assert_eq!(roots.root(1), None, "never ended a batch");
        assert_eq!(roots.root(7), None, "no such data shard");
    }

    /// The most data shards a database may have still fit one block.
    #[test]
    fn the_most_data_shards_fit() {
        let roots = Roots::new(1, 0, vec![u64::MAX - 1; 256]);
        assert!(roots.encode().is_ok());
    }

    /// A roots block is only where it says it is; a flipped bit is no
    /// roots block at all.
    #[test]
    fn a_misplaced_or_torn_roots_block_is_not_one() {
        let mut bytes = Roots::new(40, 12, vec![8]).encode().unwrap();
        assert!(matches!(Mark::at(&bytes, 40), Some(Mark::Roots(_))));
        assert!(Mark::at(&bytes, 41).is_none(), "a copy elsewhere");
        bytes[9] ^= 1;
        assert!(Mark::at(&bytes, 40).is_none());
    }
}
