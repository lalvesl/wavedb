//! The block layer: the 64-bit [`BlockDescriptor`] and a [`Run`] of
//! contiguous blocks.
//!
//! A shard's data files are one array of fixed [`BLOCK_SIZE`]-byte blocks. The
//! [`BlockAllocator`] (in [`crate::alloc`], re-exported here) hands out
//! contiguous runs and reclaims them, coalescing adjacent free space so large
//! pages always have somewhere to land. It is a **pure in-memory structure** —
//! durability (the headers naming every run) is the settle's job, not this
//! module's.

pub use crate::alloc::BlockAllocator;
use wavedb_core::wire::{Cursor, Result, WaveWire};

/// Size of one block in bytes (the allocation unit).
pub const BLOCK_SIZE: usize = 4096;

const START_BITS: u32 = 43;
const COUNT_BITS: u32 = 20;
/// Bit 0: RFC 0070's "not uploaded yet" flag ([`BlockDescriptor::pending_upload`]).
const FLAG_BITS: u32 = 1;

const START_SHIFT: u32 = COUNT_BITS + FLAG_BITS; // 21
const COUNT_SHIFT: u32 = FLAG_BITS; // 1

const START_MASK: u64 = (1 << START_BITS) - 1;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;

/// Largest representable start block (`2^43 − 1`, 32 PiB of blocks).
pub const MAX_START: u64 = START_MASK;
/// Largest representable block count in one run/page (`2^20 − 1`, 4 GiB).
pub const MAX_COUNT: u64 = COUNT_MASK;

/// A contiguous run of blocks: a start block and a length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Run {
    /// First block index.
    pub start: u64,
    /// Number of contiguous blocks (`>= 1` for a real run).
    pub count: u64,
}

impl Run {
    /// Build a run.
    #[must_use]
    pub const fn new(start: u64, count: u64) -> Self {
        Self { start, count }
    }

    /// One past the last block (`start + count`).
    #[must_use]
    pub const fn end(self) -> u64 {
        self.start + self.count
    }

    /// Byte offset of this run's first block from block 0.
    #[must_use]
    pub const fn byte_offset(self) -> u64 {
        self.start * BLOCK_SIZE as u64
    }

    /// Total byte length of this run.
    #[must_use]
    pub const fn byte_len(self) -> u64 {
        self.count * BLOCK_SIZE as u64
    }
}

/// A 64-bit page/dictionary descriptor: `start (u43) · count (u20) · flag (u1)`.
///
/// One format for both pages and dictionary runs. The flag bit is the
/// object-store backend's (RFC 0070): set on a disk page the object store has
/// not received yet. It is no part of the run — [`run`](Self::run) and
/// [`is_allocated`](Self::is_allocated) ignore it — so an **emptied** bucket
/// still waiting for its upload is `count 0 · flag 1`: nothing on disk, and a
/// removal the object store has yet to learn.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BlockDescriptor(u64);

impl BlockDescriptor {
    /// The empty descriptor — an unallocated directory slot.
    pub const EMPTY: Self = Self(0);

    /// A disk bucket whose page was **evicted** (RFC 0070 §7): nothing on
    /// disk, everything in the object store. `start 1 · count 0` — no run,
    /// like [`EMPTY`](Self::EMPTY), but distinct from it, so a bucket that
    /// is merely empty still answers "absent" without a request.
    pub const EVICTED: Self = Self(1 << START_SHIFT);

    /// Pack a descriptor.
    ///
    /// # Panics
    /// Panics (in debug) if any field exceeds its bit width.
    #[must_use]
    pub fn new(start: u64, count: u64) -> Self {
        debug_assert!(start <= MAX_START, "start {start} exceeds u43");
        debug_assert!(count <= MAX_COUNT, "count {count} exceeds u20");
        Self(
            ((start & START_MASK) << START_SHIFT)
                | ((count & COUNT_MASK) << COUNT_SHIFT),
        )
    }

    /// Pack a descriptor addressing `run`.
    #[must_use]
    pub fn from_run(run: Run) -> Self {
        Self::new(run.start, run.count)
    }

    /// Wrap a raw `u64` (as read from a directory `Vec<u64>`).
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw `u64`.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// First block of the run.
    #[must_use]
    pub const fn start(self) -> u64 {
        (self.0 >> START_SHIFT) & START_MASK
    }

    /// Number of blocks in the run.
    #[must_use]
    pub const fn count(self) -> u64 {
        (self.0 >> COUNT_SHIFT) & COUNT_MASK
    }

    /// Whether the object store has yet to receive this disk page (RFC 0070
    /// §1). Always `false` in a disk-only store and in a remote directory.
    #[must_use]
    pub const fn pending_upload(self) -> bool {
        self.0 & 1 == 1
    }

    /// Whether this disk bucket's page lives only in the object store.
    #[must_use]
    pub const fn is_evicted(self) -> bool {
        self.0 == Self::EVICTED.0
    }

    /// This descriptor with the "not uploaded yet" flag set to `pending`.
    #[must_use]
    pub const fn with_pending_upload(self, pending: bool) -> Self {
        Self((self.0 & !1) | pending as u64)
    }

    /// `true` if this slot points at a real run (`count > 0`).
    #[must_use]
    pub const fn is_allocated(self) -> bool {
        self.count() != 0
    }

    /// The [`Run`] this descriptor addresses.
    #[must_use]
    pub const fn run(self) -> Run {
        Run::new(self.start(), self.count())
    }
}

impl core::fmt::Debug for BlockDescriptor {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BlockDescriptor")
            .field("start", &self.start())
            .field("count", &self.count())
            .field("pending_upload", &self.pending_upload())
            .finish()
    }
}

impl WaveWire for BlockDescriptor {
    const STACK_SIZE: usize = 8;
    fn heap_size(&self) -> usize {
        0
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        stack.extend_from_slice(&self.0.to_le_bytes());
    }
    fn encode_heap(&self, _heap: &mut Vec<u8>) {}
    fn decode(stack: &mut Cursor, _heap: &mut Cursor) -> Result<Self> {
        Ok(Self(u64::from_le_bytes(stack.take(8)?.try_into().unwrap())))
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockDescriptor, MAX_COUNT, MAX_START, Run};
    use wavedb_core::wire::{from_wire, to_wire};

    #[test]
    fn descriptor_packs_and_unpacks() {
        let d = BlockDescriptor::new(0x0512_3456_789A, 0xA_BCDE);
        assert_eq!(d.start(), 0x0512_3456_789A);
        assert_eq!(d.count(), 0xA_BCDE);
        assert_eq!(d.raw() & 1, 0, "the flag bit stays clear");
        assert!(!d.pending_upload());
        assert!(d.is_allocated());
        assert_eq!(d.run(), Run::new(0x0512_3456_789A, 0xA_BCDE));
    }

    #[test]
    fn descriptor_boundaries() {
        let d = BlockDescriptor::new(MAX_START, MAX_COUNT);
        assert_eq!(d.start(), MAX_START);
        assert_eq!(d.count(), MAX_COUNT);
        assert_eq!(MAX_COUNT, (1 << 20) - 1);
        assert_eq!(MAX_START, (1 << 43) - 1);
        assert!(!BlockDescriptor::EMPTY.is_allocated());
        assert_eq!(BlockDescriptor::EMPTY.count(), 0);
    }

    /// The flag rides beside the run and never changes it: an emptied bucket
    /// can wait for its upload without pointing anywhere.
    #[test]
    fn the_upload_flag_is_no_part_of_the_run() {
        let d = BlockDescriptor::new(777, 9).with_pending_upload(true);
        assert!(d.pending_upload());
        assert_eq!(d.run(), Run::new(777, 9));
        assert_eq!(d.with_pending_upload(false), BlockDescriptor::new(777, 9));

        let emptied = BlockDescriptor::EMPTY.with_pending_upload(true);
        assert!(emptied.pending_upload());
        assert!(!emptied.is_allocated());
        assert_ne!(emptied, BlockDescriptor::EMPTY);
    }

    /// Evicted is no run and no flag, and is told apart from empty.
    #[test]
    fn evicted_is_neither_a_run_nor_empty() {
        let evicted = BlockDescriptor::EVICTED;
        assert!(evicted.is_evicted());
        assert!(!evicted.is_allocated());
        assert!(!evicted.pending_upload());
        assert!(!BlockDescriptor::EMPTY.is_evicted());
        assert!(!BlockDescriptor::new(1, 1).is_evicted());
    }

    #[test]
    fn descriptor_wire_roundtrip() {
        let d = BlockDescriptor::new(777, 9);
        let bytes = to_wire(&d);
        assert_eq!(bytes.len(), 8);
        assert_eq!(from_wire::<BlockDescriptor>(&bytes).unwrap(), d);
    }
}
