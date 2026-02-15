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
pub use crate::size_code::MAX_COUNT;
use crate::size_code::{blocks, code_of};
use wavedb_core::wire::{Cursor, Result, WaveWire};

/// Size of one block in bytes (the allocation unit).
pub const BLOCK_SIZE: usize = 4096;

const START_BITS: u32 = 46;
/// RFC 0070's "not uploaded yet" flag ([`BlockDescriptor::pending_upload`]).
const FLAG_BITS: u32 = 1;
/// The page's local depth ℓ (RFC 0074).
const DEPTH_BITS: u32 = 7;
/// The size code ([`crate::size_code`]).
const SIZE_BITS: u32 = 10;

const DEPTH_SHIFT: u32 = SIZE_BITS; // 10
const FLAG_SHIFT: u32 = DEPTH_SHIFT + DEPTH_BITS; // 17
const START_SHIFT: u32 = FLAG_SHIFT + FLAG_BITS; // 18

const START_MASK: u64 = (1 << START_BITS) - 1;
const DEPTH_MASK: u64 = (1 << DEPTH_BITS) - 1;
const SIZE_MASK: u64 = (1 << SIZE_BITS) - 1;
const FLAG: u64 = 1 << FLAG_SHIFT;

/// Largest representable start block (`2^46 − 1`, 256 PiB of blocks).
pub const MAX_START: u64 = START_MASK;
/// Largest representable local depth.
pub const MAX_DEPTH: u8 = DEPTH_MASK as u8;
/// The largest page, in bytes ([`MAX_COUNT`] blocks, ~253 MiB).
pub const MAX_PAGE_BYTES: u64 = MAX_COUNT * BLOCK_SIZE as u64;

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

/// A 64-bit page/dictionary descriptor:
/// `start (u46) · flag (u1) · depth (u7) · size (u10)` ([RFC 0074]).
///
/// One format for both pages and dictionary runs (a dictionary's depth is
/// 0). The flag bit is the object-store backend's (RFC 0070): set on a disk
/// page the object store has not received yet. It is no part of the run —
/// [`run`](Self::run) and [`is_allocated`](Self::is_allocated) ignore it — so
/// an **emptied** bucket still waiting for its upload is `size 0 · flag 1`:
/// nothing on disk, and a removal the object store has yet to learn.
///
/// The depth is the page's local depth ℓ: how many low hash bits its
/// records share. An empty page keeps it, so it still governs its aliases.
/// The size is a code ([`crate::size_code`]): exactly the block count below
/// 589, rounded up past that, so a run is always sized by [`round_count`].
///
/// [RFC 0074]: ../../../rfcs/0074-extendible-hashing-WIP.md
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BlockDescriptor(u64);

impl BlockDescriptor {
    /// The empty descriptor — an unallocated directory slot.
    pub const EMPTY: Self = Self(0);

    /// A disk bucket whose page was **evicted** (RFC 0070 §7): nothing on
    /// disk, everything in the object store. `start 1 · size 0` — no run,
    /// like [`EMPTY`](Self::EMPTY), but distinct from it, so a bucket that
    /// is merely empty still answers "absent" without a request.
    pub const EVICTED: Self = Self(1 << START_SHIFT);

    /// Pack a descriptor of depth 0. `count` must be one a size code stands
    /// for ([`round_count`]).
    ///
    /// # Panics
    /// Panics (in debug) if `start` exceeds its bit width or `count` has no
    /// code of its own.
    #[must_use]
    pub fn new(start: u64, count: u64) -> Self {
        debug_assert!(start <= MAX_START, "start {start} exceeds u46");
        let code = code_of(count).unwrap_or(crate::size_code::MAX_CODE);
        debug_assert!(blocks(code) == count, "{count} blocks has no code");
        Self(((start & START_MASK) << START_SHIFT) | u64::from(code))
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
        blocks((self.0 & SIZE_MASK) as u16)
    }

    /// The page's local depth ℓ.
    #[must_use]
    pub const fn depth(self) -> u8 {
        ((self.0 >> DEPTH_SHIFT) & DEPTH_MASK) as u8
    }

    /// This descriptor at local depth `depth`.
    ///
    /// # Panics
    /// Panics (in debug) if `depth` exceeds [`MAX_DEPTH`].
    #[must_use]
    pub fn with_depth(self, depth: u8) -> Self {
        debug_assert!(depth <= MAX_DEPTH, "depth {depth} exceeds u7");
        let field = (u64::from(depth) & DEPTH_MASK) << DEPTH_SHIFT;
        Self((self.0 & !(DEPTH_MASK << DEPTH_SHIFT)) | field)
    }

    /// This descriptor with its run starting at `start` instead: the same
    /// size, depth and flag.
    ///
    /// # Panics
    /// Panics (in debug) if `start` exceeds its bit width.
    #[must_use]
    pub fn moved_to(self, start: u64) -> Self {
        debug_assert!(start <= MAX_START, "start {start} exceeds u46");
        let field = (start & START_MASK) << START_SHIFT;
        Self((self.0 & !(START_MASK << START_SHIFT)) | field)
    }

    /// Whether the object store has yet to receive this disk page (RFC 0070
    /// §1). Always `false` in a disk-only store and in a remote directory.
    #[must_use]
    pub const fn pending_upload(self) -> bool {
        self.0 & FLAG != 0
    }

    /// Whether this disk bucket's page lives only in the object store,
    /// whatever its depth.
    #[must_use]
    pub const fn is_evicted(self) -> bool {
        self.0 & !(DEPTH_MASK << DEPTH_SHIFT) == Self::EVICTED.0
    }

    /// This descriptor with the "not uploaded yet" flag set to `pending`.
    #[must_use]
    pub const fn with_pending_upload(self, pending: bool) -> Self {
        Self((self.0 & !FLAG) | if pending { FLAG } else { 0 })
    }

    /// `true` if this slot points at a real run (`size > 0`).
    #[must_use]
    pub const fn is_allocated(self) -> bool {
        self.0 & SIZE_MASK != 0
    }

    /// The [`Run`] this descriptor addresses.
    #[must_use]
    pub const fn run(self) -> Run {
        Run::new(self.start(), self.count())
    }
}

/// `count` blocks rounded up to a count a descriptor can carry — `None`
/// past [`MAX_COUNT`]. Every run a descriptor will name is sized by it.
#[must_use]
pub const fn round_count(count: u64) -> Option<u64> {
    crate::size_code::round_up(count)
}

impl core::fmt::Debug for BlockDescriptor {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BlockDescriptor")
            .field("start", &self.start())
            .field("count", &self.count())
            .field("depth", &self.depth())
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
        let mut raw = [0; 8];
        raw.copy_from_slice(stack.take(8)?);
        Ok(Self(u64::from_le_bytes(raw)))
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockDescriptor, MAX_COUNT, MAX_DEPTH, MAX_START, Run};
    use wavedb_core::wire::{from_wire, to_wire};

    #[test]
    fn descriptor_packs_and_unpacks() {
        let d = BlockDescriptor::new(0x2512_3456_789A, 588).with_depth(19);
        assert_eq!(d.start(), 0x2512_3456_789A);
        assert_eq!(d.count(), 588);
        assert_eq!(d.depth(), 19);
        assert!(!d.pending_upload());
        assert!(d.is_allocated());
        assert_eq!(d.run(), Run::new(0x2512_3456_789A, 588));
        assert_eq!(d.raw() >> 18, 0x2512_3456_789A, "start in bits 63..18");
        assert_eq!((d.raw() >> 10) & 0x7F, 19, "depth in bits 16..10");
        assert_eq!(d.raw() & 0x3FF, 588, "size in bits 9..0");
    }

    #[test]
    fn descriptor_boundaries() {
        let d =
            BlockDescriptor::new(MAX_START, MAX_COUNT).with_depth(MAX_DEPTH);
        assert_eq!(d.start(), MAX_START);
        assert_eq!(d.count(), MAX_COUNT);
        assert_eq!(d.depth(), MAX_DEPTH);
        assert!(!d.pending_upload(), "no field spills into the flag");
        assert_eq!(MAX_START, (1 << 46) - 1);
        assert_eq!(MAX_DEPTH, 127);
        assert!(!BlockDescriptor::EMPTY.is_allocated());
        assert_eq!(BlockDescriptor::EMPTY.count(), 0);
        assert_eq!(BlockDescriptor::EMPTY.depth(), 0);
    }

    /// A run past 588 blocks is sized by its code, which a run allocated
    /// through `round_count` always has.
    #[test]
    fn large_runs_round_to_their_code() {
        let k = super::round_count(4_097).unwrap();
        assert!(k >= 4_097);
        assert_eq!(BlockDescriptor::new(9, k).count(), k);
        assert_eq!(super::round_count(MAX_COUNT + 1), None);
    }

    /// The flag rides beside the run and never changes it: an emptied bucket
    /// can wait for its upload without pointing anywhere.
    #[test]
    fn the_upload_flag_is_no_part_of_the_run() {
        let d = BlockDescriptor::new(777, 9).with_pending_upload(true);
        assert!(d.pending_upload());
        assert_eq!(d.raw() >> 17 & 1, 1, "the flag is bit 17");
        assert_eq!(d.run(), Run::new(777, 9));
        assert_eq!(d.with_pending_upload(false), BlockDescriptor::new(777, 9));

        let emptied = BlockDescriptor::EMPTY.with_pending_upload(true);
        assert!(emptied.pending_upload());
        assert!(!emptied.is_allocated());
        assert_ne!(emptied, BlockDescriptor::EMPTY);
    }

    /// An emptied page keeps its depth: no run, still governing its aliases.
    #[test]
    fn the_depth_is_no_part_of_the_run() {
        let d = BlockDescriptor::new(777, 9).with_depth(5);
        assert_eq!(d.run(), Run::new(777, 9));
        assert_eq!(d.with_depth(0), BlockDescriptor::new(777, 9));
        let emptied = BlockDescriptor::EMPTY.with_depth(5);
        assert!(!emptied.is_allocated());
        assert_eq!(emptied.depth(), 5);
        assert!(!emptied.is_evicted());
    }

    /// Evicted is no run and no flag, and is told apart from empty, at any
    /// depth.
    #[test]
    fn evicted_is_neither_a_run_nor_empty() {
        let evicted = BlockDescriptor::EVICTED;
        assert!(evicted.is_evicted());
        assert!(evicted.with_depth(7).is_evicted());
        assert!(!evicted.is_allocated());
        assert!(!evicted.pending_upload());
        assert!(!BlockDescriptor::EMPTY.is_evicted());
        assert!(!BlockDescriptor::new(1, 1).is_evicted());
    }

    #[test]
    fn descriptor_wire_roundtrip() {
        let d = BlockDescriptor::new(777, 9).with_depth(3);
        let bytes = to_wire(&d);
        assert_eq!(bytes.len(), 8);
        assert_eq!(from_wire::<BlockDescriptor>(&bytes).unwrap(), d);
    }
}
