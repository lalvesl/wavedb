//! The trailer ending a disk header's last block ([RFC 0079] §2): the
//! header's length, so the newest one is found from the data file's last
//! block — whole in that read when it fits a block, one more read
//! otherwise.
//!
//! `[crc32][wire]` like every checked boundary, its magic distinct from the
//! roots block's, at the very end of the block: a header's envelope opens
//! its first block and the trailer closes its last, so a one-block header
//! holds both. A header of one block may follow the page it shares the
//! block with ([RFC 0081] §1): the trailer says where it starts.
//!
//! [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

use wavedb_core::wire::{
    CRC_PREFIX_LEN, WaveWire, from_wire_checked, to_wire_checked,
};

/// What every trailer's wire opens with: "WDBTRAIL".
const MAGIC: u64 = u64::from_le_bytes(*b"WDBTRAIL");

/// A header's length, as its last block ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, WaveWire)]
struct Trailer {
    magic: u64,
    /// The header's length in blocks, this one included.
    blocks: u16,
    /// Where the header starts in its first block: 0, or past the page
    /// it shares a block with.
    offset: u16,
}

/// Bytes a trailer takes at the end of a header's last block.
pub const TRAILER_BYTES: usize = CRC_PREFIX_LEN + Trailer::STACK_SIZE;

/// End `header` — its bytes padded to `blocks` whole blocks — with the
/// trailer saying so.
#[cfg(test)]
pub fn close(header: &mut [u8], blocks: u16) {
    close_after(header, blocks, 0);
}

/// End `header`, padded to the end of `blocks` whole blocks, the first of
/// which it starts `offset` bytes into, with the trailer saying so.
pub fn close_after(header: &mut [u8], blocks: u16, offset: u16) {
    let trailer = to_wire_checked(&Trailer {
        magic: MAGIC,
        blocks,
        offset,
    });
    let end = header.len();
    if let Some(tail) = end
        .checked_sub(trailer.len())
        .and_then(|from| header.get_mut(from..))
    {
        tail.copy_from_slice(&trailer);
    }
}

/// The length of the header whose last block `block` is, if it ends with
/// a trailer.
#[must_use]
pub fn blocks_of(block: &[u8]) -> Option<u16> {
    trailer_of(block).map(|t| t.blocks)
}

/// Where in `bytes` — a header's blocks — the header starts: past the page
/// it shares its block with, or at once.
#[must_use]
pub fn offset_of(bytes: &[u8]) -> usize {
    trailer_of(bytes).map_or(0, |t| usize::from(t.offset))
}

fn trailer_of(bytes: &[u8]) -> Option<Trailer> {
    let from = bytes.len().checked_sub(TRAILER_BYTES)?;
    from_wire_checked::<Trailer>(bytes.get(from..)?)
        .ok()
        .filter(|t| t.magic == MAGIC && t.blocks > 0)
}

#[cfg(test)]
mod tests {
    use super::{TRAILER_BYTES, blocks_of, close, close_after, offset_of};
    use crate::block::BLOCK_SIZE;

    #[test]
    fn a_trailer_says_the_length_of_its_header() {
        let mut header = vec![7u8; 3 * BLOCK_SIZE];
        close(&mut header, 3);
        assert_eq!(blocks_of(&header[2 * BLOCK_SIZE..]), Some(3));
        assert_eq!(TRAILER_BYTES, 16, "a crc, a magic, a length, an offset");
        assert_eq!(offset_of(&header), 0);
    }

    /// A header after a page in one block says where it starts.
    #[test]
    fn a_trailer_says_where_a_shared_block_header_starts() {
        let mut block = vec![7u8; BLOCK_SIZE];
        close_after(&mut block[1000..], 1, 1000);
        assert_eq!(blocks_of(&block), Some(1));
        assert_eq!(offset_of(&block), 1000);
    }

    /// A block not ending a header — a page, zeros, a flipped bit — has no
    /// trailer.
    #[test]
    fn a_block_without_one_says_nothing() {
        assert_eq!(blocks_of(&[0; BLOCK_SIZE]), None);
        let mut block = vec![0u8; BLOCK_SIZE];
        close(&mut block, 1);
        block[BLOCK_SIZE - 1] ^= 1;
        assert_eq!(blocks_of(&block), None);
        assert_eq!(blocks_of(&[1, 2]), None, "shorter than a trailer");
    }
}
