//! A sub-header: one range of a lane's keys, and how the `u32` entries under
//! it are read ([RFC 0078] §3).
//!
//! An entry is `q << 16 | address << size_bits | size`:
//! - `q` is the quantum the page's coverage starts at,
//!   `(slice(key, depth) − first_key) >> key_shift`;
//! - `address` counts `address_factor` blocks from `base_block`, the all-ones
//!   value of the field marking a **hole** — quanta no page covers, which hold
//!   no key;
//! - `size` is the page's length less one, in units of `size_factor` blocks.
//!
//! [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::cmp::Ordering;

use super::key::{cmp_padded, slice};

/// The coarsest quantization: a quantum of `2^48`, so 65 536 of them span
/// every slice.
pub const MAX_SHIFT: u8 = 48;

/// Quanta in one sub-header's range.
pub const QUANTA: u32 = 1 << 16;

/// How a sub-header reads its entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    key_shift: u8,
    address_bits: u8,
    address_factor: u8,
    size_factor: u8,
}

impl Shape {
    /// A shape, if it is one: `key_shift ≤ 48`, `address_bits` in `1..=15`
    /// (the size field takes the rest of 16), both factors at least 1.
    #[must_use]
    pub const fn new(
        key_shift: u8,
        address_bits: u8,
        address_factor: u8,
        size_factor: u8,
    ) -> Option<Self> {
        let valid = key_shift <= MAX_SHIFT
            && address_bits >= 1
            && address_bits <= 15
            && address_factor >= 1
            && size_factor >= 1;
        if valid {
            Some(Self {
                key_shift,
                address_bits,
                address_factor,
                size_factor,
            })
        } else {
            None
        }
    }

    /// `q = (slice − first_key) >> key_shift`.
    #[must_use]
    pub const fn key_shift(self) -> u8 {
        self.key_shift
    }

    /// Width of the address field.
    #[must_use]
    pub const fn address_bits(self) -> u8 {
        self.address_bits
    }

    /// Blocks per unit of the address field.
    #[must_use]
    pub const fn address_factor(self) -> u8 {
        self.address_factor
    }

    /// Blocks per unit of the size field.
    #[must_use]
    pub const fn size_factor(self) -> u8 {
        self.size_factor
    }

    const fn size_bits(self) -> u8 {
        16 - self.address_bits
    }

    /// The address field's hole marker, all ones.
    const fn hole(self) -> u32 {
        (1 << self.address_bits) - 1
    }

    /// The same shape quantizing by `key_shift`.
    pub(crate) const fn with_shift(self, key_shift: u8) -> Self {
        Self { key_shift, ..self }
    }

    /// The same shape with a size unit large enough for `blocks`, if one
    /// fits in a `u8`.
    pub(crate) fn sized_for(self, blocks: u64) -> Option<Self> {
        let units = 1u64 << self.size_bits();
        let need = blocks.div_ceil(units).max(u64::from(self.size_factor));
        let size_factor = u8::try_from(need).ok()?;
        Some(Self {
            size_factor,
            ..self
        })
    }
}

/// One range of a lane's keys. Its start is `prefix ++ first_key` (big-endian),
/// and the ranges of a lane are disjoint and sorted by it; a key belongs to
/// the last range starting at or before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubHeader {
    /// First block the entries' addresses count from.
    pub base_block: u64,
    /// The 8 bytes at `depth` of the range's first key.
    pub first_key: u64,
    /// The bytes every key in the range shares; its length is the depth.
    pub prefix: Box<[u8]>,
    pub shape: Shape,
}

impl SubHeader {
    /// Bytes of key every key in the range shares.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.prefix.len()
    }

    /// Where the range starts: its prefix, then its first slice.
    #[must_use]
    pub fn start(&self) -> Vec<u8> {
        let mut start = self.prefix.to_vec();
        start.extend_from_slice(&self.first_key.to_be_bytes());
        start
    }

    /// Where the range starts against `key`, both padded with zeros.
    #[must_use]
    pub fn start_cmp(&self, key: &[u8]) -> Ordering {
        let head = &key[..key.len().min(self.depth())];
        match cmp_padded(&self.prefix, head) {
            Ordering::Equal => self.first_key.cmp(&slice(key, self.depth())),
            unequal => unequal,
        }
    }

    /// The quantum `key` falls in, if the range can express it: the key
    /// shares the prefix and lies within 65 536 quanta of the start.
    #[must_use]
    pub fn quantum(&self, key: &[u8]) -> Option<u16> {
        let head = &key[..key.len().min(self.depth())];
        if cmp_padded(&self.prefix, head) != Ordering::Equal {
            return None;
        }
        let offset = slice(key, self.depth()).checked_sub(self.first_key)?;
        u16::try_from(offset >> self.shape.key_shift).ok()
    }

    /// The first slice of quantum `q`, if it is not past the end of keys.
    #[must_use]
    pub fn quantum_start(&self, q: u32) -> Option<u64> {
        let offset =
            u64::from(q).checked_shl(u32::from(self.shape.key_shift))?;
        if offset >> self.shape.key_shift != u64::from(q) {
            return None;
        }
        self.first_key.checked_add(offset)
    }

    /// The entry for a page at quantum `q`, if this range can address it.
    #[must_use]
    pub fn encode(&self, q: u16, address: u64, blocks: u64) -> Option<u32> {
        let shape = self.shape;
        let offset = address.checked_sub(self.base_block)?;
        let factor = u64::from(shape.address_factor);
        if blocks == 0 || offset % factor != 0 {
            return None;
        }
        let unit = offset / factor;
        let units = blocks.div_ceil(u64::from(shape.size_factor));
        if unit >= u64::from(shape.hole()) || units > 1 << shape.size_bits() {
            return None;
        }
        let field = (unit << shape.size_bits()) | (units - 1);
        Some((u32::from(q) << 16) | field as u32)
    }

    /// The entry for a hole at quantum `q`.
    #[must_use]
    pub const fn hole(&self, q: u16) -> u32 {
        ((q as u32) << 16) | (self.shape.hole() << self.shape.size_bits())
    }

    /// The page an entry names, `(address, blocks)`; `None` for a hole.
    #[must_use]
    pub fn decode(&self, entry: u32) -> Option<(u64, u64)> {
        let shape = self.shape;
        let field = entry & 0xFFFF;
        let unit = field >> shape.size_bits();
        if unit == shape.hole() {
            return None;
        }
        let size = field & ((1 << shape.size_bits()) - 1);
        let address =
            self.base_block + u64::from(unit) * u64::from(shape.address_factor);
        let blocks = (u64::from(size) + 1) * u64::from(shape.size_factor);
        Some((address, blocks))
    }
}

/// The quantum an entry starts at.
#[must_use]
pub const fn q_of(entry: u32) -> u16 {
    (entry >> 16) as u16
}

/// `entry` moved to quantum `q`.
#[must_use]
pub const fn with_q(entry: u32, q: u16) -> u32 {
    ((q as u32) << 16) | (entry & 0xFFFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(first_key: u64, prefix: &[u8], shape: Shape) -> SubHeader {
        SubHeader {
            base_block: 1000,
            first_key,
            prefix: prefix.into(),
            shape,
        }
    }

    #[test]
    fn a_shape_refuses_what_it_cannot_read() {
        assert!(Shape::new(49, 8, 1, 1).is_none());
        assert!(Shape::new(0, 0, 1, 1).is_none());
        assert!(Shape::new(0, 16, 1, 1).is_none());
        assert!(Shape::new(0, 8, 0, 1).is_none());
        assert!(Shape::new(20, 15, 2, 2).is_some());
    }

    #[test]
    fn a_key_quantizes_from_the_range_start() {
        let shape = Shape::new(4, 8, 1, 1).unwrap();
        let h = head(0x100, b"", shape);
        assert_eq!(h.quantum(&0x100u64.to_be_bytes()), Some(0));
        assert_eq!(h.quantum(&0x10Fu64.to_be_bytes()), Some(0));
        assert_eq!(h.quantum(&0x110u64.to_be_bytes()), Some(1));
        assert_eq!(h.quantum(&0xFFu64.to_be_bytes()), None);
        assert_eq!(h.quantum(&(0x100u64 + (1 << 20)).to_be_bytes()), None);
    }

    #[test]
    fn a_key_outside_the_prefix_has_no_quantum() {
        let shape = Shape::new(0, 8, 1, 1).unwrap();
        let h = head(0, b"ab", shape);
        assert_eq!(h.quantum(b"ab\0\0\0\0\0\0\0\x05"), Some(5));
        assert_eq!(h.quantum(b"ac"), None);
        assert_eq!(h.start_cmp(b"ac"), Ordering::Less);
        assert_eq!(h.start_cmp(b"aa"), Ordering::Greater);
    }

    #[test]
    fn an_entry_round_trips_its_page() {
        let shape = Shape::new(0, 10, 2, 4).unwrap();
        let h = head(0, b"", shape);
        let entry = h.encode(7, 1000 + 2 * 33, 9).unwrap();
        assert_eq!(q_of(entry), 7);
        // 9 blocks take three units of four.
        assert_eq!(h.decode(entry), Some((1066, 12)));
        assert_eq!(h.decode(h.hole(3)), None);
        assert_eq!(q_of(with_q(entry, 2)), 2);
    }

    #[test]
    fn an_entry_refuses_what_its_fields_cannot_hold() {
        let shape = Shape::new(0, 4, 2, 1).unwrap();
        let h = head(0, b"", shape);
        // Below the base, off the factor, at the hole marker, too large.
        assert!(h.encode(0, 999, 1).is_none());
        assert!(h.encode(0, 1001, 1).is_none());
        assert!(h.encode(0, 1000 + 2 * 15, 1).is_none());
        assert!(h.encode(0, 1000, 4097).is_none());
        assert!(h.encode(0, 1000 + 2 * 14, 4096).is_some());
    }

    #[test]
    fn a_shape_grows_its_size_unit_for_a_large_page() {
        let shape = Shape::new(0, 14, 1, 1).unwrap();
        assert_eq!(shape.sized_for(4).unwrap().size_factor(), 1);
        assert_eq!(shape.sized_for(5).unwrap().size_factor(), 2);
        assert_eq!(shape.sized_for(4 * 255).unwrap().size_factor(), 255);
        assert!(shape.sized_for(4 * 255 + 1).is_none());
    }
}
