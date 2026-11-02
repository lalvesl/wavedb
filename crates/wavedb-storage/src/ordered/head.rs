//! A sub-header: one range of a lane's keys, and how the `u32` entries under
//! it are read ([RFC 0078] §3, as [RFC 0081] §3a states it in bits).
//!
//! A range's keys share a **prefix of bits**, `prefix_bits` long; the
//! **quantum** of a key is the 16 bits after it. A range starts at quantum
//! `start` and reaches to the end of its prefix, or to the next range's
//! start, whichever comes first.
//!
//! An entry is `q << 16 | address << size_bits | size`:
//! - `q` is the quantum the page's coverage starts at;
//! - `address` counts `address_factor` blocks from `base_block`, the all-ones
//!   value of the field marking a **hole** — quanta no page covers, which hold
//!   no key;
//! - `size` is the page's length less one, in units of `size_factor` blocks.
//!
//! [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md
//! [RFC 0081]: ../../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

use std::cmp::Ordering;

use super::key::{bits_at, cmp_joined, has_prefix, join};

/// Quanta under one prefix.
pub const QUANTA: u32 = 1 << 16;

/// How a sub-header reads the address and size of its entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    address_bits: u8,
    address_factor: u8,
    size_factor: u8,
}

impl Shape {
    /// A disk lane's: 12 address bits, a 16 MiB window of 4 KiB blocks; 4
    /// size bits, a page of up to 16 blocks to a unit.
    pub const DISK: Self = Self {
        address_bits: 12,
        address_factor: 1,
        size_factor: 1,
    };

    /// The object store's: pages around 128 KiB, so 6 size bits — up to 64
    /// blocks to a unit — and a 4 MiB window.
    pub const REMOTE: Self = Self {
        address_bits: 10,
        address_factor: 1,
        size_factor: 1,
    };

    /// A shape, if it is one: `address_bits` in `1..=15` (the size field
    /// takes the rest of 16), both factors at least 1.
    #[must_use]
    pub const fn new(
        address_bits: u8,
        address_factor: u8,
        size_factor: u8,
    ) -> Option<Self> {
        let valid = address_bits >= 1
            && address_bits <= 15
            && address_factor >= 1
            && size_factor >= 1;
        if valid {
            Some(Self {
                address_bits,
                address_factor,
                size_factor,
            })
        } else {
            None
        }
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

    /// Width of the size field: what the address field leaves of 16 bits.
    #[must_use]
    pub const fn size_bits(self) -> u8 {
        16 - self.address_bits
    }

    /// The bytes a header records the shape as.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 3] {
        [self.address_bits, self.address_factor, self.size_factor]
    }

    /// The shape `bytes` record, if they are one.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 3]) -> Option<Self> {
        Self::new(bytes[0], bytes[1], bytes[2])
    }

    /// Blocks a range can address from its base: pages laid end to end in
    /// that many fit under one sub-header.
    #[must_use]
    pub const fn window(self) -> u64 {
        self.hole() as u64 * self.address_factor as u64
    }

    /// The address field's hole marker, all ones.
    const fn hole(self) -> u32 {
        (1 << self.address_bits) - 1
    }

    /// The size unit a page of `blocks` takes: one block while the size
    /// field can count it, else the least unit that brings it within the
    /// field. Every shape of one index shares the field's width, so this is
    /// the index's rule, whatever range the page lands in.
    #[must_use]
    pub const fn unit_for(self, blocks: u64) -> u64 {
        let units = 1u64 << self.size_bits();
        let need = blocks.div_ceil(units);
        if need == 0 { 1 } else { need }
    }

    /// `blocks` rounded up to whole units of [`unit_for`](Self::unit_for):
    /// the blocks a page is given, so an entry names its run exactly.
    #[must_use]
    pub const fn canonical(self, blocks: u64) -> u64 {
        let unit = self.unit_for(blocks);
        blocks.div_ceil(unit) * unit
    }

    /// The same shape with the size unit a page of `blocks` takes, if it
    /// fits in a `u8`.
    pub(crate) fn sized_for(self, blocks: u64) -> Option<Self> {
        let size_factor = u8::try_from(self.unit_for(blocks)).ok()?;
        Some(Self {
            size_factor,
            ..self
        })
    }
}

/// One range of a lane's keys. Its start is its prefix's bits, then
/// `start`'s 16; the ranges of a lane are disjoint and sorted by it, and a
/// key belongs to the last range starting at or before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubHeader {
    /// First block the entries' addresses count from.
    pub base_block: u64,
    /// The bits every key in the range shares, in
    /// `prefix_bits.div_ceil(8)` bytes; the bits past them zero.
    pub prefix: Box<[u8]>,
    /// The prefix's length in bits; the quantum is the 16 bits after it.
    pub prefix_bits: u16,
    /// The quantum the range begins at.
    pub start: u16,
    pub shape: Shape,
}

impl SubHeader {
    /// The range whose prefix is `key`'s first `prefix_bits` bits, starting
    /// at `key`'s quantum, its entries read by `shape` from `base_block`.
    #[must_use]
    pub fn at_key(
        key: &[u8],
        prefix_bits: u16,
        base_block: u64,
        shape: Shape,
    ) -> Self {
        let bits = usize::from(prefix_bits);
        let mut prefix = join(key, bits, 0);
        prefix.truncate(bits.div_ceil(8));
        Self {
            base_block,
            prefix: prefix.into_boxed_slice(),
            prefix_bits,
            start: bits_at(key, bits),
            shape,
        }
    }

    fn bits(&self) -> usize {
        usize::from(self.prefix_bits)
    }

    /// Where the range starts: its prefix, then its first quantum.
    #[must_use]
    pub fn start(&self) -> Vec<u8> {
        self.key_at(self.start)
    }

    /// The first key of quantum `q`: the prefix, then `q`.
    #[must_use]
    pub fn key_at(&self, q: u16) -> Vec<u8> {
        join(&self.prefix, self.bits(), q)
    }

    /// Where the range starts against `key`, both padded with zeros.
    #[must_use]
    pub fn start_cmp(&self, key: &[u8]) -> Ordering {
        cmp_joined(&self.prefix, self.bits(), self.start, key)
    }

    /// The quantum `key` falls in, if the range can express it: the key
    /// shares the prefix and lies at or past the start.
    #[must_use]
    pub fn quantum(&self, key: &[u8]) -> Option<u16> {
        if !has_prefix(key, &self.prefix, self.bits()) {
            return None;
        }
        let q = bits_at(key, self.bits());
        (q >= self.start).then_some(q)
    }

    /// The same range starting at quantum `q`.
    #[must_use]
    pub fn from_quantum(&self, q: u16) -> Self {
        Self {
            start: q,
            ..self.clone()
        }
        self.first_key.checked_add(offset)
    }

    /// The entry for a page at quantum `q`, if this range can address it —
    /// and name its run exactly: `blocks` a whole number of size units.
    #[must_use]
    pub fn encode(&self, q: u16, address: u64, blocks: u64) -> Option<u32> {
        let shape = self.shape;
        let offset = address.checked_sub(self.base_block)?;
        let factor = u64::from(shape.address_factor);
        let size = u64::from(shape.size_factor);
        if blocks == 0
            || !offset.is_multiple_of(factor)
            || !blocks.is_multiple_of(size)
        {
            return None;
        }
        let unit = offset / factor;
        let units = blocks / size;
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
        let entry = h.encode(7, 1000 + 2 * 33, 12).unwrap();
        assert_eq!(q_of(entry), 7);
        assert_eq!(h.decode(entry), Some((1066, 12)));
        assert_eq!(h.decode(h.hole(3)), None);
        assert_eq!(q_of(with_q(entry, 2)), 2);
        // A run that is not whole units would be named wrong: refused.
        assert!(h.encode(7, 1000, 9).is_none());
    }

    #[test]
    fn a_canonical_size_is_whole_units_of_the_index_rule() {
        let shape = Shape::new(0, 12, 1, 1).unwrap();
        assert_eq!(shape.canonical(5), 5);
        assert_eq!(shape.canonical(16), 16);
        // Past 16 blocks the unit grows: 17 takes units of two.
        assert_eq!(shape.canonical(17), 18);
        assert_eq!(shape.canonical(33), 33);
        assert_eq!(shape.canonical(35), 36);
        let sized = shape.sized_for(35).unwrap();
        let h = head(0, b"", sized);
        assert!(h.encode(0, 1000, shape.canonical(35)).is_some());
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
