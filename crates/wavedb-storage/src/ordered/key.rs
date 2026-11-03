//! Keys as the index reads them: bytes compared as if padded with zeros
//! forever, and read as a string of bits — a prefix of some bits, then the
//! 16 after it.
//!
//! The padding is what makes a sub-header's start comparable with a key of
//! any length: a start is its prefix's bits and its first quantum, and a key
//! is whatever its encoding made it. Two keys equal under the padding — `ab`
//! and `ab\0` — are the same key to the index, so the encodings it serves
//! never produce such a pair (every order-preserving key here ends in a
//! fixed-width anchor or `Id`).

use std::cmp::Ordering;

/// Byte `i` of `key`, zero past its end.
fn byte(key: &[u8], i: usize) -> u8 {
    key.get(i).copied().unwrap_or(0)
}

/// The 16 bits of `key` from bit `at` (bit 0 the first byte's highest),
/// zero past its end.
#[must_use]
pub fn bits_at(key: &[u8], at: usize) -> u16 {
    let (i, skew) = (at / 8, at % 8);
    let wide = u32::from_be_bytes([
        byte(key, i),
        byte(key, i + 1),
        byte(key, i + 2),
        0,
    ]);
    // The window's 16 bits start `skew` bits into its 24.
    ((wide << skew) >> 16) as u16
}

/// Whether `key`'s first `bits` bits are `prefix`'s (`prefix` padded too).
#[must_use]
pub fn has_prefix(key: &[u8], prefix: &[u8], bits: usize) -> bool {
    let whole = bits / 8;
    if (0..whole).any(|i| byte(key, i) != byte(prefix, i)) {
        return false;
    }
    let rest = bits % 8;
    let mask = !(0xFFu8 >> rest);
    rest == 0 || (byte(key, whole) & mask) == (byte(prefix, whole) & mask)
}

/// The bytes holding `prefix`'s first `bits` bits, then `q`'s 16: the first
/// key of quantum `q` under that prefix, `(bits + 16).div_ceil(8)` bytes.
#[must_use]
pub fn join(prefix: &[u8], bits: usize, q: u16) -> Vec<u8> {
    let len = (bits + 16).div_ceil(8);
    (0..len).map(|i| joined_byte(prefix, bits, q, i)).collect()
}

/// Byte `i` of what [`join`] builds from `prefix`, `bits` and `q`, zero
/// past its end.
fn joined_byte(prefix: &[u8], bits: usize, q: u16, i: usize) -> u8 {
    let (at, skew) = (bits / 8, bits % 8);
    // Bits of the prefix past `bits` are not its own.
    let own = match i.cmp(&at) {
        Ordering::Less => byte(prefix, i),
        Ordering::Equal => byte(prefix, i) & !(0xFFu8 >> skew),
        Ordering::Greater => 0,
    };
    // `q`'s 16 bits, `skew` bits into the three bytes from `at`.
    let wide = ((u32::from(q) << 8) >> skew).to_be_bytes();
    let ours = i
        .checked_sub(at)
        .and_then(|k| wide.get(k + 1))
        .copied()
        .unwrap_or(0);
    own | ours
}

/// What [`join`] builds from `prefix`, `bits` and `q`, against `key`, both
/// padded with zeros — without building it.
#[must_use]
pub fn cmp_joined(prefix: &[u8], bits: usize, q: u16, key: &[u8]) -> Ordering {
    let len = (bits + 16).div_ceil(8);
    for i in 0..len {
        match joined_byte(prefix, bits, q, i).cmp(&byte(key, i)) {
            Ordering::Equal => {}
            unequal => return unequal,
        }
    }
    if key.iter().skip(len).any(|&b| b != 0) {
        Ordering::Less
    } else {
        Ordering::Equal
    }
}

/// Compare `a` and `b` as if each were padded with zeros forever.
#[must_use]
pub fn cmp_padded(a: &[u8], b: &[u8]) -> Ordering {
    let common = a.len().min(b.len());
    match a[..common].cmp(&b[..common]) {
        Ordering::Equal => {}
        unequal => return unequal,
    }
    // The longer one is greater only if its tail holds a non-zero byte.
    let tail_nonzero = |t: &[u8]| t.iter().any(|&b| b != 0);
    match (tail_nonzero(&a[common..]), tail_nonzero(&b[common..])) {
        (true, _) => Ordering::Greater,
        (_, true) => Ordering::Less,
        _ => Ordering::Equal,
    }
}

/// How many leading bytes `a` and `b` share, padded as above, counted up to
/// the longer of the two.
#[must_use]
pub fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    let longest = a.len().max(b.len());
    (0..longest)
        .find(|&i| byte(a, i) != byte(b, i))
        .unwrap_or(longest)
}

/// `key`'s first `depth` bytes, zero past its end.
#[must_use]
pub fn prefix(key: &[u8], depth: usize) -> Box<[u8]> {
    (0..depth).map(|i| byte(key, i)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sixteen_bits_read_across_bytes_and_past_the_end() {
        assert_eq!(bits_at(&[0x12, 0x34, 0x56], 0), 0x1234);
        assert_eq!(bits_at(&[0x12, 0x34, 0x56], 4), 0x2345);
        assert_eq!(bits_at(&[0x12, 0x34, 0x56], 8), 0x3456);
        assert_eq!(bits_at(&[0x12, 0x34], 12), 0x4000);
        assert_eq!(bits_at(&[0x12], 40), 0);
    }

    #[test]
    fn a_prefix_of_bits_masks_its_last_byte() {
        assert!(has_prefix(&[0xAB, 0xCF], &[0xAB, 0xC0], 12));
        assert!(!has_prefix(&[0xAB, 0xDF], &[0xAB, 0xC0], 12));
        assert!(has_prefix(b"ab", b"", 0));
        assert!(has_prefix(b"a", b"a\0", 16), "padded with zeros");
        assert!(!has_prefix(b"a", b"a\x01", 16));
    }

    #[test]
    fn a_quantum_joins_its_prefix_at_any_bit() {
        assert_eq!(join(b"", 0, 0x1234), [0x12, 0x34]);
        assert_eq!(join(&[0xAB], 8, 0x1234), [0xAB, 0x12, 0x34]);
        // Four bits of prefix: the quantum starts mid-byte, and the
        // prefix's own bits past its four are dropped.
        assert_eq!(join(&[0xAF], 4, 0x1234), [0xA1, 0x23, 0x40]);
        assert_eq!(join(&[0xAB, 0xCD, 0xEF], 8, 0x1234), [0xAB, 0x12, 0x34]);
        for at in 0..24 {
            let key = join(&[0xFF, 0xFF, 0xFF], at, 0xBEEF);
            assert_eq!(bits_at(&key, at), 0xBEEF, "at {at}");
            assert!(has_prefix(&key, &[0xFF, 0xFF, 0xFF], at));
        }
    }

    #[test]
    fn a_join_compares_as_the_bytes_it_builds() {
        let keys: [&[u8]; 6] = [
            b"",
            b"\xAB",
            b"\xAB\x12\x34",
            b"\xAB\x12\x34\0",
            b"\xAB\x12\x35",
            b"\xFF",
        ];
        for key in keys {
            for (prefix, bits, q) in [
                (&[0xABu8][..], 8, 0x1234),
                (&[0xA0], 4, 0xB123),
                (&[], 0, 0),
            ] {
                assert_eq!(
                    cmp_joined(prefix, bits, q, key),
                    cmp_padded(&join(prefix, bits, q), key),
                    "{prefix:?} {bits} {q:#x} against {key:?}"
                );
            }
        }
    }

    #[test]
    fn trailing_zeros_do_not_order_keys() {
        assert_eq!(cmp_padded(b"ab", b"ab\0\0"), Ordering::Equal);
        assert_eq!(cmp_padded(b"ab", b"ab\0\x01"), Ordering::Less);
        assert_eq!(cmp_padded(b"b", b"ab\xff"), Ordering::Greater);
    }

    #[test]
    fn the_shared_prefix_counts_padding() {
        assert_eq!(shared_prefix(b"abc", b"abd"), 2);
        assert_eq!(shared_prefix(b"ab", b"ab\0"), 3);
        assert_eq!(shared_prefix(b"", b"\x01"), 0);
    }

    #[test]
    fn a_prefix_pads_a_short_key() {
        assert_eq!(&*prefix(b"ab", 4), b"ab\0\0");
        assert_eq!(&*prefix(b"abcdef", 2), b"ab");
    }
}
