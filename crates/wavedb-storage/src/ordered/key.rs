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
pub fn slice(key: &[u8], depth: usize) -> u64 {
    let mut bytes = [0u8; 8];
    if let Some(tail) = key.get(depth..) {
        let n = tail.len().min(8);
        bytes[..n].copy_from_slice(&tail[..n]);
    }
    u64::from_be_bytes(bytes)
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
    let at = |k: &[u8], i: usize| k.get(i).copied().unwrap_or(0);
    let longest = a.len().max(b.len());
    (0..longest)
        .find(|&i| at(a, i) != at(b, i))
        .unwrap_or(longest)
}

/// `key`'s first `depth` bytes, zero past its end.
#[must_use]
pub fn prefix(key: &[u8], depth: usize) -> Box<[u8]> {
    let mut out = vec![0u8; depth];
    let n = key.len().min(depth);
    out[..n].copy_from_slice(&key[..n]);
    out.into_boxed_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slice_is_big_endian_and_zero_padded() {
        assert_eq!(slice(&[1, 2], 0), 0x0102_0000_0000_0000);
        assert_eq!(
            slice(&[9, 1, 2, 3, 4, 5, 6, 7, 8, 9], 1),
            0x0102_0304_0506_0708
        );
        assert_eq!(slice(&[1, 2], 5), 0);
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
