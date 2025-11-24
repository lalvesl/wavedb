//! Keys as the index reads them: bytes compared as if padded with zeros
//! forever, and the 8 bytes at a depth read as one big-endian `u64`.
//!
//! The padding is what makes a sub-header's start comparable with a key of
//! any length: a start is its prefix and its first slice, `depth + 8` bytes,
//! and a key is whatever its encoding made it. Two keys equal under the
//! padding — `ab` and `ab\0` — are the same key to the index, so the
//! encodings it serves never produce such a pair (every order-preserving key
//! here ends in a fixed-width anchor or `Id`).

use std::cmp::Ordering;

/// The 8 bytes of `key` from `depth`, big-endian, zero past its end.
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
