//! Helper functions.
//!
//! Every read here is a safe, `const` index into the buffer: the original
//! pointer reads (`read_unaligned`) became byte indexing, which the compiler
//! folds back into one load, so `hash` can run in `const` context.

/// Read a buffer of fewer than 8 bytes into an integer, little-endian.
///
/// This assumes that `buf.len() < 8`; longer buffers keep only their low 8
/// bytes' worth of shifts, which no caller produces.
#[inline(always)]
pub const fn read_int(buf: &[u8]) -> u64 {
    let mut x = 0u64;
    let mut i = buf.len();
    while i > 0 {
        i -= 1;
        x = (x << 8) | buf[i] as u64;
    }
    x
}

/// Read the little-endian 64-bit integer at `buf[at..at + 8]`.
#[inline(always)]
pub const fn read_u64(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes([
        buf[at],
        buf[at + 1],
        buf[at + 2],
        buf[at + 3],
        buf[at + 4],
        buf[at + 5],
        buf[at + 6],
        buf[at + 7],
    ])
}

/// The diffusion function.
///
/// This is a bijective function emitting chaotic behavior. Such functions are used as building
/// blocks for hash functions.
pub const fn diffuse(mut x: u64) -> u64 {
    // These are derived from the PCG RNG's round. Thanks to @Veedrac for proposing this. The basic
    // idea is that we use dynamic shifts, which are determined by the input itself. The shift is
    // chosen by the higher bits, which means that changing those flips the lower bits, which
    // scatters upwards because of the multiplication.

    x = x.wrapping_mul(0x6eed0e9da4d94a4f);
    let a = x >> 32;
    let b = x >> 60;
    x ^= a >> b;
    x = x.wrapping_mul(0x6eed0e9da4d94a4f);

    x
}

/// Reverse the `diffuse` function.
pub const fn undiffuse(mut x: u64) -> u64 {
    // 0x2f72b4215a3d8caf is the modular multiplicative inverse of the constant used in `diffuse`.

    x = x.wrapping_mul(0x2f72b4215a3d8caf);
    let a = x >> 32;
    let b = x >> 60;
    x ^= a >> b;
    x = x.wrapping_mul(0x2f72b4215a3d8caf);

    x
}

#[cfg(test)]
mod tests {
    use super::*;

    fn diffuse_test(x: u64, y: u64) {
        assert_eq!(diffuse(x), y);
        assert_eq!(x, undiffuse(y));
        assert_eq!(undiffuse(diffuse(x)), x);
    }

    #[test]
    fn read_int_() {
        assert_eq!(read_int(&[2, 3]), 770);
        assert_eq!(read_int(&[3, 2]), 515);
        assert_eq!(read_int(&[3, 2, 5]), 328195);
    }

    #[test]
    fn read_u64_() {
        assert_eq!(read_u64(&[1, 0, 0, 0, 0, 0, 0, 0], 0), 1);
        assert_eq!(read_u64(&[2, 1, 0, 0, 0, 0, 0, 0], 0), 258);
        assert_eq!(read_u64(&[9, 2, 1, 0, 0, 0, 0, 0, 0], 1), 258);
    }

    #[test]
    fn diffuse_test_vectors() {
        diffuse_test(94203824938, 17289265692384716055);
        diffuse_test(0xDEADBEEF, 12110756357096144265);
        diffuse_test(0, 0);
        diffuse_test(1, 15197155197312260123);
        diffuse_test(2, 1571904453004118546);
        diffuse_test(3, 16467633989910088880);
    }
}
