//! The 10-bit size code a [`BlockDescriptor`] stores its block count in
//! ([RFC 0074]).
//!
//! Code `N > 0` stands for `max(N, round(2^(N/64)))` blocks: below 589 the code
//! is the count itself, so every page up to ~2.3 MiB is sized exactly, and
//! above that each code is ~1.09% (2^(1/64)) more than the one before, up to
//! [`MAX_COUNT`] (~253 MiB). A page sized between two codes takes the larger
//! one; the page's envelope carries its own length, so the slack is never
//! read.
//!
//! There is no float anywhere: `powf` is not correctly rounded, and two
//! targets disagreeing on a stored size would disagree on where a page ends.
//! The table is computed at compile time from integers alone.
//!
//! [`BlockDescriptor`]: crate::block::BlockDescriptor
//! [RFC 0074]: ../../../rfcs/0074-extendible-hashing.md

/// The largest size code (10 bits).
pub const MAX_CODE: u16 = (1 << 10) - 1;

/// The most blocks one run can span: what [`MAX_CODE`] stands for.
pub const MAX_COUNT: u64 = blocks(MAX_CODE);

/// `1.0` in Q62 fixed point.
const ONE_Q62: u128 = 1 << 62;

/// `2^(k/64)` for `k` in `0..64`, Q32 fixed point (`1 << 32` is `1.0`).
const FRAC: [u64; 64] = frac_table();

/// `2^(1/64)` as six successive square roots of 2, in Q62; then its powers
/// by rounded fixed-point products, narrowed to Q32 with rounding.
const fn frac_table() -> [u64; 64] {
    let mut root = 2 * ONE_Q62;
    let mut i = 0;
    while i < 6 {
        // √(x / 2^62) · 2^62 = √(x · 2^62).
        root = (root << 62).isqrt();
        i += 1;
    }
    let mut table = [0; 64];
    let mut power = ONE_Q62;
    let mut k = 0;
    while k < 64 {
        // Below 2 · 2^62, so it narrows without loss.
        table[k] = ((power + (1 << 29)) >> 30) as u64;
        power = (power * root + (1 << 61)) >> 62;
        k += 1;
    }
    table
}

/// The blocks size code `code` stands for (its low 10 bits); code 0 is no
/// blocks at all, an empty descriptor.
#[must_use]
pub const fn blocks(code: u16) -> u64 {
    let n = (code & MAX_CODE) as u64;
    if n == 0 {
        return 0;
    }
    let grown = ((FRAC[(n & 63) as usize] << (n >> 6)) + (1 << 31)) >> 32;
    if grown > n { grown } else { n }
}

/// The smallest code standing for `count` blocks or more — `None` past
/// [`MAX_COUNT`].
#[must_use]
pub const fn code_of(count: u64) -> Option<u16> {
    if count > MAX_COUNT {
        return None;
    }
    // `blocks` is strictly increasing: the first code reaching `count`.
    let (mut lo, mut hi) = (0, MAX_CODE);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if blocks(mid) >= count {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    Some(lo)
}

/// `count` rounded up to the nearest count a code stands for — `None` past
/// [`MAX_COUNT`].
#[must_use]
pub const fn round_up(count: u64) -> Option<u64> {
    match code_of(count) {
        Some(code) => Some(blocks(code)),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_CODE, MAX_COUNT, blocks, code_of, round_up};

    /// Every code stands for more blocks than the one before it, so no two
    /// codes alias and the encode's search is sound.
    #[test]
    fn the_code_is_strictly_increasing() {
        for n in 1..=MAX_CODE {
            assert!(blocks(n) > blocks(n - 1), "code {n}");
        }
    }

    /// Pinned values, computed outside the engine in integers alone
    /// (`round(2^(N/64)) = r` iff `(2r−1)^64 ≤ 2^(N+64) < (2r+1)^64`): the
    /// sizes a stored descriptor means must never move.
    #[test]
    fn golden_sizes_are_pinned() {
        assert_eq!(blocks(0), 0);
        assert_eq!(blocks(1), 1);
        assert_eq!(blocks(588), 588);
        assert_eq!(blocks(589), 589);
        assert_eq!(blocks(590), 596);
        assert_eq!(blocks(640), 1024);
        assert_eq!(blocks(704), 2048);
        assert_eq!(blocks(591), 602);
        assert_eq!(blocks(1000), 50_535);
        assert_eq!(blocks(MAX_CODE), 64_830);
        assert_eq!(MAX_COUNT, 64_830);
        // Every code at once: two sums of the whole table.
        let sum: u64 = (0..=MAX_CODE).map(blocks).sum();
        let weighted: u64 =
            (0..=MAX_CODE).map(|n| u64::from(n) * blocks(n)).sum();
        assert_eq!((sum, weighted), (6_137_434, 5_645_209_919));
    }

    /// Below 589 blocks a page is sized exactly.
    #[test]
    fn small_counts_are_their_own_code() {
        for k in 0..=589 {
            assert_eq!(code_of(k), Some(k as u16));
            assert_eq!(round_up(k), Some(k));
        }
    }

    /// The encode rounds up to the next size a code stands for, never down.
    #[test]
    fn the_encode_rounds_up() {
        assert_eq!(round_up(590), Some(596));
        assert_eq!(round_up(596), Some(596));
        assert_eq!(round_up(597), Some(blocks(591)));
        for k in [600, 1_000, 4_097, 33_333, MAX_COUNT - 1] {
            let code = code_of(k).unwrap();
            assert!(blocks(code) >= k, "{k}");
            assert!(blocks(code - 1) < k, "{k}: the smallest code");
        }
    }

    /// A count no code reaches is refused, never truncated.
    #[test]
    fn past_the_largest_code_is_none() {
        assert_eq!(code_of(MAX_COUNT), Some(MAX_CODE));
        assert_eq!(code_of(MAX_COUNT + 1), None);
        assert_eq!(round_up(u64::MAX), None);
    }
}
