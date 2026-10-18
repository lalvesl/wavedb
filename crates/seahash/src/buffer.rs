//! A highly optimized version of SeaHash — safe, and `const`.
//!
//! The original read the buffer through raw pointers; this one indexes it, so
//! the whole function is safe and runs in `const` context. The arithmetic is
//! unchanged, and the tests pin the output to the reference implementation and
//! to vectors recorded from the original crate.

use crate::helper;

/// A SeaHash state.
#[derive(Clone)]
pub struct State {
    /// `a`
    a: u64,
    /// `b`
    b: u64,
    /// `c`
    c: u64,
    /// `d`
    d: u64,
    /// The number of written bytes.
    written: u64,
}

impl State {
    /// Create a new state vector with some initial values.
    pub const fn new(a: u64, b: u64, c: u64, d: u64) -> State {
        State {
            a,
            b,
            c,
            d,
            written: 0,
        }
    }

    /// Hash a buffer with some seed.
    pub const fn hash(
        buf: &[u8],
        (mut a, mut b, mut c, mut d): (u64, u64, u64, u64),
    ) -> State {
        // We use 4 different registers to store seperate hash states, because this allows us
        // to update them seperately, and consequently exploiting ILP to update the states in
        // parallel.

        // The index of the current bytes.
        let mut at = 0;
        // The end of the "main segment", i.e. the biggest buffer s.t. the length is divisible
        // by 32.
        let end = buf.len() & !0x1F;

        while at < end {
            a ^= helper::read_u64(buf, at);
            b ^= helper::read_u64(buf, at + 8);
            c ^= helper::read_u64(buf, at + 16);
            d ^= helper::read_u64(buf, at + 24);

            at += 32;

            // Diffuse the updated registers. We hope that each of these are executed in
            // parallel.
            a = helper::diffuse(a);
            b = helper::diffuse(b);
            c = helper::diffuse(c);
            d = helper::diffuse(d);
        }

        // The excessive bytes: those the loop above could not handle, in whole 8-byte words
        // and then a tail of fewer than 8 bytes, which is read as a little-endian integer
        // padded with zeros.
        let excessive = buf.len() - end;
        let (_, rest) = buf.split_at(at);
        match excessive {
            0 => {}
            1..=7 => {
                a = helper::diffuse(a ^ helper::read_int(rest));
            }
            8 => {
                a = helper::diffuse(a ^ helper::read_u64(rest, 0));
            }
            9..=15 => {
                let (_, tail) = rest.split_at(8);
                a = helper::diffuse(a ^ helper::read_u64(rest, 0));
                b = helper::diffuse(b ^ helper::read_int(tail));
            }
            16 => {
                a = helper::diffuse(a ^ helper::read_u64(rest, 0));
                b = helper::diffuse(b ^ helper::read_u64(rest, 8));
            }
            17..=23 => {
                let (_, tail) = rest.split_at(16);
                a = helper::diffuse(a ^ helper::read_u64(rest, 0));
                b = helper::diffuse(b ^ helper::read_u64(rest, 8));
                c = helper::diffuse(c ^ helper::read_int(tail));
            }
            24 => {
                a = helper::diffuse(a ^ helper::read_u64(rest, 0));
                b = helper::diffuse(b ^ helper::read_u64(rest, 8));
                c = helper::diffuse(c ^ helper::read_u64(rest, 16));
            }
            _ => {
                let (_, tail) = rest.split_at(24);
                a = helper::diffuse(a ^ helper::read_u64(rest, 0));
                b = helper::diffuse(b ^ helper::read_u64(rest, 8));
                c = helper::diffuse(c ^ helper::read_u64(rest, 16));
                d = helper::diffuse(d ^ helper::read_int(tail));
            }
        }

        State {
            a,
            b,
            c,
            d,
            written: buf.len() as u64,
        }
    }

    /// Write another 64-bit integer into the state.
    pub const fn push(&mut self, x: u64) {
        // Mix `x` into `a`.
        let a = helper::diffuse(self.a ^ x);

        //  Rotate around.
        //  _______________________
        // |                       v
        // a <---- b <---- c <---- d
        self.a = self.b;
        self.b = self.c;
        self.c = self.d;
        self.d = a;

        // Increase the written bytes counter.
        self.written += 8;
    }

    /// Remove the most recently written 64-bit integer from the state.
    ///
    /// Given the value of the most recently written u64 `last`, remove it from the state.
    pub const fn pop(&mut self, last: u64) {
        // Un-mix `last` from `d`. Removes the recently written data.
        let d = helper::undiffuse(self.d) ^ last;

        //  Rotate back.
        //  _______________________
        // v                       |
        // a ----> b ----> c ----> d
        self.d = self.c;
        self.c = self.b;
        self.b = self.a;
        self.a = d;

        // Decrese the written bytes counter.
        self.written -= 8;
    }

    /// Finalize the state.
    #[inline]
    pub const fn finalize(self) -> u64 {
        let State {
            written,
            mut a,
            b,
            mut c,
            d,
        } = self;

        // XOR the states together. Even though XOR is commutative, it doesn't matter, because the
        // state vector's initial components are mutually distinct, and thus swapping even and odd
        // chunks will affect the result, because it is sensitive to the initial condition.
        a ^= b;
        c ^= d;
        a ^= c;
        // XOR the number of written bytes in order to make the excessive bytes zero-sensitive
        // (without this, two excessive zeros would be equivalent to three excessive zeros). This
        // is know as length padding.
        a ^= written;

        // We diffuse to make the excessive bytes discrete (i.e. small changes shouldn't give small
        // changes in the output).
        helper::diffuse(a)
    }
}

/// Hash some buffer.
///
/// This is a highly optimized implementation of SeaHash. It implements numerous techniques to
/// improve performance:
///
/// - Register allocation: This makes a great deal out of making sure everything fits into
///   registers such that minimal memory accesses are needed. This works quite successfully on most
///   CPUs, and the only time it reads from memory is when it fetches the data of the buffer.
/// - Bulk reads: Like most other good hash functions, we read 8 bytes a time. This obviously
///   improves performance a lot
/// - Independent updates: We make sure very few statements next to each other depends on the
///   other. This means that almost always the CPU will be able to run the instructions in parallel.
/// - Loop unrolling: The hot loop is unrolled such that very little branches (one every 32 bytes)
///   are needed.
///
/// and more.
///
/// The seed of this hash function is prechosen.
pub const fn hash(buf: &[u8]) -> u64 {
    hash_seeded(
        buf,
        0x16f11fe89b0d677c,
        0xb480a793d8e6c86c,
        0x6fe2e5aaf078ebc9,
        0x14f994a4c5259381,
    )
}

/// Hash some buffer according to a chosen seed.
///
/// The keys are expected to be chosen from a uniform distribution. The keys should be mutually
/// distinct to avoid issues with collisions if the lanes are permuted.
///
/// This is not secure, as [the key can be extracted with a bit of computational
/// work](https://github.com/ticki/tfs/issues/5), as such, it is recommended to have a fallback
/// hash function (adaptive hashing) in the case of hash flooding. It can be considered unbroken if
/// the output is not known (i.e. no malicious party has access to the raw values of the keys, only
/// a permutation thereof).), however I absolutely do not recommend using it for this. If you want
/// to be strict, this should only be used as a layer of obfuscation, such that the fallback (e.g.
/// SipHash) is harder to trigger.
///
/// In the future, I might strengthen the security if possible while having backward compatibility
/// with the default initialization vector.
pub const fn hash_seeded(buf: &[u8], a: u64, b: u64, c: u64, d: u64) -> u64 {
    State::hash(buf, (a, b, c, d)).finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::reference;

    fn hash_match(a: &[u8]) {
        assert_eq!(hash(a), reference::hash(a));
        assert_eq!(
            hash_seeded(a, 1, 1, 1, 1),
            reference::hash_seeded(a, 1, 1, 1, 1)
        );
        assert_eq!(
            hash_seeded(a, 500, 2873, 2389, 9283),
            reference::hash_seeded(a, 500, 2873, 2389, 9283)
        );
        assert_eq!(
            hash_seeded(a, 238945723984, 872894734, 239478243, 28937498234),
            reference::hash_seeded(
                a,
                238945723984,
                872894734,
                239478243,
                28937498234
            )
        );
        assert_eq!(
            hash_seeded(a, !0, !0, !0, !0),
            reference::hash_seeded(a, !0, !0, !0, !0)
        );
        assert_eq!(
            hash_seeded(a, 0, 0, 0, 0),
            reference::hash_seeded(a, 0, 0, 0, 0)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // very slow to run on miri
    fn zero() {
        let arr = [0; 4096];
        for n in 0..4096 {
            hash_match(&arr[0..n]);
        }
    }

    #[test]
    fn seq() {
        let mut buf = [0; 4096];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = i as u8;
        }
        hash_match(&buf);
    }

    #[test]
    fn position_depedent() {
        let mut buf1 = [0; 4098];
        for (i, b) in buf1.iter_mut().enumerate() {
            *b = i as u8;
        }
        let mut buf2 = [0; 4098];
        for (i, b) in buf2.iter_mut().enumerate() {
            *b = i as u8 ^ 1;
        }

        assert!(hash(&buf1) != hash(&buf2));
    }

    #[test]
    fn shakespear() {
        hash_match(b"to be or not to be");
        hash_match(b"love is a wonderful terrible thing");
    }

    #[test]
    fn zero_senitive() {
        assert_ne!(hash(&[1, 2, 3, 4]), hash(&[1, 0, 2, 3, 4]));
        assert_ne!(hash(&[1, 2, 3, 4]), hash(&[1, 0, 0, 2, 3, 4]));
        assert_ne!(hash(&[1, 2, 3, 4]), hash(&[1, 2, 3, 4, 0]));
        assert_ne!(hash(&[1, 2, 3, 4]), hash(&[0, 1, 2, 3, 4]));
        assert_ne!(hash(&[0, 0, 0]), hash(&[0, 0, 0, 0, 0]));
    }

    #[test]
    fn not_equal() {
        assert_ne!(hash(b"to be or not to be "), hash(b"to be or not to be"));
        assert_ne!(hash(b"jkjke"), hash(b"jkjk"));
        assert_ne!(hash(b"ijkjke"), hash(b"ijkjk"));
        assert_ne!(hash(b"iijkjke"), hash(b"iijkjk"));
        assert_ne!(hash(b"iiijkjke"), hash(b"iiijkjk"));
        assert_ne!(hash(b"iiiijkjke"), hash(b"iiiijkjk"));
        assert_ne!(hash(b"iiiiijkjke"), hash(b"iiiiijkjk"));
        assert_ne!(hash(b"iiiiiijkjke"), hash(b"iiiiiijkjk"));
        assert_ne!(hash(b"iiiiiiijkjke"), hash(b"iiiiiiijkjk"));
        assert_ne!(hash(b"iiiiiiiijkjke"), hash(b"iiiiiiiijkjk"));
        assert_ne!(hash(b"ab"), hash(b"bb"));
    }

    #[test]
    fn push() {
        let mut state = State::new(1, 2, 3, 4);
        state.push(!0);
        state.push(0);

        assert_eq!(
            hash_seeded(
                &[
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0,
                    0, 0, 0, 0
                ],
                1,
                2,
                3,
                4
            ),
            state.finalize()
        );
    }

    #[test]
    fn pop() {
        let mut state = State::new(1, 2, 3, 4);
        state.push(!0);
        state.push(0);
        state.pop(0);

        assert_eq!(
            hash_seeded(
                &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                1,
                2,
                3,
                4
            ),
            state.finalize()
        );
    }
}
