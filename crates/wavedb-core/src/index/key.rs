//! [`IndexKey`] — order-preserving key encoding for the chains.

use crate::local_id::LocalId;
use crate::u48::U48;

/// Encode a value so that **byte order equals value order** — a chain
/// compares keys with `memcmp` and never decodes them.
///
/// Implemented per indexed type: unsigned ints big-endian, signed ints
/// sign-flipped then big-endian, `String` `0x00`-terminated, tuples
/// concatenated in declaration order.
pub trait IndexKey {
    /// Append this value's order-preserving key bytes to `out`.
    fn encode_key(&self, out: &mut Vec<u8>);

    /// Convenience: encode into a fresh `Vec`.
    #[must_use]
    fn key_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_key(&mut out);
        out
    }
}

macro_rules! index_key_unsigned {
    ($($t:ty),*) => {$(
        impl IndexKey for $t {
            fn encode_key(&self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_be_bytes());
            }
        }
    )*};
}
index_key_unsigned!(u8, u16, u32, u64, u128);

macro_rules! index_key_signed {
    ($($t:ty => $u:ty),*) => {$(
        impl IndexKey for $t {
            fn encode_key(&self, out: &mut Vec<u8>) {
                // Flip the sign bit so negatives sort before positives in BE order.
                let bias: $u = (1 as $u) << (<$u>::BITS - 1);
                out.extend_from_slice(&((*self as $u) ^ bias).to_be_bytes());
            }
        }
    )*};
}
index_key_signed!(i8 => u8, i16 => u16, i32 => u32, i64 => u64, i128 => u128);

impl IndexKey for bool {
    fn encode_key(&self, out: &mut Vec<u8>) {
        out.push(u8::from(*self));
    }
}

impl IndexKey for char {
    fn encode_key(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(*self as u32).to_be_bytes());
    }
}

impl IndexKey for U48 {
    fn encode_key(&self, out: &mut Vec<u8>) {
        // Low 48 bits, big-endian (drop the two high zero bytes of the u64).
        out.extend_from_slice(&self.get().to_be_bytes()[2..]);
    }
}

impl IndexKey for LocalId {
    fn encode_key(&self, out: &mut Vec<u8>) {
        // KEY (8 B BE) then FLAG|SALT (2 B BE) — matches the field priority of Ord.
        out.extend_from_slice(&self.key().to_be_bytes());
        let lower = (u16::from(self.flag()) << 15) | self.salt();
        out.extend_from_slice(&lower.to_be_bytes());
    }
}

impl IndexKey for str {
    fn encode_key(&self, out: &mut Vec<u8>) {
        // 0x00-terminated: a proper prefix sorts before its extension.
        out.extend_from_slice(self.as_bytes());
        out.push(0);
    }
}

impl IndexKey for String {
    fn encode_key(&self, out: &mut Vec<u8>) {
        self.as_str().encode_key(out);
    }
}

impl<T: IndexKey + ?Sized> IndexKey for &T {
    fn encode_key(&self, out: &mut Vec<u8>) {
        (**self).encode_key(out);
    }
}

macro_rules! index_key_tuple {
    ($($name:ident $idx:tt),+) => {
        impl<$($name: IndexKey),+> IndexKey for ($($name,)+) {
            fn encode_key(&self, out: &mut Vec<u8>) {
                $(self.$idx.encode_key(out);)+
            }
        }
    };
}
index_key_tuple!(A 0, B 1);
index_key_tuple!(A 0, B 1, C 2);

#[cfg(test)]
mod tests {
    use super::IndexKey;
    use crate::u48::U48;

    fn key_of<T: IndexKey>(v: T) -> Vec<u8> {
        v.key_bytes()
    }

    #[test]
    fn unsigned_keys_are_order_preserving() {
        let mut sorted = [0u64, 1, 255, 256, u64::MAX - 1, u64::MAX];
        sorted.sort_unstable();
        for w in sorted.windows(2) {
            assert!(key_of(w[0]) < key_of(w[1]), "{} vs {}", w[0], w[1]);
        }
    }

    #[test]
    fn signed_keys_sort_negatives_first() {
        let mut sorted = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX];
        sorted.sort_unstable();
        for w in sorted.windows(2) {
            assert!(key_of(w[0]) < key_of(w[1]), "{} vs {}", w[0], w[1]);
        }
    }

    #[test]
    fn string_keys_prefix_sorts_first() {
        assert!(key_of("app".to_string()) < key_of("apple".to_string()));
        assert!(key_of("app".to_string()) < key_of("apq".to_string()));
        assert!(key_of("a".to_string()) < key_of("b".to_string()));
    }

    #[test]
    fn u48_and_tuple_keys() {
        assert!(key_of(U48::from(1u32)) < key_of(U48::from(2u32)));
        // Composite: primary field dominates, secondary breaks ties.
        assert!(key_of((1u32, 9u32)) < key_of((2u32, 0u32)));
        assert!(key_of((2u32, 0u32)) < key_of((2u32, 1u32)));
    }
}
