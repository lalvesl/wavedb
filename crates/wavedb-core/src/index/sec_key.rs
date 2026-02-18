//! [`SecKey`] — the key every chain is ordered by.
//!
//! The record's [`IndexKey`](super::IndexKey)-encoded field bytes — an
//! instant, a declared property, a gram — then its `LocalId`: the trailing
//! anchor makes entries unique when many records share one field value, and
//! names the record the entry points at (RFC 0054).

use crate::local_id::LocalId;
use crate::wire::WaveWire;

/// A chain key: the record's order-preserving field encoding, then its
/// `LocalId`.
///
/// Derived `Ord` is field-major (ties broken by the record), so equal field
/// values coexist and range scans stay contiguous.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, WaveWire)]
pub struct SecKey {
    /// The [`IndexKey`](super::IndexKey)-encoded field value(s).
    pub field: Vec<u8>,
    /// The record the entry points at.
    pub rec: LocalId,
}

#[cfg(test)]
mod tests {
    use super::SecKey;
    use crate::local_id::LocalId;

    fn sec(field: &[u8], key: u64) -> SecKey {
        SecKey {
            field: field.to_vec(),
            rec: LocalId::new(key, false, 1),
        }
    }

    #[test]
    fn sec_key_orders_field_major_then_record() {
        assert!(sec(b"a", 9) < sec(b"b", 1));
        assert!(sec(b"a", 1) < sec(b"a", 2));
        assert!(sec(b"a", 1) < sec(b"ab", 0), "prefix sorts first");
    }
}
