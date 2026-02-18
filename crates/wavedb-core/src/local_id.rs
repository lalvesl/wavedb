//! `LocalId` — compact 80-bit record identifier for tenant-scoped chains.
//!
//! ```text
//! [ KEY (u64) | FLAG (1) | SALT (15) ]
//!    MSB ──────────────────────── LSB
//! ```
//!
//! Identical to [`Id`] but with `TENANT (u48)` stripped. A chain is already
//! scoped to a tenant, so carrying it per-key wastes 6 bytes per entry.
//! Reconstruct a full [`Id`] via [`LocalId::to_id`] by injecting the ambient tenant.

use core::fmt;

use crate::id::Id;
use crate::u48::U48;
use wavedb_wire::WaveWire;

const FLAG_SHIFT: u32 = 15;
const SALT_MASK: u16 = (1 << FLAG_SHIFT) - 1;

/// Compact 80-bit record identifier — [`Id`] with `TENANT` removed.
///
/// `WaveWire` is derived: `key` (8 LE bytes) then `lower` (2 LE bytes) = 10 bytes,
/// identical to the previous hand impl.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, WaveWire,
)]
pub struct LocalId {
    /// KEY field — a `STRUCT_HASH` or `CREATED_AT` timestamp.
    key: u64,
    /// `FLAG (bit 15) | SALT (bits 14:0)`.
    lower: u16,
}

impl LocalId {
    #[must_use]
    pub const fn new(key: u64, flag: bool, salt: u16) -> Self {
        let lower = ((flag as u16) << FLAG_SHIFT) | (salt & SALT_MASK);
        Self { key, lower }
    }

    /// Strip `TENANT` from a full [`Id`].
    #[must_use]
    pub fn from_id(id: Id) -> Self {
        Self {
            key: id.key(),
            lower: (id.raw() & 0xFFFF) as u16,
        }
    }

    /// Reconstruct a full [`Id`] by injecting `tenant`.
    #[must_use]
    pub fn to_id(self, tenant: U48) -> Id {
        Id::new(self.key, tenant, self.flag(), self.salt())
    }
}

impl From<Id> for LocalId {
    fn from(id: Id) -> Self {
        Self::from_id(id)
    }
}

impl LocalId {
    #[must_use]
    pub const fn key(self) -> u64 {
        self.key
    }

    #[must_use]
    pub const fn flag(self) -> bool {
        (self.lower >> FLAG_SHIFT) & 1 == 1
    }

    #[must_use]
    pub const fn salt(self) -> u16 {
        self.lower & SALT_MASK
    }

    /// `true` for a Unique anchor (`FLAG = 1`).
    #[must_use]
    pub const fn is_unique_anchor(self) -> bool {
        self.flag()
    }
}

impl fmt::Debug for LocalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalId")
            .field("key", &self.key)
            .field("flag", &self.flag())
            .field("salt", &self.salt())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::LocalId;
    use crate::id::Id;
    use crate::u48::U48;
    use crate::wire::{WaveWire, from_wire, to_wire};

    #[test]
    fn stack_size_is_10() {
        assert_eq!(LocalId::STACK_SIZE, 10);
    }

    #[test]
    fn field_roundtrip() {
        let lid = LocalId::new(0xDEAD_BEEF_0000_0001, true, 0x1234);
        assert_eq!(lid.key(), 0xDEAD_BEEF_0000_0001);
        assert!(lid.flag());
        assert_eq!(lid.salt(), 0x1234 & 0x7FFF);
    }

    #[test]
    fn from_id_drops_tenant() {
        let tenant_a = U48::from(0xAB_CDu32);
        let tenant_b = U48::from(0xFF_FFu32);
        let id_a = Id::new(42, tenant_a, false, 7);
        let id_b = Id::new(42, tenant_b, false, 7);
        assert_eq!(LocalId::from_id(id_a), LocalId::from_id(id_b));
    }

    #[test]
    fn to_id_roundtrip() {
        let tenant = U48::from(99u32);
        let id = Id::new(0xCAFE, tenant, true, 0x55);
        let lid = LocalId::from_id(id);
        assert_eq!(lid.to_id(tenant), id);
    }

    #[test]
    fn wire_roundtrip() {
        for lid in [
            LocalId::default(),
            LocalId::new(u64::MAX, true, 0x7FFF),
            LocalId::new(1, false, 3),
        ] {
            let bytes = to_wire(&lid);
            assert_eq!(bytes.len(), 10);
            assert_eq!(from_wire::<LocalId>(&bytes), Ok(lid));
        }
    }

    #[test]
    fn ordering_matches_key_priority() {
        let lo = LocalId::new(10, true, 0x7FFF);
        let hi = LocalId::new(11, false, 0);
        assert!(hi > lo);
    }
}
