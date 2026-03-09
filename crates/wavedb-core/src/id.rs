//! The 128-bit composite `Id`.
//!
//! ```text
//! [ KEY (u64) | TENANT (u48) | FLAG (1) | SALT (15) ]
//!    MSB ──────────────────────────────────────── LSB
//! ```
//!
//! `KEY` is the most significant field, so a numeric ordering of the `u128` is an
//! ordering by key — for timestamp-keyed shapes that is chronological order. The
//! `FLAG` bit selects how `KEY` is read: `1` ⇒ a `STRUCT_HASH` (a Unique anchor),
//! `0` ⇒ a `CREATED_AT` timestamp.

use core::fmt;

use crate::u48::U48;
use crate::wire::WaveWire;

const KEY_SHIFT: u32 = 64;
const TENANT_SHIFT: u32 = 16;
const FLAG_SHIFT: u32 = 15;
const SALT_BITS: u32 = 15;
const SALT_MASK: u128 = (1 << SALT_BITS) - 1;

/// A 128-bit composite record identifier.
///
/// `WaveWire` is derived: a tuple struct over `u128`, so it encodes as the inner
/// `u128`'s 16 little-endian bytes — identical to the previous hand impl.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, WaveWire,
)]
#[wave_wire(own_view)]
pub struct Id(u128);

impl Id {
    /// Assemble an `Id` from its fields. `salt` is truncated to 15 bits.
    #[must_use]
    pub fn new(key: u64, tenant: U48, flag: bool, salt: u16) -> Self {
        let raw = (u128::from(key) << KEY_SHIFT)
            | (u128::from(tenant.get()) << TENANT_SHIFT)
            | (u128::from(flag) << FLAG_SHIFT)
            | (u128::from(salt) & SALT_MASK);
        Self(raw)
    }

    /// Wrap a raw `u128`.
    #[must_use]
    pub const fn from_raw(raw: u128) -> Self {
        Self(raw)
    }

    /// The raw `u128`.
    #[must_use]
    pub const fn raw(self) -> u128 {
        self.0
    }

    /// The `KEY` field — a `STRUCT_HASH` or a `CREATED_AT`, per [`flag`](Self::flag).
    #[must_use]
    pub const fn key(self) -> u64 {
        (self.0 >> KEY_SHIFT) as u64
    }

    /// The owning tenant.
    #[must_use]
    pub const fn tenant(self) -> U48 {
        U48::from_truncated((self.0 >> TENANT_SHIFT) as u64)
    }

    /// The `FLAG` bit: `true` ⇒ `KEY` is a struct-hash key; `false` ⇒ a timestamp.
    #[must_use]
    pub const fn flag(self) -> bool {
        (self.0 >> FLAG_SHIFT) & 1 == 1
    }

    /// The 15-bit `SALT` / discriminator.
    #[must_use]
    pub const fn salt(self) -> u16 {
        (self.0 & SALT_MASK) as u16
    }

    /// `true` for a Unique anchor (`FLAG = 1`).
    #[must_use]
    pub const fn is_unique_anchor(self) -> bool {
        self.flag()
    }

    /// The `CREATED_AT` timestamp, if `KEY` is a timestamp (`FLAG = 0`).
    #[must_use]
    pub const fn created_at(self) -> Option<u64> {
        if self.flag() { None } else { Some(self.key()) }
    }

    /// The `STRUCT_HASH` key, if `KEY` is a struct hash (`FLAG = 1`).
    #[must_use]
    pub const fn struct_hash_key(self) -> Option<u64> {
        if self.flag() { Some(self.key()) } else { None }
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Id")
            .field("key", &self.key())
            .field("tenant", &self.tenant().get())
            .field("flag", &self.flag())
            .field("salt", &self.salt())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Id;
    use crate::u48::U48;
    use crate::wire::{from_wire, to_wire};

    #[test]
    fn field_roundtrip() {
        let tenant = U48::from_truncated(0xABCD_1234_5678);
        let id = Id::new(0xDEAD_BEEF_0000_0001, tenant, true, 0x5AA5);
        assert_eq!(id.key(), 0xDEAD_BEEF_0000_0001);
        assert_eq!(id.tenant(), tenant);
        assert!(id.flag());
        assert_eq!(id.salt(), 0x5AA5 & 0x7FFF);
        assert_eq!(id.struct_hash_key(), Some(id.key()));
        assert_eq!(id.created_at(), None);
    }

    #[test]
    fn timestamp_key() {
        let id = Id::new(1_700_000_000_000_000_000, U48::from(7u32), false, 1);
        assert!(!id.flag());
        assert_eq!(id.created_at(), Some(1_700_000_000_000_000_000));
        assert_eq!(id.struct_hash_key(), None);
    }

    #[test]
    fn key_is_most_significant() {
        // Larger key ⇒ larger Id regardless of the lower fields.
        let lo = Id::new(10, U48::MAX, true, 0x7FFF);
        let hi = Id::new(11, U48::ZERO, false, 0);
        assert!(hi > lo);
    }

    #[test]
    fn wire_roundtrip() {
        let id = Id::new(42, U48::from(9u32), false, 3);
        let bytes = to_wire(&id);
        assert_eq!(bytes.len(), 16);
        assert_eq!(from_wire::<Id>(&bytes), Ok(id));
    }
}
