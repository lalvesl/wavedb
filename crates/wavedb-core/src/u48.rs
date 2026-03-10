//! The 48-bit newtype used for `TENANT` and `user` values (Rust has no `u48`).

use core::fmt;

use crate::error::{Error, Result};
use crate::wire::{Cursor, WaveWire};

/// A `u64` constrained to 48 bits. Used for tenant and user identifiers.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct U48(u64);

impl U48 {
    /// Number of significant bits.
    pub const BITS: u32 = 48;
    /// Mask of the low 48 bits.
    pub const MASK: u64 = (1 << 48) - 1;
    /// The system tenant / zero value.
    pub const ZERO: Self = Self(0);
    /// The unauthenticated-session sentinel (all 48 bits set).
    pub const MAX: Self = Self(Self::MASK);

    /// Construct from a `u64`, rejecting values that don't fit in 48 bits.
    pub const fn new(value: u64) -> Result<Self> {
        if value > Self::MASK {
            Err(Error::U48Overflow(value))
        } else {
            Ok(Self(value))
        }
    }

    /// Construct from a `u64`, keeping only the low 48 bits.
    #[must_use]
    pub const fn from_truncated(value: u64) -> Self {
        Self(value & Self::MASK)
    }

    /// The underlying value (always `< 2^48`).
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u32> for U48 {
    fn from(value: u32) -> Self {
        Self(u64::from(value))
    }
}

impl TryFrom<u64> for U48 {
    type Error = Error;
    fn try_from(value: u64) -> Result<Self> {
        Self::new(value)
    }
}

impl fmt::Debug for U48 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "U48({})", self.0)
    }
}

impl fmt::Display for U48 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl WaveWire for U48 {
    const STACK_SIZE: usize = 6; // a true 48-bit field, no padding
    fn heap_size(&self) -> usize {
        0
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        stack.extend_from_slice(&self.0.to_le_bytes()[..6]);
    }
    fn encode_heap(&self, _heap: &mut Vec<u8>) {}
    // `Result` in this file is the core workspace alias (for `U48::new`); the WaveWire
    // trait wants the wire crate's, so spell it out here.
    fn decode<'a>(
        stack: &mut Cursor<'a>,
        _heap: &mut Cursor<'a>,
    ) -> wavedb_wire::Result<Self> {
        let mut bytes = [0u8; 8];
        bytes[..6].copy_from_slice(stack.take(6)?);
        Ok(Self(u64::from_le_bytes(bytes)))
    }
    wavedb_wire::own_view!();
}

#[cfg(test)]
mod tests {
    use super::U48;
    use crate::error::Error;
    use crate::wire::{from_wire, to_wire};

    #[test]
    fn bounds() {
        assert_eq!(U48::new(0), Ok(U48::ZERO));
        assert_eq!(U48::new(U48::MASK), Ok(U48::MAX));
        assert_eq!(
            U48::new(U48::MASK + 1),
            Err(Error::U48Overflow(U48::MASK + 1))
        );
        assert_eq!(U48::from_truncated(u64::MAX), U48::MAX);
    }

    #[test]
    fn wire_roundtrip() {
        for v in [
            U48::ZERO,
            U48::MAX,
            U48::from(42u32),
            U48::from_truncated(0x1234_5678_9ABC),
        ] {
            let bytes = to_wire(&v);
            assert_eq!(bytes.len(), 6);
            assert_eq!(from_wire::<U48>(&bytes), Ok(v));
        }
    }
}
