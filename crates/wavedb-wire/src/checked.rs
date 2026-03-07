//! The checked encoding — feature `validation`: the plain encoding framed
//! with a CRC32 of its bytes, `[crc32 (4 B LE)][stack][heap]`, for every
//! disk and transport boundary.

use crate::{Error, Result, WaveWire, from_wire, view_wire};

/// Framing bytes [`to_wire_checked`] prepends: one little-endian `u32` CRC32 of
/// the `[stack][heap]` payload that follows.
pub const CRC_PREFIX_LEN: usize = 4;

/// Serialise like [`to_wire`](crate::to_wire), prefixed with a CRC32 of the payload:
/// `[crc32 (4 B LE)][stack][heap]`.
///
/// Still a single allocation: the CRC slot is reserved up front and patched
/// after the payload is written. Decode with [`from_wire_checked`] — plain
/// [`from_wire`] on this buffer would read the CRC bytes as the value's stack.
#[must_use]
pub fn to_wire_checked<T: WaveWire>(value: &T) -> Vec<u8> {
    let mut buf =
        Vec::with_capacity(CRC_PREFIX_LEN + T::STACK_SIZE + value.heap_size());
    buf.extend_from_slice(&[0u8; CRC_PREFIX_LEN]); // slot, patched below
    value.encode_stack(&mut buf);
    value.encode_heap(&mut buf);
    let crc = crc32fast::hash(&buf[CRC_PREFIX_LEN..]);
    buf[..CRC_PREFIX_LEN].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Deserialise a [`to_wire_checked`] buffer: verify the 4-byte CRC32 prefix
/// against the payload, then decode like [`from_wire`].
///
/// A buffer shorter than the prefix fails as [`Error::UnexpectedEof`]; a prefix
/// that doesn't match the payload as [`Error::CrcMismatch`].
pub fn from_wire_checked<T: WaveWire>(bytes: &[u8]) -> Result<T> {
    if bytes.len() < CRC_PREFIX_LEN {
        return Err(Error::UnexpectedEof);
    }
    let (prefix, payload) = bytes.split_at(CRC_PREFIX_LEN);
    let stored = u32::from_le_bytes(prefix.try_into().unwrap());
    let computed = crc32fast::hash(payload);
    if stored != computed {
        return Err(Error::CrcMismatch { stored, computed });
    }
    from_wire(payload)
}

/// View a [`to_wire_checked`] buffer: verify the CRC32 prefix, then view
/// like [`view_wire`] — the borrowed twin of [`from_wire_checked`].
///
/// # Errors
/// Every error [`from_wire_checked`] raises on the same bytes.
pub fn view_wire_checked<T: WaveWire>(bytes: &[u8]) -> Result<T::Ref<'_>> {
    if bytes.len() < CRC_PREFIX_LEN {
        return Err(Error::UnexpectedEof);
    }
    let (prefix, payload) = bytes.split_at(CRC_PREFIX_LEN);
    let mut stored = [0; CRC_PREFIX_LEN];
    stored.copy_from_slice(prefix);
    let stored = u32::from_le_bytes(stored);
    let computed = crc32fast::hash(payload);
    if stored != computed {
        return Err(Error::CrcMismatch { stored, computed });
    }
    view_wire::<T>(payload)
}
