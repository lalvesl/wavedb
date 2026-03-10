//! `wavedb-wire` — the standalone `WaveWire` (de)serialization format.
//!
//! Pure value ⇄ bytes. It knows **nothing** about `STRUCT_HASH`, the registry,
//! `Id`, permissions, or the engine — it is just a deterministic, platform-
//! independent codec. [`to_wire`] turns a value into a `Vec<u8>`; [`from_wire`]
//! turns bytes back into a value of a **caller-chosen** type. There is no type
//! tag on the bytes and no type validation on decode: `from_wire::<T>` simply
//! tries to read a `T` out of the buffer. The only thing that can go wrong is the
//! buffer not lining up with the type's layout — i.e. running short of bytes
//! ([`Error::UnexpectedEof`]) — plus the few intrinsic per-type checks (a `bool`
//! byte that isn't `0`/`1`, a non-scalar `char`, non-UTF-8 string bytes, an
//! out-of-range enum tag).
//!
//! A serialised value is two contiguous sections: a fixed-size **stack** section
//! (`T::STACK_SIZE` bytes, every offset a compile-time constant) followed by a
//! variable **heap** section. Dynamic fields keep a fixed slot in the stack
//! section (a `u32` length and/or flag byte) and put their payload in the heap.
//!
//! See `docs/wire_format.md` for the full specification. Two composition modes:
//!
//! - **Flattened** (struct fields, `Option`'s inner): stack slots are emitted
//!   inline into the parent stack; heap payloads append to the shared heap.
//! - **Unit** (each `Vec` element): the value is self-contained
//!   `[stack][heap]` back-to-back inside the parent's heap region.

// Pedantic/nursery lints that fight terse, byte-precise (de)serialization code.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::cast_sign_loss,
    clippy::missing_const_for_fn
)]

// The derive emits absolute `::wavedb_wire::` paths; this lets the crate use its
// own derive (e.g. in tests).
extern crate self as wavedb_wire;

use thiserror::Error;

/// Derive a [`WaveWire`] impl for a struct or enum. Re-exported from
/// [`wavedb-wire-derive`](wavedb_wire_derive); see its docs for the supported
/// shapes (structs: named/tuple/unit; enums: canonical tag form).
pub use wavedb_wire_derive::WaveWire;

// `WaveWire` impls for the supported std types: the scalars, then String,
// Vec, Option, arrays and tuples.
mod compound;
mod impls;
// Borrowed views over the bytes (RFC 0076).
mod view;
mod wire_codec;

pub use view::{ListIter, ListRef, Wire, view_wire};

// The crc32-framed encoding.
#[cfg(feature = "validation")]
mod checked;

#[cfg(feature = "validation")]
pub use checked::{
    CRC_PREFIX_LEN, from_wire_checked, to_wire_checked, view_wire_checked,
};

/// A wire (de)serialization fault.
///
/// Every variant is a layout/shape mismatch between the bytes and the type the
/// caller asked to decode — never a "wrong type" error, because the wire bytes
/// carry no type identity. The dominant case is [`UnexpectedEof`](Error::UnexpectedEof):
/// the buffer didn't match the size the type needs.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
    /// A reader ran past the end of its buffer — the bytes didn't match the
    /// size the decoded type expected.
    #[error("unexpected end of wire buffer")]
    UnexpectedEof,
    /// A `String` field held bytes that were not valid UTF-8.
    #[error("invalid utf-8 in wire string")]
    Utf8,
    /// A `char` field held a `u32` that is not a Unicode scalar value.
    #[error("invalid char scalar {0:#x}")]
    InvalidChar(u32),
    /// An enum field held a tag outside the declared variant range.
    #[error("invalid enum tag {0}")]
    InvalidTag(u8),
    /// A `bool` field held a byte other than `0` or `1`.
    #[error("invalid bool byte {0}")]
    InvalidBool(u8),
    /// A [`from_wire_checked`] buffer's CRC32 prefix did not match its payload —
    /// the bytes were corrupted or truncated in a way that still parses.
    #[cfg(feature = "validation")]
    #[error("crc mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch {
        /// The CRC32 read from the buffer's 4-byte prefix.
        stored: u32,
        /// The CRC32 computed over the payload that followed it.
        computed: u32,
    },
}

/// Shorthand for a `Result` carrying the wire [`Error`](enum@Error).
pub type Result<T> = core::result::Result<T, Error>;

/// A forward-only byte cursor over a borrowed buffer, used for both the stack
/// and heap sections during decode.
#[derive(Clone)]
pub struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// Wrap a byte slice.
    #[must_use]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes consumed so far — how `Vec` decode finds where one unit ends.
    #[must_use]
    pub const fn pos(&self) -> usize {
        self.pos
    }

    /// Bytes left in the buffer.
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Consume exactly `n` bytes, or [`Error::UnexpectedEof`].
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::UnexpectedEof);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// Read a little-endian `u8`.
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// Read a little-endian `u32` length slot.
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
}

/// A type with a deterministic, platform-independent wire layout.
///
/// Implementors guarantee that [`encode_stack`](WaveWire::encode_stack) writes
/// **exactly** `STACK_SIZE` bytes and that [`decode`](WaveWire::decode) reads exactly
/// `STACK_SIZE` bytes from its stack cursor.
pub trait WaveWire: Sized {
    /// Fixed number of bytes this type occupies in the stack section.
    const STACK_SIZE: usize;

    /// Number of heap bytes this value contributes (so a writer can reserve the
    /// whole buffer in one allocation).
    fn heap_size(&self) -> usize;

    /// Append exactly `STACK_SIZE` bytes to the stack buffer.
    fn encode_stack(&self, stack: &mut Vec<u8>);

    /// Append this value's heap payload (depth-first) to the heap buffer.
    fn encode_heap(&self, heap: &mut Vec<u8>);

    /// Reconstruct a value, reading `STACK_SIZE` bytes from `stack` and any
    /// payload from `heap`.
    fn decode<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<Self>;

    /// The borrowed twin (RFC 0076): what [`view`](WaveWire::view) builds
    /// over a value's bytes — `&'a str` for a `String`, `&'a [u8]` for a
    /// `Vec<u8>`, a [`ListRef`] for any other `Vec`, a number as itself, and
    /// for a derived `Name`, the `NameRef<'a>` the derive emits beside it.
    type Ref<'a>: Copy + core::fmt::Debug;

    /// What a `Vec` of these borrows as: a [`ListRef`], or `&'a [u8]` for
    /// bytes.
    type SliceRef<'a>: Copy + core::fmt::Debug;

    /// Build the twin from `STACK_SIZE` bytes of `stack` and this value's
    /// payload in `heap` — the walk [`decode`](WaveWire::decode) makes,
    /// borrowing what it would copy. Every check `decode` makes happens here.
    fn view<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<Self::Ref<'a>>;

    /// The owned value back from its twin: the copies `decode` would have
    /// made.
    fn from_ref(r: Self::Ref<'_>) -> Self;

    /// View a `Vec` of these from its whole heap `region`.
    ///
    /// # Errors
    /// As [`decode_slice`](WaveWire::decode_slice) on the same region.
    fn view_slice(region: &[u8]) -> Result<Self::SliceRef<'_>>;

    /// The owned `Vec` back from its view.
    fn from_slice_ref(r: Self::SliceRef<'_>) -> Vec<Self>;

    /// Heap bytes a `Vec` of these takes: each element a self-contained
    /// unit, its stack then its heap. A fixed-width scalar overrides it with
    /// a product, so a byte vector's length is not a walk over its bytes.
    fn slice_heap_size(items: &[Self]) -> usize {
        items.iter().map(|e| Self::STACK_SIZE + e.heap_size()).sum()
    }

    /// Append a `Vec` of these to `heap`, element by element as units. A
    /// fixed-width scalar overrides it, `u8` with one copy.
    fn encode_slice(items: &[Self], heap: &mut Vec<u8>) {
        for e in items {
            e.encode_stack(heap);
            e.encode_heap(heap);
        }
    }

    /// Decode a `Vec` of these from its whole heap `region`, element by
    /// element as units. A fixed-width scalar overrides it with a walk over
    /// fixed chunks, `u8` with one copy — the same bytes either way.
    ///
    /// # Errors
    /// [`Error::UnexpectedEof`] when an element runs past the region, or an
    /// element's own decode fault.
    fn decode_slice(region: &[u8]) -> Result<Vec<Self>> {
        let mut out = Vec::new();
        let mut cur = 0;
        while cur < region.len() {
            // The element's stack is the next STACK_SIZE bytes; its heap is
            // the remainder of the region, of which it consumes only what it
            // needs.
            let stack_end = cur + Self::STACK_SIZE;
            let Some(stack) = region.get(cur..stack_end) else {
                return Err(Error::UnexpectedEof);
            };
            let mut es = Cursor::new(stack);
            let mut eh = Cursor::new(&region[stack_end..]);
            out.push(Self::decode(&mut es, &mut eh)?);
            // A unit of no bytes would spin here forever on a region that
            // is not empty (a `Vec<()>`'s, say), never using it up.
            if stack_end + eh.pos() == cur {
                return Err(Error::UnexpectedEof);
            }
            cur = stack_end + eh.pos();
        }
        Ok(out)
    }
}

/// Serialise a value to a single `[stack][heap]` byte vector in one allocation.
///
/// `STACK_SIZE` (a compile-time sum over every field, including nested `WaveWire`
/// types) plus the recursively-computed `heap_size()` give the exact final length
/// up front. `encode_stack` appends the `STACK_SIZE`-byte stack section, then
/// `encode_heap` appends the heap section to the same buffer — no second
/// allocation, no concat.
#[must_use]
pub fn to_wire<T: WaveWire>(value: &T) -> Vec<u8> {
    let mut buf = Vec::with_capacity(T::STACK_SIZE + value.heap_size());
    value.encode_stack(&mut buf);
    value.encode_heap(&mut buf);
    debug_assert_eq!(buf.len(), T::STACK_SIZE + value.heap_size());
    buf
}

/// Serialise two borrowed values as the tuple `(A, B)`.
///
/// Byte-identical to `to_wire(&(a, b))` — a tuple encodes its members' stacks
/// in order, then their heaps in order — without requiring owned values to
/// build the tuple.
#[must_use]
pub fn to_wire_pair<A: WaveWire, B: WaveWire>(a: &A, b: &B) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        A::STACK_SIZE + B::STACK_SIZE + a.heap_size() + b.heap_size(),
    );
    a.encode_stack(&mut buf);
    b.encode_stack(&mut buf);
    a.encode_heap(&mut buf);
    b.encode_heap(&mut buf);
    buf
}

/// Deserialise a value from a `[stack][heap]` byte slice.
///
/// No type tag is consulted — the caller picks `T`, and decode just tries to read
/// a `T`. A buffer shorter than `T::STACK_SIZE` is the size mismatch that fails as
/// [`Error::UnexpectedEof`].
pub fn from_wire<T: WaveWire>(bytes: &[u8]) -> Result<T> {
    if bytes.len() < T::STACK_SIZE {
        return Err(Error::UnexpectedEof);
    }
    let (stack, heap) = bytes.split_at(T::STACK_SIZE);
    let mut sc = Cursor::new(stack);
    let mut hc = Cursor::new(heap);
    T::decode(&mut sc, &mut hc)
}

#[cfg(test)]
mod tests {
    use super::{WaveWire, from_wire, to_wire};

    fn roundtrip<T: WaveWire + PartialEq + core::fmt::Debug>(value: &T) {
        let bytes = to_wire(value);
        assert_eq!(bytes.len(), T::STACK_SIZE + value.heap_size());
        let back: T = from_wire(&bytes).expect("decode");
        assert_eq!(*value, back);
    }

    #[test]
    fn scalars() {
        roundtrip(&0u8);
        roundtrip(&u128::MAX);
        roundtrip(&-7i32);
        roundtrip(&2.5f64);
        roundtrip(&true);
        roundtrip(&'🌊');
    }

    #[test]
    fn strings_and_vecs() {
        roundtrip(&String::new());
        roundtrip(&"wave".to_string());
        roundtrip(&vec![1u32, 2, 3]);
        roundtrip(&vec!["a".to_string(), String::new(), "ccc".to_string()]);
        roundtrip(&vec![vec![1u8], vec![], vec![2u8, 3]]);
    }

    /// A scalar vector is its length then its elements' LE bytes back to
    /// back — the bulk paths write exactly the bytes the per-element walk
    /// did, so the layout is the same with or without them.
    #[test]
    fn scalar_vectors_keep_the_unit_layout() {
        assert_eq!(to_wire(&vec![7u8, 8, 9]), [3, 0, 0, 0, 7, 8, 9]);
        assert_eq!(to_wire(&vec![1u16, 0x0302]), [4, 0, 0, 0, 1, 0, 2, 3]);
        assert_eq!(to_wire(&Vec::<u64>::new()), [0, 0, 0, 0]);
        roundtrip(&vec![0xA5u8; 100_000]);
        roundtrip(&vec![u64::MAX, 0, 42]);
        roundtrip(&vec![i16::MIN, -1, i16::MAX]);
    }

    /// A scalar vector whose region is not a whole number of elements is
    /// short, not silently truncated.
    #[test]
    fn a_ragged_scalar_region_is_eof() {
        use super::Error;
        // Region of 3 bytes declared for a Vec<u16>.
        let bytes = [3u8, 0, 0, 0, 1, 2, 3];
        assert_eq!(from_wire::<Vec<u16>>(&bytes), Err(Error::UnexpectedEof));
    }

    #[test]
    fn options() {
        roundtrip(&Option::<u64>::None);
        roundtrip(&Some(42u64));
        roundtrip(&Some("x".to_string()));
        roundtrip(&vec![Some(1u8), None, Some(2)]);
    }

    #[test]
    fn arrays_and_tuples() {
        roundtrip(&[1u32, 2, 3]);
        roundtrip(&["a".to_string(), "bb".to_string()]); // array of heap-bearing elems
        roundtrip(&[Some(1u8), None]);
        roundtrip(&(1u8, "x".to_string(), 9u64));
        roundtrip(&(vec![1u16, 2], 'z', Option::<u32>::None, true));
    }

    #[test]
    fn short_buffer_is_eof() {
        // The defining failure: bytes that don't match the type's size.
        use super::Error;
        assert_eq!(from_wire::<u64>(&[0u8; 4]), Err(Error::UnexpectedEof));
        assert_eq!(from_wire::<u32>(&[]), Err(Error::UnexpectedEof));
    }

    // ---- #[derive(WaveWire)] ------------------------------------------------
    // `WaveWire` (trait + derive macro, same name) is imported at the top of the
    // module.

    #[derive(WaveWire, PartialEq, Debug)]
    struct Named {
        a: u64,
        b: String,
        c: Vec<u16>,
    }

    #[derive(WaveWire, PartialEq, Debug)]
    struct Tuple(u32, String);

    #[derive(WaveWire, PartialEq, Debug)]
    struct UnitS;

    #[derive(WaveWire, PartialEq, Debug)]
    enum FieldlessEnum {
        A,
        B,
        C,
    }

    #[derive(WaveWire, PartialEq, Debug)]
    enum MixedEnum {
        Nothing,
        One(u64),
        Many(u32, String),
        Struct { x: u8, label: String },
    }

    #[test]
    fn derive_structs() {
        roundtrip(&Named {
            a: 7,
            b: "wave".to_string(),
            c: vec![1, 2, 3],
        });
        roundtrip(&Tuple(42, String::new()));
        roundtrip(&UnitS);
    }

    #[test]
    fn derive_fieldless_enum_is_one_byte() {
        assert_eq!(FieldlessEnum::STACK_SIZE, 1);
        assert_eq!(to_wire(&FieldlessEnum::A), vec![0]);
        assert_eq!(to_wire(&FieldlessEnum::C), vec![2]);
        roundtrip(&FieldlessEnum::B);
    }

    #[test]
    fn derive_enum_with_fields() {
        assert_eq!(MixedEnum::STACK_SIZE, 1 + 4); // tag + payload length
        roundtrip(&MixedEnum::Nothing);
        roundtrip(&MixedEnum::One(0xDEAD_BEEF));
        roundtrip(&MixedEnum::Many(9, "x".to_string()));
        roundtrip(&MixedEnum::Struct {
            x: 5,
            label: "hello".to_string(),
        });
    }

    #[test]
    fn derive_enum_bad_tag_is_invalid_tag() {
        // tag 9, zero payload length — no such variant.
        let bytes = [9u8, 0, 0, 0, 0];
        assert_eq!(
            from_wire::<MixedEnum>(&bytes),
            Err(super::Error::InvalidTag(9))
        );
    }

    // ---- feature `validation`: crc32-framed encoding -------------------------

    #[cfg(feature = "validation")]
    mod checked {
        use super::Named;
        use crate::{
            CRC_PREFIX_LEN, Error, from_wire_checked, to_wire_checked,
        };

        #[test]
        fn roundtrips_with_crc_prefix() {
            let v = Named {
                a: 7,
                b: "wave".to_string(),
                c: vec![1, 2, 3],
            };
            let bytes = to_wire_checked(&v);
            assert_eq!(
                bytes.len(),
                CRC_PREFIX_LEN + crate::to_wire(&v).len(),
                "checked buffer = crc prefix + plain encoding"
            );
            assert_eq!(&bytes[CRC_PREFIX_LEN..], crate::to_wire(&v));
            assert_eq!(from_wire_checked::<Named>(&bytes).unwrap(), v);
        }

        #[test]
        fn tampered_payload_is_crc_mismatch() {
            let mut bytes = to_wire_checked(&42u64);
            *bytes.last_mut().unwrap() ^= 0xFF;
            assert!(matches!(
                from_wire_checked::<u64>(&bytes),
                Err(Error::CrcMismatch { .. })
            ));
        }

        #[test]
        fn a_checked_view_borrows_and_checks_the_crc() {
            let v = Named {
                a: 7,
                b: "wave".to_string(),
                c: vec![1, 2, 3],
            };
            let mut bytes = to_wire_checked(&v);
            let view = crate::view_wire_checked::<Named>(&bytes).unwrap();
            assert_eq!((view.a, view.b), (7, "wave"));
            assert_eq!(Named::from(view), v);
            *bytes.last_mut().unwrap() ^= 0xFF;
            assert!(matches!(
                crate::view_wire_checked::<Named>(&bytes),
                Err(Error::CrcMismatch { .. })
            ));
            assert_eq!(
                crate::view_wire_checked::<u64>(&[0u8; 3]),
                Err(Error::UnexpectedEof)
            );
        }

        #[test]
        fn tampered_prefix_is_crc_mismatch() {
            let mut bytes = to_wire_checked(&42u64);
            bytes[0] ^= 0xFF;
            assert!(matches!(
                from_wire_checked::<u64>(&bytes),
                Err(Error::CrcMismatch { .. })
            ));
        }

        #[test]
        fn short_buffer_is_eof() {
            assert_eq!(
                from_wire_checked::<u64>(&[0u8; 3]),
                Err(Error::UnexpectedEof)
            );
            // Prefix intact but payload truncated: crc fails first.
            let bytes = to_wire_checked(&42u64);
            assert!(matches!(
                from_wire_checked::<u64>(&bytes[..bytes.len() - 1]),
                Err(Error::CrcMismatch { .. })
            ));
        }
    }
}
