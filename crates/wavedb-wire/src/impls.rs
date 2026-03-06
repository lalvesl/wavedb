//! [`WaveWire`] implementations for the scalar std types: fixed-width
//! numbers, `bool`, `char` and `()`. Each is its own view. The compound
//! types (`String`, `Vec`, `Option`, arrays, tuples) are in `compound.rs`.
//! See the crate docs for the layout each family uses; the derive in
//! `wavedb-wire-derive` composes these.

use crate::{Cursor, Error, ListRef, Result, WaveWire};

/// The view items of a type that is its own view: a value with no heap bytes,
/// `Copy` and `Debug`, whose view is its decode. A `Vec` of them is a
/// [`ListRef`].
///
/// ```ignore
/// impl WaveWire for Instant {
///     const STACK_SIZE: usize = 8;
///     // … heap_size, encode_stack, encode_heap, decode …
///     wavedb_wire::own_view!();
/// }
/// ```
#[macro_export]
macro_rules! own_view {
    () => {
        type Ref<'a> = Self;
        type SliceRef<'a> = $crate::ListRef<'a, Self>;
        fn view<'a>(
            stack: &mut $crate::Cursor<'a>,
            heap: &mut $crate::Cursor<'a>,
        ) -> $crate::Result<Self> {
            <Self as $crate::WaveWire>::decode(stack, heap)
        }
        fn from_ref(r: Self) -> Self {
            r
        }
        fn view_slice(
            region: &[u8],
        ) -> $crate::Result<$crate::ListRef<'_, Self>> {
            $crate::ListRef::view(region)
        }
        fn from_slice_ref(r: $crate::ListRef<'_, Self>) -> Vec<Self> {
            r.to_vec()
        }
    };
}

// ---- fixed-width scalars ----------------------------------------------------

macro_rules! wire_le {
    ($($t:ty),* $(,)?) => {$(
        impl WaveWire for $t {
            const STACK_SIZE: usize = size_of::<$t>();
            type Ref<'a> = Self;
            type SliceRef<'a> = ListRef<'a, Self>;
            fn heap_size(&self) -> usize { 0 }
            fn encode_stack(&self, stack: &mut Vec<u8>) {
                stack.extend_from_slice(&self.to_le_bytes());
            }
            fn encode_heap(&self, _heap: &mut Vec<u8>) {}
            fn decode(stack: &mut Cursor, _heap: &mut Cursor) -> Result<Self> {
                Ok(<$t>::from_le_bytes(stack.take(size_of::<$t>())?.try_into().unwrap()))
            }
            fn view<'a>(stack: &mut Cursor<'a>, heap: &mut Cursor<'a>) -> Result<Self> {
                Self::decode(stack, heap)
            }
            fn from_ref(r: Self) -> Self { r }
            fn slice_heap_size(items: &[Self]) -> usize {
                items.len() * size_of::<$t>()
            }
            fn encode_slice(items: &[Self], heap: &mut Vec<u8>) {
                heap.reserve(items.len() * size_of::<$t>());
                for e in items {
                    heap.extend_from_slice(&e.to_le_bytes());
                }
            }
            fn decode_slice(region: &[u8]) -> Result<Vec<Self>> {
                let chunks = region.chunks_exact(size_of::<$t>());
                if !chunks.remainder().is_empty() {
                    return Err(Error::UnexpectedEof);
                }
                Ok(chunks
                    .map(|c| <$t>::from_le_bytes(c.try_into().unwrap()))
                    .collect())
            }
            // Any bytes are a valid number: counting is a division.
            fn view_slice(region: &[u8]) -> Result<ListRef<'_, Self>> {
                ListRef::fixed(region)
            }
            fn from_slice_ref(r: ListRef<'_, Self>) -> Vec<Self> {
                r.to_vec()
            }
        }
    )*};
}

wire_le! {
    u16, u32, u64, u128,
    i8, i16, i32, i64, i128,
    f32, f64,
}

// `u8` is the scalar above with its slice methods one copy each: a byte
// vector is the commonest heap payload (a page's records, a frame's body),
// and element by element it cost a cursor and a push per byte. Its view is
// the bytes themselves.
impl WaveWire for u8 {
    const STACK_SIZE: usize = 1;
    type Ref<'a> = Self;
    type SliceRef<'a> = &'a [Self];
    fn heap_size(&self) -> usize {
        0
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        stack.push(*self);
    }
    fn encode_heap(&self, _heap: &mut Vec<u8>) {}
    fn decode(stack: &mut Cursor, _heap: &mut Cursor) -> Result<Self> {
        stack.u8()
    }
    fn view<'a>(
        stack: &mut Cursor<'a>,
        _heap: &mut Cursor<'a>,
    ) -> Result<Self> {
        stack.u8()
    }
    fn from_ref(r: Self) -> Self {
        r
    }
    fn slice_heap_size(items: &[Self]) -> usize {
        items.len()
    }
    fn encode_slice(items: &[Self], heap: &mut Vec<u8>) {
        heap.extend_from_slice(items);
    }
    fn decode_slice(region: &[u8]) -> Result<Vec<Self>> {
        Ok(region.to_vec())
    }
    fn view_slice(region: &[u8]) -> Result<&[Self]> {
        Ok(region)
    }
    fn from_slice_ref(r: &[Self]) -> Vec<Self> {
        r.to_vec()
    }
}

impl WaveWire for bool {
    const STACK_SIZE: usize = 1;
    fn heap_size(&self) -> usize {
        0
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        stack.push(u8::from(*self));
    }
    fn encode_heap(&self, _heap: &mut Vec<u8>) {}
    fn decode(stack: &mut Cursor, _heap: &mut Cursor) -> Result<Self> {
        match stack.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(Error::InvalidBool(other)),
        }
    }
    own_view!();
}

impl WaveWire for char {
    const STACK_SIZE: usize = size_of::<Self>();
    fn heap_size(&self) -> usize {
        0
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        stack.extend_from_slice(&(*self as u32).to_le_bytes());
    }
    fn encode_heap(&self, _heap: &mut Vec<u8>) {}
    fn decode(stack: &mut Cursor, _heap: &mut Cursor) -> Result<Self> {
        let scalar = stack.u32()?;
        Self::from_u32(scalar).ok_or(Error::InvalidChar(scalar))
    }
    own_view!();
}

// The unit type: zero bytes both sections — how a `Result<()>`-returning
// `#[server]` function wires its return.
impl WaveWire for () {
    const STACK_SIZE: usize = 0;
    fn heap_size(&self) -> usize {
        0
    }
    fn encode_stack(&self, _stack: &mut Vec<u8>) {}
    fn encode_heap(&self, _heap: &mut Vec<u8>) {}
    fn decode(_stack: &mut Cursor, _heap: &mut Cursor) -> Result<Self> {
        Ok(())
    }
    own_view!();
}
