//! [`WaveWire`] implementations for the std types the codec supports:
//! fixed-width scalars, `bool`/`char`, `String`, `Vec<T>`, `Option<T>`,
//! `[T; N]`, and tuples up to arity 4. See the crate docs for the layout each
//! family uses; the derive in `wavedb-wire-derive` composes these.

use crate::{Cursor, Error, Result, WaveWire};

// ---- fixed-width scalars ----------------------------------------------------

macro_rules! wire_le {
    ($($t:ty),* $(,)?) => {$(
        impl WaveWire for $t {
            const STACK_SIZE: usize = size_of::<$t>();
            fn heap_size(&self) -> usize { 0 }
            fn encode_stack(&self, stack: &mut Vec<u8>) {
                stack.extend_from_slice(&self.to_le_bytes());
            }
            fn encode_heap(&self, _heap: &mut Vec<u8>) {}
            fn decode(stack: &mut Cursor, _heap: &mut Cursor) -> Result<Self> {
                Ok(<$t>::from_le_bytes(stack.take(size_of::<$t>())?.try_into().unwrap()))
            }
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
// and element by element it cost a cursor and a push per byte.
impl WaveWire for u8 {
    const STACK_SIZE: usize = 1;
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
    fn slice_heap_size(items: &[Self]) -> usize {
        items.len()
    }
    fn encode_slice(items: &[Self], heap: &mut Vec<u8>) {
        heap.extend_from_slice(items);
    }
    fn decode_slice(region: &[u8]) -> Result<Vec<Self>> {
        Ok(region.to_vec())
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
}

// ---- dynamic types ----------------------------------------------------------

impl WaveWire for String {
    const STACK_SIZE: usize = size_of::<u32>(); // byte-length
    fn heap_size(&self) -> usize {
        self.len()
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        stack.extend_from_slice(&(self.len() as u32).to_le_bytes());
    }
    fn encode_heap(&self, heap: &mut Vec<u8>) {
        heap.extend_from_slice(self.as_bytes());
    }
    fn decode(stack: &mut Cursor, heap: &mut Cursor) -> Result<Self> {
        let len = stack.u32()? as usize;
        let bytes = heap.take(len)?;
        Self::from_utf8(bytes.to_vec()).map_err(|_| Error::Utf8)
    }
}

impl<T: WaveWire> WaveWire for Vec<T> {
    const STACK_SIZE: usize = size_of::<u32>(); // byte-length
    fn heap_size(&self) -> usize {
        // Each element is a self-contained unit: its stack bytes then its heap.
        T::slice_heap_size(self)
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        stack.extend_from_slice(&(self.heap_size() as u32).to_le_bytes());
    }
    fn encode_heap(&self, heap: &mut Vec<u8>) {
        T::encode_slice(self, heap);
    }
    fn decode(stack: &mut Cursor, heap: &mut Cursor) -> Result<Self> {
        let region_len = stack.u32()? as usize;
        T::decode_slice(heap.take(region_len)?)
    }
}

impl<T: WaveWire> WaveWire for Option<T> {
    // 1 flag byte in stack; T's full wire representation (stack + heap) goes
    // into the parent's heap section only when Some. None costs exactly 1 byte.
    const STACK_SIZE: usize = 1;
    fn heap_size(&self) -> usize {
        self.as_ref().map_or(0, |v| T::STACK_SIZE + v.heap_size())
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        stack.push(u8::from(self.is_some()));
    }
    fn encode_heap(&self, heap: &mut Vec<u8>) {
        if let Some(v) = self {
            v.encode_stack(heap); // T's stack bytes land in parent heap
            v.encode_heap(heap);
        }
    }
    fn decode(stack: &mut Cursor, heap: &mut Cursor) -> Result<Self> {
        if stack.u8()? == 0 {
            return Ok(None);
        }
        let t_stack = heap.take(T::STACK_SIZE)?;
        let mut ts = Cursor::new(t_stack);
        Ok(Some(T::decode(&mut ts, heap)?))
    }
}

// Arrays and tuples compose **flattened**: each element's stack slots are emitted
// inline into the parent stack, heaps appended in order — so every offset stays a
// compile-time constant.

impl<T: WaveWire, const N: usize> WaveWire for [T; N] {
    const STACK_SIZE: usize = N * T::STACK_SIZE;
    fn heap_size(&self) -> usize {
        self.iter().map(WaveWire::heap_size).sum()
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        for e in self {
            e.encode_stack(stack);
        }
    }
    fn encode_heap(&self, heap: &mut Vec<u8>) {
        for e in self {
            e.encode_heap(heap);
        }
    }
    fn decode(stack: &mut Cursor, heap: &mut Cursor) -> Result<Self> {
        let mut v = Vec::with_capacity(N);
        for _ in 0..N {
            v.push(T::decode(stack, heap)?);
        }
        // `v` has exactly N elements, so the conversion never fails.
        v.try_into().map_err(|_| Error::UnexpectedEof)
    }
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
}

macro_rules! wire_tuple {
    ($($name:ident $idx:tt),+) => {
        impl<$($name: WaveWire),+> WaveWire for ($($name,)+) {
            const STACK_SIZE: usize = 0 $(+ $name::STACK_SIZE)+;
            fn heap_size(&self) -> usize { 0 $(+ self.$idx.heap_size())+ }
            fn encode_stack(&self, stack: &mut Vec<u8>) { $(self.$idx.encode_stack(stack);)+ }
            fn encode_heap(&self, heap: &mut Vec<u8>) { $(self.$idx.encode_heap(heap);)+ }
            fn decode(stack: &mut Cursor, heap: &mut Cursor) -> Result<Self> {
                Ok(( $($name::decode(stack, heap)?,)+ ))
            }
        }
    };
}

wire_tuple!(A 0, B 1);
wire_tuple!(A 0, B 1, C 2);
wire_tuple!(A 0, B 1, C 2, D 3);
