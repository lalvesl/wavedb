//! [`Wire<T>`](crate::Wire) is itself [`WaveWire`], with `T`'s exact bytes
//! (RFC 0076): a `Vec<Wire<T>>` is byte for byte a `Vec<T>`. A reply built
//! of records read from a store goes out as the bytes they were stored as,
//! and comes in as buffers to view, with nothing decoded on either side.
//!
//! Encoding writes the buffer's body as it is. Decoding views a `T` — every
//! check its decode makes — to learn where its bytes end, then keeps a copy
//! of exactly those.

use crate::{Cursor, ListRef, Result, WaveWire, Wire};

impl<T: WaveWire> WaveWire for Wire<T> {
    const STACK_SIZE: usize = T::STACK_SIZE;
    type Ref<'a> = T::Ref<'a>;
    type SliceRef<'a> = ListRef<'a, Self>;

    fn heap_size(&self) -> usize {
        self.body().len().saturating_sub(T::STACK_SIZE)
    }

    /// The body's first `STACK_SIZE` bytes — zero-padded should the body be
    /// short of them, which only bytes no view would accept can be, so the
    /// stack is always the size the format promises.
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        let body = self.body();
        let own = body.len().min(T::STACK_SIZE);
        stack.extend_from_slice(&body[..own]);
        stack.resize(stack.len() + (T::STACK_SIZE - own), 0);
    }

    fn encode_heap(&self, heap: &mut Vec<u8>) {
        heap.extend_from_slice(self.body().get(T::STACK_SIZE..).unwrap_or(&[]));
    }

    fn decode<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<Self> {
        let (stack_at, heap_at) = (stack.clone(), heap.clone());
        T::view(stack, heap)?;
        Ok(Self::new(own::<T>(stack_at, heap_at, heap.pos())?, 0))
    }

    fn view<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<T::Ref<'a>> {
        T::view(stack, heap)
    }

    fn from_ref(r: T::Ref<'_>) -> Self {
        Self::encode(&T::from_ref(r))
    }

    fn view_slice(region: &[u8]) -> Result<ListRef<'_, Self>> {
        ListRef::view(region)
    }

    fn from_slice_ref(r: ListRef<'_, Self>) -> Vec<Self> {
        r.to_vec()
    }
}

/// The bytes one `T` took — its stack from `stack`, its heap from `heap` up
/// to `heap_end` — as one buffer of its own encoding.
fn own<T: WaveWire>(
    mut stack: Cursor<'_>,
    mut heap: Cursor<'_>,
    heap_end: usize,
) -> Result<Box<[u8]>> {
    let heap_len = heap_end - heap.pos();
    let mut bytes = Vec::with_capacity(T::STACK_SIZE + heap_len);
    bytes.extend_from_slice(stack.take(T::STACK_SIZE)?);
    bytes.extend_from_slice(heap.take(heap_len)?);
    Ok(bytes.into_boxed_slice())
}
