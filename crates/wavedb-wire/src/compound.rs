//! [`WaveWire`] implementations for the compound std types: `String`,
//! `Vec<T>`, `Option<T>`, `[T; N]`, and tuples up to arity 4. A `String`
//! views as `&str`, a `Vec<T>` as `T`'s [`SliceRef`](WaveWire::SliceRef),
//! the rest as their members' views.

use crate::{Cursor, Error, ListRef, Result, WaveWire};

impl WaveWire for String {
    const STACK_SIZE: usize = size_of::<u32>(); // byte-length
    type Ref<'a> = &'a str;
    type SliceRef<'a> = ListRef<'a, Self>;
    fn heap_size(&self) -> usize {
        self.len()
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        stack.extend_from_slice(&(self.len() as u32).to_le_bytes());
    }
    fn encode_heap(&self, heap: &mut Vec<u8>) {
        heap.extend_from_slice(self.as_bytes());
    }
    fn decode<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<Self> {
        let len = stack.u32()? as usize;
        let bytes = heap.take(len)?;
        Self::from_utf8(bytes.to_vec()).map_err(|_| Error::Utf8)
    }
    fn view<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<&'a str> {
        let len = stack.u32()? as usize;
        core::str::from_utf8(heap.take(len)?).map_err(|_| Error::Utf8)
    }
    fn from_ref(r: &str) -> Self {
        r.to_owned()
    }
    fn view_slice(region: &[u8]) -> Result<ListRef<'_, Self>> {
        ListRef::view(region)
    }
    fn from_slice_ref(r: ListRef<'_, Self>) -> Vec<Self> {
        r.to_vec()
    }
}

impl<T: WaveWire> WaveWire for Vec<T> {
    const STACK_SIZE: usize = size_of::<u32>(); // byte-length
    type Ref<'a> = T::SliceRef<'a>;
    type SliceRef<'a> = ListRef<'a, Self>;
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
    fn decode<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<Self> {
        let region_len = stack.u32()? as usize;
        T::decode_slice(heap.take(region_len)?)
    }
    fn view<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<T::SliceRef<'a>> {
        let region_len = stack.u32()? as usize;
        T::view_slice(heap.take(region_len)?)
    }
    fn from_ref(r: T::SliceRef<'_>) -> Self {
        T::from_slice_ref(r)
    }
    fn view_slice(region: &[u8]) -> Result<ListRef<'_, Self>> {
        ListRef::view(region)
    }
    fn from_slice_ref(r: ListRef<'_, Self>) -> Vec<Self> {
        r.to_vec()
    }
}

impl<T: WaveWire> WaveWire for Option<T> {
    // 1 flag byte in stack; T's full wire representation (stack + heap) goes
    // into the parent's heap section only when Some. None costs exactly 1 byte.
    const STACK_SIZE: usize = 1;
    type Ref<'a> = Option<T::Ref<'a>>;
    type SliceRef<'a> = ListRef<'a, Self>;
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
    fn decode<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<Self> {
        if stack.u8()? == 0 {
            return Ok(None);
        }
        let t_stack = heap.take(T::STACK_SIZE)?;
        let mut ts = Cursor::new(t_stack);
        Ok(Some(T::decode(&mut ts, heap)?))
    }
    fn view<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<Option<T::Ref<'a>>> {
        if stack.u8()? == 0 {
            return Ok(None);
        }
        let mut ts = Cursor::new(heap.take(T::STACK_SIZE)?);
        Ok(Some(T::view(&mut ts, heap)?))
    }
    fn from_ref(r: Option<T::Ref<'_>>) -> Self {
        r.map(T::from_ref)
    }
    fn view_slice(region: &[u8]) -> Result<ListRef<'_, Self>> {
        ListRef::view(region)
    }
    fn from_slice_ref(r: ListRef<'_, Self>) -> Vec<Self> {
        r.to_vec()
    }
}

// Arrays and tuples compose **flattened**: each element's stack slots are emitted
// inline into the parent stack, heaps appended in order — so every offset stays a
// compile-time constant.

impl<T: WaveWire, const N: usize> WaveWire for [T; N] {
    const STACK_SIZE: usize = N * T::STACK_SIZE;
    type Ref<'a> = [T::Ref<'a>; N];
    type SliceRef<'a> = ListRef<'a, Self>;
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
    fn decode<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<Self> {
        let mut v = Vec::with_capacity(N);
        for _ in 0..N {
            v.push(T::decode(stack, heap)?);
        }
        // `v` has exactly N elements, so the conversion never fails.
        v.try_into().map_err(|_| Error::UnexpectedEof)
    }
    fn view<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<[T::Ref<'a>; N]> {
        // An empty `Vec` converts to `[_; 0]` without allocating.
        if N == 0 {
            return Vec::new().try_into().map_err(|_| Error::UnexpectedEof);
        }
        // `from_fn` cannot fail, so a fault is kept aside and the slots past
        // it filled with the first element — `Copy` — then the fault returned.
        let first = T::view(stack, heap)?;
        let mut fault = None;
        let out = core::array::from_fn(|i| {
            if i == 0 || fault.is_some() {
                return first;
            }
            T::view(stack, heap).unwrap_or_else(|e| {
                fault = Some(e);
                first
            })
        });
        fault.map_or(Ok(out), Err)
    }
    fn from_ref(r: [T::Ref<'_>; N]) -> Self {
        r.map(T::from_ref)
    }
    fn view_slice(region: &[u8]) -> Result<ListRef<'_, Self>> {
        ListRef::view(region)
    }
    fn from_slice_ref(r: ListRef<'_, Self>) -> Vec<Self> {
        r.to_vec()
    }
}

macro_rules! wire_tuple {
    ($($name:ident $idx:tt),+) => {
        impl<$($name: WaveWire),+> WaveWire for ($($name,)+) {
            const STACK_SIZE: usize = 0 $(+ $name::STACK_SIZE)+;
            type Ref<'a> = ($($name::Ref<'a>,)+);
            type SliceRef<'a> = ListRef<'a, Self>;
            fn heap_size(&self) -> usize { 0 $(+ self.$idx.heap_size())+ }
            fn encode_stack(&self, stack: &mut Vec<u8>) { $(self.$idx.encode_stack(stack);)+ }
            fn encode_heap(&self, heap: &mut Vec<u8>) { $(self.$idx.encode_heap(heap);)+ }
            fn decode<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> Result<Self> {
                Ok(( $($name::decode(stack, heap)?,)+ ))
            }
            fn view<'a>(stack: &mut Cursor<'a>, heap: &mut Cursor<'a>) -> Result<Self::Ref<'a>> {
                Ok(( $($name::view(stack, heap)?,)+ ))
            }
            fn from_ref(r: Self::Ref<'_>) -> Self {
                ( $($name::from_ref(r.$idx),)+ )
            }
            fn view_slice(region: &[u8]) -> Result<ListRef<'_, Self>> {
                ListRef::view(region)
            }
            fn from_slice_ref(r: ListRef<'_, Self>) -> Vec<Self> {
                r.to_vec()
            }
        }
    };
}

wire_tuple!(A 0, B 1);
wire_tuple!(A 0, B 1, C 2);
wire_tuple!(A 0, B 1, C 2, D 3);
