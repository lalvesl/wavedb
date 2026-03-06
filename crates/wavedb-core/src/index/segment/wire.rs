//! The `WaveWire` codec of a [`Segment`]: the layout of the tuple
//! `(prev, next, entries)`, and its view.

use wavedb_wire::Cursor;

use crate::local_id::LocalId;
use crate::wire::WaveWire;

use super::super::sec_key::SecKey;
use super::Segment;

// Hand impl rather than a derive: the derive would need an owned wire-shape
// twin, and copying a segment's entries to encode them would double the bytes a
// write already moves. The composition is exactly the derive's — each field's
// stack slots inline in order, heaps appended in the same order — so this is the
// tuple `(prev, next, entries)` layout without owning the tuple.
impl<P: WaveWire> WaveWire for Segment<P> {
    const STACK_SIZE: usize = <Option<LocalId> as WaveWire>::STACK_SIZE * 2
        + <Vec<(SecKey, P)> as WaveWire>::STACK_SIZE;

    fn heap_size(&self) -> usize {
        self.prev.heap_size() + self.next.heap_size() + self.entries.heap_size()
    }

    fn encode_stack(&self, stack: &mut Vec<u8>) {
        self.prev.encode_stack(stack);
        self.next.encode_stack(stack);
        self.entries.encode_stack(stack);
    }

    fn encode_heap(&self, heap: &mut Vec<u8>) {
        self.prev.encode_heap(heap);
        self.next.encode_heap(heap);
        self.entries.encode_heap(heap);
    }

    fn decode(
        stack: &mut Cursor,
        heap: &mut Cursor,
    ) -> wavedb_wire::Result<Self> {
        Ok(Self {
            prev: Option::decode(stack, heap)?,
            next: Option::decode(stack, heap)?,
            entries: Vec::decode(stack, heap)?,
        })
    }

    // The view is the tuple's view: the layout is the tuple's.
    type Ref<'a> = <Shape<P> as WaveWire>::Ref<'a>;
    type SliceRef<'a> = wavedb_wire::ListRef<'a, Self>;

    fn view<'a>(
        stack: &mut Cursor<'a>,
        heap: &mut Cursor<'a>,
    ) -> wavedb_wire::Result<Self::Ref<'a>> {
        Shape::<P>::view(stack, heap)
    }

    fn from_ref(r: Self::Ref<'_>) -> Self {
        let (prev, next, entries) = Shape::<P>::from_ref(r);
        Self {
            prev,
            next,
            entries,
        }
    }

    fn view_slice(
        region: &[u8],
    ) -> wavedb_wire::Result<wavedb_wire::ListRef<'_, Self>> {
        wavedb_wire::ListRef::view(region)
    }

    fn from_slice_ref(r: wavedb_wire::ListRef<'_, Self>) -> Vec<Self> {
        r.to_vec()
    }
}

/// The tuple a [`Segment`] encodes as.
type Shape<P> = (Option<LocalId>, Option<LocalId>, Vec<(SecKey, P)>);
