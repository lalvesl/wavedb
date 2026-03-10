//! The item frame every walk sends a record in: a head, then the record's
//! body bytes, last (RFC 0076).
//!
//! A walk item is the tuple `(H, Vec<u8>)` — `H` the `Id` and `Metadata`
//! that ride with it, or the `Metadata` of a version — whose last heap is
//! the body. So the body is the tail of the frame's own buffer, and the
//! receiver's [`Wire`] is that buffer, viewed past the head: nothing
//! decoded on the node, nothing copied out on the client. (A tuple `(H, T)`
//! would interleave `T`'s stack with `H`'s heap, leaving no `T` to keep.)

use crate::error::{Error, Result};
use crate::wire::{WaveWire, Wire, to_wire_pair, view_wire};

/// The frame of a record: `head`, then the record's body.
#[must_use]
pub fn framed<H: WaveWire, T>(head: &H, body: Wire<T>) -> Vec<u8>
where
    T: WaveWire,
{
    to_wire_pair(head, &body.into_body())
}

/// A [`framed`] item back: its head decoded, its body a [`Wire`] over the
/// frame itself — checked when viewed.
///
/// # Errors
/// [`Error::Wire`] when the frame is not a `(H, Vec<u8>)`.
pub fn unframed<H: WaveWire, T: WaveWire>(
    frame: Vec<u8>,
) -> Result<(H, Wire<T>)> {
    let (head, body) = view_wire::<(H, Vec<u8>)>(&frame)?;
    let head = H::from_ref(head);
    // Where the body sits in the frame: the address the view found it at.
    let start = body.as_ptr().addr() - frame.as_ptr().addr();
    if start + body.len() != frame.len() {
        return Err(Error::Wire(crate::wire::Error::UnexpectedEof));
    }
    Ok((head, Wire::new(frame.into_boxed_slice(), start)))
}

#[cfg(test)]
mod tests {
    use super::{framed, unframed};
    use crate::id::Id;
    use crate::metadata::Metadata;
    use crate::u48::U48;
    use crate::wire::{Wire, to_wire};

    #[test]
    fn a_framed_record_comes_back_as_a_wire_over_the_frame() {
        let id = Id::new(7, U48::from(1u32), false, 3);
        let meta = Metadata::default();
        let body = Wire::<(u32, String)>::encode(&(9, "nine".into()));
        let frame = framed(&(id, meta.clone()), body);
        let (head, wire) =
            unframed::<(Id, Metadata), (u32, String)>(frame).unwrap();
        assert_eq!(head, (id, meta));
        assert_eq!(wire.view().unwrap(), (9, "nine"));
        assert!(
            wire.bytes()
                .ends_with(&to_wire(&(9u32, "nine".to_string())))
        );
    }

    #[test]
    fn a_frame_that_is_no_item_is_a_wire_fault() {
        assert!(matches!(
            unframed::<Metadata, u32>(vec![1, 2]),
            Err(crate::Error::Wire(_))
        ));
    }
}
