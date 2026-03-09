//! Decode a node [`Reply`] into the shape a typed call expects.
//!
//! Each helper accepts exactly the reply its command produces and treats any
//! other arm as an [`Error::UnexpectedReply`] — a protocol mismatch, distinct
//! from a decode fault or a node refusal.
//!
//! (Items are `pub` in this private module — crate-visible, never exported.)

use wavedb_core::Id;
use wavedb_core::expose::Reply;
use wavedb_core::wire::{WaveWire, Wire, from_wire};

use crate::error::{Error, Result};

/// A `Get`'s reply → a [`Wire`] over the body bytes it carries, moved out
/// of the reply without a copy and checked when viewed (RFC 0076).
pub fn wire<T: WaveWire>(reply: Reply) -> Result<Option<Wire<T>>> {
    match reply {
        Reply::Value(bytes) => {
            Ok(bytes.map(|b| Wire::new(b.into_boxed_slice(), 0)))
        }
        _ => Err(Error::UnexpectedReply),
    }
}

/// A `Save`/`Update`'s reply → unit.
pub const fn done(reply: &Reply) -> Result<()> {
    match reply {
        Reply::Done => Ok(()),
        _ => Err(Error::UnexpectedReply),
    }
}

/// An `Insert`'s reply → the minted record `Id`.
pub const fn inserted(reply: &Reply) -> Result<Id> {
    match reply {
        Reply::Inserted(id) => Ok(*id),
        _ => Err(Error::UnexpectedReply),
    }
}

/// A `Remove`'s reply → whether the record was in the living set.
pub const fn removed(reply: &Reply) -> Result<bool> {
    match reply {
        Reply::Removed(was_live) => Ok(*was_live),
        _ => Err(Error::UnexpectedReply),
    }
}

/// A `ListLen`'s reply → the declared list's living count.
pub const fn count(reply: &Reply) -> Result<u64> {
    match reply {
        Reply::Count(total) => Ok(*total),
        _ => Err(Error::UnexpectedReply),
    }
}

/// A `#[server]` function's reply → its decoded return value.
pub fn returned<R: WaveWire>(reply: Reply) -> Result<R> {
    match reply {
        Reply::Returned(bytes) => {
            let value =
                from_wire::<R>(&bytes).map_err(wavedb_core::Error::from)?;
            Ok(value)
        }
        _ => Err(Error::UnexpectedReply),
    }
}
