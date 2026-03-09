//! Reading a [`NodeReply`] back into types — [`RouteHandle`]'s half of a
//! node operation.
//!
//! [`RouteHandle`]: crate::route_handle::RouteHandle

use std::future::Future;

use crate::error::{Error, Result};
use crate::metadata::Metadata;
use crate::node_op::NodeReply;
use crate::wire::{WaveWire, Wire, from_wire};
use futures::{Stream, StreamExt};

/// A reply of the wrong kind: the node's own fault, never the caller's.
pub fn mismatch() -> Error {
    Error::Backend("node operation answered with the wrong reply kind".into())
}

pub fn decode<T: WaveWire>(bytes: &[u8]) -> Result<T> {
    Ok(from_wire(bytes)?)
}

/// A `Value` reply as a [`Wire`] over the body bytes it carries: kept as
/// they came, checked when viewed (RFC 0076).
pub fn wire<T: WaveWire>(reply: NodeReply) -> Result<Option<Wire<T>>> {
    match reply {
        NodeReply::Value(bytes) => {
            Ok(bytes.map(|b| Wire::new(b.into_boxed_slice(), 0)))
        }
        _ => Err(mismatch()),
    }
}

pub fn done(reply: &NodeReply) -> Result<()> {
    match reply {
        NodeReply::Done => Ok(()),
        _ => Err(mismatch()),
    }
}

pub fn values<T: WaveWire>(reply: NodeReply) -> Result<Vec<T>> {
    match reply {
        NodeReply::Values(items) => items.iter().map(|b| decode(b)).collect(),
        _ => Err(mismatch()),
    }
}

pub fn versions<T: WaveWire>(reply: NodeReply) -> Result<Vec<(Metadata, T)>> {
    match reply {
        NodeReply::Versions(items) => items
            .into_iter()
            .map(|(meta, body)| Ok((meta, decode(&body)?)))
            .collect(),
        _ => Err(mismatch()),
    }
}

/// Yield a whole answer as a stream — or its one error.
pub fn buffered<'a, V: 'a>(
    answer: impl Future<Output = Result<Vec<V>>> + 'a,
) -> impl Stream<Item = Result<V>> + 'a {
    futures::stream::once(answer)
        .map(|answer| match answer {
            Ok(items) => {
                futures::stream::iter(items.into_iter().map(Ok)).left_stream()
            }
            Err(e) => {
                futures::stream::once(std::future::ready(Err(e))).right_stream()
            }
        })
        .flatten()
}
