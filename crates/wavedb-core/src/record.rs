//! Stored-record plumbing shared by the collection layer: the envelopes, id
//! minting, the plan-time `Overlay` view, and the `Unique`-anchor
//! operations.
//!
//! ## Envelopes
//!
//! Every stored value starts `[STRUCT_HASH (8 B LE)]` — storage backends route
//! by those first 8 bytes, and decode verifies them, so a stale or foreign
//! `Id` can't silently decode as the wrong type. Two envelope forms follow it:
//!
//! - **bare** (`Pivot` records — pure addressing, no history):
//!   `[STRUCT_HASH][WaveWire bytes]`;
//! - **record** (Unique + NonUnique user data):
//!   `[STRUCT_HASH][meta_len (u32 LE)][WaveWire(Metadata)][WaveWire body]` —
//!   the length prefix splits the two independently-decodable payloads, and
//!   carrying [`Metadata`] is what makes every version chainable.
//!
//! ## The version chain
//!
//! Saving never destroys the old bytes; the live record always sits at the
//! shape's **anchor** and every superseded version archives at a **derived
//! slot**: `KEY` = the instant that version was authored (its
//! [`Succession::CreatedAt`]), `SALT` = the type's `type_salt`, `FLAG` =
//! the anchor's bit **flipped**. Because the address is a pure function of
//! `(type, shape, instant)`, chain links are bare instants (`Metadata`'s
//! `previous` / [`Succession::Next`]) and a link written once is correct
//! forever — no archive is ever repointed. Walk backward from the live
//! record via `previous`; walk forward from any archive via `Next` — a
//! MISS at a derived slot means the successor is still the live record at
//! the anchor.
//!
//! [`Succession::CreatedAt`]: crate::metadata::Succession::CreatedAt
//! [`Succession::Next`]: crate::metadata::Succession::Next

use crate::error::{Error, Result};
use crate::id::Id;
use crate::metadata::Metadata;
use crate::store::Store;
use crate::traits::Shape;
use crate::u48::U48;
use crate::wire::{WaveWire, Wire, from_wire, to_wire};

// The plan-time read view and the minting half live in their own modules;
// re-exported so the established `crate::record::*` paths still resolve.
pub(crate) use crate::mint::{
    archive_id, keyed_id, mint_floored_id, mint_instant,
};
pub(crate) use crate::overlay::Overlay;

/// Bytes before the wire body: the `STRUCT_HASH` head.
const ENVELOPE_PREFIX: usize = 8;

/// Bytes before a record's `Metadata`: the head plus the `meta_len` slot.
const RECORD_PREFIX: usize = ENVELOPE_PREFIX + 4;

/// Serialise a value as a stored record: `[hash (8 B LE)][WaveWire bytes]`.
pub(crate) fn encode_envelope<V: crate::wire::WaveWire>(
    hash: u64,
    value: &V,
) -> Vec<u8> {
    let mut out = hash.to_le_bytes().to_vec();
    out.extend_from_slice(&to_wire(value));
    out
}

/// Decode a stored record, verifying its `STRUCT_HASH` head first.
///
/// # Errors
/// [`Error::UnknownStructHash`] if the head is not `hash` (or the value is
/// shorter than the head); [`Error::Wire`] if the body fails to decode.
pub(crate) fn decode_envelope<V: crate::wire::WaveWire>(
    hash: u64,
    bytes: &[u8],
) -> Result<V> {
    let head: [u8; ENVELOPE_PREFIX] = bytes
        .get(..ENVELOPE_PREFIX)
        .and_then(|s| s.try_into().ok())
        .ok_or(Error::UnknownStructHash(0))?;
    let got = u64::from_le_bytes(head);
    if got != hash {
        return Err(Error::UnknownStructHash(got));
    }
    Ok(from_wire::<V>(&bytes[ENVELOPE_PREFIX..])?)
}

// ---- Record envelope (Metadata-carrying) ----------------------------------------

/// Serialise a user record:
/// `[hash (8 B LE)][meta_len (u32 LE)][Metadata][body]`.
pub(crate) fn encode_record<V: WaveWire>(
    hash: u64,
    meta: &Metadata,
    value: &V,
) -> Vec<u8> {
    encode_record_raw(hash, meta, &to_wire(value))
}

/// [`encode_record`] over already-encoded body bytes — what archiving uses to
/// move a superseded version without decoding it.
pub(crate) fn encode_record_raw(
    hash: u64,
    meta: &Metadata,
    body: &[u8],
) -> Vec<u8> {
    let meta_bytes = to_wire(meta);
    let mut out =
        Vec::with_capacity(RECORD_PREFIX + meta_bytes.len() + body.len());
    out.extend_from_slice(&hash.to_le_bytes());
    out.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&meta_bytes);
    out.extend_from_slice(body);
    out
}

/// Split a stored record into its decoded [`Metadata`] and its raw body
/// bytes, verifying the `STRUCT_HASH` head first.
///
/// # Errors
/// [`Error::UnknownStructHash`] on a head mismatch (or a value shorter than
/// the record prefix); [`Error::Wire`] if the metadata fails to decode.
pub(crate) fn split_record(
    hash: u64,
    bytes: &[u8],
) -> Result<(Metadata, &[u8])> {
    let head: [u8; ENVELOPE_PREFIX] = bytes
        .get(..ENVELOPE_PREFIX)
        .and_then(|s| s.try_into().ok())
        .ok_or(Error::UnknownStructHash(0))?;
    let got = u64::from_le_bytes(head);
    if got != hash {
        return Err(Error::UnknownStructHash(got));
    }
    let len_bytes: [u8; 4] = bytes
        .get(ENVELOPE_PREFIX..RECORD_PREFIX)
        .and_then(|s| s.try_into().ok())
        .ok_or(Error::Wire(wavedb_wire::Error::UnexpectedEof))?;
    let meta_end = RECORD_PREFIX
        .checked_add(u32::from_le_bytes(len_bytes) as usize)
        .filter(|&end| end <= bytes.len())
        .ok_or(Error::Wire(wavedb_wire::Error::UnexpectedEof))?;
    let meta = from_wire::<Metadata>(&bytes[RECORD_PREFIX..meta_end])?;
    Ok((meta, &bytes[meta_end..]))
}

/// A stored record as its [`Metadata`] and a [`Wire`] over its body — the
/// record's own buffer, kept: the body is read by viewing it (RFC 0076),
/// and only the head and the metadata are checked here.
///
/// # Errors
/// As [`split_record`].
pub(crate) fn wire_record<V: WaveWire>(
    hash: u64,
    bytes: Vec<u8>,
) -> Result<(Metadata, Wire<V>)> {
    let (meta, body) = split_record(hash, &bytes)?;
    let start = bytes.len() - body.len();
    Ok((meta, Wire::new(bytes.into_boxed_slice(), start)))
}

/// Decode a stored record into its [`Metadata`] and typed body.
///
/// # Errors
/// As [`split_record`], plus [`Error::Wire`] on an undecodable body.
pub(crate) fn decode_record<V: WaveWire>(
    hash: u64,
    bytes: &[u8],
) -> Result<(Metadata, V)> {
    let (meta, body) = split_record(hash, bytes)?;
    Ok((meta, from_wire::<V>(body)?))
}

// ---- The version chain -----------------------------------------------------------

pub(crate) use crate::record_save::{
    SavePlan, plan_chained_save, plan_chained_save_over,
};

/// Stream a record's versions **newest-first**: the live record, then each
/// archived version at the slot derived from `previous` — the chain is
/// instants, addresses are computed.
pub(crate) fn history_stream<'a, V, S>(
    store: &'a S,
    hash: u64,
    shape: Shape,
    live_id: Id,
    tenant: U48,
) -> impl futures::Stream<Item = Result<(Metadata, Wire<V>)>> + 'a
where
    V: WaveWire + 'a,
    S: Store,
{
    futures::stream::unfold(Some(live_id), move |next| async move {
        let id = next?;
        let bytes = match store.get_of(hash, id).await {
            Ok(Some(b)) => b,
            Ok(None) => return Some((Err(Error::RecordMissing(id)), None)),
            Err(e) => return Some((Err(e), None)),
        };
        match wire_record::<V>(hash, bytes) {
            Ok((meta, value)) => {
                let older =
                    meta.previous.map(|t| archive_id(hash, shape, t, tenant));
                Some((Ok((meta, value)), older))
            }
            Err(e) => Some((Err(e), None)),
        }
    })
}

// ---- Unique anchors -----------------------------------------------------------

// Split out for the file budget; re-exported so the established
// `crate::record::*` paths (collection, handle, expose) still resolve.
pub use crate::record_unique::{
    adopt_unique, get_unique, save_unique, save_unique_as, unique_history,
};
