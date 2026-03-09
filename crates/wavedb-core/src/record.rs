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

use crate::error::{Error, Result};
use crate::id::Id;
use crate::local_id::LocalId;
use crate::metadata::{Metadata, Succession};
use crate::store::{Store, Write};
use crate::traits::Shape;
use crate::u48::U48;
use crate::wire::{WaveWire, Wire, from_wire, to_wire};

// The plan-time read view and the minting half live in their own modules;
// re-exported so the established `crate::record::*` paths still resolve.
pub(crate) use crate::mint::{
    archive_id, keyed_id, mint_floored_id, mint_instant, type_salt,
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

/// One chained save's addressing and authorship, bundled for
/// [`plan_chained_save`].
pub(crate) struct SavePlan {
    /// The saved type's `STRUCT_HASH`.
    pub hash: u64,
    /// Its shape — picks the archive namespace's flipped `FLAG`.
    pub shape: Shape,
    /// The shape's anchor the live record sits at.
    pub live_id: Id,
    pub tenant: U48,
    /// Stamped as `Metadata.user`.
    pub user: U48,
    /// The pivot back-link a **first** version is stamped with (a later
    /// save carries the existing one forward).
    pub pivot_id: Option<LocalId>,
    /// A node-authoritative [`Metadata`] to write **verbatim** instead of
    /// authoring one — the cache-mirror adopt path, so a mirrored record
    /// carries the node's chain data and the mirror's archives land at the
    /// node's own derived slots. `None` = author a fresh version here.
    pub imposed: Option<Metadata>,
    /// The collection's persisted instant watermark (its recency/dead
    /// maxima) an authored version must land strictly above — `0` for a
    /// `Unique` (no collection; its chain guard alone keeps it walkable).
    pub floor: u64,
    /// Whether this write deliberately makes a **non-living** anchor live
    /// again, clearing [`Metadata::removed`].
    ///
    /// `false` for an ordinary save, which carries the removal forward: writing
    /// to a removed anchor must not silently resurrect it, or the record's own
    /// metadata would claim to be alive while every index still says otherwise.
    /// `true` only on the path that re-indexes it too — a `#[wavedb::key]`
    /// upsert at a dead anchor, or a mirror adopting that revival.
    pub revives: bool,
}

/// Plan a chained save of `value` at the shape's anchor: archive the
/// superseded version (when one exists) at its **derived slot**, stamp
/// its forward link, and write the new live record — all as `Write`s for
/// one atomic batch. Returns the writes, the superseded version's
/// authoring instant and decoded value (`None` on a first save) for the
/// caller's own needs (secondary and recency re-keying), and the new live
/// version's [`Metadata`] (what `note_mutation` carries to watchers).
///
/// The batch opens with a [`Write::Expect`] guard: two concurrent saves of
/// one anchor would derive the **same** archive slot, and the loser's
/// commit would overwrite history — the guard refuses it as a typed
/// [`Error::Conflict`] instead (the caller re-plans against the new live
/// version).
///
/// # Errors
/// Propagates a [`Store`] failure or a decode fault on the existing
/// record; [`Error::ChainCorrupt`] when the record at the anchor carries
/// an archive's `Next` (or an imposed metadata does).
pub(crate) async fn plan_chained_save<V: WaveWire, S: Store>(
    store: &S,
    plan: &SavePlan,
    value: &V,
) -> Result<(Vec<Write>, Option<(u64, V)>, Metadata)> {
    let Some(old_bytes) = store.get_of(plan.hash, plan.live_id).await? else {
        // First version: nothing to archive. The guard still rides along —
        // a concurrent first save must conflict, not silently supersede.
        let meta = plan.imposed.clone().unwrap_or_else(|| Metadata {
            succession: Succession::CreatedAt(mint_instant(plan.floor)),
            pivot_id: plan.pivot_id,
            user: plan.user,
            ..Metadata::default()
        });
        let writes = vec![
            Write::Expect(plan.hash, plan.live_id, None),
            Write::Put(plan.live_id, encode_record(plan.hash, &meta, value)),
        ];
        return Ok((writes, None, meta));
    };
    let (old_meta, old_body) = split_record(plan.hash, &old_bytes)?;
    let old_value = from_wire::<V>(old_body)?;
    let Succession::CreatedAt(authored) = old_meta.succession else {
        return Err(Error::ChainCorrupt(plan.live_id));
    };

    // The new version's instant: imposed metadata rules verbatim (the node
    // authored it); an authored-here save stays monotone within the chain
    // (equal instants would collapse two archive slots into one) and above
    // the collection floor, even against a stalled or rewound clock.
    let live_meta = plan.imposed.clone().unwrap_or_else(|| Metadata {
        previous: Some(authored),
        succession: Succession::CreatedAt(mint_instant(
            authored.max(plan.floor),
        )),
        pivot_id: old_meta.pivot_id,
        user: plan.user,
        device_created: 0,
        permission: old_meta.permission.clone(),
        // A removal carries forward unless this write is the one that
        // re-indexes the anchor into the living set.
        removed: !plan.revives && old_meta.removed,
    });
    let Succession::CreatedAt(instant) = live_meta.succession else {
        return Err(Error::ChainCorrupt(plan.live_id));
    };

    let mut writes = vec![Write::Expect(
        plan.hash,
        plan.live_id,
        Some(old_bytes.clone()),
    )];
    // Archive the superseded version byte-for-byte at its derived slot,
    // its forward link stamped with the successor's instant — written once,
    // correct forever (the successor's own archival derives the same slot
    // from the same value). A mirror whose local state does not precede the
    // imposed instant skips the archive: its stale bytes are not a version
    // the node's chain knows.
    if authored < instant {
        let slot = archive_id(plan.hash, plan.shape, authored, plan.tenant);
        let archived = Metadata {
            succession: Succession::Next(instant),
            ..old_meta
        };
        writes.push(Write::Put(
            slot,
            encode_record_raw(plan.hash, &archived, old_body),
        ));
    }
    writes.push(Write::Put(
        plan.live_id,
        encode_record(plan.hash, &live_meta, value),
    ));
    Ok((writes, Some((authored, old_value)), live_meta))
}

/// Stream a record's versions **newest-first**: the live record, then each
/// archived version at the slot derived from `previous` — the chain is
/// instants, addresses are computed.
pub(crate) fn history_stream<'a, V, S>(
    store: &'a S,
    hash: u64,
    shape: Shape,
    live_id: Id,
    tenant: U48,
) -> impl futures::Stream<Item = Result<(Metadata, V)>> + 'a
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
        match decode_record::<V>(hash, &bytes) {
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
