//! Collection membership — a NonUnique command names its collection, and acts
//! only on records of that collection (RFC 0067 step 2).
//!
//! The command executes where its Pivot lives and under that Pivot's lock, so
//! a record of another collection must answer as if it did not exist.

use crate::error::{Error, Result};
use crate::expose::Reply;
use crate::id::Id;
use crate::local_id::LocalId;
use crate::record;
use crate::store::Store;
use crate::traits::WaveDbStruct;
use crate::wire::to_wire;

/// The record at `id` as its body wire bytes, when it belongs to the
/// collection `pivot` — the NonUnique `Get` step.
///
/// A record of another collection is answered as absent, exactly as if it
/// did not exist: the command named its collection, and the collection is
/// where it executes. Removed records still resolve (history stays
/// navigable): the anchor keeps its bytes and its back-link.
///
/// # Errors
/// Propagates a [`Store`] failure or a decode fault.
pub async fn member_value<T, S>(
    store: &S,
    pivot: LocalId,
    id: Id,
) -> Result<Reply>
where
    T: WaveDbStruct,
    S: Store,
{
    let Some(bytes) = store.get_of(T::STRUCT_HASH, id).await? else {
        return Ok(Reply::Value(None));
    };
    let (meta, value) = record::decode_record::<T>(T::STRUCT_HASH, &bytes)?;
    if meta.pivot_id != Some(pivot) {
        return Ok(Reply::Value(None));
    }
    Ok(Reply::Value(Some(to_wire(&value))))
}

/// Refuse unless the record at `id` belongs to the collection `pivot` — the
/// guard an `Update`/`Remove` step runs before touching the collection.
///
/// The command executes where its Pivot lives and under that Pivot's lock,
/// so a record of another collection must not be reachable through it: it
/// would be changed outside its own collection's lock.
///
/// # Errors
/// [`Error::RecordMissing`] when `id` resolves to nothing or to a record of
/// another collection (indistinguishable on purpose).
pub async fn check_member<T, S>(store: &S, pivot: LocalId, id: Id) -> Result<()>
where
    T: WaveDbStruct,
    S: Store,
{
    let bytes = store
        .get_of(T::STRUCT_HASH, id)
        .await?
        .ok_or(Error::RecordMissing(id))?;
    let (meta, _) = record::split_record(T::STRUCT_HASH, &bytes)?;
    if meta.pivot_id == Some(pivot) {
        Ok(())
    } else {
        Err(Error::RecordMissing(id))
    }
}
