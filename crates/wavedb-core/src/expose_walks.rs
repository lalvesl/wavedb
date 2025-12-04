//! The buffered walks behind the generated `All` and `History` steps.

use futures::TryStreamExt;

use crate::collection::Collection;
use crate::error::Result;
use crate::expose::Reply;
use crate::expose_items::framed;
use crate::local_id::LocalId;
use crate::record;
use crate::store::Store;
use crate::traits::{NonUniqueStruct, WaveDbStruct};
use crate::u48::U48;
use crate::wire::Wire;

/// Walk a NonUnique collection newest-first and buffer each record
/// as the wire pair `(Id, T)` — the shared tail of the generated `All` step.
///
/// The node-minted `Id` rides with every item so a client can mirror the walk
/// into its local cache under the authoritative identity (M6); the typed
/// surface still yields values only.
///
/// Buffered (not streamed) for now: the HTTP POST tunnel answers one request
/// with one response, so a walk collects before replying. Streaming frames are
/// a later transport refinement.
///
/// # Errors
/// Propagates a [`Store`] failure or a decode fault while walking.
pub async fn all_values<T, S>(
    store: &S,
    pivot: LocalId,
    tenant: U48,
) -> Result<Reply>
where
    T: NonUniqueStruct,
    S: Store,
{
    let col = Collection::<T>::at(pivot, tenant);
    // Each entry is the wire triple `(Id, Metadata, T)`: the node-minted
    // identity and the authoritative chain data ride along so a client mirror
    // adopts them verbatim, where the typed surface yields values alone.
    //
    // Read off the **recency chain**, back from its end, for two reasons. It
    // is the same order `Collection::all` yields — most recently written
    // first — and the two surfaces must not disagree about what "all" means.
    // The chain gives membership and order; each record is then resolved at
    // its anchor, which is where it lives (RFC 0054 — no duplication by
    // default).
    col.check_pivot(store).await?;
    let mut keys = Box::pin(col.recency_chain().scan_back(store));
    let mut items: Vec<Vec<u8>> = Vec::new();
    while let Some((key, ())) = keys.try_next().await? {
        let rec = key.rec.to_id(col.tenant());
        let bytes = store
            .get_of(T::STRUCT_HASH, rec)
            .await?
            .ok_or(crate::Error::RecordMissing(rec))?;
        let (meta, body) =
            crate::record::wire_record::<T>(T::STRUCT_HASH, bytes)?;
        items.push(framed(&(rec, meta), body));
    }
    Ok(Reply::Values(items))
}

/// Walk a `Unique` record's version chain **newest-first**, buffered.
///
/// Each version rides as the wire tuple `(Metadata, T)` — the shared tail of
/// the generated `History` step. Empty when the record was never saved.
///
/// Buffered like [`all_values`]; each `Values` entry carries the version's
/// metadata alongside its body so a remote timeline walk sees the chain.
///
/// # Errors
/// Propagates a [`Store`] failure or a decode fault while walking.
pub async fn unique_history_values<T, S>(
    store: &S,
    tenant: U48,
) -> Result<Reply>
where
    T: WaveDbStruct,
    S: Store,
{
    use futures::TryStreamExt;
    let versions: Vec<(crate::metadata::Metadata, Wire<T>)> =
        record::unique_history::<T, S>(store, tenant)
            .try_collect()
            .await?;
    let entries = versions
        .into_iter()
        .map(|(meta, body)| framed(&meta, body))
        .collect();
    Ok(Reply::Values(entries))
}
