//! The node-first write-through semantics behind [`Db::open`] — what the
//! [`DbHandle`](wavedb_core::DbHandle) impl in [`crate::client_handle`]
//! delegates to when a cache is attached. Three rules:
//!
//! - **Node first, always.** The cache never answers while the node can; a
//!   successful read *is* the revalidation, and its result mirrors into the
//!   local store (skipping unchanged bytes, so read traffic never grows it).
//! - **Mirrors are best-effort.** The node acknowledged the op; a local
//!   cache fault costs warmth, never the op's outcome.
//! - **Absence is not an answer.** A read falls back to the cache only on a
//!   *transport* fault, and only when the cache actually holds the answer —
//!   a cold cache propagates the original error instead of minting an
//!   authoritative-looking `None`. Node refusals ([`Error::Node`]) are
//!   authoritative and never fall back.
//!
//! Mirrors take the cache's turn ([`Turn`](crate::cache::Turn)): each is a
//! read-plan-apply over a store that suspends, and two interleaved would
//! lose an index entry.
//!
//! Offline writes (W8): a Unique `save` is **queued** for node-first replay
//! ([`crate::offline_queue`]) and mirrored locally, so it succeeds
//! provisionally instead of losing the work; the queue drains FIFO when the
//! node returns, so the cache converges without a merge. NonUnique writes
//! (whose ids are node-minted) still refuse for now — a later W8 phase.
//!
//! [`Db::open`]: crate::Db::open
//! [`Error::Node`]: crate::Error::Node

use futures::{Stream, StreamExt, TryStreamExt};
use wavedb_core::expose::Command;
use wavedb_core::index::Pivot;
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_core::{
    Collection, DbHandle, Id, LocalHandle, LocalId, NonUniqueStruct, Store,
    UniqueStruct,
};

use crate::cache::CacheStore;
use crate::db::Db;
use crate::error::{Error, Result};

/// The cache's local handle with its turn taken — what every mirror writes
/// through, one at a time ([`Turn`](crate::cache::Turn)). `None` without a
/// cache.
async fn mirroring(
    db: &Db,
) -> Option<(LocalHandle<'_, CacheStore>, crate::cache::Taken<'_>)> {
    let cache = db.cache()?;
    let turn = cache.turn.take().await;
    Some((LocalHandle::new(&cache.store, db.tenant()), turn))
}

/// The faults that mean "the node could not answer" (fall back), as opposed
/// to "the node answered no" (authoritative).
pub const fn is_transport(err: &Error) -> bool {
    matches!(err, Error::Transport(_))
}

/// Mirror a Unique value the node acknowledged, skipping unchanged bytes.
/// The read paths' mirror — their replies carry no metadata, so the local
/// copy authors its own; the watch path uses [`mirror_unique_meta`].
pub async fn mirror_unique<T: UniqueStruct>(db: &Db, value: &T) {
    let Some((local, _turn)) = mirroring(db).await else {
        return;
    };
    let unchanged = matches!(
        local.get_unique::<T>().await,
        Ok(Some(existing)) if to_wire(&existing) == to_wire(value)
    );
    if !unchanged {
        let _ = local.save_unique(value).await;
    }
}

/// Mirror a Unique version with the node's [`Metadata`] written verbatim —
/// the watch path, whose events carry the authoritative chain data.
///
/// [`Metadata`]: wavedb_core::Metadata
pub async fn mirror_unique_meta<T: UniqueStruct>(
    db: &Db,
    meta: wavedb_core::Metadata,
    value: &T,
) {
    let Some((local, _turn)) = mirroring(db).await else {
        return;
    };
    let _ = wavedb_core::collection::adopt_unique::<T, _>(
        local.store(),
        db.tenant(),
        meta,
        value,
    )
    .await;
}

/// Serve a Unique read from the cache after the transport fault `err`.
pub async fn unique_fallback<T: UniqueStruct>(
    db: &Db,
    err: Error,
) -> Result<Option<T>> {
    if let Some(local) = db.local()
        && let Ok(Some(value)) = local.get_unique::<T>().await
    {
        return Ok(Some(value));
    }
    Err(err)
}

/// Mirror a **living** record the node acknowledged under its node-minted
/// identity: bootstrap the local pivot copy if needed, then adopt. The
/// read/write paths' mirror — their replies carry no metadata; the frames
/// that do (watch events, the `All` walk) use [`mirror_record_meta`].
pub async fn mirror_record<T: NonUniqueStruct>(
    db: &Db,
    pivot: LocalId,
    id: Id,
    value: &T,
) {
    let Some((local, _turn)) = mirroring(db).await else {
        return;
    };
    let (store, tenant) = (local.store(), DbHandle::tenant(&local));
    if Collection::<T>::adopt_pivot(store, tenant, pivot)
        .await
        .is_ok()
    {
        let _ = Collection::<T>::at(pivot, tenant)
            .adopt(store, id, value)
            .await;
    }
}

/// Mirror a living record with the node's [`Metadata`] written verbatim —
/// the local copy then carries authoritative chain data and archives at
/// the node's own derived slots.
///
/// [`Metadata`]: wavedb_core::Metadata
pub async fn mirror_record_meta<T: NonUniqueStruct>(
    db: &Db,
    pivot: LocalId,
    id: Id,
    meta: wavedb_core::Metadata,
    value: &T,
) {
    let Some((local, _turn)) = mirroring(db).await else {
        return;
    };
    let (store, tenant) = (local.store(), DbHandle::tenant(&local));
    if Collection::<T>::adopt_pivot(store, tenant, pivot)
        .await
        .is_ok()
    {
        let _ = Collection::<T>::at(pivot, tenant)
            .adopt_with(store, id, meta, value)
            .await;
    }
}

/// Mirror a removal the node acknowledged (nothing to do when the local
/// collection was never adopted).
pub async fn mirror_remove<T: NonUniqueStruct>(
    db: &Db,
    pivot: LocalId,
    id: Id,
) {
    if let Some((local, _turn)) = mirroring(db).await {
        let _ = local.remove::<T>(pivot, id).await;
    }
}

/// Serve a by-id read from the cache after the transport fault `err`.
pub async fn record_fallback<T: NonUniqueStruct>(
    db: &Db,
    pivot: LocalId,
    id: Id,
    err: Error,
) -> Result<Option<T>> {
    if let Some(local) = db.local()
        && let Ok(Some(value)) = local.get_record::<T>(pivot, id).await
    {
        return Ok(Some(value));
    }
    Err(err)
}

/// The collection walk with the cache in the loop: stream the node's
/// `(Id, Metadata, T)` frames, mirroring each living record (chain data
/// verbatim) as it passes; when the *transport* fails and the local
/// collection copy exists, serve the warm walk instead (same ids, same
/// order — the mirror adopted the node's).
pub fn cached_all<T: NonUniqueStruct + 'static>(
    db: &Db,
    pivot: LocalId,
) -> impl Stream<Item = Result<T>> + '_ {
    futures::stream::once(async move {
        let node = db
            .command_stream(T::STRUCT_HASH, Command::All, to_wire(&pivot))
            .await;
        match node {
            Ok(items) => items
                .and_then(move |bytes| async move {
                    let (id, meta, value) =
                        from_wire::<(Id, wavedb_core::Metadata, T)>(&bytes)
                            .map_err(wavedb_core::Error::from)
                            .map_err(Error::Core)?;
                    mirror_record_meta(db, pivot, id, meta, &value).await;
                    Ok(value)
                })
                .left_stream()
                .left_stream(),
            Err(err) => {
                // Warm only when the fault is transport-shaped AND the local
                // collection copy exists — a cold cache propagates `err`.
                let warm_store = match db.local() {
                    Some(local) if is_transport(&err) => {
                        let store = local.store();
                        store
                            .get_of(
                                <T::Pivot as Pivot>::STRUCT_HASH,
                                pivot.to_id(db.tenant()),
                            )
                            .await
                            .is_ok_and(|bytes| bytes.is_some())
                            .then_some(store)
                    }
                    _ => None,
                };
                warm_store.map_or_else(
                    || {
                        futures::stream::once(std::future::ready(Err(err)))
                            .right_stream()
                    },
                    |store| {
                        Collection::<T>::at(pivot, db.tenant())
                            .all(store)
                            .map_ok(|(_, value)| value)
                            .map_err(Error::Core)
                            .right_stream()
                            .left_stream()
                    },
                )
            }
        }
    })
    .flatten()
}
