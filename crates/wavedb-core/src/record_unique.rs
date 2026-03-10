//! The `Unique`-anchor operations — split from [`crate::record`] for the
//! file budget. A `Unique` type's live record sits at a **fixed anchor**
//! (`KEY = STRUCT_HASH`, `FLAG = 1`, `SALT = 0`) under its tenant; `save` is
//! the upsert and chains history exactly like a NonUnique update.

use crate::error::Result;
use crate::id::Id;
use crate::metadata::Metadata;
use crate::record::{history_stream, plan_chained_save, wire_record};
use crate::store::Store;
use crate::u48::U48;
use crate::wire::Wire;

/// The fixed anchor a `Unique` type's live record sits at under `tenant`.
fn unique_anchor<T: crate::traits::WaveDbStruct>(tenant: U48) -> Id {
    Id::new(T::STRUCT_HASH, tenant, true, 0)
}

/// Fetch a `Unique` record from its anchor (`KEY = STRUCT_HASH`, `FLAG = 1`,
/// `SALT = 0`) under `tenant`, as a [`Wire`] over its body (RFC 0076).
/// `None` = never saved.
///
/// # Errors
/// Propagates a [`Store`] failure, or a head or metadata fault; the body is
/// checked when viewed.
pub async fn get_unique<T, S>(store: &S, tenant: U48) -> Result<Option<Wire<T>>>
where
    T: crate::traits::WaveDbStruct,
    S: Store,
{
    let anchor = unique_anchor::<T>(tenant);
    match store.get_of(T::STRUCT_HASH, anchor).await? {
        Some(bytes) => Ok(Some(wire_record(T::STRUCT_HASH, bytes)?.1)),
        None => Ok(None),
    }
}

/// Save (insert-or-overwrite) a `Unique` record at its anchor under `tenant`.
/// `save` **is** the upsert — `Unique` types have no separate create.
///
/// A save over an existing record archives the superseded version and links
/// the modification chain (the timeline stays walkable via
/// [`unique_history`]); everything commits as one atomic batch.
///
/// # Errors
/// Propagates a [`Store`] failure or a decode fault on the existing record.
pub async fn save_unique<T, S>(store: &S, tenant: U48, value: &T) -> Result<()>
where
    T: crate::traits::WaveDbStruct,
    S: Store,
{
    save_unique_as(store, tenant, tenant, value).await
}

/// [`save_unique`] with authorship: `Metadata.user` is stamped `user` (the
/// verified caller, M8) instead of the tenant.
///
/// # Errors
/// As [`save_unique`].
pub async fn save_unique_as<T, S>(
    store: &S,
    tenant: U48,
    user: U48,
    value: &T,
) -> Result<()>
where
    T: crate::traits::WaveDbStruct,
    S: Store,
{
    let anchor = unique_anchor::<T>(tenant);
    let plan = crate::record::SavePlan {
        hash: T::STRUCT_HASH,
        shape: T::SHAPE,
        live_id: anchor,
        tenant,
        user,
        pivot_id: None,
        imposed: None,
        // No collection, no persisted watermark: the chain guard alone
        // keeps a Unique walkable (catch-up is chain-forward navigation).
        floor: 0,
        revives: false,
    };
    let (writes, _old, live_meta) =
        plan_chained_save::<T, S>(store, &plan, value).await?;
    store.apply(&writes).await?;
    store.note_mutation(|| crate::notify::Mutation {
        struct_hash: T::STRUCT_HASH,
        tenant,
        pivot: None,
        id: anchor,
        kind: crate::notify::MutationKind::Saved,
        meta: Some(live_meta),
        body: crate::wire::to_wire(value),
    });
    Ok(())
}

/// Mirror a node-acknowledged `Unique` version into a **local cache
/// store**, its [`Metadata`] written verbatim.
///
/// The mirror then carries the node's authoritative chain data, and its
/// archives land at the node's own derived slots. Skips entirely when the
/// anchor already holds exactly this version (meta and bytes), so
/// read-path mirroring never grows the store. Never notes a mutation (a
/// mirrored record is not a new one).
///
/// # Errors
/// Propagates a [`Store`] failure or a decode fault on the existing local
/// record.
pub async fn adopt_unique<T, S>(
    store: &S,
    tenant: U48,
    meta: Metadata,
    value: &T,
) -> Result<()>
where
    T: crate::traits::WaveDbStruct,
    S: Store,
{
    let anchor = unique_anchor::<T>(tenant);
    if let Some(bytes) = store.get_of(T::STRUCT_HASH, anchor).await? {
        let (existing, body) =
            crate::record::split_record(T::STRUCT_HASH, &bytes)?;
        if existing == meta && body == crate::wire::to_wire(value) {
            return Ok(());
        }
    }
    let plan = crate::record::SavePlan {
        hash: T::STRUCT_HASH,
        shape: T::SHAPE,
        live_id: anchor,
        tenant,
        user: meta.user,
        pivot_id: None,
        imposed: Some(meta),
        floor: 0,
        revives: false,
    };
    let (writes, _old, _live) =
        plan_chained_save::<T, S>(store, &plan, value).await?;
    store.apply(&writes).await
}

/// Stream a `Unique` record's versions **newest-first** (the live record,
/// then each archived version along the modification chain). Empty when the
/// record was never saved.
pub fn unique_history<'a, T, S>(
    store: &'a S,
    tenant: U48,
) -> impl futures::Stream<Item = Result<(Metadata, Wire<T>)>> + 'a
where
    T: crate::traits::WaveDbStruct + 'a,
    S: Store,
{
    use futures::StreamExt;
    let anchor = unique_anchor::<T>(tenant);
    futures::stream::once(async move {
        store
            .get_of(T::STRUCT_HASH, anchor)
            .await
            .map(|b| b.is_some())
    })
    .flat_map(move |exists| match exists {
        Ok(true) => history_stream::<T, S>(
            store,
            T::STRUCT_HASH,
            T::SHAPE,
            anchor,
            tenant,
        )
        .left_stream(),
        Ok(false) => futures::stream::empty().left_stream().right_stream(),
        Err(e) => futures::stream::once(async move { Err(e) })
            .right_stream()
            .right_stream(),
    })
}
