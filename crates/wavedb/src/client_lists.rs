//! Declared-list reads with the cache in the loop — the client half of
//! [`Command::Listed`] / [`Command::ListLen`].
//!
//! Same three rules as [`crate::client_cache`]: node first, mirrors are
//! best-effort, absence is not an answer. What is specific to a list is
//! **paging**.
//!
//! ## Why the unbounded reader pages
//!
//! The wire command is bounded — the caller names a `limit` — while the typed
//! [`listed`](wavedb_core::DbHandle::listed) surface streams to the end of the
//! list. So the unbounded reader asks in chunks and stops when a chunk comes
//! back short, which is the ordinary pager rule and needs no truncation flag:
//! the client chose the limit, so a full page means "there may be more" and a
//! short one is the end.
//!
//! Paging is **not** a snapshot. Between two chunks the list can change, so a
//! record may be seen twice or missed if its ordering property moves under the
//! walk. That is inherent to an offset pager and is the honest cost of reading
//! a live list without holding one; a caller who needs a coherent view of
//! "what changed" wants [`watch_collection`](crate::Db::watch_collection),
//! which is built for exactly that.
//!
//! [`Command::Listed`]: wavedb_core::expose::Command::Listed
//! [`Command::ListLen`]: wavedb_core::expose::Command::ListLen

use futures::{Stream, StreamExt, TryStreamExt};
use wavedb_core::expose::{Command, unframed};
use wavedb_core::index::Pivot;
use wavedb_core::wire::{Wire, to_wire};
use wavedb_core::{Collection, Id, LocalId, Metadata, NonUniqueStruct, Store};

use crate::client_cache::{is_transport, mirror_record_meta};
use crate::db::Db;
use crate::error::{Error, Result};

/// How many records the unbounded reader asks for per round trip.
///
/// Deliberately **not** the list's declared `page`: that number is the segment
/// capacity a rendered page wants, and a type declaring `page = 4` would turn a
/// full walk into a round trip per four records. A pager that does want exactly
/// one declared page asks for it — that is what
/// [`listed_page`](wavedb_core::DbHandle::listed_page) is for.
const CHUNK: u32 = 256;

/// Fetch one page of declared list `index` as its raw framed
/// `(Id, Metadata)` + body items — the single wire exchange both readers are built from.
async fn fetch<T: NonUniqueStruct>(
    db: &Db,
    pivot: LocalId,
    index: usize,
    offset: u64,
    limit: u32,
) -> Result<Vec<Vec<u8>>> {
    let payload = to_wire(&(pivot, index_u32(index), offset, limit));
    // A `Values` reply always unpacks into item frames on the wire, so a page
    // arrives on the streaming path even though it is bounded — the frames are
    // the transport's shape, not a statement about the command's size.
    db.command_stream(T::STRUCT_HASH, Command::Listed, payload)
        .await?
        .try_collect()
        .await
}

/// The wire carries the index as a `u32` (`usize` is never encodable). An
/// index past `u32::MAX` cannot name a declaration, so it saturates into one
/// the node will refuse as out of range rather than wrapping into a valid one.
fn index_u32(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}

/// Decode one page's frames, mirroring each record under the node's identity
/// and chain data as it passes.
async fn absorb<T: NonUniqueStruct>(
    db: &Db,
    pivot: LocalId,
    page: Vec<Vec<u8>>,
) -> Vec<Result<Wire<T>>> {
    let mut out = Vec::with_capacity(page.len());
    for bytes in page {
        out.push(absorb_one::<T>(db, pivot, bytes).await);
    }
    out
}

/// One frame: its record a [`Wire`] over the frame, mirrored — decoded only
/// when there is a cache to mirror into.
async fn absorb_one<T: NonUniqueStruct>(
    db: &Db,
    pivot: LocalId,
    bytes: Vec<u8>,
) -> Result<Wire<T>> {
    let ((id, meta), wire) = unframed::<(Id, Metadata), T>(bytes)?;
    if db.local().is_some() {
        let value = wire.decode()?;
        mirror_record_meta(db, pivot, id, meta, &value).await;
    }
    Ok(wire)
}

/// Stream declared list `index` from `offset` to its end, chunked over the
/// wire and mirrored as it passes; a transport fault on the **first** chunk
/// falls back to the warm local list, the way `cached_all` decides once.
pub fn cached_listed<T: NonUniqueStruct + 'static>(
    db: &Db,
    pivot: LocalId,
    index: usize,
    offset: u64,
) -> impl Stream<Item = Result<Wire<T>>> + '_ {
    futures::stream::once(async move {
        match fetch::<T>(db, pivot, index, offset, CHUNK).await {
            Ok(first) => {
                pages::<T>(db, pivot, index, offset, first).left_stream()
            }
            Err(err) => warm::<T>(db, pivot, index, offset, err).right_stream(),
        }
    })
    .flatten()
}

/// One bounded page of declared list `index` — one wire exchange, no chunking,
/// which is what a pager rendering "rows 50…75 of M" actually wants.
pub fn cached_listed_page<T: NonUniqueStruct + 'static>(
    db: &Db,
    pivot: LocalId,
    index: usize,
    offset: u64,
    limit: u32,
) -> impl Stream<Item = Result<Wire<T>>> + '_ {
    futures::stream::once(async move {
        match fetch::<T>(db, pivot, index, offset, limit).await {
            Ok(page) => {
                futures::stream::iter(absorb::<T>(db, pivot, page).await)
                    .left_stream()
            }
            Err(err) => warm::<T>(db, pivot, index, offset, err)
                .take(limit as usize)
                .right_stream(),
        }
    })
    .flatten()
}

/// The chunk loop: yield the page in hand, then ask for the next one only
/// while the last came back full.
fn pages<T: NonUniqueStruct + 'static>(
    db: &Db,
    pivot: LocalId,
    index: usize,
    offset: u64,
    first: Vec<Vec<u8>>,
) -> impl Stream<Item = Result<Wire<T>>> + '_ {
    futures::stream::try_unfold(
        Some((offset, first)),
        move |state| async move {
            let Some((at, page)) = state else {
                return Ok::<_, Error>(None);
            };
            let served = page.len() as u64;
            let rows = absorb::<T>(db, pivot, page).await;
            // A full page means there may be more; a short one is the end.
            let next = if served == u64::from(CHUNK) {
                let more =
                    fetch::<T>(db, pivot, index, at + served, CHUNK).await?;
                Some((at + served, more))
            } else {
                None
            };
            Ok(Some((futures::stream::iter(rows), next)))
        },
    )
    .try_flatten()
}

/// Serve the list from the local cache after the fault `err` — but only when
/// the fault was transport-shaped **and** the local collection copy exists.
/// A cold cache propagates `err` rather than minting an empty list, which
/// would read as an authoritative "there is nothing here".
fn warm<T: NonUniqueStruct + 'static>(
    db: &Db,
    pivot: LocalId,
    index: usize,
    offset: u64,
    err: Error,
) -> impl Stream<Item = Result<Wire<T>>> + '_ {
    futures::stream::once(async move {
        local_store::<T>(db, pivot, &err).await.map_or_else(
            // A cold cache propagates the fault: an empty list would read as
            // an authoritative "there is nothing here".
            || {
                futures::stream::once(std::future::ready(Err(err)))
                    .right_stream()
            },
            |store| {
                Collection::<T>::at(pivot, db.tenant())
                    .listed_at(store, index, offset)
                    .map_ok(|(_, value)| value)
                    .map_err(Error::Core)
                    .left_stream()
            },
        )
    })
    .flatten()
}

/// How many living records declared list `index` holds, node first.
///
/// # Errors
/// The node's refusal, or a transport fault a cold cache cannot cover.
pub async fn cached_list_len<T: NonUniqueStruct>(
    db: &Db,
    pivot: LocalId,
    index: usize,
) -> Result<u64> {
    let payload = to_wire(&(pivot, index_u32(index)));
    match db.command(T::STRUCT_HASH, Command::ListLen, payload).await {
        Ok(reply) => crate::reply::count(&reply),
        Err(err) => match local_store::<T>(db, pivot, &err).await {
            Some(store) => Collection::<T>::at(pivot, db.tenant())
                .list_len(store, index)
                .await
                .map_err(Error::Core),
            None => Err(err),
        },
    }
}

/// The local store, but only when `err` is transport-shaped **and** the local
/// collection copy exists — the shared "may I answer warm?" test, the same one
/// `cached_all` applies before serving a warm walk.
async fn local_store<'a, T: NonUniqueStruct>(
    db: &'a Db,
    pivot: LocalId,
    err: &Error,
) -> Option<&'a crate::cache::CacheStore> {
    let local = db.local()?;
    if !is_transport(err) {
        return None;
    }
    let store = local.store();
    store
        .get_of(<T::Pivot as Pivot>::STRUCT_HASH, pivot.to_id(db.tenant()))
        .await
        .is_ok_and(|bytes| bytes.is_some())
        .then_some(store)
}
