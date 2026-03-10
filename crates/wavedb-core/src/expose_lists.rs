//! The declared-list wire reads — the engine behind [`Command::Listed`] and
//! [`Command::ListLen`], split from [`crate::expose`] for the file budget the
//! way [`crate::expose_changes`] is.
//!
//! A `#[wavedb::list]` is the only structure that stores records densely in a
//! declared order ([RFC 0051]), and a rendered page is one segment read
//! ([RFC 0052]) — but until these two commands existed, all of that was
//! reachable only from a `LocalHandle` or a `#[server]` body. The thing that
//! renders the page is exactly the thing that could not ask for it.
//!
//! ## Why these are buffered and `search_by` still is not
//!
//! [`all_values`](crate::expose::all_values) buffers a whole collection
//! because the POST tunnel answers one request with one response; that is a
//! compromise waiting on streaming frames, and a `search_by` range — unbounded
//! by nature — is waiting with it.
//!
//! A list page is not waiting on anything: `limit` is the caller's page size,
//! so the answer is bounded **by construction**. The rule the reply carries is
//! the ordinary pager one — exactly `limit` entries means there may be more, a
//! shorter answer is the end — and it is the client that chose `limit`, so it
//! is never guessing about truncation.
//!
//! The limit is deliberately **not capped**. `All` buffers an entire
//! collection with no bound at all, so capping the narrower command would
//! protect nothing while making one command lie about what it served; a
//! node-wide read budget belongs to the M8 gates, not to one op.
//!
//! [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
//! [RFC 0052]: https://github.com/wavedb/wavedb/blob/main/rfcs/0052-segment-size-as-the-pagination-unit.md

use futures::{StreamExt, TryStreamExt};

use crate::collection::Collection;
use crate::error::Result;
use crate::expose::Reply;
use crate::expose_items::framed;
use crate::id::Id;
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::store::Store;
use crate::traits::NonUniqueStruct;
use crate::u48::U48;
use crate::wire::Wire;

/// One page of declared list `index`, ascending from `offset`, at most
/// `limit` records — the shared tail of the generated `Listed` step.
///
/// Each entry is the wire triple `(Id, Metadata, T)`, the same shape
/// [`all_values`](crate::expose::all_values) ships, so a client mirrors the
/// page into its cache under the node's identity and the node's chain data.
///
/// A `limit` of zero yields nothing — an empty page is a legitimate request,
/// not an error, and answering it costs no descent.
///
/// # Errors
/// [`Error::ListOutOfRange`](crate::Error::ListOutOfRange) for an undeclared
/// index; otherwise a [`Store`] failure or a decode fault while walking.
pub async fn listed_values<T, S>(
    store: &S,
    pivot: LocalId,
    tenant: U48,
    index: usize,
    offset: u64,
    limit: u32,
) -> Result<Reply>
where
    T: NonUniqueStruct,
    S: Store,
{
    let page: Vec<(Id, Metadata, Wire<T>)> = Collection::<T>::at(pivot, tenant)
        .listed_meta_at(store, index, offset)
        .take(limit as usize)
        .try_collect()
        .await?;
    Ok(Reply::Values(
        page.into_iter()
            .map(|(id, meta, body)| framed(&(id, meta), body))
            .collect(),
    ))
}

/// How many living records declared list `index` holds — one cold read, since
/// the sparse index's root carries the sum ([RFC 0052]).
///
/// [RFC 0052]: https://github.com/wavedb/wavedb/blob/main/rfcs/0052-segment-size-as-the-pagination-unit.md
///
/// # Errors
/// [`Error::ListOutOfRange`](crate::Error::ListOutOfRange) for an undeclared
/// index, or a [`Store`] failure.
pub async fn list_len_value<T, S>(
    store: &S,
    pivot: LocalId,
    tenant: U48,
    index: usize,
) -> Result<Reply>
where
    T: NonUniqueStruct,
    S: Store,
{
    let total = Collection::<T>::at(pivot, tenant)
        .list_len(store, index)
        .await?;
    Ok(Reply::Count(total))
}
