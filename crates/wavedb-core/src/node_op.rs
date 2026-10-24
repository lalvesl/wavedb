//! Node operations: what a `#[server]` body asks of the engine, as data
//! (RFC 0067 step 2).
//!
//! A server body coordinates; it does not execute. Every operation it runs
//! belongs to an [`Owner`] — a collection's Pivot, or a Unique type under a
//! tenant — and runs **where that owner lives, under that owner's lock**:
//! on another thread more often than not. So the body hands each operation
//! to a [`Route`](crate::Route) as a [`NodeOp`], and the route brings back a [`NodeReply`].
//!
//! Both are plain data. Record bodies travel as their wire bytes, because a
//! job crossing a thread cannot be generic over the type without `dyn`; the
//! receiving side picks the type back up from the `STRUCT_HASH` through the
//! registry's generated match ([`Exposure::run_node`]), and the typed
//! execution is [`run_unique`] / [`run_nonunique`].
//!
//! Nothing here is on the network: a node operation is reachable only from
//! node code, so it serves `store` types too and runs no client gate — the
//! node is the authority, not a principal being checked.
//!
//! [`Exposure::run_node`]: crate::expose::Exposure::run_node

use futures::{StreamExt, TryStreamExt};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::fuzzy::{Fuzzy, Scored};
use crate::handle::DbHandle;
use crate::id::Id;
use crate::index::Bound;
use crate::local_handle::LocalHandle;
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::placement::Owner;
use crate::store::Store;
use crate::traits::{NonUniqueStruct, UniqueStruct, WaveDbStruct};
use crate::u48::U48;
use crate::wire::{from_wire, to_wire};

/// One operation of a [`DbHandle`], as data. Bodies are wire bytes.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeOp {
    GetUnique,
    SaveUnique(Vec<u8>),
    UniqueHistory,
    /// Create a collection at a pivot minted by the caller
    /// ([`Collection::mint_pivot`]), so the owner is known before it exists.
    CreatePivot(LocalId),
    Insert {
        pivot: LocalId,
        body: Vec<u8>,
    },
    GetRecord {
        pivot: LocalId,
        id: Id,
    },
    Update {
        pivot: LocalId,
        id: Id,
        body: Vec<u8>,
    },
    Remove {
        pivot: LocalId,
        id: Id,
    },
    All {
        pivot: LocalId,
    },
    SearchBy {
        pivot: LocalId,
        index: usize,
        bound: Bound,
    },
    /// A declared list from `offset`; `limit` bounds a page.
    Listed {
        pivot: LocalId,
        index: usize,
        offset: u64,
        limit: Option<u32>,
    },
    ListLen {
        pivot: LocalId,
        index: usize,
    },
    Fuzzy {
        pivot: LocalId,
        index: usize,
        query: String,
        mode: Fuzzy,
        limit: usize,
    },
    RecordHistory {
        pivot: LocalId,
        id: Id,
    },
}

impl NodeOp {
    /// The collection this operation names, if it is a collection operation.
    #[must_use]
    pub const fn pivot(&self) -> Option<LocalId> {
        match self {
            Self::GetUnique | Self::SaveUnique(_) | Self::UniqueHistory => None,
            Self::CreatePivot(pivot)
            | Self::Insert { pivot, .. }
            | Self::GetRecord { pivot, .. }
            | Self::Update { pivot, .. }
            | Self::Remove { pivot, .. }
            | Self::All { pivot }
            | Self::SearchBy { pivot, .. }
            | Self::Listed { pivot, .. }
            | Self::ListLen { pivot, .. }
            | Self::Fuzzy { pivot, .. }
            | Self::RecordHistory { pivot, .. } => Some(*pivot),
        }
    }

    /// What this operation on `struct_hash` under `tenant` belongs to — the
    /// same owner a wire command on it would have.
    #[must_use]
    pub const fn owner(&self, tenant: U48, struct_hash: u64) -> Owner {
        match self.pivot() {
            Some(pivot) => Owner::Collection(pivot),
            None => Owner::Unique {
                tenant,
                struct_hash,
            },
        }
    }
}

/// What a [`NodeOp`] produced. Bodies are wire bytes.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeReply {
    Done,
    Value(Option<Vec<u8>>),
    Inserted(Id),
    Removed(bool),
    Count(u64),
    Values(Vec<Vec<u8>>),
    Versions(Vec<(Metadata, Vec<u8>)>),
    Scored(Vec<Scored<(Id, Vec<u8>)>>),
}

/// The refusal for an operation the type's shape does not have.
const fn refuse<T: WaveDbStruct>() -> Result<NodeReply> {
    Err(Error::UnknownStructHash(T::STRUCT_HASH))
}

/// Run `op` on the Unique type `T` — the generated `__wavedb_node` step.
///
/// # Errors
/// The engine's, a body that does not decode, or
/// [`Error::UnknownStructHash`] for a collection operation.
pub async fn run_unique<T, S>(
    store: &S,
    tenant: U48,
    op: NodeOp,
) -> Result<NodeReply>
where
    T: UniqueStruct + 'static,
    S: Store,
{
    let db = LocalHandle::new(store, tenant);
    match op {
        NodeOp::GetUnique => {
            let value = db.get_unique::<T>().await?;
            Ok(NodeReply::Value(value.as_ref().map(to_wire)))
        }
        NodeOp::SaveUnique(body) => {
            db.save_unique(&from_wire::<T>(&body)?).await?;
            Ok(NodeReply::Done)
        }
        NodeOp::UniqueHistory => {
            let versions = db
                .unique_history::<T>()
                .map_ok(|(meta, value)| (meta, to_wire(&value)))
                .try_collect()
                .await?;
            Ok(NodeReply::Versions(versions))
        }
        _ => refuse::<T>(),
    }
}

/// Run `op` on the NonUnique type `T` — the generated `__wavedb_node` step.
///
/// # Errors
/// The engine's, a body that does not decode, or
/// [`Error::UnknownStructHash`] for a Unique operation.
pub async fn run_nonunique<T, S>(
    store: &S,
    tenant: U48,
    op: NodeOp,
) -> Result<NodeReply>
where
    T: NonUniqueStruct + 'static,
    S: Store,
{
    let db = LocalHandle::new(store, tenant);
    let values =
        |items: Vec<T>| NodeReply::Values(items.iter().map(to_wire).collect());
    Ok(match op {
        NodeOp::CreatePivot(pivot) => {
            Collection::<T>::create_at(store, tenant, pivot).await?;
            NodeReply::Done
        }
        NodeOp::Insert { pivot, body } => NodeReply::Inserted(
            db.insert(pivot, &from_wire::<T>(&body)?).await?,
        ),
        NodeOp::GetRecord { pivot, id } => {
            let value = db.get_record::<T>(pivot, id).await?;
            NodeReply::Value(value.as_ref().map(to_wire))
        }
        NodeOp::Update { pivot, id, body } => {
            db.update(pivot, id, &from_wire::<T>(&body)?).await?;
            NodeReply::Done
        }
        NodeOp::Remove { pivot, id } => {
            NodeReply::Removed(db.remove::<T>(pivot, id).await?)
        }
        NodeOp::All { pivot } => {
            values(db.all::<T>(pivot).try_collect().await?)
        }
        NodeOp::SearchBy {
            pivot,
            index,
            bound,
        } => {
            values(db.search_by::<T>(pivot, index, bound).try_collect().await?)
        }
        NodeOp::Listed {
            pivot,
            index,
            offset,
            limit,
        } => values(match limit {
            Some(limit) => {
                db.listed_page::<T>(pivot, index, offset, limit)
                    .try_collect()
                    .await?
            }
            None => db.listed::<T>(pivot, index, offset).try_collect().await?,
        }),
        NodeOp::ListLen { pivot, index } => {
            NodeReply::Count(db.list_len::<T>(pivot, index).await?)
        }
        NodeOp::Fuzzy {
            pivot,
            index,
            query,
            mode,
            limit,
        } => {
            let hits = db
                .fuzzy_search::<T>(pivot, index, &query, mode, limit)
                .await?;
            NodeReply::Scored(
                hits.into_iter()
                    .map(|hit| Scored {
                        item: (hit.item.0, to_wire(&hit.item.1)),
                        score: hit.score,
                        rank: hit.rank,
                    })
                    .collect(),
            )
        }
        NodeOp::RecordHistory { pivot, id } => NodeReply::Versions(
            db.record_history::<T>(pivot, id)
                .map(|v| v.map(|(meta, value)| (meta, to_wire(&value))))
                .try_collect()
                .await?,
        ),
        NodeOp::GetUnique | NodeOp::SaveUnique(_) | NodeOp::UniqueHistory => {
            return refuse::<T>();
        }
    })
}
