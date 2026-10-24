//! `wavedb-core` — primitives shared by every node kind and by proc-macro
//! generated code: the composite [`Id`], the [`U48`] newtype, the [`WaveWire`]
//! serialization format, and the workspace [`Error`]. **No I/O.**
//!
//! See `crates/wavedb-core/README.md` for the design.

// Pedantic/nursery lints that fight terse, byte-precise (de)serialization code.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::cast_sign_loss,
    clippy::missing_const_for_fn
)]
// The `Store` / `BpTree` backend contracts use `async fn` in traits deliberately:
// these are internal seams, not a public Send-bounded API, so the absence of an
// auto `Send` bound is intended, not an oversight. Same stance for the futures
// `BpTree`'s methods return — they are only `Send` when the backing `Store` is.
#![allow(async_fn_in_trait)]
#![allow(clippy::future_not_send)]

// The derive (once it lands in `wavedb-macros`) emits absolute
// `::wavedb_core::` paths; this lets the crate use its own derive.
extern crate self as wavedb_core;

pub mod collection;
mod collection_adopt;
mod collection_fuzzy;
mod collection_fuzzy_read;
pub mod collection_handle;
mod collection_keyed;
mod collection_lists;
mod collection_read;
mod collection_recency;
mod collection_reindex;
mod collection_remove;
mod collection_roots;
mod collection_write;
pub mod error;
pub mod expose;
mod expose_changes;
mod expose_lists;
mod expose_member;
mod expose_salt;
mod expose_walks;
pub mod fn_identity;
pub mod fuzzy;
pub mod handle;
pub mod hooks;
pub mod id;
pub mod index;
pub mod lease;
pub mod local_handle;
pub mod local_id;
pub mod metadata;
mod mint;
pub mod node_op;
pub mod notify;
pub mod overlay;
pub mod permission;
pub mod placement;
pub mod record;
mod record_unique;
pub mod route;
pub mod route_handle;
mod route_reply;
pub mod store;
pub mod traits;
pub mod u48;
pub mod wire;

pub use collection::Collection;
pub use collection_handle::CollectionHandle;
pub use error::{Error, Result};
pub use expose::Caller;
pub use fn_identity::FnArgTag;
pub use handle::DbHandle;
pub use lease::{Leases, Locked};
pub use local_handle::LocalHandle;
pub use node_op::{NodeOp, NodeReply};
pub use route::{LocalRoute, Route};
pub use route_handle::RouteHandle;
// Generated `by_<field>` lookups return `impl Stream`, and a
// stream-returning `#[server]` dispatch collects with `TryStreamExt`;
// re-exported so macro expansions can name them without adding a `futures`
// dep to schema crates.
pub use futures::{Stream, TryStreamExt};
pub use hooks::LookupHooks;
pub use id::Id;
pub use index::{
    Bound, BpTree, ChainRoots, Except, IdStreamExt, IndexKey, Intersect,
    LogRoots, NodeKey, Pivot, SecKey, Union,
};
pub use local_id::LocalId;
pub use metadata::{Metadata, Succession};
// Generated `natural_key` impls derive their anchor through this one fn,
// so every build agrees on the address a key-field value set maps to.
pub use mint::{natural_key_hash, type_salt};
pub use notify::{Mutation, MutationKind};
pub use permission::PermissionRef;
pub use store::{Store, Write};
pub use traits::{
    NonUniqueStruct, PivotHandle, Shape, UniqueStruct, WaveDbStruct,
};
pub use u48::U48;
pub use wire::{Cursor, WaveWire, from_wire, to_wire};
