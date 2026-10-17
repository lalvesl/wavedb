//! `wavedb` — the user-facing client library.
//!
//! The one dependency an application's schema crate and clients need: it
//! re-exports the `#[wavedb]` / `#[server]` derives and the `expose_*!`
//! declaration macros, the core value types, and the [`Db`] handle with its
//! typed CRUD.
//!
//! ```text
//! use wavedb::prelude::*;
//!
//! let db = Db::connect("127.0.0.1:7700", 42.into(), 42.into()).await?;
//!
//! // Unique: one record per tenant; save is an upsert.
//! let profile: Option<AboutUser> = db.get::<AboutUser>().await?;
//! db.save(&AboutUser { city: "Lisbon".into() }).await?;
//!
//! // NonUnique: open the collection from a stored handle, then mutate.
//! let orders = db.collection::<Order>(user.orders);
//! let id = orders.insert(Order { amount: 120 }).await?;
//! let one = orders.get(id).await?;
//! ```
//!
//! **M4 scope.** The transport is HTTP POST (`wavedb-net`); there is no local
//! write-through cache yet (`Db::open`, M6). The typed surface is the
//! generated one — `T::get(&db)` / `value.save(&db)` for Unique,
//! `T::collection(pivot)` + `col.insert(&db, v)` for NonUnique — resolved
//! through the [`DbHandle`](wavedb_core::DbHandle) this crate implements for
//! [`Db`] (command frames) and [`ServerDb`] (node operations, each run at its
//! owner). Walk-shaped
//! reads are buffered under the hood; streaming frames are a transport
//! refinement that won't change the call sites.

// Typed calls hold `&Db` across awaits: their futures are only `Send` when the
// transport is, which the current-thread client path never requires. The typed
// object traits use `async fn` deliberately — a public but internal-shaped
// seam, not a `Send`-bounded API. Same stance the core engine seams take.
#![allow(clippy::future_not_send, async_fn_in_trait)]

pub mod auth;
pub mod cache;
mod client_cache;
mod client_handle;
mod client_lists;
mod db;
mod error;
mod offline_queue;
mod reply;
mod server_db;
mod server_reply;
pub mod watch;

pub use auth::TokenPair;
pub use cache::CacheStore;
#[cfg(target_arch = "wasm32")]
pub use cache::IdbStore;
pub use db::Db;
pub use error::{Error, Result};
pub use server_db::ServerDb;
pub use watch::{CollectionWatch, UniqueWatch, WatchEvent};

// The compile-time front door, re-exported so a schema crate names one
// dependency: `wavedb::wavedb` / `wavedb::server` / `wavedb::expose_*!`.
pub use wavedb_macros::{expose_client, expose_server, server, wavedb};

/// Everything an application touches, in one glob.
pub mod prelude {
    pub use crate::{
        CollectionWatch, Db, Error, Result, ServerDb, UniqueWatch, WatchEvent,
    };

    // The declaration + object macros (one import surface for schema crates).
    pub use wavedb_macros::{expose_client, expose_server, server, wavedb};

    // Core value types and the traits generated code and app code name —
    // `DbHandle` in particular, so `db.as_tenant(..)` and the generated
    // methods' bounds resolve from the one glob import.
    pub use wavedb_core::{
        CollectionHandle, DbHandle, Id, LocalId, Metadata, NonUniqueStruct,
        PermissionRef, PivotHandle, U48, UniqueStruct, WaveDbStruct, WaveWire,
    };

    // What a `#[wavedb::fuzzy]` lookup takes and yields (RFC 0056).
    pub use wavedb_core::fuzzy::{Fuzzy, Scored};

    // Collection iterators are async streams.
    pub use futures::{Stream, StreamExt, TryStreamExt};
}
