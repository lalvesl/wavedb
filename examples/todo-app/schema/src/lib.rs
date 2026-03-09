//! The todo-app schema crate — compiled into the node AND every client.
//!
//! The M4 target surface: the app's whole wire API is its nine `#[server]`
//! functions; **every struct is storage-only** (`store` entries — engine
//! slots, no wire address). The patterns this pins down:
//!
//! - a global username registry at the **system tenant** (0): a Unique
//!   anchor holding the `PivotId` of a NonUnique `UserEntry` collection with
//!   a secondary index on `username`, lazily bootstrapped;
//! - server-side **cross-tenant writes** via `db.as_tenant(..)` (`register`
//!   bootstraps `Auth` + `Profile` + the todo collection in the new tenant's
//!   space) — a seam no client command can reach;
//! - the **profile→pivot path**: every todo function re-derives the
//!   collection from `Profile::get(db)`; a `PivotId` never crosses the wire.
//!
//! Auth here is a placeholder (sha256 + timestamp token) — real tokens and
//! the permission gates are M8.
//!
//! **Sides.** One source, two builds: the macros gate `#[server]` bodies +
//! `expose_server!` under the `server-side` feature and the client stubs +
//! `expose_client!` under `client-side` (both on by default for the tests
//! here; the server/client binaries each pull only their side). Helpers
//! that exist *outside* `#[server]` bodies but serve them — everything
//! under "Private helpers" below — carry `#[cfg(feature = "server-side")]`
//! by hand: that is the schema author's half of the no-leak contract.

// The DbHandle-generic helpers hold `&D` across awaits: their futures are
// only `Send` when the context is — the workspace stance on every
// Store-generic seam.
#![allow(clippy::future_not_send)]

use wavedb::prelude::*;

// ── Exposure: what each side actually serves / can call ───────────────────
//
// The lists ARE the registry. Only the functions are wire-reachable; the
// `store` entries register each type's engine slots so the node can open its
// `PageStore` — a client command naming any struct hash is refused as
// unknown, indistinguishable from a type that never existed.

wavedb::expose_server! {
    fn register, fn login, fn refresh, fn logout,
    fn add_todo, fn all_todos, fn search_todos, fn complete_todo, fn delete_todo,
    store AllUserNamesToTenants,
    store UserEntry,
    store Auth,
    store Profile,
    store Todo,
    store wavedb::auth::AuthSession,
    store wavedb::auth::AuthSessions,
}

wavedb::expose_client! {
    fn register, fn login, fn refresh, fn logout,
    fn add_todo, fn all_todos, fn search_todos, fn complete_todo, fn delete_todo,
}

// ── Global username registry (system tenant = 0) ──────────────────────────

/// Unique registry record that lives at the system tenant (0). Holds the
/// `PivotId` of the entire username→tenant collection.
#[wavedb]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllUserNamesToTenants {
    pub entries: <UserEntry as WaveDbStruct>::PivotId,
}

/// One record per registered user. The secondary index on `username` gives
/// the O(log n) lookup `register`/`login` need.
#[wavedb(NonUnique)]
#[wavedb::pivot(username)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UserEntry {
    pub username: String,
    pub tenant_id: u64,
}

// ── Per-tenant records ─────────────────────────────────────────────────────

/// Auth — Unique, one per tenant. Placeholder hash (real Argon2 later);
/// sessions live in `wavedb::auth` records now.
#[wavedb]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Auth {
    pub password_hash: String,
}

/// Profile — Unique, one per tenant. Owns the todo collection handle.
#[wavedb]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub username: String,
    pub todos: <Todo as WaveDbStruct>::PivotId,
}

/// Todo item — NonUnique, many per tenant.
///
/// `#[wavedb::fuzzy]` on `title` adds an n-gram posting tree beside the
/// record ([RFC 0056]), which is what makes [`search_todos()`] find
/// `"Buy milk"` from a typed-out `"mlk"`. It sits on the **field**: the index is
/// built over exactly that string.
///
/// The cost is honest and worth stating next to the declaration: an insert
/// writes `L + n - 1` posting keys (a 20-character title is ~22), all inside
/// the same atomic batch as the record. What it does *not* cost is a save
/// that leaves the title alone — a posting holds a gram, a length and an
/// anchor, never the record, so an unchanged title has nothing to rewrite.
/// `complete_todo` is exactly that save, and it touches this index not at all.
///
/// [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md
#[wavedb(NonUnique)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Todo {
    #[wavedb::fuzzy]
    pub title: String,
    pub completed: bool,
}

/// One [`search_todos()`] result: the todo, its stable `Id` so the caller can
/// act on it, and how well it matched.
///
/// The `Id` is here because a search result is the one read you almost always
/// want to *do* something with — `complete_todo(db, hit.id)` is the point.
/// It is **not** a DTO, and the distinction matters here: there is no
/// translation layer, no second definition of `Todo`. This is a shaped
/// *return* — the same pattern `wavedb::TokenPair` uses — and it carries the
/// real record inside it.
#[derive(Debug, Clone, PartialEq, wavedb_core::WaveWire)]
pub struct TodoHit {
    pub id: Id,
    pub todo: Todo,
    /// How much of the query appears in the title, 0.0…1.0 — higher is closer.
    pub score: f64,
}

/// A `#[server]` return has to carry a signature tag, so that changing any
/// type in the signature **renames the function** and a stale client fails
/// the header gate instead of mis-decoding bytes.
///
/// Composed from `Todo`'s own `STRUCT_HASH` rather than fixed: this shape
/// exists to carry a `Todo`, so editing `Todo` must rename `search_todos`
/// too. (`TokenPair` uses a fixed tag because its shape belongs to the
/// platform, not to any app schema — the opposite case.)
impl wavedb_core::FnArgTag for TodoHit {
    const TAG: u64 = wavedb_core::fn_identity::compose(
        0x0054_6F64_6F48_6974, // "TodoHit"
        &[<Todo as WaveDbStruct>::STRUCT_HASH],
    );
}

// ── Todo server functions (called on the user's tenant connection) ────────

/// Add a new todo. Returns the stable record `Id`.
#[server]
pub async fn add_todo(db: &Db, title: String) -> Result<Id> {
    let profile = get_profile(db).await?;
    Todo::collection(profile.todos)
        .insert(
            db,
            &Todo {
                title,
                completed: false,
            },
        )
        .await
}

/// Every todo, **most recently changed first** — an async iterator streamed
/// item-by-item over the wire (there is no query DSL; filtered/derived reads
/// are functions like this).
///
/// The order is the record chain's (RFC 0050): a completed todo moves to the
/// front, because the chain is ordered by when each record was last written.
#[server]
pub fn all_todos(db: &Db) -> impl Stream<Item = Result<Todo>> {
    async_profile_todos(db)
}

/// The stream behind [`all_todos`]: resolve the profile, then walk its
/// collection — one `try_stream`-free composition over the handle.
#[cfg(feature = "server-side")]
fn async_profile_todos<D: DbHandle<Error = Error>>(
    db: &D,
) -> impl Stream<Item = Result<Todo>> {
    futures::stream::once(get_profile(db))
        .map(move |profile| match profile {
            Ok(p) => Todo::collection(p.todos).all(db).left_stream(),
            Err(e) => {
                futures::stream::once(std::future::ready(Err(e))).right_stream()
            }
        })
        .flatten()
}

/// Todos whose title approximately matches `query`, best first.
///
/// This is what "filtered reads are `#[server]` functions" looks like when the
/// filter is a *fuzzy* one. There is no query DSL and the client never names a
/// struct — it calls this, and the node does the work next to the data:
///
/// ```text
/// search_todos(&db, "by mlik".into(), 5)  →  [ TodoHit { "Buy milk", 0.4… } ]
/// ```
///
/// `Fuzzy::contains(t)` is the type-ahead mode: "at least this fraction of
/// what you typed appears in the title". It is **asymmetric**, which is what
/// makes a short query work against a long title — `"milk"` scores 0.67
/// against `"Buy milk"` and would score the same against
/// `"Buy milk before the shop closes"`.
///
/// The other two modes answer different questions off the same postings:
/// `Fuzzy::similarity(t)` is symmetric Jaccard ("are these two strings
/// alike?" — right for "did someone already add this?"), and
/// `Fuzzy::distance(k)` is exact edit distance ("within k typos").
///
/// **Ranked, therefore buffered** — unlike [`all_todos`], which streams. A
/// best-first order is not known until the last candidate has been scored, so
/// there is nothing honest to emit early.
#[server]
pub async fn search_todos(
    db: &Db,
    query: String,
    limit: u32,
) -> Result<Vec<TodoHit>> {
    let profile = get_profile(db).await?;
    // 0.3: a whole word matches around 0.5–0.7 and one dropped letter still
    // clears 0.4, while an unrelated query lands at 0.0. Tuning this is the
    // app's call — it is the knob between "forgiving" and "noisy".
    let hits = Todo::collection(profile.todos)
        .fuzzy_title(db, &query, Fuzzy::contains(0.3), limit as usize)
        .await?;
    Ok(hits
        .into_iter()
        .map(|hit| TodoHit {
            id: hit.item.0,
            todo: hit.item.1,
            score: hit.score,
        })
        .collect())
}

/// Mark a todo completed (the old version stays on the history chain).
#[server]
pub async fn complete_todo(db: &Db, id: Id) -> Result<()> {
    let profile = get_profile(db).await?;
    let col = Todo::collection(profile.todos);
    let mut todo: Todo = col
        .get(db, id)
        .await?
        .ok_or_else(|| Error::not_found("todo not found"))?
        .try_into()?;
    todo.completed = true;
    col.save(db, id, &todo).await
}

/// Remove a todo (moved to the dead tree — bytes kept, history navigable).
#[server]
pub async fn delete_todo(db: &Db, id: Id) -> Result<()> {
    let profile = get_profile(db).await?;
    Todo::collection(profile.todos).remove(db, id).await?;
    Ok(())
}

#[cfg(feature = "server-side")]
mod helpers;
#[cfg(feature = "server-side")]
use helpers::get_profile;

pub mod auth_fns;
pub use auth_fns::{login, logout, refresh, register};
