# Worked example — auth + a typed request

An end-to-end sketch of how authentication and a normal request fit together.
**Illustrative**: the API names follow the design (see
[`wavedb-net` §Authentication](../crates/wavedb-net/README.md#authentication) and
[`wavedb-macros` §Server functions](../crates/wavedb-macros/README.md#server-functions--server)),
but nothing here is implemented yet.

The whole flow in one line each:

> **unauth connect → `login` mints access+refresh + a `Session` record →
> `Db::with_token` → `create`/`get`/server-fn → `refresh` rotates → revoke flips
> `Session.revoked`.**

Every read is `get` / `::all` / a `#[server]` function — there is **no query
DSL**. Every server-function body (`login`, `refresh`, `pinned_notes`) compiles
**only into the node**; the client sees typed stubs that ship `WaveWire`-encoded
arguments over the transport.

`login` / `refresh` are **`#[server(public)]`** — reachable before any token
exists; everything else requires a logged-in session. The auth guard is injected
into the **function body**, not the dispatch `match`, so the registry stays a
uniform `struct_hash → body` router.

```rust
use wavedb::prelude::*;

// ---- schema crate: compiled into client AND node ----------------------------

#[wavedb]                                   // Unique: one per tenant
pub struct AboutUser { pub name: String, pub city: String }

#[wavedb(NonUnique)]                          // many per tenant
pub struct Note { pub body: String, pub pinned: bool }

// Login record — credentials are just data (Unique object per user).
#[wavedb]
pub struct Credentials { pub argon2: String }

// Session record the refresh token is bound to (revocation handle).
#[wavedb]
pub struct Session { pub user: U48, pub tenant: U48, pub issued: u64, pub revoked: bool }

#[derive(WaveWire)] pub struct Tokens { pub access: String, pub refresh: String }

// ---- exposure: what each side actually serves / can call --------------------
// Credentials and Session are NOT listed: they exist in storage and are read
// and written inside the server-fn bodies below, but no client command can
// ever name them — an unexposed STRUCT_HASH is refused as unknown.
wavedb::expose_server! { AboutUser, Note, login, refresh, pinned_notes }
wavedb::expose_client! { AboutUser, Note, login, refresh, pinned_notes }

// ---- server functions: body runs ONLY on the node ---------------------------

#[server(public)]                            // public: reachable before any token exists
async fn login(db: &Db, user: U48, password: String) -> Result<Tokens> {
    let cred = Credentials::get_for(db, user).await?.ok_or(Error::NoUser)?;
    argon2_verify(&cred.argon2, &password)?;            // or: verify OAuth token
    let sid = Session { user, tenant: user, issued: now(), revoked: false }
        .create(db).await?;                              // session record = revocation handle
    Ok(Tokens { access: mint_access(user, user), refresh: mint_refresh(sid) })
}

#[server(public)]                            // public: authenticates via the refresh token in-body
async fn refresh(db: &Db, refresh: String) -> Result<Tokens> {
    let sid = verify_refresh(&refresh)?;                 // HMAC ok...
    let s = Session::get_by_id(db, sid).await?.ok_or(Error::Revoked)?;
    if s.revoked { return Err(Error::Revoked); }         // ...AND record live
    Ok(Tokens { access: mint_access(s.user, s.tenant), refresh: rotate_refresh(sid) })
}

#[server]                                    // login-required (default): filtered read, no query DSL
fn pinned_notes(db: &Db) -> impl Stream<Item = Result<Note>> {  // async iterator, streamed
    Note::all(db).try_filter(|n| future::ready(n.pinned))
}

// ---- client (native or wasm): same calls, bodies are stubs over the wire -----

async fn flow() -> Result<()> {
    // 1. connect unauthenticated (user = U48::MAX) — only login + public reads
    let pub_db = Db::connect("wss://app.example", U48::MAX, U48::MAX).await?;

    // 2. login → tokens (access short-TTL stateless, refresh revocable)
    let t = login(&pub_db, U48::from(42), "hunter2".into()).await?;

    // 3. authed handle — node derives user/tenant from the access token
    let db = Db::with_token("wss://app.example", &t.access).await?;

    // 4. ordinary requests: save (versioned) + get (1 lookup)
    Note { body: "ship it".into(), pinned: true }.create(&db).await?;
    let me: Option<Wire<AboutUser>> = AboutUser::get(&db).await?; // .view()? reads it

    // 5. filtered read via server fn — async iterator (collect, or .next().await)
    let pins: Vec<Note> = pinned_notes(&db).try_collect().await?;

    // 6. access expires → swap with refresh (rotates)
    let t = refresh(&pub_db, t.refresh).await?;
    let db = Db::with_token("wss://app.example", &t.access).await?;

    // 7. revoke = one record write; next refresh fails, access dies within TTL
    //    (done by an admin / server fn: Session.revoked = true)
    Ok(())
}
```

## What each step shows

| Step | Concept                                                                                                                                                                             |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | Unauthenticated tier — `user = U48::MAX`, login + public reads only.                                                                                                                |
| 2    | `login` is a `#[server]` fn: verifies a credential (local Argon2 **or** OAuth), creates the `Session` record, mints the **access + refresh** pair.                                  |
| 3    | The node derives identity from the **access token**, never the request body.                                                                                                        |
| 4    | Typed CRUD: `create` (versioned, history-chained), `get` (single-lookup Unique anchor).                                                                                             |
| 5    | Filtered/derived reads are server functions — the body runs node-side with DB access; the client ships a `WaveWire` call.                                                               |
| 6    | Short access TTL lapses → the **refresh** token mints a new access token and **rotates** itself (replay of the old one ⇒ theft signal).                                             |
| 7    | **Revocation** = mark the `Session` record `revoked` (one write). The next `refresh` is refused and the live access token dies within one short TTL — no per-request session store. |
