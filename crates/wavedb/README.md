# wavedb

The user-facing **client library** — the `Db` handle, typed object CRUD, and the
client bindings that call server functions. Same code path on servers, native
clients, and (compiled to WASM) browsers.

> For the project-wide idea see the [root README](../../readme.md). For the
> wire/transport layer see [`wavedb-net`](../wavedb-net/README.md); for object
> declaration see [`wavedb-macros`](../wavedb-macros/README.md).

> Status: `Db::connect` (transport-only) and the M6 `Db::open` family are
> built, with these divergences from the target below. The landed spelling is
> `Db::open(CLIENT_REGISTRY, addr, user, tenant, app)` — the client registry
> names the native engine's slots; `app` resolves the default store location
> (`$XDG_CACHE_HOME`/`~/.cache` → `<app>` on unix, `%LOCALAPPDATA%` on
> Windows — the app is the leaf directly under the base, no shared `wavedb/`
> parent; in the browser the IndexedDB database is named exactly `app`) —
> plus native
> `Db::open_at(…, dir)` and `db.local()` (the cache's direct typed surface).
> The cache is **node-first**, not local-first: every op goes to the node,
> acknowledged results mirror into the local engine (journal + data files
> native / IndexedDB wasm, under node-minted ids via `Collection::adopt`),
> and reads serve locally only on a transport fault the cache can answer —
> absence propagates the fault, refusals never fall back. An offline **Unique**
> save no longer refuses: it queues for node-first replay on reconnect
> (`db.drain_offline_queue()`), mirroring locally in the meantime; NonUnique
> offline and a durable-across-restart queue stay deferred
> ([RFC 0036](../../rfcs/0036-offline-write-queue-PLANNED-LOW.md)).
> No cluster front door / failover URL, no Drop notification, no
> `tokio::broadcast`; cross-tenant work is `db.as_tenant(t)`, and the typed
> surface is `T::get(&db)` / `v.save(&db)` / `T::collection(pivot)`
> ([RFC 0024](../../rfcs/0024-client-db-and-cache.md)).

## Entry points

| Mode                                        | Storage location         | Tenant model                |
| ------------------------------------------- | ------------------------ | --------------------------- |
| `Db::open(url, path, user)`                 | Local file at `path`     | `tenant = user_id`          |
| `Db::open(url, path, user, default_tenant)` | Local file at `path`     | Tenant explicit (companies) |
| `Db::open(url, user, default_tenant)`       | Browser IndexedDB (WASM) | Tenant explicit             |
| `Db::open(url, user)`                       | Browser IndexedDB (WASM) | `tenant = user_id`          |

`url` resolves to the cluster's front door; the request is redirected to the
Quick-Node owning the user's tenant, and the **backup** Quick-Node URL is
returned alongside for failover. Native modes use the local `path` as a
write-through cache; WASM modes use IndexedDB in the same role. A `Db` can spawn
another handle for a different tenant (server-side cross-tenant work):

```rust
let other = db.as_tenant(other_tenant_id);
```

The `Drop` impl notifies the Quick-Node so the session is released promptly.

## The partition key is structural

The tenant is bound **once, at connect** — you never restate it on a read. The
engine only ever touches the connected tenant's data. The typed methods come from
per-struct traits the `#[wavedb]` macro implements by shape (`UniqueObject` /
`NonUniqueObject`, see below).

```rust
use wavedb::prelude::*;

let db = Db::connect("wss://wavedb.example", /* user */ 42, /* tenant */ 42).await?;

// Unique (UniqueObject): one record per tenant. No create — save is upsert.
let mut profile: AboutUser = AboutUser::get(&db).await?.unwrap_or_default();
profile.city = "Lisbon".into();
profile.save(&db).await?;

// NonUnique (NonUniqueObject): open the collection from a stored PivotId.
let orders = Order::collection(&db, profile.orders); // Collection<Order>, carries the PivotId
let id = orders.insert(&db, Order { amount: 120 }).await?; // assigns identity Id, adds to BpTree
let mut recent = orders.all(&db);                          // async iterator: streams BpTree → Ids → fetch
while let Some(order) = recent.next().await { let order = order?; /* … */ }

let mut o = orders.get(&db, id).await?.unwrap();
o.amount = 130;
o.save(&db).await?;          // reindex current + secondary trees; old version chained, dead untouched
orders.remove(&db, id).await?; // move Id current → dead BpTree (history kept)
```

## Object lifecycle & the typed traits

There is **no `create`** — `save` is an upsert. Every typed call does two things
internally and nothing leaks: **write-through to the local `Store`** (native file
/ web IndexedDB) **and send a self-contained `Request` to the owner node** over
`wavedb-net`. The request wraps the access token around a command frame
`{ STRUCT_HASH, command, payload }` — `command` is `Get`/`Save` for Unique,
`Insert`/`Update`/`Remove` for NonUnique (the `save()` method emits `Update`); see
[`wavedb-net` §Command envelope](../wavedb-net/README.md#command-envelope--dispatch).
WaveDB uses **no HTTP headers/cookies** — identity rides inside the body
(transport = dumb tunnel). The transport is **HTTP POST only for now** (WebSocket
sends the token once at handshake; push deferred). The node is the authoritative
writer — it runs the `Pivot`/`BpTree` engine; the client never does.

The macro implements one typed trait per shape (defined in
[`wavedb-core`](../wavedb-core/README.md#typed-object-traits-per-struct-macro-implemented)):

| Shape         | Trait             | Methods                                                                                          |
| ------------- | ----------------- | ------------------------------------------------------------------------------------------------ |
| **Unique**    | `UniqueObject`    | `T::get(&db)`, `record.save(&db)`                                                                |
| **NonUnique** | `NonUniqueObject` | `T::collection(&db, pivot) → Collection<T>` (`insert`/`get`/`all`/`remove`) + `record.save(&db)` |

`Id`s are client-known (Unique deterministic; NonUnique minted at `insert`), so
the write-through is immediate; the node confirms. Same calls native and wasm —
only the local `Store` swaps.

**Collection reads are async iterators.** `Collection::all` and every generated
`by_<field>` lookup return `impl Stream<Item = Result<T>>`, not a buffered `Vec` —
the two-phase BpTree walk streams records as it resolves `Id`s, so a caller can
stop early without fetching the whole collection. Iterate with `.next().await`, or
`.try_collect().await?` to materialise a `Vec`. The prelude re-exports
`Stream` / `StreamExt`.

## Filtered / derived reads: server functions

There is **no client-side query DSL**. Anything past "get this" or "list the
collection" — filtering, aggregation, joining across collections, derived views —
is a **server function**: an `async fn` that runs on the node with full DB
access, declared with `#[server]` and called through a generated typed binding
that ships the arguments over the wire. The client awaits it like a local async
fn; the body never enters the client binary.

```rust
// Declared once in the schema crate; the body runs only on the node.
#[server]
fn orders_over(db: &Db, min: u64) -> impl Stream<Item = Result<Order>> {
    Order::all(db).try_filter(|o| future::ready(o.amount > min))
}

// Client side: a generated binding with the same signature — an async iterator.
let big: Vec<Order> = orders_over(&db, 100).try_collect().await?;
```

Mechanism (the `#[server]` macro, wire-encoded args, transport) lives in
[`wavedb-macros`](../wavedb-macros/README.md#server-functions--server).

## Everything is `async`

Every public API on `Db`, every storage actor, every evolution hook is `async`
end to end — no blocking IO surface, no hidden thread-pool dispatch. Native runs on
Embassy carriers (RFC 0066), with no tokio anywhere; the browser on
`wasm_bindgen_futures`. The public API is identical.

## Unauthenticated sessions

A client without credentials connects with `user = U48::MAX`. The session sees
only **public data**; the API is restricted to login (local password or OAuth)
and reading world-readable data. Login mints a stateless HMAC **access** token
(short TTL, identity derived node-side) plus a revocable **refresh** token;
structure in [`wavedb-net`](../wavedb-net/README.md#authentication).
