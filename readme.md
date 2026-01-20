# 🌊 WaveDB — A User-Data-Partitioned, Tenant-Centric, Full-Stack Database

> _"Technology works like ocean waves — going back and forth, but always advancing toward the shore (global-warm)."_

---

## What WaveDB is

WaveDB is a **database shipped as a Rust crate** — you add it as a library and
compile it _into_ your application. It is **full-stack**: the exact same code and
schema run **client-side** (native apps and browser/WASM) and **server-side**
(the storage nodes), so there is no separate API/DTO/ORM layer to keep in sync —
the schema crate _is_ the protocol.

Four properties define it:

1. **Tenant-partitioned data ownership.** Every record belongs to a tenant. A
   tenant is the unit of write-ownership across the cluster.
2. **Horizontal distribution & redundancy.** More than one node serves and
   stores a user's data; ownership of writes is assigned by tenant (and, later,
   by `STRUCT_HASH`), so scaling out and replicating is structural, not bolted
   on.
3. **Timeline / history as a first-class citizen.** Saving never destroys the
   old bytes — every modification is tracked so you can navigate a record's
   timeline and read its past states.
4. **Schema evolution by `STRUCT_HASH`.** A struct's identity is a hash of its
   shape, so changing it just makes a new type; clients, servers, and nodes can
   run different builds at once. Bridging old and new data is done with two
   application hooks (`first_try` / `fallback_not_found`) — no global migration
   step, no backup-and-restore.

On top of that: a **permission manager** for sharing data between users and
tenants (tenant-only / public / a specific list of tenants / a group — groups
deferred), and a storage layer that adapts to its compile target — **native**
builds (Android, Windows, Linux, macOS, iOS) use the filesystem (data files +
journal); **web** builds use **IndexedDB directly** (no journal needed).

> This file is the **idea + quickstart**. Every detailed mechanism lives in the
> crate that implements it — see the [Crate map](#crate-map). The per-idea design
> record and current progress live in [`rfcs/`](rfcs/README.md).

---

## The Core Idea

### The problem with traditional SQL (for applications)

Conventional SQL organises storage **by type — not by who owns the data or what
it relates to**. Two kinds of unrelated data end up physically interleaved on
the same pages, or the engine has to navigate between leaf nodes to find the
right id, for the right user, with the right relation to another table — and
application reads pay for it twice or more as the joins compound. _ONE QUERY, 10
JOINS._ WaveDB explores serving an application read with **one disk IO**, without
tens of GB of cache papering over the layout.

#### Mixing 1 — every tenant's rows share one table

```sql
-- Every tenant's orders share one table, so the partition key
-- ("whose data?") has to ride in every query next to the real filter.
SELECT * FROM orders
WHERE user_id = 42      -- partition key — restated on every read
  AND amount > 100;     -- the business filter you actually wanted
```

The `orders` table holds **every user's orders**, interleaved row-by-row across
shared pages. To serve one user the engine index-walks or scans through bytes
belonging to thousands of unrelated tenants, then filters down to the few rows
that matter. Every page pulled into the buffer pool is mostly data the query
throws away.

In WaveDB the partition key is **structural, not a predicate**: the tenant is
bound once at connect and never appears in a query again — only the business
filter is left.

#### Mixing 2 — unrelated child rows share one table (the nested case)

Even inside a single tenant, a classic schema puts every invoice's line items in
one `invoice_lines` table:

```sql
SELECT * FROM invoice_lines WHERE invoice_id = 1001;
```

The lines for invoice `1001` sit scattered among the lines of every other
invoice — and, combined with Mixing 1, every other tenant's invoices too. The
rows you actually want (one parent's children) are never colocated on disk.
Fetching them is a scatter of random page reads guided by an index, never one
sequential pull.

#### The shared root cause

Both problems stem from laying out storage **by table/type instead of by access
pattern**. Application reads are almost always _"give me this one tenant's — and
often this one parent's — data."_ But the bytes that answer that question are
sprayed across pages shared with data nobody asked for. The cost surfaces as
**cache misses and wasted disk IOPS**.

**WaveDB starts from that endpoint.** Data is partitioned by tenant from the
first byte, and a collection's members are reached through a per-collection index
(`Pivot` → `BpTree`) so the bytes a query needs sit together. The CPU saved from
join processing goes to compression instead.

### The ocean wave analogy

| Era     | Web Frontend                                          | Database                                       |
| ------- | ----------------------------------------------------- | ---------------------------------------------- |
| Past    | Static pages (fast, not dynamic)                      | DB and server tightly coupled                  |
| Present | Client-side rendering (dynamic, slow)                 | Independent DBs with ORMs as glue              |
| Future  | Server-side dynamic rendering (Next.js, Nuxt, Leptos) | **Tenant-partitioned, application-centric DB** |

The cycle closes. Each iteration looks like regression but carries forward the
best properties of both worlds — and here the database itself is shipped as the
library that the full-stack app compiles in.

---

## Tenants and users

A **tenant** is an organization — a junction of many users. This is the common
shape for B2B (a company and its employees). For a B2C application the tenant
number **equals** the user number, because the organization has exactly one user:
itself.

- Data of different tenants **never mixes** in any structure (`Unique`,
  `NonUnique`, `Pivot`, `BpTree`).
- A user belonging to one tenant **may read or write another tenant's data — but
  only with permission** (see [Permissions](#permissions)).
- On disk, data is grouped only to make **dictionaries and compression** more
  effective — grouping is a storage optimisation, never a sharing mechanism.

---

## Data Model

### The ID

Every record has a composite ID of **128 bits**. The most significant field is
the **key**, so a numeric sort of the `u128` is a sort by key — which for
timestamp-keyed records is a chronological order, ideal for the `BpTree`.

```
[ KEY (u64) | TENANT (u48) | FLAG (1) | SALT (15) ]
   MSB ──────────────────────────────────────── LSB
```

| Field    | Width   | Meaning                                                                                    |
| -------- | ------- | ------------------------------------------------------------------------------------------ |
| `KEY`    | `u64`   | Either a `STRUCT_HASH` (Unique) **or** a `CREATED_AT` timestamp — disambiguated by `FLAG`. |
| `TENANT` | `u48`   | Owning tenant. For B2C this is the user id.                                                |
| `FLAG`   | 1 bit   | `1` ⇒ `KEY` is a struct-hash key (Unique anchor); `0` ⇒ `KEY` is a `CREATED_AT` timestamp. |
| `SALT`   | 15 bits | Collision breaker within one `(KEY, TENANT)`.                                              |

The 15-bit `SALT` only breaks collisions: **Unique** = `0`; every timestamp-keyed
shape (**NonUnique / BpTree / Pivot**) = 15 random bits. It carries **no
struct-hash truncation** — the type is known from the per-`STRUCT_HASH` storage
directory and the wire envelope, and `TENANT` (48 bits) separates tenants.

**`CREATED_AT` is nanosecond precision** measured from a fixed WaveDB epoch
(a constant Rust instant). `Id` **uniqueness** within a `(KEY, TENANT)` comes from
the random `SALT`, not clock monotonicity — distinct even in the same nanosecond
or under an NTP step. Ordering by `CREATED_AT` is **best-effort** chronological
(fine for the `BpTree` index), not a strict total order under clock skew.

There is **no `STRUCT_ID` and no schema-version field** in the ID anymore — type
identity lives in `STRUCT_HASH` (below), which subsumes both.

### `STRUCT_HASH`

Each declared struct gets a `STRUCT_HASH: u64`, computed **at compile time** by
the `#[wavedb]` macro as a `const` hash of:

```
hash( STRUCT_NAME + SHAPE(Unique|NonUnique|NestedNonUnique)
      + each PROPERTY_NAME + each PROPERTY_TYPE )
```

Because the hash folds in field names and types, **any schema change produces a
new `STRUCT_HASH`** — a changed struct is simply a different type. The old model's
separate "struct id + numeric version" is gone; bridging old and new data is done
with the `first_try` / `fallback_not_found` hooks (below).

### Base data types

| Type          | Declared as                        | Cardinality                                  | ID layout (`KEY · TENANT · FLAG · SALT`) |
| ------------- | ---------------------------------- | -------------------------------------------- | ---------------------------------------- |
| **Unique**    | `#[wavedb]` (default)              | Exactly one live record per tenant           | `STRUCT_HASH · TENANT · 1 · 0`           |
| **NonUnique** | `#[wavedb(NonUnique)]`             | Many per tenant; may nest in other NonUnique | `CREATED_AT · TENANT · 0 · salt15`       |
| **Pivot**     | type generated; instance on demand | One per tenant per definition (the handle)   | `CREATED_AT · TENANT · 0 · salt15`       |
| **BpTree**    | type generated                     | Index nodes addressing a collection          | `CREATED_AT · TENANT · 0 · salt15`       |

**Unique** is the default — the everyday "one record per tenant" object:

```rust
#[wavedb]
pub struct AboutUser {
    pub name: String,
    pub surname: String,
    pub phone: String,
    pub address: String,
}
```

Its single live record sits at a **directly computable anchor address**
(`STRUCT_HASH · TENANT · 1 · 0`), so a read is one lookup with no index walk.

**NonUnique** objects exist many times within a tenant and may nest recursively
inside other NonUnique objects. A collection is referenced through a **`PivotId`**
held in a field. The `Pivot` is **not created automatically**: you explicitly
create it (one per holder — a nesting NonUnique mints one per record, so a type
has as many collections as there are holders) and **store the returned `PivotId`** in
a `Unique` struct or a nesting `NonUnique`; then you `get().await?` that pivot to
navigate into the collection:

```rust
#[wavedb]
pub struct UserInterestedFruits {
    // a handle into a NonUnique collection of Fruit, reached via its Pivot
    pub list_of_fruits: <Fruit as WaveDbStruct>::PivotId,
}
```

**Pivot** — the macro generates the _type_; an _instance_ is created on demand
(one per holder) and its `PivotId` stored by the holder. It carries
no business data — only the addressing into the index trees:

```rust
pub struct Pivot {
    pub current:    LocalId,               // 80-bit pointer to the B+tree of living records
    pub dead:       LocalId,               // 80-bit pointer to the B+tree of deleted records
    pub permission: Option<PermissionRef>, // collection-default access (per-record Metadata overrides)
    // + one LocalId per #[wavedb::pivot(...)] secondary index
}
```

No element counter: a count would force a `Pivot` write on every insert/remove;
the `Pivot` instead stays effectively immutable, written only if a `BpTree` root
moves.

**BpTree** (also auto-generated) is the index itself — a B+tree keyed by
`CREATED_AT`, holding addresses of NonUnique records (not the records' bytes).
There is one tree for living data and one for deleted data.

**Secondary indexes** — a NonUnique struct can declare extra `BpTree`s on its
properties with `#[wavedb::pivot(field)]` or `#[wavedb::pivot((f1, f2))]`. Each
adds a `BpTree` root to the `Pivot` and a typed lookup on the collection handle
(`by_field`), resolved two-phase like the primary tree. `insert`/`remove` update
every index; a `save` only touches a secondary tree if it changed that field.

> Pivot and BpTree are generated _per_ NonUnique type. If two such generated
> types share the same name and field shape their `STRUCT_HASH` may collide —
> that is harmless, because they are only ever addressed within their own
> tenant/collection context.

### Operations

- **Unique** — `get` and `save`. There is **no `create`** (a create that errors
  when the record exists is just friction): `save` is an upsert — it writes the
  live bytes at the fixed anchor and chains the previous bytes into history via
  `Metadata` (timeline preserved).
- **NonUnique** — `insert`, `save` (update), `remove`, reached through the
  collection's **`Pivot`**:
  - a record's **identity `Id` is assigned at `insert`** (that moment's
    `CREATED_AT`) and is its **stable anchor** — references point at it, it never
    changes.
  - **`save` / update re-keys only the trees whose field changed** — the `current`
    `BpTree` is keyed by the immutable `CREATED_AT` (never re-keyed), and each
    `#[wavedb::pivot(...)]` secondary is re-keyed only if its field changed. The
    **`dead`** tree is **not** touched: update is not a delete — the superseded
    version is archived at a derived slot and linked through `Metadata`
    (see [History](#history--timeline)). Reaching the tree roots needs the `Pivot`,
    found via **`Metadata.pivot_id`** — so `save` reindexes without the handle.
  - **`remove`** moves the record from `current` to the **dead** tree — the **only**
    op that writes `dead`. Nothing is erased; history stays navigable.

  `insert` / `remove` / `all` / `get(id)` are methods of a **collection handle**
  opened from a stored `PivotId`; `save` is a method on the record (it reaches the
  `Pivot` through `Metadata.pivot_id`). `insert` stamps `Metadata.pivot_id` into the new
  record from the handle's `PivotId`.

### History / timeline

Saving never frees the old bytes. The **live** version sits at the record's
anchor; each superseded version is archived at a **derived slot** — an address
computed from the instant that version was authored — and versions chain through
**`Metadata.succession`** by instant, not by pointer, so a version written once is
never repointed and the full timeline is walkable. This is why update needs **no
`dead`-tree write**. Removed NonUnique records live on in the `dead` `BpTree` (the
only thing that populates it).

---

## Permissions

Access control is stored inline in each record's `Metadata`, scoped per record:

| Setting         | Who can access                                            |
| --------------- | --------------------------------------------------------- |
| **Tenant-only** | Only the owning tenant's users (the common case).         |
| **Public**      | World-readable.                                           |
| **Tenant list** | A specific list of other tenants.                         |
| **Group**       | A shared permission group _(deferred — not implemented)_. |

This is the mechanism by which a user of one tenant can act on another tenant's
data: the data's owner grants it.

For a **NonUnique collection** permission is two-level: the collection's `Pivot`
holds a **default** (seeds new inserts, gates collection-scope ops), and each
record's `Metadata` holds the **authoritative** value for that record (a record
may override its collection). The per-record copy is what makes an atomic
`Update` validate without reading the `Pivot`. See
[`wavedb-core` §Permissions](crates/wavedb-core/README.md#permissions).

---

## Schema evolution

A struct change yields a new `STRUCT_HASH` (the old "version number" concept is
gone). There is no migration chain or auto-upgrade walk — bridging old and new
data is done with two optional application hooks: **`first_try`** runs before a
read hits storage (synthesise the value, e.g. from an older `STRUCT_HASH`), and
**`fallback_not_found`** runs after a miss (fetch or derive a default). Details:
[`wavedb-core`](crates/wavedb-core/README.md#schema-evolution--lookup-hooks).

---

## Simple usage

```rust
use wavedb::prelude::*;

// For a B2C app the tenant *is* the user — bound once, at connect time.
let db = Db::connect("wss://wavedb.example", /* user */ 42, /* tenant */ 42).await?;

// Unique: one record per tenant. No create — save is upsert.
let profile: Option<AboutUser> = AboutUser::get(&db).await?;
profile_var.save(&db).await?;

// NonUnique: open the collection from a stored PivotId.
let orders = Order::collection(&db, user.orders);   // handle carries the PivotId
let id = orders.insert(&db, Order { amount: 120 }).await?;  // assigns identity Id, adds to BpTree
let mut recent = orders.all(&db);                          // async iterator: streams BpTree → Ids → fetch
while let Some(order) = recent.next().await { let order = order?; /* … */ }

order.save(&db).await?;            // reindex current + secondary trees; old version chained, dead untouched
orders.remove(&db, id).await?;     // move Id current → dead BpTree (history kept)

// Filtered / derived reads are server functions — run on the node, called through
// a generated client binding (no client-side query DSL). A collection-returning
// server fn yields an async iterator, streamed item-by-item over the wire.
#[server]
fn orders_over(db: &Db, min: u64) -> impl Stream<Item = Result<Order>> {
    Order::all(db).try_filter(|o| future::ready(o.amount > min))
}
let big: Vec<Order> = orders_over(&db, 100).try_collect().await?; // or iterate with .next().await
```

Every `#[wavedb]` struct and `#[server]` function carries its own generated
**execution steps** — the derive emits the per-item ops at the item itself
(`get`/`save` for Unique; `insert`/`update`/`remove`/`get`/`search` for
NonUnique; the call arm for a function). But **nothing is reachable until it
is exposed**: each side declares, in an explicit module, exactly which items
it serves or calls — and those declarations _are_ the registry. The macro
expands to a **`match` on each `STRUCT_HASH`** over exactly the listed items —
concrete, monomorphized arms; no build-time scanner, no `build.rs`, no `dyn`,
no sum type:

```rust
// server module — what the node serves. Unlisted ⇒ unknown hash ⇒ refused.
wavedb::expose_server! {
    AboutUser,                                   // full generated op set
    Order { save: audited_save, remove: never }, // override / exclude ops
    login, refresh,                              // #[server] functions
}
// Credentials, Session: NOT listed — they exist in storage and inside
// server-fn bodies, but the wire cannot name them.

// client module — what this client binary can call.
wavedb::expose_client! { AboutUser, Order, login, refresh }
```

The explicit list is a **security surface**: exposure is an allowlist, a type
can be storage-only (never wire-addressable), and any generated op can be
excluded or swapped for a hardened reimplementation. Attaching the server
exposure turns a generic node into _your_ backend:

```rust
Server::bind("0.0.0.0:7700")
    .tenant(42)
    .data_dir("./data")
    .registry(app_schema::REGISTRY) // emitted by expose_server!
    .serve()
    .await
```

### How a write travels

A typed call (`record.save(&db)`) does two things: **write-through to the local
`Store`**, then send a **command frame** over the network. The frame
`{ STRUCT_HASH, command, payload }` is wrapped with the **access token** into one
self-contained `Request` — WaveDB uses **no HTTP headers/cookies**; the POST body
carries everything, identity included (transport = dumb tunnel). `command` is
`Get`/`Save` for Unique, `Insert`/`Update`/`Remove` for NonUnique. The transport
for now is **HTTP POST only** (WebSocket sends the token once at handshake; push
deferred). Node-side the request is gated (identity from
the access token → header → decode → permission → `validate` → `preprocess`),
then routed by the registry: **`match struct_hash`** to the concrete type, then
**`match command`** to that type's compile-time engine fn, which drives the
storage internals (allocator, journal, pages, `Pivot`/`BpTree`). A `#[server]`
function call rides the **same `CommandFrame`** in the **same `struct_hash`
space** — no separate call frame; you can't tell a call from an object op at the
frame level, only the `match struct_hash` can. A function arm decodes `payload` as
its args and runs the body, whose auth + permission checks live **inside the
body**, not the match. Full path:
[`wavedb-net`](crates/wavedb-net/README.md#command-envelope--dispatch) and
[`wavedb-quick-node`](crates/wavedb-quick-node/README.md#node-side-enforcement).

The full client API and object lifecycle live in
[`wavedb`](crates/wavedb/README.md).

---

## Crate map

| Crate                                                     | What it owns                                                                 | Read for                                                                                                           |
| --------------------------------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| [`wavedb`](crates/wavedb/README.md)                       | Client `Db` handle, typed CRUD, server-fn call bindings                      | Quickstart, entry points, object lifecycle                                                                         |
| [`wavedb-wire`](crates/wavedb-wire/README.md)             | The standalone `WaveWire` codec — pure value ⇄ bytes, no `STRUCT_HASH`       | `WaveWire` trait, `to_wire`/`from_wire`, `#[derive(WaveWire)]`, decode = size-only failure                         |
| [`wavedb-wire-derive`](crates/wavedb-wire-derive)         | `#[derive(WaveWire)]` proc-macro (re-exported by `wavedb-wire`)              | Struct + enum `WaveWire` codegen, `::wavedb_wire::` paths                                                          |
| [`wavedb-core`](crates/wavedb-core/README.md)             | `Id`, `Metadata`, `STRUCT_HASH`, schema-evolution hooks, permissions, wire   | ID layout, struct-hash identity, **schema evolution**                                                              |
| [`wavedb-macros`](crates/wavedb-macros/README.md)         | `#[wavedb]`, `#[server]`, `expose_server!`/`expose_client!`, generated `Pivot`/`BpTree` | Object declaration, `STRUCT_HASH` derivation, **exposure** + per-hash `match` dispatch                             |
| [`wavedb-storage`](crates/wavedb-storage/README.md)       | The per-node engine                                                          | **Block manager, per-`STRUCT_HASH` page directory, linear hashing**, pages, dictionaries, journal + cache pipeline |
| [`wavedb-quick-node`](crates/wavedb-quick-node/README.md) | Serving/storage node                                                         | Tenant write-ownership ring, replication, routing/failover, node-side validation                                   |
| [`wavedb-net`](crates/wavedb-net/README.md)               | Transport                                                                    | WebSocket / HTTP dumb tunnel, live sync (subscriptions + navigation catch-up)                                                               |
| [`wavedb-wasm`](crates/wavedb-wasm/README.md)             | Browser client                                                               | IndexedDB key→value storage (no pages, no journal)                                                                 |

Tooling: `wavedb-examples`, `wavedb-bench`, `wavedb-test-cluster`.

A cold/history tier (slow-node) was **removed** — history is single-tier in
the data files, unbounded growth accepted for now. A multi-node cluster and its
monitors are **deferred**; those crates are intentionally absent.

---

## Non-Goals

- **Not OLAP.** Cross-tenant aggregations belong in a dedicated analytics pipeline.
- **Not a general consensus system.** Consistency is tenant-scoped; multi-tenant eventual consistency is by design.
- **Not a SQL replacement.** No query DSL — reads are `get` / collection walk /
  server functions; there is no ad-hoc cross-table query language.
- **Not offline-first (yet).**

---

## Implementation language

Rust — the `#[wavedb]` proc-macro computes `STRUCT_HASH` and enforces ID/Metadata
structure at compile time. The `WaveWire` format defines the byte layout explicitly
(no `repr(C)`, no serde — the macro emits the encode/decode), `async` end to end,
and the same source compiles to native (Tokio, filesystem)
and browser (WASM via `wasm_bindgen_futures`, `fetch`, `gloo_net`, IndexedDB).

---

## Status

🔬 **Research / rebuild phase.** The workspace is a clean reimplementation: the
design below is being rebuilt from the ground up. These docs describe the
**target** architecture, not shipped code.

**No format versioning, no migrations — by policy.** WaveDB does not exist yet,
so no energy goes into tracking on-disk history: the `MANIFEST`'s
`FORMAT_VERSION` is **pinned at `1`** and every layout (`MANIFEST`, journal
frames, pages, wire encodings, `STRUCT_HASH` inputs) may change freely between
commits with no bump, no changelog, and no migration path. A database written
by an older build is simply unsupported — start fresh. Version discipline begins at the first release.
