# wavedb-core

Core primitives shared by **every** node kind and by proc-macro generated code:
the composite `Id`, `STRUCT_HASH`, `Metadata`, the schema-evolution lookup hooks,
permission refs, and the `WaveWire` serialization trait.
**No I/O** — safe in WASM, in macros, everywhere.

> For the project-wide idea and quickstart see the
> [root README](../../readme.md).

## Module map

| Module       | Responsibility                                                                |
| ------------ | ----------------------------------------------------------------------------- |
| `id`         | The 128-bit composite `Id`, the `U48` newtype, and field accessors.           |
| `local_id`   | `LocalId` — compact 80-bit ID (no `TENANT`) for BpTree-internal use.          |
| `metadata`   | `Metadata` — modification chain, pivot back-link, authorship, permission ref. |
| `hooks`      | `first_try` (pre-search) and `fallback_not_found` (post-miss) hooks.          |
| `permission` | `PermissionRef` shapes.                                                       |
| `wire`       | Re-export of the standalone [`wavedb-wire`](../wavedb-wire/README.md) codec.  |
| `store`      | The `Store` backend trait (key→value over `Id` + atomic batch).               |
| `index`      | `Pivot`, `BpTree`, `IndexKey`, `Bound` — the `Store`-generic index contracts. |
| `traits`     | `WaveDbStruct`, shape markers.                                                |
| `error`      | Workspace error type.                                                         |

---

## The ID

Every record has a composite ID of exactly **128 bits**. The **key is the most
significant field**, so a numeric ordering of the `u128` _is_ an ordering by key
— for the timestamp-keyed shapes that means chronological order, which is what
the `BpTree` indexes on.

```
[ KEY (u64) | TENANT (u48) | FLAG (1) | SALT (15) ]
   MSB ──────────────────────────────────────── LSB
```

| Field    | Type    | Description                                                                                          |
| -------- | ------- | ---------------------------------------------------------------------------------------------------- |
| `KEY`    | `u64`   | `STRUCT_HASH` when `FLAG = 1` (Unique anchor), or a `CREATED_AT` timestamp when `FLAG = 0`.          |
| `TENANT` | `u48`   | Owning tenant. `0` reserved for the system; `U48::MAX` for unauthenticated sessions. B2C: = user id. |
| `FLAG`   | 1 bit   | `1` ⇒ `KEY` is a struct-hash key; `0` ⇒ `KEY` is a `CREATED_AT` timestamp.                           |
| `SALT`   | 15 bits | Collision breaker within one `(KEY, TENANT)`.                                                        |

### The `SALT` field (15 bits)

The trailing 15 bits **only break collisions** within a single `(KEY, TENANT)`:

| Shape                          | `SALT[14..0]`                                 |
| ------------------------------ | --------------------------------------------- |
| **Unique**                     | `0` (the fixed anchor needs no salt)          |
| **NonUnique / BpTree / Pivot** | 15 bits of writer-supplied random/fixed value |

There is **no struct-hash truncation in the `Id`**. The type is always known
without it — every lookup is scoped to one `STRUCT_HASH` directory (storage) or
carried in the wire envelope, and the 48-bit `TENANT` already separates tenants.
15 random bits plus the nanosecond `KEY` make a same-tick collision within one
tenant astronomically unlikely; this also keeps the in-memory KV cache's key
space clean (no 256-bucket truncation aliasing).

> **Future.** The 15-bit `SALT` may be masked per connected user to shrink the
> KV-cache collision probability further.

### `U48` — the 48-bit newtype

Rust has no native `u48`, so the 48-bit `TENANT`/`user` values are a newtype
wrapping a `u64`:

```rust
pub struct U48(u64); // invariant: value < 2^48
```

It range-checks on construction, exposes the `U48::MAX` (unauthenticated) and `0`
(system) sentinels, and packs into the `Id`/`Metadata` layout as exactly 48 bits.
Accessors such as `.tenant_id()` and `Metadata::user` return `U48`, never a raw
`u64`. Everywhere this README writes `u48`, the Rust type is `U48`.

### `CREATED_AT` time base — the guarantee

`CREATED_AT` is a **nanosecond** count from a fixed WaveDB epoch (a `const` Rust
reference instant). What is and isn't guaranteed:

- **Uniqueness (guaranteed):** the `Id` is unique within a `(KEY, TENANT)` because
  of the **15-bit random `SALT`**, _not_ because of clock monotonicity. Two writes
  in the same nanosecond — or even with the clock running backwards (NTP step) —
  get distinct `Id`s from the salt. At low scale 15 random bits are ample; a
  64-write burst in one ns has a sub-1e-15 collision chance.
- **Ordering (best-effort):** sorting by `CREATED_AT` is approximately
  chronological — good enough for the `BpTree`'s time index — but it is **not a
  strict total order** under clock skew/adjustment. WaveDB does not promise that
  `Id` order equals real-time order across NTP corrections.

> **Future.** At higher scale the `SALT` becomes a **per-user-session mask** (a
> chunk reserved per connected session) so concurrent sessions can't collide and
> the in-memory KV cache stays clean — uniqueness by construction, not just luck.

### Why the `FLAG` bit

It lets one `Id` type encode two addressing schemes in the same `KEY` slot:

- **Unique anchor** — `FLAG = 1`, `KEY = STRUCT_HASH`, `SALT = 0`: a directly
  computable, single-record address. One lookup, no index.
- **Everything timestamped** (NonUnique records, Pivots, BpTree nodes) —
  `FLAG = 0`, `KEY = CREATED_AT`: chronologically ordered, indexable by `BpTree`.

There is **no `STRUCT_ID`** and **no schema-version field** in the ID. Type
identity and schema generation are both folded into `STRUCT_HASH`.

---

## `STRUCT_HASH`

A `u64` identity computed at **compile time** (by `#[wavedb]` /
`#[server]`, see [`wavedb-macros`](../wavedb-macros/README.md)) over the
canonical string:

```
STRUCT_NAME + SHAPE + each PROPERTY_NAME + each PROPERTY_TYPE
```

**Algorithm: SeaHash**, over the canonical string with a fixed four-lane WaveDB
seed (domain-separated from the random per-database seed used for page routing).
SeaHash is **portable across every build, machine, architecture, and endianness**
for a given seed — exactly what type identity needs: clients and servers must
agree on a type's identity. The `seahash` crate is **pinned to an exact version**,
because the algorithm is identity-load-bearing: an unreviewed bump that changed it
would invalidate every stored record's `STRUCT_HASH`.

Folding field names and types into the hash means **any schema change yields a
new `STRUCT_HASH`** — a changed struct is simply a different type. There is no
version counter; bridging old and new is done with the lookup hooks below. The
`STRUCT_HASH` does **not** appear inside the `Id`'s `SALT`; the type is known from
the per-`STRUCT_HASH` storage directory and the wire envelope.

---

## Metadata

```rust
pub struct Metadata {
    pub previous: Option<u64>,                // predecessor's authoring instant; None = first version
    pub succession: Succession,               // CreatedAt(instant) live · Next(instant) on an archive
    pub pivot_id: Option<LocalId>,            // None = Unique record
    pub user: U48,                            // who wrote this version (48-bit newtype)
    pub device_created: u64,                  // which device produced it
    pub permission: Option<PermissionRef>,    // access rule; None = tenant-only
}
```

No `struct_version` field — the stored record's `STRUCT_HASH` (carried in the
wire envelope) already says which schema it was written under.

The version chain links by **instant, not address**: `previous` and
`Succession::Next` carry the `u64` instant a version was authored, and an archive's
slot is a pure function of `(type, shape, instant)`, so a link written once is
never repointed. `Succession` is hand-encoded as a fixed 9-byte stack
(`tag + instant`); the whole `Metadata` stack is **26 bytes**, and a Unique first
version (every `Option` `None`) adds zero heap. Full model:
[RFC 0009](../../rfcs/0009-anchors-succession-and-history.md) /
[RFC 0010](../../rfcs/0010-metadata-and-record-envelopes.md).

### `LocalId` — 80-bit compact ID

```
[ KEY (u64) | FLAG (1) | SALT (15) ]
   MSB ─────────────────────────── LSB
```

`LocalId` is `Id` with `TENANT (u48)` stripped — 10 bytes on the wire. The BpTree
is already scoped to a tenant, so `TENANT` is derivable from context. Reconstruct
a full `Id` with `local_id.to_id(tenant)` — two or three CPU cycles, in memory,
never disk.

### `pivot_id` — the NonUnique reindex back-link

A NonUnique `save` (update) re-keys the collection trees whose field changed — the
`current` `BpTree` is keyed by the immutable `CREATED_AT` (never re-keyed), each
`#[wavedb::pivot(...)]` secondary only when its field changed — so it must reach the
tree roots, which live in the collection's **`Pivot`**. The record carries its
owning `PivotId` here as a `LocalId` (the typed `<T>::PivotId` is the compile-time
view only — core never names macro types).

- **Stamped at `insert`** from the collection handle's `PivotId`; `None` for Unique.
- Lets `save` reindex from the record alone, without re-passing the handle.
- It is **outside `STRUCT_HASH`**, so it changes **no** struct's identity — only
  Metadata's own wire layout.

Why not the `dead` tree on update? Because the superseded version is archived at a
derived slot and linked by `Succession` (above), so update never writes `dead`;
`dead` is populated **only** by `remove`. The record's identity `Id` (the insert
anchor) stays stable across updates so references never break.

---

## Schema evolution — lookup hooks

There is **no migration chain, no rollback graph, no auto-upgrade walk.** A
schema change just yields a new `STRUCT_HASH`; old records keep their old hash.
Bridging that transition is the application's job, through two optional async
hooks declared on a struct:

| Hook                 | Runs                        | Signature                                   |
| -------------------- | --------------------------- | ------------------------------------------- |
| `first_try`          | **before** the DB search    | `async fn<Db>(&Db) -> Result<Option<Self>>` |
| `fallback_not_found` | **after** the search misses | `async fn<Db>(&Db) -> Result<Option<Self>>` |

- **`first_try`** lets you produce the value before touching storage — e.g.
  synthesise it from records stored under a previous `STRUCT_HASH`, or
  short-circuit a known case.
- **`fallback_not_found`** runs only when the normal lookup returns `None` — the
  place to fetch, derive a default, or lift an old record forward.

Both are plain functions you write; the engine simply calls them at those two
points in the read path. Nothing else about versioning is built in.

---

## Permissions

Access control is stored **inline in `Metadata`**, scoped per record:

| Value                                  | Semantics                                              | Wire cost |
| -------------------------------------- | ------------------------------------------------------ | --------- |
| `None`                                 | Tenant-only — the owning tenant's users (common case). | 1 byte    |
| `Some(PermissionRef::Public)`          | World-readable.                                        | 1 byte    |
| `Some(PermissionRef::Tenants(list))`   | A specific list of other tenant ids.                   | 1 + list  |
| `Some(PermissionRef::Group(group_id))` | Reference to a shared permission group _(deferred)_.   | 1 + ref   |

A grant is what lets a user of one tenant act on another tenant's data; without
it, tenants never see each other's records.

### NonUnique: collection default + per-record override

For a NonUnique collection, permission is **two-level** — stored in both the
`Pivot` and each record's `Metadata`, on purpose:

- the collection's **`Pivot` carries a default** `PermissionRef`, applied to a
  record at `insert` when it specifies none, and checked for collection-scope ops
  (`Insert`, `All`) where no single record is loaded yet;
- each **record's `Metadata.permission` is authoritative** for that record — a
  record may diverge from its collection default (override).

The per-record copy keeps an `Update`/`Remove`/`Get(id)` **atomic and
Pivot-free**: the single batch validates and rewrites permission from the
record alone, no `Pivot` read. The `Pivot` default keeps `Insert`/`All`
checkable before any record is read, and seeds new records. Changing the `Pivot`
default does **not** rewrite existing records — it is the default for *new*
inserts, not a broadcast. Enforcement runs node-side — see
[`wavedb-quick-node`](../wavedb-quick-node/README.md#permission-enforcement).

> The cross-tenant **serving path** (which node serves tenant A's data to a
> tenant-B user, and where the grant is enforced across nodes) is **deferred** —
> the model above is the contract; the multi-node routing is not built yet.

There is **no query expression tree**. Reads are: a Unique `get`, a NonUnique
collection walk through its `Pivot` → `BpTree` (ordered by `CREATED_AT`), or a
**server function** for anything filtered or derived — an `async fn` that runs on
the node with DB access and is called by a typed client binding (see
[`wavedb-macros`](../wavedb-macros/README.md#server-functions--server)).

---

## Registry

The **registry that maps a `STRUCT_HASH` to its arm is declared, not
discovered** — and not here. The derive macros generate each item's execution
steps in place (`#[wavedb]`: the shape's engine ops; `#[server]`: the call
arm); an explicit **exposure module on each side** (`expose_server!` in the
node's code, `expose_client!` in the client's) lists what is actually
reachable, and expands to **one `match` on the 64-bit `STRUCT_HASH` per
operation**, *not* an `Object` enum. **The matches *are* the registry**:
`from_wire` / `to_wire`, the `first_try` / `fallback_not_found` hooks, the
generated `Pivot`/`BpTree` accessors, the engine ops, and the server-function
call all dispatch by `match struct_hash { … }` to a **concrete, monomorphized**
arm — **no sum type, no `dyn`, no runtime registration, no descriptor table,
no build-time scanner**. An unlisted item is an unknown hash at that boundary:
a type can be storage-only (used inside server-fn bodies, never
wire-addressable), and a listed op can be excluded or overridden with a
hardened reimplementation. Per-type static facts (`STACK_SIZE`, `SHAPE`) are
inherent `const`s on the struct, reached through the matched arm's concrete
type — not a duplicated data table. A sum type with one variant per struct is
deliberately avoided: it is sized to its largest variant and grows with the
schema; a bare `match` to monomorphized arms costs nothing at runtime and
scales to any schema size. Server functions share the **same `STRUCT_HASH`
space** — a function's hash is composed by SeaHash from its argument/return
objects' `STRUCT_HASH`es, so there is **no separate `FN_HASH`**. The mechanism
lives in
[`wavedb-macros` § exposure](../wavedb-macros/README.md#exposure--the-declared-registry).

---

## How a call runs: local store + network

The developer only ever writes the typed call — `unique_var.save(&db)`,
`T::get(&db)`, the collection methods. Each one does **two things internally**,
and nothing else leaks:

1. **write-through to the local store** — a key→value map of `Id → wire bytes`
   kept on the client. Gives warm reads and survives reconnects.
2. **send to the owner node** over `wavedb-net` — the node is the **authoritative
   writer**: it runs the real engine (confirm `Id`, pages, `Pivot`/`BpTree`,
   history, replication) and acks.

```rust
unique_var.save(&db).await?;   // ← the only thing the app writes

// inside (lives in `wavedb`, over the Db handle):
let bytes = unique_var.to_wire();      // [STRUCT_HASH][Metadata][body]
db.local().update(id, &bytes).await?;   // 1. local write-through (Store)
db.send(Op::Save, bytes).await?;        // 2. network → node
```

The `Id` is known client-side without a round-trip: a Unique anchor is
deterministic (`STRUCT_HASH · TENANT · 1 · 0`); a NonUnique record's identity `Id`
is minted at `insert` (`CREATED_AT` + tenant + random `SALT`). So the
write-through happens immediately; the node confirms.

## `Store` — the local backend trait

`Store` is the **backend seam** — the only thing that differs native vs web. Core
declares the contract (async, no I/O — WASM/macro-safe). It is key→value over `Id`
plus an **atomic batch** (`apply`) so a multi-record op (record + `BpTree` node)
commits all-or-nothing:

```rust
pub trait Store {                  // key→value over Id + wire bytes
    async fn get(&self, id: Id) -> Result<Option<Vec<u8>>>;
    // Type-directed fetch: the typed layers (Collection, BpTree) always know
    // the value's STRUCT_HASH at compile time, so a backend with per-type
    // storage (PageStore's StructStorage statics) routes straight to that
    // type's slot. Default = plain `get` — flat backends need nothing extra.
    async fn get_of(&self, struct_hash: u64, id: Id) -> Result<Option<Vec<u8>>>;
    async fn apply(&self, batch: &[Write]) -> Result<()>;  // atomic: all-or-nothing
    // …and resolved only once durable (RFC 0072). Default = `apply`: a store
    // with no settle of its own (IndexedDB) is durable when `apply` resolves.
    async fn apply_durable(&self, batch: &[Write]) -> Result<()>;
}
// `Put`'s type is the STRUCT_HASH at the head of its bytes; the other two
// carry it, because an Id names no type and a per-type backend would
// otherwise have to search every slot for one (RFC 0063).
pub enum Write {
    Put(Id, Vec<u8>),
    Remove(u64, Id),
    Expect(u64, Id, Option<Vec<u8>>),
}
```

- **native node** impl: the page engine (`wavedb-storage`) — `apply` = one batch
  committed to the record cache, atomic; durable at the next settle (RFC 0072),
  and `apply_durable` waits for that settle.
- **web** impl: IndexedDB — `apply` = one IDB readwrite transaction.
- **native client** impl: the same page engine on a carrier thread of its own,
  reached by channel through a proxy store (`wavedb::cache::CacheStore`).

**Durable operations.** Every write of the typed surface has a `_durable`
twin — `save_durable`, `insert_durable`, `update_durable`,
`remove_durable`, `create_pivot_durable` — answered once its batch is
durable. None of them is written twice: `Durability<S>` is a store whose
`apply` goes to `apply_durable` on a flag, and a durable operation is the
plain one over it (`LocalHandle`), a `NodeOp` carrying the flag
(`RouteHandle`, `ServerDb`), or a `CommandFrame` with its `durable` bit set
(the client `Db`).

The pages/headers/allocator live **behind** this trait, inside the native page
`Store`. The `Pivot`/`BpTree` logic sits **above** it (the `index` contracts below)
and is `Store`-generic — so the **same index code runs on the node (PageStore) and
on web (IndexedDB)**; only the backend swaps.

**Reads.** `get(id)` checks the local `Store` first; on a miss it fetches from the
node and back-fills. Collection queries (`all` / `by_field`) need the `BpTree` —
which is `Store`-generic, so it runs **wherever the engine is linked**: on the node
(PageStore) for a thin client, or **in-browser over IndexedDB** for a serverless app.

## Index contracts — `Pivot`, `BpTree`, `IndexKey`

The index lives **above** `Store` and depends only on it (`get` + `apply`), so it is
**portable** — pages and headers are `PageStore` internals, never named here. Core
declares the contracts; the same code compiles for the node (PageStore) and the web
(IndexedDB).

```rust
/// Order-preserving key encoding: byte order == value order, so the `BpTree`
/// compares keys by memcmp with no decode. Macro-implemented per indexed type
/// (u64 → big-endian, i64 → sign-flipped, String → 0x00-terminated, tuples → concat).
pub trait IndexKey {
    fn encode_key(&self, out: &mut Vec<u8>);
}

/// The collection's roots holder. `#[wavedb]` generates one per NonUnique type;
/// this trait is the portable shape the engine reads and rewrites. No element
/// counter — the `Pivot` is rewritten only when a `BpTree` root moves or its
/// default permission changes (rare, not a per-write cost).
///
/// Root pointers are `LocalId` (80-bit): the tree is tenant-scoped so `TENANT`
/// is derivable from context — no need to repeat 6 bytes per pointer.
pub trait Pivot: Wire + Sized {
    const STRUCT_HASH: u64;                       // identity of the stored pivot record

    fn current(&self)     -> LocalId;             // living-records B+tree root
    fn dead(&self)        -> LocalId;             // deleted-records B+tree root
    fn secondaries(&self) -> &[LocalId];          // one root per `#[wavedb::pivot(...)]`
    fn permission(&self)  -> Option<&PermissionRef>; // collection default; record metadata overrides
    // engine writes back every moved root (current, dead, one per secondary)
    fn replace_roots(&self, current: LocalId, dead: LocalId, secondaries: &[LocalId]) -> Self;
}

/// A search bound over the order-preserving key space.
pub enum Bound {
    All,
    Exact(Vec<u8>),
    Range { lo: Vec<u8>, hi: Vec<u8> },  // half-open [lo, hi)
    Prefix(Vec<u8>),
}

/// A B+tree index over any `Store`, generic over its key (no trait objects):
/// nodes are values read by `LocalId`, all I/O is delegated to `Store`, so the
/// same type serves native PageStore and web IndexedDB. The **primary** tree
/// is `BpTree<LocalId>` (the default — the record pointer is the key, order =
/// `CREATED_AT`); a **secondary** index is `BpTree<SecKey>`, keyed by the
/// record's `IndexKey`-encoded field bytes plus its `LocalId` (unique under
/// duplicate values). It carries `tenant` because node pointers are
/// tenant-stripped `LocalId`s that must inflate to full `Id`s for
/// `Store::get`. `search` returns full record `Id`s (two-phase: index →
/// `Id`s → fetch).
pub struct BpTree<K: NodeKey = LocalId> { /* root: LocalId, tenant: U48, caps */ }
pub struct SecKey { pub field: Vec<u8>, pub rec: LocalId }

impl BpTree {
    pub const fn at(root: LocalId, tenant: U48) -> Self;         // open a tree at a root
    pub async fn create<S: Store>(store: &S, tenant: U48) -> Result<Self>; // empty tree
    pub const fn root(&self) -> LocalId;                         // current root pointer

    pub fn search<S: Store>(&self, store: &S, bound: Bound)
        -> impl Stream<Item = Result<Id>>;                       // walk → matching record Ids

    pub async fn insert<S: Store>(&mut self, store: &S, id: Id) -> Result<()>;
    pub async fn remove<S: Store>(&mut self, store: &S, id: Id) -> Result<bool>;
}
```

`insert`/`remove` take a record `Id` (full 128-bit — external address) and key it
by its 10-byte `LocalId` (order = `CREATED_AT`); they update `root()` in place
when a split or collapse moves it. When a root moves the holder rewrites the
`Pivot`, otherwise the `Pivot` stays immutable. `remove` merges or redistributes
underfull nodes (<¼ capacity) with a sibling and collapses an empty root, so
deleted space is reclaimed. `search` prunes descent by the bound's `CREATED_AT`
range. Comparison is byte order on the encoded key (== `Ordering`), never a
typed decode; secondary indexes will key by `IndexKey`-encoded bytes on the
same machinery. Each mutating op also has a `plan_*` variant returning the node
`Write`s without applying, so a caller can fold index + record + `Pivot` into
**one atomic batch**.

### `Collection<T>` — the layer application code actually touches

The `BpTree` is engine-internal. What `#[wavedb(NonUnique)]` hands application
code is the **typed collection** (`wavedb_core::Collection<T>`), driven through
generated wrappers — no raw tree, no raw `Id` minting, no pivot bookkeeping:

```rust
let todos = Todo::create_pivot(&store, tenant).await?; // explicit, never automatic
let col   = Todo::collection(todos, tenant);           // cheap typed handle

let id = col.insert(&store, &Todo { title, completed: false }).await?; // → stable Id
col.save(&store, id, &updated).await?;    // update at the same Id (no reindex)
col.remove(&store, id).await?;            // current → dead; bytes kept (history)
col.get(&store, id).await?;               // direct address → Option<Todo>
col.all(&store);                          // Stream<Item = Result<(Id, Todo)>>, newest write first
```

Every mutating op is **one `Store::apply` batch** (record + touched nodes +
`Pivot` rewrite when a root moved); every stored value is enveloped as
`[STRUCT_HASH (8 B LE)][wire]` and decode verifies the head, so a foreign `Id`
can't mis-decode. `Unique` types instead get anchor ops — `T::get(store,
tenant)` / `value.save(store, tenant)` (save **is** the upsert) — and since
they don't implement `NonUniqueStruct`, driving one through a collection is a
compile error.

### BpTree node encoding

A BpTree node is an ordinary `STRUCT_HASH`-headed value packed into the shared
per-`STRUCT_HASH` page directory — **not** a dedicated one-node-per-page format.
(A 32 KiB page-per-node layout was measured to waste the dominant B2C case of
millions of small trees, and dropped —
[RFC 0031](../../rfcs/0031-node-per-page-bptree-DEPRECATED.md).) Both node kinds
use the same 18-byte entry:

```
entry = [ key: [u8; 8] ][ LocalId: 10 bytes ]
```

- **Internal node**: `LocalId` = child node pointer.
- **Leaf node**: `LocalId` = NonUnique record pointer.

All `LocalId`s inflate to a full `Id` via `local_id.to_id(tenant)` — 2–3 CPU
cycles, never disk. Node layout and split/merge policy live in
[RFC 0011](../../rfcs/0011-bptree-index-and-collections.md).

### Composite — set algebra on `Id` streams

Combining indexes (the no-DSL composite query) is free functions over the `Id`
streams `search` yields — `Store`-agnostic, so they too run native or web:

```rust
pub trait IdStreamExt: Stream<Item = Result<Id>> + Sized {
    fn intersect<S>(self, other: S) -> Intersect<Self, S>; // AND
    fn union<S>(self, other: S)     -> Union<Self, S>;     // OR  (dedup)
    fn except<S>(self, other: S)    -> Except<Self, S>;    // NOT (difference)
}
```

Streams from different indexes arrive in different orders, so `intersect`/`except`
buffer the **smaller** side into an `Id` set and probe the larger; `union` merges +
dedups. A `#[server]` body composes these, then resolves survivors with a fetch.

## Typed object traits (per struct, macro-implemented)

The calls above are surfaced as **typed traits, one per shape**, that `#[wavedb]`
implements for each struct. The methods route through the `Db` handle (which owns
the local `Store` and the transport) — they never expose `Store` or the network to
app code.

```rust
// Unique
pub trait UniqueObject: WaveDbStruct {
    async fn get(db: &Db) -> Result<Option<Self>>;   // local → (miss) node
    async fn save(&self, db: &Db) -> Result<()>;     // local write-through + send
}

// NonUnique — collection ops open a handle from a stored PivotId.
pub trait NonUniqueObject: WaveDbStruct + Sized {
    fn collection(db: &Db, pivot: PivotId) -> Collection<Self>;
    async fn save(&self, db: &Db) -> Result<()>;     // update at identity Id (local + send)
}

impl<T: NonUniqueObject> Collection<T> {
    async fn insert(&self, db: &Db, record: T) -> Result<Id>;  // mint Id, local + send
    async fn get(&self, db: &Db, id: Id) -> Result<Option<T>>;
    fn all(&self, db: &Db) -> impl Stream<Item = Result<T>>;   // node-side BpTree walk, streamed
    async fn remove(&self, db: &Db, id: Id) -> Result<()>;
    // + a `by_<field>` lookup per `#[wavedb::pivot(...)]` secondary index, also a Stream.
}
```

**Collection reads are async iterators.** `all` and every generated `by_<field>`
return `impl Stream<Item = Result<T>>`, never a buffered `Vec`: the two-phase
BpTree walk (index → `Id`s → fetch) streams records as it resolves each `Id`, so a
caller can stop early without materialising the whole collection. Use
`.try_collect().await?` when a `Vec` is actually wanted. The prelude re-exports
`Stream` / `StreamExt`.

The macro picks `UniqueObject` vs `NonUniqueObject` by shape; `db: &Db` is the
macro's generic `__WaveDbDb` parameter, resolved at the call site (no `dyn`). The
same typed code compiles native and wasm — only the local `Store` impl swaps.
