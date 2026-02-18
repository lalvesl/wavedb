# wavedb-macros

The compile-time front door. `#[wavedb]` turns a plain Rust struct into a WaveDB
object; `#[server]` turns an `async fn` into a server-only body with a client call
binding. The derives generate every item's **execution steps** in place; the
**registry** that ties them together is an explicit **exposure declaration**
(`expose_server!` / `expose_client!`, below) — nothing is wire-reachable unless
listed. All schema rules are written once and enforced on client and server
because both compile this crate.

> For the project-wide idea and quickstart see the
> [root README](../../readme.md).

## Module map

| Module              | Responsibility                                                  |
| ------------------- | --------------------------------------------------------------- |
| `lib`               | The `#[wavedb]` and `#[server]` macro entry points.             |
| `server`            | `#[server]`: server body + client stub + composed `STRUCT_HASH`. |
| `args`              | Parse `#[wavedb(...)]` attribute arguments.                     |
| `struct_hash`       | Compute the `STRUCT_HASH: u64` const from name/shape/fields.    |
| `wire_derive`       | `WaveWire` — the no-serde, no-`repr(C)` `WaveWire` impl.            |
| `generated`         | Auto-emit the per-NonUnique `Pivot` + `PivotId` types.          |
| `crud`              | Generated accessors / CRUD glue (the per-item execution steps). |
| `expose`            | `expose_server!` / `expose_client!` — the declared registry.    |
| `codegen` / `utils` | Shared emit helpers.                                            |

> There is **no build-time scanner and no `build.rs` step**. The former
> `wavedb-build` crate (a `src/`-walking `generate_registry()`) is removed:
> discovery is replaced by explicit declaration. See _Exposure_ below.

---

## What `#[wavedb]` does at compile time

1. **Computes `STRUCT_HASH: u64`** — **SeaHash** (the pinned `seahash` crate) over
   `STRUCT_NAME + SHAPE + each PROPERTY_NAME + each PROPERTY_TYPE`, with a fixed
   four-lane WaveDB seed. SeaHash is portable across every build, machine,
   architecture, and endianness, so clients and servers always agree on a type's
   identity; the crate is pinned exactly so a version bump can't silently change
   every hash. Because field names and types are folded in, **any schema change
   changes the hash** — a changed struct is simply a different type. No `version =`
   attribute, no version-from-type-name rule; identity is the hash.
2. **Implements `Id` + `Metadata` accessors.** `.tenant_id()`, `.key()`,
   `.created_at()`, `.struct_hash()`, plus full `Metadata` getters/setters — no
   call-site boilerplate.
3. **Emits the `WaveWire` impl.** Byte layout is defined explicitly by `WaveWire`
   (see `docs/wire_format.md`) — no `serde`, no `repr(C)`. The macro decides
   every field's stack/heap placement, so layout never depends on the Rust
   compiler's struct representation.
4. **Generates the collection machinery for `NonUnique`** — a `Pivot` type
   naming every chain the collection holds, one secondary chain per
   `#[wavedb::pivot(...)]` attribute among them (below).
5. **Implements the typed object trait by shape** — `UniqueObject` (`get` /
   `save`) for Unique, `NonUniqueObject` (`collection` → `insert`/`get`/`all`/
   `remove`, plus record `save`) for NonUnique. On the **client** each method
   routes through the `Db` handle: **write-through to the local `Store` + send a
   command frame over the network** to the node. On the **node** the macro emits
   the matching compile-time **engine fns** (`get`/`save` for Unique;
   `insert`/`update`/`remove` for NonUnique) that the registry's `match command`
   calls — these drive the storage internals (block allocator, record cache, the
   object's page directory, `Pivot`/`Chain`) directly, monomorphized, no `dyn`.
   So the write half of the wire command set is `Get`/`Save` (Unique) and
   `Insert`/`Update`/`Remove` (NonUnique); the client-side `save()` method emits
   the `Update` command for NonUnique. The read half adds `All`, `History`,
   `Changes` (catch-up) and — for declared lists — `Listed`/`ListLen`. Same typed calls compile native and wasm —
   only the local [`Store`](../wavedb-core/README.md#store--the-local-backend-trait) swaps.
   Every write has a `_durable` twin — `save_durable` (Unique),
   `create_pivot_durable` (NonUnique), and on the collection handle
   `insert_durable` / `save_durable` / `remove_durable` — answered once the
   write is durable; on the wire it is the same command with the frame's
   `durable` bit set, so the engine steps are not duplicated
   ([RFC 0072](../../rfcs/0072-the-settle-is-the-durability-point.md) §12).
6. **Wires up the schema-evolution hooks** — the optional `first_try` /
   `fallback_not_found` functions; see
   [`wavedb-core`](../wavedb-core/README.md#schema-evolution--lookup-hooks).
7. **Emits the type's native storage declaration** (`#[cfg(not(target_arch =
   "wasm32"))]` only) — a `static wavedb_storage::StructStorage` carrying this
   type's `STRUCT_HASH` and compression policy, reached as
   `T::struct_storage()`, plus `T::storage_entries()` (the declarations —
   record + generated Pivot + lanes — to register at `PageStore::open`). A
   compile-time registry instead of runtime registration; each open store
   keeps the types' caches, directories and dictionaries itself, one lock per
   type (RFC 0067). `#[wavedb(compress = false)]` opts the type's pages out
   of zstd (storage policy — never folded into `STRUCT_HASH`). Consequence: on
   non-wasm targets a schema crate needs `wavedb-storage` as a target-gated
   dependency (the wasm expansion omits the slots — IndexedDB has no pages).
   See [`wavedb-storage` § per-type state](../wavedb-storage/README.md#per-type-state-is-compile-time--structstorage).

```rust
#[wavedb]                       // Unique by default
pub struct AboutUser {
    pub name: String,
    pub surname: String,
    pub phone: String,
    pub address: String,
}
```

All `#[wavedb]` structs serialize through WaveDB's own `WaveWire` format; the hook
fns are generic over `Db` so the macro's `__WaveDbDb` parameter resolves at the
call site.

---

## Data shapes

| Marker (none = Unique) | Cardinality per tenant                       | ID `KEY`               |
| ---------------------- | -------------------------------------------- | ---------------------- |
| _(default)_ **Unique** | Exactly one live record per tenant           | `STRUCT_HASH` (anchor) |
| `NonUnique`            | Many per tenant; may nest in other NonUnique | `CREATED_AT`           |

```rust
#[wavedb(NonUnique)]
pub struct Order {
    pub amount: u64,
    pub note: String,
}
```

`NestedNonUnique` is **not a separate marker** — you nest by holding a `PivotId`
of another NonUnique collection in a field. The collection is reached by
`get()`-ing that pivot (see below).

---

## Generated collection types (`Pivot` + `PivotId`)

`#[wavedb(NonUnique)]` auto-generates the two helper **types** per family. They
carry no business data — pure addressing — so applications rarely name them
directly; they reference a collection through its `PivotId`.

```rust
// Generated type. The handle for one NonUnique collection: the roots of every
// chain it holds (RFC 0050, RFC 0075). No counter — a size field would force a
// Pivot write on every insert/remove — and a chain's roots are permanent, so
// the Pivot is written at creation and then essentially never.
pub struct NotePivot {
    pub recency:     ChainRoots,            // living records, by live instant
    pub removals:    LogRoots,              // the removal log
    pub secondaries: [ChainRoots; S],       // one per `#[wavedb::pivot(...)]`
    pub lists:       [ChainRoots; L],       // one per `#[wavedb::list]`
    pub fuzzy:       [ChainRoots; F],       // one per `#[wavedb::fuzzy]`
    pub permission:  Option<PermissionRef>, // collection default; record Metadata overrides
}
```

### The type is generated; the **instance is created explicitly**

The macro emits the `Pivot` _type_, but a `Pivot` _instance_ is **not
created automatically**. A collection comes into being when you **call create on
its `Pivot`** — there is **one `Pivot` per tenant per definition** (per NonUnique
struct type) — and then **store the returned `PivotId`** in a field of a `Unique`
struct or of a nesting `NonUnique` (recursive collections). No stored `PivotId`
⇒ no collection; the holder owns the handle.

Referencing a collection from another object:

```rust
#[wavedb]
pub struct UserInterestedFruits {
    // a handle into a NonUnique collection of Fruit, reached via its Pivot
    pub list_of_fruits: <Fruit as WaveDbStruct>::PivotId,
}
```

> The generated `Pivot` types share a field shape across
> families, so their `STRUCT_HASH` may collide. That is harmless — each is
> addressed only within its own per-`STRUCT_HASH` directory and tenant/collection
> context, and the type is never inferred from the `Id` (the 15-bit `SALT` is pure
> collision breaker — see
> [`wavedb-core`](../wavedb-core/README.md#the-salt-field-15-bits)).

### Secondary indexes — `#[wavedb::pivot(...)]`

By default a NonUnique collection orders by recency only (its recency chain).
Declare **secondary indexes on properties** with the repeatable
`#[wavedb::pivot(...)]` attribute — single field or a composite tuple — each a
pointer chain of `[field][anchor]` keys in a lane of its own (RFC 0075):

```rust
#[wavedb(NonUnique)]
#[wavedb::pivot(amount)]              // a chain keyed by `amount`
#[wavedb::pivot((customer, date))]   // a composite chain keyed by (customer, date)
pub struct Order {
    pub amount: u64,
    pub customer: u64,
    pub date: u64,
}
```

Per declared pivot the macro:

- adds a **`ChainRoots` to the `Pivot`** (one per index), and folds the
  declaration into the `STRUCT_HASH` as `#pivot{i}` — reordering or retyping
  them is a new type;
- generates a **typed lookup** on the collection handle — e.g. `by_amount(&db, v)`
  / a range query, and `by_customer_date(&db, (c, d))` — returning a record async
  iterator (`impl Stream<Item = Result<T>>`), resolved two-phase (index → `Id`s →
  fetch): one descent to the bound's least key, then the chain in order.

**Maintenance cost.** `insert` and `remove` index / de-index the record in
every secondary; a `save` (update) **re-keys each secondary whose field values
changed** — the old key comes out, the new key goes in, all inside the op's one
atomic batch (the old record's bytes supply the stale keys). A secondary write
rewrites one segment of pointers and never the `Pivot`. Cost scales with the
secondary-index count (see
[`wavedb-storage`](../wavedb-storage/README.md#io-cost-per-operation)):
secondary indexes are a write-amplification trade — faster lookups, costlier
updates.

> **Status: implemented** through `Collection` — `#[wavedb::pivot(...)]` emits
> the key hooks (`NonUniqueStruct::secondary_key`) plus a typed lookup trait
> `{Name}Secondaries` implemented for `Collection<{Name}>` (`by_amount`,
> `by_customer_date`, … — `String` fields take `&str`). Range/prefix scans go
> through `Collection::search_by(index, Bound)` directly.

---

## Validation & preprocessing hooks

Two attributes, run identically native and wasm32, dispatched through a static
monomorphised fn table (no `dyn`, no async in the WASM binary):

```rust
#[wavedb(NonUnique, validate = validate_payment, preprocess = preprocess_payment)]
pub struct Payment { pub amount_cents: u64, pub currency: String }
```

| Hook         | Client (pre-send)                | Node (pre-commit)                         | Purpose                       |
| ------------ | -------------------------------- | ----------------------------------------- | ----------------------------- |
| `validate`   | ✓ — typed error, zero round-trip | ✓ — the security boundary                 | invariant checks              |
| `preprocess` | ✗                                | ✓ — re-encoded result is what's committed | normalisation, derived fields |

The node runs `validate` **before** `preprocess`. Hooks are synchronous and
pure; hook-less types skip decode entirely via compile-time `HAS_VALIDATE` /
`HAS_PREPROCESS` consts. Node-side enforcement (the gate order) is documented in
[`wavedb-quick-node`](../wavedb-quick-node/README.md#node-side-enforcement).

---

## Server functions — `#[server]`

The replacement for a query DSL, and the "backend" half of full-stack. A server
function is an `async fn` whose **body exists only on the server** (it has DB
access) but which the client can **call** as if it were local. `#[server]`
generates the binding; arguments and the return value travel through `WaveWire` over
[`wavedb-net`](../wavedb-net/README.md).

```rust
#[server]
fn orders_over(db: &Db, min: u64) -> impl Stream<Item = Result<Order>> {
    // compiled ONLY into the node binary; full DB access
    Order::all(db).try_filter(|o| future::ready(o.amount > min)) // streamed over the wire
}

// On any client (native or wasm) the same name is a thin stub — an async iterator:
let big: Vec<Order> = orders_over(&db, 100).try_collect().await?;
```

What the macro emits:

| Side               | What is compiled                                                                                                                                                                                                                   |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Server** (`cfg`) | The real body + a dispatch arm under the function's own `STRUCT_HASH` (below) so the node can route an incoming call.                                                                                                              |
| **Client** (`cfg`) | A stub with the **same signature**: `WaveWire`-encode the args → send a `CommandFrame { struct_hash, command, payload = args }` over `wavedb-net` (the **same** frame an object op uses — no separate call frame) → `WaveWire`-decode the return. The body is **not** in the binary (keeps wasm small, keeps server logic private). |

> **Status: implemented — the side features.** The `cfg` in the table is
> real and its names are pinned: a schema crate declares two cargo features
> named exactly **`server-side`** and **`client-side`**, and the macros gate
> emission on them —
>
> - `server-side` → the `#[server]` **body** and its `__wavedb_dispatch`
>   step, plus the whole `expose_server!` output (`REGISTRY` and every
>   dispatch arm);
> - `client-side` → the **client stub** and the whole `expose_client!`
>   output (`CLIENT_REGISTRY`);
> - always (both sides) → the fn-type and its `STRUCT_HASH`, and everything
>   `#[wavedb]` emits for structs — the schema IS the protocol.
>
> A deployed binary depends on the schema with `default-features = false`
> and exactly its side (see `examples/todo-app/{server,client}/Cargo.toml`),
> so the other side's code is **never compiled** into the artifact — no
> string, constant, or code path of a `#[server]` body can reach a client
> build. This is deliberately stronger than trusting LTO/dead-code
> elimination. The convention is `default = ["server-side", "client-side"]`
> so the schema crate's own tests drive the full loop; server-only deps ride
> the feature (`server-side = ["dep:sha2"]`). Two duties stay with the
> schema author: (1) helpers written *outside* `#[server]` bodies that serve
> them must carry `#[cfg(feature = "server-side")]` by hand (todo-app's
> `hash_password` et al. are the worked example); (2) never enable
> `server-side` for a wasm32 build — `expose_server!` hard-refuses it with a
> `compile_error!` (a browser is never a node). The guarantee applies to the
> artifact's own build graph (`cargo build -p <client>`): a `--workspace`
> build unifies features with the schema's default-on test build.

- **A function has a `STRUCT_HASH: u64` too — there is no separate `FN_HASH`.** It
  is computed at compile time by the **same SeaHash + fixed four-lane WaveDB seed**
  as a struct's, but **composed from its input/output objects' `STRUCT_HASH`es**:
  `seahash_wavedb(fn_name + each ARG::STRUCT_HASH + RETURN::STRUCT_HASH)`. Builtin
  args (`u64`, `String`, …) fold a fixed per-type wire tag so the whole expression
  stays a `const`. Because an argument object's hash is folded in, **evolving any
  input type's schema changes that object's `STRUCT_HASH` and therefore the
  function's** — schema evolution propagates transitively into the call identity,
  and a signature change is a new function, caught at the boundary. Functions and
  structs share **one uniform `STRUCT_HASH` space**, routed by the same
  per-`STRUCT_HASH` `match`.
- **Wire bounds** — every argument and the return type must implement `WaveWire`; the
  `db: &Db` receiver is supplied by the node, never sent. A collection-shaped
  return is an `impl Stream<Item = Result<T>>` whose **item** `T: WaveWire` — the macro
  ships items one at a time over the wire (back-pressured), and the client stub
  re-exposes the same async iterator instead of buffering a `Vec`.
- **Auth** — **every server function requires a logged-in session by default**;
  `#[server(public)]` opens one to anyone (incl. the unauthenticated tier,
  `user = U48::MAX`) — that is how `login` / `refresh` are reachable before an
  access token exists. The macro injects the auth guard **into the generated
  body**, not into the registry `match`: the dispatch only routes
  `struct_hash → body`, so the build-time match stays uniform (one arm per fn, no
  per-fn auth policy) — a deliberately simpler builder. Identity inside the body
  is the verified Access token's `user`/`tenant`, never the request body.
- **Security** — the body runs on the node, so permission checks and validation
  apply there; the client cannot bypass them by crafting a request, only call the
  declared function with typed arguments.

```rust
#[server(public)]                            // open: reachable before login
async fn login(db: &Db, user: U48, password: String) -> Result<Tokens> { /* … */ }
```

Server functions are aggregated into the same per-`STRUCT_HASH` dispatch as
structs (below), so a node links them the same way it links the schema.

---

## Exposure — the declared registry

The registry that lets **storage, server, and client "know the structs"** is
**declared, not discovered**. There is no `build.rs` scanner (the former
`wavedb-build` crate is removed); nothing becomes wire-reachable as a side
effect of merely existing.

> **Status: implemented for structs.** `expose_server!` emits a zero-sized
> `ServerRegistry` + `REGISTRY` implementing `wavedb_core::expose::Exposure`
> (`knows` / `decode_check` / `async execute` — one `match` per operation
> over the listed items' generated `__wavedb_<op>` steps, with per-op
> `never` exclusion and path overrides substituted inside the arm);
> `expose_client!` emits `ClientRegistry` / `CLIENT_REGISTRY` with the
> reachability half. Every refusal — unlisted type, excluded op, wrong-shape
> command — is a uniform `UnknownStructHash`. `#[server]` functions join the
> same lists in M4 (they need `Db`); streaming reads (`All`) join with the
> transport. The whole `expose_server!` emission rides the schema crate's
> `server-side` feature and `expose_client!`'s its `client-side` feature —
> the same no-leak contract as `#[server]` (see the status note in _Server
> functions_ above); per-op **overrides** are server code and must carry
> `#[cfg(feature = "server-side")]` at their definition (schema-smoke's
> `audited_invoice_save` is the worked example).

**Division of labor (the two halves).**

- **`#[wavedb]` / `#[server]` (proc-macros)** do all the _per-item_ work, in
  place: `STRUCT_HASH` const, `WaveWire` impl, `PivotId`/`Pivot` types,
  hook wiring — **and the item's execution steps**: the shape's engine ops
  (`get`/`save` for Unique; `insert`/`update`/`remove`/`get`/`search` for
  NonUnique) and a function's call arm. Each item is fully self-contained
  after the macro runs — defined, but not yet reachable.
- **`expose_server!` / `expose_client!` (declaration macros)** do the
  _aggregation_: an explicit module on each side lists which items that side
  serves (node) or calls (client), and the macro expands the list into **one
  `match struct_hash { … }` per operation** referencing the macro-generated
  paths. The lists **are** the registry. There is **no `Object` enum** — see
  below.

```rust
// server.rs — compiled into the node. Unlisted ⇒ unknown hash ⇒ refused.
wavedb::expose_server! {
    AboutUser,                                   // full generated op set
    Note,
    Order { save: audited_save, remove: never }, // per-op override / exclusion
    login, refresh, pinned_notes,                // #[server] functions
}

// client.rs — compiled into the client binary: which stubs can route.
wavedb::expose_client! { AboutUser, Note, Order, login, refresh, pinned_notes }
```

**Why explicit (the security surface).** The old scanner exposed every
`#[wavedb]` struct under `src/` to the full command set. Exposure inverts
that into an **allowlist**:

- **Storage-only types.** A struct can exist, be stored, and be read/written
  inside `#[server]` bodies without ever being nameable from the wire —
  `Credentials` and `Session` in
  [`docs/example_auth.md`](../../docs/example_auth.md) are the canonical case:
  `login` reads them; no client command can.
- **Per-op exclusion.** `remove: never` serves a collection read/insert-only —
  the arm is simply absent, so the command fails as unknown, not as "denied".
- **Per-op override.** `save: audited_save` swaps the generated step for a
  hardened reimplementation (extra invariants, audit, rate limits) with the
  same signature — the dispatch shape doesn't change, only the arm's body.
- **Asymmetric surfaces.** Client and server lists are independent: an
  admin-only server fn is exposed server-side but left out of the public
  client build.

**Scope: anything you can path-reference.** Because entries are ordinary Rust
paths, the old scanner limits disappear: items from **dependency crates**,
`cfg`-gated items, and macro-generated items all register fine — you name
them. The trade is deliberate: auto-discovery is gone; adding a struct means
adding a line to the exposure list, and forgetting it fails loudly (unknown
hash at the boundary), never silently over-exposes.

> **Implementation constraint — no `dyn`, ever.** `expose_server!` /
> `expose_client!` must expand to **static dispatch only**: a plain `match` on
> the `STRUCT_HASH` whose arms call the concrete, monomorphized fns by path.
> Forbidden in the expansion: trait objects (`Box<dyn …>` / `&dyn …`),
> fn-pointer registration tables, any runtime registry the lists get inserted
> into. This applies to **overrides too** — `save: audited_save` substitutes
> the path *inside the arm* at expansion time (the compiler resolves and can
> inline it), it does not park a callback in a table. Rationale: the optimiser
> sees through every arm (no vtable hop, no allocation), the wasm binary
> carries only the listed items' code, and the `match` scales with schema size
> where a dispatch table or sum type cannot.

### Dispatch: a `match` on `STRUCT_HASH` — no sum type

The expanded artifact is **not** an enum with one variant per struct. A sum type
like that is sized to its largest variant (memory waste), grows and recompiles as
the schema grows, and forces every consumer to handle every variant — it does not
scale. Instead the exposure macro emits, **per operation, one `match` on the
64-bit `STRUCT_HASH`** whose arms decode into the **concrete** type *inside the
arm*, do the work, and return a uniform result. The concrete type never escapes
the arm, so there is **no `Object` value, no trait object, no `dyn`** — only
monomorphized calls the optimiser sees through:

```rust
// expose_server!-expanded — no `Object` enum. One `match` per operation;
// structs and server functions live in the SAME `STRUCT_HASH` space.
pub fn decode_validate(struct_hash: u64, bytes: &[u8]) -> wavedb_core::Result<()> {
    match struct_hash {
        crate::user::AboutUser::STRUCT_HASH => validate::<crate::user::AboutUser>(bytes),
        crate::notes::Note::STRUCT_HASH     => validate::<crate::notes::Note>(bytes),
        crate::srv::login::STRUCT_HASH      => call::<crate::srv::login>(bytes), // server fn
        _ => Err(wavedb_core::Error::UnknownStruct(struct_hash)),
    }
}

// Each arm's helper is generic — instantiated once per matched type, nothing boxed.
fn validate<T: WaveDbStruct>(bytes: &[u8]) -> wavedb_core::Result<()> {
    T::validate(&wavedb_core::from_wire::<T>(bytes)?)
}
```

From a `STRUCT_HASH` (read off the wire envelope or the `Id`) the expanded
matches give the engine, each as its own `match` over the **listed** items:

- the right **wire parser** — decode `bytes` into the concrete `T`;
- the server-side **hooks** — `first_try(struct_hash, db, …)` /
  `fallback_not_found(struct_hash, db, …)` route to the declared struct's fns;
- the matching generated **`Pivot` / `PivotId`** types for a collection;
- the **execution steps** — the listed (or overridden) engine op per command;
- the **server-function call** — the *same hash space*: a `STRUCT_HASH` arm decodes
  the `WaveWire` args and invokes the node-side body.

Because each is plain macro-expanded code, every arm is a concrete type the
optimiser monomorphizes fully, and the same matches compile into client,
server, and wasm — the single source of truth, with no per-struct descriptor
table, no sum type to size or maintain, and no build step to run.

> **Planned (future).** The exposure macro is the natural place to grow, by
> static dispatch (still no `dyn`):
>
> - **`update_call`** — an additional generated call kind alongside the current
>   server-function dispatch, for update-shaped operations.
>
> (Secondary indexes via `#[wavedb::pivot(...)]` are specced above, not here.)
