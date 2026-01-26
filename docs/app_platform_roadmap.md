# WaveDB Application Platform — Roadmap

Goal: WaveDB as the foundation of a real full-stack application — **one schema
crate** shared by browser/native clients and by the serving nodes, "Next.js-style"
code sharing where the DB **is** the server and ships as a library.

```
my-app/
├── crates/
│   ├── app-schema/   # #[wavedb] structs + evolution hooks + the exposure
│   │                 #   modules (expose_server!/expose_client!) — compiled
│   │                 #   into EVERY binary below
│   ├── app-server/   # #[server] functions, validation, jobs (server-only)
│   ├── app-node/     # fn main() { wavedb_quick_node::run(config, REGISTRY, hooks) }
│   ├── app-client/   # native client binary / library
│   └── app-web/      # wasm32 build, typed API over IndexedDB + WebSocket
```

One `cargo build` workspace, several artifacts, zero DTO layer: the schema crate
is the protocol.

> Status: clean rebuild. Nothing below is implemented yet — this is the target
> sequence, written against the current design (see the crate READMEs).

---

## M1 — Foundations: schema crate compiles everywhere

The `app-schema` crate (`#[wavedb]` structs + explicit **exposure modules** —
`expose_server!` / `expose_client!`; no `build.rs`, no scanner) builds for
native and `wasm32`, producing `STRUCT_HASH`es, `WaveWire` impls, the
auto-generated `Pivot`/`BpTree` types, the derive-generated **execution steps**
(`get`/`save`/`insert`/`update`/`remove`/`search`, server-fn call arms), and —
from the exposure lists — the **per-`STRUCT_HASH` `match` dispatch** (not an
`Object` enum). This is the keystone — every other milestone consumes the
exposure, and derive-generated ops + the declared per-hash `match` is what lets
storage/server/client all know the structs by static dispatch (no `dyn`, no sum
type), with reachability as an explicit allowlist (unlisted types stay
storage-only; listed ops can be excluded or overridden for hardening).

**Exit:** the schema crate builds on both targets; `from_wire(struct_hash, …)`
round-trips through the exposure dispatch; an **unlisted** struct's command is
refused as an unknown hash; `WaveWire` encode/decode is property-tested.

## M2 — Storage engine

The block manager, per-`STRUCT_HASH` linear-hashed page directory, page format,
dictionaries, and the record-cache / settle pipeline. Since RFC 0072 there is no
journal: the settle is the durability point, and a reopen reconstructs from the
headers each batch ends with.

**Exit:** durable single-node `get`/`save` for Unique and `insert`/`remove` for
NonUnique through `Pivot`/`BpTree`; kill-during-write test recovers cleanly.

## M3 — Registry-aware node

A node that links the schema enforces shapes and serves records. A request is a
**command frame** `{ struct_hash, command, payload }`; the node gates it
(identity from the access token → header → decode → **permission** → `validate` →
`preprocess`) then dispatches **`match struct_hash → match command`** (`Get`/`Save`
for Unique, `Insert`/`Update`/`Remove` for NonUnique) to the type's compile-time
engine fn. Unique `get` and NonUnique collection walks (`Pivot` → `BpTree`) served
from storage. Transport is **HTTP POST only for now** (WebSocket deferred).

**Exit:** clients' `get` and collection reads return real data from storage
through a registry-linked node; cross-tenant read without a grant is refused.

## M4 — Typed client API + server functions, end-to-end

`Db::connect`, typed CRUD (`UniqueObject` / `NonUniqueObject` over the `Store`
trait), collection navigation through `PivotId`, `remove` → dead `BpTree`, and
**server functions** (`#[server]`: server-only body + client
binding, `WaveWire`-encoded args/return over `wavedb-net`, dispatched by the function's
composed `STRUCT_HASH`) —
the replacement for a query DSL. The `first_try` / `fallback_not_found` hooks
bridge mixed-build clusters.

**Exit:** example apps run against a live node instead of in-process mocks; a
filtered read works through a `#[server]` function end to end.

## M5 — Browser target

`wavedb` + `wavedb-net` compile for `wasm32`; IndexedDB key→value store (no pages); `gloo_net`/`fetch` transports behind the same async API. Measure the
registry's per-struct wasm cost.

**Exit:** a browser demo performs typed `save` + collection reads (and a
`#[server]` call) against a node over WebSocket, with IndexedDB caching reads.

## M6 — Local cache & `Db::open`

The core `Store` trait (`get`/`update`/`remove` over `Id` + wire bytes) gains a
local write-through impl: native file store, wasm IndexedDB. Reads hit local
first, miss → fetch from owner → back-fill; writes go to owner and write-through
on ack. Read-your-writes
between the local store and notifications.

**Exit:** client survives node restart with warm local reads.

## M7 — Live sync

Owner node fans out mutations to subscribed sessions (WS push; HTTP piggyback +
idle ticks). Live sync = declared subscriptions + navigation catch-up (journal-cursor design superseded by DB-1 2026-07-17; Bloom filter rejected 2026-07-10). Client event API
(`Order::watch(&db)` over a collection / key).

**Exit:** client A saves; client B's watcher fires within one round-trip (WS) /
one poll tick (HTTP).

## M8 — Auth & permission enforcement

Stateless HMAC **access** tokens (short TTL, carry `user`/`tenant`/expiry/purpose,
verified per request with no store) + a tracked **refresh** token for revocation
(bound to a session record; revoke = mark the record `revoked`). Node derives
identity from the **token, never an unsigned field of the operation** — the token
rides inside the WaveDB request envelope (the POST body), not an HTTP header
(transport stays a dumb tunnel; WebSocket sends it once at handshake). Login is a `#[server]`
function minting the access+refresh pair from either a local Argon2 credential
object **or** an external OAuth/OIDC provider (same path, same pair).
Unauthenticated tier (`user = U48::MAX`) restricted to login + public reads.
**Every `#[server]` fn requires a logged-in session; `#[server(public)]` (e.g.
`login`/`refresh`) opens one to the unauthenticated tier** — the auth guard is
injected into the function **body**, not the registry `match`, so dispatch stays a
uniform `struct_hash → body` router. Permission checks on every
read/write/delete (tenant-only / public / tenant-list) — applied inside
server-function bodies too, since they run on the node. NonUnique permission is
**two-level**: the `Pivot` default seeds inserts / gates collection ops, each
record's `Metadata` overrides (authoritative, keeps `Update` atomic). Full
structure: [`wavedb-net`](../crates/wavedb-net/README.md#authentication).

**Exit:** cross-tenant access without a grant rejected at the node; a server
function rejecting a write surfaces as a typed client error; revoking a session
record blocks its next `Refresh` and its access stops within one short TTL.

## M9 — Developer experience

`cargo-generate` template (the workspace skeleton above, one struct per shape, a
`first_try`/`fallback_not_found` example, node + native + wasm binaries, a local
dev-cluster). "Building an
app on WaveDB" guide; schema-evolution cookbook (`first_try` /
`fallback_not_found` patterns).
Versioning policy for the platform crates.

---

## Deferred (explicitly out of scope for this rebuild)

- **Multi-node cluster** — ring ownership, replication, routing/failover. The
  rebuild targets a **single node** first; durability is the settle. (So the
  async-replication durability window is moot for now.)
- **Cold history tier (slow-node) — removed** for now (not just deferred).
  History stays single-tier in the data files, **grows unbounded**; no pruning,
  compaction, or archive tier yet — accepted.
- **Permission groups.**
- **`STRUCT_HASH`-grained write-ownership** — tenant-grained for now.
- **Cross-tenant read _path_** — the multi-node routing + where the grant is
  enforced when tenant B reads tenant A's data. The permission _model_
  (tenant-list grant in `Metadata`) stays; the serving path is not a problem for
  now.
- **Offline-first reconciliation.**

### Planned exposure extensions (post-rebuild)

The exposure macros are the natural place to grow, all by static
per-`STRUCT_HASH` `match` dispatch (no `dyn`, no sum type):

- **`update_call`** — an additional generated call kind beside the server-function
  dispatch, for update-shaped operations.
- **Secondary indexes — `#[wavedb::pivot(field)]` / `#[wavedb::pivot((f1, f2))]`**
  (design specced): extra `BpTree`s on chosen properties beyond the default
  `CREATED_AT` tree, each adding a `Pivot` root + a typed `by_field` lookup.
  Index-maintenance cost on writes that change an indexed field.

## Sequencing

```
M1 (schema) ─► M2 (storage) ─► M3 (node) ─► M4 (typed E2E) ─► M7 (live sync)
                                   │                │
                                   │                └─► M8 (auth/permissions)
                                   └─► M5 (wasm) ─► M6 (local cache)
                                                          M9 (template) — last
```

## Risks / open questions

| Risk                                                              | Mitigation                                                                                                                      |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Registry code grows the wasm binary as schemas grow               | The registry is just a per-`STRUCT_HASH` `match` (no sum type — nothing sized to the largest variant); the only per-struct code is the `WaveWire` encode/decode the app needs anyway — no descriptor tables, no stored names. M5 measures per-struct cost early. |
| Server functions need stable identity across client/server builds | A server fn's identity is a `STRUCT_HASH` (no separate `FN_HASH`) composed by SeaHash from its argument/return objects' `STRUCT_HASH`es, bound at compile time; a signature change — or a schema change to any argument type — is a new function, caught at the boundary. |
| Runtime abstraction (tokio vs wasm) leaks into public API         | Keep it internal to `wavedb`/`wavedb-net`; public API stays `async fn`.                                                         |
| ID / block-descriptor bit budgets                                 | Resolved: `Id` = `KEY u64·TENANT u48·FLAG 1·SALT 15`; descriptor `u40·u20·u4` (pages + dictionary).                             |
