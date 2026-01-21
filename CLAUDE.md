# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Docs describe the TARGET, not the code

This is a clean rebuild of WaveDB. Every README (root and per-crate) describes the
**target** architecture; much of it is not built. Before assuming a crate, API, or
mechanism exists:

1. Check `Cargo.toml` workspace `members` vs `exclude` — excluded crates do not build.
2. Check `rfcs/` — the numbered design + progress record. `rfcs/README.md` indexes
   every idea with its status and opens with a "Current state" snapshot; filename
   markers (`WIP`/`PLANNED`/`PLANNED-LOW`/`DEPRECATED`) show what is in flight,
   planned, or dropped. (This corpus replaced the former `todo.md`/`todo_done.md`.)

When an idea lands or changes, add or update its RFC in `rfcs/` (process in
`rfcs/0000-rfc-process.md`). READMEs carry `> Status:` blocks where a documented
mechanism isn't built yet — keep that honesty when editing docs.
Smaller deliberate deferrals — "simple now, better later" — live in
`docs/improvements.md`; an entry that grows into a design becomes an RFC.

## Commands

Development runs inside the Nix dev shell (`nix develop`); CI does the same. If already
in the shell (direnv), plain `cargo` works. The pre-commit bar:

```sh
cargo fmt --all
cargo clippy --workspace --all-targets   # zero warnings (pedantic + nursery are warn = live)
cargo test --workspace                   # all green
scripts/check_file_length.sh             # 350 non-test lines per .rs file
```

Other commands:

```sh
cargo test -p wavedb-core                          # one crate
cargo test -p wavedb-storage --test nonunique_collection   # one integration test file
cargo test -p wavedb-core collection::             # filter by test name/path
cargo doc --workspace --no-deps                    # doc build (CI-gated)
cargo deny check                                   # license/advisory audit (CI-gated)
cargo nextest run --workspace --tests --release    # CI e2e job
nix build .#wasm                                   # size-optimised wasm artifact
# browser-only tests (IndexedDB store), from crates/wavedb-wasm — needs the
# nixpkgs chromedriver (the one wasm-pack downloads can't run on NixOS):
nix shell nixpkgs#chromedriver --command bash -c \
  'CHROMEDRIVER=$(which chromedriver) wasm-pack test --headless --chrome'
scripts/browser_demo.sh    # M5 exit: live node + browser demo (starts the
                           # contact-book example node, handles chromedriver)
scripts/registry_size.sh   # per-struct wasm cost of the exposure match
                           # (examples/registry-size at widths 1/16/64)
```

Toolchain is pinned by `rust-toolchain.toml` (1.99.0, edition 2024, includes
wasm32-unknown-unknown). Never build against anything else.

## Hard rules (breaking one is an architecture change)

Full rationale in `docs/development_standards.md`. The load-bearing ones:

- **No `dyn`, no sum-type registries.** All dispatch is a generated `match` on the
  64-bit `STRUCT_HASH` to concrete, monomorphized arms. This applies to macro
  expansions too — no fn-pointer tables, no runtime registration.
- **No serde.** Byte layouts are the `WaveWire` codec (`docs/wire_format.md`):
  `[STACK fixed-size][HEAP variable]`, little-endian, `usize`/`isize` never encodable.
- **`seahash` is pinned `=4.1.0`** — STRUCT_HASH identity is load-bearing; never loosen.
  It is vendored in `crates/seahash` (safe, `const fn`, edition 2024 — thanks to Redox OS);
  `tests/golden.rs` there pins its output to the published 4.1.0 bit for bit, and a
  change to the algorithm is a change to every stored identity.
- **Everything that reaches stored bytes folds into `STRUCT_HASH`.** Fields, shape,
  `#[wavedb::key]`, `compress`, `page = N`, and every `#[wavedb::list]`
  declaration **including their order** (RFCs 0051/0052) — as a synthetic `#name`
  hash entry when it isn't a real field. The engine does **no** migration (RFC 0040), so the only
  coherent answer to "can I change this on live data?" is that changing it yields a
  **new type** and moving the data across is application code. Never add a knob that
  touches stored bytes without folding it: a knob that can be flipped in place is a
  knob that silently falsifies whatever guarantee it declares. Behaviour that never
  reaches disk doesn't fold.
- **No format versioning pre-release (policy).** `FORMAT_VERSION` pinned at 1; on-disk
  layouts change freely between commits with no bump, no migration notes. An older
  database is simply unsupported.
- **Errors are typed per layer** (`wavedb_wire::Error`, `wavedb_core::Error`,
  `StorageError`, net/node/client errors). Never fabricate a foreign layer's error
  inline — convert at the documented seam (`StorageError` → `core::Error::Backend`,
  core → `NodeError::from_core`, etc.). No `unwrap`/`expect`/`panic!` in library paths.
- **File budget: 350 non-test lines per `.rs`** (colocated `#[cfg(test)]` doesn't
  count). Over budget ⇒ split by layer. Complexity thresholds live in `clippy.toml`
  only (single source of truth, ratchet down only) — don't repeat numbers in docs.
- **`async` end to end**; the engine's futures are deliberately non-`Send`
  (one executor per thread: Embassy carriers in the node, RFC 0066) —
  `#![allow(clippy::future_not_send)]` at crate root is the established stance
  in core/storage/quick-node.

## Architecture (bottom-up)

Dependency chain: {`wire`, `platform`} → `core` → {`macros`, `storage`} → `net` →
`quick-node` → `wavedb` → `wavedb-wasm`.
The schema crate compiles into client and node — the schema IS the protocol; there is
no DTO layer and no query DSL (filtered reads = `#[server]` functions).

- **wavedb-platform** — the native ⇄ browser seam, cfg-switched (no traits): `time`
  (`SystemTime` / `Date.now()` — `SystemTime::now()` **panics** on wasm32; + `sleep`), `s3`
  (native only: the object-store client half — ranged GET, conditional PUT,
  DELETE, one-key LIST, SigV4 over RustCrypto; RFC 0070),
  `rand` (`RandomState` keys / `window.crypto`), `http` (the tunnel's **client half**:
  hand-rolled TcpStream POST / `fetch` + streamed body), `ws` (the WebSocket client
  half: hand-rolled RFC 6455 / browser `WebSocket`; `Conn::split()` for reader-task
  patterns), `task` (browser only: `spawn_local` and `spawn_detached`, which runs the
  client manager there — natively it runs on a carrier), `io` (native sockets from
  `async-io`, a node's listeners an `SO_REUSEPORT` group on Linux via `rustix`;
  protocols are generic over `embedded-io-async`), `limits` (native: the
  open-files soft limit raised to the hard one, since a node holds every data
  file open — RFC 0071) and `carrier`
  (native: an OS thread running one Embassy executor whose tasks come from
  static pools — the node runs on these).
  Everything above must route clock/entropy/client-HTTP through it — never name
  `SystemTime` or a socket directly.
- **wavedb-wire / wavedb-wire-derive** — standalone `WaveWire` codec (no STRUCT_HASH,
  no engine coupling) + derive. Gotcha: `#[derive(WaveWire)]` emits absolute
  `::wavedb_wire::` paths, so any crate using it needs `wavedb-wire` as a direct dep.
  Feature `validation` adds `to_wire_checked`/`from_wire_checked` (`[crc32][wire]`) —
  every disk/transport boundary uses it; no structure hand-rolls a byte layout.
- **wavedb-core** — `Id` (`KEY u64 · TENANT u48 · FLAG 1 · SALT 15`), `LocalId`
  (80-bit, tenant-stripped), `U48`, `Metadata` (version chain + `pivot_id` back-link +
  permission), the `Store` trait (`get`/`get_of`/atomic `apply`), the `Store`-generic
  `BpTree<K: NodeKey>` index, `Collection<T>` (the developer surface over the tree),
  `record.rs` (envelopes, id minting, `plan_chained_save`), `Overlay` (batch-pending
  read view so multiple plans on one tree compose into one atomic batch), and the
  `expose` module (`Command`/`Reply`/`Exposure` — the registry contract).
- **wavedb-macros** — `#[wavedb]` computes `STRUCT_HASH` (SeaHash over
  name+shape+fields — any schema change = new type), emits `WaveWire`, generated
  `{Name}Pivot`/`{Name}PivotId`, per-command exec steps `__wavedb_{get,save,insert,update,remove,all}`,
  per-type `static StructStorage` slots (native only; wasm expansion omits them), and
  secondary-index hooks from `#[wavedb::pivot(field)]`, and natural-key anchors
  from `#[wavedb::key(f1, …)]` (NonUnique only; emits `natural_key()` — seahash
  of the fields' wire bytes — and folds the declaration into the STRUCT_HASH,
  so changing the key is a schema change), and **declared lists** from
  `#[wavedb::list]` (field-level) / `#[wavedb::list((f1, f2))]` (struct-level,
  composite), each taking an optional `page = N` of its own — one more chain per
  declaration, this one holding **whole records inline**, sorted by that
  property, with `listed_by_*` / `_at_page` / `_len` readers. **A list is the
  only way to ask for duplication** (RFC 0054); declarations, their capacities
  *and their order* fold into the STRUCT_HASH (RFCs 0051/0052). `#[server]` emits a fn-type
  (own STRUCT_HASH + dispatch), the body retyped onto `ServerDb`, and a client stub.
  `expose_server!`/`expose_client!` are the **declared allowlist registry**: one match
  per operation over exactly the listed items; unlisted/excluded/wrong-shape all refuse
  as uniform `UnknownStructHash` (deliberately indistinguishable — security).
  **Side features (no-leak contract)**: a crate expanding these macros declares
  features named exactly `server-side`/`client-side`; body + dispatch +
  `expose_server!` compile only under `server-side`, stubs + `expose_client!` only
  under `client-side` (fn-type/STRUCT_HASH always). Deployed binaries pull the schema
  `default-features = false` + their side — the other side is never *compiled* in
  (the cfg is the guarantee, not LTO). Server-only helpers outside `#[server]` bodies
  carry the cfg by hand; wasm32 + `server-side` is a `compile_error!`. Defaults keep
  both on so schema-crate tests run the full loop. `expose_server!` also emits
  `wavedb_quick_node::node_tasks!(ServerRegistry)` — the node's Embassy tasks made
  concrete for the registry (static pools need the concrete type, RFC 0066) — so a
  schema crate's `server-side` feature must enable an optional `wavedb-quick-node`
  dependency (native target).
- **wavedb-storage** — the node engine behind `Store`: per shard, **data files**
  of 4 KiB blocks named by the block each starts at (16 hex digits, found by
  listing the directory; writing only appends, a file closes at
  `StoreOptions::file_bytes`, 10 GiB — RFC 0071), per-STRUCT_HASH linear-hashed page directories, `SlotPage`
  (checked-wire envelope, per-type zstd dictionaries with version = prefix length),
  journal-first WAL (append + cache commit under the journal lock = the atomic unit;
  one barrier per batch by default, or one per elapsed `StoreOptions::relax_window`
  — RFC 0061, `flush()` forces it; with `use_barrier(Barrier)` the barrier is
  taken on a helper thread and `apply` waits for it, grouped per round — each
  node worker has one, RFC 0067 §4),
  replay on open. Per-type routing is compile-time (`StructStorage` statics are
  declarations: hash + compression policy); the per-type state (directory,
  dictionary) belongs to the open store (RFC 0067 step 1), and so does the
  record cache — one log per type in chunks + one Dashtable-shaped index;
  settledness is a cursor, eviction drops settled chunks (RFC 0068). Compaction
  carries the runs of the emptiest data files (≥ `victim_free_percent`
  free, 50) forward into the active one, and every checkpoint's opening cuts
  the free tails and deletes the emptied files; stepped, on the node's
  triggers (daily UTC time, disk ceiling, free share — RFC 0071).
  A database created with `StoreOptions::remote` (`Server::object_store` on a
  node) keeps its pages in an S3-compatible object store, the disk caching
  them (RFC 0070 phase 3): settle rounds flag the pages they write, `upload`
  reassembles the flagged ones into the object store's own linear hash
  (128 KiB pages, files of `[32 KiB header][pages]`), and the remote
  directories ride the disk's addressing log; the mode is fixed in the
  `MANIFEST`. Past the compaction ceiling the least recently used uploaded
  pages leave the disk (phase 4): a read of one goes to the object store, a
  write brings its bucket back before it commits, so a settle never meets
  an evicted bucket. A database whose disk is gone opens from its files
  alone (phase 5): chained headers name every type and restate the oldest
  entries, so no snapshot object is needed; only a **cut** (an upload that
  left nothing waiting) is recovered, so no batch comes back in part.
  **Object mode** (`PageStore::open_object`, `Server::object_mode`; phase 6)
  has no disk at all: pages in RAM until uploaded and as a cache, no
  journal — a write is durable once an upload carries it, on 8 MiB or
  `upload_interval` (10 s) — every open a recovery from the files; no
  lease, but every file is PUT create-only, so a second writer on the
  prefix is `Fenced`, never merged. The object store **compacts by file**
  (phase 7, `compact_remote`): a survey run like a recovery finds the
  horizon and the files nothing names (the store, the newest cut, the
  disk's two roots), which are DELETEd blind; mostly free files below the
  horizon have their pages carried forward first. `convert(dir, types,
  options, Backend)` moves a database from or to disk mode offline (the
  object store's files are kept on the way down). A store
  is used from one thread: plain cells, no lock, no atomic; `Send`, not `Sync`. **One open `PageStore`
  per data directory** — every data file and journal is locked while open, a
  second open is `EngineBusy` — and stores over different directories coexist
  in one process. A database is **sharded** (RFC 0067): `MANIFEST` (magic,
  version, the shard count `P` — 64 on a node, fixed at creation — the seed and
  the mode; RFC 0071 §2) + `shards/00..3f/`, each a whole `PageStore`; a single
  `PageStore::open(dir)` is a database of one shard, the same shape
  (`open_shards`; the journals replay in parallel ranges, `open_shards_with`);
  a record lives in shard `seahash(owner bytes) % P` (`core::placement`).
- **wavedb-net** — hand-rolled minimal HTTP/1.1 POST as a **dumb tunnel**: no headers,
  cookies, or status semantics as API; the body is a self-contained
  `Request { tenant, CommandFrame { struct_hash, command, payload } }` and a WaveDB
  refusal is a 200 carrying `NodeError`. Functions and structs share one hash space —
  a fn call is indistinguishable from an object op at the frame level. `NetClient` +
  `frames::FrameReader` are target-independent (POST/body via `wavedb-platform`);
  only the server half (`net::http`) is native-gated. **Every exchange routes through
  `net::manager`** — one never-ending background task per process (M7, user-directed):
  it runs all POSTs, multiplexes all watches of one `(addr, identity)` over ONE
  WebSocket connection (`Hello` once, per-topic subscribe, fan-out; lifecycle owned by
  the manager loop), and can watch over plain HTTP instead ("anything new?" polls —
  `net::sync`, reserved hash `"WDB.SYNC"` routed before the registry; **stateless**:
  each declared topic carries an instant cursor and the node answers by navigating
  the recency/dead logs or the Unique chain (`Command::Changes`) — no per-session
  node state, an outage or restart loses nothing; same-record writes within one tick
  coalesce to the live state).
- **wavedb-quick-node** — library (no bin): `Server::new(REGISTRY).data_dir(d).serve(addr)`;
  `.threads(n)` workers each **own** a contiguous range of the data shards — their
  `PageStore`s move onto the worker's thread, operations are plain calls there,
  and a BG task beside the jobs steps settle/checkpoint when they leave room
  (RFC 0067 step 4; one executor per thread, no `unsafe`). Each worker also
  **accepts its own connections** — one listener per worker, an
  `SO_REUSEPORT` group on Linux (`Listener::bind_many`; one listener
  elsewhere) — and routes every operation by owner, to its own queue or
  another's; a topic's subscriptions live on its owner's worker, which
  publishes straight into each session's outbox (step 5). On shutdown each
  worker closes and drops its stores before reporting. Node-side seeding goes through
  `RouteHandle::new(&bound.route(), tenant)` — each op on its owner's shard.
  `expose_server!` also emits `StorageRegistry`, so `.registry(REGISTRY)` alone opens
  the `PageStore`. Gates 4–6 (permission/validate/preprocess) are an M8 seam.
- **wavedb (client)** — `Db::connect(addr, user, tenant)` (transport-only) and the
  M6 `Db::open(CLIENT_REGISTRY, addr, user, tenant, app)` family attaching the local
  **write-through cache** — WaveDB caching WaveDB, cfg-switched like the platform
  seam: native = `PageStore` under `~/.cache|XDG_CACHE_HOME|%LOCALAPPDATA%`
  `/<app>` (the app is the leaf directly under the base, XDG-style, no shared
  `wavedb/` parent; auto-created; `open_at` for an explicit dir), wasm =
  `wavedb::cache::IdbStore` (`wavedb-wasm` only re-exports it). Semantics
  (`client_cache.rs`): **node-first** — acknowledged ops mirror best-effort under
  node-minted ids (`Collection::adopt`; `All` frames carry `(Id, T)` for this);
  reads fall back only on `Error::Transport` and only when the cache holds the
  answer (absence propagates the fault; refusals never fall back); offline writes
  refuse (queueing is M7). `db.local()` is the cache's direct `LocalHandle`. One
  engine per process ⇒ a `Db::open` client and a node never share one (tests run
  the node as a child process — see `local_cache_e2e.rs`). Typed calls spell
  `T::get(&db)` / `v.save(&db)` / `T::collection(pivot)` via the `DbHandle` seam;
  `ServerDb` mirrors it node-side for `#[server]` bodies, plus
  `db.lock(&[Owner])`: owners held across awaits (leases, RFC 0067 §5) — all
  in one call, nothing reached past them (`Error::LockOrder`). Live sync (M7):
  `db.watch_unique::<T>()` / `db.watch_collection::<T>(pivot)` (token required)
  yield typed `WatchEvent`s and mirror each into the cache before yielding; watches
  multiplex through `net::manager` (one WS connection per identity), or poll over
  HTTP with `db.watch_via_polling(interval)`.

## Data-model invariants

- `save` is an upsert — there is no `create`. The live record sits at the shape's
  **anchor** (Unique: `KEY = STRUCT_HASH`/`FLAG=1`; NonUnique: the immutable
  insert id/`FLAG=0`); a save archives the superseded version at a **derived
  slot** — `KEY` = that version's authoring instant, `SALT = trunc(STRUCT_HASH)`
  (types never collide in a flat keyspace, e.g. IndexedDB), `FLAG` = the anchor's
  bit **flipped** — and chains it through `Metadata`
  (`previous: Option<u64>` + `Succession::{CreatedAt(u64), Next(u64)}`): links are
  **instants, addresses are computed**, so no archive is ever repointed; a forward
  walk that MISSes a derived slot has reached the live anchor. Bytes are never
  destroyed; only `remove` writes the `dead` tree. Every save batch opens with a
  `Write::Expect` guard (validated pre-batch under the commit lock, stripped
  before journaling) — a lost race is a typed `Error::Conflict`, never
  overwritten history. The chain records **who/when/permission per version**;
  it is state review, not domain data, and ends at the STRUCT_HASH boundary.
- NonUnique record identity `Id` is minted at `insert` and never changes; `save`
  re-keys only the secondary indexes whose fields changed (primary key is the
  immutable `CREATED_AT`). All minting bottoms out in `platform::time::key_nanos()`
  — real ms × 1e6 + a process counter in the sub-ms digits (same formula both
  targets; collision-free even on the browser's millisecond clock).
- `#[wavedb::key(f1, …)]` (NonUnique only) makes identity **content-derived**:
  anchor `KEY` = seahash over the key fields' wire bytes (`core::natural_key_hash`,
  one per-build derivation), so `insert` IS the upsert at that anchor — vacant →
  guarded first version; living → chained save; dead → **revival** (chains onto
  the prior history; the dead log keeps the removal as a historical event and
  catch-up merges both tails by instant, so `Removed` then `Saved` replays in
  order). A save addressing a foreign anchor refuses typed (`Error::KeyMismatch`)
  — renaming = explicit `remove` + `insert`. The key declaration folds into
  STRUCT_HASH (synthetic `#key` entry); keyed walks come out in hash order, not
  insertion order (modification order lives in the record chain, and any other
  order is a declared `#[wavedb::list]`).
- **No duplication by default** (RFC 0054). A record lives at its anchor and
  nowhere else — it has to, since history and the dead log resolve it there, and
  for a time-keyed NonUnique the anchor's own `KEY` **is** its `CREATED_AT`, so
  creation order needs no structure at all.
  Every collection carries two instant-keyed `Chain`s in its Pivot, **the same
  shape as each other**: ids and nothing else (`Chain<()>`, keyed
  `[instant BE][anchor]` — the key already names the record). They are the
  **recency** chain — one entry per living record at its live version's
  authoring instant (insert adds, save re-keys, remove deletes) — and the
  **dead** log, keyed by the removal instant. Together they answer "what
  changed" and "what died"; a tail scan from a cursor over both is exactly
  "changed since" (the W6 catch-up structure). One read per segment gives
  membership and order; each record is then resolved at its anchor.
- **`all()` is recency-ordered, and that is the feature.** Most-recently-changed
  first is what a listing usually wants, and it is why the same structure serves
  live sync. A save therefore *does* move a record to the front — a caller who
  wants a stable, domain-meaningful order declares a `#[wavedb::list]` on the
  property they want it ordered by.
  Every collection-minted instant goes through `core::mint::mint_instant(floor)`
  where `floor` = both chains' maxima — strictly monotone per collection even
  against a rewound clock (Unique needs no floor: catch-up is chain-forward).
- `#[wavedb::list]` is **the only opt-in to duplication** (RFC 0051): each
  declaration adds one more chain of the same records, same lane, this time
  holding **whole records inline**, sorted by the declared property and
  tie-broken by the **anchor** (immutable ⇒ a save relocates only when that
  property changed — but it still **rewrites** the record in every list, since
  the bytes are duplicated there and only the position is unchanged). Copies of
  each record on disk: **1** by default, **1 + K** with K declared lists. Its
  `page = N` (per list, falling back to the struct's) is what makes a rendered
  page one segment read; the payload-free chains take the large log capacity
  instead, since there is nothing in them to paginate. Every list is maintained in the
  same atomic batch, gated on the built-in chain's liveness verdict, and read by
  the generated `listed_by_*` / `_at_page` / `_len`. It costs a full copy of
  every record per declaration — that duplication *is* the design.
- Every mutating collection op is exactly **one atomic `Store::apply` batch**
  (record + touched chain segments and sparse-index nodes + touched B+tree nodes
  + Pivot rewrite when a root moves).
- Stored values are STRUCT_HASH-headed: user records
  `[STRUCT_HASH][meta_len][Metadata][body]`; Pivots `[STRUCT_HASH][wire]`; BpTree
  nodes `[BPTREE_NODE_HASH][kind u8][wire]`. Decode verifies the head.
- Pivot instances are created explicitly (`create_pivot`), **one per holder** —
  *not* one per tenant per type: the API imposes no such limit, so a nesting
  NonUnique mints one per record and a type has as many collections as there
  are holders (the shop workload: one `Product` collection per `Shopping`).
  The holder stores the `PivotId`. The Pivot is rewritten only when a root
  moves. Two collections of one type share nothing but their storage slot,
  which is what makes the Pivot instance the unit of concurrency (RFC 0064).

## Testing conventions

- Unit tests colocate in `#[cfg(test)] mod tests`; cross-module behaviour in `tests/`.
- Codecs get roundtrip + failure cases asserting the **specific** error variant.
- Storage changes need a durability angle (reopen-and-replay or kill-during-write).
- Test names state behaviour (`tampered_payload_is_crc_mismatch`), not the method.
- Anything touching `PageStore` opens it over its **own** directory (a tempdir
  per test); a directory has one open store at a time. Tests over different
  directories run in parallel — no gate needed.
- A node is one per process (the token secret is process-global, set at bind),
  so node tests keep the single-test pattern
  (`crates/wavedb-quick-node/tests/node_http.rs`). Cache-and-node tests run the
  node as a child process — re-exec the test binary
  (`examples/contact-book/tests/local_cache_e2e.rs`) — which is also the real
  deployment shape.

## Commits

Conventional commits (`feat:`/`fix:`/`docs:`/`refactor:`/`perf:`/`test:`), imperative
subject, body explains why when the diff doesn't.
