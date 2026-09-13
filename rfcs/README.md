# WaveDB RFCs

This directory is the **design record and progress tracker** of WaveDB: one
numbered document per idea. Where the crate READMEs describe the *target*
architecture, an RFC captures **the idea itself** — the problem it solves, the
shape of the solution, its current status, and the alternatives that lost — so a
decision can be understood (and revisited) years later without re-deriving it
from the code.

See [RFC 0000](0000-rfc-process.md) for how this process works (numbering,
statuses, the filename-marker convention).

## Current state

_A snapshot for orientation; each RFC's status header is authoritative._

- **Shipped (M1–M7, M8):** the wire codec, type identity, the platform seam, the
  full data model (anchors + `Succession` history, B+tree collections, natural
  keys), the macro & exposure system, the storage engine + journal-rooted
  recovery, the HTTP + WebSocket transport, the connection manager, live sync by
  navigation catch-up over both poll and WebSocket (including W6 reconnect —
  [0034](0034-ws-reconnect-catchup.md)) and W7 poll efficiency (idle backoff +
  command piggyback — [0035](0035-http-piggyback-and-idle-backoff.md)), the
  client `Db` + write-through cache, the wasm/IndexedDB target, and auth. RFCs
  0003–0026 are *Implemented* (except the two Partial noted below; 0014 and 0040
  are both *Deprecated* — see the next bullet).
- **Schema migration is the developer's, entirely.** Both engine-side designs —
  the 0014 hook seam and the 0040 version chain — are *Deprecated*; a changed
  struct is simply a new type ([0004](0004-struct-hash-and-schema-evolution.md))
  and moving data between two types is application code. What survived is the
  exposure registry's **compile-time** collision guard (full-`STRUCT_HASH` clash
  = error, 15-bit `type_salt` clash = warning), recorded in
  [0040](0040-schema-migration-and-version-skew-DEPRECATED.md).
- **The storage write path (2026-07-28 → 07-29):**
  [0041](0041-single-barrier-checkpoint.md) *(Implemented)* replaced the per-id
  settle — which read-modify-wrote a bucket page once per record — with a
  planned window: pages grouped per bucket in RAM, one contiguous best-fit
  allocation, **one** positioned write covering pages + dictionary + directory
  chains, then the descriptor swap. A settle round takes no barrier at all; a
  checkpoint takes one, the window's (see 0046 for how the `Commit` frame
  stopped taking a second). The per-mutation path was already one append +
  fsync and did not change.
  [0042](0042-free-space-defragmentation.md) *(Implemented)* keeps such windows
  available: a background pass relocates live pages stranded between holes to
  fresh tail blocks, so the space they vacate coalesces into the extent the next
  checkpoint's best-fit lands in. And
  [0043](0043-descriptors-in-the-commit-frame.md) *(Implemented)* moved the
  addressing state **into** the `Commit` frame — one journal append carries every
  type's descriptor vector plus the retired journal's DONE marker, so `data.bin`
  holds pages and dictionaries and nothing else, and the copy-on-write directory
  chain is gone.
  Finally [0046](0046-directory-deltas-in-the-window.md) *(Implemented)* fixed
  the one cost 0043 left behind — a frame carrying every bucket of every type
  scales with the **database**, not the change (1 MiB per checkpoint at 2 GiB,
  100 MiB at 200 GiB) — by moving the descriptor changes into the settle
  window itself: metadata for **zero** extra IOps, with the frame reduced to a
  snapshot address plus the deltas since it, compacted by a periodic full
  chunk. Because that frame is only a pointer into already-durable state it
  needs no barrier of its own — a checkpoint costs **one**, the `data.bin`
  sync, with the retirement it authorises deferred until an ordinary write's
  fsync carries the frame.
  [0047](0047-generational-journal-retirement.md) *(Implemented)* closed the
  accounting: that retirement is now disposed of by the **next** checkpoint,
  which holds the journal carrying the frame and can read its barrier count,
  rather than being chased from the write path (a lock and an `unlink` inside a
  batch) or from an idle timer (a whole barrier for housekeeping). Two journals
  on disk is the steady state — disk being the abundant resource — and no
  barrier is paid anywhere on a checkpoint's behalf.
  Alongside them, [0049](0049-elastic-pages-and-load-driven-splits.md)
  *(Implemented)* stopped the engine enforcing a page size. Linear hashing's
  split order is derived from the directory length and cannot be aimed, so a
  per-page overflow trigger split ~N/2 innocent buckets to relieve one — and
  never terminated at all for a record larger than the threshold, since splits
  distribute whole records: every touching round burned its full 64-split
  budget forever. The trigger now asks about the bucket whose turn it is, so a
  split only happens where it relieves something, an over-target bucket simply
  spans more blocks until its turn, and a large object is just a large page.
  And [0048](0048-chained-addressing-log.md) *(Implemented)* took the last
  recurring cost out of the frame: it was rewritten in full at every checkpoint
  to name state the previous frame already named, so a compaction cycle of N
  checkpoints cost 4N² journal bytes — quadratic in the interval, and therefore
  a ceiling on it, forcing the O(database) snapshot far more often than
  necessary. Each chunk now names the one before it, the frame names only the
  head, and it is a fixed 16 bytes however long the log grows.
- **The default, settled 2026-07-31 —
  [0054](0054-no-duplication-by-default.md):** a record lives at its **anchor and
  nowhere else**. A collection's two instant-keyed chains — **recency** and the
  **removal log** — are the same shape, ids and nothing else (`Chain<()>`; a
  `SecKey` already carries the anchor), answering "what changed" and "what died".
  One segment read gives membership and order; each record is resolved at the
  address it always had. `all()` stays recency-ordered on purpose: a save moving
  a record to the front is the feature, and a caller wanting a stable
  domain order declares a `#[wavedb::list]` for it.
  `#[wavedb::list(...)]` is the **only** opt-in to duplication: 1 copy by
  default, `1 + K` with K lists. It opened as `layout = anchored`, a declared
  alternative, and the knob was built and deleted the same day — "not anchored"
  is not a state a record can be in (history and the dead log resolve it there),
  so the knob only ever controlled whether there was an *extra* copy, which is
  what a list already controls. And since `Chain<P>` was already payload-generic
  and the removal log was already `Chain<()>`, the whole model is one type
  parameter, not a second engine.
- **The structure it rides on (storage & query):**
  [0050](0050-clustered-record-chains.md) — a collection's records are **additionally** stored inline in a chain of
  segments ordered by the live version's **authoring instant**, so a scan costs one
  read per segment instead of one page read *and* one zstd decompression per record,
  a range keeps its logarithmic descent through the chain's sparse index, and — since
  that is exactly the key `recency` uses — **the `recency` log disappears into the
  chain**. (As proposed the chain held the records inline, a byte-identical derived
  duplicate; [0054](0054-no-duplication-by-default.md) kept the merge and dropped the
  payload — the chain that survives is `recency` itself, ids only. Records inline is
  now what a declared list is.) The record at its anchor stays put and
  stays authoritative, so every computed address, history walk and `Expect` guard is
  untouched. No chain stores a pointer
  *to* a record — position is derived from the record's own key — so splits are free of
  consequences, and because a split can always give its new id to the *interior* side,
  a chain's `head` and `tail` ids are permanent and a growing chain never rewrites the
  `Pivot`. The `current` B+tree disappears (the chain is the membership set, liveness
  moves into the anchor's `Metadata`), `dead` stays as a reference-only log chain in a
  lane of its own with **no index at all**, since nothing ever *searches* it (post-0054
  `recency` is that same shape and has its own lane too — an ~18-byte id entry and a
  segment of whole records are different content, and a per-type zstd dictionary can
  only model one of them well), and a
  dense B+tree exists only where the developer declares one. The cost is storage,
  duplicated write bytes, and `all()` changing from insertion to modification order.
  **Phases 1–7b are implemented**, and phase 8 ("compaction") was dissolved on
  2026-07-31 once it turned out to name two unrelated things: the chain already
  rebalances synchronously on every removal (phase 3b), leaving only the
  sparse index's missing merge — **taken as accepted debt**, since that index
  holds one entry per *segment* and a drained version of it is still two
  levels, and written up as
  [0055](0055-sparse-index-merge-PLANNED-LOW.md) so the bound is on the
  record — and removal-log retention, which is a client-liveness policy and
  got moved out to its own planning. The structure
  (`index/{segment,sparse,sparse_write,chain,chain_remove}.rs`): locate, insert
  with a 50/50 split at 2N, remove with a merge at N/2 or a redistribute when
  folding would breach the band, all as one batch. Liveness on the record
  (`Metadata.removed`). The `Pivot` carrying the chain roots, with the `current`,
  `recency` and `dead` B+trees now **deleted** — they were written alongside the
  chain for one phase so a test could assert the two agreed entry for entry and
  byte for byte, then retired, taking `Collection::search` (the `CREATED_AT`
  range) with them: zero callers, and a contract already false for
  `#[wavedb::key]` types, whose anchor is a content hash rather than an instant.
  Both reads come off the chain: `all()` and the wire `All` walk it back from the
  tail (post-0054 resolving each entry at its anchor — one segment read still gives
  membership and order); and reconnect catch-up (`Changes`)
  scans the chain's and the removal log's tails past the client's cursor, stopping
  at the first segment that reaches it — so a caught-up client pays three reads for
  "nothing new" whatever the collection's size, and a client behind pays segment
  reads rather than one random read per change. Plus `page = N`, the
  developer-declared segment capacity, folded into the identity so a chain is only
  ever laid out one way. Phase 7b then landed 0051 in full, and phase 8 dissolved;
  [0051](0051-ordered-record-lists.md) — **landed 2026-07-31** as 0050 phase 7b,
  built on it, and the repair for the one thing it gives up: a declared property materialises a *second* chain of
  the same records, kept sorted at write time (affordable because K extra segment
  rewrites still cost one barrier), with a **sparse** index above it — one entry
  per segment instead of per record, so the descent is two or three nodes **cold**
  (nothing is resident: 0053 forbids pinning in a multi-tenant engine, so the
  guarantee is bounded size, never residency). Ordered and range reads cost one
  dense read per segment of hits instead of one random read per hit; the price is
  `(K+1)` copies of every record on disk — the anchor plus K declared lists; it was
  `(K+2)` until 0054 emptied the built-in chain — accepted deliberately, and after
  0054 it is the *only* duplication there is. Its **wire commands landed
  2026-08-01** — `Command::Listed` (bounded: `(pivot, index, offset, limit)`) and
  `Command::ListLen` answering a new `Reply::Count` — closing the gap where an app
  could declare a list, pay a full record copy per save for it, and still not
  render a page without wrapping it in a `#[server]` fn. They needed none of the
  streaming-frame work `search_by` waits on, because a page is bounded by the
  caller's own `limit`; the typed surface gained `listed_page` so that limit is
  reachable, and the unbounded `listed` pages over it at a fixed chunk;
  [0052](0052-segment-size-as-the-pagination-unit.md) — the developer
  declares a chain's capacity as a **minimum** N, normally the page size the UI
  renders (undeclared: **16** for record chains, **256** for the removal log); a
  segment holds N…2N records, splits 50/50 at 2N with the endpoint keeping its own
  id, and **merges at N/2** — which keeps an insert to one segment rewrite where an
  exact size would cascade, and stops a chain decaying into near-empty segments as
  saves relocate their records to the growth end. A rendered page is one
  segment read, two when the window straddles a boundary — and the first is almost
  certainly still cached from the previous page. Exactness is not the goal anyway:
  `search` is an async iterable, so a tick yields whatever the segment held and the
  row count belongs to the layer above, filters included. The sparse index carries
  element counts (leaf and subtree), making it an order-statistic tree — "jump to
  page k" is one descent regardless of k, and an unfiltered pager's "of M" is the
  root's sum. Costs are quoted **cold**: WaveDB is multi-tenant, so nothing may be
  pinned in RAM. **Landed 2026-07-31** — most of it as 0050's phases 3b/2/3a/7a,
  then the per-list `page` (`#[wavedb::list(page = 25)]`), which exists because
  the two chain kinds have opposite write profiles: the built-in chain is
  rewritten whole at its growth end on every save and wants a small N, a list is
  rewritten in place and can hold the page a view renders. The counts are proven
  across a **crash and replay** too (`counts_survive_recovery`, over `PageStore`:
  a mid-batch tear during recovery is what makes the index and the segments
  disagree, and the test reads that disagreement off the public `list_len` vs
  the rows `listed` actually serves). What is left is two policy questions;
  [0054](0054-no-duplication-by-default.md) — which then **inverted the default**
  of all three (see above): duplication moved from something every collection pays
  to something a `#[wavedb::list]` asks for, and 0050's chain machinery is what
  both the pointer chains and the declared lists are built out of;
  [0053](0053-tenant-fair-cache-retention-PLANNED.md) — the policy that follows
  from that: which entries deserve to stay hot without one tenant monopolising the
  budget (navigational vs streaming, per-tenant accounting, no pinning ever). Held
  at *Planned* until a measured workload justifies it — the baseline of read,
  deposit, flush, and accept the miss is already correct;
  [0044](0044-page-cache-PLANNED-LOW.md) — a page-granular cache so the read
  that precedes a write also serves the settle's read-modify-write (the weaker
  answer to 0050's problem: it pays to tolerate a random access pattern rather
  than removing it — still worth having), **superseded 2026-08-01** by
  [0057](0057-page-arena-and-checkpoint-staging.md), which gives it a single
  block-aligned arena keyed by `BlockDescriptor` (copy-on-write descriptors make
  invalidation *disappear* rather than merely be cheap), lets a settle round
  assemble its window inside that arena so the pages it just wrote stay resident
  for the next round, and — the reason it opened — records why two neighbouring
  proposals are rejected: a crash-recovery "was this already written?" check
  (there is no fault to detect; a `Commit` retires only the rotated-out journal,
  so data in `data.bin` ahead of the boundary is never *only* there), and
  pre-building the checkpoint window between checkpoints (a page image depends
  on its bucket's full contents and on a zstd dictionary that is not final until
  the round closes);
  [0058](0058-per-type-actors-DEPRECATED.md) — the concurrency the two above ran
  into: `drain`/`settle`/`commit_journal` are synchronous `fn`s on a
  current-thread runtime, so a checkpoint blocks the request path by occupying
  the only thread — and that single-threadedness is *silently* supplying two
  invariants nothing documents (batch application being atomic, and
  `pending` empty ⇒ everything settled, which `evict_settled` trusts while
  `drain`'s `mem::take` has already emptied it mid-round). The answer is **one
  actor per type**, owning that type's whole family — the partition already
  exists in the storage layout (six slots per type; a batch never spans two user
  types), just not in the concurrency — with the journal and the writer staying
  single because one file means one fsync, and one fsync shared across many
  types' batches is group commit rather than a bottleneck. It needs a
  `Lane::Tree` first: `BPTREE_NODE_STORAGE` is one process-global slot for every
  type's B+tree nodes, and it is the only thing that makes the partition
  incomplete. **The engine becomes `Send` on native** (user decision
  2026-08-01, reversing `CLAUDE.md`'s non-`Send` stance): `Store`'s
  async-fn-in-trait desugars to `-> impl Future + MaybeSend`, a cfg'd bound in
  `wavedb-platform` keeps wasm paying nothing, and work-stealing then balances
  *different* actors across cores — which is what the pinned-thread alternative
  could never do, since an actor cannot migrate without its state (it does not
  make any *one* actor faster; a hot type is one core at a time regardless).
  `Sync`, by contrast, mostly *disappears*, and that is the actor's doing rather
  than `Send`'s: actors own instead of sharing, so the locks that exist only to
  satisfy `Sync` on a `static` stop existing rather than stop contending.
  **Parked 2026-08-04, then deprecated outright 2026-08-22** (see 0064 below;
  the file is reduced to what it proposed, why it lost, and what survived). The
  gate it fails:
  the engine must be able to run as **one actor on one thread** (the wasm
  target), and "N actors as N tasks on one worker" is a different execution
  model, not that one — the degenerate case is where the failures live and it
  has no design here. Behind it: actor-to-actor request/reply is an unaddressed
  deadlock surface, `Lane::Tree` charges a `STRUCT_HASH` break for a concurrency
  refactor, and every performance number in it is an estimate. What survives is
  the *motivation* — the synchronous `fn`s on the request path and the two
  invariants single-threadedness silently supplies, documented nowhere else.
  **Its successor is
  [0063](0063-engine-yield-map-and-interruptible-engine-PLANNED.md)** *(Planned,
  opened 2026-08-21)*, which takes that gate seriously and reaches two findings,
  the second contradicting the word "actor" in it. **First, a prior question:
  where may the engine give up its thread?** Today, nowhere —
  `wavedb-storage` has no await point at all, so any scheduling scheme wrapped
  around it is that code plus overhead. So 0063 is first a **map**: every
  blocking site, what it blocks on, under which locks — and then the design
  content, which of those sites the engine may legally be *observed* at, because
  a yield point is a visibility commitment rather than a performance knob.
  Yielding is illegal inside `apply_inner`'s journal-lock section (it would
  expose half a batch), inside `place_in` (the window must not be handed out
  twice), and inside `evict_settled` (it quiesces writers on purpose); it is
  legal between `drain`'s rounds, the one point wasm needs and where a checkpoint
  stops being a stall. **Second, the base case is not an actor**: a mailbox is
  not what makes a yield point safe (the map is), and the engine is not missing
  an executor — tokio and `wasm_bindgen_futures` already are one. What it lacks
  is **interruptibility**, i.e. await points inside its own long-running work, so
  a message loop in front of a single-threaded engine whose locks never contend
  re-implements the scheduler underneath it. 0058 had filed exactly this as an
  unresolved gap (*"the per-type state separation buys nothing and costs a
  routing layer"*); the resolution is removal. An actor is about **ownership
  across threads** — `RwLock<BTreeMap>` becoming `BTreeMap` — which is real when
  there are threads to share between and worth nothing on one, so it belongs to
  the elaboration. The base case is therefore `drain` becoming re-entrant, a
  macrotask yield on wasm, and nothing else: no mailbox, no `Send` migration, no
  `MaybeSend` seam, no `Lane::Tree` and therefore **no `STRUCT_HASH` break**.
  Falling out of the map, wrong on one thread too, and therefore **fixed the
  same day**: every guarded write *did* a full-slot disk scan under the journal
  lock — `apply_inner` validated each `Write::Expect` by calling `read_any`
  *inside* the lock, and `Expect(id, None)` (the guard on every first save)
  matches nothing, so it preaded and zstd-decoded once per registered type while
  no other write could proceed. `Write::Remove` and `Write::Expect` now carry
  the STRUCT_HASH they route by (`Put` does not — its bytes are already headed
  with it), so a guard is one routed lookup whatever the schema's width,
  committing a `Remove` reads no page, `commit_to_caches` became infallible and
  `owner_of` is deleted. Five construction sites outside tests, every one of
  which already had the hash in hand. `Write` is a wire type — journal layout
  changes, free under the pre-release policy, and it folds into no
  `STRUCT_HASH`, so no schema breaks.
  **[0064](0064-pivot-owned-concurrency-PLANNED.md)** *(Planned, opened
  2026-08-22)* then settled the unit, which is what 0058 had got wrong and 0063
  had deferred to an elaboration: **the unit of concurrency is the Pivot
  instance, not the type.** It was invisible because four documents claimed a
  Pivot is created "one per tenant per type" — the API imposes no such limit
  (`create_pivot::<T>()` is just `Collection::<T>::create`) and the shop
  workload nests one `Product` collection per `Shopping`, so a type has as many
  collections as it has holders. Two collections of one type share *nothing*:
  Pivot record, B+tree roots, recency/dead chains and list segments are all
  their own; only the items inside one collection are inherently serial,
  because those really do share a tree. **The whole design slots in behind the
  `Store` trait** — `Collection` and `BpTree` reach their backend through four
  methods and take the store *per call* rather than owning one, so
  `impl Store for PageStore` becoming `impl Store for ShardStore` is the extent
  of the seam and the index layer is untouched. Everything expensive then falls
  away.
  There is **no hot-type ceiling** (the busiest type in the shop is one owner
  per order, not one owner); **no balancer** (`shard = hash(pivot_id) % N` is
  uniform because the units are millions, so it is a function rather than a
  routing table — nothing splits, nothing migrates); and **no `Lane::Tree` and
  therefore no `STRUCT_HASH` break**, because ownership need not follow storage
  partitioning: the disk sits behind a single owner regardless, so the one
  global `BPTREE_NODE_STORAGE` slot costs nothing and only the **caches** need
  repartitioning — cache by owner, page directory by type, two axes free to
  differ. The hierarchy is one scheme at three grains (tenant → pivot instance →
  serial), so distributed sharding later is a *coarsening* rather than a second
  design. A journal actor owns the journal (group commit) and a page actor owns
  `data.bin` — single ownership being what makes block de-duplication possible,
  and the natural home for the [0044](0044-page-cache-PLANNED-LOW.md) page cache
  and for zstd, following the actual Redis lesson (*one thread owns the data
  structure; everything that is not data-structure mutation leaves it* — Redis 6
  moved I/O off the core, it was never one thread doing everything). Routing is
  verified against `Command`: `Insert`/`All`/`Listed`/`ListLen`/`Changes` carry
  the pivot, Unique ops route by `(tenant, STRUCT_HASH)`, and `Get`/`Update`/
  `Remove` carry only an `Id`, and **must change to carry the pivot**. Resolving
  it node-side is impossible, not merely slower: cache and tombstones are
  shard-owned and only the settled page is the page actor's, so a page actor
  answering alone misses an unsettled write, serves stale bytes past a newer
  cached version, and resurrects a record whose tombstone sits in a shard — a
  read needs its owner, the owner needs the pivot, the pivot needs a read. The
  exit is that `Collection<T>` **already holds the pivot and already passes it**
  (`db.get_record(self.pivot, id)`); the client stub drops it before the wire and
  the node then pays a read to recover it. Carrying it deletes `record_pivot`
  outright — one `get_of` plus a decode per update/remove, and a page read on a
  cold cache — and it is the **third instance of one shape**: an `Id` names
  neither its type (A2) nor its tree (`BPTREE_NODE_STRUCT_HASH`) nor its owner,
  the caller always knows, and every time the search was happening in the most
  expensive place available.
  Request/reply is `await`, not a synchronous message, so 0058's deadlock
  surface disappears; `Rc` survives and **the engine does not become `Send`** —
  only the disk actors' message types do. The compile-time single/multi switch is
  just `N`: one shard with inlined disk actors (today, and wasm) or N
  `current_thread` runtimes — tokio's *work-stealing scheduler* is what to
  avoid, not tokio, since stealing migrates tasks and destroys the locality the
  design exists for. **A shard cannot drift between threads, and that is a
  compile error rather than a scheduler promise**: `platform::task::
  spawn_detached` already boots a dedicated thread with its own current-thread
  runtime and bounds the future *without* `Send`, so the state cannot be moved
  — the migration that matters (work stealing, possible at every await) is ruled
  out by types, for free. Core pinning is a separate and *optional* question
  that buys only cache warmth: nothing in the workspace pins today, correctness
  does not depend on it, and a fixed mask is wrong under a `cpu.max` quota or
  across SMT siblings — though the usual objection (a blocked thread idles its
  core) does not apply, since a shard never touches disk. `hash(pivot) % N` with
  `N` fixed at startup means a pivot never changes shard either; the price is
  that changing `N` is a restart. It also dissolves 0063's I1 (a batch belongs to one owner,
  so it is atomic by ownership — no seqlock, no staged publish), leaves I2
  untouched, and leaves 0063 Parts 1–3 standing as the next executable step. Its
  own open question is the settle path, which straddles the boundary: `plan_slot`
  reads owner-side caches and writes storage-side pages and the type's zstd
  dictionary.
  **First slice landed 2026-08-22** (`crates/wavedb-quick-node/src/shard/`):
  the disk actor owns the `PageStore` and the node reaches it only by message;
  `ShardStore` gives each shard its cache (and, because that cache remembers
  *absence*, only a shard may have one — everything else takes the cacheless
  handle); `OwnerLocks` is the brake, **one table for the node behind an `Arc`**
  since two ops on one owner can now be picked up by two threads
  (`CONCURRENCY_BRAKE.md`); and a POST is **routed** to a shard worker with its
  own thread, runtime and cache, with committed mutations handed back to the
  accept thread that owns the subscription table. The router keys on exactly the
  brake's key — route one way and brake the other and the two locks are
  different locks — so both narrow to the Pivot together or neither does.
  Deferred with reasons in place: the journal/page actor split and the per-type
  caches moving into shards (both wait on the settle straddle), Pivot-grained
  routing (the wire change), and WebSocket `Call` routing (a session binds
  identity once at `Hello`, so re-verifying per command would start refusing a
  long watch at token expiry — a behaviour change, not a refactor);
  [0045](0045-vector-search-PLANNED.md) — nearest-neighbour search as a
  declared index kind (IVF over the existing `BpTree`, per-tenant centroids);
  [0056](0056-fuzzy-string-search-WIP.md) *(engine side landed 2026-08-01;
  no wire command, so a client reaches it through a `#[server]` fn — see
  `search_todos` in the todo-app)* — the same move for a different
  distance: `#[wavedb::fuzzy]` **on the field**, n-gram postings keyed
  `[gram][len][anchor]` in an ordinary `BpTree<SecKey>`, so one gram lookup is a
  `Bound::Prefix` scan and the length filter rides in bytes the scan already
  read. A count filter (`T - n*k` shared grams) bounds the candidates, then each
  survivor is verified at its anchor. Unlike a list it duplicates **no record
  bytes** — a posting is a pure function of the indexed field, so a save that
  left the field alone writes nothing at all; the price is `L + n - 1` scattered
  key writes per record.
- **Where the bytes live, opened 2026-08-08 —
  [0059](0059-object-storage-capacity-tier-PLANNED.md):** three tiers (memory for
  the write path and the hottest pages, `data.bin` for the working set, an
  S3-compatible bucket for capacity), addressed by one reserved bit in the
  existing `BlockDescriptor` — a remote page is a descriptor whose remaining
  bits name a derived object key instead of a block run, so a tier change *is* a
  descriptor change and rides the settle window like any other
  ([0043](0043-descriptors-in-the-commit-frame.md)/[0046](0046-directory-deltas-in-the-window.md)).
  The target is **history**: archives are identifiable from the id alone (the
  shape's anchor `FLAG`, inverted — no read), immutable, and reached only by
  `record_history`/`unique_history`, so they are the one cold set whose sole
  access path already tolerates 100 ms. The enabling change is not the tier
  though — it is giving archives **their own lane**, a fifth emitted
  `StructStorage` slot beside `WDB.SEG`/`WDB.REC`/`WDB.DEAD`/`WDB.IDX`: because
  the directory hashes the id and an archive id is effectively random, archives
  and live anchors are evenly mixed across every bucket, so after N saves the
  average page is 95% ballast and *no page is cold* — temperature measurement has
  nothing true to report, and the fix has to be spatial. That phase 1 is worth
  landing alone (homogeneous live pages, a second dictionary, history stops
  spending the split budget). Idle *live* pages are phase 3 and explicitly low:
  it is nearly free to build and it is the only part that changes what a `get`
  promises. This revives [0033](0033-cold-history-slow-node-tier-DEPRECATED.md)'s
  **problem** while still rejecting its answer — a bucket rather than a slow node,
  and 0033's three charges (a migration path, hot/cold routing, cross-tier
  consistency) are respectively already built, a compile-time bit test, and
  vacuous for immutable single-reader data.
- **Measuring it, opened 2026-08-12; built 2026-08-13/24, then restructured —
  [0060](0060-comparative-benchmark-suite-DEPRECATED.md) *(Deprecated)* →
  [0065](0065-benchmark-suite-ii-the-row-as-the-unit.md) *(Planned)*:** until
  0060, nothing in the repository was measured (`crates/wavedb-net/benches/`
  empty, `criterion` unused), so every performance claim above was an argument
  about the code rather than a number. 0060 built a reproducible **insert /
  read / update** comparison against **MongoDB**, PostgreSQL, MySQL and SQLite,
  plus a second **e-commerce** workload reporting p50/p99 milliseconds rather
  than a rate. Mongo is the reference peer, being the closest model — a document
  *is* a record, `_id` *is* an anchor, both encode client-side, and the oplog is
  structurally the recency chain
  ([0022](0022-live-sync-navigation-catchup.md)). Its methodology stands and is
  carried forward whole: the **cage** (`systemd-run` bounds memory *and page
  cache*, `taskset` the CPUs, `bwrap` only the PID namespace) with every
  server's cache pinned to the same 256 MB, because each otherwise sizes itself
  from the machine's RAM rather than the cgroup's; **footprint at four points**
  with preallocated log capacity split from payload (MongoDB's journal is
  200 MB beside a 22 MB collection — configuration, not data) and an empty-system
  baseline as a correction term; **seeds as Nix derivations**, so a
  `flake.lock` bump invalidates a datadir that a `~/.cache` dir would hand to
  the wrong major version; and predictions written *before* the first run.
  What it measured already moved the design: the **storage prediction was
  wrong** — WaveDB settles at 0.93× amplification against SQLite's 1.20× *while
  retaining every version*, the zstd dictionaries paying for the history and
  then some — while the real outlier is the **cold read**, ~950× slower than hot
  on an NVMe with the database in page cache, so the miss path is CPU and not
  IO, and it is the same defect as the update row (an update reads before it
  writes). It also showed the write cost is the **barrier, not the bytes**,
  which is what [0061](0061-relaxed-durability-window.md) answers.
  **0065 supersedes the shape, not the method.** 0060 made *the run* the unit —
  one process, every system in sequence, one JSON — and that bound in three
  places: peers were remeasured on every ~50-minute pass although the WaveDB
  commit is not among their inputs; there was no multi-threaded WaveDB row and
  could not be, since a harness driving `Store` directly has no requests to
  route (`engine.rs:184` is `Shards::start(store, 1)`, and its comment is right
  about why); and generation ran *serially with* the operation it fed while the
  servers' background threads ran *concurrently with* the timed window. 0065
  makes **the row** the unit — one `(system, variant, durability, workload,
  tier)` measurement with its own identity, cage, process and stored file — and
  all three resolve as consequences: a peer row's digest omits the WaveDB SHA so
  it is reused rather than remeasured, a generator thread feeding **N = 3**
  key-partitioned consumers is finally a source of requests (so
  [0064](0064-pivot-owned-concurrency-PLANNED.md)'s shape becomes measurable),
  and everything competes inside one budget. Two consequences worth naming:
  throughput must become wall-clock while latency stays per-operation (summing
  overlapping windows over-reports by ~N), and `StoreOptions` gains
  `page_cache_bytes` + `record_cache_bytes` — neither reaches disk, so neither
  folds into the `STRUCT_HASH` — so a **fill** can run uncaged with both
  in-memory layers turned up, which is what makes the multi-million-row tiers
  affordable to build.
- **The relaxed row exists now, landed 2026-08-21 —
  [0061](0061-relaxed-durability-window.md) *(Implemented)*:** the measurement
  above showed the write cost is the **barrier, not the bytes** (SQLite pays
  84.7 kB per durable insert of a 255-byte record against WaveDB's 101.6 kB —
  the same number, because an `fsync` on btrfs costs ~85–100 kB whatever the
  database), and that the same SQLite goes 36× faster by not syncing per
  operation. `PageStore::open_with(.., StoreOptions { relax_window })` buys
  that: `apply` appends without a barrier and syncs once per elapsed window,
  one branch under the journal lock the write path already held — no timer, no
  flusher task, nothing new on disk. **Default zero**, so every existing caller
  keeps one barrier per batch; `flush()` forces it for the write that needs the
  stronger answer. A crash inside the window loses a *suffix* of acknowledged
  writes and never corrupts, which is `synchronous_commit=off`'s bargain and
  the reason it is offerable at all. The refinements it left out —
  bounded loss when a store goes quiet, `fdatasync`, per-write strength, a
  relaxed client cache — are
  [0062](0062-relaxed-mode-refinements-PLANNED.md).
- **Deferred (low priority):**
  [0036](0036-offline-write-queue-PLANNED-LOW.md) — W8 offline write queue
  (slice 1, Unique offline, *shipped*; the NonUnique/durable/conflict-surface
  remainder is deferred — the near-term focus is online + small offline),
  [0037](0037-multi-node-cluster-PLANNED-LOW.md) (multi-node cluster),
  [0038](0038-argon2-and-oauth-credentials-PLANNED-LOW.md) (Argon2/OAuth),
  [0039](0039-developer-experience-PLANNED-LOW.md) (M9 dev tooling),
  [0055](0055-sparse-index-merge-PLANNED-LOW.md) — the sparse index's merge,
  redistribute and root collapse: it grows and never shrinks, which is bounded
  (one entry per *segment*, so drained is still two or three levels) but
  permanent, and the sideways read a merge needs has to go through the
  `Overlay` rather than the `Store` — the reason this half of 0050 phase 3a
  was skipped while the rest landed.
- **Partial seams:** [0013](0013-permissions.md) (per-record grants, gate 4),
  [0023](0023-quick-node-and-gates.md) (node gates 5–6).

## Index

### Meta
| # | Title | Status |
|---|-------|--------|
| [0000](0000-rfc-process.md) | The RFC process | Accepted |

### Vision & rules
| # | Title | Status |
|---|-------|--------|
| [0001](0001-vision-and-non-goals.md) | Vision, motivation, and non-goals | Accepted |
| [0002](0002-architectural-hard-rules.md) | Architectural hard rules | Accepted |

### Foundations
| # | Title | Status |
|---|-------|--------|
| [0003](0003-wavewire-wire-format.md) | The WaveWire wire format | Implemented |
| [0004](0004-struct-hash-and-schema-evolution.md) | STRUCT_HASH identity & schema evolution | Implemented |
| [0005](0005-composite-ids-and-bit-budgets.md) | Composite IDs and bit budgets | Implemented |
| [0006](0006-platform-seam.md) | The platform seam (native ⇄ browser) | Implemented |

### Data model
| # | Title | Status |
|---|-------|--------|
| [0007](0007-tenancy-and-data-ownership.md) | Tenancy and data ownership | Implemented |
| [0008](0008-store-trait-and-atomic-batch.md) | The Store trait and the atomic batch | Implemented |
| [0009](0009-anchors-succession-and-history.md) | Anchors, Succession, and history (DB-1) | Implemented |
| [0010](0010-metadata-and-record-envelopes.md) | Metadata and record envelopes | Implemented |
| [0011](0011-bptree-index-and-collections.md) | B+tree index, collections, and Pivots | Implemented |
| [0012](0012-natural-keys.md) | Natural keys (`#[wavedb::key]`) | Implemented |
| [0013](0013-permissions.md) | Permissions | Partial |

### Macros & exposure
| # | Title | Status |
|---|-------|--------|
| [0015](0015-wavedb-macro.md) | The `#[wavedb]` declarative macro | Implemented |
| [0016](0016-server-functions.md) | Server functions (`#[server]`) | Implemented |
| [0017](0017-exposure-registry-and-side-features.md) | The exposure registry & schema side-features | Implemented |

### Storage engine
| # | Title | Status |
|---|-------|--------|
| [0018](0018-storage-engine.md) | The storage engine | Implemented |
| [0019](0019-journal-rooted-recovery.md) | Journal-rooted recovery | Implemented |

### Transport, node & sync
| # | Title | Status |
|---|-------|--------|
| [0020](0020-net-transport-dumb-tunnel.md) | The net transport (dumb tunnel) | Implemented |
| [0021](0021-connection-manager.md) | The connection manager | Implemented |
| [0022](0022-live-sync-navigation-catchup.md) | Live sync by navigation catch-up | Implemented |
| [0023](0023-quick-node-and-gates.md) | The quick-node and enforcement gates | Partial |

### Client & targets
| # | Title | Status |
|---|-------|--------|
| [0024](0024-client-db-and-cache.md) | The client `Db` and write-through cache | Implemented |
| [0025](0025-wasm-indexeddb-target.md) | The wasm / IndexedDB target | Implemented |
| [0026](0026-auth-tokens.md) | Auth: access & refresh tokens | Implemented |

### Roadmap — in progress & planned
| # | Title | Status |
|---|-------|--------|
| [0034](0034-ws-reconnect-catchup.md) | W6: WebSocket reconnect catch-up | Implemented |
| [0035](0035-http-piggyback-and-idle-backoff.md) | W7: HTTP piggyback + idle backoff | Implemented |
| [0036](0036-offline-write-queue-PLANNED-LOW.md) | W8: Offline write queue (slice 1 shipped) | Planned (low) |
| [0037](0037-multi-node-cluster-PLANNED-LOW.md) | Multi-node cluster | Planned (low) |
| [0038](0038-argon2-and-oauth-credentials-PLANNED-LOW.md) | Argon2 & OAuth/OIDC credentials | Planned (low) |
| [0039](0039-developer-experience-PLANNED-LOW.md) | Developer experience (M9) | Planned (low) |
| [0041](0041-single-barrier-checkpoint.md) | Single-barrier checkpoint | Implemented |
| [0042](0042-free-space-defragmentation.md) | Free-space defragmentation | Implemented |
| [0043](0043-descriptors-in-the-commit-frame.md) | Descriptors in the `Commit` frame | Implemented |
| [0044](0044-page-cache-PLANNED-LOW.md) | The page cache | Planned (low) |
| [0045](0045-vector-search-PLANNED.md) | Vector search | Planned |
| [0046](0046-directory-deltas-in-the-window.md) | Directory deltas in the settle window | Implemented |
| [0047](0047-generational-journal-retirement.md) | Generational journal retirement | Implemented |
| [0048](0048-chained-addressing-log.md) | The addressing log as a chain | Implemented |
| [0049](0049-elastic-pages-and-load-driven-splits.md) | Elastic pages and load-driven splits | Implemented |
| [0050](0050-clustered-record-chains.md) | Clustered record chains (B+trees become opt-in) | Implemented |
| [0051](0051-ordered-record-lists.md) | Declared lists: sorted chains + sparse index | Implemented |
| [0052](0052-segment-size-as-the-pagination-unit.md) | Segment size as the pagination unit | Implemented |
| [0053](0053-tenant-fair-cache-retention-PLANNED.md) | Tenant-fair cache retention | Planned |
| [0054](0054-no-duplication-by-default.md) | No duplication by default | Implemented |
| [0055](0055-sparse-index-merge-PLANNED-LOW.md) | Sparse-index merge and root collapse | Planned (low) |
| [0056](0056-fuzzy-string-search-WIP.md) | Fuzzy string search | In progress |
| [0057](0057-page-arena-and-checkpoint-staging.md) | The page arena and checkpoint staging | Planned |
| [0059](0059-object-storage-capacity-tier-PLANNED.md) | Object storage as the capacity tier | Planned |
| [0061](0061-relaxed-durability-window.md) | Relaxed durability: a group-commit window | Implemented |
| [0062](0062-relaxed-mode-refinements-PLANNED.md) | Relaxed mode refinements | Planned |
| [0063](0063-engine-yield-map-and-interruptible-engine-PLANNED.md) | The yield map, and an interruptible engine | Planned |
| [0064](0064-pivot-owned-concurrency-PLANNED.md) | Pivot-owned concurrency | Planned (first slice built) |
| [0065](0065-benchmark-suite-ii-the-row-as-the-unit.md) | Benchmark suite II: the row as the unit | Planned |

### Deprecated / superseded
| # | Title | Superseded by |
|---|-------|---------------|
| [0014](0014-schema-evolution-hooks-DEPRECATED.md) | Schema-evolution lookup hooks | [0040](0040-schema-migration-and-version-skew-DEPRECATED.md) |
| [0040](0040-schema-migration-and-version-skew-DEPRECATED.md) | Schema migration & node/client version skew | *dropped* — migration is the developer's ([0004](0004-struct-hash-and-schema-evolution.md)) |
| [0027](0027-doubly-linked-modification-chain-DEPRECATED.md) | Doubly-linked modification chain | [0009](0009-anchors-succession-and-history.md) |
| [0028](0028-journal-commit-cursor-sync-DEPRECATED.md) | Journal commit-cursor sync | [0022](0022-live-sync-navigation-catchup.md) |
| [0029](0029-bloom-filter-screen-sync-DEPRECATED.md) | Bloom-filter screen-sync | [0022](0022-live-sync-navigation-catchup.md) |
| [0030](0030-superblock-pointer-checkpoint-DEPRECATED.md) | Superblock-pointer checkpoint | [0019](0019-journal-rooted-recovery.md) |
| [0031](0031-node-per-page-bptree-DEPRECATED.md) | One-node-per-page B+tree format | [0011](0011-bptree-index-and-collections.md) |
| [0032](0032-node-side-poll-buffer-DEPRECATED.md) | Node-side stateful poll buffer | [0022](0022-live-sync-navigation-catchup.md) |
| [0033](0033-cold-history-slow-node-tier-DEPRECATED.md) | Cold/history slow-node tier | removed |
| [0058](0058-per-type-actors-DEPRECATED.md) | Per-type actors, and a `Send` engine | [0064](0064-pivot-owned-concurrency-PLANNED.md) (the unit) + [0063](0063-engine-yield-map-and-interruptible-engine-PLANNED.md) (the motivation) |
| [0060](0060-comparative-benchmark-suite-DEPRECATED.md) | Comparative benchmark suite (vs MongoDB/PostgreSQL/MySQL/SQLite) | [0065](0065-benchmark-suite-ii-the-row-as-the-unit.md) (the row, not the run) |

## Status vocabulary

Delivery status is a header field and, for the non-baseline states, also a
**filename marker** (like `DEPRECATED`) so a directory listing *is* the roadmap:

| Status | Filename marker | Meaning |
|--------|-----------------|---------|
| **Accepted** | — | The decision stands; may be a policy rather than code. |
| **Implemented** | — | Landed and proven; the landing date is in the RFC's status header. |
| **Partial** | — | Core built, a seam remains; the RFC names it. |
| **In progress** | `WIP` | Actively being built now. |
| **Planned** | `PLANNED` | Accepted, will be built, not started. |
| **Planned (low)** | `PLANNED-LOW` | Deferred; someday. |
| **Deprecated** | `DEPRECATED` | Replaced/rejected; body points at what replaced it. |

Changing status is a **rename** (the number and history stay); a deprecated or
superseded idea keeps its file so the dead idea stays findable. The number is
never reused.
