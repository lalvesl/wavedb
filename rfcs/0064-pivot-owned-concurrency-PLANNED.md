# RFC 0064 — Pivot-owned concurrency

- **Status:** Planned — opened 2026-08-22. **First slice built**, see
  "What is built" below.
- **Supersedes:** [RFC 0058](0058-per-type-actors-DEPRECATED.md) (per-type
  actors). 0058's *motivation* stands and is recorded in
  [0063](0063-engine-yield-map-and-interruptible-engine-PLANNED.md); its
  central choice — the type as the unit of ownership — is what this RFC
  replaces.
- **Builds on:** [0063](0063-engine-yield-map-and-interruptible-engine-PLANNED.md)
  (the yield map, and the A2 fix that came out of it),
  [0011](0011-bptree-index-and-collections.md) (the Pivot, whose real
  cardinality this RFC turns out to depend on)
- **Amended by:** [0067](0067-thread-per-core-shards-WIP.md) — the
  single disk actor becomes one engine per shard, owned by the shard's
  thread; `shard_of` becomes a frozen on-disk placement.
- **Crates:** `wavedb-storage` (cache ownership), `wavedb-quick-node` (the
  shards and the disk actors), `wavedb-core` (routing metadata)
- **Code:** `crates/wavedb-core/src/collection.rs`,
  `crates/wavedb-core/src/collection_handle.rs`,
  `crates/wavedb-core/src/local_handle.rs`,
  `crates/wavedb-storage/src/struct_storage.rs`,
  `crates/wavedb-storage/src/apply.rs`,
  `crates/wavedb-core/src/expose.rs`,
  `crates/wavedb-macros/src/exec_ops.rs`

## Summary

The unit of concurrency in WaveDB is the **Pivot instance** — one collection —
and not the type. Two collections of the same type share nothing: different
Pivot record, different B+tree roots, different chain segments, different list
segments. Only the items *inside* one collection are inherently serial, because
those genuinely share a tree.

This was not visible because the documentation said the wrong thing. Four
places claimed a Pivot is created "one per tenant per type"; the API imposes no
such limit and the shop workload contradicts it directly — each `Shopping`
holds its own `Product` collection, so there are as many `Product` collections
as there are orders. Corrected in this RFC's first commit.

The consequence is large enough to replace 0058's design outright:

- **No hot-type ceiling.** 0058 accepted that a busy type is one core. Under
  Pivot ownership the busiest type in the shop is not one actor, it is one
  actor per order.
- **No balancer.** With millions of naturally independent units,
  `shard = hash(pivot_id) % N` is uniform by construction: a function, not a
  routing table. Nothing splits, nothing migrates.
- **No `Lane::Tree`, and therefore no `STRUCT_HASH` break.** 0058 needed it to
  finish a partition that this one does not need at all.
- **The developer controls concurrency by modelling.** Nesting Pivots declares
  parallel units. That is explainable in a sentence, which range-sharding is
  not.

One piece is **separable and worth doing on its own**, the way A2 came out of
0063: `Get`, `Update` and `Remove` must carry their pivot on the wire (see
[Routing](#routing-at-ingress)). It is what makes *Pivot-grained* routing
possible, it deletes a read per update and remove today, and it needs none of
the rest of this RFC.

## What is built

The ownership boundary and the ingress path, at type granularity.

- **`shard::disk`** — one actor owns the `PageStore`. Nothing else can reach
  it, so the process-wide `EngineClaim` has one holder by construction. Two
  queues, requests over maintenance, arbitrated by `shard::priority` against
  the journal's length; `settle_step` makes a settle round interruptible so
  the valve has something to interleave between.
- **`shard::store`** — `ShardStore`: a shard's cache in front of the actor.
  Non-`Send` by construction. It remembers absence, so **only a shard may
  cache**; every other holder takes the cacheless `Shards::store()`.
- **`shard::lock`** — `OwnerLocks`, the brake. One table for the node behind
  an `Arc`, because two operations on one owner can now be picked up by two
  threads. `CONCURRENCY_BRAKE.md` is the standalone account.
- **`shard::router` / `shard::worker`** — ingress routing. A POST is decoded
  on the accept thread and executed on a shard worker: its own thread, its own
  runtime, its own cache. The accept loop holds no engine on that path.
  Committed mutations come back as plain `Mutation`s to the accept thread's
  publisher, because the WebSocket subscription table lives there.

**Measured.** The comparative suite reports WaveDB four times — two durability
modes across two engine seams, `wavedb` (the `PageStore` in the caller's thread)
and `wavedb-sharded` (this design: disk actor on its own thread, reached by
message through a `ShardStore`). Same workload, same index layer, generic over
the seam. It measures **the cost of the boundary and not the benefit of
parallelism**, because the harness issues one operation at a time *and* because
the brake's key is the type — so this workload is one owner whatever `N` is.
That pair is the honest measurement available before the two things the
concurrency actually needs (a concurrent client, and the Pivot-grained brake)
exist. One caveat rides with it: `ShardStore` memoises on read while the
engine's per-type cache is a write cache, so the sharded row carries a read
cache the direct row lacks — RFC 0044's gap, closed as a side effect — and
`read_cold` is therefore not comparable across the pair. `benches/README.md`
carries this, and each sharded row repeats it in its own `notes`.

**Not built, and each says why in place:** the journal/page actor split (the
settle straddle below), the per-type caches moving into the shards (same),
Pivot-grained routing and braking (the wire change above — both must narrow
together, or an `Insert` on a Pivot key and an `Update` on a type key exclude
nothing), and WebSocket `Call` routing (a session binds identity once at
`Hello`; routing re-verifies per command, so a long watch would start refusing
at token expiry — a behaviour change, not a refactor).

## Motivation — why the type is the wrong unit

RFC 0058 chose the type because the storage layout already partitions by it
(per-`STRUCT_HASH` page directories, six slots per type). That is a real
partition, and it is the right one *for storage*. It is the wrong one for
ownership, for two reasons.

**First, one slot is global.** Every B+tree node of every tree of every type
settles into a single reserved slot:

```rust
// crates/wavedb-core/src/index/node.rs:41
pub const BPTREE_NODE_STRUCT_HASH: u64 = 0x42_50_54_52_45_45_00_01; // "BPTREE\0\x01"
// crates/wavedb-storage/src/struct_storage.rs:216
pub static BPTREE_NODE_STORAGE: StructStorage =
    StructStorage::without_compression(BPTREE_NODE_STRUCT_HASH);
```

Node ids are unique globally by `key_nanos()`'s fused counter
(`mint_node_id`), not by being scoped to a tree — which is exactly what lets
them share one flat keyspace. Every mutating op touches the index, so under
per-type actors this one slot's owner sees 100% of write traffic. 0058 found
this and named the fix (`Lane::Tree`, splitting the slot per type) while
recording its price: a `STRUCT_HASH` break for every indexed type, charged to
a concurrency refactor.

**Second, and this is what dissolves the first: ownership does not have to
follow storage partitioning.** The disk sits behind a single owner regardless
(see the disk actors below), so the shared *slot* costs nothing. What needs an
owner is the **cache**. A tree's nodes belong to whoever owns that tree, and
whoever mints a node knows which tree it is for — the raw `Id` does not name
its owner, but the caller always does. That is the same shape as 0063's A2
finding, and the same resolution: pass the owner down rather than search for
it.

Once the two axes are allowed to differ — **cache partitioned by owner, page
directory partitioned by type** — `Lane::Tree` has nothing left to buy.

## Design

### Nothing above `Store` changes

The whole index layer — `Collection`, `BpTree`, the chains, the declared lists,
everything `#[wavedb]` expands to — reaches its backend through four methods
(`get`, `get_of`, `apply`, `note_mutation`) and takes the store **per call**
rather than owning one (`Collection<T>` holds a pivot, a tenant and two
capacities; `BpTree<K>` holds a root and two capacities). So this entire design
slots in *behind* that trait, and the layer above is untouched.

`impl Store for PageStore` becomes `impl Store for ShardStore`. That is the
extent of the seam.

### The shape

```
                    ingress (net) — routes once
                    │
       ┌────────────┼────────────┬─────────────┐
       ▼            ▼            ▼             ▼
   shard 0      shard 1      shard 2       shard N-1     ← current_thread rt, one per core
   ─────────────────────────────────────────────────
   owns : the record cache for the pivots assigned to it
   runs : Collection / BpTree — today's code, unchanged
   is   : impl Store for ShardStore
       │                             │
       │ SPSC ring                   │ SPSC ring
       ▼                             ▼
  ┌───────────────────┐       ┌───────────────────────────┐
  │ journal actor     │       │ page actor                │
  │ ───────────────── │       │ ───────────────────────── │
  │ append + fsync    │       │ data.bin (pread/pwrite)   │
  │ group commit      │       │ alloc, meta, retiring     │
  │ sequential writes │       │ directories, dictionaries │
  │                   │       │ page cache (0044) + zstd  │
  │                   │       │ settle / checkpoint /     │
  │                   │       │ defrag, and block dedup   │
  └───────────────────┘       └───────────────────────────┘
```

Two files, two access patterns, two threads. The journal is append-only and
fsync-bound; `data.bin` is random-access and is what has a cache worth owning.
Splitting the disk owner in two is a physical distinction, not an arbitrary one
— and keeping page *writes* with page *reads* is what would let a page cache
stay coherent without either side telling the other to invalidate.

**Scope:** the page cache in that box is
[0044](0044-page-cache.md), which is *Planned (low)* and **not part
of this work**. It appears here only because this RFC decides *where it would
belong* if it is ever built — the claim is placement, not delivery. The same
goes for the zstd move: today decompression happens on whatever thread reads,
and putting it in the page actor is a consequence of this design rather than a
separate feature.

### Where every lock goes

This is the concrete form of "ownership instead of synchronisation": each field
of today's `PageStore` and `StructStorage` ends up with exactly **one** owner,
and its lock disappears for want of a second party rather than getting faster.

| today | owner |
|---|---|
| `journal: Mutex<Journal>` | journal actor |
| `file: BlockFile` | page actor |
| `alloc: Mutex<BlockAllocator>` | page actor |
| `meta: Mutex<MetaLog>` | page actor |
| `retiring: Mutex<Option<Retiring>>` | page actor |
| `StructStorage.dir: StructDirectory` | page actor |
| `StructStorage.dict: StructDictionary` | page actor |
| `StructStorage.cache: StructMemCache` | **shards**, partitioned by pivot |
| `StructStorage.dead: RwLock<BTreeSet<u128>>` | **shards**, likewise |
| `pending: Mutex<Touched>` | per shard, drained to the page actor |
| `types: Vec<&'static StructStorage>` | unchanged — read-only after open |

`dir` and `dict` stay **per type and in the page actor**; they are never
partitioned. That row is why `Lane::Tree` is unnecessary: the global B+tree node
slot belongs to the page actor, and there is only one page actor regardless.

### An insert, step by step

1. ingress reads `Insert(pivot, body)` → `shard = hash(pivot) % N`
2. the shard runs `Collection::insert` — today's code, unmodified
3. the tree descent's reads hit the shard's own cache; a miss becomes a message
   to the page actor and an `.await`
4. the plan comes out as a `Vec<Write>` with its `Expect` guard in front
5. the shard **validates the `Expect` locally** — it owns those records, so this
   needs no lock at all. Today it happens *inside* the journal lock, which is
   where RFC 0063 found A2.
6. the batch goes to the journal actor; the shard awaits the ack
7. the journal actor merges whatever else arrived while it was syncing —
   **group commit as such**, "everything that landed during the last fsync"
   rather than [0061](0061-relaxed-durability-window-DEPRECATED.md)'s timer
8. on the ack, the shard commits to its own cache and queues the touched ids
9. later, the page actor drains and settles

### Inside one shard

A shard is not a serial loop: it is a `current_thread` runtime with many tasks
sharing its state through `Rc` with no synchronisation. The ordering rule is the
one the unit already implies:

- **concurrent across different pivots** — they share nothing;
- **serial within one pivot** — they share a tree, chains and lists.

The second half is load-bearing rather than decorative. Between sending a batch
and receiving the ack (steps 6–8) the task is parked, so another task on the
same shard runs — and would otherwise observe pre-batch state. Serialising per
pivot forbids it, and the serialisation is local to one thread: no atomics, no
cross-core traffic.

#### What keeps a shard from moving

Three separate questions hide behind "how do you stop a shard drifting between
threads", and they have different kinds of answer — one is a guarantee, one is
a knob, one is a definition.

**Task ↔ thread: enforced by the type system, and the mechanism already
exists.** `wavedb-platform::task::spawn_detached` boots a dedicated thread
carrying its own `new_current_thread` runtime, and its bound is

```rust
F:   FnOnce() -> Fut + Send + 'static,
Fut: Future<Output = ()> + 'static,     // note: no Send bound
```

with a doc comment that already states the shard model: *"the future is built
**on** the new thread, so it may own non-`Send` state (channels into it are the
only thing that crosses threads)."* A shard is one of these. A non-`Send` future
**cannot** be moved to another thread — that is a compile error, not a scheduler
promise — and a current-thread runtime has no work-stealing to attempt it.

This is the migration that matters. Work stealing can move a task at *every
await point*, which is thousands of times a second and is what destroys the
locality this design exists for. Type-level ownership rules it out for free,
which is why the multi-thread build is N current-thread runtimes rather than one
`multi_thread` runtime.

**Thread ↔ core: the OS decides, and nothing here pins.** There is no affinity
code in the workspace (`sched_setaffinity`, `core_id`: none; the only CPU-mask
read is `benches/src/host.rs`, which is the harness). Linux may migrate a thread
under load imbalance, though CFS's wake affinity keeps a busy thread largely put
— pinning removes tail cases, not the average.

Explicit pinning should be **opt-in, not default**: under a `cpu.max` quota with
every core visible a fixed mask is either wrong or meaningless; pinning onto two
SMT siblings of one physical core silently halves throughput; and a library
running inside someone else's process cannot take the machine the way an
appliance (Seastar, ScyllaDB) does.

The usual objection to pinning — a blocked thread idles its core — does **not**
apply here, and that is worth recording: a shard never touches disk, since all
I/O belongs to the two actors. A pinned shard thread never blocks in a syscall,
which is exactly the condition under which pinning pays.

Crucially, **correctness does not depend on it.** Ownership is guaranteed by the
first point; pinning only buys cache warmth. It is a tuning knob, not a
prerequisite.

**Pivot ↔ shard: a pure function.** `hash(pivot_id) % N` with `N` fixed at
startup. A pivot never changes shard for the life of the process, so there is no
ownership migration and no table to go stale — which is the same property that
removed the balancer. The price, stated plainly: changing `N` means a restart.
Live resharding would need cache entries moved between threads, which is the
machinery range-sharding required and this design does not.

### The unit, and what it does and does not share

An owner is one **Pivot instance** (a NonUnique collection), or the pair
`(tenant, STRUCT_HASH)` for a Unique type, which has no Pivot.

| | shared between two collections of one type? |
|---|---|
| Pivot record, B+tree roots, recency/dead chains, list segments | **no** — this is the whole point |
| `mem_cache` for the type's `STRUCT_HASH` | today yes; **must be repartitioned by owner** |
| page directory + zstd dictionary for the type | yes — and that is fine, it lives behind the disk actors |
| `data.bin`, the journal | yes — one file, one owner, group commit |

Inside one collection everything is shared and therefore serial. That is not a
limitation to fix: those records share a tree. A developer who wants
parallelism nests Pivots, which is a modelling decision they already make for
other reasons.

### Assignment: a function, not a balancer

```
shard = hash(pivot_id) % N          // N = worker threads
```

No routing table, no split, no migration, no rebalancing pass. Uniformity comes
from there being millions of units, not from anyone measuring load. This is the
direct answer to the objection that sank the range-sharding alternative below.

### One scheme, three granularities

```
tenant             →  distributed shard   (later; the Id already carries TENANT u48)
  pivot instance   →  thread              (this RFC)
    items          →  serial              (inherent — they share the tree)
```

Every Pivot belongs to exactly one tenant (`Collection` holds `tenant: U48`;
`BpTree` is tenant-scoped), so the distributed step is a *coarsening* of the
same hierarchy rather than a second design. Nothing here is rebuilt to take it.

### Routing at ingress

Verified against `Command` (`crates/wavedb-core/src/expose.rs`) and the
generated exec ops (`crates/wavedb-macros/src/exec_ops.rs`):

| command | pivot on the wire | routes by |
|---|---|---|
| `Insert`, `All`, `Listed`, `ListLen`, `Changes` | yes, in the payload | the pivot, directly |
| `Save`, `History` | Unique — none exists | `(tenant, STRUCT_HASH)` |
| `Get`, `Update`, `Remove` | **no**, only the `Id` | **must change** — see below |

#### Why the missing three cannot be resolved node-side

The tempting answer is that the node recovers the pivot itself: `Update` and
`Remove` already do, by reading the record's `Metadata` back-link
(`expose::record_pivot` → `get_of` → `meta.pivot_id`), and that read wants no
tree and no pivot. So it looks like the page actor could do it before routing.

**It cannot**, and the reason generalises to `Get`. Read the read path against
the ownership table above:

```rust
// apply.rs — read_of
if let Some(bytes) = slot.get(id) { return Ok(Some(bytes)); }  // cache  → shard
self.read_from_pages(slot, id)                                  // page   → page actor
// …and read_from_pages opens with `if slot.is_removed(id)`     // dead   → shard
```

The cache and the tombstone set are **shard-owned**; only the settled page is
the page actor's. A page actor answering a read alone is therefore wrong three
ways: it misses a record written but not yet settled, it serves stale bytes when
a newer version sits in a shard's cache, and it resurrects a record whose
tombstone lives in a shard. (For `record_pivot` specifically a *stale* read
would still be harmless — `pivot_id` is immutable for the life of a record — but
a *missing* one is not, and a just-inserted record is on no page at all.)

So a read cannot be served without its owner, the owner cannot be found without
the pivot, and the pivot cannot be found without a read. That circle has no
node-side exit.

#### The pivot is already in the caller's hand

```rust
// crates/wavedb-core/src/collection_handle.rs
pub async fn get(...)    { db.get_record(self.pivot, id).await }
pub async fn save(...)   { db.update(self.pivot, id, value).await }
pub async fn remove(...) { db.remove::<T>(self.pivot, id).await }
```

`Collection<T>` holds `pivot: LocalId` and all three hand it to the handle. The
**client stub drops it** before the wire, and the node then pays a read to
recover what the caller was holding.

The change is to stop dropping it: `Get`, `Update` and `Remove` carry the pivot
in their payload, exactly as the other five already do. Then every NonUnique
command routes at ingress with no read at all, and `record_pivot` is **deleted**
— removing one `get_of` plus an envelope decode per update and remove, and a
real page read whenever the cache is cold. Like A2, it is worth doing on one
thread too.

It is a `Command` payload change, so a wire change: free under the pre-release
policy, and `Command` is engine plumbing rather than a user type, so it folds
into **no** `STRUCT_HASH`.

#### Routing is a property of the operation, not of each id

Worth stating separately, because "hash the id" is the obvious implementation
and it is **wrong**. A Unique `save` writes two ids in one atomic batch:

```rust
// crates/wavedb-core/src/record.rs:269
let slot = archive_id(plan.hash, plan.shape, authored, plan.tenant); // KEY = the instant
writes.push(Write::Put(slot, …));                                    // superseded version
writes.push(Write::Put(plan.live_id, …));                            // KEY = the STRUCT_HASH
```

Two ids with nothing in common. `hash(id) % N` would put them on different
shards and there would be no owner for the batch to be atomic in.

So the owner is decided **once, at ingress**, and every id the resulting batch
writes belongs to it whatever that id hashes to. The same already holds for a
collection — its B+tree nodes and chain segments carry ids that would route
elsewhere — but a Unique type makes it plain, since there is no Pivot to
suggest the answer.

A Unique owner is `(tenant, STRUCT_HASH)`: the anchor is `KEY = STRUCT_HASH`
under that tenant, and a Unique type has no tree and no chains, so an anchor
plus derived archive slots is everything it stores.

Its routing key is the two **concatenated** — `(tenant << 15) | type_salt` —
rather than combined, and the reason is worth keeping:

- **Both components must be in it.** The tenant alone puts every type of a
  heavy writer on one shard; the type alone puts every tenant's `User` there,
  and `User` is exactly the type every tenant has.
- **Concatenation, because 48 + 15 = 63 bits fit.** The key is then injective,
  so distinct owners can only collide at the final `% shards`, which nothing
  avoids. `tenant ^ struct_hash` is not injective and its collisions are
  *systematic*: for any two types `h1`, `h2`, every tenant pair with
  `t2 = t1 ^ h1 ^ h2` maps to one key. A family, not an accident.
- **The 15-bit `type_salt`, not the whole hash**, is what makes the fit
  possible — 48 + 64 does not. The price is that two types sharing their low
  15 bits route together within one tenant, which is already a known condition
  the exposure registry warns about at compile time.

A live record and its whole version history therefore land on one shard — and
**not because their ids agree**. They do not: the anchor is
`Id::new(STRUCT_HASH, tenant, true, 0)`, salt *zero*, while an archive is
`Id::new(instant, tenant, false, type_salt(hash))`. They converge because the
owner comes from the operation, so the anchor's salt is never consulted.

#### The pattern, named

This is the third instance of one shape, and it is worth stating as a rule
rather than rediscovering a fourth time:

| where | what the `Id` failed to name | fix |
|---|---|---|
| `Write::Remove` / `Expect` (0063 A2) | the **type** — so the guard scanned every slot under the journal lock | carry the `STRUCT_HASH`; *landed 2026-08-21* |
| `BPTREE_NODE_STRUCT_HASH` nodes | the **tree** — every tree's nodes share one flat keyspace | the minter knows; pass the owner down |
| `Get` / `Update` / `Remove` | the **owner** — see above | carry the pivot; this RFC |

**An `Id` in WaveDB names neither its type nor its owner, and the caller always
knows both.** Every time that has cost something, the answer was to pass the
knowledge down rather than search for it — and every time, the search was
happening in the most expensive place available.

### The disk actors, and the Redis lesson

The two named above — the journal actor and the page actor. Single ownership of
the page actor is what makes de-duplication possible: two shards wanting the
same block produce one read, not two. It is also the natural home for
[0044](0044-page-cache.md)'s page cache, which needs exactly one
owner to stay coherent.

Redis is the instructive precedent and it is usually mis-cited. Its model was
never "one thread does everything" — Redis 6 added I/O threads because parsing
and syscalls dominated, and the *data core* stayed single-threaded. The rule is
**one thread owns the data structure; everything that is not data-structure
mutation leaves it.** Applied here that gives a third candidate beyond the two
above: **zstd**. Decompression is heavy CPU that is not data-structure work, and
it belongs to the page actor, which is already holding the page's bytes.

What does *not* transfer: a Redis op is a hash lookup in the hundreds of
nanoseconds; a WaveDB op is a B+tree descent plus wire decode. One thread
saturates far sooner, which is why the unit has to be fine-grained — and, under
this RFC, is.

### Request/reply without blocking

An operation that needs a block sends to the page actor and does not block; the
result arrives as a message and the operation resumes. That is what `await`
already is, provided each request is its own task inside the shard's
current-thread runtime:

```rust
let bytes = page_actor.get(block).await;   // this task parks; the shard runs another
```

All tasks in a shard share its non-`Send` state through `Rc` with no
synchronisation. The suspended state machine is generated rather than
hand-written, and **0058's unaddressed actor-to-actor deadlock surface
disappears** — it only exists when the message is synchronous.

### The compile-time single/multi switch

The switch is `N`:

- **single** — one shard; the disk actors are inlined as direct calls. This is
  what exists today, and it is what wasm gets.
- **multi** — N shards, one `current_thread` runtime per core, disk actors on
  their own threads. Core pinning is optional and buys only cache warmth; see
  [What keeps a shard from moving](#what-keeps-a-shard-from-moving).

The shard does not know which it is. Two consequences worth stating because
they reverse what earlier drafts of this work assumed:

- **tokio's work-stealing scheduler is the thing to avoid, not tokio.** Work
  stealing migrates tasks between cores and destroys the locality the design
  exists for — and it is *why* it demands `Send`. N current-thread runtimes are
  supported and are what this wants.
- **`Rc` survives; the engine does not become `Send`.** What crosses threads is
  the *messages* to the disk actors — `Vec<Write>`, a block request — which are
  plain data and already wire types. The `Send` requirement collapses from "the
  whole engine" to "the message types". No `MaybeSend` seam, no desugaring of
  `Store` off async-fn-in-trait.

### What this does to RFC 0063

0063's two invariants land differently:

- **I1 (a batch is atomic across the per-type caches) dissolves.** A batch
  belongs entirely to one owner, so it is atomic by ownership rather than by a
  lock — no seqlock, no epoch, no staged publish. This was going to be the one
  genuinely expensive part of going multi-threaded.
- **I2 (`pending` empty ⇒ settled) is unaffected**; `evict_settled` already
  quiesces writers deliberately.
- **The yield map survives entirely**, and so does the interruptible `drain`:
  the single-shard build still has one thread, and wasm still needs to give it
  up. 0063 Part 3 remains the next executable step and is unblocked by this RFC.

## Alternatives

- **Per-type actors ([0058](0058-per-type-actors-DEPRECATED.md)).** Superseded.
  It relocates the write hotspot rather than removing it (the queue replaces the
  lock; a queue round-trip costs *more* atomic traffic than an uncontended
  `parking_lot` lock, not less), it accepts a hot-type ceiling this RFC does not
  have, and it charges a `STRUCT_HASH` break for `Lane::Tree`.
- **Range-sharding inside a collection.** The design this RFC was drafted as
  first, and it loses on a hard rule: a B+tree spans ranges, so splitting
  ownership forces the index to become two-level — a change to stored bytes, and
  therefore a `STRUCT_HASH` break for every indexed type. It also needs the
  balancer, the routing table and the runtime migration that Pivot assignment
  does without. It remains the answer for one collection too large for a thread,
  and that is a real case, but it is not the opening move.
- **Tenant-sharding only.** Correct and free, but too coarse: single-tenant is
  a stated sweet spot, and there it degenerates to one shard. It is the *outer*
  granularity here, not the unit.
- **Sharing by lock, finer-grained.** Keeps the statics and leaves I1 as
  discipline, which is how it came to be undocumented.
- **`spawn_blocking` around the synchronous engine.** Still a legitimate interim
  native measure for the checkpoint stall, and still does nothing for wasm. It
  composes with this rather than competing.
- **A `static` per actor reached through `unsafe`** (raised during design).
  Rejected twice over: `OnceLock` gives the same access safely for a relaxed
  load, and more to the point per-shard state should not be `static` at all — if
  it belongs to a thread it is a local of that thread's loop, and there is no
  global access left to arrange.

## Open questions

- **The settle path straddles the boundary — the main unresolved question.**
  `plan_slot` reads the caches (shard-side) and writes pages and the type's zstd
  dictionary (page-actor-side). Putting the dictionary in the page actor settles
  the *divergence* half — two shards must not grow one type's dictionary
  independently, since a page records its dictionary as a prefix length — but it
  leaves the page actor needing to **pull record bytes out of shards** in order
  to plan, and the shape of that exchange is not designed. The alternative
  (shards plan their own page images) hands the dictionary problem straight back.
- **`pending` and the ordering of `Touched`** are process-global today. Per-shard
  queues are the obvious shape; how they compose into one checkpoint round is
  part of the question above.
- **The `Expect` guard** is validated under the journal lock against pre-batch
  state. Under this design the guarded records belong to the shard, so it should
  validate locally and the journal actor only appends — this looks *simpler*, but
  it has not been checked against replay ordering.
- **Whether the per-pivot serialisation needs a structure at all.** "Serial
  within a pivot" may fall out of how operations are driven, or may need an
  explicit per-pivot queue inside the shard. The visibility hole it closes
  (another task observing pre-batch state while the first awaits its ack) is
  real either way, and does not exist today because the engine never awaits.
- **Cost of repartitioning `mem_cache`.** It is a real edit to
  `struct_storage.rs` and the honest price of the design.
- **The nesting chain crosses shards, and that was not examined.** A Unique
  record routes by `(tenant, STRUCT_HASH)` and the collections it holds route
  by their Pivots, so `User` and its `Shopping` collection land on different
  shards — and a profile read walks exactly that link. The crossing is the
  common path, not a corner.
  For reads it costs one message (µs) against a page read (tens of µs) and
  needs no atomicity across the hops, so it is probably acceptable; nothing
  measures it. The tension it exposes is structural, and neither end of it is
  free: routing by **Pivot** maximises parallelism and pays locality along the
  nesting chain; routing by **tenant** maximises locality and collapses to one
  shard in the single-tenant case, which is a stated sweet spot.
- **Nothing here is measured.** 0058 was parked partly for treating estimates as
  findings; the orders of magnitude quoted in discussion (uncontended lock vs
  channel round-trip) are from the literature, not from this machine, and the
  hot-unit ceiling is not measured at all.
