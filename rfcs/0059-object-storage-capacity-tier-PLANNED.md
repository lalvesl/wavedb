# RFC 0059 — Object storage as the capacity tier

- **Status:** Planned — opened 2026-08-08. Design only; nothing is built.
- **Superseded in part by:** [RFC 0070](0070-object-storage-backend-WIP.md) — §2–§5
  (remote descriptor, object per bucket, demotion, read path). §1, the
  archive lane, stands on its own.
- **Crates:** `wavedb-storage` (the tier), `wavedb-macros` (one emitted slot),
  `wavedb-quick-node` (configuration)
- **Code (target):** `crates/wavedb-storage/src/{directory,block,commit,checkpoint,defrag,page_store}.rs`,
  `crates/wavedb-macros/src/storage_statics.rs`
- **Revives the problem of:** [RFC 0033](0033-cold-history-slow-node-tier-DEPRECATED.md)
  (cold/history tier — deprecated, *answer* rejected, problem left open)
- **Builds on:** [RFC 0043](0043-descriptors-in-the-commit-frame.md) and
  [RFC 0046](0046-directory-deltas-in-the-window.md) (the descriptor is the only
  addressing state, and it already travels as a delta),
  [RFC 0042](0042-free-space-defragmentation-DEPRECATED.md) (the background-pass shape this
  copies), [RFC 0057](0057-page-arena-and-checkpoint-staging.md) (the memory tier),
  [RFC 0053](0053-tenant-fair-cache-retention-PLANNED.md) (the policy constraint
  phase 3 inherits)

## Summary

Three tiers, addressed by one existing 64-bit word:

| Tier | Holds | Latency | Mechanism today |
|------|-------|---------|-----------------|
| **Memory** | the write path in flight, and the hottest pages | ns–µs | per-type record cache + the journal's staged batch; the [0057](0057-page-arena-and-checkpoint-staging.md) arena |
| **Local disk** (`data.bin`) | the working set — everything a request may touch | 10–100 µs | the page directory, one `BlockDescriptor` per bucket |
| **Object store** (S3-compatible) | **archived versions**, and later idle live pages | 20–200 ms | *this RFC* |

The mechanism is a **remote descriptor**: one reserved bit in the existing
`BlockDescriptor` says "this bucket's page is not in `data.bin`", and the rest of
the word names the object instead of the block run. Nothing above the directory
changes — a descriptor is already the only thing that says where a page is, it
already rides the settle window as a delta ([0046](0046-directory-deltas-in-the-window.md)),
and it is already copy-on-write, so a page that moves tiers is a normal
descriptor change with a normal atomic swap.

The enabling change is not the remote tier at all. It is **segregating archives
into their own lane**, so that "cold" becomes a structural property of a page
rather than a statistic about it. That change is worth landing on its own, before
any object store exists.

## Motivation

### The growth is history, and history is the only cold set with a known shape

Saves never destroy bytes ([RFC 0009](0009-anchors-succession-and-history.md)):
every superseded version stays, at a derived slot, chained through `Metadata`.
`remove` writes the removal log and de-indexes, and still destroys nothing. So
`data.bin` grows with the **total number of writes ever made**, while the working
set grows with the number of *live records* — two different curves, and the
database pays for the first on every metric that matters (disk, defrag work,
directory size, split budget, backup time).

Archives are unusually well-behaved as a cold set, in three ways that no
heuristic could give us:

1. **They are identifiable from the id alone, with no read.** An archive slot's
   `FLAG` is the shape's anchor bit inverted (`record.rs`, `SavePlan::shape`), so
   a bit test on the `Id` classifies it. The shape is a compile-time property of
   the type, so the per-type storage slot can carry it as one `const bool`.
2. **They are immutable.** An archive is written once and never rewritten — a
   forward link is stamped *at archive time*, and addresses are computed rather
   than stored, so no later save ever repoints an existing archive. A tier whose
   contents never change needs no write-back path and no coherence protocol.
3. **They have exactly one reader, and it is a stream.** Nothing resolves a
   record through an archive: `get`, `all()`, every index, every chain and every
   `Expect` guard address the anchor. Archives are reached only by
   `record_history` / `unique_history`, which are `Stream`s that a caller opens
   deliberately to review state. A 100 ms first byte is acceptable there and
   nowhere else in the engine.

That third point is the whole argument for doing history first: it is the only
part of the database where object-store latency does not change what an
operation promises.

### Why the obvious answer does not work

The obvious answer is "measure page temperature and demote the cold ones". It
fails on today's layout, and the reason is worth writing down because it is what
dictates the design.

A type's page directory hashes the record id (`directory::hash_of(id: u128, …)`)
and takes a bucket. The id of an archive is its authoring instant with a
truncated `STRUCT_HASH` as salt — as far as the hash is concerned, uniformly
random. So **archives and live anchors are evenly mixed across every bucket of a
type.** After N saves per record, the average page is `1/N` live and `(N-1)/N`
archive, and *no page is cold*: every one of them holds an anchor somebody may
read this second.

Temperature measurement cannot fix that, because the temperature is real. The
page genuinely is hot; it is merely 95% ballast. The fix has to be spatial, not
statistical.

## Design

### 1. The archive lane (the enabling change; no object store required)

Give every `#[wavedb]` type a second storage slot for its archive namespace,
derived exactly like the four that already exist:

```rust
// crates/wavedb-macros/src/storage_statics.rs
let archive = crate::struct_hash::lane_hash(b"WDB.ARC", hash);
```

This is not a new mechanism. `lane_statics` already emits `WDB.SEG`, `WDB.REC`,
`WDB.DEAD` and `WDB.IDX` slots, each with its own directory, dictionary and cache,
for the reason stated there: *"Each lane is its own storage directory, which is
what keeps a page homogeneous."* Archives are the largest homogeneous population
in the database and they are the one that never got a lane.

Routing is one bit test in `PageStore::get_of` / the batch's write path, both of
which already receive `(struct_hash, id)`: if `id.flag()` is the type's archive
polarity, resolve through the archive slot; otherwise the live one. Unlike the
existing four lanes this one is emitted for **both** shapes — a `Unique` type has
no Pivot and no chains, but it does have history.

What it buys, before any tier exists:

- **Live pages stop being diluted.** A bucket of live anchors holds only live
  anchors, so the same 4 KiB reads N× more of what a request wanted, and the
  live directory's bucket count tracks live records rather than total writes.
- **Two dictionaries instead of one.** A zstd dictionary trained on a mixed
  population models neither well; anchors and archives are different content
  (different `Succession` variant, different `previous`, and in practice
  different value distributions).
- **The split budget stops being spent on history.**
  ([RFC 0049](0049-elastic-pages-and-load-driven-splits.md)'s trigger asks about
  the bucket whose turn it is; a directory that is 95% archive spends 95% of its
  attention there.)
- **And a whole page becomes tierable**, which is the rest of this RFC.

Cost: one more directory per type in the addressing log. That is the log
[0048](0048-chained-addressing-log.md) made a fixed 16 bytes in the frame plus
deltas, so the marginal cost is a second delta stream, not a second snapshot.

This phase changes on-disk layout and does **not** change any `STRUCT_HASH`: the
lane hash is *derived from* the type's hash, the record encoding is untouched, and
the developer sees no schema change. Old `data.bin` files are simply unsupported,
per the pre-release policy ([RFC 0002](0002-architectural-hard-rules.md)).

### 2. The remote descriptor

`BlockDescriptor` is `start (u40) · count (u20) · occupation (u4)` — 64 bits,
fully packed (`block.rs`). There is no spare bit, so one is taken from `start`:

```
local :  [0][ start : u39 ][ count : u20 ][ occ : u4 ]
remote:  [1][  gen  : u39 ][ count : u20 ][ occ : u4 ]
```

- The local address space halves from 4 PiB to **2 PiB** of `data.bin`. This is
  not a constraint anyone will meet; the tier exists precisely so the local file
  stays small.
- `gen` is a per-bucket **generation counter**, not an address. A remote page's
  object key is derived, not stored:
  `<prefix>/<lane_hash:016x>/<bucket>/<gen:016x>`.
- `count` keeps meaning "blocks", now of the *stored object*, so a reader sizes
  its buffer (and a range GET) without a HEAD request. `occupation` keeps its
  meaning unchanged.

Derived keys are what keep this free: no side table mapping buckets to object
names, nothing extra in the `Commit` frame, and no new durable state anywhere.
The generation counter exists only so that a re-upload of the same bucket writes
a *new* object rather than overwriting a live one — which is what makes the
ordering rule below safe under S3's own semantics.

### 3. Demotion, and the ordering rule

Demotion is a background pass with the same shape as defrag
([RFC 0042](0042-free-space-defragmentation-DEPRECATED.md)): it reads pages the request path
is not using, produces descriptor changes, and hands them to the next settle
window ([RFC 0046](0046-directory-deltas-in-the-window.md)) — so it costs **zero
extra IOps of its own** on the local side and takes no barrier.

The order is fixed and one-directional:

1. Read the archive page (or find it in the arena).
2. `PUT` it to the object store at generation `gen + 1`. Wait for the ack.
3. Only then, stage the descriptor swap into the next window.
4. Only after that window commits, return the local blocks to the allocator.

Both crash points are **leaks, never data loss**, which is the same asymmetry
[RFC 0057](0057-page-arena-and-checkpoint-staging.md) relies on:

- crash between 2 and 3 → an orphan object, still named by a generation no
  descriptor points at;
- crash between 3 and 4 → local blocks nobody frees, which the allocator rebuilds
  as free anyway on the next open (it is derived from the live descriptors).

A **sweep** reconciles the first: list the prefix, drop every object whose
generation is not the one the live descriptor names. It is a maintenance task, not
a recovery step — nothing waits on it.

The reverse order (swap first, upload after) is not an option to be traded off; it
is a window in which the only copy of the data is in neither place.

### 4. Reads, and the one seam that has to move

Everything above `Directory::read_page` is already `async` end to end, but the
seam itself is not: `Directory::read_page(&self, …, file: &BlockFile)` is a
synchronous `fn` calling a positioned `pread` (`block_file.rs::read_run`), and
so are the other three call sites (`commit.rs`, `edit.rs`, `defrag.rs`). A remote
page cannot be fetched from inside a synchronous call on the request thread, so
this seam has to become awaitable.

This is the one part of the design with a real dependency on unfinished work: it
is the same synchronous-`fn`-on-the-request-path problem
[RFC 0058](0058-per-type-actors-DEPRECATED.md) names, and 0058 is parked. This
RFC does **not** depend on 0058's answer; it needs only that the read seam can
yield. The narrow move — make `read_page` `async` and dispatch the remote fetch
there, leaving `read_run` synchronous for the local case — is enough and does not
touch the concurrency model.

Read policy, once it can:

- **A live-lane page is never remote in phase 2.** A `get` that resolves an anchor
  keeps its current cost profile exactly.
- **An archive-lane read may await the object store**, and only history streams
  reach it (§Motivation.3).
- **A remote read deposits into the arena and nothing else** — it does *not*
  re-materialise locally. An archive read is a one-shot audit; writing it back
  would undo the demotion the pass just paid for, and repeated history walks are
  served by the arena for as long as they are actually repeated.

### 5. What a node is configured with

```rust
Server::new(REGISTRY)
    .data_dir(d)
    .object_tier(ObjectTier::s3(endpoint, bucket, prefix, creds))  // optional
    .serve(addr)
```

Absent, the engine behaves **exactly as it does today**: one tier, no remote
descriptor ever minted, no background pass, no dependency compiled in. The tier is
a deployment choice, not a schema or format choice — the same `data.bin` and the
same types work either way, because a descriptor that is never demoted is just a
descriptor.

wasm is out of scope. The browser target's store is IndexedDB
([RFC 0025](0025-wasm-indexeddb-target.md)), which has no page directory and no
`BlockDescriptor` to carry the bit; and a client cache holding a bounded mirror is
not the thing with a growth problem.

### 6. Phase 3 — idle *live* data (explicitly lower priority)

The same descriptor bit can demote a live-lane page. It is deliberately second,
and not because it is harder to build — it is nearly free once §2–§4 exist. It is
second because it is the only part that **changes what a read promises**: an
ordinary `get` would sometimes cost 100 ms instead of 50 µs, and no gauge is
reliable enough to make that trade blindly.

What it would need, sketched rather than decided:

- a per-bucket access recency, decayed, held in RAM only (the descriptor's
  `occupation` nibble is spoken for, and a durable counter would put a write on
  the read path);
- a long idle threshold measured in hours, not seconds, and a hard floor on how
  much of a type may be remote at once;
- promotion on the *second* hit within a window, not the first, so one stray scan
  does not drag a whole cold type back to local disk;
- and the [RFC 0053](0053-tenant-fair-cache-retention-PLANNED.md) constraint,
  inverted: that RFC forbids one tenant monopolising memory, and this one must
  equally forbid one tenant's idleness being charged as another's latency.

Everything here is a guess until there is a workload to measure. It is written
down so the mechanism is designed to allow it, not so it gets built next.

## What this deliberately does not do

- **It is not durability or replication.** The object store is a *tier*, not a
  copy: a demoted page lives there and nowhere else. Losing the bucket loses
  history exactly as losing the disk would. Backup remains an open gap (it is
  absent today too).
- **It is not compaction, pruning, or retention.** Tiering moves bytes; it does
  not delete them. "History grows forever" stays open — this RFC makes it *cheap*,
  which is a different problem from making it *bounded*, and conflating the two
  would be the mistake [RFC 0033](0033-cold-history-slow-node-tier-DEPRECATED.md)
  is deprecated for.
- **It is not per-tenant erasure.** A bucket hashes ids from every tenant, so an
  object holds several tenants' archives and no object deletion can be
  tenant-scoped. Erasure needs its own design against the retention work above.
- **It is not a cluster.** No coordination, no second process, no leader — one
  node owns its prefix ([RFC 0037](0037-multi-node-cluster-PLANNED-LOW.md) is
  where sharing one would be designed).

## Relationship to RFC 0033

[RFC 0033](0033-cold-history-slow-node-tier-DEPRECATED.md) proposed a cold tier
for aged history and was deprecated as premature, with a specific charge: *"a
second tier adds a migration path, a routing decision (hot vs cold), and
cross-tier consistency — real complexity, to solve a growth problem no deployment
has hit."* Its **problem statement stands and is still unaddressed**; its answer
is still rejected. The difference is what the intervening engine work bought:

| 0033's charge | Why it costs less now |
|---|---|
| a migration path | descriptors are copy-on-write and already travel as deltas in the settle window ([0043](0043-descriptors-in-the-commit-frame.md), [0046](0046-directory-deltas-in-the-window.md)) — a tier change *is* a descriptor change, and defrag ([0042](0042-free-space-defragmentation-DEPRECATED.md)) is already a background pass that relocates pages |
| a hot/cold routing decision | there is no decision: one bit of the `Id` classifies a slot with no read, and after §1 the routing is which lane, which is compile-time |
| cross-tier consistency | archives are immutable and single-reader, so there is no coherence problem to have |

And where 0033 proposed a *slow node* — a second WaveDB process, a crate, cluster
monitors — this proposes a bucket. A bucket needs no process, no protocol, no
liveness, and no code of ours that can be wrong.

## Alternatives

- **Per-record tiering with an indirection table.** Move individual archives and
  keep a map from id to location. Rejected twice over: the table is sized by the
  number of records, which is the quantity that grows, so the index of the cold
  data becomes the new growth problem; and it re-introduces a stored pointer *to*
  a record, which [RFC 0050](0050-clustered-record-chains.md) removed on purpose
  (position is derived from the record's own key, which is why splits are free of
  consequences).
- **Tier by region of `data.bin` — a "cold file" for old blocks.** Rejected: block
  order is allocation order, not age order, and defrag actively relocates live
  pages, so no contiguous region is uniformly cold. This is the §Motivation
  scatter argument in a different coordinate system.
- **Measured page temperature as the primary mechanism, without the archive
  lane.** Rejected as primary, kept as phase 3: while a bucket mixes anchors and
  archives, no page is cold and the statistic has nothing true to report.
- **A second WaveDB node as the cold tier** (0033's original). Rejected — see the
  table above.
- **A side table mapping bucket → object key**, instead of stealing a descriptor
  bit. Rejected: it is durable state that must be committed atomically with the
  descriptor swap, i.e. exactly the thing the descriptor already is. Deriving the
  key from `(lane_hash, bucket, gen)` costs nothing and cannot drift.
- **Overwrite the object in place instead of a generation counter.** Rejected: it
  makes step 2 of the ordering rule destroy the previous copy before step 3 has
  committed, which is the failure mode the whole ordering exists to avoid.

## Open questions

1. **The `async` read seam.** Making `read_page` awaitable is stated above as the
   narrow move; whether it stays narrow depends on `edit.rs` and `commit.rs`,
   which read pages from inside the write pipeline. Neither should ever touch a
   remote page — a demoted page is immutable archive — so the likely answer is
   that those two keep the synchronous path and *refuse* a remote descriptor as a
   typed error. To be confirmed against the code before building.
2. **Which client.** An S3 SDK is a large dependency for a database that
   hand-rolls its HTTP ([RFC 0020](0020-net-transport-dumb-tunnel.md)). The tier
   needs GET, PUT, LIST and DELETE with SigV4 — a candidate for the same
   treatment, behind an optional feature, but the crypto (HMAC-SHA256) is the part
   worth not hand-rolling.
3. **How aged is aged.** Demoting an archive the instant it is created is the
   simplest rule and possibly the right one, since nothing reads archives outside
   an explicit history walk. The alternative — a grace window — exists only to
   serve a "show me what just changed" UI, which is what the recency chain already
   answers without touching an archive at all. Leaning toward: demote eagerly.
4. **Batching.** One 4 KiB PUT per bucket is a poor object; a demotion round
   should probably concatenate many pages into one object and address them by
   range GET. That trades the derived-key simplicity of §2 for far better
   economics, and it is the one place where a side table might genuinely earn its
   keep. Not decided.

## Phasing

| Phase | Content | Depends on | Priority |
|---|---|---|---|
| **1** | The archive lane (§1) — local only, no object store | nothing | **first, and worth landing alone** |
| **2** | Remote descriptor, demotion pass, sweep, read path (§2–§5) — archives only | phase 1, the `async` read seam | primary goal |
| **3** | Idle live pages (§6) | phase 2, a measured workload | explicitly low |

Phase 1 is the load-bearing one and it stands on its own merits: it makes live
pages homogeneous, gives archives their own dictionary, and stops history
consuming the live directory's split budget — all without an object store
existing. Phase 2 is then a descriptor bit and a background pass.
