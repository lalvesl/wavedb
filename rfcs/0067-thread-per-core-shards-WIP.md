# RFC 0067 — Thread per core: the shard as a database, the thread as placement

- **Status:** Planned — opened 2026-10-08. Nothing built.
- **Builds on:** [0064](0064-pivot-owned-concurrency-PLANNED.md) (the owner
  as the unit of concurrency, and `shard_of`),
  [0066](0066-embassy-executor-and-async-io.md) (the executor, the reactor
  and the bounded queues this runs on),
  [0063](0063-engine-yield-map-and-interruptible-engine-PLANNED.md) (the
  interruptible settle the background work needs),
  [0041](0041-single-barrier-checkpoint.md) (the checkpoint that stays atomic
  per shard), [0018](0018-storage-engine.md) (the per-type dictionaries)
- **Amends:** 0064's single disk actor (one engine per **shard** instead);
  0018's one-`PageStore`-per-process rule (one per shard); 0066 step 4
  (three executors per thread, not one). The thread-local owner locks this
  RFC asked for were delivered by 0066 step 5; this RFC adds the leases.
- **Related:** [0059](0059-object-storage-capacity-tier-PLANNED.md) (object
  storage, which a per-shard prefix makes a per-shard decision)
- **Crates:** `wavedb-storage` (per-type state out of the statics; one engine
  per shard), `wavedb-quick-node` (the threads, ingress, routing, leases),
  `wavedb-platform` (the thread loop)

## Summary

The node becomes **shared-nothing, thread per core**, the shape of Dragonfly
and of Redis's single data thread multiplied: every thread does everything,
for the shards it owns.

- **The shard is a whole database.** A fixed number of shards — **64**,
  chosen at creation — each with its own `data.bin`, journal, allocator,
  per-type directories and dictionaries, under its own path
  `shards/00` … `shards/3f`.
- **The thread is only placement.** `T` threads (one per usable core) each
  own a contiguous range of shards. Restarting with more cores reassigns the
  ranges and touches nothing else, and a future cluster assigns ranges to
  nodes with the same function.
- **Inside a thread, three priorities.** Data operations first, then the
  network edge and business logic, then background maintenance — each its own
  Embassy executor, polled in that order by the thread's own loop.
- **A request on the wrong thread hops.** The kernel hands out connections,
  not requests, so the connection's thread routes each operation to its
  owner's thread and writes the answer back. The connection never moves.

The same code runs as one thread holding every role and every shard, which is
the embedded shape and the test shape: no hop at all.

## Motivation

RFC 0064 drew the ownership boundary with **one** disk actor owning the one
`PageStore`. Its own module docs name the cost (`shard/disk.rs`): the actor
serves one message at a time, so a read waits behind a whole checkpoint, and
every shard's page traffic funnels through one thread. The engine's
compile-time `StructStorage` statics are why there is one: they allow one open
store per process (`EngineClaim`).

Two observations close the gap:

1. **The owner is already the unit of atomicity.** Every mutating operation is
   one `Store::apply` batch belonging to one owner (0064, "Routing is a
   property of the operation"). If storage is partitioned by the same
   function, no batch ever spans two partitions — a shard's journal and pages
   are atomic on their own.
2. **Partitioning by owner is the load balancer.** There are millions of
   owners (one Pivot per holder), so a hash spreads them uniformly. Placing a
   Pivot on a different thread from its holder is not a cost to minimise; it
   is the balancing.

Redis keeps one thread on the data and lets everything else prepare work for
it, which is what makes it predictable. Dragonfly keeps that property per
thread and runs one such thread per core. Embassy fits the per-thread part:
it schedules tasks deterministically on one thread, and it already expresses
priority the way this RFC needs — one executor per priority level.

## Design

### 1. Shards: fixed at creation, routed by remainder

- **`P = 64` by default**, chosen when the database is created and written to
  `<root>/MANIFEST` (shard count, format), which is immutable afterwards.
  Configurable at creation.
- **Routing is SeaHash, then a plain remainder:**
  `shard = seahash(owner bytes) % P`, over
  - the Pivot's `LocalId` bytes for a NonUnique collection;
  - the tenant (6 bytes, LE) followed by the **full** `STRUCT_HASH` (8 bytes,
    LE) for a Unique type.

  SeaHash is the project's identity hash, pinned `=4.1.0` and already called
  at runtime (`mint::natural_key_hash`, segment keys). Nothing about it is
  compile-time: it runs over the bytes at hand. The pin is what this needs,
  because the shard becomes an on-disk address (below): the hash can never
  drift under a dependency bump. The function lives in `wavedb-core` next to
  `natural_key_hash`.
- **This replaces today's `route.rs`**, which only picks a thread and uses
  SplitMix64's finalizer over hand-packed keys: `bits(pivot)`, and
  `unique_key = tenant << 15 | type_salt`. The packing was needed only to fit
  a `u64` for that mixer. It is why a Unique routed by the 15-bit salt rather
  than the whole `STRUCT_HASH`, so two types sharing their low 15 bits shared
  a shard within a tenant. Hashing bytes removes that limit.
- **A hash before the remainder is not optional**, and the reason is
  arithmetic, not taste. The bare key modulo 64 collapses in both cases:
  - **Unique:** `unique_key = tenant << 15 | type_salt`. Since `2^15` is a
    multiple of 64, `unique_key % 64 = type_salt % 64`: the tenant drops out,
    and every tenant's `User` lands on one shard.
  - **Pivot:** a Pivot's key is `key_nanos()` = `ms · 10^6 + counter`, and
    `10^6 = 15 625 · 64`, so `key % 64 = counter % 64`. The counter is one
    process-wide sequence shared by every mint. A loop that mints `k` ids per
    created collection (the shop: one Pivot, then the holder's id and its
    instants) deals Pivots out every `k`-th shard, so only `64 / gcd(k, 64)`
    shards are ever used — 16 of them when `k = 4`.

  A hash over the bytes removes both patterns, and the remainder after it is
  uniform for any `P`.
- **The routing function becomes load-bearing on disk.** Today it only
  decides a thread, and changing it costs a restart. Once it decides which
  `data.bin` holds a record, changing it puts every record in the wrong shard,
  the same way changing `STRUCT_HASH` would. The byte layout of both keys is
  frozen and pinned by golden-vector tests, and the SeaHash pin covers the
  rest.
- **Growing `P` later** is not designed here. If it is ever wanted, keeping
  `P` a power of two makes it a local split: going from 64 to 128 splits
  shard `s` into `s` and `s + 64` by one more bit of the same hash.

### 2. Layout: each shard is self-contained

```text
<root>/
  MANIFEST            P = 64, format — written once
  shards/
    00/  data.bin  journal/
    01/  …
    3f/  …
```

Everything a store needs to recompose itself is **per shard**. Each shard's
`data.bin` keeps today's structure, unchanged:

- the superblock, with its own random directory seed;
- the per-type **linear-hashed directories**, which split themselves as the
  type grows in that shard. The bucket hash is today's `bucket_of(id)`; only
  the population is smaller;
- the slot pages;
- the **addressing log** and the `Commit` frame that names it (RFCs
  0041/0043/0046). They describe where every directory, dictionary and page
  lives, and on open they rebuild the directories, the dictionaries and the
  **allocator** of that shard alone;
- the per-type **dictionaries**.

A shard is therefore exactly one of today's stores, opened at its own path.

The dictionaries stay per shard. They are not trained: each is a capped (64 KiB),
append-only sample of the type's record bytes, versioned by length, rebuilt
deterministically by journal replay (`wavedb-storage/src/dictionary.rs`).
Per shard they cost at most 64 KiB per type and keep every shard rebuildable
from its own journal. A shared `dict/` directory would be the only
dependency between shards; that is rejected.

A type's directory is created in a shard **on its first write there**, so a
type with few records occupies only the shards that hold them.

### 3. Threads: placement, decided at startup

- `T` defaults to `platform::cpu::available()` (the affinity mask and the
  container quota, as now). Thread `t` owns the contiguous range
  `[t·P/T, (t+1)·P/T)`.
- **Startup:** read `MANIFEST`; spawn `T` threads; each opens its shards on
  its own thread (the engine state is non-`Send` and is never built where it
  could be moved from — today's `Shards::store` rule), replays their journals,
  and signals ready. Recovery is parallel across threads.
- **Restart with a different `T`** reassigns ranges. Each journal belongs to
  a shard and travels with it, so nothing is replayed differently and
  nothing migrates.
- A future cluster assigns ranges to nodes with the same function. That needs
  the hop in §6 to cross a network, which is out of scope.

### 4. Inside a thread: three executors, one loop

```text
loop {
    data.poll();                  // owners' operations: never wait behind the rest
    if data has woken work { continue }
    edge.poll();                  // sockets, frames, coordination, business logic
    if data or edge woke { continue }
    bg.poll();                    // one round of maintenance, which yields per slice
    if shutdown { break }
    park();                       // reactor or a cross-thread wake unparks
}
```

| executor | runs | must hold |
|---|---|---|
| **DATA** | operations on this thread's owners (`Collection`, `BpTree`, record cache), journal appends, publishing mutations | short and bounded; synchronous engine calls |
| **EDGE** | listener and connections, HTTP/WS parsing and encoding, coordination (§6), `#[server]` bodies and application tasks | may await freely; heavy CPU goes through an explicit offload |
| **BG** | settle, page compression, checkpoint, defragment, eviction | yields every K pages (0063 part 3) |

This is Embassy's own priority model: on a microcontroller each priority is a
separate executor driven by an interrupt level; here it is a separate
`raw::Executor` driven by the loop. The journal-length valve RFC 0064 built
(`priority.rs`) decides when BG must be polled ahead of EDGE so maintenance
cannot starve into an out-of-memory.

**Helper threads** carry no executor and own no shard. Each does one blocking
call and wakes the requester through a channel:

- **fsync**, one helper per thread: after a window, the thread hands it the
  shards with unsynced journal bytes, and only those.
- **DNS resolution**, already in place (`platform::io` resolves a hostname
  on a short-lived thread).

### 5. Business logic and owner locks

Business logic is light on CPU but often awaits while holding an owner — a
Unique `(tenant, type)` or a Pivot — and lets the thread run other tasks in
the meantime.

- The lock **lives in the owner's thread**, in that thread's table —
  already built by [0066](0066-embassy-executor-and-async-io.md) step 5
  (`shard::OwnerLocks`): a chain of waiters per owner, a release handing the
  owner to the next waiter and waking only it, sizes fixed at compile time.
  (`embassy-sync`'s `Mutex`, first planned here, keeps a single waker and
  makes its waiters wake each other in a loop.) The shards a thread carries
  share its one table. The engine on that thread runs operations to
  completion without yielding, as `PageStore` does today, so the table
  guards held leases, not the engine.
- A task on another thread acquires it as a **lease**:

  ```text
  let g = db.lock(owner).await;   // Lock message to the owner's thread
  // ... awaits; this thread runs other tasks
  // operations carrying g run on the owner's thread under the lease
  drop(g);                        // Unlock: try_send into a reserved slot, cannot fail
  ```

- **Several owners:** `lock_many` acquires in `(shard, owner)` order, so two
  tasks cannot hold one each and wait for the other.
- Within one thread nothing changes; the lease is the only new piece.

### 6. Ingress and coordination

- Each thread binds its **own listener** on the node's address with
  `SO_REUSEPORT`; the kernel spreads connections across them.
- A connection lives on the thread that accepted it. For each frame it
  computes the owner's shard: if the thread owns it, the operation runs
  locally; otherwise a job goes to the owner thread's inbox (bounded
  `async-channel`) with a reply channel, and the connection writes the answer
  when it comes back.
- With `T` threads the operation is local `1/T` of the time, so the hop is
  the normal path. Each hop is a channel push and a wake of the other thread;
  inboxes are drained in bursts. RFC 0065's **~2.3×** `wavedb/multi` speedup
  was measured with hops of this kind already.
- **WebSocket subscriptions:** the owner publishes into the connection's
  outbound queue on the connection's thread, under 0066's full policy (a full
  queue disconnects the client, which catches up by cursor).

### 7. Durability and atomicity

- **One journal per shard.** A batch is one owner and so one shard: the
  shard's journal-then-pages sequence is atomic on its own, and recovery
  never needs another shard.
- **Nothing is atomic across owners, and that is not new.** Creating a nested
  collection is already two operations today (`Product::create_pivot`, then
  the holder's insert, in `benches/src/systems/drivers/shop/wavedb.rs`). The
  order **create, then link** means a holder never points at nothing; a crash
  between the two leaves an empty orphan Pivot, at the cost of one block.
- **Journal opt-in per type** (future, its own RFC): a type without a journal
  is durable at each checkpoint, which 0041 already makes atomic — it can lose
  the writes since the last checkpoint but never tear a batch. It changes a
  promise about future writes, not the bytes of a record, so by the project's
  rule it would not fold into `STRUCT_HASH`. That RFC decides it.

### 8. Disk I/O

- **Now:** synchronous `pread`/`pwrite` on the owning thread (the page cache
  absorbs most of it) and `fsync` on the helper (§4).
- **Later:** `io_uring` per thread behind `wavedb-platform`, as Dragonfly
  does.
- **Object storage (0059):** a shard's path is its prefix, so moving a shard
  to S3 is a per-shard decision. `data.bin` is random block writes, which an
  object store does not offer, so the format inside a shard would change
  there; the layout above does not.

## What changes

| today | after |
|---|---|
| one disk actor owns the one `PageStore` | one engine per shard, owned by the shard's thread |
| one open store per process (`EngineClaim`, `StructStorage` statics) | statics keep compile-time constants; per-type runtime state lives in each shard's engine |
| one journal, one `data.bin` | 64 of each, under `shards/xx/` |
| shard workers + disk actor + serve loop on a `LocalSet` | `T` identical threads, three executors each |
| `OwnerLocks`: one table per thread (0066 step 5), local owners only | the same table, plus leases for owners on other threads |
| `shard_of`: SplitMix64 over packed keys, `% shards`, a thread choice | `seahash(owner bytes) % P`, a frozen on-disk placement |

## Delivery

1. **Per-type state out of the statics.** `StructStorage`'s runtime half
   (cache, directory, dictionary handles) moves into the engine instance so
   several `PageStore`s can be open in one process; `EngineClaim` goes. The
   largest step and a prerequisite for the rest; it also simplifies the
   tests' `engine_gate()`.
2. **The shard layout.** `MANIFEST`, `shards/xx/`, open and replay per shard,
   routing by SeaHash and remainder, with golden vectors.
3. **Interruptible background work** (0063 part 3), so BG can share a thread
   with DATA.
4. **The thread loop** with three executors — 0066 step 4, reshaped.
5. **Ingress per thread** (`SO_REUSEPORT`), coordination by hop, leases.
6. **Measure** at `small`, against the pre-0066 baseline:
   - the `wavedb/multi` to `wavedb/single` ratio, which must not fall;
   - throughput as `T` grows from 1 to the core count.

0066 steps 2 and 3 (bounded channels, timers) are unaffected. `OwnerLocks`
became per-thread in 0066 step 5, once every operation ran on its shard;
step 5 here adds only the leases.

## Alternatives

- **One journal per thread.** Fewer `fsync`s per window, but a shard stops
  being self-contained. A restart with a different `T` must replay and
  checkpoint every journal before ranges can be reassigned, and a shard can
  no longer move to another node or to object storage alone. Rejected.
- **Partition by `hash % T`.** No fixed `P`, but changing the core count
  repartitions the data. Rejected.
- **Dictionaries shared per type in `dict/`.** One sample per type instead of
  64, at the cost of the only cross-shard dependency and of dictionaries that
  no longer follow from each shard's own replay. Rejected; the per-shard cost
  is at most 64 KiB per type.
- **Keep one disk actor.** The current slice. It serialises all page I/O on
  one thread, and its reads wait behind checkpoints (0064's own note).
- **Separate edge or application threads** (Redis 6+ I/O threads). Kept as a
  placement option rather than the default: on a 4-core machine dedicated
  edge threads take cores from the shards. Executors are per role, so moving
  EDGE onto threads of its own is configuration, not a rewrite, if
  measurement asks for it.
- **Co-locating a child Pivot with its holder** by choosing the child's id
  until it hashes to the holder's shard (about 64 tries on average). That
  would make create-and-link one atomic batch. **Rejected:** a holder record's
  shard is its *collection's* shard, so every child of every holder in one
  collection — every `Product` collection of every `Shopping` — would land on
  one shard and the balancing would be gone. A many-to-many child can follow
  at most one of its holders, and a Unique holder's shard is fixed by
  `(tenant, type)`.

## Open questions

1. **`async-io`'s reactor is process-wide**: one epoll set that one thread at
   a time drives. With a listener per thread, every thread's sockets go
   through it. Start with it and measure. A per-thread reactor is the
   shared-nothing answer, but the `polling` crate's registration is `unsafe`
   and the workspace has none, so it likely arrives with `io_uring` behind
   the platform seam.
2. **`fsync` fan-out.** A thread owning 16 shards may sync up to 16 files per
   window. That is cheap on NVMe (parallel queues) and expensive on SATA or a
   spinning disk. Measure; batching through `io_uring` is the likely answer.
3. **Minimum footprint** per (type, shard) pair that holds data: one
   directory and one page at least. Lazy creation bounds it; it should be
   measured, given the footprint the `large` seed already showed.
4. **A hot owner is one thread.** A very hot Pivot or Unique type has no
   parallelism of its own, by design (0064). The answer remains modelling
   (more Pivots), not splitting an owner.
5. **Lease lifetime in a cluster.** In one process a lease ends when its guard
   drops. Across nodes, a vanished holder needs a timeout.
