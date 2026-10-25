# RFC 0067 — Thread per core: the shard as a database, the thread as placement

- **Status:** In progress — opened 2026-10-08. **Steps 1–5 landed**
  (per-type state out of the statics; the routing function; the Pivot on
  the wire; server bodies at the edge; the shard layout; the stepped
  checkpoint; workers owning their shards, the disk actor gone; ingress
  per thread, the accept thread gone; leases; the fsync helper; parallel
  recovery; one executor per thread, decided); see "Progress".
- **Builds on:** [0064](0064-pivot-owned-concurrency-PLANNED.md) (the owner
  as the unit of concurrency, and `shard_of`),
  [0066](0066-embassy-executor-and-async-io.md) (the executor, the reactor
  and the bounded queues this runs on),
  [0063](0063-engine-yield-map-and-interruptible-engine-PLANNED.md) (the
  interruptible settle the background work needs),
  [0041](0041-single-barrier-checkpoint.md) (the checkpoint that stays atomic
  per shard), [0018](0018-storage-engine.md) (the per-type dictionaries)
- **Amends:** 0064's single disk actor (one engine per **shard** instead);
  0018's one-`PageStore`-per-process rule (one per shard). 0066 step 4's
  one executor per thread is kept (§4). The thread-local owner locks this
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
- **Inside a thread, one executor.** Data operations, the network edge and
  business logic, and background maintenance share it. The edge cannot
  outrun the data — it reaches it through a bounded queue — and maintenance
  is paced by the 0064 valve; nothing preempts.
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
compile-time `StructStorage` statics were why there is one: they allowed one
open store per process (`EngineClaim`, removed by step 1).

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
it schedules tasks deterministically on one thread, from static pools.

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

### 4. Inside a thread: one executor, priority by structure

A thread runs **one** Embassy executor (0066's carrier, unchanged). Its tasks
fall into three roles:

| role | runs | must hold |
|---|---|---|
| **DATA** | operations on this thread's owners (`Collection`, `BpTree`, record cache), journal appends, publishing mutations | short and bounded; synchronous engine calls |
| **EDGE** | listener and connections, HTTP/WS parsing and encoding, coordination (§6), `#[server]` bodies and application tasks | may await freely; the CPU it spends is the thread's |
| **BG** | settle, page compression, checkpoint, defragment, eviction | yields every bounded step (0063 part 3) |

The roles are not separate executors, and the executor knows nothing of
them. DATA's precedence comes from the shape of the pipeline, not from the
scheduler:

- **EDGE cannot outrun DATA.** EDGE reaches DATA only through the worker's
  job queue, which is bounded (`JOB_DEPTH`). A connection can get at most a
  queue's worth of jobs ahead of the worker; then its `send` waits, it stops
  reading its socket, and TCP holds the client back. EDGE admits work at the
  rate DATA drains it.
- **EDGE is the pipeline's two ends, not a competitor.** It decodes the
  request and writes the answer. Holding it back to favour DATA would speed
  nothing up: an answer DATA has produced still needs EDGE to leave.
- **BG regulates itself.** The journal-length valve RFC 0064 built
  (`priority.rs`) decides when a maintenance step goes next, so maintenance
  neither delays jobs below the valve's band nor starves into an
  out-of-memory above it.

**Preemption is not a goal.** No option here preempts: Embassy is
cooperative, so a polled task runs to its next `.await` whichever executor
holds it. The engine's own steps are bounded (one settle round, one
checkpoint close). A `#[server]` body that spends a long time on the CPU
spends it on its worker — the developer's choice, and the developer's to
move elsewhere.

What this gives up, unmeasured: under saturation a ready job can wait one
executor round behind other ready tasks. If step 6 shows it in the tail
latency, per-task priority on the same executor is the next step (see
"The executor decision").

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
  let g = db.lock(&[owner]).await?; // Lease job to the owner's thread
  // ... awaits; this thread runs other tasks
  // operations on the owner run on its thread under the lease
  drop(g);                          // Unlock: the lease's queue closes, cannot fail
  ```

- **Several owners:** one `lock` takes them all, in `Owner`'s total order
  whatever order they are named in, so two bodies cannot hold one each and
  wait for the other. (Planned as `lock_many` in `(shard, owner)` order; any
  total order breaks the cycle, and one call for one owner or many is the
  smaller surface.)
- **Hold, then stay inside.** A body holding owners that waited for another
  would be half a deadlock even with ordered locks, so while a lock is held
  a second `lock` and an operation outside the held owners are refused
  (`Error::LockOrder`) — except on a collection the body creates under the
  lock, which nobody else can name yet.
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
| shard workers + disk actor + serve loop on a `LocalSet` | `T` identical threads, one executor each |
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
4. **The thread's executor**: one per thread, priority by structure (§4).
5. **Ingress per thread** (`SO_REUSEPORT`), coordination by hop, leases.
6. **Measure** at `small`, against the pre-0066 baseline:
   - the `wavedb/multi` to `wavedb/single` ratio, which must not fall;
   - throughput as `T` grows from 1 to the core count.

0066 steps 2 and 3 (bounded channels, timers) are unaffected. `OwnerLocks`
became per-thread in 0066 step 5, once every operation ran on its shard;
step 5 here adds only the leases.

## Progress

### Step 1 — per-type state out of the statics (landed 2026-10-08)

- **`StructStorage` is a declaration.** The `#[wavedb]` static keeps only
  what is fixed at compile time: the `STRUCT_HASH` and the compression
  policy. The macros emit it exactly as before; the three runtime accessors
  they used to generate (`storage_mem_cache`, `storage_directory`,
  `storage_dictionary`) are gone, and nothing outside a test called them.
- **The state belongs to the store.** `PageStore::open` builds one
  `TypeSlot` per registered declaration — cache, directory, dictionary and
  removal tombstones, a lock each — and owns them. Routing is unchanged: a
  sorted table built once at open from the compile-time list.
- **`EngineClaim` is gone; the directory is locked instead.** `BlockFile`
  takes an exclusive advisory lock on `data.bin` (`File::try_lock`) for as
  long as it is open. A second open of the same directory, in this process
  or another, is `EngineBusy`; stores over different directories open side
  by side. That is stronger than the old claim, which never stopped a
  second *process*.
- **Tests run in parallel.** The storage tests' `engine_gate()` mutexes are
  gone (the storage suite went from ~9 s to ~5 s), and a new test opens two
  stores side by side and replays each from its own directory. A node is
  still one per process — the token secret is process-global — so node
  tests keep their single-test pattern.

### Step 2 — routing by owner and the shard layout (landed 2026-10-09)

- **`wavedb_core::placement`**: `Owner` and `shard = seahash(owner bytes)
  % shards`, over the frozen layouts of §1 — the Pivot's `LocalId` (key 8
  LE, then `FLAG|SALT` 2 LE) and the tenant (6 LE) followed by the full
  `STRUCT_HASH` (8 LE). Golden vectors pin both hashes and their shard at
  `P = 64`; a test reproduces the `key_nanos()` counter pattern and checks
  every shard is used.
- **`route.rs` re-exports it**, so the worker an operation runs on and the
  store that will hold it come from one function. SplitMix64 and the 15-bit
  salt packing are gone: types sharing their low 15 bits no longer share a
  shard within a tenant.
- **The Pivot is on the wire (2b, landed 2026-10-09).** `Get`, `Update`
  and `Remove` now open their payload with the collection's Pivot, like
  every other collection command, so `Owner::of_command` reads the owner off
  the front of any command without knowing the type (`Changes` decodes its
  fixed `(Option<LocalId>, Option<u64>)` whole — an `Option`'s value sits in
  the heap). The registry says how each hash is addressed
  (`Exposure::addressing`: Unique, collection or function), so the router
  holds the registry and the connection task is one of `node_tasks!` again.
  The owner-lock key is `(tenant, Owner)`: two collections of one type no
  longer serialise. The steps act only on a record of the named collection
  (`expose::member_value` / `check_member`): through another Pivot a record
  is absent and cannot be changed, since it would be changed outside its own
  owner's lock — and once shards are directories it would not be there.
- **A server body coordinates; its operations run at their owners (2c,
  landed 2026-10-09).** A `#[server]` function runs at the edge — the
  request's own thread — and never on a shard, so it can never wait on the
  shard it occupies. Its `ServerDb` is generic over a `Route`; each operation
  is a `NodeOp` (plain data, bodies as wire bytes) that hops to its owner's
  shard as `Job::Node`, runs under the owner's lock through the registry's
  `run_node` match — which serves `store` types too, with no client gate —
  and comes back as a `NodeReply`. `create_pivot` mints the id at the edge
  (`Collection::mint_pivot`) and creates the collection where the id places
  it. Gate 1 runs once, at the edge, for a POST as for a WebSocket session;
  the sync poll and the piggyback navigate each topic at its owner
  (`edge.rs`). A function is called (`call_fn`), never executed: `execute`
  has no function arms, so no shard future embeds a server body — embedding
  them through `LocalRoute` overflowed rustdoc's layout depth on the shard
  task pool. In process, `LocalRoute` runs a body's operations on one store.
  `server_fn_e2e` runs eight concurrent bodies inserting into one collection
  on four shards; without the `Job::Node` lock 35 of their 40 inserts are
  lost.
- **The layout (2d, landed 2026-10-09).** `wavedb_storage::open_shards`
  opens `MANIFEST` + `shards/00..xx/`, each a whole `PageStore`; the
  `MANIFEST` (checked wire, written last and atomically) fixes `P` at
  creation — 64 for a node — and a reopen keeps it whatever the caller asks.
  An unsharded directory is refused rather than shadowed. The page-cache and
  record-cache budgets are the database's, shared equally by the shards.
  The disk actor holds every shard until step 4: a request names its shard
  (`DiskRequest::{Get, GetOf, Apply}.shard`), stats sum the journals and take
  the most fragmented shard's free extent, a settle steps one shard at a time
  round-robin, and checkpoint, eviction (budget split), defragmentation and
  the shutdown commit cover them all. A worker keeps one cache and runs each
  operation on a `ShardView` of its owner's data shard; worker `w` serves the
  contiguous shards `p` with `p·T/P = w`. `Server::shards(n)` is now
  `Server::threads(n)` (clamped to `1..=P`). Node-side seeding uses
  `Bound::route()` — a `Seeder` placing each operation like a served one —
  under a `RouteHandle`.

### Step 3 — the checkpoint is stepped (landed 2026-10-09)

The settle was already a bounded step (`settle_step(budget)`, RFC 0063
part 3); the checkpoint was not — `commit_journal` rotated and then
`drain`ed the whole old journal in one call, and the disk actor ran that for
all 64 shards back to back, so every request queued behind it waited for
all of it.

- **The pending queue has two halves** (`wavedb_storage::pending`): *owed*,
  the ids committed into the journal an open checkpoint rotated out, and
  *fresh*, everything since. The rotation moves fresh to owed under the
  journal lock the commit path also queues under, so the owed set is exactly
  the old journal's. Rounds draw owed first and remember their half; a
  failed round goes back to it, and a round still being written keeps the
  checkpoint open.
- **A checkpoint opens, steps and closes** (`commit_steps`):
  `begin_checkpoint` claims, rotates and retires (bounded);
  `checkpoint_step(budget)` settles one owed round, or — nothing owed —
  syncs `data.bin` and appends the `Commit` frame. Writes between steps go
  to the new journal and are not owed, so a steady write stream cannot keep
  a checkpoint from closing. `commit_journal` is now "close any open one,
  then run one whole" — the shutdown and `run_now` contract.
- **The disk actor only opens** a checkpoint on the hint; the rounds and the
  close are owed work stepped round-robin between requests like a settle,
  so the longest a request waits behind maintenance is one round of
  `SETTLE_BUDGET_IDS` ids or one close's sync.
- Tests (`commit_steps`, `pending`): bounded rounds with the journal
  covered and cleaned on reopen; writes between steps not owed and replayed;
  a store dropped mid-checkpoint replays the old journal; `commit_journal`
  closing an open checkpoint before covering the live journal.

The yield between `plan_slot` calls inside one round, which RFC 0063 left
undecided, is not used: steps break only at round boundaries.

### Step 4 — each worker owns its shards (landed 2026-10-09)

The disk actor and `ShardStore` are deleted. `Router::start` takes the
database's stores and moves each worker's contiguous range onto its thread
(`shard::Owned`); an operation is a plain call on its owner's `PageStore`,
wrapped in `NotifyStore` for the live feed. Nothing crosses a thread but the
`Job` and its answer.

- **BG beside DATA, one executor.** §4 then drew three executors per
  thread; polling several on one thread needs `raw::Executor::poll`, which
  is `unsafe`, so this step kept one executor and got the priority
  cooperatively (later made the design, "The executor decision"): a BG
  task per worker (`shard::background`) takes one bounded step
  (`Owned::step`: a settle round, a checkpoint round or a checkpoint's
  close) only when the 0064 valve (`Priority`) puts maintenance next —
  immediately when no job is executing or queued, and under load once
  the journal is in the valve's band — and yields after every step. EDGE is
  not on worker threads until step 5, so two classes are all a worker has.
- **Policy per worker.** Every `UPKEEP_PERIOD` (200 ms), whatever the load,
  each store gets the node's policy at its per-shard share: open a
  checkpoint past `checkpoint_after_bytes / P` of journal, evict to
  `cache_budget_bytes / P`, defragment below the extent threshold. The
  accept thread keeps only the mutation publisher.
- **Shutdown.** Dropping the router closes each worker's queue; the root
  waits for its jobs and BG to let go of the stores, then `Owned::close`
  commits, forces retirement and **drops every store before reporting** —
  so `run_with_shutdown` returns only when a restart can reopen at once.
- **Seeding** borrows the stores (`Bound::route()` → `Seeder<'_>`) before
  `run` hands them to the workers; the record cache in front of a record is
  its owner's `PageStore`, and no second holder exists.
- **Benchmark.** `wavedb/multi` opens one data shard per consumer and each
  consumer owns its store (RFC 0065 §11); the bench no longer links
  `wavedb-quick-node`.
- Tests: `tests/shard_workers.rs` (ingress and a `RouteHandle` over the
  router; clean close; reopen with nothing to replay; every record in the
  shard placement names and no other) and `shard::owned` unit tests.

### Step 5a — ingress per thread (landed 2026-10-09)

The accept thread is gone. Each worker accepts its own connections, and the
node has no thread that is not a worker.

- **One listener per worker.** `Listener::bind_many` (`wavedb-platform`)
  binds them on one address. On Linux they form one `SO_REUSEPORT` group,
  set up through `rustix`'s safe socket calls (already in the tree under
  `async-io`, same features), and the kernel spreads connections across
  them. Joining the group takes the option **and** the same user, so a plain
  bind of the address still fails as in use. macOS does not balance such a
  group and Windows has no equivalent that is not also a way to take over a
  port, so elsewhere `bind_many` binds one listener and the node accepts on
  one worker; the operations are still placed by owner.
- **A connection executes nothing.** Its accept loop runs on the worker's
  carrier, beside the jobs, and each connection is a task there. Every
  operation becomes a `Job` posted through the `Router` — to this worker's
  own queue when it owns the operation, to another's otherwise. Running a
  local operation inline in the connection was rejected: a task's size is
  its largest state, so the connection pool's ~2 KB slots would grow to the
  job's ~6 KB across all 1024 of them (measured on the release binary),
  for a saving of one channel push on the same thread.
- **Subscriptions live with the topic's owner.** A topic is an owner, so its
  mutations commit on one worker; that worker keeps the topic's subscribers,
  and `NotifyStore` publishes straight into each session's outbox, an
  `async-channel` drained on the session's thread. `Router::watch` posts a
  `Job::Watch` and waits until it is registered, and only then reads the
  tail the ack carries, so no mutation falls between the two; the receive
  loop applies a subscription inline, before any later job. A subscription
  is named by its outbox (`Sender::same_channel`), so no connection id is
  minted across threads. The node-wide mutation feed, its lag signal and
  the publisher task (0066 §2) are deleted: a slow client is still cut off
  by its own full outbox, and nothing else can lag.
- **Shutdown.** Every worker's queue exists before any worker starts, and
  each worker's accept loop holds a router onto all of them. Closing the
  node's `stop` ends every accept loop and then its connections; once no
  router is left anywhere the queues close, and each worker finishes its
  last job and closes its stores. A listener fault reaches the node on a
  channel and stops every worker the same way.
- **Pools.** The accept pool is per worker (`MAX_SHARDS`); `MAX_NODES` and
  the publisher's pool are gone.
- Tests: `listen` unit tests (a group shares one address; a plain bind
  cannot join; the kernel spreads 64 connections over more than one member)
  and `node_ws` now serving on four workers.

### Step 5b — leases (landed 2026-10-09)

A `#[server]` body can hold owners across its awaits, so a read, a decision
and a write become one step.

- **Surface.** `ServerDb::lock(&[Owner]) -> Locked` (`Owner::unique::<T>`,
  `Owner::Collection(pivot)`; `Owner` is in the prelude). While the guard
  lives, the body's operations on those owners run under their leases —
  the handle knows what its body holds, through a `Leases` table the
  generated dispatch builds per invocation — so a body never queues behind
  its own lock. Dropping the guard releases everything.
- **Core.** `Route` gained `type Lease: Clone`, `lease(tenant, owner)` and
  `run_leased`. `RouteHandle::leasing` carries the table, routes each
  operation (`Plain` / `Leased` / `Created` / `Refused`) and enforces the
  rules of §5 with `Error::LockOrder` (on the wire, `NodeErrorKind::LockOrder`).
  `LocalRoute` and the seeding route have no lock table, so their lease is
  `()`.
- **Node.** `Router::lease` posts a `Job::Lease` to the owner's worker. Its
  job task takes the owner's lock — waiting in the owner's chain like any
  operation — and answers with a queue into itself (`shard::Lease`), then
  runs what arrives, one operation at a time, until the queue closes; then
  the lock goes. Releasing is dropping the `Lease`, so it cannot fail, and a
  body dropped mid-await (connection closed, node halting) releases the same
  way. A worker whose job pool is full refuses a lease instead of serving it
  inline, which would hold its queue for the length of a body. A held lease
  counts as busy for the maintenance valve only while an operation runs
  under it.
- Files: `wavedb-core/src/lease.rs` (the table and the rules, unit tests),
  `route.rs` (the trait, moved out of `node_op.rs`), `route_handle.rs`;
  `wavedb-quick-node/src/shard/lease.rs`; `wavedb-macros/src/server_args.rs`
  (the per-call table, shared by both `#[server]` shapes).
- Tests: `wavedb/tests/server_lock_e2e.rs` on four workers — eight bodies
  doing read-modify-write on one counter under its lock end exact and
  without a `Conflict` (the same test without the lock fails with one); two
  ledgers locked in opposite orders by eight bodies complete; a second lock
  and an operation outside the lock are refused and write nothing; a
  collection created under the lock is filled and linked in one step.

### The barrier helper (landed 2026-10-09)

§4's fsync helper. A batch's barrier no longer blocks the worker that
committed it.

- **Storage (`barrier.rs`).** `Barrier::spawn` starts a helper thread;
  `PageStore::use_barrier` hands it the store's batch barriers. `apply` then
  writes the frame and commits it to the caches under the journal lock as
  before, asks the helper for a barrier on that file, and **suspends** until
  it is done. The helper takes every request queued since its last round
  and syncs each file once for all of them — group commit falls out of the
  queue, with no window and no timer. A windowed store (RFC 0061) asks the
  helper once per window and does not wait.
- **Why suspending is safe here.** A batch is visible in the caches before
  it is durable, but only to its own owner's next operation, and the owner
  stays locked until `apply` returns — on the node every operation, read or
  write, runs under its owner's lock (`CONCURRENCY_BRAKE.md`). A store
  without a helper — the client cache, tests, seeding — never suspends, as
  before.
- **Retirement across threads.** RFC 0047 retires a journal once a barrier
  *since* the checkpoint's `Commit` frame has landed. A journal file now
  counts barriers **started** and **completed**; a frame appended at
  `started = n` is durable once `completed > n`, which holds whatever thread
  took the barriers and in whatever order they finished.
- **A failed barrier** marks the file failed: the waiting `apply` reports it
  and every later append is refused (`StorageError::BarrierFailed`) until the
  store is reopened and recovers from what is on disk — the batch was
  already visible, and an `fsync` that failed leaves the file's state
  unknown.
- **Node.** Each worker spawns one helper (`wavedb-fsync`) and gives it to
  every store in its range; it ends when the last of them is dropped.
  Checkpoint barriers (`data.bin`, one per checkpoint) stay on the worker,
  inside the stepped checkpoint.
- Tests: `barrier` unit tests (started/completed counts; sixteen queued
  waits cost one `fsync`; a failed file refuses appends) and
  `tests/barrier_helper.rs` (an acknowledged batch survives a reopen;
  retirement completes with barriers on the helper). Every node test now
  runs with it.

### Parallel recovery (landed 2026-10-09)

§3's startup: the shards' journals replay at once, not one after another.

- `open_shards_with(…, threads)` splits the shards into the contiguous
  ranges the workers will own (`p · T / P = t`) and opens each range on a
  scoped thread; the database is open once the slowest range is. The node
  passes its worker count; `open_shards` uses the machine's parallelism.
- **Opened on one thread, owned by another.** §3 had each worker open its
  shards on its own thread, because the engine state was not `Send` then.
  Since step 4 a `PageStore` moves onto its worker whole, so opening it on a
  recovery thread changes nothing — and it keeps seeding as it is: the
  stores exist before serving, for `Bound::route()` to borrow.
- **All or nothing.** The first fault in shard order fails the open, and
  every shard that did open is dropped before it returns, so a retry finds
  its locks free.
- Tests: `shard_set` (eight shards recovered on three threads come back in
  shard order, each with its own journal replayed and nobody else's; a shard
  that cannot open fails the database and releases the others).

### The executor decision (2026-10-09)

§4 used to draw three Embassy executors per thread — DATA, EDGE, BG —
polled in that order by a loop of the thread's own. With EDGE on the
workers since step 5a, the choice came due, and the thread keeps **one**
executor (§4 as it now reads). Nothing in the code changes: what step 4
built is the design.

What was weighed:

- **Three executors per thread.** The priority is real, per batch of ready
  tasks. It needs `raw::Executor::poll` (`unsafe`), and its pender: the
  `std` platform's `__pender` reads its context as its own private
  signaller, so raw executors of our own mean dropping `executor-thread` and
  exporting a pender of ours (`#[unsafe(export_name)]`) — a thread executor
  maintained by this project over an Embassy internal. Strict
  DATA > EDGE > BG would also starve BG under saturation, so the 0064 valve
  is still needed. **Not taken.**
- **Per-task priority on one executor.** Embassy 0.10's `scheduler-priority`
  feature orders the run queue by a per-task `u8` set on the `SpawnToken`
  before spawning — no `unsafe`, priority per task rather than per batch.
  The cost is an insertion sort of each round's woken tasks (`O(n·m)`),
  unmeasured. **Kept in reserve:** the next step if step 6 shows a ready job
  waiting behind other ready tasks in the tail latency.
- **An EDGE class in the valve**, consulted by connections before admitting
  a request. Priority only where code remembers to ask, and an extra yield
  per check. **Not taken.**
- **One executor, priority by structure.** DATA cannot be outrun — EDGE
  reaches it through the bounded job queue, and backpressure reaches the
  socket — and BG has the valve. Preemption is not a goal: no option above
  preempts, the engine's steps are bounded, and a `#[server]` body's CPU is
  its developer's choice. **Taken.**

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
- **Keep one disk actor.** The slice before step 4. It serialises all page I/O on
  one thread, and its reads wait behind checkpoints (0064's own note).
- **Separate edge or application threads** (Redis 6+ I/O threads). Kept as a
  placement option rather than the default: on a 4-core machine dedicated
  edge threads take cores from the shards, and every operation pays a
  cross-thread hop that step 5a removed. EDGE tasks are spawned apart from
  DATA's, so moving them onto threads of their own stays possible if
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
