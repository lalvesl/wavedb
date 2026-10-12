# The concurrency brake

> **Read this before putting any suspending `Store` under the node.**
> Getting it wrong loses records with no error, no panic and no log line.

## The hazard

A collection op is a sequence: read the Pivot, descend the B+tree, read the
chain's tail segment, build one batch, apply it. Every one of those steps is an
`.await`.

Today that sequence is atomic **by accident**. `PageStore`'s futures all resolve
on the first poll — `get` and `apply` have synchronous bodies — so on a
current-thread runtime a collection op never actually yields between its reads
and its write, and cannot interleave with another.

Nothing declares that. It is a property of the *executor and the backend*, not
of any structure in the engine.

Swap in a backend that genuinely suspends — `IndexedDB`, anything over a
network, or `shard::ShardStore`, whose every cache miss is a message to the disk
actor — and it breaks.

## What breaks, exactly

It is **not** an ordering problem. Two inserts landing in either order is fine:
`all()` is recency-ordered and any domain order is a declared
`#[wavedb::list]`.

The problem is that one of them **disappears**:

1. Task A reads the tree's leaf: `[k1, k2]`
2. Task A computes the successor `[k1, k2, kA]`, builds its batch, awaits
3. Task B reads **the same** leaf — still `[k1, k2]`
4. Task B computes `[k1, k2, kB]`, builds its batch, awaits
5. Both apply. The last writer wins on that node.

The leaf ends as `[k1, k2, kB]`. Record A was written at its anchor, so the
bytes are on disk and durable — and **nothing points at them**. `all()` will not
list it, `Listed` will not, a search will not find it. The caller was told
`Ok(id)`.

Three things make it worse than it first looks:

- **Declaring more structure makes it more likely, not less.** A
  `#[wavedb::list]` is another shared read-modify-write surface with the same
  race.
- **The recency chain is not optional.** Two inserts append to the same tail
  segment; the loser vanishes from recency, and therefore from `all()`.
- **`Write::Expect` does not cover it.** The guard protects the *record's
  anchor*, not the index nodes. B+tree node writes carry no guard at all.

## This was already known

`crates/wavedb-core/tests/concurrent_node_clobber.rs` exists for exactly this
and says so in its header:

> `PageStore`'s futures all resolve on first poll (its `get`/`apply` bodies are
> synchronous), so on a current-thread runtime a collection op never yields
> between its reads and its `apply` and cannot interleave with another. A
> `Store` that genuinely suspends — IndexedDB, or anything network-backed —
> breaks that, and the loss is silent: the record is live at its anchor and
> absent from the index.

It drives two synthetic backends, identical but for whether `get` suspends.
It does **not** cover `ShardStore`, so it stays green while the real hazard is
introduced elsewhere.

This is the third invariant single-threadedness was supplying tacitly.
[RFC 0063](rfcs/0063-engine-yield-map-and-interruptible-engine-PLANNED.md) found
two more (I1: a batch is atomic across the per-type caches; I2: an empty pending
queue means everything is settled).

## The brake

`crates/wavedb-quick-node/src/shard/lock.rs` — `OwnerLocks`.

An async lock per owner, held for the **whole operation** rather than for any
single read or write. It has to survive the operation's awaits, which a
`RefCell` borrow cannot do and a blocking mutex would deadlock on.

Uncontended it is a map insert. Contended, the task joins the owner's **chain**
of waiters with its `Waker` and leaves the run queue, and the shard runs
something else, which is the point: **the brake is on one owner, not on the
shard.** A release hands the owner to the head of the chain and wakes only that
task, so waiters run in arrival order and none is polled for nothing.
(`embassy-sync`'s `Mutex` keeps one waker, so its waiters wake each other in a
loop; it is not used.) The chain's links are slot indices into a fixed array,
not pointers between futures, so it needs no `unsafe`; the array is sized at
compile time by the job pool, since every waiter is a job.

### One table per thread

Only shards execute — a WebSocket `Call` is routed like a POST — and **the
router keys on exactly this key**, so every operation on one owner lands on one
shard, and a shard runs on one thread. Two operations on one owner therefore
always meet on one thread, and a table per thread excludes everything a table
for the node would, with no atomics and nothing shared. Today a thread carries
one shard, so the table lives in its worker; under RFC 0067 a thread's shards
share one. The table is runtime state only and starts empty with the thread.

The condition is the routing: route one way and brake the other and the two
locks are different locks. Both narrow to the Pivot together or neither does,
and an owner reached from another thread (RFC 0067) needs a lease, not this
table.

### Why the key is the type and not the Pivot

Ownership is per Pivot ([RFC 0064](rfcs/0064-pivot-owned-concurrency-PLANNED.md)),
so a Pivot-grained key is what this wants to be. It cannot be yet: `Get`,
`Update` and `Remove` carry only an `Id` on the wire, so their Pivot is not
known where the lock must be taken.

A **mixed** granularity would be worse than a coarse one — an `Insert` holding a
Pivot key and an `Update` holding a type key do not exclude each other, which is
the original bug with extra steps.

So the key is `(tenant, STRUCT_HASH)`, concatenated as `(tenant << 15) |
type_salt`: uniform, injective, safe. Two collections of one type serialise
where they need not; different types and different tenants do not. It narrows to
the Pivot the moment those three commands carry one — separable work RFC 0064
already lists as worth doing on its own.

## State

- The brake exists and is tested, including negative controls: remove the lock
  and `one_owner_runs_one_operation_at_a_time` fails with `a1 b1 b2 a2`, the
  exact interleaving in which one op reads the node another is about to
  overwrite.
- **Wired.** The node runs entirely behind the disk actor: `Server` no longer
  holds a `PageStore`, a POST is routed to a shard worker
  (`shard::Router`), and `dispatch::call` takes the lock around the whole
  operation. A WebSocket `Call` is routed the same way (`Router::call`,
  carrying the caller bound at `Hello`) and takes the same lock — that path
  once executed unbraked on the accept thread, which was a hole in its own
  right.
- **Only shards cache.** `ShardStore` remembers *absence*, which is sound only
  while one holder reaches a record. Everything that is not a shard — node-side
  seeding before serving — gets the cacheless `Shards::store()`; the accept
  thread has no store at all. A second caching store over one record is how a stale
  `None` outlives another holder's insert.

### Still coarser than it should be

The key is the type, so two collections of one type serialise where they need
not. Narrowing it needs `Get`/`Update`/`Remove` to carry their Pivot on the
wire, and the router has to narrow in the same commit.

## The rule

**Any `Store` whose futures can suspend needs the brake.** That is
`ShardStore`, `IndexedDB`, and anything network-backed. `PageStore` alone on a
current-thread runtime does not — which is precisely why the requirement is
invisible until someone changes the backend.
