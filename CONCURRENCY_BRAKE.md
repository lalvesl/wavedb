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
network, or the worker cache the node had in front of a disk actor until RFC
0067 step 4, whose every miss was a message — and it breaks.

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
Since RFC 0067 step 4 the node ships no suspending store at all — a worker
calls its own `PageStore` — so this pair is what keeps the brake honest until
one returns (per-thread `io_uring`, or step 5's cross-thread leases).

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
locks are different locks. Both use one key, and an owner reached from another thread (RFC 0067) needs a lease, not this
table.

### The key is the owner

Ownership is per Pivot ([RFC 0064](rfcs/0064-pivot-owned-concurrency-PLANNED.md)),
and every collection command names its Pivot on the wire — `Get`, `Update` and
`Remove` included, since RFC 0067 step 2 — so the key is the operation's
`Owner`: a collection's Pivot, or a Unique type under a tenant
(`Owner::of_command`), together with the tenant reaching it. The router places
by the same value. Two collections of one type no longer serialise; two
commands on one collection always do.

A **mixed** granularity would be worse than a coarse one — an `Insert` holding a
Pivot key and an `Update` holding a type key do not exclude each other, which is
the original bug with extra steps. That is why the key stayed
`(tenant, STRUCT_HASH)` until all three commands carried the Pivot, and why
`Update`/`Remove` now refuse a record of another collection
(`expose::check_member`): reaching it through a foreign Pivot would change it
outside its own owner's lock.

A `#[server]` function has no owner. Its body runs at the edge — the
request's own thread — and only coordinates: each operation it performs is a
`NodeOp` that hops to its owner's shard and runs under that owner's lock (RFC
0067 step 2). Remove that lock and `server_fn_e2e`'s eight concurrent bodies
keep 5 of their 40 inserts.

## State

- The brake exists and is tested, including negative controls: remove the lock
  and `one_owner_runs_one_operation_at_a_time` fails with `a1 b1 b2 a2`, the
  exact interleaving in which one op reads the node another is about to
  overwrite.
- **Wired.** The node's workers own their data shards (RFC 0067 step 4): each
  calls the `PageStore` of its owner's shard on its own thread. A POST passes gate 1 at the edge (`edge::handle`) and
  its command is routed to its owner's shard (`shard::Router`), where
  `dispatch::call` takes the lock around the whole operation. A WebSocket
  `Call` takes the same path (`Router::call`, carrying the caller bound at
  `Hello`) — that path once executed unbraked on the accept thread, which was
  a hole in its own right — and so does every operation of a `#[server]`
  body (`Route for Router`, `Job::Node`).
- **One holder per record.** A record is reached only through the worker that
  owns its shard, and that worker's `PageStore` is the only cache in front of
  it. Node-side seeding (`Bound::route()`) borrows the same stores before the
  workers take them, so it is never a second holder; a connection has no
  store at all.
- **The key is the owner.** `(tenant, Owner)` since RFC 0067 step 2b — the
  Pivot rides the front of every collection command — so two collections of
  one type no longer serialise.

## The rule

**Any `Store` whose futures can suspend needs the brake.** That is
`IndexedDB`, anything network-backed, and the per-thread `io_uring` engine RFC
0067 plans. `PageStore` alone on a
current-thread runtime does not — which is precisely why the requirement is
invisible until someone changes the backend.
