# RFC 0066 — Embassy executor and async-io: tokio out of the node

- **Status:** Implemented — opened and closed 2026-10-08. All five steps
  landed; `cargo tree -i tokio` is empty for the workspace. The exit proof's
  after-step-5 benchmark rows were waived by the project owner (see
  "Progress", step 5).
- **Builds on:** [0064](0064-pivot-owned-concurrency-PLANNED.md) (the shard
  workers and disk actors this changes the carrier of),
  [0065](0065-benchmark-suite-ii-the-row-as-the-unit.md) (the `wavedb/multi`
  rows that judge it)
- **Crates:** `wavedb-platform` (the seam), `wavedb-net`, `wavedb-quick-node`,
  `wavedb` (client), the examples
- **Code:** `crates/wavedb-platform/src/task.rs`, `…/time.rs`,
  `…/ws/{codec,native}.rs`, `…/http/native.rs`;
  `crates/wavedb-net/src/{http,frames,ws_session}.rs`, `…/manager/`;
  `crates/wavedb-quick-node/src/{serve,serve_ws,subscribe,dispatch,upkeep}.rs`,
  `…/shard/{worker,disk,router,lock,waiters}.rs`

## Summary

Replace tokio with narrower pieces: **`embassy-executor`** (its `raw` API)
to run tasks, **`async-io`** as the reactor for OS sockets,
**`embassy-time`** for timers, and bounded channels — `embassy-sync` within a
thread, lock-free `async-channel` across threads, so the shards stay
genuinely parallel: one OS thread, one executor each. The protocol code
(HTTP tunnel, WebSocket codec) becomes generic over **`embedded-io-async`**,
so the same code can one day sit on `embassy-net`'s `TcpSocket` on a
microcontroller. The browser side does not change.

The near-term gain is **bounded everything**: tasks come from fixed pools,
queues have compile-time capacities, and a full pool or queue is an explicit,
typed refusal rather than unbounded growth. The long-term reason is the
embedded target: the only part of the async stack that has to be rewritten for
it is then the socket type behind the `platform` seam.

## Motivation

tokio is used thinly. `wavedb-core` and `wavedb-storage` do not depend on it
at all (the engine is async at its surface and synchronous underneath). Every
runtime in the node is already `current_thread` + `LocalSet` — the accept
loop, each shard worker, the client's connection manager — so nothing uses
work-stealing, the multi-thread scheduler, or `Send` futures. The client
manager already runs on `futures::channel` and `futures::select!`. What tokio
actually provides here:

| use | where | sites |
|---|---|---|
| spawn / `LocalSet` / runtime per thread | `platform::task`, `serve.rs`, `shard/worker.rs` | ~10 |
| `TcpListener` / `TcpStream` / `AsyncRead` / `AsyncWrite` | `net::http`, `platform::ws`, `platform::http`, `serve*` | ~25 |
| `mpsc` / `oneshot` | quick-node | ~20 |
| `sleep` / `timeout` / `interval` | `platform::time`, upkeep, tests | ~8 |
| `#[tokio::test]` + `io::duplex` | 13 integration files, unit tests | 59 + 12 |

Two things in that list are worth changing regardless of the runtime:

- **Four unbounded channels**, all at the node's edge: the WebSocket reader →
  connection loop (`serve_ws.rs`), the connection's outbound events
  (`serve_ws.rs`), the shards → publisher mutation feed (`lib.rs`), and the
  subscribe tests' fixtures. A slow WebSocket client grows the node's memory
  without limit — the least predictable thing the node does. The shard
  internals are already bounded (`JOB_DEPTH = 512`, `QUEUE_DEPTH = 1024`,
  `MAINTENANCE_DEPTH = 16`).
- **tokio's cooperative budget** forces a yield after ~128 resource
  operations. It is invisible and it is scheduling policy the code did not
  choose. Run-to-await with a FIFO wake queue — what Embassy does, and what
  the browser's microtask queue does — is the model the code already assumes.

And one thing only Embassy offers: a path to running on a microcontroller.
That is far off and is not this RFC's deliverable, but this RFC decides which
abstractions make it cheap later rather than a rewrite.

### Why not "all Embassy"

`embassy-net` is a userspace TCP/IP stack (smoltcp) driving a raw network
device. On Linux it runs only over TUN/TAP — root or `CAP_NET_ADMIN`, its own
IP address, the kernel's TCP bypassed. That is unusable for a node. Native
sockets need a reactor on epoll, and `async-io` is one that wakes any
executor's tasks through the standard `Waker`. `async-net` is only a thin
layer over it and is not needed.

## Design

### 1. Protocol over `embedded-io-async`

`net::http`, `net::frames`, `platform::ws::codec` and `net::ws_session` become
generic over `embedded_io_async::{Read, Write}` instead of tokio's traits.

- **Native:** `async_io::Async<std::net::TcpStream>` implements `futures-io`;
  `embedded-io-adapters` (`futures_03::FromFutures`) turns it into
  `embedded-io-async`. The adapter lives in `platform`, nowhere else.
- **Embedded (future):** `embassy_net::tcp::TcpSocket` implements
  `embedded-io-async` directly.
- **Browser:** unchanged — `fetch` and `WebSocket` through `platform`, which
  never touched these traits.
- **Tests:** `tokio::io::duplex` becomes an in-memory pipe implementing the
  same traits, in a test-support module of `platform`.

### 2. Channels: every queue bounded, none shared by two shards

Queues split by the boundary they cross, because the two kinds have
opposite requirements:

- **Within one thread** — a connection's reader and loop, the tasks of one
  shard: `embassy_sync::{channel::Channel, signal::Signal}` with
  `NoopRawMutex`. No lock at all; these never leave their executor.
- **Across threads** — jobs into a shard, answers and mutations out of one,
  requests into a disk actor: **`async-channel`**, bounded. It is lock-free
  (`concurrent-queue`), MPMC, and wakes through the standard `Waker`, so a
  push on one shard's thread wakes a task on another's executor with no
  shared lock in between.

The obvious alternative for the second kind, `embassy-sync` with
`CriticalSectionRawMutex`, is **rejected**: the `critical-section` crate's
`std` implementation is one process-wide lock, so every shard would
serialise on it at every queue operation — exactly the contention the shards
exist to remove (see §4, "Threads"). A per-queue `RawMutex` over
`std::sync::Mutex` would fix that, but `RawMutex` is an `unsafe trait`, and
no crate in this workspace contains `unsafe` today; this RFC does not make
the first. On a single-core microcontroller there are no cross-thread queues
to begin with, so nothing there depends on this choice.

Capacities are `const`s named next to the queue, like the shard depths
already are.

The four unbounded channels each need a **full policy**, which is design, not
mechanics. Proposed defaults:

| queue | capacity | when full |
|---|---|---|
| WS reader → connection loop | small (e.g. 16) | the reader waits — backpressure onto the client's own socket |
| connection → client (events) | `WS_OUTBOUND_DEPTH` | **disconnect the client**; it reconnects and catches up by cursor |
| shards → publisher (mutations) | `MUTATION_DEPTH` | never block a write on observers: drop, and mark the topic lagged so its subscribers are disconnected to catch up |

Disconnect-to-resync is safe *because* sync is stateless (RFC 0034/0035):
each topic carries an instant cursor and the node answers by navigating the
recency and dead logs, so a dropped connection loses nothing. A write never
waits on a reader.

### 3. Time: `embassy-time`

`platform::time::sleep`, the upkeep interval and the timeouts go through
`embassy_time::{Timer, Ticker, with_timeout}` with its `std` driver. The
clock used for **minting** (`platform::time::key_nanos`) is untouched — it is
wall time, not a timer.

### 4. Executor: `embassy_executor::raw`

Not `Executor::run()`, which is `-> !` on `arch-std`: a worker thread could
never stop and no test could return. Each carrier thread instead owns a
`raw::Executor` and a loop of its own:

```text
loop {
    executor.poll();              // run every task that was woken
    if shutdown.is_set() { break } // clean exit, which run() cannot do
    thread::park();               // the executor's pender unparks us
}
```

A `Waker` fired from another thread (the reactor, a shard, the disk actor)
calls the pender, which unparks the carrier. `platform::task::{spawn_detached,
spawn_local}` keep their signatures; `spawn_detached` now builds that loop on
its new thread. A `platform::task::block_on` built on the same loop replaces
`#[tokio::test]` and `#[tokio::main]`.

**Generic tasks without `dyn` and without generated code.** Embassy's
`#[task]` cannot be generic, and `serve_connection<E, S>` and the shard job
handlers are. A `static` pool needs the future's type named, which is
`type_alias_impl_trait` (unstable, waiting on the new trait solver). But
`raw::TaskPool<F, N>` built **once, inside the generic function**, lets the
compiler infer `F`:

```rust
// once per serve() — leaked deliberately, it lives as long as the node
let pool = Box::leak(Box::new(TaskPool::<_, MAX_CONNECTIONS>::new()));
// per connection; a full pool is a refused connection, not a queue
pool.spawn(move || serve_connection(sock, registry, node))
```

That gives fixed-size tasks, a per-type ceiling, and an explicit refusal when
full — one allocation at boot, none per connection. **This must be proven in
the first spike** (the exact `raw` API shape varies across Embassy
versions). If it does not hold, the fallback is `expose_server!` emitting
concrete `#[task]` functions in the application crate — consistent with the
"generated `match`, no registration" rule, but a macro change.

Pool sizes become declared numbers: `MAX_CONNECTIONS` for the accept loop,
`JOB_DEPTH` (already 512) for a shard's in-flight jobs.

**Threads: the shards stay genuinely parallel.** The rule this RFC must not
break is **one OS thread, one executor**. Today every shard worker
(`shard/worker.rs`) and every disk actor (`shard/disk.rs`) is its own thread
via `platform::task::spawn_detached`, each with its own current-thread
runtime; after this RFC each owns its own `raw::Executor`, its own task
pool, and its own loop, and nothing schedules across them. What crosses a
thread is data on a lock-free queue (§2) and a `Waker` that unparks the
receiving thread — never a lock held by more than one shard, never a task
moving between executors. The shared [`OwnerLocks`] table is the one
deliberate exception, and its *semantics* are unchanged by this RFC — but
not its implementation, which an earlier draft of this paragraph missed: it
was a `std::sync::Mutex` map of per-owner **`tokio::sync::Mutex`**es, taken
with owned guards so a lock can be held across an operation's awaits.
**Resolved in step 5**, without an interim `async-lock`: once every
operation runs on its shard, the table is per thread — see "Progress",
step 5. Leases for an owner on another thread stay
[RFC 0067](0067-thread-per-core-shards-PLANNED.md)'s.

Process-wide pieces that remain, and why each is acceptable:

- **`async-io`'s reactor** is one epoll thread. Only the accept thread does
  socket I/O; the shards receive jobs, not sockets, so it is not on their
  path.
- **`embassy-time`'s std driver** keeps one timer queue. The shards' hot
  path has no timers; if a profile ever shows otherwise, per-thread timers
  are an open question below.

[`OwnerLocks`]: ../crates/wavedb-quick-node/src/shard/lock.rs

### 5. The browser

Stays on `wasm_bindgen_futures::spawn_local` and JS timers. What is unified
across targets is the **semantics** the code above the seam relies on — one
thread, non-`Send` futures, `spawn_local`, `sleep` — and that is already
true. Embassy's `arch-wasm` executor could replace `spawn_local` later; it
would unify an API the seam already unifies, so it is not part of this RFC.

## Delivery

One step per commit, each green on the full bar and each leaving the node
working. tokio and the new stack coexist until step 5.

1. **I/O seam** — protocol over `embedded-io-async`; sockets via `async-io` +
   adapter inside `platform`; the in-memory pipe for tests. tokio still runs
   the executor.
2. **Bounded channels** — `embassy-sync` within a thread, `async-channel`
   across threads; the three full policies above, a test per policy (a slow
   WS client is disconnected; a write never blocks on a lagging publisher).
   `OwnerLocks` is left to [RFC 0067](0067-thread-per-core-shards-PLANNED.md).
3. **Timers** — `embassy-time` behind `platform::time`.
4. **Executor spike, then the switch** — prove `TaskPool` inference on
   `serve_connection<E, S>`; then `platform::task`, the serve loop, the shard
   workers and the disk actor move to `raw::Executor`; `block_on` replaces
   the test macros. RFC 0067 reshapes the carrier loop to poll **three**
   executors per thread (data, edge, background) in priority order; this
   step can build the single-executor loop first, as long as nothing assumes
   one executor per thread.
5. **tokio removed** from every manifest; `cargo tree -i tokio` empty for the
   workspace and the bench crate (which dropped it in step 2).

**Exit proof:** the full bar, the two-process e2e tests (contact-book cache,
offline queue), the browser demo, and the RFC 0065 `wavedb/multi` and
`wavedb/single` rows at `small` measured before step 1 and after step 5 —
latency percentiles, throughput, RSS. A regression there is a finding to
explain before this RFC is marked Implemented. The **sharded speedup** is
the specific number to hold: RFC 0065 measured `wavedb/multi` at **~2.3×**
`wavedb/single` on reads (`profile` 6 951/s against 3 021) — the project's
first measurement of real parallelism. A ratio that falls means something
started serialising the shards.

## Progress

**Step 1 — landed 2026-10-08.** `wavedb_platform::io` is the one place a
socket is named (`connect`, `Listener`, `Stream` with owned halves, a
`Prefixed` reader for bytes a handshake pulled early, a loopback `pair` for
tests). The WebSocket codec, both native clients, the tunnel's server half
(`net::http`) and the node's serve loops are generic over
`embedded_io_async::{Read, Write}` with `Error = IoError`. tokio still runs
the executor; it no longer provides a socket anywhere in the workspace.
Three behaviours were kept deliberately, each with a test or a comment
saying why: dropping a write half shuts the socket's write side (tokio's
`OwnedWriteHalf` did, and a tunnel response ends on it); a hostname
resolves on a short-lived thread rather than blocking the executor in
`getaddrinfo`; `Server::bind` stays `async` with nothing to await, because
bringing a network up is asynchronous on `embassy-net`. Workspace bar green
(491 tests), bench crates and the wasm target build.

**Step 2 — landed 2026-10-08.** No queue in `wavedb-quick-node` is unbounded
any more, and none comes from tokio:

| queue | now | depth | when full |
|---|---|---|---|
| shard jobs, disk requests, disk maintenance | `async-channel` | 512 / 1024 / 16 (unchanged) | sender waits; maintenance hints drop, as before |
| one-shot replies (disk answers, routed jobs, shutdown) | `futures::channel::oneshot` | 1 | — |
| WS reader → session loop | `embassy-sync` `Channel`, `NoopRawMutex` | `WS_INBOUND_DEPTH` = 16 | the reader stops reading |
| publisher → connection (events) | `async-channel` | `WS_OUTBOUND_DEPTH` = 256 | the connection is closed |
| shards → publisher (mutations) | `async-channel` + a lag signal (`feed.rs`) | `MUTATION_DEPTH` = 4096 | dropped; every subscriber is disconnected |

Three choices differ from §2 as written, each for a reason that only showed
in the code:

- **One-shot replies use `futures`' oneshot**, not a capacity-one
  `async-channel`. It is runtime-independent, it is a future itself (so no
  call site changed beyond the import), and it is what the bench crate
  needed in order to drop tokio entirely.
- **The outbound event queue is `async-channel`**, although today both ends
  share a thread. Under [RFC 0067](0067-thread-per-core-shards-PLANNED.md)
  the owner publishes into a connection on another thread, so this queue
  crosses threads in the target. It also needs `close()`, the publisher's
  way to disconnect a client, which `embassy-sync`'s channel does not have.
  The reader-to-loop queue, which never leaves its connection's thread, is
  the `embassy-sync` one.
- **A dropped mutation disconnects every subscriber**, not just the
  dropped event's topic. A shard that finds the feed full raises a
  capacity-one lag signal *before* pushing anything newer; the publisher
  checks it after each pop and before delivering. So no subscriber ever
  receives a later event of a topic without the earlier one. Knowing which
  topics lost an event would need a structure the shards share; a full feed
  is an overload, and a coarse recovery is the right price for it.

Tests, one per policy: `a_full_inbound_queue_stops_the_reader`,
`a_full_outbox_disconnects_the_slow_client`,
`a_full_feed_drops_and_signals_instead_of_waiting`,
`a_lagging_publisher_disconnects_the_subscribers` (plus an unlagged-order
test and `disconnect_all`). `select`/`join` in the node's own code come
from `embassy-futures`. What still uses tokio in quick-node: the `LocalSet`,
`spawn_local` and accept-loop `select!` (step 4), the upkeep interval
(step 3), `OwnerLocks` (RFC 0067), and `#[tokio::test]`. Workspace bar green
(497 tests), bench crates green, `cargo deny` clean.

**Step 3 — landed 2026-10-08.** `platform::time::sleep` runs on
`embassy_time::Timer` natively (the browser keeps `setTimeout`), and a new
`platform::time::timeout` races a future against that `sleep` with
`embassy-futures`' `select`. It is one definition for both targets, and it
returns a typed `Elapsed`. Uses:
- the node's upkeep loop sleeps between rounds instead of a tokio
  `interval`. It is a sleep and not an `embassy_time::Ticker`, because a
  ticker that fell behind fires back to back, which is what tokio's
  `MissedTickBehavior::Delay` had been set to prevent;
- the connection manager's poll and backoff timers already went through
  `platform::time::sleep`;
- the hang guards in the net manager's tests, the contact-book e2e tests
  and the showcase client use `platform::time::timeout`.

No `tokio::time` is left in the workspace.

One configuration detail decides whether this works at all. Without a queue
feature, `embassy-time`'s `std` driver expects the **integrated** timer
queue, which lives inside `embassy-executor`'s tasks, and linking fails
(`__embassy_time_queue_item_from_waker`). The workspace enables
**`generic-queue-128`**, which accepts any executor's `Waker`. That is
required as long as timers are awaited under tokio and under
`futures::executor::block_on`, and it stays useful after step 4 for tests
and helpers outside Embassy tasks. Past 128 pending timers the generic queue
wakes one of them early; an Embassy timer re-checks the clock and re-arms, so
overflow costs a spurious wake, never a lost one. The node holds a handful:
upkeep, one per connection manager, and test guards.

The platform's tokio is down to `rt` (the `spawn_detached` runtime, step 4).
Workspace bar green (500 tests: 3 new timer tests), bench crates green,
wasm32 build and clippy clean.

**Step 4, node half — landed 2026-10-08.** The node no longer runs on tokio.
Step 4 is split in two. This half covers the node; the client's connection
manager (`net::manager`) is still on `platform::task`'s tokio runtime and
moves in the second half.

*The spike* (two crates, standing in for the node and an application) settled
§4's question on Embassy 0.10:
- a `#[task]` takes non-`Send` arguments (an `Rc`) when spawned by its
  thread's own `Spawner`;
- `pool_size` takes any constant expression, including one from another
  crate;
- `embassy_executor = path` points the generated code at a re-export, so an
  application never names `embassy-executor` itself;
- one static pool serves executors on several threads at once;
- the `std` platform's `Executor::run_until` **returns**, so a carrier can end
  without the `unsafe` that `raw::Executor::poll` requires.

The deduced-`TaskPool` route was dropped in favour of generated tasks: it
would have put the pool on the heap, and generated tasks need no allocator at
all.

*What landed:*
- **`platform::carrier`**: an OS thread with one Embassy executor. Tasks take
  a `Here` (the carrier's `Spawner` plus a `Live` token) and pass clones on.
  The thread returns when the last token drops. Embassy cannot abort a task,
  and a task left unpolled would hold its pool slot forever, so stopping is
  cooperative: each task on the accept carrier selects on one `halt`
  channel, and the WebSocket reader stops through a drop guard on its
  session.
- **`NodeTasks` and `node_tasks!`**: the four registry-generic tasks
  (accept, connection, shard, job) are declared by `expose_server!` in the
  schema crate, inside an `allow(clippy::future_not_send)` `const _` block,
  which also lets an expansion inside a function body see its registry. A
  schema crate's `server-side` feature therefore enables an optional
  `wavedb-quick-node` dependency.
- **Concrete tasks** declared by the node itself: the disk actor, the
  background task (upkeep plus publisher) and the WebSocket reader.
- **Pools** (`__rt`): `MAX_CONNECTIONS` = 1024 (one more is refused: its
  socket closes), `MAX_JOBS` = 1024 shared by every shard, `MAX_SHARDS` =
  256, `MAX_NODES` = 4. A shard that finds the job pool full **runs the job
  itself**. The job travels in a `JobSlot` because a refused spawn drops its
  arguments, and the slot is how the shard gets the job back. So nothing is
  lost, and the shard's bounded queue absorbs the backlog.
- **The public API is unchanged.** `Server::new(REGISTRY)`, `bind().await`
  and `run().await` stay as they were: `run` starts the accept carrier and
  only awaits its outcome and its `Finished`, so it works under any executor.
  Every test and example ran unmodified.

*Measured* (a temporary probe, reverted): a connection task is about 6 KB
with `schema-smoke`'s registry. About 5 KB of that is `Exposure::execute`,
whose largest arm is a collection insert. An idle WebSocket connection costs
about 58 KB, and 51 KB of that is the outbound queue: a bounded
`async-channel` preallocates all `WS_OUTBOUND_DEPTH` slots of 192 bytes. Two
reductions were identified and deliberately deferred (user decision):
- a lazily allocated outbound queue with the cap checked on push;
- routing WebSocket `Call`s through the `Router`, which would move the 5 KB
  out of the connection and into the job pool.

*Benchmarked against the pre-0066 commit (`462a8bb`):*
- **Setup.** Shop rows at `smoke`, one run each, both commits on the same
  machine, each started below load 1.0. The cage was raised to 4 GB and
  the results kept outside the corpus, because shop rows do not fit 500 MB
  at either commit (see `benches/PROGRESS.md`).
- **The control is noisy.** `wavedb/single` does not touch the changed
  code: it runs a `PageStore` in process, with no carrier. Yet between the
  two runs its durable checkout went from 62/s to 31/s (an fsync-bound
  phase), and its reads moved by ±9%. Differences of that size are noise in
  this setup.
- **`wavedb/multi`, which runs on the carriers, stayed within that noise.**
  The relaxed variant matched in every phase: profile 1 729 against
  1 789/s, order_page 473 against 476, order_detail 297 against 298. The
  durable variant's reads came out 11–14% lower (order_page 410 against
  475/s), inside the band the control just showed.
- **The multi/single ratio was ~1.2–1.3× at both commits** in this shape —
  not the ~2.3× of the original smoke shape. So that number depends on the
  shape, and it did not move.
- **Verdict:** no regression measurable at one run each. Resolving
  differences under ~15% needs repeated runs, which belongs to step 5's exit
  proof.

Workspace bar green (502 tests: 2 new carrier tests), bench crates green,
wasm32 build clean, docs warning-free against the baseline. The WebSocket
reader and the disk actor's maintenance work moved to `ws_read.rs` and
`shard/disk_work.rs` for the file budget.

**Step 4, client half — landed 2026-10-08.** The connection manager runs on a
carrier of its own, and its subtasks come from static pools
(`net::manager::tasks`):

| task | pool |
|---|--:|
| POST exchange | `MAX_POSTS`: 1024, set at build time with `WAVEDB_CLIENT_MAX_POSTS` (an `option_env!` parsed by a `const fn`; a malformed value fails the build) |
| WebSocket watch actor | 64 |
| HTTP poll watch actor | 64 |
| WebSocket reader | 128 (a reconnect's new reader may overlap the old one) |

**When a pool is full the manager refuses** — user decision, simple first —
with a new `wavedb_net::Error::Saturated(what)`:
- **POSTs** are counted in the manager loop rather than left to the pool.
  The count is how a refusal answers its caller with a cause, because a
  refused spawn drops its arguments, the answer channel among them. On one
  thread the count cannot disagree with the pool: a task's slot is freed
  before anything else polls.
- **A watch** refused for a full actor pool hears `Saturated` on its ack.
- **A reader** refused for a full pool is a dial fault, so the actor's
  reconnect loop backs off and retries.

The improvement — a simple client-side retry with no attempt limit and
growing pauses, an automatic balancer of overload — is recorded in the new
`docs/improvements.md`, alongside the other deliberate deferrals.

The browser keeps `spawn_local` behind the same spawn functions, so the
manager's code does not branch on target. `platform::task` is now
**browser-only**: natively nothing used it any more, and a generic `spawn`
cannot exist over static pools. With it went the platform's last runtime
dependency on tokio. Workspace bar green (504 tests: 2 new), wasm32 clean.

**`block_on` — landed 2026-10-08, closing step 4.** `platform::carrier::block_on`
runs one future on the calling thread, parking it while the future waits.
It needs no runtime underneath: sockets wake through `async-io`'s reactor
and timers through `embassy-time`'s driver. It replaces:
- all 59 `#[tokio::test]`, turned into plain tests that block on their body;
- the eleven test-harness runtimes, the threads that run a node;
- the five `#[tokio::main]` of the examples.

Test servers that were `tokio::spawn`ed now run on threads of their own,
and `tokio::sync::oneshot`, `tokio::join!` and `yield_now` in tests became
`futures`' and `embassy-futures`'. No manifest in the workspace lists tokio
any more except `wavedb-quick-node`, for one thing: `OwnerLocks`
(`tokio::sync::Mutex`). `cargo tree -i tokio` shows only that path.

### Step 5 — landed 2026-10-08

No interim lock: the table became per thread for real, which first needed
every operation to run on a shard.

- **WebSocket `Call`s are routed** like POSTs (`Router::call`). The job
  carries the caller the session's gate 1 resolved at `Hello`
  (`Work::Call`), so the shard runs gates 2–3; a POST still carries its
  token and runs gate 1 on the shard. The subscribe ack's tail is routed
  too. The accept thread holds sockets and the subscription table and has
  **no store**. A connection no longer depends on the registry, so its task
  left `node_tasks!` (which now declares three tasks: accept, shard, job)
  and is declared once in the node, one pool per process.
- **`OwnerLocks` is one table per thread** (`shard::lock`,
  `shard::waiters`). The router keys on the lock's key, so one owner's
  operations always meet on one thread: no atomics, no `Arc`. A contended
  waiter joins the owner's **chain** with its `Waker` and leaves the run
  queue; a release hands the owner to the chain's head and wakes only it —
  arrival order, no barging, nobody polled for nothing. `embassy-sync`'s
  `Mutex` was the plan and was rejected on reading it: it keeps one waker,
  so two waiters wake each other in a loop (a test pins that ours does
  not). Links are slot indices, not pointers between futures, so no
  `unsafe`; the slot array (`MAX_WAITERS` = `MAX_JOBS` + 1, since every
  waiter is a job) and the `heapless` held-owner map are fixed at compile
  time and allocated once per thread.
- **tokio is gone** from every manifest of the workspace. The bench crate
  now declares the tokio feature `mongodb`'s sync wrapper needs; it used to
  arrive by accident, through the node's tokio inheriting `full`.

**Exit proof, as run.** The full bar is green (workspace tests including
the two-process e2e tests — contact-book cache and offline queue — benches,
wasm32 clippy, docs, deny). Step 5 touched no browser code, and the browser
demo was not re-run. The benchmark rows after step 5 were **waived** by the
project owner (2026-10-08: no measurements for now); the comparison made
after step 4 above, with no regression beyond noise, is the last
measurement of this RFC.

**Every step invalidates the benchmark seeds.** `bench-gen` links
`wavedb-storage`, which links `wavedb-platform`, and `benches/nix/gen.nix`
hashes every `.rs` under `crates/` plus the root manifest. Step 1 moved the
`large` WaveDB seed from `fh8mmn…` to `y5pqmg…` with no change to a single
stored byte. The `large` seed already building at that moment finishes as
the **pre-0066 baseline**; the seed for this RFC's code is rebuilt once,
after step 5. Narrowing `gen.nix` to the crates `bench-gen` actually links
would stop `net`, `quick-node` and `wavedb` edits from doing the same, but
not `platform`'s.

## Alternatives

- **Keep tokio, trimmed.** Drop `features = ["full"]`, bound the four
  channels. Gets most of the predictability for little work, and steps 1–3
  of this RFC are that work anyway. Loses the embedded path, keeps the
  coop budget.
- **smol's `LocalExecutor` + `async-io`.** Native to std, generic tasks,
  returns normally — the smallest change. Rejected for the embedded
  direction: it would be replaced again on the way to a microcontroller,
  while Embassy's executor is the one that runs there.
- **`embassy-net` over TUN/TAP on Linux.** One network stack everywhere, at
  the price of root, a private IP and bypassing the kernel's TCP. Not a
  server.
- **Generated `#[task]` functions from the start.** Works on stable, needs
  no allocation, and is the fallback if the `TaskPool` inference does not
  hold — but it moves runtime plumbing into the macros, which is a larger
  and less local change.

## Open questions

1. **`TaskPool` inference** — resolved in step 4 by choosing generated
   tasks instead: a pool deduced inside a generic function would live on the
   heap, while `node_tasks!` gives static pools with no allocator at all,
   which is the embedded direction. See "Progress".
2. **Capacities.** `MAX_CONNECTIONS`, `WS_OUTBOUND_DEPTH`, `MUTATION_DEPTH`:
   starting numbers are guesses until the suite measures them.
3. **The bench crate** — resolved in step 2: it used tokio only for the
   disk actor's one-shot reply, which is now `futures`', so it no longer
   depends on tokio at all.
4. **Timers per thread.** `embassy-time`'s std driver is one queue for the
   process. Not on the shards' hot path today; if it ever is, each carrier
   thread could keep its own timer wheel driven from its own loop.
5. **TLS is out of scope, and absent today.** The node speaks plain HTTP —
   no `rustls`, `native-tls` or `openssl` in any manifest — so transport
   security is a reverse proxy's job for now. When the node terminates TLS
   itself, the `embedded-io-async` seam is where it goes: `rustls` over the
   `async-io` socket natively, `embedded-tls` over `embassy-net` on a
   microcontroller. One trap to rule out then: Embassy-world examples
   commonly run `embedded-tls` **without certificate verification** (or
   with a self-signed certificate, which is why `curl` reports it invalid
   unless given `-k` or `--cacert`). Verification is not optional for a
   node; a TLS RFC must say how the device gets its trust anchors.
