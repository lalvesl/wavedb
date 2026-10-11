# RFC 0066 — Embassy executor and async-io: tokio out of the node

- **Status:** In progress — opened 2026-10-08. **Step 1 landed 2026-10-08**
  (see "Progress").
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
  `…/shard/{worker,disk,router,lock}.rs`

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
is a `std::sync::Mutex` map of per-owner **`tokio::sync::Mutex`**es, taken
with owned guards so a lock can be held across an operation's awaits. Its
runtime-independent equivalent is `async-lock`'s `Mutex::lock_arc()`
(owned guard, wakes through the standard `Waker`, same ecosystem as
`async-channel`); it moves in step 2 with the channels.

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
2. **Bounded channels and the owner locks** — `embassy-sync` within a
   thread, `async-channel` across threads, `async-lock` for `OwnerLocks`;
   the three full policies above, a test per policy (a slow WS client is
   disconnected; a write never blocks on a lagging publisher).
3. **Timers** — `embassy-time` behind `platform::time`.
4. **Executor spike, then the switch** — prove `TaskPool` inference on
   `serve_connection<E, S>`; then `platform::task`, the serve loop, the shard
   workers and the disk actor move to `raw::Executor`; `block_on` replaces
   the test macros.
5. **tokio removed** from every manifest; `cargo tree -i tokio` empty for the
   workspace (the bench crate keeps its own until it is ported).

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

1. **`TaskPool` inference** in a generic context on the pinned Embassy
   version (step 4's spike decides between it and generated tasks).
2. **Capacities.** `MAX_CONNECTIONS`, `WS_OUTBOUND_DEPTH`, `MUTATION_DEPTH`:
   starting numbers are guesses until the suite measures them.
3. **The bench crate** uses tokio only for `sync` (to `block_on` the disk
   actor's replies). It follows once the node does; until then it keeps its
   own dependency, outside the workspace.
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
