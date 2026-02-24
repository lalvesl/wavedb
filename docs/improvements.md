# Points to improve

Known, deliberately deferred improvements: each is a choice made to keep the
current step simple, recorded so it is not forgotten. Each entry says
**what is there now**, **what to do instead**, and **where it was decided**.

An entry that grows into a design becomes an RFC (`rfcs/`) and is removed
from here. One that is done is removed too; the commit says so.

## Client

### Retry when the client is saturated — the overload auto-balancer

- **Now.** The connection manager runs its tasks from static pools
  (RFC 0066 step 4). When one is full it **refuses**, with
  `wavedb_net::Error::Saturated`:
  - a POST beyond `MAX_POSTS` (1024 by default, set at build time with
    `WAVEDB_CLIENT_MAX_POSTS`);
  - a watch connection beyond 64 per kind;
  - a WebSocket reader beyond 128.

  Nothing was sent, so trying again is safe. But nothing tries again: the
  application sees the error. Through `wavedb::Db` the error is
  `Error::Transport`, so a read may fall back to the local cache.
- **Instead.** A simple retry in the client, with **no attempt limit** and a
  **growing pause** between attempts (exponential backoff with a cap), only
  for `Saturated`. A burst then spreads itself out over time instead of
  failing — an automatic balancer of overload, with no queue to bound and
  no new state.
- **Decided** 2026-10-08, in RFC 0066 step 4 (client half): refuse now,
  keep it simple.

### At most 32 native caches open in one process

- **Now.** Each `Db::open` puts its cache's `PageStore` on a carrier of
  its own, served by one Embassy task from a static pool of 32
  (`cache::native::MAX_CACHES`). A 33rd open cache refuses with
  `Error::Backend` until one is dropped.
- **Instead.** One cache carrier per process with a task per cache, or a
  pool sized at build time like `WAVEDB_CLIENT_MAX_POSTS` — whichever an
  application that holds many app caches open at once actually needs.
- **Decided** 2026-10-16, in RFC 0072 phase 4: a client opens one cache,
  tests a handful; a fixed pool is the Embassy way.

### The cache's tick settles everything at once

- **Now.** Every 200 ms the cache's carrier runs a whole `drain` — every
  round of the batch and its `fsync` — before serving the next request; a
  durable write settles inline the same way. A large batch holds the
  application's cache reads for as long as it takes.
- **Instead.** Step the cycle in bounded rounds (`cycle_step`) between
  requests, as a node's BG task does, with a barrier helper so the
  `fsync` leaves the carrier too.
- **Decided** 2026-10-16, in RFC 0072 phase 4: a client's batches are
  small, and the node's valve would be machinery with nothing to weigh.

## Node

### Pool sizes are plain constants

- **Now.** `MAX_CONNECTIONS`, `MAX_JOBS`, `MAX_SHARDS`
  (`wavedb_quick_node::__rt`) are fixed in the source.
- **Instead.** Use the same build-time knob as the client's `MAX_POSTS`
  (`option_env!` parsed by a `const fn`). The pools are statics, so the size
  can only change at build time.

### Memory per connection

Measured 2026-10-08 with `schema-smoke`'s registry:
- a connection task was ~6 KB, of which ~5 KB was `Exposure::execute`,
  whose largest arm is a collection insert. Since RFC 0066 step 5 a
  connection routes every command to a shard, so that part now lives in
  the job pool, sized by operations in flight rather than connections open;
- an idle WebSocket connection is ~58 KB.

Fine for a server, and deferred for that reason ("escovação de bit"). In
order of cost:

1. **WebSocket outbound queue preallocates.** A bounded `async-channel`
   allocates all `WS_OUTBOUND_DEPTH` (256) slots of 192 bytes up front:
   ~51 KB per connection, empty or not. Use an unbounded channel and check
   `len()` against the depth on push; the disconnect policy is unchanged.
2. **A size budget test.** It fails when the connection future grows past a
   limit, ratcheting down only, like the 350-line rule. With static pools,
   future size is reserved memory.
3. **Shrink the insert/save path.** Hold fewer large values across
   `.await`, and flatten pass-through layers. `Box::pin` only rare arms
   (the natural-key upsert) last.

## Platform

### Every object-store request opens its own connection

- **Now.** `s3::exchange` sends `connection: close` and dials afresh for
  each request: a TCP handshake, and over HTTPS a TLS one too — about two
  extra round trips per GET or PUT against a public endpoint. rustls
  resumes sessions across connections (the `ClientConfig` is shared), which
  saves the certificate check but no round trip. The read of an evicted
  page waits on it.
- **Instead.** Keep connections alive: a small pool per `Bucket` in a
  `RefCell` (one thread, no lock), the response read by its own framing —
  which `exchange` already does (`content-length` or the last chunk) — and
  a request on a connection the peer closed while idle sent once more on a
  fresh one.
- **Decided** 2026-10-16, after RFC 0070 phase 9 (HTTPS): not now.

### A large page is one PUT of one buffer

- **Now.** A file goes up as one PUT of a body held whole in RAM. A page of
  up to 4 GiB makes a file of about that size (RFC 0070 §3) — inside S3's
  5 GiB single-PUT limit, but a fault anywhere sends all of it again.
- **Instead.** Multipart upload past a size threshold: parts sent and
  retried one by one, the object appearing only at the completing request
  (which must keep the create-only fence, `If-None-Match: *`).
- **Decided** 2026-10-16, after RFC 0070 phase 9: not now.

## Wire

### Decoding copies every record out of its buffer — zero-copy views

- **Now.** `from_wire::<T>` builds an owned `T`: every `String` and
  `Vec<u8>` is a copy of its bytes (one `memcpy` each since the bulk slice
  paths), every `Vec<T>` an allocation, and a page body one allocation per
  record (391 of them in a micro page). A decoded value is free to move —
  `Send`, no `Rc` — but paid for in allocations and copies its reader may
  never look at.
- **Instead.** Zero-copy reads: an owned buffer (`Wire<T>`, one
  `Box<[u8]>`, `Send`) with accessors the derive generates —
  `rec.name() -> &str`, `rec.blob() -> &[u8]`, `rec.tags() ->
  ListView<'_, u64>`, a nested struct as its `View<'_>`, scalars by value
  through `from_le_bytes` — and `rec.to_owned()` for the owned `T` when a
  write needs it. The struct a developer declares keeps its `String` and
  `Vec` (the owned shape writes take); nothing in it borrows, so the only
  lifetime a user meets is the `'_` of a view held past one call. The
  format moves each heap region's **offset** into the stack, so any field is
  O(1) without summing the ones before it. The first user is `SlotPage`:
  its decompressed buffer plus a sorted `(Id, offset)` index instead of a
  `BTreeMap<Id, Vec<u8>>`; then collection reads returning `Wire<T>`; then
  `#[server]` arguments and replies handed between threads as buffers.
  Safe Rust only: `&[T]` for `u8` alone (any other `T` needs aligned bytes
  and `unsafe`, so it is a `ListView` reading each element), and `&str`
  revalidated per access (`from_utf8`), unless an explicit `unsafe`
  exception is granted. Building the owned struct inside one allocation is
  not an option: `GlobalAlloc::dealloc` must receive the exact pointer and
  layout `alloc` returned, so a `Vec`/`String` carved out of a larger block
  is undefined behaviour when it drops.
- **Decided** 2026-10-18, after the read probe measured a page decode at
  1.39 ms, 1.1 ms of it the codec walking `Vec<u8>` byte by byte: the bulk
  `memcpy` for byte vectors now, the views as an RFC when they are taken up.

## Core

### A keyed insert reads a vacant anchor three times

- **Now.** A `#[wavedb::key]` insert at a vacant anchor reads that anchor
  three times before its batch commits:
  1. `anchor()` in `upsert_keyed`, to tell vacant, living and dead apart;
  2. `get_of` in `plan_chained_save`, to find the version to archive;
  3. the `Expect(id, None)` guard, in `apply`.

  A vacant anchor is never in the record cache, so every one of the three
  falls through to its page. The first may reach the disk; the other two
  likely hit the page cache. A plain NonUnique insert reads none of this:
  its id is minted by `mint_floored_id` and cannot collide, so it carries
  no guard.
- **Instead.** Read the anchor once and pass what was read down:
  `upsert_keyed` hands its verdict and the bytes to `plan_chained_save`,
  and the guard compares against them. The guard itself stays: it is what
  turns a race on one key into `Error::Conflict` instead of a lost version.
- **Decided** 2026-10-18, found while reviewing the write path after
  RFCs 0074/0075. The micro bench does not exercise it (`Thing` is not
  keyed), so nothing measured it yet.

## Storage

### A bulk fill leaves its free space inside the data files

- **Now.** The `large` seed's `data.bin` was 9.6 GB for 5 million inserts:
  ~1 920 bytes per row against ~260 in the dataset, where the 200 000-row
  tier suggests ~100 bytes live. Every settle relocates pages
  copy-on-write, and the bench fill never compacts.
- **Since** 2026-10-10 the storage half exists
  ([RFC 0069](../rfcs/0069-compaction-into-the-holes-DEPRECATED.md)): `PageStore::space()`
  reports total, free and deferred blocks, and `compact()` empties the holed
  files — since 2026-10-15 by carrying their runs forward into the active
  data file and deleting them
  ([RFC 0071](../rfcs/0071-data-files-by-address.md)).
  The embedded row's `compacted` point now runs it.
- **Instead.** Read `space()` at the end of the fill, and decide whether the
  fill should end with `compact()` — a bench question, needing a run.
- **Decided** 2026-10-08: not now.

## Benchmarks

### Shop rows do not fit the 500 MB cage

- **Now.** All four WaveDB shop rows are OOM-killed at `smoke` and
  `small`, before and after RFC 0066 (`benches/PROGRESS.md`). They cannot be
  recorded on the 4c-500m lane.
- **Instead.** Find which part of the cgroup's memory crosses 500 MB
  (process RSS or dirtied page cache), then shrink the fixture or give shop
  its own lane.
