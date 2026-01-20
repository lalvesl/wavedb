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

## Storage

### A bulk fill leaves its free space inside the data files

- **Now.** The `large` seed's `data.bin` was 9.6 GB for 5 million inserts:
  ~1 920 bytes per row against ~260 in the dataset, where the 200 000-row
  tier suggests ~100 bytes live. Every checkpoint relocates pages
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
