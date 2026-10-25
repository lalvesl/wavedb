# RFC 0068 — The record cache as a log

- **Status:** Planned — opened 2026-10-10
- **Crates:** `wavedb-storage`
- **Code (today):** `crates/wavedb-storage/src/{struct_storage,pending,settle,apply,plan,read_through}.rs`
- **Builds on:** [0041](0041-single-barrier-checkpoint.md) (the checkpoint and
  its settle), [0063](0063-engine-yield-map-and-interruptible-engine-PLANNED.md)
  part 3 (the settle in bounded rounds), [0067](0067-thread-per-core-shards-WIP.md)
  (one store per shard, owned by one worker)
- **Related:** [0044](0044-page-cache-PLANNED-LOW.md) (the page cache, a
  separate layer), [0053](0053-tenant-fair-cache-retention-PLANNED.md) (which
  entries deserve to stay hot — the policy this mechanism leaves open)

## Summary

A store's record cache becomes two structures: an **index** in the shape of
Dragonfly's Dashtable — a small directory over fixed-size segments, each a
flat open-addressed table — and a **log of fixed-size chunks** holding the
records' bytes, appended in commit order. Whether a record is settled is
then a matter of **position**: a cursor `S` marks how far the settle has
reached, everything before it is settled and may be dropped, everything from
it to the write end `W` may still be owed to a page. Eviction drops whole
chunks before `S`, so it never has to ask about one record, and the pending
queue, the tombstone sets and the per-record heap allocation all go. Memory
becomes what the budget says: segments and chunks of known size, and nothing
handed to the allocator per record.

## Motivation

A node holding 1.3 GB of data was seen using more than 7 GB. Reading the
cache's code gives causes that compound:

1. **Eviction is all or nothing.** `evict_settled` (`settle.rs`) returns
   without evicting anything while the pending queue holds a single id —
   correct, since an unsettled record's page is stale, but under steady writes
   the queue is never empty when the upkeep looks, so nothing is ever evicted
   and the budget (`Maintenance::cache_budget_bytes`, 1 GiB) does nothing.
2. **The budget counts payload only.** `cached_bytes` sums `Vec::len()`. An
   entry costs its key (16 B), its `Vec` (24 B), its `BTreeMap` share (a leaf
   of 11 slots ≈ 450 B, ~55 % full under the monotone ids `key_nanos` mints)
   and the allocator's header and rounding — ~115–130 B beyond the payload, so
   a budget of 1 GiB of small records is about 2 GiB of RAM.
3. **One allocation per record.** Millions of small `Vec`s freed in the middle
   of the heap are memory the allocator rarely returns: the resident size stays
   at its peak after an eviction. (The zstd contexts `page.rs` builds with the
   dictionary for every page it compresses or reads — large allocations, made
   and dropped per page — add to it; that is a separate fix.)
4. **The pending queue grows with writes, not records.** `note_touched` pushes
   an id per write, so a record saved a thousand times is queued a thousand
   times.

(Records are cached as their wire bytes, uncompressed, while pages are zstd
with per-type dictionaries — so the same data is several times larger in RAM
than in `data.bin` before any of the above. That is a property of a write
cache, not a defect.)

Fixing the first alone — skip pending ids instead of refusing — needs a
pending *set*, a count of settle rounds in flight, and a scan of the cache
per eviction, and leaves 2–4 as they are. Changing the container alone does
not help: a `HashMap` costs about what the `BTreeMap` does per entry, keeps
the allocation per record, and doubles its table on growth — a transient
1.5× of a multi-GB map. The waste is in *one heap object per record*, and in
deciding settledness *per record*; this RFC removes both.

## Design

### Two structures per store

**One log and one index per store**, not per type. With 64 shards and a
dozen types, a half-filled chunk per type per shard is real memory; one per
store is one chunk. The index is keyed by the record's `Id` alone: ids do
not collide across types — the same property the flat-keyspace stores
(`wavedb::cache::IdbStore`, `MemStore`) already rely on, and that the salt
guard (`wavedb-core/src/expose_salt.rs`) protects — and the type is the hash
the bytes begin with, which is how `apply` routes a write today.

On a node a store belongs to one worker (RFC 0067), so one lock per store
contends with nothing; per-type locks bought nothing there.

### The log

Fixed-size chunks (`Box<[u8]>`, 64 KiB to start), each entry
`[Id 16 B][len u32][bytes]`, appended at the write end `W` under the journal
lock — so **the log's order is the journal's order**. A record rewritten is
appended again and the index repointed; its old copy stays where it was, dead,
until its chunk goes. A remove appends a **tombstone** (an entry with a
removed flag and no bytes).

The allocator sees one allocation size, and the store can take its whole
budget at open if it wants a fixed resident size.

### The index

Dashtable's shape: a directory (a `Vec` of segment indexes) over segments of
fixed size; a segment is a flat table of buckets with a few stash buckets for
spill, and a full segment **splits in two** — only it, never the table — so
growth is one segment at a time with no rehash spike. A slot holds the `Id`
and the entry's location in the log (chunk `u32`, offset `u32`); a bucket's
one-byte fingerprints sit together so a probe compares them in one pass. That
comparison is written as plain code the compiler vectorises (on the pinned
toolchain a 16-byte compare loop compiles to `movdqu`/`pcmpeqb`/`pmovmskb`);
hand-written intrinsics need `unsafe` at their call site and are not used.

The hash is seeded per process (`seahash::hash_seeded`, seeds from
`platform::rand`): a `#[wavedb::key]` id is a hash of client data, and an
unseeded table would let a client choose collisions and force splits.

### Settledness is a position

Three marks on the log:

```text
 chunk 0   chunk 1   chunk 2   chunk 3   chunk 4
[───────][───────][───────][──•────][────•──]
                              S         W
 ◄──── settled: may be dropped ──────►◄─ may be owed ─►
```

- **`W`** — where the next entry goes.
- **`S`** — how far the settle has reached. Before `S`, every entry's page has
  been written; from `S` to `W`, an entry may still be owed to its page.
- **`R`** — set to `W` when a checkpoint rotates the journal. The checkpoint
  owes a settle while `S < R` — the `owed`/`fresh` split of `pending.rs`,
  reduced to a comparison, and exact for the same reason: the rotation and the
  appends share the journal lock.

### The operations

- **Write.** Append at `W`, repoint the index. Nothing else.
- **Read.** The index: a live entry is the answer, a tombstone is `None`, a
  miss falls through to the pages as today.
- **Settle round.** Take the live entries from `S` onward, up to the round's
  budget, **copying them out** under a short lock; build the pages from the
  copies with no lock held — the slow part, with its page reads — write them;
  then advance `S` past what the round took. One round at a time per store.
  - An entry whose index slot points elsewhere is dead and skipped: a newer
    copy exists further on and a later round settles it. A record saved a
    thousand times inside the window is settled once, at its latest.
  - A write to the same record **during** a round appends a new copy and
    repoints the index; readers get it. The round still writes the version it
    copied, and the page is briefly behind — but nobody reads that page while
    the record's live copy is after `S`. The next round writes the newer
    version, and only once `S` passes it can its chunk go. One round at a time
    is what keeps an older version from landing after a newer one.
  - A failed round leaves `S` where it was; the next retries it.
  - The copy is what lets the round run without holding anything: `S` and `W`
    can share the current chunk, and Rust will not let a round read the front
    of a buffer that a write is appending to. The settle already reads the
    cache by cloning (`plan.rs`); the copy of one round is small beside the
    page reads it then makes.
- **Settled tombstones.** When `S` passes a tombstone its page no longer holds
  the record, so its index slot is removed (if it still names that tombstone).
  This replaces `TypeSlot`'s `dead` set.
- **Eviction.** Drop whole chunks **before `S`**, oldest first, until the
  store is inside its budget. Everything there is settled by construction, so
  there is nothing to check and nothing to carry forward; for each entry of a
  dropped chunk, the index slot is removed if it still names that entry.
  Eviction never touches `[S, W)`, and `S` moves only after a round's pages
  are written — so no count of rounds in flight is needed either.
- **Drain** is "settle until `S = W`"; **has pending** is `S < W`.
- **Recovery** replays the journal into the log, as it replays into the caches
  today: every replayed entry lands after `S`.

### What the budget means now

The store's footprint is its segments plus its chunks, each of known size, and
the budget is compared against that — not against payload. What the budget
cannot evict is `[S, W)`, as today it cannot evict the pending set; that span
is bounded by the journal threshold, which forces a checkpoint, which forces
the settle up to `R`.

Per record, beyond its bytes: a slot (~38 B at a typical fill) and the entry
head (20 B) — about 58 B, against ~115–130 B today. Dead copies inside
`[S, W)` are waste bounded by the same window; dead copies before `S` leave
with their chunk.

### Unchanged

The on-disk format, the journal, the page and settle formats, the `Store`
trait, and every caller above `wavedb-storage`. No `unsafe`.

## Deliberately out of scope

- **Keeping settled records hot.** Eviction is FIFO by chunk, so a hot record
  — a B+tree's interior node — can leave and be read back from its page, as it
  can today (`evict_up_to` pops by smallest id). Keeping it would mean copying
  it forward into a region the settle skips, and reads do not deposit into the
  cache today either. Both belong with [0053](0053-tenant-fair-cache-retention-PLANNED.md).
- **The settle waiting on the disk.** A round runs synchronously on its worker;
  a cache miss while building a page stops the worker for the read. Bounded
  by the round's budget today, and fixed by suspending on a read helper (0063),
  not by the cache's shape.
- **The zstd contexts made per page** (`page.rs`): a separate fix, but measured
  together with this one.

## Delivery

1. **The index** (`record_index.rs`): the directory and segments, insert /
   get / remove / split, the seeded hash; unit tests including split under a
   skewed hash and the fingerprint probe compiled to vector code.
2. **The log** (`record_log.rs`): chunks, `W`/`S`/`R`, append, tombstones, the
   round's copy-out, chunk drop with index cleanup.
3. **The switch.** `apply`, reads, the settle and the checkpoint use the log;
   `Pending` and `TypeSlot`'s cache and `dead` set are deleted; the budget is
   measured against the footprint.
4. **Proof.**
   - storage: settle part of a burst, evict — the settled leave and read back
     from pages, the unsettled stay; a write during a round reads back new and
     settles new; reopen and replay;
   - under steady writes the footprint stays inside the budget (it does not
     today);
   - a row of the bench's memory columns before and after.

## Alternatives

- **Keep the `BTreeMap`, evict per record.** A pending *set*, a count of
  rounds in flight, a scan per eviction that skips pending ids. Fixes cause 1
  only.
- **A `HashMap`.** About the `BTreeMap`'s cost per entry, the same allocation
  per record, and a doubling rehash: a transient 1.5× of the whole map.
- **The Dashtable alone.** Dragonfly's table stores keys and values as
  pointers; larger values are separate allocations, left to the allocator.
  It fixes the index, not the bytes — the bytes are the problem here.
- **A different global allocator** (mimalloc, jemalloc). Less fragmentation,
  none of it bounded, and a C dependency; the budget would still count
  payload.
- **Linear hashing, as on disk.** Splits buckets in a fixed order rather than
  the full one, so it needs overflow chains; its address stability is what the
  disk wants and RAM does not.
- **Settle only sealed chunks**, shared as `Arc<[u8]>` with no copy. Under
  light writes the current chunk never fills, the settle never reaches `R`
  and the checkpoint stalls — unless chunks are sealed early, wasting their
  remainder.
- **A log per type.** One partly filled chunk per type per shard: 64 shards ×
  a dozen types × 64 KiB is ~50 MB of mostly empty chunks, for a lock split
  that a worker-owned store does not need.

## Open questions

1. **Chunk size.** 64 KiB to start: small enough that one partly filled chunk
   per shard is nothing, large enough that its head is negligible. A record
   larger than a chunk gets a chunk of its own size.
2. **Segment and bucket geometry.** Dragonfly's 56 + 4 buckets of 14 slots is a
   starting point, not a finding.
3. **Whether `S` should be per type** for settling: one round settles several
   types today; one cursor keeps that.
