# RFC 0068 — The record cache as a log

- **Status:** WIP — opened 2026-10-10
- **Crates:** `wavedb-storage`
- **Code:** `crates/wavedb-storage/src/{record_index,record_log,record_compact,record_cache,settle,apply,plan}.rs`, `tests/record_cache.rs`
- **Builds on:** [0041](0041-single-barrier-checkpoint.md) (the checkpoint and
  its settle), [0063](0063-engine-yield-map-and-interruptible-engine-PLANNED.md)
  part 3 (the settle in bounded rounds), [0067](0067-thread-per-core-shards-WIP.md)
  (one store per shard, owned by one worker)
- **Related:** [0044](0044-page-cache.md) (the page cache, a
  separate layer), [0053](0053-tenant-fair-cache-retention-PLANNED.md) (which
  entries deserve to stay hot — the policy this mechanism leaves open)

## Summary

A store's record cache becomes two structures: an **index** in the shape of
Dragonfly's Dashtable — a small directory over fixed-size segments, each a
flat open-addressed table — and, per type, a **log of chunks** holding the
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

**One log per type, one index per store.** The logs keep the types apart: a
page holds one type, so a settle round is per type anyway and goes straight
into that type's plan, and a type that is rarely used will be the first a
retention policy lets go of ([0053](0053-tenant-fair-cache-retention-PLANNED.md)).
What made a log per type expensive — a half-filled 64 KiB chunk per type per
shard, ~360 MiB at 90 types × 64 shards — is answered by the chunk's growth
(below), not by mixing the types. The index is shared: its slot already names
the type, and an index per type would be one mostly empty segment per type
per shard.

The key is `(Id, type)`. A slot names the type's log, so the type costs
nothing to compare, and two types that did share an id would still be two
records rather than one silently replacing the other.

A store is used from one thread (RFC 0067), so the cache — like the rest of
the store's state — lives in a plain cell: no lock, no atomic (see
*Implementation decisions*).

### The log

Chunks of at most 64 KiB, each entry `[Id 16 B][len u32][bytes]`, appended at
the write end `W` right after the journal append — so **the log's order is
the journal's order**. A record rewritten is appended again and the index
repointed; its old copy stays where it was, dead, until its chunk goes. A
remove appends a **tombstone** (`len = u32::MAX`, no bytes).

The chunk being filled starts at **512 B and doubles** as it fills; at 64 KiB
it is sealed and the next one starts small again. So the room a chunk has not
used yet never exceeds what it already holds: a type barely written holds a
few hundred bytes, and the worst case — every type of every shard just past a
doubling — wastes at most as much as those chunks hold, never a fixed 64 KiB
per type. A record larger than 64 KiB gets a chunk of its own size. Chunks are
`Vec<u8>`s that grow by power-of-two steps, so the allocator sees a handful of
sizes, not one per record.

Positions are logical byte counts over everything ever appended, so `S`, `W`
and `R` are plain integers and a chunk starts where the previous one ended.

### The index

Dashtable's shape: a directory (a `Vec` of segment indexes) over segments of
fixed size; a segment is a flat table of buckets with a few stash buckets for
spill, and a full segment **splits in two** — only it, never the table — so
growth is one segment at a time with no rehash spike. Geometry is Dragonfly's:
56 regular + 4 stash buckets of 14 slots; a key lives in its home bucket, the
next one, or the stash.

A slot holds **only the entry's location** — type slot `u16`, offset `u16`
(an entry starts inside a 64 KiB chunk), chunk `u32` — 8 bytes, plus a
one-byte fingerprint. The `Id` is not repeated: it heads the entry the slot
names, so a fingerprint match is confirmed by reading it there, and a split
rehashes a key the same way. That is ~9 B per slot instead of ~25, and the
type is part of the key for free. The fingerprints of a bucket sit together
so a probe compares them in one pass, written as plain code the compiler
vectorises (on the pinned toolchain a 16-byte compare loop compiles to
`movdqu`/`pcmpeqb`/`pmovmskb`); hand-written intrinsics need `unsafe` at
their call site and are not used.

The hash is seeded per open store (`seahash::hash_seeded`, with a seed from
the same OS entropy the superblock's seed comes from): a `#[wavedb::key]` id
is a hash of client data, and an unseeded table would let a client choose
collisions and force splits. Keys a split cannot separate anyway go to a
small overflow list once the directory would outgrow its segments by more
than 2⁴ — so no input makes an insert loop or the directory explode.

### Settledness is a position

Three marks on each type's log (the checkpoint owes while **any** log has
`S < R`; a round may take entries from several logs):

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

Per record, beyond its bytes: a slot (9 B, ~13 B at a typical fill) and the
entry head (20 B) — about 33 B, against ~115–130 B before. The index never
shrinks: segments split and do not merge, so it stays sized for the most
records it has held at once (open question 4). Dead copies inside
`[S, W)` are waste bounded by the same window; dead copies before `S` leave
with their chunk.

### Unchanged

The on-disk format, the journal, the page and settle formats, the `Store`
trait, and every caller above `wavedb-storage` — except that a store is now
`Send` and not `Sync` (*Implementation decisions*). No `unsafe`.

## Implementation decisions (2026-10-10)

- **A log per type** (user decision), not one per store: see *Two structures
  per store*. The chunk being filled grows from 512 B, which is what made it
  affordable.
- **A slot holds no `Id`** — only the entry's 8-byte location and a
  fingerprint; the id is read from the log entry to confirm a match. The key
  is `(Id, type)`.
- **No lock, no atomic** (user decision). There is no parallel access to a
  store's data: a node moves each store onto the worker that owns it, a
  benchmark consumer builds its own, and nothing else reaches it. So the
  whole store — the record cache, and with it the journal, allocator,
  addressing log, checkpoint state, per-type directories and dictionaries,
  the page cache and the I/O counters — moved from `parking_lot` locks and
  atomics to `RefCell`/`Cell`. `PageStore` is `Send` (it moves to its owner
  once; the parallel open builds stores on scoped threads) and deliberately
  not `Sync`; `lib.rs` asserts the first. `parking_lot` left the workspace.
  The barrier helper (RFC 0067 §4), the one real second thread, shares no
  memory either: each request carries its own duplicate of the journal's
  descriptor and a channel to answer on, and the barrier counters and the
  failed mark are plain fields of the worker's journal, changed only when it
  asks or reads an answer. `wavedb-storage` holds no lock, atomic, `Arc` or
  `Rc`; what synchronises the two threads is inside the channels (`std`'s
  `mpsc`, `futures`' `oneshot`), as any cross-thread message must be.
  `async-channel` left the crate.
- **One round at a time needs no guard**: a settle step is synchronous and
  the store is used from one thread, so two rounds cannot interleave.
- **The client's local cache is `Rc`**, as the browser's store already was:
  a native `Db` opened with a cache is no longer `Send`.
- **Records over `u32::MAX - 1` bytes** are refused before the journal sees
  them (`StorageError::RecordTooLarge`) — a log entry's length is a `u32`.

## Amendment (2026-10-18): compact before dropping

The read probe (the `probe` feature's `reads` lines) measured what FIFO
eviction does to a collection: every write rewrites the chain segment and
the sparse-index node it touches **whole**, so the log holds a dead copy of
each per write — on the micro bench ~8 KiB of segment and ~12 KiB of index
node beside each 300-byte record. At the end of the inserts the 96 MiB
cache held 1 437 of 200 000 records, ~85 % of it dead copies, and a hot
read hit it 0.8 % of the time. Dropping a chunk to reclaim its dead copies
dropped the records beside them.

So eviction now **compacts first** (`record_compact.rs`):

- Each chunk counts its **dead bytes**: a write that repoints the index
  marks the copy it replaced, and a settled tombstone leaving the index
  marks itself.
- A chunk wholly before `S`, not the one being filled, with at least a
  quarter of it dead is **compactable**. Eviction compacts the deadest such
  chunk across every type while the store is over budget, and drops
  settled chunks oldest first only when none is left.
- A compaction rewrites the chunk with the entries the index still names,
  repointing each (`RecordIndex::repoint`), and **folds** it into the chunk
  before it when both fit 64 KiB, so the log does not end as one tiny chunk
  per original one. A chunk left empty is freed.
- Chunks live in a **slab** and keep their number — a `Loc`'s `chunk` — for
  as long as they are held, a freed number reused; the log's order is a
  separate queue of numbers. That is what lets a chunk leave from the middle
  of the log without moving any other `Loc`.
- A compacted chunk keeps its logical span: only chunks wholly before `S`
  are rewritten and a round reads from `S` on, so no position a round or a
  mark uses moves.

What it does not reach: `[S, W)`. A store that settles in a long window
holds that window's dead copies until it settles — on the relaxed micro row
(a settle per second) up to ~70 MB of the 96 MiB, so its records still
leave and hot reads there still miss; a node settles every 200 ms, and a
durable write settles at once. Compacting the unsettled span would need the
round's scan to step over the holes compaction leaves, and the batch mark
to pass a span with nothing left in it — not done. The root of the volume,
a segment rewritten whole per write, is the chain's (RFC 0050).

Measured on the relaxed micro row (probe, caged): over the update phase the
index held 22 220 records against 3 576 before, a read 9.5 µs against 11.0.

## Amendment (2026-10-23): pressure

What is unsettled cannot be evicted, so a store settled only on a clock
lets a burst between two ticks fill its budget with bytes no eviction may
touch — the relaxed micro row above, ~70 MB of 96 MiB. `PageStore::under_pressure()`
says when the unsettled bytes (`W − S`, summed over the types: one
subtraction each, cheap after every write) pass `StoreOptions::pressure_percent`
of the record budget — 50 by default, a `u8` read as at most 100, worked out
at open from the store's own budget, so a sharded database's line is each
data shard's share like its budget. The store does not act on it, as it does
not evict on its own; a caller asks after a write and settles, then evicts.
A read never adds to either, so it asks nothing.

The benchmark's drivers had evicted after every operation, reads included,
each time summing every chunk's capacity to find nothing to do — paid by
every hot read, outside its timed window (how much of the read row's wall
time it was is for the next run to say). They now act only after a write:
a durable one evicts (it settled already), a relaxed one settles and evicts
on its window or on pressure. A node's valve already weighs the unsettled
bytes against a band of its own.

## Deliberately out of scope

- **Keeping settled records hot** (beyond the compaction of the amendment
  above, which keeps every live entry of a mostly dead chunk). Eviction is
  FIFO by chunk, so a hot record
  — a B+tree's interior node — can leave and be read back from its page, as it
  could before (`evict_up_to` popped by smallest id). Keeping it would mean copying
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
   skewed hash and the fingerprint probe compiled to vector code. **Done.**
2. **The log** (`record_log.rs`): chunks, `W`/`S`/`R`, append, tombstones, the
   round's copy-out, chunk drop with index cleanup. **Done**, with the
   index and logs joined in `record_cache.rs`.
3. **The switch.** `apply`, reads, the settle and the checkpoint use the log;
   `Pending` and `TypeSlot`'s cache and `dead` set are deleted; the budget is
   measured against the footprint. **Done.**
4. **Proof.**
   - storage: settle part of a burst, evict — the settled leave and read back
     from pages, the unsettled stay; a write during a round reads back new and
     settles new; reopen and replay;
   - under steady writes the footprint stays inside the budget (it did not
     before);
   - a row of the bench's memory columns before and after.

   The storage proofs are done (`tests/record_cache.rs`, plus the unit tests
   of the three modules; a write during a round is proved at the cache level,
   where the round can be interleaved). The bench row waits for a run the
   user approves.

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
- **One log per store, fixed-size chunks** — this RFC's first draft. Mixes
  the types, so a settle round has to route each entry back to its type's
  directory, and a retention policy cannot tell a cold type from a hot one.
  Its reason — a log per type with fixed 64 KiB chunks holds ~360 MiB of
  mostly empty chunks at 90 types × 64 shards — is answered by growing the
  chunk being filled instead.

## Open questions

1. **Chunk size.** 512 B growing to 64 KiB: whether the cap or the start
   deserves tuning is for a measurement to say.
2. **Segment and bucket geometry.** Dragonfly's 56 + 4 buckets of 14 slots is a
   starting point, not a finding.
3. ~~Whether `S` should be per type~~ — it is, with a log per type.
4. **Shrinking the index.** Segments only split; after a burst is evicted the
   index stays sized for it. Merging two sibling segments whose keys fit in
   one is the inverse of a split and would bound that, at the cost of a merge
   check on eviction.
