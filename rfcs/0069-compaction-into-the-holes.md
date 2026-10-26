# RFC 0069 — Compaction into the holes

- **Status:** Implemented (landed 2026-10-10)
- **Supersedes:** [RFC 0042](0042-free-space-defragmentation-DEPRECATED.md)
- **Crates:** `wavedb-storage` (the mechanism), `wavedb-quick-node` (the policy)
- **Code:** `crates/wavedb-storage/src/{compact,defrag,alloc,checkpoint}.rs`,
  `crates/wavedb-quick-node/src/upkeep.rs`, `crates/wavedb-quick-node/src/shard/owned.rs`

## Summary

Copy-on-write leaves a hole wherever a page used to be, and nothing ever
gave that space back: `data.bin` only grew. Compaction takes the runs nearest
the end of the file — pages and dictionaries — and rewrites them, verbatim,
into the **lowest-addressed hole** that holds them. A run only ever moves
down, so the live data packs toward the front and what it vacated becomes
one free extent at the back, which is cut off the file. The old addressing
chain is replaced by one snapshot, and two checkpoints release the vacated
runs before the truncation. A node compacts when one of three configurable
triggers is met: a daily UTC time, a disk ceiling (10 GiB by default), or a
share of the file that is free (50% by default).

## Motivation

[RFC 0042](0042-free-space-defragmentation-DEPRECATED.md) defended IOps, not
disk: it moved stranded pages **to the tail** so that holes would coalesce
into windows a checkpoint's best-fit could land in. Two things went wrong
with that.

- **It grew the file to save it.** Every move appended to the tail, and the
  holes it opened were only reused if a later window happened to fit them. A
  5 000-row bench store came out of `defragment` 127 KB *bigger* than it went
  in (`README.md`, the `compacted` footprint).
- **Nothing ever shrank `data.bin`.** `BlockAllocator::truncate` had no caller
  outside its tests. The `large` bench seed sat at 9.6 GB for ~1 GB of live
  data (`docs/improvements.md`).

The user's decision (2026-10-10): the better path is to **reuse the empty
space**, and to let the deployment say when — by time of day, by a disk
ceiling, or by how much of the file is holes.

## Design

### Placement: first fit, by address, below the run

`BlockAllocator::alloc_below(count, limit)` takes the lowest-addressed free
extent that holds `count` blocks and ends at or before `limit`, or nothing.
Best-fit takes the smallest hole *wherever it lies*, which could move a run
up; first fit by address only moves it down and fills the front of the file
first. `alloc_lowest` is the same without a limit, growing the file only when
no hole fits.

### A round is a settle window

A round picks the lowest hole of at least two blocks and fills it with the
**highest** runs that fit — at most 128 images, keeping one block for the
round's addressing delta. The images are written as **one window** through
the ordinary checkpoint path (`place_in` with `Placement::Below(hole_end)`):
one positioned write, the delta in the same window
([RFC 0046](0046-directory-deltas-in-the-window.md)), then the descriptor
swap. If the round with its delta outgrows every hole below the limit, it
carries one image fewer and tries again; no hole at all means no round, and
nothing has changed.

The bytes move verbatim, as in 0042: a page is position-independent, so it
is read, trimmed to its own length prefix, and written again — no decode, no
recompression. A dictionary moves as its current image; growth is
append-only, so every page's prefix of it stays valid.

### Phases

The store keeps where a compaction is (`PageStore.compaction`, a `Cell` — the
store is single-threaded) and the caller steps it with
`PageStore::compact_step(budget_blocks)`:

1. **Moving.** Each step is one pass of rounds until about `budget_blocks`
   were copied (at least one image, however large, so a step always makes
   progress). Before a round, a snapshot the addressing log owes is written
   first, so a long compaction never outgrows `MAX_EDIT_CHUNKS`. The pass
   that finds no run able to move down ends the phase.
2. **The old addressing goes.** The moves leave a chain of deltas, and the
   chunks before them may sit anywhere in the file. One snapshot in the
   lowest hole that holds it (`Placement::Snapshot`) replaces the whole chain
   ([RFC 0048](0048-chained-addressing-log.md)) and frees every chunk it
   supersedes.
3. **Sealing.** The vacated runs are still named by the last `Commit` frame,
   which a crash would reopen from, so the allocator holds them back
   ([RFC 0047](0047-generational-journal-retirement.md)). One checkpoint names
   the moved state — an already-open one does as well as a fresh one — and
   the step that **opens** the next checkpoint proves that frame durable and
   drops the old generation.
4. **Truncating.** `PageStore::reclaim_tail` cuts the trailing free extent
   off the allocator and off the file. Only blocks no recovery root names are
   ever in that extent, so no reopen can need them; a resize that fails
   leaves a file longer than the allocator thinks, which a reopen reads as
   free tail. If the snapshot of step 2 had to land high (no hole held it
   then), it is rewritten **once** more into the room the release opened and
   sealed again — at most one extra round of two checkpoints.

A `Sealing` step leaves a checkpoint open; the node's ordinary maintenance
steps close it (`Owned::step`, which already runs whatever checkpoint is
open). `PageStore::compact()` runs all of it to the end, stepping the
checkpoints itself, for a caller with nothing else to do (the bench, tests).

### The page cache lets go when a run dies

A round that supersedes a page run now invalidates its image in the page
cache at once ([RFC 0044](0044-page-cache-PLANNED-LOW.md)): no directory names
the run any more, so no read will ask for it, and before this it occupied the
budget until the FIFO pushed it out. The cache's queue also gained a stamp
per entry, so an image re-inserted after an invalidation can no longer be
evicted early by its stale queue entry, and leftovers are tidied once they
outnumber live entries, so the queue cannot grow without bound.

### The policy (`wavedb-quick-node`)

`Maintenance::compaction: Compaction`:

| Field | Default | Meaning |
|-------|---------|---------|
| `daily_at: Option<UtcTime>` | `None` | Every day at `hour:minute` UTC. A node started after today's time waits for tomorrow's. |
| `above_bytes: Option<u64>` | 10 GiB | Once the database's files pass this. Each shard holds its share (`/ P`), like the cache budget — workers share no memory, so none sees the whole. |
| `above_free_percent: Option<u8>` | 50 | Once this share of a file is free or held back. |
| `budget_blocks: u64` | 256 | Blocks one pass may copy — the IO a 200 ms upkeep tick spends. |

Any trigger set and met starts a compaction, which then runs to its end, one
pass per tick per store. The size triggers also need something to give back
(at least `budget_blocks` unused) and, after a compaction has finished, the
unused space to have grown by `budget_blocks` again, so a shard whose live
data alone passes the ceiling is not compacted over and over for nothing.
The daily trigger ignores both guards: it is the deployment saying "now".

## Testing

- `defrag::tests` — placement only goes down (`alloc_below`, `alloc_lowest`);
  a round fills the lowest hole from the top and keeps a block for the delta;
  a packed file has no round.
- `tests/compaction.rs` — a pocked store (1 500 incompressible records, two in
  three removed) shrinks from 547 to 162 blocks with no free block left, the
  file on disk matches, every record reads back from the moved pages with the
  record cache emptied, and again after a reopen; stepped with a small budget
  and writes between steps, it ends the same and keeps every write; a store
  dropped mid-move reopens whole from its last commit and can compact from
  there.
- `shard::owned` — a met trigger starts a compaction and the upkeep ticks plus
  the ordinary checkpoint steps carry it to its end; the next tick, with
  nothing new to give back, does not start another.
- `upkeep` — the daily trigger fires once a day at its time and not on a
  start after it; the ceiling is per-shard and needs room to free; the free
  share fires at its percentage; a finished compaction re-arms only on new
  free space.
- `page_cache` — a re-inserted run is not evicted by its old queue entry; the
  queue stays proportional to what is held.

Not measured yet: the bench `compacted` footprint now runs `compact()` (the
row's sequence is unchanged otherwise). Running it needs the user's OK.

## Alternatives

- **Keep 0042's tail moves.** They keep a large window open for checkpoints,
  which is worth IOps under steady writes, but they grow the file and never
  give space back. The triggers bound when compaction runs, so the IOps it
  spends are spent when the deployment chose.
- **Split a checkpoint's window across holes** (0042's open question). It
  would reuse holes without moving anything, but turns a round's one write
  into several and leaves the file as long as it ever was.
- **Compact on open.** Free for a running node, but a node that stays up for
  months would never shrink.

## Open questions

- A round's hole must hold two blocks (an image and the delta), so one-block
  holes are only reclaimed when a neighbour frees. A delta stored apart from
  its round would lift that, at the cost of a second write.
- Pages a settle round writes at the tail *during* a compaction (no hole fit
  them) stay above the vacated space until the next compaction moves them.
- The edit-chain chunks of the compaction itself are moved only by the next
  snapshot; they land low already, being written into the holes.
