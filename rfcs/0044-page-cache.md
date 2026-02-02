# RFC 0044 — The page cache

- **Status:** Implemented — opened 2026-07-28; the cache by run landed
  2026-07-29; pages held decoded since 2026-10-17 (see Progress)
- **Crates:** `wavedb-storage`
- **Code:** `crates/wavedb-storage/src/{page_cache,block_file,directory_pages,plan,checkpoint,read_through}.rs`
- **Builds on:** [RFC 0041](0041-single-barrier-checkpoint.md) (the settle's one
  read per touched page is what this eliminates)

## Summary

A second, **page-granular** cache beside the existing per-type record cache:
`(type, bucket) → the page's stored bytes`, validated by the descriptor they were
read from. A record read that misses the record cache already pulls, decodes and
discards a whole page; keeping that page means its siblings are free, and — the
real prize — the settle's read-modify-write of the same bucket finds it resident
and issues **no read at all**.

## Motivation

Read a record, then write it: the overwhelmingly common application shape. Today
that costs two page reads of the *same* page, in two different layers:

1. `read_from_pages` → `Directory::read_page` → decompress → extract one record,
   discard the rest;
2. later, the settle's `plan_slot` → `staged_page` → `Directory::read_page` again,
   to apply the write.

After [RFC 0041](0041-single-barrier-checkpoint.md) a settle round is one write
and *one read per touched page* — that read is now the entire remaining IO cost
of settling, and it is precisely the page the reader just had in hand.

The same waste shows up on collection walks: a `BpTree` node page is read once
per node visit, and node pages are the hottest thing in the engine (every walk
touches them, and they are compression-off, so re-reading them buys nothing).

## Design

As built (see Progress for how it got here):

- **Key: the run, in its lane** — the block extent a page occupies on disk,
  or its descriptor in the object store (two address spaces). Page writes
  are copy-on-write, so a disk run's page never changes while the run is
  allocated; an entry leaves when a round supersedes its run or a write
  lands on its extent. An object store's file is PUT once and never again,
  so a remote entry never goes stale; it leaves by the budget.
- **Value: the page decoded** — a `SlotPage`, decompressed and decoded once
  when it came from disk or the object store. A hit is a lookup lending the
  page: no read, no zstd, no decode. The budget charges what the page takes
  decoded (`SlotPage::footprint`), so the same RAM holds fewer pages than
  stored bytes would — CPU per hit traded for RAM per page, on purpose.
- **The settle takes its pages out** instead of decoding them again — the
  round rewrites each into a new run, superseding the old — and **holds the
  pages it writes** under their new runs once the window is written: the
  page just settled is the one read next. A page restored from the object
  store is held the same way.
- **Object mode**: the RAM backing keeps the stored images — it is the
  store's disk — and this cache over it holds them decoded, as over a disk.

The original design, kept for the record:

- **Key `(slot index, bucket)`, value the page's *stored* bytes** — the same
  image `write_run` put down, not a decoded `SlotPage`. One `memcpy` to
  populate, no decode state to keep coherent, and compressed pages stay
  compressed in RAM (the point is saving the IOp, not the CPU).
- **Validity is the descriptor, so invalidation is free.** An entry records the
  `BlockDescriptor` it was read under; a lookup compares it against the
  directory's current one and treats a mismatch as a miss. The checkpoint's
  descriptor swap is the *only* thing that changes a descriptor, so there is no
  invalidation protocol to get wrong — a stale entry simply stops matching.
- **Write-through from the plan.** `plan_slot` builds each new page image
  anyway; publishing it into the page cache alongside the descriptor costs
  nothing and means the page the checkpoint just wrote is immediately warm.
- **Its own byte budget, evicted first.** RAM is the scarce resource, so this
  cache is explicitly subordinate: `evict_settled` drains the page cache to zero
  before it touches a single record-cache entry. A page-cache miss is never a
  correctness event, only an IOp.
- **Reserved `BpTree` node slot included.** Node pages are the highest-value
  entries; they are also the most likely to be superseded, which the descriptor
  check handles for free.

## Why it is the lowest-priority cache

The record cache already absorbs the hot path: an acknowledged write is served
from `StructStorage::mem_cache` until it is settled *and* evicted, so the
read-then-write pattern only pays a page read once the working set has fallen
out of the record cache. This is a second-order win — real, measurable, and
worth doing after the write path is settled, not before.

## Open questions

- **Stored bytes or decoded `SlotPage`?** Settled 2026-10-17: decoded. Every
  hit paid zstd and the decode again; a page that came from disk or the
  object store now stays decompressed in RAM.
- **Does it subsume the record cache?** A page cache with a good hit rate makes
  the record cache partly redundant memory. Merging them is a bigger change (the
  record cache is also the unsettled-write buffer, i.e. correctness state, which
  a page cache must never be) and is deliberately out of scope here.
- **Admission policy.** A full collection scan would sweep the cache with pages
  it will not revisit; a scan-resistant admission rule (or a "do not admit while
  streaming" flag on the walk) may be needed.
- **Metric.** `BlockFile::io()` already counts reads, so hit rate is
  `1 - reads_after/reads_before` on a fixed workload; the bench baseline should
  record it before this lands.

## Progress

- **2026-07-29 — the cache by run** (`page_cache`, `block_file`): stored
  images keyed by run, invalidated by overlap on a write and when a round
  supersedes a run; lent to the decoder, never copied.
- **2026-10-17 — pages held decoded** (`page_cache`, `block_file`,
  `directory_pages`, `plan`, `checkpoint`, `evict`, `read_through`,
  `upload`, `upload_plan`): `SlotPage`s charged by their decoded footprint;
  `Directory::with_page` lends one to a read (`get_record` copies out the
  one record), `take_page` hands it to the settle; a round holds the pages
  it writes, a restore the page it brings back; the object store's pages —
  a read of an evicted bucket, a restore, an upload's merge — go through
  the same cache in their own lane (`PageStore::with_remote_page`).
