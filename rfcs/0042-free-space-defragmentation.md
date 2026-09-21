# RFC 0042 — Free-space defragmentation

- **Status:** Implemented (landed 2026-07-28)
- **Crates:** `wavedb-storage`, `wavedb-quick-node` (the maintenance policy)
- **Code:** `crates/wavedb-storage/src/{defrag,alloc,checkpoint}.rs`
- **Companion:** [RFC 0041](0041-single-barrier-checkpoint.md) — the
  single-barrier checkpoint that consumes the windows this produces

## Summary

An async background process that **relocates isolated live runs so free extents
coalesce into windows large enough to hold a whole checkpoint**. The checkpoint
itself needs no knowledge of it: it asks `BlockAllocator::alloc(total)` for one
contiguous run, and best-fit lands in a recycled window whenever one exists. The
defragmenter's only job is to keep such windows existing.

## Motivation

Copy-on-write page rewrites free the old run and allocate elsewhere, so
`data.bin` accumulates holes whose *total* size is ample but whose *largest
contiguous* extent shrinks. Under [RFC 0041](0041-single-barrier-checkpoint.md)
a checkpoint asks for one run sized to the whole touched set; once no hole fits,
every checkpoint grows the tail and the file grows monotonically even though most
of it is free.

The resource being defended is **IOps, not disk space**. Spending reads and
writes in the background to keep one large window available is worth it precisely
because it converts each future checkpoint from "grow the tail" into "reuse a
window", and keeps the file bounded without ever putting a compaction on the
serving path.

## Design

- **Selection uses state the allocator already keeps.** `free_neighbours(run)`
  reads `by_pos` for the free extents touching a live run on each side; a run
  with space on both sides is the cheapest target, because moving it merges
  three extents into one. `largest_free_extent` (over `by_size`) is the gauge.
  The live runs themselves come from walking each slot's directory descriptors —
  the allocator tracks free space, not occupancy.
- **The heuristic is coalesced blocks per block copied**, and candidates are
  ranked by it, so a small page stranded between two large holes goes first.
- **Two guards keep a pass from churning.** A run must merge at least **twice**
  what copying it costs — otherwise it is only being shifted sideways into the
  hole next door — and a run backing onto the file's *trailing* free space is
  skipped outright, since "moving" it would land it essentially where it already
  is and re-dirty its page every tick.
- **Relocation reuses the RFC 0041 write path**, with one difference: the window
  is allocated with `alloc_tail`, never best-fit. Best-fit would cheerfully drop
  a page back beside the hole it was vacating. Writing forward leaves the
  neighbourhood to coalesce into the large window ordinary checkpoints consume.
- **Page bytes move verbatim.** A stored page is position-independent
  (`[len][crc][envelope]`), so a move reads the run, trims it to its own length
  prefix, and places those bytes — no decode, no recompression, no re-planning,
  and every `dict_len` stamp stays valid.
- **Safety is the checkpoint's, unchanged.** The vacated runs are freed through
  the allocator's protected set, so a run the last durable `Commit` still names
  is only released after the next one; the directory is merely marked dirty, so
  until a checkpoint rewrites the chain, recovery still resolves the old — and
  still intact — locations.
- **Trigger and budget** live in `wavedb-quick-node`'s `maintain` loop:
  `defrag_below_blocks` (largest free extent under which a pass runs, default
  256 blocks = 1 MiB) and `defrag_budget_blocks` (blocks one pass may copy,
  default 256). A pass that finds no candidate costs nothing.
- **Tail truncation** stays `BlockAllocator::truncate`.

## Testing

- `moving_an_isolated_run_merges_its_neighbours` / `a_packed_run_is_not_a_candidate`
  — the allocator's neighbourhood view, which selection is built on.
- `tail_allocation_never_lands_in_a_hole` — the placement rule that makes the
  pass worth running.
- `defragment_relocates_pages_without_losing_records` — 200 records, rewritten
  and checkpointed to pock the file, then defragmented: the pass must not
  fragment further, every record still reads back **with the caches evicted**
  (so every read comes off a moved page), and it survives a reopen.

## Open questions

- Whether the gauge should be relative (largest extent ÷ recent window size)
  rather than the absolute block count it is now.
- Interaction with unbounded history growth (no cold tier —
  [RFC 0033](0033-cold-history-slow-node-tier-DEPRECATED.md)): archived versions
  are cold and never rewritten, so they are ideal relocation filler, but they
  also mean live-data density falls over time.
- Whether a checkpoint should ever *split* its window across holes instead of
  waiting for the cleaner (cheaper, but reopens the "one run or a few" question
  in [RFC 0041](0041-single-barrier-checkpoint.md)).
