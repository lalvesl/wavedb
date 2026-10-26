# RFC 0042 — Free-space defragmentation

- **Status:** Deprecated (2026-10-10; implemented 2026-07-28)
- **Superseded by:** [RFC 0069](0069-compaction-into-the-holes.md)
- **Crates:** `wavedb-storage`, `wavedb-quick-node`

## What it proposed

A background pass that relocated isolated live pages — runs with free space
on both sides — **forward to the tail** (`alloc_tail`), so the holes they
vacated coalesced into windows large enough for a whole checkpoint's
best-fit allocation ([RFC 0041](0041-single-barrier-checkpoint.md)). Pages
were ranked by blocks coalesced per block copied, a run had to merge at
least twice what it cost, and runs backing onto the trailing free space were
skipped. The node ran it when the largest free extent fell below
`defrag_below_blocks` (256), copying at most `defrag_budget_blocks` (256) per
tick. The resource it defended was IOps, not disk.

## Why it lost

- **It grew the file to save it.** Every move appended to the tail, and the
  holes it opened were only reused if a later window happened to fit them: a
  5 000-row bench store came out of `defragment` 127 KB bigger than it went
  in.
- **Nothing ever gave space back.** `BlockAllocator::truncate` had no caller;
  `data.bin` only grew (9.6 GB for ~1 GB of live data on the `large` seed).
- The user chose to **reuse the empty space** instead, on triggers the
  deployment sets: [RFC 0069](0069-compaction-into-the-holes.md) moves the
  highest runs *down* into the lowest holes and truncates the free tail.

What survived: the verbatim page move (a page is position-independent), the
relocation through the ordinary settle window, and the safety argument — the
vacated runs are freed through the checkpoint's protected sets.
