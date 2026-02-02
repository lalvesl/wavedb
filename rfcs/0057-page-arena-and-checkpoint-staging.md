# RFC 0057 — The page arena, and what the checkpoint may stage into it

- **Status:** Planned — opened 2026-08-01
- **Crates:** `wavedb-storage`
- **Code:** `crates/wavedb-storage/src/{plan,checkpoint,read_through,commit}.rs`
- **Supersedes:** [RFC 0044](0044-page-cache.md) — same cache, given
  a concrete allocation shape and a decision about the write path
- **Builds on:** [RFC 0041](0041-single-barrier-checkpoint.md) (the settle's one
  read per touched page is what this removes), [RFC 0047](0047-generational-journal-retirement-DEPRECATED.md)
  (the retirement boundary the safety argument below rests on)

## Summary

Give the page cache RFC 0044 describes a **single allocation**: one `Vec<u8>`
arena divided into 4 KiB blocks, plus a `HashMap<BlockDescriptor, u32>` from a
page's disk address to its block offset inside the arena. Reads consult it before
`data.bin`; the settle's read-modify-write finds hot buckets resident and issues
no read at all; and a settle round's *output* stays in the arena, so the pages a
round just wrote are exactly the ones the next round is most likely to need.

This RFC also **rejects** two things that were proposed alongside it, and the
reasons are the substance:

1. a crash-recovery check for "this data was already written" — there is nothing
   to check, and the safety comes from somewhere else entirely;
2. progressively pre-building the checkpoint's window buffer between checkpoints
   — the two inputs it would need are both non-final until the round closes.

## The premise that does not hold

The concern this RFC opened with was: a checkpoint is slow, writes keep landing
during it, so the window can contain data belonging to the *next* checkpoint —
and then a crash leaves `data.bin` holding bytes the commit does not cover.

**That situation is real, expected, and already safe**, and the guarantee is not
idempotence. It is the retirement boundary:

> `Commit { journal_ts: old.ts(), head }` retires **only the journal that was
> rotated out**. Every write that lands after the rotation is in the *new*
> journal, which this checkpoint never covers — so `restore` always replays it.

Which yields the invariant that answers the whole question:

> A page in `data.bin` may hold data newer than the checkpoint's boundary, but
> that data is **never only** in `data.bin`. The journal holding it survives the
> checkpoint.

Both crash windows follow from it:

- **Before the `Commit` frame is durable.** Recovery roots at the *previous*
  commit. The window's pages and its edit chunk are simply unreferenced blocks:
  the allocator is rebuilt `from_layout(len_blocks, &used)` over the old commit's
  reachable set, so those blocks come back as free space. Both journals survive
  and replay. Nothing acked is lost, nothing stale is adopted.
- **After it is durable.** The old journal is covered and deleted; the new one
  replays in full, including the batches that settled early. They are re-applied
  to the caches and re-settled — writing the same bytes twice, which converges
  because a plan derives from the caches' current state.

`commit.rs` already states this at step 3 ("writes landing in the new journal may
settle too: harmless, their journal survives and re-settling converges"). So the
proposed `if already_written { skip }` on replay is **not** a small fix to make —
it is a read added to detect a condition that is not a fault. Correctness here
comes from the boundary, not from noticing duplicates.

### And the write lock does not exist either

The other half of the premise was a choice between locking writes during the
checkpoint or copying the cache to avoid it. Neither is needed, because the
write path and the settle path do not share a lock:

| path | holds |
|---|---|
| `apply_inner` (every write) | `journal` |
| `place_in` (carve → `write_run` → free) | `alloc`, `meta` |

Step 1 of `commit_journal` rotates under the journal lock in microseconds and
writers redirect; `drain()` then runs the expensive part holding neither. A write
can land, journal, commit to cache and queue itself while the window is being
written to disk.

There is **one** real contention point, and this RFC's cache is what fixes it:
`plan_slot` holds `slot.directory().lock()` and `slot.dictionary().lock()` while
it reads pages from disk, so a cache-miss read of that same type blocks for the
duration of those reads.

## What is actually expensive

`crates/wavedb-storage/src/plan.rs` — the vacant arm of the staged-page map:

```rust
let page = dir.read_page(slot.struct_hash(), &store.file, bucket, dict)?;
```

Every touched bucket not already staged in this round is a **random disk read**
before it can be rewritten. After RFC 0041 collapsed the round to one positioned
write, this is the entire remaining IO cost of settling — and on the read-then-
write shape that dominates applications, it is a re-read of the very page the
reader just decoded and threw away.

That is the cost worth attacking, and it is the one RFC 0044 already named.

## Design: the arena

```rust
pub struct PageArena {
    /// One allocation, `capacity_blocks * BLOCK_SIZE`, block-aligned.
    arena: Vec<u8>,
    /// Disk address → the block offset holding it. A `BlockDescriptor` already
    /// carries the run's length, so the extent inside the arena is derived, not
    /// stored twice.
    map: HashMap<BlockDescriptor, u32>,
    /// Free block runs inside the arena — the same allocator discipline as
    /// `data.bin`, at a much smaller scale.
    free: BlockAllocator,
}
```

- **The key is the `BlockDescriptor`, not `(slot, bucket)`.** This is the change
  from RFC 0044, and it makes invalidation disappear rather than cheap: pages are
  copy-on-write, so a rewritten bucket gets a *new* descriptor and the old entry
  is simply never looked up again. Nothing has to be invalidated at the
  checkpoint's descriptor swap; the stale extent is reclaimed by the arena's own
  allocator when it needs room.
- **Stored bytes, not decoded pages.** The same image `write_run` puts down, so
  populating is one `memcpy` and there is no decode state to keep coherent.
  Compressed pages stay compressed in RAM — the point is saving the IOp.
- **A fixed budget, declared.** The arena's size is what it is at open; there is
  no growth path and no emergent footprint. This is the shape RFC 0053's stance
  demands: bounded, never pinned, and a number an operator can set rather than
  one that discovers itself under load.

The read path becomes: record cache → **arena** → `data.bin`. The settle's
`read_page` takes the same route, which is where the win lands.

## The synergy that survives: a round warms its own successor

`assemble()` builds the window's bytes and hands them to `write_run`. If it
assembles **into the arena** and the map keeps those blocks under their freshly
installed descriptors, then the pages a round just wrote stay resident at no
extra cost — and hot buckets are hot precisely because they get touched again,
so the next round's read-modify-write of them is a RAM hit.

This also removes one full-window allocation and one copy per round: today
`assemble` does `vec![0u8; window.byte_len()]` and copies every page image into
it, after `plan_slot` already produced each image as its own `Vec<u8>`.

## What this RFC rejects: progressive pre-fill

The proposal was to build the checkpoint's buffer *between* checkpoints, so the
round's CPU and reads are spread out and the checkpoint itself is only the disk
write. It does not work, for two independent reasons:

**A page image is a function of the bucket's whole contents at settle time.**
Build it early and the next write to that bucket invalidates it. Buckets that
receive no further writes are the only ones where pre-building pays, and those
are exactly the ones whose settle is cheapest anyway. For a hot bucket the
result is repeated rebuilds — more CPU, not less.

**The dictionary is not final until the round closes.** `SlotPage` compresses
against a per-type zstd dictionary whose version *is* its prefix length, and
`SlotPlan::dict` may grow during the round. Every image built against version `N`
is invalid the moment the round ships `N+1`. The one input pre-building depends
on is the one that moves last.

A third, smaller obstacle: building in place at a page's final window offset
needs the offset, which comes from `carve()`, which needs the sizes, which need
the images. Predicting compressed sizes to break that cycle is a worse trade than
the copy it saves.

What remains true from the proposal is the useful half — the arena, and the fact
that the settle's output is worth keeping.

## Alternatives

- **Keep `Vec<u8>` per cached page**, as RFC 0044 sketched. Simpler, and correct.
  Loses the single allocation, the block alignment that lets a cached run be
  handed straight to `write_run`, and the fragmentation control an arena gets for
  free. Worth measuring before assuming the arena wins.
- **`mmap` `data.bin`.** The operating system's page cache is exactly this
  structure, already written. Rejected on the same ground as everywhere else in
  this engine: the write path's durability rests on knowing when bytes reach the
  disk, and `mmap` moves that decision into the kernel — and it does not exist on
  the wasm target at all, so the seam would fork.
- **Cache decoded `SlotPage`s** instead of stored bytes. Saves the zstd
  decompression too, not just the IOp. Rejected for now because it doubles the
  coherence surface (two representations of the same page) for a CPU cost that is
  not the measured bottleneck — but it is the natural follow-up if it ever is.

## Open questions

- **Eviction policy.** The arena needs one, and RFC 0053 already frames it:
  per-tenant fairness, nothing pinned, navigational entries (`BpTree` nodes)
  worth more than streaming ones (record pages walked once). That RFC is held at
  *Planned* until a measured workload justifies it; this one can ship with plain
  LRU over the free-run allocator and inherit the policy later.
- **Whether the arena also serves the record cache.** Today `StructStorage`
  keeps per-type `id → Vec<u8>` maps with their own eviction. Two budgets that do
  not know about each other is a worse story than one, but merging them means
  variable-size entries in a block-aligned arena — a different allocator.
- **Contention.** `plan_slot` holding the directory and dictionary locks across
  its reads is tolerable when the reads are RAM hits and much less so when they
  are not. Whether the arena makes that lock hold acceptable, or whether the hold
  should be narrowed regardless, wants measuring rather than guessing.
