# RFC 0043 — Descriptors in the `Commit` frame

- **Status:** Implemented (landed 2026-07-28)
- **Superseded by:** [RFC 0046](0046-directory-deltas-in-the-window.md), then
  [RFC 0072](0072-the-settle-is-the-durability-point.md) (2026-10-16) — there is no `Commit` frame any more.
- **Supersedes:** the directory-chain blocks of
  [RFC 0019](0019-journal-rooted-recovery-DEPRECATED.md) (the `Commit` frame's *roots*)
- **Amended by:** [RFC 0046](0046-directory-deltas-in-the-window.md)
  *(Implemented 2026-07-29)* — the "Consequences" bullet below is right that
  the frame grows with the schema's total bucket count, and wrong that the
  checkpoint interval
  is enough to bound it: the volume scales with the **database**, not the
  change (1 MiB per checkpoint at 2 GiB, 100 MiB at 200 GiB, most of it types
  that did not change). 0046 keeps the conclusion that reached here — metadata
  must not be a scattered structure of its own — and moves the descriptor
  *changes* into the settle window's existing write, leaving the frame a
  snapshot address plus the list of deltas since it.
- **Crates:** `wavedb-storage`
- **Code:** `crates/wavedb-storage/src/{journal,commit}.rs`

## Summary

A checkpoint's addressing state travels **inside the `Commit` frame**: one
journal append carries every registered type's whole bucket-descriptor vector
*and* the retired journal's DONE marker. `data.bin` holds pages and dictionaries
and nothing else — the copy-on-write directory chain, its block allocation, its
protection bookkeeping, and the whole `chain` module are gone.

## Motivation

The chain persisted a type's `Vec<BlockDescriptor>` as linked 4 KiB blocks in
`data.bin`, and the frame carried only their root addresses. That cost more than
it bought:

- **It was the one part of a checkpoint that scaled with the directory, not with
  the change.** A dirty type rewrote its entire address vector —
  `ceil(buckets/507)` blocks — because `ChainNode` is doubly linked, so
  copy-on-writing one node cascades into its neighbours' links. A single `save`
  against a type with a million buckets wrote 16 KiB of page and ~7.7 MiB of
  chain.
- **The volume was identical either way.** All those descriptors have to be
  persisted per commit regardless, since the retired journal — the only older
  mention — is deleted immediately after. Writing them as blocks in `data.bin`
  merely turned one sequential append into a run of scattered block writes plus
  allocate/free/protect churn for the blocks themselves.
- **The frame already named every type.** `roots: Vec<(u64, u64)>` listed all
  registered types every commit; carrying `Vec<(u64, Vec<u64>)>` instead changes
  the frame's size, not its shape or its guarantees.

So the chain was a level of indirection whose only effect was fragmenting a
write that wanted to be sequential.

## Design

```rust
pub struct CommitFrame {
    /// The retired journal's timestamp — this frame IS its DONE marker.
    pub journal_ts: u64,
    /// (STRUCT_HASH, every bucket's raw BlockDescriptor), for every type.
    pub slots: Vec<(u64, Vec<u64>)>,
    /// (STRUCT_HASH, dictionary run descriptor raw) — 0 = no dictionary.
    pub dicts: Vec<(u64, u64)>,
}
```

A checkpoint is now:

1. rotate the journal;
2. settle the pending pages — one planned window, one positioned write
   ([RFC 0041](0041-single-barrier-checkpoint.md)) — then `fsync` `data.bin`;
3. **one** crc-framed append carrying `slots` + `dicts` + the DONE marker
   (`fsync` inside `Journal::append`);
4. delete the retired journal;
5. roll the allocator's protected set forward.

Recovery gets simpler in the same motion: `load_commit` *is* the directory
install — `Directory::from_slots(frame.slots[i])` — with no chain to walk and no
extra read. The allocator's `used` set is now just the reserved head, the page
runs named by those descriptors, and the dictionary runs.

**Ordering is unchanged and still load-bearing.** The pages must be durable
before the frame that addresses them; a crash in between leaves the frame
absent, so recovery falls back to the previous `Commit` and the retained journal
replays. The window written in step 2 is referenced by nothing and is reclaimed
as free space automatically.

## Consequences

- `chain.rs` deleted; `ChainTrack` (root / blocks / dirty) removed from
  `StructStorage`, and with it the last piece of per-type state that was neither
  cache, directory, nor dictionary.
- A checkpoint's `data.bin` write no longer contains metadata at all, which is
  what lets [RFC 0042](0042-free-space-defragmentation-DEPRECATED.md) reason about the file
  as pages-only when it relocates.
- The frame grows with the schema's total bucket count (8 bytes per bucket per
  commit). That is the same volume the chain wrote, now sequential — but it does
  mean a checkpoint's journal append is no longer small, and the checkpoint
  interval is the knob that bounds it.

## Alternatives

- **Keep the chain but make it partially rewritable** — a root block indexing
  node addresses instead of node-to-node links, so only dirty nodes are
  rewritten. Rejected: it optimises a structure this RFC removes outright, and
  the frame must carry every type's state anyway.
- **Write descriptors to `data.bin` and keep only roots in the frame** (the
  status quo ante). Rejected above.
- **Split the frame across appends** (one per type). Rejected: the retirement
  marker and the state it certifies must be atomic, and the crc framing gives
  that for free only within one frame.
