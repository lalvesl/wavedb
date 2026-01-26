# RFC 0030 — Superblock-pointer checkpoint — DEPRECATED

- **Status:** Deprecated — superseded by
  [RFC 0019 — Journal-rooted recovery](0019-journal-rooted-recovery-DEPRECATED.md)
- **Was:** the interim S2/S3 checkpoint (landed then superseded, 2026-07-07)

## What it proposed

Make `data.bin` an authoritative checkpoint (so the journal could truncate and
open could replay only the tail) by having the **superblock hold a pointer** to
the current directory projection. A checkpoint wrote the new projection and then
**updated the superblock pointer** to name it; recovery followed the pointer.

## Why it was replaced

Superseded the same day it landed:

1. **A mutable superblock pointer reintroduces write-in-place at the most
   dangerous block.** Block 0 is the root of everything; updating a pointer there
   on every checkpoint is exactly the torn-write hazard a write-once superblock
   avoids.
2. **Two sources of truth.** The pointer and the journal both claimed to name the
   live roots, needing a reconciliation rule on recovery.

## What replaced it

[RFC 0019](0019-journal-rooted-recovery-DEPRECATED.md): recovery **roots in the newest valid
`Commit` frame** in the journal — a single frame naming every type's roots,
written into a freshly rotated journal that retires the old one. The superblock
goes back to **write-once**; there is one source of truth (the newest `Commit`),
and no pointer is ever mutated in place.
