# RFC 0069 — Compaction into the holes

- **Status:** Deprecated (2026-10-15) — implemented 2026-10-10, then
  superseded
- **Supersedes:** [RFC 0042](0042-free-space-defragmentation-DEPRECATED.md)
- **Superseded by:** [RFC 0071](0071-data-files-by-address.md)

## What it proposed

One `data.bin` per shard, best-fit reuse of the holes copy-on-write leaves,
and a compaction that moved the runs nearest the end of the file **down**
into the lowest hole that held them, so the free space gathered at the tail
and was cut off. Stepped (`compact_step`): moving, one addressing snapshot,
two checkpoints sealing the moves, then the truncation. The node started one
on a daily UTC time, a disk ceiling (10 GiB) or a free share (50%).

## Why it lost

- **One file is the size of its history.** A single live run near the end
  pinned the file's length until everything above every hole had moved.
- **Hole reuse scatters.** A settle round landed wherever a hole fit it, so
  the free space — and the work of giving it back — spread across the whole
  file.
- **Two models for one problem.** The object store (RFC 0070) already keeps
  append-only files named by address and empties them by carrying their live
  pages forward; the disk now does the same.

[RFC 0071](0071-data-files-by-address.md) replaces the placement and the
reclaim: append-only data files named by the block they start at, and a
compaction that carries the emptiest files' runs forward, cutting and
deleting what they leave. What survived from here: the stepped phases, the
rounds with their addressing delta in the window, the sealing by two
checkpoints, the node's triggers and their guards, and the page cache's
invalidation on a dead run.
