# RFC 0055 — Sparse-index merge and root collapse

- **Status:** Deprecated 2026-10-31 — superseded by
  [RFC 0078](0078-ordered-lanes-over-a-quantized-page-index-WIP.md) §7, which
  removed the sparse index: a chain's order is its lane's, and the page index
  that finds it is cut, refined and journaled by the store. Opened 2026-08-01
  as Planned (low).
- **Crates:** `wavedb-core`
- **Related:** [RFC 0050](0050-clustered-record-chains.md) phase 3a is where this
  was skipped and phase 8a is where it was taken as debt;
  [RFC 0052](0052-segment-size-as-the-pagination-unit-DEPRECATED.md) owns the N…2N band this
  would mirror one level up

## Summary

The sparse index above every chain grows but never shrinks. An emptied node is
dropped from its parent and deleted from the store — removals leak nothing — but
an **underfull** node is left underfull, and a root reduced to a single child is
not collapsed. Nodes drain and stay drained; a tree that grew a level never gives
it back. This RFC is the missing half of the removal path: merge at half capacity,
redistribute when folding would breach the band, collapse a single-child root —
the same cycle the dense `BpTree` already has in `tree_delete`, and the same one
`chain_remove` applies to the segments below.

It is filed low on purpose. This is **accepted debt with a known bound**, not a
correctness gap.

## Motivation

The reason to leave it undone is arithmetic. This index holds one entry per
*segment*, not per record. At `DEFAULT_SPARSE_CAP = 700` and a chain capacity of
N=16, a million records is ~62 500 segments — under two full levels. Drained to a
quarter occupancy it is still two or three levels, which is the number the design
quotes **cold**. A structure whose worst case and best case are the same descent
depth does not urgently need rebalancing.

The reason to eventually do it is that "drained" is not a bounded state. The band
is only ever enforced downward: a workload that inserts a large collection and
then removes most of it keeps every node it ever allocated, each holding whatever
survived. Nothing reclaims them, so the cost is one node read per level that
should not exist, on every descent, forever — paid by `find_offset`, by
`list_len`'s root sum, and by the catch-up tail scans. It is small and it is
permanent, which is exactly the profile of debt worth retiring once something
more valuable is not competing for the same afternoon.

## Design sketch

Mirror `chain_remove`, because the shapes already match: a `Slot` and a `Branch`
are the same triple (least key, pointer, count), so one merge routine serves both
levels — unlike `BpTree`, whose leaf and internal bodies differ.

- **Merge at `cap / 2`.** After a removal leaves a node below half, fold it into
  its left sibling when the two together fit; otherwise **redistribute** — move
  entries across until both sit inside the band. Folding first and splitting the
  result is not equivalent: it rewrites two nodes to reach a state one rewrite
  could have.
- **Collapse a single-child root.** The root keeps its id when it splits
  (`plan_upsert` mints the *child* instead, which is what keeps the `Pivot`
  permanent), so collapsing must be the same trick inverted: the surviving
  child's entries move **into** the root's id and the child is deleted. The root
  pointer never moves, the `Pivot` is never rewritten, and the endpoint
  permanence RFC 0050 relies on is preserved in both directions.
- **Counts are the invariant to protect.** Every entry carries an element count
  and the root's sum is `list_len`'s answer. A merge that moved entries without
  moving their counts would leave the index and the segments disagreeing about a
  number no reader can cross-check — the `counts_survive_recovery` failure mode,
  reached without a crash. The merge path needs the same assertion the split path
  has: children's counts sum to the parent's entry.
- **One batch, planned not committed.** Like everything in `sparse_write`, the
  routine returns a `Write` batch the chain folds into the *same* atomic batch as
  the segment writes it describes. This is what makes an index entry and its
  segment unable to disagree, and it is not negotiable.

### The cost that made it wait

Collapsing means **reading a sibling** during a descent that is already planning
writes — and that sibling may have a pending write in the batch being planned.
The read has to go through the `Overlay`, not the `Store`, or the merge folds a
stale copy over a fresh one. That is the whole reason this half was skipped while
the rest of phase 3a landed: the insert path never needs to look sideways, and
the removal path does.

## Alternatives

- **A background compaction pass**, like RFC 0042 does for free space. Rejected
  for the same reason phase 8 dissolved: the chain rebalances synchronously on
  every removal, and an index that rebalanced asynchronously would be a second
  consistency model for a structure whose entire correctness argument is "same
  batch as the segment".
- **Rebuild the index from the chain** when it drains past a threshold. Simple,
  and O(segments) — which is the one cost profile everything since RFC 0046 has
  been removing. A merge is O(depth).
- **Leave it forever.** Defensible, and it is the status quo. It stops being
  defensible if a real workload ever shows a collection that shrinks by an order
  of magnitude and stays shrunk — which is the trigger to promote this RFC.
