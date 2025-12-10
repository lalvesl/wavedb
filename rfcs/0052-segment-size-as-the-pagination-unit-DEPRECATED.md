# RFC 0052 — Segment size as the pagination unit

- **Status:** Deprecated 2026-10-31 — superseded by
  [RFC 0078](0078-ordered-lanes-over-a-quantized-page-index-WIP.md) §7: a
  chain is a key range of an ordered lane, so there are no segments to size,
  a page of a list is a skip and a limit on its range, and the skip passes
  whole pages by their counts. `page = N` was retired with them — it no
  longer laid anything out, and a knob in the identity that changes nothing
  on disk would only mint new types. What follows is the record as it
  stood.
- **Was:** Implemented 2026-07-31 — opened 2026-07-29, revised 2026-07-30.
  Most of it landed as [RFC 0050](0050-clustered-record-chains.md) phases 3b
  (the N…2N band, the 50/50 split, the merge at N/2 and its two rules), 2/3a
  (element counts, the offset descent, the root's total) and 7a (the struct's
  `page = N`, folded); the per-list `page` landed here on 2026-07-31.
  **Remaining:** two policy questions that want a real application to settle;
  the mechanism and its durability are done.
- **Crates:** `wavedb-core`, `wavedb-macros`
- **Builds on:** [RFC 0050](0050-clustered-record-chains.md) (the segment),
  [RFC 0051](0051-ordered-record-lists.md) (the sparse index),
  [RFC 0049](0049-elastic-pages-and-load-driven-splits.md) (a page is however many
  blocks it needs, read in one positioned read — which is why a segment's *element*
  count can be fixed without capping its size)
- **Constrained by:** [RFC 0053](0053-tenant-fair-cache-retention-PLANNED.md) —
  nothing may be pinned in RAM, so every cost here is quoted cold

## Summary

The developer declares a chain's segment capacity as a **minimum N**, normally the
page size their interface renders. A segment then holds between N and 2N records and
splits at 2N. Reading a UI page costs one segment read, sometimes two, and the
second is nearly always a cache hit because the first read happened milliseconds
earlier in the same pagination session.

Exactness is neither achievable nor wanted: `Collection::search` is an **async
iterable**, so each `.next()` hands up whatever the segment it just read contained
and the layer above decides how many rows to render. Filters make an exact row count
meaningless anyway. What the sparse index adds is the *unfiltered* pager's two
questions — "jump to page k" and "of M" — answered by one descent instead of a walk,
via element counts stored leaf and subtree.

> **Revised 2026-07-30.** The first draft made the declared size exact and promised
> literally one IOp per rendered page. Exact sizing forces a cascading write on
> middle insertions (an element must displace into the neighbour), and the promise
> was false the moment a filter entered the query. N…2N with an async-iterable read
> path is both cheaper to write and honest about what a UI actually asks for.

## Motivation

The read an application performs is not "get a record" — it is "fill this table".
Every layer of a conventional stack turns that into P random reads plus an index
descent, and RFCs 0050 and 0051 exist to collapse the P.

Letting the developer choose the segment's element count makes the collapse land
where it is useful rather than where a byte heuristic happens to put it. A table
showing 50 rows, backed by 50…100-record segments, is one read per page.

RFC 0049 is what makes it safe to fix a count instead of a byte size: a page spans
however many blocks its content needs, with no ceiling, and is read with a single
positioned read of exactly that run. Fifty fat records are still one seek — the
transfer is longer, the IOp count does not depend on record size. Fixing a byte
target instead (RFC 0050's default) makes the element count vary with record size,
which is what a paginated view cannot use.

## Design

### The declaration is a minimum

```rust
#[wavedb(NonUnique, page = 50)]              // the built-in chain
struct Contact {
    #[wavedb::list(page = 25)]              // this ordering's own chain
    name: String,
    city: String,
}
```

Unset, it falls back to a **default element count**, not to a byte target:

| lane | default N | why |
| --- | --- | --- |
| `SEG(T)` — record chains | **16** | a segment holds 16…32 whole records |
| `DEAD(T)` — the removal log | **256** | entries are `[instant][anchor]`, ~18 bytes, and the only read is a sequential tail scan |

An earlier draft said an undeclared capacity falls back to RFC 0050's byte target.
That contradicts this RFC's own argument: with a byte target the element count
varies with record size, and both the split (2N) and merge (N/2) rules are stated
in elements. The count is the primitive; bytes follow from it.

16 is deliberately small, and the reason is the write side rather than the read
side. Because the built-in chain is modification-ordered, **every save rewrites
the growth-end segment** — whole records, re-compressed. At N=16 that is 16…32
records' bytes per save; at N=200 it would be 200…400. A collection that
paginates declares `page = N` and pays the larger rewrite knowingly; a collection
that does not gets the cheap default and still turns a scan into 1 read per 16
records instead of 1 per record.

The cost is a larger sparse index than this RFC's worked example: at N=16 a
million records are ~62 500 segments rather than ~5 000, so the descent is 2–3
nodes rather than 2. Still bounded, still cold, still logarithmic — the property
that matters is unchanged.

**Capacity folds into `STRUCT_HASH`** — like every other declaration that reaches
stored bytes (see [the rule](#everything-that-reaches-stored-bytes-folds) below).

An earlier draft said it does not, on the grounds that it "changes neither the
wire shape nor any address derivation". Both halves are true and neither is the
point. What changing `page` on a live collection actually does is **silently
falsify the guarantee the knob exists to provide**:

Take `page = 16` → `page = 50` with the identity unchanged. Existing segments hold
16…32 entries. The new N puts the merge trigger at 25 and the split at 100, so
every existing segment below 25 is now underfull and gets folded into a neighbour
on the next removal that touches it, while those at 25…32 sit in limbo — above the
merge, nowhere near the split. Nothing corrupts. But throughout that period the
chain's segments hold 16…32 records, not 50…100, so "a rendered page is one
segment read" is **false** — and it stays false until enough writes churn through
to re-level the chain. A guarantee that is temporarily and invisibly wrong is
worse than one that is absent.

With the fold, `page = 50` is a different type: different lane hashes (they derive
from `STRUCT_HASH`), a fresh empty chain, and the guarantee holding from the first
record. A mixed-capacity chain stops being a state to manage and becomes
unreachable by construction.

### Everything that reaches stored bytes folds

The general rule, and it is a project invariant rather than this RFC's local
decision: **a declaration that reaches stored bytes or their layout folds into
`STRUCT_HASH`.** `page = N`, `compress`, `#[wavedb::key]`, `#[wavedb::list]`, and
whatever knob comes next.

The reasoning is the migration stance ([RFC 0040](0040-schema-migration-and-version-skew-DEPRECATED.md)):
WaveDB does no engine-side migration at all. Given that, the only coherent
position is that changing anything about how bytes are laid out yields a **new
type** — the developer declares a second struct and moves the data across as
application code, exactly as for a changed field. The alternative is an engine
that must reason, per knob, about whether existing data survives the change, and
that reasoning is where the silent-falsehood class of bug lives (the `page`
example above is one instance of it, not the only possible one).

`compress` is the mildest case — zstd framing is per-page and self-describing, so
mixing compressed and uncompressed pages genuinely works — and it folds anyway.
Uniformity is the feature: there is no per-knob judgment call to get wrong, and no
doc that can drift from the code about which knobs are "safe".

### Why N…2N and not exactly N

A fixed exact size is the expensive choice, not the precise one. Holding a segment at
exactly N means an insertion into a full one must displace an element into its
neighbour — which may itself be full, so a single insert writes two segments in the
lucky case and cascades in the unlucky one. And middle insertions are not an edge
case: every declared list (RFC 0051) is keyed by a domain value, so arrivals land
wherever the value falls.

With a band, an insert is **one segment write** anywhere in N…2N, and only the 2N-th
pays a second one to split. The amortised cost is one extra write per N inserts, and
no insert ever cascades.

The same rule serves the appending case, which is RFC 0050's built-in chain: its key
is the authoring instant and instants only increase, so arrivals concentrate at the
tail, and a tail reaching 2N splits by sealing its older half.

### Split 50/50, merge at N/2, and why the band is really N/2…2N

Splitting halves at 2N: the endpoint keeps its own half **and its own id**, the
other half is minted a fresh id and linked in as an interior segment. On the
built-in chain, whose arrivals all land at the tail, that means the tail keeps
the newest N and a sealed segment of N appears behind it. Only the tail is ever
in the upper band; sealed segments start at exactly N.

Symmetrically, a segment falling to **N/2 merges** with a neighbour — and the
merge is what makes 50/50 the right split rather than a lossy one:

> **Correction (2026-07-30).** An earlier draft of this section argued for sealing
> asymmetrically (keep `k` at the growth end, seal `2N−k`) on the grounds that a
> 50/50 seal freezes interior segments at N forever, the bottom of the band. That
> reasoning holds only for an append-only chain, and the built-in chain is not one:
> a save **relocates** its record to the growth end, so interior segments *drain*.
> Sealing high buys density that evaporates at the same rate. What actually keeps
> the chain dense is the merge rule, which the RFC previously lacked entirely.

So occupancy lives in `[N/2, 2N]`, with a hysteresis factor of 4 between the two
triggers — a segment must lose half its entries or double them to be touched
again, which is what stops a chain from thrashing on a workload that hovers at a
boundary. In practice the mass sits at N…1.5N.

Two rules the merge needs, neither of which is a free choice:

- **Combined > 2N ⇒ redistribute, don't merge.** Neighbours at N/2 and 2N sum to
  2.5N; merging would breach the band and the next insert would split it straight
  back. Redistributing evenly leaves both at ≥ N.
- **If an endpoint takes part, it is the survivor.** Otherwise the left segment
  survives. Without this the merge can delete a head or tail and undo the
  endpoint permanence that keeps the `Pivot` from being rewritten (RFC 0050).

Note what the split actually costs on the wire: **three segment writes, not two.**
The two halves, plus the neighbour on the far side of the new segment, whose
`next` (or `prev`) must now name it. Only a split at a chain end with no outer
neighbour costs two. A merge costs the same three. Amortised that is one extra
segment write per N inserts, inside the one window a batch already writes
(RFC 0041) — bytes, not IOps — but the earlier text said "a second segment write"
and undercounted.

> **Reviewed and accepted 2026-07-31 (user).** The third write is the neighbour's
> re-link, and it is the price of a doubly-linked list: a new segment between two
> others has to be named by the one before it. Left as is — the alternative
> (singly-linked, or a level of indirection between segments) costs the backward
> walk that `all()` and catch-up both depend on. Revisit if the write side ever
> becomes the binding constraint.

### What that costs a reader, and why it is nearly free

A segment holding 1.5N records does not line up with an N-row page. The second page
of that chain takes the last 0.5N records of the first segment plus 0.5N from the
next — two reads instead of one.

But the first segment was read **one tick earlier**, by the same iterator, for the
previous page. It is in the cache with near-certainty. This is temporal locality
inside a single query, not the residency assumption RFC 0053 forbids: the honest
figure is "one read plus one very likely hit; two reads worst case".

Generally, with occupancy in `[N, 2N]`, a window of P ≤ N consecutive records spans
at most **two** segments — the band is exactly what bounds it at two, since a
segment always holds at least a full page.

### The read path does not count rows

`Collection::search` and friends return an async `Stream`. The chain implementation
should therefore:

1. descend the sparse index once to the first segment the bound admits;
2. yield the records that segment holds, all of them, without regard to any page
   size;
3. follow `next` when the consumer asks for more, and stop as soon as the bound
   fails.

The quantity a caller wants belongs to the caller — the user-side consumer, or a
`#[server]` function shaping a reply. Two consequences worth stating: the storage
layer never reads a segment the consumer did not ask for, and a filtered query
(`search_by`, or any predicate the caller applies) needs no special handling, since
"how many rows survive" was never the chain's business.

This is also why exact sizing was never worth its cost. A filter makes the number of
rows per segment unpredictable, so no layout can guarantee one segment per rendered
page in the general case.

### "Page k" must not be a walk

A chain has no random access: reaching the k-th segment by following `next` is k
reads. That would make page 100 cost 100 IOps.

The fix makes the sparse index of RFC 0051 an **order-statistic tree**. A leaf entry
gains the element count of the segment it names, and an internal entry gains the sum
of its subtree's counts:

```text
leaf entry:      (min sort key, segment id, count)
internal entry:  (separator, child id, subtree_count)
```

Locating global offset *o* is then a single descent — at each level, pick the child
whose running sum first exceeds *o* — costing at most the index's depth, about two
reads for a million records, with nothing assumed cached. Offset pagination costs
what keyset pagination costs.

Subtree sums are why this is a descent rather than a prefix sum. Summing leaf counts
would have to read every entry before *k*: fine if the index were guaranteed
resident, and it is not, so it would be up to every leaf of the index on a cold read.

Maintenance: an insert, removal or split rewrites the leaf entry **and the counts
along its path to the root** — about two extra nodes, in the same batch, hence the
same window and the same barrier.

### The contract, stated cold

For a chain with minimum N and a UI page P ≤ N:

- **Render a page**: 1 segment read, or 2 when the window straddles a boundary,
  where the first is almost certainly a hit. Rows come decoded from the segment; no
  per-row fetch, because RFC 0050 stores records inline. Without inlining this would
  be 1 index read + P record reads — the inline decision is what the contract rests
  on. "One read" is one **seek**: the bytes transferred and decompressed are the
  whole page the segment sits in, so the segment lane's bucket target should be
  sized near one segment (RFC 0050) or the amplification is
  `bucket_target / segment_size`.
- **Jump to page k**: ≤ 2 reads for the descent, independent of k.
- **Next / previous page**: 1 read, often 0.
- **Total count for the pager**: the root's subtree sum — 1 read cold, 0 warm, and
  exact **for an unfiltered listing**. A filtered total is a scan, here as anywhere.

## Alternatives

- **Exactly N per segment** (the first draft). Precise-looking and more expensive:
  cascading displacement on middle insertions, and the precision evaporates under a
  filter.
- **Engine-chosen capacity** (RFC 0050's byte target alone). Simpler and correct when
  nobody paginates, but the element count then varies with record size, so a view can
  never assume a bounded number of segments per page.
- **A separate pagination index** (offset → segment) instead of counts on the sparse
  entries. Same effect, one more structure to keep consistent; the count is 8 bytes
  on an entry already being rewritten.
- **Cap the element count *and* the byte size**, splitting on whichever hits first.
  Keeps segments from becoming huge with fat records, at the cost of breaking the
  bound exactly when records are large — the case where it is worth the most.
  Rejected: RFC 0049 removed the reason to fear a large page.
- **Offset pagination by walking `next`.** Free to build, O(k) reads. Acceptable only
  for infinite scroll that never jumps.
- **One index entry per distinct *value* instead of per segment**, to compress the
  long homogeneous runs a low-cardinality ordering produces. Considered and
  **rejected**, for two reasons that only became clear once the sort key was fixed
  as `(value, anchor)`:
  1. *It buys no correctness.* The trailing anchor makes every separator distinct
     even inside a run of one repeated value, so the ordinary "last separator ≤ k"
     descent already lands exactly, and an insertion into a run needs no search for
     where the value changes.
  2. *It buys almost no size.* The index's entry count depends on the **segment
     count**, never on cardinality: a million records at N=16 is ~62 500 entries
     whether the column holds three distinct values or a million. What run
     compression would remove is byte redundancy between neighbouring separators,
     which the per-type zstd dictionary already models.

  And it would cost the thing this RFC exists for: per-value entries break the
  offset descent, since locating an offset *inside* a run would become a walk.

## Open questions

- **Does `page` belong on the type or on the query?** A schema-level declaration
  serves one UI; two views of the same collection with different page sizes get one
  aligned and one not. A per-view override cannot change the layout, so the answer is
  probably "declare the largest, let smaller views subdivide it" — worth confirming
  against a real application before committing the syntax.
- ~~**Split point.**~~ **Resolved (2026-07-30): 50/50, endpoint keeps its half and
  its id, merge at N/2.** See [the section above](#split-5050-merge-at-n2-and-why-the-band-is-really-n22n)
  — the asymmetric alternative was arguing about a density that a draining chain
  does not preserve anyway.
- **Do counts need to survive a crash independently?** They are derived from the
  segments they name, so replay rebuilds them; but if an index entry and its segment
  could ever disagree, a pager would be silently wrong. The single-batch rule should
  make it impossible — worth an explicit test rather than an argument.

  **Partly answered 2026-07-31.** The *agreement* half is now pinned at both
  levels: the bare chain's `check()` walks every segment asserting the index
  names it with the right id and the right count, and
  `collection_lists::tests::counts_agree` does the same where the batch is
  **composed** — the collection interleaves the record write, the built-in
  chain, every declared list and the `Pivot` into one `apply`, which is traffic
  the bare-chain tests never see. Both were mutation-checked: filing `len − 1`,
  and skipping the upsert when the separator is unchanged, each fail the suite.

  **Answered in full 2026-07-31.** The crash half is now
  `wavedb-storage/tests/counts_survive_recovery.rs`: a collection with a
  declared list is grown past several splits and drained to `data.bin`, then
  driven through saves, removals and inserts that are deliberately **not**
  drained, and the store is dropped — so recovery crosses both paths, settled
  pages and replayed journal frames, inside one chain.

  The check runs through the **public** surface rather than the engine's
  internals, which turned out to be the better assertion: `list_len` is the
  index's answer (the root's subtree sum) and counting what `listed` yields is
  the segments' answer, so comparing them is the user-visible form of "an entry
  and its segment disagree". Three mutations to recovery are caught — replaying
  only the first frame, tearing a batch's tail, and tearing it mid-way. The last
  is the one that matters: it fails on the count comparison itself, reporting
  *"the index reports 24 rows but the segments hold 27"*, which is precisely the
  silently-wrong pager this bullet was worried about.

  A more valuable hole turned up while mutating for the above, and is worth
  recording because it is the class of bug this whole family of RFCs creates: a
  save that leaves the ordering property alone must **still** rewrite the record
  in every declared list, because the bytes are duplicated there and only the
  *position* is unchanged. Skipping the list when `old_key == new_key` reads
  like an obvious optimisation, produces perfectly consistent counts, and serves
  stale bodies. Nothing in the suite caught it;
  `a_save_refreshes_a_list_whose_order_it_did_not_change` now does. A count
  check cannot see this failure — a stale body has exactly the right length —
  which is the argument for checking derived **bytes** against their anchor
  wherever a structure duplicates records.

  And the shortcut is not merely unsafe, it is **unavailable**: deciding it
  needs "did anything *else* about this record change?", whose only authority is
  the anchor's previous version. Reading that costs an IOp to avoid a write
  already inside the window the batch pays for — the scarce resource spent to
  save the abundant one. So an unconditional rewrite of every list on every save
  is the rule, not a simplification awaiting an optimisation.
- **Compaction and stable pagination.** Re-levelling moves boundaries, so a cursor
  held across a compaction may skip or repeat rows. Keyset cursors are immune (they
  name a sort key); offset cursors are not. Does compaction defer while a watch holds
  an offset cursor, or is "offset pagination is not stable under concurrent writes"
  the documented answer, as it is everywhere else?
