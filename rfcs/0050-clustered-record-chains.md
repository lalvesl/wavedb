# RFC 0050 — Clustered record chains (B+trees become opt-in)

- **Status:** Implemented 2026-07-31 — opened 2026-07-29, revised 2026-07-30,
  implementation started 2026-07-30. Phases 1–7b landed; phase 8 dissolved (see
  the progress table). Remote list reads still refuse over the client
  transport, alongside `search_by`.
- **Crates:** `wavedb-core` (the index and collection layers), `wavedb-macros`
- **Code (today):** `crates/wavedb-core/src/index/{tree,tree_insert,tree_delete,stream}.rs`,
  `collection.rs`, `collection_write.rs`, `collection_keyed.rs`, `collection_recency.rs`
- **Builds on:** [RFC 0049](0049-elastic-pages-and-load-driven-splits.md) (a page is
  however many blocks its content needs, with no ceiling)
- **Completed by:** [RFC 0051](0051-ordered-record-lists.md) (the sparse
  index and further lists), [RFC 0052](0052-segment-size-as-the-pagination-unit-DEPRECATED.md)
  (segment sizing), [RFC 0053](0053-tenant-fair-cache-retention-PLANNED.md) (why
  every cost here is quoted cold)

> **Superseded in its default 2026-07-31 by
> [RFC 0054](0054-no-duplication-by-default.md).** Everything below about the
> chain — the segments, the sparse index, the size band, the permanent
> endpoints, the single-batch rule — stands and is what runs. What changed is
> **what rides in a segment entry**: the built-in chain holds *pointers*, not
> records, so there is no second copy by default. Read every "records inline"
> here as describing a `#[wavedb::list]` (RFC 0051), which is now the only
> declaration that asks for duplication. The reason is in 0054: "not anchored" is
> not a state a record can be in, so the inline copy was an *extra*, and an extra
> belongs where a read actually wants it.

## Summary

A collection's living records are **additionally** stored inline inside a chain of
segments, ordered by the live version's **authoring instant** —
`Metadata.succession`'s `CreatedAt`, rewritten on every save (`metadata.rs:26`), so
the chain is in modification order with the most recently changed record at its
newest end. Each segment is a stored value under its own `Id`, holding between N and
2N records plus the ids of its neighbours; above the chain sits the sparse index of
RFC 0051, one entry per segment.

Because that is exactly the key `recency` uses, **the `recency` log disappears**:
the chain is the modification log, with the records inline instead of references.

The record at its **anchor stays where it is and stays authoritative**: a computed
address, one IOp, the version chain, the `Expect` guard. The chain is a derived,
read-optimised duplicate — byte-for-byte the same record envelope.

What the chain replaces is the `current` B+tree: it is the enumeration, the
membership set, and (through its sparse index) the range descent, so that tree
disappears and liveness moves into the anchor's `Metadata`. `dead` becomes a chain of
the same shape with references instead of records. A dense B+tree exists only where
the developer declares one (`#[wavedb::pivot(field)]`, unchanged syntax, unchanged
meaning).

The trade: **a scan costs one read per segment instead of one page read and one
zstd decompression per record; every point lookup, history walk and conflict guard
is untouched; the price is storage and the bytes of a duplicated write.**

> **Revised 2026-07-29.** The first draft *moved* records into the chain, costing
> the computed address for live lookups and forcing two compensations: an implicit
> anchor→segment index for keyed types, and a `Store`-level answer for
> segment-granular `Expect`. Keeping the anchor copy deletes both.
>
> **Revised 2026-07-30 (a).** The chain was append-only and unordered, which needed
> a back-pointer in `Metadata` and gave up the range descent. Ordering it by a key
> the record already carries removes the back-pointer (position is derived) and
> restores the range descent. Splits became legal everywhere, so the asymmetry the
> previous revision had to explain is gone.
>
> **Revised 2026-07-30 (b).** The ordering key moved from the *anchor* to the live
> version's authoring instant, which **absorbs the `recency` log entirely** — one
> structure, one lane and one write per mutation fewer. Its two consumers (catch-up
> and the instant floor) are served better by the chain than by the tree they used.
> The `Pivot` now also holds each chain's **head and tail**, and both ids are
> permanent, so a growing chain never rewrites the `Pivot`.

## Implementation progress

Phased so the suite stays green at every step; each phase is additive until phase 4,
which is the one that changes the on-disk layout.

| # | Phase | State |
| --- | --- | --- |
| 1 | `index/segment.rs` — the segment value: lane identity, byte form, in-place edits | **landed 2026-07-30** |
| 2 | `index/sparse.rs` — the sparse-index node: counts, key descent, offset descent | **landed 2026-07-30** |
| 3a | `index/sparse_write.rs` — the index's write half: upsert, remove, node splits, count propagation | **landed 2026-07-30** |
| 3b | `index/chain.rs` + `index/chain_remove.rs` — locate / insert / remove over a `Store`, split and merge policy, separator upkeep | **landed 2026-07-30** |
| 4 | `Metadata` gains liveness (`removed`), stamped by `remove` and cleared only by a revival | **landed 2026-07-30** |
| 5 | `Pivot` roots become `(head, tail, index)`, **and** the collection write paths maintain the chain — as a *dual write* beside the trees, see below | **landed 2026-07-30** |
| 5b | **Catch-up rewritten against the chain** — `expose_changes.rs` read the `recency` tree, which phase 5c deletes | **landed 2026-07-30** |
| 5c | Retire `current`, `recency` and the `dead` tree: delete them, their roots, the dual write, and `Collection::search` | **landed 2026-07-31** |
| 6 | Collection read paths: `all` (and the wire `All`) walk the chain | **landed 2026-07-30** — `search`/`search_by` still read trees, see below |
| 7a | Macros: `page = N` — parsed, folded into the identity, threaded to the chain | **landed 2026-07-31** |
| 7b | Macros: `#[wavedb::list]` — declared lists ([RFC 0051](0051-ordered-record-lists.md) in full) | **landed 2026-07-31** |
| 8a | Sparse-index merge — the half phase 3a did not ship | **accepted debt 2026-07-31**, see below |
| 8b | Removal-log retention | **out of scope 2026-07-31** — separate planning |

Phase 3 was one row until the design was worked through: the index's write half is
a B+tree write path in its own right (splits, count propagation up the path, root
growth) and the chain sits *on top* of it, so they are a dependency pair rather
than one unit of work.

Phase **5b** was missing from this table entirely, which was the plan's worst
omission: the W6/W7 catch-up is Implemented, has e2e coverage, and was written
directly against the structure 5c deletes.

Phase 4 lost the `Pivot` half to phase 5, for a reason worth writing down: the
`Pivot` shape is **not separable** from the write paths. Changing it additively —
new roots beside the old — means teaching the trait three methods that seven
hand-written test fixtures must then implement with values nothing reads, only to
delete them again a phase later. Changing it destructively breaks
`collection_write`, `collection_keyed` and `collection_adopt` the same instant. So
the shape and the paths that use it land together, and what phase 4 keeps is the
part that is genuinely independent: liveness on the record.

**Phase 1** landed `Lane` (the derived per-type lane hash), `mint_segment_id`, and
`Segment<P>` with a hand-written `WaveWire` impl — hand-written because a derive would
need an owned wire-shape twin, and copying a segment's entries to encode them would
double the bytes a write already moves. `P` is the payload: record bytes in a record
lane, `()` in the removal log, which the tests exercise both of. Splitting is
deliberately a pure container operation (`split_off` moves entries and nothing else),
leaving re-linking and id minting to phase 3 — that is what lets a split always hand
the *new* id to the interior side and keep a head's or tail's id permanent.

Proven by mutation: dropping the lane tag from the hash derivation fails three tests
(`lanes_and_types_never_share_a_hash`, `a_foreign_lane_tag_is_refused`,
`segment_ids_are_minted_apart_and_never_repeat`); appending instead of placing in key
order fails `inserts_land_in_key_order_whatever_the_arrival_order`; skipping the tag
check on decode fails `a_foreign_lane_tag_is_refused`.

**Phase 2** landed the sparse-index node as its **own structure**, `index/sparse.rs`,
not a count retrofitted onto `BpTree`. Three findings forced that, and they are worth
keeping because they are the reasons a future reader should not "simplify" the two
back together:

1. **A dense tree would pay for it.** Eight bytes per entry buys a dense secondary
   nothing and shrinks its leaf capacity — `DEFAULT_LEAF_CAP` is sized so a node fits
   a 32 KiB page, so counts would mean more nodes and more reads for trees that never
   consult them.
2. **`BpTree` has no update path for a mutable payload.** `plan_insert` returns
   `Ok(Vec::new())` when the key is already present (`tree_insert.rs:63`), so revising
   a count through it would be a *silent no-op* — the worst available failure mode.
3. **`NodeBody::Internal` has nowhere to put the leftmost child's count.** It holds
   `leftmost` plus the separators *between* children, so the first subtree owns no
   entry. `sparse::Branch` gives every child its own least key, which is exactly what
   lets every child own a count.

The node carries `Slot { key, count }` in leaves and `Branch { first, node, count }`
in internals, with `step_to_key` / `step_to_offset` returning one level of descent.
A `Slot` needs no segment field: `SecKey.rec` already *is* the segment id. Index nodes
take `Lane::Index`, their own lane, because they are navigational where segments are
streaming ([RFC 0053](0053-tenant-fair-cache-retention-PLANNED.md)) — which lets the
cache and the bucket target treat them differently. `Error::SegmentBadTag` from phase
1 generalised to `Error::LaneBadTag`, since both kinds are now lane-tagged values.

The tree's **write** half (insert, split, count maintenance up the path) deliberately
waits for phase 3: the chain is what triggers it, and the two must land in one atomic
batch, so writing them apart would mean writing the seam twice.

Proven by mutation: an off-by-one in the offset descent (`<=` for `<`) fails four
tests; making the key descent take the first entry at-or-above instead of the last
at-or-below fails `a_key_descent_takes_an_exact_match_over_its_predecessor`; matching
empty entries instead of skipping them fails
`an_offset_descent_skips_empty_entries` (a pager landing on an empty segment would
render a blank page); making `total` count entries instead of elements fails
`the_total_is_the_sum_of_the_counts`.

**Phase 3a** landed `SparseTree`, and building it turned up a **defect in phase 2's
`Slot`**, which stored only a `SecKey` and reused its trailing `rec` as the segment
id. `SecKey` orders by `field` then `rec`, so whenever a search key and a separator
share a `field` the comparison falls through to that trailing pointer — and a minted
segment id bears no relation to a record anchor, so the descent lands on the wrong
side of the boundary about half the time. It is reachable on an exact hit in the
built-in chain and **routine** in a declared list over a low-cardinality column,
where a whole run of segments shares one value. `Slot` now mirrors `Branch` exactly
— least key, pointer, count. None of phase 2's tests could see it; the regression
that guards it is `a_run_of_segments_sharing_one_value_still_descends_exactly`.

One design decision the phase added: **the index root id is permanent too.** A root
that overflows keeps its id — its contents move into a freshly minted child and the
root becomes the internal node above the two halves. One extra node write on a level
growth buys the same `Pivot` permanence the chain's endpoints give, and it is why
every `SparseTree` method takes `&self`.

**Phase 3b** landed `Chain` (locate, insert, split) and `chain_remove` (remove,
merge, redistribute), keeping the segment writes and the index writes in one batch
through an `Overlay` so later steps of a plan read what earlier ones wrote.

Two things the tests forced out that the design text had wrong or missing:

1. **A drained index could not be written to again.** When the last child of an
   internal root is dropped, the root becomes an `Internal` naming nobody, and a
   write descent steps through no child and faults. *Reads* stay fine — which is
   exactly why a suite that only read after draining missed it. An emptied root
   now returns to the empty leaf it started as.
2. **The endpoint-survives-a-merge rule had no test.** Forcing the merge to always
   keep the left side left every test green, because the code still moved the
   endpoint pointer and stayed *correct* — just at the cost of a `Pivot` rewrite
   per merge, which is the whole thing the discipline buys.
   `a_merge_never_deletes_an_endpoint` pins it.

Proven by mutation: giving the split's new id to the endpoint instead of the
interior fails `a_tail_split_keeps_the_tail_and_moves_the_head_exactly_once`;
skipping the outer neighbour's relink fails five; never dropping a stale separator
fails five; leaking the absorbed segment fails four; and in the index, counting
entries instead of elements, unlinking a node without deleting it, and never
splitting each fail their own guard.

**Phase 4** put liveness on the record: `Metadata.removed`, stamped by `remove` on
the anchor it already holds, carried forward by an ordinary save, and cleared only
by the path that re-indexes the anchor into the living set (`SavePlan::revives` —
a `#[wavedb::key]` upsert at a dead anchor, or a mirror adopting that revival). The
`current` tree still answers liveness until phase 5; this makes the record itself
agree ahead of that tree going away.

It is a **flag, not the removal instant**, and a test is what settled that. The
first cut stored `removed_at: Option<u64>` on the grounds that the instant costs
nothing extra while a record lives. It broke `adopting_a_revival_chains_the_mirrors_dead_copy`:
removal instants are minted per store, so a node and a mirror that both removed the
same record hold anchors differing in those eight bytes — and every later archive of
that version differs too, which is exactly the byte-identity the mirror path
promises. A flag converges. *When* it died is the `dead` log's business, keyed by
that instant, which is what a catch-up reads anyway.

Proven by mutation: not stamping the flag on removal fails the removal test and the
keyed-revival test; clearing it on every save fails
`a_removal_is_stamped_on_the_anchor_and_a_save_does_not_undo_it` — the case where
the record's own metadata would claim life the walk does not grant.

**Phase 5** landed as a **dual write** rather than a switchover, which turned out
to be the cheaper path *and* the better-tested one. The `Pivot` gained
`records: ChainRoots` and `removals: LogRoots` beside `current`/`dead`/`recency`;
`create` builds the chain and the log; and `insert`, `save`, `remove` and the
keyed revival maintain them in the same atomic batch as the trees.

The earlier plan avoided this on the grounds that teaching seven hand-written test
fixtures three new methods was throwaway work. That was wrong in the direction
that mattered: the chain roots are the **end state**, so the fixtures keep them —
what gets deleted later is the tree roots they already had. And running both at
once buys the strongest check available, which a switchover could not have:
`the_record_chain_tracks_the_trees_it_is_replacing` asserts, after every step of a
mixed workload, that the chain holds exactly the recency tree's entries, the log
holds exactly the dead tree's, and every inline copy is **byte-identical** to the
anchor it derives from. Removing the chain insert, or the save's relocation, fails
it.

Two things the phase forced that the design text had not:

1. **Every lane needs a registered `StructStorage` slot.** The native engine routes
   by `STRUCT_HASH` and refuses an unregistered one, so the first end-to-end run
   died with `no StructStorage registered`. `storage_entries()` now carries five
   slots for a NonUnique type — record, pivot, and the three lanes. The RFC named
   this cost in passing; it is a hard requirement, not a footnote.
2. **The lane hash is derived twice.** A `static` needs a `const` initialiser and
   SeaHash is not a `const fn`, so `wavedb-macros` computes each lane's hash at
   expansion time while `Lane::hash` computes it at runtime. Two implementations
   of one identity, and a drift between them would write a chain into a directory
   nothing reads — silently. `lane_hashes_match_the_engines` pins them together
   against a real generated type.

Two file splits came with it, both along the existing layer seam:
`collection_read.rs` (the reading half — where phase 6 lands) and, in phase 4,
`collection_remove.rs`.

**Phase 5b** moved reconnect catch-up onto the chain — the migration this plan had
originally left out of the table, and the riskiest one left, so it was taken while
the dual write still made every answer checkable against the trees it replaces.

The surface turned out to be two lines. `collection_changes` scanned the `recency`
and `dead` trees for keys past the cursor, then **fetched each changed record by
address**; it now walks the record chain and the removal log back from their tails.
The records come inline, so the per-change fetch is gone entirely: catching up
costs segment reads, not record reads. `net/sync.rs` and the HTTP piggyback path
needed no change at all — both ride on top of `Command::Changes` and never touched
a tree.

The scan stops at the first segment whose least key has reached the cursor, which
is what makes the common case cheap rather than merely cheaper: a **caught-up**
client pays exactly three reads for "nothing new" — the pivot, the record chain's
tail segment, the removal log's tail segment — regardless of how large the
collection is. `a_collection_catch_up_reads_segments_not_records` pins both halves
of the claim through a `Store` that records every address it is asked for: no
record anchor is ever among them, and 40 changes arrive in fewer reads than there
are records. Proven by mutation: never stepping past the tail segment loses the
older half of the answer; making the cursor inclusive (`>=` for `>`) replays a
change the client already has.

**Three follow-ups landed with it**, all of them "the chain already answers this,
stop asking the tree":

**Liveness reads off the anchor.** Three probes — the keyed upsert and both
`adopt` paths — descended `current` to ask "does this record live?", and each
then fetched the record anyway to tell a *vacant* anchor from a *dead* one. One
`Collection::anchor` read now answers all three cases and hands back the bytes,
so `adopt` on an unchanged record went from a tree descent plus two fetches to a
single read. The swap is only safe if `Metadata.removed` and `current`
membership never disagree, so the dual-write agreement check now asserts exactly
that, in both directions, after every step of its mixed workload.

**The instant floor is two endpoint reads.** `instant_floor` asked the `recency`
and `dead` trees for their maxima — two descents to a rightmost leaf, taken on
*every* mint. It now reads each chain's tail segment and takes its last key.
Both halves are load-bearing and the test says so: a removed record leaves the
record chain, so once a collection has been emptied its history survives only in
the removal log — dropping either half fails
`the_floor_is_the_greater_of_the_two_chain_tails`, and dropping the record half
also fails `imposed_future_instants_floor_local_minting`. That gap was real
before the test: the removal-log half had no coverage at all.

**The salt guard sees the lanes.** The registry's 15-bit collision guard
compared declared record hashes pairwise — but a NonUnique type reserves three
lane hashes as well, each with a `type_salt` of its own, so a registry of `n`
such types puts `4n` occupants in a 32768-slot space and the guard was checking
one in four. That is not a rounding error: it is the guard for exactly the
property the lanes exist to give ("a segment id can never equal a record anchor,
an archive slot, or a tree node"), and at 80 occupants a birthday collision is
already a ~9% event. `WaveDbStruct::LANE_HASHES` now carries the lane list (the
macro emits literals — SeaHash is not a `const fn`, the same reason the
`StructStorage` slots do), and `expose_salt.rs` compares whole occupant sets,
plus a per-entry self-check since a type can clash with its own lane alone. The
list is a *third* derivation of one identity, so it is pinned against
`Lane::hash` beside the storage slots.

**Phase 6** moved the enumeration onto the chain. `Collection::all` and the wire
`All` command both walk it back from the tail, and the records come **inline**:
no `get_of` per record, so a listing costs one read per segment where it used to
cost one page read *and* one dictionary decompression per record. The wire path
gains more than the typed one — it needs each record's `Metadata`, which now
falls out of the same decode instead of a second fetch.

Two things worth recording.

**The order changed, and it changed in two places at once.** `all()` used to
stream in insertion order; it now streams most-recently-written first, which the
"What this gives up" section below has always named as the price. What the plan
did *not* anticipate is that the wire `All` never went through `Collection::all`
— it walked the `current` tree itself, for the metadata. Changing only the typed
surface left the two disagreeing about what "all" means, and the client e2e caught
it. Both walk the chain now. Nine assertions and four doc comments across the
workspace moved with them.

**A save must not resurrect a dead record.** `plan_recency_rekey` had always
guarded this — it returns early when the old entry is absent, so "a record outside
the living set must not enter the modification log through a save". The chain's
equivalent did not, and inserted unconditionally: a save aimed at a removed anchor
put the record back in `all()` while its own `Metadata` still read removed. Phase
4's liveness test is what caught it, which is the argument for having landed that
flag before the read paths rather than after.

**Phase 5c** deleted the three B+trees the chains had been shadowing —
`current`, `recency`, and `dead` — along with the dual write and
`Collection::search` (the `CREATED_AT` range; see the answered open question).
The `Pivot` lost three roots and two rewrite constructors, keeping
`secondaries` / `records` / `removals` / `permission` and one
`replace_roots`; the write paths lost every tree plan except the declared
secondary indexes; `remove`'s "was it in the living set?" gate — the last
liveness probe — became the same `Collection::anchor` read the other three
took in 5b's follow-up, which also folded in the record fetch it did next.

Two consequences worth naming. **A `Pivot` rewrite is now rare rather than
routine**: chain endpoints move at most once in a chain's life and index roots
never move, so a collection with no declared secondary index stops rewriting
its `Pivot` entirely after the first split — where `current`/`recency` moved
their roots constantly. And **`search_by` is the only B+tree read left** in a
collection; it stays one deliberately, because a secondary index is *declared*,
so RFC 0051's lists are what replace it, not the built-in chain.

The agreement test that justified the dual write lost its comparand, so it was
rewritten rather than deleted: `the_record_chain_agrees_with_the_anchors_it_
derives_from` keeps the same mixed workload and asserts what still stands —
every inline copy byte-identical to its anchor, and chain membership agreeing
with `Metadata.removed` in both directions.

**Phase 7a** landed `page = N` — the small, independent half of the macro work.
It parses as another `#[wavedb(...)]` argument, folds into the identity as a
synthetic `#page` entry, and reaches the chain through a new
`NonUniqueStruct::PAGE` const that `records_chain` feeds to `Chain::with_min`.

Two things it forced, both worth keeping:

**Zero is refused, not clamped.** The split trigger is `len >= 2N`, so `N = 0`
splits every segment on every insert, including an empty one. Clamping would also
be a lie now that the value folds — the type would not be the one the declaration
names — so the macro errors at the declaration site instead.

**The capacity is not applied at creation.** A mutation deleting the `with_min`
from `create_rooted` left the suite green, and the reason is that the line was
dead: creation only seeds the one empty segment that is both endpoints, and the
handle it builds is read solely for `roots()`. The right response to a surviving
mutation is sometimes to delete the code rather than to test it, and this was one
— `records_chain` applies the capacity on every subsequent open, which is where
splits and merges are actually decided.

Proven by mutation in both directions: dropping the `#page` hash entry makes a
`page = 4` type identical to the default one (which would let one chain hold
segments laid out two ways); dropping `with_min` from `records_chain` puts 20
records into a single default-sized segment instead of the 3+ that `page = 4`
demands.

**Phase 7b** landed declared lists — [RFC 0051](0051-ordered-record-lists.md)
— and the striking thing is how little of it is new mechanism. A list *is* the
phase-3 chain, at the same capacity, in the same lane, with a different key. The
whole write half is three planners over `plan_insert` / `plan_remove`
(`collection_lists.rs`), and the read half is `all`'s segment walk with the
direction flipped.

Three decisions are worth keeping:

**`Pivot::replace_roots` takes a struct, not parameters.** Adding a fourth root
kind to a four-positional-argument signature would have put two slices
(`secondaries`, `lists`) next to each other where a transposition compiles and
silently swaps a collection's indexes for its lists. `index::Roots` makes the
call sites named, and the seven hand-written fixtures churn once instead of once
per future root kind.

**The liveness gate is applied once, not per structure.** `plan_chain_move` now
returns the envelope it wrote, or `None` when the record was not in the built-in
chain — a dead record saved by address, a chainless first version. The lists ride
that answer rather than each re-deriving it, so "a record the built-in chain does
not hold enters no other chain" is one branch instead of K. Returning the
envelope rather than a bool is what makes it free: the lists hold the same bytes,
so the gate and the encode come out of the same call.

**Ordering is part of the identity, and so is the fold order.** A list is
addressed by its index in the `Pivot`'s root array, so reordering two
declarations re-points two live chains at each other's data. Both the
declarations and their order fold as `#list0`, `#list1`, … — reordering yields a
new type, which is the only coherent answer under no-migration.

The **wire commands followed on 2026-08-01** (RFC 0051, "Reaching a list over
the wire"): `Command::Listed` / `Command::ListLen` plus `Reply::Count`, so
`listed` / `listed_page` / `list_len` now resolve against the client transport
too. They did not have to wait for the streaming frames `search_by` is waiting
on, because a list page is bounded by the caller's `limit` — the answer has a
size the caller named, where a range does not.

**Phase 8** was one row called "compaction", and writing the plan out showed the
name was hiding two problems of different natures. Neither is a gap in the
chain's own upkeep: `chain_remove` merges a starved segment with its neighbour
below `N/2` and redistributes when the merge would breach the band, synchronously,
in the same atomic batch as the removal — including the removal half of a save's
relocation. That landed in **phase 3b** and is the mechanism people usually mean
by compaction here.

**8a — the sparse index never merges.** `sparse_write::plan_remove` is
`slots.remove(i)` followed by `climb` (`sparse_write.rs:214`): no underflow check,
no sibling merge, no root collapse. So index nodes drain and stay drained, and a
tree that grew a level never gives it back. This is not a design position, it is
the half of phase 3a that did not get written — the dense `BpTree` has the full
cycle (merge under ¾ capacity, redistribute otherwise, collapse the root:
`tree_delete.rs:1`), and the fix is to mirror it, synchronously in the same batch,
not as a background pass.

**Taken as accepted debt** (user, 2026-07-31), on the size argument: the sparse
index holds **one entry per segment**, not per record. At `N = 256` a million
living records are roughly four thousand entries — a two-level tree. A drained
version of it is still small enough that the wasted descent does not show up
against the IOps the chain itself saves. Cheap to owe, cheap to pay later.

**8b — the removal log grows forever, by construction.** `dead` is a `Chain`, so
it inherits the merge rule, but nothing ever removes an entry from it, so the rule
never fires. That is not a structural-health gap — the structure is healthy and
correctly growing without bound. It is the absence of a **retention policy**, and
the policy question is not "when does a node merge" but "when is it legitimate to
forget a removal", which is really a question about how long a client may be
absent before its catch-up develops a hole, and how it learns that it has one.
**Out of scope for this RFC** (user, 2026-07-31): it gets its own planning, and
answering it here would have meant designing a client-liveness horizon inside a
storage-layout change.

The dense `BpTree` is **not** being retired — it is the right structure for cold or
small collections, and [RFC 0054](0054-no-duplication-by-default.md)
records that as a declared choice so the option survives this work.

## Motivation

### What a scan costs today

`Collection::all` is `search(Bound::All)`: descend the `current` tree, yield `Id`s
in `CREATED_AT` order, and for each one call `store.get_of(STRUCT_HASH, id)`
(`collection.rs:314`). The tree half is cheap — `DEFAULT_LEAF_CAP` is **1819**
entries (`index/tree.rs:40`), so ten thousand records are six leaves.

The fetch half is where the IOps go. Records of a type are routed to a page by
**SeaHash over the `Id`'s 16 bytes**, seeded per database (`directory.rs:12`). So
consecutive ids in tree order land in unrelated buckets: the scan touches the
type's pages in random order. And a read that misses the per-type record cache
"already pulls, decodes and discards a whole page" to serve one record — RFC 0044's
own summary of today's read path. Pages are optionally dictionary-compressed
(`page.rs:11`), so that discarded work includes a zstd decompression.

A cold scan of a collection larger than the record-cache budget therefore costs
**one page read and one page decompression per record**, for a collection that
physically occupies a few dozen pages. The amplification is not in the index — it
is in the *placement*.

The browser makes it starker. `wavedb::cache::IdbStore` is a flat `Id → Vec<u8>`
map and every `get` is an async round trip; ten thousand of them to list a
collection is the difference between usable and not.

### This is a read optimisation, and only a read optimisation

Since the anchor copy stays, an insert still dirties the anchor's hash-designated
bucket exactly as today, *plus* the chain's segment. Bulk inserts still scatter
across as many buckets as they do now.

What does not get worse is the **barrier** count: it is one either way, because RFC
0041 collapses a batch into one contiguous window write. The extra copy is paid in
bytes inside that one window, not in seeks. The write side is unchanged in IOps and
larger in bytes — the stated bargain, not a regression to explain away.

### Why the obvious answer is not enough

A page cache (RFC 0044) keeps the pulled page so the scan's siblings come free. It
helps and should still happen. But it only *caches* an access pattern that stays
random: a collection larger than the budget evicts pages before their siblings are
wanted, and the browser store has no pages to cache at all. Clustering removes the
randomness instead of paying to tolerate it.

## Design

### The segment

One stored value, under its own `Id`, holding whole records:

```text
Segment<P> {
    prev: Option<LocalId>,          // toward smaller keys; None = the head
    next: Option<LocalId>,          // toward larger keys;  None = the tail
    entries: Vec<(SecKey, P)>,      // sorted by key
}
```

**Every chain keys by `SecKey`** — the existing `{ field: Vec<u8>, rec: LocalId }`
(`index/node_key.rs:73`), whose trailing anchor already "makes entries unique when
many records share one field value". The modification-ordered chain puts the instant
in `field`, which is byte-for-byte what `recency` does today
(`collection_recency.rs:31`); a declared list puts the encoded field value there.
So there is one key type across every chain and every sparse index, and no new key
machinery at all.

What varies is the *payload*: record bytes in a `SEG(T)` chain, nothing in the `dead`
log, where the anchor in the key is the whole entry.

Doubly linked: `next` lets a scan run forward from a descent, `prev` lets a
catch-up run backward from the tail and stop early.

**The payload is the anchor's bytes verbatim.** A record's stored envelope
(`[STRUCT_HASH][meta_len][Metadata][body]`) is copied into the segment unchanged,
so there is exactly **one** record format on disk, the duplicate is byte-comparable
to its source (which makes the consistency invariant testable rather than
arguable), and the per-type zstd dictionary compresses both equally well.

A segment is an ordinary `WaveWire` value stored under an `Id`, so the page
directory, RFC 0049's elastic sizing, the dictionaries and the checkpoint all treat
it as another record of another type: the mechanism lives in `wavedb-core` above the
`Store` seam, which is what keeps it portable to IndexedDB unchanged. One tuning
knob in `wavedb-storage` does move — see [the bucket target](#one-storage-change-after-all-the-bucket-target).

### Segments are their own type, so pages stay homogeneous

A segment's `Id` is `key_nanos()` as the key and the type discriminator of a
reserved segment hash as the salt — precisely the device `BpTree` nodes already use
(`index/node.rs:124`):

```rust
LocalId::new(key_nanos(), true, type_salt(SEGMENT_STRUCT_HASH))
```

The salt puts segments in their own lane of the flat keyspace, so a segment id can
never equal a record anchor, an archive slot, or a tree node, whatever the
timestamps do.

Worth stating plainly, because phase 5b's follow-up made it a live question: the
salt is **not** what keeps minted keys from repeating — `key_nanos()` fuses a
process-global atomic counter into the sub-ms digits and `mint_instant` layers a
`LAST` watermark on top, so no key repeats within a process, and nothing in the
engine ever *dispatches* on the salt (decode verifies the full 64-bit head). On
native it is inert: the `PageStore` routes by whole `STRUCT_HASH` into per-type
directories, so identical `Id`s of two types never meet. Its one live role is the
seam the counter cannot cover — IndexedDB's flat keyspace holding ids minted by
*two* processes, since the client cache adopts node-minted metadata verbatim and
files archives at the node's derived slots.

That makes the salt narrow, not cosmetic, and the `SALT` field has reserved
future use (user-directed). So the registry's salt guard stays and covers every
occupant including the lanes (`expose_salt.rs`) — deleting it would be trading a
compile-time-only check for nothing.

The lane hash also routes segments into their own storage directory, which
**preserves the existing invariant that a page holds records of exactly one
`STRUCT_HASH`** (`page.rs:3`) — anchors, log entries and segments never share a
page. That is worth keeping deliberately: homogeneity is what makes a per-type
zstd dictionary work, and mixing kinds in a page would buy nothing, since hash
placement means co-location cannot be arranged anyway.

**Two reserved lanes per user type:**

| lane | holds | entry payload | index | growth |
| --- | --- | --- | --- | --- |
| `SEG(T)` | the modification-ordered chain + every declared list | records, verbatim envelopes | sparse, one per chain | with the live set |
| `DEAD(T)` | the `dead` chain | `[instant][anchor]` | **none** | forever |

They separate on three counts. **Content:** a dictionary is per directory, and fat
record segments trained together with skinny `[instant][anchor]` pairs give a
mediocre model for both. **Lifetime:** the live chains are bounded by the live set;
`dead` is an append-only removal log that is never pruned — bytes are never
destroyed — so it grows without bound, and sharing a directory would let the
unbounded structure drive the bucket splits, diluting the bounded one across an
ever-growing bucket array. **Temperature:** `dead` is written on removal and read
almost never, so mixing would put cold entries in the pages a live read pulls.

Separating also lets the tuning differ: `dead`'s only read is a sequential tail
scan, so it wants larger segments and a larger bucket target than anything else
here.

Within `SEG(T)`, every chain shares one encoding — same key type, same payload type —
so no discriminator is needed: which chain a segment belongs to is decided by the
`Pivot` root that reaches it. Per type rather than one global segment lane (which is
what `BpTree` nodes do today): a segment carries user record bytes, so a per-type
dictionary is worth real compression. The cost of the split is one extra per-type
`StructStorage` static (`no dyn` means every lane needs its own generated slot —
visible in `scripts/registry_size.sh`).

### Why `dead` needs no index

Removed data does not need good read performance — but the reason it can drop the
index is structural, not a concession. Nothing ever *searches* `dead`:

- **"Is this record dead?"** is answered by the liveness field in the anchor's
  `Metadata`, one read, because the record's bytes are never destroyed and
  `Collection::get` still resolves them by address.
- **Revival** (a keyed insert at a dead anchor) does not consume the dead entry —
  the removal stays as a historical event, so there is nothing to look up.
- **`remove`** appends at the tail, which needs only the tail pointer.
- **Catch-up** scans backward from the tail until instants fall below the client's
  cursor. No descent: the walk's length is proportional to how much was removed
  since the cursor, which is exactly the payload the client is about to receive.

So `dead` needs append-at-tail and a bounded backward scan, both pure chain
operations. `recency` keeps its sparse index because it needs the one thing `dead`
does not: **deletion by key.** A save re-keys a record's recency entry from its old
instant to the new one, and a record untouched for a year has its old entry deep in
the chain — one descent with the index, a long walk without it.

### One storage change after all: the bucket target

A page is a bucket holding **whatever hashes into it** — `SlotPage` is an
`Id → bytes` map (`page.rs:89`) and segment ids are minted, so they scatter
uniformly. With the current 32 KiB bucket target
(`DEFAULT_TARGET_BLOCKS_PER_BUCKET = 8`, `page_store.rs:88`), a page holds ~32 KiB
of segments: a 10 KiB segment shares its page with two unrelated neighbours, and
reading it reads and decompresses all three.

So "one read per segment" is one **seek**, with byte and CPU amplification bounded
by `bucket_target / segment_size`. The fix is to let a segment lane carry its own
target, sized near one segment, so a bucket holds about one — average ~1:1 with a
short Poisson tail instead of a fixed 3×. `target_blocks_per_bucket` is already a
field on the store and `Directory` already "carries the per-type policy"
(`directory.rs:8`), so this is a small change — but it **is** a change, and it
corrects this RFC's earlier claim that `wavedb-storage` is untouched.

### Ordered by the modification instant, and split at 2N

The sort key is `(live version's authoring instant, anchor)`: the instant from
`Metadata.succession` (`metadata.rs:26`), the anchor breaking ties and identifying
the record. `mint_instant` is strictly monotone per collection, so ties cannot
actually occur — the anchor is there for uniqueness and because it is the payload
identity anyway.

Ascending by that key, the chain's **head** holds the least recently modified
records and its **tail** the most recent. Both shapes behave the same way, which the
anchor key could not manage: a keyed type's anchor is a content hash, so an
anchor-ordered chain put it in a meaningless order, while modification order is
meaningful for every shape.

Locally minted mutations land at the tail, because every one of them stamps a fresh
instant:

- an **insert** appends at the tail;
- a **save** removes the record from wherever it sat and appends it at the tail —
  one descent to find the old position, two segment writes;
- a **remove** deletes it from its segment and appends to the `dead` chain.

**That is a fast path, not an invariant**, and the exception is not exotic: the
client cache's `adopt` writes the node's `Metadata` *verbatim* (`collection_adopt.rs`),
so its instant never passes through `mint_instant` and the local watermark does not
know it. A client that made a local optimistic write and then receives a catch-up
from the node inserts **into the middle of its own chain**. The chain handles it —
the sparse index is exactly what makes an interior insert affordable — but any code
written on the assumption "appends only" would corrupt a browser cache silently, so
the general path stays the one that is implemented and the tail is only a
comparison away from it.

A segment holds between N and 2N records, **splits at 2N and merges at N/2**
(RFC 0052 has the rules and the reason 50/50 is right). Since arrivals concentrate
at the tail, the tail is the usual splitter, and its split seals the older half
behind it while keeping its own id.

### No back-pointers, anywhere

Every chain is sorted by a key the record already carries, so **a record's position
is derived, never stored**: to find, update or remove a record's copy, descend the
sparse index with its key. Nothing points *at* a record, so nothing breaks when a
split moves it.

This is what the 2026-07-30 revision bought. The previous draft's unordered chain
needed a `Metadata` back-pointer, which in turn forbade splits (a split invalidates
every pointer to the half that moved) and forced the append-only rule.

### The chains, and the `Pivot`

Two roots remain where there were three, and each is a **pair of endpoints** rather
than a single pointer:

| Root | Today | Under this RFC |
| --- | --- | --- |
| `current` | B+tree of living `LocalId`s | record chain keyed by modification instant: `(head, tail)` **+ sparse index** |
| `recency` | `[instant BE][anchor]` tree, one entry per living record | **gone** — absorbed by the chain above |
| `dead` | `[removed_at BE][anchor]` tree | log chain keyed by removal instant: `(head, tail)`, **no index** |

#### Head and tail, both in the `Pivot`

Each chain carries both ends, so picking a direction costs nothing: ascending starts
at `head`, descending at `tail`, and neither needs a descent to find where to begin.
That matters most for the two hottest reads, which both want the newest records
first — the default listing and the catch-up scan. Both become "read the tail id from
the `Pivot`, one segment read, walk `prev`".

**Both ids are permanent.** A split may always assign the *new* id to the interior
side and let the endpoint keep its own: a head split keeps the lower half at the
head, a tail split keeps the upper half at the tail, an interior split is interior
either way. So once a chain has two segments, its `head` and `tail` never change
again — new ids only ever appear in the middle.

The consequence is an IOp saved on the write path: today "the `Pivot` is rewritten
only when a `BpTree` root moves", and a growing tree moves roots often. A growing
chain never moves its endpoints, so **the `Pivot` is written once at creation and
then essentially never** — not on a split, not on an append, not on a rebalance.

Two details complete the invariant. With a single segment, `head == tail`, and the
first split is the one moment an endpoint id changes — after that they are frozen. And
an emptied chain keeps its last segment as an empty shell rather than dropping it, so
the ids survive even a collection that loses every record.

#### Liveness, which the `current` tree used to answer

Today it is membership: living records are in `current`, removed ones in `dead`. It
becomes a field of the anchor's `Metadata`, answered by the same single read that
fetches the record — strictly cheaper than today for the operation that asks most
often. A keyed upsert currently descends the `current` tree to ask `contains` before
every write (`collection_keyed.rs:55`) and would now read the anchor it is about to
write anyway: absent → first version, living → chained save, dead → revival, one
read, no index.

#### What happened to `recency`

`recency` was never a liveness list, whatever its name suggests. It had exactly two
readers, and the modification-ordered chain serves both better:

| reader | with `recency` | with the chain |
| --- | --- | --- |
| **`Changes` catch-up** (`expose_changes.rs`) | descend the recency tree to the cursor instant, walk its tail, then fetch each record by anchor — one random read per change | read `tail` from the `Pivot`, walk `prev` until instants fall below the cursor — the records are *inline*, so no fetch at all — **done, phase 5b** |
| **The instant floor** (`collection_recency.rs`) | `max_key` descent of the recency tree | the tail segment's last key, an O(1) endpoint read — **done, phase 5b** |

So the structure is not dropped for being useless — it is dropped because the chain
*is* it, with the records inline instead of references. The write path loses a whole
re-key: a save used to rewrite the record, remove one recency entry and insert
another; now it moves the record itself, and the log is the movement.

The instant floor becomes `max(live chain's tail instant, dead chain's tail instant)`
— two endpoint reads instead of two tree descents, still strictly monotone against a
rewound clock, which is the guarantee that keeps a cursor a client already advanced
past from ever being authored below.

What no other structure could have served is worth keeping on the record, because it
is the reason this key had to be the modification instant and not the anchor:
**insertion order is not modification order.** A record inserted a year ago and saved
five minutes ago sits deep in an *anchor*-ordered chain — its anchor key is the
year-old insert instant — yet a catch-up from ten minutes ago must return it. Keying
the chain by modification instant is what lets one structure do both jobs; keying it
by the anchor forced a second one.

### The anchor keeps everything that depended on a computed address

- **`Collection::get(store, id)`** resolves by direct address, for live *and*
  removed records — what makes history navigable (`collection.rs:245`).
- **Archives** are written to their derived slots exactly as before: links are
  instants, addresses are computed, no archive is ever repointed.
- **`#[wavedb::key]`** anchors keep being *addresses*: `insert` computes the content
  anchor and goes straight to it. No implicit index, no chain walk.
- **`Write::Expect`** guards the anchor's bytes, so the conflict unit stays one
  record — no false sharing between records sharing a segment, no change to `Store`.
- **Declared `#[wavedb::pivot(field)]` trees** keep their syntax and their current
  value, field bytes → anchor. A hit resolves in one read at the anchor, so an entry
  carries nothing new and `IdStreamExt`'s set algebra is unchanged.

#### The solitary anchor is what lets `dead` stay skinny

The chains hold the **live** set, so a removed record leaves every one of them. Its
bytes then live in exactly one place: its anchor. That is not a convenience — it is
what makes the removal log affordable.

Without the anchor copy, a `remove` would have to either leave the record in a chain
(contradicting what the chain *is*) or copy its bytes into the `dead` log. And `dead`
is the one structure that **grows forever** — bytes are never destroyed, so nothing
prunes it. A fat `dead` log would accumulate every version of every record ever
removed, in a lane no reader wants to page through.

So the anchor lane carries what nothing else can:

- **removed records' bytes**, keeping `Collection::get` honest after a `remove` and
  letting `dead` be `[instant][anchor]` and nothing more;
- **archived versions** at their derived slots, and the `Succession` chain that walks
  them;
- the **`Expect`** target, which is why the conflict unit is one record.

Which reframes the duplication: the anchor is not a copy of the record, it *is* the
record. The chains are derived read paths over it.

### What this gives up

- **Space, and write bytes.** Two copies of every living record instead of one,
  segments between half and fully packed, and a whole-segment rewrite per mutation
  in the CoW window. A save carries the record's bytes twice. Accepted deliberately.
  Note this is *occupancy*, not fragmentation: a removal rewrites the segment
  without its entry and merges below `N/2`, so segments hold no holes to reclaim.
- **A derived copy can disagree with its source.** An invariant that did not exist
  before. The single-batch rule makes it structural — anchor, segment, index and
  logs land in one atomic `Store::apply` or none of them do — and the verbatim
  envelope makes it byte-testable.
- **A split is two more segment writes.** The two halves plus the neighbour on the
  far side of the newly minted segment, whose `next` (or `prev`) must name it —
  three writes, or two only at a chain end. A merge costs the same. Amortised one
  extra per N inserts, in the window a batch already writes.
- **A save relocates the record**, since its instant changes: one descent to find the
  old position, two segment writes. Against today's save — rewrite the record, remove
  one `recency` entry, insert another — it is a write *fewer* and a structure fewer,
  so the relocation pays for itself. But it is a bigger move than before: whole record
  bytes rather than a 18-byte log entry.
- **`all()` and `search()` change meaning.** They stream in `CREATED_AT` order today,
  where `CREATED_AT` is the *insertion* instant; they will stream in modification
  order, newest first. For a keyed type this is a strict improvement (its
  anchor-ordered walk was in meaningless hash order). For a time-keyed type it is a
  real semantic change to a public API, and the honest framing is that
  "most-recently-changed first" is what a listing usually wants — but a caller who
  needed insertion order must now declare an ordering to get it.

Note what is *not* given up: with a sparse index above the chain, `Bound::Range`
keeps its logarithmic descent (`index/mod.rs:64`) and gains a dense scan. The first
draft's main regression is gone.

## Alternatives

- **Page cache only (RFC 0044).** Cheaper and strictly additive, but pays to
  tolerate a random access pattern instead of removing it, and does nothing for the
  browser store. Should still land; not a substitute.
- **A forwarding stub at the anchor instead of the record.** Keeps every address
  computation valid and makes `get(id)` two reads instead of one. It saves the
  duplicated bytes and buys nothing here: the stub dirties the same anchor page the
  full copy would, so the write cost is identical and only reads got worse.
- **Move the records, compensate with indexes** (the first draft). Saves one copy,
  and pays in the `Store` seam, in `#[wavedb::key]`'s addressing story, and in a
  per-record index the duplicate makes unnecessary. Rejected in favour of spending
  the space.
- **Unordered append-only chain** (the first draft's chain). Simplest to write and the
  cheapest insert, but needs a stored back-pointer per record and gives up the range
  descent. Rejected once a key the record already carries turned out to be free.
- **Keyed by the anchor** (the second draft). Preserves today's `all()` order exactly
  and never relocates a record on save, since the anchor is immutable. Rejected
  because it cannot absorb `recency`: insertion order is not modification order, so a
  second instant-keyed structure stays mandatory, with its own lane, its own index and
  a re-key on every save. It also leaves keyed types walking in hash order.
- **Leave it alone and widen the record cache.** Trades the scarce resource (RAM)
  for the abundant one — backwards.

## Open questions

- ~~**Is the anchor copy ever worth eliding?**~~ **Resolved (2026-07-30): no.** The
  earlier framing imagined a pure log type paying for a copy nobody reads. It misses
  that a removal evicts the record from every chain, so the anchor is the only
  remaining home for its bytes — eliding it would force the `dead` log to carry
  records, and `dead` is the one structure that grows forever. Add `Collection::get`
  being public API for every shape, and the anchor is load-bearing in all cases.
- ~~**What does a `CREATED_AT` range mean once `current` is gone?**~~
  **Answered 2026-07-31: `Collection::search` was deleted with the tree.**

  `search(bound)` bounded on the *insertion* instant while the chain is keyed by
  the *modification* one — two different questions, so there was nothing to
  migrate. Three answers were open: rebind the bound to the chain's key (a
  silent meaning change to a public API), keep one instant-keyed tree purely for
  it (most of what `current` cost), or declare it an ordering under RFC 0051.

  What settled it was checking the caller side rather than arguing semantics.
  Three facts pointed the same way: `search` had **zero** production callers (no
  generated wrapper, no wire `Command`, nothing on `DbHandle` — only tests); its
  contract was **already false** for `#[wavedb::key]` types, whose anchor `KEY`
  is a content hash, so the "chronological order" it documented was really hash
  order; and the pre-release policy lets the API break without ceremony.
  Building the ordering now would have been building for a hypothetical user.
  When a real one appears, RFC 0051 is the mechanism —
  `#[wavedb::list(created_at)]` materialises the insertion-ordered chain with a
  sparse index, and `search` returns as a read of *that*, with declared
  semantics instead of implicit ones.
- ~~**Compaction trigger.**~~ **Dissolved 2026-07-31.** The question assumed a
  background pass over segments holding reclaimable holes. There are none: the
  chain rebalances synchronously on every removal (phase 3b), so there is no
  occupancy ratio to watch and no maintenance tick to schedule. What was left
  under the name split into two unrelated items, both settled above — **8a**
  (sparse-index merge, taken as debt) is a same-batch fix with no trigger policy
  at all, and **8b** (removal-log retention) is a client-liveness question moved
  out of this RFC. The one part of the original phrasing that survives belongs to
  8b: whether a prune may run while a watch holds a cursor into what it is
  dropping.
- **Migration.** None, by policy — `FORMAT_VERSION` is pinned and old `data.bin`
  files are unsupported pre-release. Named because this is the largest on-disk
  layout change since the anchor restructure.
