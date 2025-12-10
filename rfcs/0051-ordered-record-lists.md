# RFC 0051 — Declared lists: sorted record chains with a sparse index

- **Status:** Implemented 2026-07-31 — opened 2026-07-29, revised 2026-07-30.
  Landed as [RFC 0050](0050-clustered-record-chains.md) phase 7b: the
  declaration, the identity fold, the sorted chains and their maintenance, and
  the `listed_by_*` readers. The **wire commands** followed on 2026-08-01
  (`Command::Listed` / `Command::ListLen`, `Reply::Count`) — see
  "Reaching a list over the wire" below.
- **Crates:** `wavedb-core`, `wavedb-macros`, `wavedb`, `wavedb-quick-node`
- **Builds on:** [RFC 0050](0050-clustered-record-chains.md) — whose built-in
  modification-ordered chain **is** the first instance of this RFC's mechanism; here
  it becomes general, one chain per declared list

> **Promoted 2026-07-31 by [RFC 0054](0054-no-duplication-by-default.md).** A
> declared list is no longer "a second chain beside the built-in one" — it is the
> **only** structure that duplicates records at all. The built-in chain holds
> pointers, so the copy count is `1 + K`, not `K + 2`. Everything else in this
> RFC is unchanged, and its argument gets stronger: the duplication now lands
> exactly where a read asked for it.

## Summary

A developer may declare that a property **orders** a collection. Each such
declaration materialises a second chain of segments holding the same records
inline, kept sorted by that property at write time — cheap, because a mutation's
extra segment rewrites all land in the one window write a batch already costs.
Above each sorted chain sits a **sparse index**: one B+tree entry per *segment*,
not per record — the separator that opens it. Since the records inside a segment
are already sorted and cover only that separator's range, the index shrinks by
the segment's record count, ~200×, which collapses a million-record index from
hundreds of leaves to about three: a **descent of two nodes, worst case, cold**.

Ordered and range reads then cost one dense read per segment of hits instead of
one random read per hit. Insertion into the middle of a sorted chain is what makes
the index **mandatory** rather than optional: without it, finding the position
is a walk.

## Motivation

Two facts set this up.

The first is what RFC 0050 leaves incomplete. Its chain is sorted by the **live
version's authoring instant**, so modification order reads densely and prunes
properly — and that one order absorbed the `recency` log. Every *other* order gets
nothing: "give me records 200–250 sorted by name" still descends a dense secondary
and pays one random read per row. The mechanism 0050 built for one key generalises to
any key, and this RFC is that generalisation.

The second is what makes it affordable: **writes got cheap enough to spend.** RFC 0041
collapses a batch into one contiguous window write, so a mutation touching five
segments costs one barrier and a bigger window, not five seeks. Ordering work
moved to write time is therefore paid in bytes — the abundant resource — not in
IOps. That inverts the usual reasoning about maintaining sorted copies.

And what does an ordered read cost *today*, with dense secondaries? `search_by`
descends the secondary tree, yields anchors in field order, and issues one
`get_of` per anchor (`collection.rs:283`) — which, per RFC 0050's motivation, is
one page read and one zstd decompression per record. Sorting is already done at
write time today; what is missing is that the sorted order does not make the
*fetch* any less random.

## Design

### The declaration

A new field attribute beside `#[wavedb::pivot(field)]`:

```rust
#[wavedb(NonUnique)]
struct Contact {
    #[wavedb::list]           // one sorted chain, by this field
    name: String,
    city: String,
    created: u64,
}
```

Multi-field lists spell out the tuple — `#[wavedb::list(city, name)]` at
the struct level — because `IndexKey` already encodes several fields into one
order-preserving byte string (`index/node_key.rs:74` calls the encoded bytes
"field value(s)").

Each declaration folds into `STRUCT_HASH` like every other schema fact, so
adding or removing a list is a new type, not a migration.

#### Why `list` and not `order`

The attribute names the **artifact**, not the sort. What a declaration buys is a
materialised second copy of every record, kept sorted at write time and readable
at roughly one IOp per page of results — that duplication *is* the design, and
`order` describes only the arrangement while hiding the cost. It also lines the
vocabulary up with what already exists: RFC 0050's built-in chain **is** a list,
the one every collection gets, ordered by modification instant; this RFC makes
lists declarable. One is the list you are given, the others are the lists you
ask for.

### The generated surface

```rust
impl Contact {
    // #[wavedb::pivot(city)] — a B+tree lookup, unchanged, takes a value
    pub fn by_city(db: &D, city: String) -> impl Stream<Item = Result<Contact>>;

    // #[wavedb::list] — an enumeration; takes nothing, yields the whole list
    pub fn listed_by_name(db: &D) -> impl Stream<Item = Result<(Id, Contact)>>;
    pub fn listed_by_city_name(db: &D) -> impl Stream<Item = Result<(Id, Contact)>>;

    // …and the order-statistic jump the sparse index's counts make O(descent)
    pub fn listed_by_name_at_page(db: &D, page: usize)
        -> impl Stream<Item = Result<(Id, Contact)>>;
}
```

The two shapes differ deliberately: a lookup takes the value it is looking for, a
list takes nothing because it *is* the whole ordering. `_at_page` reads as what it
is — the same list, entered at a page boundary — and pairs with the `page = N`
declaration of [RFC 0052](0052-segment-size-as-the-pagination-unit-DEPRECATED.md),
which is what makes a page one segment read.

### The sort key is (value, anchor), and the anchor is why it's stable

The total order is exactly today's `SecKey`: the `IndexKey`-encoded value
followed by the record's `LocalId`, whose trailing position "makes entries unique
when many records share one field value" (`index/node_key.rs:6`). Nothing new is
needed — the ordering a dense secondary already imposes becomes the ordering the
chain is laid out in.

One refinement on the tie-break. For the default time-keyed NonUnique shape the
anchor's key **is** `CREATED_AT` (`index/node_key.rs:4`), so "ties break by
`created_at`" and "ties break by anchor" are the same statement — as intended.
Spelling it as the *anchor* rather than as the live version's authoring instant
matters though: the authoring instant advances on every save
(`Succession::CreatedAt` is rewritten), so using it would **relocate a record
inside every sorted chain on every save**, even a save that changed nothing
relevant. The anchor is immutable in both shapes — the insert instant for
time-keyed records, the content hash for `#[wavedb::key]` records — so a record
moves only when the ordering property itself changes.

The contrast with RFC 0050's built-in chain is deliberate, not an inconsistency:
*there* the authoring instant is the whole key, so a record relocating on every save
**is** the mechanism — that movement is the modification log. Here, in an ordering
over a domain field, the same instant as a tie-break would be pure cost.

### Splitting is free of consequences

No chain stores a pointer *to* a record: the sparse index addresses segments, and a
record is located by descending with its sort key. So a middle insertion into a full
segment simply splits it, and the entire cost is one extra segment write plus one
extra index entry — nothing to repoint, nothing to invalidate.

This is uniform across every chain, RFC 0050's built-in one included. The band a
segment lives in (N…2N) and the split point are RFC 0052's subject.

### The sparse index

One entry per segment: the **minimum sort key it contains**, mapping to the
segment's `LocalId`. A lookup for key *k* takes the last separator ≤ *k* and
reads that one segment. This is an ordinary `BpTree<SecKey>` — the same
monomorphized machinery, the same node encoding, no new structure.

The size argument is the heart of the idea. With a page-sized segment holding
~200 records and `DEFAULT_LEAF_CAP` at 1819 entries (`index/tree.rs:40`):

| | dense secondary (today) | sparse index (this RFC) |
| --- | --- | --- |
| entries for 1 000 000 records | 1 000 000 | ~5 000 |
| leaves | ~550 | ~3 |
| descent | root → internal → leaf | root → leaf |

The number that matters is the **cold** one: a two-node descent. Nothing here is
pinned in RAM — WaveDB is multi-tenant, so a structure that assumed residency
would be one tenant's index holding memory every other tenant needs. The
guarantee is bounded size, not residency: a middle insertion's position search
costs at most two reads, and when the shared cache happens to hold the index it
costs none. That is what makes the insertion affordable — the worst case, not the
lucky case.

A separator is updated when a segment's first record changes, which is a segment
rewrite anyway; a split writes two.

[RFC 0052](0052-segment-size-as-the-pagination-unit-DEPRECATED.md) adds element
counts to the index, turning it into an order-statistic tree: "the segment holding
global offset *o*" becomes one descent, and the pager's total is the root's sum.
It also sets the segment's size band and shows why a declared capacity is a
**minimum** rather than an exact count.

### Records inline, and the anchor stays authoritative

The sorted chains hold **whole records**, duplicated. That is the point: a dense
segment read yields ~200 records in order with no indirection, and an ordered
scan of the whole collection is ~1/200th of the reads. It is also the cost the
original idea already accepted — high storage in exchange for read IOps.

The duplication is explicit and linear, not a side effect to be minimised:

```text
live bytes ≈ (K + 1) × collection size        # the anchor + K declared lists
           + K × (collection size / N)        # the sparse indexes, one entry per segment
           + the dead chain                   # key-sized entries, not records
```

So four declared lists mean roughly five copies of every record on disk, and
zero declared lists mean one (RFC 0054).
Copy-on-write and un-compacted holes sit on top of that. This is the accepted
price of the design, stated here so no later reader mistakes it for an oversight.

The **anchor copy is the source of truth** (RFC 0050): it holds the version chain,
it is what `Expect` guards, and it is what a computed address resolves to. Every
chain — the built-in one, the declared ones, dead — is derived from it. That
ranking decides two things: a disagreement resolves toward the anchor, and a
compaction or rebuild may rewrite any chain wholesale without consulting anything
but the anchors.

### What a mutation costs

For a type with K declared lists, one insert is:

- 1 write of the authoritative record at its anchor (RFC 0050);
- 1 insert into RFC 0050's built-in modification-ordered chain (one segment write);
- K × (one sparse-index descent — ≤ 2 reads cold, 0 cached — plus one segment read
  and one segment rewrite, and a split's second segment and index entry when the
  segment is full);
- the `dead` chain on removal — and no `recency` maintenance at all, since RFC 0050's
  built-in chain absorbed it.

All of it is one `Store::apply` batch, hence one window and **one barrier**
(RFC 0041). The IOps do not scale with K; the bytes do.

A save is the same shape with one wrinkle: if the ordering property changed, the
record must leave one segment and enter another — two segment rewrites in that
chain instead of one. If it did not change, the record is rewritten in place in
every sorted chain regardless, because its bytes are duplicated there. So **a
save carries K× the record's bytes**. For the read-heavy, write-light shape
WaveDB targets that is the intended bargain; a write-heavy collection should
declare few lists, and that is a documentation matter, not a mechanism.

### What reads become

- **Ordered scan** (`all` in a declared order): descend the sparse index once,
  then follow `next` — dense reads, ~1 per 200 records.
- **Range** (`Bound::Range`/`Prefix` on the ordered property): descend to the
  first segment, walk until the bound fails. O(hits / N) reads, versus O(hits)
  random reads with a dense secondary today.
- **Pagination**: an offset walk becomes a segment walk, and a keyset cursor is
  just a sort key — the sparse index turns it into one descent.
- **Point lookup by the ordered property**: one descent, one segment read.

## Alternatives

- **Dense secondary index only** (RFC 0050's declared `BpTree`). Smaller — no
  duplicated bytes — and the right choice when a property is used to *find* a
  record rather than to *list* records. The two should coexist: `#[wavedb::pivot]`
  for lookup, `#[wavedb::list]` for enumeration. Making one sugar for the other
  is tempting and wrong; they optimise opposite operations.
- **Covering projection instead of whole records.** Store the sort key, the
  anchor, and only the fields a listing renders. Bounds the duplication to what
  is actually read and keeps ordered scans dense. The cost is a schema-level
  declaration of *which* fields, and a second fetch when a caller wants the rest.
  The best candidate for a follow-up once real listings exist to measure.
- **Sort at read time.** Needs RAM proportional to the result set and redoes the
  work per query — trading the scarce resource for the abundant one, backwards.
- **A dense index whose leaves hold the records** (clustered index). Equivalent read
  performance — a leaf holding records *is* a segment — but the index above it stays
  dense, one entry per record, ~200× larger for no gain. The sparse form is the same
  idea with the redundant levels removed: the chain's `next` pointers do the work
  the lowest index levels were doing.

## Reaching a list over the wire (landed 2026-08-01)

For one release this RFC shipped a structure no client could read. A declared
list resolved against a `LocalHandle` or a `ServerDb` and refused over the
transport exactly as `search_by` does — so an app could declare a list, pay a
full copy of every record for it on every save, and still not render a page
without wrapping it in a `#[server]` function. The thing that renders the page
was the one thing that could not ask for it.

Two commands close that:

```
Command::Listed   payload = (LocalId pivot, u32 index, u64 offset, u32 limit)
Command::ListLen  payload = (LocalId pivot, u32 index)
Reply::Values(Vec<Vec<u8>>)   // frames of (Id, Metadata, T), as `All` ships
Reply::Count(u64)             // new — the pager's "of M"
```

**Why this did not have to wait for streaming frames.** `All` buffers a whole
collection because the POST tunnel answers one request with one response; that
is a compromise, and `search_by` — an unbounded range — is waiting with it. A
list page is not waiting on anything, because `limit` is the caller's page
size: the answer is bounded **by construction**. The reply carries the ordinary
pager rule and needs no truncation flag — exactly `limit` entries means there
may be more, a shorter answer is the end — and since the client chose `limit`,
it is never guessing.

The limit is deliberately **uncapped**. Capping the narrower command while `All`
buffers an entire collection unbounded would protect nothing and would make one
command lie about what it served; a node-wide read budget is the M8 gates'
business, not one op's.

The typed surface gained `listed_page(db, index, offset, limit)` alongside the
unbounded `listed`/`listed_at`, because otherwise the wire's `limit` is
unreachable from typed code: `listed_at(..).take(25)` would fetch the client's
whole internal chunk and discard most of it. The unbounded reader pages at a
fixed 256 — deliberately *not* the declared `page`, since a type declaring
`page = 4` would turn a full walk into a round trip per four records.

Paging is **not** a snapshot: between two chunks the list can change, so a
record can be seen twice or missed if its ordering property moves under the
walk. That is inherent to any offset pager, and a caller who needs a coherent
"what changed" wants `watch_collection`, which is built for it.

The cache follows the same three rules as every other read (node first, mirrors
best-effort, absence is not an answer): each page mirrors under the node's
identity and metadata as it passes, and a *transport* fault on the first chunk
falls back to the warm local list — where a cold cache propagates the fault
rather than minting an empty list, which would read as "there is nothing here".

## Open questions

- **Is the separator enough, or does the index need the segment's max too?**
  Contiguous segments make the max derivable from the next separator; storing it
  would let a lookup detect an empty range without reading a segment. Probably
  not worth the doubled entry.
- **A ceiling on K.** Should the macro refuse (or warn on) more than a handful of
  lists, given each one multiplies save bytes?
- **Interaction with `#[wavedb::key]`.** A keyed type's anchor is a content hash,
  so `all()` over it comes out in hash order and its sorted chains are the only
  structure with meaningful order. Should declaring a key *imply* an ordering over
  the key fields — the listing a keyed collection almost certainly wants — or is
  that the developer's call to make explicitly?
- **Rebuild.** Derived chains can in principle be reconstructed from the
  anchors. Pre-release policy says an inconsistent `data.bin` is simply
  unsupported — but a `rebuild_lists` maintenance op is cheap to write and
  makes the "derived" claim testable.
