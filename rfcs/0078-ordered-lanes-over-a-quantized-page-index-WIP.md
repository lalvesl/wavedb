# RFC 0078 — Ordered lanes over a quantized page index

- **Status:** In progress. Opened 2026-10-30. Phases 1–3 and 6 landed
  2026-10-30: the index (`wavedb-storage::ordered`), every stored value in
  ordered pages keyed by its `Id`, the header and reconstruction, and the
  object store on the same index. Phase 4 landed 2026-10-31: chains are key
  ranges of ordered lanes, the segments, the sparse index and the Pivot's
  roots are gone, and a declared list's pages are counted (see "As built").
  Phase 5 closed 2026-10-31 without code: measured, a seed per key type
  and a density hint buy nothing, so one seed stays and the hint is
  dropped (see "Phase 5, measured"). Phase 7, measuring, remains.
- **Revises:**
  - [RFC 0074](0074-extendible-hashing.md): the hashed page directories go,
    anchors included.
  - [RFC 0050](0050-clustered-record-chains.md) and
    [RFC 0075](0075-secondary-indexes-as-chains.md): a chain keeps its
    surface, but stops being segments under ids with a sparse index stored
    in a lane.
  - [RFC 0072](0072-the-settle-is-the-durability-point.md) §3–§5: the
    header names index changes, not `(bucket, address)` pairs.
- **Builds on:**
  - [RFC 0068](0068-record-cache-as-a-log-WIP.md) and RFC 0072: the record
    cache and the settle cycle stay as they are;
  - [RFC 0071](0071-data-files-by-address.md) and
    [RFC 0073](0073-data-shards-and-storage-shards.md): data files,
    allocator, storage shards;
  - [RFC 0070](0070-object-storage-backend.md): the object store.
- **Crates:**
  - `wavedb-storage`: the index, the lanes, the header;
  - `wavedb-core`: the chain's storage, the Pivot's roots;
  - `wavedb-macros`: the lanes' slots (the density hint was dropped, phase 5).

## Summary

Inside each lane, the pages stop being placed by **hash** and are placed by
**key order**. A page is `k × 4 KiB` blocks holding one contiguous range of
keys, sorted. Every key is bytes in an order-preserving encoding:
- an anchor's key is its `Id`;
- a chain entry's key is `[pivot][declaration][chain key]`, so a collection
  is a range of its lane, not a structure of its own.

The partition above the lanes is unchanged: data shard, then type, then
lane.

The only thing that finds a page is an index held in RAM:
- a sorted `Vec<SubHeader>`;
- under each sub-header, a `Vec<u32>` with **one entry per page**:
  `[q: u16 | address and size: 16 bits]`.

`q` is the page's first key, quantized into one of 65 536 steps of the
sub-header's range. The other 16 bits are the page's offset from the
sub-header's base and its size in blocks, each scaled by a factor the
sub-header carries. Only `q` is ordered; addresses fall wherever copy-on-write
put the page.

Finding a key's page is two binary searches in RAM, then one page read.

A batch rewrites only the pages its keys fall in, and those are neighbours in
key order. On the common paths, a batch's pages are new or already in RAM:
- inserts keyed by time (NonUnique anchors, recency, the removal log) land on
  the tail page of their range;
- a hot range is in the page cache.

So no page is read in order to write one, and the disk cost of a write is its
share of the batch's pages.

Every change to the index is a few bytes. It goes in the header that already
ends every window on disk and heads every file in the object store. RAM per
page drops from a `u64` directory entry, or several of them, to one `u32`. A
list lane's pages take one byte more, for their count.

## Motivation

The engine's write amplification is structural, and no tuning has moved it.

**What was measured:**
- **Per insert:** a durable micro insert writes **202 KiB** (RFC 0074,
  "Measured").
- **The micro run:** on 2026-10-30, 200 000 durable inserts of ~260 B
  (~52 MB of records) wrote **~41 GB**.
- **The multi run:** it left 9.4 GB on disk for 50 MB of logical data.
- **The record cache as a write buffer only:** branch `bench-node-multi`,
  `0cc869f`. It changed the bytes written by about nothing.

**Where the bytes come from:**
1. **A hash scatters a batch.** A record's page is chosen by its hash, so
   records written together land in unrelated pages. Each page is rewritten
   whole to carry a few hundred bytes of change.
2. **A chain is two structures.** A segment is a *value* inside a hash bucket.
   Changing a segment rewrites:
   - the bucket, along with the unrelated segments that share it;
   - the sparse-index node, in another lane, another bucket;
   - the Pivot, when a root moves.

   The order lives in links and in the sparse index. The bytes live where the
   hash put them.
3. **The header is 32 KiB** per round, however little the round changed.
4. **RAM.** A directory holds a `u64` descriptor per entry and has `2^d`
   entries, so several of them point at one page.

The key order already exists for most of the data:
- `key_nanos()` makes a NonUnique anchor's `KEY` its creation time.
- The recency chain and the removal log are keyed by instant.
- Secondary and list keys are order-preserving bytes already
  (`[field][anchor]`).

A structure ordered by those keys turns the common writes into appends, and
makes the pages a batch touches neighbours.

## Design

### 1. Lanes and keys

The partition stays:
- the data shard, chosen by the owner (`core::placement`);
- then the type;
- then the lane.

There is **one ordered structure per lane**. Every collection of the type
shares it, and a collection is a **key range** whose keys start with its
Pivot:

| structure | key | payload |
|---|---|---|
| anchors (records, Pivots) | `[Id]` | the record |
| recency | `[pivot][instant BE][anchor]` | — |
| removal log | `[pivot][instant BE][anchor]` | — |
| secondaries | `[pivot][declaration][field][anchor]` | — |
| lists | `[pivot][declaration][property][anchor]` | the record |
| fuzzy postings | `[pivot][declaration][gram][anchor]` | — |

These are today's lanes. The `[declaration]` byte keeps a type's
declarations apart inside their lane.

Nothing is nested. A chain is not a structure stored inside the ordered
structure; it is a range of it:
- **A small collection** sits in part of a page, beside other small
  collections of any user. This is the shop case: a cart with three
  products.
- **A large collection** spans pages, and those pages hold nothing else
  (§4, the split at a collection's edge).

The anchors' structure is shared the same way, by nature. It holds:
- every tenant's Unique records;
- every NonUnique anchor and archive slot;
- every holder's Pivot, as a type of its own.

**The `Id` key is an order-preserving projection** of the `Id`,
`[KEY BE][TENANT BE][FLAG | SALT]`. It is not the wire encoding, which is
little-endian. What that order does to each kind of anchor:
- NonUnique anchors sort by creation time, so inserts append at the tail.
- `#[wavedb::key]` anchors sort by hash, which is uniform: the index's
  easiest case.
- Unique anchors share `KEY = STRUCT_HASH`, so they sort by tenant.

### 2. Pages

A page is `k × 4 KiB` blocks. It holds the keys of one contiguous range,
sorted, in the existing `SlotPage` form: the checked-wire envelope, the three
columns, and zstd with the lane's dictionary.

**A page boundary is always a quantum boundary.** Page *p* covers exactly the
quanta `[q_p, q_{p+1})` of its sub-header. That makes the index exact:
- A key maps to one quantum, and that quantum to one page.
- A lookup never reads a second page to find out whether it read the right
  one.

### 3. The index in RAM

```rust
/// One range of a lane's keys, and how its entries are read.
struct SubHeader {
    /// First block the entries' addresses count from.
    base_block: u64,
    /// The range's first key: the 8 bytes of the key at `depth`, big-endian.
    first_key: u64,
    /// Bytes of key every key in the range shares. They are held whole in
    /// the lane's arena of bounds.
    depth: u16,
    /// Quantization: `q = (slice(key, depth) − first_key) >> key_shift`.
    key_shift: u8,
    /// Width of the address field. The size field gets the rest of the 16
    /// bits.
    address_bits: u8,
    /// Blocks per unit of the address field: a multiplier.
    address_factor: u8,
    /// Blocks per unit of the size field: a multiplier.
    size_factor: u8,
}

/// The page entries under one sub-header, ascending by `q`, as two parallel
/// columns.
struct Entries<M> {
    /// `q << 16 | address << size_bits | size`.
    pages: Vec<u32>,
    /// Per page: `()` where nothing is counted, and in a list's lane a
    /// `u8`, how many entries the page holds (§7).
    meta: Vec<M>,
}
```

**The range.** A sub-header covers
`[first_key, first_key + 2^(16 + key_shift))` at its depth. Its successor's
start is therefore known when it is created: this is the "next sub-header,
guessed ahead".

**Ordering.** Sub-headers do not overlap, and gaps between them are empty key
space.

**Inserts in the middle.** A new sub-header is inserted where its key range
falls. It is named by its first key, never by its position, so the position
can move.

**Strings and composite keys.** These need nothing special. `depth` skips the
prefix the range shares, and `key_shift` quantizes the next 8 bytes. A
timestamp is the case `depth = 0` on an 8-byte big-endian key.

**Lookup of a key:**
1. **The sub-header.** Binary search the sub-headers by first key. The
   comparison covers the bounds' bytes in the arena up to `depth`, then
   `first_key`.
2. **The quantum.** Compute `q`. If it is past `2^16`, the key lies beyond
   this sub-header: it is absent, or it belongs to a gap.
3. **The entry.** `entries.partition_point(|e| e >> 16 <= q)` minus one.
   Before the first entry, the key is absent.
4. **The page.** Its address is `base_block + address × address_factor`; its
   size in blocks is `(size + 1) × size_factor`.

`q` sits in the high bits, so step 3 is the same as `e < (q + 1) << 16` on
the raw `u32`. No mask is needed, which is what a later SIMD scan of short
vectors would use (§11).

**Worked example (a steady stream).** Use 8 KiB blocks
(`address_factor = size_factor = 2`), 15 address bits and 1 size bit:
- one sub-header reaches 256 MiB of file;
- a page is 8 or 16 KiB.

For an instant key at `key_shift = 21`:
- a quantum is ~2 ms;
- a sub-header spans ~2.3 minutes.

**RAM.** A sub-header is ~32 bytes plus its `Vec`. A full sub-header has up to
65 536 entries (256 KiB) behind those 32 bytes, so its cost dilutes into the
per-page count:

| data | pages | RAM today (`u64`, ≥ 1 per page) | RAM here (`u32` per page) |
|---|---|---|---|
| 1 TiB at 8 KiB | 134 M | ≥ 1 GiB | 512 MiB |
| 1 TiB at 64 KiB | 16.8 M | ≥ 128 MiB | 64 MiB |
| 1 TiB at 1 MiB | 1 M | ≥ 8 MiB | 4 MiB |

### 4. Splitting, and new sub-headers

All of these happen in RAM during the round that writes the page.

**A page over target that spans more than one quantum** splits at a quantum
boundary: two entries where there was one. The `Vec` insert moves at most
256 KiB.

**A split prefers a collection's edge.** A page holding more than one
collection splits at the edge between two of them, the one nearest its
middle. A collection that outgrows a page on its own is cut off at its
edges first, and split in its middle only after that.

So a page holding several collections holds only whole ones:
- a collection either fits whole in one page, shared or not;
- or it spans pages that hold nothing else.

An edge is a split point like any other, so it must fall on a quantum
boundary. When the last key of one collection and the first of the next
share a quantum, the range refines, as below.

**A page over target inside a single quantum** cannot split there. Its range
gets a sub-header of its own:
- with a finer `key_shift`;
- or, once `key_shift` is 0, with a greater `depth`.

**Shifts are powers of two.** So refining a range's quantization recomputes
the existing entries' `q` in RAM: every coarse boundary is also a fine one,
and no page is touched.

**A value repeated very often** (one category on most of the records) goes
down in `depth` past the value into the anchor bytes, which sort by time. Its
pages become an append stream of their own. Detecting such a value against
the sub-header's `first_key`, and other treatments of it, are a later RFC.

**An address outside the window.** When a rewritten page's new address does
not fit `address_bits × address_factor` from `base_block`, the sub-header
splits at that entry. The entries after it move under a new sub-header with
its own base.
- **The allocator is asked for affinity first:** a free run inside
  `[base_block, base_block + window)`. Only when none exists does the page go
  elsewhere.
- **Compaction re-bases:** when it moves runs, it rebuilds the sub-headers
  over the new addresses.

**A size the field cannot express** splits the page, or opens a sub-header
with a larger `size_factor`.

A lane whose pages are rewritten all over the disk degrades toward one
sub-header per page. That is the cost the affinity and the later regroup
(§12) exist to bound.

**A range isolated next to one that can address it folds into it.** The
pages a round writes lie side by side, so the two halves of a cut, and
neighbours in key order rewritten together, land in one window. When a
page is isolated, its new range and each neighbour become one sub-header
if one can name both:
- the same prefix;
- the finer of the two quantizations, which keeps every entry's start
  (shifts are powers of two);
- based at the lowest page, every page addressable from there, at the
  larger size unit;
- reaching as far as the second range did, or to the start of the range
  after it;
- 1024 entries at most.

No page moves: every key finds the page it found before, and quanta between
the two that no entry covered become a hole. The folded range is recorded
whole and the range it took is recorded gone. What lies far apart on disk
stays apart; bringing it together is the regroup's work (§12).

**A range boundary is always an entry boundary**, so every page is named by
exactly one entry. When a reshape gives a page a range of its own, the
quanta the page covered but no key of it falls in become **holes**: entries
whose address field is all ones.
- A hole holds no key. A lookup there answers "absent", and the first write
  there fills the hole with a page, in the hole's range.
- A range left holding only a hole is kept. A later key fills it with that
  range's quantization, where a range opened afresh could only guess one.
- A range is dropped once it has no entry at all.

There is one exception. A range's quanta run on past the next range's start,
because a key belongs to the last range starting at or before it. A hole-only
piece that would start where the next range already starts is therefore
never made: its quanta are the next range's.

As built (`wavedb-storage::ordered`):
- `PageIndex::find` answers a page, a hole, or uncovered.
- `place` opens a range for an uncovered key.
- `retarget` points an entry at a rewritten page, and isolates the page when
  its range cannot address it; the isolated range folds into a neighbour
  that can (`fold_around`, `ordered/fold.rs`).
- `cut` splits a page on a quantum boundary, refining when the page lies in
  one quantum.
- `remove` hands a page's quanta to the entry before it.

### 5. Writes

The record cache, the settle cycle, the rounds, the barrier and the
`_durable` operations are unchanged (RFC 0068, RFC 0072). A durable write
opens its batch at once (RFC 0072 §12).

What a round does with what it owes:
1. Group the owed writes by lane, and sort them by key.
2. Walk the sorted keys against the index. Each run of keys that falls in one
   page makes that page's new version:
   - a page **in the page cache** is rebuilt from RAM;
   - a **tail page** (the range's last) takes the keys appended;
   - a **cold page in the middle** is read once and rewritten (copy-on-write).
3. Write the new pages into the round's window, with the allocator's
   affinity (§4).
4. Update the entries, split as §4 says, and record the changes for the
   header (§6).

Two cases cost a page read, and only these two:
- a cold page in the middle of a range;
- the semantic reads the data model already makes, which happen before the
  batch and come from the anchor: the `Expect` guard, and the previous field
  values a secondary re-key removes.

Writing cold pages without reading them (cumulative deltas, as in the
Bw-tree) is deferred to a later RFC.

### 6. The index's changes on disk and in the object store

Every change to the index is recorded where the window's addresses are
recorded today:
- in the **header at the end of the window** on disk;
- in the **header that opens the file** in the object store, after its roots
  block.

The `TypeEntry.changed: Vec<(u32, u64)>` of RFC 0072 §3 becomes, per lane:

```rust
struct LaneEntry {
    lane: u64,
    /// The previous header with entries for this lane.
    last: u64,
    /// Sub-headers created or changed, named by their full first key.
    sub_headers: Vec<SubHeaderRecord>,
    /// Sub-headers merged away, by first key.
    dropped: Vec<KeyBound>,
    /// Per sub-header: entries written (`u32`), and quanta whose entry is
    /// gone.
    entries: Vec<EntryDelta>,
}
```

**Size.** A changed page costs about 4–8 bytes in the header, where RFC 0072
spends 12.

**The header stays 32 KiB.** A known size is one read: one disk IOp, one
ranged GET. A batch whose changes overflow it chains another header, as
today:
- in sequence in the object store;
- in reverse sequence on disk.

At 4–8 bytes a change, a header holds about 4 000–8 000 changes where it held
about 2 700, so the overflow gets rarer.

**The spare room** (RFC 0072 §5) restates the lane restated longest ago.
- **The unit is a sub-header:** its record and its entries.
- **A large one is restated in slices.**
- **The lane's sub-header list rides every restatement.** It is small, so a
  reconstruction knows which ranges it still owes.

The rotation goes on: the spare room keeps restating, oldest first, so a
reopen finds the hot lanes in the newest headers. The cleanup of headers no
longer used also goes on, beside the copy-on-write cleanup (RFC 0072 §8).

**Reconstruction** is RFC 0072 §6's walk:
- The first value seen for a (sub-header, quantum) is its newest.
- A lane is closed once its sub-header list is known and every sub-header's
  entries are.
- The horizon, the crc check of the last batch, and the deferred frees stay
  as they are.

### 7. Chains on ordered lanes

`Chain<P>` keeps its surface: `plan_insert`, `plan_remove`, `scan_from` and
the stream. Its storage changes:
- **Segment ids, `prev`/`next` links and `mint_segment_id` go:** the lane's
  pages are the segments.
- **The sparse-index lane (`WDB.IDX`) goes:** the index in RAM is the descent.
- **`ChainRoots` and `LogRoots` in the Pivot go:** a chain is found by its
  `[pivot][declaration]` prefix.
  - The Pivot keeps its identity and its holder's references.
  - Its shape changes, and so does its `STRUCT_HASH`. There is no format
    bump, per policy.
- **`create_pivot` only mints an id,** and no chain write ever rewrites the
  Pivot.

**Counts.** Only a declared list counts: `list_len` (`_len`,
`Command::ListLen`) and `listed_at` (`_at_page`) are the readers of the
sparse index's counts today. Recency, the removal log, secondaries and fuzzy
postings count nothing.

A list's lane keeps a count per page in its entries' metadata column (§3):
- The count is an exact `u8`: how many entries the page holds. A list page
  holds whole records: 30–100 in 8 KiB.
- A page that would pass 255 splits, as it does when its size outgrows the
  size field.

The split at a collection's edge (§4) makes a page's count a collection's
count:
- **A collection spanning pages** owns them. `_len` sums its pages' counts
  in RAM. `_at_page(k)` sums them up to row k, then reads one page.
- **A collection inside one page** is read from that page. Its count and its
  rows come with the one read a listing makes anyway.

**Not a multiplicative factor.** A count of `n / f` is off by up to `f − 1`
per page, and the error adds up along a list. After 100 pages, "jump to
row k" lands a hundred rows away. A count must be exact. Should a later
regroup make list pages larger than 255 records, the exact form is a base
held in the sub-header: the count is `count_base + byte`.

### 8. The object store

The object store's directory (RFC 0070, 128 KiB pages) becomes the same
index, with larger factors.

An upload carries the pages as the disk holds them. Regrouping them into
larger pages is the later regroup (§12).

### 9. The density hint

> **Dropped, measured (phase 5).** Neither the typed seed nor the hint was
> built: the coarse seed refined on demand matches a typed one, and a seed
> too fine fragments a lane into a range per key. See "Phase 5, measured".

The macro knows each key's type: an instant, an `Id`, an integer, a string, a
composite. From that it seeds a structure's first sub-header (`key_shift`,
factors, target page size).

An optional hint on a declaration overrides the seed for data the developer
knows to be sparse or dense. The syntax is to be settled with phase 5.

The hint is **not folded** into the `STRUCT_HASH`. Every sub-header records
its own parameters, so flipping the hint changes only how new ranges start,
and cannot misread a stored one. It is behaviour that never reaches disk.

### 10. Complexity

**A read:**
- in RAM, a binary search over the lane's sub-headers and one over at most
  65 536 entries;
- then one page read, or none when the page is cached.

**A write:**
- nothing on disk until its batch settles;
- then its share of the batch's pages.

A batch of *n* records into *m* touched pages writes *m* pages. For time-keyed
inserts, *m* is about *n* / (records per page).

**A scan** of a range is one descent, then sequential pages.

### 11. Search

The first implementation uses the standard library's
`slice::partition_point`, a binary search.

A SIMD scan of a short `Vec<u32>` is a later step:
- compare the raw entries against `(q + 1) << 16` (§3);
- count the lanes that pass.

Binary search stays for long vectors.

### 12. Later RFCs (out of scope here)

- **Regroup.** Its first step is taken here (phase 8): a phase of
  compaction rewrites a stretch of fragmented ranges' pages verbatim, side
  by side in key order, and the fold (§4) makes them one sub-header.
  Beyond that, a pass reopens a range's pages and rewrites them as fewer,
  larger pages:
  - larger factors and fewer sub-headers;
  - less RAM and better compression;
  - long sequential runs for analytical scans.

  Durable writes and pressure leave small pages and many sub-headers behind;
  this is what undoes that, on disk and in the object store.
- **Repeated values.** Detecting a hot value against a sub-header's
  `first_key`, and a dedicated treatment of it.
- **Blind writes into cold pages.** Cumulative per-page deltas, packed per
  batch, consolidated in the background. Reads cost at most two pages.
- **Versions at their authoring instant.** Every version written once at its
  own instant, which makes archiving free.
- **SIMD search** (§11).

## Gone

- The extendible directories of RFC 0074:
  - `BlockDescriptor`'s local depth and size code;
  - the directory pages;
  - `split.rs` as it is.
- Chain segments as values:
  - segment ids, `prev`/`next` links;
  - `Lane::Index` and the sparse-index nodes;
  - `ChainRoots` and `LogRoots` in the Pivot.
- `page = N` (RFC 0052), on the struct and on a list: a page of a list is a
  skip and a limit on its range, so no capacity is the declaration's to
  choose.

## Phases

Each phase leaves the suite green.

1. **The index alone.** `SubHeader`, the entries, lookup, the split rules,
   refinement, re-basing.
   - **Unit tests:** exact lookup on every boundary; a refinement leaves
     every lookup unchanged; a window overflow splits; a single-quantum
     page goes deeper.
2. **An ordered lane in `PageStore`.** Pages by key range, a round per lane,
   copy-on-write, the allocator's affinity. Anchors first, keyed by the `Id`
   projection.
3. **The header.** Lane entries, the spare room by
   sub-header, reconstruction.
   - **Durability tests:** reopen after each kind of change; a crash cut in
     the last batch; a lane restated in slices.
4. **Chains on ordered lanes.**
   - Collections as key ranges, the split at a collection's edge, list
     counts.
   - Drop the segment links, the index lane and the roots.
   - **Tests:** a collection growing out of a shared page into pages of its
     own; `_len` and `_at_page` across sub-headers; reopen.
5. **The macro seed and the density hint.** Closed without code: measured,
   it buys nothing (see "Phase 5, measured").
6. **The object store's directory** as the same index.
7. **Measure** against RFC 0074's rows:
   - bytes written per durable and relaxed insert, on micro and shop;
   - RAM per page;
   - read latency, hot and cold.
8. **The fold and the regroup's first step.**
   - The fold (§4): an isolated range merges into a neighbour that can
     address it. In RAM, no page moved.
   - The regroup, as a phase of compaction: on the same triggers, budget
     and sealing, the pages of a stretch of fragmented ranges are rewritten
     verbatim, side by side in key order, so the fold makes them one range.
     The compaction's own moves go in key order for the same reason. Pages
     merged into fewer, larger ones are left for §12.
   - Decided 2026-10-31.

## As built (phases 1–3 and 6)

The extendible directories are gone (`directory.rs`, `directory_pages.rs`,
`split.rs`). Every type's pages — records, Pivots, and, until phase 4, the
chains' segments and sparse-index nodes, which are values under ids too —
live in one `PageIndex<u8>` per type (`crate::pages`): the key is the
`Id`'s raw `u128` big-endian, the `u8` beside each page its flags,
`PENDING` (not uploaded) and `EVICTED` (gone from disk).

- **Seeds.** A disk range starts at the coarsest quantization (`MAX_SHIFT`),
  12 address bits (a 16 MiB window) and 4 size bits; the object store's, 10
  and 6. Refinement does the rest.
- **Exact sizes.** An entry names its run exactly: a page is given
  `canonical(blocks)` — whole units of the size unit the index's rule picks
  for it — so freeing a page frees what was allocated. A page past 255
  units is `PageTooLarge`.
- **A round** (`crate::shape`, shared by the settle and the upload) routes
  its records in key order to the pages they fall in; a key with no page
  gets a new one whose entry points at a stand-in its range can always
  address (`placeholder_at`). Pages are then shaped from the last back: a
  page emptied goes — or, in an object-backed store, becomes a `PENDING`
  hole (`punch`), so the object store learns its keys are gone — and one
  over target is cut at its middle or the quantum boundary nearest it.
  Every page comes out with its first record's key; once the window is
  placed, `install` points that key's entry at the run (`retarget`, which
  isolates a page its range cannot address) and cuts any range past 1024
  entries (`split_long`).
- **Headers.** `TypeEntry` carries the type's range count and
  `Vec<RangeRecord>`: `Whole`, `Entries` — which restates the range's base
  and shape, so the last batch's pages decode for the crash check — and
  `Gone`. The spare room restates whole ranges from a cursor by start key.
  The walk rebuilds each index with `Rebuild`, a type done once it knows as
  many ranges as its newest entry counts.
- **Two readers of the remote index.** Its own journal is drained by the
  object store's file headers; a copy of every upload's changes, kept on the
  type (`remote_journal`), is drained by the disk's headers.
- **Uploads** take every `PENDING` entry, page or hole, with its coverage
  `[lo, hi)`: the remote pages meeting it are fetched, every record within
  it is replaced by the disk entry's own, and the remote pages are shaped
  like a settle's. The files are laid out first, the remote index pointed
  at the runs, and the headers written over it; an upload whose headers
  could not name everything is not a cut.
- **Eviction** flags an entry `EVICTED` and frees its run; its keys still
  route to it. A restore rebuilds the page from the remote pages meeting
  its coverage. An adoption from the object store takes the remote index
  as the disk's, every page evicted.

**Known cost.** Data files only append, so a page rewritten long after its
neighbours lands far from its range's base, and its range isolates it: a
workload of scattered rewrites drifts toward one range per page. The fold
(§4) merges back the ranges whose pages already share a window; the
regroup (§12) is what brings the rest together. Phase 5 measured it (below), and the decision is to
keep it: an address is always one `u32` relative to its range's base, so
every page is found — and every run the allocator rebuilds is named — the
same way, and the sub-headers isolation adds are what the regroup folds
back later.

## As built (phase 4)

**The store seam.** Every value is stored under bytes. `Store` gained
`Write::Set(lane, key, value)` and `Write::Unset(lane, key)`, `scan(lane,
&Span)` — `Span { lo, hi, rev, skip, limit }` — and `count(lane, lo, hi)`;
`read_span` and `read_count` answer them for a sorted map, which is how
`MemStore`, the overlay of a batch being planned and the tests model a
lane.
- **`PageStore`.** A lane is a registered slot of its own. A `SlotPage`
  holds its keys in a column, back to back: one width when every key has
  it, else their ends. The record cache logs `[klen u16][key][len
  u32][bytes]`, and keeps an ordered lane's keys in a `BTreeSet` per slot.
  A scan (`lane_read`, `lane_walk`) walks the index from the span's edge,
  one coverage at a time, and merges each page with the cache's keys over
  it; it stops at an evicted page, restores it and walks again.
- **`IdbStore`.** One more object store, `lanes`, keyed
  `[lane BE][key]`, read by cursor (`DB_VERSION` 2).
- **`CacheStore`** forwards scans and counts to the cache's carrier.

**Chains.** A chain is the range `[pivot Id 16 B][declaration u8]` of its
lane, then each entry's field bytes and its anchor's 10 bytes
(`index::chain`). An insert is one blind `Set`; a removal one `Unset`,
`plan_remove` reading first where a caller gates on the verdict. A stream
(`chain_scan`) reads 256 entries a scan, from a key, past a skip, or back
from the end. Five lanes per NonUnique type: records (the declared lists),
recency, dead, secondary and fuzzy; each declaration is its own range.
- **The Pivot** holds only `permission`. Creating a collection is one
  `Put`; a collection op checks the Pivot exists (`check_pivot`) and
  writes nothing to it.
- **Concurrent inserts** from one state no longer clobber each other: both
  are blind writes to different keys (`concurrent_chain_inserts`).

**Counts.** A declared list's lane is registered counted
(`StructStorage::counted`). Its index keeps a third column beside each
page's entry and flags, how many entries the page holds; no other lane
pays the byte. The round sets a page's count when it installs a page it
built; a page moved (compaction) or restored keeps its count. The counts
ride the headers with the entries — `WholeRange::counts`, and `counts` in
`RangeRecord::Entries` — and come back with them.
- **Passing a page.** A walk passes a page without reading it when the
  span covers its whole coverage, its count is known, and every key the
  record cache holds in that coverage is settled (`RecordCache::settled_in`
  — a settled entry's page holds it). A list's length and a skip into it
  read their edge pages only (`a_counted_lane_counts_and_skips_reading_only
  _its_edges`).
- **A saturating byte, not a split at 255.** A count of 255 means "255 or
  more": that page is read. A count is never a guess, and a list of tiny
  records needs no extra pages for it.
- **No split at a collection's edge.** Pages are still cut at their middle
  or the nearest quantum boundary. A collection's first and last pages are
  then shared with its neighbours, and a count reads them — two pages at
  most — where a split at the edge would read none.

**`page = N` retired.** It no longer laid anything out, so it was a knob in
the identity that changed nothing on disk. `_at_page(page, page_size)` and
`listed_page(offset, limit)` take the size from the caller.

## Phase 5, measured

The index alone (the cut and refinement rules of `crate::shape`, pages of
30 or 250 keys, a fresh run of 2 blocks for every page written), 60 000
keys of each shape the engine stores. Ranges per page:

| keys | seed shift 48 (today) | a typed seed | a seed too fine |
|---|---|---|---|
| A. NonUnique anchors, ~1 ms apart | 0.01 | 0.01 (21) | 1.00 (0) |
| B. one collection's recency | 0.09 | 0.09 (21) | 0.08 (0) |
| C. recency of 2 000 collections | 0.98 | 0.97 (21) | 1.00 (0) |
| D. one list by a string | 0.96 | 0.96 (21) | 0.96 (0) |
| E. a `u64` secondary, 50 collections | 2.11 | 2.16 (21) | 1.87 (0) |
| F. keyed anchors, uniform | 0.95 | 1.00 (21) | 1.00 (32) |

- **The seed barely matters.** The coarsest quantization, refined where a
  cut needs it, lands within a few ranges of the best typed seed. A seed
  too fine is the only one that matters, and it is a disaster: keys past
  its range's quanta each open a range, and a uniform key at shift ≤ 32
  makes one page and one range per key. A hint would hand the developer
  that failure and nothing else.
- **What fragments is the address window.** C, D and F rewrite pages
  scattered over their key space, each landing past its range's 16 MiB
  window and isolated into a range of its own. Fifteen address bits only
  take it to ~0.82 a page; appending files lose locality however wide the
  window.
- **Decided.** One seed (`Shape::DISK`, `Shape::REMOTE`), no hint, and the
  isolation kept: more sub-headers now, reorganized later by the regroup
  (§12). A second encoding of an address — absolute addresses in a table
  beside the entries — was weighed and refused: every reader of an entry
  (lookup, compaction's moves, upload, eviction, the allocator's rebuild)
  would carry both forms, and one that forgot the second would leak or
  double-allocate blocks without an error.

## Alternatives

- **Keep the extendible hash for anchors.** It would mean two structures and
  two recovery paths. Uniform hash keys are the quantized index's easiest
  case, and time-keyed anchors are its best one.
- **A pure log ordered by write time** (Kafka's time index, 12 bytes an
  entry). Reads by `Id` would need a second, mutable map for every record not
  found at its write time: updated anchors, hash-keyed ones, late arrivals.
  This RFC orders by the key itself instead.
- **An LSM tree.** Writes are blind, but merging rewrites every byte once per
  level (10–30×), and a read probes levels. Neither the O(1) read nor the
  write cost would be met.
- **`u64` entries.** RFC 0074's descriptor already costs this, and Kafka's
  index costs 12 bytes. The point of quantizing per sub-header is to halve
  that at least.
- **A learned index with float slopes** (PGM). Its search is approximate and
  needs a correction window. Integer quantization with power-of-two shifts and
  page boundaries on quantum boundaries is exact.
- **Bw-tree deltas from the start.** These are blind writes to cold pages,
  traded for reads of up to two pages. They are deferred (§12), so the first
  version keeps one page per read.

## Open questions

None at this draft.
