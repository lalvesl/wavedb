# RFC 0078 — Ordered lanes over a quantized page index

- **Status:** In progress. Opened 2026-10-30. Phase 1, the index alone
  (`wavedb-storage::ordered`), landed 2026-10-30; phase 2, an ordered lane
  in `PageStore`, is next.
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
  - `wavedb-macros`: the density hint.

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
  its range cannot address it.
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

- **Regroup.** Beside the copy-on-write cleanup, a pass reopens a range's
  pages and rewrites them as fewer, larger pages:
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
5. **The macro seed and the density hint.**
6. **The object store's directory** as the same index.
7. **Measure** against RFC 0074's rows:
   - bytes written per durable and relaxed insert, on micro and shop;
   - RAM per page;
   - read latency, hot and cold.

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
