# RFC 0081 — Commit blocks, slices and the cleaner

- **Status:** Planned. Opened 2026-11-11.
- **Revises:** [0072](0072-the-settle-is-the-durability-point.md) §3–§5
  (a round's window and its header),
  [0078](0078-ordered-lanes-over-a-quantized-page-index-WIP.md) §3–§6 and
  §12 (the sub-header, cuts, the index's changes on disk, the regroup),
  [0079](0079-headers-sized-to-their-change-WIP.md) §2 and §4 (the header
  and its trailer become the commit block's),
  [0071](0071-data-files-by-address.md) §7 (compaction gains the cleaner's
  phases).
- **Keeps:** [0072](0072-the-settle-is-the-durability-point.md)'s premise —
  no journal; a batch is durable when its windows and one barrier are — and
  its deferred frees (§8); [0073](0073-data-shards-and-storage-shards.md)'s
  storage shards; one read per point lookup.
- **Crates:** `wavedb-storage`
- **Code (today):** `checkpoint.rs`, `window.rs`, `shape.rs`, `plan.rs`,
  `ordered/` (`head.rs`, `index.rs`, `reshape.rs`, `fold.rs`, `journal.rs`,
  `persist.rs`), `header.rs`, `header_room.rs`, `trailer.rs`, `restate.rs`,
  `alloc.rs`, `block.rs`, `compact.rs`, `regroup.rs`, `regroup_merge.rs`,
  `lane_read.rs`, `lane_walk.rs`, `page.rs`, `upload*.rs` (`dictionary.rs`
  went in phase 9)

## Summary

A durable round today rewrites, whole, every page it touches — on an
append-only lane that is the tail page of each lane, 20–50 KiB to carry a
few hundred bytes — and then writes a header block and a roots block. This
RFC makes a round write **one commit block**: whole blocks holding the
round's new data, a section per type, followed by the diff of the index and
the trailer. The index points straight into it. A small commit is **one
block**.

Two index mechanisms make that possible without rewriting old pages:
- **Fragments.** What a round appends to a lane is a new entry pointing at
  the lane's section of the commit block.
- **Slices.** What a round changes in the middle of a page — an update, an
  insert, a removal — becomes a slice laid over the range that names the
  page: the changed quanta, routed to the commit block, about 12 bytes in
  RAM. The old page is not rewritten, and the range keeps its entries.

A commit block is then named by several lanes, so the allocator **counts
references** — derived in RAM from the indexes at open, never stored.

Fragments, slices and shared runs cost RAM and spoil compression and scans.
The **cleaner** pays that back: in idle time, on a schedule, or when an
index budget is passed, it consolidates each range into full pages of one
type, which clears its slices and fragments, the shared references and the
small sections. The engine is write-optimised while it is busy and
read- and memory-optimised once it has rested.

The target is one write per durable insert, one per durable update, one
read per point lookup, and RAM spent on caches rather than on the index.

## Motivation

**What a durable write costs now** (`micro`, `small`, single, `32f83e5`,
`benches/results/index.md`):
- 159.1 KiB written per durable insert, 163.6 per update; 129 inserts/s.
- About 80 KiB of that is the filesystem's: on the bench host's btrfs a
  Valkey AOF append of ~300 B costs 80.7 KiB per `fsync`. The other systems
  sit at 79–92 KiB and ~260 inserts/s.
- The engine's own ~80 KiB per round, by the probe: the lane's tail page
  ~50 KiB (recency, ~4 300 entries), the records' tail page ~21 KiB (~350
  records), the header ~4 KiB, the roots block 4 KiB.

So a durable write pays twice the others' bytes and half their rate, and the
difference is the tail pages.

**What a relaxed round costs** depends on how dense it is. Sequential keys
touch about one page per lane, so a dense round is nearly free (0.1 KiB per
insert). Random keys touch every page of a lane: a round of ~2 650 updates
rewrites ~8 MB, close to the whole store (3.0 KiB per update). A sparse
round degrades toward the durable cost.

**Page fill depends on how a lane was written.** A page over target is cut
in its middle. On an append-only lane written a record at a time, the left
half is never written again, so every page stays half full. The same data
compacted takes 33.1 MB when written durably and 17.9 MB when written
relaxed.

**What must not change:**
- one read per point lookup;
- no RAM per record (the index stays one entry per page, plus a bounded
  slack);
- no journal: the data is written once in the hot path, not to a log and
  again to a page;
- RAM is for the caches.

## Design

### 1. The commit block

A round's window becomes, in whole 4 KiB blocks:

```
[ envelope | section directory | sections … | index diff | trailer ]
```

- **Envelope:** the block count, the round's batch, a crc over the whole
  window. A torn window fails its crc; the batch it belongs to is the newest
  and is cut, as today (RFC 0072 §6).
- **Section directory:** one row per type (slot) with data in the block:
  `(struct_hash, offset, length, raw_length, codec)`. A slot has at most one
  section per commit block.
- **Sections:** each a page body as today (`SlotPage`'s columnar layout),
  holding the records or entries the round adds or changes in that slot,
  sorted by key, compressed or raw (§9).
- **Index diff:** the header of RFC 0079, unchanged in meaning: the
  `RangeRecord`s the journals drain — sub-headers created, changed or
  dropped, entries written — plus the restatement the spare room carries
  and the horizon.
- **Trailer:** RFC 0079's, in the last block: it says how long the commit
  block is, so the open finds the newest from the file's last block.

A `HeaderLink { at, blocks }` names a commit block. The walk of a reopen
reads commit blocks where it read headers. A small commit is one block, the
size of today's smallest header, so the walk costs what it costs now.

**Own storage drops its roots block.** A store writing its own files finds
its newest root from the last block already (`roots_find`); the `Roots`
record it wrote in a separate block is carried in the commit block's diff.
A storage thread still ends each storage write with a roots block naming
every data shard's root (RFC 0073 §4): it is one block per storage write,
shared by every data shard in it.

**Inline or paged.** A slot's section stays in the commit block while it is
small. A round whose slot data reaches a page target writes that slot's
data as pages of its own, as today — well formed, full, one type — and the
commit block holds the rest and the diff. The commit block is capped like
the header today (`MAX_HEADER_BLOCKS`, 32 blocks); what does not fit goes
to pages.

### 2. Fragments: appending without rewriting the tail

On a lane keyed by time — NonUnique anchors, recency, the removal log — a
round's keys all lie past the lane's last key. Today they are merged into
the tail page, which is rewritten. Here they become a **fragment**: a new
entry after the last one, quantum-aligned at the first new key, pointing at
the commit block's run. The lookup finds the slot's section through the
directory.

The `u32` entry is unchanged: it addresses blocks (`q | address | size`),
and the slot is known from the lane, so no entry names a section.

A lane appended a record per durable round grows a fragment per round. That
is RAM per write, and is bounded by §6.

### 3. Slices: changing the middle of a page without rewriting it

A round that changes keys inside a page `P` — an update of a record, an
insert into a secondary or a list, a removal — does not rewrite `P`, and does
not split the sub-header `R` that names it. `R` keeps its entries, its
quantization and its prefix; it gains a short list of **slices** laid over
its entries:

```
before:   R [a ──────────────── c)    … → P → …

after:    R [a ──────────────── c)    … → P → …      (entries unchanged)
            slices: [ q(k)..q(k′) → C ]               (the round's commit block)
```

```rust
struct SubHeader {
    // … its fields (§3a) …
    /// Quanta that no longer read from their entry's page. `None` in a
    /// clean range: 8 bytes, no allocation.
    slices: Option<Box<[Slice]>>,
}

/// Quanta `[from, to)` of the range, and where they read from now.
struct Slice {
    from: u16,
    to: u16,
    /// Blocks past the range's `base_block`. A commit block is always newer
    /// than the range's pages, so it lies ahead; `u32::MAX` is a hole.
    offset: u32,
    /// The run's length in blocks (a commit block is at most 32).
    blocks: u8,
    /// In a counted lane: the keys the slice holds, and the keys of the
    /// entry's page it hides (`COUNT_SATURATED` when the round does not
    /// know).
    count: u8,
    replaced: u8,
}
```

A slice costs about 12 bytes. Splitting `R` into three sub-headers instead —
the halves before and after the change, both still naming `P`, and one for
the change — would cost two sub-headers, ~400 bytes, and a second reference
to `P`.

- **A point lookup** finds `R`, then searches its slices by the key's
  quantum: a slice routes the key to its run (the slot's section in the
  commit block), and a key in no slice reads its entry's page, as today.
  One read either way.
- **A new slice over older ones** trims or replaces them, so a range's
  slices stay sorted and disjoint.
- **A removal** is a hole slice. It writes no data, only its diff.
- **Coarse quanta.** On a finely quantized lane — keys by instant — a quantum
  isolates one key. On a coarse one, a quantum may hold neighbours of the
  changed key; the slice then carries them too, copied from `P`, so it covers
  whole quanta. That needs `P`'s records, which the round reads anyway when
  it plans an update against the stored version.
- **On disk,** a range's slices ride the commit block's diff: the
  `SubHeaderRecord` names the slices it adds or drops, ~9–12 bytes each, and
  the `Rebuild` lays them back.
- **Restated with their range.** A range's slices are part of its state, so
  the rotation that restates the range (RFC 0072 §5, RFC 0079 §5) restates
  its slices with it, whole, in the same record. A restatement that carried
  the entries alone would let the walk's horizon pass the commit block that
  made a slice: the open would rebuild the range without it, and read the
  old version still held in `P`. So a range's restatement grows with its
  slices, and a restating header that cannot hold a range and all its slices
  skips the range rather than splitting them. That is one more reason a
  range's slices are bounded (question 9).
- **The cleaner** rewrites `P` with its slices applied and clears the list
  (§7). The range is clean again; nothing about it is rebuilt.

**Clipped reads.** A page is now read through an interval narrower than its
keys: its entry's quanta, minus the slices over them. Every reader that
walks a page's records clips them so:
- scans (`lane_read.rs`, `lane_walk.rs`);
- the lists' counts: a page covered whole counts `count − replaced` of its
  slices plus their `count`, in RAM; only a `replaced` the round did not know
  (`COUNT_SATURATED`) makes the page be read;
- the upload, which rebuilds the object store's pages by key range;
- the merge, the regroup and the moves of compaction.

A point lookup needs no clipping: a slice routes it before its entry does.

### 3a. The sub-header in bits

Today a range is quantized by two numbers that move at different steps
(RFC 0078 §3): its `prefix` in whole bytes (the depth), and `key_shift` in
steps of 16 bits within the 8 bytes after it, from a `first_key` the range
starts at. Refining past a shift of 0 sinks the range 8 bytes deeper, a case
of its own (`reshape.rs`, `refine`).

The same quantization, normalized:
- **The prefix is a string of bits:** every bit the range's keys share.
- **The quantum is the 16 bits after it.**
- **`start: u16`** is the quantum the range begins at.

```rust
struct SubHeader {
    base_block: u64,
    /// The bits every key of the range shares, padded to whole bytes.
    prefix: Box<[u8]>,
    /// The prefix's length in bits; the quantum is the 16 bits after it.
    prefix_bits: u16,
    /// The quantum the range begins at.
    start: u16,
    address_bits: u8,
    address_factor: u8,
    size_factor: u8,
    slices: Option<Box<[Slice]>>,
}
```

`first_key` and `key_shift` go: `q` is the 16 bits of the key at
`prefix_bits`, and a refine adds 16 to `prefix_bits`, always. The forms are
the same quantization (`prefix_bits = 8·depth + 48 − key_shift`, for a
`first_key` aligned to its quantum). One thing changes: a range no longer
crosses the edge of its prefix. A stream running past it opens the sibling
range, once every 65 536 quanta.

Ranges of every prefix length sit in one sorted list, found by one binary
search over their starts (`prefix` then `start`): an adaptive radix tree,
flattened. The prefix's storage stays a `Box<[u8]>` per range; a better
model is left open (question 8).

### 4. What a round writes

| change | today | here |
|---|---|---|
| append to a time-keyed lane | the tail page, rewritten | a fragment in the commit block |
| update of a record (anchor rewritten, old version archived) | the record's page, the archive's page, the recency's two pages | slices and fragments in the commit block |
| insert into a secondary or a list (middle) | the page, rewritten | a slice in the commit block |
| removal | the page, rewritten | a hole slice: the diff alone |
| a slot's data past a page target | pages | pages, full (§8) |

A durable round touching a few lanes writes one commit block: a block or a
few, plus the filesystem's barrier.

### 5. Counted references, derived

A run can be named several times: a commit block, by the fragments and slices
of every lane whose section it holds. A page a slice lies over is still named
once, by its entry. The allocator frees a run when nothing names it any
more.

**The count lives in RAM and is never written.** The indexes are rebuilt
from the commit blocks at every open (RFC 0072 §6), and a run's count is
the number of entries and slices naming it. The walk counts them as it
rebuilds, so the count costs no write. The extent tree a filesystem like btrfs keeps on disk
for this, and the writes that update it, have no counterpart here.

- Runs named once — nearly all of them in a cleaned store — carry no
  counter: a map holds the counts of the runs named more than once.
- The install of a round increments the count of every run its new entries
  and slices name, and decrements those its retired ones named.
- A run whose count reaches zero is retired: a deferred free, a hole once
  the next batch is durable (RFC 0072 §8), as any run now.

### 6. The index budget

Fragments (an entry, 4–5 bytes) and slices (~12 bytes) are RAM per write.
They are bounded by an **index budget** (`StoreOptions::index_budget_bytes`),
accounted beside the caches:
- **Under the budget,** they accumulate. The cleaner (§7) pays them back
  when it runs.
- **Past it,** the rounds **consolidate inline**: each round also
  consolidates the most fragmented ranges, until the index is back under
  the budget. It pays the write amplification then, which is today's cost,
  not worse.

The budget makes the bound explicit: the index is never more than the
budget plus the clean index of the data. A store under a write burst with no
idle time behaves as today; one with idle time does better.

**Consolidating a range** reads the pages its entries and slices name — its
pages, its fragments, the commit blocks behind them — and writes the range's
live keys as full pages of one type (§8). Its slices clear, its fragments
fold into the new pages' entries, and its references drop to one each.

### 7. The cleaner

Compaction (RFC 0071 §7, `compact.rs`) runs in steps, is safe across a crash
by the batch, and already restates, merges, moves and regroups. The cleaner
adds two phases to it:
- **Consolidate:** ranges with fragments or slices, consolidated as in §6.
- **Unshare:** commit blocks still named by several slots' entries. Each
  slot's live section is consolidated into that slot's pages, so the block
  is named by nothing and goes.

**When it runs:**
- in idle time, between a node's jobs, as compaction steps today;
- on the schedule compaction already has (daily UTC time);
- on the index budget (§6): inline for its merges, and for the ranges a
  regroup folds, by asking for the cleaner;
- on space, as today.

**What it takes first:** the log-structured filesystem's question, answered
by cost and benefit. The benefit of a range is the RAM it frees (slices and
fragments) and the space (dead versions in the pages its slices lie over and
in shared commit blocks). Its cost is
the bytes to rewrite. Ranges with the most benefit per byte go first; hot
ranges, still being written, can wait.

**An address is an age.** Writing only appends, on the disk and in the
object store's numbered files, so how far behind the newest block a page
lies says how long ago it was written — the age the log-structured
cleaner weighs, for free. The walk that finds what to merge reads every
entry's address anyway: a range whose pages all lie behind the newest
quarter of the files has not been written since, and is cold; a data file
far behind is old, and its live pages are cold too — moved once, they stay
put, while a young file may still empty by itself.

Every data shard cleans its own indexes. Commit blocks never mix data
shards: a storage thread gathers each data shard's commit blocks into one
storage write, but each is whole.

### 8. Full pages

The pages a round or the cleaner writes should be full:
- **The rightmost split.** When the page being cut is its range's last and
  the round added only keys past its old last key, it is cut at the target
  (the left page full, the right one holding the rest), not in its middle.
  This is the B+tree append split (PostgreSQL's fastpath, the 90/10 split).
- **The cleaner packs cold ranges full.** Today's merge only joins
  neighbours that together fill at most half a page, so a write does not cut
  them apart again. The cleaner writes a consolidated range as full pages;
  it takes ranges no round is writing.

### 9. Compression

Sections in the hot path and pages from the cleaner want different things:
- **A section below `COMPRESS_MIN` bytes is raw.** Compressing a few hundred
  bytes without a dictionary gains nothing.
- **A larger section, and every cleaner page,** is compressed with zstd at a
  low level and no dictionary. A page grows past the size where a dictionary
  helps once the cleaner fills it. A durable batch on a node gathers every
  write that waited for its barrier, so its sections are often large.

That removes the per-type dictionaries:
- the dictionary as a page of its own, versioned by prefix length;
- its slot in the window and its moves in the header;
- the object store's dictionaries.

It also lightens the read path: in the probe of the `read_hot` phase, 32% of
the misses went through a page read with its zstd decode.

**Decided by measurement** (phase 0): ratio, compression and decompression
time — with a dictionary, without one, raw — over real pages of 4, 16, 32 and
128 KiB. If a dictionary still wins much on cleaner pages, it stays there
alone, trained by the cleaner, off the hot path.

**Measured** (`cargo bench -p wavedb-storage --bench compression`, the
micro workload's records, the dictionary sampled from other records of
theirs, 64 KiB; 2.3 GHz):

| page | stored / records: dict · none · raw | encode: dict · none · raw | decode: dict · none · raw |
|---|---|---|---|
| 4 KiB | 0.251 · 0.271 · 1.023 | 248 · 36 · 4.2 µs | 13.1 · 10.0 · 0.69 µs |
| 16 KiB | 0.221 · 0.223 · 1.015 | 362 · 120 · 20 µs | 42 · 34 · 2.3 µs |
| 32 KiB | 0.214 · 0.213 · 1.013 | 472 · 208 · 53 µs | 76 · 62 · 4.7 µs |
| 128 KiB | 0.210 · 0.215 · 1.012 | 1 136 · 799 · 274 µs | 272 · 224 · 19 µs |

The dictionary buys nothing from 16 KiB up and 8% at 4 KiB, and costs at
every size: about 210 µs a page to encode — the compressor loads the 64 KiB
dictionary every call, seven times the encode of a 4 KiB page — a quarter
more to decode, and its image written whole by every round it grows (7 to
11 blocks a round while a type is young, in the micro row's probe). So no
dictionary, on cleaner pages either: they are the 32 and 128 KiB rows.

It is also simpler to make durable: a page compressed against a dictionary
can be read only with that dictionary, so a write would be durable only
once the dictionary state its pages name is too. A page zstd'd alone needs
nothing but its own bytes.

### 10. The object store

Nothing changes in the object store's files. The upload rebuilds 128 KiB
pages by key range from the flagged entries (RFC 0070 §5); it reads commit
blocks' sections and clipped pages as it reads pages today. Object mode has
no disk and writes no commit block.

### 11. What it costs, and what it saves

| | today | here |
|---|---|---|
| durable insert | tail page per lane touched + header + roots (~80 KiB of the engine's own) | one commit block: one block or a few |
| durable update | the record's, the archive's and the recency's pages + header | one commit block |
| point read | 1 read | 1 read |
| scan | full pages in sequence | plus one read per fragment, until cleaned |
| RAM | one entry per page | that, plus at most the index budget |
| deferred cost | none | the cleaner's rewrite, about once per byte, in idle time |

The filesystem's barrier stays: on the bench host's btrfs a commit block of
4 KiB still shows as ~84 KiB per durable insert. The gain shows as the rate
(the other systems' ~260 durable inserts/s) and in the engine's own bytes.

## Phases

Each phase keeps a durability test: a crash at every point of a window, and
a torn last block.

0. **Measure.** A census of pages at the compacted point of every WaveDB row
   (pages, fill against the target, ranges, sub-headers, index bytes, bytes
   per block, by lane). The compacted point of the multi and shop rows. The
   compression experiment of §9. **Landed:** the compacted point of the
   multi and shop rows, and the census (`PageStore::census`, the rows'
   `census`, RFC 0065 §13), and the compression experiment
   (`benches/compression.rs`, §9: no dictionary).
1. **The rightmost split** (§8). Independent and small.
2. **The sub-header in bits** (§3a). `prefix_bits` and `start` replace
   `first_key` and `key_shift`, in RAM and in `SubHeaderRecord`; the refine
   has one case. No behaviour changes. **Landed:** an entry's `q` is the
   absolute quantum, so a range cut, refined or split keeps its entries as
   they are, and two ranges fold by joining them; a fresh range opens at
   the whole bytes it shares with the one before (as the old depth did).
   `Shape` lost `key_shift` and is three bytes on disk.
3. **Clipped reads** (§3). Every reader clips a page to its entry's
   interval less its slices; `SubHeader` gains `slices`, empty. No behaviour
   changes yet. **Landed, the clip by coverage:** whatever takes a page's
   records to write them again — a round's routing, the regroup's merge,
   the upload — reads them through its entry (`pages::clip`); scans and the
   eviction already kept to the coverage, and a point lookup is routed. A
   fragment needs it before any slice exists: when the round's first new key
   shares a quantum with the tail page's last, the fragment takes that
   quantum and the tail page keeps keys its entry gave up. The `slices`
   field comes with phase 6, which uses it.
4. **Counted references** (§5). Several entries and slices may name one
   run; the counts are derived at open and kept at install. **Landed with
   the packs** (`shares.rs`): a map of the runs named more than once — the
   names past the first — counted from the indexes at every open, raised by
   the round that places a pack, and lowered by `release`, which every
   page run an entry stops naming goes through (a round's retired runs, an
   eviction); the run is retired when the last name goes.
5. **The commit block** (§1, §2). The format, sections inline, fragments
   for appends, the header and the `Roots` of `Own` folded in.
   **Landed first, fragments as pages of their own** (`fragment.rs`): a
   settle's round whose records for a page all lie past its last key leaves
   the page and writes them under a new entry cut at the quantum boundary
   between them — taking along the page's keys of the quantum they share,
   the page keeping them past its entry. A page of one block is rewritten
   instead (the fragment would cost the same block and an entry more), so a
   fragment grows to two blocks before the next one starts; a merge and an
   upload take no fragments. A durable append of 1 KiB past a ten-block page
   (`tests/fragments.rs`) went from 8.9 blocks to 3.3: the fragment, the
   header, the roots block.
   **Then the sections, as packs** (`pack.rs`, `window_pack.rs`): of a
   settle round's pages that fit one block, the smallest of each slot's go
   into one run — a section per slot, each a whole page image with its own
   crc, as many as take fewer blocks together than apart — and every
   member's entry names the pack. A run starting with a tag no page length
   can have is a pack; every reader takes a page through `page_image`, by
   its slot, and the page cache keys a page by run and slot. A compaction
   carries a pack out a section at a time, each as a page of its own, so
   no move repoints several slots at once. The micro row's durable insert
   (probe, 4 000 rows, NVMe) is three blocks a round — the pack of the
   anchor's, the recency's and the secondary's pages, the header, the roots
   block: 12.1 KiB, from ~80 KiB of the engine's own before fragments
   (25 KiB with fragments alone). A durable update is still 137 KiB: the
   anchor page rewritten for the record in its middle, which slices take.
   **The roots block of `Own`, gone** (`roots_find::find_own_root`): a store
   writing its own files roots in its newest whole batch's ending header,
   found back from the header its files end with by `prev` — a header the
   walk cannot read is the newest write cut, and the walk looks below it;
   a header records its own size (`Header::blocks`) so the one found from
   the files' end can be named. A storage thread still ends each write
   with one roots block for all its data shards. The micro row's durable
   insert is two blocks a round, the pack and the header: 8–10 KiB; its
   durable update 13.6 KiB. Folding the header into the pack — one block —
   is what is left of the commit block. A finding on the way: while a type's dictionary grows, every round
   writes it whole again (11 blocks a round in that test, compressed) — §9's
   case for no dictionary in the hot path.
6. **Slices** (§3, §4). Updates, middle inserts, removals, with their counts.
   **Landed — as entries, not an overlay** (`partial.rs`; for review). A
   round that changes a page in one span of its keys writes that span
   alone, under an entry of its own, and the page stays named by the entry
   before the span and by a new one after it. Two entries cost what a
   `Slice` would (~5–6 bytes each against ~12–16), and nothing new reads
   them: a lookup is routed by entries, whatever walks a page's records
   takes it through its entry (phase 3), counts are per entry, the journal
   records and restates them as it does any entry, the upload, the
   eviction and the moves already handle them, and a page named twice is
   what the counted references count. Fragments (phase 5) are the span past
   the last key. A span widens to a quantum boundary where a cut cannot
   part two keys; one emptied by removals is a hole (never dropped: its
   quanta would go back to an entry naming the page). A page of one block
   is rewritten whole, so is one whose span would not take fewer blocks,
   and so is one that would keep less than half of what it held live — the
   bound on the dead space a span leaves in its page until the cleaner
   (phase 8). One cost to know: every new entry changes its range's shape, so its range is recorded
   whole in the round's header (up to 1 024 entries, ~6 KiB) — the micro
   row's headers grow from 4 to ~7.6 KiB a round. The index diff of §1 —
   entries inserted, not ranges restated — is where that goes.
   The micro row's durable update (probe, 4 000 rows) is 19.1 KiB a round,
   from 137: the pack, the header, the roots block.
   **What entries cannot do, and who pays for it.** An entry addresses
   12 bits of blocks from its range's base, 16 MiB. A span's run lands at
   the end of the active file; the page it lies over may be far behind.
   Then no entry of the range can name the span, `isolate` gives it a range
   of its own, and the page's range is cut in three: the part before, the
   span, the part after — both parts as large as before. A first answer
   was a guard: a span only where the range reaches where the round's
   pages land, a page farther behind rewritten whole. It made the update
   of a cold page pay its page again, and was dropped: **a write pays its
   span, and the ranges it leaves are the cleaner's.** The overlay with a
   wide offset (§3) is dropped too — a slice is an entry here, and the
   alternative to an entry the range cannot name is one range more, not a
   second kind of reference. Measured (`tests/far_spans.rs`: 2 000 records
   of ~1 KiB, 24 MiB of another type after them, then 666 durable updates
   at random): the updates wrote 1 339 blocks, about two each against a
   page's six or seven, and took `A`'s index from 4 ranges and 843 B to
   688 ranges and 80 KB. The compaction then gave nothing back (581
   ranges): the regroup took only runs of ranges each under 8 pages, and
   the parts either side of a span are larger; a merge of the page with
   its span lands far behind again, and is isolated again. So the regroup
   takes **every two neighbouring ranges one of which is small**
   (`PageIndex::scattered_runs`) — the parts either side go with their
   span, two large ranges side by side are not worth a move — as many as
   one window addresses, moved 128 pages a round; a page
   named either side of a span is moved once under both names
   (`PlannedPage::also`, counted by `shares`), not a copy each; and a
   merge after the regroup (`Phase::Consolidating`) takes the page with
   its spans, now in one range and landing where it reaches. The same
   compaction leaves 4 ranges and 1 088 B. Until something better is
   found, that move — the whole cut range — is the price of a span over a
   cold page, paid on idle time.
   Two read-path fixes came with it: a round reads the pages it loads out
   of the page cache instead of taking them out (a page rewritten in part
   stays, and is read next), and a run an entry stops naming while others
   still do loses only that slot's image from the cache — a durable micro
   row's cold reads had 170 page misses of 2 000, now none.
7. **The index budget and inline consolidation** (§6). **Landed**
   (`consolidate.rs`, `StoreOptions::index_budget_bytes`, 4 MiB by
   default). With spans and fragments as entries, what the budget counts
   is the index's RAM past a clean index of the same data: the entries a
   merge would give back — runs of small neighbouring pages, and a page a
   span left named either side with the spans over it (the compaction's
   `small_runs`) — at `PageIndex::entry_bytes` each (`ram_bytes` is the
   whole index's, the census's too). Finding them walks every entry, so a
   batch does not: a walk notes the clean part (what the index held, less
   what it found) and the next is due only once the index passes that by
   the budget. Past it, a batch's first round is a merge round — the
   compaction's `plan_merge`, up to 64 runs of the slot that gives back
   most, each checked again — and its records then settle onto the merged
   pages. One such round a batch: the whole-page rewrites of before the
   spans, paid only while over budget. A crash after it reopens from the
   batch before (`tests/index_budget.rs`). Holes are not counted yet: a
   merge does not take them (still open, with the cleaner). The ranges a
   regroup would fold (phase 6) count too, at `PageIndex::range_bytes`
   each, but no batch pays for them — folding them moves whole ranges.
   A walk that finds them asks for the cleaner
   (`PageStore::wants_regroup`, a node's compaction trigger) and counts
   them clean until it runs, so the batches after it do not walk again.
8. **The cleaner's phases and triggers** (§7, §8). **Landed, in the
   compaction it extends:**
   - *Two zstd levels* (`page::Compression`): the write path's (3) and the
     cleanup's (19, short of the ultra levels a page cannot use — 19
     against 22 is for the closing measurement). A compaction's merges and
     moves take the cleanup's: a moved page is decoded and compressed
     again rather than copied. The inline consolidation of phase 7 stays
     at the write path's, since a write may wait on its batch.
   - *Age by address* (above): the files to empty go by
     `(1 − u) · age / (1 + u)` (`defrag::worth`), and a merge takes a
     whole page of a cold range (`merge_runs::small_runs`, below
     `cold_line`), half of a recent one.
   - *Holes go with the merge*: the holes among and after a run of small
     pages are taken with it — removed, their quanta to an entry the merge
     rewrites — and runs of holes alone close up at once (the index
     installed, the next header naming it). A hole still to be told to the
     object store parts runs. The budget counts them.
   - *No restatement without a file to empty*: a store of its own files
     restated every address on every compaction; with no file worth
     emptying it now goes straight to the merges and the regroup, as a
     storage thread's data shard did.
   - *The regroup takes the ranges spans cut* (phase 6): neighbouring
     ranges one of which is small, a page named twice moved once, and a
     merge after it — the consolidating step — for the page and its spans.
   - *The quiet trigger* (`Compaction::after_quiet`, 30 s): a store written
     to and then left alone is compacted once per quiet spell — the idle
     time the cleaner is for — beside the daily, ceiling and free-space
     triggers and the index budget (phase 7). Writes are the user's
     (`PageStore::has_pending`), never the cleanup's own windows.
   Not built: the *unshare* phase on its own — a compaction's move already
   carries a pack out a section at a time, and a merge takes a pack's
   section with its neighbours; a pack whose sections all stay live keeps
   its few blocks. Nor are a file's ranges rewritten whole by key range
   when it is emptied: its small runs are merged before the moves, and the
   rest is moved run by run, compressed again — the entries kept.
9. **Compression** (§9), as phase 0 decides. **Landed:** no dictionaries
   anywhere — `dictionary.rs`, each type's dictionary state on disk and in
   the object store, its slot in the window and in the header, its moves in
   compaction and the defrag are gone. A compressing type's page is zstd'd
   alone (level 3), and stored raw when that does not shrink it; there is
   no `COMPRESS_MIN` yet, since the raw fallback already keeps a small page
   from growing. `PagePayload::Zstd` lost its `dict_len`: pages written
   before do not read (no format bump, by policy). `benches/compression.rs`
   now compares zstd and raw. Without a dictionary's old states keeping
   old files live, the compaction tests remove 60% of their records instead
   of 40%, so each file is still more than half holes, and the storage
   shards' test removes whole runs of keys (scattered removals rewrite a
   span, the page kept live either side). The dictionary had hidden two
   faults, both fixed here: an upload of removals alone placed no page, so
   it opened no file and wrote no header, yet cleared its flags — the
   removed records came back once the disk was lost (`layout` now opens one
   file at least); and the active file, all holes but its live header (its
   pages evicted to the object store), was never closed for a compaction,
   since only a file holding a run to move was one to empty — the
   dictionary's image had been that run (`candidates` now counts the
   active file and a live header).
10. **Measure.** The micro and shop suites, the opens, the census.

## Alternatives

- **A journal** (proposal 2: a durable batch writes its records to a bounded
  log, pages settle later). It writes every byte twice in the hot path, and
  it reverses RFC 0072. Here the data is written once, where the index then
  points; the second write is the cleaner's.
- **Bw-tree delta chains.** A delta is a record prepended to a page's chain;
  a read walks the chain. Here the index routes past the old page, so a read
  is one page.
- **An LSM tree.** Reads consult several levels and need bloom filters in
  RAM. Here a key has one entry.
- **Append blocks to the tail page** (a page as a prefix plus blocks added
  later). A read of the tail needs the prefix and the added blocks: two
  reads unless they lie together, and they do not on an append-only file.
- **In-place pages with a doublewrite buffer** (InnoDB). Pages are rewritten
  in place and protected by a second write; it contradicts append-only files
  (RFC 0071).
- **A refcount tree on disk** (btrfs's extent tree). Every count change is a
  write. Here the counts are derived from what the open rebuilds anyway.

## Open questions

1. The commit block's cap, and the size past which a slot's data goes to
   pages of its own.
2. The index budget's default (4 MiB a store for now, unmeasured), and how it
   is shared among a node's data shards.
3. How many fragments a range may hold before a round consolidates it, even
   under the budget, so scans of hot ranges stay short.
4. `COMPRESS_MIN`, and whether a dictionary survives on cleaner pages
   (phase 0).
5. Whether restating headers become commit blocks of their own or ride the
   round's.
6. The cleaner's cost-benefit weights, and how it tells a hot range from a
   cold one.
7. Torn writes of one block: atomic on a disk with 4 KiB sectors, caught by
   the crc otherwise. Whether a device with 512 B sectors needs anything
   more than the crc.
8. How a range's prefix is held. It stays a `Box<[u8]>` per range (§3a);
   neighbouring ranges of one collection repeat the same bytes, ~32 B each.
9. How many slices a range may hold before it is consolidated, so its
   search stays short. (Slices are entries now, phase 6: a range holds at
   most 1 024 of them, and its span count is its entries'.)
10. A cheaper answer to a span its range cannot name than moving the whole
    range on idle time (phase 6).
