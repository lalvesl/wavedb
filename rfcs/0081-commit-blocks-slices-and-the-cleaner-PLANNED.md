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
  `lane_read.rs`, `lane_walk.rs`, `page.rs`, `dictionary.rs`, `upload*.rs`

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
  insert, a removal — becomes a new sub-header covering just the changed
  quanta, pointing at the commit block, between two sub-headers that both
  still point at the old page. The old page is not rewritten; it is shared.

A run may then be named by several entries, so the allocator **counts
references** — derived in RAM from the indexes at open, never stored.

Fragments, slices and shared runs cost RAM and spoil compression and scans.
The **cleaner** pays that back: in idle time, on a schedule, or when an
index budget is passed, it consolidates each range into full pages of one
type, which drops the extra sub-headers, the shared references and the
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
insert into a secondary or a list, a removal — does not rewrite `P`. It
splits the sub-header `R` that holds `P`'s entry, by reference:

```
before:   R  [a ─────────────────── c)    … → P → …

after:    R1 [a ─── k)      … → P                  (R's entries up to P)
          Rk [k ─ k′)       → C                    (the round's commit block)
          R2 [k′ ─── c)     P → …                  (P again, then R's rest)
```

- `Rk` covers the quanta that hold the changed keys, and points at the
  slot's section in the commit block `C`.
- `R1` keeps `R`'s entries up to and including `P`'s.
- `R2` takes `P`'s entry again and `R`'s entries after it.
- `P` is named twice, so its run's count is two (§5). `P` still holds the
  old versions of the changed keys; nothing routes to them.

Point lookups stay one read: a key in `[k, k′)` reaches `C`, any other key
of `P` reaches `P` through `R1` or `R2`.

**A removal** is a slice whose entry is a hole: the quanta hold no key. It
writes no data, only its diff.

**Quantization.** `q = (slice(key, depth) − first_key) >> key_shift`
(RFC 0078 §3). For `R2` to keep `R`'s entries without re-encoding them, a
sub-header's **quantization base is separated from its start**:
- `first_key` stays the base the entries count from;
- a new `start: u16` is the quantum the range begins at. Today every range
  begins at 0;
- an entry may start before `start` (`P`'s, in `R2`), and is clipped by it.

The invariant "a range starts at quantum 0" becomes "a range's entries
cover its quanta from `start`". `SubHeaderRecord` carries `start`.

On a finely quantized lane — keys by instant — a quantum isolates one key.
On a coarse one, a quantum may hold neighbours of the changed key. The
slice then carries them too, copied from `P`, so `Rk` covers whole quanta.
That needs `P`'s records, which the round reads anyway when it plans an
update against the stored version.

**Clipped reads.** A page can now be named by entries whose intervals are
narrower than its keys. Every reader that walks a page's records clips them
to its entry's interval `[this entry's quantum, the next entry's)`:
- scans and counts (`lane_read.rs`, `lane_walk.rs`);
- the lists' counts: an entry's `u8` counts its interval. A slice on a
  counted lane writes `COUNT_SATURATED` (255, "read it") until the cleaner
  consolidates it;
- the upload, which rebuilds the object store's pages by key range;
- the merge, the regroup and the moves of compaction;
- the `Rebuild`, which already reads entries, not pages.

A point lookup needs no change: it never reaches a key its entry does not
cover.

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

A run can be named by several entries: the halves of a slice, and a commit
block named by the fragments and slices of several lanes. The allocator
frees a run when nothing names it any more.

**The count lives in RAM and is never written.** The indexes are rebuilt
from the commit blocks at every open (RFC 0072 §6), and a run's count is the
number of entries naming it. The walk counts them as it rebuilds, so the
count costs no write. The extent tree a filesystem like btrfs keeps on disk
for this, and the writes that update it, have no counterpart here.

- Runs named once — nearly all of them in a cleaned store — carry no
  counter: a map holds the counts of the runs named more than once.
- The install of a round increments the count of every run its new entries
  name and decrements those its retired entries named.
- A run whose count reaches zero is retired: a deferred free, a hole once
  the next batch is durable (RFC 0072 §8), as any run now.

### 6. The index budget

Fragments, slices and the sub-headers they create are RAM per write. They
are bounded by an **index budget** (`StoreOptions::index_budget_bytes`),
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

**Consolidating a range** reads the pages its entries name — its fragments,
its slices, the shared pages behind them — and writes the range's live keys
as full pages of one type (§8). Its extra sub-headers fold away, and its
references drop to one each.

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
- on the index budget (§6), inline;
- on space, as today.

**What it takes first:** the log-structured filesystem's question, answered
by cost and benefit. The benefit of a range is the RAM it frees (sub-headers
and entries) and the space (dead versions held in shared pages). Its cost is
the bytes to rewrite. Ranges with the most benefit per byte go first; hot
ranges, still being written, can wait.

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
   compression experiment of §9.
1. **The rightmost split** (§8). Independent and small.
2. **Clipped reads and the range start** (§3). Every reader clips to its
   entry's interval; `SubHeader` and `SubHeaderRecord` gain `start`. No
   behaviour changes yet.
3. **Counted references** (§5). Two entries may name one run; the counts
   are derived at open and kept at install.
4. **The commit block** (§1, §2). The format, sections inline, fragments
   for appends, the header and the `Roots` of `Own` folded in.
5. **Slices** (§3, §4). Updates, middle inserts, removals.
6. **The index budget and inline consolidation** (§6).
7. **The cleaner's phases and triggers** (§7, §8).
8. **Compression** (§9), as phase 0 decides.
9. **Measure.** The micro and shop suites, the opens, the census.

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
2. The index budget's default, and how it is shared among a node's data
   shards.
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
