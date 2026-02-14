# RFC 0074 — Extendible hashing for the page directories

- **Status:** In progress (opened 2026-10-17; phase 1, the descriptor, landed
  2026-10-18)
- **Revises:** [RFC 0049](0049-elastic-pages-and-load-driven-splits.md) (the
  split's trigger and its pacing; elastic pages stay),
  [RFC 0018](0018-storage-engine.md) (the directory's addressing)
- **Builds on:** [RFC 0072](0072-the-settle-is-the-durability-point.md) (the
  header and the walk that rebuilds a directory),
  [RFC 0070](0070-object-storage-backend.md) (the object store's own directory)
- **Crates:** `wavedb-storage`
- **Code:** `crates/wavedb-storage/src/{block,directory,plan,header,walk,restate,disk_header,upload_plan}.rs`

## Summary

Every page directory moves from linear hashing to **extendible hashing**. The
directory stays a `Vec<BlockDescriptor>`, now always 2^d entries long and
indexed by the hash's low d bits. Several entries may point at one page. A page
over target **splits at once, by itself**, during the round that wrote it. The
directory doubles by copying itself (`extend_from_within`), and no record moves
when it does.

Each page's **local depth ℓ** goes into its descriptor. To make room, the
descriptor is repacked:

```
bits 63..18  start    u46  first block (4 KiB blocks: 256 PiB)
bit  17      pending  u1   not uploaded yet (RFC 0070)
bits 16..10  ℓ        u7   local depth
bits  9..0   size     u10  blocks = max(N, round(2^(N/64))), ≤ ~253 MiB
```

The decode involves no float anywhere: the size code is read through an
integer table.

## Motivation

### The split pointer cannot be aimed

In linear hashing the bucket that splits next is derived from the directory's
length (`next_split_bucket`), because addressing depends on that length alone.
RFC 0049 made this safe: a bucket over target splits only when its turn comes.
It also made it slow, because a bucket that fills up waits about N/2 turns for
the pointer. Meanwhile it keeps growing, and every round that touches it
rewrites the whole page.

The settle-round probe measured the cost on the micro workload at 60 000
records (`scripts/probe_summary.sh`), in KiB written per durable insert:

| component | KiB/insert |
|---|---|
| B+tree nodes | 110 |
| index chains | 30 |
| records | 27 |
| header | 32 |
| pivot + roots | 8 |
| **total** | **~213** (305 at 200 000) |

The target is 8 blocks, or 32 KiB. Yet the B+tree buckets hold 3–5 nodes of
~28 KiB each, and the chain buckets hold 10–20 segments. Every insert touches
one node, and that rewrites its whole bucket. The cost grows with the database
because the pointer's lap grows with it.

### The obvious fixes and why they fail

- **Split the overflowing bucket out of turn under linear hashing.** Linear
  addressing cannot express that.
- **Trigger on any page and split the turn's bucket.** This was the pre-0049
  behaviour, and it splits ~N/2 innocent buckets to relieve one (RFC 0049).

## Design

### Addressing

With `dir.len() == 2^d`:

```rust
let entry = (hash & (dir.len() as u64 - 1)) as usize;
let desc = dir[entry];
```

Two depths govern the directory:
- d, the **global depth**, is never stored: it is `dir.len().trailing_zeros()`.
- ℓ ≤ d, the **local depth**, is the number of hash bits a page actually uses.

A page with local depth ℓ is pointed at by the 2^(d−ℓ) entries
`j ≡ c (mod 2^ℓ)`. Its **bucket** (its identity, the key every per-bucket map
uses) is the canonical entry `c = j & (2^ℓ − 1)`, the lowest of them.
`Directory::bucket_of(id)` returns `c`, so `staged`, `pages`, the touched sets
and the page cache stay keyed by page as they are today.

`set_descriptor(c, desc)` writes every alias of `c`, which is 2^(d−ℓ) stores.
A new `buckets()` iterator yields `(c, desc)` once per page. Every caller that
walks `slots()` today to visit pages (defrag, evict, convert, the remote
compaction and live counts, the upload) switches to `buckets()`, so no page is
visited twice.

### Splitting

During a round, after the round's records are applied, `plan_splits` looks at
the buckets that have an image. A bucket splits while it meets all of these
conditions:

- its image is over `target_blocks_per_bucket`;
- it holds two records or more;
- its ℓ is below `D_MAX`.

Each split proceeds the same way:

1. **Double first if needed.** If ℓ = d, the directory doubles:
   `slots.extend_from_within(..)`. Nothing else changes, because every
   page's aliases double with it.
2. **Partition by bit ℓ.** Records with hash bit ℓ clear stay at `c`, and the
   rest go to the new bucket `c + 2^ℓ`. Both halves take ℓ + 1, and each half's
   aliases are set.
3. **Re-check both halves.** A half still over target, because every record
   landed on one side, splits again in the same round.

A split costs **no extra IO**. The source page was being rewritten this round
anyway, and its two halves together are the same bytes.
`MAX_SPLITS_PER_ROUND` and the evicted-turn wait both go away: only staged
pages split, and a staged page is never evicted, because a write restores its
bucket before it commits (RFC 0070 §7).

Some pages never split:
- A page holding **one** record never splits; a large record is a large page,
  as RFC 0049 settled.
- A page at `D_MAX = 32` stays over target. This bounds the directory against
  records that share their low hash bits. The seed is per database, so an
  attacker cannot aim at this.
- A page whose split would double the directory waits while
  `dir.len() ≥ DIR_RATIO × pages`, with `DIR_RATIO = 16`. It stays over
  target this round, as RFC 0049 already allowed.

**Why the ratio.** A directory indexed by the low bits is as long as its
deepest page needs, whatever the volume of data. With records near the target
(two of 20 KiB already pass 32 KiB), every pair must split until their hashes
differ, and among n records some pair shares about 2·log₂ n low bits: the
directory grows as n². At 15 000 such records per shard that is d ≈ 28, 2 GiB
of directory per type. Growing the directory one entry at a time, linear
hashing style, does not help: a page of depth ℓ still needs the entry
`c + 2^(ℓ−1)`. The ratio caps the directory at 128 bytes of RAM per page, and
it releases itself: pages below d still split without doubling, which raises
the page count. With records of ~4 KiB (8 a page) the natural ratio is about
11, so the cap rarely binds.

The page count is kept by the `Directory` itself: one more per split, and
counted from the canonical entries when a directory is rebuilt.

### The descriptor

The layout is the one in the summary, and `EMPTY` is still `size 0`.
- An emptied half keeps its ℓ: `EMPTY` with ℓ set is a page of no blocks that
  still governs its aliases. This removes the ambiguity a derived ℓ would have
  had.
- `EVICTED` stays `start 1 · size 0`. `is_evicted` now masks ℓ out.
- Dictionary runs use the same format with ℓ = 0.

**The size code.** `size` is a 10-bit code N, decoded as
`blocks(N) = max(N, round(2^(N/64)))`:
- below N = 589 the code is the block count itself, so every page up to
  ~2.3 MiB is sized exactly;
- above that the step is 2^(1/64), about 1.09%;
- `blocks(1023)` ≈ 64 830 blocks, ~253 MiB, the largest page.

The page's own envelope carries its length, so the slack of a rounded-up run
is never read.

```rust
/// 2^(k/64) for k in 0..64, Q32 fixed point (1 << 32 = 1.0).
const FRAC: [u64; 64] = frac_table();

const fn blocks(n: u16) -> u64 {
    let n = n as u64 & 0x3FF;
    let grown = ((FRAC[(n & 63) as usize] << (n >> 6)) + (1 << 31)) >> 32;
    if grown > n { grown } else { n }
}
```

`frac_table` is a `const fn`, integer only:
- it takes 2^(1/64) as six successive integer square roots of 2 in Q62;
- it builds the powers by fixed-point multiplication, rounded;
- it narrows them to Q32.

Its output is the same on every target, wasm included, because it is evaluated
at compile time from integers. Floating point is ruled out because `powf` is
not correctly rounded, and two targets could disagree on a stored size.

The encode is the smallest N with `blocks(N) ≥ K`, a binary search over 1024
codes. Nothing past `blocks(1023)` is ever truncated: a record too large for a
page of its own is refused at the commit (`RecordTooLarge`), and a page
that grows past it at the plan (`PageTooLarge`).

### The header and the walk

A doubling would change 2^d entries at once, and naming them all would
overflow the 32 KiB header. Nothing needs naming, though, because ℓ travels in
the descriptor:

- `TypeEntry.buckets: u32` becomes `depth: u8`, the type's d.
- `changed` items stay `(u32, u64)`, but name **buckets** (canonical entries),
  never aliases.
  - A round names the buckets it rewrote, both halves of every split included.
  - A doubling names nothing beyond the new `depth`.
- The walk sizes a type's directory at `2^depth`. For each named `(c, raw)`,
  it fills every still-unknown alias `j ≡ c (mod 2^ℓ)`, where ℓ is read from
  `raw`. An item with `c ≥ 2^ℓ`, or with ℓ above `depth`, is `Corrupt`.

Filling only unknown entries, newest first, is correct because buckets only
split. Take the bucket currently covering entry j. The split that created it
is the last change to j's mapping, and that round named it. Every later header
that names a bucket covering j names that same bucket, because no other bucket
covered j after it. So the first naming the walk meets for j, reading newest
first, is j's current page.

The spare room (`restate`) rotates over buckets instead of slots. A whole
rotation is every bucket named, and `horizon` keeps its meaning.

### The object store's directory

The remote directory (`upload_plan.rs`, `REMOTE_TARGET_BLOCKS = 32`) takes the
same addressing and the same split. `split_remote` no longer fetches an
unstaged source page, because it splits only what the upload staged.

### What it buys

The probe's numbers predict ~100 KiB per durable insert at 60 000 records,
down from ~213:
- every B+tree node gets a bucket of its own;
- chain buckets drop to ~4 segments;
- the cost stops growing with the database.

The rest of the gap to the "few pages per insert" goal belongs to other
designs: the B+tree's node size, the fixed 32 KiB header and the Pivot
rewrite.

## Phases

1. **The descriptor.**
   - Repack the descriptor, and add the size code with `frac_table`.
   - Keep ℓ = 0 everywhere, so behaviour does not change.
   - Tests:
     - the descriptor packs and unpacks;
     - `blocks` is strictly increasing;
     - its golden values are pinned (`blocks(0) = 0`, `blocks(588) = 588`,
       `blocks(1023)`), with no float even in the tests;
     - the encode rounds up.
2. **The directory.**
   - Add extendible addressing, alias-writing `set_descriptor`, `buckets()`,
     doubling, the `DIR_RATIO` cap and the new `plan_splits`.
   - Remove `next_split_bucket`, `split_bit` and `MAX_SPLITS_PER_ROUND`.
   - Convert every `slots()` page walk to `buckets()`.
3. **The header and the walk.**
   - Make `depth` part of `TypeEntry`, name items by bucket, fill aliases in
     the walk and restate by bucket.
   - Durability tests:
     - a reopen after a doubling;
     - a reopen after cascading splits;
     - a cut batch whose round doubled.
4. **The remote directory.** Make the same change in `upload_plan.rs`, and
   update its disk-to-remote mapping test.
5. **Measure.** Re-run the probe and the micro rows on the bench lane, then
   update `benches/results/index.md`. On landing, update CLAUDE.md and the
   storage README, which say "linear-hashed".

No format version is bumped and nothing is migrated: an older database is
unsupported, per policy.

## Alternatives

- **Keep linear hashing (RFC 0049).** It is the cost measured above, and it
  grows with the database.
- **ℓ in a parallel `Vec<u8>`.** This works, but it is a second array to keep
  in step, and the header would have to carry ℓ separately. In the descriptor,
  ℓ travels wherever the address does.
- **ℓ derived from the array** (the smallest ℓ whose aliases all hold the
  same descriptor). It costs zero bytes, but it is ambiguous for empty pages
  and costs a scan per lookup of a bucket.
- **The size as an f64 power.** It is not reproducible across targets, as
  above.
- **The size as exponent and mantissa (4 + 6 bits).** It is integer and
  simple, but coarser (1.56%) and exact only up to 127 blocks. The 2^(N/64) code is
  exact up to 588 blocks, which is where pages live.
- **6 bits for ℓ, 11 for size.** `D_MAX` fits either way. Seven bits were
  kept; a finer size step buys nothing.
- **Shrinking (merging buddies, halving the directory).** Deferred. Linear
  hashing never shrank either. The walk's argument holds for merges as
  well: the round that changes an entry's mapping names its new page.
