# RFC 0079 — Headers sized to their change

- **Status:** WIP. Opened 2026-11-07. Phase 1 landed 2026-11-07.
- **Revises:** [0072](0072-the-settle-is-the-durability-point.md) §3 and §5
  (the 32 KiB header and its spare room),
  [0078](0078-ordered-lanes-over-a-quantized-page-index-WIP.md) §6 ("the
  header stays 32 KiB"), [0073](0073-data-shards-and-storage-shards.md) §5
  (the roots block names each root), [0070](0070-object-storage-backend.md)
  §5 (an object-store file's layout).
- **Crates:** `wavedb-storage`
- **Code:** `header.rs` (`Header`, `TypeEntry`), `disk_header.rs`,
  `disk_chain.rs`, `restate.rs`, `roots.rs`, `roots_find.rs`,
  `recover_headers.rs`, `remote.rs` (`HEADER_BYTES`, `HEADER_BLOCKS`),
  `upload_file.rs`, `reopen.rs`

## Summary

A header is as long as what it names, rounded up to whole 4 KiB blocks,
instead of always 32 KiB. **Every pointer to a header carries the header's
size in blocks**, beside its address: `prev`, each `TypeEntry.last`, each
root in a roots block. A reconstruction that walks back therefore reads each
header in **one** request of the right length, on disk and in the object
store alike. Only the newest header is found without a pointer. It is placed
last, and a trailer in its last block says how long it is, so finding it is
one read of the final block: whole in that read when it fits a block, the
common case, and one more read otherwise. The spare room keeps restating old
ranges, into the free end of the last block. When the walk back to the
horizon passes `K` headers, a **restating header** of 32–128 KiB carries the
rotation forward in one jump, preferably in a round that writes a lot anyway.

## Motivation

The header is a fixed cost of every durable batch. Measured with the probe
on the `micro` bench, single durable at `404a19b`, one insert per batch:

| part of a round | KiB |
|---|---:|
| the lane's tail page (recency, ~4 300 entries) | ~50 |
| **the header** | **32** |
| the records' tail page (~410 records) | ~24 |
| the roots block | 4 |
| **total** | **~110** |

A durable insert changes two or three pages. At 4–8 bytes per changed page
(RFC 0078 §6), what it names is a few dozen bytes. The other ~32 KiB are the
spare room's restatements and zeros. That is ~29 % of everything a durable
write puts on disk, and the share is the same in every durable row of the
suite.

RFC 0078 kept 32 KiB for one reason: "a known size is one read". This RFC
keeps that property, one read per header, by moving the size into the
pointer that leads to each header.

## Design

### 1. A header reference

```rust
/// Where a header is, and how many blocks it takes.
struct HeaderLink {
    /// The block it starts at: on disk, or in the object store's address
    /// space (a file's number times its 2048 blocks, plus the offset).
    at: u64,
    /// Its length in 4 KiB blocks, 1 at least.
    blocks: u16,
}
```

(`HeaderLink`, not `HeaderRef`: the `WaveWire` derive already names
`Header`'s borrowed twin `HeaderRef<'a>`.)

These become `HeaderLink`:

- `Header.prev`;
- `TypeEntry.last`, for the disk's own types and for the `remote` ones;
- `Roots.by_shard[i]`;
- `Header.roots` names a roots block, which is one block, and stays a plain
  address.

`Header.horizon` is a bound that a walk compares against and never reads.
It stays a plain address.

`blocks` is a `u16`, so a header can reach 256 MiB. The cap actually used is
§4's.

There is no format bump, by policy (no `FORMAT_VERSION` change before
release).

### 2. On disk: the trailer, and finding the newest header

A window stays `[pages][header]`, and the header is now `n` blocks. Its last
block ends with a **trailer**:

```rust
struct Trailer {
    magic: u64,  // distinct from the roots block's
    blocks: u16, // the header's length, this block included
    crc: u32,    // over the trailer
}
```

Finding where to root (`roots_find.rs`):

1. Read the newest data file's **last block**. It is a roots block (its
   magic) or a header's last block (the trailer's magic).
2. For a header, the trailer gives `blocks`. When `blocks == 1`, the header
   is already in hand. Otherwise read the `blocks − 1` before it: one more
   read, only at open, and only when the newest header outgrew a block.
3. A torn end is handled as today: read back, a chunk at a time, until a
   roots block or a trailer checks.

Every header after the root is reached through a `HeaderLink`, which is one
read of exactly `blocks`.

### 3. In the object store: the header at the file's end

Today a file is `[roots block][32 KiB header][pages]`. The header names the
addresses of the file's own pages, so with a size known only once it is
encoded, it cannot come first. The file becomes:

```text
[pages][header][roots block]
```

- The roots block moves to the end and gains the header's size, the same
  trailer as on disk.
- A `HeaderLink` of a file is read with **one suffix range GET**,
  `Range: bytes=-((blocks + 1) × 4096)`: the header and the roots block,
  without knowing the file's length.
- The newest file, found by the LIST, is read the same way: a suffix GET of
  a few blocks. When the header fits in that read, which is the common case,
  it costs one GET. Otherwise it costs one more GET, at open only.

PUTs are unchanged: one per file, create-only (`Fenced`).

### 4. Size, cap and chaining

- A header is the bytes of what it names, rounded up to whole blocks.
- **Cap:** a header grows to at most `MAX_HEADER_BLOCKS`. A batch whose
  changes do not fit chains another header, as today. The cap only sets how
  often that happens. 32 KiB, today's size, is the floor of the choice
  (open question 1).
- **Only a header that leaves nothing unrecorded ends a batch.** This rule is
  unchanged (`disk_header.rs`).

### 5. The spare room, and restating headers

Today the spare room is whatever the changes leave of 32 KiB, and it is
free. A reopen reads every header between its root and the `horizon`, so the
spare room is what bounds a reopen: the faster the rotation, the shorter the
walk. Small headers alone would slow the rotation, and the walk would grow
long in small headers. So the spare room becomes two things:

1. **The rest of the last block.** It is free: the block is written anyway.
   The rotation (`restate.rs`) fills it as today, the type restated longest
   ago first, and a large sub-header in slices.
2. **A restating header, when the walk grows past `K`.** The store counts
   the headers between the newest and the horizon it carries, which is the
   walk a reopen rooted there would make. When that passes `K`, the next
   header is a **restating header**:
   - it restates, from the rotation's cursor, as much as brings the walk
     back under `K`;
   - its size follows what that takes, at least 32 KiB and at most
     `MAX_RESTATE` (128 KiB);
   - at 4–8 bytes a page, 128 KiB restates ~16 000–32 000 pages in one jump.

   **The trigger is the walk's length, not a count of windows.** The walk is
   what a reopen pays. Windows come by the thousand a minute in a durable
   stream and almost never in an idle store, so a count of them would fire
   too often in one case and never in the other. A time ceiling beside `K`
   covers an idle store whose few headers are old.

   **It rides a large round when it can.** A relaxed settle, a round under
   pressure or a compaction already writes a lot. When the walk is near `K`
   (say past `K / 2`) and such a round comes, the restating header goes
   there, where it costs least in proportion. Only when the walk reaches `K`
   without one does a durable round carry it.

**Amortised.** With `K = 64`, a 128 KiB restating header every 64 windows is
~2 KiB a window, against the ~28 KiB of zeros and restatements a durable
window pays today.

**A full rotation is the index's size.** An index of 1 TiB in 64 KiB pages is
~16.8 M pages, ~100 MB of entries: about 800 restating headers of 128 KiB.
That size is inherent; the restating header only keeps it from spreading
over tens of thousands of small headers.

The horizon keeps its meaning: every type that existed when the header was
written is named at or above it. A reconstruction reads at most about `K`
headers past the root. Compaction's rule ("after restating every address, so
no header in them is live", RFC 0071) is unchanged. A file's headers stop
being live once the horizon passes them, which the restating headers keep
moving.

### 5a. Reading the walk back in large reads (disk)

On disk, a durable stream's windows lie end to end in the active file:
`[pages][header][pages][header]…`. A walk going back by `prev` can read the
file backwards in large chunks, 1 MiB for instance, and find several headers
in one read: each header's `HeaderLink` says where the previous one is, and
when that falls inside the chunk already read, it needs no read of its own.
This is an optimisation of the open and changes no format. The object store
does not need it: a file there is an upload (8 MiB or 10 s) with one header
per data shard's section, so there are no small headers per insert.

### 6. What it saves

| | today | here |
|---|---:|---:|
| header of a durable insert's round | 32 KiB | 4 KiB (one block), plus ~2 KiB of restating headers amortised |
| a durable insert, logical | ~110 KiB | ~84 KiB |
| a reconstruction | 1 read per header, 32 KiB each | 1 read per header, its size; ~`K` headers past the root; on disk, several per read (§5a) |
| finding the root | last 8 blocks | last block (+1 read if larger) |

The tail pages (~74 KiB of the 82) are the larger part of what is left. They
are out of scope here.

## Phases

1. **`HeaderLink`.** Pointers carry the size, and every header is still 32 KiB
   (`blocks = 8`). The format and the walks change, the sizes do not.
   **Landed 2026-11-07:** `Header.prev`, `TypeEntry.last`, `Roots.by_shard`,
   the RAM chains (`Rotation.last`, the disk's `tail` and `last_end`, the
   object store's `tail` and `cut`, `ChainRecord`), the seals and the remote
   shard's cuts; the disk's live headers are kept with their size, which is
   the run freed when they die; every walk reads a header by its link.
2. **Disk.** The trailer, headers sized to their change, the rest of the last
   block as spare room, and restating headers on the walk's length (`K`),
   riding large rounds.
   **Landed 2026-11-07** (`header_room.rs`, `trailer.rs`), with these
   choices:
   - **The header is reserved after the pages are placed.** Its size is
     only known then: placing a page far from its range's base isolates
     it, and the range is named whole. So a round reserves its pages and
     dictionaries, places them, sizes the header from the journals exactly
     as it will drain them, and reserves the header (and the roots block
     after it) — a second reservation, arithmetic for a store's own files,
     a message for a storage thread. The two lie end to end; a store's own
     files write them as one positioned write, and a storage thread joins
     them with every window it gathered.
   - **A restating header spends what one pass over the indexes takes, up
     to 128 KiB**, with no 32 KiB floor: a pass smaller than that would be
     padded with zeros that restate nothing.
   - **Restating headers are spaced by `K / 2`.** An index whose one pass
     takes many restating headers would otherwise keep its walk past `K`
     and restate in every window; spaced, it costs one every `K / 2`
     windows, like the rotation today but in fewer, larger headers.
   - **Values:** `MAX_HEADER_BLOCKS` = 32 (128 KiB, the same as
     `MAX_RESTATE`), `K` = 64, a round is large from 256 blocks of pages
     (1 MiB), and the time ceiling is 300 s. The torn-end scan overlaps its
     chunks by `MAX_HEADER_BLOCKS - 1` blocks.
3. **Object store.** The `[pages][header][roots]` layout and the suffix GET.
4. **The walk read back in large reads** (§5a), on disk.
5. **Measure.** The `micro` suite on the bench host, with KiB written per
   insert and per update for the durable rows, and the time and reads to
   open a database written by the durable rows.

Every phase keeps a durability test: reopen after a crash at every point
of a window, and a torn last block.

## Alternatives

- **Keep 32 KiB.** One read, known in advance, at ~29 % of a durable write's
  bytes.
- **A size field at the header's start.** That gives two reads per header,
  one to learn the size and one for the rest, or a guessed read and often a
  second one. Carrying the size in the pointer gives one read and no guess.
- **Smaller fixed headers** (4 or 8 KiB). Large rounds chain more headers,
  and the restatement rotation slows for everyone.
- **Restating only in relaxed rounds.** A store that only writes durably
  would never move its horizon. The walk's bound `K` covers it instead.
- **Growing every header a little past a bound.** It keeps the walk at the
  bound all the time, in small headers. A restating header moves the horizon
  in one jump and lets the walk shrink again.
- **A restating header every N windows.** Windows say nothing about the
  walk: thousands a minute in a durable stream, almost none when idle.

## Open questions

1. `MAX_HEADER_BLOCKS`: 32 for now (phase 2), so a big relaxed round chains
   fewer headers. To revisit with phase 5's numbers.
2. `K` = 64 and the `K / 2` share for now (phase 2). To revisit with
   phase 5's open times.
3. `MAX_RESTATE`: 128 KiB, the same as question 1 (phase 2).
4. Whether the disk's window should also end with the roots block in `Own`
   storage, so that both storages and the object store end every write the
   same way.
