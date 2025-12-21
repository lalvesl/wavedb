# RFC 0079 — Headers sized to their change

- **Status:** Planned. Opened 2026-11-07.
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
ranges, into the free end of the last block, and grows a header only when
the walk back to the horizon would pass a bound.

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
struct HeaderRef {
    /// Disk: the block it starts at. Object store: the file's number.
    at: u64,
    /// Its length in 4 KiB blocks, 1 at least.
    blocks: u16,
}
```

These become `HeaderRef`:

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

Every header after the root is reached through a `HeaderRef`, which is one
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
- A `HeaderRef` of a file is read with **one suffix range GET**,
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

### 5. The spare room

Today the spare room is whatever the changes leave of 32 KiB, and it is
free. Now it is:

1. **The rest of the last block.** It is free: the block is written anyway.
   The rotation (`restate.rs`) fills it as today, the type restated longest
   ago first, and a large sub-header in slices.
2. **More, only when the walk would grow too long.** If the horizon this
   header would carry is more than `MAX_WALK` headers behind it, the header
   grows by the blocks it needs to restate until it is within the bound
   again. A durable stream of small batches thus pays an extra block now and
   then, not 28 KiB every time.

The horizon keeps its meaning: every type that existed when the header was
written is named at or above it. A reconstruction reads at most `MAX_WALK`
headers past the root. Compaction's rule ("after restating every address, so
no header in them is live", RFC 0071) is unchanged. A file's headers stop
being live once the horizon passes them, which the bound keeps moving.

### 6. What it saves

| | today | here |
|---|---:|---:|
| header of a durable insert's round | 32 KiB | 4 KiB (one block) |
| a durable insert, logical | ~110 KiB | ~82 KiB |
| a reconstruction | 1 read per header, 32 KiB each | 1 read per header, its size |
| finding the root | last 8 blocks | last block (+1 read if larger) |

The tail pages (~74 KiB of the 82) are the larger part of what is left. They
are out of scope here.

## Phases

1. **`HeaderRef`.** Pointers carry the size, and every header is still 32 KiB
   (`blocks = 8`). The format and the walks change, the sizes do not.
2. **Disk.** The trailer, headers sized to their change, the rest of the last
   block as spare room, and the `MAX_WALK` bound.
3. **Object store.** The `[pages][header][roots]` layout and the suffix GET.
4. **Measure.** The `micro` suite on the bench host, with KiB written per
   insert and per update for the durable rows, and the time to open a
   database written by the durable rows.

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
  would never move its horizon. The `MAX_WALK` bound covers it instead.

## Open questions

1. `MAX_HEADER_BLOCKS`: 8 (today's size, as a cap) or larger, so a big
   relaxed round chains fewer headers.
2. `MAX_WALK`: what a reopen may read past its root. Today's rotation gives a
   number to start from.
3. Whether the disk's window should also end with the roots block in `Own`
   storage, so that both storages and the object store end every write the
   same way.
