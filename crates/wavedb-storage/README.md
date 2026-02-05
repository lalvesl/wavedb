# wavedb-storage

The per-node **storage engine**: the block manager, the per-`STRUCT_HASH` page
directory (linear-hashed), the page format, dictionaries, and the write
pipeline whose settle is the durability point. This is where most of WaveDB's engineering energy lives.

> For the project-wide idea and quickstart see the
> [root README](../../readme.md). The browser does **not** use this engine — see
> [`wavedb-wasm`](../wavedb-wasm/README.md) for the IndexedDB key→value path.

## Module map

| Module            | Responsibility                                                                                                                                       |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `block`           | `BlockDescriptor` (u43·u20·u1) + `Run`.                                                                                                              |
| `alloc`           | `BlockAllocator` — runs appended to the active data file, rollover at the ceiling, holes per file, free tails to cut (RFC 0071).                     |
| `file_space`      | One data file as the allocator sees it: its end, its holes (coalesced within the file), what a reclaim cuts.                                         |
| `manifest`        | The database's `MANIFEST`: magic, format version, shard count, seed, mode — written once at creation, a copy in the object store.                   |
| `block_file`      | A shard's data files, named by the block each starts at and found by listing: positioned run I/O, create/cut/delete, the lock, fsync.              |
| `data_file`       | One data file: its handle and name (16 hex digits), the per-file lock, create/cut/delete.                                                              |
| `dictionary`      | Per-`STRUCT_HASH` raw-content zstd dictionary (`Dictionary` + `DictState`): capped append-only buffer, version = prefix length, own run persistence. |
| `directory`       | The per-`STRUCT_HASH` page directory container + the linear-hashing addressing math (pure addressing).                                               |
| `directory_pages` | The directory's page **reads**: resolving a bucket to its `SlotPage`, decompressing against the passed-in `DictState`.                               |
| `plan`            | A settle round's phase 1: touched ids → page images grouped per bucket, splits decided, nothing written.                                             |
| `checkpoint`      | One window for every image at the end of the active file, ending with its header; one positioned write, then the descriptor swap.                  |
| `header`          | The header every batch ends with, disk and object store alike (`Header`, `TypeEntry`), and where the object store's chain stands.                 |
| `restate`         | The header's spare room: restating the addresses of the type restated longest ago, and the horizon that moves.                                     |
| `disk_header`     | The disk's header for a window: what is unrecorded, named; the spare room; the headers a reconstruction no longer reaches.                         |
| `disk_chain`      | What the disk's headers keep between windows: the unrecorded changes, the chain's tail, rotations, live headers, full restatements.                |
| `cycle`           | The settle cycle: a batch opened by a mark, settled in bounded rounds, ended by a header, made durable by one barrier; `drain`.                    |
| `barrier`         | The `fsync` helper thread, the batches asked of it, and a durable write's wait (`Durable`).                                                       |
| `walk`            | The walk that rebuilds directories from headers, newest first, down by `prev` while a registered type is unnamed, to the root's horizon.           |
| `reopen`          | Reconstruction on open: from a data shard's root, the walk, the allocator; nothing replayed.                                                       |
| `roots`           | The roots block every storage write ends with: each data shard's root, by position (RFC 0073 §5).                                                 |
| `roots_find`      | Where a reconstruction roots: the newest roots block whose storage write is whole, read back past a torn end.                                     |
| `wait`            | A round's wait for its window: the store's `Gate` (one round through the directories at a time) and `park_on` for synchronous callers.          |
| `link`            | A data shard's side of its storage shard: its own files, or a storage thread's — reserve, hand over, seal.                                         |
| `reader`          | A data shard's read handles on a storage thread's files, and the windows held readable until they land.                                         |
| `storage_shard`   | A storage shard's thread: reservations, the gathered write and its roots block, the `fsync` on its helper, the seals answered.                   |
| `storage_compact` | A storage shard's compaction: the evacuation's files, the data shards done with them, the reclaim after every durable write.                     |
| `storage_open`    | Opening a storage shard its data shards share: the roots, each data shard's walk, the allocator, the thread.                                     |
| `settle`          | The record cache's side of a settle: what is unsettled, and the cache-eviction budget.                                                             |
| `compact`         | Compaction's phases and surface (`compact_step`, `compact`, `reclaim`, `space`): move, drop the old addressing, seal, cut and delete.              |
| `defrag`          | Compaction's moves: the emptiest data file's runs, highest first, verbatim, into the active file — one window write per round.                    |
| `remote_compact`  | The object store's compaction (RFC 0070 §8): a survey run like a recovery, blind DELETEs below the horizon, mostly free files carried forward. |
| `convert`         | Converting a database from or to disk mode, offline: the pages flagged for upload, or brought back from the object store.          |
| `page`            | `SlotPage` — the homogeneous record page: `[len][to_wire_checked(PageEnvelope)]`, body raw or zstd.                                                  |
| `struct_storage`  | `StructStorage` — one type's own cache + directory slot (`#[wavedb]` emits one `static` per type).                                                   |
| `page_store`      | `PageStore` — the node's authoritative `Store`: per-type record logs → settle cycles into pages, each batch durable at its end.                   |
| `error`           | `StorageError` / `StorageResult`; flattens to `wavedb_core::Error::Backend` at the `Store` seam.                                                     |

Settle and rebalance are **off the mutation path**: `apply` only commits to the
caches; the node's maintenance loop opens a settle cycle every 200 ms, and
`apply_durable` waits for it ([RFC 0072](../../rfcs/0072-the-settle-is-the-durability-point.md)). The
`Pivot`/`BpTree` index layer lives in `wavedb_core::index`, not here.

---

## Where this crate runs

This crate is the **node's authoritative engine** — native only: per shard, the
data files (block runs, each settle window ending with its header). It provides the native **`PageStore`** — the
disk-optimised `Store` backend (`get` + atomic `apply`) — that the `Store`-generic
`Pivot`/`BpTree` contracts in [`wavedb-core`](../wavedb-core/README.md#index-contracts--pivot-bptree-indexkey)
run over. The index logic itself is portable; pages, blocks, headers, and the
allocator are this crate's internals, hidden behind the `Store` seam.

It is **not** the client-side local store. The client's local key→value cache
(`Store` trait) is a separate, lighter thing: a file kv on native, IndexedDB on
web ([`wavedb-wasm`](../wavedb-wasm/README.md)) — no pages, no headers. See
[`wavedb-core` § the `Store` trait](../wavedb-core/README.md#store--the-local-backend-trait).

---

## Block manager

A shard's blocks are **one address space of 4 KiB blocks, cut into data files**
([RFC 0071](../../rfcs/0071-data-files-by-address.md)). Each file is named
by the block it starts at — 16 hex digits, `storage/00/0000000000280000` holds
the blocks from `0x280000` on — and the store finds them by **listing** the
shard's directory into a `BTreeMap<start, file>`: a run at block `s` lives in
the file with the greatest start `≤ s`. Descriptors never name a file, only a
block.

- **Allocate** — only ever **appends**, at the end of the newest (*active*)
  file. Once that file has reached the ceiling (`StoreOptions::file_bytes`,
  10 GiB by default) the next run opens a new file named after where it ended.
  A run is never split, so the run that reaches the ceiling lands whole (a
  page may be up to 4 GiB). The ceiling decides only when a file closes, so it
  may change between runs.
- **Free** — a superseded run becomes a hole in its file, never handed out
  again; holes coalesce within a file, never across two.
- **Cut** — a file's free tail is cut off, and a file left empty is deleted
  ([`reclaim`](#write-pipeline--concurrency)); compaction is what empties them.

The allocator is a pure in-memory structure. On open it is rebuilt from the
runs the reconstruction names — pages, dictionaries, the headers it read;
what lies past a file's last used run — what a crash cut before its batch
ended — is cut, and a file no run names is deleted. A freed run becomes a
hole only once the batch that stopped naming it is durable
([RFC 0072](../../rfcs/0072-the-settle-is-the-durability-point.md) §8).

### Storage shards

A database's `P` data shards — placement, ownership, caches, directories —
write through `S` **storage shards** (`S` dividing `P`, both in the
`MANIFEST`; a node creates 64 through 4,
[RFC 0073](../../rfcs/0073-data-shards-and-storage-shards-WIP.md)): data
shard `p` writes through storage shard `p / (P / S)`, whose data files live
in `storage/<n>/`.

- **One data shard per storage shard** (`Storage::Own`) — the client cache,
  `PageStore::open`: the store allocates, writes and takes its barriers
  itself, and the window that ends a batch ends with a **roots block**.
- **Several** (`Storage::Shared`): a thread per storage shard owns the
  allocator and the files. A round reserves its window by message, awaits
  the answer (`wait::Gate` keeps a round's directories its own meanwhile),
  builds the window at its real addresses and hands it over; its pages are
  read from the bytes held back until they land (`reader`). Every 200 ms
  the thread writes what gathered — each run of windows lying end to end as
  one write — and a roots block naming every data shard's root, then one
  `fsync` on its helper makes every batch sealed in it durable. One storage
  write is in flight at a time, so only the newest can be cut by a crash.
- **Reopening** roots in the newest roots block whose storage write is
  whole (`roots_find`), each data shard walking from its own root through
  its own read handles; the allocator is the union of what they name.
- **Compaction** is an evacuation: the storage shard says which files to
  empty, every data shard carries its runs out, and the thread cuts and
  deletes after every durable write whatever is left only holes.

---

## On-disk metadata — the `MANIFEST` & the WaveWire rule

### The `MANIFEST`

A database's fixed facts live in **one** record at its root, `MANIFEST`
(`crate::manifest`, RFC 0071 §2) — beside `storage/00` … `storage/xx`, one
directory of data files per storage shard (RFC 0073), each data shard a
`PageStore`. A single `PageStore::open(dir)` is a database of one shard, the
same shape. Written once, atomically, when the database is created, and read
on every open:

| Field                       | Meaning                                                                             |
| --------------------------- | ----------------------------------------------------------------------------------- |
| magic                       | `WAVEDBMF`, a raw 8-byte prefix. Mismatch ⇒ `StorageError::BadMagic`.               |
| `to_wire_checked(Manifest)` | `[crc32][wire]` of `{ version, shards, storage, seed, object_backed }`.             |

The format version is pinned at 1 pre-release (`BadVersion` otherwise; see
the [root README](../../readme.md)); the shard count and the seed are part of
every record's address, and the mode is fixed. An object-backed database keeps
a copy at `<prefix>MANIFEST`, so one whose disk is gone still knows every
fact; the two copies must agree. A data file is blocks and nothing else.

### One layout language: WaveWire (+ checked framing)

Engine metadata is described with **the `WaveWire` codec only** — the same
layout language records use. No structure gets its own bespoke byte format:

- **`MANIFEST` body** (version, shards, seed, mode) — a plain `WaveWire`
  struct behind the magic. The 8-byte magic itself stays a raw prefix _outside_ the wire payload:
  it must be checkable before any decode is attempted;
- **block descriptor** (`start u43 · count u20 · flag u1`, exact-packed
  into one `u64` — see the table below) — wire-encodes as that `u64`;
- **page / dictionary directories** (`Vec<u64>` of descriptors) — the standard
  `WaveWire` `Vec` encoding;
- **page body** (`struct_hash` + the `(id, bytes)` entries) — a plain
  `WaveWire` struct (`PageBody`).

Where a structure must survive disk corruption — **pages, headers** —
it uses the wire crate's [`validation` feature](../wavedb-wire/README.md#feature-validation--crc32-framed-encoding):
`to_wire_checked` / `from_wire_checked` frame the payload as
`[crc32 (4 B LE)][wire bytes]`, so integrity verification is the codec's job —
not a hand-patched CRC slot re-implemented in every format. A corrupted page
surfaces as the wire `CrcMismatch` before any decode runs.

> **Status.** Fully in effect: the `MANIFEST` body, the headers, **and
> pages** are all on checked `WaveWire`. No engine structure hand-rolls its
> byte layout anymore; the only raw prefixes left are the 8-byte `MANIFEST`
> magic (checkable before any decode) and the `u32` length that delimits a
> payload inside a zero-padded run.

---

## Per-`STRUCT_HASH` page directory

A shard's pages are **partitioned by type**: each `STRUCT_HASH` owns its own page
directory — an in-memory `Vec<u64>` where every entry is a 64-bit **page
descriptor** pointing at one homogeneous page (a run of blocks holding records of
exactly that `STRUCT_HASH`). One `Vec<u64>` per type, so unrelated types never
share a page and each page compresses against one tight dictionary.

> **"bucket" = "page", 1:1.** A _bucket_ is one directory slot — the logical
> hash-table slot records hash into (the linear-hashing term). A _page_ is the
> physical run of blocks that slot points to. Each directory entry is exactly one
> page, so the two words name the same thing: "bucket" stresses its role in the
> hash table, "page" its on-disk form.

### Block descriptor (`u64`) — one format everywhere

Pages (for every `STRUCT_HASH`) **and** dictionary runs share a single 64-bit
descriptor — there is no second format:

| Bits   | Field         | Width | Meaning                                                         |
| ------ | ------------- | ----- | --------------------------------------------------------------- |
| 63..21 | `start_block` | `u43` | First 4 KiB block (2⁴³ × 4 KiB ≈ **32 PiB** of address).        |
| 20..1  | `block_count` | `u20` | Contiguous blocks (1 ≤ n ≤ 2²⁰ ⇒ ≤ **~4 GiB** per page/run).    |
| 0      | `flag`        | `u1`  | Not uploaded yet (RFC 0070): set on a disk page the object store has not received. |

`43 + 20 + 1 = 64` — exact fit. (A 4-bit fill gauge used to sit in the low bits;
nothing ever read it, so its bits went to the address and the flag.)

### The `Id` hash

`hash_of(id)` is **SeaHash over the 16 little-endian bytes of the `Id`** — fast and
DoS-resistant. It is seeded with a **per-database random `[u64; 4]` seed persisted
in the `MANIFEST`**, read once at startup. The seed is
per-database (not the fixed `STRUCT_HASH` seed): each node rebuilds its own
directory consistently, and an attacker can't precompute bucket collisions. SeaHash
is portable across architectures and endianness, so data files reopened on
another machine route every record to the same bucket. The resulting
`u64` feeds the linear-hashing reduction below.

### Linear hashing (not `%`)

Addressing uses **linear hashing**, not `hash % len`. The directory starts as one
page (~16 KiB) and grows **one bucket at a time** when a fill warning trips, so a
grow rehashes only a single bucket — never the whole type.

```rust
// Which bucket does this id land in?
pub fn index(&self, id: u128) -> usize {
    let m = self.dir.len() as u64;
    let level = m.ilog2();
    let base = 1u64 << level;
    let s = m - base;
    let h = hash_of(id);
    let mut b = h & (base - 1);
    if b < s {
        b = h & ((base << 1) - 1);
    }
    b as usize
}

// Split the next bucket in round-robin order; append one new bucket.
pub fn split_next(&mut self, file: &BlockFile) -> StorageResult<()> {
    let m = self.dir.len() as u64;
    let level = m.ilog2();
    let s = (m - (1u64 << level)) as usize; // bucket to split

    let desc = self.dir[s];
    let bucket = self.read_bucket(file, desc)?;
    // Partition by bit `level`: 0 stays in s, 1 moves to the new bucket.
    let mut keep = SlotPage::new(self.struct_hash);
    let mut moved = SlotPage::new(self.struct_hash);
    for (id, entry) in bucket.entries {
        if (hash_of(id) >> level) & 1 == 0 {
            keep.upsert_entry(id, entry);
        } else {
            moved.upsert_entry(id, entry);
        }
    }
    let keep_desc = self.place_if_nonempty(file, &keep)?;
    let moved_desc = self.place_if_nonempty(file, &moved)?;
    if desc.is_allocated() {
        file.free(desc.run());
    }
    self.dir[s] = keep_desc;
    self.dir.push(moved_desc); // new bucket at index M (= s + base)
    Ok(())
}
```

### Two kinds of growth

| Growth           | Trigger                                   | Cost                                                                                                                |
| ---------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Page grow**    | a bucket's page outgrows its run          | allocate a larger run, copy the page, free the old run, rewrite **one** descriptor. No keys move.                   |
| **Bucket split** | the warning limit trips                   | `split_next` splits one bucket by the next hash bit and appends one directory slot. Scoped to one type, one bucket. |

**Page grow is the cheap, common case** — relocate one contiguous run to a bigger
hole, patch one pointer. Growing the page in place lets writes proceed **without
rebalancing on the hot path**; a background task runs `split_next` to keep pages
from getting too large (see _Write pipeline_).

For safety, the order is: allocate the larger run → write the updated page →
**then** rewrite the directory entry. The directory only ever points at a fully
written page.

---

## Page format

A page is **homogeneous** — records of exactly one `STRUCT_HASH`. `SlotPage` is
the one page format today (`Unique` anchors, NonUnique records, Pivots, and
`BpTree` nodes all ride it), and its layout is the **checked wire encoding** —
no page-private byte format:

```
[ payload_len (u32 LE) ][ to_wire_checked(PageEnvelope) ]   … zero-padded to the run
                         └── [ crc32 (u32 LE) ][ wire bytes ] ──┘

PageEnvelope (plain WaveWire struct):
    struct_hash: u64                  — identifies the type; selects the dictionary
    payload: PagePayload              — the body, raw or compressed

PagePayload (plain WaveWire enum):
    Raw( to_wire(PageBody) )          — compression off for this type, or zstd gained nothing
    Zstd { dict_len, raw_len, bytes } — zstd(to_wire(PageBody)) against dictionary[..dict_len]

PageBody (plain WaveWire struct):
    records: Vec<(u128, Vec<u8>)>     — (Id.raw, record wire bytes), ascending Id
```

- **`payload_len`** — the only framing outside the codec: a page reads back
  from a run of whole blocks padded with zeros, and `from_wire_checked` must
  get the exact payload slice for its crc to hold (same pattern as a
  header).
- **crc + decode** — both the codec's job (`CrcMismatch` before any decode
  runs), surfaced as `StorageError::Corrupt`. The crc covers the **stored**
  bytes, so corruption is caught before zstd runs; `struct_hash` and the
  payload kind stay readable without the dictionary.
- **per-type compression policy** — each `Directory` decides whether its pages
  compress (`with_compression`); `PageStore` turns it **off for `BpTree` node
  pages** (hot, rewritten on every index mutation) and on for record/Pivot
  pages. Even when on, a body zstd cannot shrink is stored `Raw` — a page
  never grows for having been "compressed".

What a page's records hold, by the kind of `STRUCT_HASH` that routed them (a
future per-kind split — e.g. its own dictionary — could specialise these without
changing the framing):

| Kind          | What the records hold                                              |
| ------------- | ------------------------------------------------------------------ |
| **Unique**    | The single live record per tenant at its fixed anchor address.     |
| **NonUnique** | The collection's records (timestamp-keyed).                        |
| **Pivot**     | Collection handles: `current`/`dead` BpTree pointers (no counter). |
| **BpTree**    | Serialised B+tree nodes (packed in the shared page directory).     |

### BpTree node storage

> Status: the dedicated **32 KiB, one-node-per-page** format was **dropped
> (2026-07-07)** — see [RFC 0031](../../rfcs/0031-node-per-page-bptree-DEPRECATED.md).
> A `BpTree` exists per tenant, so a B2C node hosts millions of _small_ trees and
> a page per node wastes exactly the dominant case.

Nodes ride the shared linear-hash `SlotPage` directory instead: a node is an
ordinary `STRUCT_HASH`-headed value (`[hash][kind u8][WaveWire]`), packed with
everything else of its type so many tenants' small nodes share common buckets.
Entry format, split/rebalance, and merge policy live in
[RFC 0011](../../rfcs/0011-bptree-index-and-collections.md).

---

## Dictionaries

Because a page holds exactly one `STRUCT_HASH`, each type has **one dictionary**
with nothing foreign to dilute it.

### Raw-content dictionary (no trainer)

zstd accepts **any bytes** as a dictionary, not just a trained `ZDICT` — so the
dictionary is simply a **capped, append-only buffer of record bytes** for that
`STRUCT_HASH`. No training pass: as writes settle, record bytes are sampled
into the buffer until the cap (`DICT_CAP`), then it freezes. Simpler than a
trainer and naturally incremental.

The dictionary is stored in the data files like a page: a block run handed out by
the block manager (`[len][to_wire_checked(buf)]`, same framing as everything
else), tracked by a descriptor in the type's `DictState` — the compression
slot the open store keeps for the type — and repointed
with the same crash-safe write-then-free ordering pages use. A compression-off
type (`#[wavedb(compress = false)]`, or the reserved `BpTree`-node slot) never
samples and never allocates a run.

### Versioning (the one rule)

A page compressed against dictionary state X **must** be decompressed against
the exact same bytes — the dictionary never mutates under existing pages.
Because the buffer is **append-only, a state is fully identified by its
length**: each `Zstd` page stamps `dict_len` at compression time and
decompresses against `buf[..dict_len]`. Every superseded state is a prefix of
the same live buffer, so old pages stay readable forever with **no
recompression and no superseded-run bookkeeping** — one stored buffer serves
all versions. (A background recompress-cold-pages pass remains possible later
as a pure space optimisation, not a correctness need.)

On open the dictionary is read back from the run the newest header names, so
every `dict_len` a page stamps resolves against it.

CPU is free here — there is no join processing competing for it. Per-value
(strings/blobs) heap compression stays future work.

---

## Write pipeline & concurrency

```
mutation → the type's record log (RAM) → (client confirmed)
                     │
                     └─ every 200 ms: a settle cycle — rounds into the data
                        files, each window ending with its header; one barrier
                        ends the batch: durable (apply_durable waits for it)
```

1. **The record cache first.** Every insert/save/remove commits to the type's
   log in RAM and returns — durable at the next settle cycle, within a tick
   (200 ms on a node) and a barrier. `apply_durable` waits for that cycle;
   everything committed before a durable write is durable with it
   ([RFC 0072](../../rfcs/0072-the-settle-is-the-durability-point.md)).
2. **Cache as a log per type.** Confirmed mutations live in the type's own log
   (RFC 0068); reads serve from it directly.
3. **The settle cycle — one window per round.** A cycle marks every log's end
   (the batch: every write before the mark), then settles it in bounded
   rounds: the round's ids grouped by bucket, each touched page read once,
   every resulting image written into **one contiguous window** with a single
   positioned write ([RFC 0041](../../rfcs/0041-single-barrier-checkpoint.md)).
   The descriptors are swapped only after the write lands, so a reader sees
   either the old pages (still allocated — page writes are copy-on-write) or
   the new ones. A tick with nothing written opens no cycle: no window, no
   barrier.
4. **Rebalance is part of that plan.** Splits are decided in RAM, before
   anything is serialised, so an over-sized bucket is repartitioned and both
   halves are written once — never a page written twice in one round.
5. **The window ends with its header** — 32 KiB naming the addresses it
   changed, type by type, and `last`, the header before with entries for that
   type; its spare room restates the addresses of the type restated longest
   ago, so every type keeps appearing near the newest header. The round that
   settles the last of the batch marks its header `end_of_batch`, and the
   batch's barrier is taken — inline, or on the worker's helper thread, whose
   answer releases every durable write the batch carries. A cycle opens only
   once the previous batch is durable.

6. **Compaction.** Copy-on-write leaves a hole wherever a page used to be,
   and writing only appends, so nothing fills it. Compaction picks the data
   files that are mostly holes (`victim_free_percent`, 50 by default), the
   fewest live blocks first, and carries their runs — pages and dictionaries,
   verbatim, highest first — into the active file; the active file itself is
   closed first when it is the one to empty. A header cannot move, so a
   compaction first restates every address: once that batch and the next have
   ended, no older header is live, and their files can be emptied. The moved
   runs are holes once a batch over them is durable; then the free tails are
   cut and the emptied files deleted
   ([RFC 0071](../../rfcs/0071-data-files-by-address.md), superseding
   [RFC 0069](../../rfcs/0069-compaction-into-the-holes-DEPRECATED.md)'s holes).
   It is stepped, a bounded pass at a time; the node starts one on a daily
   UTC time, a disk ceiling, or a free-space share.

7. **The object store** ([RFC 0070](../../rfcs/0070-object-storage-backend.md),
   phases 3–6). A database created with `StoreOptions::remote` keeps its pages
   in an object store, the disk caching them. Settle rounds flag what they
   write; `upload` reassembles the flagged pages, record by record, into the
   object store's own linear hash of 128 KiB pages, packed into files of
   `[32 KiB header][pages…]` (every page starts within the file's first
   8 MiB), and clears the flags. The remote directories ride the disk's
   headers (their own lane, and the object store's chain), so a reopen
   restores them from the disk alone. Past the
   compaction ceiling the least recently used uploaded pages leave the disk
   (`BlockDescriptor::EVICTED`; phase 4): a read of one goes to the object
   store and marks it wanted back, and a write restores its bucket before it
   commits, so a page on disk is always the whole truth about its bucket.
   A database whose disk is gone opens from its files alone (phase 5): the
   same header and the same walk as the disk's (RFC 0072), and the store
   adopts what the newest whole **cut** says (an upload that left nothing waiting: every batch up
   to a point, none in part), every disk bucket evicted. In **object mode**
   (`PageStore::open_object`, phase 6) there is no disk at all: what would
   be the data files sit in RAM, there is no settle to disk — a write is durable once
   an upload carries it, which happens on 8 MiB or an interval — and every
   open is such a recovery. Every file is created only if absent, so a
   second writer on the prefix is fenced (`StorageError::Fenced`), never
   merged; what navigation reads is resident (`StructStorage::resident`).
   The object store compacts by file (phase 7, `compact_remote`): a survey
   run like a recovery finds the horizon — the lowest header a recovery
   reads — and the files nothing names (the store's directories, the
   newest cut, the disk's two newest batches); those are DELETEd, and the
   mostly free files below the horizon have their pages carried forward
   by uploads of the compaction's own first. The node weighs it once a
   minute on the compaction triggers.
   `convert` moves a database from or to disk mode, offline: up, every
   page flagged for upload; down, every evicted page brought back, the
   object store's files kept.

On startup the directories and the block allocator are **reconstructed from the
headers**: the newest whole batch's — the last 8 blocks of the newest data
file — roots a walk back along each type's `last`, and down by `prev` while a
registered type is still unnamed — never below the root's `horizon`, where
every type that existed is named, so one unnamed there is new and empty.
Only the newest batch can be cut by a crash
(a cycle opens once the previous batch is durable): its pages are checked by
their crc, and a batch not whole gives way to the one before. Nothing is
replayed; the caches start empty.

### Per-type routing is compile-time — `StructStorage`

> **Status: in effect.** This replaced the engine's runtime
> `HashMap<STRUCT_HASH, Directory>` + store-wide mutex. Since RFC 0067 step 1
> the static is a declaration only, and the state belongs to the open store.

There is no runtime registration and no store-wide state lock. Each type is
declared by one **`static StructStorage`**, emitted by `#[wavedb]` on the type
itself (native targets only; the wasm expansion omits it): its `STRUCT_HASH`
and its compression policy, fixed at compile time.

```rust
Todo::struct_storage()      // &'static StructStorage — hash + compression policy
Todo::storage_entries()     // the declarations to register: [Todo's, TodoPivot's, …]
```

An open `PageStore` builds one slot per registered declaration — that type's
`Directory` **and its compression state** (`DictState`: dictionary + persisted
run — per-type by nature, since a page holds one type) — and one **record
cache** (RFC 0068): a log of committed records per type, in chunks that start
at 512 B and double up to 64 KiB, with one Dashtable-shaped index over all of
them. The slots and the cache belong to that store, so stores over different
directories share nothing (RFC 0067: one per shard).

Compression policy is declared with the type — `#[wavedb(compress = false)]`
opts a type's pages out of zstd (already-compressed media, hot rewrite
patterns). Policy is storage config, **not** schema identity: it never feeds
the `STRUCT_HASH`.

`PageStore::open(dir, &[…])` takes the slots as an **explicit registry** —
declared, not discovered, the same allowlist stance as exposure. A write whose
`STRUCT_HASH` has no listed slot is refused (`UnregisteredStructHash`); the
reserved `BpTree`-node slot is always included (compression off). A data
directory has **one open `PageStore` at a time**: every data file is locked
while open (an empty one is kept for a database nothing was written to yet), so a second open of the same directory — in this process or another —
fails with `EngineBusy`. Stores over different directories open side by side.

**Threading.** A store is used from **one thread** — a node moves each onto the
worker that owns it (RFC 0067) — so its state lives in plain cells (`RefCell`,
`Cell`): no lock, no atomic. `PageStore` is `Send` and deliberately not `Sync`.
Reads (`get_of`, routed by the compile-time-known hash) are one binary search
over the sorted registry, one index probe, and a page read on a miss. Commits
run on the one thread, so the logs' order is the commits' order. A settle
round copies each record's **newest entry** out of its log and skips the
copies it superseded, so it is idempotent. Eviction drops whole chunks the settle has passed,
measured against what the cache actually allocates.

### Atomicity is the batch

A single operation can touch several records (a NonUnique insert writes the data
record **and** a `BpTree` node). These are committed to the record cache as one
batch, on the one thread — so commits are totally ordered — and a settle cycle's
mark falls between two of them: a batch on disk is every write before its mark,
and a reconstruction roots only in a batch whose end is written, so a crash
brings back whole operations or nothing. No separate transaction manager.
Visibility is per-`get` (each read sees a fully committed value, never a torn
one); a multi-`get` walk was never snapshot-isolated. (Multi-write
**server-function** transactions — several operations as one unit — remain a
separate, later concern.)

### IO cost

**Per mutation: memory only.** `Store::apply` commits the whole batch — the
record, every touched `BpTree` node, the `Pivot` rewrite when a root moved — to
the per-type logs. Nothing reaches the disk on that path.

**Per settle round: one write.** The round's ids are grouped by bucket, each
touched page is read **once**, and every resulting image is placed in **one
contiguous window**, ending with its 32 KiB header, written with a single
positioned write ([RFC 0041](../../rfcs/0041-single-barrier-checkpoint.md)).
Reads are the floor: one per page the round modifies.

**Per batch: one barrier.** The data files the cycle wrote are synced once — on
the worker's helper thread on a node — and that is the only barrier: a durable
write costs at most a tick of latency and shares its barrier with everything in
its batch. `BlockFile::io()` counts all of this, and the accounting is asserted
in `page_store`'s tests.

What still scales with the work rather than with the batch is the _logical_ index
maintenance above the store, which decides how many values a batch contains:

| Operation                         | Values in the batch                                                                               |
| --------------------------------- | ------------------------------------------------------------------------------------------------- |
| **Unique `save`**                 | the live record + the archived predecessor                                                        |
| **NonUnique `save`** (update)     | record + archive + the recency log's re-key + each of `N` secondaries **whose fields changed**    |
| **NonUnique `insert` / `remove`** | record + `current` + recency (or `dead`) + every one of `N` secondaries · `Pivot` if a root moved |

A NonUnique **`save`** never re-keys the `current` tree — its key is the record's
immutable `CREATED_AT` identity — but it always re-keys the recency log and any
secondary whose fields changed, reaching the roots through **`Metadata.pivot`**.
The `Pivot` itself is **read, not written** unless a `BpTree` root moves (no
counter). The **`dead`** tree is touched only by `remove`; an update's superseded
version is archived at a derived slot and linked by `Metadata.succession`. The
record's identity `Id` stays stable so references don't break. Reads of those
tree nodes are served from the shared `BpTree` cache slot when warm.

---

## Operations on records

- **Unique** — `get` resolves the fixed anchor (`STRUCT_HASH · TENANT · 1 · 0`)
  in one lookup. `save` writes the new live bytes to the anchor and chains the
  previous version into history via `Metadata`.
- **NonUnique** — a collection's `Pivot` is created **explicitly** (one per
  holder, so a nesting NonUnique mints one per record) and its `PivotId` stored
  by the holder; never auto-created. A
  record's **identity `Id` is fixed at `insert`** (stable anchor for references),
  so:
  - **`save`** (update) **re-keys only the trees whose field changed** — the
    `current` `BpTree` is keyed by the immutable `CREATED_AT` (never re-keyed),
    each secondary only when its field changed. It reaches the roots through
    `Metadata.pivot`. The **`dead`** tree is **not** touched: the superseded
    version is archived at a derived slot and linked by `Metadata.succession`;
  - **`insert`** adds the record to the `current` `BpTree` (and every secondary) and
    stamps `Metadata.pivot`; **`remove`** moves it to the **dead** tree — the only
    op that writes `dead`. All go through the `Pivot`.
    The record bytes are never erased, keeping the timeline navigable.

### `BpTree` stores only `Id`s — reads are two-phase

A `BpTree` holds **`Id`s, never record bytes**. Its nodes are themselves stored
as pages (the `BpTree` `STRUCT_HASH`) and linked by `Id`, so a search walks the
tree node-by-node and **returns a set/range of matching record `Id`s** — nothing
more. Resolving those `Id`s to actual records is a **separate step**: each `Id` is
fetched through its type's page directory (a normal record `get`). So a NonUnique
read is:

1. **Index search** — walk the `Pivot`'s `current` `BpTree` (ordered by
   `CREATED_AT`) → matching `Id`s;
2. **Record fetch** — resolve each `Id` to its bytes via the per-`STRUCT_HASH`
   directory.

Keeping the tree ID-only is what makes it small and shallow, and lets the two
phases be scheduled independently (e.g. fetch only the page of `Id`s the caller
actually wants).

### History & space

There is **no cold tier** (the slow-node was removed — not part of the design for
now). History is first-class and **never erased**: superseded versions and
deleted-record bytes stay in the data files, reachable via the modification chain
and the `dead` `BpTree`. So a node's data **grows unbounded** with history — and
that is **accepted for now**. History pruning / compaction / a separate archive
tier is left for later; nothing reclaims old versions today.

---

## Reliability

- **Headers** — every batch ends with one; a reopen roots in the newest whole
  batch, its pages checked by their crc, so a crash loses at most the batch
  it cut and never brings one back in part.
- **Checksums** — every page carries a CRC32 verified on read (converging on
  the wire `validation` framing — see _the WaveWire rule_ above).
- **Locks** — ID-scoped, in process memory.

---

## Configuration (initial)

| Parameter             | Description                                        | Default        |
| --------------------- | -------------------------------------------------- | -------------- |
| `block_size`          | Allocation unit; a page is `block_count` of these  | 4 KiB          |
| `first_page_size`     | Size of the first page / directory bucket          | 16 KiB         |
| `max_blocks_per_page` | Ceiling on one page's run (`block_count` is `u20`) | 2²⁰−1 (~4 GiB) |
| `warning_limit`       | Fill level that triggers a background `split_next` | tunable        |
| `cache_budget`        | RAM budget for the `BTreeMap` write/read cache     | tunable        |
