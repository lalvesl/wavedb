# wavedb-storage

The per-node **storage engine**: the block manager, the per-`STRUCT_HASH`
ordered page index ([RFC 0078](../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md)),
the page format, and the write pipeline whose settle is the
durability point. This is where most of WaveDB's engineering energy lives.

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
| `ordered`         | The quantized page index of one lane (RFC 0078): sorted sub-headers, one `u32` per page, flags, a list lane's counts; the fold; journal, rebuild. |
| `pages`           | A type's index as the store addresses it: a key's page, a page's coverage, reads through the page cache.                                          |
| `shape`           | One round's pages: records routed to the pages their keys fall in, those pages rebuilt and cut on a quantum boundary when over target.          |
| `lane_read`       | A scan or count of an ordered lane, started synchronously and finished after restoring any evicted page it meets.                                |
| `lane_walk`       | One walk of a lane's pages with the record cache's keys merged over them; a counted page passed by its count.                                    |
| `plan`            | A settle round's phase 1: touched keys → page images per lane, cuts decided, nothing written.                                                    |
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
| `storage_open`    | Opening a storage shard its data shards share: the roots, each data shard's walk, the allocator, the threads.                                    |
| `storage_write`   | A storage shard's write: the gathered windows by address, the roots block, the `fsync` on the helper, the seals answered.                       |
| `remote_shard`    | A storage shard's chain of files in the object store: its data shards' sections placed in files, one PUT for all of them.                       |
| `remote_put`      | The files going down, oldest first, each headed by a roots block naming every cut whose whole upload is in.                                      |
| `remote_shard_compact` | The object store compacted across data shards: their reports, the files none needs deleted, the mostly free ones carried out.              |
| `remote_link`     | A data shard's side of the remote thread: reserve, hand over, await the files.                                                                  |
| `remote_live`     | What of the object store a store still names: live blocks per file, the space compaction weighs, the disk's rooted files.                      |
| `settle`          | The record cache's side of a settle: what is unsettled, and the cache-eviction budget.                                                             |
| `compact`         | Compaction's phases and surface (`compact_step`, `compact`, `reclaim`, `space`): move, drop the old addressing, seal, cut and delete.              |
| `defrag`          | Compaction's moves: the emptiest data file's runs, highest first, verbatim, into the active file — one window write per round, in key order.      |
| `regroup`         | Compaction's regroup (RFC 0078 §12): runs of scattered ranges' pages moved side by side in key order, so each folds into one sub-header.           |
| `regroup_merge`   | Compaction's merge: neighbouring pages of one collection that removals left small, together at most half a page, rewritten as one.                |
| `remote_compact`  | The object store's compaction (RFC 0070 §8): a survey run like a recovery, blind DELETEs below the horizon, mostly free files carried forward. |
| `convert`         | Converting a database from or to disk mode, offline: the pages flagged for upload, or brought back from the object store.          |
| `page`            | `SlotPage` — the homogeneous record page: `[len][to_wire_checked(PageEnvelope)]`, body raw or zstd.                                                  |
| `struct_storage`  | `StructStorage` — one type's own cache + directory slot (`#[wavedb]` emits one `static` per type).                                                   |
| `page_store`      | `PageStore` — the node's authoritative `Store`: per-type record logs → settle cycles into pages, each batch durable at its end.                   |
| `error`           | `StorageError` / `StorageResult`; flattens to `wavedb_core::Error::Backend` at the `Store` seam.                                                     |

Settle and rebalance are **off the mutation path**: `apply` only commits to the
caches; the node's maintenance loop opens a settle cycle every 200 ms, and
`apply_durable` waits for it ([RFC 0072](../../rfcs/0072-the-settle-is-the-durability-point.md)). The
chains a collection keeps live in `wavedb_core::index`, not here: to this
crate they are keys of ordered lanes.

---

## Where this crate runs

This crate is the **node's authoritative engine** — native only: per shard, the
data files (block runs, each settle window ending with its header). It provides the native **`PageStore`** — the
disk-optimised `Store` backend (`get`, `scan`, `count` + atomic `apply`) — that
the `Store`-generic chains in [`wavedb-core`](../wavedb-core/README.md#index-contracts--pivot-chain-indexkey)
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
runs the reconstruction names — pages, packs, the headers it read;
what lies past a file's last used run — what a crash cut before its batch
ended — is cut, and a file no run names is deleted. A freed run becomes a
hole only once the batch that stopped naming it is durable
([RFC 0072](../../rfcs/0072-the-settle-is-the-durability-point.md) §8).

### Storage shards

A database's `P` data shards — placement, ownership, caches, directories —
write through `S` **storage shards** (`S` dividing `P`, both in the
`MANIFEST`; a node creates 64 through 4,
[RFC 0073](../../rfcs/0073-data-shards-and-storage-shards.md)): data
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
- **The object store** (`remote_shard`): the storage shard's chain of files
  lives under `storage/<n>/`, each file headed by a roots block. A remote
  thread per storage shard places every data shard's upload sections in
  the open file, PUTs the files oldest first — at its tick, or at once when
  someone waits — and deletes, once every data shard has reported what it
  still needs, the files none of them does.

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
- **page directories** (`Vec<u64>` of descriptors) — the standard
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

## Ordered page index

> Design and progress: [RFC 0078](../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md).

A shard's pages are **partitioned by type**, and a NonUnique type's chains by
kind: every registered slot — a type's records, its Pivots, and each of its
five ordered **lanes** (declared lists, recency, removals, secondary, fuzzy)
— keeps an index of its own, so unrelated content never shares a page and
each page compresses one kind of content.

Keys are bytes, compared in order. An anchor's key is its `Id`'s raw `u128`
big-endian — `KEY`, `TENANT`, `FLAG | SALT` — so a NonUnique type's records
append at their tail in creation order. A lane entry's key is its chain's
`[pivot Id 16 B][declaration u8]`, then the entry's own: a collection is a
key range.

A page holds the keys of one contiguous range, and the index finds it in
RAM:
- **sorted sub-headers**, each a range of the key space: a shared prefix,
  the next 8 bytes quantized by a power-of-two shift, a base block and the
  multipliers of the entries' address and size fields;
- under each, **one `u32` per page** — `q << 16 | address | size` — its
  first quantum and its run; beside it a flag byte (`PENDING`: not uploaded,
  `EVICTED`: gone from disk), and in a declared list's lane a **count**, how
  many entries the page holds (255 means "read it").

A lookup is two binary searches: the sub-header, then the entry. A page
boundary is always a quantum boundary, so a key names exactly one page.

A page a round rewrites over `target_blocks_per_bucket` is **cut in that
round** — at its middle, or the quantum boundary nearest it, refining the
range when its keys share one quantum. Every page is rewritten
copy-on-write into the round's window, so the index only ever points at a
fully written page. What changed is journaled and named in the round's
header (`RangeRecord`: a range whole, some entries, or gone); a reopen
rebuilds each index from the newest header down.

A **scan** walks the index from the span's edge, one page's coverage at a
time, merging each page with the record cache's keys over it. In a counted
lane a page the span covers whole, its count known and the cache holding
nothing of it unsettled, is passed by its count: a list's length and a skip
into it read only their edge pages.

---

## Page format

A page is **homogeneous** — entries of exactly one slot. `SlotPage` is the one
page format (anchors, Pivots and lane entries all ride it), and its layout is
the **checked wire encoding** — no page-private byte format:

```
[ payload_len (u32 LE) ][ to_wire_checked(PageEnvelope) ]   … zero-padded to the run
                         └── [ crc32 (u32 LE) ][ wire bytes ] ──┘

PageEnvelope (plain WaveWire struct):
    struct_hash: u64                  — identifies the slot
    payload: PagePayload              — the body, raw or compressed

PagePayload (plain WaveWire enum):
    Raw( body )                       — compression off for this slot, or zstd gained nothing
    Zstd { raw_len, bytes }           — zstd(body), the page alone

body — columns, ascending by key:
    keys back to back, then their width (one for all) or their ends,
    the values' ends, the values back to back
```

- **`payload_len`** — the only framing outside the codec: a page reads back
  from a run of whole blocks padded with zeros, and `from_wire_checked` must
  get the exact payload slice for its crc to hold (same pattern as a
  header).
- **crc + decode** — both the codec's job (`CrcMismatch` before any decode
  runs), surfaced as `StorageError::Corrupt`. The crc covers the **stored**
  bytes, so corruption is caught before zstd runs; `struct_hash` and the
  payload kind stay readable without decompressing.
- **per-slot compression policy** — a `#[wavedb(compress = false)]` type's
  pages skip zstd. Even when on, a body zstd cannot shrink is stored `Raw` —
  a page never grows for having been "compressed".
- **read as held** — a page decodes into its columns once and is lent from
  the page cache; the writes since are kept beside them until an encode
  seals them (RFC 0076).

| Slot          | What the entries hold                                                   |
| ------------- | ----------------------------------------------------------------------- |
| **Unique**    | The single live record per tenant at its fixed anchor, and its archive. |
| **NonUnique** | The collection's records at their anchors, and their archives.          |
| **Pivot**     | Collection handles: `permission` only.                                  |
| **Lanes**     | Chain entries: anchors (recency, removals, secondary, fuzzy) or whole records (lists). |

---

## Compression

A compressing type's page is zstd'd **alone** — no dictionary, nothing a
page needs besides its own bytes to read back (RFC 0081 §9). A dictionary
would be state another write must make durable before any page compressed
against it is; measured on the micro workload's records it bought nothing
from 16 KiB pages up and about 8% at 4 KiB, and cost at every size (the
compressor loads it on every call). `benches/compression.rs` measures a
page compressed and raw.

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
   rounds: the round's keys routed to their pages, each touched page read once,
   every resulting image written into **one contiguous window** with a single
   positioned write ([RFC 0041](../../rfcs/0041-single-barrier-checkpoint.md)).
   The index entries are pointed at the new runs only after the write lands, so a reader sees
   either the old pages (still allocated — page writes are copy-on-write) or
   the new ones. A tick with nothing written opens no cycle: no window, no
   barrier.
4. **Cuts are part of that plan.** They are decided in RAM, before anything
   is serialised, so an over-sized page is cut and both halves are written
   once — never a page written twice in one round.
5. **The window ends with its header** — whole blocks naming the
   addresses it changed (sized once its pages are placed, a trailer in its
   last block saying how long it is; RFC 0079), type by type, and `last`, the header before with entries for that
   type; its spare room restates the addresses of the type restated longest
   ago, so every type keeps appearing near the newest header. The round that
   settles the last of the batch marks its header `end_of_batch`, and the
   batch's barrier is taken — inline, or on the worker's helper thread, whose
   answer releases every durable write the batch carries. A cycle opens only
   once the previous batch is durable.

6. **Compaction.** Copy-on-write leaves a hole wherever a page used to be,
   and writing only appends, so nothing fills it. Compaction picks the data
   files that are mostly holes (`victim_free_percent`, 50 by default), the
   fewest live blocks first, and carries their runs — pages and packs,
   verbatim, highest first — into the active file; the active file itself is
   closed first when it is the one to empty. A header cannot move, so a
   compaction first restates every address: once that batch and the next have
   ended, no older header is live, and their files can be emptied. The moved
   runs are holes once a batch over them is durable; then the free tails are
   cut and the emptied files deleted
   ([RFC 0071](../../rfcs/0071-data-files-by-address.md), superseding
   [RFC 0069](../../rfcs/0069-compaction-into-the-holes-DEPRECATED.md)'s holes).
   Before the moves it **merges** neighbouring pages that removals left
   small (of one collection, together at most half a page), and before
   the sealing it **regroups**: a page rewritten far from its range's
   base gets a range of its own, and runs of neighbouring ranges of a few
   pages each have their pages moved the same way, side by side in key
   order, so the index folds each run into one sub-header
   ([RFC 0078](../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md) §12).
   The object store's compaction regroups its own index the same way, its
   uploads carrying the scattered runs' pages forward side by side.
   It is stepped, a bounded pass at a time; the node starts one on a daily
   UTC time, a disk ceiling, or a free-space share.
   The same merge runs **inline** past the index budget
   (`StoreOptions::index_budget_bytes`, 4 MiB by default; RFC 0081 §6): the
   entries fragments and spans add past a clean index are RAM per write,
   so once they pass the budget a batch's first round merges the runs that
   give back the most of them, and a store written without pause keeps its
   index within the budget plus the clean index of its data.

7. **The object store** ([RFC 0070](../../rfcs/0070-object-storage-backend.md),
   phases 3–6). A database created with `StoreOptions::remote` keeps its pages
   in an object store, the disk caching them. Settle rounds flag what they
   write; `upload` reassembles the flagged pages, record by record, into the
   object store's own ordered index of 128 KiB pages over the same keys, packed into files of
   `[roots block][header][pages…]` (the header whole blocks sized to what
   it names, RFC 0079; every page starts within the file's first 8 MiB; the roots block names the newest header ending a
   batch and where the chain of files stands, RFC 0073 §8), and clears the
   flags. The remote directories ride the disk's
   headers (their own lane, and the object store's chain), so a reopen
   restores them from the disk alone. Past the
   compaction ceiling the least recently used uploaded pages leave the disk
   (flagged `EVICTED`; phase 4): a read of one goes to the object store and
   marks it wanted back, and a write restores its page before it commits, so
   a page on disk is always the whole truth about its keys.
   A database whose disk is gone opens from its files alone (phase 5): the
   same header and the same walk as the disk's (RFC 0072), and the store
   adopts what the newest whole **cut** says (an upload that left nothing waiting: every batch up
   to a point, none in part), every disk page evicted. In **object mode**
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
headers**: the newest whole batch's — found from the newest data file's
last block, a roots block or a header's trailer — roots a walk back along each type's `last`, and down by `prev` while a
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
page index and its compression policy — and one **record
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
`STRUCT_HASH` has no listed slot is refused (`UnregisteredStructHash`). A data
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

A single operation can touch several values (a NonUnique insert writes the data
record **and** an entry in each of its chains). These are committed to the record cache as one
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
record and an entry set or unset per chain it touched — to the per-slot logs.
Nothing reaches the disk on that path.

**Per settle round: one write.** The round's keys are routed to their pages,
each touched page is read **once** (or not at all: a new or cached one), and every resulting image is placed in **one
contiguous window**, ending with its header, written with a single
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
| **NonUnique `save`** (update)     | record + archive + the recency re-key + every list's copy + each of `N` secondaries **whose fields changed** |
| **NonUnique `insert` / `remove`** | record + recency (or the removal log) + every list + every one of `N` secondaries                |

A NonUnique **`save`** always re-keys the recency entry and any secondary whose
fields changed, and rewrites its copy in every declared list. The `Pivot` is
**never written** after creation. The **removal log** is touched only by
`remove`; an update's superseded version is archived at a derived slot and
linked by `Metadata.succession`. The record's identity `Id` stays stable so
references don't break.

---

## Operations on records

- **Unique** — `get` resolves the fixed anchor (`STRUCT_HASH · TENANT · 1 · 0`)
  in one lookup. `save` writes the new live bytes to the anchor and chains the
  previous version into history via `Metadata`.
- **NonUnique** — a collection's `Pivot` is created **explicitly** (one per
  holder, so a nesting NonUnique mints one per record) and its `PivotId` stored
  by the holder; never auto-created. A record's **identity `Id` is fixed at
  `insert`** (stable anchor for references), so:
  - **`save`** (update) re-keys the recency entry and each secondary whose
    field changed; the superseded version is archived at a derived slot and
    linked by `Metadata.succession`;
  - **`insert`** sets an entry in the recency chain, every list and every
    secondary; **`remove`** unsets them and adds one to the removal log — the
    only op that writes it. The record bytes are never erased, keeping the
    timeline navigable.

### Chains hold anchors — reads are two-phase, lists aside

A recency, removal, secondary or fuzzy chain holds **anchors, never record
bytes**: a search scans the chain's key range and returns the matching
anchors; each is then fetched at its anchor, a normal record `get`. A
declared list is the exception it was declared to be: its entries are the
records, read with the range.

### History & space

There is **no cold tier** (the slow-node was removed — not part of the design for
now). History is first-class and **never erased**: superseded versions and
deleted-record bytes stay in the data files, reachable via the modification chain
and the removal log. So a node's data **grows unbounded** with history — and
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
| `block_size`               | Allocation unit; a page is a run of these           | 4 KiB          |
| `target_blocks_per_bucket` | A page a round rewrites past this is cut            | see `StoreOptions` |
| `record_cache_bytes`       | RAM the record cache is evicted down to             | see `StoreOptions` |
| `page_cache_bytes`         | RAM for decoded pages (RFC 0044)                    | see `StoreOptions` |
| `file_bytes`               | A data file closes at this                          | 10 GiB         |
