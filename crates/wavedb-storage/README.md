# wavedb-storage

The per-node **storage engine**: the block manager, the per-`STRUCT_HASH` page
directory (linear-hashed), the page format, dictionaries, and the journal-backed
write pipeline. This is where most of WaveDB's engineering energy lives.

> For the project-wide idea and quickstart see the
> [root README](../../readme.md). The browser does **not** use this engine — see
> [`wavedb-wasm`](../wavedb-wasm/README.md) for the IndexedDB key→value path.

## Module map

| Module            | Responsibility                                                                                                                                       |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `block`           | `BlockDescriptor` (u43·u20·u1) + `Run` + `BlockAllocator` — alloc/free/coalesce runs of 4 KiB blocks.                                                |
| `block_file`      | `data.bin` as a block-addressed file: superblock (block 0), positioned run I/O, grow/truncate, fsync.                                                |
| `dictionary`      | Per-`STRUCT_HASH` raw-content zstd dictionary (`Dictionary` + `DictState`): capped append-only buffer, version = prefix length, own run persistence. |
| `directory`       | The per-`STRUCT_HASH` page directory container + the linear-hashing addressing math (pure addressing).                                               |
| `directory_pages` | The directory's page **reads**: resolving a bucket to its `SlotPage`, decompressing against the passed-in `DictState`.                               |
| `plan`            | A checkpoint's phase 1: touched ids → page images grouped per bucket, splits decided, nothing written.                                               |
| `checkpoint`      | Phases 2–4: one best-fit window for every image, one positioned write, then the descriptor swap.                                                     |
| `edit`            | The addressing delta riding each window (`EditChunk`, chained by `prev`), its recovery fold (`Replay`), and the chain walk (`walk`).                 |
| `meta_log`        | Which chunks are live, the `Commit` frame's `head`, and when a round should compact the chain into a fresh snapshot.                                 |
| `retire`          | A checkpoint's deferred half: the retired journal + protection roll, held for the _next_ checkpoint to dispose of.                                   |
| `settle`          | The drain queue: what a round is, when it retries, and the cache-eviction budget.                                                                    |
| `chain`           | Directory chain blocks — the persisted address vector, copy-on-write, placed in the checkpoint's window.                                             |
| `compact`         | Compaction's phases and surface (`compact_step`, `compact`, `reclaim_tail`, `space`): move, drop the old addressing, seal, truncate.                 |
| `defrag`          | Compaction's moves: the highest runs, verbatim, into the lowest hole below them — one window write per round.                                        |
| `page`            | `SlotPage` — the homogeneous record page: `[len][to_wire_checked(PageEnvelope)]`, body raw or zstd.                                                  |
| `journal`         | Append-only WAL of `Write` batches (checked wire frames); fsync = durability; torn-tail-tolerant replay.                                             |
| `struct_storage`  | `StructStorage` — one type's own cache + directory slot (`#[wavedb]` emits one `static` per type).                                                   |
| `page_store`      | `PageStore` — the node's authoritative `Store`: journal-first → per-type record logs → settle into pages.                                        |
| `error`           | `StorageError` / `StorageResult`; flattens to `wavedb_core::Error::Backend` at the `Store` seam.                                                     |

Settle and rebalance are **off the mutation path**: `apply` only journals,
commits to the caches, and queues the touched ids; the node's maintenance loop
drains that queue and checkpoints on a journal-size threshold. The
`Pivot`/`BpTree` index layer lives in `wavedb_core::index`, not here.

---

## Where this crate runs

This crate is the **node's authoritative engine** — native only: filesystem
`data.bin` (block runs) + `journal`. It provides the native **`PageStore`** — the
disk-optimised `Store` backend (`get` + atomic `apply`) — that the `Store`-generic
`Pivot`/`BpTree` contracts in [`wavedb-core`](../wavedb-core/README.md#index-contracts--pivot-bptree-indexkey)
run over. The index logic itself is portable; pages, blocks, journal, and the
allocator are this crate's internals, hidden behind the `Store` seam.

It is **not** the client-side local store. The client's local key→value cache
(`Store` trait) is a separate, lighter thing: a file kv on native, IndexedDB on
web ([`wavedb-wasm`](../wavedb-wasm/README.md)) — no pages, no journal. See
[`wavedb-core` § the `Store` trait](../wavedb-core/README.md#store--the-local-backend-trait).

---

## Block manager

`data.bin` is an **array of fixed 4 KiB blocks**. The block manager owns it:

- **Allocate** a contiguous run of `n` blocks for a new or growing page,
  returning its start block.
- **Free** a run when a page is relocated, emptied, or its type is dropped.
- **Coalesce** adjacent free extents so large pages always have somewhere to
  land, and **truncate** the tail when trailing blocks are free.

Free space is tracked as **extents** indexed by position (to coalesce on free)
and by size (best-fit, to satisfy "give me `n` contiguous blocks"). The allocator
is a pure in-memory structure; durability is the pipeline's job — **every alloc
and every free is appended to the journal** so a crash can replay them and never
leak or lose blocks.

---

## On-disk metadata — the superblock & the WaveWire rule

### Block 0: the superblock

The first `RESERVED_BLOCKS` (currently 1) blocks of `data.bin` are engine
metadata — the allocator never hands them out. Block 0 is the **superblock**:
it stamps the file as a WaveDB data file and carries the per-database facts.
Layout (little-endian, zero-padded to the block):

| Offset | Field                             | Size | Meaning                                                                         |
| ------ | --------------------------------- | ---- | ------------------------------------------------------------------------------- |
| 0      | magic                             | 8 B  | `WAVEDBIN` — "this is a WaveDB data file". Mismatch ⇒ `StorageError::BadMagic`. |
| 8      | `to_wire_checked(SuperblockBody)` | 40 B | `[crc32 (u32 LE)][wire]` of `{ version: u32, seed: [u64; 4] }`.                 |

The body is the **checked wire encoding** of `SuperblockBody` — the format
version (`BadVersion` if it mismatches; pinned at 1 pre-release, see the
[root README](../../readme.md)) and the per-database random SeaHash seed that
`hash_of` routes every record with. A crc failure surfaces as
`Corrupt("superblock")`.

A fresh file mints a random seed and persists the superblock before anything
else; opening an existing file validates magic + version and loads the seed.
Keeping the seed in block 0 is what lets a `data.bin` rebuilt by journal replay
on another machine route every `Id` into the same bucket.

### One layout language: WaveWire (+ checked framing)

Engine metadata is described with **the `WaveWire` codec only** — the same
layout language records use. No structure gets its own bespoke byte format:

- **superblock body** (version + seed) — a plain `WaveWire` struct behind the
  magic. The 8-byte magic itself stays a raw prefix _outside_ the wire payload:
  it must be checkable before any decode is attempted;
- **block descriptor** (`start u43 · count u20 · flag u1`, exact-packed
  into one `u64` — see the table below) — wire-encodes as that `u64`;
- **page / dictionary directories** (`Vec<u64>` of descriptors) — the standard
  `WaveWire` `Vec` encoding;
- **page body** (`struct_hash` + the `(id, bytes)` entries) — a plain
  `WaveWire` struct (`PageBody`).

Where a structure must survive disk corruption — **pages, journal frames** —
it uses the wire crate's [`validation` feature](../wavedb-wire/README.md#feature-validation--crc32-framed-encoding):
`to_wire_checked` / `from_wire_checked` frame the payload as
`[crc32 (4 B LE)][wire bytes]`, so integrity verification is the codec's job —
not a hand-patched CRC slot re-implemented in every format. A corrupted page
surfaces as the wire `CrcMismatch` before any decode runs.

> **Status.** Fully in effect: the superblock body, journal frames, **and
> pages** are all on checked `WaveWire`. No engine structure hand-rolls its
> byte layout anymore; the only raw prefixes left are the 8-byte superblock
> magic (checkable before any decode) and the `u32` length that delimits a
> payload inside a zero-padded run / append-only log.

---

## Per-`STRUCT_HASH` page directory

`data.bin` is **partitioned by type**: each `STRUCT_HASH` owns its own page
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
in the first page of `data.bin`** (page 0), read once at startup. The seed is
per-database (not the fixed `STRUCT_HASH` seed): each node rebuilds its own
directory consistently, and an attacker can't precompute bucket collisions. SeaHash
is portable across architectures and endianness, so a `data.bin` rebuilt by journal
replay on another machine routes every record to the same bucket. The resulting
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
  get the exact payload slice for its crc to hold (same pattern as a journal
  frame).
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

The dictionary is stored in `data.bin` like a page: a block run handed out by
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

On open the dictionary is rebuilt exactly, for free: `data.bin` is a
journal-replay projection, and the settle path re-samples the same records in
the same order — every `dict_len` a rebuilt page stamps resolves against the
rebuilt buffer. Reading the persisted run back becomes load-bearing only when
settling checkpoints.

CPU is free here — there is no join processing competing for it. Per-value
(strings/blobs) heap compression stays future work.

---

## Write pipeline & concurrency

```
mutation → journal append → the type's own BTreeMap<Id> cache → (client confirmed)
                                          │
                                          └─ background: settle into data.bin,
                                             then rebalance (split_next)
```

1. **Journal first.** Every insert/save/remove — and every block alloc/free — is
   appended to the journal before anything else. This is what makes the durability
   guarantee and prevents leaks.
2. **Cache as a `BTreeMap<Id, …>` per type.** Confirmed mutations live in the
   type's own in-memory `BTreeMap` (its `StructStorage` slot, below); reads
   serve from it directly (and, because `Id` is ordered by `KEY`,
   range/timeline reads are naturally ordered).
3. **Background settle — one window per round.** A drain task takes the queued
   ids, groups them by bucket, reads each touched page once, and writes every
   resulting image into **one contiguous window** with a single positioned write
   ([RFC 0041](../../rfcs/0041-single-barrier-checkpoint.md)). The descriptors
   are swapped only after the write lands, so a reader sees either the old pages
   (still allocated — page writes are copy-on-write) or the new ones.
4. **Rebalance is part of that plan.** Splits are decided in RAM, before
   anything is serialised, so an over-sized bucket is repartitioned and both
   halves are written once — never a page written twice in one round.
5. **The window describes itself.** Its last blocks carry an `EditChunk`: the
   descriptor changes those pages just caused
   ([RFC 0046](../../rfcs/0046-directory-deltas-in-the-window.md)). Metadata
   therefore costs **no IOp of its own** — same `write_run`, same `fsync` — and
   the `Commit` frame shrinks to one address: the chain's head.
   Once the deltas outweigh the state they patch, a round emits a full chunk
   instead and the superseded runs are freed.

6. **Compaction.** Copy-on-write leaves holes, and best-fit only reuses the
   ones a window happens to fit. Compaction moves the runs nearest the end of
   the file — pages and dictionaries, verbatim — into the lowest hole below
   them, rewrites the addressing chain as one low snapshot, lets two
   checkpoints release what the moves vacated, and cuts the free tail off
   `data.bin` ([RFC 0069](../../rfcs/0069-compaction-into-the-holes.md)). It
   is stepped, a bounded pass at a time; the node starts one on a daily UTC
   time, a disk ceiling, or a free-space share.

7. **The object store** ([RFC 0070](../../rfcs/0070-object-storage-backend-WIP.md),
   phases 3–6). A database created with `StoreOptions::remote` keeps its pages
   in an object store, the disk caching them. Settle rounds flag what they
   write; `upload` reassembles the flagged pages, record by record, into the
   object store's own linear hash of 128 KiB pages, packed into files of
   `[32 KiB header][pages…]` (every page starts within the file's first
   8 MiB), and clears the flags. The remote directories ride the disk's
   addressing log, so a reopen restores them from the disk alone. Past the
   compaction ceiling the least recently used uploaded pages leave the disk
   (`BlockDescriptor::EVICTED`; phase 4): a read of one goes to the object
   store and marks it wanted back, and a write restores its bucket before it
   commits, so a page on disk is always the whole truth about its bucket.
   A database whose disk is gone opens from its files alone (phase 5): every
   header names every type and chains to the one before by hash, its spare
   room restates the oldest entries, and the store adopts what the newest
   whole **cut** says (an upload that left nothing waiting: every batch up
   to a point, none in part), every disk bucket evicted. In **object mode**
   (`PageStore::open_object`, phase 6) there is no disk at all: what would
   be `data.bin` sits in RAM, there is no journal — a write is durable once
   an upload carries it, which happens on 8 MiB or an interval — and every
   open is such a recovery. Every file is created only if absent, so a
   second writer on the prefix is fenced (`StorageError::Fenced`), never
   merged; what navigation reads is resident (`StructStorage::resident`).

On startup the directories and the block allocator rebuild by **journal replay**.

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
directory has **one open `PageStore` at a time**: `data.bin` is locked while
open, so a second open of the same directory — in this process or another —
fails with `EngineBusy`. Stores over different directories open side by side.

**Threading.** A store is used from **one thread** — a node moves each onto the
worker that owns it (RFC 0067) — so its state lives in plain cells (`RefCell`,
`Cell`): no lock, no atomic. `PageStore` is `Send` and deliberately not `Sync`.
Reads (`get_of`, routed by the compile-time-known hash) are one binary search
over the sorted registry, one index probe, and a page read on a miss. The
cache commit follows the journal append, so the logs' order is the journal's.
A settle round copies each record's **newest entry** out of its log and skips
the copies it superseded, so it is idempotent — `data.bin` stays a pure
projection of the journal. Eviction drops whole chunks the settle has passed,
measured against what the cache actually allocates.

### Atomicity is the journal entry + cache commit

A single operation can touch several records (a NonUnique insert writes the data
record **and** a `BpTree` node). These are committed as **one journal entry**,
and the cache commit runs under the journal lock — so commits are totally
ordered, and a crash before the background settle replays the journal back into
the record cache, whole batches or nothing. No separate transaction manager: the
journal entry + cache commit _is_ the atomic unit. Visibility is per-`get`
(each read sees a fully committed value, never a torn one); a multi-`get` walk
was never snapshot-isolated — an apply can land between two gets, exactly as
under the old store-wide mutex. (Multi-write **server-function** transactions —
several operations as one unit — remain a separate, later concern.)

### IO cost

**Per mutation: one write, one barrier.** `Store::apply` appends the whole batch
— the record, every touched `BpTree` node, the `Pivot` rewrite when a root moved
— as **one** journal frame (`write_all_at` + `fsync`), commits it to the per-type
caches, and queues the touched ids. Nothing else reaches the disk on that path,
and how many records a batch carries changes its size, not its IO count.

A store opened with a **durability window** (`StoreOptions { relax_window }`,
[RFC 0061](../../rfcs/0061-relaxed-durability-window.md)) keeps the one write
and drops the per-batch barrier: `apply` appends without `fsync` and syncs once
per elapsed window, so a burst amortises to a single barrier. `Ok` then means
_journalled, ordered, and durable within the window_ — a crash loses a suffix
of acknowledged writes and never corrupts. `PageStore::flush` forces the
barrier. **Default zero**: one barrier per batch, unchanged.

**Per settle round: one write, no barrier.** The pending ids are grouped by
bucket, each touched page is read **once**, and every resulting image is placed
in **one contiguous window** and written with a single positioned write
([RFC 0041](../../rfcs/0041-single-barrier-checkpoint.md)). Reads are the floor:
one per page the round modifies. No `fsync` — durability stays the journal's
until a checkpoint.

**Every window also carries its own descriptor delta** — an `EditChunk` naming
the buckets that round moved, in the same write, for no extra IOp
([RFC 0046](../../rfcs/0046-directory-deltas-in-the-window.md)).

**Per checkpoint: one write, ONE barrier.** The window additionally carries a
grown dictionary; then `data.bin` is synced — and that is the only barrier. The
`Commit` frame is a _pointer_ into state that sync already made durable, so it
is appended **unsynced** and the next ordinary write's fsync carries it. Each
chunk names the one before it, so the frame names only the head
([RFC 0048](../../rfcs/0048-chained-addressing-log.md)): a fixed 16 bytes
whatever the database's size **and** whatever the log's length — where
carrying the descriptors cost 8 bytes per bucket, and carrying the chunk list
made a compaction cycle quadratic in its own interval.

Deleting the retired journal and rolling the allocator's protection wait for that
durability, and are then done by the **next** checkpoint — which holds the
journal carrying the frame and reads its barrier count to know
([RFC 0047](../../rfcs/0047-generational-journal-retirement.md)). So two journals
on disk is the steady state, nothing is chased from the write path, and **no
barrier is paid anywhere on a checkpoint's behalf**; `force_retirement()` remains
for shutdown, where there is no next checkpoint. `BlockFile::io()` and
`Journal::barriers()` count all of this, and the accounting is asserted in
`page_store`'s tests.

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
deleted-record bytes stay in `data.bin`, reachable via the modification chain and
the `dead` `BpTree`. So a node's `data.bin` **grows unbounded** with history — and
that is **accepted for now**. History pruning / compaction / a separate archive
tier is left for later; nothing reclaims old versions today.

---

## Reliability

- **Journal** — append-only, replayed on startup; record writes and the
  alloc/free deltas are journaled together so a mid-mutation crash can't desync
  the directory from the data.
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
