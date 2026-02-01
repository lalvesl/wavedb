# RFC 0072 — The settle is the durability point

- **Status:** Implemented — opened 2026-10-16; phases 1–4 landed 2026-10-16
  (see Progress)
- **Crates:** `wavedb-storage` (the cycle, the header, the reconstruction),
  `wavedb-quick-node` (the tick, the barrier), `wavedb-core` + `wavedb-macros`
  + `wavedb-net` + `wavedb` (the `_durable` operations, the client cache's
  tick)
- **Supersedes:** [0041](0041-single-barrier-checkpoint.md)'s checkpoint (its
  one-window settle round is kept),
  [0043](0043-descriptors-in-the-commit-frame.md),
  [0046](0046-directory-deltas-in-the-window.md)'s `Commit` frame (the delta
  riding the window is kept, as the header),
  [0047](0047-generational-journal-retirement-DEPRECATED.md),
  [0048](0048-chained-addressing-log-DEPRECATED.md),
  [0061](0061-relaxed-durability-window-DEPRECATED.md); changes
  [0070](0070-object-storage-backend.md) §5–§6 (the file header and the
  recovery walk become the disk's) and
  [0071](0071-data-files-by-address.md) §5–§7 (protection and sealing go)
- **Builds on:** [0067](0067-thread-per-core-shards-WIP.md) §4 (the barrier
  helper), [0068](0068-record-cache-as-a-log-WIP.md) (the logs and their
  settle cursors)

## Summary

There is no journal file any more. A write lands in the record cache and the
task goes on; every 200 ms a shard **settles** what was written into one
window at the end of its active data file, and that window ends with a
**header of 32 KiB** naming the addresses the batch changed, type by type.
One `fsync` per settle makes it durable. On open, the last 8 blocks of the
newest data file are the newest header, and the per-type `Vec<u64>` of
addresses are rebuilt by walking headers back — the object store's recovery
(RFC 0070 §6), now shared by both.

Writes come in two kinds. `save`, `insert`, `update`, `remove` change the
cache and return — durable at the next settle. `save_durable`,
`insert_durable`, `update_durable`, `remove_durable` change the cache and
**suspend** until the settle carrying them is `fsync`ed; the owner stays
locked meanwhile, so nobody reads it before it is durable.

## Motivation

- **Every byte was written twice.** A batch went to the journal, then — at a
  settle — to its page; the journal existed to be thrown away. The page write
  is the one that has to happen, so it should be the durable one.
- **The journal was a second recovery model.** Recovery replayed batches from
  journals and rooted the pages in a `Commit` frame naming a chain of edit
  chunks; the object store recovered from file headers alone. Two walks, two
  formats, two sets of tests for one question: where is every page?
- **Durability is per operation, not per store.** RFC 0061's window made a
  whole store relaxed or strict. An application wants the cart relaxed and the
  receipt strict; that is a choice of call, not of store.
- **Checkpoint machinery.** Rotation, retirement, two protected generations
  and the deferred `Commit` frame all exist to keep the journal and the pages
  in step. Without a journal none of it has a job.

## Design

### 1. Two kinds of write

| | in the cache | returns when |
|---|---|---|
| `save` / `insert` / `update` / `remove` | at once | at once |
| `*_durable` | at once | the settle carrying it is `fsync`ed |

A non-durable write is lost if the process dies before the next settle is
durable: at most a tick (200 ms) plus the `fsync`. A durable one waits for
that tick — anywhere from ~0 to 200 ms, 100 ms on average, plus the `fsync`.

A settle is a **cut** (§2): it holds everything committed before it. So a
durable write also makes everything committed before it **on the same
store** durable — on a node, the same shard. Within one store a durable
insert into a new collection would carry its pivot along; on a node the
pivot and the record holding its `PivotId` usually live on different shards,
so `create_pivot` has a durable twin too (§12): the collection made durable
before its holder is, and a crash never leaves a holder naming nothing.

The `Write::Expect` guard and the atomic batch are unchanged.

### 2. The settle cycle

Every 200 ms, per shard (the node's BG task, §9):

1. **Open**: mark every type's log at its current end — the `rotate` the
   checkpoint already had, now with no journal behind it. The engine runs on
   one thread, so the mark falls between two batches.
2. **Rounds**: bounded rounds of what the mark owes, the jobs running between
   them as today. A round is one positioned write at the end of the active
   file: `[pages][header]`. A round ends early when its header is full, and
   the rest goes to the next round of the same cycle.
3. **Close**: the round that settles the last owed entry marks its header
   `end_of_batch`. The cycle's `fsync` is asked of the barrier helper; when it
   answers, every durable waiter of the cycle is answered.
4. A cycle opens only once the previous one's `fsync` has answered, so **at
   most one batch is not durable** at any time.

A tick with nothing to settle is skipped: no window, no header, no `fsync`.
An idle node or client — the common case — costs nothing on the disk, and a
chain that does not grow needs no restating.

The waiting reuses the helper as it is (RFC 0067 §4): the task that asked
for the `fsync` returns to the executor, which runs other tasks — most likely
ones writing to the cache — while the helper thread syncs; the helper serves
every request queued with one `fsync` per file.

### 3. The header

32 KiB (8 blocks), at the **end** of each window, encoded with the wire
(`#[derive(WaveWire)]`, `to_wire_checked` — its crc is the check):

```rust
struct Header {
    /// Where this header's 32 KiB start (disk), or this file's number
    /// (object store): a header copied elsewhere is not this one.
    at: u64,
    /// Where the previous header's 32 KiB start — the reconstruction's way
    /// back when a type is still missing.
    prev: u64,
    /// The lowest header a reconstruction rooted here reads (§8's line):
    /// every type that existed when this header was written is named at or
    /// above it.
    horizon: u64,
    /// The last header of a batch (§2).
    end_of_batch: bool,
    /// The object store's files: the oldest file not deleted, and the
    /// remote end once the batch is in. Zero on disk.
    start: u64,
    end: u64,
    /// The types this batch changed, then the types the spare room restates.
    types: Vec<TypeEntry>,
    /// Disk + object mode: the object store's directories, the same way,
    /// and where its chain of files stands.
    remote: Vec<TypeEntry>,
    chain: Option<ChainRecord>,
}

struct TypeEntry {
    struct_hash: u64,
    /// The directory's bucket count: a linear-hashing split grows it.
    buckets: u32,
    /// The previous header with entries for this type.
    last: u64,
    dict: Option<u64>,
    /// `(bucket, address)`: what the batch changed, and what is restated.
    changed: Vec<(u32, u64)>,
}
```

An item is 12 bytes, so a header holds about 2 700 addresses. Overflowing it
takes a shard rewriting 2 700 pages in 200 ms — 10 to 85 MB, 50 to
400 MB/s per shard — which a bulk import or a compaction pass may do, and
which only makes the cycle take one more round: a header names as much of
what its window changed as it holds, the rest stays **unrecorded** for the
next header, and only a header that leaves nothing unrecorded may end a
batch. What uploads and evictions change between windows waits the same way.

No hash chain: nothing is ever rewritten in place — the disk only appends,
the object store only creates — so a header's crc and its own `at` are the
whole check.

### 4. Per type, in RAM

```rust
struct TypeAddresses {
    addresses: Vec<u64>,
    /// The newest header with entries for this type.
    last_header: u64,
    /// Where the next restated addresses begin.
    shift: u64,
    /// The header in which the spare room last restated this type.
    last_shift_header: u64,
}
```

The same for the disk and for the object store (whose pages are larger).

### 5. The spare room

What the changed types leave of the 32 KiB restates addresses of the types
with the **smallest `last_shift_header`** — the disk only allocates forward,
so the smallest is the one restated longest ago — from their `shift`, as many
as fit. A type restated has its `last_header`, `shift` and
`last_shift_header` moved; a type only changed has its `last_header` moved.

Ordering by `last_header` would not do: a type changed in every batch would
never get the spare room, and WaveDB's fixed ids make that the common case —
a collection's `Pivot`, its B+tree nodes and chain segments keep their ids
when they change, so the same few buckets of the node type are rewritten in
every batch while the rest of its buckets hold still. Their addresses would
live only in ever older headers, the reconstruction would walk back to them,
and the compaction could never let those headers go.

A registered type never written has `last_shift_header = 0`, so the first
batch names every such type, with `buckets = 0`; and a type with nothing in
it keeps appearing in its turn. So every registered type is named within a
bounded number of headers, and the reconstruction never searches the whole
chain for one.

### 6. Reconstruction

1. Read the **last 8 blocks** of the newest data file: the newest header
   (crc, and `at` must be where it was read).
2. Go back by `prev` to the newest `end_of_batch` header. The batch it closes
   — its headers back to the previous `end_of_batch` — is the root.
3. Linear, with a `BTreeMap<address, Header>` of the headers read: start
   every type present in the newest header; follow each type's `last`, the
   first address seen for a bucket being its newest. While a registered type
   is missing, read the header before (`prev`), and follow the missing type
   from where it appears. A type is closed once every bucket is known.
   The way down stops at the root's `horizon`: every type that existed when
   the root was written is named at or above it, so a registered type still
   missing there was added to the code since — it is new, and empty. Without
   the bound, the first open after a type is added would read down through
   every surviving header, the dead ones below the horizon included, and
   hold them live.
4. The allocator is the files' layout plus every page run, dictionary run and
   header run the walk named; the rest is holes. Past the root's window is
   what a crash left: cut.
5. Nothing to replay; the record cache starts empty.

### 7. A batch caught by the crash

Pages are wire-checked, and a header being whole does not make the pages
before it whole: one write reaches the disk in any order, so a crash before
the `fsync` can keep the header block and lose a page block. Only the last
batch can be in that state (§2.4). So the open reads the pages that batch
names and checks their crc; if one fails, that batch was never durable — no
durable waiter of it was answered — and the root is the batch before it, at
`prev`. A header whose own crc fails is found past by reading back block by
block to the newest valid one — after a crash only.

### 8. Space

- **A free waits for its batch.** The disk only appends, so a freed address
  is never written again; what must wait is giving it back — a free tail cut,
  an emptied file deleted. A free is deferred, and becomes a hole once the
  batch that stopped naming it is durable (a mark per batch, released when
  its barrier answers). The reclaim cuts only holes, at any time, and is
  idempotent: a crash in the middle leaves holes the next reconstruction
  sees free.
- **Headers die below the horizon.** A header run is live while a
  reconstruction from the newest batch may read it: for a type, at or above
  where its last whole rotation of the spare room began (or the newest header
  naming it, for a type with no address); before any rotation since the open,
  at or above the lowest header the open read; and never below the header
  ending the batch before, which the check of §7 reads. At each batch's end
  the headers below that line are freed, deferred like any run.
- **Compaction** starts with a **full restatement** (`record_all`): every
  address of every type unrecorded, so the headers of the next batch name
  them all — and once that batch and the next have ended, every older header
  is dead. A file holding a live header is never a victim (a header cannot
  move); the active file, if worth emptying, is closed first so the
  restatement lands in a new one. The victims' runs are carried forward as
  before; they become holes once a batch over the moves is durable, and the
  reclaim then cuts and deletes. Phases: restating, moving, sealing.
- `convert` and the adoption of a database recovered from the object store
  use the same full restatement where they wrote a snapshot and a
  checkpoint.

### 9. The object store

The same `Header`, the same walk: `at` is the file's number, `prev` the file
before (file numbers are not contiguous — a large page skips numbers). The
`prev_hash` chain goes; a multi-file upload is whole when its files are all
present, valid, and the last says `end_of_batch`. The spare room follows §5.
Object mode is unchanged otherwise: no disk, durable once uploaded.

### 10. The node

- BG opens a cycle every 200 ms on each store with something to settle, and
  steps it when the valve says; the valve weighs the bytes not settled. A
  durable write waiting on an open batch goes ahead of the valve.
- The barrier helper takes the cycle's `fsync`.
- `close` settles every store and waits for its `fsync`.
- Gone: `relax_window`, `checkpoint_after_bytes`.

### 11. The client

The native cache settles on the same tick: it moves out of the `Db`'s `Rc`
onto a carrier with the same 200 ms task, and the `Db` reaches it by channel,
as it reaches the connection manager. A store nobody ticks — a test, an
embedding with no carrier — runs the cycle inline inside a durable write.

What crosses the channel is the `Store` seam itself — a read, a batch, a
durable batch — so every typed operation (mirrors, adoption, the warm walks)
runs where it did, over a proxy store. The carrier owns the `PageStore`
alone and serves one request at a time; a durable batch is settled inline
there (no barrier helper), so the `fsync` is on the cache's thread, never
the application's. Each tick drains what was committed — nothing when
nothing was — and evicts the record cache to its budget. Dropping the last
`Db` closes the channel, and the drop returns once the carrier has settled
and closed the store, so the directory reopens at once.

A store reached by channel suspends, as IndexedDB always did, and a mirror
is a read-plan-apply: two interleaved on one executor (a watch's and a
write's) would each write a tree node built without the other's entry. So
every mirror takes the cache's **turn** first — a flag and its waiters, one
thread, no lock — on both targets.

The client's writes mirror into the cache as ordinary writes either way: the
node is the truth, and a cache that loses a mirror refills by catch-up. A
durable save is never queued offline — the queue lives in memory and could
not keep the promise — so it surfaces the transport fault.

The wasm IndexedDB store is a plain key-value map, durable at its
transaction's commit: untouched by this RFC.

### 12. The `_durable` operations

- `Store::apply_durable`, defaulting to `apply` where a store has no settle
  (the in-memory store, IndexedDB). A store that forwards to another — `Rc`,
  `&S`, the node's notifying wrapper — forwards it too.
- **`Durability<S>`** (core): a store whose `apply` goes to `apply_durable`
  when a flag says so. Every mutating operation is one `apply`, written once
  in the collection and record layers; a durable operation is the same
  operation over this wrapper, so no write path is written twice. The flag is
  a field rather than a type, so a node runs one monomorphised dispatch for
  both kinds of command.
- `DbHandle` gains `save_unique_durable`, `create_pivot_durable`,
  `insert_durable`, `update_durable`, `remove_durable`, implemented by
  `LocalHandle` (over `Durability`), `RouteHandle` (the flag on the
  `NodeOp`), `ServerDb` and `Db` (the flag on the frame).
  `CollectionHandle` gains `insert_durable` / `save_durable` /
  `remove_durable`, and the macros `save_durable` (Unique) and
  `create_pivot_durable` (NonUnique). The engine-facing `Collection` takes a
  `Store`, so it needs no twin: `Durability::new(store, true)` is one.
- On the wire, a `durable` flag on the command frame — one bit, rather than
  four more commands; a read and a function call ignore it. The node runs
  the frame's step over `Durability::new(store, frame.durable)`, the owner
  held until the batch is durable.

## Gone

`journal.rs`, `journal_frame.rs`, `commit.rs`, `commit_steps.rs`, `retire.rs`,
`meta_log.rs`, the edit chunk and its replay; `commit_journal`, `force_retirement`,
`begin_checkpoint`/`checkpoint_step`, `flush`, `journal_len`; the allocator's
protected sets; `StoreOptions::relax_window`. `drain()` becomes whole cycles,
`fsync` included, and a store dropped drains.

## Phases

1. **The header and the walk**, shared by disk and object store (§3–§6, §9).
2. **The engine without a journal**: the cycle, `apply_durable`, the
   reconstruction and the last batch's check, the allocator and the reclaim,
   compaction's full restatement, `convert`, the adoption.
3. **The node**: the tick, the helper, the valve, the configuration.
4. **The surface and the client**: §11–§12.

## Progress

- **Phase 1 — landed 2026-10-16** (`header`, `restate`, `walk`,
  `upload_file`, `recover`, `recover_headers`): one `Header` for the object
  store's files, no hash chain, the spare room by the oldest restatement,
  empty types first; the linear walk with the way down by `prev`.
- **Phases 2–3 — landed 2026-10-16** (`cycle`, `disk_header`, `disk_chain`,
  `reopen`, `checkpoint`, `barrier`, `alloc`, `compact`, `defrag`;
  `wavedb-quick-node`'s `shard::{owned,background}`):
  - `apply` commits and returns; `apply_durable` waits for its batch — on a
    store with no helper it settles the batch itself; in object mode it waits
    for an upload. A store dropped settles what it holds.
  - Reconstruction as §6–§7, the root's header and every header the walk read
    kept live, the torn tail cut.
  - The node ticks a cycle every 200 ms (`Owned::upkeep`), steps it from BG,
    and a durable write waiting is urgent; `close` drains. `relax_window` and
    `checkpoint_after_bytes` are gone.
  - The benchmarks keep their durable and relaxed rows: the driver settles
    after every write (inside the timed window) or once per window.
  - Tested by crashes as the disk is left: the files copied while the store
    holds them open (`tests/durability.rs`, `compaction`, `eviction`,
    `object_store`, `remote_compaction`, `counts_survive_recovery`) — a
    write not yet settled lost, a batch cut before its end not back at all,
    a torn page or header giving way to the batch before.
- **Phase 4 — landed 2026-10-16** (`wavedb-core`'s `durability`, `handle`,
  `local_handle`, `route_handle::ops`, `node_op`; `wavedb-net`'s
  `CommandFrame::durable`; `wavedb-quick-node`'s `dispatch`; `wavedb`'s
  `client_writes`, `cache::{native, turn}`; the macros):
  - The `_durable` operations on every context, as §12; the node answers a
    durable frame or node operation once the owner's shard settled it.
  - The native client cache on a carrier of its own with the 200 ms tick
    (§11); mirrors take the cache's turn on both targets.
  - `create_pivot` gained a durable twin after all (§1): on a node the
    collection and its holder live on different shards.
  - Tested: each twin is one durable batch (`local_handle`), a durable node
    operation and a durable frame reach `apply_durable` (`local_handle`,
    `dispatch`), the engine's seam is durable on return
    (`tests/durability.rs`), the client cache durable at once / on the tick /
    on drop (`wavedb/tests/local_cache_settle.rs`); the contact-book
    end-to-end tests kill the node right after durable writes, with no
    wait, and fail when the node ignores the flag.

- **The horizon in the header — landed 2026-10-17** (`header`,
  `disk_header`, `upload_file`, `recover`, `walk`): `Header::horizon`, the
  line §8 frees below, now written into every header — the disk's from its
  own rotations and the object store's lanes, an uploaded file's from the
  remote rotations (`Chain::floor`, recorded in `ChainRecord` and set by a
  recovery from the root's horizon) — and the way down by `prev` stops
  there; it only ever goes down, so a block a dead header was freed from and
  that holds a newer one since cannot lead it back up. Covered by the
  existing reopen and recovery tests.

## Testing

- Reopen without anything to replay: every record of every type back, types
  never written included.
- A crash in the last batch: a page block of it zeroed, the header kept —
  the reopen roots in the batch before; a torn header — the same.
- A batch spread over several rounds and cut before `end_of_batch`: none of
  it comes back.
- A durable write is answered only after its cycle's `fsync`; a non-durable
  one at once.
- The spare room: a database with many types reconstructs from a bounded
  number of headers; a type never written is named in the first batch.
- Compaction: after a full restatement, the victims are deleted and the
  reconstruction reads no header in them.
- The object store's recovery, unchanged in what it proves, over the new
  header.

## Alternatives

- **A hash of each window's body in the header.** The pages carry their own
  crc already; checking the last batch's pages by it costs the same reads.
- **Two `fsync`s per cycle** — pages, then the header — so a whole header
  proves its pages. Twice the barriers, for a check needed only after a
  crash.
- **Every type in every header**, as the object store had: the newest header
  alone would name every type, at ~30 bytes per type per header; the spare
  room's turn (§5) names them within a few headers for nothing.
- **A larger header.** The same cost in every batch and every read, for a
  case that only adds a round.
- **Ordering the spare room by `last_header`**, the newest header naming the
  type: a type changed in every batch would never be restated (§5).
