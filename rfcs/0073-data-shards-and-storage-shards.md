# RFC 0073 — Data shards and storage shards

- **Status:** Implemented — opened 2026-10-17; phases 1–6 landed 2026-10-17
  (see Progress)
- **Crates:** `wavedb-storage` (the split of the store, the roots block, the
  reconstruction), `wavedb-quick-node` (the storage threads, the routing
  of settle rounds), `wavedb-platform` (the thread, `pwritev`)
- **Changes:** [0067](0067-thread-per-core-shards-WIP.md) §2 (a shard no
  longer owns files), §7 and §8 (durability and I/O per storage shard);
  [0072](0072-the-settle-is-the-durability-point.md) §2, §3, §6–§8 (the
  window goes through a storage shard, the header names its data shard, a
  reopen roots in the roots block); [0070](0070-object-storage-backend.md)
  §4–§6 (one chain of files per storage shard);
  [0071](0071-data-files-by-address.md) (the data files belong to the
  storage shard)
- **Builds on:** [0067](0067-thread-per-core-shards-WIP.md) §4 (the barrier
  helper: a thread that shares no memory, only channels)

## Summary

A database keeps its **P data shards** — the unit of placement, of
ownership and of the record cache, `seahash(owner) % P` as today — and
gains **S storage shards**, S ≤ P and dividing it, fixed at creation: P = 64
may write to S = 4. A storage shard is a set of data files (and, with an
object store, one chain of files); data shard `p` writes to storage shard
`p / (P / S)` and to no other.

Each storage shard has **a thread of its own**, outside the Embassy
executors, that owns its allocator and its files. The workers keep doing
everything they do today, and send each settle round — its pages and its
header, built where the data lives — to the storage thread by channel. The
storage thread gathers what arrived, writes it with **one** `pwritev`
followed by a **roots block**, and **one** `fsync` makes every data shard in
it durable. On S3, the same gathering makes **one PUT** per storage shard.

## Motivation

- **IOPS scale with P, not with the work.** Today each of the 64 shards
  settles into its own active file: up to 64 writes and 64 `fsync`s per
  200 ms tick, each a device round trip, however little each one carries.
  The scarce resources are RAM and disk IOPS; the shard count is a
  placement decision and should not multiply them.
- **S3 requests scale with P.** Every shard uploads its own files, so the
  PUT count and the object count are per shard (0070). PUTs are the
  expensive request; one upload per storage shard divides them by P / S.
- **Files scale with P.** A node holds every data file open (0071 raised the
  open-files limit for it); 64 active files grow, roll and compact
  separately.
- **The obvious answer — fewer data shards — is wrong.** P is the
  parallelism and the size of every per-owner structure (the directories,
  the record cache, the owner locks); it is fixed at creation because
  placement is an on-disk address. What should shrink is only the number of
  places that are written.

## Design

### 1. Two counts in the `MANIFEST`

The `MANIFEST` records P and S, both fixed at creation, S dividing P.
Placement does not change: an owner's data shard is `seahash % P`
(`core::placement`, golden-pinned). Its storage shard is the contiguous
range `p / (P / S)`, so a worker owning a contiguous range of data shards
(0067 §3) talks to as few storage shards as the counts allow. A worker may
still own data shards of two storage shards; each data shard talks to its
own.

```text
<root>/
  MANIFEST            P = 64, S = 4, seed, mode
  storage/
    0/  <data files, named by first block>
    1/  …
```

A data shard has no directory any more. `PageStore::open(dir)` — the
client cache, the tests — is a database of P = S = 1.

### 2. The split of the store

What a `PageStore` holds today splits along the seam this RFC introduces:

| data shard (on its worker) | storage shard (on its thread) |
|---|---|
| the per-type directories, local and remote | the allocator (`BlockAllocator`) |
| the record cache, the page cache | the data files' writer, the barrier |
| the dictionaries | the roots block, its chain |
| the settle plan, the header and its rotation | the object store's chain of files |
| a read handle per data file | the file locks, rolling, cutting, deleting |

A data shard **reads** the files directly — its own descriptors, `pread`,
nothing shared — and **writes** only through its storage shard. In a store
of P = S = 1 nobody runs a thread: the storage side is driven inline, as "a
store nobody ticks settles inline" is today. The seam is an enum of the two
(inline, or a channel), never a trait object.

### 3. A settle round, through the storage shard

A round's header carries the absolute address of every page it places and
its own `at`, under a crc. So the address must exist **before** the bytes:

1. **Reserve.** The data shard plans its round (pages, dictionary, header
   size — fixed, 32 KiB) and sends `Reserve { shard, blocks }`. The storage
   thread allocates at once — allocation is arithmetic in RAM, it waits on
   no device — and answers `base`.
2. **Build.** The data shard carves the window at `base` and builds its
   pages and header with the real addresses, synchronously, and sends
   `Write { shard, bytes, frees }`. `frees` are the runs the round
   superseded and the dead headers (0072 §8), which the allocator holds
   back until the write that frees them is durable.
3. **Done.** Once the storage write carrying it is `fsync`ed, the storage
   thread answers `Done`, with the data files opened or deleted since the
   last answer. Only then does the data shard publish its round: the new
   descriptors, the frees, the waiting `_durable` writes.

A data shard that fails between 1 and 2 sends `Abandon`; its reserved run
becomes a hole. Between 1 and 3 the settle task awaits a channel — parked,
not blocking: the worker's executor runs every other task meanwhile — so the
round may not hold a `RefCell` borrow across the await. Today's round is
synchronous end to end (`checkpoint.rs`); this is the reshaping it needs.

### 4. The storage thread

A plain OS thread, no executor:

```text
loop {
    match inbox.recv_timeout(until the next tick) {
        Reserve | Write | Abandon | Synced | Compact… => …
        Timeout => write what gathered
    }
}
```

- **One write per tick, every data shard in it.** At its tick (200 ms) the
  thread writes every `Write` that arrived, in reservation order, then the
  roots block (§5) — one `pwritev`, the data shards' buffers handed to the
  kernel as they are, no copy. It does not wait for every data shard: a busy
  worker would hold up everyone's durability; what comes late goes in the
  next write. A `_durable` write waiting makes the write go at once, as
  today.
- **The `fsync` goes to its helper** (0067 §4), and its answer, `Synced`,
  comes back on the same inbox. So the thread is never blocked on the device
  while there are reservations to answer.
- **One storage write in flight.** The next write is not written before the
  previous one is durable, so only the newest write can be cut by a crash
  (0072 §7). Reservations and `Write`s for it are taken meanwhile and wait in
  RAM.
- **Channels, not shared memory.** The inbox is an `async-channel` (as the
  workers use between themselves): a worker sends without blocking, the
  storage thread receives blocking; the answers go back on one-shot channels
  whose wakers the storage thread wakes. The atomics are inside the channel;
  our code holds no `Arc`, no lock, no atomic — the barrier helper's stance.
  (Embassy's `Channel` needs a `RawMutex`, and across threads that is
  `CriticalSectionRawMutex`, a global lock on `std`.)

### 5. The roots block

Every storage write ends with one block (4 KiB):

```text
Roots { at, prev, roots: Vec<u64> }    checked wire, the header's envelope
```

`roots[i]` is the newest header ending a batch of the storage shard's `i`-th
data shard — by position, so 8 bytes a data shard (512 bytes for 64), with
`NO_FILE` for one that never wrote. `prev` is the roots block of the write
before. A data shard idle for hours still has its root here, in the newest
block; nothing has to be followed to find it.

A roots block is dead once a newer one is durable: it is freed like a dead
header (0072 §8), and compaction need not carry it.

The header gains the **data shard it belongs to**, so a `prev` or a `last`
that lands on another data shard's header is caught as corrupt. A data
shard's own headers still chain by `prev` and `last`, its batches still end
with `end_of_batch`, and its walk is unchanged (bounded by the horizon,
RFC 0072 §6).

The header also names **the newest roots block written before it**
(`roots`). A storage write may be followed by windows of the next one
before that one's roots block — a round reserved and written meanwhile, or
a crash in the middle — so the newest data file may end with a header; it
leads to the roots block in one read, rather than a scan back across every
window written since.

256 data shards' roots are 2 KiB with their envelope: the block is always
one block.

### 6. Reconstruction

1. **The storage shard's root.** The newest valid roots block is the last
   block of the newest data file — scanned back, as the newest header is
   today (`reopen::newest_below`). The write it ends is whole if every
   header between `prev` and it holds its crc and every page and dictionary
   they place is intact (0072 §6's check, over one storage write instead of
   one batch). If not, its `prev` is the root, which is durable: only the
   newest write can be cut.
2. **The data shards' walks.** Each data shard walks from `roots[i]`, on its
   own worker, in parallel, reading the files read-only through its own
   handles.
3. **The allocator.** The storage thread rebuilds it from the files' layout
   and the runs every data shard reports in use (directories, dictionaries,
   the headers each walk read) plus the live roots block; the rest is holes
   and the tail past the root is cut, as today.

### 7. Compaction

The files belong to the storage shard; the directories naming what is in
them belong to the data shards. So moving a run is the data shard's:

1. The storage thread picks a victim file (≥ `victim_free_percent` free,
   0071) and sends each data shard with runs in it `Evacuate { file }`.
2. Each data shard carries its live runs in the file forward in its next
   rounds — read, rewritten through §3 — after restating every address, so
   no header of its in the file is live (0071's rule, per data shard).
3. When every data shard has answered and a storage write over the moves is
   durable, the storage thread cuts and deletes the file, and the next
   `Done` tells the data shards to close their read handles of it (on Linux
   the space returns only when the last descriptor closes).

### 8. The object store

One chain of files per storage shard. The data shards build their sections —
their remote pages and their header — as an upload builds them today; the
storage thread assembles one file and PUTs it:

```text
[ roots ][ header of data shard a ][ pages of a ][ header of b ][ pages of b ] …
```

The roots block comes first, so a ranged GET of the newest file's head finds
every data shard's root. The fields that describe the chain of files —
`start`, `end`, and the `end`, `start` and `tail` of the `ChainRecord` the
disk's headers carry — are the storage shard's, so they move from the data
shard's header into the roots block; the per-type `last` of the remote
directories stays in the data shard's header, as its directories do.
Object mode (no disk) gathers the same way. Reads stay per data shard: a
ranged GET each (the client does not reuse connections yet,
`docs/improvements.md`).

### 9. Durability

Per storage shard. A `_durable` write on data shard `p` answers once the
storage write carrying its batch is durable; the other data shards in that
write become durable with it. A write or an `fsync` that fails fails every
data shard in it. Nothing is atomic across owners, which is not new (0067
§7).

## Phases

1. **Format:** the `MANIFEST`'s S, the `storage/` layout, the header's data
   shard, the roots block. P = S = 1 everywhere; nothing else moves.
2. **The split in one thread:** the storage side out of `PageStore`, driven
   inline; the round in three steps (§3), the await reshaping of the
   settle.
3. **The storage thread:** the channel protocol, the gathered write, the
   `fsync` on its helper; the node with S < P.
4. **Reconstruction** across data shards (§6), in parallel.
5. **Compaction** by evacuation (§7).
6. **The object store** (§8): sections, one PUT per storage shard, recovery
   from the roots block, `compact_remote`, `convert`.

## Alternatives

- **The storage task rotating over the workers, by the CPU's counter
  (RDTSC / `CNTVCT_EL0`) and time slots, over a matrix of raw pointers.**
  Lost: time is not exclusion. A worker descheduled by the OS, or held by a
  long job on its cooperative executor, is still inside its slot when the
  next one begins; two threads then touch the same cell — a data race,
  undefined behaviour, on the path that writes the disk. The `unsafe` would
  be correct only as long as the timing held.
- **The same rotation with a baton** — the storage shard's state moved by
  channel to `(j + round) % T`. Sound, and no thread would hold two storage
  shards, but unnecessary: the coordination is a few messages and a bump
  allocation, and the write and the `fsync` need a thread that may block
  anyway — on a worker they would stop every data shard it serves (0067 §4).
- **A pointer from each header to the next one in the same write.** Finds
  only the data shards that wrote in the newest write; an idle one's root
  lies further back. The roots block names all of them.
- **A shift applied after the write.** The header is built with absolute
  addresses and its own `at` under a crc; correcting them afterwards means
  re-encoding it, or making every new address relative to the window,
  which changes the format and the check that `at` provides. Reserving
  first costs one in-RAM round trip.
- **The `fsync` inline on the storage thread.** Every reservation would
  wait for the device.
- **`io_uring` on the workers**, with no storage thread at all. The only way
  to write without a thread that blocks; Linux only, and `unsafe` in its
  bindings. Left for later, behind `wavedb-platform` (0067 §8).

## Progress

- **Phase 1 — landed 2026-10-17** (`manifest`, `shard_set`, `header`,
  `roots`, `disk_header`, `checkpoint`, `window`, `reopen`, `walk`): the
  `MANIFEST` records S (`StoreOptions::storage_shards`, `0` one per data
  shard), the data files live in `storage/<n>/`, the header names its data
  shard and the newest roots block before it, and the window that asks to
  end a batch ends with a roots block naming the store's root — the newest
  header ending a batch, or the one before when this window could not end
  it. A reopen roots in the newest roots block, found from the end of the
  newest file (itself, or through the header's `roots`), and falls back
  along `prev` when the batch it names is not whole; every roots block but
  the newest is freed with the batch that superseded it. Every database is
  still one storage shard per data shard: a count below it is refused at
  open until the storage thread (phase 3). Tested: a torn roots block gives
  way to the batch before (`tests/durability.rs`).
- **Phase 2, the round in three steps — landed 2026-10-17** (`checkpoint`,
  `wait`, `cycle`, `compact`, `defrag`, `evict`, `recover`;
  `wavedb-quick-node`'s `shard::{owned,background}`): a round sizes its
  window, **reserves** it — an `await`, at once from a store's own files —
  then builds it at its real addresses and writes it. Everything that
  publishes into the directories goes through the store's `Gate`, one
  round at a time, from the copy it plans from to the swap: a settle round,
  a compaction's move round from the reading of its runs, a restore from
  its copy of the directory — so a job restoring a bucket while a round
  waits for its window cannot be overwritten by it. The node awaits
  `settle_step` and `compact_pass`; `cycle_step`, `compact_step` and `drain`
  run the same steps parked on the calling thread (`wait::park_on`).
- **Phases 3–5, the storage thread, reconstruction and evacuation —
  landed 2026-10-17** (`link`, `reader`, `storage_shard`, `storage_open`,
  `storage_compact`, `roots_find`, `reopen`, `cycle`, `compact`, `defrag`,
  `shard_set`; `wavedb-quick-node`'s `Server::storage_shards`, 4 by
  default). As designed, with these choices made on the way:
  - **`std::sync::mpsc` for the inbox**, not `async-channel`: the storage
    thread blocks in `recv_timeout`, the data shards only send (unbounded,
    never waiting), and the answers come back on `futures` one-shots their
    tasks await — no new dependency, the barrier helper's stance.
  - **A round publishes when it hands its window over**, not at `Done`: a
    data shard's next round plans from the directories this one publishes,
    so waiting for the write — up to a tick — would serialise a batch's
    rounds on the ticks. Its window's bytes are held readable instead
    (`reader`), until the batch carrying them is durable; the frees still
    wait for the seal, the durable writes for the `fsync`.
  - **A seal** follows the window that ends a batch, carrying the batch's
    frees and waiters; a seal someone waits on (a durable write, a `drain`)
    makes the storage thread write at once, as do 8 MiB gathered.
  - **One write per run of windows end to end** in a file, by address — a
    window still being built leaves a gap — then the roots block.
  - **Reclaim after every durable storage write**: free tails cut, files
    left all holes deleted; the data shards hear of the deletions with
    their next reservation and close their handles.
  - **Evacuation by asking**: the first data shard to compact asks which
    files to empty and opens the evacuation (the active file rolled first
    if it is one); the others asking get the same files, and it ends when
    every data shard said it was done. Each reports its share of the
    storage shard's space, so the node's triggers keep their meaning.
  - **The storage shards reconstruct in parallel**, each one's data shards
    one after the other (their walks are bounded by the horizon).
  - An object-backed database keeps one storage shard per data shard until
    phase 6: `Server::object_store` and `object_mode` ask for that.
  - Tested (`tests/storage_shards.rs`): four data shards through two
    storage shards reopen; an idle data shard's root survives thirty
    batches of its neighbour; a durable write survives a crash; a torn
    newest storage write gives way to the roots block before for every data
    shard; an evacuation shrinks the files and every record reads back,
    then and after a reopen.
- **Phase 6, step 1: the object store's format — landed 2026-10-17**
  (`roots`, `header`, `upload_file`, `recover_headers`, `recover`,
  `remote_compact`, `remote`): the chain of files lives under
  `storage/<n>/`, every file opens with a roots block — the newest header
  ending a batch, the oldest live file and the remote end, which leave the
  header — and its section after it; a header is named by the block it
  starts at, read by a ranged GET of its file. A recovery roots in the
  newest file whose roots block names a whole batch, trying each number
  down.
- **Phase 6, step 2: the object store behind shared storage shards —
  landed 2026-10-17** (`remote_shard`, `remote_put`,
  `remote_shard_compact`, `remote_link`, `upload`, `upload_file`,
  `storage_open`, `remote_compact`, `convert`, `shard_set`;
  `wavedb-quick-node`'s object store and object mode keep the node's four
  storage shards). As designed, with these choices made on the way:
  - **A remote thread per storage shard**, beside the storage thread (the
    only one in object mode): a PUT is a network round trip, and it must
    not hold up a reservation of the disk.
  - **An upload reserves its sections** — laid out from block 0 as before
    and moved to where the thread places them (`upload_file::rebase`) —
    each in the open file while it fits inside its 8 MiB of address space,
    or at the head of the next; a section larger than a file has one of
    its own. It hands them over and awaits every file holding them.
  - **The files go down oldest first, one at a time**, each once every
    section reserved in it is written: at the tick (1 s), at once when
    someone waits (`upload_all`), or when full. Its roots block names a
    data shard's cut only once every file of that upload is in, so a
    roots block never names a batch a failed PUT left without its part.
  - **Compaction by reports**: each data shard compacting reports its
    horizon, the files it still needs below it and its live blocks per
    file; once every data shard has reported, the thread deletes every
    file below the lowest horizon no report names, and answers with the
    files mostly free across all of them, which the data shards' uploads
    carry their pages out of. An older report stays good — below its
    horizon a data shard only ever stops naming files.
  - **Opening** starts the remote thread where the data shards last saw
    the chain — their disk records, or what a recovery from the object
    store found for each — past every file the object store holds.
  - Tested (`tests/shared_object_store.rs`): four data shards through two
    storage shards upload together in one file; the object store alone
    brings every data shard back; object mode shares the files; a
    compaction deletes what no data shard needs, and the database comes
    back from what is left.

## Open questions

- The default S of a node: 4 for now (P = 64), a measurement away from
  settled.
- `posix_fadvise(RANDOM)` on the data shards' read handles: reads are random
  by construction (copy-on-write placement, seahash buckets), so read-ahead
  only spends IOPS.
