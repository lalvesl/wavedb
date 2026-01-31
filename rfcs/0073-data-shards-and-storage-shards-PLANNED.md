# RFC 0073 — Data shards and storage shards

- **Status:** Planned — opened 2026-10-17
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
`TODO.md` item 1).

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

## Open questions

- The default S of a node (the example throughout: P = 64, S = 4).
- Whether the roots and the chain fields fit one block for every P / S, or
  the block grows to a fixed few.
- `posix_fadvise(RANDOM)` on the data shards' read handles: reads are random
  by construction (copy-on-write placement, seahash buckets), so read-ahead
  only spends IOPS.
