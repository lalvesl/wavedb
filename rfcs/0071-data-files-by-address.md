# RFC 0071 — Data files named by address

- **Status:** Implemented (accepted and landed 2026-10-15)
- **Amended by:** [RFC 0072](0072-the-settle-is-the-durability-point.md) (2026-10-16) — §5–§7's protected generations and
  two-checkpoint sealing give way to frees deferred to the batch's barrier, and
  a compaction restates every address before it moves anything.
- **Amended by:** [RFC 0073](0073-data-shards-and-storage-shards-WIP.md) (2026-10-17) — the data files belong
  to a storage shard (`storage/<n>/`), which its data shards write through;
  compaction is an evacuation the storage shard directs.
- **Supersedes:** [RFC 0069](0069-compaction-into-the-holes-DEPRECATED.md)'s placement
  and reclaim (its triggers, rounds and sealing are kept)
- **Crates:** `wavedb-storage` (the files, the compaction),
  `wavedb-quick-node` (the policy, the descriptor limit), `wavedb` (the
  client cache opens a one-shard database)
- **Code:** `crates/wavedb-storage/src/{manifest,block_file,data_file,alloc,file_space,checkpoint,commit,compact,defrag,retire,shard_set}.rs`,
  `crates/wavedb-platform/src/limits.rs`, `crates/wavedb-quick-node/src/{lib,storage_config}.rs`;
  `superblock.rs` is gone

## Summary

A shard's pages stop living in one `data.bin` that only grows. They live in
**data files named by the block address they start at** —
`shards/00/0000000000280000` holds the blocks from `0x280000` on — found by
listing the shard's directory into a `BTreeMap<start, file>`. Every write
appends to the newest file, which closes once it passes a configurable size
(10 GiB by default) and the next one is named after where it ended. Compaction
copies the live pages of the emptiest files forward into the newest one,
highest first, and truncates each old file from its tail as checkpoints
release what it held, deleting it once empty. It is the object store's model
(RFC 0070: files named by address, live pages carried forward, a file deleted
once empty) on a disk, which can also truncate. Block descriptors do not
change. The per-shard superblock goes: the database's fixed facts — magic,
format version, shard count, seed, mode — live in the one `MANIFEST` at the
database root, as they already do in the object store.

## Motivation

- **One file per shard is the size of its history.** Best-fit reuses holes,
  but nothing gives space back until compaction (RFC 0069) has moved every
  run above the last hole down; one live run near the end pins the file's
  length.
- **Hole reuse scatters.** A settle round lands wherever a hole fits it, and
  compaction must move runs *down*, so a hole is only useful if something
  above it fits it. The free space is spread across the whole file, and so is
  the work of giving it back.
- **Two models for one problem.** The object store already has the shape
  this wants: files that are only appended to, named by address, emptied by
  carrying their live pages forward. Phase 7 of RFC 0070 (compaction of the
  object store's files) and the disk's compaction become the same rule:
  which file to empty, and in which order.
- **The superblock is per file, the facts are per database.** The seed, the
  mode and the format version are the database's, and the object store
  already keeps its fixed facts in one `MANIFEST`. With several data files, a
  superblock would need a home among them, and that home would move
  whenever the file holding it was compacted away.

## Design

### 1. Layout

```text
db/
  MANIFEST                  magic · version · shards · seed · mode
  shards/00/
    0000000000000000        data file: blocks 0x0 … (its length)
    0000000000280000        data file: blocks 0x280000 …
    journal_<ts>.log        the journal (RFC 0047), unchanged
  shards/01/ …
```

A data file is blocks of pages, dictionaries and addressing chunks, and
nothing else: no header, no superblock. Its name is its first block, as 16
hex digits; a directory entry that is not exactly that is not a data file.

### 2. The `MANIFEST`

One record at the database root, checked wire bytes, written once at
creation (create-only) and read on every open: magic, `FORMAT_VERSION`, the
shard count, the hash seed and the mode (disk, or disk + object store). The
**seed becomes one per database** (it was one per shard), which changes
nothing about routing. The object store's `MANIFEST` (RFC 0070 §4) holds the
same record, so the seed leaves the object store's file headers: each fact
has one home.

`PageStore::open(dir, …)` opens a **database of one shard** — `dir/MANIFEST`
plus `dir/shards/00` — so the client cache and the tests keep their call,
and every database on disk has the same shape. `open_shards` reads the
`MANIFEST` once and opens each shard with it.

### 3. Addressing: a listing, then a lookup

Descriptors keep `start u43 · count u20`; `start` is a block address in the
shard's own address space, which begins at block 0 (no block is reserved any
more) and is never reused once a recovery root has named it: only what was
written after the last `Commit` is cut on a reopen and written over. On open the shard's
directory is listed into a `BTreeMap<u64, DataFile>` keyed by first block.
A run starting at `s` lives in the file `range(..=s).next_back()`, at byte
`(s − first) · 4096`; a run that does not lie inside that file's length is
`Corrupt`. The store is used from one thread, so the map sits in a
`RefCell` beside the rest of the store's state: no lock, no `Arc`.

`2^43` blocks are 32 PiB of address space per shard, counting everything
ever written there, compaction's copies included — about ten years of a
continuous 100 MB/s into one shard.

### 4. Writing: append only, with a size ceiling

Every window — a settle round, a checkpoint's, a compaction round, an
addressing snapshot, a page restored from the object store — is one
positioned write at the **end of the newest file** (the *active* one). No
hole is ever reused, so the allocator loses its best-fit and below-a-limit
placements; what it keeps is the bookkeeping per file: free extents,
checkpoint protection and deferred frees (RFC 0047), and the free tail.

- **Rollover.** When the active file's length has reached the ceiling, the
  next window opens a new file named after the active file's end. A window
  is never split: it may run past the ceiling (a single page may be up to
  4 GiB), since the ceiling bounds where a window *starts*.
- **The ceiling** is `StoreOptions::file_bytes` (`Server::data_file_bytes`
  on a node), 10 GiB by default. It never reaches stored bytes — the listing
  says where every file is — so it can change between runs, and files of
  different sizes coexist.
- **A new file** is created, locked (§6), and the directory synced before
  the window that first writes it: once per file.

A settle round is still one write; a checkpoint is still one sync — per file
written since the last one, which is the active file, or two across a
rollover.

### 5. Truncating and deleting

A file other than the active one only ever shrinks. Whenever a checkpoint
opens — which is when the previous generation's held-back runs are released
(RFC 0047) — each such file's free tail is cut off, and a file with nothing
used and nothing held back is deleted, its page-cache entries dropped. Its
name is never used again, since addresses only grow.

Neither takes a barrier. A crash that loses a truncation leaves a file
longer than its last used run, which the reopen reads as free tail; one
that loses a deletion leaves a file no run names, which the reopen deletes.
The reopen rebuilds each file's allocator from the runs the recovered
directories, dictionaries and addressing log use, as `from_layout` does for
`data.bin` today. That includes the active file: what lies past its last used
run is what settle rounds wrote after the last `Commit`, which the journal
replay writes again, so it is cut off and writing resumes right after that
run.

### 6. One open store per directory

`File::try_lock` (an advisory `flock`) on every data file **and every
journal** the store opens read-write: a second store over the same directory
meets the first file locked and is `EngineBusy`. A file created by a rollover,
and a journal created by a rotation, is locked when it is created. The
journals are what make it hold for a shard with no data file yet — one whose
writes are all still in its journal. There is no lock file and no directory
lock, as the object store has no lease. (Opening a file read-write
does not lock it by itself on Linux or macOS; the `try_lock` is what does.)

### 7. Compaction: forward, emptiest first

RFC 0069's triggers decide **when** (a daily UTC time, a disk ceiling, a
share of the shard that is free), and its rounds, addressing snapshot and
sealing are unchanged. What changes is **which runs move, and where to**:

- **Victims.** Files whose free share — free plus held back — is at least
  `victim_free_percent` (50 by default), the emptiest first, the oldest
  (lowest address) on a tie. A file mostly alive is not rewritten for little.
- **The active file can be a victim.** Compaction then rolls it over first.
  Without this, a shard smaller than the ceiling — one file — would never
  give anything back.
- **Moves.** A victim's live runs, **highest first**, are copied verbatim
  into the active file, at most 128 images per round with the round's
  addressing delta in the same window (RFC 0069). Highest first is what
  frees a victim's tail first, so its truncation follows the moves.
- **Space comes back during a compaction**, not only at its end: every
  checkpoint the node takes meanwhile releases and truncates what had moved
  before it (§5). The compaction ends with 0069's sealing — two checkpoints
  — so the last victim goes without waiting for the next one. A victim
  shrinks while the active file grows, so a compaction needs no free disk
  beyond what moved since the last checkpoint.
- **What 0069 drops:** the second snapshot rewrite "into the room the release
  opened" — a snapshot is appended to the active file like everything else,
  and is never left high.

### 8. Object mode

With no disk (RFC 0070 §9) the blocks are in RAM, and they take the same
shape: the map holds RAM files (`Backing::Memory`), the active one closes at
the same ceiling, and compaction carries live pages forward the same way.
Nothing is held back, since no checkpoint names a run, so a move frees at
once and an emptied RAM file is dropped, giving its memory back. Pages that
leave by eviction (RFC 0070 §7) free their blocks like any superseded page,
on disk and in RAM.

### 9. Open files

Each shard has its active file open at least: 64 descriptors for a
64-shard database, plus one per closed file still live.

| Database | 10 GiB files | 1 GiB files |
|----------|--------------|-------------|
| 1 TB     | ~160         | ~1 100      |
| 10 TB    | ~1 100       | ~10 000     |

The usual soft limit is 1 024, so a node raises its soft `RLIMIT_NOFILE` to
the hard limit when it binds.

### 10. What the object store shares

The model, not the code. The object store's files are immutable, cover a
fixed 8 MiB of address space each (`start / 2048` names the file) and are
deleted whole once below the recovery horizon; the disk's are any length,
found by listing, and truncated from the tail once two checkpoints release
them. RFC 0070 phase 7 picks its victims by the same rule as §7.

## Testing

As built (`cargo test -p wavedb-storage`):

- **`alloc`** — runs append to the active file; a hole is never reused; a
  full file rolls over where it ended and a run is never split; a rolled
  file takes no more runs; frees coalesce within a file only; `reclaim`
  cuts free tails and deletes empty files, sparing the active file unless
  asked; protection defers frees across two generations; `from_layout`
  frees the gaps per file.
- **`block_file`** — a run is read from the file its address names, and a
  reopen lists the same files; only 16 lowercase hex digits name a data
  file; a cut shortens a file and a cut to its start deletes it; a second
  open is `EngineBusy`, on a file it found and on one it created; RAM files
  work alike.
- **`manifest`** — roundtrip; a wrong magic is `BadMagic`, a torn body
  `Corrupt`, another version `BadVersion`; a reopen keeps the created facts.
- **`shard_set`** — a single store is a one-shard database, and a database of
  more is not opened as one; a shard held by another process (its journal
  locked) fails the open and releases the others.
- **`tests/compaction.rs`** — a pocked store across several 64-block files
  ends with the holed files cut or deleted, the bytes on disk matching the
  allocator, every record read from pages, before and after a reopen; a store
  of one file compacts (the active rollover); stepped with writes between
  steps it keeps every write; a store dropped mid-move, after a `reclaim`
  meanwhile, reopens whole — **mutation-checked**: with protection disabled
  the reopen fails; a crash's leftovers (a file longer than its last run, a
  file no run names) are cut on reopen; a window larger than the ceiling
  lands whole, the next file is named past it, and a reopen with another
  ceiling reads both.
- **`tests/object_store.rs`** — the mode is fixed at creation, checked
  against the local `MANIFEST` before the two copies are compared.
- **`limits`** — the soft limit on open files rises to the hard one.

What a lost journal leaves is only detectable with a data file present: a
shard whose writes were all still in its journal looks new without it.

## Phasing

| Phase | What |
|-------|------|
| 1 | The `MANIFEST` replaces the superblock: root record, one-shard `PageStore::open`, the object store's `MANIFEST` carries the seed, which leaves the file headers. |
| 2 | Data files by address: the listing, the map, append-only placement, rollover, per-file sync, the per-file lock, the reopen's cleanup. |
| 3 | Compaction over files: victims, the active rollover, release and truncation at every checkpoint opening, deletion; the node's `victim_free_percent` and ceiling. |
| 4 | The node's descriptor limit; docs; RFC 0069 renamed `-DEPRECATED`. RFC 0070 phase 7 follows on top. |

All four landed together on 2026-10-15.

## Alternatives

- **A fixed span per file** (file = `start / 2 621 440`, the object store's
  arithmetic). No lookup, but the span is then part of every descriptor's
  meaning and could never change as the database grows. The listing costs
  one `BTreeMap` lookup per read.
- **One `data.bin` with hole reuse** (RFC 0069) — what this replaces.
- **A header in every data file** carrying the superblock's facts. The facts
  would be copied into every file and a reader would need to know which one
  to trust; the `MANIFEST` is one place, as in the object store.
- **A lock file, or a lock on the directory.** A second thing that exists
  only to be locked; the data files are already open.
- **Keep best-fit into the old files' holes.** It would scatter writes back
  into the files compaction is trying to empty, and a file with a write in
  its tail cannot be truncated.
- **A separate destination file for compaction's copies** (cold pages apart
  from fresh writes). Fewer moves of cold data over time; not needed to
  start, and it can be added without touching the format.
