# RFC 0070 — Object storage as a page backend

- **Status:** Implemented — opened 2026-10-10, landed 2026-10-16. Phases 1–9 done (descriptor, test store, S3 client; disk + object: the flag, the remote directories, the upload, reopen from disk; eviction from disk and the read that falls back to the object store; recovery from the object store alone; object mode, with no disk and no journal; compaction by file; converting from or to disk mode; HTTPS to a public endpoint).
- **Supersedes:** [RFC 0059](0059-object-storage-capacity-tier-PLANNED.md) §2–§5
  (the remote descriptor bit and the object per bucket). 0059 §1, the archive
  lane, stays and is independent of this.
- **Crates:** `wavedb-storage` (the backend), `wavedb-platform` (the S3 client
  half), `wavedb-quick-node` (configuration)
- **Builds on:** [RFC 0046](0046-directory-deltas-in-the-window.md) /
  [RFC 0048](0048-chained-addressing-log-DEPRECATED.md) (descriptors travel as chained
  deltas), [RFC 0047](0047-generational-journal-retirement-DEPRECATED.md) (protection
  generations), [RFC 0061](0061-relaxed-durability-window-DEPRECATED.md) (group commit),
  [RFC 0067](0067-thread-per-core-shards-WIP.md) (a shard is a database),
  [RFC 0068](0068-record-cache-as-a-log-WIP.md) (the record log),
  [RFC 0069](0069-compaction-into-the-holes-DEPRECATED.md) (compaction and its triggers)

## Summary

A database stores its pages in one of three ways. Disk and disk + object
are fixed when the database is created and recorded in its `MANIFEST`
beside the hash seed (RFC 0071 §2; opening it the other way is
`StorageError::ModeMismatch`); disk + object and object hold the same
files, and a database closed in one opens in the other (§9):

| Mode | Pages live in | Disk is | Journal |
|------|---------------|---------|---------|
| **disk** | the data files (RFC 0071) | the store | local, fsync |
| **disk + object** | the object store | a **cache** of it, plus the pages not uploaded yet | local, fsync |
| **object** | the object store | absent — RAM caches | none: a write is durable once uploaded (§9) |

Each type has two `Vec<u64>` directories, each a linear hash of its own: the
disk's (32 KiB pages) and the object store's (128 KiB pages, for a denser
vector). The disk pages the settle rounds wrote and the object store has not
received yet are reassembled, record by record, into the object store's
pages; those fill files in which every page *starts* within the first
8 MiB, whose first 32 KiB hold the
addressing changes for the pages in it, chained file to file as the disk's
addressing log is. A remote descriptor turns into a
file name and a byte range by arithmetic. Everything that navigates stays in
RAM; the object store is asked for record bytes, and for nothing else on a
read.

## Motivation

Copy-on-write already gives the engine the discipline an object store
demands: a page is never rewritten in place, a settle round writes one
contiguous window, and the addressing is a chain of deltas. An object store
has immutable objects, whole-object PUT and range GET. The two only need to
be joined.

[RFC 0059](0059-object-storage-capacity-tier-PLANNED.md) joined them one
bucket at a time — an object per demoted bucket and a stolen descriptor bit —
which made the object count follow the directory, and only for archives. This
design ships settle output in files of about 8 MiB instead, and makes the object store
the place pages *live*, so a deployment with little or no disk is served too.

## Design

### 1. The descriptor

`start (u43) · count (u20) · flag (u1)` — 4 KiB blocks, as on disk. The
4-bit occupation gauge was never read and is gone (2026-10-10): its bits went
to the address (32 PiB) and to the flag.

- In a **disk** directory, `flag` = **not uploaded yet**. A settle round
  writes a page with it set; the upload that carries the page clears it, and
  the clearing rides the next round's addressing delta like any descriptor
  change. It therefore survives a crash for free: a page whose flag is still
  set on reopen is uploaded again (a duplicate in the object store that is
  born dead — harmless).
- In a **remote** directory, `flag` is zero.

### 2. Two directories per type

```rust
TypeSlot {
    disk:   Directory,          // 32 KiB pages in the data files
    remote: Option<Directory>,  // 128 KiB pages in the object store
}
```

**Two linear hashes, independent.** Each splits by its own page size — the
disk's at 32 KiB, the object store's at **128 KiB** (decided 2026-10-10),
four times larger so its vector is four times denser: 1 TB is 8 M remote
buckets, 64 MB. Neither needs any fixed relation to the other: what moves
between them is **records**, each routed by `hash(id)` through the directory
it goes into. Disk bucket `d` (level `ld`) and remote bucket `s` (level `ls`)
can share records exactly when `d ≡ s (mod 2^min(ld, ls))` — arithmetic, no
lookup.

Lookup: RAM (record log, page cache) → the `disk` page if the disk holds one
→ the remote page that covers the id, filtered to it. The disk always holds
the newer copy when it holds one: a bucket is evicted from disk only when its
flag is clear, i.e. the object store has everything in it.

### 3. Addressing: arithmetic, not lookup

A file *starts* pages within 8 MiB of address space (2 048 blocks):

```
file   = start >> 11
offset = (start & 2047) · 4096
range  = bytes=offset-(offset + count · 4096 − 1)
```

- **The 8 MiB bound is on where a page starts, not on where it ends**
  (decided 2026-10-10). The upload appends pages while its cursor is below
  the file's 8 MiB mark and closes the file at the first page that ends past
  it — nothing checks whether a page *fits*, and nothing is ever split. The
  last page may run as far past the mark as it is long: a page of 4 GiB (the
  descriptor's `count` limit) makes a file of about 4 GiB, still one PUT
  (S3's single-PUT limit is 5 GiB). Since every page starts inside its
  file's window, `file = start >> 11` holds for every page and a page's
  range is always inside one object.
- The next file starts at the first file boundary at or after the cursor:
  the numbers a large page ran over are simply never used. Address space is
  not scarce — 2^43 blocks is 2^32 files.
- A file holds what was written to it and no more: its size is stored
  nowhere. It is written block-aligned — its last page padded to its block
  boundary — so a page's range never runs past the end of its file.

### 4. Files, names and the order LIST gives

Per shard, under `<prefix>/shards/<shard:02x>/`:

```
1seg/<!file:016x>        the files — named by the bitwise complement of the
                         number, so LIST returns the newest first
```

and `<prefix>/MANIFEST`, the shard count, beside the disk's copy (in
object mode, instead of it).

The numeric prefixes fix the order LIST returns. `LIST prefix=1seg/
max-keys=1` names the newest file in one request.

**No `start` object** (decided 2026-10-10). The oldest live file
(`first_file`) is a field of every file's header, and the newest file's
header is the one recovery reads first anyway — so knowing it costs no
request, where a `start` object cost a PUT (a tier-1 request) each time it
advanced and a GET on recovery. The field takes 8 bytes of a header that
is fixed at 32 KiB, so it costs no space either.
It is written as the oldest file not yet deleted *when the header is
written*, so it can only lag behind the truth, never run ahead of it: a
recovery that finds it low re-issues DELETEs for files already gone, and a
DELETE of an absent key succeeds. (Directory buckets of S3
Express One Zone do not list in lexicographic order; they are out of scope.)

### 5. Upload: reassembling the flagged pages

Settle rounds run as today and flag what they write. When the flagged disk
pages reach about 8 MiB, an upload reassembles them into remote pages, which
fill one or more files:

```
[ 32 KiB header ][ page ][ page ] … the last page starts before 8 MiB
```

1. **The input** is every disk page whose flag is set, read by its current
   descriptor. Each is complete (§7): it is the whole truth about its disk
   bucket, removals included. A page superseded before its upload is simply
   its newer version; pages that compaction (0069) moved did not change and
   carry no flag.
2. **The remote pages that change** are those that can share records with a
   flagged disk bucket (§2's congruence).
3. **Each new remote page** is the old one minus every record whose disk
   bucket is flagged, plus the flagged pages' records that route to it. A
   record removed from a disk page therefore leaves the remote page too.
4. **Encode** each page with the type's dictionary and fill files, block-
   aligned.

It runs concurrently without sharing memory, as the barrier helper does:
the old remote pages not in cache are fetched all at once (a `join` on the
worker — network wait, no CPU), and the compression can be handed to a helper
thread as owned messages — the records and a copy of the dictionary in, the
finished blobs out — so files assemble while the worker keeps serving. It
starts on the worker and moves to the helper when measurement says so.

- **The header** is the file's addressing delta (every field in §6): per
  type, the bucket count and `(bucket, remote descriptor)` for every page in
  the file, the
  descriptor of the type's dictionary when the file carries it (the
  dictionary is an image in the file like a page, since at up to 64 KiB it
  would not fit the header), and `first_file` (§4). **Always 32 KiB** (decided
  2026-10-10): pages start in at most 2 040 blocks after it, ~12 bytes
  each, ~24 KiB. Each type in the file adds ~30 bytes of its own, so a file
  touching very many types could outgrow it; the upload therefore also
  closes a file when the next page's entry would not fit its header. A
  header can never overflow, whatever the schema. The file is assembled in
  RAM before its PUT, so the header is written once, exact.
- **The headers form a chain** (§6): each names the file before it
  (`prev`). A number a large page ran over (§3) has no object; a GET of it
  answers `404`. (Until RFC 0072 the link carried the hash of that header
  too; a number is never written twice and every PUT is create-only, so a
  header's crc and its own number are the whole check.)
- After the PUTs, the upload publishes in RAM: the new remote directories,
  and the flag cleared on each disk page still at the descriptor it was
  taken at (one a settle rewrote meanwhile is newer and keeps its flag).
  What changed waits, unrecorded, for **the next window on disk**, whatever
  else that window carries (decided 2026-10-10: writes are constant, and a
  crash in between costs one upload again — not worth a write of its own).
  A crash in between uploads those pages again.
- A crash after a PUT and before anything records it leaves files at or
  above what the disk knows. **A file number is never written twice**
  (decided 2026-10-10, phase 5): a store reopening over its disk lists the
  newest file (one `LIST max-keys=1`) and moves its end past it, so the redo
  goes to new numbers and the leftovers stay — a branch no chain and no
  `last` names. Rewriting them in place had a hole: a redo that replaced
  only the first file of an earlier lost upload left a later lost upload
  whole, linked to a file that still existed, while the pages it named were
  gone.

### 6. Recovery

- **disk + object**: the disk's own addressing log restores both directories
  (the remote changes ride it), and the flags say what is still to go up. The
  object store is not read at all.
- **object**, and a disk lost in the other mode — **from the headers alone,
  no snapshot object** (decided 2026-10-10: more GETs, which are cheap,
  instead of more PUTs and objects). Built in phase 5 (`recover.rs`):
  - **A header names the types its upload changed** (RFC 0072 §3, which
    made this the disk's header too): each with its bucket count, its
    dictionary's remote descriptor, and `last` — the newest earlier file
    with entries for it. The header also carries `start` (`first_file`),
    the remote end, its own number (`at`), `prev`, and `end_of_batch` on
    the last file of a cut.
  - **Spare room restates old entries**: after its own pages, a header
    spends what is left (typically ~31 KiB, ~2 600 entries) on the type
    restated longest ago (one never restated goes first, an empty one
    before any, so a type never written is named in the first header),
    from a **rotating cursor** —
    not at random, which would need `(B/S)·ln B` files to cover `B` buckets
    instead of `B/S` (1 TB: ~49 000 files against ~3 100). Every type keeps
    appearing near the tail, so a recovery reads a bounded number of
    headers. (Phase 5 ordered by smallest `last`, but a type written in
    every upload is mentioned in every header and could lose every tie, its
    untouched buckets never restated and the horizon stuck below them;
    phase 7 ordered by rotation; RFC 0072 orders by the header that last
    restated the type, `Rotation::shifted`, RAM only.)
  - **The newest whole cut**: `LIST 1seg/ max-keys=1` names the highest
    file. It is taken only if it ends a batch — the last file of an upload
    that is a cut (§9) — and the batch is whole: every header back to the
    one ending the batch before present. Otherwise the next number down is
    tried. So the files a crashed upload left above the
    recorded end, a part of a backlog, a copy under another key, or bytes
    that are no header are never taken for the newest.
  - **The type walk** (RFC 0072 §6): every type the newest header names
    starts there and follows its `last`; while a registered type is not
    named yet, the headers are read down by `prev` until one names it.
    Always the highest header first, so the first value seen for a bucket
    is its newest. A type is done when every bucket is known or its chain
    ends (a bucket no header names was never written: empty). Headers come
    32 per round.
  - **Adoption**: an object-backed store opening with no data file asks the
    object store first, and if it holds files the database is recovered —
    never created over. The store opens with the `MANIFEST`'s seed (the object store keeps a
    copy, RFC 0071 §2), every disk
    bucket evicted (reads go to the object store, the disk refills as a
    cache), the dictionaries written to disk, then one snapshot and one
    checkpoint make the disk a root of its own.
  - **The recovery horizon** is the lowest file a recovery asks a header
    of — the batch's headers back to the one ending the batch before, and
    the walk. Compaction by file (§8) deletes only below it.
  - In disk + object mode, losing the disk loses what was not uploaded yet
    (the flagged pages and the journal); in object mode a crash loses what
    was not uploaded either, which `upload_interval` bounds (§9).

### 7. Eviction from disk

In disk + object mode the disk is a cache. A bucket whose flag is clear may
have its disk page freed when the disk passes its budget; a read then goes to
the remote page covering the id. A page on disk is always complete,
removals included, and no tombstone has to outlive anything.

Decided 2026-10-10, built in phase 4:

- **The budget is the compaction ceiling** (RFC 0069 `above_bytes`, per
  shard). Past it, the coldest uploaded pages leave the disk down to nine
  tenths of the share, and the ceiling trigger then compacts the room they
  left out of the file. Eviction frees runs; compaction is what shrinks
  the data files.
- **Cold is least recently used**: a `u32` stamp per disk bucket, in RAM
  (`evict::Heat`), moved by every page read and every page a settle round
  writes or a restore brings back. Its clock halves every stamp when it
  runs out. A reopen starts everything equally cold.
- **Evicted is its own descriptor**, `start 1 · count 0 · flag 0`
  (`BlockDescriptor::EVICTED`): no run, like `EMPTY`, but distinct from
  it, so a bucket that is merely empty still answers "absent" without a
  request.
- **A write brings its bucket back before it commits** (`Store::apply`
  restores, re-checks after its awaits, then commits synchronously), and so
  does a replayed one at open. A settle round therefore never meets an
  evicted bucket — it refuses one as corruption — and the settle stays
  synchronous. A restored page is the object store's copy filtered to the
  bucket, which is exactly what the object store holds, so its flag is clear.
- **A read of an evicted bucket** fetches the page covering the id and marks
  the bucket *wanted*; the node's upkeep brings wanted buckets back, 16 per
  tick. A split whose turn's bucket is evicted waits for it the same way.
- **The guard of a first save** (`Expect(id, None)`) on an evicted bucket
  costs a GET — accepted: the answer is a disk cache kept as large as it can
  be and a hot/cold tracker that works, not a structure to prove absence.
- **Recording**: an eviction frees a run, so unlike an upload's results it
  must never be missing from the addressing a `Commit` frame names. Every
  checkpoint writes what is unrecorded before its frame
  (`record_unrecorded`); between two checkpoints a crash reopens from a root
  whose runs were held back, so it simply undoes the eviction.

### 8. Compaction deletes files

The object store cannot cut an object, so space comes back by deleting
whole files. Built in phase 7 (`remote_compact.rs`), after RFC 0071 gave the
disk the same model: files named by address, live pages carried forward,
a file deleted once nothing needs it.

- **Deletable**: a file below the **horizon** (§6) that **no live page is
  in** — not in the store's remote directories, not in what a recovery from
  the object store would rebuild today (the two differ until a cut carries
  the store's state), and, with a disk, not in what the disk's recovery
  roots name: the newest checkpoint's remote directories and the one's
  before while its frame is unproven (`ObjectState::rooted`, recorded where
  the allocator protects its runs, RFC 0047).
- **Found by running a recovery**, not by keeping state: `survey` lists the
  newest file, finds the newest whole cut, walks the chains and reports the
  lowest header it asked for and every file the rebuilt directories name.
  GETs and one LIST per pass, no new object, no new PUT. A file's live pages
  only leave it and the horizon only rises, so a file found deletable stays
  deletable.
- **The upload turn is held** throughout: no upload is in flight while the
  compaction looks, so no cut taken before it can land after it naming a
  file it deleted — and the remote directories do not change meanwhile.
- **Mostly free files are carried forward**: below the horizon, a file
  whose live pages are less than `100 − victim_free_percent` % of its
  nominal 8 MiB (with 50, under 4 MiB — which takes in the small files an
  upload interval leaves) has its pages restaged into uploads the
  compaction makes itself (`SlotUpload::moved`, up to `MOVE_BLOCKS`, 8 MiB,
  per upload), its dictionary with them; once those stand as cuts — and,
  with a disk, after two checkpoints the compaction takes — the files are
  deleted in the same pass.
- **DELETEs are blind**: every number below the horizon that nothing names,
  up to 32 at a time. A DELETE costs nothing on S3 and a LIST costs what a
  PUT does, so listing first would cost more than deleting a number with no
  file; numbers already deleted are remembered in RAM
  (`ObjectState::deleted`) and deleted once more after a reopen. The
  chain's `start` (the oldest live file) advances and rides the next
  header.
- **A read across a deletion**: a read whose page was carried forward and
  whose file was deleted while its GET was out fails once, finds the
  descriptor moved, and reads again at the new one.
- **The node's triggers** are 0069's, weighed against `remote_space()` —
  every file not deleted at its nominal 8 MiB, free whatever is not a live
  page — once a minute (`REMOTE_CHECK_EVERY`: weighing walks every remote
  directory). The size triggers need `Maintenance::object_compact_free_bytes`
  free (512 MiB, 64 files) and as much again after each compaction: at a
  10 s upload interval, a compaction per ~11 minutes of steady writes at
  most, each a LIST, a few header GETs and a PUT or two per 8 MiB carried.

### 9. Object mode

**Decided 2026-10-10: no journal in the object store, and no lease.** A
journal object per group-commit window would cost a PUT per window per
shard, and a lease a PUT per renewal; neither buys what object mode is for.
Built in phase 6 (`object_mode.rs`, `backing.rs`):

- **No disk at all.** `PageStore::open_object` (`open_object_shards`, and
  `Server::object_mode` on a node) lays out what would be the data files in RAM
  (`Backing::Memory`, 1 MiB chunks: growing never copies, a truncation
  gives whole chunks back). The settle rounds, the flag, the upload, the
  eviction and the compaction are the disk + object ones, unchanged. What a
  disk keeps only so that it can reopen from itself — the journal, the
  addressing log, the checkpoints, the protection generations — is not
  kept: nothing reopens from RAM.
- **A write is durable once an upload carried it.** A store uploads once it
  holds back `upload_after_bytes` (8 MiB, about a file), or once its oldest
  write not uploaded has waited `upload_interval` (default 10 s; the
  user's figure). That interval is the durability window: a crash loses at
  most it, and each shard with a write waiting costs one PUT per interval
  (all 64 shards dirty all the time: ~553 000 PUTs a day, ~US$ 83 a month
  at US$ 0.005 per 1 000; 60 s: ~US$ 14). A close uploads everything first
  (`upload_all`), so a planned restart loses nothing.
- **Every open is a recovery** (§6) from the files: the store opens with
  every bucket evicted, and RAM fills as reads and writes want pages.
- **Only a cut is recovered.** An upload first settles every committed
  batch, then takes the flagged pages with no await between; if the take
  leaves nothing flagged behind, the upload is a **cut**
  (`FileHeader::cut`), and with the uploads before it holds every batch up
  to that point and no batch in part. Recovery takes the newest whole cut.
  An upload of part of a backlog — the 8 MiB input bound leaving pages
  behind — is never recovered on its own: a batch's writes to two buckets
  could otherwise come back one without the other. (This held for phase 5's
  disaster recovery too, which took any whole upload; the cut fixes both.)
  Files with no cut among them — a first upload that never finished — open
  as an empty database, their numbers stepped over.
- **Converting** between disk + object and object is a close and an open:
  the files are the same. Closed in disk + object mode with everything
  uploaded, a database reopens in object mode; opened from object mode with
  a disk, it is adopted as phase 5 adopts it. The `MANIFEST` lives in the
  object store too, so the shard count travels with the files.
- **Converting from or to disk mode** moves the pages
  (`wavedb_storage::convert(dir, types, options, Backend)`, offline,
  decided 2026-10-16). To the object store: the prefix must hold no
  database (`ObjectStoreInUse`); its `MANIFEST` goes up, every page on disk
  is flagged and a checkpoint names the flags, and the local `MANIFEST`
  says object-backed last — the next open uploads it all as any backlog.
  To the disk: opened object-backed (recovered from the object store when
  there is no disk yet), every evicted page comes back, the flags are
  cleared, a checkpoint names it, and the local `MANIFEST` says disk-only
  last; the object store's files are **kept** as they were (the user's
  call), and a disk-only open drops the remote directories its addressing
  log still carries. The stored mode changes only at the last step, so a
  conversion stopped anywhere is finished by running it again.

### 10. Navigation is resident

The directories, the navigation lanes (`WDB.REC`, `WDB.DEAD`, `WDB.IDX`),
Pivots and B+tree nodes are kept in RAM and never evicted: only the pages
holding record bytes — a type's own slot and `WDB.SEG` — are fetched on
demand. The cost is RAM proportional to the navigation structures.

As built (phase 6): `StructStorage::resident()`, a declaration flag the
macros set on the recency, removal and index lanes and on every Pivot, and
the engine on its `BpTree` node slot; `evict_cold` passes those types
over. It is behaviour, not bytes, so it does not fold into the
`STRUCT_HASH`. After a recovery they start evicted like everything else and
stay once a read or a write brings them back.

### 11. One writer per prefix

The lock on each data file and journal makes a second open
`EngineBusy`. Object mode has no
such lock, and **no lease** (decided 2026-10-10): running one node per
prefix is the deployment's to arrange. A second writer is caught, not
merged: every file is PUT with `If-None-Match: *`, and a file number is
never written twice — not after a crash, and not after a failed PUT either
(the next upload starts past the attempted files). So a refused PUT means
another writer's file is there: the store is **fenced**
(`StorageError::Fenced`), takes no more writes, and what it held that was
not uploaded is lost. Neither writer's files are ever overwritten.

### 12. Signing requests

Every request carries an AWS Signature V4: a SHA-256 of the body
(`x-amz-content-sha256`) and of the canonical request, and a signing key
derived by four chained HMAC-SHA256 from the secret, the date, the region and
the service. The primitives come from **RustCrypto** (`sha2`, `hmac`;
decided 2026-10-10, their source read before they are added) — a hash written
by hand is where a subtle bug passes every test and becomes a hole. The
protocol around them (canonical request, scope, headers) is ours. The body is
hashed rather than sent `UNSIGNED-PAYLOAD`: over plain HTTP nothing else
vouches for it.

### 12a. HTTPS

**Decided 2026-10-16.** An `https://` endpoint speaks TLS 1.2/1.3 through
**rustls** on the ***ring*** provider (`aws-lc-rs`, rustls' default, needs
cmake and a larger C build), its certificate verified against **Mozilla's
roots** compiled in (`webpki-roots`) plus any the caller trusts
(`Bucket::trusting`, for an endpoint a private authority signed); the
sources were read before they were added. `http://host[:port]` or a bare
`host:port` stays plain HTTP, for a local server. rustls is a state machine
over bytes, so it is driven by hand over the same `async-io` socket
(`s3::tls`), with no async adapter crate, and offers the plaintext as
`embedded-io-async` I/O, so the exchange runs over either. A peer that
closes without `close_notify` — common with `connection: close` — ends the
plaintext as a clean close would; the response's own framing says whether
the body is whole. Public endpoints answer some requests with
`transfer-encoding: chunked`, which the exchange now decodes. rustls holds
its configuration behind an `Arc` and takes it no other way — the one shared
pointer on this path, immutable.

### 13. Ids that do not exist

A `get` reaches the object store only when the record cache misses, the
id's disk bucket was evicted (a page on disk is complete, so its absence
there is an answer), and the id did not come from navigation (navigation is
resident, and what it finds exists). What is left is a client asking for an
id of its choosing — and nothing short of a structure growing with the
number of keys can prove absence: `n · log₂(1/ε)` bits for `n` keys at error
`ε`, ~13 000 bits for a page of 1 900 records. A Bloom filter in the
descriptor's spare bits, or a `u64` per page, is saturated at a few dozen
keys and answers "maybe" for everything.

**Decided 2026-10-10: no filter.** Absence is bounded by business rules
instead — the deployment's rules on what a client may ask for, and a budget
of object-store requests per tenant (every request is authenticated, so the
cost is attributable). A page holding only a bucket's ids, kept on disk when
its values are evicted, would answer exactly at ~5 bytes per record; it is
noted, not built.

### 14. The read seam becomes asynchronous

`BlockFile::read_run` and `Directory::read_page` are synchronous `pread`s. A
GET must be awaited, so that the worker serves other owners meanwhile (one
executor per thread, [RFC 0067](0067-thread-per-core-shards-WIP.md)). Where
a page is read is the bucket's own descriptor: `EVICTED` sends the read to
the object store — a plain `match` (`read_through::Settled`), not a
registry and not `dyn`.

As built in phase 4: a read does its synchronous half — record cache, page
on disk — when it is *called*, as before, and its future awaits only for an
evicted bucket; `apply` commits on the call unless the batch writes to an
evicted bucket. A disk-only store's futures are therefore still ready at
once and never suspend.

- **It lands with the remote read, not before.** `Store::get_of` already
  returns a future; what is synchronous is the chain beneath it. Made `async`
  ahead of anything that awaits, it would be plumbing with no await in it
  (`clippy::unused_async`), so the chain turns asynchronous in the phase that
  adds the first remote read **on the read path — phase 4** (moved from
  phase 3, 2026-10-10): until a disk page can be evicted, every page is on
  disk and complete, so no `get` reaches the object store. Phase 3's only
  remote reads are the upload's own (`PageStore::upload`, and
  `read_uploaded`, the read phase 4 builds the fallback on), and those are
  `async` already.
- **No cell is borrowed across the await.** The read copies the bucket's
  descriptor out of the directory, awaits the bytes, then borrows the
  dictionary to decode: a settle round publishing on the same thread must
  never find the directory borrowed.
- **A suspending read is safe on the node, and only there.**
  `concurrent_node_clobber.rs` (core) shows that a collection op which
  suspends between its reads and its `apply` can silently lose an index
  update. The node serialises every operation per owner
  (`quick-node::shard::lock`), which exists precisely so that an engine that
  yields keeps that property. The object-backed modes are node deployments;
  the embedded and client-cache uses stay in disk mode, where no read
  suspends.

## Testing

- The test object store is **versitygw 1.8.0** (`flake.nix`): an S3 gateway
  over a plain directory, so every test owns one in its tempdir and the
  server starts in milliseconds. Probed 2026-10-10 with `curl --aws-sigv4`:
  - `If-None-Match: *` on an existing key answers `412`; `If-Match` answers
    `200` with the current ETag and `412` with another — the conditional
    PUT the fencing relies on (§11);
  - a range inside an object answers `206` with exactly its bytes, one
    running past the end `206` cut at the end, one starting past it `416`;
    files are block-aligned anyway, so a page's range never runs past its
    file;
  - `LIST prefix=1seg/ max-keys=1` answers the smallest key — the newest
    file, by the complemented names; DELETE answers `204`.
- Rejected: **Garage** 1.3.1 (Rust) ignores conditional headers on PUT —
  `If-None-Match: *` answers `200` and overwrites (`src/api/s3/put.rs` never
  reads them); **MinIO** is abandoned upstream and marked insecure in
  nixpkgs.
- `wavedb-platform`'s `test-server` feature runs one versitygw per test
  over its own directory (`s3::test_server::TestServer`), shared by the
  client's tests, the engine's and the node's.
- Durability angles: reopen from the object store alone (phase 5); a crash
  between a PUT and the window that records it; a crash between a
  flag-clearing round and the next checkpoint.

### Phase 3, as built (2026-10-10)

- `StoreOptions::remote` (`Remote`: a bucket and a key prefix; each shard
  keeps its objects under `shards/<xx>/` below it). The endpoint and the
  credentials come with every open and are stored nowhere.
- A settle round writes its pages flagged; compaction keeps the flag of what
  it moves. `pending_upload_bytes` is the tally (an emptied bucket counts a
  block), kept where descriptors change.
- `PageStore::upload` (async): takes up to 8 MiB of flagged pages, fetches
  the remote pages they share records with (32 requests in flight),
  rebuilds and splits them at 128 KiB, packs files, PUTs them, publishes.
- `EditChunk` carries the remote directories' changes and the remote end;
  a snapshot carries them whole. Reopen restores both from the disk alone.
- The node uploads from each worker's BG task once a store holds back
  `Maintenance::upload_after_bytes` (8 MiB); `Server::object_store` turns
  the mode on.
- Proved against versitygw (`crates/wavedb-storage/tests/object_store.rs`):
  every settled record reads back from the object store; removals and
  rewrites reach it; a reopen restores what was uploaded and the next upload
  continues after it; an upload lost in a crash is made again, past the
  lost files; compaction keeps the flags; the mode is fixed. The
  congruence is checked against real routing for every directory length
  pair up to 40 × 20.
- **Phase 5** is proved in `tests/recovery.rs`: a database whose disk is
  deleted opens from its files and carries on (and again after a second
  loss); the newest of three uploads of each record wins; a copy of a file
  under another number and bytes that are no header are skipped; and the
  files crashed uploads left are not taken for the newest. In-crate tests
  build chains by hand: an upload missing a file, or linked to another
  header, is passed over for the one before it.
- **Phase 6** is proved in `tests/object_mode.rs`: a database lives in the
  object store alone (no disk directory is ever created) and carries on
  across reopens; a crash loses only what no upload carried; an upload of
  part of a backlog is never recovered — the test fails with the cut check
  removed — and its files are stepped over; a second writer on the prefix
  is fenced, and nothing either wrote is overwritten — the test fails with
  the PUT made unconditional; evicted pages give their RAM back once
  compacted; navigation is never evicted; and a sharded disk + object
  database reopens in object mode with its own shard count. The node's
  upkeep uploads on the interval below a file's worth, and its close
  uploads the rest (`shard::owned`).
- **Phase 7** is proved in `tests/remote_compaction.rs`: rewritten files are
  deleted and every record reads, through the store and after a reopen;
  a disk + object store compacts and reopens from its disk and from its
  files alone; small files are carried forward and deleted; a file the
  last cut names is kept, and a crash after the compaction recovers that
  cut — the test fails with the cut's names ignored, and with the horizon
  ignored; a file the disk's checkpoint names is kept, and the store
  reopened from that checkpoint uploads on — the test fails with the roots
  ignored. In-crate: the survey's horizon is the lowest header a recovery
  reads (`remote_compact`), and a type written in every upload still comes
  round in the restating order — the test fails with the old order
  (`upload_file`). The node compacts the object store when a trigger is met
  (`shard::owned`).
- **Converting** is proved in `tests/convert.rs`: a disk database moves to
  the object store, uploads everything, refuses a disk-only open, and opens
  from its files alone — the test fails with the flags left unset; a prefix
  holding a database is refused; an object-store database comes down to a
  disk it never had, its files kept, and refuses an object-backed open; a
  disk + object database brings its evicted pages back before the mode
  changes — the test fails without that — and carries on as a disk
  database; nothing to convert is `NoDatabase`; converting twice does
  nothing the second time; a database of four shards goes up and comes
  back down to a disk it never had, every record read through its shard —
  the test fails with any one direction converting shard 0 alone.
- **HTTPS** is proved in `wavedb-platform/tests/s3_https.rs` against a
  versitygw serving TLS with a certificate for `127.0.0.1` from an authority
  made for the test (`TestServer::start_tls`, `openssl` from the dev shell):
  a 3 MiB body both ways, a range, a conditional PUT refused, the listing
  and a delete; a certificate no trusted authority signed is refused before
  any request. A database lives in the object store over HTTPS and recovers
  through it (`tests/object_mode.rs`). In-crate: endpoints parse to their
  address, host header and certificate name; a chunked body is whole only
  at its end.
- The test server now counts as started only once it serves a marker
  written into its own root: two servers handed the same free port made
  tests talk to another test's server, now and then.

## Phasing

| Phase | Content |
|-------|---------|
| 1 | The descriptor repacked; the test object store (versitygw) in the dev shell, its behaviour verified — done |
| 2 | The S3 client half (range GET, PUT, DELETE, LIST, conditional PUT, SigV4): `wavedb-platform::s3` — done |
| 3 | disk + object: the remote directory, the flag, the upload that reassembles flagged pages into files with the header, reopen from disk — done |
| 4 | Eviction from disk (LRU, past the compaction ceiling); the read path falling back to the object store; writes, replays and wanted reads bringing a bucket back — done |
| 5 | Recovery from the object store alone: chained headers that name every type, spare room restating the oldest entries, the newest whole upload, adoption on open — done |
| 6 | Object mode: no disk, the pages in RAM until uploaded and as a cache, no journal (a write is durable once uploaded, on bytes or an interval), only cuts recovered, writers fenced by create-only PUTs, navigation resident — done |
| 7 | Compaction by file: the horizon and the deletable files found by a survey, mostly free files carried forward, blind DELETEs, the disk's roots kept; the restating order by rotation; the node's triggers — done |
| 8 | Converting from or to disk mode: `convert`, offline, the object store's files kept on the way down — done |
| 9 | HTTPS to a public endpoint: rustls on *ring*, Mozilla's roots plus trusted ones, chunked bodies — done |

## Open questions

1. ~~HTTPS to a public endpoint~~ — built 2026-10-16 (§12a). TLS between
   WaveDB and its clients is a separate question (future work).
2. ~~The disk cache's budget and eviction order~~ — decided 2026-10-10: the
   compaction ceiling, least recently used (§7).
