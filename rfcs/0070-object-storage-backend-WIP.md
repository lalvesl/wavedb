# RFC 0070 — Object storage as a page backend

- **Status:** In progress — opened 2026-10-10. Design agreed; phase 1 being built.
- **Supersedes:** [RFC 0059](0059-object-storage-capacity-tier-PLANNED.md) §2–§5
  (the remote descriptor bit and the object per bucket). 0059 §1, the archive
  lane, stays and is independent of this.
- **Crates:** `wavedb-storage` (the backend), `wavedb-platform` (the S3 client
  half), `wavedb-quick-node` (configuration)
- **Builds on:** [RFC 0046](0046-directory-deltas-in-the-window.md) /
  [RFC 0048](0048-chained-addressing-log.md) (descriptors travel as chained
  deltas), [RFC 0047](0047-generational-journal-retirement.md) (protection
  generations), [RFC 0061](0061-relaxed-durability-window.md) (group commit),
  [RFC 0067](0067-thread-per-core-shards-WIP.md) (a shard is a database),
  [RFC 0068](0068-record-cache-as-a-log-WIP.md) (the record log),
  [RFC 0069](0069-compaction-into-the-holes.md) (compaction and its triggers)

## Summary

A database stores its pages in one of three ways, fixed when it is created
and recorded in the `MANIFEST` beside `P`:

| Mode | Pages live in | Disk is | Journal |
|------|---------------|---------|---------|
| **disk** | `data.bin` | the store | local, fsync |
| **disk + object** | the object store | a **cache** of it, plus the pages not uploaded yet | local, fsync |
| **object** | the object store | absent — RAM caches | the object store |

Each type has two `Vec<u64>` directories, each a linear hash of its own: the
disk's (32 KiB pages) and the object store's (128 KiB pages, for a denser
vector). The disk pages the settle rounds wrote and the object store has not
received yet are reassembled, record by record, into the object store's
pages; those fill files of at most 8 MiB, whose first 64 KiB hold the
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
design ships settle output in files of up to 8 MiB instead, and makes the object store
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
    disk:   Directory,          // 32 KiB pages in data.bin
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

A file is 8 MiB of address space (2 048 blocks):

```
file   = start >> 11
offset = (start & 2047) · 4096
range  = bytes=offset-(offset + count · 4096 − 1)
```

- A file holds what was written to it and no more: its size is stored
  nowhere. It is written block-aligned — its last page padded to its block
  boundary — so a page's range never runs past the end of its file. A page never crosses a file boundary; one larger than 8 MiB spans
  consecutive files, read with one GET per file.
- 2^43 blocks is 2^32 files.

### 4. Files, names and the order LIST gives

Per shard, under `<prefix>/shards/<shard:02x>/`:

```
0journal/<seq:016x>      object mode only: one object per group-commit window
1start                   the oldest live file (first_file)
2seg/<!file:016x>        the files — named by the bitwise complement of the
                         number, so LIST returns the newest first
3snap/<!file:016x>/<n>   addressing snapshots (§6)
```

The numeric prefixes fix the order LIST returns. `LIST prefix=2seg/
max-keys=1` names the newest file in one request. (Directory buckets of S3
Express One Zone do not list in lexicographic order; they are out of scope.)

### 5. Upload: reassembling the flagged pages

Settle rounds run as today and flag what they write. When the flagged disk
pages reach about 8 MiB, an upload reassembles them into remote pages, which
fill one or more files:

```
[ 64 KiB header ][ page ][ page ] … ≤ 8 MiB
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

- **The header** is the file's addressing delta: per type, the bucket count
  and `(bucket, remote descriptor)` for every page in the file, plus any
  dictionary. At most 2 048 pages fit in 8 MiB, ~12 bytes each, so 64 KiB
  holds the worst case. The file is assembled in RAM before its PUT, so the
  header is written once, exact.
- **The chain is implicit**: the header of file `n` follows file `n − 1`'s,
  until a header that names a snapshot.
- After the PUT, the clearing of each flag and the new remote descriptors
  ride the next round on disk. A crash in between uploads those pages again.
- A crash after a PUT and before anything records it leaves a file at or
  above what the disk knows; the next upload overwrites it under the same
  name. Nothing names it, so that is safe.

### 6. Recovery

- **disk + object**: the disk's own addressing log restores both directories
  (the remote changes ride it), and the flags say what is still to go up. The
  object store is not read at all.
- **object**, and a disk lost in the other mode: `LIST 2seg/ max-keys=1` → the
  newest file; `GET 1start` → the oldest; then the 64 KiB headers back to the
  last snapshot. The names are computable, so those GETs go in parallel.
  Snapshots are periodic, as on disk ([RFC 0048](0048-chained-addressing-log.md)),
  and stored apart (`3snap/`) since a whole directory does not fit in a
  header.

### 7. Eviction from disk

In disk + object mode the disk is a cache. A bucket whose flag is clear may
have its disk page freed when the disk passes its budget; a read then goes to
the remote page covering the id. A settle round on an evicted bucket first
rebuilds it — the remote pages that share records with it (§2), filtered to
its own — and writes the merged page to disk with the flag set. Settle runs
in the background, so those GETs are off every request path. A page on disk
is therefore always complete, removals included, and no tombstone has to
outlive anything.

### 8. Compaction deletes files

The object store cannot reuse a hole inside an object, so space comes back
by deleting whole files:

- **One counter per file, no allocator.** A `Vec<u32>` of live blocks per
  file (`file − first_file`), built at open by scanning the remote
  directories, increased by an upload, decreased when a page is superseded.
  It never decides where to write — writing is always at the end.
- A file below the 0069 free share has its live pages fetched (one GET of the
  whole file) and written into the current upload; after the two sealing
  checkpoints of 0069 nothing names it, and it is DELETEd. `1start` advances
  when the oldest goes.
- **Only files older than the latest snapshot are deleted**, so the header
  chain recovery walks is never broken.

### 9. Object mode

There is no disk: the rounds accumulate in RAM until a file is ready, and
reads of pages not yet uploaded are served from there. A batch is durable
when its frame is in `0journal/` — one PUT per group-commit window
([RFC 0061](0061-relaxed-durability-window.md)), so an acknowledgement costs
one PUT's latency, shared by every write in the window. Each file header
records the journal sequence it covers; recovery replays the journal objects
after it until the first 404, and deletes the covered ones.

### 10. Navigation is resident

The directories, the navigation lanes (`WDB.REC`, `WDB.DEAD`, `WDB.IDX`),
Pivots and B+tree nodes are kept in RAM and never evicted: only the pages
holding record bytes — a type's own slot and `WDB.SEG` — are fetched on
demand. The cost is RAM proportional to the navigation structures.

### 11. One writer per prefix

The lock on `data.bin` makes a second open `EngineBusy`. In object mode the
node holds a lease object (`<prefix>/LOCK`) taken and renewed with
conditional PUTs (`If-None-Match`, `If-Match`).

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
pages are read sits behind a plain `enum` of backends, matched at the read
and write points — not a registry and not `dyn`.

- **It lands with the remote read, not before.** `Store::get_of` already
  returns a future; what is synchronous is the chain beneath it. Made `async`
  ahead of anything that awaits, it would be plumbing with no await in it
  (`clippy::unused_async`), so the chain turns asynchronous in the phase that
  adds the first remote read.
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
    PUTs the journal, `1start` and the lease rely on;
  - a range inside an object answers `206` with exactly its bytes, one
    running past the end `206` cut at the end, one starting past it `416`;
    files are block-aligned anyway, so a page's range never runs past its
    file;
  - `LIST prefix=2seg/ max-keys=1` answers the smallest key — the newest
    file, by the complemented names; DELETE answers `204`.
- Rejected: **Garage** 1.3.1 (Rust) ignores conditional headers on PUT —
  `If-None-Match: *` answers `200` and overwrites (`src/api/s3/put.rs` never
  reads them); **MinIO** is abandoned upstream and marked insecure in
  nixpkgs.
- Durability angles: reopen from the object store alone; a crash between a
  PUT and the round that records it; a crash between a flag-clearing round
  and the next checkpoint.

## Phasing

| Phase | Content |
|-------|---------|
| 1 | The descriptor repacked; the test object store (versitygw) in the dev shell, its behaviour verified — done |
| 2 | The S3 client half (range GET, PUT, DELETE, LIST, conditional PUT, SigV4) |
| 3 | disk + object: the backend `enum`, the asynchronous read chain, the remote directory, the flag, the upload that reassembles flagged pages into files with the header, reopen from disk |
| 4 | Eviction from disk and the settle that fetches an evicted page |
| 5 | Recovery from the object store alone; snapshots in `3snap/` |
| 6 | Object mode: rounds in RAM, the journal in the object store, the lease |
| 7 | Compaction by file, on the 0069 triggers |

## Open questions

1. **HTTPS to a public endpoint.** The tests run plain HTTP against a local
   server; a public endpoint is HTTPS. This is the node's outbound
   connection, a separate question from TLS between WaveDB and its clients
   (future work).
2. **The disk cache's budget and eviction order** (least recently read, or
   oldest written).
