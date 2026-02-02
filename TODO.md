# TODO

Work in hand, item by item. Designs live in `rfcs/`, deliberate deferrals in
`docs/improvements.md`; this file is only the working list.

## 1. The horizon in the header — a bounded walk (RFC 0072)

A type added to the code is unnamed in every header, so the first open
after it descends by `prev` through every surviving header — dead ones
below the horizon included, which it then holds live. The horizon the
engine already computes (and frees below, `drop_dead_headers`) is the
proof: every type that existed when a header was written is named at or
above that header's horizon. Recorded in the header, it bounds the
descent; a registered type still unnamed there is new, and empty.

- [x] 1.1 `Header` gains `horizon: u64` (`header.rs`), documented;
      `BARE_BYTES` follows by itself. Literals: `disk_header.rs`,
      `upload_file.rs`, and the tests in `header.rs`, `recover.rs`,
      `remote_compact.rs`.
- [x] 1.2 One function for the coverage line, shared by `header_at` and
      `drop_dead_headers`: the min of `horizon(...)` over the disk and the
      remote lane, with `floor`, `.max(restated)`. The `last_end` clamp
      stays in `drop_dead_headers` only (it is the batch-wholeness check).
- [x] 1.3 `header_at` fills `horizon` after the `restate(...)` calls,
      before `encode`.
- [x] 1.4 The object store's file headers (`upload_file.rs` `finish()`)
      fill it from the remote rotations; `Chain` gains a `floor`, set by
      the recovery (`recover.rs`).
- [x] 1.5 `walk.rs`: the descent by `prev` stops at `below < top.horizon`;
      the `last` chains stay free. Module and `walk()` docs: an unnamed type
      below the horizon is new, and empty.
- [x] 1.6 `reopen.rs`: with the bounded descent, `DiskHeaders.lowest` /
      `floor` and `seen` no longer hold dead headers live — check nothing
      else relies on it.
- [x] 1.7 Docs: RFC 0072 §6 (the stop at the horizon) + a Progress entry;
      the `header.rs` module doc on `prev`; `CLAUDE.md`'s storage paragraph
      ("down by `prev` while a registered type is unnamed, never below the
      root's horizon").
- [x] 1.8 The pre-commit bar: fmt, clippy, test, `check_file_length`, doc,
      nextest, deny, bench clippy. No new tests (existing reopen/recover
      tests cover the regression).

## 2. The page cache holds pages decompressed (RFC 0044)

Today `PageCache` holds a run's image exactly as stored — still
compressed — so every hit pays the zstd decompression and the decode into
a `SlotPage` again (`Directory::read_page` → `read_run_with`). A page that
came from disk or the object store should be decompressed once, and stay
so in RAM.

- [x] 2.1 `PageCache` holds a decoded `SlotPage` per entry instead of
      `Box<[u8]>`; the budget counts its decoded size (record bytes plus a
      per-record overhead), so the 64 MiB default holds fewer pages than
      before — the trade made on purpose: CPU per hit over RAM per page.
- [x] 2.2 `Directory::read_page` decodes on a miss and holds the result;
      a hit lends `&SlotPage`. `get_record` takes the one record from the
      lent page — no clone of the page.
- [x] 2.3 The settle's read-modify-write (`plan.rs`) takes the page **out**
      of the cache instead of cloning it: the round places it in a new run,
      superseding the old one anyway.
- [x] 2.4 A round holds the pages it writes, decoded, under their new runs
      — the page just settled is the one read next.
- [x] 2.5 Pages from the object store (`read_remote`, an evicted bucket;
      `upload.rs`) go through the same cache, decoded. Today they are not
      held at all. The key tells the lanes apart: a disk run and a remote
      descriptor are different address spaces.
- [x] 2.6 Object mode: the RAM backing keeps the stored (compressed) images
      — it is the store's "disk"; the page cache over it holds them decoded,
      as on disk.
- [x] 2.7 Docs: `page_cache.rs` module doc (the "still compressed"
      rationale reversed), `block_file.rs` `read_run_with`,
      `directory_pages.rs`, RFC 0044 (design + status), `CLAUDE.md`'s
      storage paragraph if it names the cache's form.
- [x] 2.8 The pre-commit bar.

## 3. Data shards and storage shards (RFC 0073)

P data shards (placement, ownership, caches — as today) write through S
storage shards (S ≤ P, dividing it, fixed at creation: 64 → 4). Each storage
shard has its own OS thread, outside Embassy, owning the allocator and the
files; workers send their rounds by channel, and one `pwritev` + one roots
block + one `fsync` (one PUT on S3) per tick makes every data shard in it
durable. Design: `rfcs/0073-data-shards-and-storage-shards-PLANNED.md`.

- [ ] 3.1 Format: the `MANIFEST`'s S, the `storage/<n>/` layout, the
      header's data shard, the `Roots { at, prev, roots: Vec<u64> }` block
      (positional, `NO_FILE` for a shard never written). P = S = 1
      everywhere; nothing else moves.
- [ ] 3.2 The split in one thread: the storage side (allocator, writer,
      barrier, roots, file locks) out of `PageStore`, driven inline when
      nobody runs a thread; the seam an enum (inline | channel), no `dyn`.
- [ ] 3.3 The round in three steps — `Reserve` → build with real addresses
      → `Write { bytes, frees }` → `Done` — and the settle reshaped around
      the await (no `RefCell` borrow across it; `checkpoint.rs`).
- [ ] 3.4 The storage thread: `recv_timeout` loop on an `async-channel`
      inbox, the gathered `pwritev` in reservation order, the roots block,
      the `fsync` on its barrier helper (`Synced` on the same inbox), one
      write in flight, a `_durable` write sends it at once; `Abandon`.
- [ ] 3.5 The node with S < P: storage threads started beside the workers,
      data shard `p` → storage shard `p / (P / S)`; read handles per data
      shard, opened/closed as `Done` reports files.
- [ ] 3.6 Reconstruction: the newest valid roots block (scan back), the
      storage write's wholeness check, `prev` on a cut; each data shard
      walks from `roots[i]` in parallel; the allocator from the runs they
      report.
- [ ] 3.7 Compaction by evacuation: `Evacuate { file }` to the data shards
      with runs in it, carried forward through their rounds, the file cut
      and deleted once a write over the moves is durable; dead roots blocks
      freed like dead headers.
- [ ] 3.8 The object store: sections built by the data shards, one file
      `[roots][header a][pages a][header b][pages b]…` and one PUT per
      storage shard; the chain fields in the roots block; recovery,
      `compact_remote`, `convert`, object mode.
- [ ] 3.9 Docs: RFC 0073 Progress per phase; RFC 0067 §2/§7/§8, 0072,
      0070, 0071 where they change; the storage and node READMEs;
      `CLAUDE.md`'s storage and node paragraphs.
- [ ] 3.10 The pre-commit bar at every phase.
