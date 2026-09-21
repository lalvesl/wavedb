# RFC 0041 — Single-barrier checkpoint

- **Status:** Implemented (landed 2026-07-28)
- **Crates:** `wavedb-storage` (`wavedb-quick-node`'s maintenance policy is the only affected caller)
- **Code:** `crates/wavedb-storage/src/{plan,checkpoint,settle,commit}.rs`
- **Builds on:** [RFC 0018](0018-storage-engine.md), [RFC 0019](0019-journal-rooted-recovery.md)
- **Companion:** [RFC 0042](0042-free-space-defragmentation.md) — supplies the large
  contiguous windows this checkpoint consumes (not a correctness dependency:
  without it the window falls back to growing the tail)

## Summary

A checkpoint becomes **one sequential write plus one fsync**. Every page rewrite,
directory chain and dictionary run it produces is planned in RAM — grouped **per
bucket** instead of per record — allocated as **one contiguous `Run`** through the
allocator's existing best-fit, written with a single `BlockFile::write_run`, and
made durable by a single `BlockFile::sync`. The visible commit is the in-memory
swap of each type's descriptor vector and its `ChainTrack`; the `Commit` frame
appended to the new journal carries **no barrier of its own** — it becomes durable
on the next `Batch` fsync. A crash anywhere leaves the window unreferenced, so
recovery discards it for free and re-settles from the retained journal.

## Motivation

The scarce resources in a large database are **RAM and the disk's IOPS ceiling**,
not disk space. The per-mutation path already respects that: `Journal::append`
writes a whole batch with one `write_all_at` and fsyncs once, and `settle` is off
the hot path behind the `pending` queue ([RFC 0019](0019-journal-rooted-recovery.md)).
The checkpoint does not respect it.

`PageStore::settle` walks the pending queue **per `Id`**, and every id runs
`Directory::upsert_record`: read the bucket page, upsert one record, allocate a
new run, write it, free the old one. A hundred ids landing in one bucket cost a
hundred read-modify-write cycles of the *same* page and a hundred alloc/free
pairs, each write positioned wherever best-fit found a hole — scattered, never
sequential. `commit_journal` then syncs `data.bin` and appends the `Commit` frame
with a second fsync.

Two properties the engine already has make the fix cheap:

- **Page writes are already copy-on-write.** `place` allocates a new run, writes
  it, and only then repoints the directory slot, freeing the old run afterwards
  (`directory_pages`).
- **Settle writes cache state, not batch state** — which is what makes
  re-settling idempotent, and means the checkpoint never re-reads the journal:
  the bytes it must write already sit in `StructStorage::mem_cache()`.

What is missing is batching the writes and collapsing the barriers.

## Design

The checkpoint splits into three clean stages — **plan (RAM) → write (one IOp) →
commit (pointer swap)** — replacing today's interleaved plan-and-write.

### Phase 0 — rotate (unchanged)

Under `self.journal.lock()`, `Journal::create` + `mem::replace`. Writers redirect
to the new journal immediately; no settle work happens under that lock.

### Phase 1 — plan in RAM, grouped by bucket

The drained `Touched` (`Vec<(slot_idx, Vec<Id>)>`) is reorganised per
`StructStorage` slot:

1. group the `Id`s by `Directory::bucket_of(id.raw())`;
2. read each touched bucket **once** (`Directory::read_page` → `SlotPage::from_bytes`
   against the slot's `DictState`) — the only read IO left in a checkpoint, and
   the floor: one read per touched page;
3. apply every id of that bucket to the in-memory `SlotPage`, taking bytes from
   `StructStorage::mem_cache()` — present ⇒ `upsert`, absent ⇒ `remove` (the
   existing "settle writes cache state" rule, unchanged);
4. resolve splits **here**, as a logical decision: the `maybe_split` /
   `split_threshold_blocks` policy runs over the assembled pages and a split
   yields two new `SlotPage`s. Nothing is written yet, so no page is ever written
   twice in one checkpoint;
5. `DictState::warm` **once per type** (not per record), before serialising, so
   every page of the checkpoint stamps the same `dict_len`;
6. serialise (`SlotPage::to_bytes`) into `(bytes, blocks)` pairs;
7. build the directory-chain blocks in RAM for every type whose
   `ChainTrack.dirty` is set — what `chain::write_chain` does today, reduced to
   producing bytes.

A fault in this phase returns the round to `pending` exactly as `drain` does
today; nothing has been allocated or written.

### Phase 2 — one allocation

Sum the blocks of everything (pages + chain blocks + a grown dictionary run) and
call `alloc.alloc(total)` **once**. Best-fit returns the smallest free extent that
holds the whole checkpoint — when [RFC 0042](0042-free-space-defragmentation.md)
has consolidated a large window, recycling happens here with no special case; when
no hole fits, the file grows at the tail. The returned `Run` is carved into
per-page `BlockDescriptor::from_run_used(...)`. The allocator is not told about
the subdivision: each page is freed individually later and `by_pos` coalesces the
window back together.

### Phase 3 — one write, one barrier

Concatenate the buffers in carve order, `BlockFile::write_run(window, &buf)` — a
single sequential `write_all_at` — then **one** `BlockFile::sync`. That is the
checkpoint's only durability barrier: all pages, all chains, the dictionary.

### Phase 4 — logical commit (the descriptor swap)

Only now, and only in memory: per type, the new descriptors replace the slots
under `StructStorage::directory()`, and `ChainTrack { root, blocks, dirty: false }`
lands under `StructStorage::chain()`. Readers begin seeing the new pages — which
are already durable. This closes the window today's settle leaves open, where the
directory is mutated *before* `file.sync()`.

### Phase 5 — the `Commit` frame

`JournalFrame::Commit(CommitFrame { journal_ts, roots, dicts })` is appended to
the **new** journal, and the retired journal is deleted.

> **Amended while implementing (2026-07-28), then restored (2026-07-29).** The
> design first called for appending this frame *without* its own fsync — letting
> the next `Batch` fsync carry it, so a checkpoint would cost exactly one
> barrier. It was implemented with the fsync instead, because deleting the
> retired journal is only safe once the frame is durable and the retired journal
> is the *only* place the previous `Commit` lives: a crash between the unlink and
> the frame reaching disk would leave recovery with neither.
>
> The objection was to doing the *delete* eagerly, not to deferring the *fsync* —
> and [RFC 0046](0046-directory-deltas-in-the-window.md) separated the two. Its
> `crate::retire` holds the retired journal and the protection roll until an
> append makes the frame durable, and forces the barrier explicitly for the two
> cases that motivated the objection (back-to-back checkpoints, an idle node).
> So the original intent stands: **one barrier per checkpoint**, the window's.

### Phase 6 — release

`alloc.free()` the superseded runs (old pages, old chain, old dictionary run),
deferred automatically while the previous checkpoint still protects them, then
`set_protected(&used)` with the new set, which releases the previous cycle's
deferred frees. Unchanged.

### Phase 7 — cache

`evict_settled(policy.cache_budget_bytes)` drops what just became a page.

### Crash model

`restore` finds the newest decodable `Commit`; `load_commit` derives `used` from
the chains, pages and dictionary runs it names; `BlockAllocator::from_layout`
turns **everything else in the file into free extents**. A window written half-way
in Phase 3 is referenced by no chain, so it is discarded for free — there is no
dirty state to clean. The retired journal is still on disk (Phase 5), so its
`Batch` frames replay through the normal path.

That costs extra CPU and IO **only** on the crash path. The trade is deliberate:
the data cannot be lost (the journal is the recovery root), so paying a barrier
per checkpoint to make a rare re-settle cheaper is the wrong side of the trade.

### What does not change

- The per-mutation `Store::apply` path: route, `Expect` guards, one journal
  append + fsync, cache commit under the journal lock.
- **Read cost.** A page stays whole and contiguous — this is CoW of full pages,
  not a delta log. No read amplification, no LSM-style merge on the read path.
- The page format (`SlotPage`), the dictionary's prefix versioning, `Id` routing,
  linear hashing, the protected-set/deferred-free rule.

### The one tuning trade-off

The checkpoint's RAM peak is **the sum of the touched pages**, not the sum of the
journal's bytes: a bucket touched once is rewritten whole. So
`Maintenance::checkpoint_after_bytes` controls both knobs at once — more frequent
⇒ fewer touched buckets per checkpoint ⇒ smaller buffer (less RAM, fewer MB per
IOp) but more checkpoints; less frequent ⇒ more write-coalescing on repeatedly
touched pages (less total amplification) at a higher peak. The implementation
must expose both numbers (window bytes, touched buckets) as metrics, because this
is the only tuning dimension left.

### Testing

`BlockFile` counts its own reads, writes and syncs (`BlockFile::io()` →
`IoCounts::snapshot`), so the claims are assertions rather than prose:

- `a_settle_round_is_one_write_and_one_read_per_touched_page` — 40 records
  re-settle in **one** write, **zero** syncs, at most one read per bucket.
- `a_checkpoint_is_one_write_and_one_barrier` — pages, dictionary and delta
  share one window write; one sync; and the result is a real recovery root
  (reopen resolves the records).
- The existing durability suite is unchanged and still green:
  `torn_commit_frame_falls_back_to_the_old_journal`,
  `covered_journal_left_behind_is_skipped_and_cleaned`,
  `unsettled_writes_read_correctly_and_survive_reopen`,
  `many_records_trigger_split_and_stay_readable` (splits now decided in the
  plan), `tombstone_hides_stale_page_until_settle`.

Still open as follow-ups: a kill-during-window-write test (the abandoned window
must read back as free space), and a baseline in
`crates/wavedb-bench/results/`.

## Alternatives

- **Commit record at the tail of `data.bin`**, so one write covers pages *and*
  commit. Rejected: recovery would need a backward scan with sequence numbers,
  and the journal-rooted `Commit` ([RFC 0019](0019-journal-rooted-recovery.md))
  already provides an atomic marker that now costs no barrier of its own.
- **Fsync the `Commit` frame immediately** (two barriers per checkpoint).
  Rejected: it buys nothing except a cheaper crash path, which is the exception,
  not the workload.
- **Keep the per-id settle and add write-combining inside `BlockFile`.**
  Rejected: it removes neither the repeated read-modify-write of the same page
  nor the alloc/free churn — the waste is in the plan, not in the syscalls.
- **Always append at the tail and truncate later.** Rejected: the file then only
  grows and depends on the tail happening to be free. Best-fit over coalesced
  extents — which `BlockAllocator` already implements — keeps it bounded.

## Open questions

- **One `Run` or a few?** The design forces one contiguous window (tail fallback).
  Allowing 2–3 runs when only smaller holes exist would avoid some tail growth at
  the cost of a few more `write_all_at` calls under the same single fsync.
- **`sync_all` vs `sync_data`.** Growing the tail changes the file size, so
  metadata must be synced; a recycled window does not, and `fdatasync` would be
  cheaper. Wants a `BlockFile::sync_data` and a preallocation policy.
- **Where the split threshold reads from** once splits are planned in RAM:
  serialized bytes or block count. *(Implemented as the serialized block count,
  re-checked after each split, bounded by `MAX_SPLITS_PER_ROUND`.)*
- **The chain is the one part that scales with the directory, not with the
  change.** A dirty type rewrites its whole address vector — `ceil(buckets/507)`
  blocks — even if one bucket moved, because `ChainNode` is doubly linked and
  copy-on-writing one node cascades into its neighbours' links. Making it
  `dirty_nodes + 1` needs a different chain shape (a root block indexing the
  node addresses instead of node-to-node links). Deliberately left alone here;
  it is a format change and wants its own RFC.
