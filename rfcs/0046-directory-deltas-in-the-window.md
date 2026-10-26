# RFC 0046 — Directory deltas in the settle window

- **Status:** Implemented (landed 2026-07-29)
- **Amends:** [RFC 0043](0043-descriptors-in-the-commit-frame.md) (which put the
  *whole* addressing state in every `Commit` frame)
- **Builds on:** [RFC 0041](0041-single-barrier-checkpoint.md) (the one
  contiguous window a settle already writes — this rides it)
- **Crates:** `wavedb-storage`
- **Code:** `crates/wavedb-storage/src/{edit,retire,checkpoint,commit,journal}.rs`

## Summary

A settle window stops being only pages and becomes **self-describing**: its last
blocks carry the `BlockDescriptor` changes those pages caused. The metadata
therefore costs **zero additional IOps** — it is bytes inside a positioned write
that was already happening, made durable by an `fsync` that was already
happening. The `Commit` frame shrinks from "every bucket of every type" to a
short list of the edit chunks written since the last full snapshot.

A checkpoint's metadata cost stops scaling with the size of the database and
starts scaling with the size of the change.

## The problem

[RFC 0043](0043-descriptors-in-the-commit-frame.md) made a checkpoint's
metadata one journal IOp, which was right, but it made that IOp carry every
registered type's whole `Vec<BlockDescriptor>` — 8 bytes per bucket, every
bucket, every checkpoint. That volume is a function of the database, not of the
work:

| `data.bin` | buckets (≈16 KiB pages) | `Commit` frame, **per checkpoint** |
|---|---|---|
| 2 GiB | 131 072 | 1 MiB |
| 20 GiB | 1 310 720 | 10 MiB |
| 200 GiB | 13 107 200 | 100 MiB |

Beside the work it certifies, the amplification is the whole story. A checkpoint
that settles 100 pages writes ~1.6 MiB of pages either way; at 2 GiB it adds
1 MiB of frame (metadata ≈ 40 % of the checkpoint), at 200 GiB it adds 100 MiB
(≈ 98 %). Every type is written every time, including the ones untouched since
the database was created.

The engine's scarce resources are RAM and disk IOps. This spends IOps
proportional to the *stock* to record a change in the *flow* — the last place in
the write path that still does, now that the directory chain is gone.

### Why the frame has to be self-sufficient today

One line causes it: the retired journal is deleted immediately after the
`Commit` frame lands, so the newest frame is the only surviving mention of the
addressing state. The fix is not to shrink the frame but to give the state a
home that is **not** deleted every checkpoint — and `data.bin` already is one.

## Design

### The window becomes self-describing

[RFC 0041](0041-single-barrier-checkpoint.md) already carves one contiguous
window per settle round and fills it with page images and a grown dictionary.
Add one more target:

```text
window = [ page ][ page ][ … ][ dict ][ edit chunk ]
```

The **edit chunk** is a crc-framed record of exactly what this round changed:

```rust
pub struct EditChunk {
    pub slots: Vec<SlotEdit>,
}

pub struct SlotEdit {
    pub struct_hash: u64,
    /// Bucket count *after* the round, so a linear-hashing split needs no
    /// record of its own — replay grows the vector to it.
    pub buckets: u32,
    /// Only the buckets whose descriptor moved.
    pub changed: Vec<(u32, u64)>, // (bucket, raw BlockDescriptor)
    /// The dictionary's run — `Some` only when it changed.
    pub dict: Option<u64>,
}
```

On disk it is `[len u32 LE][crc32][wire]` — the same self-delimiting envelope a
page uses, because a run is block-padded and the length prefix is what bounds
the decode.

This is one more `Target` in `targets_of`, one more entry in `carve`, one more
`copy_from_slice` in `assemble`. **The same `write_run`, the same `fsync`.** A
round that changes 500 buckets adds ~6 KiB to a window that was already several
MiB — and it stays ~6 KiB when the database is 200 GiB.

Sizing is known in time: the number of changed buckets is `targets_of`'s page
count plus any buckets `plan_splits` appended, all decided before `carve`; the
chunk is serialised after `install` has set the new descriptors, into space the
carve already reserved.

**No dirty-tracking state is needed anywhere.** A round records the changes that
round made, in the window that made them. The `Directory` gains nothing, and the
RAM cost of the whole mechanism is the chunk buffer itself, alive for the
duration of one write.

### The frame carries the list, not a chain

The chunks need to be findable. The obvious shape is a back-pointer in each
chunk to the previous one, walked at startup back to the snapshot. The cheaper
shape is to skip the pointers: the `Commit` frame already exists, is already
appended once per checkpoint, and is now nearly empty.

```rust
pub struct CommitFrame {
    /// The retired journal's timestamp — this frame IS its DONE marker.
    pub journal_ts: u64,
    /// The full-state snapshot's run (the "princípio" the deltas patch).
    pub snapshot: u64,
    /// Every edit chunk written since that snapshot, oldest first.
    pub edits: Vec<u64>,
}
```

`edits` is O(settle rounds since the last snapshot), not O(database) — bounded
by the compaction threshold, so a few hundred bytes. Dropping the back-pointers
buys two things: a chunk is pure payload (nothing points at anything, so the
order in which superseded chunks are freed cannot break a link), and startup is
bounded by construction instead of by "however far back the chain goes".

### The frame takes no barrier — a checkpoint costs one

Because the frame is *only* a pointer into state the window's `fsync` already
made durable, it does not need a barrier of its own. It is written unsynced
(`Journal::append_deferred`) and the next ordinary `Batch` append carries it,
since `fsync` flushes the file rather than one write. **A checkpoint therefore
costs one barrier: the `data.bin` sync.** This was
[RFC 0041](0041-single-barrier-checkpoint.md)'s original intent, abandoned then
because the frame carried the addressing state itself; carrying a pointer brings
it back.

What has to wait for that durability is what the frame *authorises*
(`crate::retire`):

- **deleting the retired journal** — if the frame is torn or never lands,
  recovery falls back to the previous `Commit`, and that journal's batches are
  the only record of what happened since;
- **rolling the allocator's protected set forward** — which releases frees
  deferred under the *previous* commit, i.e. runs a fallback recovery still
  reads.

Both are held in a `Retiring` until an append makes the frame durable. There is
at most one pending: a checkpoint forces any previous one before it rotates, so
the pending frame always lives in the *current* journal. `force_retirement()`
pays the barrier explicitly for the cases where no write is coming — an idle
maintenance tick, a graceful shutdown.

The cost of deferral is that a retired journal lingers until the next write.
Recovery already handles a retained-but-covered journal (it deletes it), and on
an idle node the maintenance tick forces within one period.

> **Followed by [RFC 0047](0047-generational-journal-retirement.md)**
> (Implemented 2026-07-29).
> That forcing is the one barrier this design still pays on a checkpoint's
> behalf, just from a different call site — and completing the retirement from
> `apply` puts a lock and an `unlink` on the write path. 0047 drops both by
> holding the retirement until the *next* checkpoint, where the journal that
> carries the frame is the one being rotated out.

### Compaction

When `edits` grows past its threshold, the next settle round emits, alongside
its pages, a **full snapshot image** of every type's descriptor vector — from
the directories already resident in RAM. The frame then names the new snapshot
and an empty `edits`; the old snapshot run and every superseded chunk are freed
through the allocator's existing deferred-free path, releasing when protection
rolls forward at the following commit.

A snapshot is nothing special on the wire: it is a chunk whose `changed` covers
every bucket, so applying it over any prior state yields exactly that state.
Replay needs no flag, and the types the compacting round did not touch
contribute their resident directories — without that, a quiet type is lost at
the next reopen (there is a test for exactly this).

So yes — as you put it, the reclamation is a deallocation rather than an
`unlink`. It is also the only part of this that is not free, and it is
accepted: at the threshold below, roughly one round in N pays it, the snapshot
is a sequential image of a vector already in RAM, and it rides the same window
write as everything else.

**As built** (`MetaLog::wants_snapshot`), a round compacts when either

- `edit_blocks >= max(snapshot_blocks, 16)` — the ratio, so the deltas never
  outweigh the state they patch, with a 64 KiB floor so a small database does
  not snapshot every round; or
- `edits.len() >= 1024` — a hard cap, so a recovery's scattered reads stay
  bounded even when the snapshot is large enough that the ratio alone would
  allow far more.

### Recovery

1. Read the newest decodable `Commit` frame — unchanged, `journal.rs` already
   does this.
2. Read `snapshot` (one contiguous run) → install the directories.
3. Read each run in `edits`, oldest first, applying `SlotEdit`s → the current
   directories. A chunk whose crc fails is a corrupt database, exactly like a
   corrupt page (the frame naming it is the assertion that it is durable).
4. Derive the allocator from the resulting descriptors **plus** the snapshot and
   chunk runs — the loop `load_commit` already runs, with two more entries.
5. Journals with `ts <= journal_ts` are covered — delete them; the rest replay.

Reads: 1 + `edits.len()`, scattered, bounded by the compaction threshold. That
is more read IOps at startup than today's single frame; startup is the one place
this design deliberately spends, and it is the right place to spend.

### What it costs

| | today ([0043](0043-descriptors-in-the-commit-frame.md)) | this |
|---|---|---|
| metadata bytes per checkpoint | every bucket of every type | only changed buckets |
| …at 2 GiB / 131 072 buckets | 1 MiB | a few KiB |
| …at 200 GiB | ~100 MiB | a few KiB |
| **extra write IOps for metadata** | 1 (the big journal append) | **0** — rides the window |
| barriers per checkpoint | 2 | **1** — the `Commit` frame is deferred |
| extra RAM | — | one chunk buffer, per round |
| startup reads | 1 | 1 + chunks since snapshot |
| files in the data directory | 2 kinds | 2 kinds |

The honest cost, beyond startup: the snapshot and the chunks become **allocator
tenants**. They are short-lived runs interleaved among long-lived page runs, so
freeing them at compaction leaves holes — the pattern
[RFC 0042](0042-free-space-defragmentation-DEPRECATED.md) exists to clean up. Given that
disk space is not the scarce resource and the defragmenter is already running,
this is a fair price for a metadata path that costs no IOps at all.

## Alternatives

### A separate append-only manifest file

`manifest_<ts>.log`: a `Snapshot` frame per generation plus one small `Edit`
appended per checkpoint, compacted by writing a new generation and unlinking the
old. It keeps the metadata log out of the allocator entirely, so it never
fragments `data.bin` and compaction is `write new; fsync; unlink old` with no
protected-set interaction at all.

**Rejected — the IOp accounting is identical and it gives up the free ride.**
Both designs are 2 writes and 2 barriers per checkpoint (window + `fsync`, then
one small append + `fsync` to either the manifest or the journal). But the
manifest's append is a *new* write that has to happen, where an edit chunk is
bytes appended to a buffer that is written anyway. The manifest also costs a
third file, its own framing, its own rotation and its own recovery root — and it
breaks the property that `data.bin` plus a journal is a complete database, which
matters for copying and backing one up. The allocator churn it avoids is real
but small, bounded, and already handled.

### Two-level directory: group blocks in `data.bin`, roots in the frame

Persist descriptors as 4 KiB group blocks (~508 buckets each) and keep only the
group addresses in the frame; rewrite dirty groups. Rejected: the frame is still
O(directory) (8 KiB per checkpoint at 2 GiB, 800 KiB at 200 GiB — better, not
fixed), and one changed bucket still rewrites a whole 4 KiB group.

### Keep the deltas in the journal and retain journals until a snapshot

Then the journal *is* the delta log and nothing new exists. Rejected: journals
carry record batches, so retaining them to preserve a few KiB of addressing
deltas retains megabytes of redo — and releasing exactly those bytes is what a
checkpoint is for.

### Implicit placement — no directory to persist

Bucket `N` always at block `f(N)`, or double-buffered across two fixed slots with
one bit per bucket saying which is live. Dissolves the problem entirely, and
gives up copy-on-write: in-place page writes need torn-page recovery, and fixed
addresses destroy the contiguous-window property
[RFC 0041](0041-single-barrier-checkpoint.md) is built on. An architecture
change, not an optimisation.

## What landed

`crates/wavedb-storage/src/edit.rs` — the chunk types, `chunk_of` (both passes),
`Replay` (the recovery fold), and `MetaLog` (the log + its compaction policy).
`checkpoint.rs` gained a `Target::Edit`, the two-pass reserve/fill, and the
`meta.record` → free-the-superseded step; `commit.rs`'s `load_commit` became a
replay of the log; `CommitFrame` lost `slots`/`dicts` and gained
`snapshot`/`edits`. `retire.rs` holds the deferred half of a checkpoint, with
`Journal::append_deferred`/`sync`/`barriers` under it and
`PageStore::force_retirement` for when no write is coming; `apply` completes a
pending retirement right after its own fsync.

Proven by: `a_settle_round_is_one_write_and_one_read_per_touched_page` and
`a_checkpoint_is_one_write_and_one_barrier` (the chunk rides the existing
write), `a_commit_frame_tracks_the_log_not_the_directory`
(a wide directory framed in fewer bytes than 8 per bucket),
`compaction_bounds_the_log_and_the_chain_still_restores` (120 checkpoints keep
the frame bounded, and a quiet second type survives the snapshot), plus unit
tests for the envelope, the shape-stable length, the replay fold, and the
thresholds. The deferral adds `a_deferred_commit_that_never_lands_replays_the_retained_journal`
(truncate the unsynced frame away — every acked write still replays out of the
retained journal), and `a_checkpoint_is_one_write_and_one_barrier` now asserts
both files: one `data.bin` sync, zero journal barriers, then the next write's
own fsync completing the retirement.

## Open questions

- **Compaction threshold.** `max(snapshot_blocks, 16 blocks)` and a 1024-chunk
  cap are reasoned defaults, not measured ones; the bench baseline should tune
  them against real checkpoint cadence.
- **Back-pointers after all?** If `edits` in the frame ever feels wrong — say
  the threshold is raised far enough that the list is no longer trivial — a
  back-pointer per chunk restores O(1) frame size at the cost of ordering
  constraints on frees. The list is the better default; the chain is the escape
  hatch. *(Taken up by [RFC 0048](0048-chained-addressing-log.md), which
  reaches the condition from the other side: the threshold cannot be raised
  while the list is in the frame, because the log's write cost is quadratic in
  the interval. The free-ordering objection is answered by RFC 0047's
  two-generation protected set.)*
- **A whole-type edit variant.** A type whose buckets nearly all moved — a large
  defrag round, a rehash storm — is cheaper as a full descriptor vector than as
  a `changed` list. Letting `SlotEdit` carry either is a small addition; worth
  doing only if defrag rounds measure badly.
- **Defrag and chunk size.** Every relocation is a changed descriptor, so
  [RFC 0042](0042-free-space-defragmentation-DEPRECATED.md)'s budget now also bounds how
  large an edit chunk gets. Same knob, second effect — worth stating there.
