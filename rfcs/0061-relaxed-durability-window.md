# RFC 0061 — Relaxed durability: a group-commit window

- **Status:** Implemented (landed 2026-08-21) — opened 2026-08-14, prompted by
  the measurement in [0060](0060-comparative-benchmark-suite-DEPRECATED.md).
- **Crates:** `wavedb-storage` (the write path), `wavedb-quick-node` (the
  node-side setting). No schema-crate or macro change; nothing folds into
  `STRUCT_HASH`.
- **Code:** `crates/wavedb-storage/src/{apply,journal,page_store,retire}.rs`,
  `crates/wavedb-quick-node/src/lib.rs` (`Server::relax_window`).
- **Follow-on:** [0062](0062-relaxed-mode-refinements-PLANNED.md) — the
  refinements this RFC deliberately left out.
- **Related:** [0041](0041-single-barrier-checkpoint.md) (the checkpoint's one
  barrier), [0046](0046-directory-deltas-in-the-window.md) (which introduced
  `append_deferred`, and whose "ride the next barrier" trick this generalises),
  [0047](0047-generational-journal-retirement.md) (the retirement that already
  depends on barrier counting),
  [0060](0060-comparative-benchmark-suite-DEPRECATED.md) (the measurement below).

## Summary

WaveDB `fsync`s once per collection operation, always, with no way to ask for
less. Every competitor offers a relaxed mode and every one of them is roughly
30× faster in it. This RFC adds a **durability window**: a store may be opened
with a non-zero window, in which `apply` appends the batch without a barrier and
syncs only when the window since the last sync has elapsed. Ordering, crash
consistency and the `Expect` guard are unchanged; what changes is the meaning of
`Ok` from a write, which is why this is an RFC and not a flag.

## Motivation

RFC 0060's first measurements, on one i5-8300H on btrfs:

```
system/row          bracket  insert/s read_hot/s read_cold/s update/s kB/insert kB/update
wavedb/durable     embedded       162    2976558        645      139     101.6      99.5
sqlite/durable     embedded       289     219115      271419      251      84.7      84.4
sqlite/relaxed     embedded     10441     338746      252589    11709       9.7       8.0
```

Three things fall out of that table, and together they are the whole argument.

**The bottleneck is the barrier, not the bytes.** SQLite writes 84.7 kB to the
block layer per insert of a 255-byte record; WaveDB writes 101.6 kB. Those are
the same number. On this filesystem an `fsync` costs ~85–100 kB of real writes
whatever the database is — btrfs COW, metadata, the log tree — so WaveDB is not
being wasteful with bytes. It is paying for 150 000 barriers, and the disk sits
at 100% while the CPU idles.

**Dropping the per-op barrier is worth ~36×.** The same SQLite, same records,
same machine, goes from 289/s to 10 441/s and from 84.7 kB to 9.7 kB per insert
by not syncing per operation. That is the size of the prize, measured rather than
argued.

**WaveDB has no such row to offer**, and RFC 0060 records that asymmetry as a
result: the durability axis has a hole in it where WaveDB's relaxed row should
be. The e-commerce workload (0060 §3.1) makes the same point from the
application's side — a checkout is one order plus its line items, the other four
commit it in one transaction for one barrier, and WaveDB pays one per record.

The obvious objection is that per-op durability is the guarantee, and dropping it
is dropping the product. It is not, for the same reason `synchronous_commit=off`
is not: the choice belongs to the application, once, at open. A shopping cart's
line items and a payment receipt do not want the same answer, and today WaveDB
forces the receipt's answer on both.

## Design

### 1. The window

`PageStore::open` gains a durability setting, and the whole change is at the one
place a barrier is taken:

```rust
// apply.rs — the durability point, replacing the single `journal.append(…)?`
if self.relax_window.is_zero() {
    journal.append(&frame)?;             // today: write + fsync
} else {
    journal.append_deferred(&frame)?;    // write, no barrier
    if journal.since_last_sync() >= self.relax_window {
        journal.sync()?;                 // one barrier for the whole window
    }
}
```

Both halves already existed. `append_deferred` was built for
RFC 0046's checkpoint frame — "the bytes are in the page cache, durable only once
someone syncs this file" — and `sync` is the barrier the
checkpoint and the retirement already use. Nothing new is written to disk and no
byte layout changes. The only state added is a `last_sync: Instant` on the
`Journal` (`since_last_sync()`), and `relax_window: Duration` on the store.

**No background task.** The window is checked under the journal lock that
`apply` already holds, so a burst of operations inside one window amortises to
one barrier without a timer, a flusher thread, or any interaction with the
non-`Send` current-thread model. A store that goes quiet leaves its tail
unsynced until the next write, the next checkpoint (`commit_journal`) or
shutdown (`force_retirement`, `retire.rs:105`, which already syncs) — and the
next section is why that is safe rather than merely convenient.

### 2. What does not change, and why

- **Ordering.** The journal is one sequential log and `sync` flushes the file,
  not a write — `append`'s own doc says so: it is "durable once this returns —
  **and so is every frame appended before it**, including any left deferred". A
  barrier therefore makes a *prefix* durable, always.
- **Crash consistency.** Frames are length-prefixed and CRC-checked
  (`to_wire_checked`), and replay stops at the first torn frame. A crash inside
  the window loses a **suffix of acknowledged writes** and can never leave a
  half-applied batch, a torn record, or a dangling index. This is exactly
  PostgreSQL's `synchronous_commit=off` semantics, and exactly why that setting
  is safe to offer while `fsync=off` is not: the failure mode is *lost*, never
  *corrupt*.
- **Journal retirement** (RFC 0047) asks `carrier.barriers() <=
  retiring.frame_barrier` — "was *this file* flushed since the frame was
  appended?" — which is the right question in both modes, so the deferred
  `Commit` frame stays correct and the fallback barrier stays the exception. It
  is the surrounding *comment* that had to change: "every `Batch` append
  `fsync`s" becomes "once per elapsed window", and a journal grown enough to
  trigger a checkpoint has crossed many windows. Cost, not correctness.
- **The `Expect` guard** reads through `read_any`, which checks
  the in-memory caches first, and the caches are still committed under the
  journal lock immediately after the append. A conflict is detected identically
  in both modes.
- **`STRUCT_HASH`.** Durability timing never reaches a stored byte. The hard
  rule folds "everything that reaches stored bytes" and explicitly exempts
  "behaviour that never reaches disk", so this is a runtime setting on the store
  — not a schema change, and not a knob that silently falsifies a declared
  guarantee.

### 3. The contract change, which is the actual cost

Today `save(&db).await?` returning `Ok` means the bytes are on the platter. With
a window it means: *in the journal, ordered, and durable within the window
unless the process dies first*. That is a weaker promise and it has to be said
out loud — in the API docs, in the crate README, and in `CLAUDE.md`'s
"one op is one batch is one barrier" line, which becomes "…unless a window is
configured".

It is weaker in a way that interacts with WaveDB's conflict stance. The engine
deliberately never auto-merges: it surfaces `Error::Conflict` and the developer
resolves it. But after a crash inside the window, an application cannot ask
*which* of its acknowledged writes survived — there is no ack-carrying position
to compare against. Two things follow:

- **`flush()` is part of this RFC, not a follow-up.** A store-level
  `flush().await` that forces the barrier lets an application be relaxed for the
  cart and strict for the receipt. Without it the window is an all-or-nothing
  setting, which is the version of this feature that gets misused.
- **The window is opened, never inferred.** No heuristic, no "relax when busy".
  A durability mode that changes by itself is a guarantee nobody can reason
  about.

### 4. Defaults and surface

- Default **zero** — today's behaviour, unchanged, for every existing caller.
  A weaker default is not a performance win, it is a silent downgrade of a
  promise the whole engine was built around.
- `PageStore::open_with(dir, types, StoreOptions { relax_window })`, with
  `open` left as it is and now delegating with `StoreOptions::default()` (a
  two-argument call is most of the test suite).
- `PageStore::flush()` forces the barrier.
- `wavedb-quick-node` exposes `Server::relax_window(Duration)` on the builder,
  beside `.data_dir()`.
- The client cache (`Db::open`'s write-through `PageStore`) is a candidate for a
  non-zero default in a *later* pass — it is a cache of node-owned state, so a
  lost suffix is re-fetched rather than lost — but not in this RFC, because
  "the cache is authoritative while offline" (RFC 0036) makes that a real
  decision rather than an obvious one.

### 5. How it gets measured

RFC 0060 already had the harness, and the row **landed 2026-08-22**: both
WaveDB adapters take a `Durability` like the other four, each row gets its own
store directory (sharing one would let the second row's `insert` phase measure
a rewrite), and the measured window is chosen at the reopen — the fill's window
never survives it. `RELAXED_WINDOW` is one second, mid-pack against SQLite's
`synchronous = NORMAL`, PostgreSQL's `synchronous_commit = off`, MySQL's
`innodb_flush_log_at_trx_commit = 2` and MongoDB's `j: false`.

The `kB/insert` column was named here as the direct check on whether the
barrier really was the cost — *"if the relaxed row does not fall to roughly a
tenth, the diagnosis above was wrong."* On a `--quick` smoke (40 users, **not**
a measurement and not recorded) it fell from **107.2 to 28.8**, with inserts
going 178/s → 1826/s and a shop checkout 9.91 ms → 1.10 ms at the median,
62.14 ms → 1.65 ms at p99. Directionally the diagnosis holds; the real number
waits on a recordable run.

A durability claim also needs a durability test. Three landed in
`page_store.rs`:

- `a_zero_window_takes_one_barrier_per_batch` — the default is what guards the
  promise from changing quietly, so it is the test that matters most.
- `a_burst_inside_one_window_costs_one_barrier` — 32 batches under a window
  long enough not to elapse take **no** barrier; `flush()` then takes exactly
  one.
- `an_elapsed_window_syncs_and_the_batch_replays` — the barrier is taken and a
  reopen finds the record.

Still owed: the **kill-during-write** angle, proving that a crash mid-window
leaves a lost *suffix* rather than a broken store. The property follows from
crc-framed frames and a replay that stops at the first torn one — the same
mechanism `torn_tail_is_discarded_and_truncated` already covers — but the
window makes a torn tail routine instead of rare, which is worth its own
two-process test.

## Alternatives

- **Do nothing; keep one barrier per op.** Honest, and the current position. It
  costs a measured ~36× on write-heavy work and leaves the e-commerce checkout
  paying one barrier per line item. Defensible for a single-user app; not for
  the "small shop" case the project actually targets.
- **A multi-record transaction instead.** This would fix the checkout properly —
  one batch, one barrier, all-or-nothing — and is strictly better *for that
  case*. It is also a much larger change (batch composition across collections,
  the `Expect` guard's scope, the settle queue's unit of work) and it does not
  help the single-record write at all. The two are complements: this RFC is the
  cheap general win, a transaction is the expensive specific one. Worth its own
  RFC.
- **A background flusher task.** A timer task syncing every N ms regardless of
  traffic. It bounds the loss window even when writes stop, which the in-line
  check does not — but it needs a task in a deliberately non-`Send`,
  current-thread engine, and it syncs a quiet store forever. The in-line version
  is chosen for being one branch under a lock already held; the bounded-loss
  variant can follow if the unbounded tail turns out to matter.
- **Batch size instead of time (`sync` every K appends).** Simpler to reason
  about but bounds nothing a user cares about: what an application wants to
  state is "I can lose at most 50 ms", not "at most 200 records".
- **`O_DSYNC` / `fdatasync` instead of `fsync`.** A constant-factor saving on
  the same number of barriers. Worth doing regardless, and orthogonal — it does
  not turn 150 000 barriers into 300.

## Resolved while implementing

1. **The window's clock is `std::time::Instant`.** The platform seam is the
   required route for *wall* clock, because `SystemTime::now()` panics on
   wasm32 — but `wavedb-storage` is a `cfg(not(target_arch = "wasm32"))`
   dependency and never compiles to wasm at all (the browser store is
   IndexedDB and has no journal). What a window wants is monotonic anyway: a
   rewound wall clock would either stall the window forever or sync on every
   append.
2. **`flush()` is on `PageStore`, not `Store`.** `MemStore` has nothing to
   flush, and a no-op impl on a trait is how a guarantee becomes a lie.
3. **The settle queue does not participate.** A batch is journalled and then
   committed to caches under the same lock; settling to pages happens later and
   is already crash-safe via replay, and none of that depends on *when* the
   journal was flushed.

## Open questions

1. **A quiet store keeps an unsynced tail** until the next write, checkpoint or
   shutdown. Bounding it needs a task in a deliberately non-`Send` engine —
   deferred to [0062](0062-relaxed-mode-refinements-PLANNED.md) along with the
   rest of the refinements.
