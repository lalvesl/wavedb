# RFC 0063 — The yield map, and an interruptible engine

- **Status:** Planned — opened 2026-08-21. The map (Parts 1–2) is done and one
  thing it found, **A2, is fixed and landed the same day**. Part 3's base
  case has landed: the settle is a driven `settle_step(budget)`, and since
  2026-10-09 the checkpoint is stepped too (RFC 0067 step 3). Still open: the
  wasm macrotask yield, and whether a yield between `plan_slot` calls is legal.
- **Crates:** `wavedb-storage` (the map and the work), `wavedb-quick-node` (the
  loop that drives it), later `wavedb-platform` (the wasm yield)
- **Successor to:** [RFC 0058](0058-per-type-actors-DEPRECATED.md). This RFC
  supplies the base case 0058 named as its own gate, and finds that the base
  case is **not** an actor. *(0058 was still parked when this was written; it
  was deprecated outright on 2026-08-22 — see the note on Part 4.)*
- **Part 4 is superseded by**
  [RFC 0064](0064-pivot-owned-concurrency-PLANNED.md). Parts 1–3 — the map, the
  legality analysis, and the interruptible engine — stand unchanged and remain
  the next executable step.
- **Related:** [0041](0041-single-barrier-checkpoint.md) /
  [0046](0046-directory-deltas-in-the-window.md) (the checkpoint whose blocking
  is the motivation), [0057](0057-page-arena-and-checkpoint-staging.md) (the
  arena that removes many of the reads mapped here),
  [0061](0061-relaxed-durability-window-DEPRECATED.md) (the one place the engine already
  chooses when to block), [0002](0002-architectural-hard-rules.md) (the
  non-`Send` stance the *elaboration* eventually amends — this RFC does not)

## Summary

RFC 0058 designed one actor per type, and on being parked stated the requirement
it had skipped: **the whole engine must be able to run as a single actor on a
single thread** — the wasm target — and that is not the same execution model as
N mailboxes interleaving on one worker.

Taking that requirement seriously produces two findings, and the second one
contradicts the word "actor" in it.

**First: there is a prior question nobody has answered.** Before anything can be
scheduled, *where may the engine give up its thread?* Today the answer is
**nowhere** — `wavedb-storage` contains no await point at all. Every `async fn`
on the request path has a fully synchronous body, and the maintenance loop calls
plain `fn`s that run to completion. Any scheduling scheme wrapped around code
that never yields is that code plus overhead.

**Second: once the yield points are mapped, the base case needs no scheduler.**
A mailbox is not what makes a yield point safe — the map is. And the engine is
not missing an executor: tokio natively and `wasm_bindgen_futures` in the
browser already are one. What it is missing is **interruptibility** — await
points inside its own long-running work, so the executor that already exists can
do its job. Building a message loop in front of a single-threaded engine whose
locks never contend is re-implementing what is already underneath it.

So the deliverable is the **yield map**: every site where the engine blocks,
what on, under which locks — and then the design content, **which of those sites
the engine may legally be observed at**.

The payoff is stated up front because it decides the roadmap: the base case
needs **no `Send` migration, no `MaybeSend` seam, no `Lane::Tree`, and therefore
no `STRUCT_HASH` break** — and no mailbox either. Every expensive and
irreversible part of 0058 belongs to the elaboration.

## Motivation — a yield point is a visibility commitment, not a performance knob

The instinct is to read "yield point" as *where we could go faster*. In a
database it is the opposite kind of statement. A yield point says: **at this
instant another operation may look at the engine's state, and what it sees must
be coherent.**

That reframing is why the map has to come first. Single-threadedness today is
silently supplying two invariants that nothing documents as depending on it
(RFC 0058 found both; they are restated here because they are what the map is
*for*):

- **I1 — batch application is atomic across the per-type caches.**
  `commit_to_caches` takes each type's write lock separately. Only the absence
  of interleaving stops a reader seeing half a batch.
- **I2 — `pending` empty ⇒ everything is settled.** `evict_settled`
  (`settle.rs:54`) trusts it, while `drain`'s `std::mem::take`
  (`settle.rs:23`) has already emptied the queue for the duration of a round.
  A concurrent eviction would drop a cached id whose page does not hold it yet,
  and the next read would fall through to stale bytes — a live-consistency
  fault with no crash involved.

Both are invariants about *when the engine may be observed*. So the map is not a
survey of blocking calls that happens to be useful; it is the statement of I1
and I2 in a form a reviewer can check.

## Part 1 — The map

Every site below is a synchronous call in `wavedb-storage`, reached from an
`async fn` that never awaits. Magnitudes are orders, not measurements — RFC 0058
was parked partly for treating estimates as findings, and that is not repeated
here. What *is* verified is the control flow and the lock set.

### Request path — `Store::apply`

| # | site | blocks on | locks held | order |
|---|---|---|---|---|
| A1 | `route_batch` (`apply.rs:131`) | memory | — | ns |
| A2 | `Expect` guard (`apply.rs:44` → `read_of` `apply.rs:91`) | ~~pread × N slots~~ → **one routed pread** | journal | 10–100 µs |
| A3 | `journal.append` (`apply.rs`) | **fsync** | journal | 10²–10³ µs |
| A4 | commit to the per-type caches (`commit_to_caches`) | memory, one write lock per type | journal | µs |
| A5 | `pending` push | memory | journal + pending | ns |

A2 is shown **as fixed**; the paragraph below is what it was and why it went
first. A4 likewise no longer probes pages to route a `Remove`, so the whole
journal-lock section is now one bounded read plus memory work plus the barrier.

A3 is the durability point and the only site the engine already reasons about
explicitly — RFC 0061's window is precisely a decision about *when* to block
there.

### Request path — `Store::get` / `get_of`

| # | site | blocks on | locks held | order |
|---|---|---|---|---|
| R1 | cache hit (`get_of`, `apply.rs:163`) | memory | one slot's cache read lock | ns |
| R2 | miss → `read_from_pages` (`read_through.rs:39`) | **pread + zstd decode** | that slot's directory + dictionary | 10–100 µs |
| R3 | untyped `get` → `read_any` | same as R2, **× N slots** | each slot's in turn | N × R2 |

### Maintenance path — same thread, 200 ms tick (`quick-node/src/lib.rs:304`)

| # | site | blocks on | locks held | order |
|---|---|---|---|---|
| M1 | `plan_slot` (`plan.rs:105`) | **pread per touched bucket, zstd decode, merge, zstd encode** | that slot's directory + dictionary | **unbounded in the round's size** |
| M2 | `place_in` window write (`checkpoint.rs:106`) | **pwrite of the whole window** | **alloc + meta, across carve → write → free** | 10²–10³ µs, grows with the round |
| M3 | `commit_journal` `data.bin` sync (`commit.rs:79`) | **fsync** | — | 10²–10³ µs |
| M4 | `evict_settled` (`settle.rs:54`) | memory walk of every slot's cache | **journal** + pending | µs–ms |
| M5 | `compact_step` → `relocate` (`checkpoint.rs`) | as M2 | alloc + meta | as M2 |

M1 and M2 together are the answer to "why does a checkpoint stall the node": not
a lock, but occupancy of the only thread, for a duration that scales with how
much was written since the last one.

### The finding that stands on its own — A2 *(fixed, landed 2026-08-21)*

**Every guarded write did a full-slot disk scan while holding the lock that
serializes all writers.**

`apply_inner` takes `self.journal.lock()` and *then*, inside it, validates each
`Write::Expect` by calling `read_any` (`apply.rs:39`). `read_any` probes every
slot's cache, and on a miss calls `read_from_pages` for **each slot in turn**
(`apply.rs:85`). `read_page` short-circuits only when that specific bucket's
descriptor is unallocated (`directory_pages.rs:33`) — so in a populated database
each probe is a real pread plus a zstd decode.

The worst shape is the common one. `Write::Expect(id, None)` — "this anchor is
vacant", the guard on every **first** save of a record — matches nothing, so it
pays the full N-slot scan. And it pays it under the journal lock, where by
construction no other write can proceed.

This is worth stating separately from everything else here because it is not a
concurrency bug: it is wrong on one thread too, and it is fixable without any of
what follows. The map is what made it visible — A2 is the only entry in the
`apply` table that blocks on the disk *and* is not the barrier the engine
intends to take there.

#### What was done

`Write::Remove` and `Write::Expect` now carry the STRUCT_HASH they route by —
the fix RFC 0058 had already sketched for a different reason (under actors the
scan is a broadcast to N mailboxes) and filed as future work. The yield map
promoted it: the same scan is a live cost on one thread, in the worst possible
place.

```rust
Put(Id, Vec<u8>)                    // type = the head of its own bytes
Remove(u64, Id)                     // + struct_hash
Expect(u64, Id, Option<Vec<u8>>)    // + struct_hash
```

`Put` deliberately gains nothing: its bytes are STRUCT_HASH-headed, and a
second copy on the variant could only disagree with the head.

Consequences, all of them narrowing:

- The guard is `read_of(hash, id)` — one binary search on the route table, at
  most one page read, **independent of how many types the schema declares**.
- `commit_to_caches` routes a `Remove` by its hash instead of calling
  `owner_of`, so it reads **no page at all**. It is now infallible and returns
  `Touched` rather than `StorageResult<Touched>` — the commit section can state
  that it cannot touch a page, which is what Part 2 needs of it.
- `owner_of` is **deleted**. Nothing searches for an id's type any more.
- `route_batch` validates all three variants uniformly, so an unregistered hash
  is refused before the journal rather than becoming a silent no-op.

The two rejected alternatives, recorded because they were live options:
resolving guards from the caches alone (a miss would have to refuse rather than
guess, changing behaviour); and hoisting the scan out of the lock with
revalidation inside (correct, but keeps a scan nobody needs).

Only the construction sites had to change, and there were **five** outside
tests — two `Expect` in `record.rs` (both `plan.hash`) and three `Remove` in
the index layer (`chain.rs` and `sparse_write.rs`: `self.lane_hash`; `tree.rs`:
`BPTREE_NODE_STRUCT_HASH`). Every caller already had the hash in hand, which is
the evidence that the field was missing rather than unknowable.

`Write` is a wire type, so this changes the journal's on-disk layout — free
under the pre-release policy (`FORMAT_VERSION` pinned, old `data.bin`
unsupported), and it folds into **no** `STRUCT_HASH`: `Write` is engine
plumbing, not a user type, so no schema changes.

Proven by two tests in `page_store.rs`, each checked to fail against the old
code: `a_guard_costs_the_same_whatever_the_schemas_width` (measured 1 read with
one type and 2 with two before the fix; 1 and 1 after) and
`a_remove_reads_no_page` (1 before, 0 after).

## Part 2 — Which sites are legal yield points

Applying I1 and I2 to the map. This is the design content; the table above is
the survey.

**Illegal — the journal-lock section of `apply_inner` (A2–A5).** Yielding
between A3 and A4 lets a second `apply` journal its frame before the first
commits its cache, so replay order and observed order disagree. Yielding inside
A4 exposes half a batch, violating I1 directly. The existing
`#[allow(clippy::significant_drop_tightening)]` and its comment already say the
guard must span the commit; the map makes it a rule rather than a note.

*Consequence:* A2 had to leave that section — not as an optimisation, but
because it was the only unbounded blocking call inside a region that may not
yield, which is the worst possible place for one. Done; the section is now one
routed page read, memory work, and the barrier.

**Illegal — `place_in` (M2, M5).** `alloc` and `meta` are held across
carve → write → free deliberately: a window must not be handed out twice, and a
superseded run must not return to the pool before its replacement is on disk.
Any yield inside breaks both.

**Illegal — `evict_settled` (M4).** It holds the journal lock precisely to
quiesce writers so "queue empty" cannot race a commit whose ids are not queued
yet. A yield inside re-opens exactly the I2 fault.

**Legal — between rounds in `drain` (`settle.rs:22`).** The queue boundary is
consistent by construction: writes landing during a round are picked up by the
next one, and that is already how the loop is written. **This is the single most
valuable yield point in the engine, and the one wasm needs**: it is where a
checkpoint stops being one long stall and becomes a sequence of resumable steps.

**Undecided — between `plan_slot` calls inside `settle` (`checkpoint.rs:72`).**
Tempting, because M1 is the unbounded one. A plan is built from the caches'
current bytes; a write landing between two plans would change bytes an earlier
plan already read, so the window writes a page that is stale on arrival. The
record is still in the journal and its id is back in `pending`, so the next
round rewrites the page — which *suggests* correctness with waste. That is an
argument, not a proof, and it is exactly the class of reasoning that produced I2
by accident. **This RFC does not claim it.** It has to be proven or refused
before any implementation depends on it.

## Part 3 — The base case is interruptibility, not an actor

An earlier draft called the base case "one actor on one thread", carrying the
word over from RFC 0058. That was wrong, and the correction is worth recording
because it *removes* machinery:

- **A mailbox is not what makes a yield point safe.** The map is. If the only
  legal point is between `drain` rounds, and the state at that point is coherent
  by construction, there is nothing for a queue to serialize. The
  `parking_lot` locks already present handle fine-grained access, and on one
  thread they never contend.
- **The engine is not missing an executor.** tokio natively and
  `wasm_bindgen_futures` in the browser already schedule. A message loop with a
  resumable state machine in front of a single-threaded engine is a second
  scheduler layered on the first.
- **RFC 0058 already said so** and it was not heard: *"with everything in one
  actor, the per-type state separation buys nothing and costs a routing layer."*
  It filed that as an unresolved gap in the actor design. The resolution is that
  the base case is not an actor.

So the base case is:

> The engine's long-running work becomes **interruptible at the points Part 2
> declares legal**, and nothing else changes. No mailbox, no routing layer, no
> executor, no ownership migration.

### What that requires

1. **`drain` becomes re-entrant.** Today it loops to exhaustion
   (`settle.rs:22`). It has to become something that can do one round and hand
   control back — an `async fn` awaiting at the round boundary, or a plain
   `fn step() -> Progress` the caller drives. **This is the whole job**, and the
   choice between the two shapes is the one real design decision left (see the
   open questions: wasm code size decides it, and a driven `step()` is not a
   scheduler either — it is closer to an iterator).
2. **A yield on wasm must be a macrotask.** `platform::time::sleep` (i.e.
   `setTimeout`), not an already-resolved promise: microtasks run to exhaustion
   before the browser paints, so a microtask yield gives the UI nothing. This is
   the difference between a checkpoint that stutters and one that freezes.
3. ~~**A2 moves out of the journal-lock section**~~ — done, and it was
   independently worth doing.

### What it explicitly does not require

| 0058 prerequisite | needed for the base case? |
|---|---|
| a mailbox / message loop | **no** |
| `Send` migration of the engine | **no** |
| `MaybeSend` / `MaybeSync` / `Shared` platform seam | **no** |
| desugaring `Store` off async-fn-in-trait | **no** |
| `Lane::Tree`, and its **`STRUCT_HASH` break for every indexed type** | **no** |
| `Arc` in place of `Rc` | **no** |
| `get_many` | **no** (it amortises mailbox crossings that will not exist) |

Nothing in the base case reaches stored bytes, reverses a documented hard rule,
or costs a schema change. It is additive and reversible — the right shape for a
first step into an area where the order already came out wrong once.

## Part 4 — The elaboration, and where an actor does earn its keep

> **Superseded 2026-08-22 by [RFC 0064](0064-pivot-owned-concurrency-PLANNED.md).**
> This part assumed the elaboration would re-inherit 0058's design and its six
> remaining open points. It does not: the unit of ownership is the **Pivot
> instance**, not the type, so `Lane::Tree` and the `STRUCT_HASH` break it
> charged are unnecessary, and the `Send` migration collapses to the disk
> actors' message types. Two claims below are also overtaken — I1 does not need
> a mechanism (a batch belongs to one owner, so it is atomic by ownership), and
> the `Send` migration is not the elaboration's price. The rest of this section
> is left as written, as the record of the reasoning 0064 replaced.


Only once the base case exists does the multi-thread question become well-posed,
because *then* "another task may run here" has a defined meaning: it is the same
set of legal points, observed by someone else.

And there the actor argument is sound, for a reason that does not apply to one
thread: **an actor is about ownership across threads.** Actors own instead of
sharing, so the locks that exist only to satisfy `Sync` on a `static` stop
existing rather than stop contending — `RwLock<BTreeMap>` becomes `BTreeMap`.
That is a real saving when there are threads to share between, and no saving at
all when there is one thread and the locks are uncontended. Which is precisely
why it belongs to the elaboration and not the base.

The elaboration therefore remains 0058's design, and re-inherits 0058's open
list — with two of the eight resolved by the base case:

- **#2, "the single-actor collapse has no design"** — resolved by removal: there
  is no single-actor collapse, because the base case is not an actor.
- **#1, actor-to-actor deadlock** — the base case has no mailboxes, so the hops
  0058 worried about (type → journal → writer) stay ordinary calls, and the
  question only arises when the elaboration introduces the mailboxes that create
  it. It stops being a precondition and becomes part of that design.

The remaining six (`Lane::Tree`'s schema price, the width of the `Send`
migration, unmeasured numbers, cross-actor ordering, `get_many`, arena
ownership) stand unchanged and stay with 0058.

Embedded — the second target named when this work was commissioned — inherits
the base case directly: one thread with explicit, declared yield points is the
configuration a single-core target wants, and here it is the *base* rather than
a stripped-down special case.

## Alternatives

- **Wrap the synchronous engine in `spawn_blocking` and stop.** Genuinely fixes
  the native node's stall for a fraction of the work. Rejected as *the* answer
  because it fixes the target that already has threads and does nothing for the
  one that does not: on wasm there is no thread to hand off to, so the
  checkpoint still freezes the frame. It remains a legitimate interim native
  measure, and it composes with this RFC rather than competing.
- **A message loop for the single-threaded engine** (the earlier draft of this
  RFC). Rejected in Part 3: it re-implements scheduling the target already
  provides, and buys none of the ownership benefit that justifies actors in the
  multi-thread case.
- **Continue 0058 as written — N actors first, single-thread as a degenerate
  case.** Rejected by 0058's own parking note, and by the table in Part 3: it
  front-loads a `STRUCT_HASH` break and a wide `Send` migration to buy
  parallelism on the target that is not the priority.
- **A multi-thread runtime with finer-grained locks, no actors.** Keeps the
  statics and leaves I1 and I2 as discipline — which is how they came to be
  undocumented in the first place.
- **Do the `MaybeSend` seam first, since the elaboration needs it anyway.**
  Mechanical and safe. Rejected as the *opening* move: it is prerequisite only
  to the elaboration, and committing to the elaboration's prerequisites before
  the base case exists is the mistake being corrected.

## Open questions

- **`async fn` or a driven `step()` for the re-entrant `drain`?** The `async fn`
  reads better; the explicit step may be smaller in the wasm artifact and keeps
  the engine free of any executor assumption, which matters for embedded.
  `nix build .#wasm` makes this measurable rather than arguable, and it should
  be measured before it is chosen.
- **Is a yield between `plan_slot` calls safe?** Part 2 leaves it undecided. It
  is the difference between yielding per round and yielding per slot plan — i.e.
  whether M1's unbounded cost is interruptible at all.
- **What granularity does the wasm yield want** — per round, per slot plan, per
  N pages? A frame budget (~16 ms) is the natural unit and nothing currently
  measures against it.
- **Does the maintenance tick stay a timer?** With a re-entrant `drain`, "settle
  a round whenever the loop is idle" becomes available and fits better than a
  200 ms poll — but it changes *when* checkpoints happen, which RFC 0041/0047
  reason about.
