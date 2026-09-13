# RFC 0062 — Relaxed mode refinements

- **Status:** Planned — opened 2026-08-21. Not started.
- **Builds on:** [RFC 0061](0061-relaxed-durability-window.md) (the durability
  window, which landed with the deliberately smallest version of itself)
- **Crates:** `wavedb-storage`, `wavedb-quick-node`, possibly `wavedb`
  (the client cache). Nothing folds into `STRUCT_HASH`.

## Summary

Improve the relaxed mode. RFC 0061 shipped one branch under a lock the write
path already held, which was the right first version and is not the whole
feature. This RFC collects what that deliberately left out.

## Motivation

The window as it stands has three known edges, each recorded in 0061 as an
alternative not taken or an open question:

- **A quiet store keeps an unsynced tail** until the next write, checkpoint or
  shutdown. The in-line check bounds the loss by *traffic*, not by time, which
  is not what "I can lose at most 50 ms" means.
- **The window is the whole store's.** An application that wants to be relaxed
  for a cart and strict for a receipt has `flush()` and nothing finer.
- **The barrier itself is untuned.** `fsync` flushes file metadata the journal
  does not need; `fdatasync` is the same number of barriers at lower cost, and
  is orthogonal to grouping them.

## Design sketch

Not decided, listed in the order they are probably worth doing:

1. **Bounded loss.** A flusher that syncs a store gone quiet, so the window
   states a time rather than a traffic pattern. The cost is a task in a
   deliberately non-`Send`, current-thread engine — which is exactly why 0061
   did not take it.
2. **`fdatasync` instead of `fsync`** on the journal.
3. **Per-write strength.** A way to spell "this one is a receipt" other than
   flushing the whole store after it.
4. **A relaxed client cache.** `Db::open`'s write-through cache is a candidate
   for a non-zero default — a lost suffix is re-fetched from the node rather
   than lost — except that RFC 0036 makes the cache authoritative while
   offline, which turns an obvious default into a real decision.

## Open questions

1. Which of the four are worth it at all, and in what order — the measurement
   that answers this is [RFC 0060](0060-comparative-benchmark-suite-DEPRECATED.md)'s
   `wavedb/relaxed` row, which does not exist yet.
2. Whether bounded loss can be had without a background task (a deadline
   checked by the settle drain, which already runs, is the cheap candidate).
