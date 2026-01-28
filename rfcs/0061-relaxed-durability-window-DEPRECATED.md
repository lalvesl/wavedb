# RFC 0061 — Relaxed durability: a group-commit window — DEPRECATED

- **Status:** Deprecated — superseded by
  [RFC 0072 — The settle is the durability point](0072-the-settle-is-the-durability-point.md) (2026-10-16)
- **Was:** Implemented (landed 2026-08-21)

## What it proposed

`StoreOptions::relax_window` (`Server::relax_window`): a store whose batches
were appended to the journal without a barrier and synced once per elapsed
window, so a burst cost one `fsync`; `flush()` forced the barrier. Zero, the
default, kept one barrier per batch.

## Why it was replaced

Durability was a property of the store, when an application wants it per
write — relaxed for the cart, strict for the receipt.

## What replaced it

[RFC 0072](0072-the-settle-is-the-durability-point.md): every write is relaxed — durable at the next settle, a tick
away — and `apply_durable` (the `_durable` operations) waits for that settle;
everything before a durable write is durable with it.
