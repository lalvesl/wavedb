# RFC 0062 — Relaxed mode refinements — DEPRECATED

- **Status:** Deprecated — superseded by
  [RFC 0072 — The settle is the durability point](0072-the-settle-is-the-durability-point-WIP.md) (2026-10-16)
- **Was:** Planned (opened 2026-08-21, not started)

## What it proposed

To refine [RFC 0061](0061-relaxed-durability-window-DEPRECATED.md)'s window: a
quiet store's unsynced tail bounded by time rather than traffic, durability
finer than the whole store's, and a tuned barrier.

## Why it was replaced

RFC 0072 answers the first two: the settle runs on a tick, whatever the
traffic, and durability is chosen per write (`_durable`).

## What replaced it

[RFC 0072](0072-the-settle-is-the-durability-point-WIP.md).
