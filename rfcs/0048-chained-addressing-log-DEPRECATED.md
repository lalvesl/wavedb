# RFC 0048 — The addressing log as a chain — DEPRECATED

- **Status:** Deprecated — superseded by
  [RFC 0072 — The settle is the durability point](0072-the-settle-is-the-durability-point.md) (2026-10-16)
- **Was:** Implemented (landed 2026-07-29)

## What it proposed

Each settle window's `EditChunk` named the chunk before it (`prev`), so the
journal's `Commit` frame carried one address — the chain's head — and the
chain was compacted into a fresh snapshot once its deltas outweighed it.

## Why it was replaced

With no journal there is no frame to carry the head: the newest header is
simply the last 8 blocks of the newest data file. And a snapshot rewrote the
whole addressing state; the header's spare room restates it a little in every
window instead.

## What replaced it

[RFC 0072](0072-the-settle-is-the-durability-point.md) §3–§6: the 32 KiB header every window ends with — the same one
the object store's files carry — chained by `prev` and per type by `last`,
walked on open.
