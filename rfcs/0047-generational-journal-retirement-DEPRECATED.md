# RFC 0047 — Generational journal retirement — DEPRECATED

- **Status:** Deprecated — superseded by
  [RFC 0072 — The settle is the durability point](0072-the-settle-is-the-durability-point-WIP.md) (2026-10-16)
- **Was:** Implemented (landed 2026-07-29)

## What it proposed

A checkpoint's `Commit` frame was appended without a barrier, so the journal it
retired and the older generation's protected blocks were disposed of by the
**next** checkpoint, once that frame was proven durable by the journal's
barrier count — two journals and two protected generations on disk as the
steady state.

## Why it was replaced

There is no journal any more (RFC 0072): nothing to retire. What the
protection guarded — a run freed while an older root still named it — is now a
deferred free, released once the batch that stopped naming it is durable.

## What replaced it

[RFC 0072](0072-the-settle-is-the-durability-point-WIP.md) §8: frees deferred to the batch's barrier; the reclaim cuts only
holes.
