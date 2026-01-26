# RFC 0019 — Journal-rooted recovery — DEPRECATED

- **Status:** Deprecated — superseded by
  [RFC 0072 — The settle is the durability point](0072-the-settle-is-the-durability-point-WIP.md) (2026-10-16)
- **Was:** Implemented (landed 2026-07-07 — the journal work J1–J5)
- **Supersedes:** [RFC 0030 — superblock-pointer checkpoint](0030-superblock-pointer-checkpoint-DEPRECATED.md)

## What it proposed

Durability as a **journal-first WAL**: every batch appended to a journal file
and synced, then committed to the cache; pages settled later. A checkpoint
rotated the journal, settled what the old one held, and wrote a `Commit`
frame naming where the addressing state lived; recovery rooted in the newest
valid `Commit` frame and replayed every batch after it.

## Why it was replaced

Every byte was written twice — once to the journal, once to its page — and the
journal existed to be thrown away. Keeping the journal and the pages in step
took rotation, retirement, two protected generations and a deferred frame
(RFCs 0041–0048), and recovery was a second model beside the object store's
recovery from file headers alone.

## What replaced it

[RFC 0072](0072-the-settle-is-the-durability-point-WIP.md): the settle writes each batch into one window ending with a
header naming the addresses it changed; one barrier makes the batch durable;
a reopen walks the headers from the newest whole batch, replaying nothing.
