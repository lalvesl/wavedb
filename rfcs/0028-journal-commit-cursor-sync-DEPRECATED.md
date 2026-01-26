# RFC 0028 — Journal commit-cursor sync — DEPRECATED

- **Status:** Deprecated — superseded by
  [RFC 0022 — Live sync by navigation catch-up](0022-live-sync-navigation-catchup.md)
- **Was:** the original W6 catch-up design

## What it proposed

Reconnect catch-up as *"give me everything since sequence N."* The storage
journal ([RFC 0019](0019-journal-rooted-recovery-DEPRECATED.md)) assigns a monotonic commit
sequence; a watcher would remember the last sequence it saw and, on reconnect,
ask the node to replay every commit after it. One global cursor, one replay path.

## Why it was replaced

Rejected the same day the DB-1 model landed (2026-07-17), for three reasons:

1. **Rotated journals are deleted.** The journal truncates on checkpoint
   ([RFC 0019](0019-journal-rooted-recovery-DEPRECATED.md)), so a cursor older than the
   newest `Commit` names bytes that no longer exist — the replay can't answer.
2. **`Batch` frames are physical, not logical.** A journal frame is blocks and
   roots, not "record X changed"; a chained save writes several
   metadata-indistinguishable record `Put`s and a remove may rewrite no record —
   the op-level meaning lives *above* the batch, not in it.
3. **The resync fallback had to exist anyway.** Once you accept that a too-old
   cursor must fall back to navigating current state, the navigation path is the
   whole answer and the journal cursor is redundant.

## What replaced it

[RFC 0022](0022-live-sync-navigation-catchup.md): catch-up **navigates the data
itself** — the recency/dead log tails for a collection, the Unique chain forward
— past a per-topic **instant** cursor (`Command::Changes`). The node keeps no
per-session state; a restart loses nothing. The disk structures the history model
already maintains ([RFC 0009](0009-anchors-succession-and-history.md),
[RFC 0011](0011-bptree-index-and-collections.md)) *are* the sync log.
