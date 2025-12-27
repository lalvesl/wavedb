# RFC 0080 — Streamed writes to object stores (draft)

- **Status:** Planned (low). Draft, opened 2026-11-07. Not the focus now: it
  records what object stores offer for writing a file in pieces, so a later
  design starts from it.
- **Related:** [0070](0070-object-storage-backend.md) (object storage as a
  page backend; no journal, no lease, every PUT create-only),
  [0079](0079-headers-sized-to-their-change-WIP.md) (headers sized to
  their change; its §3 weighed a header at a file's end, read by one
  suffix GET, which a stream would need)
- **Crates:** `wavedb-platform` (`s3`), `wavedb-storage` (`remote*`,
  `upload*`, `object_mode`)

## Summary

Today a write reaches the object store as a whole file, PUT once, on 8 MiB
or `upload_interval` (10 s). Until then it lives on the disk or, in object
mode, only in RAM. Some object stores can grow one object in pieces: an
**append**, or an upload kept open and durable part by part. Using that, a
store could send its data as it accumulates, in small pieces, close the
object on a graceful shutdown, and lose at most the last piece on a crash.
That is a safer object mode, and it needs no page rewrite in place.

This draft lists the mechanisms and what each costs. It also sketches the
seam: the mechanism is fixed in the `MANIFEST` at creation, like the mode.

## What exists

To be checked against each provider's current documentation before a design
relies on it.

| mechanism | where | grows an object by | durable per piece | readable before close | limits |
|---|---|---|---|---|---|
| **Append** (`PutObject` with a write offset) | AWS S3 Express One Zone (directory buckets) | appending at the current end | yes | yes | one zone only (lower durability than Standard); storage ~5–6× Standard per GB, requests several times cheaper |
| **Append Object** (`PUT ?append&position=N`) | Ceph RGW; Alibaba OSS and Tencent COS | appending at a position | yes | yes | provider extension, outside the S3 API |
| **Append blob** (Append Block) | Azure Blob | appending blocks | yes | yes | block and count limits (about 4 MiB × 50 000) |
| **Resumable upload** | Google Cloud Storage | chunks, multiples of 256 KiB | yes (the server keeps the progress) | no | a session expires after about a week |
| **Compose** | Google Cloud Storage | joining up to 32 objects into one | — | yes | one request per compose |
| **Multipart Upload** | AWS S3, MinIO, R2, B2, Ceph, Garage… | parts | yes, once a part is accepted | **no**, until `CompleteMultipartUpload` | parts 5 MiB–5 GiB except the last; up to 10 000; R2 wants equal parts but the last |
| **SeaweedFS filer append** | SeaweedFS | `?op=append` on the filer's HTTP API | yes | yes | not the S3 API |

Multipart is the one S3-compatible way, and it does not help here: a part
under 5 MiB is refused unless it is the last, so a store could not send less
than 5 MiB at a time. That is the same granularity as today's 8 MiB uploads,
with a file still split into a few pieces.

## A sketch

- **The mechanism is chosen at creation and fixed in the `MANIFEST`.** It is
  one more field beside the mode (RFC 0070), and a database never switches
  it in place. With none chosen, the store behaves as today.
- **A stream file.** Each storage shard keeps one object open. It sends a
  piece on a small threshold (bytes or time), writing each piece at the
  object's known end. A piece is a whole number of windows: pages, header and
  roots block (RFC 0079 §3), so a header always lands after the pages it
  names.
- **Durable when the piece is.** A `_durable` write is answered once the
  piece carrying it is accepted. With an append mechanism, that is per piece,
  not per 8 MiB.
- **Graceful shutdown** sends the last piece and closes the object.
- **Crash.** A reopen reads the stream object's end, a suffix GET (as
  RFC 0079 §3 weighed it), and roots in the newest whole window it finds. A torn last piece
  is discarded like a torn disk window. For a resumable upload, the session
  is resumed or finished first.
- **Fencing.** An append at a stated position fails when the object has
  grown past it, which is the same answer `Fenced` gives today for a second
  writer. Each mechanism needs that checked.
- **Compaction** is unchanged in kind: a stream object closes at a size cap
  and becomes an ordinary file, compacted by file (RFC 0070 phase 7).

## Questions before a design

1. Whether this reopens RFC 0070's "no journal in the object store". A
   stream of windows is the same data the files hold, only sent sooner. It
   is not a second copy, but the request count per byte goes up.
2. Request cost per mechanism against the stance "minimise PUTs; GETs are
   cheap". It changes with the provider: on Express One Zone a request is
   the cheap part.
3. Readers: other nodes, and an open from the object store alone. Only the
   append mechanisms let them read a stream object before it closes.

## Alternatives

- **Multipart with 5 MiB parts.** It sends nothing smaller than 5 MiB, and
  the object stays unreadable until it is completed. It gains nothing over
  today's uploads.
- **Padding a small part with compressed zeros.** An object store keeps the
  bytes it is sent, and `Content-Encoding: gzip` is metadata it does not
  decode. A part's size is its stored size, so compressed zeros remain a
  small part and are refused. The last part needs no padding anyway.
- **One integration per provider now.** This is deliberately not the focus.
  The seam above is one manifest field, so each mechanism can be added when
  someone needs it.
