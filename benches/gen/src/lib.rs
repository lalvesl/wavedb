//! The seed side of the benchmark: the schema every system is filled with,
//! the fill itself, and `bench-gen`, which runs both inside a Nix builder.
//!
//! A crate of its own so that `bench-gen`'s source is exactly what it
//! compiles. It is a build input of **every seed**, and a derivation hashes
//! every file in its source: while it was a binary of the measuring crate,
//! editing the harness — a driver, a table, a comment — changed the seeds'
//! store paths, and at the `large` tier that is a four-hour rebuild of data
//! that did not change. The measuring crate depends on this one and
//! re-exports it, so the harness reads the same schema the seeds were filled
//! from — the STRUCT_HASHes cannot drift apart.

// Bench-scale arithmetic: row counts and byte totals are far inside the
// ranges these lints guard.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    // The workspace's own stance (root `Cargo.toml` lints): product names in
    // prose are not code spans.
    clippy::doc_markdown
)]

/// The durability window every **fill** in this suite opens its WaveDB store
/// with (RFC 0061).
///
/// A fill is not a measurement, and one op is one batch is one barrier, so a
/// durable fill of a few million records is a few million `fsync`s: the reason
/// the WaveDB seed took minutes where the others took seconds, and the reason
/// a very large shop preload is not affordable at all. This buys build time;
/// which window a *measured* phase runs under is the durability row's
/// question, not this one. The other four systems get the same courtesy under
/// different names — `.import`, `\copy`, `LOAD DATA` and `mongoimport` are not
/// the per-statement commit path either.
pub const FILL_WINDOW: std::time::Duration =
    std::time::Duration::from_millis(200);

/// Journal bytes that trigger a checkpoint during a **seed fill** — and it is
/// deliberately enormous.
///
/// A bare `PageStore` has no background maintenance, so a fill that never
/// checkpoints grows its journal for the whole fill (4.8 GB at 200 000 rows)
/// and its record cache with it. Both have to be bounded somewhere.
///
/// But bounding them *tightly* is worse than not bounding them at all. Page
/// writes are copy-on-write (RFC 0041): every intermediate settle rewrites
/// pages that the next settle rewrites again, so a frequent checkpoint is a
/// write amplifier. Measured at 200 000 rows: a 64 MiB trigger (~75 rounds)
/// took **4:25 and left a 303 MB store**; no trigger at all took **2:17 and
/// left 20 MB**. The end-of-fill settle writes each page once, and that is
/// the fill's optimum — the threshold exists only to stop the journal and the
/// cache from running away before it.
///
/// So: as rare as the disk allows. In **bytes**, never in operations —
/// per-operation log size depends on the data, and an earlier 5 000-op
/// trigger never fired once while 649 MB accumulated.
pub const FILL_CHECKPOINT_BYTES: u64 = 4 << 30;

pub mod schema;
pub mod seed;
