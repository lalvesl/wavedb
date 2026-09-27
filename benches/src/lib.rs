//! WaveDB comparative benchmark — RFC 0060.
//!
//! Two binaries share this library: `wavedb-bench` measures, and `bench-gen`
//! fills. The split exists because the fill has to run **inside a Nix builder**
//! to become a cached seed derivation (§6), where nothing is timed and nothing
//! is recorded.

// Bench-scale arithmetic: row counts, byte totals and nanosecond sums are all
// far inside the ranges these lints guard, and the alternative — `try_from`
// plumbing on every count — would bury the measurement logic.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    // The workspace's own stance (root `Cargo.toml` lints): product names in
    // prose are not code spans.
    clippy::doc_markdown,
    // The error mappers exist to be passed as `map_err(sql)`, which needs the
    // by-value signature the lint objects to.
    clippy::needless_pass_by_value
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

/// The window the **`relaxed` durability row** measures WaveDB under — the
/// counterpart of the knob each competitor's own documentation calls relaxed.
///
/// One second, which lands mid-pack rather than flattering: PostgreSQL's
/// `synchronous_commit = off` risks about 3 × `wal_writer_delay` (~600 ms),
/// MySQL's `innodb_flush_log_at_trx_commit = 2` flushes once a second, and
/// SQLite's `synchronous = NORMAL` in WAL mode holds until a checkpoint —
/// potentially far longer than any of them. None of these are equal to each
/// other, and the row does not pretend they are: it reports each system in the
/// configuration its own docs call relaxed, with the window named in the
/// settings so a reader can discount it.
pub const RELAXED_WINDOW: std::time::Duration =
    std::time::Duration::from_secs(1);

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

pub mod cage;
pub mod corpus;
pub mod footprint;
pub mod guard;
pub mod harness;
pub mod host;
pub mod io_counters;
pub mod json;
pub mod plan;
pub mod report;
pub mod row;
pub mod schema;
pub mod seed;
pub mod shop;
pub mod supervise;
pub mod systems;
