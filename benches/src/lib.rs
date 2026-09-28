//! WaveDB comparative benchmark — RFC 0060.
//!
//! The measuring side: `bench` supervises, `bench-row` measures one row. The
//! filling side — `bench-gen`, which runs **inside a Nix builder** to become a
//! cached seed derivation (§6), where nothing is timed — is the
//! `wavedb-bench-gen` crate in `gen/`.

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

/// A path argument made absolute where it is read.
///
/// The work directory reaches servers that resolve it after changing into
/// their own data directory: PostgreSQL given a relative `-k` fails with
/// `could not create lock file … No such file or directory`, and the row
/// then dies on a 300-second readiness timeout that names neither. Made
/// absolute once, at the command line, rather than per server.
///
/// # Errors
/// When the current directory cannot be read, the only way a relative
/// path has no absolute form.
pub fn absolute(path: &str) -> Result<std::path::PathBuf, String> {
    std::path::absolute(path).map_err(|e| format!("{path}: {e}"))
}

// The seed side lives in its own crate so that editing the harness cannot
// change a seed's store path (see `wavedb_bench_gen`). Re-exported under
// the old names: the harness reads the very schema the seeds were filled from.
pub use wavedb_bench_gen::{FILL_CHECKPOINT_BYTES, FILL_WINDOW, schema, seed};

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
pub mod shop;
pub mod supervise;
pub mod systems;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_relative_work_dir_is_anchored_at_the_current_directory() {
        let got = absolute("target/probe").expect("cwd is readable");
        assert!(got.is_absolute(), "{got:?}");
        assert!(got.ends_with("target/probe"), "{got:?}");
    }

    #[test]
    fn an_absolute_work_dir_is_kept_as_given() {
        let got = absolute("/var/tmp/x").expect("absolute");
        assert_eq!(got, std::path::PathBuf::from("/var/tmp/x"));
    }
}
