//! One module per measured system: what is left of them after RFC 0065.
//!
//! The `run` functions that used to live here are gone — each owned its own
//! loop, its own timing and its own phase boundaries, which is exactly how ten
//! adapters drift apart. Measurement moved to `harness` + `drivers`; what
//! survives here is the part that was never a measurement: the preloads, the
//! DDL, and the open helpers.
//!
//! No trait and no `dyn` still holds. The systems have genuinely different
//! setup, quiescence and compaction steps, and hiding that behind one
//! interface would only make the differences harder to read.

pub mod drivers;
pub mod engine;
pub mod server;
pub mod shop;
pub mod wavedb;

// The server bracket. Behind a feature so `bench-gen` — a build input of every
// seed derivation — does not compile three database clients to write a TSV.

use std::path::PathBuf;

/// What to run. Sizes are explicit rather than derived so a results file can
/// be reproduced from its own record.
pub struct Cfg {
    pub rows: u64,
    pub reads: u64,
    pub updates: u64,
    pub seed: u64,
    /// Scratch root; each system gets a fresh subdirectory under it.
    pub work_dir: PathBuf,
    /// Prefilled seed (a Nix store path). Present ⇒ the fill is already done
    /// and the insert phase is skipped — the insert benchmark *is* a fill, so
    /// it can never be served from a seed (RFC 0060 §6).
    pub seed_wavedb: Option<PathBuf>,
    pub seed_sqlite: Option<PathBuf>,
    pub seed_postgres: Option<PathBuf>,
    pub seed_mysql: Option<PathBuf>,
    pub seed_mongodb: Option<PathBuf>,
}

/// Durability row (RFC 0060 §2). `WaveDB` has only one mode to offer — that
/// asymmetry is a result, not an omission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    Durable,
    Relaxed,
}

impl Durability {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Relaxed => "relaxed",
        }
    }
}
