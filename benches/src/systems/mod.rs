//! One module per measured system, plus the shape they all report in.
//!
//! No trait, no `dyn`: each adapter is a concrete `run` the orchestrator calls
//! by name. The systems have genuinely different setup, quiescence and
//! compaction steps, and hiding that behind one interface would only make the
//! differences harder to read.

pub mod drivers;
pub mod engine;
pub mod server;
pub mod shop;
pub mod wavedb;

// The server bracket. Behind a feature so `bench-gen` — a build input of every
// seed derivation — does not compile three database clients to write a TSV.

use std::path::PathBuf;

use crate::footprint::{Footprint, Point};
use crate::metrics::Phase;

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

/// One system, in one configuration, over one dataset.
pub struct SystemReport {
    pub system: &'static str,
    /// The bracket this row belongs to; rows from different brackets are never
    /// comparable (RFC 0060 §2).
    pub bracket: &'static str,
    /// Which workload produced this row: `micro` or `shop`. The two are
    /// different questions and never share a table.
    pub workload: &'static str,
    pub durability: Durability,
    pub version: String,
    /// The actual settings, not a profile name — a later reader must not have
    /// to guess what "durable" meant on this machine.
    pub settings: Vec<(String, String)>,
    pub compression: &'static str,
    /// Whether superseded versions are still readable at the end of the run.
    /// `WaveDB` retains; nobody else does. Printed beside every update row.
    pub retains_history: bool,
    pub phases: Vec<Phase>,
    pub footprints: Vec<(Point, Footprint)>,
    pub live_records: u64,
    pub logical_bytes: u64,
    /// Caveats that belong to this row specifically.
    pub notes: Vec<String>,
    /// The seed this row ran against, if any — recorded so a result names the
    /// exact dataset it measured, not just its size.
    pub seed_path: Option<String>,
    /// Time spent copying the seed out of the read-only store. Reported, and
    /// deliberately outside every measurement window.
    pub materialise_ms: u64,
}

impl SystemReport {
    #[must_use]
    pub fn label(&self) -> String {
        format!("{}/{}", self.system, self.durability.name())
    }

    #[must_use]
    pub fn footprint(&self, point: Point) -> Footprint {
        self.footprints
            .iter()
            .find(|(p, _)| *p == point)
            .map_or_else(Footprint::default, |(_, f)| *f)
    }

    #[must_use]
    pub fn phase(&self, name: &str) -> Option<&Phase> {
        self.phases.iter().find(|p| p.name == name)
    }
}
