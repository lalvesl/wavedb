//! The `micro` workload on the [RFC 0065] harness — dispatch and assembly.
//!
//! One generator, N consumers, a wall-clock denominator and pooled
//! percentiles — the shape [`bridge`](super::bridge) could not have, because
//! each RFC 0060 adapter owned its own loop.
//!
//! The rows themselves live next door: [`embedded`](super::embedded) for the
//! two in-process systems, [`server`](super::server) for the three that own a
//! process. What they share is here — how a stored row is assembled from the
//! harness's phases, and what a phase record keeps.
//!
//! ## What this path still does not carry
//!
//! **Read counters.** `read_bytes`/`rchar` arrive in phase 4, and their
//! absence is why a `mongod` observed reading ~1 TB against a 27.5 MB dataset
//! could not be classified from the corpus.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use wavedb_storage::PageStore;

use super::{RowRun, embedded};
use crate::corpus::{FootprintRecord, PhaseRecord, RowRecord};
use crate::harness::micro::MicroWorkload;
use crate::harness::{PhaseResult, percentiles};
use crate::schema::logical_bytes;
use crate::systems::engine::{Engine, Sharded};
use crate::systems::{Cfg, Durability};

/// The note a harness row carries in place of
/// [`PHASE1_NOTE`](super::bridge::PHASE1_NOTE).
pub const HARNESS_NOTE: &str = "Measured on the RFC 0065 producer/consumer harness: `wall_ns` is the \
     phase's wall clock, `total_ns` is the sum of the timed windows, and the \
     percentiles are pooled across every consumer. Read counters are absent \
     (phase 4).";

/// Run `run` on the harness, or answer `None` if this row is not a `micro`
/// row and belongs to [`bridge`](super::bridge).
///
/// # Errors
/// The store could not be opened, a server would not start, or an operation
/// was refused.
pub fn measure(
    run: &RowRun,
    d: Durability,
) -> Result<Option<RowRecord>, String> {
    if run.key.workload != "micro" {
        return Ok(None);
    }
    match run.key.system.as_str() {
        "sqlite" => embedded::sqlite(run, d).map(Some),
        "wavedb" => match super::engine_of(&run.key.variant)? {
            Engine::Direct => embedded::wavedb::<PageStore>(run, d).map(Some),
            Engine::Sharded => embedded::wavedb::<Sharded>(run, d).map(Some),
        },
        #[cfg(feature = "servers")]
        "postgres" => super::server::postgres(run, d).map(Some),
        #[cfg(feature = "servers")]
        "mysql" => super::server::mysql(run, d).map(Some),
        #[cfg(feature = "servers")]
        "mongodb" => super::server::mongodb(run, d).map(Some),
        #[cfg(not(feature = "servers"))]
        "postgres" | "mysql" | "mongodb" => Err(format!(
            "{}: built without the `servers` feature",
            run.key.system
        )),
        other => Err(format!("no micro adapter for {other}")),
    }
}

/// Everything about a row that is not a phase.
pub struct Meta {
    /// `embedded` or `server`. Rows from different brackets are never
    /// comparable.
    pub bracket: &'static str,
    pub settings: Vec<(String, String)>,
    pub compression: &'static str,
    pub retains_history: bool,
    pub notes: Vec<String>,
    pub seed_path: Option<String>,
    pub materialise_ms: u64,
    pub footprints: Vec<(String, FootprintRecord)>,
}

pub(super) const fn workload(cfg: &Cfg, seeded: bool) -> MicroWorkload {
    MicroWorkload {
        rows: cfg.rows,
        reads: cfg.reads,
        updates: cfg.updates,
        seed: cfg.seed,
        seeded,
    }
}

/// Put the seed in place, or make an empty directory. Answers whether the row
/// is seeded, and what materialising it cost.
pub(super) fn materialise(
    dir: &std::path::Path,
    seed: Option<&std::path::PathBuf>,
) -> Result<(bool, u64), String> {
    match seed {
        Some(src) => {
            let took = crate::seed::materialise(src, dir)?;
            Ok((true, took.as_millis() as u64))
        }
        None => {
            std::fs::create_dir_all(dir).map_err(|e| format!("mkdir: {e}"))?;
            Ok((false, 0))
        }
    }
}

pub(super) fn path_of(p: Option<&std::path::PathBuf>) -> Option<String> {
    p.map(|p| p.display().to_string())
}

/// Assemble the stored row from the harness's phases.
pub(super) fn record(
    run: &RowRun,
    phases: Vec<PhaseResult>,
    meta: Meta,
) -> RowRecord {
    let mut notes = meta.notes;
    notes.push(HARNESS_NOTE.to_string());

    RowRecord {
        key: run.key.clone(),
        timestamp: run.timestamp.clone(),
        dirty: run.dirty,
        caged: run.caged,
        forced: run.forced,
        bracket: meta.bracket.into(),
        compression: meta.compression.into(),
        retains_history: meta.retains_history,
        settings: meta.settings,
        phases: phases.into_iter().map(phase_record).collect(),
        footprints: meta.footprints,
        live_records: run.cfg.rows,
        logical_bytes: logical_bytes(run.cfg.rows, run.cfg.seed),
        notes,
        seed_path: meta.seed_path,
        materialise_ms: meta.materialise_ms,
    }
}

fn phase_record(mut p: PhaseResult) -> PhaseRecord {
    let (p50_ns, p95_ns, p99_ns, max_ns) = percentiles(&mut p.samples);
    PhaseRecord {
        name: p.name,
        count: p.samples.len() as u64,
        wall_ns: p.wall_ns,
        // The summed windows, kept beside the wall clock rather than instead
        // of it: their ratio is what says how much concurrency the row got.
        total_ns: p.samples.iter().sum(),
        p50_ns,
        p95_ns,
        p99_ns,
        max_ns,
        bytes_written: p.bytes_written,
        read_bytes: 0,
        rchar: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{HARNESS_NOTE, phase_record, workload};
    use crate::harness::{PhaseResult, Workload};
    use crate::systems::Cfg;

    fn cfg() -> Cfg {
        Cfg {
            rows: 100,
            reads: 50,
            updates: 25,
            seed: 42,
            work_dir: std::path::PathBuf::from("/nonexistent"),
            seed_wavedb: None,
            seed_sqlite: None,
            seed_postgres: None,
            seed_mysql: None,
            seed_mongodb: None,
        }
    }

    /// The wall clock is the phase's, and the summed windows are kept beside
    /// it rather than in its place — that substitution is exactly what the
    /// phase-1 bridge had to do, and what this path exists to stop.
    #[test]
    fn a_phase_keeps_both_clocks() {
        let rec = phase_record(PhaseResult {
            name: "insert".into(),
            samples: vec![10, 20, 30, 40],
            wall_ns: 1_000,
            bytes_written: 4096,
        });
        assert_eq!(rec.wall_ns, 1_000);
        assert_eq!(rec.total_ns, 100);
        assert_eq!(rec.count, 4);
        assert_eq!(rec.bytes_written, 4096);
    }

    /// Percentiles come from the pooled samples, sorted here rather than by
    /// each consumer: a percentile of per-thread percentiles is not a
    /// percentile of anything.
    #[test]
    fn percentiles_come_from_the_pooled_samples() {
        let rec = phase_record(PhaseResult {
            name: "read_hot".into(),
            samples: vec![90, 10, 50, 99, 1],
            wall_ns: 500,
            bytes_written: 0,
        });
        assert!(rec.p50_ns <= rec.p95_ns);
        assert!(rec.p95_ns <= rec.p99_ns);
        assert_eq!(rec.max_ns, 99);
    }

    /// An empty phase is a zero, not a division.
    #[test]
    fn an_empty_phase_records_zeroes() {
        let rec = phase_record(PhaseResult {
            name: "update".into(),
            samples: Vec::new(),
            wall_ns: 0,
            bytes_written: 0,
        });
        assert_eq!((rec.count, rec.total_ns, rec.max_ns), (0, 0, 0));
        assert!((rec.throughput() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_seeded_row_asks_for_the_seeded_phase_list() {
        assert_eq!(workload(&cfg(), true).phases(), ["read_cold", "update"]);
        assert_eq!(workload(&cfg(), false).phases().len(), 4);
    }

    /// The note is the corpus's only record that read counters are absent by
    /// design rather than by accident.
    #[test]
    fn the_harness_note_names_what_is_missing() {
        assert!(HARNESS_NOTE.contains("wall clock"));
        assert!(HARNESS_NOTE.contains("Read counters are absent"));
    }
}
