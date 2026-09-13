//! What remains of RFC 0060: converting an adapter's [`SystemReport`] into a
//! stored row.
//!
//! The adapters this serves own their own loops, their own timing and their
//! own phase boundaries — which is why they could drift from each other, and
//! why [RFC 0065] replaces them. Until every workload has a driver, rows still
//! measured this way carry [`PHASE1_NOTE`], so the corpus **says** what its
//! throughput denominator was rather than leaving it to be inferred: at one
//! consumer the two denominators differ only by the untimed gaps, so the
//! number is exactly what the old corpus recorded, and saying so is what stops
//! it being mistaken for a wall-clock throughput later.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use super::{RowRun, point_name};
use crate::corpus::{FootprintRecord, PhaseRecord, RowRecord};
use crate::systems::SystemReport;

/// The note every phase-1 row carries.
pub const PHASE1_NOTE: &str = "Measured with the RFC 0060 adapters at one consumer: `wall_ns` is the \
     sum of the timed windows, not the phase's wall clock, and read counters \
     are absent. Both arrive with the producer/consumer harness (RFC 0065 \
     phases 2 and 4).";

/// Convert an adapter's report into a stored row.
#[must_use]
pub fn bridge(run: &RowRun, report: SystemReport) -> RowRecord {
    let mut notes = report.notes;
    notes.push(PHASE1_NOTE.to_string());

    RowRecord {
        key: run.key.clone(),
        timestamp: run.timestamp.clone(),
        dirty: run.dirty,
        caged: run.caged,
        forced: run.forced,
        bracket: report.bracket.to_string(),
        compression: report.compression.to_string(),
        retains_history: report.retains_history,
        settings: report.settings,
        phases: report
            .phases
            .iter()
            .map(|p| PhaseRecord {
                name: p.name.to_string(),
                count: p.dist.count,
                // See `PHASE1_NOTE`: identical to what RFC 0060 recorded, so
                // a phase-1 row and an old run agree exactly.
                wall_ns: p.dist.total_ns,
                total_ns: p.dist.total_ns,
                p50_ns: p.dist.p50_ns,
                p95_ns: p.dist.p95_ns,
                p99_ns: p.dist.p99_ns,
                max_ns: p.dist.max_ns,
                bytes_written: p.bytes_written,
                read_bytes: 0,
                rchar: 0,
            })
            .collect(),
        footprints: report
            .footprints
            .iter()
            .map(|(point, f)| {
                (
                    point_name(*point).to_string(),
                    FootprintRecord {
                        apparent_bytes: f.apparent_bytes,
                        allocated_bytes: f.allocated_bytes,
                        log_bytes: f.log_bytes,
                        files: f.files,
                    },
                )
            })
            .collect(),
        live_records: report.live_records,
        logical_bytes: report.logical_bytes,
        notes,
        seed_path: report.seed_path,
        materialise_ms: report.materialise_ms,
    }
}
