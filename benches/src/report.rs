//! Provenance — who and when, for a row.
//!
//! What is left of RFC 0060.s results writer. The record itself moved to
//! [`crate::corpus`], where it is one file per **row** rather than one per
//! run ([RFC 0065] §1); the per-run JSON, its `index.md` line and the tables
//! that rendered them went with it. This kept the part that is still true of
//! any measurement whenever it happens: the commit it was taken at, whether
//! the tree was dirty, and the stamp it is filed under.
//!
//! [RFC 0065]: ../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::path::Path;

use crate::json::fnv1a;

/// A system that did not produce its row.
///
/// Recorded rather than dropped: a corpus row that looks complete and quietly
/// lacks a system is worse than one that says which system is missing and why.
pub struct Skipped {
    pub name: String,
    pub reason: String,
}

pub struct Provenance {
    pub git_sha: String,
    pub dirty: bool,
    pub flake_lock: String,
    pub timestamp: String,
    pub load_average: f64,
    /// Did this run measure inside the declared 500 MB / 4 CPU cage?
    ///
    /// Recorded rather than assumed, and only ever `false` beside `forced`:
    /// the guard refuses to record otherwise. A reader should not have to
    /// re-derive it from the budgets to know whether a row is standard.
    pub caged: bool,
    /// Was a guard overridden with `--force`?
    ///
    /// Without this a forced row is indistinguishable from a clean one on a
    /// casual read — the load average is recorded, but nobody recomputes the
    /// budget from the CPU count to check it.
    pub forced: bool,
}

impl Provenance {
    pub fn probe(repo: &Path) -> Self {
        let git_sha = git(repo, &["rev-parse", "--short=7", "HEAD"])
            .unwrap_or_else(|| "unknown".into());
        let dirty = git(repo, &["status", "--porcelain"])
            .is_some_and(|s| !s.trim().is_empty());
        let flake_lock = std::fs::read(repo.join("flake.lock")).map_or_else(
            |_| "absent".into(),
            |b| format!("{:016x}", fnv1a(&b)),
        );
        Self {
            git_sha,
            dirty,
            flake_lock,
            timestamp: utc_stamp(),
            load_average: crate::host::load_average(),
            // Both are the runner's to answer: it holds the flags and has
            // fingerprinted the host by the time it can tell.
            caged: false,
            forced: false,
        }
    }
}

fn git(repo: &Path, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// `YYYY-MM-DDTHH-MMZ`, computed from the epoch so the record needs no clock
/// crate. Colons are avoided: the stamp is a filename.
fn utc_stamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (hour, min) = (rem / 3600, (rem % 3600) / 60);
    let (y, m, d) = civil_from_days(days as i64);
    format!("{y:04}-{m:02}-{d:02}T{hour:02}-{min:02}Z")
}

/// Howard Hinnant's `civil_from_days`, the standard days-to-date algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}
