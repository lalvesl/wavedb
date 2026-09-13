//! The supervisor's two jobs: knowing what versions are on the machine, and
//! running each row in its own cage ([RFC 0065] §2).
//!
//! It runs **uncaged**, deliberately. It builds nothing and measures nothing,
//! and charging a row's budget for the orchestration around it would put work
//! inside the measurement that is not the measurement.
//!
//! [RFC 0065]: ../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use crate::cage::{CageSpec, Outcome, exec_row};
use crate::corpus::RowKey;

/// How each system reports its version, and what to ask.
///
/// `sqlite` is absent because `rusqlite::version()` answers in-process — it
/// links the library, so spawning `sqlite3` would ask a *different* binary
/// than the one being measured.
const PROBES: [(&str, &str); 3] = [
    ("postgres", "postgres"),
    ("mysql", "mysqld"),
    ("mongodb", "mongod"),
];

/// A version, or why the system cannot be measured here.
pub enum Probe {
    Version(String),
    /// The binary is not on `PATH`. Its rows are skipped — and skipping four
    /// rows is the phase-1 improvement over the pass that used to die with
    /// `spawn mongod: No such file or directory` after forty minutes of work.
    Missing,
}

/// Ask every system its version.
///
/// The **binary** is asked, not a running server: that is milliseconds and no
/// server start, and it is what keeps peer reuse correct — `flake.lock` pins
/// the package, so a bump changes the string, which changes the digest, which
/// retires every row measured against the old one.
#[must_use]
pub fn probe_versions() -> BTreeMap<String, Probe> {
    let mut out = BTreeMap::new();
    out.insert(
        "wavedb".to_string(),
        Probe::Version(env!("CARGO_PKG_VERSION").to_string()),
    );
    out.insert(
        "sqlite".to_string(),
        Probe::Version(rusqlite::version().to_string()),
    );
    for (system, binary) in PROBES {
        out.insert(system.to_string(), probe_one(binary));
    }
    out
}

fn probe_one(binary: &str) -> Probe {
    let Ok(out) = Command::new(binary).arg("--version").output() else {
        return Probe::Missing;
    };
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text.lines().next().unwrap_or_default();
    version_from(line).map_or(Probe::Missing, Probe::Version)
}

/// The first dotted number in a `--version` line.
///
/// The three servers announce themselves in three shapes —
/// `postgres (PostgreSQL) 18.1`, `mysqld  Ver 8.4.0 for Linux on x86_64`,
/// `db version v8.0.4` — and the whole line is not usable as an identity
/// because `mysqld` prints its own **store path**, which changes on rebuilds
/// that do not change the version.
///
/// A mis-parse here is not cosmetic: the version is in the digest, so it would
/// silently retire or resurrect every peer row. Hence the tests.
#[must_use]
pub fn version_from(line: &str) -> Option<String> {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        let start = i;
        let mut dots = 0;
        while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.')
        {
            if bytes[i] == b'.' {
                // A trailing dot ends the number rather than joining a
                // sentence to it.
                if i + 1 >= bytes.len() || !bytes[i + 1].is_ascii_digit() {
                    break;
                }
                dots += 1;
            }
            i += 1;
        }
        if dots >= 1 {
            return line.get(start..i).map(str::to_string);
        }
    }
    None
}

/// What running one row came to.
pub struct RowOutcome {
    pub key: RowKey,
    pub outcome: Result<Outcome, String>,
}

impl RowOutcome {
    #[must_use]
    pub fn is_ok(&self) -> bool {
        matches!(self.outcome, Ok(Outcome::Ok))
    }

    /// One line for the run's summary.
    #[must_use]
    pub fn line(&self) -> String {
        let k = &self.key;
        let what = match &self.outcome {
            Ok(Outcome::Ok) => "ok".to_string(),
            Ok(Outcome::OutOfMemory) => "OOM-killed by the cage".to_string(),
            Ok(Outcome::Failed(code)) => format!("exit {code}"),
            Ok(Outcome::Signalled(sig)) => format!("signal {sig}"),
            Err(e) => format!("could not run: {e}"),
        };
        format!(
            "{}/{} {} {} — {what}",
            k.system, k.variant, k.durability, k.workload
        )
    }
}

/// Run every row in `rows`, each in its own cage, and keep going past a
/// failure.
///
/// **A row fails alone.** That is the whole reason for the split: RFC 0060
/// lost a fifty-minute pass to one `mysqld` startup timeout, and the fix is
/// not a longer timeout but a smaller blast radius.
pub fn run_rows(
    spec: &CageSpec,
    program: &Path,
    rows: &[RowKey],
    common: &[String],
) -> Vec<RowOutcome> {
    let mut out = Vec::with_capacity(rows.len());
    for (n, key) in rows.iter().enumerate() {
        eprintln!("[{}/{}] {}", n + 1, rows.len(), describe(key));
        let mut args = row_args(key);
        args.extend(common.iter().cloned());
        let outcome = exec_row(spec, &key.digest(), program, &args);
        let result = RowOutcome {
            key: key.clone(),
            outcome,
        };
        if !result.is_ok() {
            eprintln!("  ! {}", result.line());
        }
        out.push(result);
    }
    out
}

fn describe(key: &RowKey) -> String {
    format!(
        "{}/{} {} · {} · {} consumers",
        key.system, key.variant, key.durability, key.workload, key.consumers
    )
}

/// The identity, as flags. Every field goes across rather than being
/// re-derived in the child: the supervisor already computed the digest to
/// decide this row needed measuring, and a child that disagreed would file its
/// result under a name nobody looks for.
#[must_use]
pub fn row_args(key: &RowKey) -> Vec<String> {
    let mut args = vec![
        "--system".into(),
        key.system.clone(),
        "--system-version".into(),
        key.system_version.clone(),
        "--variant".into(),
        key.variant.clone(),
        "--durability".into(),
        key.durability.clone(),
        "--workload".into(),
        key.workload.clone(),
        "--tier".into(),
        key.tier.clone(),
        "--host-key".into(),
        key.host_key.clone(),
        "--consumers".into(),
        key.consumers.to_string(),
        "--dataset-revision".into(),
        key.dataset_revision.to_string(),
        "--cage-revision".into(),
        key.cage_revision.to_string(),
        "--seed".into(),
        key.generator_seed.to_string(),
    ];
    if let Some(sha) = &key.wavedb_git_sha {
        args.push("--wavedb-sha".into());
        args.push(sha.clone());
    }
    args
}

#[cfg(test)]
mod tests {
    use super::{Probe, probe_versions, row_args, version_from};
    use crate::corpus::RowKey;

    fn key() -> RowKey {
        RowKey {
            system: "wavedb".into(),
            system_version: "0.1.0".into(),
            variant: "multi".into(),
            durability: "durable".into(),
            workload: "shop".into(),
            tier: "large".into(),
            dataset_revision: 1,
            generator_seed: 42,
            consumers: 3,
            host_key: "lane".into(),
            cage_revision: 1,
            wavedb_git_sha: Some("04085ec".into()),
        }
    }

    /// The three shapes the servers actually print. A mis-parse is not
    /// cosmetic — the version is in the digest, so it would retire or
    /// resurrect every peer row on the machine.
    #[test]
    fn the_three_servers_version_lines_parse() {
        assert_eq!(
            version_from("postgres (PostgreSQL) 18.1").as_deref(),
            Some("18.1")
        );
        assert_eq!(
            version_from("mysqld  Ver 8.4.0 for Linux on x86_64 (Source)")
                .as_deref(),
            Some("8.4.0")
        );
        assert_eq!(version_from("db version v8.0.4").as_deref(), Some("8.0.4"));
    }

    /// `mysqld` prints its own store path, whose hash changes on rebuilds
    /// that do not change the version — so the whole line cannot be the
    /// identity, and the parse must not pick a number out of the path.
    #[test]
    fn a_nix_store_path_does_not_become_the_version() {
        let line = "/nix/store/abc123-mysql-8.4.0/bin/mysqld  Ver 8.4.0 for \
                    Linux";
        assert_eq!(version_from(line).as_deref(), Some("8.4.0"));
    }

    #[test]
    fn a_line_with_no_version_is_none() {
        assert_eq!(version_from("mongod: command not found"), None);
        assert_eq!(version_from(""), None);
        // A bare integer is not a version — it would match far too much.
        assert_eq!(version_from("built 2026"), None);
    }

    #[test]
    fn a_trailing_dot_does_not_join_the_sentence() {
        assert_eq!(
            version_from("Ver 8.4. Built later").as_deref(),
            Some("8.4")
        );
    }

    /// The two systems that need no spawn always answer.
    #[test]
    fn the_in_process_systems_always_have_a_version() {
        let v = probe_versions();
        for system in ["wavedb", "sqlite"] {
            assert!(
                matches!(v.get(system), Some(Probe::Version(s)) if !s.is_empty()),
                "{system} has no version"
            );
        }
    }

    /// Every identity field crosses to the child. If one stopped, the child
    /// would recompute a different digest and file the row where nobody looks.
    #[test]
    fn every_identity_field_reaches_the_child() {
        let k = key();
        let args = row_args(&k);
        for (flag, value) in [
            ("--system", k.system.as_str()),
            ("--system-version", k.system_version.as_str()),
            ("--variant", k.variant.as_str()),
            ("--durability", k.durability.as_str()),
            ("--workload", k.workload.as_str()),
            ("--tier", k.tier.as_str()),
            ("--host-key", k.host_key.as_str()),
            ("--consumers", "3"),
            ("--dataset-revision", "1"),
            ("--cage-revision", "1"),
            ("--seed", "42"),
            ("--wavedb-sha", "04085ec"),
        ] {
            let i = args
                .iter()
                .position(|a| a == flag)
                .unwrap_or_else(|| panic!("{flag} missing from {args:?}"));
            assert_eq!(args[i + 1], value, "{flag}");
        }
    }

    /// A peer row carries no SHA, and must not acquire an empty one — the
    /// child spells `None` by the flag's absence.
    #[test]
    fn a_peer_row_sends_no_sha_flag() {
        let mut k = key();
        k.system = "postgres".into();
        k.wavedb_git_sha = None;
        assert!(!row_args(&k).iter().any(|a| a == "--wavedb-sha"));
    }
}
