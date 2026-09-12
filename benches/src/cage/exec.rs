//! Running one row inside its own cage.
//!
//! ## Three tools, and only two of them cap anything
//!
//! - **`systemd-run --scope`** — the cgroup, and the only one of the three
//!   that can bound memory at all. It bounds the **page cache** as well as
//!   anonymous memory, which is the point: without it a dataset simply lives
//!   in RAM and a "cold" read is a memory read.
//! - **`taskset`** — the CPU budget. `AllowedCPUs` on the scope would be
//!   tidier, but `cpuset` is not among the controllers delegated to a user
//!   scope here, so affinity it is — and affinity is what `nproc` reports, so
//!   a server sizes its thread pools from the budget rather than the host.
//! - **`bwrap`** — the namespace, and **nothing else**. Bubblewrap caps no
//!   resource whatsoever; it is here for a private PID namespace, so a killed
//!   row cannot leave a `mongod` behind, and for one uniform filesystem shape.
//!   `--dev-bind / /` is deliberate: the databases must write to the real
//!   disk, and a tmpfs would put them in RAM and measure the wrong thing.
//!
//! ## Why the command line is built apart from being run
//!
//! [`command_line`] is a pure function and [`exec_row`] is the thing with an
//! effect. The cage is the load-bearing part of every recorded number, so what
//! it actually asks the kernel for should be assertable in a unit test on a
//! machine with no `systemd` on it.

use std::path::{Path, PathBuf};
use std::process::Command;

use super::verify::{BUDGET_ENV, CPUS_ENV};

/// Also exported to the child, so a `bench-row` can name the mask it was
/// pinned to without re-deriving it.
pub const MASK_ENV: &str = "BENCH_CPUS";

/// The one measured configuration.
///
/// The defaults are the recorded ones and are duplicated in `benches/nix`, on
/// purpose: a run launched through the flake gets them from the environment,
/// and a `cargo run` gets the same numbers rather than a different, silently
/// uncaged shape. Which of the two happened is settled by
/// [`verify`](super::verify), not here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CageSpec {
    /// `memory.max`, in bytes, as a string because that is how it is written.
    pub mem_max: String,
    /// The `taskset` mask, e.g. `0-3`.
    pub cpus: String,
    /// How many CPUs `cpus` names — the number the guard compares against.
    pub cpu_budget: u32,
}

impl Default for CageSpec {
    fn default() -> Self {
        Self {
            mem_max: "524288000".into(), // 500 MB
            cpus: "0-3".into(),
            cpu_budget: 4,
        }
    }
}

impl CageSpec {
    /// The budgets the wrapper declared, falling back to the defaults.
    #[must_use]
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            mem_max: std::env::var(BUDGET_ENV)
                .map_or(d.mem_max, |v| v.trim().to_string()),
            cpus: std::env::var(MASK_ENV)
                .map_or(d.cpus, |v| v.trim().to_string()),
            cpu_budget: std::env::var(CPUS_ENV)
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(d.cpu_budget),
        }
    }
}

/// How a caged row ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The row ran and wrote its own record.
    Ok,
    /// The row exited non-zero. Its own stderr already said why — this
    /// carries the code so the supervisor can report which row, not what.
    Failed(i32),
    /// Killed by the cgroup for exceeding the memory budget.
    ///
    /// Called out rather than folded into [`Failed`] because under a 500 MB
    /// cage it is not an anomaly, it is the expected way a row fails: a server
    /// whose cache is not pinned asks for the machine's RAM and gets none of
    /// it. Reporting "exit 137" would send the reader hunting for a bug in a
    /// row that behaved exactly as the design predicts.
    OutOfMemory,
    /// Terminated by some other signal.
    Signalled(i32),
}

/// The full argv for running `program args…` inside one row's cage.
///
/// Ends with the program, so a reader can see the whole chain in one line:
/// scope, then affinity, then namespace, then the row.
#[must_use]
pub fn command_line(
    spec: &CageSpec,
    unit: &str,
    program: &Path,
    args: &[String],
) -> Vec<String> {
    let mut argv = vec![
        "systemd-run".into(),
        "--user".into(),
        "--scope".into(),
        "-q".into(),
        // A named unit so an interrupted row is findable in
        // `systemctl --user`. Scopes are transient and vanish with the
        // process, so the name is for humans, not for lifecycle.
        format!("--unit={unit}"),
        // The long form: `systemd-run` rejects `-p=NAME=VALUE` ("Unknown
        // assignment"), taking either `-p NAME=VALUE` as two argv entries or
        // `--property=NAME=VALUE` as one. One entry is what `Command::args`
        // wants, so the long form it is.
        format!("--property=MemoryMax={}", spec.mem_max),
        // No swap. A row that swaps is measuring the swap device, and the
        // whole point of bounding memory is that the page cache cannot hide
        // the disk.
        "--property=MemorySwapMax=0".into(),
        "--property=Delegate=yes".into(),
        "--".into(),
        "taskset".into(),
        "-c".into(),
        spec.cpus.clone(),
        "bwrap".into(),
        "--dev-bind".into(),
        "/".into(),
        "/".into(),
        "--unshare-pid".into(),
        "--proc".into(),
        "/proc".into(),
        "--".into(),
        program.display().to_string(),
    ];
    argv.extend(args.iter().cloned());
    argv
}

/// A unit name for one row, unique enough that two passes cannot collide.
#[must_use]
pub fn unit_name(digest: &str) -> String {
    format!("wavedb-bench-{digest}-{}", std::process::id())
}

/// Run `program args…` in a fresh cage and wait for it.
///
/// Stdio is inherited: a row prints its own progress, and twenty-two rows
/// buffered until the end would turn a fifty-minute pass into a blind one.
///
/// # Errors
/// The cage could not be started at all — `systemd-run`, `taskset` or `bwrap`
/// missing, or the user scope refused. That is a fault in the harness rather
/// than in the row, and is reported as such.
pub fn exec_row(
    spec: &CageSpec,
    digest: &str,
    program: &Path,
    args: &[String],
) -> Result<Outcome, String> {
    let argv = command_line(spec, &unit_name(digest), program, args);
    let (head, rest) = argv.split_first().ok_or("empty command line")?;

    let status = Command::new(head)
        .args(rest)
        // The child verifies these against what it actually observes, so
        // passing them is how the guard has anything to compare with.
        .env(BUDGET_ENV, &spec.mem_max)
        .env(CPUS_ENV, spec.cpu_budget.to_string())
        .env(MASK_ENV, &spec.cpus)
        .status()
        .map_err(|e| format!("start the cage ({head}): {e}"))?;

    Ok(outcome_of(status.code(), signal_of(&status)))
}

/// Classify an exit.
///
/// Split out because the interesting case — telling an OOM kill apart from an
/// ordinary failure — is pure logic that deserves a test, and spawning a
/// process that gets OOM-killed on purpose is not a unit test.
#[must_use]
pub const fn outcome_of(code: Option<i32>, signal: Option<i32>) -> Outcome {
    // A signal is checked BEFORE the exit code, and the order is the
    // decision: a process that was killed did not succeed, whatever code
    // accompanies it. On Unix `code()` is `None` when a process is
    // signalled, so the pair cannot arise today — but the arm that would
    // read a killed row as `Ok` should not be sitting here waiting for a
    // platform where it can.
    match (code, signal) {
        // SIGKILL, which under this cgroup means the memory bound. A row
        // killed by anything else says so.
        (_, Some(9)) => Outcome::OutOfMemory,
        (_, Some(sig)) => Outcome::Signalled(sig),
        (Some(0), None) => Outcome::Ok,
        (Some(code), None) => Outcome::Failed(code),
        (None, None) => Outcome::Failed(-1),
    }
}

#[cfg(unix)]
fn signal_of(status: &std::process::ExitStatus) -> Option<i32> {
    std::os::unix::process::ExitStatusExt::signal(status)
}

#[cfg(not(unix))]
const fn signal_of(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

/// Where `bench-row` lives, beside the binary that is running.
///
/// Derived rather than searched: the supervisor and the row are built from one
/// `cargo build`, so they are siblings, and finding the row on `PATH` could
/// pick up a stale install built from other sources.
#[must_use]
pub fn sibling_binary(name: &str) -> Option<PathBuf> {
    let me = std::env::current_exe().ok()?;
    Some(me.parent()?.join(name))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{CageSpec, Outcome, command_line, outcome_of, unit_name};

    fn spec() -> CageSpec {
        CageSpec::default()
    }

    fn argv() -> Vec<String> {
        command_line(
            &spec(),
            "wavedb-bench-abc-1",
            Path::new("/build/bench-row"),
            &["--system".into(), "postgres".into()],
        )
    }

    /// The whole chain, in order, in one assertion — because the order *is*
    /// the design: the scope must wrap the affinity must wrap the namespace.
    #[test]
    fn the_cage_wraps_the_row_in_the_documented_order() {
        let a = argv();
        let at = |needle: &str| {
            a.iter()
                .position(|s| s == needle)
                .unwrap_or_else(|| panic!("{needle} missing from {a:?}"))
        };
        assert!(at("systemd-run") < at("taskset"));
        assert!(at("taskset") < at("bwrap"));
        assert!(at("bwrap") < at("/build/bench-row"));
    }

    #[test]
    fn the_memory_bound_and_the_swap_refusal_are_both_set() {
        let a = argv();
        assert!(
            a.contains(&"--property=MemoryMax=524288000".to_string()),
            "{a:?}"
        );
        assert!(
            a.contains(&"--property=MemorySwapMax=0".to_string()),
            "{a:?}"
        );
        assert!(a.contains(&"--property=Delegate=yes".to_string()), "{a:?}");
    }

    #[test]
    fn the_cpu_mask_reaches_taskset() {
        let mut s = spec();
        s.cpus = "2-5".into();
        let a = command_line(&s, "u", Path::new("/x"), &[]);
        let i = a.iter().position(|v| v == "taskset").expect("taskset");
        assert_eq!(a[i + 1], "-c");
        assert_eq!(a[i + 2], "2-5");
    }

    /// `--dev-bind / /` is deliberate and load-bearing: a tmpfs root would put
    /// the databases in RAM and measure the wrong thing entirely.
    #[test]
    fn the_namespace_binds_the_real_disk() {
        let a = argv();
        let i = a.iter().position(|v| v == "--dev-bind").expect("dev-bind");
        assert_eq!((a[i + 1].as_str(), a[i + 2].as_str()), ("/", "/"));
        assert!(a.contains(&"--unshare-pid".to_string()));
    }

    #[test]
    fn the_rows_own_arguments_come_last() {
        let a = argv();
        assert_eq!(
            &a[a.len() - 3..],
            ["/build/bench-row", "--system", "postgres"]
        );
    }

    /// Two passes must not collide on a unit name, and the digest must be in
    /// it so an interrupted row is findable.
    #[test]
    fn a_unit_name_carries_the_digest_and_is_unique_to_this_process() {
        let name = unit_name("469a43f95db259c7");
        assert!(name.contains("469a43f95db259c7"), "{name}");
        assert!(name.ends_with(&std::process::id().to_string()), "{name}");
    }

    /// The classification that matters: under a 500 MB cage, SIGKILL is the
    /// *expected* failure, and reporting it as "exit 137" would send a reader
    /// hunting for a bug in a row that behaved as predicted.
    #[test]
    fn a_sigkill_reads_as_out_of_memory_not_as_exit_137() {
        assert_eq!(outcome_of(None, Some(9)), Outcome::OutOfMemory);
        assert_eq!(outcome_of(Some(137), Some(9)), Outcome::OutOfMemory);
    }

    #[test]
    fn other_exits_keep_their_own_shape() {
        assert_eq!(outcome_of(Some(0), None), Outcome::Ok);
        assert_eq!(outcome_of(Some(1), None), Outcome::Failed(1));
        assert_eq!(outcome_of(None, Some(15)), Outcome::Signalled(15));
        assert_eq!(outcome_of(None, None), Outcome::Failed(-1));
    }

    /// A success that arrived by signal is not a success.
    #[test]
    fn a_signal_outranks_a_zero_exit_code() {
        assert_eq!(outcome_of(Some(0), Some(9)), Outcome::OutOfMemory);
    }

    #[test]
    fn the_default_spec_is_the_recorded_configuration() {
        let s = CageSpec::default();
        assert_eq!(s.mem_max, "524288000");
        assert_eq!(s.cpu_budget, 4);
        assert_eq!(s.cpus, "0-3");
    }
}
