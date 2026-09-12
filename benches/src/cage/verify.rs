//! The run's cage: 500 MB and four CPUs, from first instruction to last.
//!
//! The budget (RFC 0060 §5) is what makes the **measurement** honest: it
//! bounds the page cache, so a cold read is a disk read and a dataset larger
//! than memory is larger than memory. It is also what makes the *comparison*
//! honest, which is the stronger reason — PostgreSQL, MySQL and MongoDB each
//! size their caches from the machine's RAM by default, so an uncaged run does
//! not compare five systems on one machine, it compares five different
//! opinions about how much of the machine to take.
//!
//! So there is **one** measured configuration and no exceptions to it:
//!
//! - the scope is created at the budget by `nix run .#bench` — not created
//!   loose and tightened, which left a window at startup and an escape hatch
//!   (`BENCH_FILL_MEM`) for fills;
//! - fills run inside it too. That is not a concession: giving the fill the
//!   machine measured **worse**, not better — 8 000 shop users took 27 s
//!   filled at 500 MB and 483 s filled at 10 GB on an idle machine. A fill is
//!   write-bound, and extra RAM only lets dirty pages pile up until something
//!   forces the kernel to write them all back at once;
//! - [`verify`] refuses to *record* outside it, so an uncaged number cannot
//!   quietly enter the corpus under its own host lane.
//!
//! [`init`] still writes `memory.max` rather than trusting the wrapper: it is
//! idempotent when the scope was already created tight, and it is the whole
//! guarantee when it was not.
//!
//! Everything here is inert outside the flake's cage — no `BENCH_MEM_MAX`, no
//! cgroup v2, or a file we may not write — so `cargo run` still works. It just
//! cannot record.

use std::path::PathBuf;

/// Set by `nix run .#bench` to the measurement budget, in bytes.
const BUDGET_ENV: &str = "BENCH_MEM_MAX";

/// Set by `nix run .#bench` to the number of CPUs `taskset` pins the run to.
const CPUS_ENV: &str = "BENCH_CPU_BUDGET";

/// Hold the cgroup at the declared budget.
///
/// Call once, before anything is probed: [`crate::host`] reads `memory.max` to
/// key the lane, and the lane must be named for the budget the *numbers* ran
/// under.
pub fn init() {
    let (Some(path), Ok(budget)) = (memory_max(), std::env::var(BUDGET_ENV))
    else {
        return;
    };
    // Lowering below current usage makes the kernel reclaim rather than fail.
    let _ = std::fs::write(path, budget.trim());
}

/// What the wrapper declares it is giving this run.
fn declared() -> Option<(u64, u64)> {
    let mem = std::env::var(BUDGET_ENV).ok()?.trim().parse().ok()?;
    let cpus = std::env::var(CPUS_ENV).ok()?.trim().parse().ok()?;
    Some((mem, cpus))
}

/// Is this run inside the declared cage, with the budgets it actually asked
/// for?
#[must_use]
pub fn is_caged(mem_budget: u64, cpu_budget: u64) -> bool {
    declared() == Some((mem_budget, cpu_budget))
}

/// Why this run may not be recorded — `Ok` when it may.
///
/// The arguments are what the process *observes* (`Cpus_allowed_list` and the
/// cgroup's `memory.max`, via [`crate::host`]), deliberately not what the
/// environment claims: an environment variable is a statement of intent, and
/// the guard exists for the case where intent and reality parted company.
pub fn verify(mem_budget: u64, cpu_budget: u64) -> Result<(), String> {
    let mb = |b: u64| b / (1 << 20);
    match declared() {
        Some((mem, cpus)) if (mem, cpus) == (mem_budget, cpu_budget) => Ok(()),
        Some((mem, cpus)) => Err(format!(
            "the cage declares {} MB and {cpus} cpus, but this process has {} \
             MB and {cpu_budget} — something between the wrapper and here \
             changed the budget, and the numbers belong to neither \
             configuration (--force to override)",
            mb(mem),
            mb(mem_budget)
        )),
        None => Err(format!(
            "not running inside the benchmark cage: this process sees {} MB \
             and {cpu_budget} cpus, and every recorded row is measured at 500 \
             MB and 4 cpus. An uncaged run is a different machine — it would \
             record into its own host lane and never be comparable with the \
             corpus. Run it through `scripts/bench.sh` (--quick to \
             smoke-test without recording, --force to record anyway)",
            mb(mem_budget)
        )),
    }
}

/// This process's cgroup v2 `memory.max`, or `None` on v1 / no cgroup.
fn memory_max() -> Option<PathBuf> {
    let rel = std::fs::read_to_string("/proc/self/cgroup")
        .ok()?
        .lines()
        .find_map(|l| l.strip_prefix("0::").map(str::to_string))?;
    Some(PathBuf::from(format!("/sys/fs/cgroup{rel}/memory.max")))
}
