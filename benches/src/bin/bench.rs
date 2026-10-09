//! `bench` — the supervisor.
//!
//! It resolves which rows this pass needs, runs each one in its own cage, and
//! reports. It measures nothing itself and runs **uncaged**: charging a row's
//! 500 MB for the orchestration around it would put work inside the
//! measurement that is not the measurement ([RFC 0065] §2).
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::collections::BTreeMap;
use std::path::PathBuf;

use wavedb_bench::cage::{CageSpec, sibling_binary};
use wavedb_bench::corpus::Corpus;
use wavedb_bench::plan::resolve::{Refresh, Request, resolve};
use wavedb_bench::supervise::{Probe, probe_versions, run_rows};
use wavedb_bench::{absolute, guard, host, report};

fn main() {
    if let Err(e) = run() {
        eprintln!("bench: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let opts = Opts::parse(std::env::args().skip(1))?;
    std::fs::create_dir_all(&opts.work_dir)
        .map_err(|e| format!("work dir: {e}"))?;

    // Keyed at the CAGE's budgets, not this process's: the supervisor runs
    // uncaged and would otherwise file 4-cpu / 500 MB rows under a lane named
    // for the whole machine.
    let spec = CageSpec::from_env();
    let host = host::Host::probe_with_budget(
        &opts.work_dir,
        spec.mem_max.parse().unwrap_or(0),
        u64::from(spec.cpu_budget),
    );
    let prov = report::Provenance::probe(&opts.repo);

    // Versions first: a missing binary skips that system's rows instead of
    // killing the pass forty minutes in, which is what RFC 0060 did.
    let probes = probe_versions();
    let (versions, missing) = split(&probes);
    for system in &missing {
        eprintln!("skipping {system}: binary not on PATH");
    }

    let req = Request {
        systems: opts
            .systems
            .iter()
            .filter(|s| !missing.contains(s))
            .cloned()
            .collect(),
        workloads: opts.workloads.clone(),
        tier: opts.tier.clone(),
        consumers: opts.consumers,
        refresh: opts.refresh.clone(),
        host_key: host.key.clone(),
        cage_revision: opts.cage_revision,
        dataset_revision: opts.dataset_revision,
        generator_seed: opts.seed,
        wavedb_git_sha: prov.git_sha.clone(),
        versions,
        seeded_systems: seeded_systems(&opts.seeds),
    };
    // Both guards, before any row is spawned: a refusal here costs nothing,
    // where the same refusal at the end of a pass costs the pass.
    if !opts.force {
        guard::check_space(&host)?;
        guard::check_load(prov.load_average, host.cpu_budget)?;
    }

    let corpus = Corpus::at(&opts.results);
    let mut plan = resolve(&req, &corpus)?;
    // A system whose binary is gone cannot be measured even if the filter let
    // it through; drop those rows rather than spawning a cage to fail in.
    plan.measure.retain(|k| !missing.contains(&k.system));

    for bad in &plan.broken {
        eprintln!("unreadable row {}: {}", bad.path.display(), bad.why);
    }
    eprintln!(
        "{} · {} · tier {} · load {:.2} · {} to measure, {} reused",
        host.key,
        prov.git_sha,
        opts.tier,
        prov.load_average,
        plan.measure.len(),
        plan.reused()
    );
    eprintln!("{}", seed_line(&opts.seeds));
    if opts.dry_run {
        for key in &plan.measure {
            println!(
                "{}/{} {} {} {}",
                key.system,
                key.variant,
                key.durability,
                key.workload,
                key.digest()
            );
        }
        return Ok(());
    }

    let program = sibling_binary("bench-row")
        .ok_or("cannot locate bench-row beside this binary")?;
    let outcomes =
        run_rows(&spec, &program, &plan.measure, &opts.child_args(&prov));

    let failed = outcomes.iter().filter(|o| !o.is_ok()).count();
    eprintln!(
        "\n{} measured, {} failed, {} reused",
        outcomes.len() - failed,
        failed,
        plan.reused()
    );
    for o in outcomes.iter().filter(|o| !o.is_ok()) {
        eprintln!("  {}", o.line());
    }
    // A failed row is a fact about the pass, not about the supervisor: the
    // rows that did run are stored and usable, so this exits non-zero to say
    // so without pretending nothing was produced.
    if failed > 0 {
        return Err(format!("{failed} row(s) did not complete"));
    }
    Ok(())
}

fn split(
    probes: &BTreeMap<String, Probe>,
) -> (BTreeMap<String, String>, Vec<String>) {
    let mut versions = BTreeMap::new();
    let mut missing = Vec::new();
    for (system, probe) in probes {
        match probe {
            Probe::Version(v) => {
                versions.insert(system.clone(), v.clone());
            }
            Probe::Missing => missing.push(system.clone()),
        }
    }
    (versions, missing)
}

struct Opts {
    tier: String,
    systems: Vec<String>,
    workloads: Vec<String>,
    consumers: u32,
    refresh: Refresh,
    cage_revision: u32,
    dataset_revision: u64,
    seed: u64,
    repo: PathBuf,
    results: PathBuf,
    work_dir: PathBuf,
    dry_run: bool,
    force: bool,
    sizes: Vec<String>,
    /// `--seed-<system> PATH` pairs for every row; see [`seed_args`].
    seeds: Vec<String>,
}

impl Opts {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut o = Self {
            tier: "small".into(),
            systems: Vec::new(),
            workloads: Vec::new(),
            consumers: 3,
            refresh: Refresh::None,
            cage_revision: 1,
            dataset_revision: 1,
            seed: 42,
            repo: PathBuf::from("."),
            results: PathBuf::from("benches/results"),
            work_dir: std::env::temp_dir().join("wavedb-bench"),
            dry_run: false,
            force: false,
            sizes: Vec::new(),
            seeds: seed_args(|name| std::env::var(name).ok()),
        };
        let mut it = args.peekable();
        while let Some(arg) = it.next() {
            let mut text =
                || it.next().ok_or_else(|| format!("{arg} needs a value"));
            match arg.as_str() {
                "--tier" => o.tier = text()?,
                "--only" => o.systems = list(&text()?),
                "--workload" => o.workloads = list(&text()?),
                "--consumers" => {
                    o.consumers = text()?
                        .parse()
                        .map_err(|e| format!("--consumers: {e}"))?;
                }
                "--refresh" => o.refresh = Refresh::Systems(list(&text()?)),
                "--refresh-all" => o.refresh = Refresh::All,
                "--cage-revision" => {
                    o.cage_revision = text()?
                        .parse()
                        .map_err(|e| format!("--cage-revision: {e}"))?;
                }
                "--dataset-revision" => {
                    o.dataset_revision = text()?
                        .parse()
                        .map_err(|e| format!("--dataset-revision: {e}"))?;
                }
                "--seed" => {
                    o.seed =
                        text()?.parse().map_err(|e| format!("--seed: {e}"))?;
                }
                "--repo" => o.repo = PathBuf::from(text()?),
                "--results" => o.results = PathBuf::from(text()?),
                "--work-dir" => o.work_dir = absolute(&text()?)?,
                "--dry-run" => o.dry_run = true,
                "--force" => o.force = true,
                // Workload sizes are passed straight through to every row
                // rather than re-declared here: the supervisor has no opinion
                // about them, and duplicating the list would be one more place
                // for the two binaries to disagree.
                "--rows" | "--reads" | "--updates" | "--users"
                | "--signups" | "--checkouts" | "--profile-reads"
                | "--page-reads" | "--detail-reads" | "--orders-max"
                | "--items-max" => {
                    o.sizes.push(arg.clone());
                    o.sizes.push(text()?);
                }
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                other => return Err(format!("unknown flag {other}")),
            }
        }
        Ok(o)
    }

    /// Flags every child gets, on top of its own identity.
    fn child_args(&self, prov: &report::Provenance) -> Vec<String> {
        let mut args = vec![
            "--timestamp".into(),
            prov.timestamp.clone(),
            "--results".into(),
            self.results.display().to_string(),
            "--work-dir".into(),
            self.work_dir.display().to_string(),
        ];
        if prov.dirty {
            args.push("--dirty".into());
        }
        if self.force {
            args.push("--force".into());
        }
        args.extend(self.sizes.iter().cloned());
        args.extend(self.seeds.iter().cloned());
        args
    }
}

/// Systems whose rows can start from a prebuilt seed.
const SEEDED: [&str; 5] = ["wavedb", "sqlite", "postgres", "mysql", "mongodb"];

/// The seeds the `bench-seeded` app exports (`BENCH_SEED_<SYSTEM>`), as the
/// `--seed-<system>` flags every row is given.
///
/// Read here because the supervisor is what the app runs. Until this existed
/// the app built all five seeds, exported their paths, and nothing read them:
/// every row filled from empty inside its own run and recorded
/// `seed_path: ""`, which is the only place the loss was visible. Each row
/// uses the one seed for its own system and ignores the rest.
fn seed_args(env: impl Fn(&str) -> Option<String>) -> Vec<String> {
    SEEDED
        .iter()
        .filter_map(|system| {
            let path = env(&format!("BENCH_SEED_{}", system.to_uppercase()))?;
            (!path.is_empty()).then(|| [format!("--seed-{system}"), path])
        })
        .flatten()
        .collect()
}

/// The systems named by `--seed-<system>` flags.
fn seeded_systems(seeds: &[String]) -> Vec<String> {
    seeds
        .iter()
        .filter_map(|a| a.strip_prefix("--seed-"))
        .map(str::to_string)
        .collect()
}

/// Which systems' rows start from a seed, said out loud: a row that fills
/// in-run is a valid row, so nothing else would show that the seeds were lost.
fn seed_line(seeds: &[String]) -> String {
    let named = seeded_systems(seeds);
    if named.is_empty() {
        "seeds: none — every row fills from empty in-run".into()
    } else {
        format!("seeds: {}", named.join(", "))
    }
}

fn list(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

const USAGE: &str = "\
bench — run the comparative benchmark, one caged row at a time (RFC 0065)

  --tier NAME            dataset tier (default: small)
  --only a,b             only these systems
  --workload micro,shop  only these workloads
  --consumers N          consumers for wavedb/multi on shop (default: 3)
  --refresh a,b          remeasure these systems even if stored
  --refresh-all          remeasure everything
  --dry-run              print the plan and stop
  --force                let rows record outside the cage
  --results DIR          corpus root (default: benches/results)
  --repo DIR             repository root, for provenance
";

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn exported_seeds_become_row_flags() {
        let args = seed_args(env(&[
            ("BENCH_SEED_WAVEDB", "/nix/store/w"),
            ("BENCH_SEED_POSTGRES", "/nix/store/p"),
        ]));
        assert_eq!(
            args,
            [
                "--seed-wavedb",
                "/nix/store/w",
                "--seed-postgres",
                "/nix/store/p"
            ]
        );
        assert_eq!(seed_line(&args), "seeds: wavedb, postgres");
    }

    #[test]
    fn no_seeds_is_said_rather_than_silent() {
        let args = seed_args(env(&[("BENCH_SEED_SQLITE", "")]));
        assert!(args.is_empty(), "an empty export is no seed: {args:?}");
        assert!(seed_line(&args).starts_with("seeds: none"));
    }
}
