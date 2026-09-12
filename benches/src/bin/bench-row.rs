//! `bench-row` — measure exactly one row, inside exactly one cage.
//!
//! It is what `bench` execs, and it does the one thing the supervisor cannot:
//! run in a process whose cgroup, CPU mask and PID namespace belong to this
//! row alone ([RFC 0065] §2).
//!
//! Every field of the row's identity arrives as a flag rather than being
//! re-derived here. That is deliberate: the supervisor already computed the
//! digest to decide this row needed measuring, and a child that recomputed it
//! from its own view of the world could disagree — and would then file the
//! result under a name nobody looks for.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::path::{Path, PathBuf};

use wavedb_bench::corpus::{Corpus, RowKey};
use wavedb_bench::row::{RowRun, measure};
use wavedb_bench::{cage, host, systems};

fn main() {
    if let Err(e) = run() {
        eprintln!("bench-row: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let opts = Opts::parse(std::env::args().skip(1))?;
    cage::init();

    std::fs::create_dir_all(&opts.work_dir)
        .map_err(|e| format!("work dir: {e}"))?;
    // The disk under the benchmark, not the one under `/`.
    let host = host::Host::probe(&opts.work_dir);
    if !opts.force {
        cage::verify(host.mem_budget, host.cpu_budget)?;
    }

    // A scratch of this row's own, cleared first.
    //
    // Not tidiness: two runs of one row in a shared directory make the second
    // one measure a **rewrite** where it reports an insert — `table thing
    // already exists` is the loud version, and a WaveDB store that simply
    // reopened would have been the silent one. Naming it by the digest also
    // means two rows can never collide, whatever order they run in.
    let key = opts.key();
    let scratch = opts.work_dir.join(key.digest());
    if scratch.exists() {
        std::fs::remove_dir_all(&scratch)
            .map_err(|e| format!("clear {}: {e}", scratch.display()))?;
    }
    std::fs::create_dir_all(&scratch).map_err(|e| format!("scratch: {e}"))?;

    let run = RowRun {
        caged: cage::is_caged(host.mem_budget, host.cpu_budget),
        forced: opts.force,
        dirty: opts.dirty,
        timestamp: opts.timestamp.clone(),
        cfg: opts.cfg(&scratch),
        shop: opts.shop_cfg(&scratch),
        key: key.clone(),
    };

    eprintln!(
        "row {}/{} {} · {} · {} consumers · {}",
        key.system,
        key.variant,
        key.durability,
        key.workload,
        key.consumers,
        key.digest()
    );

    let record = measure(&run)?;
    let path = Corpus::at(&opts.results).store(&record)?;
    eprintln!("row stored: {}", path.display());
    // Kept on failure, removed on success: a row that worked has nothing left
    // to look at, and twenty-two `large`-tier stores would fill the disk.
    let _ = std::fs::remove_dir_all(&scratch);
    Ok(())
}

/// One row's flags. No defaults for the identity fields — a row measured
/// against a guessed tier or seed would be filed under a digest that does not
/// describe it, so every one of them is required.
struct Opts {
    system: String,
    system_version: String,
    variant: String,
    durability: String,
    workload: String,
    tier: String,
    consumers: u32,
    dataset_revision: u64,
    cage_revision: u32,
    wavedb_git_sha: String,
    host_key: String,
    seed: u64,
    timestamp: String,
    dirty: bool,
    force: bool,
    results: PathBuf,
    work_dir: PathBuf,
    rows: u64,
    reads: u64,
    updates: u64,
    users: u64,
    signups: u64,
    checkouts: u64,
    profile_reads: u64,
    page_reads: u64,
    detail_reads: u64,
    orders_max: u64,
    items_max: u64,
    seed_wavedb: Option<PathBuf>,
    seed_sqlite: Option<PathBuf>,
    seed_postgres: Option<PathBuf>,
    seed_mysql: Option<PathBuf>,
    seed_mongodb: Option<PathBuf>,
}

impl Opts {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut o = Self {
            system: String::new(),
            system_version: String::new(),
            variant: "-".into(),
            durability: String::new(),
            workload: String::new(),
            tier: String::new(),
            consumers: 1,
            dataset_revision: 0,
            cage_revision: 0,
            wavedb_git_sha: String::new(),
            host_key: String::new(),
            seed: 42,
            timestamp: String::new(),
            dirty: false,
            force: false,
            results: PathBuf::from("benches/results"),
            work_dir: std::env::temp_dir().join("wavedb-bench-row"),
            rows: 100_000,
            reads: 50_000,
            updates: 50_000,
            users: 20_000,
            signups: 100,
            checkouts: 200,
            profile_reads: 10_000,
            page_reads: 1000,
            detail_reads: 1000,
            orders_max: 20,
            items_max: 5,
            seed_wavedb: None,
            seed_sqlite: None,
            seed_postgres: None,
            seed_mysql: None,
            seed_mongodb: None,
        };
        let mut it = args.peekable();
        while let Some(arg) = it.next() {
            let mut text =
                || it.next().ok_or_else(|| format!("{arg} needs a value"));
            match arg.as_str() {
                "--system" => o.system = text()?,
                "--system-version" => o.system_version = text()?,
                "--variant" => o.variant = text()?,
                "--durability" => o.durability = text()?,
                "--workload" => o.workload = text()?,
                "--tier" => o.tier = text()?,
                "--host-key" => o.host_key = text()?,
                "--wavedb-sha" => o.wavedb_git_sha = text()?,
                "--timestamp" => o.timestamp = text()?,
                "--consumers" => o.consumers = num(&mut it, &arg)? as u32,
                "--dataset-revision" => {
                    o.dataset_revision = num(&mut it, &arg)?
                }
                "--cage-revision" => {
                    o.cage_revision = num(&mut it, &arg)? as u32
                }
                "--seed" => o.seed = num(&mut it, &arg)?,
                "--rows" => o.rows = num(&mut it, &arg)?,
                "--reads" => o.reads = num(&mut it, &arg)?,
                "--updates" => o.updates = num(&mut it, &arg)?,
                "--users" => o.users = num(&mut it, &arg)?,
                "--signups" => o.signups = num(&mut it, &arg)?,
                "--checkouts" => o.checkouts = num(&mut it, &arg)?,
                "--profile-reads" => o.profile_reads = num(&mut it, &arg)?,
                "--page-reads" => o.page_reads = num(&mut it, &arg)?,
                "--detail-reads" => o.detail_reads = num(&mut it, &arg)?,
                "--orders-max" => o.orders_max = num(&mut it, &arg)?,
                "--items-max" => o.items_max = num(&mut it, &arg)?,
                "--results" => o.results = PathBuf::from(text()?),
                "--work-dir" => o.work_dir = PathBuf::from(text()?),
                "--seed-wavedb" => o.seed_wavedb = Some(text()?.into()),
                "--seed-sqlite" => o.seed_sqlite = Some(text()?.into()),
                "--seed-postgres" => o.seed_postgres = Some(text()?.into()),
                "--seed-mysql" => o.seed_mysql = Some(text()?.into()),
                "--seed-mongodb" => o.seed_mongodb = Some(text()?.into()),
                "--dirty" => o.dirty = true,
                "--force" => o.force = true,
                other => return Err(format!("unknown flag {other}")),
            }
        }
        o.check()?;
        Ok(o)
    }

    fn check(&self) -> Result<(), String> {
        for (name, value) in [
            ("--system", &self.system),
            ("--durability", &self.durability),
            ("--workload", &self.workload),
            ("--tier", &self.tier),
            ("--host-key", &self.host_key),
            ("--timestamp", &self.timestamp),
        ] {
            if value.is_empty() {
                return Err(format!("{name} is required"));
            }
        }
        Ok(())
    }

    fn key(&self) -> RowKey {
        RowKey {
            system: self.system.clone(),
            system_version: self.system_version.clone(),
            variant: self.variant.clone(),
            durability: self.durability.clone(),
            workload: self.workload.clone(),
            tier: self.tier.clone(),
            dataset_revision: self.dataset_revision,
            generator_seed: self.seed,
            consumers: self.consumers,
            host_key: self.host_key.clone(),
            cage_revision: self.cage_revision,
            wavedb_git_sha: (self.system == "wavedb")
                .then(|| self.wavedb_git_sha.clone()),
        }
    }

    fn cfg(&self, work_dir: &Path) -> systems::Cfg {
        systems::Cfg {
            rows: self.rows,
            reads: self.reads,
            updates: self.updates,
            seed: self.seed,
            work_dir: work_dir.to_path_buf(),
            seed_wavedb: self.seed_wavedb.clone(),
            seed_sqlite: self.seed_sqlite.clone(),
            seed_postgres: self.seed_postgres.clone(),
            seed_mysql: self.seed_mysql.clone(),
            seed_mongodb: self.seed_mongodb.clone(),
        }
    }

    fn shop_cfg(&self, work_dir: &Path) -> systems::shop::ShopCfg {
        systems::shop::ShopCfg {
            users: self.users,
            signups: self.signups,
            checkouts: self.checkouts,
            profile_reads: self.profile_reads,
            page_reads: self.page_reads,
            detail_reads: self.detail_reads,
            orders_max: self.orders_max,
            items_max: self.items_max,
            seed: self.seed,
            work_dir: work_dir.to_path_buf(),
        }
    }
}

fn num(
    it: &mut impl Iterator<Item = String>,
    arg: &str,
) -> Result<u64, String> {
    it.next()
        .ok_or_else(|| format!("{arg} needs a number"))?
        .parse()
        .map_err(|e| format!("{arg}: {e}"))
}
