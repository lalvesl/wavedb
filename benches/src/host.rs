//! The host fingerprint and the **host key** derived from it.
//!
//! Performance numbers are not portable across machines, so every recorded run
//! carries the machine it ran on and the corpus is read in lanes: two rows are
//! comparable only when their host keys match (RFC 0060 §7).

use std::fmt::Write as _;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use crate::json::fnv1a;

pub struct Host {
    pub cpu: String,
    /// CPUs the machine has.
    pub cores: u64,
    /// CPUs this run may actually use — `Cpus_allowed_list`, so a `taskset`
    /// cage shows up. `/proc/cpuinfo` would keep reporting all of them.
    pub cpu_budget: u64,
    /// RAM the machine has.
    pub mem_bytes: u64,
    /// RAM this run may actually use — the cgroup's `memory.max`, falling back
    /// to physical when unlimited. This bounds the **page cache** too, which is
    /// what makes a working set genuinely exceed memory (RFC 0060 §5).
    pub mem_budget: u64,
    pub kernel: String,
    pub filesystem: String,
    pub rotational: Option<bool>,
    pub virtualised: bool,
    /// `data_dir`'s btrfs allocation state, or `None` off btrfs. Deliberately
    /// **not** in the host key: it changes between runs on one machine, so it
    /// is a guard and a recorded fact, not a lane.
    ///
    /// `None` **while `filesystem == "btrfs"`** means the probe failed, which
    /// is not the same thing as "no risk" — it is "the guard is blind". Say so
    /// wherever this is displayed or recorded; the first version of this probe
    /// failed exactly that way and the guard never fired.
    pub btrfs: Option<BtrfsSpace>,
    /// `<cpu-slug>-<budget>c-<budget>g-<fs>-<hash4>` — the lane identity. The
    /// budgets are in it on purpose: a caged run and an uncaged one on the same
    /// machine are different lanes, because they are different machines as far
    /// as any number here is concerned.
    pub key: String,
}

impl Host {
    /// Fingerprint the machine, describing the filesystem of `data_dir`
    /// specifically — the disk under the benchmark is the one that matters,
    /// not the one under `/`.
    pub fn probe(data_dir: &Path) -> Self {
        let cores = num_cpus();
        let mem_bytes = mem_total();
        Self::probe_with_budget(
            data_dir,
            cgroup_memory_max().unwrap_or(mem_bytes).min(mem_bytes),
            allowed_cpus().unwrap_or(cores),
        )
    }

    /// Fingerprint the machine **as it will be seen at these budgets**.
    ///
    /// The supervisor needs this and [`probe`](Self::probe) would give it the
    /// wrong answer: it runs uncaged, so it observes the whole machine, while
    /// every row it schedules runs at the cage's budgets. Keying the lane
    /// from the supervisor's own view would file 4-cpu / 500 MB measurements
    /// under an `8c-15835m` name — a lane that names a machine none of its
    /// numbers ran on.
    ///
    /// A caged process passing what it observes gets the same answer, so the
    /// two sides agree by construction rather than by coincidence.
    pub fn probe_with_budget(
        data_dir: &Path,
        mem_budget: u64,
        cpu_budget: u64,
    ) -> Self {
        let cpu = cpu_model();
        let cores = num_cpus();
        let mem_bytes = mem_total();
        let kernel = read_trim("/proc/sys/kernel/osrelease");
        let filesystem = fs_type(data_dir);
        let rotational = rotational(data_dir);
        let virtualised = cpu_flag("hypervisor");
        // Megabytes, not gigabytes: the cage is 500 MB and integer-dividing
        // that by a GiB spelled the lane `-0g-`, which is both meaningless and
        // a collision — every sub-gigabyte budget would share one key. The
        // fingerprint below would still separate them, but a key a human
        // cannot read is a key nobody checks.
        let mut key = format!(
            "{}-{cpu_budget}c-{}m-{filesystem}",
            slug(&cpu),
            mem_budget / (1 << 20)
        );
        let fingerprint = format!(
            "{cpu}|{cores}|{cpu_budget}|{mem_bytes}|{mem_budget}|{kernel}|\
             {filesystem}|{rotational:?}|{virtualised}"
        );
        let _ = write!(key, "-{:04x}", fnv1a(fingerprint.as_bytes()) & 0xffff);
        Self {
            cpu,
            cores,
            cpu_budget,
            mem_bytes,
            mem_budget,
            kernel,
            filesystem,
            rotational,
            virtualised,
            btrfs: btrfs_space(data_dir),
            key,
        }
    }
}

/// 1-minute load average — the noise guard's input. A slow row recorded on a
/// busy machine is worse than a missing one: a future bisect blames a commit.
#[must_use]
pub fn load_average() -> f64 {
    read_trim("/proc/loadavg")
        .split_whitespace()
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}

fn cpu_model() -> String {
    for line in read_trim("/proc/cpuinfo").lines() {
        if let Some(v) = line.strip_prefix("model name") {
            // `model name\t: Intel(R) …` — the tab is part of the separator.
            return v.trim_start_matches([' ', '\t', ':']).trim().to_string();
        }
    }
    "unknown-cpu".into()
}

fn cpu_flag(flag: &str) -> bool {
    read_trim("/proc/cpuinfo")
        .lines()
        .find(|l| l.starts_with("flags"))
        .is_some_and(|l| l.split_whitespace().any(|f| f == flag))
}

fn num_cpus() -> u64 {
    read_trim("/proc/cpuinfo")
        .lines()
        .filter(|l| l.starts_with("processor"))
        .count() as u64
}

/// CPUs in this process's affinity mask, parsed from `Cpus_allowed_list`
/// (`0-3`, `0,2,4`, …). This is what `taskset` moves and `/proc/cpuinfo` does
/// not.
fn allowed_cpus() -> Option<u64> {
    let list = read_trim("/proc/self/status").lines().find_map(|l| {
        l.strip_prefix("Cpus_allowed_list:").map(str::to_string)
    })?;
    let mut n = 0;
    for part in list.trim().split(',') {
        match part.split_once('-') {
            Some((lo, hi)) => {
                let (lo, hi): (u64, u64) = (lo.parse().ok()?, hi.parse().ok()?);
                n += hi.saturating_sub(lo) + 1;
            }
            None if !part.is_empty() => n += 1,
            None => {}
        }
    }
    (n > 0).then_some(n)
}

/// This process's cgroup v2 `memory.max`, or `None` when unlimited or v1.
fn cgroup_memory_max() -> Option<u64> {
    // `0::/user.slice/…` — the unified hierarchy's line has an empty
    // controller field.
    let rel = read_trim("/proc/self/cgroup")
        .lines()
        .find_map(|l| l.strip_prefix("0::").map(str::to_string))?;
    let raw = read_trim(format!("/sys/fs/cgroup{rel}/memory.max"));
    raw.trim().parse().ok()
}

fn mem_total() -> u64 {
    for line in read_trim("/proc/meminfo").lines() {
        if let Some(v) = line.strip_prefix("MemTotal:") {
            let kb: u64 =
                v.trim().trim_end_matches(" kB").parse().unwrap_or_default();
            return kb * 1024;
        }
    }
    0
}

/// The mount whose path is the longest prefix of `dir` owns it.
fn fs_type(dir: &Path) -> String {
    mount_of(dir).map_or_else(|| "unknown".into(), |(_, kind)| kind)
}

/// `(device, fstype)` of the mount owning `dir`.
fn mount_of(dir: &Path) -> Option<(String, String)> {
    let dir = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let mounts = read_trim("/proc/mounts");
    let mut best: Option<(usize, String, String)> = None;
    for line in mounts.lines() {
        let mut f = line.split_whitespace();
        let (Some(dev), Some(point), Some(kind)) =
            (f.next(), f.next(), f.next())
        else {
            continue;
        };
        if dir.starts_with(point)
            && best.as_ref().is_none_or(|(n, _, _)| point.len() >= *n)
        {
            best = Some((point.len(), dev.to_string(), kind.to_string()));
        }
    }
    best.map(|(_, dev, kind)| (dev, kind))
}

/// What btrfs's allocator has to work with, as fractions of the device.
#[derive(Clone, Copy)]
pub struct BtrfsSpace {
    /// Device bytes belonging to **no** block group yet. This is the one that
    /// governs: while it lasts, a write that finds existing groups awkward
    /// simply carves a fresh chunk, which is cheap. It is what a `balance`
    /// gives back, and what running out of turns a write benchmark into a
    /// measurement of the allocator.
    pub unallocated: f64,
    /// How full the allocated **data** block groups are. Fragmentation
    /// pressure — informative, but not a fault on its own: right after a
    /// balance this is *high by design*, because balance is what packed them.
    pub data_fill: f64,
}

/// Read `dir`'s btrfs allocation state, or `None` off btrfs.
///
/// The pair matters more than either half. The state that correlated with a
/// 22× spread on one machine — the same 8 000-user fill measuring 59 s and
/// 1 297 s, nothing else changed — was **96% data fill *and* 8.5%
/// unallocated**: existing groups packed and nowhere cheap left to carve. A
/// balance then moved it to 92% fill and 27% unallocated, healthier on the
/// axis that governs and *worse* on the other. Guarding on data fill alone
/// would refuse precisely the disk somebody had just repaired.
///
/// (Correlation, not a controlled experiment: the thresholds below are a
/// precaution, not a proven cliff.)
fn btrfs_space(dir: &Path) -> Option<BtrfsSpace> {
    let (dev, kind) = mount_of(dir)?;
    if kind != "btrfs" {
        return None;
    }
    // Match the filesystem by the device's **number**, not its name. The
    // mount's device is whatever `/proc/mounts` recorded — commonly a
    // `/dev/mapper/*` symlink under LUKS or LVM — while sysfs lists the
    // kernel name (`dm-0`). Comparing basenames silently found nothing here,
    // and a guard that finds nothing is a guard that never fires.
    let rdev = std::fs::metadata(Path::new(&dev).canonicalize().ok()?)
        .ok()?
        .rdev();
    let want = format!("{}:{}", libc_major(rdev), libc_minor(rdev));
    for entry in std::fs::read_dir("/sys/fs/btrfs").ok()?.flatten() {
        let fs = entry.path();
        // `/sys/fs/btrfs` holds a `features/` directory beside the per-uuid
        // ones, and it has no `devices/`. `?` here would abandon the whole
        // probe on reaching it — order-dependently, since `read_dir` promises
        // none. `continue` is the difference between a guard and a coin flip.
        let Ok(listed) = std::fs::read_dir(fs.join("devices")) else {
            continue;
        };
        let mut capacity = 0.0;
        let mut matched = false;
        for d in listed.flatten() {
            matched |= read_trim(d.path().join("dev")) == want;
            // `size` is in 512-byte sectors, and every device counts: the
            // allocator draws from the pool, not from one member.
            capacity += read_trim(d.path().join("size"))
                .parse::<f64>()
                .unwrap_or_default()
                * 512.0;
        }
        if !matched || capacity <= 0.0 {
            continue;
        }
        let group = |kind: &str, field: &str| -> f64 {
            read_trim(fs.join("allocation").join(kind).join(field))
                .parse()
                .unwrap_or_default()
        };
        let data_total = group("data", "total_bytes");
        let allocated = data_total
            + group("metadata", "total_bytes")
            + group("system", "total_bytes");
        return Some(BtrfsSpace {
            unallocated: (capacity - allocated).max(0.0) / capacity,
            data_fill: if data_total > 0.0 {
                group("data", "bytes_used") / data_total
            } else {
                0.0
            },
        });
    }
    None
}

/// `/sys/dev/block/<major>:<minor>/queue/rotational`, falling back to the
/// parent device when `dir` sits on a partition.
fn rotational(dir: &Path) -> Option<bool> {
    let meta = std::fs::metadata(dir).ok()?;
    let dev = meta.dev();
    let (major, minor) = (libc_major(dev), libc_minor(dev));
    let base = format!("/sys/dev/block/{major}:{minor}");
    for path in [
        format!("{base}/queue/rotational"),
        format!("{base}/../queue/rotational"),
    ] {
        if let Ok(v) = std::fs::read_to_string(&path) {
            return Some(v.trim() == "1");
        }
    }
    None
}

// glibc's major/minor are macros; the encoding is stable enough to inline
// rather than take a `libc` dependency for two shifts.
const fn libc_major(dev: u64) -> u64 {
    ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff)
}

const fn libc_minor(dev: u64) -> u64 {
    (dev & 0xff) | ((dev >> 12) & !0xff)
}

fn slug(s: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for c in s.chars().take(48) {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    out.trim_end_matches('-').to_string()
}

fn read_trim(path: impl AsRef<Path>) -> String {
    std::fs::read_to_string(path)
        .map(|s| s.trim_end().to_string())
        .unwrap_or_default()
}
