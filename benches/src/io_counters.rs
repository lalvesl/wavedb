//! The `/proc/<pid>/io` counters a phase brackets ([RFC 0065] §8, phase 4).
//!
//! ## Why three counters and not one
//!
//! `write_bytes` alone answers "what did this cost the disk to store". It says
//! nothing about what the disk was asked to *serve*, which is the other half
//! of an engine's IO story and the half this suite could not see: a `mongod`
//! observed reading ~1 TB against a 27.5 MB dataset could not be classified
//! from the corpus, because no read counter was recorded.
//!
//! - `read_bytes` — bytes the block layer actually served.
//! - `rchar` — bytes the process asked for, cache hit or not.
//!
//! Their ratio is the number worth reading. Above 1, the engine fetched more
//! than it used: page-granular reads of small records, a B+tree descent, a
//! scan where a lookup would do. Below 1, a cache served the difference — and
//! *whose* cache is the interesting question, since the OS page cache is warm
//! for every system here while the engine's own is what `read_cold` empties.
//!
//! ## Why one pass
//!
//! `/proc/<pid>/io` is a snapshot. Reading it three times gives three answers
//! that never coexisted, and an amplification computed from mismatched samples
//! is worse than no amplification at all. So: one read, one parse, three
//! fields.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

/// One snapshot of a process's IO counters.
///
/// Cumulative since the process started, so a phase is the difference between
/// two of them ([`since`](Self::since)).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Io {
    /// Bytes fetched from the block layer — what the disk actually served.
    pub read_bytes: u64,
    /// Bytes the process asked for, served from the page cache or not.
    pub rchar: u64,
    /// Bytes sent to the block layer. Not `wchar`, which counts writes the
    /// page cache absorbed and the disk may never see.
    pub write_bytes: u64,
}

impl Io {
    /// This process's counters.
    #[must_use]
    pub fn current() -> Self {
        Self::parse(&read_proc("self"))
    }

    /// One process's counters, by pid. Zero if it is gone or unreadable —
    /// a server that exited mid-phase leaves no counters to read, and a
    /// failed row is already failing for a better-stated reason.
    #[must_use]
    pub fn of_pid(pid: u32) -> Self {
        Self::parse(&read_proc(&pid.to_string()))
    }

    /// `pid` plus every descendant, which is what a database server is.
    ///
    /// The walk is not defensive programming, it is required: PostgreSQL is
    /// process-per-connection, so the postmaster this suite spawns does
    /// essentially no IO of its own and every byte comes from a backend, the
    /// WAL writer or the checkpointer. Reading the postmaster alone reported a
    /// flat `0.0 kB/insert` — a wrong number rather than a missing one, which
    /// is the failure mode this suite is built against. MySQL and MongoDB are
    /// threaded and would have been fine either way.
    #[must_use]
    pub fn tree(pid: u32, children: &ChildMap) -> Self {
        let mut total = Self::of_pid(pid);
        let mut frontier = vec![pid];
        while let Some(p) = frontier.pop() {
            for child in children.get(&p) {
                total = total.plus(Self::of_pid(child));
                frontier.push(child);
            }
        }
        total
    }

    /// What happened between `before` and this snapshot.
    ///
    /// Saturating per field: a server restarted at a phase boundary resets its
    /// counters to zero, so the "after" can legitimately be smaller than the
    /// "before". Zero is the honest answer there — the phase's IO went with
    /// the process that did it.
    #[must_use]
    pub fn since(self, before: Self) -> Self {
        Self {
            read_bytes: self.read_bytes.saturating_sub(before.read_bytes),
            rchar: self.rchar.saturating_sub(before.rchar),
            write_bytes: self.write_bytes.saturating_sub(before.write_bytes),
        }
    }

    /// Bytes the disk served per byte the process asked for.
    ///
    /// Above 1: the engine fetched more than it used. Below 1: something
    /// cached the difference. Zero `rchar` means the phase asked for nothing,
    /// and `0.0` says that better than a division would.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn read_amplification(self) -> f64 {
        if self.rchar == 0 {
            return 0.0;
        }
        self.read_bytes as f64 / self.rchar as f64
    }

    fn plus(self, other: Self) -> Self {
        Self {
            read_bytes: self.read_bytes.saturating_add(other.read_bytes),
            rchar: self.rchar.saturating_add(other.rchar),
            write_bytes: self.write_bytes.saturating_add(other.write_bytes),
        }
    }

    /// One pass over the file's `key: value` lines.
    ///
    /// `strip_prefix` anchors at the start, which is what keeps
    /// `cancelled_write_bytes:` from being read as `write_bytes:` — the two
    /// lines are adjacent in every kernel that emits them.
    fn parse(text: &str) -> Self {
        let mut io = Self::default();
        for line in text.lines() {
            if let Some(v) = line.strip_prefix("rchar:") {
                io.rchar = num(v);
            } else if let Some(v) = line.strip_prefix("read_bytes:") {
                io.read_bytes = num(v);
            } else if let Some(v) = line.strip_prefix("write_bytes:") {
                io.write_bytes = num(v);
            }
        }
        io
    }
}

fn num(v: &str) -> u64 {
    v.trim().parse().unwrap_or(0)
}

fn read_proc(who: &str) -> String {
    std::fs::read_to_string(format!("/proc/{who}/io")).unwrap_or_default()
}

/// Parent → children, read once per snapshot.
///
/// Built once and passed in rather than rebuilt per pid: walking every
/// `/proc/<pid>/status` for each node of the tree would be quadratic, and the
/// walk happens at a phase boundary where the cost is not measured but the
/// wall clock still passes.
pub struct ChildMap {
    /// `(parent, child)` pairs, sorted by parent.
    pairs: Vec<(u32, u32)>,
}

impl ChildMap {
    /// Every process's parent, from `/proc/<pid>/status`'s `PPid`.
    #[must_use]
    pub fn read() -> Self {
        let mut pairs = Vec::new();
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return Self { pairs };
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(pid) = name.to_str().and_then(|s| s.parse::<u32>().ok())
            else {
                continue; // /proc holds more than pids
            };
            if let Some(ppid) = parent_of(pid) {
                pairs.push((ppid, pid));
            }
        }
        pairs.sort_unstable();
        Self { pairs }
    }

    /// `pid`'s direct children.
    #[must_use]
    pub fn get(&self, pid: &u32) -> Vec<u32> {
        let start = self.pairs.partition_point(|(p, _)| p < pid);
        self.pairs[start..]
            .iter()
            .take_while(|(p, _)| p == pid)
            .map(|(_, c)| *c)
            .collect()
    }
}

fn parent_of(pid: u32) -> Option<u32> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    text.lines()
        .find_map(|l| l.strip_prefix("PPid:"))
        .and_then(|v| v.trim().parse().ok())
}

/// Whose IO a phase should be attributed to.
///
/// The embedded bracket reads and writes in this process; the server bracket
/// does it in the server's, and reading `self` for a server row would report a
/// flat zero for every phase — a wrong number rather than a missing one.
///
/// Named for what it does rather than what it counts: it used to be `Writer`,
/// which stopped being true the moment reads were recorded beside writes.
#[derive(Debug, Clone, Copy)]
pub enum Meter {
    Current,
    Pid(u32),
}

impl Meter {
    /// This meter's cumulative counters, right now.
    ///
    /// The server case rebuilds the process map on every call. That is a walk
    /// of `/proc` at a phase boundary, which costs wall-clock that nothing
    /// measures — and the alternative, caching it, would miss the backend a
    /// connection forked after the snapshot.
    #[must_use]
    pub fn io(self) -> Io {
        match self {
            Self::Current => Io::current(),
            Self::Pid(pid) => Io::tree(pid, &ChildMap::read()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ChildMap, Io};

    /// A real kernel's file, verbatim. The adjacency of `write_bytes` and
    /// `cancelled_write_bytes` is the whole reason `parse` anchors.
    const SAMPLE: &str = "\
rchar: 2097152
wchar: 4096
syscr: 42
syscw: 7
read_bytes: 8192
write_bytes: 12288
cancelled_write_bytes: 4096
";

    #[test]
    fn parse_takes_three_counters_and_no_neighbours() {
        let io = Io::parse(SAMPLE);
        assert_eq!(io.rchar, 2_097_152);
        assert_eq!(io.read_bytes, 8192);
        assert_eq!(
            io.write_bytes, 12288,
            "cancelled_write_bytes must not be read as write_bytes"
        );
    }

    #[test]
    fn a_missing_or_empty_file_reads_as_zero() {
        assert_eq!(Io::parse(""), Io::default());
        assert_eq!(Io::of_pid(0), Io::default(), "pid 0 is not a process");
    }

    /// A server restarted at a phase boundary starts its counters again, so
    /// "after" can be smaller than "before". Zero is the honest answer.
    #[test]
    fn a_restarted_process_reports_zero_rather_than_wrapping() {
        let before = Io::parse(SAMPLE);
        let after = Io::default();
        assert_eq!(after.since(before), Io::default());
    }

    #[test]
    fn amplification_is_disk_bytes_per_requested_byte() {
        let io = Io {
            read_bytes: 4096,
            rchar: 1024,
            write_bytes: 0,
        };
        assert!((io.read_amplification() - 4.0).abs() < f64::EPSILON);
        let cached = Io {
            read_bytes: 0,
            rchar: 1024,
            write_bytes: 0,
        };
        assert!(cached.read_amplification().abs() < f64::EPSILON);
        assert!(
            Io::default().read_amplification().abs() < f64::EPSILON,
            "a phase that asked for nothing has no ratio, not a division"
        );
    }

    /// Against the real file, not a string: a parser proven only on a literal
    /// proves the literal. This is the one that would catch a kernel that
    /// spells the fields differently.
    #[test]
    fn current_sees_this_process_actually_read() {
        // Its own directory, owned and removed: `temp_dir()` is shared, and a
        // test that walked a busy one has already raced here once.
        let dir = std::env::temp_dir()
            .join(format!("wavedb-bench-io-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch");
        let path = dir.join("payload");
        let payload = vec![b'x'; 1 << 20];
        std::fs::write(&path, &payload).expect("write");

        let before = Io::current();
        let got = std::fs::read(&path).expect("read");
        let after = Io::current();

        assert_eq!(got.len(), payload.len());
        let delta = after.since(before);
        assert!(
            delta.rchar >= payload.len() as u64,
            "rchar must count the bytes we asked for, got {}",
            delta.rchar
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_child_map_groups_by_parent() {
        let map = ChildMap {
            pairs: vec![(1, 10), (1, 11), (2, 20), (5, 50)],
        };
        assert_eq!(map.get(&1), vec![10, 11]);
        assert_eq!(map.get(&5), vec![50]);
        assert!(map.get(&99).is_empty(), "no children is empty, not a panic");
    }

    /// Above any `pid_max` the kernel allows, so never a live process.
    const GONE: u32 = u32::MAX;

    /// A server that exited mid-phase — or a child that did — contributes
    /// zero to the walk instead of failing it.
    #[test]
    fn vanished_processes_in_the_tree_count_as_zero() {
        let map = ChildMap {
            pairs: vec![(GONE - 1, GONE)],
        };
        assert_eq!(Io::tree(GONE - 1, &map), Io::default());
    }

    /// Observing is reading: each snapshot of `self` adds its own file's
    /// size to `rchar`, so two snapshots are never equal and a phase's
    /// `since` carries one snapshot's worth (~100 bytes) of measurement.
    /// Noise against any phase that reads at all — but it rules out
    /// asserting equality between live snapshots, which is why the walk is
    /// tested on vanished pids above.
    #[test]
    fn observing_the_counters_moves_rchar() {
        let first = Io::current();
        let second = Io::current();
        assert!(second.rchar > first.rchar, "{first:?} then {second:?}");
        assert!(
            second.since(first).rchar < 4096,
            "{first:?} then {second:?}"
        );
    }
}
