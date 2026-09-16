//! Process lifecycle for the server bracket (RFC 0060 §2).
//!
//! The three server adapters differ in almost everything — connection string,
//! durability knob, compaction command — but share four mechanics, and those
//! are here so each adapter reads as its own database rather than as plumbing.
//!
//! One of them is load-bearing for the measurement: **write bytes come from the
//! server's `/proc/<pid>/io`, not ours**. In the embedded bracket the engine
//! writes in the benchmark's own process, so `/proc/self/io` is exactly right;
//! in the server bracket our process writes nothing but socket traffic, and
//! reading `self` would report a flat zero for every phase.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A server this process started, and is responsible for stopping cleanly.
///
/// Clean shutdown is not politeness: a data directory left in crash state makes
/// the *next* thing that opens it pay recovery, which would land inside a
/// measured phase or a footprint (RFC 0060 §6).
pub struct Server {
    child: Child,
    pub pid: u32,
}

impl Server {
    /// Start `cmd` detached from our stdio, with its own log file.
    pub fn spawn(cmd: &str, args: &[&str], log: &Path) -> Result<Self, String> {
        let out = std::fs::File::create(log)
            .map_err(|e| format!("{cmd}: log {}: {e}", log.display()))?;
        let err = out
            .try_clone()
            .map_err(|e| format!("{cmd}: log clone: {e}"))?;
        let child = Command::new(cmd)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .spawn()
            .map_err(|e| format!("spawn {cmd}: {e}"))?;
        let pid = child.id();
        Ok(Self { child, pid })
    }

    /// Bytes this server sent to the block layer so far, from its own
    /// `/proc/<pid>/io`. Zero if the process is gone — a stopped server's
    /// counter is not recoverable, so phases must be measured while it runs.
    #[must_use]
    pub fn write_bytes(&self) -> u64 {
        write_bytes(self.pid)
    }

    /// Ask the server to stop with its own shutdown command, then reap it.
    ///
    /// The command is the system's own (`pg_ctl stop`, `mysqladmin shutdown`,
    /// `mongod --shutdown`) rather than a signal, because each of them means
    /// "flush and close", and a signal only means "die".
    pub fn stop(mut self, cmd: &str, args: &[&str]) -> Result<(), String> {
        let out = Command::new(cmd)
            .args(args)
            .output()
            .map_err(|e| format!("{cmd}: {e}"))?;
        if !out.status.success() {
            let _ = self.child.kill();
            let _ = self.child.wait();
            return Err(format!(
                "{cmd} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        self.child
            .wait()
            .map_err(|e| format!("{cmd}: wait: {e}"))
            .map(|_| ())
    }
}

/// `write_bytes` for `pid` **and every descendant of it**.
///
/// The tree walk is not defensive programming, it is required: PostgreSQL is
/// process-per-connection, so the postmaster we spawn writes essentially
/// nothing and every byte comes from a backend, the WAL writer or the
/// checkpointer. Reading the postmaster alone reported a flat `0.0 kB/insert` —
/// a wrong number rather than a missing one, which is the failure mode this
/// suite is built against. MySQL and MongoDB are threaded and would have been
/// fine either way.
#[must_use]
pub fn write_bytes(pid: u32) -> u64 {
    let mut total = own_write_bytes(pid);
    let mut frontier = vec![pid];
    let children = child_map();
    while let Some(p) = frontier.pop() {
        for child in children.get(&p).into_iter().flatten() {
            total += own_write_bytes(*child);
            frontier.push(*child);
        }
    }
    total
}

fn own_write_bytes(pid: u32) -> u64 {
    let Ok(text) = std::fs::read_to_string(format!("/proc/{pid}/io")) else {
        return 0;
    };
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("write_bytes:") {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// Parent → children, from `/proc/<pid>/status`'s `PPid`. Cheap enough to
/// rebuild per sample (twice per phase), and the run's PID namespace keeps the
/// table to this run's own processes.
fn child_map() -> std::collections::HashMap<u32, Vec<u32>> {
    let mut map: std::collections::HashMap<u32, Vec<u32>> =
        std::collections::HashMap::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return map;
    };
    for entry in entries.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue; // not a process directory
        };
        let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status"))
        else {
            continue; // exited between the readdir and here
        };
        if let Some(ppid) = status
            .lines()
            .find_map(|l| l.strip_prefix("PPid:"))
            .and_then(|v| v.trim().parse::<u32>().ok())
        {
            map.entry(ppid).or_default().push(pid);
        }
    }
    map
}

/// How long a server may take to become connectable.
///
/// Generous on purpose, and shared so the three adapters cannot drift apart.
/// The per-site 60 s and 90 s this replaces were sized on an idle machine and
/// are far too tight inside the cage: `mysqld` 8.4 building its data dictionary
/// and redo logs on **4 CPUs against a contended disk** was still initialising
/// InnoDB when its 90 s expired, and that one timeout discarded a 50-minute
/// pass. Waiting costs nothing when the server is healthy — [`wait_for`] polls
/// and returns the moment it connects.
pub const STARTUP_SECS: u64 = 300;

/// Poll `ready` until it answers true or `secs` elapse.
///
/// Every server here takes seconds to become connectable, and every one of them
/// reports "started" long before it accepts a connection. Polling the thing the
/// benchmark actually needs — a working connection — is the only honest probe.
pub fn wait_for(
    what: &str,
    secs: u64,
    mut ready: impl FnMut() -> bool,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if ready() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!("{what}: not ready after {secs}s"))
}

/// Run a setup command to completion, failing loudly. Nothing here is timed:
/// these are `initdb`, `mysqld --initialize-insecure` and friends.
pub fn run(cmd: &str, args: &[&str]) -> Result<String, String> {
    let out = Command::new(cmd)
        .args(args)
        .output()
        .map_err(|e| format!("{cmd}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "{cmd} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// The tail of a server's log, for an error message that says what happened.
#[must_use]
pub fn log_tail(log: &Path, lines: usize) -> String {
    let Ok(text) = std::fs::read_to_string(log) else {
        return "no log".into();
    };
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// The cache budget every server in the suite is given, spelled the same way
/// three times because the three servers spell it differently.
///
/// It is **pinned rather than inferred**, and that is the whole point: each of
/// these sizes its cache from the machine's RAM by default, not from the
/// cgroup's — so under the suite's 500 MB cage an unpinned MongoDB asks for
/// gigabytes it cannot have and is OOM-killed, while MySQL and PostgreSQL
/// quietly take different fractions of a machine none of them can see. Equal
/// budgets are what makes the row a comparison (RFC 0060 §5).
/// 256 MB — **MongoDB's floor** (`--wiredTigerCacheSizeGB` refuses less than
/// 0.25), which is what sets the number for all three: equal budgets are what
/// make the row a comparison, so the least-adjustable server picks it. Half
/// the suite's 500 MB cage, leaving the other half for the server's non-cache
/// memory and the benchmark process that shares the cgroup with it. Only one
/// server runs at a time.
pub const CACHE_GB: &str = "0.25";
pub const CACHE_MYSQL: &str = "256M";
pub const CACHE_POSTGRES: &str = "256MB";

/// Kill a server that was never stopped.
///
/// Not tidiness. A row that fails mid-way — a refused operation, a preload
/// that would not load — unwinds past its `stop`, and the child keeps running
/// with the data directory open. The next run of that row then **clears a
/// directory another process is still writing to**, because the scratch is
/// named by the row's digest and is therefore the same path every time. That
/// is not a hypothetical: it truncated a `WiredTiger.wt` and the second
/// `mongod` died on `failed to read 4096 bytes at offset 77824`.
///
/// A kill rather than the system's own shutdown command, and that is the right
/// asymmetry: [`Server::stop`] means "flush and close" and belongs on the path
/// where the measurement succeeded. This one only has to guarantee the process
/// is not there any more.
impl Drop for Server {
    fn drop(&mut self) {
        // `stop` reaps the child, so this sees `Some(status)` and does
        // nothing. Only an unstopped server is still running here.
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
