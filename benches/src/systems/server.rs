//! Process lifecycle for the server bracket (RFC 0060 §2).
//!
//! The three server adapters differ in almost everything — connection string,
//! durability knob, compaction command — but share four mechanics, and those
//! are here so each adapter reads as its own database rather than as plumbing.
//!
//! The measurement's side of a server — its IO is read from the server's
//! process tree, never ours — lives with the counters, in
//! [`crate::io_counters::Meter`]; what is here only starts, waits for and
//! stops the process.

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

/// How long a server may take to become connectable.
///
/// Generous on purpose, and shared so the three adapters cannot drift apart.
/// The per-site 60 s and 90 s this replaces were sized on an idle machine and
/// are far too tight inside the cage: `mysqld` 8.4 building its data dictionary
/// and redo logs on **4 CPUs against a contended disk** was still initialising
/// InnoDB when its 90 s expired, and that one timeout discarded a 50-minute
/// pass. Waiting costs nothing when the server is healthy — [`Server::wait_ready`] polls
/// and returns the moment it connects.
pub const STARTUP_SECS: u64 = 300;

impl Server {
    /// Poll `ready` until it answers true, the server exits, or `secs` elapse.
    ///
    /// Every server here takes seconds to become connectable, and every one
    /// of them reports "started" long before it accepts a connection. Polling
    /// the thing the benchmark actually needs — a working connection — is the
    /// only honest probe.
    ///
    /// The exit check is what keeps a dead server from looking like a slow
    /// one. Without it a PostgreSQL that refused its socket path and exited in
    /// 2 ms was waited on for the full 300 s and reported as `not ready` —
    /// the cause was in its log, and the error arrived five minutes late
    /// without it.
    ///
    /// # Errors
    /// The server exiting first, or `secs` passing without a connection.
    pub fn wait_ready(
        &mut self,
        what: &str,
        secs: u64,
        mut ready: impl FnMut() -> bool,
    ) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < deadline {
            if ready() {
                return Ok(());
            }
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    return Err(format!(
                        "{what}: exited ({status}) before accepting a connection"
                    ));
                }
                Ok(None) => {}
                Err(e) => return Err(format!("{what}: wait: {e}")),
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Err(format!("{what}: not ready after {secs}s"))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A server that dies is reported as dead, at once — not as one that
    /// never became ready, five minutes later.
    #[test]
    fn a_server_that_exits_fails_the_wait_immediately() {
        let dir = std::env::temp_dir()
            .join(format!("wavedb-bench-server-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch");
        let mut server =
            Server::spawn("false", &[], &dir.join("false.log")).expect("spawn");
        let started = Instant::now();
        let err = server
            .wait_ready("false", STARTUP_SECS, || false)
            .expect_err("a dead server is never ready");
        assert!(err.contains("exited"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(5), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_ready_server_returns_without_waiting() {
        let dir = std::env::temp_dir()
            .join(format!("wavedb-bench-server-ok-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch");
        let mut server = Server::spawn("sleep", &["5"], &dir.join("sleep.log"))
            .expect("spawn");
        assert_eq!(server.wait_ready("sleep", 1, || true), Ok(()));
        let _ = server.child.kill();
        let _ = server.child.wait();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
