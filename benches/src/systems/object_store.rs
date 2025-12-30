//! The object store the `wavedb/object` row writes to: a versitygw over a
//! directory of the row's scratch, on the same disk — and the requests it
//! served, counted from its access log.
//!
//! ## Why a local gateway, and what it can say
//!
//! A cloud bucket would measure the network to it, and a run would bill.
//! versitygw is an S3 gateway over plain files that the dev shell and the
//! storage tests already use: the requests a store makes are exactly the
//! ones it would make of S3, so **counting them** is the finding; their
//! time is a local disk's, a lower bound for any real object store.
//!
//! Every request is a line of the access log naming its operation
//! (`s3_GetObject`, …). A server is started per measured open, so its log
//! holds that open's requests and nothing else.

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use wavedb_platform::s3::{Bucket, Credentials};
use wavedb_storage::{Remote, StoreOptions};

const ACCESS: &str = "wavedb-bench";
const SECRET: &str = "wavedb-bench-secret";
const BUCKET: &str = "wavedb-bench";
/// The database's prefix in the bucket.
const PREFIX: &str = "db";

/// A versitygw serving a directory, killed on drop.
pub struct ObjectStore {
    child: Child,
    addr: String,
    log: PathBuf,
}

/// The requests a server answered, by kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Requests {
    /// `GetObject` and `HeadObject`.
    pub gets: u64,
    pub puts: u64,
    /// `ListObjects` / `ListObjectsV2`.
    pub lists: u64,
    pub deletes: u64,
}

impl Requests {
    /// What was served between `before` and this count.
    #[must_use]
    pub const fn since(self, before: Self) -> Self {
        Self {
            gets: self.gets.saturating_sub(before.gets),
            puts: self.puts.saturating_sub(before.puts),
            lists: self.lists.saturating_sub(before.lists),
            deletes: self.deletes.saturating_sub(before.deletes),
        }
    }
}

impl ObjectStore {
    /// Serve `root` (its bucket created) on a free local port, logging every
    /// request to `log`, and wait until it accepts connections.
    ///
    /// # Errors
    /// versitygw is not on `PATH`, or does not start.
    pub fn start(root: &Path, log: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(root.join(BUCKET))
            .map_err(|e| format!("{}: {e}", root.display()))?;
        // The port was free a moment ago; one taken since makes the server
        // exit, and another try takes another.
        for _ in 0..5 {
            if let Some(server) = Self::try_start(root, log)? {
                return Ok(server);
            }
        }
        Err("versitygw did not start".into())
    }

    fn try_start(root: &Path, log: &Path) -> Result<Option<Self>, String> {
        let addr = TcpListener::bind("127.0.0.1:0")
            .and_then(|probe| probe.local_addr())
            .map_err(|e| format!("no free port: {e}"))?
            .to_string();
        let child = Command::new("versitygw")
            .args(["--access", ACCESS, "--secret", SECRET, "--port", &addr])
            .arg("--access-log")
            .arg(log)
            .arg("--quiet")
            .arg("posix")
            .arg(root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("versitygw: {e}"))?;
        let mut server = Self {
            child,
            addr,
            log: log.to_path_buf(),
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if matches!(server.child.try_wait(), Ok(Some(_)) | Err(_)) {
                return Ok(None);
            }
            if TcpStream::connect(&server.addr).is_ok() {
                return Ok(Some(server));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = server.child.kill();
        Ok(None)
    }

    /// The options of a store kept in this object store.
    #[must_use]
    pub fn options(&self) -> StoreOptions {
        let creds = Credentials {
            access: ACCESS.into(),
            secret: SECRET.into(),
            region: "us-east-1".into(),
        };
        StoreOptions {
            remote: Some(Remote::new(
                Bucket::new(&self.addr, BUCKET, creds),
                PREFIX,
            )),
            ..StoreOptions::default()
        }
    }

    /// Every request served so far, once the log has caught up: the
    /// gateway writes it behind the answers, so it is read until it stops
    /// growing.
    #[must_use]
    pub fn requests(&self) -> Requests {
        let mut last = None;
        for _ in 0..50 {
            let len = std::fs::metadata(&self.log).map_or(0, |m| m.len());
            if last == Some(len) {
                break;
            }
            last = Some(len);
            std::thread::sleep(Duration::from_millis(100));
        }
        count(&std::fs::read_to_string(&self.log).unwrap_or_default())
    }
}

impl Drop for ObjectStore {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Count an access log's requests by kind.
fn count(log: &str) -> Requests {
    let mut r = Requests::default();
    for op in log.split_whitespace().filter_map(|w| w.strip_prefix("s3_")) {
        match op {
            "GetObject" | "HeadObject" => r.gets += 1,
            "PutObject" => r.puts += 1,
            "ListObjects" | "ListObjectsV2" => r.lists += 1,
            "DeleteObject" | "DeleteObjects" => r.deletes += 1,
            _ => {}
        }
    }
    r
}

#[cfg(test)]
mod tests {
    use super::{Requests, count};

    /// Each line names its operation once; the log's header line and an
    /// unknown operation count as nothing.
    #[test]
    fn an_access_log_is_counted_by_operation() {
        let log = "log starts 2026-11-08 12:51:58 -0300\n\
            - b [08/November/2026:12:52:00 -0300] 127.0.0.1 - X \
              s3_GetObject x /b/x 200 - 0 0 0 0 - c - Y SigV4\n\
            - b [08/November/2026:12:52:00 -0300] 127.0.0.1 - X \
              s3_PutObject x /b/x 200 - 0 0 0 0 - c - Y SigV4\n\
            - b [..] s3_ListObjectsV2 /b 200\n\
            - b [..] s3_HeadObject /b/y 404\n\
            - b [..] s3_CreateBucket /b 200\n";
        assert_eq!(
            count(log),
            Requests {
                gets: 2,
                puts: 1,
                lists: 1,
                deletes: 0,
            }
        );
    }
}
