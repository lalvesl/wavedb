//! A loc            .map_or_else(|e| panic!("no free port: {e}"), |a| a.to_string());l object store for tests ([RFC 0070]).
//!
//! The dev shell's versitygw is an S3 gateway over a plain directory, so
//! each test owns one in its own tempdir and it starts in milliseconds.
//! Behind the `test-server` feature — only test builds ask for it.
//!
//! [RFC 0070]: ../../../../rfcs/0070-object-storage-backend-WIP.md

use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use super::{Bucket, Credentials};

/// The key pair every test server accepts.
pub const ACCESS: &str = "wavedb-test";
/// Its secret.
pub const SECRET: &str = "wavedb-test-secret";
/// The bucket every test server serves.
pub const BUCKET: &str = "wavedb-test";

/// A versitygw serving a directory, killed on drop.
#[derive(Debug)]
pub struct TestServer {
    child: Child,
    addr: String,
}

impl TestServer {
    /// Serve `root` (a directory under it is a bucket; [`BUCKET`] is
    /// created) on a free local port, and wait until it accepts.
    ///
    /// # Panics
    /// When versitygw is not on the `PATH` (run inside `nix develop`), or
    /// does not start within ten seconds — a test harness has no better
    /// answer than to stop.
    #[must_use]
    pub fn start(root: &Path) -> Self {
        let bucket = root.join(BUCKET);
        if let Err(e) = std::fs::create_dir_all(&bucket) {
            panic!("bucket directory {}: {e}", bucket.display());
        }
        let addr = TcpListener::bind("127.0.0.1:0")
            .and_then(|probe| probe.local_addr())
            .map_or_else(|e| panic!("no free port: {e}"), |a| a.to_string());
        let child = Command::new("versitygw")
            .args(["--access", ACCESS, "--secret", SECRET, "--port", &addr])
            .arg("posix")
            .arg(root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!("versitygw — run the tests inside `nix develop`: {e}")
            });
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(&addr).is_err() {
            assert!(Instant::now() < deadline, "versitygw did not start");
            std::thread::sleep(Duration::from_millis(20));
        }
        Self { child, addr }
    }

    /// `host:port` it listens on.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// The test bucket, signed with the test key pair.
    #[must_use]
    pub fn bucket(&self) -> Bucket {
        self.bucket_signed_with(SECRET)
    }

    /// The test bucket, signed with `secret` — a wrong one is refused.
    #[must_use]
    pub fn bucket_signed_with(&self, secret: &str) -> Bucket {
        Bucket::new(
            self.addr.clone(),
            BUCKET,
            Credentials {
                access: ACCESS.into(),
                secret: secret.into(),
                region: "us-east-1".into(),
            },
        )
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
