//! A local object store for tests ([RFC 0070]).
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
use crate::carrier::block_on;

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
    /// created) on a free local port, and wait until it answers.
    ///
    /// The port is one the OS had free a moment before, so another server
    /// may take it first: a server counts as started only once it serves a
    /// marker written into *this* root, and one whose port was taken — it
    /// exits, or another answers — is started again on a fresh port.
    ///
    /// # Panics
    /// When versitygw is not on the `PATH` (run inside `nix develop`), or
    /// does not start within a few tries — a test harness has no better
    /// answer than to stop.
    #[must_use]
    pub fn start(root: &Path) -> Self {
        let bucket = root.join(BUCKET);
        if let Err(e) = std::fs::create_dir_all(&bucket) {
            panic!("bucket directory {}: {e}", bucket.display());
        }
        let marker = format!("probe-{}", std::process::id());
        let mark = format!("{:?}", Instant::now());
        if let Err(e) = std::fs::write(bucket.join(&marker), &mark) {
            panic!("probe marker: {e}");
        }
        for _ in 0..5 {
            if let Some(server) = Self::try_start(root, &marker, &mark) {
                let _ = std::fs::remove_file(bucket.join(&marker));
                return server;
            }
        }
        panic!("versitygw did not start");
    }

    /// One attempt: `None` once the server exits, or stays unready for ten
    /// seconds.
    fn try_start(root: &Path, marker: &str, mark: &str) -> Option<Self> {
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
        let mut server = Self { child, addr };
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if matches!(server.child.try_wait(), Ok(Some(_)) | Err(_)) {
                return None; // it could not bind: the port was taken
            }
            if TcpStream::connect(&server.addr).is_ok() {
                let got = block_on(server.bucket().get(marker, None));
                if matches!(got, Ok(Some(ref b)) if b == mark.as_bytes()) {
                    return Some(server);
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = server.child.kill();
        None
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
