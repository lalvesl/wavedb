//! A local object store for tests ([RFC 0070]).
//!
//! The dev shell's versitygw is an S3 gateway over a plain directory, so
//! each test owns one in its own tempdir and it starts in milliseconds.
//! Behind the `test-server` feature — only test builds ask for it.
//!
//! [`TestServer::start_tls`] serves HTTPS instead, with a certificate for
//! `127.0.0.1` signed by an authority made for it (`openssl`, from the dev
//! shell); its [`bucket`](TestServer::bucket) trusts that authority.
//!
//! [RFC 0070]: ../../../../rfcs/0070-object-storage-backend.md

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
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
    /// For HTTPS: the certificate files, and the authority to trust (DER).
    tls: Option<Tls>,
}

/// A test server's certificate, and the authority that signed it.
#[derive(Debug, Clone)]
struct Tls {
    cert: PathBuf,
    key: PathBuf,
    authority: Vec<u8>,
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
        Self::start_with(root, None)
    }

    /// [`start`](Self::start), serving HTTPS: a certificate for `127.0.0.1`
    /// signed by an authority made in `root` for it.
    ///
    /// # Panics
    /// As [`start`](Self::start), and when `openssl` cannot make the
    /// certificates.
    #[must_use]
    pub fn start_tls(root: &Path) -> Self {
        let tls = make_tls(&root.join("tls"));
        Self::start_with(root, Some(&tls))
    }

    fn start_with(root: &Path, tls: Option<&Tls>) -> Self {
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
            if let Some(server) =
                Self::try_start(root, &marker, &mark, tls.cloned())
            {
                let _ = std::fs::remove_file(bucket.join(&marker));
                return server;
            }
        }
        panic!("versitygw did not start");
    }

    /// One attempt: `None` once the server exits, or stays unready for ten
    /// seconds.
    fn try_start(
        root: &Path,
        marker: &str,
        mark: &str,
        tls: Option<Tls>,
    ) -> Option<Self> {
        let addr = TcpListener::bind("127.0.0.1:0")
            .and_then(|probe| probe.local_addr())
            .map_or_else(|e| panic!("no free port: {e}"), |a| a.to_string());
        let mut command = Command::new("versitygw");
        command.args(["--access", ACCESS, "--secret", SECRET, "--port", &addr]);
        if let Some(tls) = &tls {
            command.arg("--cert").arg(&tls.cert);
            command.arg("--key").arg(&tls.key);
        }
        let child = command
            .arg("posix")
            .arg(root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!("versitygw — run the tests inside `nix develop`: {e}")
            });
        let mut server = Self { child, addr, tls };
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
        let creds = Credentials {
            access: ACCESS.into(),
            secret: secret.into(),
            region: "us-east-1".into(),
        };
        match &self.tls {
            None => Bucket::new(&self.addr, BUCKET, creds),
            Some(tls) => {
                Bucket::new(format!("https://{}", self.addr), BUCKET, creds)
                    .trusting(tls.authority.clone())
            }
        }
    }

    /// The test bucket over HTTPS trusting Mozilla's roots alone — which
    /// never signed this server's certificate.
    #[must_use]
    pub fn bucket_untrusting(&self) -> Bucket {
        let creds = Credentials {
            access: ACCESS.into(),
            secret: SECRET.into(),
            region: "us-east-1".into(),
        };
        Bucket::new(format!("https://{}", self.addr), BUCKET, creds)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// An authority and a certificate for `127.0.0.1` it signed, made in `dir`
/// by `openssl` — P-256 keys, valid two days.
fn make_tls(dir: &Path) -> Tls {
    if let Err(e) = std::fs::create_dir_all(dir) {
        panic!("tls directory {}: {e}", dir.display());
    }
    let ec = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1"];
    let ext = dir.join("server.ext");
    let server_ext = "subjectAltName=IP:127.0.0.1,DNS:localhost\n\
        basicConstraints=critical,CA:FALSE\n\
        keyUsage=critical,digitalSignature\n\
        extendedKeyUsage=serverAuth\n";
    if let Err(e) = std::fs::write(&ext, server_ext) {
        panic!("server extensions: {e}");
    }
    openssl(
        dir,
        &[
            &["req", "-x509"][..],
            &ec,
            &[
                "-nodes",
                "-keyout",
                "ca.key",
                "-out",
                "ca.pem",
                "-days",
                "2",
                "-subj",
                "/CN=wavedb-test-ca",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign",
            ],
        ],
    );
    openssl(
        dir,
        &[
            &["req"][..],
            &ec,
            &[
                "-nodes",
                "-keyout",
                "server.key",
                "-out",
                "server.csr",
                "-subj",
                "/CN=localhost",
            ],
        ],
    );
    openssl(
        dir,
        &[&[
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.pem",
            "-days",
            "2",
            "-extfile",
            "server.ext",
        ][..]],
    );
    openssl(
        dir,
        &[&["x509", "-in", "ca.pem", "-outform", "DER", "-out", "ca.der"][..]],
    );
    let authority = std::fs::read(dir.join("ca.der"))
        .unwrap_or_else(|e| panic!("authority certificate: {e}"));
    Tls {
        cert: dir.join("server.pem"),
        key: dir.join("server.key"),
        authority,
    }
}

/// Run `openssl` in `dir` with `args` (in parts), or stop.
fn openssl(dir: &Path, args: &[&[&str]]) {
    let status = Command::new("openssl")
        .current_dir(dir)
        .args(args.iter().flat_map(|a| a.iter()))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap_or_else(|e| panic!("openssl — run inside `nix develop`: {e}"));
    assert!(status.success(), "openssl {args:?} failed");
}
