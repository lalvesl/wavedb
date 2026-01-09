//! The object-store client against a real S3 server ([RFC 0070]): the dev
//! shell's versitygw, one per test over its own directory (a directory under
//! its root is a bucket). Every behaviour the engine will lean on is pinned
//! here — the signature accepted, ranges cut at the end, the conditional
//! PUTs refused with `412`, the listing in key order, deletes idempotent.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

// The client's futures share the platform's current-thread stance.
#![allow(clippy::future_not_send)]

use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use wavedb_platform::carrier::block_on;
use wavedb_platform::s3::{Bucket, Credentials, Put, When};

const ACCESS: &str = "wavedb-test";
const SECRET: &str = "wavedb-test-secret";
const NAME: &str = "wavedb-test";

/// A versitygw serving a fresh directory, killed on drop.
struct Server {
    child: Child,
    addr: String,
    _dir: tempfile::TempDir,
}

impl Server {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(NAME)).unwrap();
        let addr = {
            let probe = TcpListener::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap().to_string()
        };
        let child = Command::new("versitygw")
            .args(["--access", ACCESS, "--secret", SECRET, "--port", &addr])
            .arg("posix")
            .arg(dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("versitygw — run the tests inside `nix develop`");
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(&addr).is_err() {
            assert!(Instant::now() < deadline, "versitygw did not start");
            std::thread::sleep(Duration::from_millis(20));
        }
        Self {
            child,
            addr,
            _dir: dir,
        }
    }

    fn bucket(&self) -> Bucket {
        Bucket::new(
            self.addr.clone(),
            NAME,
            Credentials {
                access: ACCESS.into(),
                secret: SECRET.into(),
                region: "us-east-1".into(),
            },
        )
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn bytes(n: usize) -> Vec<u8> {
    (0..n).map(|i| u8::try_from(i * 7 % 251).unwrap()).collect()
}

#[test]
fn an_object_reads_back_whole_and_by_range() {
    let server = Server::start();
    let b = server.bucket();
    block_on(async {
        let data = bytes(10_000);
        assert!(matches!(
            b.put("shards/07/2seg/fe", &data, When::Always)
                .await
                .unwrap(),
            Put::Stored { .. }
        ));
        let whole = b.get("shards/07/2seg/fe", None).await.unwrap();
        assert_eq!(whole.as_deref(), Some(&data[..]));
        let page = b.get("shards/07/2seg/fe", Some(4096..8192)).await;
        assert_eq!(page.unwrap().as_deref(), Some(&data[4096..8192]));
        let tail = b.get("shards/07/2seg/fe", Some(8192..20_000)).await;
        assert_eq!(
            tail.unwrap().as_deref(),
            Some(&data[8192..]),
            "a range past the end is cut at the end"
        );
        assert_eq!(b.get("shards/07/2seg/none", None).await.unwrap(), None);
    });
}

/// The journal and `1start` rely on these: a key written only if absent,
/// and replaced only while it still holds the version read.
#[test]
fn conditional_puts_refuse_instead_of_overwriting() {
    let server = Server::start();
    let b = server.bucket();
    block_on(async {
        let Put::Stored { etag } =
            b.put("1start", b"7", When::Absent).await.unwrap()
        else {
            panic!("a new key must be stored");
        };
        assert_eq!(
            b.put("1start", b"8", When::Absent).await.unwrap(),
            Put::Refused
        );
        assert_eq!(
            b.put("1start", b"8", When::Holds("\"other\"".into()))
                .await
                .unwrap(),
            Put::Refused
        );
        assert_eq!(
            b.get("1start", None).await.unwrap().as_deref(),
            Some(&b"7"[..])
        );
        assert!(matches!(
            b.put("1start", b"8", When::Holds(etag)).await.unwrap(),
            Put::Stored { .. }
        ));
        assert_eq!(
            b.get("1start", None).await.unwrap().as_deref(),
            Some(&b"8"[..])
        );
    });
}

/// File names are complemented, so the smallest key is the newest file.
#[test]
fn the_first_key_under_a_prefix_is_the_newest_file() {
    let server = Server::start();
    let b = server.bucket();
    block_on(async {
        assert_eq!(b.first_key("2seg/").await.unwrap(), None);
        for n in 1..=3u64 {
            let key = format!("2seg/{:016x}", !n);
            b.put(&key, b"f", When::Always).await.unwrap();
        }
        b.put("3snap/x", b"s", When::Always).await.unwrap();
        assert_eq!(
            b.first_key("2seg/").await.unwrap(),
            Some(format!("2seg/{:016x}", !3u64))
        );
    });
}

#[test]
fn a_delete_drops_the_key_and_deleting_again_is_fine() {
    let server = Server::start();
    let b = server.bucket();
    block_on(async {
        b.put("2seg/gone", b"x", When::Always).await.unwrap();
        b.delete("2seg/gone").await.unwrap();
        assert_eq!(b.get("2seg/gone", None).await.unwrap(), None);
        b.delete("2seg/gone").await.unwrap();
    });
}

/// A wrong secret is refused by the server, as a status — never as data.
#[test]
fn a_wrong_secret_is_refused() {
    let server = Server::start();
    let b = Bucket::new(
        server.addr.clone(),
        NAME,
        Credentials {
            access: ACCESS.into(),
            secret: "not-the-secret".into(),
            region: "us-east-1".into(),
        },
    );
    block_on(async {
        let err = b.get("anything", None).await.unwrap_err();
        assert!(
            matches!(err, wavedb_platform::Error::Status(403)),
            "{err:?}"
        );
    });
}
