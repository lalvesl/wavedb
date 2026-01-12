//! The object-store client against a real S3 server ([RFC 0070]): the dev
//! shell's versitygw, one per test over its own directory (a directory under
//! its root is a bucket). Every behaviour the engine will lean on is pinned
//! here — the signature accepted, ranges cut at the end, the conditional
//! PUTs refused with `412`, the listing in key order, deletes idempotent.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

// The client's futures share the platform's current-thread stance.
#![allow(clippy::future_not_send)]

use wavedb_platform::carrier::block_on;
use wavedb_platform::s3::test_server::TestServer;
use wavedb_platform::s3::{Put, When};

/// A test server over its own directory, kept alive with it.
struct Server {
    server: TestServer,
    _dir: tempfile::TempDir,
}

impl Server {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        Self {
            server: TestServer::start(dir.path()),
            _dir: dir,
        }
    }

    fn bucket(&self) -> wavedb_platform::s3::Bucket {
        self.server.bucket()
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
            b.put("shards/07/1seg/fe", &data, When::Always)
                .await
                .unwrap(),
            Put::Stored { .. }
        ));
        let whole = b.get("shards/07/1seg/fe", None).await.unwrap();
        assert_eq!(whole.as_deref(), Some(&data[..]));
        let page = b.get("shards/07/1seg/fe", Some(4096..8192)).await;
        assert_eq!(page.unwrap().as_deref(), Some(&data[4096..8192]));
        let tail = b.get("shards/07/1seg/fe", Some(8192..20_000)).await;
        assert_eq!(
            tail.unwrap().as_deref(),
            Some(&data[8192..]),
            "a range past the end is cut at the end"
        );
        assert_eq!(b.get("shards/07/1seg/none", None).await.unwrap(), None);
    });
}

/// The journal and the lease rely on these: a key written only if absent,
/// and replaced only while it still holds the version read.
#[test]
fn conditional_puts_refuse_instead_of_overwriting() {
    let server = Server::start();
    let b = server.bucket();
    block_on(async {
        let Put::Stored { etag } =
            b.put("LOCK", b"7", When::Absent).await.unwrap()
        else {
            panic!("a new key must be stored");
        };
        assert_eq!(
            b.put("LOCK", b"8", When::Absent).await.unwrap(),
            Put::Refused
        );
        assert_eq!(
            b.put("LOCK", b"8", When::Holds("\"other\"".into()))
                .await
                .unwrap(),
            Put::Refused
        );
        assert_eq!(
            b.get("LOCK", None).await.unwrap().as_deref(),
            Some(&b"7"[..])
        );
        assert!(matches!(
            b.put("LOCK", b"8", When::Holds(etag)).await.unwrap(),
            Put::Stored { .. }
        ));
        assert_eq!(
            b.get("LOCK", None).await.unwrap().as_deref(),
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
        assert_eq!(b.first_key("1seg/").await.unwrap(), None);
        for n in 1..=3u64 {
            let key = format!("1seg/{:016x}", !n);
            b.put(&key, b"f", When::Always).await.unwrap();
        }
        b.put("2snap/x", b"s", When::Always).await.unwrap();
        assert_eq!(
            b.first_key("1seg/").await.unwrap(),
            Some(format!("1seg/{:016x}", !3u64))
        );
    });
}

#[test]
fn a_delete_drops_the_key_and_deleting_again_is_fine() {
    let server = Server::start();
    let b = server.bucket();
    block_on(async {
        b.put("1seg/gone", b"x", When::Always).await.unwrap();
        b.delete("1seg/gone").await.unwrap();
        assert_eq!(b.get("1seg/gone", None).await.unwrap(), None);
        b.delete("1seg/gone").await.unwrap();
    });
}

/// A wrong secret is refused by the server, as a status — never as data.
#[test]
fn a_wrong_secret_is_refused() {
    let server = Server::start();
    let b = server.server.bucket_signed_with("not-the-secret");
    block_on(async {
        let err = b.get("anything", None).await.unwrap_err();
        assert!(
            matches!(err, wavedb_platform::Error::Status(403)),
            "{err:?}"
        );
    });
}
