//! The object-store client over HTTPS ([RFC 0070], open question 1): the dev
//! shell's versitygw serving TLS with a certificate for `127.0.0.1` from an
//! authority made for the test. Everything the plain client does works the
//! same through TLS — large bodies both ways, ranges, conditional PUTs, the
//! listing — and a certificate no trusted authority signed is refused.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

// The client's futures share the platform's current-thread stance.
#![allow(clippy::future_not_send)]

use wavedb_platform::carrier::block_on;
use wavedb_platform::s3::test_server::TestServer;
use wavedb_platform::s3::{Put, When};

fn bytes(n: usize) -> Vec<u8> {
    (0..n).map(|i| u8::try_from(i * 7 % 251).unwrap()).collect()
}

/// Many TLS records each way: a body of 3 MiB up and down, a range of it,
/// a conditional PUT refused, the listing, a delete.
#[test]
fn the_client_works_the_same_over_tls() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_tls(dir.path());
    let bucket = server.bucket();
    let body = bytes(3 << 20);
    block_on(async {
        let put = bucket.put("1seg/a", &body, When::Absent).await.unwrap();
        assert!(matches!(put, Put::Stored { .. }), "{put:?}");
        let again = bucket.put("1seg/a", b"x", When::Absent).await.unwrap();
        assert_eq!(again, Put::Refused);
        let got = bucket.get("1seg/a", None).await.unwrap();
        assert_eq!(got.as_deref(), Some(&body[..]));
        let range = bucket.get("1seg/a", Some(1_000..70_000)).await.unwrap();
        assert_eq!(range.as_deref(), Some(&body[1_000..70_000]));
        let first = bucket.first_key("1seg/").await.unwrap();
        assert_eq!(first.as_deref(), Some("1seg/a"));
        bucket.delete("1seg/a").await.unwrap();
        assert_eq!(bucket.get("1seg/a", None).await.unwrap(), None);
    });
}

/// Verification is on: the same server, its authority not trusted, is
/// refused before any request is made.
#[test]
fn a_certificate_no_trusted_authority_signed_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let server = TestServer::start_tls(dir.path());
    let untrusting = server.bucket_untrusting();
    let err = block_on(untrusting.get("anything", None)).unwrap_err();
    assert!(
        format!("{err}").contains("certificate"),
        "refused for its certificate: {err}"
    );
}
