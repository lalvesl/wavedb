//! The object-store client half ([RFC 0070]) — native only: what a node needs
//! of an S3-compatible bucket, and nothing more.
//!
//! - [`Bucket::get`] — an object, or a byte range of one (`None` = absent);
//! - [`Bucket::put`] — store an object, optionally only if the key is absent
//!   or still holds a known version (`412` comes back as [`Put::Refused`]);
//! - [`Bucket::delete`] — drop a key (absent already is fine);
//! - [`Bucket::first_key`] — the smallest key under a prefix, which is how
//!   the newest file is found (the names are complemented, RFC 0070 §4).
//!
//! Path-style addressing (`/bucket/key`) over HTTP/1.1, one connection per
//! request, every request signed ([`sign`]). An `https://` endpoint speaks
//! TLS (`tls`): the certificate must chain to Mozilla's roots, or to one the
//! caller trusts ([`Bucket::trusting`]); `http://` or a bare `host:port` is
//! plain HTTP, for a local server.
//!
//! [RFC 0070]: ../../../../rfcs/0070-object-storage-backend.md

mod exchange;
pub mod sign;
#[cfg(feature = "test-server")]
pub mod test_server;
mod tls;

use core::fmt::Write as _;
use core::ops::Range;
use std::sync::Arc;

use rustls::ClientConfig;

use crate::error::{Error, Result};
use exchange::{Response, exchange};
pub use sign::Credentials;

/// A bucket on an S3-compatible endpoint.
#[derive(Debug, Clone)]
pub struct Bucket {
    endpoint: Endpoint,
    name: String,
    creds: Credentials,
    /// Certificates trusted besides Mozilla's roots, DER.
    trusted: Vec<Vec<u8>>,
}

/// Where a bucket's endpoint is, and how to speak to it.
#[derive(Debug, Clone)]
struct Endpoint {
    /// `host:port` to connect to.
    addr: String,
    /// The `host` header: the endpoint as given, its port too if named.
    host: String,
    /// The name the certificate must carry, over TLS: the host alone.
    name: String,
    /// `None` for plain HTTP; for HTTPS the client configuration, or why
    /// there is none (a certificate given to trust that is not one).
    tls: Option<core::result::Result<Arc<ClientConfig>, &'static str>>,
}

impl Endpoint {
    /// `https://host[:port]`, `http://host[:port]`, or a bare `host:port`
    /// (plain HTTP, as a local test server is reached).
    fn parse(endpoint: &str, trusted: &[Vec<u8>]) -> Self {
        let plain = || {
            endpoint
                .strip_prefix("http://")
                .map_or((false, endpoint, 80), |rest| (false, rest, 80))
        };
        let (secure, rest, port) = endpoint
            .strip_prefix("https://")
            .map_or_else(plain, |rest| (true, rest, 443));
        let rest = rest.trim_end_matches('/');
        let named_port = rest.rsplit_once(':').filter(|(host, port)| {
            !host.is_empty()
                && !port.is_empty()
                && port.bytes().all(|b| b.is_ascii_digit())
        });
        let (addr, name) = match named_port {
            Some((name, _)) => (rest.to_owned(), name),
            None => (format!("{rest}:{port}"), rest),
        };
        Self {
            addr,
            host: rest.to_owned(),
            name: name
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned(),
            tls: secure.then(|| tls_config(trusted)),
        }
    }
}

/// The TLS configuration trusting `trusted` besides Mozilla's roots, or
/// why there is none.
fn tls_config(
    trusted: &[Vec<u8>],
) -> core::result::Result<Arc<ClientConfig>, &'static str> {
    tls::config(trusted).map_err(|e| match e {
        Error::Http(why) => why,
        _ => "TLS configuration",
    })
}

/// When a [`put`](Bucket::put) may store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum When {
    /// Unconditionally.
    Always,
    /// Only if the key does not exist (`If-None-Match: *`).
    Absent,
    /// Only if the key still holds this version (`If-Match: <etag>`).
    Holds(String),
}

/// How a [`put`](Bucket::put) went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Put {
    /// Stored; the new version's ETag.
    Stored { etag: String },
    /// The condition did not hold (`412`); nothing was written.
    Refused,
}

impl Bucket {
    /// The bucket `name` at `endpoint` — `https://host[:port]`, or
    /// `http://host[:port]` or a bare `host:port` for plain HTTP — signed
    /// with `creds`.
    #[must_use]
    pub fn new(
        endpoint: impl AsRef<str>,
        name: impl Into<String>,
        creds: Credentials,
    ) -> Self {
        Self {
            endpoint: Endpoint::parse(endpoint.as_ref(), &[]),
            name: name.into(),
            creds,
            trusted: Vec::new(),
        }
    }

    /// Trust `der`, a certificate, as a root besides Mozilla's — for an
    /// endpoint whose certificate a private authority signed. A certificate
    /// that is not one fails every request, with [`Error::Http`].
    #[must_use]
    pub fn trusting(mut self, der: Vec<u8>) -> Self {
        self.trusted.push(der);
        if self.endpoint.tls.is_some() {
            self.endpoint.tls = Some(tls_config(&self.trusted));
        }
        self
    }

    /// `key`'s bytes, or the `range` of them; `None` when the key is absent.
    ///
    /// A range running past the end comes back cut at the end.
    ///
    /// # Errors
    /// [`Error::Status`] for any answer but `200`, `206` or `404`; socket
    /// and protocol faults.
    pub async fn get(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<Vec<u8>>> {
        let range = range
            .map(|r| format!("bytes={}-{}", r.start, r.end.saturating_sub(1)));
        let extra: Vec<(&str, &str)> =
            range.as_deref().map(|r| ("range", r)).into_iter().collect();
        let r = self.send("GET", key, &[], &extra, &[]).await?;
        match r.status {
            200 | 206 => Ok(Some(r.body)),
            404 => Ok(None),
            code => Err(Error::Status(code)),
        }
    }

    /// Store `body` at `key` when `when` holds.
    ///
    /// # Errors
    /// [`Error::Status`] for any answer but `200` or `412`; socket and
    /// protocol faults.
    pub async fn put(&self, key: &str, body: &[u8], when: When) -> Result<Put> {
        let extra: Vec<(&str, &str)> = match &when {
            When::Always => Vec::new(),
            When::Absent => vec![("if-none-match", "*")],
            When::Holds(etag) => vec![("if-match", etag.as_str())],
        };
        let r = self.send("PUT", key, &[], &extra, body).await?;
        match r.status {
            200 => Ok(Put::Stored {
                etag: r.etag.unwrap_or_default(),
            }),
            412 => Ok(Put::Refused),
            code => Err(Error::Status(code)),
        }
    }

    /// Delete `key`; deleting an absent key succeeds.
    ///
    /// # Errors
    /// [`Error::Status`] for any answer but `200`, `204` or `404`; socket
    /// and protocol faults.
    pub async fn delete(&self, key: &str) -> Result<()> {
        let r = self.send("DELETE", key, &[], &[], &[]).await?;
        match r.status {
            200 | 204 | 404 => Ok(()),
            code => Err(Error::Status(code)),
        }
    }

    /// The smallest key under `prefix`, if any (one `ListObjectsV2` with
    /// `max-keys=1`).
    ///
    /// # Errors
    /// [`Error::Status`] for any answer but `200`; socket and protocol
    /// faults, or a listing that is not XML naming a key.
    pub async fn first_key(&self, prefix: &str) -> Result<Option<String>> {
        let query = [("list-type", "2"), ("max-keys", "1"), ("prefix", prefix)];
        let r = self.send("GET", "", &query, &[], &[]).await?;
        if r.status != 200 {
            return Err(Error::Status(r.status));
        }
        let text = core::str::from_utf8(&r.body)
            .map_err(|_| Error::Http("object listing is not utf-8"))?;
        Ok(between(text, "<Key>", "</Key>").map(unescape))
    }

    /// Sign and send one request: `key` empty addresses the bucket itself.
    async fn send(
        &self,
        method: &str,
        key: &str,
        query: &[(&str, &str)],
        extra: &[(&str, &str)],
        body: &[u8],
    ) -> Result<Response> {
        let path = if key.is_empty() {
            format!("/{}", sign::encode(&self.name, true))
        } else {
            format!(
                "/{}/{}",
                sign::encode(&self.name, true),
                sign::encode(key, false)
            )
        };
        let payload = sign::hex_sha256(body);
        let date = sign::amz_date(crate::time::unix_secs());
        let mut headers = vec![
            ("host", self.endpoint.host.as_str()),
            ("x-amz-content-sha256", payload.as_str()),
            ("x-amz-date", date.as_str()),
        ];
        headers.extend_from_slice(extra);
        let auth = sign::authorization(
            &self.creds,
            &sign::Canonical {
                method,
                path: &path,
                query,
                headers: &headers,
                payload: &payload,
            },
            &date,
        );
        let mut head = format!("{method} {path}");
        for (i, (k, v)) in query.iter().enumerate() {
            let sep = if i == 0 { '?' } else { '&' };
            let _ = write!(
                head,
                "{sep}{}={}",
                sign::encode(k, true),
                sign::encode(v, true)
            );
        }
        head.push_str(" HTTP/1.1\r\n");
        for (k, v) in &headers {
            let _ = write!(head, "{k}: {v}\r\n");
        }
        let _ = write!(
            head,
            "authorization: {auth}\r\ncontent-length: {}\r\n\
             connection: close\r\n\r\n",
            body.len()
        );
        exchange(&self.endpoint, &head, body).await
    }
}

/// The text between the first `open` and the `close` after it.
fn between<'a>(text: &'a str, open: &str, close: &str) -> Option<&'a str> {
    let start = text.find(open)? + open.len();
    let len = text.get(start..)?.find(close)?;
    text.get(start..start + len)
}

/// XML's five predefined entities, the only escapes a key can carry.
fn unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::{Bucket, Credentials, Endpoint, between, unescape};

    #[test]
    fn a_listing_yields_its_first_key_unescaped() {
        let xml = "<ListBucketResult><Contents><Key>1seg/a&amp;b</Key>\
                   </Contents><Contents><Key>1seg/z</Key></Contents>";
        assert_eq!(
            between(xml, "<Key>", "</Key>").map(unescape),
            Some("1seg/a&b".into())
        );
        assert_eq!(between("<ListBucketResult/>", "<Key>", "</Key>"), None);
    }

    /// Where to connect, what the `host` header says, the name the
    /// certificate must carry, and whether it is TLS at all.
    #[test]
    fn an_endpoint_names_its_address_host_and_scheme() {
        let parts = |e: &str| {
            let p = Endpoint::parse(e, &[]);
            (p.addr, p.host, p.name, p.tls.is_some())
        };
        let s = String::from;
        assert_eq!(
            parts("127.0.0.1:7070"),
            (
                s("127.0.0.1:7070"),
                s("127.0.0.1:7070"),
                s("127.0.0.1"),
                false
            )
        );
        assert_eq!(
            parts("https://s3.us-east-1.amazonaws.com/"),
            (
                s("s3.us-east-1.amazonaws.com:443"),
                s("s3.us-east-1.amazonaws.com"),
                s("s3.us-east-1.amazonaws.com"),
                true
            )
        );
        assert_eq!(
            parts("https://minio.local:9000"),
            (
                s("minio.local:9000"),
                s("minio.local:9000"),
                s("minio.local"),
                true
            )
        );
        assert_eq!(
            parts("http://[::1]:9000"),
            (s("[::1]:9000"), s("[::1]:9000"), s("::1"), false)
        );
        assert_eq!(parts("https://[::1]").0, "[::1]:443");
    }

    /// A certificate to trust that is not one fails the configuration — and
    /// every request after — rather than being skipped.
    #[test]
    fn a_trusted_certificate_that_is_not_one_is_refused() {
        let creds = Credentials {
            access: "a".into(),
            secret: "s".into(),
            region: "us-east-1".into(),
        };
        let bucket =
            Bucket::new("https://example.com", "b", creds).trusting(vec![1, 2]);
        assert!(matches!(bucket.endpoint.tls, Some(Err(_))));
    }
}
