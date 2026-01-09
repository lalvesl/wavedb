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
//! Path-style addressing (`/bucket/key`) over plain HTTP/1.1, one
//! connection per request, every request signed ([`sign`]). HTTPS to a
//! public endpoint is an open question of the RFC; the tests run against a
//! local server.
//!
//! [RFC 0070]: ../../../../rfcs/0070-object-storage-backend-WIP.md

mod exchange;
pub mod sign;

use core::fmt::Write as _;
use core::ops::Range;

use crate::error::{Error, Result};
use exchange::{Response, exchange};
pub use sign::Credentials;

/// A bucket on an S3-compatible endpoint.
#[derive(Debug, Clone)]
pub struct Bucket {
    /// `host:port` of the endpoint.
    addr: String,
    name: String,
    creds: Credentials,
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
    /// The bucket `name` at `addr` (`host:port`), signed with `creds`.
    #[must_use]
    pub fn new(
        addr: impl Into<String>,
        name: impl Into<String>,
        creds: Credentials,
    ) -> Self {
        Self {
            addr: addr.into(),
            name: name.into(),
            creds,
        }
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
            ("host", self.addr.as_str()),
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
        exchange(&self.addr, &head, body).await
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
    use super::{between, unescape};

    #[test]
    fn a_listing_yields_its_first_key_unescaped() {
        let xml = "<ListBucketResult><Contents><Key>2seg/a&amp;b</Key>\
                   </Contents><Contents><Key>2seg/z</Key></Contents>";
        assert_eq!(
            between(xml, "<Key>", "</Key>").map(unescape),
            Some("2seg/a&b".into())
        );
        assert_eq!(between("<ListBucketResult/>", "<Key>", "</Key>"), None);
    }
}
