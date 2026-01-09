//! AWS Signature Version 4 ([RFC 0070] §12) — what makes an object store
//! accept a request.
//!
//! Every request carries the SHA-256 of its body (`x-amz-content-sha256`) and
//! an `authorization` header: an HMAC-SHA256 over a *canonical* form of the
//! request, keyed by a signing key derived from the secret through four
//! chained HMACs (date, region, service, `aws4_request`). The primitives are
//! RustCrypto's (`sha2`, `hmac`) — a hash written by hand is where a subtle
//! bug passes every test and becomes a hole; the protocol around them is
//! this file.
//!
//! [RFC 0070]: ../../../../rfcs/0070-object-storage-backend-WIP.md

use core::fmt::Write as _;

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

/// What the signature is scoped to: the key pair and the region.
#[derive(Clone)]
pub struct Credentials {
    pub access: String,
    pub secret: String,
    pub region: String,
}

/// Never the secret: a derived `Debug` would print it into any log that
/// formats a bucket.
impl core::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Credentials")
            .field("access", &self.access)
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

/// The parts of a request the signature covers.
pub struct Canonical<'a> {
    pub method: &'a str,
    /// The path, already [`encode`]d (`/bucket/key`).
    pub path: &'a str,
    /// `(name, value)` query pairs, unencoded and in any order.
    pub query: &'a [(&'a str, &'a str)],
    /// `(lowercase name, value)` for every signed header, in any order.
    /// Must include `host`, `x-amz-content-sha256` and `x-amz-date`.
    pub headers: &'a [(&'a str, &'a str)],
    /// Hex SHA-256 of the body ([`hex_sha256`]).
    pub payload: &'a str,
}

/// The `authorization` header value for `request`, sent at `amz_date`
/// (`YYYYMMDDTHHMMSSZ`, [`amz_date`]).
#[must_use]
pub fn authorization(
    creds: &Credentials,
    request: &Canonical<'_>,
    amz_date: &str,
) -> String {
    let (canonical, signed) = canonical_request(request);
    let day = amz_date.get(..8).unwrap_or_default();
    let scope = format!("{day}/{}/s3/aws4_request", creds.region);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex_sha256(canonical.as_bytes())
    );
    let key = [day, &creds.region, "s3", "aws4_request"]
        .iter()
        .fold(format!("AWS4{}", creds.secret).into_bytes(), |key, part| {
            hmac(&key, part.as_bytes())
        });
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, \
         Signature={}",
        creds.access,
        hex(&hmac(&key, to_sign.as_bytes()))
    )
}

/// The canonical request and its `SignedHeaders` list.
fn canonical_request(r: &Canonical<'_>) -> (String, String) {
    let mut query: Vec<(String, String)> = r
        .query
        .iter()
        .map(|(k, v)| (encode(k, true), encode(v, true)))
        .collect();
    query.sort();
    let query: Vec<String> =
        query.into_iter().map(|(k, v)| format!("{k}={v}")).collect();
    let mut headers: Vec<(String, &str)> = r
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim()))
        .collect();
    headers.sort();
    let mut canonical_headers = String::new();
    for (k, v) in &headers {
        let _ = writeln!(canonical_headers, "{k}:{v}");
    }
    let signed: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();
    let signed = signed.join(";");
    let canonical = format!(
        "{}\n{}\n{}\n{canonical_headers}\n{signed}\n{}",
        r.method,
        r.path,
        query.join("&"),
        r.payload
    );
    (canonical, signed)
}

/// URI-encode `s` as SigV4 wants it: every byte but the unreserved set
/// (`A-Z a-z 0-9 - _ . ~`) as `%XX`, and `/` encoded only when `encode_slash`
/// (a path keeps it, a query value does not).
#[must_use]
pub fn encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'~' => out.push(char::from(b)),
            b'/' if !encode_slash => out.push('/'),
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// Hex SHA-256 of `bytes` — the `x-amz-content-sha256` of a body.
#[must_use]
pub fn hex_sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// `unix` seconds as `YYYYMMDDTHHMMSSZ`, UTC.
#[must_use]
pub fn amz_date(unix: u64) -> String {
    let (y, m, d) = civil(unix / 86_400);
    let secs = unix % 86_400;
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        secs / 3_600,
        secs / 60 % 60,
        secs % 60
    )
}

/// Days since 1970-01-01 as `(year, month, day)` — Hinnant's
/// `civil_from_days`, for the non-negative days a clock can report.
const fn civil(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    // HMAC accepts a key of any length, so this never fails.
    let Ok(mut mac) = <Hmac<Sha256> as KeyInit>::new_from_slice(key) else {
        return Vec::new();
    };
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        Canonical, Credentials, amz_date, authorization, encode, hex_sha256,
    };

    /// The key pair and date of AWS's published SigV4 examples for S3
    /// ("Signature Calculations for the Authorization Header: Transferring
    /// Payload in a Single Chunk").
    fn example() -> Credentials {
        Credentials {
            access: "AKIAIOSFODNN7EXAMPLE".into(),
            secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            region: "us-east-1".into(),
        }
    }

    const DATE: &str = "20130524T000000Z";

    fn empty() -> String {
        hex_sha256(b"")
    }

    /// AWS's "GET Object" example: a ranged read.
    #[test]
    fn the_published_get_object_example_signs_to_its_signature() {
        let payload = empty();
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", payload.as_str()),
            ("x-amz-date", DATE),
        ];
        let auth = authorization(
            &example(),
            &Canonical {
                method: "GET",
                path: "/test.txt",
                query: &[],
                headers: &headers,
                payload: &payload,
            },
            DATE,
        );
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/\
             us-east-1/s3/aws4_request, SignedHeaders=host;range;\
             x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e8\
             57bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// AWS's "GET Bucket (List Objects)" example: query parameters, sorted.
    #[test]
    fn the_published_list_objects_example_signs_to_its_signature() {
        let payload = empty();
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", payload.as_str()),
            ("x-amz-date", DATE),
        ];
        let auth = authorization(
            &example(),
            &Canonical {
                method: "GET",
                path: "/",
                query: &[("prefix", "J"), ("max-keys", "2")],
                headers: &headers,
                payload: &payload,
            },
            DATE,
        );
        assert!(
            auth.ends_with(
                "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670\
                 ed5711ef69dc6f7"
            ),
            "{auth}"
        );
    }

    #[test]
    fn dates_are_utc_calendar_dates() {
        assert_eq!(amz_date(0), "19700101T000000Z");
        assert_eq!(amz_date(1_369_353_600), DATE);
        assert_eq!(amz_date(951_782_400 + 3_661), "20000229T010101Z");
    }

    #[test]
    fn encoding_keeps_only_the_unreserved_set_and_path_slashes() {
        assert_eq!(encode("a/b c+~", false), "a/b%20c%2B~");
        assert_eq!(encode("a/b", true), "a%2Fb");
    }
}
