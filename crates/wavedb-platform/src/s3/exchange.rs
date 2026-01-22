//! One object-store request on a fresh connection: write the head and the
//! body, read the status, the headers the client uses (`content-length`,
//! `transfer-encoding`, `etag`) and the body. Hand-rolled HTTP/1.1 over
//! [`io::connect`], like the tunnel's client half, or over TLS to an
//! `https://` endpoint (`super::tls`) — `connection: close`, so the peer's
//! close also ends a body that names no length.
//!
//! A body is framed by its `content-length`, by chunks (`transfer-encoding:
//! chunked`, which public endpoints send for listings), or by the close.

use embedded_io_async::{ErrorType, Read, Write};

use super::Endpoint;
use super::tls;
use crate::error::{Error, Result};
use crate::io::{self, IoError};

/// Cap on a response head (status line + headers).
const MAX_HEAD: usize = 16 * 1024;

/// What came back.
#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub etag: Option<String>,
    pub body: Vec<u8>,
}

/// Send `head` (status line and headers, ending in the blank line) and
/// `body` to `endpoint`, and read the whole response.
///
/// # Errors
/// Socket and TLS faults, or a response that is not minimal HTTP/1.1.
pub async fn exchange(
    endpoint: &Endpoint,
    head: &str,
    body: &[u8],
) -> Result<Response> {
    match &endpoint.tls {
        None => {
            let stream = io::connect(&endpoint.addr).await?;
            talk(stream, head, body).await
        }
        Some(Ok(config)) => {
            let stream =
                tls::connect(&endpoint.addr, &endpoint.name, config.clone())
                    .await?;
            talk(stream, head, body).await
        }
        Some(Err(fault)) => Err(Error::Http(fault)),
    }
}

/// The request and its response over `stream`.
async fn talk<S>(mut stream: S, head: &str, body: &[u8]) -> Result<Response>
where
    S: Read + Write + ErrorType<Error = IoError>,
{
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = vec![0u8; 64 * 1024];
    let end = loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
        if buf.len() > MAX_HEAD {
            return Err(Error::Http("object store head too large"));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::Http("object store closed before answering"));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let mut body = buf.split_off(end + 4);
    buf.truncate(end);
    let head = Head::parse(&buf)?;
    loop {
        if head.length.is_some_and(|len| body.len() >= len) {
            break;
        }
        if head.chunked && dechunk(&body)?.is_some() {
            break;
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            if head.length.is_some() || head.chunked {
                return Err(Error::Http("object store body cut short"));
            }
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    if let Some(len) = head.length {
        body.truncate(len);
    } else if head.chunked {
        body = dechunk(&body)?.unwrap_or_default();
    }
    Ok(Response {
        status: head.status,
        etag: head.etag,
        body,
    })
}

/// A chunked body's content, once `raw` holds all of it (the last, empty
/// chunk and the trailers after it); `None` while it does not yet.
///
/// # Errors
/// [`Error::Http`] for a chunk size that is not hexadecimal, or data not
/// followed by its line break.
fn dechunk(raw: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut out = Vec::new();
    let mut at = 0;
    loop {
        let Some(eol) = find_crlf(raw, at) else {
            return Ok(None);
        };
        let line = core::str::from_utf8(&raw[at..eol])
            .map_err(|_| Error::Http("object store chunk size"))?;
        let size = line.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size, 16)
            .map_err(|_| Error::Http("object store chunk size"))?;
        at = eol + 2;
        if size == 0 {
            // Trailers, if any, end with an empty line.
            loop {
                let Some(eol) = find_crlf(raw, at) else {
                    return Ok(None);
                };
                if eol == at {
                    return Ok(Some(out));
                }
                at = eol + 2;
            }
        }
        if raw.len() < at + size + 2 {
            return Ok(None);
        }
        out.extend_from_slice(&raw[at..at + size]);
        if &raw[at + size..at + size + 2] != b"\r\n" {
            return Err(Error::Http("object store chunk not ended"));
        }
        at += size + 2;
    }
}

/// The position of the first `\r\n` at or after `from`.
fn find_crlf(raw: &[u8], from: usize) -> Option<usize> {
    raw.get(from..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|i| from + i)
}

/// The parts of a response head the client reads.
#[derive(Debug, PartialEq, Eq)]
struct Head {
    status: u16,
    length: Option<usize>,
    chunked: bool,
    etag: Option<String>,
}

impl Head {
    fn parse(head: &[u8]) -> Result<Self> {
        let text = core::str::from_utf8(head)
            .map_err(|_| Error::Http("object store head is not utf-8"))?;
        let mut lines = text.split("\r\n");
        let mut status_line = lines.next().unwrap_or_default().split(' ');
        if !status_line.next().is_some_and(|v| v.starts_with("HTTP/1.")) {
            return Err(Error::Http("object store did not answer http/1.x"));
        }
        let status = status_line
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or(Error::Http("object store status line"))?;
        let (mut length, mut chunked, mut etag) = (None, false, None);
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                length =
                    Some(value.parse().map_err(|_| {
                        Error::Http("object store content-length")
                    })?);
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                chunked = value.split(',').any(|coding| {
                    coding.trim().eq_ignore_ascii_case("chunked")
                });
            } else if name.eq_ignore_ascii_case("etag") {
                etag = Some(value.to_owned());
            }
        }
        // A chunked body names its length chunk by chunk (RFC 9112 §6.3).
        if chunked {
            length = None;
        }
        Ok(Self {
            status,
            length,
            chunked,
            etag,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Head, dechunk};

    #[test]
    fn a_head_gives_its_status_length_and_etag() {
        let head = Head::parse(
            b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4096\r\n\
              ETag: \"abc\"\r\nx-other: 1",
        )
        .unwrap();
        assert_eq!(
            head,
            Head {
                status: 206,
                length: Some(4096),
                chunked: false,
                etag: Some("\"abc\"".into()),
            }
        );
    }

    #[test]
    fn a_head_that_is_not_http_is_refused() {
        assert!(Head::parse(b"SSH-2.0 hello").is_err());
        assert!(Head::parse(b"HTTP/1.1 abc").is_err());
        assert!(
            Head::parse(b"HTTP/1.1 200 OK\r\ncontent-length: x").is_err(),
            "a length that is not a number"
        );
    }

    #[test]
    fn a_chunked_head_names_no_length() {
        let head = Head::parse(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\
              Content-Length: 12",
        )
        .unwrap();
        assert!(head.chunked);
        assert_eq!(head.length, None);
    }

    /// The chunks joined, once the last one and the trailers are in — and
    /// nothing before.
    #[test]
    fn a_chunked_body_is_whole_only_at_its_end() {
        let raw = b"4;ext=1\r\n<Lis\r\nb\r\ntBucket/>..\r\n0\r\nx-t: 1\r\n\r\n";
        let body = dechunk(raw).unwrap().unwrap();
        assert_eq!(body, b"<ListBucket/>..");
        for cut in 0..raw.len() {
            assert_eq!(dechunk(&raw[..cut]).unwrap(), None, "cut at {cut}");
        }
        assert!(dechunk(b"zz\r\n").is_err(), "not hexadecimal");
        assert!(dechunk(b"2\r\nabcd\r\n").is_err(), "data overruns its size");
    }
}
