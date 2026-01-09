//! One object-store request on a fresh connection: write the head and the
//! body, read the status, the two headers the client uses (`content-length`,
//! `etag`) and the body. Hand-rolled HTTP/1.1 over [`io::connect`], like the
//! tunnel's client half — `connection: close`, so the peer's close also ends
//! a body that names no length.

use embedded_io_async::{Read, Write};

use crate::error::{Error, Result};
use crate::io;

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
/// `body` to `addr`, and read the whole response.
///
/// # Errors
/// Socket faults, or a response that is not minimal HTTP/1.1.
pub async fn exchange(addr: &str, head: &str, body: &[u8]) -> Result<Response> {
    let mut stream = io::connect(addr).await?;
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
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            if head.length.is_some() {
                return Err(Error::Http("object store body cut short"));
            }
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    if let Some(len) = head.length {
        body.truncate(len);
    }
    Ok(Response {
        status: head.status,
        etag: head.etag,
        body,
    })
}

/// The parts of a response head the client reads.
#[derive(Debug, PartialEq, Eq)]
struct Head {
    status: u16,
    length: Option<usize>,
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
        let (mut length, mut etag) = (None, None);
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
            } else if name.eq_ignore_ascii_case("etag") {
                etag = Some(value.to_owned());
            }
        }
        Ok(Self {
            status,
            length,
            etag,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Head;

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
}
