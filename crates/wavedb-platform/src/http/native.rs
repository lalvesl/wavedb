//! Native `post`: hand-rolled HTTP/1.1 over a fresh
//! [`io::Stream`](crate::io::Stream).

use embedded_io_async::{Read, Write};

use crate::error::{Error, Result};
use crate::io::{self, Stream};

/// Cap on the response head (status line + headers).
const MAX_HEAD: usize = 8 * 1024;

/// POST `body` to `addr` (`host:port`) on a fresh connection and position
/// the [`Body`] at the response bytes, requiring a `200` head.
///
/// # Errors
/// [`Error::Status`] on a non-200 answer; otherwise socket faults or a
/// head that was not minimal HTTP/1.1.
pub async fn post(addr: &str, body: &[u8]) -> Result<Body> {
    let mut stream = io::connect(addr).await?;
    let head = format!(
        "POST / HTTP/1.1\r\nhost: {addr}\r\n\
         content-type: application/octet-stream\r\n\
         content-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    read_ok_head(stream).await
}

/// Read the response head up to its blank line, require a `200`, and wrap
/// any body bytes already pulled off the socket.
async fn read_ok_head(mut stream: Stream) -> Result<Body> {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = head_end(&buf) {
            let leftover = buf.split_off(end + 4);
            buf.truncate(end);
            require_200(&buf)?;
            return Ok(Body {
                stream,
                leftover: Some(leftover),
            });
        }
        if buf.len() > MAX_HEAD {
            return Err(Error::Http("head too large"));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::Http("connection closed before response"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Position of the `\r\n\r\n` head terminator, if present.
fn head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Require an `HTTP/1.1 200` status line at the top of `head`.
fn require_200(head: &[u8]) -> Result<()> {
    let text = core::str::from_utf8(head)
        .map_err(|_| Error::Http("head is not utf-8"))?;
    let first = text.split("\r\n").next().unwrap_or_default();
    let mut parts = first.split(' ');
    if parts.next() != Some("HTTP/1.1") {
        return Err(Error::Http("not http/1.1"));
    }
    let code: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or(Error::Http("bad status line"))?;
    if code == 200 {
        Ok(())
    } else {
        Err(Error::Status(code))
    }
}

/// A response body streaming in over the connection; the peer's close (the
/// tunnel's `connection: close`) delimits it.
#[derive(Debug)]
pub struct Body {
    stream: Stream,
    /// Bytes read past the head before the body was handed over.
    leftover: Option<Vec<u8>>,
}

impl Body {
    /// The next run of body bytes in arrival order; `None` = the peer
    /// closed (clean end of stream — mid-frame is the caller's judgement).
    ///
    /// # Errors
    /// A socket fault.
    pub async fn chunk(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(bytes) = self.leftover.take()
            && !bytes.is_empty()
        {
            return Ok(Some(bytes));
        }
        let mut buf = vec![0u8; 4096];
        let n = self.stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.truncate(n);
        Ok(Some(buf))
    }
}

#[cfg(test)]
mod tests {
    use embedded_io_async::{Read, Write};

    use super::{Error, post};
    use crate::io::Listener;

    /// Serve one canned response on a fresh port, consuming the request.
    fn one_shot(response: &'static [u8]) -> String {
        let listener = Listener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            crate::carrier::block_on(async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut sink = [0u8; 1024];
                let _ = sock.read(&mut sink).await;
                sock.write_all(response).await.unwrap();
            });
        });
        addr
    }

    #[test]
    fn post_streams_the_body_across_chunks() {
        crate::carrier::block_on(async {
            let addr = one_shot(
                b"HTTP/1.1 200 OK\r\nconnection: close\r\n\r\nhello world",
            );
            let mut body = post(&addr, b"req").await.unwrap();
            let mut got = Vec::new();
            while let Some(chunk) = body.chunk().await.unwrap() {
                got.extend_from_slice(&chunk);
            }
            assert_eq!(got, b"hello world");
        });
    }

    #[test]
    fn non_200_is_a_status_error() {
        crate::carrier::block_on(async {
            let addr = one_shot(
                b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n",
            );
            let err = post(&addr, b"req").await.unwrap_err();
            assert!(matches!(err, Error::Status(404)));
        });
    }

    #[test]
    fn close_before_any_response_is_a_fault() {
        crate::carrier::block_on(async {
            let addr = one_shot(b"");
            let err = post(&addr, b"req").await.unwrap_err();
            assert!(matches!(
                err,
                Error::Http("connection closed before response")
            ));
        });
    }
}
