//! Minimal HTTP/1.1 framing for the **server half** of the dumb tunnel
//! (native only — a node is never a browser; the client half lives in
//! `wavedb_platform::http`).
//!
//! WaveDB uses no HTTP semantics: a request is `POST` + `content-length` +
//! the envelope bytes; a response is a `200` head with **no content-length**
//! followed by length-prefixed frames (`[len u32 LE][bytes]`, each a wire
//! [`StreamFrame`](crate::frame::StreamFrame)) written progressively — the
//! `connection: close` the tunnel always sends is what delimits the body.
//! The one other thing a peer may say is a **WebSocket upgrade** (`GET` +
//! `Upgrade: websocket`), which hands the socket to the WS transport (M7).
//! Anything else on the socket is a transport fault, not a protocol.
//! Identity, commands, and refusals never touch this layer — they live in
//! the [`frame`](crate::frame) envelopes.

use embedded_io_async::{Read, Write};
use wavedb_platform::io::IoError;

use crate::error::{Error, Result};
use crate::frames::MAX_BODY;

/// Cap on the head section (request/status line + headers).
const MAX_HEAD: usize = 8 * 1024;

/// A parsed head: the first line plus the headers the tunnel reads.
#[derive(Debug)]
struct Head {
    first_line: String,
    content_length: Option<usize>,
    /// `Sec-WebSocket-Key`, when the peer asks to upgrade.
    websocket_key: Option<String>,
}

/// One request off the socket: a POST body (the tunnel), or a WebSocket
/// upgrade offer to be answered with [`write_switching_head`].
#[derive(Debug)]
pub enum Incoming {
    /// A `POST`'s complete body bytes.
    Post(Vec<u8>),
    /// A `GET` + `Upgrade: websocket`; `key` is the `Sec-WebSocket-Key`.
    Upgrade {
        /// The handshake nonce [`write_switching_head`] answers.
        key: String,
    },
}

/// Split the head bytes into the first line + the read headers.
fn parse_head(bytes: &[u8]) -> Result<Head> {
    let text = core::str::from_utf8(bytes)
        .map_err(|_| Error::Http("head is not utf-8"))?;
    let mut lines = text.split("\r\n");
    let first_line = lines.next().ok_or(Error::Http("empty head"))?.to_owned();
    let mut content_length = None;
    let mut websocket_key = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            let n: usize = value
                .trim()
                .parse()
                .map_err(|_| Error::Http("bad content-length"))?;
            content_length = Some(n);
        } else if name.eq_ignore_ascii_case("sec-websocket-key") {
            websocket_key = Some(value.trim().to_owned());
        }
    }
    Ok(Head {
        first_line,
        content_length,
        websocket_key,
    })
}

/// Position of the `\r\n\r\n` head terminator, if present.
fn head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Read up to and including the blank line; return `(head, leftover)` where
/// `leftover` is any body bytes already pulled off the socket. `None` = the
/// peer closed before sending anything (a clean end of connection).
async fn read_head<R>(r: &mut R) -> Result<Option<(Head, Vec<u8>)>>
where
    R: Read<Error = IoError>,
{
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = head_end(&buf) {
            let leftover = buf.split_off(end + 4);
            buf.truncate(end);
            return Ok(Some((parse_head(&buf)?, leftover)));
        }
        if buf.len() > MAX_HEAD {
            return Err(Error::Http("head too large"));
        }
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(Error::Http("connection closed mid-head"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Read the declared body, reusing bytes already read past the head.
async fn read_body<R>(
    r: &mut R,
    declared: usize,
    mut leftover: Vec<u8>,
) -> Result<Vec<u8>>
where
    R: Read<Error = IoError>,
{
    if declared > MAX_BODY {
        return Err(Error::BodyTooLarge {
            limit: MAX_BODY,
            have: declared,
        });
    }
    if leftover.len() > declared {
        return Err(Error::Http("body longer than declared"));
    }
    let missing = declared - leftover.len();
    if missing > 0 {
        let start = leftover.len();
        leftover.resize(declared, 0);
        r.read_exact(&mut leftover[start..])
            .await
            .map_err(IoError::from)?;
    }
    Ok(leftover)
}

/// Server side: read one request — a `POST` body or a WebSocket upgrade
/// offer. `None` = the peer closed the connection instead of sending one.
pub async fn read_request<R>(r: &mut R) -> Result<Option<Incoming>>
where
    R: Read<Error = IoError>,
{
    let Some((head, leftover)) = read_head(r).await? else {
        return Ok(None);
    };
    if head.first_line.starts_with("POST ") {
        let declared = head
            .content_length
            .ok_or(Error::Http("missing content-length"))?;
        return Ok(Some(Incoming::Post(
            read_body(r, declared, leftover).await?,
        )));
    }
    if head.first_line.starts_with("GET ")
        && let Some(key) = head.websocket_key
    {
        // Frames may not be pipelined ahead of the server's 101.
        if leftover.is_empty() {
            return Ok(Some(Incoming::Upgrade { key }));
        }
        return Err(Error::Http("bytes before the upgrade completed"));
    }
    Err(Error::Http("only POST or websocket upgrade"))
}

/// Server side: write the `200` head that a frame sequence follows. No
/// `content-length` — the frames stream out as produced and
/// `connection: close` delimits the body.
///
/// `access-control-allow-origin: *` lets a browser page on any origin read
/// the tunnel's bytes. CORS is not a security boundary here: requests carry
/// no ambient credentials (identity is the signed token *inside* the body,
/// never a cookie or header), so the header only unblocks the browser
/// client — enforcement stays in the WaveDB gates.
pub async fn write_ok_head<W>(w: &mut W) -> Result<()>
where
    W: Write<Error = IoError>,
{
    w.write_all(
        b"HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\n\
          access-control-allow-origin: *\r\n\
          connection: close\r\n\r\n",
    )
    .await?;
    Ok(())
}

/// Server side: write one `[len u32 LE][bytes]` frame and flush, so the peer
/// sees it before the next one is produced.
pub async fn write_frame<W>(w: &mut W, bytes: &[u8]) -> Result<()>
where
    W: Write<Error = IoError>,
{
    let len = u32::try_from(bytes.len())
        .map_err(|_| Error::Http("frame too large"))?;
    w.write_all(&len.to_le_bytes()).await?;
    w.write_all(bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Server side: answer a WebSocket upgrade with the `101` head.
///
/// `accept` is [`accept_key`](wavedb_platform::ws::codec::accept_key) over
/// the offer's `Sec-WebSocket-Key`; the socket then speaks RFC 6455 frames.
///
/// # Errors
/// A socket fault.
pub async fn write_switching_head<W>(w: &mut W, accept: &str) -> Result<()>
where
    W: Write<Error = IoError>,
{
    let head = format!(
        "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
         connection: Upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n"
    );
    w.write_all(head.as_bytes()).await?;
    w.flush().await?;
    Ok(())
}

/// Server side: reject bytes that never became a WaveDB request. This is
/// the only non-200 the node sends — it means the *transport* broke.
pub async fn write_bad_request<W>(w: &mut W) -> Result<()>
where
    W: Write<Error = IoError>,
{
    w.write_all(
        b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\
          access-control-allow-origin: *\r\n\
          connection: close\r\n\r\n",
    )
    .await?;
    w.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use embedded_io_async::{Read, Write};
    use wavedb_platform::io::{IoError, pair};

    use super::{
        Error, Incoming, head_end, parse_head, read_request, write_frame,
        write_ok_head,
    };

    /// Unwrap a read request into its POST body.
    async fn read_post<R>(r: &mut R) -> super::Result<Option<Vec<u8>>>
    where
        R: Read<Error = IoError>,
    {
        match read_request(r).await? {
            Some(Incoming::Post(body)) => Ok(Some(body)),
            Some(Incoming::Upgrade { .. }) => panic!("unexpected upgrade"),
            None => Ok(None),
        }
    }

    #[test]
    fn parse_head_reads_first_line_and_length() {
        let head =
            parse_head(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 12\r\n")
                .unwrap();
        assert_eq!(head.first_line, "POST / HTTP/1.1");
        assert_eq!(head.content_length, Some(12));
        assert_eq!(head.websocket_key, None);
    }

    #[test]
    fn parse_head_rejects_bad_length() {
        let err = parse_head(b"POST / HTTP/1.1\r\ncontent-length: x\r\n")
            .unwrap_err();
        assert!(matches!(err, Error::Http("bad content-length")));
    }

    #[test]
    fn head_end_finds_terminator() {
        assert_eq!(head_end(b"a\r\n\r\nbody"), Some(1));
        assert_eq!(head_end(b"a\r\nb"), None);
    }

    #[test]
    fn post_body_and_frame_bytes_roundtrip_over_a_socket_pair() {
        wavedb_platform::carrier::block_on(async {
            let (mut client, mut server) = pair().await.unwrap();

            // Client writes a request by hand; the server side reads it.
            client
                .write_all(b"POST / HTTP/1.1\r\ncontent-length: 5\r\n\r\nhello")
                .await
                .unwrap();
            let body = read_post(&mut server).await.unwrap().unwrap();
            assert_eq!(body, b"hello");

            // Server answers with a head and two frames, then closes; the raw
            // bytes are exactly what `wavedb_net::frames::FrameReader` expects
            // (the client-side reassembly is tested there over a real socket).
            write_ok_head(&mut server).await.unwrap();
            write_frame(&mut server, b"one").await.unwrap();
            write_frame(&mut server, b"world").await.unwrap();
            drop(server);

            let mut raw = Vec::new();
            let mut chunk = [0u8; 256];
            loop {
                let n = client.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&chunk[..n]);
            }
            let mut expected = b"HTTP/1.1 200 OK\r\n\
              content-type: application/octet-stream\r\n\
              access-control-allow-origin: *\r\n\
              connection: close\r\n\r\n"
                .to_vec();
            expected.extend_from_slice(&3u32.to_le_bytes());
            expected.extend_from_slice(b"one");
            expected.extend_from_slice(&5u32.to_le_bytes());
            expected.extend_from_slice(b"world");
            assert_eq!(raw, expected);
        });
    }

    #[test]
    fn clean_close_reads_as_none() {
        wavedb_platform::carrier::block_on(async {
            let (client, mut server) = pair().await.unwrap();
            drop(client);
            assert!(read_post(&mut server).await.unwrap().is_none());
        });
    }

    #[test]
    fn plain_get_is_refused() {
        wavedb_platform::carrier::block_on(async {
            let (mut client, mut server) = pair().await.unwrap();
            client
                .write_all(b"GET / HTTP/1.1\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            let err = read_request(&mut server).await.unwrap_err();
            assert!(matches!(
                err,
                Error::Http("only POST or websocket upgrade")
            ));
        });
    }

    #[test]
    fn websocket_upgrade_surfaces_its_key() {
        wavedb_platform::carrier::block_on(async {
            let (mut client, mut server) = pair().await.unwrap();
            client
                .write_all(
                    b"GET / HTTP/1.1\r\nupgrade: websocket\r\n\
                  connection: Upgrade\r\n\
                  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                  sec-websocket-version: 13\r\n\r\n",
                )
                .await
                .unwrap();
            let Some(Incoming::Upgrade { key }) =
                read_request(&mut server).await.unwrap()
            else {
                panic!("must be an upgrade");
            };
            assert_eq!(key, "dGhlIHNhbXBsZSBub25jZQ==");
        });
    }

    #[test]
    fn frames_pipelined_before_the_101_are_refused() {
        wavedb_platform::carrier::block_on(async {
            let (mut client, mut server) = pair().await.unwrap();
            client
                .write_all(
                    b"GET / HTTP/1.1\r\nsec-websocket-key: abc\r\n\r\n\x82\x00",
                )
                .await
                .unwrap();
            let err = read_request(&mut server).await.unwrap_err();
            assert!(matches!(
                err,
                Error::Http("bytes before the upgrade completed")
            ));
        });
    }

    #[test]
    fn oversized_body_is_capped_before_reading() {
        wavedb_platform::carrier::block_on(async {
            let (mut client, mut server) = pair().await.unwrap();
            let head = format!(
                "POST / HTTP/1.1\r\ncontent-length: {}\r\n\r\n",
                super::MAX_BODY + 1
            );
            client.write_all(head.as_bytes()).await.unwrap();
            let err = read_post(&mut server).await.unwrap_err();
            assert!(matches!(err, Error::BodyTooLarge { .. }));
        });
    }
}
