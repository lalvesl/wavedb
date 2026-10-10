//! Hand-rolled RFC 6455 framing — native only.
//!
//! The browser's `WebSocket` object frames itself, so this is native
//! only; it is shared by the native client half
//! ([`ws::connect`](super::connect)) and the server half in
//! `wavedb-net::ws`.
//!
//! WaveDB uses none of the protocol's optional surface: **binary messages
//! only** (a text frame is a protocol fault), no extensions, no
//! subprotocols. Control frames are handled for liveness — ping is
//! answered, close ends the stream — and fragmented messages reassemble
//! (a browser may fragment a large send). Per the RFC, client→server
//! frames are masked and server→client frames are not; the server side
//! passes `require_masked = true` and refuses bare frames.

use embedded_io_async::{Read, Write};
use sha1::{Digest, Sha1};

use crate::error::{Error, Result};
use crate::io::IoError;

/// Cap on one reassembled message — mirrors the tunnel's body cap.
pub const MAX_MESSAGE: usize = 16 * 1024 * 1024;

/// Frame opcodes (RFC 6455 §5.2) — only the ones the codec speaks.
const OP_CONTINUATION: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
/// The one data opcode WaveDB sends.
pub const OP_BINARY: u8 = 0x2;
/// Connection close.
pub const OP_CLOSE: u8 = 0x8;
/// Liveness probe — answered with a pong carrying the same payload.
pub const OP_PING: u8 = 0x9;
/// A ping's answer.
pub const OP_PONG: u8 = 0xA;

/// The 7-bit length value meaning "a 16-bit length follows".
const LEN_16: u8 = 0x7E;
/// The 7-bit length value meaning "a 64-bit length follows".
const LEN_64: u8 = 0x7F;

/// One decoded message, control frames included — the caller decides how
/// to answer a ping (it owns the write half).
#[derive(Debug, PartialEq, Eq)]
pub enum Msg {
    /// A complete (reassembled) binary message.
    Binary(Vec<u8>),
    /// A liveness probe; answer with [`OP_PONG`] + the same payload.
    Ping(Vec<u8>),
    /// A ping's answer — ignorable (nothing here sends pings).
    Pong(Vec<u8>),
    /// The peer is closing; nothing meaningful follows.
    Close,
}

/// Standard base64 (RFC 4648, padded) — encode only, which is all the
/// handshake needs. ~20 lines beats a dependency.
#[must_use]
pub fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n =
            (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                let idx = (n >> (18 - 6 * i)) & 0x3F;
                out.push(char::from(ALPHABET[idx as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The `Sec-WebSocket-Accept` value for a handshake `key` (RFC 6455 §1.3):
/// base64 of SHA-1 over the key concatenated with the RFC's fixed GUID.
#[must_use]
pub fn accept_key(key: &str) -> String {
    let mut h = Sha1::new();
    h.update(key.as_bytes());
    h.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64(&h.finalize())
}

/// Write one unfragmented frame and flush. `masked` draws a fresh 4-byte
/// mask from platform entropy (the client side); the server writes bare.
///
/// # Errors
/// A socket fault, or a payload past `u64` (unreachable in practice).
pub async fn write_message<W>(
    w: &mut W,
    opcode: u8,
    payload: &[u8],
    masked: bool,
) -> Result<()>
where
    W: Write<Error = IoError>,
{
    let mut head = Vec::with_capacity(14);
    head.push(0x80 | opcode); // FIN — outgoing messages never fragment.
    let mask_bit = if masked { 0x80 } else { 0x00 };
    if let Ok(len @ ..=125) = u8::try_from(payload.len()) {
        // Fits the 7-bit length — the length IS the byte.
        head.push(mask_bit | len);
    } else if let Ok(len) = u16::try_from(payload.len()) {
        head.push(mask_bit | LEN_16);
        head.extend_from_slice(&len.to_be_bytes());
    } else {
        let len = u64::try_from(payload.len())
            .map_err(|_| Error::Http("ws frame too large"))?;
        head.push(mask_bit | LEN_64);
        head.extend_from_slice(&len.to_be_bytes());
    }
    if masked {
        let mut key = [0u8; 4];
        crate::rand::fill(&mut key)?;
        head.extend_from_slice(&key);
        w.write_all(&head).await?;
        let mut body = payload.to_vec();
        for (i, b) in body.iter_mut().enumerate() {
            *b ^= key[i % 4];
        }
        w.write_all(&body).await?;
    } else {
        w.write_all(&head).await?;
        w.write_all(payload).await?;
    }
    w.flush().await?;
    Ok(())
}

/// An incremental message reader over a connection's read half, holding
/// the partial state a fragmented message needs across control frames.
#[derive(Debug)]
pub struct Messages<R> {
    r: R,
    /// Accumulated fragments of an unfinished binary message; `None` = not
    /// mid-message.
    partial: Option<Vec<u8>>,
}

impl<R> Messages<R>
where
    R: Read<Error = IoError>,
{
    /// Wrap a read half positioned at the frame stream (past the
    /// handshake).
    #[must_use]
    pub const fn new(r: R) -> Self {
        Self { r, partial: None }
    }

    /// Read one frame's payload, unmasking when the mask bit is set.
    /// Returns `(fin, opcode, payload)`; `None` = the peer closed cleanly
    /// between frames.
    async fn frame(
        &mut self,
        require_masked: bool,
    ) -> Result<Option<(bool, u8, Vec<u8>)>> {
        let mut first = [0u8; 1];
        if self.r.read(&mut first).await? == 0 {
            return Ok(None);
        }
        let b0 = first[0];
        if b0 & 0x70 != 0 {
            return Err(Error::Http("ws reserved bits set"));
        }
        let mut b1 = [0u8; 1];
        self.r.read_exact(&mut b1).await.map_err(IoError::from)?;
        let masked = b1[0] & 0x80 != 0;
        if require_masked && !masked {
            return Err(Error::Http("unmasked client frame"));
        }
        let len = match b1[0] & 0x7F {
            LEN_16 => {
                let mut n = [0u8; 2];
                self.r.read_exact(&mut n).await.map_err(IoError::from)?;
                usize::from(u16::from_be_bytes(n))
            }
            LEN_64 => {
                let mut n = [0u8; 8];
                self.r.read_exact(&mut n).await.map_err(IoError::from)?;
                usize::try_from(u64::from_be_bytes(n))
                    .map_err(|_| Error::Http("ws frame too large"))?
            }
            n => usize::from(n),
        };
        if len > MAX_MESSAGE {
            return Err(Error::Http("ws frame too large"));
        }
        let key = if masked {
            let mut k = [0u8; 4];
            self.r.read_exact(&mut k).await.map_err(IoError::from)?;
            Some(k)
        } else {
            None
        };
        let mut payload = vec![0u8; len];
        self.r
            .read_exact(&mut payload)
            .await
            .map_err(IoError::from)?;
        if let Some(key) = key {
            for (i, b) in payload.iter_mut().enumerate() {
                *b ^= key[i % 4];
            }
        }
        Ok(Some((b0 & 0x80 != 0, b0 & 0x0F, payload)))
    }

    /// The next message: binary messages reassemble across continuation
    /// frames (control frames may interleave and surface immediately);
    /// `None` = the peer closed the socket cleanly between frames.
    ///
    /// # Errors
    /// A socket fault, a text frame, a protocol violation (stray
    /// continuation, reserved bits, an unmasked frame when
    /// `require_masked`), or a message past [`MAX_MESSAGE`].
    pub async fn next(&mut self, require_masked: bool) -> Result<Option<Msg>> {
        loop {
            let Some((fin, opcode, payload)) =
                self.frame(require_masked).await?
            else {
                return Ok(None);
            };
            match opcode {
                OP_BINARY => {
                    if self.partial.is_some() {
                        return Err(Error::Http("ws data frame mid-message"));
                    }
                    if fin {
                        return Ok(Some(Msg::Binary(payload)));
                    }
                    self.partial = Some(payload);
                }
                OP_CONTINUATION => {
                    let Some(mut sofar) = self.partial.take() else {
                        return Err(Error::Http("ws stray continuation"));
                    };
                    if sofar.len() + payload.len() > MAX_MESSAGE {
                        return Err(Error::Http("ws frame too large"));
                    }
                    sofar.extend_from_slice(&payload);
                    if fin {
                        return Ok(Some(Msg::Binary(sofar)));
                    }
                    self.partial = Some(sofar);
                }
                OP_PING => return Ok(Some(Msg::Ping(payload))),
                OP_PONG => return Ok(Some(Msg::Pong(payload))),
                OP_CLOSE => return Ok(Some(Msg::Close)),
                OP_TEXT => return Err(Error::Http("ws text frame")),
                _ => return Err(Error::Http("ws unknown opcode")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_MESSAGE, Messages, Msg, OP_BINARY, OP_PING, accept_key, base64,
        write_message,
    };
    use crate::error::Error;
    use crate::io::pair;

    #[test]
    fn base64_alignment_vectors() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"M"), "TQ==");
        assert_eq!(base64(b"Ma"), "TWE=");
        assert_eq!(base64(b"Man"), "TWFu");
        assert_eq!(base64(b"Many hands"), "TWFueSBoYW5kcw==");
    }

    #[test]
    fn accept_key_matches_the_rfc_worked_example() {
        // RFC 6455 §1.3.
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[tokio::test]
    async fn masked_roundtrip_across_every_length_form() {
        // Sizes straddling the 7-bit / 16-bit / 64-bit length encodings.
        const SIZES: [usize; 6] = [0, 5, 125, 126, 65_535, 65_536];
        let (mut client, server) = pair().await.unwrap();
        // Write from a task: the larger payloads exceed the socket buffers, so
        // the writer must make progress against a concurrent reader.
        let writer = tokio::spawn(async move {
            for len in SIZES {
                write_message(&mut client, OP_BINARY, &vec![0xA5u8; len], true)
                    .await
                    .unwrap();
            }
        });
        let mut msgs = Messages::new(server);
        for len in SIZES {
            let msg = msgs.next(true).await.unwrap().unwrap();
            assert_eq!(msg, Msg::Binary(vec![0xA5u8; len]));
        }
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn unmasked_frame_refused_when_masking_is_required() {
        let (mut client, server) = pair().await.unwrap();
        write_message(&mut client, OP_BINARY, b"bare", false)
            .await
            .unwrap();
        let mut msgs = Messages::new(server);
        let err = msgs.next(true).await.unwrap_err();
        assert!(matches!(err, Error::Http("unmasked client frame")));
    }

    #[tokio::test]
    async fn fragmented_message_reassembles_across_a_ping() {
        use embedded_io_async::Write;
        let (mut client, server) = pair().await.unwrap();
        // Hand-built fragments: binary "he" (no FIN), a ping, then the
        // final continuation "llo" (FIN) — unmasked for byte clarity.
        client.write_all(&[0x02, 2, b'h', b'e']).await.unwrap();
        client.write_all(&[0x89, 1, b'!']).await.unwrap();
        client
            .write_all(&[0x80, 3, b'l', b'l', b'o'])
            .await
            .unwrap();
        let mut msgs = Messages::new(server);
        assert_eq!(
            msgs.next(false).await.unwrap().unwrap(),
            Msg::Ping(vec![b'!'])
        );
        assert_eq!(
            msgs.next(false).await.unwrap().unwrap(),
            Msg::Binary(b"hello".to_vec())
        );
    }

    #[tokio::test]
    async fn oversized_frame_is_capped_before_reading() {
        use embedded_io_async::Write;
        let (mut client, server) = pair().await.unwrap();
        let mut head = vec![0x82u8, 127];
        let too_big = u64::try_from(MAX_MESSAGE).unwrap() + 1;
        head.extend_from_slice(&too_big.to_be_bytes());
        client.write_all(&head).await.unwrap();
        let mut msgs = Messages::new(server);
        let err = msgs.next(false).await.unwrap_err();
        assert!(matches!(err, Error::Http("ws frame too large")));
    }

    #[tokio::test]
    async fn clean_close_between_frames_reads_as_none() {
        let (client, server) = pair().await.unwrap();
        drop(client);
        let mut msgs = Messages::new(server);
        assert!(msgs.next(false).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ping_payload_surfaces_for_the_caller_to_answer() {
        let (mut client, server) = pair().await.unwrap();
        write_message(&mut client, OP_PING, b"beat", true)
            .await
            .unwrap();
        let mut msgs = Messages::new(server);
        assert_eq!(
            msgs.next(true).await.unwrap().unwrap(),
            Msg::Ping(b"beat".to_vec())
        );
    }
}
