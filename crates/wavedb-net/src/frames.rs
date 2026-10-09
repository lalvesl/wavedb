//! The response's `[len u32 LE][bytes]` frame sequence, reassembled from
//! the platform body stream.
//!
//! The node writes each frame and flushes it (`wavedb-net::http`
//! server-side); the body arrives here as arbitrary chunks — a socket's
//! reads natively, a fetch stream's in the browser. Chunk boundaries are
//! transport artifacts; this layer restores the frame boundaries. Being
//! written over [`wavedb_platform::http::Body`] is what makes
//! [`NetClient`](crate::NetClient) compile for both targets.

use wavedb_platform::http::Body;

use crate::error::{Error, Result};

/// Cap on a declared request body and on one response frame. One frame is
/// one operation or one walk item — nothing legitimate approaches this.
pub const MAX_BODY: usize = 16 * 1024 * 1024;

/// An incremental reader over a response's frames — each is available as
/// soon as the node flushes it, which is what makes a walk streamable
/// without buffering the whole collection.
#[derive(Debug)]
pub struct FrameReader {
    body: Body,
    /// Bytes received past the previous frame boundary.
    buf: Vec<u8>,
    /// The body reported its end of stream.
    done: bool,
}

impl FrameReader {
    /// Wrap a platform body positioned at the frame sequence (i.e. past
    /// the response head).
    #[must_use]
    pub const fn new(body: Body) -> Self {
        Self {
            body,
            buf: Vec::new(),
            done: false,
        }
    }

    /// Pull chunks until `self.buf` holds at least `n` bytes. `Ok(false)` =
    /// the body ended first with the buffer empty (a clean end of stream).
    async fn fill(&mut self, n: usize) -> Result<bool> {
        while self.buf.len() < n {
            let chunk = if self.done {
                None
            } else {
                self.body.chunk().await?
            };
            let Some(bytes) = chunk else {
                self.done = true;
                if self.buf.is_empty() {
                    return Ok(false);
                }
                return Err(Error::Http("connection closed mid-frame"));
            };
            self.buf.extend_from_slice(&bytes);
        }
        Ok(true)
    }

    /// The next frame's bytes; `None` on a clean end of stream (the body
    /// ended exactly on a frame boundary).
    ///
    /// # Errors
    /// A transport fault, an end mid-frame, or an oversized frame.
    pub async fn next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        if !self.fill(4).await? {
            return Ok(None);
        }
        // The length prefix was just filled to 4 bytes.
        let len = u32::from_le_bytes(self.buf[..4].try_into().expect("4 bytes"))
            as usize;
        if len > MAX_BODY {
            return Err(Error::BodyTooLarge {
                limit: MAX_BODY,
                have: len,
            });
        }
        if !self.fill(4 + len).await? {
            return Err(Error::Http("connection closed mid-frame"));
        }
        let mut frame = self.buf.split_off(4);
        self.buf.clear();
        if frame.len() > len {
            self.buf = frame.split_off(len);
        }
        Ok(Some(frame))
    }
}

#[cfg(test)]
mod tests {
    use embedded_io_async::{Read, Write};
    use wavedb_platform::io::Listener;

    use super::{Error, FrameReader, MAX_BODY};

    /// Serve one canned frame-sequence body behind a real `200` head and
    /// hand back the reader over it — the client path end to end.
    async fn reader_over(response_body: Vec<u8>) -> FrameReader {
        let listener = Listener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut sink = [0u8; 1024];
            let _ = sock.read(&mut sink).await;
            sock.write_all(b"HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
            sock.write_all(&response_body).await.unwrap();
        });
        let body = wavedb_platform::http::post(&addr, b"req").await.unwrap();
        FrameReader::new(body)
    }

    fn frame(bytes: &[u8]) -> Vec<u8> {
        let mut v = u32::try_from(bytes.len()).unwrap().to_le_bytes().to_vec();
        v.extend_from_slice(bytes);
        v
    }

    #[tokio::test]
    async fn frames_arrive_in_order_then_a_clean_end() {
        let mut body = frame(b"one");
        body.extend(frame(b"world"));
        let mut frames = reader_over(body).await;
        assert_eq!(frames.next_frame().await.unwrap().unwrap(), b"one");
        assert_eq!(frames.next_frame().await.unwrap().unwrap(), b"world");
        assert!(frames.next_frame().await.unwrap().is_none(), "clean end");
    }

    #[tokio::test]
    async fn close_mid_frame_is_a_transport_fault() {
        // A length prefix promising 8 bytes, then only 3, then close.
        let mut body = 8u32.to_le_bytes().to_vec();
        body.extend_from_slice(b"abc");
        let mut frames = reader_over(body).await;
        let err = frames.next_frame().await.unwrap_err();
        assert!(matches!(err, Error::Http("connection closed mid-frame")));
    }

    #[tokio::test]
    async fn oversized_frame_is_capped_before_reading() {
        let too_big = u32::try_from(MAX_BODY + 1).unwrap();
        let mut frames = reader_over(too_big.to_le_bytes().to_vec()).await;
        let err = frames.next_frame().await.unwrap_err();
        assert!(matches!(err, Error::BodyTooLarge { .. }));
    }
}
