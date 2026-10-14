//! Sockets — native only ([RFC 0066]).
//!
//! The one place a TCP socket is named. Everything that speaks a protocol over
//! one — the WebSocket codec, the tunnel's HTTP framing, the node's accept
//! loop — is generic over `embedded_io_async::{Read, Write}` carrying this
//! target's [`IoError`], so the same code can one day sit on `embassy-net`'s
//! `TcpSocket` and only this module changes.
//!
//! The socket is `async-io`'s: a reactor on the OS's readiness API that wakes
//! tasks through the standard `Waker`, so it works under whichever executor
//! polls them — the Embassy carriers, or a plain `block_on`.
//!
//! [RFC 0066]: ../../../rfcs/0066-embassy-executor-and-async-io.md

use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_io::Async;
use embedded_io_adapters::futures_03::FromFutures;
use embedded_io_async::{ErrorType, Read, Write};

use crate::error::{Error, Result};

/// The error every stream on this target reports.
///
/// Protocol code bounds its streams with `Read<Error = IoError>`, which keeps `std::io::Error`'s
/// detail natively (it converts into [`Error::Io`] unchanged) while leaving
/// room for another target's error type.
pub type IoError = std::io::Error;

/// One socket, shared by the halves of a split stream.
///
/// `Arc` rather than `Rc` so a stream may be handed across a thread boundary
/// whole; `async-io` reads and writes through `&Async<_>`, which is what
/// lets two halves use one socket at once.
#[derive(Debug, Clone)]
struct Shared(Arc<Async<TcpStream>>);

impl futures::io::AsyncRead for Shared {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut socket: &Async<TcpStream> = &self.0;
        Pin::new(&mut socket).poll_read(cx, buf)
    }
}

impl futures::io::AsyncWrite for Shared {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut socket: &Async<TcpStream> = &self.0;
        Pin::new(&mut socket).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut socket: &Async<TcpStream> = &self.0;
        Pin::new(&mut socket).poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut socket: &Async<TcpStream> = &self.0;
        Pin::new(&mut socket).poll_close(cx)
    }
}

/// A connected TCP stream.
pub struct Stream(FromFutures<Shared>);

/// The reading half of a [`Stream::split`].
pub struct ReadHalf(FromFutures<Shared>);

/// The writing half of a [`Stream::split`].
///
/// Dropping it shuts the socket's write side down, so the peer reads an end of stream even while the read
/// half lives on — the tunnel's `connection: close` depends on that.
pub struct WriteHalf(FromFutures<Shared>);

impl Stream {
    fn new(socket: Async<TcpStream>) -> Self {
        Self(FromFutures::new(Shared(Arc::new(socket))))
    }

    /// Split into halves that read and write independently — a reader task
    /// beside a writer, which frame reads (not cancel-safe) require.
    #[must_use]
    pub fn split(self) -> (ReadHalf, WriteHalf) {
        let shared = self.0.into_inner();
        (
            ReadHalf(FromFutures::new(shared.clone())),
            WriteHalf(FromFutures::new(shared)),
        )
    }
}

// By hand: the adapter is not `Debug`; the socket inside it is.
impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Stream").field(self.0.inner()).finish()
    }
}

impl std::fmt::Debug for ReadHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ReadHalf").field(self.0.inner()).finish()
    }
}

impl std::fmt::Debug for WriteHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("WriteHalf").field(self.0.inner()).finish()
    }
}

impl Drop for WriteHalf {
    fn drop(&mut self) {
        // Best effort: the peer may already be gone, and a drop cannot report.
        let _ = self.0.inner().0.get_ref().shutdown(Shutdown::Write);
    }
}

impl ErrorType for Stream {
    type Error = IoError;
}

impl ErrorType for ReadHalf {
    type Error = IoError;
}

impl ErrorType for WriteHalf {
    type Error = IoError;
}

impl Read for Stream {
    async fn read(
        &mut self,
        buf: &mut [u8],
    ) -> core::result::Result<usize, IoError> {
        self.0.read(buf).await
    }
}

impl Read for ReadHalf {
    async fn read(
        &mut self,
        buf: &mut [u8],
    ) -> core::result::Result<usize, IoError> {
        self.0.read(buf).await
    }
}

impl Write for Stream {
    async fn write(
        &mut self,
        buf: &[u8],
    ) -> core::result::Result<usize, IoError> {
        self.0.write(buf).await
    }

    async fn flush(&mut self) -> core::result::Result<(), IoError> {
        self.0.flush().await
    }
}

impl Write for WriteHalf {
    async fn write(
        &mut self,
        buf: &[u8],
    ) -> core::result::Result<usize, IoError> {
        self.0.write(buf).await
    }

    async fn flush(&mut self) -> core::result::Result<(), IoError> {
        self.0.flush().await
    }
}

/// Connect to `addr` (`host:port`), trying each resolved address in turn.
///
/// # Errors
/// Resolution failing, or every resolved address refusing.
pub async fn connect(addr: &str) -> Result<Stream> {
    let mut last = None;
    for target in resolve(addr).await? {
        match Async::<TcpStream>::connect(target).await {
            Ok(socket) => return Ok(Stream::new(socket)),
            Err(e) => last = Some(e),
        }
    }
    Err(Error::Io(last.unwrap_or_else(|| {
        IoError::new(
            std::io::ErrorKind::NotFound,
            "address resolved to nothing",
        )
    })))
}

/// `addr` as socket addresses, without blocking the executor.
///
/// A literal `ip:port` parses in place — every test and most deployments.
/// A hostname needs `getaddrinfo`, which blocks for as long as DNS takes, so
/// it runs on a short-lived thread of its own and is awaited.
async fn resolve(addr: &str) -> Result<Vec<SocketAddr>> {
    if let Ok(literal) = addr.parse::<SocketAddr>() {
        return Ok(vec![literal]);
    }
    let owned = addr.to_owned();
    let (tx, rx) = futures::channel::oneshot::channel();
    std::thread::Builder::new()
        .name("wavedb-resolve".into())
        .spawn(move || {
            let _ = tx.send(owned.to_socket_addrs().map(Iterator::collect));
        })?;
    rx.await
        .map_err(|_| IoError::other("resolver thread ended without an answer"))?
        .map_err(Error::Io)
}

/// A bound TCP listener.
#[derive(Debug)]
pub struct Listener(Async<TcpListener>);

impl Listener {
    /// Bind `addr` (`host:port`; port `0` picks a free one).
    ///
    /// Resolution here may block: binding happens once, at startup, before
    /// there is anything for the executor to run.
    ///
    /// # Errors
    /// Resolution failing, or the address being unavailable.
    pub fn bind(addr: &str) -> Result<Self> {
        let target = addr.to_socket_addrs()?.next().ok_or_else(|| {
            IoError::new(
                std::io::ErrorKind::NotFound,
                "address resolved to nothing",
            )
        })?;
        Ok(Self(Async::<TcpListener>::bind(target)?))
    }

    /// The address actually bound — the chosen port, after binding `:0`.
    ///
    /// # Errors
    /// The socket refusing to report it.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.0.get_ref().local_addr()?)
    }

    /// The next connection. Cancel-safe: dropping the future loses nothing,
    /// which the accept loop's race against shutdown relies on.
    ///
    /// # Errors
    /// A fault on the listening socket.
    pub async fn accept(&self) -> Result<(Stream, SocketAddr)> {
        let (socket, peer) = self.0.accept().await?;
        Ok((Stream::new(socket), peer))
    }
}

/// Two connected ends over loopback TCP — a real socket pair, for tests that
/// need both sides of a conversation in one process.
///
/// # Errors
/// Loopback being unavailable.
pub async fn pair() -> Result<(Stream, Stream)> {
    let listener = Listener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?.to_string();
    let (near, far) = futures::join!(connect(&addr), listener.accept());
    Ok((near?, far?.0))
}

/// A reader that yields `head` before reading from `inner`.
///
/// For bytes a handshake pulled off the socket past its own end: they belong
/// to whatever reads next, and putting them back in front keeps that reader
/// unaware there was a handshake at all.
#[derive(Debug)]
pub struct Prefixed<R> {
    head: Vec<u8>,
    at: usize,
    inner: R,
}

impl<R> Prefixed<R> {
    /// `head` first, then `inner`.
    #[must_use]
    pub const fn new(head: Vec<u8>, inner: R) -> Self {
        Self { head, at: 0, inner }
    }
}

impl<R: ErrorType> ErrorType for Prefixed<R> {
    type Error = R::Error;
}

impl<R: Read> Read for Prefixed<R> {
    async fn read(
        &mut self,
        buf: &mut [u8],
    ) -> core::result::Result<usize, R::Error> {
        let rest = &self.head[self.at..];
        if rest.is_empty() {
            return self.inner.read(buf).await;
        }
        let n = rest.len().min(buf.len());
        buf[..n].copy_from_slice(&rest[..n]);
        self.at += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use embedded_io_async::{Read, Write};

    use super::{Listener, Prefixed, connect, pair};

    /// Read until the peer's end of stream.
    async fn read_to_end(r: &mut impl Read) -> Vec<u8> {
        let mut out = Vec::new();
        let mut chunk = [0u8; 64];
        loop {
            let n = r.read(&mut chunk).await.expect("read");
            if n == 0 {
                return out;
            }
            out.extend_from_slice(&chunk[..n]);
        }
    }

    #[test]
    fn a_pair_carries_bytes_both_ways() {
        crate::carrier::block_on(async {
            let (mut a, mut b) = pair().await.unwrap();
            a.write_all(b"ping").await.unwrap();
            let mut got = [0u8; 4];
            b.read_exact(&mut got).await.unwrap();
            assert_eq!(&got, b"ping");
            b.write_all(b"pong").await.unwrap();
            a.read_exact(&mut got).await.unwrap();
            assert_eq!(&got, b"pong");
        });
    }

    /// The tunnel's response ends when the node drops its write half; the
    /// client must read that as an end of stream while the node's read half
    /// is still alive.
    #[test]
    fn dropping_the_write_half_ends_the_peers_stream() {
        crate::carrier::block_on(async {
            let (client, server) = pair().await.unwrap();
            let (_server_read, mut server_write) = server.split();
            server_write.write_all(b"bye").await.unwrap();
            drop(server_write);
            let (mut client_read, _client_write) = client.split();
            assert_eq!(read_to_end(&mut client_read).await, b"bye");
        });
    }

    #[test]
    fn prefixed_bytes_come_before_the_stream() {
        crate::carrier::block_on(async {
            let (mut a, b) = pair().await.unwrap();
            a.write_all(b"world").await.unwrap();
            drop(a);
            let mut r = Prefixed::new(b"hello ".to_vec(), b);
            assert_eq!(read_to_end(&mut r).await, b"hello world");
        });
    }

    #[test]
    fn a_hostname_resolves_off_the_executor() {
        crate::carrier::block_on(async {
            let listener = Listener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let host = format!("localhost:{port}");
            let (connected, accepted) =
                futures::join!(connect(&host), listener.accept());
            connected.unwrap();
            accepted.unwrap();
        });
    }

    #[test]
    fn nobody_listening_is_an_io_fault() {
        crate::carrier::block_on(async {
            // Bind then drop: the port is known and, for a moment, unused.
            let addr = Listener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .to_string();
            let err = connect(&addr).await.unwrap_err();
            assert!(matches!(err, crate::Error::Io(_)), "{err:?}");
        });
    }
}
