//! HTTPS to the object store ([RFC 0070] open question 1): TLS 1.2/1.3 by
//! rustls on the *ring* provider, certificates verified against Mozilla's
//! roots (`webpki-roots`) and any the caller trusts besides.
//!
//! rustls is a state machine over bytes — TLS records in, TLS records out,
//! plaintext through its reader and writer — so it is driven here by hand
//! over the same socket the plain exchange uses, with no async adapter
//! crate: [`TlsStream`] reads and writes the socket only when the state
//! machine asks, and offers the plaintext as `embedded-io-async` I/O, so
//! the exchange runs over either.
//!
//! A peer that closes the connection without a `close_notify` — many S3
//! endpoints do, with `connection: close` — ends the plaintext like a clean
//! close would: the response's own framing (`content-length`, or the last
//! chunk) is what says whether the body is whole.
//!
//! [RFC 0070]: ../../../../rfcs/0070-object-storage-backend.md

use std::io::{ErrorKind, Read as _, Write as _};
use std::sync::Arc;

use embedded_io_async::{ErrorType, Read, Write};
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore};

use crate::error::{Error, Result};
use crate::io::{self, IoError};

/// Bytes read from the socket at a time: one TLS record and its overhead.
const RECORD: usize = 16 * 1024 + 512;

/// A client configuration trusting Mozilla's roots and the DER certificates
/// in `extra`. rustls shares it between connections behind an `Arc`, and
/// takes it no other way.
///
/// # Errors
/// [`Error::Http`] when a certificate in `extra` is not one.
pub fn config(extra: &[Vec<u8>]) -> Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    for der in extra {
        roots
            .add(CertificateDer::from(der.clone()))
            .map_err(|_| Error::Http("a trusted certificate is not one"))?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| Error::Http("no TLS version the provider supports"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// A TLS connection to the object store, over one socket.
pub struct TlsStream {
    tcp: io::Stream,
    /// Boxed: the state machine is kilobytes, and would otherwise sit in
    /// every future that awaits a request.
    conn: Box<ClientConnection>,
    incoming: Vec<u8>,
}

/// Connect to `addr` and complete a TLS handshake with it as `host`.
///
/// # Errors
/// Socket faults; a host name that is no DNS name or IP address; a
/// handshake refused — a certificate not trusted, or not `host`'s, among
/// others.
pub async fn connect(
    addr: &str,
    host: &str,
    config: Arc<ClientConfig>,
) -> Result<TlsStream> {
    let name = ServerName::try_from(host.to_owned())
        .map_err(|_| Error::Http("object store host name"))?;
    let conn = Box::new(ClientConnection::new(config, name).map_err(fault)?);
    let tcp = io::connect(addr).await?;
    let mut stream = TlsStream {
        tcp,
        conn,
        incoming: vec![0; RECORD],
    };
    while stream.conn.is_handshaking() {
        stream.send_pending().await?;
        if stream.conn.wants_read() && !stream.receive().await? {
            return Err(Error::Http(
                "object store closed during the handshake",
            ));
        }
    }
    stream.send_pending().await?;
    Ok(stream)
}

impl TlsStream {
    /// Write every TLS record the state machine has ready.
    async fn send_pending(&mut self) -> core::result::Result<(), IoError> {
        while self.conn.wants_write() {
            let mut out = Vec::new();
            self.conn.write_tls(&mut out)?;
            self.tcp.write_all(&out).await?;
        }
        self.tcp.flush().await
    }

    /// Read once from the socket into the state machine; `false` when the
    /// peer has closed it.
    async fn receive(&mut self) -> core::result::Result<bool, IoError> {
        let n = self.tcp.read(&mut self.incoming).await?;
        let mut rest = &self.incoming[..n];
        loop {
            // An empty `rest` tells the state machine the socket closed.
            self.conn.read_tls(&mut rest)?;
            self.conn
                .process_new_packets()
                .map_err(|e| IoError::new(ErrorKind::InvalidData, e))?;
            if rest.is_empty() {
                return Ok(n > 0);
            }
        }
    }
}

/// A rustls fault as this crate's error.
fn fault(e: rustls::Error) -> Error {
    Error::Io(IoError::new(ErrorKind::InvalidData, e))
}

impl ErrorType for TlsStream {
    type Error = IoError;
}

impl Read for TlsStream {
    async fn read(
        &mut self,
        buf: &mut [u8],
    ) -> core::result::Result<usize, IoError> {
        loop {
            match self.conn.reader().read(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    self.send_pending().await?;
                    // A close is seen by the reader on the next turn.
                    self.receive().await?;
                }
                // Closed with no `close_notify`: the framing decides.
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(0),
                Err(e) => return Err(e),
            }
        }
    }
}

impl Write for TlsStream {
    async fn write(
        &mut self,
        buf: &[u8],
    ) -> core::result::Result<usize, IoError> {
        let n = self.conn.writer().write(buf)?;
        self.send_pending().await?;
        Ok(n)
    }

    async fn flush(&mut self) -> core::result::Result<(), IoError> {
        self.send_pending().await
    }
}
