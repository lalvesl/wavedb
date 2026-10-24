//! Listening sockets — native only ([RFC 0066], [RFC 0067] §6).
//!
//! A node serves on several threads, and each one accepts its own
//! connections: [`Listener::bind_many`] binds one listener per thread on one
//! address. On Linux they are separate sockets in one `SO_REUSEPORT` group,
//! and the **kernel** spreads incoming connections across them — no thread
//! accepts for another, and no connection crosses a thread to be served.
//!
//! Elsewhere the kernel does not balance such a group (macOS hands a group's
//! connections to one socket; Windows has no equivalent that is not also a
//! way to steal a port), so there `bind_many` binds **one** listener and the
//! node accepts on one thread. That is a placement difference only: every
//! operation still runs on the thread that owns it.
//!
//! [RFC 0066]: ../../../rfcs/0066-embassy-executor-and-async-io.md
//! [RFC 0067]: ../../../rfcs/0067-thread-per-core-shards-WIP.md

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};

use async_io::Async;

use crate::error::Result;
use crate::io::{IoError, Stream};

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
        Ok(Self(Async::<TcpListener>::bind(resolve(addr)?)?))
    }

    /// Bind up to `count` listeners on `addr`, one per accepting thread, all
    /// on the same address — port `0` is resolved by the first and shared by
    /// the rest.
    ///
    /// On Linux this is `count` sockets in one `SO_REUSEPORT` group, which
    /// the kernel load-balances by connection. Any other socket that joins
    /// the group must set the option too and run under the **same user** —
    /// the kernel's rule — so a plain `bind` of the address still fails as
    /// in use. Elsewhere it is one listener, whatever `count` says (see the
    /// module documentation).
    ///
    /// # Errors
    /// Resolution failing, or the address being unavailable.
    pub fn bind_many(addr: &str, count: usize) -> Result<Vec<Self>> {
        let target = resolve(addr)?;
        let first = Self::wrap(group::bind(target)?)?;
        let bound = first.local_addr()?;
        let mut listeners = vec![first];
        for _ in 1..group::size(count) {
            listeners.push(Self::wrap(group::bind(bound)?)?);
        }
        Ok(listeners)
    }

    fn wrap(listener: TcpListener) -> Result<Self> {
        Ok(Self(Async::new(listener)?))
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

/// The first address `addr` resolves to.
fn resolve(addr: &str) -> Result<SocketAddr> {
    let target = addr.to_socket_addrs()?.next().ok_or_else(|| {
        IoError::new(
            std::io::ErrorKind::NotFound,
            "address resolved to nothing",
        )
    })?;
    Ok(target)
}

/// One member of a load-balanced group: a socket set up exactly as `std`
/// sets up a listener (`SO_REUSEADDR`, close-on-exec, a backlog of 128),
/// plus `SO_REUSEPORT`.
#[cfg(any(target_os = "linux", target_os = "android"))]
mod group {
    use std::net::{SocketAddr, TcpListener};

    use rustix::net::{
        AddressFamily, SocketFlags, SocketType, bind as bind_to, listen,
        socket_with, sockopt,
    };

    use crate::error::Result;
    use crate::io::IoError;

    /// `std`'s backlog on this target.
    const BACKLOG: i32 = 128;

    /// The group has as many members as were asked for, and at least one.
    pub const fn size(count: usize) -> usize {
        if count == 0 { 1 } else { count }
    }

    pub fn bind(target: SocketAddr) -> Result<TcpListener> {
        let family = if target.is_ipv4() {
            AddressFamily::INET
        } else {
            AddressFamily::INET6
        };
        let fd =
            socket_with(family, SocketType::STREAM, SocketFlags::CLOEXEC, None)
                .map_err(IoError::from)?;
        sockopt::set_socket_reuseaddr(&fd, true).map_err(IoError::from)?;
        sockopt::set_socket_reuseport(&fd, true).map_err(IoError::from)?;
        bind_to(&fd, &target).map_err(IoError::from)?;
        listen(&fd, BACKLOG).map_err(IoError::from)?;
        Ok(TcpListener::from(fd))
    }
}

/// No balanced group on this target: one plain listener.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
mod group {
    use std::net::{SocketAddr, TcpListener};

    use crate::error::Result;

    pub const fn size(_count: usize) -> usize {
        1
    }

    pub fn bind(target: SocketAddr) -> Result<TcpListener> {
        Ok(TcpListener::bind(target)?)
    }
}

#[cfg(test)]
mod tests {
    use super::Listener;

    /// Every listener of a group answers on the one address the first bound.
    #[test]
    fn a_group_shares_one_address() {
        let group = Listener::bind_many("127.0.0.1:0", 3).unwrap();
        let addr = group[0].local_addr().unwrap();
        assert!(group.iter().all(|l| l.local_addr().unwrap() == addr));
        #[cfg(any(target_os = "linux", target_os = "android"))]
        assert_eq!(group.len(), 3);
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        assert_eq!(group.len(), 1);
    }

    /// Joining the group takes the option: a plain bind of the address is
    /// refused as in use, as it would be for any other listener.
    #[test]
    fn a_plain_bind_cannot_join_the_group() {
        let group = Listener::bind_many("127.0.0.1:0", 2).unwrap();
        let addr = group[0].local_addr().unwrap().to_string();
        assert!(Listener::bind(&addr).is_err());
    }

    /// The kernel spreads connections across the group: every connection is
    /// accepted by some member, and more than one member takes a share.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn the_kernel_spreads_connections_across_the_group() {
        const CONNECTIONS: usize = 64;
        let group = Listener::bind_many("127.0.0.1:0", 4).unwrap();
        let addr = group[0].local_addr().unwrap().to_string();
        let clients: Vec<std::net::TcpStream> = (0..CONNECTIONS)
            .map(|_| std::net::TcpStream::connect(&addr).unwrap())
            .collect();
        // Every connection already sits in some member's queue: drain each
        // (the sockets are non-blocking, so an empty queue ends the count).
        let taken: Vec<usize> = group
            .iter()
            .map(|l| std::iter::from_fn(|| l.0.get_ref().accept().ok()).count())
            .collect();
        drop(clients);
        assert_eq!(taken.iter().sum::<usize>(), CONNECTIONS, "{taken:?}");
        assert!(taken.iter().filter(|&&n| n > 0).count() > 1, "{taken:?}");
    }
}
