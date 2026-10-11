//! The WebSocket reader: the half of a session that reads the socket.
//!
//! Frame reads are not cancel-safe, so the session loop never awaits them
//! directly. A reader task owns the read half, decodes each frame, and hands
//! the message over a small queue; the loop selects on that queue, which is.
//! Split from `serve_ws` by layer: this is everything that reads.

use std::rc::Rc;

use embassy_futures::select::select;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;
use embassy_sync::signal::Signal;
use wavedb_core::wire::from_wire;
use wavedb_net::ws::ClientMsg;
use wavedb_platform::carrier::Here;
use wavedb_platform::io::ReadHalf;
use wavedb_platform::ws::codec::{Messages, Msg};

/// Client messages decoded and waiting for the session loop.
///
/// Small: when it is full the reader stops reading, which leaves the bytes in
/// the socket and pushes back onto the client's own send window. A client
/// can only make the node buffer this many of its messages, not more.
pub const WS_INBOUND_DEPTH: usize = 16;

/// What the reader task forwards to the session loop — a decoded client
/// message, a ping to answer (the loop owns the write half), or the end of
/// the stream.
pub enum FromClient {
    Msg(ClientMsg),
    Ping(Vec<u8>),
    End,
}

/// The reader-to-loop queue. Both ends run on the connection's thread, so
/// it needs no lock at all.
pub type Inbound = Channel<NoopRawMutex, FromClient, WS_INBOUND_DEPTH>;

/// Raised when the session ends, to stop its reader.
pub type Halt = Signal<NoopRawMutex, ()>;

/// Raises the reader's [`Halt`] when dropped. A guard rather than a call at
/// the end, because a halting node drops the session mid-await and the
/// reader must still stop: Embassy cannot abort a task, and a reader left
/// running would hold its pool slot and its carrier forever.
pub struct StopReader(pub Rc<Halt>);

impl Drop for StopReader {
    fn drop(&mut self) {
        self.0.signal(());
    }
}

/// The reader half as its own task: frame reads are not cancel-safe, so the
/// session loop never awaits them directly.
#[wavedb_platform::carrier::rt::task(
    pool_size = crate::__rt::MAX_CONNECTIONS,
    embassy_executor = wavedb_platform::carrier::rt
)]
pub async fn reader(
    read: ReadHalf,
    inbound: Rc<Inbound>,
    halt: Rc<Halt>,
    here: Here,
) {
    select(read_loop(read, inbound), halt.wait()).await;
    drop(here); // held to the end: the carrier waits for the reader
}

/// The reader half: decode masked binary frames into [`ClientMsg`]s (and
/// pings) and forward them. Any close, decode fault, or socket error ends the
/// stream, and the session hears [`FromClient::End`].
pub async fn read_loop(read: ReadHalf, inbound: Rc<Inbound>) {
    forward(read, &inbound).await;
    inbound.send(FromClient::End).await;
}

/// Forward until the stream ends. A full queue suspends the send, and with it
/// the reading: that is the inbound backpressure.
async fn forward(read: ReadHalf, inbound: &Inbound) {
    let mut msgs = Messages::new(read);
    loop {
        let forward = match msgs.next(true).await {
            Ok(Some(Msg::Binary(bytes))) => {
                match from_wire::<ClientMsg>(&bytes) {
                    Ok(msg) => FromClient::Msg(msg),
                    Err(_) => return, // an undecodable envelope ends the session
                }
            }
            Ok(Some(Msg::Ping(payload))) => FromClient::Ping(payload),
            Ok(Some(Msg::Pong(_))) => continue,
            // Close, clean end, or any fault: stop reading.
            _ => return,
        };
        inbound.send(forward).await;
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::time::Duration;

    use embassy_futures::select::{Either, select};
    use wavedb_core::wire::to_wire;
    use wavedb_net::ws::{ClientMsg, Topic};
    use wavedb_platform::io::pair;
    use wavedb_platform::ws::codec::{self, OP_BINARY};

    use super::{FromClient, Inbound, WS_INBOUND_DEPTH, read_loop};

    /// The inbound policy: a client sending faster than its session handles
    /// stops being read once `WS_INBOUND_DEPTH` messages wait. The rest stay
    /// in the socket — the node does not buffer them for it.
    #[tokio::test]
    async fn a_full_inbound_queue_stops_the_reader() {
        let (server, client) = pair().await.expect("socket pair");
        let (read, _server_write) = server.split();
        let (_client_read, mut write) = client.split();
        let msg = to_wire(&ClientMsg::Unsubscribe(Topic {
            struct_hash: 1,
            pivot: None,
        }));
        for _ in 0..WS_INBOUND_DEPTH + 4 {
            codec::write_message(&mut write, OP_BINARY, &msg, true)
                .await
                .expect("client write");
        }

        let inbound = Rc::new(Inbound::new());
        let reader = read_loop(read, Rc::clone(&inbound));
        let settle = wavedb_platform::time::sleep(Duration::from_millis(100));
        let Either::Second(()) = select(reader, settle).await else {
            panic!("the reader must be waiting on the full queue, not done");
        };

        assert_eq!(inbound.len(), WS_INBOUND_DEPTH);
        while let Ok(item) = inbound.try_receive() {
            assert!(matches!(item, FromClient::Msg(_)), "no end: still open");
        }
    }
}
