//! The WebSocket session loop (native, server side): one identity-bound
//! connection carrying commands **and** live subscription events.
//!
//! The socket arrives here already upgraded (`serve` wrote the `101`). The
//! first message must be [`ClientMsg::Hello`] — gate 1 runs once, binding
//! the connection to a [`Caller`]; every later message executes as it, no
//! per-message token. Then the loop multiplexes two sources:
//!
//! - the **client** — [`Call`](ClientMsg::Call)s (gates 2–3, answered by
//!   `Item*`/`End`, FIFO per connection) and
//!   [`Subscribe`](ClientMsg::Subscribe)/[`Unsubscribe`](ClientMsg::Unsubscribe),
//!   each acked with a [`TopicOk`](ServerMsg::TopicOk) once applied (FIFO ⇒
//!   an acked watch cannot miss a later mutation); an **anonymous**
//!   `Subscribe` is refused by closing the connection;
//! - the **event channel** — mutations the [`NotifyStore`] pushed for this
//!   connection's subscriptions, forwarded verbatim.
//!
//! Frame reads are not cancel-safe, so a dedicated reader task owns the
//! read half and forwards decoded messages over a channel; the loop selects
//! on that channel (cancel-safe) and the event channel. On disconnect every
//! subscription the connection held is dropped.
//!
//! [`NotifyStore`]: crate::subscribe::NotifyStore

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use async_channel::Sender;
use embassy_futures::select::{Either, select};
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;
use tokio::task::spawn_local;
use wavedb_core::Store;
use wavedb_core::expose::{Caller, Exposure, Reply};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_net::frame::Response;
use wavedb_net::ws::{ClientMsg, ServerMsg, Topic};
use wavedb_platform::io::{ReadHalf, WriteHalf};
use wavedb_platform::ws::codec::{
    self, Messages, Msg, OP_BINARY, OP_CLOSE, OP_PONG,
};

use crate::dispatch;
use crate::subscribe::SubTable;

/// Client messages decoded and waiting for the session loop.
///
/// Small: when it is full the reader stops reading, which leaves the bytes in
/// the socket and pushes back onto the client's own send window. A client
/// can only make the node buffer this many of its messages, not more.
pub const WS_INBOUND_DEPTH: usize = 16;

/// Events queued for one connection before it is disconnected.
///
/// Deliberately not a backpressure point: the publisher serves every
/// connection, so waiting on one slow reader would stall them all. A client
/// that falls this far behind is cut off and catches up by cursor
/// (`SubTable::publish`).
pub const WS_OUTBOUND_DEPTH: usize = 256;

/// What the reader task forwards to the session loop — a decoded client
/// message, a ping to answer (the loop owns the write half), or the end of
/// the stream.
enum FromClient {
    Msg(ClientMsg),
    Ping(Vec<u8>),
    End,
}

/// The reader-to-loop queue. Both ends run on the connection's thread, so
/// it needs no lock at all.
type Inbound = Channel<NoopRawMutex, FromClient, WS_INBOUND_DEPTH>;

/// Run one upgraded connection to its end. Returns `Ok` on any clean or
/// faulted disconnect — a per-connection fault never propagates (the caller
/// drops it), so the node stays up.
///
/// # Errors
/// A socket write fault while answering (the connection is being torn down
/// regardless).
pub async fn serve<E, S>(
    read: ReadHalf,
    mut write: WriteHalf,
    registry: &E,
    store: &S,
    locks: &crate::shard::OwnerLocks,
    secret: &[u8; 32],
    subs: &Rc<RefCell<SubTable>>,
) -> wavedb_net::Result<()>
where
    E: Exposure,
    S: Store,
{
    let inbound = Rc::new(Inbound::new());
    let reader = spawn_local(read_loop(read, Rc::clone(&inbound)));

    // Gate 1, once: the first message must be a `Hello` whose identity
    // verifies — otherwise the connection is refused and closed.
    let FromClient::Msg(ClientMsg::Hello(auth)) = inbound.receive().await
    else {
        close(&mut write).await;
        reader.abort();
        return Ok(());
    };
    let Ok(caller) = dispatch::identify(&auth, secret) else {
        close(&mut write).await;
        reader.abort();
        return Ok(());
    };
    send(&mut write, &ServerMsg::HelloOk).await?;

    let (event_tx, event_rx) = async_channel::bounded(WS_OUTBOUND_DEPTH);
    let conn = subs.borrow_mut().new_conn();
    // The connection's own subscription set: keeps `subscribe` one-per-topic
    // and drives teardown on disconnect.
    let mut topics: HashSet<Topic> = HashSet::new();

    // Both receives are cancel-safe: losing the race leaves the message
    // queued for the next round.
    let result = loop {
        match select(inbound.receive(), event_rx.recv()).await {
            Either::First(FromClient::End) => break Ok(()),
            Either::First(item) => {
                if let Err(e) = handle_client(
                    &mut write,
                    registry,
                    store,
                    locks,
                    caller,
                    subs,
                    conn,
                    &event_tx,
                    &mut topics,
                    item,
                )
                .await
                {
                    break Err(e);
                }
            }
            Either::Second(Ok(msg)) => {
                if let Err(e) = send(&mut write, &msg).await {
                    break Err(e);
                }
            }
            // `event_tx` is held here, so the outbox only fails when the
            // publisher closed it: this client fell behind, or the feed
            // dropped a mutation. Say so and end; the client reconnects and
            // catches up by cursor.
            Either::Second(Err(_)) => {
                close(&mut write).await;
                break Ok(());
            }
        }
    };

    // Teardown: unregister every subscription, then stop the reader.
    let mut table = subs.borrow_mut();
    for topic in &topics {
        table.unsubscribe(caller.tenant, *topic, conn);
    }
    drop(table);
    reader.abort();
    result
}

/// Handle one message from the client. The returned `Err` is a socket write
/// fault only — a *command* refusal is a `Response::Err` sent over the wire.
#[allow(clippy::too_many_arguments)]
async fn handle_client<E, S>(
    write: &mut WriteHalf,
    registry: &E,
    store: &S,
    locks: &crate::shard::OwnerLocks,
    caller: Caller,
    subs: &Rc<RefCell<SubTable>>,
    conn: u64,
    event_tx: &Sender<ServerMsg>,
    topics: &mut HashSet<Topic>,
    item: FromClient,
) -> wavedb_net::Result<()>
where
    E: Exposure,
    S: Store,
{
    match item {
        FromClient::Ping(payload) => {
            codec::write_message(write, OP_PONG, &payload, false).await?;
        }
        // Ended by the session loop before it gets here.
        FromClient::End => {}
        FromClient::Msg(ClientMsg::Call(frame)) => {
            // Under the same brake the routed path takes, against the same
            // table. Without it a WebSocket write and a POST write on one
            // owner exclude nothing — and this path never took it at all,
            // which was a hole even before routing existed.
            let response = {
                let _owner = locks
                    .acquire(crate::shard::OwnerKey::of(
                        caller.tenant,
                        frame.struct_hash,
                    ))
                    .await;
                dispatch::execute(registry, store, caller, frame).await
            };
            write_reply(write, response).await?;
        }
        FromClient::Msg(ClientMsg::Subscribe(topic)) => {
            // Only an authenticated caller may watch a tenant's data; an
            // anonymous connection (public functions only) asking to is a
            // protocol violation — refused loudly (close), never silently
            // ignored (a client waiting on the ack must not hang).
            if caller.is_anonymous() {
                close(write).await;
                return Err(wavedb_net::Error::Http("anonymous subscribe"));
            }
            if topics.insert(topic) {
                subs.borrow_mut().subscribe(
                    caller.tenant,
                    topic,
                    conn,
                    event_tx.clone(),
                );
            }
            // FIFO ack: once this arrives the subscription is live, so a
            // watcher established before a mutation cannot miss its event.
            // The tail seeds the client's reconnect cursor (W6).
            let tail = topic_tail(registry, store, caller, topic).await;
            send(write, &ServerMsg::TopicOk(topic, tail)).await?;
        }
        FromClient::Msg(ClientMsg::Unsubscribe(topic)) => {
            if topics.remove(&topic) {
                subs.borrow_mut().unsubscribe(caller.tenant, topic, conn);
            }
            send(write, &ServerMsg::TopicOk(topic, 0)).await?;
        }
        // A second `Hello` is a protocol violation — end the session.
        FromClient::Msg(ClientMsg::Hello(_)) => {
            close(write).await;
            return Err(wavedb_net::Error::Http("second hello"));
        }
    }
    Ok(())
}

/// The topic's current tail instant — the cursor a fresh subscribe seeds its
/// reconnect catch-up from (W6). Runs the type's `Changes` step with
/// `since = None` (registration) through the registry and reads the cursor it
/// answers; 0 if the topic hash is not served (the subscribe still acks).
async fn topic_tail<E, S>(
    registry: &E,
    store: &S,
    caller: Caller,
    topic: Topic,
) -> u64
where
    E: Exposure,
    S: Store,
{
    use wavedb_core::expose::{Change, Command};

    let payload = to_wire(&(topic.pivot, None::<u64>));
    match registry
        .execute(store, caller, topic.struct_hash, Command::Changes, &payload)
        .await
    {
        Ok(Reply::Values(entries)) => entries
            .first()
            .and_then(|entry| from_wire::<Change>(entry).ok())
            .map_or(0, |change| match change {
                Change::Cursor(cursor) => cursor,
                _ => 0,
            }),
        _ => 0,
    }
}

/// The reader half: decode masked binary frames into [`ClientMsg`]s (and
/// pings) and forward them. Any close, decode fault, or socket error ends the
/// stream, and the session hears [`FromClient::End`].
async fn read_loop(read: ReadHalf, inbound: Rc<Inbound>) {
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

/// Ship a command's [`Response`] as WS messages: a `Values` walk unpacks
/// into one [`ServerMsg::Item`] per record, then an [`ServerMsg::End`];
/// everything else is a bare `End`. Mirrors the HTTP frame shape.
async fn write_reply(
    write: &mut WriteHalf,
    response: Response,
) -> wavedb_net::Result<()> {
    let end = match response {
        Response::Ok(Reply::Values(entries)) => {
            for entry in entries {
                send(write, &ServerMsg::Item(entry)).await?;
            }
            Response::Ok(Reply::Done)
        }
        other => other,
    };
    send(write, &ServerMsg::End(end)).await
}

/// Write one server message as a bare (unmasked) binary frame.
async fn send(
    write: &mut WriteHalf,
    msg: &ServerMsg,
) -> wavedb_net::Result<()> {
    codec::write_message(write, OP_BINARY, &to_wire(msg), false).await?;
    Ok(())
}

/// Best-effort close frame — the socket is going away either way.
async fn close(write: &mut WriteHalf) {
    let _ = codec::write_message(write, OP_CLOSE, &[], false).await;
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
