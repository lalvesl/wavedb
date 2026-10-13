//! The WebSocket session loop (native, server side): one identity-bound
//! connection carrying commands **and** live subscription events.
//!
//! The socket arrives here already upgraded (`serve` wrote the `101`). The
//! first message must be [`ClientMsg::Hello`] — gate 1 runs once, binding
//! the connection to a [`Caller`]; every later message executes as it, no
//! per-message token. Then the loop multiplexes two sources:
//!
//! - the **client** — [`Call`](ClientMsg::Call)s (routed to the owning
//!   shard with the bound caller, which runs gates 2–3; answered by
//!   `Item*`/`End`, FIFO per connection) and
//!   [`Subscribe`](ClientMsg::Subscribe)/[`Unsubscribe`](ClientMsg::Unsubscribe),
//!   each acked with a [`TopicOk`](ServerMsg::TopicOk) once applied (FIFO ⇒
//!   an acked watch cannot miss a later mutation); an **anonymous**
//!   `Subscribe` is refused by closing the connection;
//! - the **event channel** — mutations the [`NotifyStore`] pushed for this
//!   connection's subscriptions, forwarded verbatim.
//!
//! Frame reads are not cancel-safe, so a dedicated reader task
//! ([`crate::ws_read`]) owns the read half and forwards decoded messages over
//! a channel; the loop selects on that channel (cancel-safe) and the event
//! channel. On disconnect every subscription the connection held is dropped.
//!
//! The session executes nothing itself: every command, and the tail a
//! subscribe acks with, runs on the shard that owns it
//! ([`Router::call`](crate::shard::Router::call)). This thread holds the
//! socket and the subscription table, and no store.
//!
//! [`NotifyStore`]: crate::subscribe::NotifyStore

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use async_channel::Sender;
use embassy_futures::select::{Either, select};
use wavedb_core::expose::{Caller, Change, Command, Reply};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_net::frame::{CommandFrame, Response};
use wavedb_net::ws::{ClientMsg, ServerMsg, Topic};
use wavedb_platform::carrier::Here;
use wavedb_platform::io::{ReadHalf, WriteHalf};
use wavedb_platform::ws::codec::{self, OP_BINARY, OP_CLOSE, OP_PONG};

use crate::dispatch;
use crate::serve::Node;
use crate::shard::Router;
use crate::subscribe::SubTable;
use crate::ws_read::{FromClient, Halt, Inbound, StopReader, reader};

/// Events queued for one connection before it is disconnected.
///
/// Deliberately not a backpressure point: the publisher serves every
/// connection, so waiting on one slow reader would stall them all. A client
/// that falls this far behind is cut off and catches up by cursor
/// (`SubTable::publish`).
pub const WS_OUTBOUND_DEPTH: usize = 256;

/// Run one upgraded connection to its end. Returns `Ok` on any clean or
/// faulted disconnect — a per-connection fault never propagates (the caller
/// drops it), so the node stays up.
///
/// # Errors
/// A socket write fault while answering (the connection is being torn down
/// regardless).
pub async fn serve(
    read: ReadHalf,
    mut write: WriteHalf,
    node: &Node,
    here: &Here,
) -> wavedb_net::Result<()> {
    let (router, secret, subs) = (&node.router, &node.secret, &node.subs);
    let inbound = Rc::new(Inbound::new());
    let halt = Rc::new(Halt::new());
    // However the session ends — returning, or dropped mid-await when the
    // node halts — the reader is told to stop with it.
    let _stop_reader = StopReader(Rc::clone(&halt));
    let reader = reader(read, Rc::clone(&inbound), halt, here.clone());
    if here.spawn(reader).is_err() {
        close(&mut write).await; // no reader slot: refuse the session
        return Ok(());
    }

    // Gate 1, once: the first message must be a `Hello` whose identity
    // verifies — otherwise the connection is refused and closed.
    let FromClient::Msg(ClientMsg::Hello(auth)) = inbound.receive().await
    else {
        close(&mut write).await;
        return Ok(());
    };
    let Ok(caller) = dispatch::identify(&auth, secret) else {
        close(&mut write).await;
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
                    router,
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

    // Teardown: unregister every subscription; the reader stops as
    // `_stop_reader` drops.
    let mut table = subs.borrow_mut();
    for topic in &topics {
        table.unsubscribe(caller.tenant, *topic, conn);
    }
    drop(table);
    result
}

/// Handle one message from the client. The returned `Err` is a socket write
/// fault only — a *command* refusal is a `Response::Err` sent over the wire.
#[allow(clippy::too_many_arguments)]
async fn handle_client(
    write: &mut WriteHalf,
    router: &Router,
    caller: Caller,
    subs: &Rc<RefCell<SubTable>>,
    conn: u64,
    event_tx: &Sender<ServerMsg>,
    topics: &mut HashSet<Topic>,
    item: FromClient,
) -> wavedb_net::Result<()> {
    match item {
        FromClient::Ping(payload) => {
            codec::write_message(write, OP_PONG, &payload, false).await?;
        }
        // Ended by the session loop before it gets here.
        FromClient::End => {}
        FromClient::Msg(ClientMsg::Call(frame)) => {
            // Awaited before the next message is read: FIFO per connection.
            let response = router.call(caller, frame).await;
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
            let tail = topic_tail(router, caller, topic).await;
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
/// reconnect catch-up from (W6). Runs the type's `Changes` command with
/// `since = None` (registration) on the owning shard and reads the cursor it
/// answers; 0 if the topic hash is not served (the subscribe still acks).
async fn topic_tail(router: &Router, caller: Caller, topic: Topic) -> u64 {
    let frame = CommandFrame {
        struct_hash: topic.struct_hash,
        command: Command::Changes,
        payload: to_wire(&(topic.pivot, None::<u64>)),
    };
    match router.call(caller, frame).await {
        Response::Ok(Reply::Values(entries)) => entries
            .first()
            .and_then(|entry| from_wire::<Change>(entry).ok())
            .map_or(0, |change| match change {
                Change::Cursor(cursor) => cursor,
                _ => 0,
            }),
        _ => 0,
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
