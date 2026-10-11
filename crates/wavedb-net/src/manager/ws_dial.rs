//! Establishing one WebSocket connection for a [`ws_conn`](super::ws_conn)
//! actor — dial, upgrade, present the identity once, verify `HelloOk`, and
//! hand the receive half to a reader subtask (frame reads are not
//! cancel-safe). Shared by the actor's first connect and every reconnect.

use std::collections::HashMap;

use futures::StreamExt;
use futures::channel::mpsc;
use wavedb_platform::ws::{self, Received, RecvHalf, SendHalf};
use wavedb_wire::{from_wire, to_wire};

use super::tasks::{self, Here};
use super::ws_conn::{TopicState, ack_topic, deliver};
use super::{ConnKey, sync_call};
use crate::error::{Error, Result};
use crate::ws::{ClientMsg, RecordEvent, ServerMsg, Topic};

/// The decoded messages a connection's reader subtask forwards to its actor.
pub(super) type MsgRx = mpsc::UnboundedReceiver<Received>;

/// Dial, upgrade, present `key`'s identity, verify `HelloOk`, and spawn the
/// reader subtask that pushes each decoded message onto the returned channel.
///
/// # Errors
/// A dial/handshake fault, or [`Error::Http`] `"websocket hello refused"`
/// when the node closes instead of answering `HelloOk` (a bad identity) —
/// the caller treats that specific error as **fatal** (ends the watches).
pub(super) async fn open(
    key: &ConnKey,
    here: &Here,
) -> Result<(SendHalf, MsgRx)> {
    let conn = ws::connect(&key.addr).await?;
    let (mut recv, mut send) = conn.split();
    send.send(&to_wire(&ClientMsg::Hello(key.auth.clone())))
        .await?;
    loop {
        match recv.next().await? {
            Some(Received::Binary(bytes))
                if from_wire::<ServerMsg>(&bytes) == Ok(ServerMsg::HelloOk) =>
            {
                break;
            }
            Some(Received::Ping(payload)) => send.pong(&payload).await?,
            // The node refuses a bad identity by closing without a word.
            _ => return Err(Error::Http("websocket hello refused")),
        }
    }
    let (msg_tx, msg_rx) = mpsc::unbounded();
    if !tasks::spawn_reader(here, recv, msg_tx) {
        // The reader pool is full. A transport-class fault: the caller's
        // reconnect loop backs off and dials again.
        return Err(tasks::saturated("websocket readers"));
    }
    Ok((send, msg_rx))
}

/// The reader half as its own task: push each decoded message onto `msgs`
/// until the socket ends or the actor does.
pub(super) async fn read_into(
    mut recv: RecvHalf,
    msgs: mpsc::UnboundedSender<Received>,
) {
    loop {
        match recv.next().await {
            Ok(Some(received)) => {
                if msgs.unbounded_send(received).is_err() {
                    return; // The actor ended first.
                }
            }
            // Close, clean end, or a fault: dropping `msgs` tells the actor
            // the connection is over.
            _ => return,
        }
    }
}

/// Re-subscribe every topic on a fresh socket and catch up past each cursor
/// before the actor trusts the resumed push. Live events arriving during the
/// catch-up are buffered so they cannot advance the cursor ahead of the
/// navigation, then released (deduped) once it is done.
///
/// # Errors
/// A socket fault, a node refusal ([`Error::Node`] — fatal for the caller),
/// or a close before every topic is re-acked.
pub(super) async fn resubscribe(
    key: &ConnKey,
    send: &mut SendHalf,
    msgs: &mut MsgRx,
    topics: &mut HashMap<Topic, TopicState>,
) -> Result<()> {
    for (topic, state) in topics.iter_mut() {
        state.live = false;
        send.send(&to_wire(&ClientMsg::Subscribe(*topic))).await?;
    }
    let mut buffered: Vec<RecordEvent> = Vec::new();
    while topics.values().any(|state| !state.live) {
        match msgs.next().await {
            None => {
                return Err(Error::Http("socket closed during resubscribe"));
            }
            Some(Received::Ping(payload)) => send.pong(&payload).await?,
            Some(Received::Binary(bytes)) => {
                match from_wire::<ServerMsg>(&bytes)? {
                    ServerMsg::TopicOk(topic, tail) => {
                        ack_topic(topics, topic, tail);
                    }
                    ServerMsg::Event(event) => buffered.push(event),
                    _ => {
                        return Err(Error::Http(
                            "unexpected websocket message",
                        ));
                    }
                }
            }
        }
    }
    // Catch up each topic past its pre-disconnect cursor.
    let cursors: HashMap<Topic, Option<u64>> =
        topics.iter().map(|(t, s)| (*t, s.cursor)).collect();
    let reply = sync_call::sync_once(&key.addr, &key.auth, &cursors).await?;
    for event in reply.events {
        deliver(topics, &event);
    }
    for (topic, cursor) in reply.cursors {
        if let Some(state) = topics.get_mut(&topic) {
            state.cursor = Some(state.cursor.map_or(cursor, |c| c.max(cursor)));
        }
    }
    // Release the buffered live events, deduped against the advanced cursor.
    for event in buffered {
        deliver(topics, &event);
    }
    Ok(())
}
