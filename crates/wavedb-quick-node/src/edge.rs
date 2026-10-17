//! The edge: what a request's own thread does before and around its
//! operations (RFC 0067 §4–§6).
//!
//! Gate 1 runs here, once. Then every piece of work goes where its owner
//! lives:
//!
//! - a **struct command** hops to its owner's shard, which runs gates 2–3
//!   under the owner's lock ([`dispatch::call`]);
//! - a **`#[server]` function** runs *here*. Its body only coordinates: each
//!   operation it performs is a [`NodeOp`](wavedb_core::NodeOp) that hops to
//!   that operation's owner and runs under that owner's lock. A body never
//!   executes on a shard, so it can never wait on the shard it occupies;
//! - a **sync poll** and the **piggyback** navigate each topic at the topic's
//!   owner.
//!
//! Where "hops" lead is an [`Exec`]: the node's [`Router`](crate::shard::Router),
//! or, in tests, one store in hand.

use std::future::Future;

use wavedb_core::Error;
use wavedb_core::expose::{
    Addressing, Caller, Change, Command, Exposure, Reply,
};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_net::frame::{CommandFrame, NodeError, Request, Response};
use wavedb_net::sync::{SYNC_STRUCT_HASH, SyncReply, SyncRequest, TopicCursor};
use wavedb_net::ws::{EventKind, RecordEvent};

use crate::dispatch::{self, Answer};

/// Where the edge sends work: each piece to its owner.
pub trait Exec: wavedb_core::Route {
    /// Run a struct command at its owner, gates 2–3 under the owner's lock.
    fn owned(
        &self,
        caller: Caller,
        frame: CommandFrame,
    ) -> impl Future<Output = Response>;
}

/// One request, start to finish: gate 1, then the work at its owners, then
/// the piggyback. Never returns a transport error.
pub async fn handle<E, X>(
    registry: &E,
    exec: &X,
    secret: &[u8; 32],
    request: Request,
) -> Answer
where
    E: Exposure,
    X: Exec,
{
    let Request { auth, frame, sync } = request;
    let Ok(caller) = dispatch::identify(&auth, secret) else {
        return Answer {
            response: dispatch::unauthorized(frame.struct_hash),
            sync: None,
        };
    };
    // The reserved sync exchange (HTTP-poll watch) routes before the
    // registry — no schema hash can reach it, and it can shadow none. It
    // carries its own topics, so it never also piggybacks.
    if frame.struct_hash == SYNC_STRUCT_HASH {
        let response = sync_poll(exec, caller, &frame.payload).await;
        return Answer {
            response,
            sync: None,
        };
    }
    let response = command(registry, exec, caller, frame).await;
    let delta = piggyback(exec, caller, &sync).await;
    Answer {
        response,
        sync: delta,
    }
}

/// One identified command: a function runs here, anything else at its
/// owner. Shared by a POST ([`handle`]) and a WebSocket `Call`.
pub async fn command<E, X>(
    registry: &E,
    exec: &X,
    caller: Caller,
    frame: CommandFrame,
) -> Response
where
    E: Exposure,
    X: Exec,
{
    let h = frame.struct_hash;
    if registry.addressing(h) != Some(Addressing::Function) {
        return exec.owned(caller, frame).await;
    }
    match registry.call_fn(exec, caller, h, &frame.payload).await {
        Ok(reply) => Response::Ok(reply),
        Err(err) => Response::Err(NodeError::from_core(h, &err)),
    }
}

/// The W7 piggyback: the declared topics' delta, wire-encoded for a
/// `StreamFrame::Sync` riding the command reply. Best-effort — `None` when
/// none were declared or the navigation refused; the next poll picks it up.
async fn piggyback<X: Exec>(
    exec: &X,
    caller: Caller,
    topics: &[TopicCursor],
) -> Option<Vec<u8>> {
    if topics.is_empty() {
        return None;
    }
    match navigate(exec, caller, topics).await {
        Response::Ok(Reply::Returned(bytes)) => Some(bytes),
        _ => None,
    }
}

/// The "anything new?" answer — **stateless**: each declared topic rides
/// the caller's cursor and is answered by navigating the disk at its owner,
/// so nothing is buffered node-side and nothing can be missed or expire.
/// Requires an authenticated identity, matching the WebSocket subscribe; an
/// unlisted topic hash refuses the whole sync exactly like a command would.
async fn sync_poll<X: Exec>(
    exec: &X,
    caller: Caller,
    payload: &[u8],
) -> Response {
    if caller.is_anonymous() {
        return dispatch::unauthorized(SYNC_STRUCT_HASH);
    }
    match from_wire::<SyncRequest>(payload) {
        Ok(request) => navigate(exec, caller, &request.topics).await,
        Err(e) => Response::Err(NodeError::from_core(
            SYNC_STRUCT_HASH,
            &Error::from(e),
        )),
    }
}

/// The stateless "what changed?" navigation shared by [`sync_poll`] and
/// [`piggyback`]: each topic's `Changes` command runs at the topic's owner.
/// The wire `SyncReply` comes back inside `Reply::Returned`, or a
/// `Response::Err` if a topic's navigation refused.
async fn navigate<X: Exec>(
    exec: &X,
    caller: Caller,
    topics: &[TopicCursor],
) -> Response {
    let mut reply = SyncReply {
        events: Vec::new(),
        cursors: Vec::new(),
    };
    for declared in topics {
        let topic = declared.topic;
        let frame = CommandFrame {
            struct_hash: topic.struct_hash,
            command: Command::Changes,
            payload: to_wire(&(topic.pivot, declared.since)),
        };
        let entries = match exec.owned(caller, frame).await {
            Response::Ok(Reply::Values(entries)) => entries,
            Response::Ok(_) => {
                return Response::Err(NodeError::from_core(
                    topic.struct_hash,
                    &Error::Backend(
                        "changes answered with a non-values reply".into(),
                    ),
                ));
            }
            refused @ Response::Err(_) => return refused,
        };
        for entry in entries {
            match from_wire::<Change>(&entry) {
                Ok(Change::Cursor(cursor)) => {
                    reply.cursors.push((topic, cursor));
                }
                Ok(Change::Saved(id, meta, body)) => {
                    reply.events.push(RecordEvent {
                        topic,
                        id,
                        kind: EventKind::Saved,
                        meta: Some(meta),
                        body,
                    });
                }
                Ok(Change::Removed(id, at)) => {
                    reply.events.push(RecordEvent {
                        topic,
                        id,
                        kind: EventKind::Removed(at),
                        meta: None,
                        body: Vec::new(),
                    });
                }
                Err(e) => {
                    return Response::Err(NodeError::from_core(
                        topic.struct_hash,
                        &Error::from(e),
                    ));
                }
            }
        }
    }
    Response::Ok(Reply::Returned(to_wire(&reply)))
}

/// An [`Exec`] over one store: every owner is here, behind one lock table.
#[cfg(test)]
pub(crate) struct Direct<'a, E, S> {
    pub registry: &'a E,
    pub store: &'a S,
    pub locks: crate::shard::OwnerLocks,
}

#[cfg(test)]
impl<E: Exposure, S: wavedb_core::Store> wavedb_core::Route
    for Direct<'_, E, S>
{
    async fn run(
        &self,
        tenant: wavedb_core::U48,
        struct_hash: u64,
        op: wavedb_core::NodeOp,
    ) -> wavedb_core::Result<wavedb_core::NodeReply> {
        let owner = op.owner(tenant, struct_hash);
        let key = crate::shard::OwnerKey::new(tenant, owner);
        let _held = self.locks.acquire(key).await;
        self.registry
            .run_node(self.store, tenant, struct_hash, op)
            .await
    }
}

#[cfg(test)]
impl<E: Exposure, S: wavedb_core::Store> Exec for Direct<'_, E, S> {
    async fn owned(&self, caller: Caller, frame: CommandFrame) -> Response {
        dispatch::call(self.registry, self.store, &self.locks, caller, frame)
            .await
    }
}
