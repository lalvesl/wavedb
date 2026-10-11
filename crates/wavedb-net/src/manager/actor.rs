//! The manager task's routing loop — single-threaded, never ends.
//!
//! Each POST becomes its own subtask (exchanges run concurrently; the
//! manager never serialises requests behind one another); each watched
//! `(addr, identity)` gets one long-lived actor ([`ws_conn`] or [`poll`])
//! the loop routes watch registrations to.
//!
//! **Lifecycle authority lives here**, not in the actors: the loop counts
//! each actor's watchers (by watch id) and drops the actor's channel when
//! the last one unregisters — the actor drains what is already queued and
//! exits, and a *later* watch necessarily arrives after the removal, so it
//! always gets a fresh actor (no ack can race a dying connection). An
//! actor dead for its own reasons (the socket broke) is detected by its
//! closed channel and replaced on the next watch — reconnection is a fresh
//! dial.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use futures::StreamExt;
use futures::channel::{mpsc, oneshot};
use wavedb_wire::{from_wire, to_wire};

use super::tasks::{self, Here, InFlight, Post, Refuse};
use super::{Cmd, ConnKey, PostFrames, WatchMode, poll, ws_conn};
use crate::frame::{Request, StreamFrame};
use crate::frames::FrameReader;
use crate::sync::{SyncReply, TopicCursor};

/// One live actor: its channel plus the watch ids riding it. The set (not
/// a bare count) makes stale unregistrations — a guard of a connection
/// that already died — harmless to a respawned successor.
struct Entry<C> {
    tx: mpsc::UnboundedSender<C>,
    watchers: HashSet<u64>,
}

/// Run the manager until its command channel closes (it never does — the
/// process-wide handle holds the sender).
pub(super) async fn run(mut rx: mpsc::UnboundedReceiver<Cmd>, here: Here) {
    // POSTs in flight. Counted here rather than left to the pool so a
    // refusal can answer `connected` with a cause: a refused spawn would
    // drop it unanswered.
    let in_flight = Rc::new(Cell::new(0usize));
    let mut ws_conns: HashMap<ConnKey, Entry<ws_conn::ConnCmd>> =
        HashMap::new();
    let mut polls: HashMap<ConnKey, Entry<poll::PollCmd>> = HashMap::new();
    while let Some(cmd) = rx.next().await {
        match cmd {
            Cmd::Post {
                addr,
                auth,
                frame,
                connected,
            } => {
                // Piggyback (W7): if a poll watch of this identity is live,
                // hand its channel to the exchange so it can declare the
                // cursors and route any returned delta straight back to it.
                let key = ConnKey {
                    addr: addr.clone(),
                    auth: auth.clone(),
                };
                let poll = polls.get(&key).map(|entry| entry.tx.clone());
                if in_flight.get() >= tasks::MAX_POSTS {
                    let _ = connected.send(Err(tasks::saturated("posts")));
                    continue;
                }
                let post = Post {
                    addr,
                    auth,
                    frame,
                    connected,
                    poll,
                    slot: InFlight::take(&in_flight),
                };
                // Cannot be refused: the count above keeps the pool from
                // filling, and a slot is released before its count is.
                let _ = tasks::spawn_post(&here, post);
            }
            Cmd::Watch {
                key,
                mode,
                topic,
                watch,
                events,
                ack,
            } => match mode {
                WatchMode::WebSocket => register(
                    &mut ws_conns,
                    key,
                    watch,
                    ws_conn::ConnCmd::Subscribe {
                        topic,
                        watch,
                        events,
                        ack,
                    },
                    |key| ws_conn::spawn(key, &here),
                ),
                WatchMode::HttpPoll(every) => register(
                    &mut polls,
                    key,
                    watch,
                    poll::PollCmd::Subscribe {
                        topic,
                        watch,
                        events,
                        ack,
                    },
                    |key| poll::spawn(key, every, &here),
                ),
            },
            Cmd::Unwatch {
                key,
                poll: is_poll,
                topic,
                watch,
            } => {
                if is_poll {
                    unregister(
                        &mut polls,
                        &key,
                        watch,
                        poll::PollCmd::Unsubscribe { topic, watch },
                    );
                } else {
                    unregister(
                        &mut ws_conns,
                        &key,
                        watch,
                        ws_conn::ConnCmd::Unsubscribe { topic, watch },
                    );
                }
            }
        }
    }
}

/// Deliver a watch registration to `key`'s actor, spawning one (or
/// replacing a dead one — its channel closed when its connection broke).
fn register<C: Refuse>(
    actors: &mut HashMap<ConnKey, Entry<C>>,
    key: ConnKey,
    watch: u64,
    cmd: C,
    spawn: impl FnOnce(&ConnKey) -> Option<mpsc::UnboundedSender<C>>,
) {
    let mut pending = cmd;
    if let Some(entry) = actors.get_mut(&key) {
        match entry.tx.unbounded_send(pending) {
            Ok(()) => {
                entry.watchers.insert(watch);
                return;
            }
            Err(returned) => pending = returned.into_inner(),
        }
    }
    let Some(tx) = spawn(&key) else {
        pending.refuse(); // the actor pool is full
        return;
    };
    let _ = tx.unbounded_send(pending);
    let watchers = match actors.remove(&key) {
        // Keep the dead predecessor's ids: their guards' unregistrations
        // are still coming and must not count against the successor.
        Some(dead) => dead.watchers,
        None => HashSet::new(),
    };
    let mut entry = Entry { tx, watchers };
    entry.watchers.insert(watch);
    actors.insert(key, entry);
}

/// Unregister a watcher; when the last one leaves, drop the actor's
/// channel — it drains what is queued and exits, closing its connection.
fn unregister<C>(
    actors: &mut HashMap<ConnKey, Entry<C>>,
    key: &ConnKey,
    watch: u64,
    cmd: C,
) {
    let Some(entry) = actors.get_mut(key) else {
        return; // Already gone — nothing was watching.
    };
    let _ = entry.tx.unbounded_send(cmd);
    entry.watchers.remove(&watch);
    if entry.watchers.is_empty() {
        actors.remove(key);
    }
}

/// One POSTed command exchange: snapshot the identity's live poll cursors (if
/// any) to piggyback (W7), build + send the `Request`, answer `connected`,
/// then pump the response frames to the caller — peeling a **leading**
/// piggyback `Sync` frame off the front and routing its delta back to the poll
/// actor, so only that first frame is ever inspected.
pub(super) async fn run_post(post: Post) {
    let Post {
        addr,
        auth,
        frame,
        connected,
        poll,
        slot: _slot, // held to the end: counted until the exchange is over
    } = post;
    let sync = match &poll {
        Some(handle) => snapshot(handle).await,
        None => Vec::new(),
    };
    let body = to_wire(&Request { auth, frame, sync });
    let mut frames = match wavedb_platform::http::post(&addr, &body).await {
        Ok(stream) => FrameReader::new(stream),
        Err(e) => {
            let _ = connected.send(Err(e.into()));
            return;
        }
    };
    let (tx, rx) = mpsc::unbounded();
    if connected.send(Ok(PostFrames { rx })).is_err() {
        return; // The caller went away before the head arrived.
    }
    let mut lead = poll.as_ref(); // set until the first frame is classified
    loop {
        match frames.next_frame().await {
            Ok(Some(bytes)) => {
                if let Some(handle) = lead.take()
                    && route_piggyback(handle, &bytes)
                {
                    continue; // leading Sync consumed — do not forward it
                }
                if tx.unbounded_send(Ok(bytes)).is_err() {
                    return; // The caller stopped reading mid-stream.
                }
            }
            // A clean end closes the channel by dropping the sender.
            Ok(None) => return,
            Err(e) => {
                let _ = tx.unbounded_send(Err(e));
                return;
            }
        }
    }
}

/// Ask a poll actor for its current cursor declaration to piggyback on a
/// command POST; empty if the actor is gone (no piggyback — the command still
/// runs, and the next ordinary tick catches the watch up).
async fn snapshot(
    handle: &mpsc::UnboundedSender<poll::PollCmd>,
) -> Vec<TopicCursor> {
    let (reply, answer) = oneshot::channel();
    if handle
        .unbounded_send(poll::PollCmd::Snapshot { reply })
        .is_err()
    {
        return Vec::new();
    }
    answer.await.unwrap_or_default()
}

/// Classify the leading response frame: a piggyback `Sync` delta is routed to
/// the poll actor and reported as `true` (consumed); anything else is `false`,
/// so the caller forwards it as an ordinary response frame.
fn route_piggyback(
    handle: &mpsc::UnboundedSender<poll::PollCmd>,
    bytes: &[u8],
) -> bool {
    let Ok(StreamFrame::Sync(delta)) = from_wire::<StreamFrame>(bytes) else {
        return false;
    };
    if let Ok(reply) = from_wire::<SyncReply>(&delta) {
        let _ = handle.unbounded_send(poll::PollCmd::ApplyDelta(reply));
    }
    true // even an undecodable delta was a Sync frame — never forward it
}
