//! One WebSocket connection actor — the identity presented once, N topics
//! multiplexed over it, events fanned out to every watcher of their topic.
//!
//! The socket splits: a reader subtask owns the receive half (frame reads are
//! not cancel-safe) and forwards messages over a channel ([`ws_dial`](super::ws_dial));
//! the actor selects over that and its command channel, owning the send half.
//! A topic is wire-subscribed once however many watchers share it, and
//! wire-unsubscribed when its last watcher leaves.
//!
//! **The actor survives a dropped socket (W6).** On loss it re-dials,
//! re-subscribes every live topic, and *catches up* each one — navigating the
//! node past the topic's cursor (the greatest instant delivered) via the
//! stateless sync exchange — before trusting the resumed push, so a transient
//! blip does not end a watch and no mutation committed during the outage is
//! missed. The actor ends only when its last watcher leaves (its command
//! channel closes) or the node refuses its identity (fatal).

use core::time::Duration;
use std::collections::HashMap;

use futures::channel::{mpsc, oneshot};
use futures::{FutureExt, StreamExt, select};
use wavedb_platform::ws::{Received, SendHalf};
use wavedb_wire::{from_wire, to_wire};

use super::ConnKey;
use super::tasks::{self, Here};
use super::ws_dial::{self, MsgRx};
use crate::error::{Error, Result};
use crate::ws::{ClientMsg, RecordEvent, ServerMsg, Topic};

/// Bounded reconnect backoff.
const MIN_BACKOFF: Duration = Duration::from_millis(200);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// What the manager routes to a connection actor.
pub(super) enum ConnCmd {
    Subscribe {
        topic: Topic,
        watch: u64,
        events: mpsc::UnboundedSender<RecordEvent>,
        ack: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        topic: Topic,
        watch: u64,
    },
}

/// One topic's client-side state on this connection.
#[derive(Default)]
pub(super) struct TopicState {
    /// The node acked the subscription — events can arrive.
    pub(super) live: bool,
    /// The watchers fanned out to, keyed by watch id.
    subs: Vec<(u64, mpsc::UnboundedSender<RecordEvent>)>,
    /// Subscribe acks awaiting the node's `TopicOk`.
    pending: Vec<oneshot::Sender<Result<()>>>,
    /// Reconnect cursor: the greatest node instant delivered here. Seeded
    /// (only when unset) from the subscribe ack's tail, advanced by every
    /// delivered event; catch-up navigates the node past it.
    pub(super) cursor: Option<u64>,
}

/// Spawn the actor for `key`; the returned sender is the manager's route to
/// it (its closing is how a dead actor is detected).
pub(super) fn spawn(
    key: &ConnKey,
    here: &Here,
) -> Option<mpsc::UnboundedSender<ConnCmd>> {
    let (tx, rx) = mpsc::unbounded();
    tasks::spawn_ws(here, key.clone(), rx).then_some(tx)
}

pub(super) async fn run(
    key: ConnKey,
    mut cmds: mpsc::UnboundedReceiver<ConnCmd>,
    here: Here,
) {
    // The first connect fails fast: the initial watch() gets the error.
    let (mut send, mut msgs) = match ws_dial::open(&key, &here).await {
        Ok(halves) => halves,
        Err(error) => {
            fail_queued(&mut cmds, error);
            return;
        }
    };
    let mut topics: HashMap<Topic, TopicState> = HashMap::new();
    loop {
        let exit = serve(&mut send, &mut msgs, &mut topics, &mut cmds).await;
        let _ = send.close().await;
        match exit {
            Exit::Teardown => break,
            Exit::Reconnect => {
                match reconnect(&key, &mut topics, &mut cmds, &here).await {
                    Some((s, m)) => (send, msgs) = (s, m),
                    None => break,
                }
            }
        }
    }
    // Dropping `topics` drops every event sender (streams end) and every
    // pending ack (their watch() calls error out).
}

/// Why the connection loop returned.
enum Exit {
    /// The command channel closed — the last watcher left. Stop for good.
    Teardown,
    /// The socket dropped (or a protocol fault) — reconnect and catch up.
    Reconnect,
}

/// The steady-state select loop over one live connection.
async fn serve(
    send: &mut SendHalf,
    msgs: &mut MsgRx,
    topics: &mut HashMap<Topic, TopicState>,
    cmds: &mut mpsc::UnboundedReceiver<ConnCmd>,
) -> Exit {
    loop {
        select! {
            cmd = cmds.next() => {
                let Some(cmd) = cmd else { return Exit::Teardown };
                if handle_cmd(send, topics, cmd).await.is_err() {
                    return Exit::Reconnect; // a send failed — socket lost
                }
            }
            msg = msgs.next() => {
                let Some(msg) = msg else { return Exit::Reconnect }; // ended
                if handle_msg(send, topics, msg).await.is_err() {
                    return Exit::Reconnect;
                }
            }
        }
    }
}

async fn handle_cmd(
    send: &mut SendHalf,
    topics: &mut HashMap<Topic, TopicState>,
    cmd: ConnCmd,
) -> Result<()> {
    match cmd {
        ConnCmd::Subscribe {
            topic,
            watch,
            events,
            ack,
        } => {
            let state = topics.entry(topic).or_default();
            state.subs.push((watch, events));
            if state.live {
                let _ = ack.send(Ok(())); // already acked by the node
            } else {
                let first = state.pending.is_empty();
                state.pending.push(ack);
                if first {
                    send.send(&to_wire(&ClientMsg::Subscribe(topic))).await?;
                }
            }
        }
        ConnCmd::Unsubscribe { topic, watch } => {
            let Some(state) = topics.get_mut(&topic) else {
                return Ok(());
            };
            state.subs.retain(|(id, _)| *id != watch);
            if state.subs.is_empty() && state.pending.is_empty() {
                topics.remove(&topic);
                send.send(&to_wire(&ClientMsg::Unsubscribe(topic))).await?;
            }
        }
    }
    Ok(())
}

async fn handle_msg(
    send: &mut SendHalf,
    topics: &mut HashMap<Topic, TopicState>,
    msg: Received,
) -> Result<()> {
    let bytes = match msg {
        Received::Ping(payload) => {
            send.pong(&payload).await?;
            return Ok(());
        }
        Received::Binary(bytes) => bytes,
    };
    match from_wire::<ServerMsg>(&bytes) {
        Ok(ServerMsg::TopicOk(topic, tail)) => {
            ack_topic(topics, topic, tail);
            Ok(())
        }
        Ok(ServerMsg::Event(event)) => {
            deliver(topics, &event);
            Ok(())
        }
        // A watch connection has no call in flight — anything else is
        // protocol confusion; drop the connection (and reconnect).
        _ => Err(Error::Http("unexpected websocket message")),
    }
}

/// Mark a topic live, seed its cursor (only when unset — a reconnect must not
/// let the fresh tail skip the outage), and resolve pending acks.
pub(super) fn ack_topic(
    topics: &mut HashMap<Topic, TopicState>,
    topic: Topic,
    tail: u64,
) {
    if let Some(state) = topics.get_mut(&topic) {
        state.live = true;
        let _ = state.cursor.get_or_insert(tail);
        for ack in state.pending.drain(..) {
            let _ = ack.send(Ok(()));
        }
    }
}

/// Fan an event out to its topic's watchers, **deduped** against the cursor
/// (an instant already at or below it was caught up — dropped) and advancing
/// the cursor past it. A watcher that dropped its receiver is pruned.
pub(super) fn deliver(
    topics: &mut HashMap<Topic, TopicState>,
    event: &RecordEvent,
) {
    let Some(state) = topics.get_mut(&event.topic) else {
        return;
    };
    if let Some(instant) = event.instant() {
        if state.cursor.is_some_and(|cursor| instant <= cursor) {
            return; // already delivered (overlap after a catch-up)
        }
        state.cursor = Some(state.cursor.map_or(instant, |c| c.max(instant)));
    }
    state
        .subs
        .retain(|(_, tx)| tx.unbounded_send(event.clone()).is_ok());
}

/// Fail the commands queued behind a first-dial fault — the first ack gets
/// the real error. New watches respawn a fresh actor.
fn fail_queued(cmds: &mut mpsc::UnboundedReceiver<ConnCmd>, error: Error) {
    cmds.close();
    let mut error = Some(error);
    while let Ok(cmd) = cmds.try_recv() {
        if let ConnCmd::Subscribe { ack, .. } = cmd {
            let _ = ack.send(Err(error
                .take()
                .unwrap_or(Error::Http("websocket connect failed"))));
        }
    }
}

/// Reconnect after a drop: apply any watches that changed during the outage,
/// re-dial with bounded backoff, and re-establish. `None` = teardown (the last
/// watcher left) or a fatal identity refusal.
async fn reconnect(
    key: &ConnKey,
    topics: &mut HashMap<Topic, TopicState>,
    cmds: &mut mpsc::UnboundedReceiver<ConnCmd>,
    here: &Here,
) -> Option<(SendHalf, MsgRx)> {
    let mut backoff = MIN_BACKOFF;
    loop {
        drain_offline(cmds, topics);
        match ws_dial::open(key, here).await {
            Ok((mut send, mut msgs)) => {
                match ws_dial::resubscribe(key, &mut send, &mut msgs, topics)
                    .await
                {
                    Ok(()) => return Some((send, msgs)),
                    Err(Error::Node(_)) => return None, // authoritative refusal
                    Err(_) => {
                        let _ = send.close().await; // transport fault → retry
                    }
                }
            }
            Err(Error::Http("websocket hello refused")) => return None, // fatal
            Err(_) => {} // transport fault → retry
        }
        // Bounded backoff, woken early by a command (new subscribe / teardown).
        select! {
            cmd = cmds.next() => match cmd {
                None => return None,
                Some(cmd) => apply_offline(cmd, topics),
            },
            () = wavedb_platform::time::sleep(backoff).fuse() => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Apply every command already queued (a subscribe/unsubscribe that landed
/// during the outage) to `topics`. Channel closure (teardown) is caught by
/// the caller's backoff `select!`, not here.
fn drain_offline(
    cmds: &mut mpsc::UnboundedReceiver<ConnCmd>,
    topics: &mut HashMap<Topic, TopicState>,
) {
    while let Ok(cmd) = cmds.try_recv() {
        apply_offline(cmd, topics);
    }
}

/// Apply one command with no socket to send on — the wire (re)subscribe is
/// deferred to [`resubscribe`]; the pending ack rides until then.
fn apply_offline(cmd: ConnCmd, topics: &mut HashMap<Topic, TopicState>) {
    match cmd {
        ConnCmd::Subscribe {
            topic,
            watch,
            events,
            ack,
        } => {
            let state = topics.entry(topic).or_default();
            state.subs.push((watch, events));
            state.pending.push(ack);
            state.live = false;
        }
        ConnCmd::Unsubscribe { topic, watch } => {
            if let Some(state) = topics.get_mut(&topic) {
                state.subs.retain(|(id, _)| *id != watch);
                if state.subs.is_empty() && state.pending.is_empty() {
                    topics.remove(&topic);
                }
            }
        }
    }
}

// `resubscribe` (re-subscribe every topic + catch up past its cursor on a
// fresh socket) lives in `ws_dial` — connection establishment — alongside
// `open`, so this module stays within the file budget.
