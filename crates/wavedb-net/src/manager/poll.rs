//! The HTTP-poll actor — a watch for clients that cannot hold a WebSocket
//! open: one loop per `(addr, identity)` asking the node "anything new?"
//! on an adjustable timer ([`WatchMode::HttpPoll`](super::WatchMode)) that
//! **backs off when idle** — the interval grows after consecutive empty
//! ticks (capped at [`MAX_IDLE_FACTOR`]× the base) and snaps back to the base
//! on the first non-empty answer or a new subscription, so a quiet watch
//! stops hammering the node every interval (W7).
//!
//! Every tick (and immediately on a new watcher — the first successful
//! sync is the liveness ack) it declares the **whole** current topic list,
//! each topic with its **cursor** — the greatest node instant seen there
//! (`None` on a fresh topic: the node answers its current tail, so the watch
//! begins at "now"). The node holds no session state: it navigates the disk
//! past each cursor and answers the changes plus the advanced cursors, which
//! the actor stores for the next tick (the exchange is [`super::sync_call`]).
//! Events fan out to watchers exactly like the WebSocket path.
//!
//! Outage semantics differ from a pushed watch on purpose: a **transport**
//! fault is tolerated (polling is loosely coupled — the next tick retries,
//! watchers just see silence), while a **node refusal** (an expired token,
//! an anonymous sync) is authoritative and ends the actor, closing every
//! watcher's stream. Events committed while the node was down — or before
//! it restarted — are **not** missed: the cursor survives here, and the
//! next successful tick navigates everything past it (W6).

use core::time::Duration;
use std::collections::HashMap;

use futures::channel::{mpsc, oneshot};
use futures::{FutureExt, StreamExt, select};

use super::tasks::{self, Here};
use super::{ConnKey, sync_call};
use crate::error::{Error, Result};
use crate::sync::{SyncReply, TopicCursor};
use crate::ws::{RecordEvent, Topic};

/// Idle backoff (W7): an empty tick multiplies the poll interval by this.
const IDLE_GROWTH: u32 = 2;
/// The idle interval ceiling, as a multiple of the caller's base interval.
const MAX_IDLE_FACTOR: u32 = 16;

/// What the manager routes to a poll actor.
pub(super) enum PollCmd {
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
    /// A command POST wants this identity's live cursors to piggyback (W7):
    /// the actor answers each watched topic with its current cursor.
    Snapshot {
        reply: oneshot::Sender<Vec<TopicCursor>>,
    },
    /// A command reply carried a piggyback delta (W7): apply it like a tick
    /// (fan-out + advance cursors), deduped against concurrent ticks.
    ApplyDelta(SyncReply),
}

/// Spawn the poll actor for `key`, ticking at most every `every` (the base
/// interval; idle backoff may stretch it, never below it).
pub(super) fn spawn(
    key: &ConnKey,
    every: Duration,
    here: &Here,
) -> Option<mpsc::UnboundedSender<PollCmd>> {
    let (tx, rx) = mpsc::unbounded();
    tasks::spawn_poll(here, key.clone(), every, rx).then_some(tx)
}

/// The watchers per topic, each topic's sync cursor, plus the acks a next
/// successful sync completes.
#[derive(Default)]
struct PollState {
    topics: HashMap<Topic, Vec<(u64, mpsc::UnboundedSender<RecordEvent>)>>,
    /// The greatest node instant seen per topic — absent until the first
    /// successful sync registers it (the node answers the tail).
    cursors: HashMap<Topic, u64>,
    pending: Vec<oneshot::Sender<Result<()>>>,
}

/// Why the poll loop woke.
enum Wake {
    /// The command channel closed — the last watcher left. Stop for good.
    Closed,
    /// A subscribe/unsubscribe arrived.
    Cmd(PollCmd),
    /// The idle timer fired.
    Tick,
}

pub(super) async fn run(
    key: ConnKey,
    every: Duration,
    mut cmds: mpsc::UnboundedReceiver<PollCmd>,
) {
    let mut state = PollState::default();
    let mut interval = every;
    loop {
        let wake = select! {
            cmd = cmds.next() => cmd.map_or(Wake::Closed, Wake::Cmd),
            () = wavedb_platform::time::sleep(interval).fuse() => Wake::Tick,
        };
        match wake {
            Wake::Closed => break,
            // A new subscription registers + drains now (its first sync is the
            // liveness ack) and snaps the idle backoff back to the base.
            Wake::Cmd(cmd) => {
                if apply_cmd(&mut state, cmd) {
                    interval = every;
                    if sync(&key, &mut state).await.is_err() {
                        break;
                    }
                }
            }
            Wake::Tick => {
                if state.topics.is_empty() {
                    continue;
                }
                match sync(&key, &mut state).await {
                    Err(()) => break, // refusal — end every watch stream
                    Ok(delivered) => {
                        interval = next_interval(interval, every, delivered);
                    }
                }
            }
        }
    }
}

/// Apply one command to `state`; `true` = a sync is due now (a new
/// subscription — its first sync is the liveness ack). Unsubscribe drops the
/// watcher (and, when a topic empties, forgets its cursor so a later re-watch
/// starts at "now", exactly like a fresh WebSocket subscribe) and never syncs.
fn apply_cmd(state: &mut PollState, cmd: PollCmd) -> bool {
    match cmd {
        PollCmd::Subscribe {
            topic,
            watch,
            events,
            ack,
        } => {
            state.topics.entry(topic).or_default().push((watch, events));
            state.pending.push(ack);
            true
        }
        PollCmd::Unsubscribe { topic, watch } => {
            if let Some(subs) = state.topics.get_mut(&topic) {
                subs.retain(|(id, _)| *id != watch);
                if subs.is_empty() {
                    state.topics.remove(&topic);
                    state.cursors.remove(&topic);
                }
            }
            false
        }
        PollCmd::Snapshot { reply } => {
            let _ = reply.send(declaration(state));
            false
        }
        PollCmd::ApplyDelta(delta) => {
            apply_reply(state, delta);
            false
        }
    }
}

/// This identity's declared subscriptions, each with its cursor — the
/// piggyback declaration (W7) a command POST rides on the caller's behalf.
fn declaration(state: &PollState) -> Vec<TopicCursor> {
    state
        .topics
        .keys()
        .map(|topic| TopicCursor {
            topic: *topic,
            since: state.cursors.get(topic).copied(),
        })
        .collect()
}

/// Fan a sync reply out to the watchers, **deduped** against each topic's
/// cursor (an instant already delivered — by a concurrent tick or a command
/// piggyback — is dropped), then merge the reply's advanced cursors. Shared by
/// the tick ([`sync`]) and the piggyback ([`PollCmd::ApplyDelta`]).
fn apply_reply(state: &mut PollState, reply: SyncReply) {
    for event in reply.events {
        if let Some(instant) = event.instant()
            && state
                .cursors
                .get(&event.topic)
                .is_some_and(|c| instant <= *c)
        {
            continue; // already delivered — overlap between the two sources
        }
        if let Some(subs) = state.topics.get_mut(&event.topic) {
            subs.retain(|(_, tx)| tx.unbounded_send(event.clone()).is_ok());
        }
    }
    // Advanced cursors only for topics still watched (an unsubscribed one
    // stays gone); never regress a cursor a concurrent source moved past.
    for (topic, cursor) in reply.cursors {
        if state.topics.contains_key(&topic) {
            let entry = state.cursors.entry(topic).or_insert(cursor);
            *entry = (*entry).max(cursor);
        }
    }
}

/// The next poll interval under idle backoff: the base on a non-empty answer,
/// otherwise `current` grown by [`IDLE_GROWTH`] and capped at
/// [`MAX_IDLE_FACTOR`]× the base.
fn next_interval(
    current: Duration,
    base: Duration,
    delivered: bool,
) -> Duration {
    if delivered {
        base
    } else {
        current
            .saturating_mul(IDLE_GROWTH)
            .min(base.saturating_mul(MAX_IDLE_FACTOR))
    }
}

/// One "anything new?" exchange. `Ok(true)` delivered ≥1 event, `Ok(false)`
/// was an empty tick (or a tolerated transport fault — treated as idle).
/// `Err` = a node refusal (fatal). A transport fault fails only the pending
/// (initial) acks — established watchers ride the outage and the next tick
/// retries.
async fn sync(
    key: &ConnKey,
    state: &mut PollState,
) -> core::result::Result<bool, ()> {
    let cursors: HashMap<Topic, Option<u64>> = state
        .topics
        .keys()
        .map(|topic| (*topic, state.cursors.get(topic).copied()))
        .collect();
    match sync_call::sync_once(&key.addr, &key.auth, &cursors).await {
        Ok(reply) => {
            let delivered = !reply.events.is_empty();
            apply_reply(state, reply);
            for ack in state.pending.drain(..) {
                let _ = ack.send(Ok(()));
            }
            Ok(delivered)
        }
        Err(error) => {
            let fatal = matches!(error, Error::Node(_));
            let mut error = Some(error);
            for ack in state.pending.drain(..) {
                let _ = ack.send(Err(error
                    .take()
                    .unwrap_or(Error::Http("sync poll failed"))));
            }
            if fatal { Err(()) } else { Ok(false) }
        }
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use super::{IDLE_GROWTH, MAX_IDLE_FACTOR, next_interval};

    const BASE: Duration = Duration::from_millis(500);

    #[test]
    fn a_non_empty_answer_snaps_back_to_the_base() {
        // However far the interval has grown, one delivered event resets it.
        assert_eq!(next_interval(BASE * 8, BASE, true), BASE);
    }

    #[test]
    fn empty_ticks_grow_the_interval_geometrically() {
        let mut interval = next_interval(BASE, BASE, false);
        assert_eq!(interval, BASE * IDLE_GROWTH);
        interval = next_interval(interval, BASE, false);
        assert_eq!(interval, BASE * IDLE_GROWTH * IDLE_GROWTH);
    }

    #[test]
    fn the_idle_interval_is_capped_at_the_ceiling() {
        let ceiling = BASE * MAX_IDLE_FACTOR;
        let mut interval = BASE;
        for _ in 0..12 {
            interval = next_interval(interval, BASE, false);
        }
        assert_eq!(interval, ceiling);
    }
}
