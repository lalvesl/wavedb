//! The connection manager — **one never-ending background task per
//! process** that owns every exchange with a node.
//!
//! Every request the client makes — an HTTP POST command or a WebSocket
//! watch — routes through this task; it is the single place connections
//! are dialed, shared, and torn down (and the natural seam for the M7
//! reconnect cursor and the offline write queue to land on). Natively it
//! lives on a dedicated thread with its own current-thread runtime; in the
//! browser it is a detached `spawn_local` task — no tokio in wasm
//! ([`wavedb_platform::task`] is the seam). It boots lazily on first use
//! and never ends.
//!
//! **Watches multiplex.** All watches presenting the same identity to the
//! same address share ONE WebSocket connection (`ws_conn` — the `Hello`
//! runs once, each topic subscribes once, events fan out to every watcher
//! of their topic). No pumping falls on the watchers: the connection's
//! reader pushes each event into the right watcher channel as it arrives.
//!
//! **Watches can ride plain HTTP** ([`WatchMode::HttpPoll`]): a
//! per-identity `poll` loop asks the node "anything new?" on an adjustable
//! timer and fans the buffered events out the same way — for clients whose
//! path to the node cannot hold a WebSocket open. Here the timer *is* the
//! pump.

mod actor;
mod boot;
mod poll;
mod sync_call;
mod ws_conn;
mod ws_dial;

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;

use futures::StreamExt;
use futures::channel::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::frame::{Auth, CommandFrame};
use crate::ws::{RecordEvent, Topic};

/// How a watch's events reach the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchMode {
    /// Push over one shared WebSocket connection per identity (default).
    WebSocket,
    /// "Anything new?" POST polls at the given interval — for clients
    /// whose path to the node cannot hold a WebSocket open.
    HttpPoll(Duration),
}

/// One connection's identity key: everything on it executes as this
/// `(addr, auth)` pair — a different token is a different connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ConnKey {
    pub addr: String,
    pub auth: Auth,
}

/// The response frames of one POSTed exchange, pumped by the manager.
#[derive(Debug)]
pub(crate) struct PostFrames {
    rx: mpsc::UnboundedReceiver<Result<Vec<u8>>>,
}

impl PostFrames {
    /// The next `[len][bytes]` frame; `None` = the response ended cleanly.
    pub(crate) async fn next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        self.rx.next().await.transpose()
    }
}

/// What the manager task routes.
enum Cmd {
    /// POST a command `frame` under `auth` to `addr`; `connected` resolves
    /// once the response head is in — with the frame channel, or the
    /// establishment fault. The manager builds the `Request` so it can
    /// piggyback the identity's live poll cursors (W7).
    Post {
        addr: String,
        auth: Auth,
        frame: CommandFrame,
        connected: oneshot::Sender<Result<PostFrames>>,
    },
    /// Register a watcher; `ack` resolves once the subscription is live.
    Watch {
        key: ConnKey,
        mode: WatchMode,
        topic: Topic,
        watch: u64,
        events: mpsc::UnboundedSender<RecordEvent>,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Unregister a watcher (fired by [`WatchGuard`] on drop).
    Unwatch {
        key: ConnKey,
        poll: bool,
        topic: Topic,
        watch: u64,
    },
}

/// A cloneable handle into the manager task.
#[derive(Debug, Clone)]
struct Handle {
    tx: mpsc::UnboundedSender<Cmd>,
}

impl Handle {
    fn send(&self, cmd: Cmd) -> Result<()> {
        self.tx
            .unbounded_send(cmd)
            .map_err(|_| Error::ManagerUnavailable)
    }
}

/// POST one command through the manager. Resolves once the response head is
/// in, preserving the establish-vs-mid-stream error split the callers rely on
/// (a cache falls back on an establishment fault). The manager assembles the
/// wire `Request`, attaching the identity's live poll cursors when one is
/// watching (W7 piggyback), and peels any returned delta back to that watch.
pub(crate) async fn post(
    addr: &str,
    auth: Auth,
    frame: CommandFrame,
) -> Result<PostFrames> {
    let handle = boot::handle()?;
    let (connected, ready) = oneshot::channel();
    handle.send(Cmd::Post {
        addr: addr.to_owned(),
        auth,
        frame,
        connected,
    })?;
    ready.await.map_err(|_| Error::ManagerUnavailable)?
}

/// Ends its watch on drop — fire-and-forget unregistration; when a
/// connection's last watcher leaves, the manager closes the connection.
#[derive(Debug)]
pub struct WatchGuard {
    handle: Handle,
    key: ConnKey,
    poll: bool,
    topic: Topic,
    watch: u64,
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        let _ = self.handle.send(Cmd::Unwatch {
            key: self.key.clone(),
            poll: self.poll,
            topic: self.topic,
            watch: self.watch,
        });
    }
}

/// Watcher id source — only disambiguates senders on a shared topic.
static WATCH_IDS: AtomicU64 = AtomicU64::new(0);

/// Open a live watch on `topic`, presenting `auth` to `addr` over `mode`.
///
/// Returns once the subscription is **live** (acked over WebSocket;
/// registered by a first successful sync over HTTP poll), so a mutation
/// committed after this call cannot be missed. The channel closing means
/// the connection ended (a poll watch instead rides outages silently —
/// its loop retries every tick).
///
/// # Errors
/// A dial/handshake/poll fault, a refused identity, or
/// [`Error::ManagerUnavailable`] when the manager task cannot run.
pub async fn watch(
    addr: &str,
    auth: Auth,
    mode: WatchMode,
    topic: Topic,
) -> Result<(mpsc::UnboundedReceiver<RecordEvent>, WatchGuard)> {
    let handle = boot::handle()?;
    let key = ConnKey {
        addr: addr.to_owned(),
        auth,
    };
    let watch = WATCH_IDS.fetch_add(1, Ordering::Relaxed);
    let (events_tx, events_rx) = mpsc::unbounded();
    let (ack_tx, ack_rx) = oneshot::channel();
    handle.send(Cmd::Watch {
        key: key.clone(),
        mode,
        topic,
        watch,
        events: events_tx,
        ack: ack_tx,
    })?;
    let guard = WatchGuard {
        handle,
        key,
        poll: matches!(mode, WatchMode::HttpPoll(_)),
        topic,
        watch,
    };
    // A dropped ack = the connection died before answering; the guard
    // going down with it unregisters whatever survived.
    ack_rx
        .await
        .map_err(|_| Error::Http("watch connection closed before the ack"))??;
    Ok((events_rx, guard))
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use core::time::Duration;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use wavedb_core::expose::{Command, Reply};
    use wavedb_core::{Id, Metadata, Succession, U48};
    use wavedb_platform::io::{Listener, ReadHalf, WriteHalf};
    use wavedb_platform::ws::codec::{self, Messages, Msg, OP_BINARY};
    use wavedb_wire::{from_wire, to_wire};

    use futures::StreamExt as _;

    use super::{WatchMode, post, watch};
    use crate::frame::{Auth, CommandFrame, Request, Response, StreamFrame};
    use crate::http;
    use crate::sync::{SYNC_STRUCT_HASH, SyncReply};
    use crate::ws::{ClientMsg, EventKind, RecordEvent, ServerMsg, Topic};

    const TOPIC_A: Topic = Topic {
        struct_hash: 0xA,
        pivot: None,
    };
    const TOPIC_B: Topic = Topic {
        struct_hash: 0xB,
        pivot: None,
    };
    /// Subscribing to this makes the server push one event on A and one on
    /// B first — a FIFO barrier: they fan out before the trigger acks.
    const TRIGGER: Topic = Topic {
        struct_hash: 0xF00D,
        pivot: None,
    };

    fn event(topic: Topic) -> RecordEvent {
        RecordEvent {
            topic,
            id: Id::new(topic.struct_hash, U48::from(1u32), false, 0),
            kind: EventKind::Saved,
            meta: None,
            body: vec![1],
        }
    }

    async fn send(w: &mut WriteHalf, msg: &ServerMsg) {
        codec::write_message(w, OP_BINARY, &to_wire(msg), false)
            .await
            .expect("server send");
    }

    /// A watch-shaped mini node: upgrade, `Hello`→`HelloOk`, ack every
    /// subscription, push the canned events on the trigger. Counts accepts.
    async fn serve_conn(mut msgs: Messages<ReadHalf>, mut w: WriteHalf) {
        while let Ok(Some(msg)) = msgs.next(true).await {
            let Msg::Binary(bytes) = msg else { continue };
            match from_wire::<ClientMsg>(&bytes).expect("decode") {
                ClientMsg::Hello(_) => send(&mut w, &ServerMsg::HelloOk).await,
                ClientMsg::Subscribe(topic) => {
                    if topic == TRIGGER {
                        send(&mut w, &ServerMsg::Event(event(TOPIC_A))).await;
                        send(&mut w, &ServerMsg::Event(event(TOPIC_B))).await;
                    }
                    send(&mut w, &ServerMsg::TopicOk(topic, 0)).await;
                }
                ClientMsg::Unsubscribe(topic) => {
                    send(&mut w, &ServerMsg::TopicOk(topic, 0)).await;
                }
                ClientMsg::Call(_) => panic!("a watch never calls"),
            }
        }
    }

    fn mini_node() -> (String, Arc<AtomicUsize>) {
        // Bind before spawning: on the test's current-thread runtime a
        // blocking wait for the spawned task would deadlock.
        let listener = Listener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let conns = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&conns);
        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.expect("accept");
                counted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let (mut r, mut w) = sock.split();
                    let Ok(Some(http::Incoming::Upgrade { key })) =
                        http::read_request(&mut r).await
                    else {
                        return;
                    };
                    http::write_switching_head(
                        &mut w,
                        &codec::accept_key(&key),
                    )
                    .await
                    .expect("101");
                    serve_conn(Messages::new(r), w).await;
                });
            }
        });
        (addr, conns)
    }

    #[tokio::test]
    async fn watches_of_one_identity_share_one_connection() {
        let (addr, conns) = mini_node();
        let auth = Auth::Anonymous {
            tenant: U48::from(7u32),
        };

        // Hang guard: any broken path must fail the test, not wedge it.
        macro_rules! within {
            ($fut:expr) => {
                wavedb_platform::time::timeout(
                    std::time::Duration::from_secs(30),
                    $fut,
                )
                .await
                .expect("timed out")
            };
        }

        // Three watches, two topics, one identity — and later, a trigger.
        let (mut a1, g1) =
            within!(watch(&addr, auth.clone(), WatchMode::WebSocket, TOPIC_A))
                .expect("watch a1");
        let (mut b, g2) =
            within!(watch(&addr, auth.clone(), WatchMode::WebSocket, TOPIC_B))
                .expect("watch b");
        let (mut a2, g3) =
            within!(watch(&addr, auth.clone(), WatchMode::WebSocket, TOPIC_A))
                .expect("watch a2 — shared topic acks locally");
        let (_trigger_rx, g4) =
            within!(watch(&addr, auth.clone(), WatchMode::WebSocket, TRIGGER))
                .expect("trigger");

        assert_eq!(within!(a1.next()), Some(event(TOPIC_A)), "first A watcher");
        assert_eq!(within!(a2.next()), Some(event(TOPIC_A)), "fanned out");
        assert_eq!(within!(b.next()), Some(event(TOPIC_B)));
        assert_eq!(
            conns.load(Ordering::SeqCst),
            1,
            "one identity = one connection, however many watches"
        );

        // Dropping every guard closes the connection; the next watch of
        // the same identity dials a fresh one — never a dying actor.
        drop((g1, g2, g3, g4));
        let (_rx, _g) =
            within!(watch(&addr, auth, WatchMode::WebSocket, TOPIC_A))
                .expect("fresh watch after teardown");
        assert_eq!(conns.load(Ordering::SeqCst), 2, "a fresh dial");
    }

    /// A `Saved` event carrying a live-version instant in its metadata, so
    /// [`RecordEvent::instant`] resolves — the cursor can advance and dedup.
    fn saved_event(topic: Topic, instant: u64) -> RecordEvent {
        RecordEvent {
            topic,
            id: Id::new(topic.struct_hash, U48::from(1u32), false, 0),
            kind: EventKind::Saved,
            meta: Some(Metadata {
                succession: Succession::CreatedAt(instant),
                ..Metadata::default()
            }),
            body: vec![1],
        }
    }

    /// The catch-up POST answer: a `SyncReply` carrying the "downtime" event
    /// (instant 20) the client missed while its socket was down.
    async fn answer_catch_up(w: &mut WriteHalf, topic: Topic) {
        let reply = SyncReply {
            events: vec![saved_event(topic, 20)],
            cursors: vec![(topic, 20)],
        };
        let end =
            StreamFrame::End(Response::Ok(Reply::Returned(to_wire(&reply))));
        http::write_ok_head(w).await.expect("ok head");
        http::write_frame(w, &to_wire(&end)).await.expect("frame");
    }

    /// One WS connection of the reconnect node: `Hello`, ack the subscribe,
    /// and — on the **first** connection only — push a live event (instant 10)
    /// then drop the socket, forcing the client to reconnect.
    async fn reconnect_ws(
        mut msgs: Messages<ReadHalf>,
        mut w: WriteHalf,
        first: bool,
    ) {
        while let Ok(Some(Msg::Binary(bytes))) = msgs.next(true).await {
            match from_wire::<ClientMsg>(&bytes).expect("decode") {
                ClientMsg::Hello(_) => send(&mut w, &ServerMsg::HelloOk).await,
                ClientMsg::Subscribe(topic) => {
                    send(&mut w, &ServerMsg::TopicOk(topic, 0)).await;
                    if first {
                        send(&mut w, &ServerMsg::Event(saved_event(topic, 10)))
                            .await;
                        return; // drop the socket → the client reconnects
                    }
                }
                ClientMsg::Unsubscribe(topic) => {
                    send(&mut w, &ServerMsg::TopicOk(topic, 0)).await;
                }
                ClientMsg::Call(_) => panic!("a watch never calls"),
            }
        }
    }

    #[tokio::test]
    async fn a_watch_survives_a_dropped_socket_and_catches_up() {
        let listener = Listener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let ws_conns = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&ws_conns);
        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.expect("accept");
                let counted = Arc::clone(&counted);
                tokio::spawn(async move {
                    let (mut r, mut w) = sock.split();
                    match http::read_request(&mut r).await {
                        Ok(Some(http::Incoming::Upgrade { key })) => {
                            http::write_switching_head(
                                &mut w,
                                &codec::accept_key(&key),
                            )
                            .await
                            .expect("101");
                            let first =
                                counted.fetch_add(1, Ordering::SeqCst) == 0;
                            reconnect_ws(Messages::new(r), w, first).await;
                        }
                        Ok(Some(http::Incoming::Post(body))) => {
                            let request =
                                from_wire::<Request>(&body).expect("request");
                            assert_eq!(
                                request.frame.struct_hash,
                                SYNC_STRUCT_HASH
                            );
                            answer_catch_up(&mut w, TOPIC_A).await;
                        }
                        _ => {}
                    }
                });
            }
        });

        macro_rules! within {
            ($fut:expr) => {
                wavedb_platform::time::timeout(Duration::from_secs(30), $fut)
                    .await
                    .expect("timed out")
            };
        }

        let auth = Auth::Anonymous {
            tenant: U48::from(3u32),
        };
        let (mut events, _guard) =
            within!(watch(&addr, auth, WatchMode::WebSocket, TOPIC_A))
                .expect("watch");

        // The live event before the drop, then — after the socket dies and the
        // manager reconnects — the event missed during the outage, delivered by
        // navigation catch-up. The stream never ended.
        assert_eq!(
            within!(events.next()),
            Some(saved_event(TOPIC_A, 10)),
            "the pre-drop live event"
        );
        assert_eq!(
            within!(events.next()),
            Some(saved_event(TOPIC_A, 20)),
            "the missed event, delivered by catch-up after reconnect"
        );
        assert!(
            ws_conns.load(Ordering::SeqCst) >= 2,
            "the watch re-dialed after the socket dropped"
        );
    }

    /// An HTTP mini-node for the piggyback path: it answers a poll register
    /// (SYNC hash, `since: None`) with the tail cursor and no events, and any
    /// other command by leading its reply with a `Sync` delta carrying one
    /// event — after asserting the command actually declared the poll cursor.
    fn piggyback_node() -> String {
        let listener = Listener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.expect("accept");
                tokio::spawn(async move {
                    let (mut r, mut w) = sock.split();
                    let Ok(Some(http::Incoming::Post(body))) =
                        http::read_request(&mut r).await
                    else {
                        return;
                    };
                    let request = from_wire::<Request>(&body).expect("request");
                    http::write_ok_head(&mut w).await.expect("ok head");
                    if request.frame.struct_hash == SYNC_STRUCT_HASH {
                        // Poll register: seed the cursor at tail 100, no events.
                        let reply = SyncReply {
                            events: Vec::new(),
                            cursors: vec![(TOPIC_A, 100)],
                        };
                        let end = StreamFrame::End(Response::Ok(
                            Reply::Returned(to_wire(&reply)),
                        ));
                        http::write_frame(&mut w, &to_wire(&end))
                            .await
                            .expect("end");
                    } else {
                        assert!(
                            request
                                .sync
                                .iter()
                                .any(|c| c.topic == TOPIC_A
                                    && c.since == Some(100)),
                            "the command carried the live poll cursor"
                        );
                        let delta = SyncReply {
                            events: vec![saved_event(TOPIC_A, 150)],
                            cursors: vec![(TOPIC_A, 150)],
                        };
                        http::write_frame(
                            &mut w,
                            &to_wire(&StreamFrame::Sync(to_wire(&delta))),
                        )
                        .await
                        .expect("sync frame");
                        let end = StreamFrame::End(Response::Ok(Reply::Value(
                            Some(vec![1]),
                        )));
                        http::write_frame(&mut w, &to_wire(&end))
                            .await
                            .expect("end");
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn a_command_piggybacks_the_delta_to_a_live_poll_watch() {
        macro_rules! within {
            ($fut:expr) => {
                wavedb_platform::time::timeout(Duration::from_secs(30), $fut)
                    .await
                    .expect("timed out")
            };
        }

        let addr = piggyback_node();
        let auth = Auth::Anonymous {
            tenant: U48::from(5u32),
        };
        // A long interval so no automatic tick fires — the only delivery is
        // the one piggybacked onto the command below.
        let mode = WatchMode::HttpPoll(Duration::from_secs(1_000));
        let (mut events, _guard) =
            within!(watch(&addr, auth.clone(), mode, TOPIC_A))
                .expect("poll watch registered");

        // Issue an ordinary command; the manager attaches the live cursor and
        // routes the returned delta back to the watch.
        let frame = CommandFrame {
            struct_hash: 0x1234,
            command: Command::Get,
            payload: Vec::new(),
        };
        let mut frames = within!(post(&addr, auth, frame)).expect("command");
        // Drain the command's own frames to the End (the caller's behaviour);
        // the leading Sync frame was already peeled by the manager.
        while let Ok(Some(_)) = frames.next_frame().await {}

        assert_eq!(
            within!(events.next()),
            Some(saved_event(TOPIC_A, 150)),
            "the watch saw the event the command piggybacked — no dedicated poll"
        );
    }
}
