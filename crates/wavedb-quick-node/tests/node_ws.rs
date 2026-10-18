//! M7 (W4) end-to-end: a live node speaks the WebSocket transport. One
//! connection presents its identity once (`Hello`), runs commands as that
//! bound caller, and a *second* connection's declared subscriptions receive
//! the mutations the first makes — exact-topic push, no polling.
//!
//! Drives the node over real RFC 6455 frames via the native
//! `wavedb_platform::ws` client; the envelopes (`ClientMsg`/`ServerMsg`) are
//! hand-encoded here (the typed `Db::watch` sugar is W5). One engine per
//! process ⇒ the node runs on its own thread, the test thread is the client.

// Test helpers hold `&mut Conn` across awaits on the test's one thread.
#![allow(clippy::future_not_send)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use futures::channel::oneshot;
use schema_smoke::{AboutUser, Note, REGISTRY};
use wavedb_core::expose::{Command, Reply};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_core::{Id, LocalId, U48};
use wavedb_net::auth::{AccessClaims, TokenPurpose, sign};
use wavedb_net::frame::{Auth, CommandFrame, Response};
use wavedb_net::ws::{ClientMsg, EventKind, RecordEvent, ServerMsg, Topic};
use wavedb_platform::ws::{Conn, connect};
use wavedb_quick_node::{Bound, Server};

const TENANT: u32 = 8;
const SECRET: [u8; 32] = [8; 32];

/// A signed access token for the test tenant, wrapped for `Hello`.
fn hello() -> ClientMsg {
    ClientMsg::Hello(Auth::Token(sign(
        &SECRET,
        &AccessClaims {
            user: U48::from(TENANT),
            tenant: U48::from(TENANT),
            expires_at: wavedb_net::auth::unix_now() + 3600,
            purpose: TokenPurpose::Access,
            session: 0,
            nonce: 0,
        },
    )))
}

struct Node {
    addr: SocketAddr,
    pivot: LocalId,
    stop: oneshot::Sender<()>,
    thread: thread::JoinHandle<()>,
}

impl Node {
    fn shutdown(self) {
        self.stop.send(()).expect("server still listening");
        self.thread.join().expect("server thread panicked");
    }
}

/// Start a node in `dir`, seed one `Note` collection `Pivot`, return once
/// it is listening.
fn start(dir: PathBuf) -> Node {
    let (info_tx, info_rx) = mpsc::channel();
    let (stop, stop_rx) = oneshot::channel::<()>();
    let thread = thread::spawn(move || {
        wavedb_platform::carrier::block_on(async move {
            let bound: Bound<_> = Server::new(REGISTRY)
                .secret(SECRET)
                .data_dir(&dir)
                .bind("127.0.0.1:0")
                .await
                .expect("open + bind");
            let addr = bound.local_addr().expect("local addr");
            // Seeding goes through the node's route: each operation lands on
            // its owner's data shard, as a served one would.
            let route = bound.route();
            let seed = wavedb_core::RouteHandle::new(&route, U48::from(TENANT));
            let pivot = Note::create_pivot(&seed).await.expect("seed pivot");
            info_tx
                .send((addr, pivot.local_id()))
                .expect("test dropped");
            bound
                .run_with_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await
                .expect("serve");
        });
    });
    let (addr, pivot) = info_rx.recv().expect("server never bound");
    Node {
        addr,
        pivot,
        stop,
        thread,
    }
}

async fn ws_send(conn: &mut Conn, msg: &ClientMsg) {
    conn.send(&to_wire(msg)).await.expect("ws send");
}

async fn ws_recv(conn: &mut Conn) -> ServerMsg {
    let bytes = conn.recv().await.expect("ws recv").expect("not closed");
    from_wire::<ServerMsg>(&bytes).expect("decode server msg")
}

/// Open a connection, present the identity, and consume the `HelloOk`.
async fn open(addr: &str) -> Conn {
    let mut conn = connect(addr).await.expect("ws connect");
    ws_send(&mut conn, &hello()).await;
    assert_eq!(ws_recv(&mut conn).await, ServerMsg::HelloOk);
    conn
}

/// Send one command and drain its `Item`s + final `End` (no events expected
/// on the calling connection).
async fn ws_call(
    conn: &mut Conn,
    frame: CommandFrame,
) -> (Vec<Vec<u8>>, Response) {
    ws_send(conn, &ClientMsg::Call(frame)).await;
    let mut items = Vec::new();
    loop {
        match ws_recv(conn).await {
            ServerMsg::Item(bytes) => items.push(bytes),
            ServerMsg::End(response) => return (items, response),
            other => panic!("unexpected message during a call: {other:?}"),
        }
    }
}

const fn frame(
    struct_hash: u64,
    command: Command,
    payload: Vec<u8>,
) -> CommandFrame {
    CommandFrame {
        struct_hash,
        command,
        payload,
    }
}

/// The next message on `conn`, required to be a subscription event.
async fn expect_event(conn: &mut Conn) -> RecordEvent {
    let ServerMsg::Event(event) = ws_recv(conn).await else {
        panic!("expected a subscription event");
    };
    event
}

#[test]
fn websocket_binds_identity_runs_commands_and_pushes_subscriptions() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = start(dir.path().to_path_buf());
        let addr = node.addr.to_string();

        // Two independent identity-bound connections: `alice` writes, `bob` watches.
        let mut alice = open(&addr).await;
        let mut bob = open(&addr).await;

        // Bob declares interest in one Unique anchor and one collection.
        let anchor = Topic {
            struct_hash: AboutUser::STRUCT_HASH,
            pivot: None,
        };
        let collection = Topic {
            struct_hash: Note::STRUCT_HASH,
            pivot: Some(node.pivot),
        };
        // Each Subscribe is acked once applied (FIFO) — the ack IS the barrier
        // proving both subscriptions are live before alice mutates anything.
        ws_send(&mut bob, &ClientMsg::Subscribe(anchor)).await;
        assert!(
            matches!(ws_recv(&mut bob).await, ServerMsg::TopicOk(t, _) if t == anchor)
        );
        ws_send(&mut bob, &ClientMsg::Subscribe(collection)).await;
        assert!(
            matches!(ws_recv(&mut bob).await, ServerMsg::TopicOk(t, _) if t == collection)
        );

        // Alice saves a Unique record; bob sees it (exact topic, body, anchor id).
        let ada = AboutUser {
            name: "Ada".into(),
            city: "London".into(),
        };
        let save = frame(AboutUser::STRUCT_HASH, Command::Save, to_wire(&ada));
        assert_eq!(
            ws_call(&mut alice, save).await.1,
            Response::Ok(Reply::Done)
        );
        let event = expect_event(&mut bob).await;
        assert_eq!(event.topic, anchor);
        assert_eq!(event.kind, EventKind::Saved);
        assert_eq!(
            event.id,
            Id::new(AboutUser::STRUCT_HASH, U48::from(TENANT), true, 0)
        );
        assert_eq!(from_wire::<AboutUser>(&event.body).unwrap(), ada);

        // Alice inserts into the watched collection; bob sees the minted id.
        let memo = Note {
            body: "hi".into(),
            pinned: false,
        };
        let insert = frame(
            Note::STRUCT_HASH,
            Command::Insert,
            to_wire(&(node.pivot, memo.clone())),
        );
        let Response::Ok(Reply::Inserted(note_id)) =
            ws_call(&mut alice, insert).await.1
        else {
            panic!("insert must mint an id");
        };
        let event = expect_event(&mut bob).await;
        assert_eq!(event.topic, collection);
        assert_eq!(event.kind, EventKind::Saved);
        assert_eq!(event.id, note_id);
        assert_eq!(from_wire::<Note>(&event.body).unwrap(), memo);

        unsubscribe_isolates_topics(&mut alice, &mut bob, &node, anchor, &memo)
            .await;
        node.shutdown();
    });
}

/// After bob unsubscribes from the collection, a collection insert must not
/// reach him — while the still-watched anchor still does. The anchor event
/// arriving *next* is what proves the suppressed insert never came.
async fn unsubscribe_isolates_topics(
    alice: &mut Conn,
    bob: &mut Conn,
    node: &Node,
    anchor: Topic,
    memo: &Note,
) {
    let collection = Topic {
        struct_hash: Note::STRUCT_HASH,
        pivot: Some(node.pivot),
    };
    ws_send(bob, &ClientMsg::Unsubscribe(collection)).await;
    // The ack proves the Unsubscribe landed before the insert below.
    assert!(
        matches!(ws_recv(bob).await, ServerMsg::TopicOk(t, _) if t == collection)
    );

    // A collection insert bob must NOT hear about.
    let insert = frame(
        Note::STRUCT_HASH,
        Command::Insert,
        to_wire(&(node.pivot, memo.clone())),
    );
    ws_call(alice, insert).await;

    // The still-watched anchor: a fresh save must arrive, and it must be the
    // *next* event (proving the unsubscribed insert never came through).
    let relocated = AboutUser {
        name: "Ada".into(),
        city: "Paris".into(),
    };
    let save =
        frame(AboutUser::STRUCT_HASH, Command::Save, to_wire(&relocated));
    ws_call(alice, save).await;
    let event = expect_event(bob).await;
    assert_eq!(event.topic, anchor);
    assert_eq!(from_wire::<AboutUser>(&event.body).unwrap(), relocated);
}
