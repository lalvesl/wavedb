//! M3 end-to-end: a real client drives a registry-linked node over HTTP POST,
//! through the exposure dispatch into a durable `PageStore`, and the data
//! survives a node restart.
//!
//! The node runs on its own threads (its Embassy carriers + the
//! process-global engine claim); the test thread is the client. `create_pivot`
//! is not wire-reachable in M3 (no `#[server]` yet), so the collection's
//! `Pivot` is seeded node-side before serving — everything else travels as
//! ordinary command frames.

// Test helpers hold `&NetClient` across awaits: their futures are only `Send`
// when the client is, which is irrelevant on the current-thread test runtime.
#![allow(clippy::future_not_send)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use futures::channel::oneshot;
use schema_smoke::{AboutUser, Note, REGISTRY};
use wavedb_core::expose::{Command, Reply};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_core::{LocalId, U48, WaveDbStruct};
use wavedb_net::NetClient;
use wavedb_net::auth::{AccessClaims, TokenPurpose, sign};
use wavedb_net::frame::{Auth, NodeErrorKind};
use wavedb_quick_node::{Bound, Server};

const TENANT: u32 = 7;
const SECRET: [u8; 32] = [7; 32];

/// A signed access token for the test tenant — struct commands refuse the
/// anonymous tier, so every object op authenticates.
fn auth() -> Auth {
    Auth::Token(sign(
        &SECRET,
        &AccessClaims {
            user: U48::from(TENANT),
            tenant: U48::from(TENANT),
            expires_at: wavedb_net::auth::unix_now() + 3600,
            purpose: TokenPurpose::Access,
            session: 0,
            nonce: 0,
        },
    ))
}

/// A running node plus the handles to address it and shut it down.
struct Node {
    addr: SocketAddr,
    pivot: LocalId,
    stop: oneshot::Sender<()>,
    thread: thread::JoinHandle<()>,
}

impl Node {
    /// Shut the node down and wait for its engine to drop (releasing the
    /// process-wide store claim so the next open succeeds).
    fn shutdown(self) {
        self.stop.send(()).expect("server still listening");
        self.thread.join().expect("server thread panicked");
    }
}

/// Start a node in `dir` on an ephemeral port, seed one `Note` collection
/// `Pivot`, and return once it is listening.
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
            // create_pivot is node-side only in M3 (no server fns yet).
            // Node-side seeding now goes through the disk actor like
            // everything else: `store()` hands back a `ShardStore`, not the
            // engine, and it has to outlive the handle borrowing it.
            let engine = bound.store();
            let seed =
                wavedb_core::LocalHandle::new(&*engine, U48::from(TENANT));
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

#[test]
fn node_serves_records_over_http_and_survives_restart() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();

        // ── phase 1: write through the live node ──────────────────────────────
        let node = start(path.clone());
        let client = NetClient::new(node.addr.to_string());

        // Unique: save is an upsert; get returns the live record.
        let ada = AboutUser {
            name: "Ada".into(),
            city: "London".into(),
        };
        assert_eq!(
            save(&client, AboutUser::STRUCT_HASH, &ada).await,
            Reply::Done
        );
        assert_eq!(get_unique::<AboutUser>(&client).await, Some(ada));

        // A second save chains history; get returns the newest version.
        let ada2 = AboutUser {
            name: "Ada".into(),
            city: "Paris".into(),
        };
        save(&client, AboutUser::STRUCT_HASH, &ada2).await;
        assert_eq!(get_unique::<AboutUser>(&client).await, Some(ada2.clone()));

        // NonUnique: insert (mints an Id), get by id, update at that Id — all
        // over the wire, through the pivot seeded node-side.
        let item = Note {
            body: "hi".into(),
            pinned: false,
        };
        let Reply::Inserted(note_id) = client
            .call_ok(
                auth(),
                Note::STRUCT_HASH,
                Command::Insert,
                to_wire(&(node.pivot, item.clone())),
            )
            .await
            .expect("insert")
        else {
            panic!("insert must mint an id");
        };
        assert_eq!(
            get_by_id(&client, Note::STRUCT_HASH, note_id).await,
            Some(item)
        );

        let pinned = Note {
            body: "hi".into(),
            pinned: true,
        };
        client
            .call_ok(
                auth(),
                Note::STRUCT_HASH,
                Command::Update,
                to_wire(&(note_id, pinned.clone())),
            )
            .await
            .expect("update");
        assert_eq!(
            get_by_id(&client, Note::STRUCT_HASH, note_id).await,
            Some(pinned.clone())
        );

        // An unlisted hash is refused as an unknown hash — inside a 200, as a
        // structured NodeError (the transport did its job).
        let refusal = client
            .call(auth(), 0xDEAD_BEEF, Command::Get, Vec::new())
            .await
            .expect("transport ok");
        assert_eq!(
            refusal.expect_err("must refuse").kind,
            NodeErrorKind::UnknownStructHash
        );

        node.shutdown();

        // ── phase 2: reopen the same data dir — the journal replays ───────────
        let node = start(path.clone());
        let client = NetClient::new(node.addr.to_string());

        assert_eq!(
            get_unique::<AboutUser>(&client).await,
            Some(ada2),
            "the newest Unique version survives a restart"
        );
        assert_eq!(
            get_by_id(&client, Note::STRUCT_HASH, note_id).await,
            Some(pinned),
            "the updated NonUnique record survives a restart"
        );

        node.shutdown();
    });
}

/// Save a Unique record and return the node's reply.
async fn save<T: WaveDbStruct + wavedb_core::WaveWire>(
    client: &NetClient,
    struct_hash: u64,
    value: &T,
) -> Reply {
    client
        .call_ok(auth(), struct_hash, Command::Save, to_wire(value))
        .await
        .expect("save")
}

/// Get a Unique record by its anchor (empty payload → the tenant's anchor).
async fn get_unique<T>(client: &NetClient) -> Option<T>
where
    T: WaveDbStruct + wavedb_core::WaveWire,
{
    let reply = client
        .call_ok(auth(), T::STRUCT_HASH, Command::Get, Vec::new())
        .await
        .expect("get");
    decode_value(&reply)
}

/// Get a record by its `Id` (the `Get` payload for the NonUnique shape).
async fn get_by_id<T>(
    client: &NetClient,
    struct_hash: u64,
    id: wavedb_core::Id,
) -> Option<T>
where
    T: wavedb_core::WaveWire,
{
    let reply = client
        .call_ok(auth(), struct_hash, Command::Get, to_wire(&id))
        .await
        .expect("get by id");
    decode_value(&reply)
}

/// Decode a `Reply::Value` body into `T`.
fn decode_value<T: wavedb_core::WaveWire>(reply: &Reply) -> Option<T> {
    match reply {
        Reply::Value(Some(bytes)) => {
            Some(from_wire::<T>(bytes).expect("body decodes"))
        }
        Reply::Value(None) => None,
        other => panic!("expected a value reply, got {other:?}"),
    }
}
