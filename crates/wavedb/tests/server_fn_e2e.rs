//! M4 `#[server]` end-to-end: a function declared once, its body running on
//! the node against the local store, called from the client over the wire.
//!
//! `set_city` / `get_city` take + return `WaveWire` values; their bodies use
//! the **unified generated spelling** (`Profile::get(db)` / `me.save(db)`) —
//! the same call sites a client or an engine test writes.
//!
//! The visit log proves where a body runs (RFC 0067 step 2): on a node with
//! four shards, a body coordinates at the edge and each operation hops to its
//! owner's shard and runs under the owner's lock — so concurrent bodies
//! inserting into one collection lose nothing, and a `store`-only type, which
//! the wire cannot name, is reachable from a body.

#![allow(clippy::future_not_send)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use futures::channel::oneshot;
use wavedb::prelude::*;
use wavedb_quick_node::{Bound, Server};

// ── schema: compiled into node AND client ─────────────────────────────────

/// Unique profile, storage-only for the wire but registered for the engine.
#[wavedb]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Profile {
    pub city: String,
}

/// Set the tenant's city (upsert), returning the stored value.
#[server]
pub async fn set_city(db: &Db, city: String) -> Result<String> {
    let mut me = Profile::get(db).await?.unwrap_or_default();
    me.city = city;
    me.save(db).await?;
    Ok(me.city)
}

/// Read the tenant's city (empty when unset).
#[server]
pub async fn get_city(db: &Db) -> Result<String> {
    let me = Profile::get(db).await?.unwrap_or_default();
    Ok(me.city)
}

/// One visit. `store`-only: no wire command can reach it, only node code.
#[wavedb(NonUnique)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Visit {
    pub n: u32,
}

/// Open a new, empty visit log and return its Pivot.
#[server]
pub async fn open_log(db: &Db) -> Result<LocalId> {
    Ok(Visit::create_pivot(db).await?.0)
}

/// Insert `count` visits into `log`, each one its own operation.
#[server]
pub async fn add_visits(db: &Db, log: LocalId, count: u32) -> Result<u32> {
    let col = Visit::collection(VisitPivotId(log));
    for n in 0..count {
        col.insert(db, &Visit { n }).await?;
    }
    Ok(count)
}

/// How many visits `log` lists.
#[server]
pub async fn count_visits(db: &Db, log: LocalId) -> Result<u64> {
    use futures::TryStreamExt as _;
    let all: Vec<Visit> = Visit::collection(VisitPivotId(log))
        .all(db)
        .try_collect()
        .await?;
    Ok(all.len() as u64)
}

// Profile is listed (so its storage slot registers); the functions ride the
// same list as `fn` entries.
wavedb::expose_server! {
    Profile,
    store Visit,
    fn set_city,
    fn get_city,
    fn open_log,
    fn add_visits,
    fn count_visits,
}
wavedb::expose_client! {
    fn set_city,
    fn get_city,
    fn open_log,
    fn add_visits,
    fn count_visits,
}

const TENANT: u32 = 5;
const SECRET: [u8; 32] = [3; 32];

/// M8: non-public functions refuse the anonymous tier — the test signs an
/// access token against the node's fixed secret.
fn access_token() -> Vec<u8> {
    use wavedb_net::auth::{AccessClaims, TokenPurpose, sign, unix_now};
    sign(
        &SECRET,
        &AccessClaims {
            user: U48::from(TENANT),
            tenant: U48::from(TENANT),
            expires_at: unix_now() + 3600,
            purpose: TokenPurpose::Access,
            session: 0,
            nonce: 0,
        },
    )
}

struct Node {
    addr: SocketAddr,
    stop: oneshot::Sender<()>,
    thread: thread::JoinHandle<()>,
}

impl Node {
    fn shutdown(self) {
        self.stop.send(()).expect("server still listening");
        self.thread.join().expect("server thread panicked");
    }
}

fn start(dir: PathBuf) -> Node {
    let (info_tx, info_rx) = mpsc::channel();
    let (stop, stop_rx) = oneshot::channel::<()>();
    let thread = thread::spawn(move || {
        wavedb_platform::carrier::block_on(async move {
            let bound: Bound<_> = Server::new(REGISTRY)
                .secret(SECRET)
                .threads(4)
                .data_dir(&dir)
                .bind("127.0.0.1:0")
                .await
                .expect("open + bind");
            info_tx
                .send(bound.local_addr().expect("addr"))
                .expect("dropped");
            bound
                .run_with_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await
                .expect("serve");
        });
    });
    let addr = info_rx.recv().expect("server never bound");
    Node { addr, stop, thread }
}

#[test]
fn server_function_runs_on_the_node() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = start(dir.path().to_path_buf());
        let db = Db::connect(
            node.addr.to_string(),
            U48::from(TENANT),
            U48::from(TENANT),
        )
        .await
        .expect("connect")
        .with_access_token(access_token());

        // Empty before any write.
        assert_eq!(get_city(&db).await.expect("get_city"), "");

        // The function body runs node-side, upserts Profile, returns the value.
        let stored = set_city(&db, "Lisbon".into()).await.expect("set_city");
        assert_eq!(stored, "Lisbon");

        // A fresh call sees the persisted state.
        assert_eq!(get_city(&db).await.expect("get_city"), "Lisbon");

        // Eight bodies at once, all inserting into one collection: each insert
        // hops to the log's shard and waits for its owner there, so every one
        // of them lands in the index.
        let log = open_log(&db).await.expect("open_log");
        let calls = (0..8).map(|_| add_visits(&db, log, 5));
        for added in futures::future::join_all(calls).await {
            assert_eq!(added.expect("add_visits"), 5);
        }
        assert_eq!(count_visits(&db, log).await.expect("count"), 40);

        // A second log is another owner, wherever it was placed.
        let other = open_log(&db).await.expect("open_log");
        add_visits(&db, other, 3).await.expect("add_visits");
        assert_eq!(count_visits(&db, other).await.expect("count"), 3);
        assert_eq!(count_visits(&db, log).await.expect("count"), 40);

        node.shutdown();
    });
}
