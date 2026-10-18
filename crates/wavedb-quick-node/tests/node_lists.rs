//! The declared-list wire commands at the frame level: `Listed` and
//! `ListLen` driven straight over HTTP POST, with no typed client between.
//!
//! `crates/wavedb/tests/client_e2e.rs` covers the same ground through the
//! typed surface; what only this level can show is the exact shape of a
//! refusal — an undeclared ordering and a shape with no lists at all are
//! **different** answers, and one of them is deliberately indistinguishable
//! from a type that never existed.
//!
//! Its own file (and so its own process) because the engine allows one open
//! `PageStore` per process — the same reason `node_http.rs` is a single test.

// Test helpers hold `&NetClient` across awaits: their futures are only `Send`
// when the client is, which is irrelevant on the current-thread test runtime.
#![allow(clippy::future_not_send)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use futures::TryStreamExt;
use futures::channel::oneshot;
use schema_smoke::{AboutUser, REGISTRY, Row};
use wavedb_core::expose::{Command, Reply};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_core::{Id, LocalId, Metadata, U48};
use wavedb_net::NetClient;
use wavedb_net::auth::{AccessClaims, TokenPurpose, sign};
use wavedb_net::frame::{Auth, NodeErrorKind};
use wavedb_quick_node::{Bound, Server};

const TENANT: u32 = 9;
const SECRET: [u8; 32] = [9; 32];

/// A signed access token for the test tenant — struct commands refuse the
/// anonymous tier, so every list read authenticates.
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

/// A running node plus the `Row` collection to address.
struct Node {
    addr: SocketAddr,
    rows: LocalId,
    stop: oneshot::Sender<()>,
    thread: thread::JoinHandle<()>,
}

/// Start a node in `dir`, seed a `Row` collection, return once it is
/// listening. `create_pivot` is not wire-reachable, so the `Pivot` is seeded
/// node-side; everything else travels as ordinary command frames.
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
            let rows = Row::create_pivot(&seed).await.expect("seed rows");
            info_tx.send((addr, rows.local_id())).expect("test dropped");
            bound
                .run_with_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await
                .expect("serve");
        });
    });
    let (addr, rows) = info_rx.recv().expect("server never bound");
    Node {
        addr,
        rows,
        stop,
        thread,
    }
}

#[test]
fn node_serves_declared_list_pages_over_http() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = start(dir.path().to_path_buf());
        let client = NetClient::new(node.addr.to_string());

        // `Row` declares `#[wavedb::list]` on `n` at `page = 4`, so ten records
        // span three segments and a page read really does cross them. The values
        // go in scrambled: a list is sorted at write time, so what comes back
        // ascending proves the order is the list's and not the arrival order's
        // (which is what `All` would answer with).
        for n in [7u64, 2, 9, 0, 5, 3, 8, 1, 6, 4] {
            client
                .call_ok(
                    auth(),
                    Row::STRUCT_HASH,
                    Command::Insert,
                    to_wire(&(node.rows, Row { n })),
                )
                .await
                .expect("insert row");
        }

        assert_eq!(
            list_len(&client, node.rows, 0).await,
            10,
            "the pager's `of M` comes off the sparse index's root sum"
        );

        // The pages are the caller's: `limit` is what it renders.
        assert_eq!(listed(&client, node.rows, 0, 0, 4).await, vec![0, 1, 2, 3]);
        assert_eq!(listed(&client, node.rows, 4, 0, 4).await, vec![4, 5, 6, 7]);
        // A short page is the end of the list — no truncation flag needed, since
        // the caller chose the limit.
        assert_eq!(listed(&client, node.rows, 8, 0, 4).await, vec![8, 9]);
        // Past the end yields nothing rather than refusing.
        assert!(listed(&client, node.rows, 99, 0, 4).await.is_empty());
        // A zero limit is a legitimate empty page.
        assert!(listed(&client, node.rows, 0, 0, 0).await.is_empty());

        // An undeclared ordering on a type that *has* lists is a typed refusal —
        // the caller asked for something specific and wrong, and an empty page
        // would be a lie about the collection. It arrives as the stream's final
        // word, which is where a walk's failures live.
        let refusal = client
            .call_stream(
                auth(),
                Row::STRUCT_HASH,
                Command::Listed,
                to_wire(&(node.rows, 1u32, 0u64, 4u32)),
            )
            .await
            .expect("transport ok")
            .try_collect::<Vec<_>>()
            .await
            .expect_err("must refuse");
        assert!(
            matches!(
                refusal,
                wavedb_net::Error::Node(ref e)
                    if e.kind == NodeErrorKind::ListOutOfRange
            ),
            "an undeclared list index must refuse typed, got {refusal:?}"
        );

        // A Unique shape has no lists at all, and that is not a detail the wire
        // admits: the command refuses like a hash that never existed, the uniform
        // answer every unsupported op gives. The contrast with the arm above is
        // the point — "wrong index" is a caller error, "wrong shape" leaks nothing.
        let refusal = client
            .call(
                auth(),
                AboutUser::STRUCT_HASH,
                Command::ListLen,
                to_wire(&(node.rows, 0u32)),
            )
            .await
            .expect("transport ok");
        assert_eq!(
            refusal.expect_err("must refuse").kind,
            NodeErrorKind::UnknownStructHash
        );

        node.stop.send(()).expect("server still listening");
        node.thread.join().expect("server thread panicked");
    });
}

/// One page of a declared list, as the `n` values it served.
async fn listed(
    client: &NetClient,
    pivot: LocalId,
    offset: u64,
    index: u32,
    limit: u32,
) -> Vec<u64> {
    // A `Values` reply rides as item frames, so a page reads off the
    // streaming path — bounded, but still frames.
    let items: Vec<Vec<u8>> = client
        .call_stream(
            auth(),
            Row::STRUCT_HASH,
            Command::Listed,
            to_wire(&(pivot, index, offset, limit)),
        )
        .await
        .expect("listed")
        .try_collect()
        .await
        .expect("list frames");
    items
        .iter()
        .map(|bytes| {
            // Every frame is `(Id, Metadata, T)` — the same triple `All`
            // ships, so a client mirrors the page under the node's identity
            // and the node's chain data.
            from_wire::<(Id, Metadata, Row)>(bytes)
                .expect("list frame")
                .2
                .n
        })
        .collect()
}

/// A declared list's living count.
async fn list_len(client: &NetClient, pivot: LocalId, index: u32) -> u64 {
    let reply = client
        .call_ok(
            auth(),
            Row::STRUCT_HASH,
            Command::ListLen,
            to_wire(&(pivot, index)),
        )
        .await
        .expect("list_len");
    let Reply::Count(total) = reply else {
        panic!("a list length answers as Count");
    };
    total
}
