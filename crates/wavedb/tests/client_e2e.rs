//! M4 end-to-end: the unified typed surface against a live node.
//!
//! The exact spelling the docs promise — `AboutUser::get(&db)` /
//! `value.save(&db)` for Unique, `Note::collection(pivot)` +
//! `col.insert(&db, v)` for NonUnique — driven over HTTP POST into a real
//! `PageStore`, through the same generated methods engine tests run against a
//! `LocalHandle`. The collection `Pivot` is seeded node-side (`create_pivot`
//! is not wire-reachable; apps bootstrap inside `#[server]` bodies).

// The typed client futures hold `&Db` across awaits (non-Send by design on the
// current-thread test runtime).
#![allow(clippy::future_not_send)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use futures::channel::oneshot;
use schema_smoke::{AboutUser, Note, NotePivotId, REGISTRY, Row, RowPivotId};
use wavedb::prelude::*;
use wavedb_quick_node::{Bound, Server};

const TENANT: u32 = 7;
const SECRET: [u8; 32] = [7; 32];

/// A signed access token for the test tenant — struct commands refuse the
/// anonymous tier (M8); the test signs against the node's fixed secret.
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
    pivot: NotePivotId,
    /// A collection of a type declaring a `#[wavedb::list]`.
    rows: RowPivotId,
    stop: oneshot::Sender<()>,
    thread: thread::JoinHandle<()>,
}

impl Node {
    fn shutdown(self) {
        self.stop.send(()).expect("server still listening");
        self.thread.join().expect("server thread panicked");
    }
}

/// Start a node in `dir`, seed a `Note` collection pivot, return once bound.
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
            let rows = Row::create_pivot(&seed).await.expect("seed rows");
            info_tx.send((addr, pivot, rows)).expect("test dropped");
            bound
                .run_with_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await
                .expect("serve");
        });
    });
    let (addr, pivot, rows) = info_rx.recv().expect("server never bound");
    Node {
        addr,
        pivot,
        rows,
        stop,
        thread,
    }
}

#[test]
fn typed_surface_drives_a_live_node() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        let node = start(path);

        let db = Db::connect(
            node.addr.to_string(),
            U48::from(TENANT),
            U48::from(TENANT),
        )
        .await
        .expect("connect")
        .with_access_token(access_token());

        unique_phase(&db).await;
        nonunique_phase(&db, node.pivot).await;
        list_phase(&db, node.rows).await;

        // ── ops without a wire command refuse uniformly ────────────────────────
        let err = Note::create_pivot(&db).await.expect_err("must refuse");
        assert!(
            matches!(
                err,
                Error::Core(wavedb_core::Error::UnknownStructHash(_))
            ),
            "create_pivot is not wire-reachable: {err}"
        );

        node.shutdown();
    });
}

/// Unique: get (empty) → save → upsert → history over the wire.
async fn unique_phase(db: &Db) {
    assert_eq!(AboutUser::get(db).await.expect("get"), None);

    let mut me = AboutUser {
        name: "Ada".into(),
        city: "London".into(),
    };
    me.save(db).await.expect("save");
    assert_eq!(AboutUser::get(db).await.expect("get"), Some(me.clone()));

    // save is an upsert — a second save overwrites the live record.
    me.city = "Paris".into();
    me.save(db).await.expect("resave");
    assert_eq!(
        AboutUser::get(db).await.expect("get").unwrap().city,
        "Paris"
    );

    // History walks the version chain newest-first (pillar 3) — with each
    // version's metadata riding the wire.
    let versions: Vec<(Metadata, AboutUser)> =
        AboutUser::history(db).try_collect().await.expect("history");
    assert_eq!(
        versions
            .iter()
            .map(|(_, u)| u.city.as_str())
            .collect::<Vec<_>>(),
        vec!["Paris", "London"],
        "timeline newest-first"
    );
    assert!(
        matches!(
            versions[0].0.succession,
            wavedb_core::Succession::CreatedAt(_)
        ),
        "the live version carries its authoring instant, not a successor"
    );
}

/// NonUnique: insert → get → save(update) → walk → remove.
async fn nonunique_phase(db: &Db, pivot: NotePivotId) {
    let notes = Note::collection(pivot);

    let id = notes
        .insert(
            db,
            &Note {
                body: "buy milk".into(),
                pinned: false,
            },
        )
        .await
        .expect("insert");
    assert_eq!(
        notes.get(db, id).await.expect("get").unwrap().body,
        "buy milk"
    );

    notes
        .save(
            db,
            id,
            &Note {
                body: "buy milk".into(),
                pinned: true,
            },
        )
        .await
        .expect("update");
    assert!(notes.get(db, id).await.expect("get").unwrap().pinned);

    // A second insert, then walk the whole collection — newest write first.
    notes
        .insert(
            db,
            &Note {
                body: "write docs".into(),
                pinned: false,
            },
        )
        .await
        .expect("insert 2");
    let all: Vec<Note> = notes.all(db).try_collect().await.expect("all");
    assert_eq!(
        all.iter().map(|n| n.body.as_str()).collect::<Vec<_>>(),
        vec!["write docs", "buy milk"],
        "collection walk, most recently written first"
    );

    assert!(notes.remove(db, id).await.expect("remove"));
    // Removing again reports it was no longer in the living set.
    assert!(!notes.remove(db, id).await.expect("remove-again"));
    // The walk now yields only the survivor.
    let all: Vec<Note> =
        notes.all(db).try_collect().await.expect("all after remove");
    assert_eq!(
        all.iter().map(|n| n.body.as_str()).collect::<Vec<_>>(),
        vec!["write docs"]
    );
}

/// Declared lists over the wire: the ordering, the pager's two reads, and the
/// chunk loop the unbounded reader is built from.
///
/// Until `Command::Listed` existed all three refused here — a declared list was
/// reachable only from a `LocalHandle` or a `#[server]` body, which is to say
/// from everywhere except the thing that renders a page.
async fn list_phase(db: &Db, pivot: RowPivotId) {
    // Enough to cross the client's internal chunk (256) so the loop really
    // runs more than once, and by a margin that makes the last page short.
    const N: u64 = 270;

    let rows = Row::collection(pivot);
    // Descending arrival: the list must sort them anyway, which is what
    // separates its order from `all()`'s.
    for n in (0..N).rev() {
        rows.insert(db, &Row { n }).await.expect("insert row");
    }

    assert_eq!(
        rows.list_len(db, 0).await.expect("list_len"),
        N,
        "the pager's `of M` now crosses the wire"
    );

    // The unbounded reader: chunked underneath, whole and ascending on top.
    let all: Vec<u64> = rows
        .listed(db, 0)
        .map_ok(|r| r.n)
        .try_collect()
        .await
        .expect("listed");
    assert_eq!(
        all,
        (0..N).collect::<Vec<_>>(),
        "the whole list, in the declared order, across chunk boundaries"
    );

    // The bounded reader: exactly the window a pager renders, one exchange.
    let page: Vec<u64> = rows
        .listed_page(db, 0, 50, 25)
        .map_ok(|r| r.n)
        .try_collect()
        .await
        .expect("listed_page");
    assert_eq!(page, (50..75).collect::<Vec<_>>(), "rows 50…75 of M");

    // A window running off the end comes back short rather than refusing.
    let tail: Vec<u64> = rows
        .listed_page(db, 0, N - 3, 25)
        .map_ok(|r| r.n)
        .try_collect()
        .await
        .expect("listed_page tail");
    assert_eq!(tail, vec![N - 3, N - 2, N - 1]);

    // An undeclared ordering refuses typed, where an empty answer would be a
    // lie about the collection.
    let err = rows
        .listed(db, 9)
        .map_ok(|r| r.n)
        .try_collect::<Vec<_>>()
        .await
        .expect_err("must refuse");
    assert!(
        matches!(err, Error::Node(ref e) if e.message.contains("out of range")),
        "an undeclared list index must refuse: {err}"
    );
}
