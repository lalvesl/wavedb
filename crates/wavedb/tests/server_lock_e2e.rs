//! RFC 0067 §5 end-to-end: a `#[server]` body holds owners across its
//! awaits, on a node with four workers.
//!
//! What is being proven:
//!
//! 1. **read-modify-write under a lock is one step** — eight bodies bumping
//!    one Unique counter, each a read then a save, all under the counter's
//!    lock: no save loses a race (no `Conflict`), and the count is exact;
//! 2. **two owners locked in opposite orders do not deadlock** — the lock
//!    takes them in one order whatever order the body names them in;
//! 3. **a body cannot reach past what it holds** — a second lock, or an
//!    operation on an owner outside the lock, is refused as `LockOrder`;
//! 4. **except a collection it creates under the lock**, which nobody else
//!    can name yet.

#![allow(clippy::future_not_send)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use futures::channel::oneshot;
use wavedb::prelude::*;
use wavedb_net::frame::NodeErrorKind;
use wavedb_quick_node::{Bound, Server};

// ── schema: compiled into node AND client ─────────────────────────────────

/// A counter, one per tenant.
#[wavedb]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tally {
    pub n: u32,
}

/// One entry of a ledger collection.
#[wavedb(NonUnique)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Entry {
    pub n: u32,
}

/// The tenant's counter as an owner to lock.
fn tally<D: DbHandle>(db: &D) -> Owner {
    Owner::unique::<Tally>(db.tenant())
}

/// Bump the counter `times` times, each a read then a save, under its lock.
#[server]
pub async fn bump(db: &Db, times: u32) -> Result<u32> {
    let held = db.lock(&[tally(db)]).await?;
    let mut last = 0;
    for _ in 0..times {
        let mut t = Tally::get(db)
            .await?
            .map(Tally::try_from)
            .transpose()?
            .unwrap_or_default();
        t.n += 1;
        t.save(db).await?;
        last = t.n;
    }
    drop(held);
    Ok(last)
}

/// Read the counter.
#[server]
pub async fn read_tally(db: &Db) -> Result<u32> {
    Ok(Tally::get(db)
        .await?
        .map(Tally::try_from)
        .transpose()?
        .unwrap_or_default()
        .n)
}

/// Open an empty ledger.
#[server]
pub async fn open_ledger(db: &Db) -> Result<LocalId> {
    Ok(Entry::create_pivot(db).await?.0)
}

/// One entry into each of two ledgers, both held for the whole body — named
/// in whatever order the caller gives.
#[server]
pub async fn post_both(db: &Db, a: LocalId, b: LocalId) -> Result<()> {
    let _held = db
        .lock(&[Owner::Collection(a), Owner::Collection(b)])
        .await?;
    Entry::collection(EntryPivotId(a))
        .insert(db, &Entry { n: 1 })
        .await?;
    Entry::collection(EntryPivotId(b))
        .insert(db, &Entry { n: 1 })
        .await?;
    Ok(())
}

/// How many entries `ledger` lists.
#[server]
pub async fn count_entries(db: &Db, ledger: LocalId) -> Result<u64> {
    let all: Vec<Entry> = Entry::collection(EntryPivotId(ledger))
        .all(db)
        .try_collect()
        .await?;
    Ok(all.len() as u64)
}

/// A second lock while one is held.
#[server]
pub async fn lock_twice(db: &Db) -> Result<()> {
    let _held = db.lock(&[tally(db)]).await?;
    let _again = db.lock(&[tally(db)]).await?;
    Ok(())
}

/// An operation on a ledger the held lock does not cover.
#[server]
pub async fn reach_past(db: &Db, ledger: LocalId) -> Result<()> {
    let _held = db.lock(&[tally(db)]).await?;
    Entry::collection(EntryPivotId(ledger))
        .insert(db, &Entry { n: 1 })
        .await?;
    Ok(())
}

/// Create a ledger under the counter's lock, fill it, and link its size
/// into the counter — the create-then-link shape, as one step.
#[server]
pub async fn create_under_lock(db: &Db) -> Result<u32> {
    let _held = db.lock(&[tally(db)]).await?;
    let ledger = Entry::create_pivot(db).await?;
    let col = Entry::collection(ledger);
    for n in 0..3 {
        col.insert(db, &Entry { n }).await?;
    }
    let all: Vec<Entry> = col.all(db).try_collect().await?;
    let len = u32::try_from(all.len()).unwrap_or(u32::MAX);
    Tally { n: len }.save(db).await?;
    Ok(len)
}

wavedb::expose_server! {
    Tally,
    store Entry,
    fn bump,
    fn read_tally,
    fn open_ledger,
    fn post_both,
    fn count_entries,
    fn lock_twice,
    fn reach_past,
    fn create_under_lock,
}
wavedb::expose_client! {
    fn bump,
    fn read_tally,
    fn open_ledger,
    fn post_both,
    fn count_entries,
    fn lock_twice,
    fn reach_past,
    fn create_under_lock,
}

const TENANT: u32 = 6;
const SECRET: [u8; 32] = [4; 32];

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

fn lock_order(result: Result<impl std::fmt::Debug>) {
    match result {
        Err(Error::Node(e)) if e.kind == NodeErrorKind::LockOrder => {}
        other => panic!("expected a LockOrder refusal, got {other:?}"),
    }
}

#[test]
fn a_server_body_holds_owners_across_its_awaits() {
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

        // ---- 1. read-modify-write under the lock: exact, no conflict ------
        let bodies = (0..8).map(|_| bump(&db, 5));
        for bumped in futures::future::join_all(bodies).await {
            bumped.expect("a locked bump never loses its race");
        }
        assert_eq!(read_tally(&db).await.expect("read"), 40);

        // ---- 2. two owners, opposite orders, no deadlock -------------------
        let a = open_ledger(&db).await.expect("open a");
        let b = open_ledger(&db).await.expect("open b");
        let posts = (0..8).map(|i| {
            let (x, y) = if i % 2 == 0 { (a, b) } else { (b, a) };
            post_both(&db, x, y)
        });
        for posted in futures::future::join_all(posts).await {
            posted.expect("post_both");
        }
        assert_eq!(count_entries(&db, a).await.expect("count a"), 8);
        assert_eq!(count_entries(&db, b).await.expect("count b"), 8);

        // ---- 3. reaching past the lock is refused --------------------------
        lock_order(lock_twice(&db).await);
        lock_order(reach_past(&db, a).await);
        assert_eq!(
            count_entries(&db, a).await.expect("count a"),
            8,
            "the refused insert wrote nothing"
        );

        // ---- 4. a collection created under the lock is reachable -----------
        assert_eq!(create_under_lock(&db).await.expect("create"), 3);
        assert_eq!(read_tally(&db).await.expect("read"), 3);

        // Released every time: the counter is free for a plain body.
        assert_eq!(bump(&db, 1).await.expect("bump"), 4);

        node.shutdown();
    });
}
