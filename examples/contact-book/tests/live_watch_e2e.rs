//! The M7 W5 exit (WS half): **client A saves; client B's watcher fires
//! within one round-trip** — typed, in order — and because every event
//! mirrors into B's local cache before it is yielded, B answers warm after
//! the node dies, for a collection B never once read online.
//!
//! Two processes, like `local_cache_e2e`: B's `Db::open` cache owns THIS
//! process's engine slot, so the node runs as a child (this same test
//! binary re-executed; `node_process` is a no-op in a normal run). A is a
//! transport-only `Db::connect` handle in the test process.

#![allow(clippy::future_not_send)]

use std::io::{BufRead as _, Write as _};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use contact_book::{
    CLIENT_REGISTRY, Contact, ContactBook, DEMO_SECRET, REGISTRY, open_book,
};
use wavedb::prelude::*;

const TENANT: u32 = 7;

/// The child role: serve contact-book's registry until killed. Selected by
/// `WAVEDB_NODE_DIR` (the parent sets it); without it this passes vacuously.
#[test]
fn node_process() {
    let Some(dir) = std::env::var_os("WAVEDB_NODE_DIR") else {
        return;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");
    rt.block_on(async move {
        let bound = wavedb_quick_node::Server::new(REGISTRY)
            .secret(DEMO_SECRET)
            .data_dir(&dir)
            .bind("127.0.0.1:0")
            .await
            .expect("open the engine and bind");
        let addr = bound.local_addr().expect("read the bound address");
        println!("LISTENING {addr}");
        std::io::stdout().flush().expect("flush the address line");
        bound.run().await.expect("serve");
    });
}

/// Spawn this test binary as the node (child process), returning it plus
/// the address it bound.
fn spawn_node(dir: &std::path::Path) -> (Child, SocketAddr) {
    let mut child = Command::new(std::env::current_exe().expect("own path"))
        .args(["--exact", "node_process", "--nocapture"])
        .env("WAVEDB_NODE_DIR", dir)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn the node child");
    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(line) = line else { return };
            if let Some(addr) = line.strip_prefix("LISTENING ") {
                let _ = tx.send(addr.to_string());
                return;
            }
        }
    });
    let addr = rx
        .recv_timeout(Duration::from_mins(1))
        .expect("node never printed LISTENING")
        .parse()
        .expect("parse the bound address");
    (child, addr)
}

/// A signed access token for the test tenant — subscriptions (and struct
/// commands) refuse the anonymous tier.
fn access_token() -> Vec<u8> {
    use wavedb_net::auth::{AccessClaims, TokenPurpose, sign, unix_now};
    sign(
        &DEMO_SECRET,
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

fn contact(name: &str, phone: &str, city: &str) -> Contact {
    Contact {
        name: name.into(),
        phone: phone.into(),
        city: city.into(),
    }
}

/// Await a watcher's next event with a hang guard (a broken push path must
/// fail the test, not wedge it).
macro_rules! next_event {
    ($watch:expr) => {
        wavedb_platform::time::timeout(Duration::from_secs(30), $watch.next())
            .await
            .expect("watcher timed out — no event arrived")
            .expect("watch stream fault")
            .expect("node closed the watch connection")
    };
}

#[tokio::test]
async fn watcher_sees_remote_mutations_and_keeps_the_cache_warm() {
    let node_dir = tempfile::tempdir().expect("node dir");
    let cache_dir = tempfile::tempdir().expect("cache dir");
    let (mut node, addr) = spawn_node(node_dir.path());
    let tenant = U48::from(TENANT);

    // B: the watching client, with its local cache. A: a plain transport
    // handle — a different client of the same tenant (another device).
    let b = Db::open_at(
        CLIENT_REGISTRY,
        addr.to_string(),
        tenant,
        tenant,
        cache_dir.path(),
    )
    .await
    .expect("open the watcher client")
    .with_access_token(access_token());
    let a = Db::connect(addr.to_string(), tenant, tenant)
        .await
        .expect("connect the writer client")
        .with_access_token(access_token());

    // A watch refuses a token-less handle immediately (the node would
    // refuse the subscription anyway — this keeps the refusal typed).
    let anon = Db::connect(addr.to_string(), tenant, tenant)
        .await
        .expect("connect anonymous");
    assert!(matches!(
        anon.watch_unique::<ContactBook>().await,
        Err(wavedb::Error::Unauthorized(_))
    ));

    // Bootstrap: A opens the book; B reads it once to learn the pivot.
    open_book(&a, "Ada".into()).await.expect("open_book");
    let book = ContactBook::get(&b).await.expect("get").expect("exists");

    // B's watchers — live from the moment these return (acked subscribe).
    let mut book_watch =
        b.watch_unique::<ContactBook>().await.expect("watch anchor");
    let mut contact_watch = b
        .watch_collection::<Contact>(book.contacts)
        .await
        .expect("watch collection");

    // A mutates: insert, update, insert, remove, and a Unique save.
    let col = Contact::collection(book.contacts);
    let grace_nyc = contact("Grace", "555-0001", "NYC");
    let grace_ldn = contact("Grace", "555-0001", "London");
    let alan = contact("Alan", "555-0002", "London");
    let grace_id = col.insert(&a, &grace_nyc).await.expect("insert grace");
    col.save(&a, grace_id, &grace_ldn)
        .await
        .expect("update grace");
    let alan_id = col.insert(&a, &alan).await.expect("insert alan");
    assert!(col.remove(&a, alan_id).await.expect("remove alan"));
    let renamed = ContactBook {
        owner: "Ada Lovelace".into(),
        contacts: book.contacts,
    };
    renamed.save(&a).await.expect("save the holder");

    // B sees every mutation typed, in order, under the node's own ids.
    assert_eq!(
        next_event!(contact_watch),
        WatchEvent::Saved(grace_id, grace_nyc)
    );
    assert_eq!(
        next_event!(contact_watch),
        WatchEvent::Saved(grace_id, grace_ldn.clone())
    );
    assert_eq!(next_event!(contact_watch), WatchEvent::Saved(alan_id, alan));
    assert_eq!(next_event!(contact_watch), WatchEvent::Removed(alan_id));
    let WatchEvent::Saved(_, seen_book) = next_event!(book_watch) else {
        panic!("the anchor watcher must see the save");
    };
    assert_eq!(seen_book, renamed);

    // ── Node down: the watcher's mirrors answer warm ─────────────────────
    node.kill().expect("kill the node");
    node.wait().expect("reap the node");

    // The Unique holder: the watcher's mirror superseded B's earlier read.
    let warm = ContactBook::get(&b).await.expect("warm unique read");
    assert_eq!(warm, Some(renamed), "the watched save, not the bootstrap");

    // The collection: B NEVER walked it online — everything below was put
    // into the local engine by watch events alone.
    let warm_walk: Vec<Contact> =
        col.all(&b).try_collect().await.expect("warm walk");
    assert_eq!(warm_walk, vec![grace_ldn.clone()], "insert+update−remove");
    let warm_grace = col
        .get(&b, grace_id)
        .await
        .expect("warm by-id read")
        .expect("grace cached");
    assert_eq!(warm_grace, grace_ldn);
    // By-id resolution still answers for the removed record — bytes are
    // never destroyed (remove only moves it to the dead tree, which the
    // walk above excludes); the mirror preserved the node's semantics.
    assert!(
        col.get(&b, alan_id)
            .await
            .expect("warm removed read")
            .is_some(),
        "a removed record keeps resolving by id (history stays navigable)"
    );
}
