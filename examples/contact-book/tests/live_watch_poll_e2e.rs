//! The HTTP-poll watch end-to-end: a client that cannot hold a WebSocket
//! open ([`Db::watch_via_polling`]) sees remote mutations within one poll
//! tick — typed, through the same watch API — and its cache answers warm
//! after the node dies, exactly like the pushed path (`live_watch_e2e`).
//! Both watches of the identity ride ONE poll loop.
//!
//! Poll semantics are **cursor catch-up** (W6): each tick navigates
//! "changed since" — every changed record arrives once, at its live
//! state. Same-record writes inside one tick coalesce to the newest.
//!
//! Two processes for the same reason as every cache e2e: B's `Db::open`
//! engine owns this process's slots, so the node is a re-executed child.

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

const TENANT: u32 = 9;

/// The child role: serve contact-book's registry until killed. Selected by
/// `WAVEDB_NODE_DIR` (the parent sets it); without it this passes vacuously.
#[test]
fn node_process() {
    let Some(dir) = std::env::var_os("WAVEDB_NODE_DIR") else {
        return;
    };
    wavedb_platform::carrier::block_on(async move {
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

/// A signed access token — the sync route (like every watch) refuses the
/// anonymous tier.
fn access_token() -> Vec<u8> {
    use wavedb_net::auth::{AccessClaims, TokenPurpose, sign, unix_now};
    sign(
        &DEMO_SECRET,
        &AccessClaims {
            user: U48::from(TENANT),
            tenant: U48::from(TENANT),
            expires_at: unix_now() + 3600,
            purpose: TokenPurpose::Access,
            session: 77,
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

/// Await a watcher's next event with a hang guard.
macro_rules! next_event {
    ($watch:expr) => {
        wavedb_platform::time::timeout(Duration::from_secs(30), $watch.next())
            .await
            .expect("watcher timed out — no event arrived")
            .expect("watch stream fault")
            .expect("watch stream ended")
    };
}

#[test]
fn polling_watcher_sees_mutations_and_keeps_the_cache_warm() {
    wavedb_platform::carrier::block_on(async {
        let node_dir = tempfile::tempdir().expect("node dir");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let (mut node, addr) = spawn_node(node_dir.path());
        let tenant = U48::from(TENANT);

        // B: the watching client — cache + POLLING watches (no WebSocket).
        let b = Db::open_at(
            CLIENT_REGISTRY,
            addr.to_string(),
            tenant,
            tenant,
            cache_dir.path(),
        )
        .await
        .expect("open the watcher client")
        .with_access_token(access_token())
        .watch_via_polling(Duration::from_millis(200));
        // A: another device of the same tenant, plain transport.
        let a = Db::connect(addr.to_string(), tenant, tenant)
            .await
            .expect("connect the writer client")
            .with_access_token(access_token());

        // Bootstrap, then watch — both watches share one poll loop.
        open_book(&a, "Ada".into()).await.expect("open_book");
        let book = ContactBook::get(&b)
            .await
            .expect("get")
            .map(|w| w.decode().unwrap())
            .expect("exists");
        let mut book_watch =
            b.watch_unique::<ContactBook>().await.expect("watch anchor");
        let mut contact_watch = b
            .watch_collection::<Contact>(book.contacts)
            .await
            .expect("watch collection");

        // A mutates; B's next tick navigates everything past its cursor.
        let col = Contact::collection(book.contacts);
        let grace_nyc = contact("Grace", "555-0001", "NYC");
        let grace_ldn = contact("Grace", "555-0001", "London");
        let grace_id = col.insert(&a, &grace_nyc).await.expect("insert grace");
        col.save(&a, grace_id, &grace_ldn)
            .await
            .expect("update grace");
        let renamed = ContactBook {
            owner: "Ada Lovelace".into(),
            contacts: book.contacts,
        };
        renamed.save(&a).await.expect("save the holder");

        // Both writes usually land inside one tick window and coalesce to the
        // live state; a tick between them delivers both states. Either way the
        // watcher converges on London — the guaranteed contract.
        let mut event = next_event!(contact_watch);
        if event == WatchEvent::Saved(grace_id, grace_nyc.clone()) {
            event = next_event!(contact_watch);
        }
        assert_eq!(event, WatchEvent::Saved(grace_id, grace_ldn.clone()));
        let WatchEvent::Saved(_, seen) = next_event!(book_watch) else {
            panic!("the anchor watcher must see the save");
        };
        assert_eq!(seen, renamed);

        // ── Node down: the poll goes silent; the mirrors answer warm ─────────
        node.kill().expect("kill the node");
        node.wait().expect("reap the node");

        let warm = ContactBook::get(&b)
            .await
            .expect("warm unique read")
            .map(|w| w.decode().unwrap());
        assert_eq!(warm, Some(renamed), "the watched save, not the bootstrap");
        let warm_walk: Vec<Contact> = col
            .all(&b)
            .map_ok(|w| w.decode().unwrap())
            .try_collect()
            .await
            .expect("warm walk");
        assert_eq!(
            warm_walk,
            vec![grace_ldn],
            "built by poll-watch mirrors alone — B never walked online"
        );
    });
}
