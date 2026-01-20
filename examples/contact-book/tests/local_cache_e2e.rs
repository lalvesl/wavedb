//! The M6 exit: a `Db::open` client **survives a node restart with warm
//! local reads** — writes flow through to the node and mirror into the
//! local journal + data files cache; with the node down, everything already
//! seen answers locally (same values, same ids, same order); writes refuse
//! honestly; when the node returns, calls reach it again.
//!
//! Two processes, deliberately: the client cache and the node run as they
//! are deployed — the node as a child. The child is this
//! same test binary re-executed with `--exact node_process` (no
//! cargo-in-cargo); `node_process` is a no-op in a normal test run.

#![allow(clippy::future_not_send)]

use std::io::{BufRead as _, Write as _};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;

use contact_book::{
    CLIENT_REGISTRY, Contact, ContactBook, DEMO_SECRET, REGISTRY, contacts_in,
    open_book,
};
use wavedb::prelude::*;

const TENANT: u32 = 6;

/// The child role: serve contact-book's registry until killed. Selected by
/// `WAVEDB_NODE_DIR` (the parent sets it); without it — a normal test run —
/// this passes vacuously.
#[test]
fn node_process() {
    let Some(dir) = std::env::var_os("WAVEDB_NODE_DIR") else {
        return;
    };
    let bind = std::env::var("WAVEDB_NODE_BIND")
        .unwrap_or_else(|_| String::from("127.0.0.1:0"));
    wavedb_platform::carrier::block_on(async move {
        let bound = wavedb_quick_node::Server::new(REGISTRY)
            .secret(DEMO_SECRET)
            .data_dir(&dir)
            .bind(&bind)
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
fn spawn_node(dir: &std::path::Path, bind: &str) -> (Child, SocketAddr) {
    let mut child = Command::new(std::env::current_exe().expect("own path"))
        .args(["--exact", "node_process", "--nocapture"])
        .env("WAVEDB_NODE_DIR", dir)
        .env("WAVEDB_NODE_BIND", bind)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn the node child");
    // The libtest harness prints its own lines first; scan for ours, off a
    // thread so a wedged child times out instead of hanging the test.
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
        .recv_timeout(std::time::Duration::from_mins(1))
        .expect("node never printed LISTENING")
        .parse()
        .expect("parse the bound address");
    (child, addr)
}

/// A signed access token for the test tenant (struct commands refuse the
/// anonymous tier) — the same direct-signing shortcut the other e2e takes.
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

#[test]
fn client_survives_node_restart_with_warm_local_reads() {
    wavedb_platform::carrier::block_on(async {
        let node_dir = tempfile::tempdir().expect("node dir");
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let (mut node, addr) = spawn_node(node_dir.path(), "127.0.0.1:0");

        let db = Db::open_at(
            CLIENT_REGISTRY,
            addr.to_string(),
            U48::from(TENANT),
            U48::from(TENANT),
            cache_dir.path(),
        )
        .await
        .expect("open the client with its local cache")
        .with_access_token(access_token());

        // ── Online: write through, read through ─────────────────────────────
        open_book(&db, "Ada".into()).await.expect("open_book");
        let book = ContactBook::get(&db).await.expect("get").expect("exists");
        let contacts = Contact::collection(book.contacts);
        let grace = contacts
            .insert(&db, &contact("Grace", "555-0001", "NYC"))
            .await
            .expect("insert grace");
        let alan = contacts
            .insert(&db, &contact("Alan", "555-0002", "London"))
            .await
            .expect("insert alan");
        contacts
            .save(&db, grace, &contact("Grace", "555-0001", "London"))
            .await
            .expect("update grace");
        let walked: Vec<Contact> =
            contacts.all(&db).try_collect().await.expect("walk");
        assert_eq!(walked.len(), 2);
        assert_eq!(
            contacts_in(&db, "London".into())
                .await
                .expect("fn call")
                .len(),
            2
        );

        // ── Node down: everything seen answers warm from the local engine ───
        node.kill().expect("kill the node");
        node.wait().expect("reap the node");

        let warm_book = ContactBook::get(&db).await.expect("warm unique read");
        assert_eq!(warm_book, Some(book), "the holder answers from the cache");
        let warm_grace = contacts
            .get(&db, grace)
            .await
            .expect("warm by-id read")
            .expect("grace cached");
        assert_eq!(warm_grace.city, "London", "the mirrored update, not NYC");
        let warm_walk: Vec<Contact> =
            contacts.all(&db).try_collect().await.expect("warm walk");
        assert_eq!(
            warm_walk
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Grace", "Alan"],
            "the warm walk reproduces the node's own order"
        );

        // Writes refuse honestly (write-through, no offline queue)…
        let refused = contacts
            .insert(&db, &contact("Edsger", "555-0003", "Austin"))
            .await;
        assert!(
            matches!(refused, Err(wavedb::Error::Transport(_))),
            "an offline write must surface the transport fault"
        );
        // …and so do `#[server]` functions (compute runs on the node only).
        assert!(matches!(
            contacts_in(&db, "London".into()).await,
            Err(wavedb::Error::Transport(_))
        ));

        // ── Node back (same data, same address): calls reach it again ───────
        let (mut node, _) = spawn_node(node_dir.path(), &addr.to_string());
        assert_eq!(
            contacts_in(&db, "London".into())
                .await
                .expect("fn call after restart")
                .len(),
            2,
            "the node replayed its journal — the data survived the restart"
        );
        let edsger = contacts
            .insert(&db, &contact("Edsger", "555-0003", "Austin"))
            .await
            .expect("writes flow again");
        assert!(contacts.remove(&db, edsger).await.expect("remove"));
        assert!(contacts.get(&db, alan).await.expect("get").is_some());

        node.kill().expect("kill the node");
        node.wait().expect("reap the node");
    });
}
