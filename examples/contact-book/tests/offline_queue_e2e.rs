//! W8 exit (slice 1): a `Db::open` client **queues a Unique `save` made while
//! offline and replays it node-first on reconnect** — the write succeeds
//! provisionally (mirrored into the local cache), and when the node returns a
//! drain flushes it FIFO so the node ends up authoritative.
//!
//! Two processes, like `local_cache_e2e`: the engine's per-type state is
//! process-global, so the client cache owns THIS process's engine slot and the
//! node runs as a child (this same binary re-executed with
//! `--exact node_process`). A cache-less `Db::connect` handle reads the node
//! **directly** at the end to prove the replay actually reached it, not just
//! the local mirror.

#![allow(clippy::future_not_send)]

use std::io::Write as _;
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;

use contact_book::{
    CLIENT_REGISTRY, ContactBook, DEMO_SECRET, REGISTRY, open_book,
};
use wavedb::prelude::*;

const TENANT: u32 = 8;

/// The child role: serve contact-book's registry until killed. Selected by
/// `WAVEDB_NODE_DIR`; a normal test run passes vacuously.
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

/// Spawn this test binary as the node child, returning it plus its address.
fn spawn_node(dir: &std::path::Path, bind: &str) -> (Child, SocketAddr) {
    let mut child = Command::new(std::env::current_exe().expect("own path"))
        .args(["--exact", "node_process", "--nocapture"])
        .env("WAVEDB_NODE_DIR", dir)
        .env("WAVEDB_NODE_BIND", bind)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn the node child");
    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufRead::lines(std::io::BufReader::new(stdout)) {
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
/// anonymous tier).
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

#[test]
fn a_unique_save_survives_offline_and_replays_on_reconnect() {
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

        // ── Online: create the book (owner "Ada") ───────────────────────────
        open_book(&db, "Ada".into()).await.expect("open_book");
        let book = ContactBook::get(&db).await.expect("get").expect("exists");
        assert_eq!(book.owner, "Ada");

        // ── Node down: a Unique save queues offline instead of refusing ──────
        node.kill().expect("kill the node");
        node.wait().expect("reap the node");

        let renamed = ContactBook {
            owner: "Ada Lovelace".into(),
            contacts: book.contacts,
        };
        renamed
            .save(&db)
            .await
            .expect("an offline Unique save succeeds provisionally (queued)");
        assert_eq!(db.offline_pending(), 1, "the save is queued, not lost");
        let warm = ContactBook::get(&db)
            .await
            .expect("warm read")
            .expect("exists");
        assert_eq!(
            warm.owner, "Ada Lovelace",
            "the queued save mirrored into the local cache"
        );

        // ── Node back (same data dir + address): the drain replays it ────────
        let (mut node, _) = spawn_node(node_dir.path(), &addr.to_string());
        let flushed = db.drain_offline_queue().await;
        assert_eq!(flushed, 1, "the queued save reached the node");
        assert_eq!(db.offline_pending(), 0, "the queue drained empty");

        // ── Prove the NODE has it: a cache-less handle reads it directly ─────
        let direct =
            Db::connect(addr.to_string(), U48::from(TENANT), U48::from(TENANT))
                .await
                .expect("connect")
                .with_access_token(access_token());
        let on_node = ContactBook::get(&direct)
            .await
            .expect("node read")
            .expect("exists");
        assert_eq!(
            on_node.owner, "Ada Lovelace",
            "the offline save replayed to the node, not just the local mirror"
        );

        node.kill().expect("kill the node");
        node.wait().expect("reap the node");
    });
}
