//! The M4 exit proof: the todo-app's whole client flow against a live node —
//! register/login on the system tenant (username registry + `as_tenant`
//! bootstrap), the profile→pivot todo functions on the user tenant, and the
//! data surviving a node restart.
//!
//! One test per process (one node per process: the token secret is
//! process-global); the node runs on its own carrier threads, exactly like
//! production.

#![allow(clippy::future_not_send)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use futures::channel::oneshot;
use todo_app_schema::{
    REGISTRY, add_todo, all_todos, complete_todo, delete_todo, login, refresh,
    register, search_todos,
};
use wavedb::prelude::*;
use wavedb_quick_node::{Bound, Server};

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
                .data_dir(&dir)
                .bind("127.0.0.1:0")
                .await
                .expect("open + bind");
            info_tx
                .send(bound.local_addr().expect("addr"))
                .expect("test dropped");
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

async fn connect(addr: SocketAddr, tenant: U48) -> Db {
    Db::connect(addr.to_string(), tenant, tenant)
        .await
        .expect("connect")
}

/// Collect the streamed walk (each todo arrives as its own frame).
async fn collect_todos(db: &Db) -> Result<Vec<todo_app_schema::Todo>> {
    all_todos(db)
        .map_ok(|w| w.decode().expect("decode"))
        .try_collect()
        .await
}

/// The titles `search_todos` returns for `query`, best first.
async fn found(db: &Db, query: &str) -> Vec<String> {
    search_todos(db, query.into(), 5)
        .await
        .expect("search")
        .into_iter()
        .map(|hit| hit.todo.title)
        .collect()
}

fn titles(todos: &[todo_app_schema::Todo]) -> Vec<(&str, bool)> {
    todos
        .iter()
        .map(|t| (t.title.as_str(), t.completed))
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)]
fn full_client_flow_and_restart() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();

        // ── phase 1: register, login, drive todos ──────────────────────────────
        let node = start(path.clone());
        let sys = connect(node.addr, U48::ZERO).await;

        let tenant_id = register(&sys, "alice".into(), "secret".into())
            .await
            .expect("register");
        // A duplicate username is refused by the secondary-index lookup.
        let dup = register(&sys, "alice".into(), "other".into()).await;
        assert!(dup.is_err(), "duplicate username must refuse");

        let (login_tenant, pair) = login(&sys, "alice".into(), "secret".into())
            .await
            .expect("login");
        assert_eq!(login_tenant, tenant_id);
        assert!(!pair.access.is_empty() && !pair.refresh.is_empty());
        let wrong = login(&sys, "alice".into(), "nope".into()).await;
        assert!(wrong.is_err(), "wrong password must refuse");

        // ── M8 gates ────────────────────────────────────────────────────────────
        // Anonymous (no token) may call the public fns only — a login-required
        // fn refuses even under the right claimed tenant.
        let anon =
            connect(node.addr, U48::try_from(tenant_id).expect("48-bit")).await;
        let denied = add_todo(&anon, "sneak".into()).await;
        assert!(denied.is_err(), "anonymous non-public call must refuse");

        // Refresh rotates: the traded-in token dies, the new pair works.
        let pair2 = refresh(&sys, tenant_id, pair.refresh.clone())
            .await
            .expect("refresh");
        let replayed = refresh(&sys, tenant_id, pair.refresh.clone()).await;
        assert!(replayed.is_err(), "a replayed refresh token must refuse");
        // The replay was a theft signal: the whole session is revoked — even
        // the *current* refresh token is dead now.
        let after_revoke = refresh(&sys, tenant_id, pair2.refresh).await;
        assert!(after_revoke.is_err(), "replay revokes the session");

        // A fresh login opens a fresh session; logout revokes it.
        let (_, pair3) = login(&sys, "alice".into(), "secret".into())
            .await
            .expect("re-login");
        todo_app_schema::logout(&sys, tenant_id, pair3.refresh.clone())
            .await
            .expect("logout");
        let after_logout = refresh(&sys, tenant_id, pair3.refresh).await;
        assert!(after_logout.is_err(), "a revoked session must not refresh");

        // Reconnect as the assigned tenant with a live access token (the
        // client flow). The token's claims bind user + tenant — a claimed
        // tenant cannot override them.
        let (_, pair) = login(&sys, "alice".into(), "secret".into())
            .await
            .expect("login for work");
        let db = connect(node.addr, U48::try_from(tenant_id).expect("48-bit"))
            .await
            .with_access_token(pair.access.clone());

        let id_milk = add_todo(&db, "Buy milk".into()).await.expect("add 1");
        let id_docs = add_todo(&db, "Write docs".into()).await.expect("add 2");
        add_todo(&db, "Read the Rust book".into())
            .await
            .expect("add 3");

        let all = collect_todos(&db).await.expect("all");
        assert_eq!(
            titles(&all),
            vec![
                ("Read the Rust book", false),
                ("Write docs", false),
                ("Buy milk", false),
            ],
            "most recently written first"
        );

        // ── fuzzy search over the title (RFC 0056) ────────────────────────────
        //
        // A partial word finds its todo, which is the type-ahead case and the one
        // an exact index cannot answer at all.
        assert_eq!(found(&db, "milk").await, vec!["Buy milk"]);
        assert_eq!(found(&db, "rust").await, vec!["Read the Rust book"]);
        // Case is not a distinction — the fold lowercases before cutting grams.
        assert_eq!(found(&db, "RUST").await, vec!["Read the Rust book"]);
        // …and a dropped letter still lands: `mlk` shares enough of `milk`'s
        // trigrams to clear the threshold.
        assert_eq!(found(&db, "mlk").await, vec!["Buy milk"]);
        // Something genuinely unrelated shares no grams at all, so it comes back
        // empty rather than ranked-but-wrong.
        let observed = found(&db, "quantum chromodynamics").await;
        assert!(observed.is_empty(), "{observed:?}");

        complete_todo(&db, id_milk).await.expect("complete");
        delete_todo(&db, id_docs).await.expect("delete");

        // A save that leaves the title alone writes nothing to the fuzzy index —
        // a posting holds a gram, a length and an anchor, never the record. So
        // completing "Buy milk" leaves it exactly as findable.
        assert_eq!(found(&db, "milk").await, vec!["Buy milk"]);
        // A removal, by contrast, takes every posting with it.
        assert!(
            found(&db, "docs").await.is_empty(),
            "a deleted todo must leave no postings pointing at it"
        );

        let all = collect_todos(&db).await.expect("all after mutate");
        assert_eq!(
            titles(&all),
            vec![("Buy milk", true), ("Read the Rust book", false)],
        );

        // The system tenant sees no todos — tenant spaces never mix.
        let sys_todos = collect_todos(&sys).await;
        assert!(
            sys_todos.is_err(),
            "the system tenant has no profile — the profile→pivot path refuses"
        );

        node.shutdown();

        // ── phase 2: restart — journal replay must reproduce everything ───────
        let node = start(path);
        let sys = connect(node.addr, U48::ZERO).await;

        let (tenant_again, pair) = login(&sys, "alice".into(), "secret".into())
            .await
            .expect("login after restart");
        assert_eq!(tenant_again, tenant_id, "username registry survives");

        let db = connect(node.addr, U48::try_from(tenant_id).expect("48-bit"))
            .await
            .with_access_token(pair.access);
        let all = collect_todos(&db).await.expect("all after restart");
        assert_eq!(
            titles(&all),
            vec![("Buy milk", true), ("Read the Rust book", false)],
            "todos + completion + deletion survive a restart"
        );

        node.shutdown();
    });
}
