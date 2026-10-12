//! The showcase client — a guided tour of the whole developer surface
//! against a live node, narrated on stdout.
//!
//! ```sh
//! cargo run -p showcase --example client              # the full online tour
//! cargo run -p showcase --example client -- --poll    # watches over HTTP polls
//! cargo run -p showcase --example client -- --offline # after killing the node
//! ```
//!
//! The client opens a **write-through cache** (`Db::open_at`): every
//! acknowledged op and every watch event mirrors into a local engine — the
//! same WaveDB engine, caching WaveDB. Kill the node after a tour and the
//! `--offline` run answers the reads from that cache.

// The typed handle futures hold `&Db` across awaits — non-Send by design.
#![allow(clippy::future_not_send)]

mod report;

use core::time::Duration;

use futures::TryStreamExt as _;
use showcase::{
    CLIENT_REGISTRY, DEMO_SECRET, Task, Workspace, add_project, open_workspace,
    tasks_with_status,
};
use wavedb::prelude::*;
use wavedb::watch::WatchEvent;
use wavedb::{Db, Error};

/// One demo identity: tenant 7, user 7 (B2C: user == tenant).
const TENANT: u32 = 7;

/// A signed access token — demo plumbing: a real app receives its pair
/// from a login `#[server]` fn (see todo-app); the node refuses anonymous
/// struct commands and subscriptions either way.
fn access_token() -> Vec<u8> {
    use wavedb_net::auth::{AccessClaims, TokenPurpose, sign, unix_now};
    sign(
        &DEMO_SECRET,
        &AccessClaims {
            user: U48::from(TENANT),
            tenant: U48::from(TENANT),
            expires_at: unix_now() + 3600,
            purpose: TokenPurpose::Access,
            session: 0x5107,
            nonce: 0,
        },
    )
}

fn task(title: &str, status: &str, points: u64) -> Task {
    Task {
        title: title.into(),
        status: status.into(),
        points,
    }
}

fn main() {
    wavedb_platform::carrier::block_on(async {
        let addr = std::env::var("SHOWCASE_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:4780".into());
        let offline = std::env::args().any(|a| a == "--offline");
        let poll = std::env::args().any(|a| a == "--poll");

        // Db::open_at = connect + the local write-through cache at an explicit
        // directory (Db::open picks the platform default). The cache survives
        // this process — that is what the --offline run reads.
        let cache = std::env::temp_dir().join("wavedb-showcase-cache");
        let mut db = Db::open_at(
            CLIENT_REGISTRY,
            addr.clone(),
            U48::from(TENANT),
            U48::from(TENANT),
            &cache,
        )
        .await
        .expect("open the local cache engine")
        .with_access_token(access_token());
        if poll {
            // Watches ride "anything new?" POST polls instead of a WebSocket —
            // for clients whose path to the node cannot hold one open.
            db = db.watch_via_polling(Duration::from_millis(500));
            println!("watch transport: HTTP polling every 500ms");
        }

        if offline {
            report::offline_tour(&db).await;
        } else {
            online_tour(&db).await;
            println!();
            println!("done. now kill the node and run:");
            println!("  cargo run -p showcase --example client -- --offline");
        }
    });
}

/// The full online pass: bootstrap, collections, indexes, watches,
/// history, cache peek.
async fn online_tour(db: &Db) {
    // ---- bootstrap (server-side: create_pivot has no wire command) ----
    open_workspace(db, "Ada".into())
        .await
        .expect("open workspace");
    let ws = Workspace::get(db).await.expect("get").expect("exists");
    println!("workspace of {:?} is open", ws.owner);

    let project = add_project(db, "launch".into()).await.expect("project");
    println!("project {:?} ready", project.name);

    // Walk the project collection once: the streamed frames carry each
    // record's node-minted `Id` AND `Metadata`, mirrored into the local
    // cache — what lets the --offline pass re-walk it later.
    let projects: Vec<showcase::Project> =
        showcase::Project::collection(ws.projects)
            .all(db)
            .try_collect()
            .await
            .expect("project walk");
    println!(
        "mirrored {} project(s) into the local cache",
        projects.len()
    );

    // ---- live watches (before the writes, so nothing is missed) -------
    // One WebSocket per identity, however many watches (or polls, --poll).
    let mut ws_watch = db
        .watch_unique::<Workspace>()
        .await
        .expect("watch workspace");
    let mut task_watch = db
        .watch_collection::<Task>(project.tasks)
        .await
        .expect("watch tasks");

    // ---- collection ops ----------------------------------------------
    let tasks = Task::collection(project.tasks);
    let wire = tasks
        .insert(db, &task("wire the frames", "todo", 3))
        .await
        .expect("insert");
    let docs = tasks
        .insert(db, &task("write the docs", "todo", 2))
        .await
        .expect("insert");
    let _ship = tasks
        .insert(db, &task("ship it", "doing", 5))
        .await
        .expect("insert");
    println!("inserted 3 tasks (ids are node-minted, stable for life)");

    // An update archives the superseded version at its derived slot and
    // re-keys only the changed indexes; a lost race against a concurrent
    // save is a typed conflict — retry by re-planning, never lost history.
    save_with_retry(db, project.tasks, wire, |mut t: Task| {
        t.status = "done".into();
        t
    })
    .await;
    println!("updated one task to done (conflict-safe save)");

    assert!(tasks.remove(db, docs).await.expect("remove"));
    println!("removed one task (bytes stay; only the walk excludes dead)");

    // ---- filtered reads are #[server] functions ----------------------
    let doing = tasks_with_status(db, "launch".into(), "doing".into())
        .await
        .expect("by status");
    println!(
        "in 'doing' via by_status: {:?}",
        doing.iter().map(|t| t.title.as_str()).collect::<Vec<_>>()
    );

    // ---- the walk (streamed frames, mirrored into the cache) ----------
    let all: Vec<Task> = tasks.all(db).try_collect().await.expect("walk");
    println!("full walk, newest first: {} living tasks", all.len());

    // ---- a Unique upsert (archives the old version) -------------------
    Workspace {
        owner: "Ada Lovelace".into(),
        ..ws
    }
    .save(db)
    .await
    .expect("rename");

    // ---- the watches saw all of it -----------------------------------
    drain_watches(&mut task_watch, &mut ws_watch).await;
    drop((ws_watch, task_watch));

    report::print_history(db).await;
    report::cache_peek(db).await;
}

/// Print the watches' view of the tour's writes, draining until the
/// converged state showed. Push (WS) delivers every mutation; a poll tick
/// instead navigates "changed since cursor" (W6), so same-record writes
/// inside one tick coalesce to the live state — either way the watcher
/// converges on: "wire" done, "ship it" present, the docs task removed.
async fn drain_watches(
    task_watch: &mut wavedb::watch::CollectionWatch<Task>,
    ws_watch: &mut wavedb::watch::UniqueWatch<Workspace>,
) {
    println!();
    println!("watch events (typed; poll ticks coalesce same-record writes):");
    // (The #tag is the id's sub-ms mint counter — tells records apart.)
    let (mut wire_done, mut ship_seen, mut docs_gone) = (false, false, false);
    while !(wire_done && ship_seen && docs_gone) {
        match next_event(task_watch.next()).await {
            WatchEvent::Saved(id, t) => {
                let tag = id.key() % 1_000_000;
                println!(
                    "  task saved   #{tag:<4} {:?} [{}]",
                    t.title, t.status
                );
                wire_done |= t.status == "done";
                ship_seen |= t.title == "ship it";
            }
            WatchEvent::Removed(id) => {
                let tag = id.key() % 1_000_000;
                println!("  task removed #{tag:<4}");
                docs_gone = true;
            }
        }
    }
    if let WatchEvent::Saved(_, seen) = next_event(ws_watch.next()).await {
        println!("  workspace saved: owner is now {:?}", seen.owner);
    }
}

/// A conflict-safe save: read, modify, save; a typed conflict means a
/// concurrent save won the race — re-read and re-plan. History is never
/// overwritten either way.
async fn save_with_retry(
    db: &Db,
    pivot: <Task as WaveDbStruct>::PivotId,
    id: Id,
    change: impl Fn(Task) -> Task,
) {
    let tasks = Task::collection(pivot);
    for _ in 0..3 {
        let current =
            tasks.get(db, id).await.expect("read").expect("task exists");
        match tasks.save(db, id, &change(current)).await {
            Ok(()) => return,
            Err(Error::Node(refusal))
                if refusal.kind
                    == wavedb_net::frame::NodeErrorKind::Conflict =>
            {
                println!("  (lost a save race — re-reading and retrying)");
            }
            Err(other) => panic!("save failed: {other}"),
        }
    }
    panic!("still conflicting after 3 attempts");
}

/// Await a watch's `next()` future with a hang guard, so a broken push
/// path fails the demo loudly instead of wedging it. Generic over the
/// future, so the same helper serves both watch types. Boxed, because a
/// watch future is large and the race would otherwise hold a second copy.
async fn next_event<T, E: core::fmt::Debug>(
    next: impl Future<Output = core::result::Result<Option<WatchEvent<T>>, E>>,
) -> WatchEvent<T> {
    wavedb_platform::time::timeout(Duration::from_secs(10), Box::pin(next))
        .await
        .expect("no watch event within 10s")
        .expect("decode")
        .expect("stream open")
}
