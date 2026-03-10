//! The tour's reporting half: the version-chain print, the cache peek,
//! and the whole `--offline` pass — split from `main.rs` for the file
//! budget.

use futures::TryStreamExt as _;
use showcase::{Task, Workspace};
use wavedb::Db;
use wavedb::prelude::*;
use wavedb_core::Succession;

/// The version chain is the DB's per-version contract made visible: who
/// wrote each version, when, and its permission — state review, never
/// domain data.
pub async fn print_history(db: &Db) {
    println!();
    println!("workspace history (newest first — who/when per version):");
    let versions: Vec<(Metadata, Wire<Workspace>)> = Workspace::history(db)
        .try_collect()
        .await
        .expect("history over the wire");
    // Chain links are instants: the live version carries its own
    // (`CreatedAt`); an archive carries its SUCCESSOR's (`Next` — that is
    // what its address derivation needs). An archive's own authoring
    // instant is therefore the next-newer version's `previous`.
    let mut inherited = None;
    for (meta, version) in versions.iter().take(5) {
        let authored = match meta.succession {
            Succession::CreatedAt(own) => own,
            Succession::Next(_) => inherited.unwrap_or_default(),
        };
        let live = matches!(meta.succession, Succession::CreatedAt(_));
        // `key_nanos` shape: real milliseconds + a mint counter in the
        // sub-ms digits, so same-millisecond versions still order.
        println!(
            "  owner {:?} — by user {}, at unix-ms {} (+{}){}",
            version.view().expect("view").owner,
            meta.user.get(),
            authored / 1_000_000,
            authored % 1_000_000,
            if live { "  (live)" } else { "" },
        );
        inherited = meta.previous;
    }
}

/// The local cache is a real WaveDB engine — `db.local()` reads it
/// directly, no node involved.
pub async fn cache_peek(db: &Db) {
    let local = db.local().expect("this handle carries a cache");
    let cached = local.get_unique::<Workspace>().await.expect("local read");
    println!();
    println!(
        "local cache holds the workspace of {:?} — the --offline run reads this",
        cached.map(|w| w.view().expect("view").owner.to_owned())
    );
}

/// The offline pass: the node is gone, so every read falls back to the
/// write-through cache (only on a transport fault, and only when the
/// cache can actually answer); writes refuse instead of diverging.
pub async fn offline_tour(db: &Db) {
    println!("offline pass — the node should be down now.");
    // Owned: the offline edit below builds on it.
    let ws: Workspace = Workspace::get(db)
        .await
        .expect("warm cache answers the unique read")
        .expect("run the online tour first")
        .try_into()
        .expect("decode");
    println!("workspace owner from the cache: {:?}", ws.owner);

    let projects: Vec<Wire<showcase::Project>> =
        showcase::Project::collection(ws.projects)
            .all(db)
            .try_collect()
            .await
            .expect("warm cache answers the project walk");
    for project in &projects {
        // Read in place: the name is a `&str` into the walk's frame.
        let project = project.view().expect("view");
        let tasks: Vec<Wire<Task>> = Task::collection(project.tasks)
            .all(db)
            .try_collect()
            .await
            .expect("warm cache answers the task walk");
        println!(
            "  project {:?}: {} living tasks (from the mirror)",
            project.name,
            tasks.len()
        );
    }

    // Write-through means the cache never runs ahead of the node: an
    // offline write refuses (the queue is a later milestone).
    let refused = Workspace {
        owner: "offline edit".into(),
        ..ws
    }
    .save(db)
    .await;
    println!(
        "offline write refused as expected: {}",
        refused.expect_err("writes must not diverge from the node")
    );
}
