//! The **showcase** schema — one small project-tracker domain exercising
//! every WaveDB surface in a runnable pair of processes:
//!
//! - a `Unique` holder ([`Workspace`]) owning a collection handle;
//! - a `NonUnique` collection ([`Project`]) whose records **each own
//!   another collection** ([`Task`] — pivots nest freely in records);
//! - secondary indexes (`by_name`, `by_status`) driving `#[server]`
//!   filtered reads (there is no query DSL — a filtered read IS a server
//!   function);
//! - server-side bootstrap (`create_pivot` has no wire command, so
//!   collections are born inside `#[server]` bodies);
//! - the `server-side`/`client-side` features (both on by default here;
//!   a deployed client pulls `client-side` only and the bodies below are
//!   never compiled into it).
//!
//! Run it: `cargo run -p showcase --example node`, then in another
//! terminal `cargo run -p showcase --example client` (see the README for
//! the offline and polling variants).

// The typed handle futures hold `&Db` across awaits — non-Send by design
// (current-thread model), the workspace stance.
#![allow(clippy::future_not_send)]

use wavedb::prelude::*;

// The lists ARE the registry: exactly these functions and these structs'
// generated commands are wire-reachable; anything else — including a type
// that exists in this file but were it unlisted — refuses uniformly as an
// unknown hash.
wavedb::expose_server! {
    fn open_workspace,
    fn add_project,
    fn tasks_with_status,
    Workspace,
    Project,
    Task,
}

wavedb::expose_client! {
    fn open_workspace,
    fn add_project,
    fn tasks_with_status,
    Workspace,
    Project,
    Task,
}

/// The fixed token-signing secret shared by the demo node and client.
///
/// Demo plumbing only — the client signs its own access token against it,
/// the same shortcut the e2e tests take. A real app receives its token
/// pair from a login `#[server]` function (see the todo-app example for
/// the full Argon2-less auth flow with refresh rotation).
pub const DEMO_SECRET: [u8; 32] = *b"wavedb-showcase-demo-secret-32b!";

/// Unique — exactly one live record per tenant, at the `STRUCT_HASH`
/// anchor.
///
/// The workspace holder: names the owner and holds the `PivotId` of the
/// tenant's project collection. Saving it again is the upsert (the
/// superseded version archives; `Workspace::history(&db)` walks the chain
/// newest-first with each version's `Metadata` — who wrote it, when).
#[wavedb]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    pub owner: String,
    pub projects: <Project as WaveDbStruct>::PivotId,
}

/// NonUnique — many per tenant, reached through the pivot the
/// [`Workspace`] holds.
///
/// `by_name` comes from the secondary index; each project owns its own
/// [`Task`] collection — records holding pivots is how trees nest.
#[wavedb(NonUnique)]
#[wavedb::pivot(name)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Project {
    pub name: String,
    pub tasks: <Task as WaveDbStruct>::PivotId,
}

/// NonUnique — the leaf records, indexed by `status` (`by_status`).
///
/// The record's `Id` is minted at insert and never changes; updates
/// archive the superseded version at its derived slot and re-key only
/// the indexes whose fields changed.
#[wavedb(NonUnique)]
#[wavedb::pivot(status)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Task {
    pub title: String,
    pub status: String,
    pub points: u64,
}

/// Ensure this tenant's workspace exists (idempotent): the first call
/// creates the `Project` pivot server-side and saves the `Workspace`
/// holding it. Bootstrap lives here because `create_pivot` is deliberately
/// not wire-reachable — a client can never mint collection roots.
#[server]
pub async fn open_workspace(db: &Db, owner: String) -> Result<()> {
    if Workspace::get(db).await?.is_some() {
        return Ok(());
    }
    let projects = Project::create_pivot(db).await?;
    Workspace { owner, projects }.save(db).await?;
    Ok(())
}

/// Create (or return) the project named `name`, with its own fresh task
/// collection. Idempotent by the `by_name` index — the demo can re-run
/// against a warm node without duplicating projects.
#[server]
pub async fn add_project(db: &Db, name: String) -> Result<Project> {
    use futures::TryStreamExt as _;
    let ws = Workspace::get(db)
        .await?
        .ok_or_else(|| Error::not_found("workspace missing — open it first"))?;
    let ws = ws.view()?;
    let existing: Vec<Wire<Project>> = Project::collection(ws.projects)
        .by_name(db, &name)
        .try_collect()
        .await?;
    if let Some(found) = existing.into_iter().next() {
        return Ok(found.try_into()?);
    }
    let tasks = Task::create_pivot(db).await?;
    let project = Project { name, tasks };
    Project::collection(ws.projects)
        .insert(db, &project)
        .await?;
    Ok(project)
}

/// Every task of `project` currently in `status`, via the `by_status`
/// secondary index — the filtered-read pattern: `search_by` has no wire
/// command, so an index-backed read is always a `#[server]` function.
#[server]
pub async fn tasks_with_status(
    db: &Db,
    project: String,
    status: String,
) -> Result<Vec<Wire<Task>>> {
    use futures::TryStreamExt as _;
    let ws = Workspace::get(db)
        .await?
        .ok_or_else(|| Error::not_found("workspace missing — open it first"))?;
    let ws = ws.view()?;
    let found: Vec<Wire<Project>> = Project::collection(ws.projects)
        .by_name(db, &project)
        .try_collect()
        .await?;
    let project = found
        .into_iter()
        .next()
        .ok_or_else(|| Error::not_found("no such project"))?;
    Task::collection(project.view()?.tasks)
        .by_status(db, &status)
        .try_collect()
        .await
}
