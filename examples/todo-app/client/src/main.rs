//! The todo-app client: the full M4 flow over the wire — register + login on
//! the system tenant, reconnect as the assigned tenant, then drive the todo
//! functions. Every call here is a `#[server]` stub; no struct is
//! wire-addressable.

use todo_app_schema::{
    add_todo, all_todos, complete_todo, delete_todo, login, register,
    search_todos,
};
use wavedb::prelude::*;

const SERVER: &str = "127.0.0.1:7700";
const SYSTEM_TENANT: U48 = U48::ZERO;

fn main() -> anyhow::Result<()> {
    wavedb_platform::carrier::block_on(async {
        // ── Step 1: connect as the system tenant to call the auth functions ───
        let sys = Db::connect(SERVER, SYSTEM_TENANT, SYSTEM_TENANT).await?;

        let tenant_id = register(&sys, "alice".into(), "secret".into()).await?;
        println!("registered  tenant_id={tenant_id}");

        let (tenant_id, pair) =
            login(&sys, "alice".into(), "secret".into()).await?;
        println!("logged in   tenant_id={tenant_id}");

        // ── Step 2: reconnect as the real user tenant ──────────────────────────
        let tenant = U48::try_from(tenant_id)?;
        let db = Db::connect(SERVER, tenant, tenant)
            .await?
            .with_access_token(pair.access.clone());

        // ── Write ──────────────────────────────────────────────────────────────
        let id_milk = add_todo(&db, "Buy milk".into()).await?;
        let id_docs = add_todo(&db, "Write docs".into()).await?;
        let _id_rust = add_todo(&db, "Read the Rust book".into()).await?;
        println!("added 3 todos");

        // ── Read ───────────────────────────────────────────────────────────────
        println!("\n── todos ──");
        print_todos(&db).await?;

        // ── Search ─────────────────────────────────────────────────────────────
        //
        // None of these is a title, and the first three still find one: the index
        // is built over the title's trigrams, so a partial word — or one with a
        // letter dropped — still shares enough of them. The last shares none and
        // comes back empty rather than ranked-but-wrong.
        //
        // `search` is not a DSL. It is one more `#[server]` function, run next to
        // the data, exactly like `all_todos`.
        println!("\n── search ──");
        for query in ["milk", "rust", "mlk", "quantum chromodynamics"] {
            search(&db, query).await?;
        }

        // ── Mutate ─────────────────────────────────────────────────────────────
        complete_todo(&db, id_milk).await?;
        delete_todo(&db, id_docs).await?;

        println!("\n── todos after complete + delete ──");
        print_todos(&db).await?;

        // Completing a todo does not touch the fuzzy index at all — a posting
        // holds a gram, a length and an anchor, never the record, so a save that
        // leaves the title alone has nothing to rewrite. "Buy milk" is still
        // exactly as findable, and "Write docs" is gone with its postings.
        println!("\n── search again, after the mutations ──");
        for query in ["milk", "docs"] {
            search(&db, query).await?;
        }

        Ok(())
    })
}

/// Run one fuzzy search and print what came back, best first.
async fn search(db: &Db, query: &str) -> anyhow::Result<()> {
    // Ranked, so buffered: a best-first order is not known until the last
    // candidate has been scored. Every other read here streams.
    let hits = search_todos(db, query.into(), 5).await?;
    if hits.is_empty() {
        println!("  {query:>14?} → (nothing close enough)");
        return Ok(());
    }
    for hit in hits {
        println!("  {query:>14?} → {:.2}  {}", hit.score, hit.todo.title);
    }
    Ok(())
}

async fn print_todos(db: &Db) -> anyhow::Result<()> {
    // The walk is an async iterator — each todo arrives as its own frame.
    let mut todos = std::pin::pin!(all_todos(db));
    while let Some(todo) = todos.next().await {
        let todo = todo?;
        let mark = if todo.completed { "x" } else { " " };
        println!("  [{mark}] {}", todo.title);
    }
    Ok(())
}
