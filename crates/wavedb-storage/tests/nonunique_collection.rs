//! End-to-end M2 proof: a `#[wavedb(NonUnique)]` type driven through its
//! **derived collection API** (`create_pivot` / `collection` / insert / all /
//! save / remove) over the durable [`PageStore`], surviving a reopen — records,
//! index nodes, **and the `Pivot` record** all recovered from the journal.
//!
//! The derived methods run over a [`LocalHandle`] — the `Store`-backed
//! `DbHandle` — so this also proves the unified `T::get(&db)` spelling
//! against the real engine.

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use futures::TryStreamExt;
use futures::executor::block_on;
use wavedb_core::{LocalHandle, U48};
use wavedb_macros::wavedb;
use wavedb_storage::PageStore;

#[wavedb(NonUnique)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Todo {
    pub title: String,
    pub completed: bool,
}

/// A secondary-indexed type: `by_tag` rides an extra durable B+tree.
#[wavedb(NonUnique)]
#[wavedb::pivot(tag)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Bookmark {
    pub tag: String,
    pub url: String,
}

const TENANT: u32 = 42;

/// Open the durable store serving the derived `Todo` slots (record + Pivot).
fn open(path: &std::path::Path) -> PageStore {
    PageStore::open(path, &Todo::storage_entries()).unwrap()
}

fn todo(title: &str) -> Todo {
    Todo {
        title: title.into(),
        completed: false,
    }
}

async fn walk(
    db: &LocalHandle<'_, PageStore>,
    col: wavedb_core::CollectionHandle<Todo>,
) -> Vec<Todo> {
    col.all(db).try_collect().await.unwrap()
}

#[test]
fn collection_walk_and_durable_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let tenant = U48::from(TENANT);
    let n = 60usize;

    let pivot = block_on(async {
        let store = open(dir.path());
        let db = LocalHandle::new(&store, tenant);
        let pivot = Todo::create_pivot(&db).await.unwrap();
        let col = Todo::collection(pivot);

        for i in 0..n {
            col.insert(&db, &todo(&format!("task-{i}"))).await.unwrap();
        }

        let walked = walk(&db, col).await;
        assert_eq!(
            walked.iter().map(|t| t.title.as_str()).collect::<Vec<_>>(),
            (0..n)
                .rev()
                .map(|i| format!("task-{i}"))
                .collect::<Vec<_>>(),
            "collection walks the record chain, most recently written first"
        );
        pivot
    });

    // Reopen: the data files are rebuilt from the journal — the same PivotId still
    // reaches the whole collection (roots come from the recovered Pivot record).
    block_on(async {
        let store = open(dir.path());
        let db = LocalHandle::new(&store, tenant);
        let col = Todo::collection(pivot);
        let walked = walk(&db, col).await;
        assert_eq!(walked.len(), n, "lost records across reopen");
        // Newest-first: the last insert leads the walk, the first trails it.
        assert_eq!(*walked.first().unwrap(), todo(&format!("task-{}", n - 1)));
        assert_eq!(*walked.last().unwrap(), todo("task-0"));
    });
}

#[test]
fn save_and_remove_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let tenant = U48::from(TENANT);

    let (pivot, dropped) = block_on(async {
        let store = open(dir.path());
        let db = LocalHandle::new(&store, tenant);
        let pivot = Todo::create_pivot(&db).await.unwrap();
        let col = Todo::collection(pivot);

        let a = col.insert(&db, &todo("keep")).await.unwrap();
        let b = col.insert(&db, &todo("drop")).await.unwrap();

        // Update in place: the Id is the stable identity.
        let mut done = todo("keep");
        done.completed = true;
        col.save(&db, a, &done).await.unwrap();

        assert!(col.remove(&db, b).await.unwrap());
        assert!(!col.remove(&db, b).await.unwrap(), "already dead");
        (pivot, b)
    });

    block_on(async {
        let store = open(dir.path());
        let db = LocalHandle::new(&store, tenant);
        let col = Todo::collection(pivot);

        let walked = walk(&db, col).await;
        assert_eq!(
            walked.iter().map(|t| t.title.as_str()).collect::<Vec<_>>(),
            vec!["keep"],
            "removal must survive reopen"
        );
        assert!(walked[0].completed, "update must survive reopen");

        // The dead record's bytes are still resolvable (history navigable).
        assert_eq!(col.get(&db, dropped).await.unwrap(), Some(todo("drop")));
    });
}

fn mark(tag: &str, url: &str) -> Bookmark {
    Bookmark {
        tag: tag.into(),
        url: url.into(),
    }
}

async fn tagged(
    db: &LocalHandle<'_, PageStore>,
    col: wavedb_core::CollectionHandle<Bookmark>,
    tag: &str,
) -> Vec<String> {
    col.by_tag(db, tag)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|b| b.url)
        .collect()
}

#[test]
fn version_history_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let tenant = U48::from(TENANT);

    let (pivot, id) = block_on(async {
        let store = open(dir.path());
        let db = LocalHandle::new(&store, tenant);
        let pivot = Todo::create_pivot(&db).await.unwrap();
        let col = Todo::collection(pivot);

        let id = col.insert(&db, &todo("v1")).await.unwrap();
        col.save(&db, id, &todo("v2")).await.unwrap();
        col.save(&db, id, &todo("v3")).await.unwrap();
        (pivot, id)
    });

    // The archived versions and their chain links are ordinary journaled
    // writes — a rebuild from the log must reproduce the whole timeline.
    block_on(async {
        let store = open(dir.path());
        let db = LocalHandle::new(&store, tenant);
        let col = Todo::collection(pivot);
        let versions: Vec<(wavedb_core::Metadata, Todo)> =
            col.history(&db, id).try_collect().await.unwrap();
        assert_eq!(
            versions
                .iter()
                .map(|(_, t)| t.title.as_str())
                .collect::<Vec<_>>(),
            vec!["v3", "v2", "v1"],
            "history must survive reopen, newest-first"
        );
        // The live walk still sees exactly one record.
        assert_eq!(walk(&db, col).await.len(), 1);
    });
}

#[test]
fn secondary_index_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let tenant = U48::from(TENANT);

    let pivot = block_on(async {
        let store =
            PageStore::open(dir.path(), &Bookmark::storage_entries()).unwrap();
        let db = LocalHandle::new(&store, tenant);
        let pivot = Bookmark::create_pivot(&db).await.unwrap();
        let col = Bookmark::collection(pivot);

        let a = col.insert(&db, &mark("rust", "a")).await.unwrap();
        col.insert(&db, &mark("db", "b")).await.unwrap();
        let c = col.insert(&db, &mark("rust", "c")).await.unwrap();

        assert_eq!(tagged(&db, col, "rust").await, vec!["a", "c"]);

        // Re-key one record, drop another — both must survive replay.
        col.save(&db, a, &mark("db", "a")).await.unwrap();
        assert!(col.remove(&db, c).await.unwrap());
        pivot
    });

    // Reopen: the secondary tree is rebuilt from the journal like everything
    // else; the same PivotId reaches it through the recovered Pivot record.
    block_on(async {
        let store =
            PageStore::open(dir.path(), &Bookmark::storage_entries()).unwrap();
        let db = LocalHandle::new(&store, tenant);
        let col = Bookmark::collection(pivot);
        assert_eq!(
            tagged(&db, col, "rust").await,
            Vec::<String>::new(),
            "re-key + removal must survive reopen"
        );
        // Equal field values order by the records' CREATED_AT (`a` was
        // minted before `b`; its re-key kept its identity).
        assert_eq!(tagged(&db, col, "db").await, vec!["a", "b"]);
    });
}
