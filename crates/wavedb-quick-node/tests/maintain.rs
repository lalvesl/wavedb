//! Maintenance on demand ([`Router::maintain`]): what a benchmark's phase
//! boundary asks of the workers, proven against a crash.
//!
//! Relaxed writes are acknowledged before they are durable, and the
//! workers' own maintenance settles them on its tick. `Maintain::Settle`
//! must not return until every one of them is durable — so the database's
//! files are copied **while the workers still hold them open**, which is
//! what a crash at that instant leaves on disk, and the copy is opened on
//! its own: every record has to be there.

#![allow(clippy::future_not_send)]

use std::path::Path;

use schema_smoke::{Note, REGISTRY};
use wavedb_core::{LocalHandle, RouteHandle, U48};
use wavedb_quick_node::Maintenance;
use wavedb_quick_node::shard::{Maintain, Owner, Router};
use wavedb_storage::{PageStore, StorageRegistry, StoreOptions, open_shards};

const TENANT: u32 = 11;
const SHARDS: usize = 4;

/// One storage shard under four data shards: the writes go through a
/// storage thread, which takes the barrier — the multi row's shape.
fn open(dir: &Path) -> Vec<PageStore> {
    let options = StoreOptions {
        storage_shards: 1,
        ..StoreOptions::default()
    };
    open_shards(dir, &REGISTRY.storage_entries(), options, SHARDS)
        .expect("open the database")
}

/// Copy `from` into `to`, files and directories, as they are right now.
fn snapshot(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("snapshot dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            snapshot(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy");
        }
    }
}

#[test]
fn a_settle_asked_of_the_workers_returns_once_every_write_is_durable() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("live");
        let (router, closing) = Router::start(
            REGISTRY,
            open(&live),
            [3; 32],
            2,
            Maintenance::default(),
        )
        .expect("start the workers");

        let db = RouteHandle::new(&router, U48::from(TENANT));
        let pivot = Note::create_pivot(&db).await.expect("create pivot");
        let col = Note::collection(pivot);
        let mut ids = Vec::new();
        for n in 0..200u32 {
            let note = Note {
                body: format!("note {n}"),
                pinned: n % 2 == 0,
            };
            ids.push(col.insert(&db, &note).await.expect("insert"));
        }

        router.maintain(Maintain::Settle).await.expect("settle");
        router.maintain(Maintain::Evict(0)).await.expect("evict");
        // The crash image: the files as they are, the workers still up.
        let image = dir.path().join("image");
        snapshot(&live, &image);

        // What the workers serve after an eviction comes from the pages.
        let got = col.get(&db, ids[150]).await.expect("get").expect("there");
        assert_eq!(got.view().expect("view").body, "note 150");

        drop(router);
        for worker in closing {
            worker.await.expect("reported").expect("closed");
        }

        let stores = open(&image);
        let at = Owner::Collection(pivot.local_id()).shard(SHARDS);
        let local = LocalHandle::new(&stores[at], U48::from(TENANT));
        for (n, id) in ids.iter().enumerate() {
            let note = col
                .get(&local, *id)
                .await
                .expect("read the image")
                .unwrap_or_else(|| panic!("note {n} is not in the image"));
            assert_eq!(note.view().expect("view").body, format!("note {n}"));
        }
    });
}
