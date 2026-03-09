//! Browser-only proof of the IndexedDB backend.
//!
//! Run with a browser runner — `wasm-pack test --headless --chrome -p
//! wavedb-wasm` (or `--firefox`); plain `cargo test` compiles this file
//! empty (native builds of the crate have no content). Each test opens a
//! fresh timestamp-named database so reruns never see stale records.

#![cfg(target_arch = "wasm32")]
// Browser futures hold `JsValue`s — never `Send`; workspace stance.
#![allow(clippy::future_not_send)]

use futures::TryStreamExt;
use schema_smoke::{AboutUser, Note, NoteSecondaries as _};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use wavedb_core::{Id, LocalHandle, Store, U48, Write};
use wavedb_wasm::IdbStore;

wasm_bindgen_test_configure!(run_in_browser);

fn id(key: u64) -> Id {
    Id::new(key, U48::from(9u32), false, 1)
}

/// IndexedDB is one flat `Id → bytes` map, so a write's routing hash is
/// ignored here — any value serves.
const SH: u64 = 0xABCD;

/// A database name no earlier run has touched.
fn fresh(prefix: &str) -> String {
    format!("{prefix}-{}", js_sys::Date::now())
}

#[wasm_bindgen_test]
async fn raw_batches_roundtrip_and_survive_reopen() {
    let name = fresh("wavedb-test-raw");
    let store = IdbStore::open(&name).await.unwrap();
    assert_eq!(store.get(id(1)).await.unwrap(), None);

    store
        .apply(&[Write::Put(id(1), vec![1, 2, 3]), Write::Put(id(2), vec![4])])
        .await
        .unwrap();
    assert_eq!(store.get(id(1)).await.unwrap(), Some(vec![1, 2, 3]));

    store
        .apply(&[Write::Remove(SH, id(1)), Write::Put(id(2), vec![9])])
        .await
        .unwrap();
    assert_eq!(store.get(id(1)).await.unwrap(), None, "removed");
    assert_eq!(store.get(id(2)).await.unwrap(), Some(vec![9]));

    // A second handle over the same database reads the same bytes —
    // durability is IndexedDB's, not a cache's.
    let reopened = IdbStore::open(&name).await.unwrap();
    assert_eq!(reopened.get(id(2)).await.unwrap(), Some(vec![9]));
}

#[wasm_bindgen_test]
async fn typed_surface_runs_serverless_over_indexeddb() {
    let store = IdbStore::open(&fresh("wavedb-test-typed")).await.unwrap();
    let db = LocalHandle::new(&store, U48::from(7u32));

    // Unique anchor: absent, saved, read back.
    assert!(
        AboutUser::get(&db)
            .await
            .unwrap()
            .map(|w| w.decode().unwrap())
            .is_none()
    );
    AboutUser {
        name: "ada".into(),
        city: "porto".into(),
    }
    .save(&db)
    .await
    .unwrap();
    assert_eq!(
        AboutUser::get(&db)
            .await
            .unwrap()
            .map(|w| w.decode().unwrap())
            .unwrap()
            .city,
        "porto"
    );

    // NonUnique collection: the chain segments are ordinary values in the
    // same flat keyspace — the whole engine-above-Store runs unchanged.
    let pivot = Note::create_pivot(&db).await.unwrap();
    let col = Note::collection(pivot);
    let first = col
        .insert(
            &db,
            &Note {
                body: "hello".into(),
                pinned: true,
            },
        )
        .await
        .unwrap();
    col.insert(
        &db,
        &Note {
            body: "later".into(),
            pinned: false,
        },
    )
    .await
    .unwrap();

    assert_eq!(
        col.get(&db, first)
            .await
            .unwrap()
            .map(|w| w.decode().unwrap())
            .unwrap()
            .body,
        "hello"
    );
    let all: Vec<Note> = col.all(&db).try_collect().await.unwrap();
    assert_eq!(all.len(), 2);

    // Secondary index search, then removal.
    let pinned: Vec<Note> =
        col.by_pinned(&db, &true).try_collect().await.unwrap();
    assert_eq!(pinned.len(), 1);
    assert_eq!(pinned[0].body, "hello");

    assert!(col.remove(&db, first).await.unwrap());
    let left: Vec<Note> = col.all(&db).try_collect().await.unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].body, "later");
}
