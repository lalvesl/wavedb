//! The M5 exit — the typed browser demo against a **live node**: a
//! `#[server]` call, a typed Unique save, and a streamed collection read all
//! travel browser `fetch` → node, and IndexedDB caches reads locally.
//!
//! Needs a node serving the contact-book registry: run it through
//! `scripts/browser_demo.sh`, which starts `cargo run -p contact-book
//! --example node` and passes its address in as `WAVEDB_DEMO_NODE` (a
//! compile-time env — `wasm-pack test` rebuilds when it changes). Without
//! the variable the test passes vacuously, so the plain serverless run
//! (`wasm-pack test --headless --chrome`) stays green.

#![cfg(target_arch = "wasm32")]
// Browser futures hold `JsValue`s — never `Send`; workspace stance.
#![allow(clippy::future_not_send)]

use contact_book::{Contact, ContactBook, DEMO_SECRET, contacts_in, open_book};
use futures::TryStreamExt;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use wavedb::prelude::*;
use wavedb_core::LocalHandle;
use wavedb_wasm::IdbStore;

wasm_bindgen_test_configure!(run_in_browser);

/// The node's `host:port`, baked in by the runner script; `None` = no live
/// node in this run.
const NODE: Option<&str> = option_env!("WAVEDB_DEMO_NODE");

/// A tenant no earlier run has touched, so reruns against a long-lived node
/// never see stale records (same idea as the fresh IndexedDB names).
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // ms clock fits u48
fn fresh_tenant() -> U48 {
    U48::try_from(js_sys::Date::now() as u64 & U48::MASK)
        .unwrap_or_else(|_| unreachable!("masked to 48 bits"))
}

/// Sign an access token against the demo node's fixed secret — the browser
/// side of the shortcut the native e2e takes (a real app calls a login
/// `#[server]` function instead).
fn access_token(tenant: U48) -> Vec<u8> {
    use wavedb_net::auth::{AccessClaims, TokenPurpose, sign, unix_now};
    sign(
        &DEMO_SECRET,
        &AccessClaims {
            user: tenant,
            tenant,
            expires_at: unix_now() + 3600,
            purpose: TokenPurpose::Access,
            session: 0,
            nonce: 0,
        },
    )
}

#[wasm_bindgen_test]
async fn typed_flow_against_a_live_node_with_indexeddb_cache() {
    let Some(addr) = NODE else {
        return; // no node configured — the serverless suite still proves IDB
    };
    let tenant = fresh_tenant();
    let db = Db::connect(addr, tenant, tenant)
        .await
        .expect("connect")
        .with_access_token(access_token(tenant));

    // ── `#[server]` call over fetch: bootstrap the tenant's book ──────────
    open_book(&db, "Ada".into()).await.expect("open_book");
    let book = present(ContactBook::get(&db).await.expect("get"));
    assert_eq!(book.owner, "Ada");

    // ── Typed Unique save over the wire (old version chains node-side) ────
    ContactBook {
        owner: "Ada Lovelace".into(),
        contacts: book.contacts,
    }
    .save(&db)
    .await
    .expect("save book");
    let book = present(ContactBook::get(&db).await.expect("get"));
    assert_eq!(book.owner, "Ada Lovelace");

    // ── Collection over the wire: insert / update / streamed walk ─────────
    let contacts = Contact::collection(book.contacts);
    let grace = contacts
        .insert(
            &db,
            &Contact {
                name: "Grace".into(),
                phone: "555-0001".into(),
                city: "NYC".into(),
            },
        )
        .await
        .expect("insert grace");
    let alan = contacts
        .insert(
            &db,
            &Contact {
                name: "Alan".into(),
                phone: "555-0002".into(),
                city: "London".into(),
            },
        )
        .await
        .expect("insert alan");
    contacts
        .save(
            &db,
            grace,
            &Contact {
                name: "Grace".into(),
                phone: "555-0001".into(),
                city: "London".into(),
            },
        )
        .await
        .expect("update grace");

    // The walk streams item frames through fetch's ReadableStream.
    let all = owned(contacts.all(&db)).await;
    assert_eq!(all.len(), 2);

    // A second `#[server]` call: the filtered read via the `city` secondary.
    let londoners = contacts_in(&db, "London".into()).await.expect("in");
    assert_eq!(londoners.len(), 2, "the update re-keyed grace to London");

    assert!(contacts.remove(&db, alan).await.expect("remove"));

    // ── IndexedDB caching reads: local miss → node fetch → back-fill ──────
    let idb =
        IdbStore::open(&format!("wavedb-demo-cache-{}", js_sys::Date::now()))
            .await
            .expect("open cache");
    let cache = LocalHandle::new(&idb, tenant);

    assert!(
        ContactBook::get(&cache).await.expect("cold").is_none(),
        "cold cache misses"
    );
    let fetched = present(ContactBook::get(&db).await.expect("get"));
    fetched.save(&cache).await.expect("back-fill");
    let warm = present(ContactBook::get(&cache).await.expect("warm"));
    assert_eq!(warm.owner, "Ada Lovelace", "read served from IndexedDB");

    // Cache the collection read too: one network walk, then the same typed
    // engine (chains over IndexedDB) serves it locally.
    let local_pivot = Contact::create_pivot(&cache).await.expect("local pivot");
    let local = Contact::collection(local_pivot);
    let fetched = owned(contacts.all(&db)).await;
    for contact in &fetched {
        local.insert(&cache, contact).await.expect("cache insert");
    }
    let cached = owned(local.all(&cache)).await;
    assert_eq!(cached, fetched, "collection read served from IndexedDB");
}

/// A fetched record that must be there, owned.
fn present<T: WaveWire>(got: Option<Wire<T>>) -> T {
    got.map(|w| w.decode().expect("decode")).expect("present")
}

/// A walk, owned — for the comparisons.
async fn owned<T: WaveWire, E: std::fmt::Debug>(
    walk: impl Stream<Item = core::result::Result<Wire<T>, E>>,
) -> Vec<T> {
    walk.map_ok(|w| w.decode().expect("decode"))
        .try_collect()
        .await
        .expect("walk")
}
