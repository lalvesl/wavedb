//! RFC 0064, first slice: shards over one disk actor, end to end.
//!
//! One test function, because the engine claim is process-global — the same
//! constraint `node_http.rs` works under.
//!
//! What is being proven, in order:
//!
//! 1. the index layer runs **unmodified** against `ShardStore` — `Collection`
//!    reaches storage through four `Store` methods and does not know it is now
//!    talking to another thread;
//! 2. the write really reaches the engine rather than sitting in a shard's
//!    cache, observed by a *second* shard whose cache is empty;
//! 3. a shard genuinely runs **on its own thread**, built there because its
//!    state is non-`Send` — only the `DiskHandle` crosses;
//! 4. a low-priority `Maintenance` hint travels the second queue and runs;
//! 5. a wire [`Request`] handed to the [`Router`] is executed by a shard
//!    worker and answered — the ingress path with no engine on the caller's
//!    thread at all.

// Shard futures are non-`Send` by construction; the test runtime is
// current-thread, which is the model this crate already declares.
#![allow(clippy::future_not_send)]

use std::rc::Rc;

use schema_smoke::{AboutUser, Note, REGISTRY};

use wavedb_core::expose::{Command, Reply};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_core::{LocalHandle, LocalId, U48};
use wavedb_net::auth::{AccessClaims, TokenPurpose, sign};
use wavedb_net::frame::{Auth, CommandFrame, Request, Response};
use wavedb_quick_node::shard::{
    Maintenance, Router, ShardStore, Shards, shard_of,
};
use wavedb_storage::{PageStore, StorageRegistry};

const TENANT: u32 = 7;
const SECRET: [u8; 32] = [9; 32];

/// A signed access token: struct commands refuse the anonymous tier, so the
/// routed request has to carry a real identity — which is also what the
/// router reads to pick a shard.
fn auth() -> Auth {
    Auth::Token(sign(
        &SECRET,
        &AccessClaims {
            user: U48::from(TENANT),
            tenant: U48::from(TENANT),
            expires_at: wavedb_net::auth::unix_now() + 3600,
            purpose: TokenPurpose::Access,
            session: 0,
            nonce: 0,
        },
    ))
}

/// One wire request, as the accept loop would decode it off a socket.
fn request(struct_hash: u64, command: Command, payload: Vec<u8>) -> Request {
    Request {
        auth: auth(),
        frame: CommandFrame {
            struct_hash,
            command,
            payload,
        },
        sync: Vec::new(),
    }
}

fn note(body: &str) -> Note {
    Note {
        body: body.into(),
        pinned: false,
    }
}

#[test]
fn shards_serve_collections_over_one_disk_actor() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = PageStore::open(dir.path(), &REGISTRY.storage_entries())
            .expect("open engine");

        // From here the engine is owned by the actor's thread. Nothing in this
        // test can touch it again except by message — which is the point.
        let shards = Shards::start(store, 4).expect("start shards");
        assert_eq!(shards.count(), 4);

        // ---- 1. the index layer, unmodified, over a shard ---------------------
        // A **caching** store, which is what a shard has. `Shards::store` hands
        // out the cacheless one instead, because only a shard may cache: the
        // cache remembers absence, and that is sound only while one holder
        // reaches a record. This test stands in for a shard's thread.
        let first = Rc::new(ShardStore::new(shards.handle()));
        let db = LocalHandle::new(&*first, U48::from(TENANT));

        let pivot = Note::create_pivot(&db).await.expect("create pivot");
        let col = Note::collection(pivot);
        let mut ids = Vec::new();
        for n in 0..25u32 {
            ids.push(
                col.insert(&db, &note(&format!("note {n}")))
                    .await
                    .expect("insert"),
            );
        }

        let got = col.get(&db, ids[7]).await.expect("get").expect("present");
        assert_eq!(got.body, "note 7");
        assert!(
            first.cached_len() > 0,
            "a shard that caches nothing is not a shard"
        );

        // ---- 2. the write reached the engine, not just the cache -------------
        // The cacheless store, which is exactly the right probe: it cannot answer
        // from anything of its own, so whatever it sees came back over the wire
        // from the engine. (It shares a Pivot with `first`, which routing would
        // normally forbid — sound here precisely because it caches nothing and
        // nothing writes through it.)
        let observer: Rc<ShardStore> = shards.store();
        assert_eq!(observer.cached_len(), 0, "a bypass store holds nothing");

        let cold_db = LocalHandle::new(&*observer, U48::from(TENANT));
        let cold = Note::collection(pivot);
        let seen = cold
            .get(&cold_db, ids[7])
            .await
            .expect("cold get")
            .expect("the record must have reached the engine");
        assert_eq!(
            seen.body, "note 7",
            "the first shard's cache was answering for a write that never landed"
        );
        assert_eq!(
            observer.cached_bytes(),
            0,
            "a bypass store must not memoise — a second cache over one record is \
         how a stale `None` outlives another holder's insert"
        );

        // ---- 3. a shard on a thread of its own -------------------------------
        // The factory is `Send` and captures only the handle; `ShardStore` is
        // built on the far side, because it must not be movable between threads.
        let handle = shards.handle();
        let probe = ids[7];
        let (done, wait) = std::sync::mpsc::channel::<Result<String, String>>();
        std::thread::spawn(move || {
            futures::executor::block_on(async move {
                let own = Rc::new(ShardStore::new(handle));
                let db = LocalHandle::new(&*own, U48::from(TENANT));
                let col = Note::collection(pivot);
                let answer = match col.get(&db, probe).await {
                    Ok(Some(v)) => Ok(v.body),
                    Ok(None) => Err("absent on the far thread".into()),
                    Err(e) => Err(format!("{e}")),
                };
                let _ = done.send(answer);
            });
        });

        let from_other_thread = wait
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the shard thread must answer")
            .expect("read on the far thread");
        assert_eq!(from_other_thread, "note 7");

        // ---- routing is a function of the pivot, not of anything ambient -----
        routing_is_a_pure_function_of_the_pivot(&db, pivot).await;

        for n in 0..40u32 {
            col.insert(&db, &note(&format!("more {n}")))
                .await
                .expect("insert");
        }

        // ---- 4. the low-priority class reaches the actor and runs ------------
        // Scope, stated because an earlier version of this claimed more than it
        // proved: this shows a `Maintenance` hint travelling the second queue and
        // being executed, observed through `EngineStats` (which has to be a
        // message — once the actor owns the engine, "is anything unsettled?" is
        // not answerable by looking).
        //
        // It does **not** prove starvation-freedom. Checked by replacing the valve
        // with strict priority: this still passed, because each `get` completes
        // and leaves the actor idle, so the maintenance step never has to win a
        // turn against anything. Real contention here would need many concurrent
        // reads and would be timing-dependent; the deterministic proof lives in
        // `priority::tests::maintenance_is_never_starved_under_constant_read_pressure`,
        // which strict priority fails outright.
        let before = first.engine_stats().await.expect("stats");
        assert!(before.pending, "40 inserts must leave a settle queued");

        shards.hint(Maintenance::Checkpoint);

        let mut settled = false;
        for _ in 0..200 {
            col.get(&db, ids[3]).await.expect("read");
            if !first.engine_stats().await.expect("stats").pending {
                settled = true;
                break;
            }
        }
        assert!(settled, "the maintenance hint never reached the engine");

        // ---- 4b. the *waiting* form of the same work -------------------------
        maintain_waits(&shards, &first, &db, col).await;

        // Concurrent operations on one collection are **not** exercised here, and
        // that is deliberate: `ShardStore` genuinely suspends, so two interleaved
        // collection ops can lose an index update
        // (`wavedb-core/tests/concurrent_node_clobber.rs` states the invariant).
        // What prevents it is `shard::OwnerLocks`, tested at the unit level; a
        // probe here passed one interleaving and proved nothing, so it is gone
        // rather than left looking like a guarantee.

        // ---- 5. the ingress path: a wire request, routed and answered --------
        ingress_routes_to_a_worker(&shards, &observer).await;
    });
}

/// A Pivot's shard is decided by the Pivot and nothing else — no table, no
/// ambient state, no balancer. Two collections of one type are separate owners,
/// which is the premise the whole model rests on.
async fn routing_is_a_pure_function_of_the_pivot(
    db: &LocalHandle<'_, ShardStore>,
    pivot: schema_smoke::NotePivotId,
) {
    assert_eq!(shard_of(pivot.0, 4), shard_of(pivot.0, 4));
    assert!(shard_of(pivot.0, 4) < 4);
    let second = Note::create_pivot(db).await.expect("second pivot");
    assert_ne!(pivot, second);
    let spread: std::collections::HashSet<usize> =
        [pivot, second].iter().map(|p| shard_of(p.0, 4)).collect();
    assert!(!spread.is_empty());
    // A Pivot minted here is a `LocalId`, so routing needs nothing but it.
    let _: LocalId = pivot.0;
}

/// `hint` is droppable and needs a poll loop to observe. `maintain` is the
/// counterpart for a caller who is waiting, and its contract is exactly that no
/// loop is needed: when it returns, the work is done.
///
/// Worth its own assertion because both callers of it depend on the difference
/// — a benchmark measuring a quiesced footprint and an operator compacting a
/// store are each measuring nothing if the settle they asked for was silently
/// dropped. Negative control: make the settle arm a no-op and this fails.
async fn maintain_waits(
    shards: &Shards,
    store: &ShardStore,
    db: &LocalHandle<'_, ShardStore>,
    col: wavedb_core::CollectionHandle<Note>,
) {
    for n in 0..40u32 {
        col.insert(db, &note(&format!("again {n}")))
            .await
            .expect("insert");
    }
    assert!(
        store.engine_stats().await.expect("stats").pending,
        "40 inserts must leave a settle queued"
    );
    shards
        .handle()
        .maintain(Maintenance::Settle)
        .await
        .expect("settle");
    assert!(
        !store.engine_stats().await.expect("stats").pending,
        "maintain must not return before the settle it was asked for is done"
    );
}

/// A wire request handed to the [`Router`], executed by a shard worker, and
/// answered — the whole new chain in one place.
///
/// The router picks the owning shard, the request crosses to that worker's
/// thread, the gates and the engine run *there*, and the answer comes back.
/// Nothing on the calling thread touches storage: a `Router` holds no `Store`
/// at all, which is what the accept loop now looks like.
///
/// `AboutUser` is Unique, so the anchor is `KEY = STRUCT_HASH` under the
/// tenant and the save needs no Pivot: the request is exactly what a POST
/// carries.
async fn ingress_routes_to_a_worker(shards: &Shards, bypass: &ShardStore) {
    wavedb_net::auth::set_node_secret(SECRET);
    let (mutations, _drain) =
        wavedb_quick_node::feed::feed(wavedb_quick_node::feed::MUTATION_DEPTH);
    let router =
        Router::start(REGISTRY, &shards.handle(), &mutations, SECRET, 4)
            .expect("start the shard workers");
    assert_eq!(router.count(), 4);

    let profile = AboutUser {
        name: "ada".into(),
        city: "london".into(),
    };
    let saved = router
        .dispatch(request(
            AboutUser::STRUCT_HASH,
            Command::Save,
            to_wire(&profile),
        ))
        .await;
    assert!(
        matches!(saved.response, Response::Ok(_)),
        "the routed save was refused: {:?}",
        saved.response
    );

    // Read it back the same way. Note what this does *not* show: both
    // requests carry the same key, so both land on one worker — but the read
    // would succeed from any of them, because a miss just asks the disk
    // actor. Same-owner-same-shard is a property of `shard_for`, and it is
    // pinned where it can actually fail, in `router`'s unit tests.
    let read = router
        .dispatch(request(AboutUser::STRUCT_HASH, Command::Get, Vec::new()))
        .await;
    let Response::Ok(Reply::Value(Some(bytes))) = read.response else {
        panic!("expected the saved record back: {:?}", read.response);
    };
    assert_eq!(
        from_wire::<AboutUser>(&bytes).expect("decode"),
        profile,
        "the routed read did not see the routed write"
    );

    // And it really landed in the engine, not just in that worker's cache —
    // read through the cacheless bypass store, which can answer from nothing.
    let direct = AboutUser::get(&LocalHandle::new(bypass, U48::from(TENANT)))
        .await
        .expect("bypass read")
        .expect("the routed save must have reached the engine");
    assert_eq!(direct, profile);
}
