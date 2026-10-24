//! RFC 0067 step 4: workers own their data shards, end to end. One test
//! function: the steps share one database and build on each other.
//!
//! What is being proven, in order:
//!
//! 1. a wire [`Request`] handed to the [`Router`] is executed on the worker
//!    owning its owner's shard and answered — the ingress path, with no
//!    engine on the caller's thread at all;
//! 2. a `#[server]`-style route ([`RouteHandle`] over the router) runs the
//!    index layer unmodified, each operation on its owner's worker;
//! 3. dropping the router stops the workers, and each one closes its stores
//!    cleanly — settled, checkpointed, retired — and drops them before it
//!    reports, so the database reopens at once with nothing to replay;
//! 4. every record is in the shard placement names, and in no other: the
//!    workers wrote into the stores they own.

// Engine futures are non-`Send` by construction; the test runtime is
// current-thread, which is the model this crate already declares.
#![allow(clippy::future_not_send)]

use schema_smoke::{AboutUser, Note, REGISTRY};

use wavedb_core::expose::{Command, Reply};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_core::{LocalHandle, RouteHandle, U48};
use wavedb_net::auth::{AccessClaims, TokenPurpose, sign};
use wavedb_net::frame::{Auth, CommandFrame, Request, Response};
use wavedb_quick_node::Maintenance;
use wavedb_quick_node::feed::{MUTATION_DEPTH, feed};
use wavedb_quick_node::shard::{Owner, Router};
use wavedb_storage::{PageStore, StorageRegistry, StoreOptions, open_shards};

const TENANT: u32 = 7;
const SECRET: [u8; 32] = [9; 32];
const SHARDS: usize = 4;

/// A signed access token: struct commands refuse the anonymous tier, so the
/// routed request has to carry a real identity — which is also what the
/// router reads to place a Unique type.
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

fn open(dir: &std::path::Path) -> Vec<PageStore> {
    open_shards(
        dir,
        &REGISTRY.storage_entries(),
        StoreOptions::default(),
        SHARDS,
    )
    .expect("open the database")
}

#[test]
fn workers_own_their_shards_and_close_them_cleanly() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        wavedb_net::auth::set_node_secret(SECRET);
        let (mutations, _drain) = feed(MUTATION_DEPTH);
        // From here the stores belong to the workers' threads.
        let (router, closing) = Router::start(
            REGISTRY,
            open(dir.path()),
            &mutations,
            SECRET,
            2,
            Maintenance::default(),
        )
        .expect("start the workers");
        assert_eq!(router.count(), 2);

        // ---- 1. ingress: a wire request, executed on its owner's worker ---
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
        let read = router
            .dispatch(request(AboutUser::STRUCT_HASH, Command::Get, Vec::new()))
            .await;
        let Response::Ok(Reply::Value(Some(bytes))) = read.response else {
            panic!("expected the saved record back: {:?}", read.response);
        };
        assert_eq!(from_wire::<AboutUser>(&bytes).expect("decode"), profile);

        // ---- 2. the index layer, unmodified, over the route ----------------
        let db = RouteHandle::new(&router, U48::from(TENANT));
        let pivot = Note::create_pivot(&db).await.expect("create pivot");
        let col = Note::collection(pivot);
        let mut ids = Vec::new();
        for n in 0..25u32 {
            let note = Note {
                body: format!("note {n}"),
                pinned: false,
            };
            ids.push(col.insert(&db, &note).await.expect("insert"));
        }
        let got = col.get(&db, ids[7]).await.expect("get").expect("present");
        assert_eq!(got.body, "note 7");

        // ---- 3. stop: every worker closes its stores, and says so ----------
        drop(router);
        drop(mutations);
        for worker in closing {
            worker
                .await
                .expect("the worker reported")
                .expect("closing its stores");
        }

        // ---- 4. reopen at once: nothing to replay, everything in place -----
        // A store still held by a worker would be `EngineBusy` here.
        let stores = open(dir.path());
        assert!(
            stores.iter().all(|s| !s.has_pending()),
            "a clean close leaves no journal to replay"
        );
        let tenant = U48::from(TENANT);
        let profile_at = Owner::Unique {
            tenant,
            struct_hash: AboutUser::STRUCT_HASH,
        }
        .shard(SHARDS);
        let notes_at = Owner::Collection(pivot.local_id()).shard(SHARDS);
        for (i, store) in stores.iter().enumerate() {
            let local = LocalHandle::new(store, tenant);
            let found = AboutUser::get(&local).await.expect("read profile");
            assert_eq!(
                found.as_ref(),
                (i == profile_at).then_some(&profile),
                "the profile belongs in shard {profile_at} only (shard {i})"
            );
            let note = col.get(&local, ids[7]).await.expect("read note");
            assert_eq!(
                note.is_some(),
                i == notes_at,
                "the notes belong in shard {notes_at} only (shard {i})"
            );
        }
    });
}
