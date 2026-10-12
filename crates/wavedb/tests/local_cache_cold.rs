//! The cache's honesty rules, provable without any node: against a dead
//! address, a **cold** cache propagates the transport fault (absence is not
//! an answer — never an authoritative-looking `None`), while a **warm** one
//! answers; `db.local()` is the direct typed surface that warms it.
//!
//! One test per file: the native cache is the process-global engine
//! (one open `PageStore` per process).

#![allow(clippy::future_not_send)]

use schema_smoke::{AboutUser, CLIENT_REGISTRY};
use wavedb::prelude::*;

/// Nothing listens here — reserved-for-documentation range, refused fast.
const DEAD_NODE: &str = "127.0.0.1:9";

#[test]
fn cold_cache_propagates_the_fault_and_a_warm_one_answers() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("cache dir");
        let tenant = U48::from(9u32);
        let db =
            Db::open_at(CLIENT_REGISTRY, DEAD_NODE, tenant, tenant, dir.path())
                .await
                .expect("the cache opens without a reachable node");

        // Cold: the read must surface the transport fault, not mint a `None`
        // that looks like "the record does not exist".
        let cold = AboutUser::get(&db).await;
        assert!(
            matches!(cold, Err(wavedb::Error::Transport(_))),
            "a cold cache must not answer: {cold:?}"
        );

        // Warm it through the local typed surface (what mirroring does), then
        // the same read serves the cached value under the same dead node.
        let local = db.local().expect("open_at attached a cache");
        let profile = AboutUser {
            name: "Ada".into(),
            city: "London".into(),
        };
        local.save_unique(&profile).await.expect("local save");
        assert_eq!(
            AboutUser::get(&db).await.expect("warm read"),
            Some(profile)
        );

        // W8: an offline Unique save no longer refuses — it queues for node-first
        // replay and mirrors locally, so it succeeds provisionally (the queue
        // drains when a node returns).
        let queued = AboutUser {
            name: "Grace".into(),
            city: "NYC".into(),
        }
        .save(&db)
        .await;
        assert!(
            queued.is_ok(),
            "an offline Unique save queues instead of refusing: {queued:?}"
        );
        assert_eq!(db.offline_pending(), 1, "the save is queued for replay");
        assert_eq!(
            AboutUser::get(&db)
                .await
                .expect("still warm")
                .map(|p| p.city),
            Some(String::from("NYC")),
            "the queued offline save mirrored into the cache"
        );
    });
}
