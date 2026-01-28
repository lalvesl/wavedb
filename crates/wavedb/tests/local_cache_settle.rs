//! The native cache settles on its own tick (RFC 0072 §11), proved without
//! any node: the engine lives on a carrier of its own, a durable write is
//! durable when it resolves, an ordinary one at the next 200 ms tick, and
//! dropping the `Db` settles and frees the directory.
//!
//! A crash is what the disk holds while the cache runs: its files copied
//! with the store open, then opened as a cache of their own.

#![allow(clippy::future_not_send)]

use std::path::Path;
use std::time::{Duration, Instant};

use schema_smoke::{AboutUser, CLIENT_REGISTRY};
use wavedb::prelude::*;

/// Nothing listens here — reserved-for-documentation range, refused fast.
const DEAD_NODE: &str = "127.0.0.1:9";

fn tenant() -> U48 {
    U48::from(9u32)
}

fn ada() -> AboutUser {
    AboutUser {
        name: "Ada".into(),
        city: "London".into(),
    }
}

async fn open(dir: &Path) -> Db {
    Db::open_at(CLIENT_REGISTRY, DEAD_NODE, tenant(), tenant(), dir)
        .await
        .expect("the cache opens without a reachable node")
}

/// Copy the directory tree `from` to `to`.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            copy_dir(&path, &to.join(entry.file_name()));
        } else {
            std::fs::copy(&path, to.join(entry.file_name())).unwrap();
        }
    }
}

/// What a crash now would leave of the cache at `dir`, opened: the record
/// it holds, if any.
async fn after_a_crash(dir: &Path, n: u32) -> Option<AboutUser> {
    let crashed = dir.with_extension(format!("crash{n}"));
    copy_dir(dir, &crashed);
    let db = open(&crashed).await;
    let local = db.local().expect("a cache");
    local.get_unique::<AboutUser>().await.expect("local read")
}

#[test]
fn a_durable_local_write_survives_a_crash_at_once() {
    wavedb_platform::carrier::block_on(async {
        let root = tempfile::tempdir().expect("root");
        let dir = root.path().join("cache");
        let db = open(&dir).await;
        let local = db.local().expect("a cache");
        local
            .save_unique_durable(&ada())
            .await
            .expect("durable save");
        assert_eq!(after_a_crash(&dir, 0).await, Some(ada()));
    });
}

/// An ordinary write is durable at the next tick, without anyone asking —
/// polled, since the tick runs on the cache's own thread.
#[test]
fn an_ordinary_local_write_settles_on_the_tick() {
    wavedb_platform::carrier::block_on(async {
        let root = tempfile::tempdir().expect("root");
        let dir = root.path().join("cache");
        let db = open(&dir).await;
        let local = db.local().expect("a cache");
        local.save_unique(&ada()).await.expect("save");
        let start = Instant::now();
        let mut n = 0;
        loop {
            if after_a_crash(&dir, n).await == Some(ada()) {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "no tick settled the write"
            );
            n += 1;
            wavedb_platform::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            local.get_unique::<AboutUser>().await.expect("read"),
            Some(ada()),
            "the cache still answers from its record cache"
        );
    });
}

/// Dropping the last handle settles what the cache holds and closes it
/// before the drop returns, so the directory reopens at once.
#[test]
fn dropping_the_db_settles_and_frees_the_directory() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("cache dir");
        let db = open(dir.path()).await;
        let local = db.local().expect("a cache");
        local.save_unique(&ada()).await.expect("save");
        drop(db);
        let db = open(dir.path()).await;
        let local = db.local().expect("a cache");
        assert_eq!(
            local.get_unique::<AboutUser>().await.expect("read"),
            Some(ada())
        );
    });
}

/// A durable save cannot be queued offline — the queue lives in memory and
/// could not keep the promise — so it surfaces the transport fault.
#[test]
fn a_durable_save_offline_refuses_instead_of_queueing() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("cache dir");
        let db = open(dir.path()).await;
        let refused = ada().save_durable(&db).await;
        assert!(
            matches!(refused, Err(wavedb::Error::Transport(_))),
            "a durable save offline refuses: {refused:?}"
        );
        assert_eq!(db.offline_pending(), 0, "nothing was queued");
        assert_eq!(
            db.local()
                .expect("a cache")
                .get_unique::<AboutUser>()
                .await
                .expect("read"),
            None,
            "nothing was mirrored"
        );
    });
}
