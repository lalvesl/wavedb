//! A durable write on a node does not wait for the maintenance tick.
//!
//! Its job is parked until its batch is durable, and the batch used to open
//! only on the next 200 ms upkeep tick — BG idling through the whole period
//! unless something woke it. So every durable write paid up to a tick on top
//! of its `fsync`. Now the worker wakes BG on every job, and BG opens a batch
//! for a durable write the moment it is wanted.
//!
//! Thirty durable inserts one after another. Waiting for the tick, every one
//! of them waits for the next — issued just after the last one's batch
//! ended, most of a period. Opening their batch at once, each is one `fsync`
//! of a small window. So the check is on the **fastest** of them, against
//! half a period: a total over a fixed bound measured the disk's `fsync`
//! under whatever else the machine ran, and failed under load.

#![allow(clippy::future_not_send)]

use std::time::{Duration, Instant};

use schema_smoke::{Note, REGISTRY};
use wavedb_core::{RouteHandle, U48};
use wavedb_quick_node::Maintenance;
use wavedb_quick_node::shard::{Router, UPKEEP_PERIOD};
use wavedb_storage::{StorageRegistry, StoreOptions, open_shards};

#[test]
fn a_durable_write_opens_its_batch_without_waiting_for_the_tick() {
    wavedb_platform::carrier::block_on(async {
        let dir = tempfile::tempdir().expect("tempdir");
        let stores = open_shards(
            dir.path(),
            &REGISTRY.storage_entries(),
            StoreOptions {
                storage_shards: 1,
                ..StoreOptions::default()
            },
            4,
        )
        .expect("open");
        let (router, closing) =
            Router::start(REGISTRY, stores, [4; 32], 2, Maintenance::default())
                .expect("start");
        let db = RouteHandle::new(&router, U48::from(5u32));
        let pivot = Note::create_pivot_durable(&db).await.expect("pivot");
        let col = Note::collection(pivot);

        let mut fastest = Duration::MAX;
        for n in 0..30u32 {
            let note = Note {
                body: format!("durable {n}"),
                pinned: false,
            };
            let started = Instant::now();
            col.insert_durable(&db, &note).await.expect("insert");
            fastest = fastest.min(started.elapsed());
        }
        assert!(
            fastest < UPKEEP_PERIOD / 2,
            "the fastest of 30 durable inserts took {fastest:?}: every one \
             waited for the tick"
        );

        drop(router);
        for worker in closing {
            worker.await.expect("reported").expect("closed");
        }
    });
}
