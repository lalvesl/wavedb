//! A durable write on a node does not wait for the maintenance tick.
//!
//! Its job is parked until its batch is durable, and the batch used to open
//! only on the next 200 ms upkeep tick — BG idling through the whole period
//! unless something woke it. So every durable write paid up to a tick on top
//! of its `fsync`. Now the worker wakes BG on every job, and BG opens a batch
//! for a durable write the moment it is wanted.
//!
//! Thirty durable inserts one after another: waiting for the tick, they take
//! about fifteen ticks between them (three seconds); opening their batch at
//! once, about thirty `fsync`s of a small window.

#![allow(clippy::future_not_send)]

use std::time::{Duration, Instant};

use schema_smoke::{Note, REGISTRY};
use wavedb_core::{RouteHandle, U48};
use wavedb_quick_node::Maintenance;
use wavedb_quick_node::shard::Router;
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

        let started = Instant::now();
        for n in 0..30u32 {
            let note = Note {
                body: format!("durable {n}"),
                pinned: false,
            };
            col.insert_durable(&db, &note).await.expect("insert");
        }
        let took = started.elapsed();
        assert!(
            took < Duration::from_millis(1500),
            "30 durable inserts took {took:?}: they waited for the tick"
        );

        drop(router);
        for worker in closing {
            worker.await.expect("reported").expect("closed");
        }
    });
}
