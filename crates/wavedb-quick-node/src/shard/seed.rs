//! The node's own route, for code that runs outside any request — seeding a
//! database before serving it.
//!
//! Each operation is placed exactly as a served one would be
//! ([`NodeOp::owner`]) and runs on its data shard's store, cacheless, through
//! the registry's `run_node` match. It takes no owner lock, because there is
//! no other operation to exclude: it is for use **before** the node serves,
//! and a worker's cache would not see what it writes after.

use wavedb_core::expose::Exposure;
use wavedb_core::{NodeOp, NodeReply, Route, U48};

use super::ShardStore;

/// A [`Route`] onto the database's shards, for node code outside a request.
pub struct Seeder<E> {
    registry: E,
    store: ShardStore,
    shards: usize,
}

impl<E> Seeder<E> {
    /// Operations through `registry` onto `store`'s shards — `store` must be
    /// cacheless ([`super::Shards::store`]).
    pub(crate) const fn new(
        registry: E,
        store: ShardStore,
        shards: usize,
    ) -> Self {
        Self {
            registry,
            store,
            shards,
        }
    }
}

impl<E: Exposure> Route for Seeder<E> {
    async fn run(
        &self,
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> wavedb_core::Result<NodeReply> {
        let shard = op.owner(tenant, struct_hash).shard(self.shards);
        let view = self.store.at(shard);
        self.registry.run_node(&view, tenant, struct_hash, op).await
    }
}
