//! The node's own route, for code that runs outside any request — seeding a
//! database before serving it.
//!
//! Each operation is placed exactly as a served one would be
//! ([`NodeOp::owner`]) and runs on its data shard's store through the
//! registry's `run_node` match. It takes no owner lock, because there is no
//! other operation to exclude: it borrows the stores the node has opened and
//! not yet handed to its workers, so it can only exist **before** serving.
//! For the same reason a lease is nothing here.

use wavedb_core::expose::Exposure;
use wavedb_core::{NodeOp, NodeReply, Route, U48};
use wavedb_storage::PageStore;

use super::Owner;

/// A [`Route`] onto the database's shards, for node code outside a request.
pub struct Seeder<'a, E> {
    registry: E,
    stores: &'a [PageStore],
}

impl<'a, E> Seeder<'a, E> {
    /// Operations through `registry` onto `stores`, in shard order.
    pub(crate) const fn new(registry: E, stores: &'a [PageStore]) -> Self {
        Self { registry, stores }
    }
}

impl<E: Exposure> Route for Seeder<'_, E> {
    type Lease = ();

    async fn run(
        &self,
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> wavedb_core::Result<NodeReply> {
        let shard = op.owner(tenant, struct_hash).shard(self.stores.len());
        let store = self.stores.get(shard).ok_or_else(|| {
            wavedb_core::Error::Backend(format!("seeder: no shard {shard}"))
        })?;
        self.registry.run_node(store, tenant, struct_hash, op).await
    }

    fn lease(
        &self,
        _tenant: U48,
        _owner: Owner,
    ) -> impl std::future::Future<Output = wavedb_core::Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn run_leased(
        &self,
        (): &(),
        tenant: U48,
        struct_hash: u64,
        op: NodeOp,
    ) -> wavedb_core::Result<NodeReply> {
        self.run(tenant, struct_hash, op).await
    }
}
