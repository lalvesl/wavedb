//! How a [`RouteHandle`]'s operations reach their owner — under the body's
//! lease, or plainly — and its writes, each one also as its `_durable` twin
//! (RFC 0072 §12): the same [`NodeOp`] with its flag set, so the owner's
//! worker answers once the batch is durable on its shard.

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::id::Id;
use crate::lease::Reach;
use crate::local_id::LocalId;
use crate::node_op::{NodeOp, NodeReply};
use crate::route::Route;
use crate::route_handle::RouteHandle;
use crate::route_reply::{done, mismatch};
use crate::traits::{NonUniqueStruct, UniqueStruct};
use crate::wire::to_wire;

impl<R: Route> RouteHandle<'_, R> {
    /// Run `op` on `struct_hash`'s owner: under its lease when the body
    /// holds it, plainly when the body holds nothing, refused when the body
    /// holds a lock that does not cover it.
    pub(super) async fn run(
        &self,
        struct_hash: u64,
        op: NodeOp,
    ) -> Result<NodeReply> {
        let Some(leases) = self.leases else {
            return self.route.run(self.tenant, struct_hash, op).await;
        };
        let owner = op.owner(self.tenant, struct_hash);
        let creates = matches!(op, NodeOp::CreatePivot { .. });
        match leases.reach(self.tenant, owner, creates) {
            Reach::Plain => self.route.run(self.tenant, struct_hash, op).await,
            Reach::Leased(lease) => {
                self.route
                    .run_leased(&lease, self.tenant, struct_hash, op)
                    .await
            }
            Reach::Created => {
                let reply =
                    self.route.run(self.tenant, struct_hash, op).await?;
                if creates {
                    leases.created(self.tenant, owner);
                }
                Ok(reply)
            }
            Reach::Refused => Err(Error::LockOrder(
                "an owner outside the held lock: take every owner in one lock",
            )),
        }
    }

    pub(super) async fn save_unique_with<T: UniqueStruct>(
        &self,
        value: &T,
        durable: bool,
    ) -> Result<()> {
        let op = NodeOp::SaveUnique {
            body: to_wire(value),
            durable,
        };
        done(&self.run(T::STRUCT_HASH, op).await?)
    }

    pub(super) async fn create_pivot_with<T: NonUniqueStruct>(
        &self,
        durable: bool,
    ) -> Result<LocalId> {
        // Minted here, created where the id places it: the owner of a new
        // collection is known before the collection exists.
        let pivot = Collection::<T>::mint_pivot(self.tenant);
        let op = NodeOp::CreatePivot { pivot, durable };
        done(&self.run(T::STRUCT_HASH, op).await?)?;
        Ok(pivot)
    }

    pub(super) async fn insert_with<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
        durable: bool,
    ) -> Result<Id> {
        let body = to_wire(value);
        let op = NodeOp::Insert {
            pivot,
            body,
            durable,
        };
        match self.run(T::STRUCT_HASH, op).await? {
            NodeReply::Inserted(id) => Ok(id),
            _ => Err(mismatch()),
        }
    }

    pub(super) async fn update_with<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
        durable: bool,
    ) -> Result<()> {
        let body = to_wire(value);
        let op = NodeOp::Update {
            pivot,
            id,
            body,
            durable,
        };
        done(&self.run(T::STRUCT_HASH, op).await?)
    }

    pub(super) async fn remove_with<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        durable: bool,
    ) -> Result<bool> {
        let op = NodeOp::Remove { pivot, id, durable };
        match self.run(T::STRUCT_HASH, op).await? {
            NodeReply::Removed(removed) => Ok(removed),
            _ => Err(mismatch()),
        }
    }
}
