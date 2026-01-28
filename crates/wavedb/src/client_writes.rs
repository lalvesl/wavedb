//! The client [`Db`]'s writes, each one also as its `_durable` twin
//! ([RFC 0072] §12): the same command frame with its `durable` bit set, so
//! the node answers once the batch is durable on the owner's shard.
//!
//! The mirror into the local cache stays an ordinary write either way: the
//! node is the truth, and a cache that loses a mirror in a crash refills by
//! catch-up. A durable save is never queued offline — the queue lives in
//! memory, so it could not keep the promise — and surfaces the transport
//! fault instead.
//!
//! [RFC 0072]: https://github.com/wavedb/wavedb/blob/main/rfcs/0072-the-settle-is-the-durability-point.md

use wavedb_core::expose::{Command, Reply};
use wavedb_core::wire::{to_wire, to_wire_pair};
use wavedb_core::{Id, LocalId, NonUniqueStruct, UniqueStruct};
use wavedb_net::CommandFrame;

use crate::client_cache as cache;
use crate::db::Db;
use crate::error::Result;
use crate::reply;

impl Db {
    /// One write's command, durable when asked.
    async fn write(
        &self,
        struct_hash: u64,
        command: Command,
        payload: Vec<u8>,
        durable: bool,
    ) -> Result<Reply> {
        let frame = CommandFrame::new(struct_hash, command, payload);
        let frame = if durable { frame.durable() } else { frame };
        self.command_frame(frame).await
    }

    #[allow(clippy::redundant_pub_crate)]
    pub(crate) async fn save_unique_with<T: UniqueStruct>(
        &self,
        value: &T,
        durable: bool,
    ) -> Result<()> {
        // Node-first: flush any older offline writes before this one (W8).
        self.drain_offline_queue().await;
        let payload = to_wire(value);
        match self
            .write(T::STRUCT_HASH, Command::Save, payload, durable)
            .await
        {
            Ok(r) => {
                reply::done(&r)?;
                cache::mirror_unique(self, value).await;
                Ok(())
            }
            // Offline with a cache: queue for replay and mirror locally, so
            // the save succeeds provisionally instead of losing the work (W8).
            Err(err)
                if !durable
                    && cache::is_transport(&err)
                    && self.local().is_some() =>
            {
                self.enqueue_offline(
                    T::STRUCT_HASH,
                    Command::Save,
                    to_wire(value),
                );
                cache::mirror_unique(self, value).await;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    #[allow(clippy::redundant_pub_crate)]
    pub(crate) async fn insert_with<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
        durable: bool,
    ) -> Result<Id> {
        // `(pivot, value)` as the wire tuple the node's insert step decodes.
        let payload = to_wire_pair(&pivot, value);
        let r = self
            .write(T::STRUCT_HASH, Command::Insert, payload, durable)
            .await?;
        let id = reply::inserted(&r)?;
        cache::mirror_record(self, pivot, id, value).await;
        Ok(id)
    }

    #[allow(clippy::redundant_pub_crate)]
    pub(crate) async fn update_with<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
        durable: bool,
    ) -> Result<()> {
        // The pivot leads: it owns the operation, so the node places and
        // serialises the update by it (and refuses a record of another
        // collection). Byte-identical to the triple `(pivot, id, value)`.
        let payload = to_wire_pair(&(pivot, id), value);
        let r = self
            .write(T::STRUCT_HASH, Command::Update, payload, durable)
            .await?;
        reply::done(&r)?;
        cache::mirror_record(self, pivot, id, value).await;
        Ok(())
    }

    #[allow(clippy::redundant_pub_crate)]
    pub(crate) async fn remove_with<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        durable: bool,
    ) -> Result<bool> {
        let payload = to_wire(&(pivot, id));
        let r = self
            .write(T::STRUCT_HASH, Command::Remove, payload, durable)
            .await?;
        let removed = reply::removed(&r)?;
        if removed {
            cache::mirror_remove::<T>(self, pivot, id).await;
        }
        Ok(removed)
    }
}
