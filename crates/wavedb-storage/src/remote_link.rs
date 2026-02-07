//! A data shard's side of its storage shard's chain of files ([RFC 0073]
//! §8): an upload reserves its sections, builds them at their bases and
//! hands them over, then awaits the files they landed in
//! ([`crate::remote_shard`]). Without a link, a store keeps a chain of files
//! of its own and PUTs them itself.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::sync::mpsc;

use futures::channel::oneshot;

use crate::error::{StorageError, StorageResult};
use crate::remote_shard::{RemoteMsg, RemoteReserved};
use crate::remote_shard_compact::{CompactAnswer, Report};

/// A data shard's link to its storage shard's remote thread.
#[derive(Debug)]
pub struct RemoteLink {
    inbox: mpsc::Sender<RemoteMsg>,
    /// Data shards uploading through the storage shard.
    per: u64,
}

/// The remote thread said goodbye — or crashed.
fn gone() -> StorageError {
    StorageError::Io(std::io::Error::other("the remote thread is gone"))
}

impl RemoteLink {
    /// A link onto the remote thread whose inbox is `inbox`, which `per`
    /// data shards upload through.
    #[must_use]
    pub fn new(inbox: mpsc::Sender<RemoteMsg>, per: usize) -> Self {
        Self {
            inbox,
            per: (per as u64).max(1),
        }
    }

    /// Data shards uploading through the storage shard.
    #[must_use]
    pub const fn per(&self) -> u64 {
        self.per
    }

    /// Room for sections `sizes` blocks long, in order.
    ///
    /// # Errors
    /// The remote thread gone.
    pub async fn reserve(
        &self,
        sizes: Vec<u64>,
    ) -> StorageResult<RemoteReserved> {
        let (reply, answer) = oneshot::channel();
        self.inbox
            .send(RemoteMsg::Reserve { sizes, reply })
            .map_err(|_| gone())?;
        answer.await.map_err(|_| gone())
    }

    /// Hand the built sections over and wait until every file holding them
    /// is in the object store.
    ///
    /// # Errors
    /// The PUT's fault, [`StorageError::Fenced`]; the remote thread gone.
    pub async fn write(
        &self,
        position: usize,
        sections: Vec<(u64, Vec<u8>)>,
        (cut, urgent): (bool, bool),
    ) -> StorageResult<()> {
        let (reply, answer) = oneshot::channel();
        self.inbox
            .send(RemoteMsg::Write {
                position,
                sections,
                cut,
                urgent,
                reply,
            })
            .map_err(|_| gone())?;
        answer.await.map_err(|_| gone())?
    }

    /// Reserved sections that will never be written.
    pub fn abandon(&self, bases: Vec<u64>) {
        let _ = self.inbox.send(RemoteMsg::Abandon { bases });
    }

    /// Report what the data shard at `position` still needs, and hear what
    /// the round did (`crate::remote_shard_compact`).
    ///
    /// # Errors
    /// The remote thread gone.
    pub async fn compact(
        &self,
        position: usize,
        report: Report,
    ) -> StorageResult<CompactAnswer> {
        let (reply, answer) = oneshot::channel();
        self.inbox
            .send(RemoteMsg::Compact {
                position,
                report,
                reply,
            })
            .map_err(|_| gone())?;
        answer.await.map_err(|_| gone())
    }
}

/// The last link's goodbye waits for the thread to end, as a storage
/// thread's does.
impl Drop for RemoteLink {
    fn drop(&mut self) {
        let (goodbye, answered) = mpsc::channel();
        if self.inbox.send(RemoteMsg::Closed(goodbye)).is_ok() {
            let _ = answered.recv();
        }
    }
}
