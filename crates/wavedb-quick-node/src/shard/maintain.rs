//! Maintenance on demand: every worker asked to bring its stores to a
//! stated condition **now**, between operations, instead of when its policy
//! gets to it ([`Router::maintain`](super::Router::maintain)).
//!
//! It is what a benchmark's phase boundary needs — the multi row runs on
//! these workers (`benches/`), and a read phase must not measure the
//! settling the write phase before it left behind — and what an operator's
//! "settle now" would be. It travels as a job, so the stores never leave
//! their worker: only the request and its answer cross a thread, and the
//! work runs on the worker's executor beside its other jobs and its own
//! maintenance, never blocking either.

use wavedb_storage::{StorageError, StorageResult};

use super::owned::Owned;

/// What a worker is asked to do to each of its stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Maintain {
    /// Settle everything committed and wait until it is durable
    /// ([`PageStore::settle_all`](wavedb_storage::PageStore::settle_all)).
    Settle,
    /// Drop settled record-cache entries down to this many bytes per store.
    Evict(usize),
}

impl Owned {
    /// Do `what` to every store of the range, in order.
    ///
    /// # Errors
    /// The first store's fault; every store is still attempted.
    pub async fn maintain(&self, what: Maintain) -> StorageResult<()> {
        let mut first: Option<StorageError> = None;
        for store in self.stores() {
            let done = match what {
                Maintain::Settle => store.settle_all().await,
                Maintain::Evict(bytes) => {
                    store.evict_settled(bytes);
                    Ok(())
                }
            };
            if let Err(e) = done {
                first.get_or_insert(e);
            }
        }
        first.map_or(Ok(()), Err)
    }
}
