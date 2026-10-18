//! The disk actor — sole owner of the storage engine ([RFC 0064]).
//!
//! Every shard reaches storage by message; nothing else holds the
//! [`PageStore`]. That single ownership is what the design is for, and it buys
//! three things at once:
//!
//! - the engine's locks stop having a second party to contend with;
//! - two shards wanting the same page produce one read rather than two (the
//!   de-duplication a shared cache cannot do without a single owner);
//! - the data directory's lock has exactly one holder by construction
//!   instead of by convention.
//!
//! ## Two queues
//!
//! Requests ([`DiskRequest`]) are high priority — a shard is awaiting each
//! one. Maintenance ([`Maintenance`]) is low — nobody is. Which to serve is
//! [`Priority`]'s decision, driven by the journal's length, and the reasoning
//! (including why strict priority would be an out-of-memory rather than an
//! unfairness) is in [`super::priority`].
//!
//! **The valve bounds waiting, not latency.** Because the actor serves one
//! message at a time, a read arriving while a long checkpoint runs waits for
//! that whole checkpoint — priority between *queued* items says nothing about
//! the item *executing*. Bounding read latency needs the maintenance work
//! itself to become interruptible, which is [RFC 0063]'s re-entrant `drain`.
//! That makes 0063 a prerequisite of this scheme rather than a parallel item.
//!
//! ## Not split in two yet
//!
//! RFC 0064's target splits this into a **journal actor** and a **page
//! actor**. That waits on the settle path's ownership question — `plan_slot`
//! reads shard-side caches to write page-actor-side dictionaries — which is
//! open. One actor owning the whole engine is the ownership boundary drawn
//! where it can be drawn correctly today.
//!
//! [RFC 0064]: ../../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md
//! [RFC 0063]: ../../../../rfcs/0063-engine-yield-map-and-interruptible-engine-PLANNED.md

use async_channel::{Receiver, SendError, Sender};
use embassy_futures::select::{Either, select};
use futures::channel::oneshot;
use wavedb_core::store::Store;
use wavedb_platform::carrier::{self, Here};
use wavedb_storage::PageStore;

use super::disk_work::{maintain, run_now, step};
use super::msg::{DiskRequest, EngineStats, Maintenance};
use super::priority::{Available, Class, Priority};

/// Requests in flight before a sender waits.
///
/// Bounded on purpose. An unbounded queue converts a shard outrunning the disk
/// into unbounded memory growth, which is the failure this project has already
/// hit twice by other routes; a bound converts it into backpressure, which is
/// the thing that was wanted.
pub const QUEUE_DEPTH: usize = 1024;

/// Maintenance queued before a caller waits. Short: the requests coalesce —
/// two queued settles do the work of one — so a deep queue would only hold
/// duplicates.
pub const MAINTENANCE_DEPTH: usize = 16;

/// A handle onto the actor. Cheap to clone, `Send`, and the only way to reach
/// storage once the actor owns it.
#[derive(Clone)]
pub struct DiskHandle {
    requests: Sender<DiskRequest>,
    maintenance: Sender<Maintenance>,
}

impl DiskHandle {
    /// Queue a request, waiting if the actor is behind.
    ///
    /// # Errors
    /// The actor is gone.
    pub async fn send(
        &self,
        request: DiskRequest,
    ) -> Result<(), SendError<DiskRequest>> {
        self.requests.send(request).await
    }

    /// Ask for maintenance, **dropping the request if the queue is full**.
    ///
    /// Not a failure: a full queue already holds a settle that has not run, and
    /// a second one would do the same work. Blocking here would be worse than
    /// dropping — it would make a caller who is explicitly not waiting wait.
    pub fn hint(&self, work: Maintenance) {
        let _ = self.maintenance.try_send(work);
    }

    /// Run `work` to completion and report the outcome.
    ///
    /// The waiting counterpart of [`hint`](Self::hint), and the difference
    /// between them is who is on the other end: a hint may be dropped and its
    /// faults are only printed, which is right for a policy loop and wrong for
    /// an operator or a measurement. `Settle` here drains rather than stepping.
    ///
    /// # Errors
    /// The actor is gone, or the maintenance itself faulted.
    pub async fn maintain(&self, work: Maintenance) -> wavedb_core::Result<()> {
        self.ask(|answer| DiskRequest::Maintain { work, answer })
            .await
    }

    /// What the engine currently holds.
    ///
    /// Has to be a message: once the actor owns the `PageStore`, "is anything
    /// unsettled?" is not answerable by looking.
    ///
    /// # Errors
    /// The actor is gone.
    pub async fn stats(&self) -> wavedb_core::Result<EngineStats> {
        self.ask(|answer| DiskRequest::Stats { answer }).await
    }

    /// Send a request and await its answer, naming both failures at this seam
    /// rather than inventing one further up.
    async fn ask<T>(
        &self,
        build: impl FnOnce(super::msg::Answer<T>) -> DiskRequest,
    ) -> wavedb_core::Result<T> {
        let (answer, wait) = oneshot::channel();
        self.requests.send(build(answer)).await.map_err(|_| {
            wavedb_core::Error::Backend("disk actor stopped".into())
        })?;
        wait.await.map_err(|_| {
            wavedb_core::Error::Backend("disk actor dropped the answer".into())
        })?
    }
}

/// Own the shards' stores and serve both queues until every handle is
/// dropped. A request names its shard; maintenance covers all of them.
pub async fn run(
    stores: Vec<PageStore>,
    requests: Receiver<DiskRequest>,
    maintenance: Receiver<Maintenance>,
) {
    let mut priority = Priority::default();
    // A settle left part-done by the last round. It is maintenance the actor
    // owes itself, so it counts as queued even when the channel is empty —
    // otherwise a half-settled queue would wait for the next external hint.
    let mut settling = false;
    // Where the round-robin settle picks up next.
    let mut cursor = 0usize;
    loop {
        let avail = Available {
            reads: !requests.is_empty(),
            maintenance: settling || !maintenance.is_empty(),
        };
        // One funnel for requests, so the `Shutdown` handoff below has exactly
        // one place to happen — it needs to *consume* the store, which a
        // second serving site could not do.
        let journal: u64 = stores.iter().map(PageStore::journal_len).sum();
        let next = match priority.next(avail, journal) {
            Some(Class::Read) => requests.try_recv().ok(),
            // Owed work first: finishing a settle already begun beats
            // starting another.
            Some(Class::Maintenance) if settling => {
                settling = step(&stores, &mut cursor);
                None
            }
            Some(Class::Maintenance) => {
                if let Ok(work) = maintenance.try_recv() {
                    settling = maintain(&stores, &work, &mut cursor);
                }
                None
            }
            // Both empty: block until either speaks. Without this the loop
            // would spin on an idle node.
            //
            // Requests outlive maintenance: with the maintenance senders gone
            // the actor keeps serving reads, so a closed maintenance queue
            // drops out of the wait instead of ending the loop — and must,
            // since a closed queue is always ready and would spin it.
            None if maintenance.is_closed() => match requests.recv().await {
                Ok(request) => Some(request),
                Err(_) => break,
            },
            None => match select(requests.recv(), maintenance.recv()).await {
                Either::First(Ok(request)) => Some(request),
                Either::First(Err(_)) => break,
                Either::Second(Ok(work)) => {
                    settling = maintain(&stores, &work, &mut cursor);
                    None
                }
                Either::Second(Err(_)) => None,
            },
        };
        match next {
            None => {}
            // The engine is **dropped before the answer is sent**, and that
            // ordering is the whole point: dropping it releases the data
            // directory's lock, and a caller that resumes first could re-open
            // before it was free. `Server::run_with_shutdown`
            // does exactly that on a restart, and gets `EngineBusy` if this
            // is the other way round.
            Some(DiskRequest::Shutdown { answer }) => {
                let outcome = stores
                    .iter()
                    .try_for_each(|store| {
                        store.commit_journal()?;
                        store.force_retirement()
                    })
                    .map_err(|e| {
                        wavedb_core::Error::Backend(format!("shutdown: {e}"))
                    });
                drop(stores);
                let _ = answer.send(outcome);
                return;
            }
            Some(request) => serve(&stores, request).await,
        }
    }
    // Handles all dropped without an explicit stop — nothing can ask for
    // anything again. A failure is reported and not propagated: there is no
    // caller left to receive it, and the journal still holds anything
    // unsettled for the next open to replay.
    for store in &stores {
        if let Err(e) = store.commit_journal() {
            eprintln!("disk actor: final checkpoint failed: {e}");
        }
    }
}

/// The store of `shard`, or the fault a request naming no shard gets.
fn shard(stores: &[PageStore], shard: u16) -> wavedb_core::Result<&PageStore> {
    stores.get(usize::from(shard)).ok_or_else(|| {
        wavedb_core::Error::Backend(format!("disk actor: no shard {shard}"))
    })
}

async fn serve(stores: &[PageStore], request: DiskRequest) {
    match request {
        DiskRequest::Get {
            shard: s,
            id,
            answer,
        } => {
            let _ = match shard(stores, s) {
                Ok(store) => answer.send(store.get(id).await),
                Err(e) => answer.send(Err(e)),
            };
        }
        DiskRequest::GetOf {
            shard: s,
            struct_hash,
            id,
            answer,
        } => {
            let _ = match shard(stores, s) {
                Ok(store) => answer.send(store.get_of(struct_hash, id).await),
                Err(e) => answer.send(Err(e)),
            };
        }
        DiskRequest::Apply {
            shard: s,
            batch,
            answer,
        } => {
            let _ = match shard(stores, s) {
                Ok(store) => answer.send(store.apply(&batch).await),
                Err(e) => answer.send(Err(e)),
            };
        }
        DiskRequest::Stats { answer } => {
            let _ = answer.send(Ok(EngineStats {
                pending: stores.iter().any(PageStore::has_pending),
                journal_bytes: stores.iter().map(PageStore::journal_len).sum(),
                largest_free_extent: stores
                    .iter()
                    .map(PageStore::largest_free_extent)
                    .min()
                    .unwrap_or(0),
            }));
        }
        DiskRequest::Maintain { work, answer } => {
            let _ = answer.send(run_now(stores, &work).map_err(|e| {
                wavedb_core::Error::Backend(format!("maintain: {e}"))
            }));
        }
        // Handled in the loop, which owns the store it has to drop.
        DiskRequest::Shutdown { answer } => {
            let _ = answer.send(Ok(()));
        }
    }
}

/// Disk actors alive at once. One engine per process makes it one; the
/// headroom covers an actor still returning while the next opens.
const ACTORS: usize = 4;

/// The actor as a carrier task.
#[wavedb_platform::carrier::rt::task(
    pool_size = ACTORS,
    embassy_executor = wavedb_platform::carrier::rt
)]
async fn actor(
    stores: Vec<PageStore>,
    requests: Receiver<DiskRequest>,
    maintenance: Receiver<Maintenance>,
    here: Here,
) {
    run(stores, requests, maintenance).await;
    drop(here);
}

/// Start the actor over the database's shards, on a thread of its own, and
/// return the handle workers use.
///
/// The `PageStore`s are **moved onto** the actor's thread, which is only legal
/// because the engine is `Send + Sync` — asserted in `wavedb-storage`'s
/// `thread_safety` module rather than assumed here.
///
/// # Errors
/// The carrier thread failing to spawn.
pub fn start(stores: Vec<PageStore>) -> wavedb_platform::Result<DiskHandle> {
    let (requests, rx) = async_channel::bounded(QUEUE_DEPTH);
    let (maintenance, mrx) = async_channel::bounded(MAINTENANCE_DEPTH);
    // The carrier ends with the actor: when every handle is gone, or after a
    // `Shutdown`. The caller learns of either through the handle, so the
    // `Finished` is not kept.
    drop(carrier::spawn("wavedb-disk", move |here| {
        if here.spawn(actor(stores, rx, mrx, here.clone())).is_err() {
            // The store drops here, so every handle sees the actor gone.
            eprintln!("disk actor: no free slot, engine closed");
        }
    })?);
    Ok(DiskHandle {
        requests,
        maintenance,
    })
}

#[cfg(test)]
mod tests {
    use super::{DiskRequest, QUEUE_DEPTH};
    use futures::channel::oneshot;
    use wavedb_core::Id;

    /// A queued request whose actor never arrives is still answered.
    ///
    /// The caller is awaiting a `oneshot`; dropping it yields a bare channel
    /// error with no cause, so the shutdown path has to say why instead.
    #[test]
    fn a_refused_request_reports_a_cause() {
        let (answer, rx) = oneshot::channel();
        DiskRequest::Get {
            shard: 0,
            id: Id::from_raw(7),
            answer,
        }
        .refuse("engine closed");
        let err = futures::executor::block_on(rx)
            .expect("answered")
            .expect_err("must refuse");
        assert!(
            format!("{err}").contains("engine closed"),
            "cause lost: {err}"
        );
    }

    /// The request queue bounds itself, so a shard outrunning the disk waits
    /// rather than growing the process.
    #[test]
    fn the_queue_applies_backpressure() {
        let (tx, _rx) = async_channel::bounded::<DiskRequest>(QUEUE_DEPTH);
        for _ in 0..QUEUE_DEPTH {
            let (answer, _) = oneshot::channel();
            tx.try_send(DiskRequest::Get {
                shard: 0,
                id: Id::from_raw(1),
                answer,
            })
            .expect("within depth");
        }
        let (answer, _) = oneshot::channel();
        assert!(
            tx.try_send(DiskRequest::Get {
                shard: 0,
                id: Id::from_raw(1),
                answer,
            })
            .is_err(),
            "a full queue must refuse, not grow"
        );
    }
}
