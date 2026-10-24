//! Starting the workers, and hearing that they stopped ([RFC 0067] §3).
//!
//! Every worker's queue exists before any worker does, because each one's
//! accept loop routes into all of them — its own included. So the queues
//! close only when **every** holder of a [`Router`] has let go: the
//! workers' accept loops and connections, and whoever started them. That is
//! the shutdown order, and nothing has to enforce it: a worker serves jobs
//! until no connection anywhere can send it one, then closes its stores.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

use async_channel::Receiver;
use futures::channel::oneshot;
use wavedb_platform::carrier;
use wavedb_storage::{PageStore, StorageError, StorageResult};

use super::router::Router;
use super::worker::Job;
use crate::__rt::NodeTasks;
use crate::serve::{Accept, Ingress};
use crate::upkeep::Maintenance;

/// Jobs queued on one worker before its sender waits.
///
/// Bounded for the reason every queue here is: an unbounded queue turns a
/// client outrunning a worker into unbounded memory, and a bound turns it into
/// backpressure that reaches the socket.
pub const JOB_DEPTH: usize = 512;

/// Where a worker reports how closing its stores went.
pub type Closed = oneshot::Sender<StorageResult<()>>;

/// The receiving end of a [`Closed`].
pub type Closing = oneshot::Receiver<StorageResult<()>>;

/// What a worker's root task is started with. Built on the starting thread
/// and moved onto the worker's carrier, so every field is `Send` — the
/// stores included: they move in once and never leave.
pub struct ShardCtx<E> {
    pub(super) registry: E,
    /// The first data shard of the range, and the range's stores in order.
    pub(super) first: usize,
    pub(super) stores: Vec<PageStore>,
    /// Data shards in the database — where an owner's records live.
    pub(super) shards: usize,
    pub(super) policy: Maintenance,
    pub(super) jobs: Receiver<Job>,
    pub(super) closed: Closed,
    /// This worker's accept loop, when it has a listener.
    pub(super) accept: Option<Accept<E>>,
}

/// Start `threads` workers over the database's data shards (`stores`, in
/// shard order), and return the router onto them.
///
/// Each worker takes a listener from `ingress` while they last, and the
/// result carries one receiver per worker for the outcome of closing its
/// stores. Worker `w` owns the contiguous range of shards `p` with
/// `p · threads / shards = w`, and its stores move onto its thread with it.
///
/// A worker whose root task cannot be placed ([`MAX_SHARDS`] already
/// running) drops its queue, its listener and its stores at once: every
/// request routed to it is refused as "shard stopped", and its close is
/// reported as never having run — a fault the caller sees, not a hang.
///
/// [`MAX_SHARDS`]: crate::__rt::MAX_SHARDS
///
/// # Errors
/// A worker thread failing to spawn. The workers already started keep their
/// stores and close them once every router is dropped.
pub fn start<E: NodeTasks>(
    registry: E,
    stores: Vec<PageStore>,
    secret: [u8; 32],
    threads: usize,
    policy: Maintenance,
    mut ingress: Option<Ingress>,
) -> wavedb_platform::Result<(Router<E>, Vec<Closing>)> {
    let shards = stores.len().max(1);
    let threads = threads.clamp(1, shards);
    let (senders, queues): (Vec<_>, Vec<_>) = (0..threads)
        .map(|_| async_channel::bounded::<Job>(JOB_DEPTH))
        .unzip();
    let router = Router::new(registry, senders, shards, secret);
    let mut closing = Vec::with_capacity(threads);
    let mut rest = stores.into_iter();
    let mut first = 0;
    for (w, jobs) in queues.into_iter().enumerate() {
        // Shards `p` with `p · threads / shards = w`: from `first` up to the
        // first `p` of the next worker.
        let end = ((w + 1) * shards).div_ceil(threads);
        let (closed, on_close) = oneshot::channel();
        let ctx = ShardCtx {
            registry,
            first,
            stores: rest.by_ref().take(end - first).collect(),
            shards,
            policy,
            jobs,
            closed,
            accept: ingress.as_mut().and_then(|i| i.next(&router)),
        };
        // The carrier ends by itself once its accept loop, connections and
        // last job have ended and the stores are closed; `closed` says when.
        drop(carrier::spawn("wavedb-shard", move |here| {
            let _ = E::spawn_shard(ctx, here);
        })?);
        closing.push(on_close);
        first = end;
    }
    Ok((router, closing))
}

/// Wait for every worker to report closing its stores; the first fault wins,
/// and a worker that never reported is one.
///
/// # Errors
/// The first worker's fault.
pub async fn closed_all(closing: Vec<Closing>) -> StorageResult<()> {
    let mut first = Ok(());
    for worker in closing {
        let outcome = worker.await.unwrap_or(Err(StorageError::Corrupt(
            "a worker stopped before closing its stores",
        )));
        if first.is_ok() {
            first = outcome;
        }
    }
    first
}

/// The context crosses onto the worker's carrier.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<ShardCtx<()>>();
};
