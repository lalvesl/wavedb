//! A worker's maintenance — BG, sharing the thread with its jobs (DATA)
//! ([RFC 0067] §4, step 4).
//!
//! A worker has **one** executor, and it knows nothing of priority
//! (RFC 0067 §4): jobs need none against the edge, which reaches them only
//! through the bounded job queue, and maintenance regulates itself here. BG
//! is one task that takes a single bounded step ([`Owned::step`]) only when the valve
//! ([`Priority`]) says maintenance goes next — which, with jobs executing or
//! queued, is only once what is not settled has grown into the valve's band —
//! and yields after every step. A job waits behind maintenance for at most
//! one step: one settle round of `SETTLE_BUDGET_IDS` ids.
//!
//! Every [`UPKEEP_PERIOD`] (200 ms) it applies the node's policy to each
//! store in turn ([`Owned::upkeep`]): open a settle cycle — the tick that
//! makes writes durable (RFC 0072), skipped when nothing was written —,
//! evict, compact — each bounded,
//! with a yield between stores — and an object-backed store brings back the
//! buckets reads wanted and uploads what it holds back past the threshold
//! ([`Owned::object_upkeep`], RFC 0070). An
//! upload awaits the object store: the jobs keep the thread meanwhile, and
//! this task's own steps wait for it.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

use std::rc::Rc;
use std::time::{Duration, Instant};

use async_channel::Receiver;
use embassy_futures::select::{Either, select};
use embassy_futures::yield_now;
use wavedb_platform::carrier::Here;
use wavedb_platform::time::sleep;

use super::owned::Owned;
use super::priority::{Available, Class, Priority};
use super::worker::Job;
use crate::upkeep::Maintenance;

/// How often the policy looks at the stores.
pub const UPKEEP_PERIOD: Duration = Duration::from_millis(200);

/// How long BG waits before looking again while jobs have the thread.
const BUSY_RECHECK: Duration = Duration::from_millis(1);

/// What a worker's BG task is started with.
pub struct Background {
    pub owned: Rc<Owned>,
    /// The worker's job queue — read only for "is anything waiting?".
    pub jobs: Receiver<Job>,
    pub policy: Maintenance,
    /// Data shards in the database, for the policy's per-shard shares.
    pub partitions: usize,
    /// Closed by the worker's root when its queue has closed.
    pub halt: Receiver<()>,
}

/// BG's task. One per worker, from a static pool sized to the workers a
/// process may run.
#[wavedb_platform::carrier::rt::task(
    pool_size = crate::__rt::MAX_SHARDS,
    embassy_executor = wavedb_platform::carrier::rt
)]
pub async fn background(bg: Background, here: Here) {
    run(&bg).await;
    drop(bg); // the worker's root waits for this `Rc` before closing the stores
    drop(here);
}

async fn run(bg: &Background) {
    let mut valve = Priority::default();
    let mut cursor = 0usize;
    let mut last = Instant::now();
    loop {
        if bg.halt.is_closed() {
            return;
        }
        let busy = bg.owned.working() || !bg.jobs.is_empty();
        let avail = Available {
            reads: busy,
            maintenance: bg.owned.owes(),
        };
        // A durable write waits on the open batch: it goes first.
        if bg.owned.urgent()
            || valve.next(avail, bg.owned.unsettled_bytes())
                == Some(Class::Maintenance)
        {
            bg.owned.step(&mut cursor).await;
            yield_now().await;
            continue;
        }
        let wait = if busy { BUSY_RECHECK } else { UPKEEP_PERIOD };
        if let Either::Second(_) = select(sleep(wait), bg.halt.recv()).await {
            return;
        }
        // Due regardless of load: the policy is what opens a settle cycle,
        // and a node that only settled when idle would grow its record cache
        // without bound under steady traffic.
        if last.elapsed() >= UPKEEP_PERIOD {
            for i in 0..bg.owned.len() {
                bg.owned.upkeep(i, &bg.policy, bg.partitions).await;
                bg.owned.object_upkeep(i, &bg.policy, bg.partitions).await;
                yield_now().await;
            }
            last = Instant::now();
        }
    }
}
