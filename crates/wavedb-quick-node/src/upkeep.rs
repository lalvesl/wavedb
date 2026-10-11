//! The node's background upkeep: what it asks the disk actor for, on a timer.
//!
//! Split from `lib.rs` for the file budget, and it reads better apart anyway:
//! everything here is *policy* — when to suggest work — while the actor
//! decides when to do it.
//!
//! Hints, not calls. Once the actor owns the engine (RFC 0064) this loop can
//! only suggest, and the actor weighs each suggestion against the reads it is
//! also serving (`shard::priority`). A hint dropped because the queue is full
//! is a hint the actor has already been given.

use async_channel::Receiver;
use embassy_futures::join::join;
use embassy_futures::select::select;
use wavedb_platform::carrier::Here;

use crate::feed::FeedReceiver;
use crate::subscribe::Subscriptions;
use crate::{ServerError, shard};

/// The background maintenance policy: how the node settles, checkpoints,
/// and bounds its caches while serving.
#[derive(Debug, Clone, Copy)]
pub struct Maintenance {
    /// Journal bytes that trigger a checkpoint (journal truncates to zero).
    pub checkpoint_after_bytes: u64,
    /// Cache bytes the settle task evicts down to (settled entries only).
    pub cache_budget_bytes: usize,
    /// Defragment once the largest free extent falls below this many blocks —
    /// the point where a checkpoint's window stops fitting a hole and starts
    /// growing the tail (RFC 0042).
    pub defrag_below_blocks: u64,
    /// Blocks one defragmentation pass may copy. The cleaner's whole cost is
    /// this, so it is a plain IO budget per tick.
    pub defrag_budget_blocks: u64,
}

impl Default for Maintenance {
    fn default() -> Self {
        Self {
            checkpoint_after_bytes: 64 * 1024 * 1024, // 64 MiB of journal
            cache_budget_bytes: 1024 * 1024 * 1024,   // 1 GiB — generous
            defrag_below_blocks: 256, // 1 MiB of contiguous room
            defrag_budget_blocks: 256, // ≤ 1 MiB copied per tick
        }
    }
}

/// How often the upkeep loop looks at the engine.
const UPKEEP_PERIOD: std::time::Duration =
    std::time::Duration::from_millis(200);

/// The background maintenance loop: periodically settle the pending queue,
/// checkpoint once the journal crosses the threshold, and evict settled
/// cache entries down to budget. An engine fault stops maintenance (acked
/// writes stay safe in the journal); serving continues.
pub async fn maintain(disk: shard::DiskHandle, policy: Maintenance) {
    loop {
        // A sleep per round rather than a ticker: a ticker that fell behind
        // fires back to back to catch up, and a burst of hints is exactly
        // what a busy actor does not need. (tokio's interval needed
        // `MissedTickBehavior::Delay` to say the same thing.)
        wavedb_platform::time::sleep(UPKEEP_PERIOD).await;
        // Hints, not calls: the actor owns the engine, and it decides *when*
        // to act on them against the reads it is also serving (`shard::
        // priority`). A hint dropped because the queue is full is a hint the
        // actor has already been given.
        let Ok(stats) = disk.stats().await else {
            return; // the actor is gone; serving is over too
        };
        disk.hint(shard::Maintenance::Settle);
        if stats.journal_bytes > policy.checkpoint_after_bytes {
            disk.hint(shard::Maintenance::Checkpoint);
        }
        disk.hint(shard::Maintenance::Evict {
            budget_bytes: policy.cache_budget_bytes,
        });
        // A checkpoint leaves its `Commit` frame unsynced for the next write to
        // carry, for free, and its retired journal on disk until the next
        // checkpoint disposes of it (RFC 0047). Maintenance has nothing to do
        // about either: forcing the barrier here would spend the IOp the
        // deferral exists to save, to reclaim disk — the abundant resource.
        // Keep a window large enough for the next checkpoint to land in a
        // hole rather than at the tail; a pass that finds nothing is free.
        if stats.largest_free_extent < policy.defrag_below_blocks {
            disk.hint(shard::Maintenance::Defragment {
                budget_blocks: policy.defrag_budget_blocks,
            });
        }
    }
}

/// The engine's owner is gone — nothing can be asked of it again.
pub const fn stopped() -> ServerError {
    ServerError::Storage(wavedb_storage::StorageError::Corrupt(
        "disk actor stopped before the node did",
    ))
}

/// Background tasks alive at once: one per node, with headroom for a node
/// still winding down (see `__rt::MAX_NODES`).
const BACKGROUND: usize = crate::__rt::MAX_NODES;

/// Start the node's background task on the accept carrier: [`maintain`] and
/// the mutation publisher, until `halt` closes.
///
/// # Errors
/// [`ServerError::Io`] when the background pool is full — a node that could
/// not maintain its engine refuses to start rather than serve without it.
pub fn spawn_background(
    disk: shard::DiskHandle,
    policy: Maintenance,
    feed: FeedReceiver,
    subs: Subscriptions,
    halt: Receiver<()>,
    here: &Here,
) -> crate::Result<()> {
    here.spawn(background(disk, policy, feed, subs, halt, here.clone()))
        .map_err(|_| {
            ServerError::Io(std::io::Error::other("background pool full"))
        })
}

/// Both never-ending background jobs, until the node halts.
#[wavedb_platform::carrier::rt::task(
    pool_size = BACKGROUND,
    embassy_executor = wavedb_platform::carrier::rt
)]
async fn background(
    disk: shard::DiskHandle,
    policy: Maintenance,
    feed: FeedReceiver,
    subs: Subscriptions,
    halt: Receiver<()>,
    here: Here,
) {
    let work = join(maintain(disk, policy), crate::feed::publish(feed, subs));
    select(work, halt.recv()).await;
    drop(here); // held to the end: the carrier waits for this task
}
