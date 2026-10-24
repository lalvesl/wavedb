//! The node's maintenance **policy**, and the accept thread's background task.
//!
//! The policy is plain numbers: when a worker opens a checkpoint, how far it
//! evicts, when it defragments. Each worker applies it to the stores it owns,
//! on its own thread, between its jobs (`shard::background`, RFC 0067
//! step 4). What is left on the accept thread is the mutation publisher.

use async_channel::Receiver;
use embassy_futures::select::select;
use wavedb_platform::carrier::Here;

use crate::ServerError;
use crate::feed::FeedReceiver;
use crate::subscribe::Subscriptions;

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

/// Background tasks alive at once: one per node, with headroom for a node
/// still winding down (see `__rt::MAX_NODES`).
const BACKGROUND: usize = crate::__rt::MAX_NODES;

/// Start the node's background task on the accept carrier — the mutation
/// publisher — until `halt` closes.
///
/// # Errors
/// [`ServerError::Io`] when the background pool is full — a node that could
/// not deliver its live events refuses to start rather than serve without.
pub fn spawn_background(
    feed: FeedReceiver,
    subs: Subscriptions,
    halt: Receiver<()>,
    here: &Here,
) -> crate::Result<()> {
    here.spawn(background(feed, subs, halt, here.clone()))
        .map_err(|_| {
            ServerError::Io(std::io::Error::other("background pool full"))
        })
}

/// The publisher, until the node halts.
#[wavedb_platform::carrier::rt::task(
    pool_size = BACKGROUND,
    embassy_executor = wavedb_platform::carrier::rt
)]
async fn background(
    feed: FeedReceiver,
    subs: Subscriptions,
    halt: Receiver<()>,
    here: Here,
) {
    select(crate::feed::publish(feed, subs), halt.recv()).await;
    drop(here); // held to the end: the carrier waits for this task
}
