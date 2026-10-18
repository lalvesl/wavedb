//! What crosses a thread boundary ([RFC 0064]).
//!
//! This module is the whole `Send` surface of the shard model, and that is the
//! point: everything here is **plain data** — ids, wire bytes, a `Vec<Write>`
//! — so nothing about the engine or the index layer has to become `Send` to
//! run shards on separate threads. A shard's own state (its cache, its
//! `Collection` handles, its in-flight futures) never leaves its thread and
//! stays free to be `Rc`/`RefCell`.
//!
//! The reply channel is the other half of that: a request carries the sender
//! it wants answered on, so the disk actor never blocks and the caller simply
//! awaits. Request/reply is `await`, not a synchronous message — which is why
//! the actor-to-actor deadlock surface [RFC 0058] could not resolve does not
//! arise here.
//!
//! [RFC 0064]: ../../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md
//! [RFC 0058]: ../../../../rfcs/0058-per-type-actors-DEPRECATED.md

use futures::channel::oneshot;
use wavedb_core::store::Write;
use wavedb_core::{Id, Result};

/// Where one request's answer goes back.
pub type Answer<T> = oneshot::Sender<Result<T>>;

/// One request to the disk actor.
///
/// `Apply` carries an **owned** `Vec<Write>` rather than a borrowed slice:
/// `Store::apply` takes `&[Write]`, but the batch has to outlive the caller's
/// stack frame once it is queued for another thread. That copy is the entire
/// cost of the thread crossing, and it is bounded by the batch — not by the
/// database, the cache, or the page.
pub enum DiskRequest {
    /// Untyped read — the id alone names no type, so the engine probes every
    /// registered slot. Typed callers use [`DiskRequest::GetOf`].
    Get {
        shard: u16,
        id: Id,
        answer: Answer<Option<Vec<u8>>>,
    },
    /// Type-directed read: one routed lookup instead of a scan.
    GetOf {
        shard: u16,
        struct_hash: u64,
        id: Id,
        answer: Answer<Option<Vec<u8>>>,
    },
    /// One atomic batch. The answer arrives after the batch is durable, so a
    /// shard learns of a conflict or an I/O fault exactly where it would have
    /// learnt of it in-process.
    ///
    /// **High priority, despite being a write.** RFC 0064's low-priority class
    /// is the *settle handoff*, where nobody is waiting; this is not that. The
    /// caller is blocked on the `Expect` guard's verdict, so de-prioritising it
    /// would be de-prioritising a request with someone on the other end.
    Apply {
        shard: u16,
        batch: Vec<Write>,
        answer: Answer<()>,
    },
    /// What the engine currently holds. High priority because it is cheap and
    /// because a policy blocked on it is a policy not being applied.
    Stats { answer: Answer<EngineStats> },
    /// Run one unit of [`Maintenance`] **to completion** and report it.
    ///
    /// The same work as the low-priority queue, asked for by someone who is
    /// waiting — which is exactly what moves it to this queue and this type.
    /// Not a duplicate of the hint: a hint is droppable and unobservable, and
    /// there are callers for whom both of those are wrong. An operator
    /// compacting a store needs to know it happened; a benchmark measuring a
    /// quiesced footprint is measuring nothing if the settle it asked for was
    /// silently discarded.
    Maintain {
        work: Maintenance,
        answer: Answer<()>,
    },
    /// Settle, checkpoint, and force the retirement barrier — the clean-stop
    /// sequence, so a restart replays nothing.
    ///
    /// A request rather than [`Maintenance`] precisely because someone *is*
    /// waiting: `Server::run_with_shutdown` returns this outcome, and a node
    /// that reports a clean stop it did not achieve is worse than one that
    /// reports the fault.
    Shutdown { answer: Answer<()> },
}

/// What the engine will say about itself, summed over its shards.
///
/// Introspection has to be a message like anything else: once the actor owns
/// the `PageStore`, "is anything unsettled?" is not a question the outside can
/// answer by looking. A node's maintenance policy and any test of the priority
/// valve both need it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineStats {
    /// A committed batch is waiting for its page write, in some shard.
    pub pending: bool,
    /// Committed journal bytes across the shards — what the valve weighs, and
    /// what grows without bound if maintenance never runs.
    pub journal_bytes: u64,
    /// Blocks in the largest free extent of the **most fragmented** shard —
    /// the defragmenter's trigger.
    pub largest_free_extent: u64,
}

/// Work nobody is waiting on — the low-priority class.
///
/// A separate type rather than a `DiskRequest` variant, because the two travel
/// on different queues and the type is what stops one being put on the other's.
/// No reply channel: a caller that wanted the answer would be waiting, which
/// is the property that makes this class low priority in the first place.
pub enum Maintenance {
    /// Drain the pending queue into pages. Takes no barrier.
    Settle,
    /// Settle, then checkpoint: `data.bin` synced, the journal framed.
    Checkpoint,
    /// Drop settled cache entries down to `budget_bytes`, shared equally by
    /// the shards.
    Evict { budget_bytes: usize },
    /// Relocate stranded live pages so free space coalesces (RFC 0042).
    Defragment { budget_blocks: u64 },
}

impl DiskRequest {
    /// Hand this request the failure that stopped it from being served.
    ///
    /// Used when the actor is gone: every queued request must be answered,
    /// because a caller awaiting a dropped `oneshot` would otherwise see a
    /// channel error with no cause attached to it.
    pub fn refuse(self, why: &str) {
        let cause = wavedb_core::Error::Backend(why.to_string());
        match self {
            Self::Get { answer, .. } | Self::GetOf { answer, .. } => {
                let _ = answer.send(Err(cause));
            }
            // All three answer `()`, so one arm serves them.
            Self::Apply { answer, .. }
            | Self::Maintain { answer, .. }
            | Self::Shutdown { answer } => {
                let _ = answer.send(Err(cause));
            }
            Self::Stats { answer } => {
                let _ = answer.send(Err(cause));
            }
        }
    }
}

/// A `DiskRequest` crossing a thread is the model's only `Send` requirement.
/// If this ever fails, something non-`Send` has been put in a message — which
/// would force the migration the design exists to avoid.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<DiskRequest>();
};
