//! The disk actor's maintenance work: what each [`Maintenance`] unit does to
//! the engine, as a droppable hint or for a caller who waits.
//!
//! Split from `disk` by layer: that module is the actor — its queues and
//! its loop; this one is the work the loop dispatches to. Every unit covers
//! every shard the actor owns (RFC 0067); a settle steps one shard at a
//! time, round-robin, so a round stays one bounded slice.

use wavedb_storage::PageStore;

use super::msg::Maintenance;

/// Ids a settle round may take before the actor looks at its queues again.
///
/// This is what makes maintenance interruptible in practice: without a bound,
/// one burst becomes one round and the priority valve has nothing to
/// interleave between, since it can only choose between *queued* items and not
/// preempt the one running.
///
/// Unmeasured, like the valve's marks — small enough that a round is short,
/// large enough that the per-round fixed cost (one window allocation, one
/// positioned write) is not paid per handful of records.
pub const SETTLE_BUDGET_IDS: usize = 4096;

/// Run one unit of maintenance. Returns whether a settle is still owed.
///
/// Faults are reported, never propagated: nobody asked, so there is nobody to
/// return to. Nothing acknowledged is at risk either way — the journal still
/// holds every unsettled batch, and the next round or the reopen retries.
pub(super) fn maintain(
    stores: &[PageStore],
    work: &Maintenance,
    cursor: &mut usize,
) -> bool {
    match work {
        Maintenance::Settle => return step(stores, cursor),
        // A checkpoint is deliberately **not** stepped: it drains, syncs
        // `data.bin` and frames the journal as one thing, and a partial
        // checkpoint is not a checkpoint. It is the one maintenance unit
        // that must run to completion, and therefore the one that bounds
        // read latency — see the module note on RFC 0063.
        Maintenance::Checkpoint => {
            for store in stores {
                if let Err(e) = store.commit_journal() {
                    eprintln!("disk actor: checkpoint failed: {e}");
                }
            }
        }
        Maintenance::Evict { budget_bytes } => {
            let share = budget_bytes / stores.len().max(1);
            for store in stores {
                store.evict_settled(share);
            }
        }
        Maintenance::Defragment { budget_blocks } => {
            for store in stores {
                if let Err(e) = store.defragment(*budget_blocks) {
                    eprintln!("disk actor: defragment failed: {e}");
                }
            }
        }
    }
    false
}

/// Run one unit of maintenance **to completion**, for a caller who is waiting.
///
/// The difference from [`maintain`] is not the work, it is the contract. A hint
/// is droppable and its faults are unobservable, which is right for a policy
/// loop and wrong for anyone who asked. So a fault comes back here, and
/// `Settle` **drains** rather than taking one bounded step: a caller that asked
/// to settle and was told "done" after 4096 ids would have been misled.
pub(super) fn run_now(
    stores: &[PageStore],
    work: &Maintenance,
) -> Result<(), wavedb_storage::StorageError> {
    let share = |budget: usize| budget / stores.len().max(1);
    stores.iter().try_for_each(|store| match work {
        Maintenance::Settle => store.drain(),
        Maintenance::Checkpoint => store.commit_journal(),
        Maintenance::Evict { budget_bytes } => {
            store.evict_settled(share(*budget_bytes));
            Ok(())
        }
        Maintenance::Defragment { budget_blocks } => {
            store.defragment(*budget_blocks).map(|_| ())
        }
    })
}

/// One settle round, on the next shard from `cursor` with anything pending.
/// `true` if any shard still owes a settle.
pub(super) fn step(stores: &[PageStore], cursor: &mut usize) -> bool {
    let n = stores.len();
    let next = (0..n)
        .map(|k| (*cursor + k) % n)
        .find(|&i| stores.get(i).is_some_and(PageStore::has_pending));
    let Some((i, store)) = next.and_then(|i| stores.get(i).map(|s| (i, s)))
    else {
        return false;
    };
    *cursor = (i + 1) % n;
    match store.settle_step(SETTLE_BUDGET_IDS) {
        Ok(_) => stores.iter().any(PageStore::has_pending),
        Err(e) => {
            eprintln!("disk actor: settle failed: {e}");
            // The round went back on the queue. Stop claiming to owe a settle
            // so a failing disk cannot spin the actor at full speed; the next
            // hint, or the reopen replay, retries it.
            false
        }
    }
}
