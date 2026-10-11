//! The disk actor's maintenance work: what each [`Maintenance`] unit does to
//! the engine, as a droppable hint or for a caller who waits.
//!
//! Split from `disk` by layer: that module is the actor — its queues and
//! its loop; this one is the work the loop dispatches to.

use wavedb_storage::{PageStore, Progress};

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
pub(super) fn maintain(store: &PageStore, work: &Maintenance) -> bool {
    match work {
        Maintenance::Settle => return step(store),
        // A checkpoint is deliberately **not** stepped: it drains, syncs
        // `data.bin` and frames the journal as one thing, and a partial
        // checkpoint is not a checkpoint. It is the one maintenance unit
        // that must run to completion, and therefore the one that bounds
        // read latency — see the module note on RFC 0063.
        Maintenance::Checkpoint => {
            if let Err(e) = store.commit_journal() {
                eprintln!("disk actor: checkpoint failed: {e}");
            }
        }
        Maintenance::Evict { budget_bytes } => {
            store.evict_settled(*budget_bytes);
        }
        Maintenance::Defragment { budget_blocks } => {
            if let Err(e) = store.defragment(*budget_blocks) {
                eprintln!("disk actor: defragment failed: {e}");
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
    store: &PageStore,
    work: &Maintenance,
) -> Result<(), wavedb_storage::StorageError> {
    match work {
        Maintenance::Settle => store.drain(),
        Maintenance::Checkpoint => store.commit_journal(),
        Maintenance::Evict { budget_bytes } => {
            store.evict_settled(*budget_bytes);
            Ok(())
        }
        Maintenance::Defragment { budget_blocks } => {
            store.defragment(*budget_blocks).map(|_| ())
        }
    }
}

/// One settle round. `true` if more remain.
pub(super) fn step(store: &PageStore) -> bool {
    match store.settle_step(SETTLE_BUDGET_IDS) {
        Ok(Progress::Settled) => true,
        Ok(Progress::Done) => false,
        Err(e) => {
            eprintln!("disk actor: settle failed: {e}");
            // The round went back on the queue. Stop claiming to owe a settle
            // so a failing disk cannot spin the actor at full speed; the next
            // hint, or the reopen replay, retries it.
            false
        }
    }
}
