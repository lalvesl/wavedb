//! Phases 2–4 and 6 of a checkpoint ([RFC 0041]) — one allocation, one write,
//! then the in-memory swap that publishes the new pages.
//!
//! Every image [`crate::plan`] produced — the rewritten pages and a grown
//! dictionary — is placed inside **one contiguous window**: the allocator's
//! existing best-fit finds the smallest free extent that holds the whole
//! round (a recycled window when one exists, the file's tail otherwise), the
//! window is filled block-aligned, and it reaches the disk as a single
//! positioned write. Only afterwards are the descriptor vectors swapped — so
//! a reader either sees the previous pages (still allocated, still valid:
//! page writes are copy-on-write) or the new ones, never a half-written run.
//!
//! The window's last target is the round's own [`EditChunk`](crate::edit):
//! the descriptor changes these pages caused, riding the same write
//! ([RFC 0046]). Its length is fixed by the round's *shape*, so a reservation
//! pass over the same plans predicts it exactly; the second pass, after
//! `install`, fills in the addresses.
//!
//! Nothing on disk records that these blocks were written together: the window
//! is an allocation-time concept only, and each page keeps its own descriptor.
//! That is what lets pages be freed one by one later and the extent coalesce
//! back into a large hole.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md
//! [RFC 0046]: ../../../rfcs/0046-directory-deltas-in-the-window.md

use std::collections::BTreeMap;

use crate::block::{BLOCK_SIZE, BlockAllocator, BlockDescriptor, Run};
use crate::edit;
use crate::error::{StorageError, StorageResult};
use crate::page_store::{PageStore, Touched};
use crate::plan::{SlotPlan, blocks_of};

/// Where the window itself is allocated.
#[derive(Clone, Copy)]
enum Placement {
    /// The allocator's best fit: a recycled hole when one holds the whole
    /// round, the tail otherwise. Every ordinary settle.
    BestFit,
    /// Always fresh tail blocks — the defragmenter, which must not land back
    /// in the neighbourhood it is trying to vacate.
    Tail,
}

/// Where a placed image's descriptor is installed once the window is written.
enum Target {
    Page {
        plan: usize,
        bucket: usize,
    },
    Dict {
        plan: usize,
    },
    /// The round's addressing delta — always last, always exactly one.
    Edit,
}

impl PageStore {
    /// Settle `touched` into pages: plan every slot, then write the whole
    /// round as one window and publish it.
    ///
    /// Durability is not taken here: the journal already holds the batch, and
    /// `data.bin` only becomes authoritative at
    /// [`commit_journal`](Self::commit_journal), which syncs the pages before
    /// the `Commit` frame addresses them.
    ///
    /// # Errors
    /// Propagates page read/write, corruption, or compression faults. The
    /// caller returns the round to the pending queue; re-settling is
    /// idempotent because a plan derives from the caches' current bytes.
    pub(crate) fn settle(&self, touched: &Touched) -> StorageResult<()> {
        let mut plans = Vec::with_capacity(touched.len());
        for (idx, ids) in touched {
            plans.push(self.plan_slot(*idx, ids)?);
        }
        self.place_in(plans, Placement::BestFit)
    }

    /// Write already-serialised pages to **fresh tail blocks** and repoint
    /// their buckets — the defragmenter's move
    /// ([RFC 0042](../../../rfcs/0042-free-space-defragmentation.md)).
    ///
    /// Identical to a settle round except for where the window lands: best-fit
    /// would happily reuse the very hole the move is meant to enlarge.
    ///
    /// # Errors
    /// Propagates the window write fault; nothing is repointed if it fails.
    pub(crate) fn relocate(&self, plans: Vec<SlotPlan>) -> StorageResult<()> {
        self.place_in(plans, Placement::Tail)
    }

    /// Phases 2–4 + 6: carve one window for every planned image *and* the
    /// round's edit chunk, write it, swap the descriptors in, and release the
    /// superseded runs.
    ///
    /// # Errors
    /// The window write's fault, or [`StorageError::Corrupt`] if the filled
    /// edit chunk outgrew the space its skeleton reserved (unreachable: both
    /// passes build the same shape). Either way nothing has been written,
    /// freed, or published.
    // The allocator guard deliberately spans carve → write → free: the window
    // must not be handed out twice, and a superseded run must not return to
    // the pool before the page replacing it is on disk.
    #[allow(clippy::significant_drop_tightening)]
    fn place_in(
        &self,
        mut plans: Vec<SlotPlan>,
        placement: Placement,
    ) -> StorageResult<()> {
        let mut targets = targets_of(&plans);
        if targets.is_empty() {
            return Ok(());
        }
        let mut alloc = self.alloc.lock();
        let mut meta = self.meta.lock();

        // Pass one: the chunk's shape fixes its length, so a skeleton built
        // from the same plans reserves exactly what the filled one needs.
        let full = meta.wants_snapshot();
        // A snapshot stands alone, so it ends the chain rather than extending
        // it (RFC 0048). `prev` is known before the carve and is a fixed 8
        // bytes, so it does not disturb the shape both passes agree on.
        let prev = if full { 0 } else { meta.head() };
        let reserved = edit::encode(&edit::chunk_of(
            self,
            &plans,
            full,
            &BTreeMap::new(),
            prev,
        ));
        targets.push(Target::Edit);
        let sizes: Vec<u64> = targets
            .iter()
            .map(|t| size_of(&plans, t, &reserved))
            .collect();
        let total: u64 = sizes.iter().sum();

        let window = (total > 0).then(|| match placement {
            Placement::BestFit => alloc.alloc(total),
            Placement::Tail => alloc.alloc_tail(total),
        });
        let runs = window
            .map_or_else(|| vec![None; targets.len()], |w| carve(w, &sizes));
        let freed = install(&mut plans, &targets, &runs);

        // Pass two: the same shape, now naming the runs just handed out.
        let dicts = dict_descriptors(&plans, &targets, &runs);
        let chunk =
            edit::encode(&edit::chunk_of(self, &plans, full, &dicts, prev));
        if chunk.len() > reserved.len() {
            return Err(StorageError::Corrupt("edit chunk outgrew its window"));
        }

        if let Some(window) = window {
            let buf = assemble(window, &plans, &targets, &runs, &chunk);
            self.file.write_run(window, &buf)?; // the round's one write
        }
        for run in freed {
            alloc.free(run);
        }
        if let Some(run) = runs.last().copied().flatten() {
            let desc = BlockDescriptor::from_run_used(run, chunk.len() as u64);
            for stale in meta.record(desc, full) {
                alloc.free(stale);
            }
        }
        self.publish(plans, &dicts, &mut alloc);
        Ok(())
    }

    /// Phase 4: adopt the working directories and the new dictionary runs,
    /// and clear the tombstones the pages now satisfy.
    fn publish(
        &self,
        plans: Vec<SlotPlan>,
        dicts: &BTreeMap<usize, BlockDescriptor>,
        alloc: &mut BlockAllocator,
    ) {
        for (plan, work) in plans.iter().enumerate() {
            let Some(&desc) = dicts.get(&plan) else {
                continue;
            };
            let old = self.types[work.idx].dictionary().lock().repoint(desc);
            if old.is_allocated() {
                alloc.free(old.run());
            }
        }
        for work in plans {
            let slot = &self.types[work.idx];
            for id in work.cleared {
                slot.clear_removed(id); // the page now agrees the id is gone
            }
            *slot.directory().lock() = Some(work.dir);
        }
    }
}

/// Everything this round places, in window order (the edit chunk is appended
/// by the caller once its size is known).
fn targets_of(plans: &[SlotPlan]) -> Vec<Target> {
    let mut targets = Vec::new();
    for (plan, work) in plans.iter().enumerate() {
        for bucket in work.pages.keys() {
            targets.push(Target::Page {
                plan,
                bucket: *bucket,
            });
        }
        if work.dict.is_some() {
            targets.push(Target::Dict { plan });
        }
    }
    targets
}

/// Blocks a target occupies. An emptied page occupies none — its bucket
/// simply becomes vacant.
fn size_of(plans: &[SlotPlan], target: &Target, chunk: &[u8]) -> u64 {
    match target {
        Target::Page { plan, bucket } => {
            plans[*plan].pages.get(bucket).map_or(0, |b| blocks_of(b))
        }
        Target::Dict { plan } => {
            plans[*plan].dict.as_deref().map_or(0, blocks_of)
        }
        Target::Edit => blocks_of(chunk),
    }
}

/// Assign each target its run inside `window`, in order (`None` = no space).
fn carve(window: Run, sizes: &[u64]) -> Vec<Option<Run>> {
    let mut cursor = window.start;
    sizes
        .iter()
        .map(|&blocks| {
            (blocks > 0).then(|| {
                let run = Run::new(cursor, blocks);
                cursor += blocks;
                run
            })
        })
        .collect()
}

/// Point every planned bucket at the run it was just given, returning the
/// runs those buckets superseded.
fn install(
    plans: &mut [SlotPlan],
    targets: &[Target],
    runs: &[Option<Run>],
) -> Vec<Run> {
    let mut freed = Vec::new();
    for (target, run) in targets.iter().zip(runs) {
        let Target::Page { plan, bucket } = target else {
            continue;
        };
        let used = plans[*plan].pages.get(bucket).map_or(0, Vec::len) as u64;
        let old = plans[*plan].dir.descriptor(*bucket);
        if old.is_allocated() {
            freed.push(old.run());
        }
        let desc = run.map_or(BlockDescriptor::EMPTY, |run| {
            BlockDescriptor::from_run_used(run, used)
        });
        plans[*plan].dir.set_descriptor(*bucket, desc);
    }
    freed
}

/// The dictionary descriptor each plan whose dictionary grew was just given.
fn dict_descriptors(
    plans: &[SlotPlan],
    targets: &[Target],
    runs: &[Option<Run>],
) -> BTreeMap<usize, BlockDescriptor> {
    let mut out = BTreeMap::new();
    for (plan, work) in plans.iter().enumerate() {
        let Some(run) = dict_run(targets, runs, plan) else {
            continue;
        };
        let used = work.dict.as_ref().map_or(0, Vec::len) as u64;
        out.insert(plan, BlockDescriptor::from_run_used(run, used));
    }
    out
}

/// Fill the window: every image at its carved offset, zero-padded between.
fn assemble(
    window: Run,
    plans: &[SlotPlan],
    targets: &[Target],
    runs: &[Option<Run>],
    chunk: &[u8],
) -> Vec<u8> {
    let mut buf = vec![0u8; window.byte_len() as usize];
    for (target, run) in targets.iter().zip(runs) {
        let Some(run) = run else { continue };
        let bytes: &[u8] = match target {
            Target::Page { plan, bucket } => {
                plans[*plan].pages[bucket].as_ref()
            }
            Target::Dict { plan } => {
                plans[*plan].dict.as_deref().unwrap_or(&[])
            }
            Target::Edit => chunk,
        };
        let at = ((run.start - window.start) * BLOCK_SIZE as u64) as usize;
        buf[at..at + bytes.len()].copy_from_slice(bytes);
    }
    buf
}

/// The run `plan`'s dictionary image was placed in, if it grew.
fn dict_run(
    targets: &[Target],
    runs: &[Option<Run>],
    plan: usize,
) -> Option<Run> {
    targets
        .iter()
        .zip(runs)
        .find(|(t, _)| matches!(t, Target::Dict { plan: p } if *p == plan))
        .and_then(|(_, run)| *run)
}
