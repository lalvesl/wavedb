//! The layout of one settle window ([RFC 0041]) — what it holds, where each
//! image lands inside it, and the bytes it is written as. Split from
//! [`crate::checkpoint`], which carves the window and publishes it.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md

use std::collections::BTreeMap;

use crate::block::{BLOCK_SIZE, BlockDescriptor, Run};
use crate::plan::{SlotPlan, blocks_of};
use crate::remote::{HEADER_BLOCKS, ObjectState};
use crate::roots::ROOTS_BLOCKS;

/// Where a placed image's descriptor is installed once the window is written.
pub enum Target {
    Page {
        plan: usize,
        bucket: usize,
    },
    Dict {
        plan: usize,
    },
    /// The window's header — last on disk but for the roots.
    Header,
    /// The roots block (RFC 0073 §5), after the header of a window that
    /// asks to end its batch.
    Roots,
}

/// Everything this round places, in window order (the header is appended by
/// the caller on disk).
pub fn targets_of(plans: &[SlotPlan]) -> Vec<Target> {
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
pub fn size_of(plans: &[SlotPlan], target: &Target) -> u64 {
    match target {
        Target::Page { plan, bucket } => {
            plans[*plan].pages.get(bucket).map_or(0, |b| blocks_of(b))
        }
        Target::Dict { plan } => {
            plans[*plan].dict.as_deref().map_or(0, blocks_of)
        }
        Target::Header => HEADER_BLOCKS,
        Target::Roots => ROOTS_BLOCKS,
    }
}

/// Assign each target its run inside `window`, in order (`None` = no space).
pub fn carve(window: Run, sizes: &[u64]) -> Vec<Option<Run>> {
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

/// Point every planned bucket at the run it was just given, returning each
/// bucket's `(old, new)` descriptor.
///
/// `pending` is the new pages' "not uploaded yet" flag (RFC 0070 §1): `Some`
/// sets it — a settle round, which writes what the object store has not
/// seen — and `None` keeps the old page's — compaction, which moves bytes
/// without changing them.
pub fn install(
    plans: &mut [SlotPlan],
    targets: &[Target],
    runs: &[Option<Run>],
    pending: Option<bool>,
) -> Vec<(BlockDescriptor, BlockDescriptor)> {
    let mut changed = Vec::new();
    for (target, run) in targets.iter().zip(runs) {
        let Target::Page { plan, bucket } = target else {
            continue;
        };
        let old = plans[*plan].dir.descriptor(*bucket);
        let desc = run
            .map_or(BlockDescriptor::EMPTY, BlockDescriptor::from_run)
            .with_pending_upload(
                pending.unwrap_or_else(|| old.pending_upload()),
            );
        plans[*plan].dir.set_descriptor(*bucket, desc);
        changed.push((old, desc));
    }
    changed
}

/// Account for the descriptors a written window replaced: the runs no
/// directory names any more, and the pending-upload tally.
pub fn superseded(
    changed: &[(BlockDescriptor, BlockDescriptor)],
    object: Option<&ObjectState>,
) -> Vec<Run> {
    if let Some(object) = object {
        for &(old, new) in changed {
            object.retally(old, new);
        }
    }
    changed
        .iter()
        .filter(|(old, _)| old.is_allocated())
        .map(|(old, _)| old.run())
        .collect()
}

/// The dictionary descriptor each plan whose dictionary grew was just given.
pub fn dict_descriptors(
    plans: &[SlotPlan],
    targets: &[Target],
    runs: &[Option<Run>],
) -> BTreeMap<usize, BlockDescriptor> {
    let mut out = BTreeMap::new();
    for plan in 0..plans.len() {
        let Some(run) = dict_run(targets, runs, plan) else {
            continue;
        };
        out.insert(plan, BlockDescriptor::from_run(run));
    }
    out
}

/// The run a window's header or roots block was carved, if it has one.
pub fn mark_run(
    targets: &[Target],
    runs: &[Option<Run>],
    roots: bool,
) -> Option<Run> {
    targets
        .iter()
        .zip(runs)
        .find(|(t, _)| {
            if roots {
                matches!(t, Target::Roots)
            } else {
                matches!(t, Target::Header)
            }
        })
        .and_then(|(_, run)| *run)
}

/// Fill the window: every image at its carved offset, zero-padded between.
pub fn assemble(
    window: Run,
    plans: &[SlotPlan],
    targets: &[Target],
    runs: &[Option<Run>],
    (header, roots): (&[u8], &[u8]),
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
            Target::Header => header,
            Target::Roots => roots,
        };
        let at = ((run.start - window.start) * BLOCK_SIZE as u64) as usize;
        buf[at..at + bytes.len()].copy_from_slice(bytes);
    }
    buf
}

/// The run `plan`'s dictionary image was placed in, if it grew.
pub fn dict_run(
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
