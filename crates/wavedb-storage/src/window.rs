//! The layout of one settle window ([RFC 0041]) — what it holds, where each
//! image lands inside it, and the bytes it is written as. Split from
//! [`crate::checkpoint`], which carves the window and publishes it.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md

use std::collections::BTreeMap;

use crate::block::{BLOCK_SIZE, BlockDescriptor, Run};
use crate::error::{StorageError, StorageResult};
use crate::ordered::{Found, Placed};
use crate::plan::{SlotPlan, blocks_for};
use crate::remote::HEADER_BLOCKS;
use crate::roots::ROOTS_BLOCKS;
use crate::shape::weight;

/// What a placed image becomes once the window is written.
pub enum Target {
    Page {
        plan: usize,
        page: usize,
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
        for page in 0..work.pages.len() {
            targets.push(Target::Page { plan, page });
        }
        if work.dict.is_some() {
            targets.push(Target::Dict { plan });
        }
    }
    targets
}

/// Blocks a target occupies: a page exactly what its entry can name
/// ([`canonical`](crate::ordered::PageIndex::canonical)).
pub fn size_of(plans: &[SlotPlan], target: &Target) -> u64 {
    match target {
        Target::Page { plan, page } => {
            let work = &plans[*plan];
            work.index
                .canonical(blocks_for(work.pages[*page].image.len()))
        }
        Target::Dict { plan } => plans[*plan]
            .dict
            .as_deref()
            .map_or(0, |d| blocks_for(d.len())),
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

/// Point every planned page's entry at the run it was just given, flagged
/// `flags` — or, for `None` (a move, which does not change what it moves),
/// as the page was. Returns the pending blocks the pages add.
///
/// # Errors
/// [`StorageError::Corrupt`] for a page whose entry the plan lost, or a run
/// no range can name.
pub fn install(
    plans: &mut [SlotPlan],
    targets: &[Target],
    runs: &[Option<Run>],
    flags: Option<u8>,
) -> StorageResult<u64> {
    let mut pending = 0;
    for (target, run) in targets.iter().zip(runs) {
        let (Target::Page { plan, page }, Some(run)) = (target, run) else {
            continue;
        };
        let work = &mut plans[*plan];
        let planned = &work.pages[*page];
        let meta = flags.unwrap_or(planned.flags);
        let at = match work.index.find(&planned.key) {
            Found::Page { at, .. } | Found::Hole(at) => at,
            Found::Uncovered => {
                return Err(StorageError::Corrupt(
                    "a planned page lost its entry",
                ));
            }
        };
        let placed = Placed {
            address: run.start,
            blocks: run.count,
            meta,
        };
        work.index
            .retarget(at, placed)
            .ok_or(StorageError::Corrupt("a run no range can name"))?;
        pending += weight(meta, run.count);
    }
    for work in plans.iter_mut() {
        work.index.split_long();
    }
    Ok(pending)
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
            Target::Page { plan, page } => &plans[*plan].pages[*page].image,
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
