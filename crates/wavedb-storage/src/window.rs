//! The layout of one settle window ([RFC 0041]) — what it holds, where each
//! image lands inside it, and the bytes it is written as. Split from
//! [`crate::checkpoint`], which carves the window and publishes it.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md

use std::collections::BTreeMap;

use crate::block::{BLOCK_SIZE, BlockDescriptor, Run};
use crate::error::{StorageError, StorageResult};
use crate::header_room::HeaderSize;
use crate::ordered::{Found, Placed};
use crate::plan::{SlotPlan, blocks_for};
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
    /// Small pages of several plans in one run, `image` the pack holding
    /// them (`crate::pack`): every member's entry names the run.
    Pack {
        members: Vec<(usize, usize)>,
        image: Vec<u8>,
    },
}

impl Target {
    /// The planned pages it places, as `(plan, page)`.
    #[must_use]
    pub fn pages(&self) -> Vec<(usize, usize)> {
        match self {
            Self::Page { plan, page } => vec![(*plan, *page)],
            Self::Dict { .. } => Vec::new(),
            Self::Pack { members, .. } => members.clone(),
        }
    }
}

/// A round's plans and what its window holds, between its reservations and
/// its build.
pub struct Round {
    pub(crate) plans: Vec<SlotPlan>,
    pub(crate) targets: Vec<Target>,
    /// Where each target landed in `body`.
    pub(crate) runs: Vec<Option<Run>>,
    /// The flag the pages it places take; `None` keeps theirs (a move).
    pub(crate) flags: Option<u8>,
    pub(crate) end: bool,
    /// The window's pages and dictionaries.
    pub(crate) body: Option<Run>,
    /// On disk, its header and, ending a batch of a store writing its own
    /// files, the roots block after it — reserved once the pages are
    /// placed, which is when the header's size is known (RFC 0079 §4).
    pub(crate) marks: Option<(Run, HeaderSize)>,
    /// The pending blocks its pages add.
    pub(crate) added: u64,
    pub(crate) dicts: BTreeMap<usize, BlockDescriptor>,
}

/// Everything this round places, in window order — on disk the header and
/// the roots block follow, in a run of their own (`crate::checkpoint`).
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
        Target::Pack { members, image } => {
            members.first().map_or(0, |&(plan, _)| {
                plans[plan].index.canonical(blocks_for(image.len()))
            })
        }
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
    let placed = targets.iter().zip(runs).flat_map(|(target, run)| {
        run.map(|run| target.pages().into_iter().map(move |p| (p, run)))
            .into_iter()
            .flatten()
    });
    for ((plan, page), run) in placed {
        let work = &mut plans[plan];
        let planned = &work.pages[page];
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
        let at = work
            .index
            .retarget(at, placed)
            .ok_or(StorageError::Corrupt("a run no range can name"))?;
        // A page the round built is counted afresh; one it only moved keeps
        // its count.
        if let Some(page) = &planned.decoded {
            work.index.set_count(at, page.len());
        }
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

/// Fill the window: every image at its carved offset, zero-padded between.
pub fn assemble(
    window: Run,
    plans: &[SlotPlan],
    targets: &[Target],
    runs: &[Option<Run>],
) -> Vec<u8> {
    let mut buf = vec![0u8; window.byte_len() as usize];
    for (target, run) in targets.iter().zip(runs) {
        let Some(run) = run else { continue };
        let bytes: &[u8] = match target {
            Target::Page { plan, page } => &plans[*plan].pages[*page].image,
            Target::Dict { plan } => {
                plans[*plan].dict.as_deref().unwrap_or(&[])
            }
            Target::Pack { image, .. } => image,
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
