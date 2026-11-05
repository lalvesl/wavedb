//! Which of a round's pages share a run ([RFC 0081] §1) — split from
//! [`crate::window`], which lays the window out.
//!
//! A round touching a few lanes writes a small page in each — a fragment
//! of a lane's appends, a short page — and each took a block of its own. Of
//! the pages that fit one block, the smallest of each slot's (a slot has at
//! most one section in a pack) go into one **pack**, as many as take fewer
//! blocks together than apart. A settle's round packs; a move carries what
//! it moves as it was, and a page brought back is one page.
//!
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

use std::collections::{BTreeMap, BTreeSet};

use crate::block::{BLOCK_SIZE, Run};
use crate::pack;
use crate::page_cache::Lane;
use crate::page_store::PageStore;
use crate::plan::{SlotPlan, blocks_for};
use crate::window::Target;

/// `targets` with the round's small pages packed (see the module docs);
/// `hash_of` names a plan's slot.
#[must_use]
pub fn packed(
    plans: &[SlotPlan],
    targets: Vec<Target>,
    hash_of: impl Fn(usize) -> u64,
) -> Vec<Target> {
    // Each plan's smallest page within a block: its target and its bytes.
    let mut smallest: BTreeMap<usize, (usize, usize)> = BTreeMap::new();
    for (i, target) in targets.iter().enumerate() {
        let Target::Page { plan, page } = *target else {
            continue;
        };
        let len = plans[plan].pages[page].image.len();
        if len <= BLOCK_SIZE && smallest.get(&plan).is_none_or(|s| len < s.1) {
            smallest.insert(plan, (i, len));
        }
    }
    let mut small: Vec<(usize, usize)> = smallest.into_values().collect();
    small.sort_by_key(|&(_, len)| len);
    // As many of them, smallest first, as take fewer blocks packed.
    let (mut take, mut bytes) = (0, 0);
    for (n, &(_, len)) in small.iter().enumerate() {
        bytes += len;
        if blocks_for(pack::encoded_len(n + 1, bytes)) < (n + 1) as u64 {
            take = n + 1;
        }
    }
    if take < 2 {
        return targets;
    }
    let packed: BTreeSet<usize> =
        small[..take].iter().map(|&(i, _)| i).collect();
    let (mut out, mut members, mut sections) =
        (Vec::new(), Vec::new(), Vec::new());
    for (i, target) in targets.into_iter().enumerate() {
        match target {
            Target::Page { plan, page } if packed.contains(&i) => {
                members.push((plan, page));
                sections.push((
                    hash_of(plan),
                    plans[plan].pages[page].image.clone(),
                ));
            }
            other => out.push(other),
        }
    }
    out.push(Target::Pack {
        members,
        image: pack::encode(sections),
    });
    out
}

impl PageStore {
    /// The round's pages just written are the ones read next: held decoded
    /// under their new runs (RFC 0044) — after the write, which forgot the
    /// window — and a pack named once per section (`crate::shares`).
    pub(crate) fn hold_written(
        &self,
        plans: &mut [SlotPlan],
        targets: &[Target],
        runs: &[Option<Run>],
    ) {
        for (target, run) in targets.iter().zip(runs) {
            let Some(run) = run else { continue };
            let members = target.pages();
            if members.len() > 1 {
                let names = u32::try_from(members.len()).unwrap_or(u32::MAX);
                self.shares.borrow_mut().share(run.start, names);
            }
            for (plan, page) in members {
                let hash = self.types[plans[plan].idx].struct_hash();
                if let Some(decoded) = plans[plan].pages[page].decoded.take() {
                    self.file.page_cache().insert(
                        Lane::Disk,
                        *run,
                        hash,
                        decoded,
                    );
                }
            }
        }
    }
}
