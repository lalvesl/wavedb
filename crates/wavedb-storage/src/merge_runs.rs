//! Which neighbouring entries a merge takes (`crate::regroup_merge`): runs
//! of small pages of one range — a half page together, or a whole one
//! where every page is cold (RFC 0081 §8) — the pages a span left named
//! either side, and the holes among them. A compaction merges them, and so
//! does a batch past the index budget (`crate::consolidate`).

use std::collections::{BTreeMap, BTreeSet};

use crate::ordered::{Placed, Slot};
use crate::pages::{EVICTED, Pages};

/// A run of neighbouring entries to merge: a key finding each, and the
/// blocks of the distinct runs they name.
pub struct SmallRun {
    pub keys: Vec<Vec<u8>>,
    pub blocks: u64,
}

/// Runs of neighbouring entries in one range to merge, two or more: pages
/// together at most half of `target` — a whole one where every page was
/// written before `cold_before`, cold, so no write is about to cut it
/// again — or held together by a page named either side of a span; and
/// the holes among them, which hold nothing and go with the merge. A hole
/// still to be told to the object store, or an evicted page, parts runs.
pub fn small_runs(
    index: &Pages,
    target: u64,
    cold_before: u64,
) -> Vec<SmallRun> {
    let entries: Vec<_> = index.entries_all().collect();
    // Where each run is named last: a page named either side of a span
    // (`crate::partial`) is consolidated — its group runs on to its last
    // name, whatever its size, and the page goes with its spans.
    let mut last_name = BTreeMap::new();
    for (i, (_, page)) in entries.iter().enumerate() {
        if let Some(page) = page {
            last_name.insert(page.address, i);
        }
    }
    let mut out = Vec::new();
    let mut run = Group::default();
    let mut last: Option<Slot> = None;
    for (i, &(at, page)) in entries.iter().enumerate() {
        let beside = last
            .is_some_and(|l| l.range == at.range && l.entry + 1 == at.entry);
        last = Some(at);
        let meta =
            page.map_or_else(|| index.meta_at(at).unwrap_or(0), |p| p.meta);
        let parts = match page {
            Some(_) => meta & EVICTED != 0,
            None => meta != 0,
        };
        let named_again = page.and_then(|p| {
            last_name.get(&p.address).copied().filter(|&l| l > i)
        });
        let fits = !parts
            && beside
            && run.takes(page, named_again.is_some(), i, (target, cold_before));
        if !fits {
            run.flush(&mut out);
            if parts
                || !run.takes(
                    page,
                    named_again.is_some(),
                    i,
                    (target, cold_before),
                )
            {
                continue;
            }
        }
        run.add(page, named_again, cold_before);
        run.keys.extend(index.key_of(at));
    }
    run.flush(&mut out);
    out
}

/// The run [`small_runs`] is gathering.
#[derive(Default)]
struct Group {
    keys: Vec<Vec<u8>>,
    /// Runs on disk its pages name, each counted once.
    seen: BTreeSet<u64>,
    blocks: u64,
    /// The last entry a page named again holds it to.
    until: Option<usize>,
    /// It holds pages, every one cold.
    warm: bool,
}

impl Group {
    /// Whether entry `i`, naming `page`, joins it: a hole always; a page
    /// held to it, or named already, or within the bound — half of
    /// `target`, or a whole one if it and every page so far are cold.
    fn takes(
        &self,
        page: Option<Placed<u8>>,
        named_again: bool,
        i: usize,
        (target, cold_before): (u64, u64),
    ) -> bool {
        let Some(page) = page else { return true };
        if self.until.is_some_and(|u| i <= u)
            || self.seen.contains(&page.address)
        {
            return true;
        }
        let cold = !self.warm && page.address < cold_before;
        let most = if cold { target } else { target / 2 };
        (named_again && self.blocks == 0) || self.blocks + page.blocks <= most
    }

    fn add(
        &mut self,
        page: Option<Placed<u8>>,
        named_again: Option<usize>,
        cold_before: u64,
    ) {
        let Some(page) = page else { return };
        if self.seen.insert(page.address) {
            self.blocks += page.blocks;
        }
        self.warm |= page.address >= cold_before;
        self.until = self.until.max(named_again);
    }

    /// Hand the run over if it is two entries or more, and start afresh.
    fn flush(&mut self, out: &mut Vec<SmallRun>) {
        let run = std::mem::take(self);
        if run.keys.len() >= 2 {
            out.push(SmallRun {
                keys: run.keys,
                blocks: run.blocks,
            });
        }
    }
}
