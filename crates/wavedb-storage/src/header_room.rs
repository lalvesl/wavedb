//! How long the next disk header is ([RFC 0079] §4–§5) — split from
//! [`crate::disk_header`], which writes it.
//!
//! - **What it names, in whole blocks.** A window's header is sized once
//!   its pages are placed: what every index journaled, exactly as the
//!   header will drain it, and its trailer, rounded
//!   up to whole blocks and capped at [`MAX_HEADER_BLOCKS`]. What does not
//!   fit waits for the next header, as it always did. The rest of the last
//!   block is spare room: the rotation restates into it for free.
//! - **A restating header when the walk grows.** A reconstruction rooted
//!   at the newest header reads every header down to its horizon — the
//!   walk. Small headers restate little, so the walk would grow; once it
//!   reaches [`WALK_BOUND`] headers, the next header takes room for a pass
//!   over the indexes, up to 128 KiB, which moves the horizon in one jump —
//!   less when a whole pass takes less: padding it would restate nothing. It
//!   goes sooner, from half the bound, in a round writing a lot anyway, and
//!   at least every [`RESTATE_AFTER`] in a store writing little. Restating
//!   headers are spaced by half the bound, so an index whose one pass takes
//!   many of them costs a restating header every half bound, not every
//!   window.
//!
//! [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md

use std::cell::Ref;
use std::time::{Duration, Instant};

use crate::block::BLOCK_SIZE;
use crate::disk_chain::DiskChain;
use crate::header::{ENTRY_BYTES, Header, HeaderLink};
use crate::page_store::PageStore;
use crate::pages::Pages;
use crate::plan::SlotPlan;
use crate::restate::{TypeView, horizon};
use crate::trailer::TRAILER_BYTES;

/// The most blocks a header takes (128 KiB): a round naming more chains
/// another header.
pub const MAX_HEADER_BLOCKS: u16 = 32;

/// The walk a reconstruction makes, in headers, past which the next header
/// restates (`K`).
pub const WALK_BOUND: usize = 64;

/// Bytes a restating header spends restating, at most.
const MAX_RESTATE: usize = 128 * 1024;

/// Blocks of pages a round writes that make it large: a restating header
/// rides it from half the bound on.
const RIDE_BLOCKS: u64 = 256;

/// The longest a store goes without a restating header while its walk has
/// more than its root.
pub const RESTATE_AFTER: Duration = Duration::from_secs(300);

/// Which indexes a header entry is of.
#[derive(Clone, Copy)]
pub enum Side {
    Disk,
    Remote,
}

/// The next header's length, and whether it restates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderSize {
    pub blocks: u16,
    pub restating: bool,
}

impl HeaderSize {
    /// The bytes a header of this size holds before its trailer.
    pub const fn room(self) -> usize {
        self.blocks as usize * BLOCK_SIZE - TRAILER_BYTES
    }
}

impl PageStore {
    /// The length of the header of a round whose pages, `body` blocks of
    /// them, are placed in `plans` (see the module docs).
    pub(crate) fn header_size(
        &self,
        plans: &[SlotPlan],
        body: u64,
    ) -> HeaderSize {
        let bare = Header {
            at: 0,
            blocks: 0,
            prev: HeaderLink::NONE,
            horizon: 0,
            shard: 0,
            roots: 0,
            end_of_batch: false,
            types: Vec::new(),
            remote: Vec::new(),
            chain: self.object.as_ref().map(|o| o.chain.borrow().record()),
        };
        let mut need = bare.encoded_len() + TRAILER_BYTES;
        for idx in 0..self.types.len() {
            need += self.unrecorded_len(idx, Side::Disk, plans);
            need += self.unrecorded_len(idx, Side::Remote, plans);
        }
        let restating = self.restate_due(body);
        if restating {
            need += self.restate_len(plans);
        }
        let blocks = need.div_ceil(BLOCK_SIZE).max(1);
        HeaderSize {
            blocks: u16::try_from(blocks)
                .unwrap_or(MAX_HEADER_BLOCKS)
                .min(MAX_HEADER_BLOCKS),
            restating,
        }
    }

    /// Bytes slot `idx`'s `side` takes in a header naming all it has
    /// unrecorded: an entry, and its journal drained (`record`).
    fn unrecorded_len(
        &self,
        idx: usize,
        side: Side,
        plans: &[SlotPlan],
    ) -> usize {
        let slot = &self.types[idx];
        let journaled = match side {
            Side::Disk => plans.iter().find(|p| p.idx == idx).map_or_else(
                || pending(slot.directory().borrow().as_ref()),
                |plan| pending(Some(&plan.index)),
            ),
            Side::Remote => {
                let copy = slot.remote_journal().borrow();
                (!copy.is_empty()).then(|| {
                    slot.remote_directory()
                        .borrow()
                        .as_ref()
                        .map_or(0, |index| index.pending_len_from(&copy))
                })
            }
        };
        journaled.map_or(0, |bytes| ENTRY_BYTES + bytes)
    }

    /// Whether the next header is a restating one (see the module docs).
    fn restate_due(&self, body: u64) -> bool {
        let chain = self.chain.borrow();
        // The walk from the next header: those above the horizon, and it.
        let walk = chain.live.range(chain.horizon..).count() + 1;
        let stale = chain
            .restated_at
            .is_some_and(|at| at.elapsed() >= RESTATE_AFTER);
        restates(walk, chain.since_restate, body, stale)
    }

    /// Bytes a restating header spends: a pass over every index, whole, up
    /// to its ceiling.
    fn restate_len(&self, plans: &[SlotPlan]) -> usize {
        let disk: Vec<_> =
            self.types.iter().map(|s| s.directory().borrow()).collect();
        let remote: Vec<_> = self
            .types
            .iter()
            .map(|s| s.remote_directory().borrow())
            .collect();
        let views = self.views_of(&disk, plans);
        let remote_views = self.remote_views_of(&remote);
        pass_len(views.iter().chain(&remote_views).filter_map(|v| v.index))
    }

    /// Every type's disk index as the window leaves it.
    pub(crate) fn views_of<'a>(
        &self,
        disk: &'a [Ref<'_, Option<Pages>>],
        plans: &'a [SlotPlan],
    ) -> Vec<TypeView<'a>> {
        self.types
            .iter()
            .enumerate()
            .map(|(idx, slot)| {
                let plan = plans.iter().find(|p| p.idx == idx);
                let index =
                    plan.map_or_else(|| disk[idx].as_ref(), |p| Some(&p.index));
                TypeView {
                    struct_hash: slot.struct_hash(),
                    index,
                }
            })
            .collect()
    }

    /// Every type's remote index, none in a disk-only store.
    pub(crate) fn remote_views_of<'a>(
        &self,
        remote: &'a [Ref<'_, Option<Pages>>],
    ) -> Vec<TypeView<'a>> {
        if self.object.is_none() {
            return Vec::new();
        }
        self.types
            .iter()
            .enumerate()
            .map(|(idx, slot)| TypeView {
                struct_hash: slot.struct_hash(),
                index: remote[idx].as_ref(),
            })
            .collect()
    }
}

/// Bytes a pass over `indexes`, every range whole, takes, up to a
/// restating header's ceiling.
pub fn pass_len<'a>(indexes: impl Iterator<Item = &'a Pages>) -> usize {
    let mut pass = 0;
    for index in indexes {
        pass += ENTRY_BYTES;
        for range in 0..index.range_count() {
            if pass >= MAX_RESTATE {
                return MAX_RESTATE;
            }
            pass += index.whole_record(range).1.wire_len();
        }
    }
    pass.min(MAX_RESTATE)
}

/// Whether a header from which a reconstruction walks `walk` headers,
/// `since` headers after the last restating one, in a round of `body`
/// blocks of pages, restates — `stale` once [`RESTATE_AFTER`] has passed
/// since one did (see the module docs).
pub const fn restates(
    walk: usize,
    since: usize,
    body: u64,
    stale: bool,
) -> bool {
    let spaced = since >= WALK_BOUND / 2;
    (spaced && walk >= WALK_BOUND)
        || (spaced && walk >= WALK_BOUND / 2 && body >= RIDE_BLOCKS)
        || (stale && walk > 1)
}

/// Bytes `index`'s journal takes drained, if it journaled anything.
fn pending(index: Option<&Pages>) -> Option<usize> {
    index
        .filter(|i| i.has_unrecorded())
        .map(|i| i.pending_len_from(i.journal()))
}

impl DiskChain {
    /// A header was written with this `horizon`, restating or not.
    pub(crate) fn wrote(&mut self, horizon: u64, restating: bool) {
        self.horizon = horizon;
        if restating {
            self.since_restate = 0;
            self.restated_at = Some(Instant::now());
        } else {
            self.since_restate += 1;
            self.restated_at.get_or_insert_with(Instant::now);
        }
    }
}

/// The lowest header a reconstruction from the newest one reads
/// ([`Header::horizon`]): where the rotations of the disk's indexes — and
/// in disk + object mode the object store's — last came round, or the last
/// full restatement, whichever is higher.
pub fn coverage(
    views: &[TypeView<'_>],
    remote_views: &[TypeView<'_>],
    chain: &DiskChain,
    object_backed: bool,
) -> u64 {
    let mut line = horizon(views, &chain.types, chain.floor);
    if object_backed {
        line = line.min(horizon(remote_views, &chain.remote, chain.floor));
    }
    line.max(chain.restated.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::{RIDE_BLOCKS, WALK_BOUND, restates};

    /// The walk's bound makes a header restate; a large round takes it from
    /// half the bound; restating headers are spaced by half the bound.
    #[test]
    fn a_header_restates_on_the_walks_length() {
        let (k, half) = (WALK_BOUND, WALK_BOUND / 2);
        assert!(!restates(k - 1, half, 1, false), "under the bound");
        assert!(restates(k, half, 1, false), "at the bound");
        assert!(!restates(k, half - 1, 1, false), "too soon after one");
        assert!(!restates(half, half, RIDE_BLOCKS - 1, false), "small round");
        assert!(
            restates(half, half, RIDE_BLOCKS, false),
            "rides a large one"
        );
        assert!(!restates(half - 1, half, RIDE_BLOCKS, false));
    }

    /// A store writing little restates once enough time has passed, unless
    /// its walk is its root alone.
    #[test]
    fn a_stale_walk_restates_however_short() {
        assert!(restates(2, 0, 1, true));
        assert!(!restates(1, 0, 1, true), "nothing to walk");
        assert!(!restates(2, 0, 1, false));
    }
}
