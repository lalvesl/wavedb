//! Packing an upload ([RFC 0079] §3) — split from [`crate::upload`], which
//! ships it: its files laid out, reserved through a storage shard, and the
//! object store's indexes pointed at them, the headers sized to what they
//! will name. A header's size is only known once the pages are placed, and
//! the upload is whole in RAM until its PUT, so the layout is redone with
//! the rooms the headers ask for until they fit.
//!
//! [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md

use std::collections::BTreeMap;

use crate::block::BLOCK_SIZE;
use crate::error::StorageResult;
use crate::header::{Chain, NO_FILE};
use crate::page_store::PageStore;
use crate::roots::ROOTS_BLOCKS;
use crate::upload::install_remote;
use crate::upload_file::{Named, Packed, SlotImages, layout, rebase};
use crate::upload_header::{need, restating, rooms as rooms_for};
use crate::upload_plan::SlotUpload;

/// Layouts an upload tries before its headers fit: past them, what does not
/// fit waits for the next upload.
const SIZE_TRIES: usize = 3;

/// An upload's files, laid out and placed: the files, the chain before
/// their headers, and whether the first
/// header restates.
pub type Packing = (Packed, Chain, bool);

impl PageStore {
    /// Lay the upload's `slots` out in files, reserve them through a
    /// storage shard, and point `work`'s remote indexes at them, the
    /// headers sized to what they will name (RFC 0079 §3): laid out with
    /// the rooms the last try asked for, until they fit. The files, the
    /// chain as they leave it before their headers, and whether the first
    /// header restates.
    ///
    /// # Errors
    /// A page whose entry the work lost; a remote thread gone.
    pub(crate) async fn pack(
        &self,
        work: &mut [SlotUpload],
        slots: &[SlotImages],
        cut: bool,
    ) -> StorageResult<Packing> {
        let object = self.object()?;
        let link = object.link.as_ref();
        let end = link.map_or_else(|| object.chain.borrow().end, |_| 0);
        let body = slots
            .iter()
            .flat_map(|s| &s.pages)
            .map(|&(_, blocks)| blocks)
            .sum();
        // The headers are sized once the pages are placed: laid out with
        // the rooms the last try asked for, until they fit (RFC 0079 §3).
        let mut rooms: Vec<u64> = Vec::new();
        let mut tries = 0;
        loop {
            let mut packed = layout(end, slots, &rooms);
            packed.cut = cut;
            let mut chain = object.chain.borrow().clone();
            if let Some(link) = link {
                let sizes = packed
                    .files
                    .iter()
                    .map(|f| {
                        f.bytes.len().div_ceil(BLOCK_SIZE) as u64 - ROOTS_BLOCKS
                    })
                    .collect();
                let reserved = link.reserve(sizes).await?;
                rebase(&mut packed, &reserved.bases);
                chain.roots =
                    (reserved.roots != NO_FILE).then_some(reserved.roots);
                chain.start = chain.start.max(reserved.start);
                chain.end = reserved.end;
            }
            install_remote(work, &packed)?;
            let (want, restating) = self.with_named(work, |named| {
                let restate = restating(&chain, named, body);
                let bytes = need(named) + restate.unwrap_or(0);
                (rooms_for(bytes, packed.files.len()), restate.is_some())
            });
            let fits =
                packed.files.iter().zip(&want).all(|(f, &w)| w <= f.header);
            tries += 1;
            if fits || tries == SIZE_TRIES {
                return Ok((packed, chain, restating));
            }
            if let Some(link) = link {
                link.abandon(packed.files.iter().map(|f| f.base).collect());
            }
            let had = packed.files.iter().map(|f| f.header);
            rooms = want
                .iter()
                .zip(had.chain(std::iter::repeat(1)))
                .map(|(&w, h)| w.max(h))
                .collect();
        }
    }

    /// Run `f` over every registered type's index in the object store as
    /// this upload leaves it — the work's, the others as they stand, a type
    /// never uploaded with none — as the headers name them: what sizes the
    /// headers, and what writes them ([`finish`]).
    pub(crate) fn with_named<R>(
        &self,
        work: &mut [SlotUpload],
        f: impl FnOnce(&mut [Named<'_>]) -> R,
    ) -> R {
        let mut guards: Vec<_> = self
            .types
            .iter()
            .map(|slot| slot.remote_directory().borrow_mut())
            .collect();
        let mut by_idx: BTreeMap<usize, &mut SlotUpload> =
            work.iter_mut().map(|w| (w.idx, w)).collect();
        let mut named = Vec::with_capacity(self.types.len());
        for ((idx, slot), guard) in
            self.types.iter().enumerate().zip(&mut guards)
        {
            let index = match by_idx.remove(&idx) {
                Some(w) => Some(&mut w.remote),
                None => guard.as_mut(),
            };
            named.push(Named {
                struct_hash: slot.struct_hash(),
                index,
            });
        }
        f(&mut named)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::ordered::{Cut, CutOutcome, Found, Journal, Placed};
    use crate::pages::remote_pages;
    use crate::shape::{PlannedPage, Shaped};
    use crate::upload::install_remote;
    use crate::upload_file::{Placed as Landed, SlotImages, layout};
    use crate::upload_plan::SlotUpload;

    fn key(i: u64) -> Vec<u8> {
        (i << 56).to_be_bytes().to_vec()
    }

    /// A layout redone with larger header rooms moves every page; placed
    /// again over the first placement, each entry names its new run.
    #[test]
    fn a_second_layout_repoints_every_page() {
        let n = 20;
        let mut remote = remote_pages(false);
        let blocks = remote.canonical(1);
        let page = |i: u64| Placed {
            address: 100_000 + i * blocks,
            blocks,
            meta: 0,
        };
        remote.place(&key(0), page(0)).unwrap();
        for i in 1..n {
            let Found::Page { at, .. } = remote.find(&key(i - 1)) else {
                panic!("the page before {i}");
            };
            let (left, right) = (key(i - 1), key(i));
            let cut = Cut {
                first: &left,
                left_last: &left,
                right_first: &right,
                last: &right,
            };
            let outcome = remote.cut(at, &cut, page(i));
            assert!(matches!(outcome, CutOutcome::Cut(_)), "page {i}");
        }
        let planned = (0..n)
            .map(|i| PlannedPage {
                key: key(i),
                image: vec![7; 10],
                decoded: None,
                flags: 0,
            })
            .collect();
        let mut work = [SlotUpload {
            idx: 0,
            flagged: Vec::new(),
            remote,
            fetched: BTreeMap::new(),
            moved: Vec::new(),
            shaped: Shaped {
                pages: planned,
                ..Shaped::default()
            },
            changes: Journal::new(),
        }];
        let slots = [SlotImages {
            pages: vec![(vec![7; 10], blocks); n as usize],
        }];
        let first = layout(0, &slots, &[]);
        install_remote(&mut work, &first).unwrap();
        let second = layout(0, &slots, &[5]);
        install_remote(&mut work, &second).unwrap();
        for placed in &second.placed {
            let Landed { page, run, .. } = *placed;
            let Found::Page { page: entry, .. } =
                work[0].remote.find(&key(page as u64))
            else {
                panic!("page {page} lost its entry");
            };
            assert_eq!((entry.address, entry.blocks), (run.start, run.count));
        }
        assert!(work[0].remote.has_unrecorded(), "a header names it all");
    }
}
