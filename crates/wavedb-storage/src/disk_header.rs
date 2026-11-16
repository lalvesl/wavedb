//! The header that ends every settle window on disk ([RFC 0072] §3–§5,
//! §8): the ranges the window changed ([RFC 0078] §6), the spare room
//! restating old ones, and which headers a reconstruction can no longer
//! reach.
//!
//! What a window changes is **unrecorded** until a header names it: every
//! index journals its own changes — the pages a window placed, emptied,
//! evicted, or whose flag an upload cleared. A header is sized to name all of it, in whole blocks
//! up to a cap ([RFC 0079], `crate::header_room`); what does not fit waits
//! for the next header, and only a header that leaves nothing unrecorded
//! may end a batch. Its last block ends with a trailer saying how long it
//! is (`crate::trailer`).
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md
//! [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md

use crate::error::{StorageError, StorageResult};
use crate::header::{ENTRY_BYTES, Header, HeaderLink, NO_FILE, TypeEntry};
use crate::header_room::{HeaderSize, Side, coverage};
use crate::page_store::PageStore;
use crate::plan::SlotPlan;
use crate::restate::restate;
use crate::trailer;

impl PageStore {
    /// The header at block `at`, `size` long, and whether it ends
    /// the batch: it does when `end` asks and it left nothing unrecorded.
    /// `plans` are the window's, not yet published: their indexes are what
    /// it names.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] when the header outgrew its room
    /// (unreachable: every entry and record is budgeted).
    pub(crate) fn header_at(
        &self,
        (at, size): (u64, HeaderSize),
        plans: &mut [SlotPlan],
        end: bool,
    ) -> StorageResult<(Vec<u8>, bool)> {
        let mut header = {
            let mut chain = self.chain.borrow_mut();
            if chain.restating && chain.restate_from.is_none() {
                chain.restate_from = Some(at);
            }
            Header {
                at,
                blocks: size.blocks,
                prev: chain.tail.unwrap_or(HeaderLink::NONE),
                horizon: 0,
                shard: self.position,
                roots: chain.roots.newest.unwrap_or(NO_FILE),
                end_of_batch: false,
                types: Vec::new(),
                remote: Vec::new(),
                chain: self.object.as_ref().map(|o| o.chain.borrow().record()),
            }
        };
        let link = HeaderLink::new(at, size.blocks);
        let mut room = size.room().saturating_sub(header.encoded_len());
        for idx in 0..self.types.len() {
            for side in [Side::Disk, Side::Remote] {
                self.record(idx, side, plans, &mut header, &mut room);
            }
        }
        let unrecorded = self.unrecorded_after(plans);
        header.end_of_batch = end && !unrecorded;
        let disk: Vec<_> =
            self.types.iter().map(|s| s.directory().borrow()).collect();
        let remote: Vec<_> = self
            .types
            .iter()
            .map(|s| s.remote_directory().borrow())
            .collect();
        let views = self.views_of(&disk, plans);
        let remote_views = self.remote_views_of(&remote);
        let mut chain = self.chain.borrow_mut();
        if !unrecorded
            && chain.restating
            && let Some(from) = chain.restate_from.take()
        {
            chain.restated = Some(from);
            chain.restating = false;
        }
        if self.object.is_some() && self.has_disk() {
            let half = room / 2;
            let left = restate(
                &mut header.remote,
                half,
                &remote_views,
                &mut chain.remote,
                at,
            );
            room = room - half + left;
        }
        restate(&mut header.types, room, &views, &mut chain.types, at);
        for e in &header.types {
            chain.types.entry(e.struct_hash).or_default().last = link;
        }
        for e in &header.remote {
            chain.remote.entry(e.struct_hash).or_default().last = link;
        }
        header.horizon =
            coverage(&views, &remote_views, &chain, self.object.is_some());
        chain.tail = Some(link);
        chain.live.insert(at, link.blocks);
        chain.wrote(header.horizon, size.restating);
        let mut bytes = header.encode();
        if bytes.len() > size.room() {
            return Err(StorageError::Corrupt("a header outgrew its room"));
        }
        bytes.resize(size.bytes(), 0);
        // An offset is within a block, so a `u16`.
        let offset = u16::try_from(size.offset).unwrap_or(0);
        trailer::close_after(&mut bytes, size.blocks, offset);
        Ok((bytes, header.end_of_batch))
    }

    /// Name as much of slot `idx`'s `side` as `room` holds: its index's
    /// journal drained.
    fn record(
        &self,
        idx: usize,
        side: Side,
        plans: &mut [SlotPlan],
        header: &mut Header,
        room: &mut usize,
    ) {
        let slot = &self.types[idx];
        let last = {
            let chain = self.chain.borrow();
            let rotations = match side {
                Side::Disk => &chain.types,
                Side::Remote => &chain.remote,
            };
            rotations
                .get(&slot.struct_hash())
                .map_or(HeaderLink::NONE, |r| r.last)
        };
        let (cell, entries) = match side {
            Side::Disk => (slot.directory(), &mut header.types),
            Side::Remote => (slot.remote_directory(), &mut header.remote),
        };
        let plan = matches!(side, Side::Disk)
            .then(|| plans.iter_mut().find(|p| p.idx == idx))
            .flatten();
        let mut live = cell.borrow_mut();
        let index = match plan {
            Some(plan) => Some(&mut plan.index),
            None => live.as_mut(),
        };
        // The object store's index journals for its own headers; the disk's
        // copy of its changes is the slot's.
        let mut copy = slot.remote_journal().borrow_mut();
        let journaled = match side {
            Side::Disk => index.as_ref().is_some_and(|i| i.has_unrecorded()),
            Side::Remote => !copy.is_empty(),
        };
        if !journaled || *room < ENTRY_BYTES {
            return;
        }
        *room -= ENTRY_BYTES;
        let (changed, ranges) = index.map_or((Vec::new(), 0), |index| {
            let (records, used) = match side {
                Side::Disk => index.drain(*room),
                Side::Remote => index.drain_from(&mut copy, *room),
            };
            *room -= used;
            (records, index.range_count() as u32)
        });
        entries.push(TypeEntry {
            struct_hash: slot.struct_hash(),
            ranges,
            last,
            changed,
        });
    }

    /// The batch just ended at header `at`: every header below the horizon
    /// — what a reconstruction from it still reads — is dead, and its run
    /// freed once this batch is durable.
    pub(crate) fn drop_dead_headers(&self, at: HeaderLink) {
        let disk: Vec<_> =
            self.types.iter().map(|s| s.directory().borrow()).collect();
        let remote: Vec<_> = self
            .types
            .iter()
            .map(|s| s.remote_directory().borrow())
            .collect();
        let views = self.views_of(&disk, &[]);
        let remote_views = self.remote_views_of(&remote);
        let mut chain = self.chain.borrow_mut();
        let mut line =
            coverage(&views, &remote_views, &chain, self.object.is_some());
        // A reconstruction checks the batch it roots in back to the header
        // ending the batch before.
        if let Some(before) = chain.last_end {
            line = line.min(before.at);
        }
        chain.last_end = Some(at);
        let dead: Vec<HeaderLink> = chain
            .live
            .range(..line)
            .map(|(&at, &blocks)| HeaderLink::new(at, blocks))
            .collect();
        for header in dead {
            chain.live.remove(&header.at);
            // A header sharing its block with a page goes with the last of
            // their names (`crate::shares`).
            if self.shares.borrow_mut().release(header.at) {
                self.retire_run(header.run());
            }
        }
    }
}
