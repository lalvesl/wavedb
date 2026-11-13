//! A census of a store's pages ([RFC 0081] phase 0): what each slot's index
//! holds and what its pages hold, for a benchmark to record beside a
//! footprint.
//!
//! Per slot: its ranges and entries — pages and holes — the distinct runs
//! they name and how many of them more than one entry names (a pack, a page
//! either side of a span), the blocks those runs take, and the records and
//! bytes the pages hold **through their entries** (`pages::clip`): live
//! bytes against blocks is how full the pages are, and against the target
//! how far from it. The index's own RAM is estimated from its parts.
//!
//! It reads every page on disk once, through the page cache: a benchmark's
//! point, not a hot path. An evicted page is counted, not read.
//!
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

use std::collections::BTreeMap;

use crate::block::BLOCK_SIZE;
use crate::error::StorageResult;
use crate::ordered::SubHeader;
use crate::page_store::PageStore;
use crate::pages::{EVICTED, clip, run_of, with_page};

/// What one slot's index and pages hold.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlotCensus {
    pub struct_hash: u64,
    /// Sub-headers.
    pub ranges: u64,
    /// Entries naming a page.
    pub pages: u64,
    /// Entries naming none: quanta no page covers.
    pub holes: u64,
    /// Entries whose page left the disk for the object store.
    pub evicted: u64,
    /// Distinct runs the pages' entries name on disk.
    pub runs: u64,
    /// Of those, named by more than one entry of this slot.
    pub shared_runs: u64,
    /// Blocks the distinct runs take.
    pub blocks: u64,
    /// Records the pages hold through their entries.
    pub records: u64,
    /// Their keys' and values' bytes, as held decoded.
    pub record_bytes: u64,
    /// The index's RAM, estimated: its sub-headers and their prefixes, and
    /// a `u32`, a flag byte and (counted) a count byte per entry.
    pub index_bytes: u64,
}

impl SlotCensus {
    /// How full the runs are: the live bytes over the bytes their blocks
    /// take, in percent (decoded bytes against stored blocks: compression
    /// can take it past 100).
    #[must_use]
    pub fn fill_percent(&self) -> u64 {
        (self.record_bytes * 100)
            .checked_div(self.blocks * BLOCK_SIZE as u64)
            .unwrap_or(0)
    }
}

impl PageStore {
    /// The census of every registered slot that holds a page index.
    ///
    /// # Errors
    /// A page that fails to read or decode.
    pub fn census(&self) -> StorageResult<Vec<SlotCensus>> {
        let mut out = Vec::new();
        for slot in &self.types {
            let index = slot.directory().borrow();
            let Some(index) = index.as_ref() else {
                continue;
            };
            let hash = slot.struct_hash();
            let mut c = SlotCensus {
                struct_hash: hash,
                ranges: index.range_count() as u64,
                ..SlotCensus::default()
            };
            let per_entry = 5 + u64::from(index.is_counted());
            c.index_bytes = index
                .sub_headers()
                .map(|h| (size_of::<SubHeader>() + h.prefix.len()) as u64)
                .sum::<u64>()
                + per_entry * index.entries() as u64;
            let mut names: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
            for (at, page) in index.entries_all() {
                let Some(page) = page else {
                    c.holes += 1;
                    continue;
                };
                c.pages += 1;
                if page.meta & EVICTED != 0 {
                    c.evicted += 1;
                    continue;
                }
                names.entry(page.address).or_insert((page.blocks, 0)).1 += 1;
                let (records, bytes) =
                    with_page(&self.file, run_of(page), hash, |p| {
                        let held = clip(p.clone(), index, at)?;
                        let bytes = held
                            .iter()
                            .map(|(k, v)| (k.len() + v.len()) as u64);
                        Ok::<_, crate::StorageError>((
                            held.len() as u64,
                            bytes.sum::<u64>(),
                        ))
                    })??;
                c.records += records;
                c.record_bytes += bytes;
            }
            c.runs = names.len() as u64;
            c.shared_runs = names.values().filter(|n| n.1 > 1).count() as u64;
            c.blocks = names.values().map(|n| n.0).sum();
            out.push(c);
        }
        Ok(out)
    }
}
