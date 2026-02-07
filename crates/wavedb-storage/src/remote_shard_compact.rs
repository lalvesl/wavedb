//! A storage shard's object store compacted ([RFC 0073] §8, RFC 0070 §8)
//! — split from [`crate::remote_shard`], whose thread runs it.
//!
//! A file holds sections of several data shards, so it may go only once
//! none of them needs it: each data shard compacting reports what a
//! recovery of it would read — its **horizon**, the lowest file whose
//! header it reads — and the files below that it still names, its pages'
//! and its disk's roots' alike. Once every data shard has reported, every
//! file below the lowest horizon that no report names is deleted. An older
//! report stays good: below its horizon a data shard only ever stops naming
//! files — writing is always at the end — and its horizon only rises.
//!
//! The reports carry the live blocks per file too: the files below the line
//! mostly free across all of them are the ones the data shards carry their
//! pages out of, by uploads of their own, so the next round deletes them.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::collections::{BTreeMap, BTreeSet};

use crate::remote::FILE_BLOCKS;
use crate::remote_shard::RemoteShard;

/// What a data shard compacting still needs (see the module docs).
#[derive(Debug, Clone)]
pub struct Report {
    /// The lowest file a recovery of it reads.
    pub horizon: u64,
    /// The files below the horizon it names.
    pub needed: BTreeSet<u64>,
    /// Its live blocks per file.
    pub live: BTreeMap<u64, u64>,
    /// How free a file must be to be emptied, in percent.
    pub percent: u8,
}

/// What a round of compaction did, as a data shard hears it.
#[derive(Debug, Clone, Default)]
pub struct CompactAnswer {
    /// Files deleted by this round.
    pub deleted: usize,
    /// The oldest file still standing.
    pub start: u64,
    /// The files to carry pages out of.
    pub victims: BTreeSet<u64>,
}

impl RemoteShard {
    /// Take the report of the data shard at `position`, and once every one
    /// has reported, delete what none needs (see the module docs).
    pub(crate) fn compact(
        &mut self,
        position: usize,
        report: Report,
    ) -> CompactAnswer {
        let percent = report.percent;
        if self.reports.len() < self.by_shard.len() {
            self.reports.resize(self.by_shard.len(), None);
        }
        if let Some(slot) = self.reports.get_mut(position) {
            *slot = Some(report);
        }
        let reports: Vec<&Report> = self.reports.iter().flatten().collect();
        if reports.len() < self.by_shard.len() {
            return CompactAnswer {
                start: self.start,
                ..CompactAnswer::default()
            };
        }
        let line = reports.iter().map(|r| r.horizon).min().unwrap_or(0);
        let needed: BTreeSet<u64> = reports
            .iter()
            .flat_map(|r| r.needed.iter().copied())
            .collect();
        let mut totals: BTreeMap<u64, u64> = BTreeMap::new();
        for r in &reports {
            for (&file, &blocks) in &r.live {
                *totals.entry(file).or_insert(0) += blocks;
            }
        }
        let doomed: Vec<u64> = (self.start..line)
            .filter(|f| !needed.contains(f) && !self.deleted.contains(f))
            .collect();
        let mut deleted = 0;
        for file in doomed {
            let key = self.remote.file_key(file);
            let gone = self.remote.bucket().delete(&key);
            if futures::executor::block_on(gone).is_ok() {
                self.deleted.insert(file);
                deleted += 1;
            }
        }
        let oldest = needed
            .first()
            .copied()
            .map_or(line, |f| f.min(line))
            .max(self.start);
        self.start = oldest;
        self.deleted.retain(|&f| f >= oldest);
        let most = FILE_BLOCKS * (100 - u64::from(percent.min(100))) / 100;
        let victims = totals
            .into_iter()
            .filter(|&(file, blocks)| file < line && blocks < most)
            .map(|(file, _)| file)
            .collect();
        CompactAnswer {
            deleted,
            start: self.start,
            victims,
        }
    }
}
