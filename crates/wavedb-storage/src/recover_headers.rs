//! The headers a recovery reads ([RFC 0070] §6) — fetched a batch at a
//! time, newest first, and the check that an upload is whole. Split from
//! [`crate::recover`], which walks them.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::collections::BTreeMap;

use futures::future::try_join_all;

use crate::error::StorageResult;
use crate::header::{Header, NO_FILE};
use crate::remote::{HEADER_BYTES, Remote};

/// Headers fetched per request round, newest first.
pub const BATCH: u64 = 32;

/// The headers read so far, by file number: `None` for a number with no
/// file, or whose object is not this file's header.
pub struct Headers<'a> {
    remote: &'a Remote,
    seen: BTreeMap<u64, Option<Header>>,
    /// The lowest number asked for — not merely fetched along with one.
    lowest: u64,
}

impl<'a> Headers<'a> {
    pub const fn new(remote: &'a Remote) -> Self {
        Self {
            remote,
            seen: BTreeMap::new(),
            lowest: u64::MAX,
        }
    }

    /// File `file`'s header, reading it and the [`BATCH`] − 1 numbers below
    /// it first if it was not read yet.
    pub async fn get(&mut self, file: u64) -> StorageResult<Option<Header>> {
        self.lowest = self.lowest.min(file);
        if !self.seen.contains_key(&file) {
            let from = file.saturating_sub(BATCH - 1);
            let remote = self.remote;
            let wanted = (from..=file).filter(|n| !self.seen.contains_key(n));
            let read = try_join_all(wanted.map(|n| async move {
                let key = remote.file_key(n);
                let bytes = remote
                    .bucket()
                    .get(&key, Some(0..HEADER_BYTES as u64))
                    .await?;
                let header = bytes
                    .and_then(|b| Header::decode(&b).ok())
                    .filter(|h| h.at == n);
                StorageResult::Ok((n, header))
            }))
            .await?;
            self.seen.extend(read);
        }
        Ok(self.seen.get(&file).cloned().flatten())
    }

    /// The lowest file whose header was asked for: what a recovery reads
    /// stops there, so the files below it are never read (RFC 0070 §8).
    pub const fn lowest_asked(&self) -> u64 {
        self.lowest
    }

    /// Whether the batch `top` ends is whole: every header back to the one
    /// ending the batch before — or to the first — present, each linking to
    /// a lower number than its own.
    pub async fn whole(&mut self, top: &Header) -> StorageResult<bool> {
        let (mut at, mut prev) = (top.at, top.prev);
        while prev != NO_FILE {
            if prev >= at {
                return Ok(false);
            }
            let Some(header) = self.get(prev).await? else {
                return Ok(false);
            };
            if header.end_of_batch {
                return Ok(true);
            }
            (at, prev) = (header.at, header.prev);
        }
        Ok(true)
    }
}
