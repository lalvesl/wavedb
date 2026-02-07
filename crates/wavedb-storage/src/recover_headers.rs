//! The headers and roots blocks a recovery reads ([RFC 0070] §6,
//! [RFC 0073] §8) — each by a ranged GET of the file it lies in — and the
//! check that a batch is whole. Split from [`crate::recover`], which walks
//! them.
//!
//! A header is named by the block it starts at, as on disk: its file is
//! that block's, and its bytes the 32 KiB from there. Every file opens with
//! a roots block, at its first block.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::collections::BTreeMap;

use crate::block::BLOCK_SIZE;
use crate::error::StorageResult;
use crate::header::{Header, NO_FILE};
use crate::remote::{FILE_BLOCKS, HEADER_BYTES, Remote, file_of};
use crate::roots::Roots;
use crate::walk::HeaderSource;

/// The headers read so far, by the block each starts at: `None` where
/// nothing that says it is there was found.
pub struct Headers<'a> {
    remote: &'a Remote,
    seen: BTreeMap<u64, Option<Header>>,
    /// The lowest header asked for.
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

    /// The header starting at block `at`, read if it was not yet.
    ///
    /// # Errors
    /// Object-store faults.
    pub async fn get(&mut self, at: u64) -> StorageResult<Option<Header>> {
        self.lowest = self.lowest.min(at);
        if let Some(header) = self.seen.get(&at) {
            return Ok(header.clone());
        }
        let offset = (at % FILE_BLOCKS) * BLOCK_SIZE as u64;
        let bytes = self
            .remote
            .bucket()
            .get(
                &self.remote.file_key(file_of(at)),
                Some(offset..offset + HEADER_BYTES as u64),
            )
            .await?;
        let header = bytes
            .and_then(|b| Header::decode(&b).ok())
            .filter(|h| h.at == at);
        self.seen.insert(at, header.clone());
        Ok(header)
    }

    /// The roots block heading file `file`, if it holds one that says it
    /// is there.
    ///
    /// # Errors
    /// Object-store faults.
    pub async fn roots(&self, file: u64) -> StorageResult<Option<Roots>> {
        let bytes = self
            .remote
            .bucket()
            .get(&self.remote.file_key(file), Some(0..BLOCK_SIZE as u64))
            .await?;
        Ok(bytes
            .and_then(|b| Roots::decode(&b))
            .filter(|r| r.at == file * FILE_BLOCKS))
    }

    /// The file of the lowest header asked for: what a recovery reads stops
    /// there, so the files below it are never read (RFC 0070 §8).
    pub const fn lowest_file(&self) -> u64 {
        file_of(self.lowest)
    }

    /// Whether the batch `top` ends is whole: every header back to the one
    /// ending the batch before — or to the first — present, each linking to
    /// a lower block than its own.
    ///
    /// # Errors
    /// Object-store faults.
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

impl HeaderSource for Headers<'_> {
    async fn get(&mut self, at: u64) -> StorageResult<Option<Header>> {
        Headers::get(self, at).await
    }
}

/// The bytes of a file holding `header`'s section alone, its roots block
/// naming `root` — a file an upload would have written, for the tests that
/// write them by hand.
#[cfg(test)]
#[must_use]
pub fn file_bytes(header: &Header, root: u64) -> Vec<u8> {
    use crate::upload_file::SECTION;
    let base = header.at - SECTION;
    let roots = Roots::new(base, NO_FILE, vec![root])
        .with_chain(0, base + crate::upload_file::FIRST_PAGE + 1)
        .encode()
        .unwrap_or_default();
    let mut bytes = roots;
    let mut encoded = header.encode();
    encoded.resize(HEADER_BYTES, 0);
    bytes.extend_from_slice(&encoded);
    bytes.resize(bytes.len() + BLOCK_SIZE, 0); // one page's block
    bytes
}
