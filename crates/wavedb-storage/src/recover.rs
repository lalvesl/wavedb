//! Recovery from the object store alone ([RFC 0070] §6, phase 5) — an
//! object-backed database whose disk is gone opens from its files.
//!
//! 1. **The newest whole cut.** `LIST 1seg/ max-keys=1` names the highest
//!    file. It is taken only if its upload ends a cut (`FileHeader::cut`:
//!    every batch up to a point, none in part) and is whole: every file of
//!    the group present, each linked to the one before by its header's
//!    hash, and the group linked to the file before it. Otherwise the next
//!    number down is tried — so the files a crashed upload left above the
//!    recorded end, a part of a backlog, or a copy under another key, are
//!    never taken for the newest. Files with no cut among them are a
//!    database whose first upload never finished: it opens empty, its
//!    numbers stepped over.
//! 2. **The type walk.** That header names every type, its bucket count, its
//!    dictionary and `last`, the newest earlier file with entries for it.
//!    Headers are read newest first, always the nearest file some
//!    incomplete type still points at, and the first value seen for a bucket
//!    is its newest. A type is done once every bucket is known or its chain
//!    ends — a bucket no header names was never written: it is empty.
//!    Headers are fetched a batch at a time, so the walk mostly finds the
//!    next one already read.
//! 3. **Adoption.** The store opens with the `MANIFEST`'s seed; every bucket
//!    is evicted, so reads go to the object store and the disk — or, in
//!    object mode, RAM — fills again as a cache. The dictionaries come
//!    down, and with a disk, one snapshot and one checkpoint make it a root
//!    of its own.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::collections::BTreeMap;

use futures::future::try_join_all;

use crate::block::BlockDescriptor;
use crate::dictionary::Dictionary;
use crate::directory::Directory;
use crate::error::{StorageError, StorageResult};
use crate::file_header::{Chain, FileHeader, NO_FILE};
use crate::page_store::{PageStore, StoreOptions};
use crate::plan::SlotPlan;
use crate::recover_headers::Headers;
use crate::remote::{FILE_BLOCKS, Remote};

/// What a recovery found in the object store.
pub struct Found {
    pub chain: Chain,
    pub types: Vec<FoundType>,
}

/// One type's remote directory, and its dictionary image if it has one.
pub struct FoundType {
    pub struct_hash: u64,
    pub remote: Vec<BlockDescriptor>,
    pub dict: Option<(BlockDescriptor, Vec<u8>)>,
}

/// What a store's object store holds, recovered when the store is
/// object-backed and its disk holds no data file yet — a database to open
/// from its files rather than create over them. `None` otherwise.
///
/// # Errors
/// The recovery's faults.
pub fn found_for(
    had_files: bool,
    options: &StoreOptions,
) -> StorageResult<Option<Found>> {
    match &options.remote {
        Some(remote) if !had_files => {
            futures::executor::block_on(recover(remote))
        }
        _ => Ok(None),
    }
}

/// What `remote` holds, or `None` when it holds no file at all.
///
/// # Errors
/// Object-store faults; [`StorageError::Corrupt`] when a chained header is
/// missing.
pub async fn recover(remote: &Remote) -> StorageResult<Option<Found>> {
    let Some(newest) = newest_file(remote).await? else {
        return Ok(None);
    };
    // Past every file there is, leftovers included: a number once written
    // is never written again.
    let past = (newest + 1) * FILE_BLOCKS;
    let mut headers = Headers::new(remote);
    let Some(top) = newest_cut(&mut headers, newest).await? else {
        // Files, but no cut among them: the first uploads never finished
        // one, so nothing was ever recoverable.
        let chain = Chain {
            end: past,
            ..Chain::default()
        };
        return Ok(Some(Found {
            chain,
            types: Vec::new(),
        }));
    };
    let start = top.0.start;
    let mut chain = Chain {
        end: top.0.end.max(past),
        start,
        tail: Some((top.0.file, top.1)),
        ..Chain::default()
    };
    for entry in &top.0.types {
        let newest = if entry.changed.is_empty() {
            entry.last
        } else {
            top.0.file
        };
        if newest != NO_FILE {
            chain.last.insert(entry.struct_hash, newest);
        }
    }
    let walked = walk(&mut headers, &top.0).await?;
    let types = try_join_all(walked.into_iter().map(
        |(hash, remote_slots, dict)| async move {
            let dict = match dict {
                Some(desc) => Some((desc, remote.fetch(desc).await?)),
                None => None,
            };
            StorageResult::Ok(FoundType {
                struct_hash: hash,
                remote: remote_slots,
                dict,
            })
        },
    ))
    .await?;
    Ok(Some(Found { chain, types }))
}

/// The newest whole cut at or below file `newest`, trying each number down
/// — what a recovery takes.
///
/// # Errors
/// Object-store faults.
pub async fn newest_cut(
    headers: &mut Headers<'_>,
    newest: u64,
) -> StorageResult<Option<(FileHeader, u64)>> {
    let mut top = Some(newest);
    while let Some(n) = top {
        if let Some(header) = headers.get(n).await?
            && header.0.cut
            && headers.whole(&header).await?
        {
            return Ok(Some(header));
        }
        top = n.checked_sub(1);
    }
    Ok(None)
}

/// One type's directory being gathered.
struct Gathering {
    slots: Vec<Option<u64>>,
    missing: usize,
    dict: Option<u64>,
    /// The next file with entries for it ([`NO_FILE`] once the chain ends).
    next: u64,
}

/// Every type's remote directory and dictionary descriptor, gathered from
/// `top` back along the types' chains.
pub async fn walk(
    headers: &mut Headers<'_>,
    top: &FileHeader,
) -> StorageResult<Vec<(u64, Vec<BlockDescriptor>, Option<BlockDescriptor>)>> {
    let mut types: BTreeMap<u64, Gathering> = top
        .types
        .iter()
        .map(|e| {
            let n = e.buckets as usize;
            let state = Gathering {
                slots: vec![None; n],
                missing: n,
                dict: e.dict,
                next: top.file,
            };
            (e.struct_hash, state)
        })
        .collect();
    loop {
        let next = types
            .values()
            .filter(|t| t.missing > 0 && t.next != NO_FILE)
            .map(|t| t.next)
            .max();
        let Some(file) = next else { break };
        let (header, _) = headers
            .get(file)
            .await?
            .ok_or(StorageError::Corrupt("a chained file is missing"))?;
        for entry in &header.types {
            let Some(t) = types.get_mut(&entry.struct_hash) else {
                continue;
            };
            if t.next != file {
                continue;
            }
            for &(bucket, raw) in &entry.changed {
                if let Some(slot) = t.slots.get_mut(bucket as usize)
                    && slot.is_none()
                {
                    *slot = Some(raw);
                    t.missing -= 1;
                }
            }
            t.next = entry.last;
        }
        if types.values().any(|t| t.next == file) {
            return Err(StorageError::Corrupt(
                "a chain names a file without it",
            ));
        }
    }
    Ok(types
        .into_iter()
        .map(|(hash, t)| {
            let slots = t
                .slots
                .into_iter()
                .map(|raw| {
                    raw.map_or(
                        BlockDescriptor::EMPTY,
                        BlockDescriptor::from_raw,
                    )
                })
                .collect();
            (hash, slots, t.dict.map(BlockDescriptor::from_raw))
        })
        .collect())
}

impl PageStore {
    /// Take what a recovery found: the remote directories and the chain as
    /// they are, every disk bucket evicted, the dictionaries written to disk,
    /// then a snapshot and a checkpoint, so the disk is a root of its own.
    ///
    /// # Errors
    /// A type the registry does not list; dictionary and write faults.
    pub(crate) fn adopt(&self, found: Found) -> StorageResult<()> {
        let object = self.object()?;
        let mut plans = Vec::new();
        for t in found.types {
            let idx = self
                .slot_index(t.struct_hash)
                .ok_or(StorageError::UnregisteredStructHash(t.struct_hash))?;
            let slot = &self.types[idx];
            if t.remote.is_empty() {
                continue;
            }
            let evicted = vec![BlockDescriptor::EVICTED; t.remote.len()];
            let disk = Directory::from_slots(evicted, self.seed);
            *slot.remote_directory().borrow_mut() =
                Some(Directory::from_slots(t.remote, self.seed));
            *slot.directory().borrow_mut() = Some(disk.clone());
            if let Some((desc, image)) = t.dict {
                let dict = Dictionary::from_bytes(&image)?;
                slot.uploaded_dict().set(dict.len());
                slot.remote_dict().set(desc);
                slot.dictionary()
                    .borrow_mut()
                    .load(dict, BlockDescriptor::EMPTY);
                let mut plan = SlotPlan::verbatim(idx, disk);
                plan.dict = Some(image);
                plans.push(plan);
            }
        }
        *object.chain.borrow_mut() = found.chain;
        self.place_plans(plans)?;
        self.rewrite_addressing()?;
        self.commit_journal()
    }
}

/// The highest file number in `remote`, by the one-key listing.
///
/// # Errors
/// Object-store faults; a key that is not a file name.
pub async fn newest_file(remote: &Remote) -> StorageResult<Option<u64>> {
    let prefix = remote.file_key(0);
    let prefix = &prefix[..prefix.len() - 16];
    let Some(key) = remote.bucket().first_key(prefix).await? else {
        return Ok(None);
    };
    key.get(prefix.len()..)
        .and_then(|hex| u64::from_str_radix(hex, 16).ok())
        .map(|n| Some(!n))
        .ok_or(StorageError::Corrupt("an object store file name"))
}

impl PageStore {
    /// Move the remote end past every file the object store holds — once,
    /// when a store opens over a disk that already had a database. Files a
    /// crashed upload wrote past what the disk recorded stay where they are,
    /// a branch no chain names: a number once written is never written
    /// again, so nothing a header links to is ever replaced.
    ///
    /// # Errors
    /// Object-store faults.
    pub(crate) fn step_over_leftovers(&self) -> StorageResult<()> {
        let Some(object) = &self.object else {
            return Ok(());
        };
        let newest = futures::executor::block_on(newest_file(&object.remote))?;
        if let Some(newest) = newest {
            let mut chain = object.chain.borrow_mut();
            chain.end = chain.end.max((newest + 1) * FILE_BLOCKS);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use wavedb_platform::carrier::block_on;
    use wavedb_platform::s3::When;
    use wavedb_platform::s3::test_server::TestServer;

    use super::recover;
    use crate::block::BlockDescriptor;
    use crate::file_header::{FileHeader, NO_FILE, TypeEntry, header_hash};
    use crate::remote::{FILE_BLOCKS, HEADER_BLOCKS, HEADER_BYTES, Remote};

    const SH: u64 = 7;

    /// A descriptor of a one-block page at the head of file `file`.
    fn page(file: u64) -> u64 {
        BlockDescriptor::new(file * FILE_BLOCKS + HEADER_BLOCKS, 1).raw()
    }

    /// A header for file `file` of the upload starting at `group`, naming
    /// bucket 0 in itself and following `prev` (hash `prev_hash`).
    fn header(
        file: u64,
        group: u64,
        files: u32,
        prev: (u64, u64),
    ) -> FileHeader {
        FileHeader {
            file,
            group,
            group_files: files,
            cut: true,
            prev: prev.0,
            prev_hash: prev.1,
            start: 0,
            end: file * FILE_BLOCKS + HEADER_BLOCKS + 1,
            types: vec![TypeEntry {
                struct_hash: SH,
                buckets: 2,
                last: if prev.0 == NO_FILE { NO_FILE } else { prev.0 },
                dict: None,
                changed: vec![(0, page(file))],
            }],
        }
    }

    /// PUT `header` as its file, returning its hash.
    fn put(remote: &Remote, header: &FileHeader) -> u64 {
        let encoded = header.encode();
        let mut bytes = encoded.clone();
        bytes.resize(HEADER_BYTES + 4096, 0);
        block_on(remote.bucket().put(
            &remote.file_key(header.file),
            &bytes,
            When::Always,
        ))
        .unwrap();
        header_hash(&encoded)
    }

    fn fixture() -> (tempfile::TempDir, TestServer, Remote) {
        let dir = tempfile::tempdir().unwrap();
        let server = TestServer::start(dir.path());
        let remote = Remote::new(server.bucket(), "t/");
        (dir, server, remote)
    }

    /// File 0 is a whole upload; file 1–2 a second one.
    fn two_uploads(remote: &Remote, break_link: bool) -> u64 {
        let a = put(remote, &header(0, 0, 1, (NO_FILE, 0)));
        let link = if break_link { a ^ 1 } else { a };
        let b1 = put(remote, &header(1, 1, 2, (0, link)));
        put(remote, &header(2, 1, 2, (1, b1)));
        a
    }

    #[test]
    fn the_newest_whole_upload_is_the_state() {
        let (_dir, _server, remote) = fixture();
        two_uploads(&remote, false);
        let found = block_on(recover(&remote)).unwrap().unwrap();
        assert_eq!(found.chain.tail.map(|t| t.0), Some(2));
        let t = &found.types[0];
        assert_eq!(t.remote[0].raw(), page(2), "the newest entry wins");
        assert_eq!(t.remote[1], BlockDescriptor::EMPTY, "never written");
        assert!(found.chain.end >= 3 * FILE_BLOCKS, "past every file");
    }

    #[test]
    fn an_upload_missing_a_file_is_not_the_state() {
        let (_dir, _server, remote) = fixture();
        two_uploads(&remote, false);
        block_on(remote.bucket().delete(&remote.file_key(1))).unwrap();
        let found = block_on(recover(&remote)).unwrap().unwrap();
        assert_eq!(found.chain.tail.map(|t| t.0), Some(0));
        assert_eq!(found.types[0].remote[0].raw(), page(0));
        assert!(found.chain.end >= 3 * FILE_BLOCKS, "past the leftover too");
    }

    #[test]
    fn an_upload_linked_to_another_header_is_not_the_state() {
        let (_dir, _server, remote) = fixture();
        two_uploads(&remote, true);
        let found = block_on(recover(&remote)).unwrap().unwrap();
        assert_eq!(found.chain.tail.map(|t| t.0), Some(0));
    }

    #[test]
    fn an_empty_prefix_holds_no_database() {
        let (_dir, _server, remote) = fixture();
        assert!(block_on(recover(&remote)).unwrap().is_none());
    }
}
