//! Recovery from the object store alone ([RFC 0070] §6, phase 5) — an
//! object-backed database whose disk is gone opens from its files — by the
//! walk every reconstruction makes ([RFC 0072] §6).
//!
//! 1. **The newest whole batch.** `LIST 1seg/ max-keys=1` names the highest
//!    file, whose roots block ([RFC 0073] §8) names the newest header ending
//!    a batch (`Header::end_of_batch`: every write up to a point, none in
//!    part). It is taken only if the batch is whole: every header back to
//!    the one ending the batch before present. Otherwise the next number
//!    down is tried — so a file a crashed upload left, or a copy under
//!    another key, is never taken for the newest. Files naming no batch end
//!    are a database whose first upload never finished: it opens empty, its
//!    numbers stepped over.
//! 2. **The walk.** Every type that header names is started there, and
//!    followed along its `last`, the first address seen for a bucket being
//!    its newest. While a registered type is not found yet, the headers are
//!    read down by `prev`, one after the other, until one names it; it is
//!    then followed the same way. Headers are always read highest first and
//!    a batch at a time, so a type's newest header is the first naming it. A
//!    type is done once every bucket is known or its chain ends — a bucket
//!    no header names was never written: it is empty.
//! 3. **Adoption.** The store opens with the `MANIFEST`'s seed; every bucket
//!    is evicted, so reads go to the object store and the disk — or, in
//!    object mode, RAM — fills again as a cache. The dictionaries come
//!    down, and with a disk, one durable batch naming all of it makes it a
//!    root of its own.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use futures::future::try_join_all;

use crate::block::BlockDescriptor;
use crate::dictionary::Dictionary;
use crate::directory::Directory;
use crate::error::{StorageError, StorageResult};
use crate::header::{Chain, Header, Rotation};
use crate::page_store::{PageStore, StoreOptions};
use crate::plan::SlotPlan;
use crate::recover_headers::Headers;
use crate::remote::{FILE_BLOCKS, Remote};
use crate::roots::Roots;
use crate::walk::{Lane, walk};

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
    registered: &[u64],
) -> StorageResult<Option<Found>> {
    match &options.remote {
        Some(remote) if !had_files => {
            futures::executor::block_on(recover(remote, registered, 0))
        }
        _ => Ok(None),
    }
}

/// What `remote` holds for the data shard at `position` — the
/// `registered` types and any other its headers name — or `None` when it
/// holds no file at all.
///
/// # Errors
/// Object-store faults; [`StorageError::Corrupt`] when a chained header is
/// missing.
pub async fn recover(
    remote: &Remote,
    registered: &[u64],
    position: u32,
) -> StorageResult<Option<Found>> {
    let Some(newest) = newest_file(remote).await? else {
        return Ok(None);
    };
    // Past every file there is, leftovers included: a number once written
    // is never written again.
    let past = (newest + 1) * FILE_BLOCKS;
    let mut headers = Headers::new(remote);
    let Some((roots, top)) = newest_cut(&mut headers, newest, position).await?
    else {
        // Files, but no batch end among them: the first uploads never
        // finished one, so nothing was ever recoverable.
        let chain = Chain {
            end: past,
            ..Chain::default()
        };
        return Ok(Some(Found {
            chain,
            types: Vec::new(),
        }));
    };
    let mut chain = Chain {
        end: roots.end.max(past),
        start: roots.start,
        tail: Some(top.at),
        cut: Some(top.at),
        roots: Some(roots.at),
        floor: top.horizon,
        ..Chain::default()
    };
    let walked = walk(&mut headers, &top, registered, Lane::Types).await?;
    for w in &walked {
        let rotation = Rotation {
            last: w.newest,
            ..Rotation::default()
        };
        chain.types.insert(w.struct_hash, rotation);
    }
    let types = try_join_all(walked.into_iter().map(|w| async move {
        let dict = match w.dict {
            Some(desc) => Some((desc, remote.fetch(desc).await?)),
            None => None,
        };
        StorageResult::Ok(FoundType {
            struct_hash: w.struct_hash,
            remote: w.slots,
            dict,
        })
    }))
    .await?;
    Ok(Some(Found { chain, types }))
}

/// The newest roots block at or below file `newest` whose root for the
/// data shard at `position` ends a whole batch, and that header, trying
/// each number down — what a recovery roots in. `None` once a roots block
/// says the data shard never ended one.
///
/// # Errors
/// Object-store faults.
pub async fn newest_cut(
    headers: &mut Headers<'_>,
    newest: u64,
    position: u32,
) -> StorageResult<Option<(Roots, Header)>> {
    let mut file = Some(newest);
    while let Some(n) = file {
        file = n.checked_sub(1);
        let Some(roots) = headers.roots(n).await? else {
            continue;
        };
        let Some(at) = roots.root(position as usize) else {
            return Ok(None);
        };
        if let Some(header) = headers.get(at).await?
            && header.end_of_batch
            && header.shard == position
            && headers.whole(&header).await?
        {
            return Ok(Some((roots, header)));
        }
    }
    Ok(None)
}

impl PageStore {
    /// The registered types' hashes — what a walk looks for.
    pub(crate) fn registered(&self) -> Vec<u64> {
        self.types
            .iter()
            .map(crate::struct_storage::TypeSlot::struct_hash)
            .collect()
    }

    /// Take what a recovery found: the remote directories and the chain as
    /// they are, every disk bucket evicted, the dictionaries written to disk,
    /// then a durable batch naming all of it, so the disk is a root of its
    /// own.
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
        crate::wait::park_on(self.place_plans(plans))?;
        self.record_all();
        self.drain()
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
    use crate::header::{Header, NO_FILE, TypeEntry};
    use crate::recover_headers::file_bytes;
    use crate::remote::{FILE_BLOCKS, Remote, file_of};
    use crate::upload_file::{FIRST_PAGE, SECTION};

    const SH: u64 = 7;
    /// A registered type no header names before file 3.
    const LATE: u64 = 8;

    /// Where file `file`'s header starts.
    const fn at(file: u64) -> u64 {
        file * FILE_BLOCKS + SECTION
    }

    /// A descriptor of a one-block page at the head of file `file`.
    fn page(file: u64) -> u64 {
        BlockDescriptor::new(file * FILE_BLOCKS + FIRST_PAGE, 1).raw()
    }

    /// A header for file `file`, following file `prev`'s, naming bucket 0 of
    /// a two-bucket type in itself — the type's previous entries in `prev`.
    fn header(file: u64, prev: Option<u64>, end_of_batch: bool) -> Header {
        let prev = prev.map_or(NO_FILE, at);
        Header {
            at: at(file),
            prev,
            horizon: 0,
            shard: 0,
            roots: NO_FILE,
            end_of_batch,
            types: vec![TypeEntry {
                struct_hash: SH,
                buckets: 2,
                last: prev,
                dict: None,
                changed: vec![(0, page(file))],
            }],
            remote: Vec::new(),
            chain: None,
        }
    }

    /// PUT `header` as its file, its roots block naming file `root`'s.
    fn put(remote: &Remote, header: &Header, root: u64) {
        block_on(remote.bucket().put(
            &remote.file_key(file_of(header.at)),
            &file_bytes(header, at(root)),
            When::Always,
        ))
        .unwrap();
    }

    fn fixture() -> (tempfile::TempDir, TestServer, Remote) {
        let dir = tempfile::tempdir().unwrap();
        let server = TestServer::start(dir.path());
        let remote = Remote::new(server.bucket(), "t/");
        (dir, server, remote)
    }

    /// File 0 is a whole batch; files 1–2 a second one.
    fn two_batches(remote: &Remote) {
        put(remote, &header(0, None, true), 0);
        put(remote, &header(1, Some(0), false), 0);
        put(remote, &header(2, Some(1), true), 2);
    }

    #[test]
    fn the_newest_whole_batch_is_the_state() {
        let (_dir, _server, remote) = fixture();
        two_batches(&remote);
        let found = block_on(recover(&remote, &[SH], 0)).unwrap().unwrap();
        assert_eq!(found.chain.tail, Some(at(2)));
        assert_eq!(found.chain.last(SH), at(2));
        let t = &found.types[0];
        assert_eq!(t.remote[0].raw(), page(2), "the newest entry wins");
        assert_eq!(t.remote[1], BlockDescriptor::EMPTY, "never written");
        assert!(found.chain.end >= 3 * FILE_BLOCKS, "past every file");
    }

    #[test]
    fn a_batch_missing_a_header_is_not_the_state() {
        let (_dir, _server, remote) = fixture();
        two_batches(&remote);
        block_on(remote.bucket().delete(&remote.file_key(1))).unwrap();
        let found = block_on(recover(&remote, &[SH], 0)).unwrap().unwrap();
        assert_eq!(found.chain.tail, Some(at(0)));
        assert_eq!(found.types[0].remote[0].raw(), page(0));
        assert!(found.chain.end >= 3 * FILE_BLOCKS, "past the leftover too");
    }

    /// A part of a backlog — the newest file, ending no batch — roots in
    /// the batch its roots block names, and a file copied under another
    /// file's key is stepped over.
    #[test]
    fn a_file_ending_no_batch_or_not_its_own_is_not_the_state() {
        let (_dir, _server, remote) = fixture();
        two_batches(&remote);
        put(&remote, &header(3, Some(2), false), 2);
        let found = block_on(recover(&remote, &[SH], 0)).unwrap().unwrap();
        assert_eq!(found.chain.tail, Some(at(2)));

        let copy = file_bytes(&header(2, Some(1), true), at(2));
        let key = remote.file_key(4);
        block_on(remote.bucket().put(&key, &copy, When::Always)).unwrap();
        let found = block_on(recover(&remote, &[SH], 0)).unwrap().unwrap();
        assert_eq!(found.chain.tail, Some(at(2)), "file 4 holds file 2");
    }

    /// A registered type the newest header does not name is found by going
    /// down by `prev`, then followed along its own chain; one no header
    /// names at all is simply empty.
    #[test]
    fn a_type_the_newest_header_does_not_name_is_found_further_down() {
        let (_dir, _server, remote) = fixture();
        two_batches(&remote);
        let mut named = header(3, Some(2), true);
        named.types.push(TypeEntry {
            struct_hash: LATE,
            buckets: 1,
            last: NO_FILE,
            dict: None,
            changed: vec![(0, page(3))],
        });
        put(&remote, &named, 3);
        put(&remote, &header(4, Some(3), true), 4);
        put(&remote, &header(5, Some(4), true), 5);
        let found = block_on(recover(&remote, &[SH, LATE, 9], 0)).unwrap();
        let found = found.unwrap();
        let late = found.types.iter().find(|t| t.struct_hash == LATE);
        assert_eq!(late.map(|t| t.remote[0].raw()), Some(page(3)));
        assert_eq!(found.chain.last(LATE), at(3), "its newest header");
        assert!(found.types.iter().all(|t| t.struct_hash != 9), "unnamed");
    }

    #[test]
    fn an_empty_prefix_holds_no_database() {
        let (_dir, _server, remote) = fixture();
        assert!(block_on(recover(&remote, &[SH], 0)).unwrap().is_none());
    }
}
