//! Compaction of the object store ([RFC 0070] §8, phase 7) — files deleted
//! whole, once nothing needs them.
//!
//! An object cannot be cut, so a file's space comes back only when the file
//! goes, and a file can go only when:
//!
//! 1. **no live page is in it** — in the store's remote directories, in
//!    what a recovery from the object store would rebuild today (which can
//!    differ until a cut carries the store's state), and, with a disk, in
//!    what the disk's reconstruction roots name: the newest batch's remote
//!    directories and, while it is not durable yet, the one's before
//!    (RFC 0072) — so with a disk the files emptied are deleted after a
//!    durable batch, which the compaction settles itself;
//! 2. **no recovery reads its header** — it lies below the **horizon**, the
//!    lowest file a recovery from the newest whole batch asks for: the
//!    batch's headers back to the one ending the batch before, and every
//!    header its walk reaches.
//!
//! Both are found the way a recovery finds them — by running one over the
//! object store's headers (`crate::recover`), GETs only, no state of its own.
//! A file's live pages only ever leave it (writing is always at the end), and
//! the horizon only rises, so a file found deletable stays deletable.
//!
//! The compaction holds the upload turn throughout: no upload is in flight
//! while it looks, so no cut taken before it can land after it and name a
//! file it deleted. Files mostly free below the horizon — less than
//! `victim_free_percent` of a file's 8 MiB live, which takes in the small
//! files an upload interval leaves — have their live pages carried forward
//! by uploads of its own ([`MOVE_BLOCKS`] each), and are deleted once those
//! uploads stand as a cut.
//!
//! A DELETE costs nothing on S3 and a LIST costs what a PUT does, so the
//! numbers below the horizon are deleted blind rather than listed; the ones
//! already gone are remembered in RAM, and after a reopen deleted once more.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::cell::RefCell;
use std::collections::BTreeSet;

use futures::stream::{self, StreamExt, TryStreamExt};

use crate::block::BlockDescriptor;
use crate::error::{StorageError, StorageResult};
use crate::page_store::PageStore;
use crate::recover::{newest_cut, newest_file};
use crate::recover_headers::Headers;
use crate::remote::{FILE_BLOCKS, Remote, file_of};
use crate::remote_shard_compact::Report;
use crate::upload::{IN_FLIGHT, Release};
use crate::walk::{Lane, walk};

/// Blocks of remote pages one upload carries forward at most (8 MiB), so
/// that a compaction does not double every upload.
pub const MOVE_BLOCKS: u64 = 2048;

/// What one [`compact_remote`](PageStore::compact_remote) did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemoteCompacted {
    /// File numbers deleted (a DELETE of a number with no file succeeds).
    pub deleted_files: usize,
    /// Remote pages carried forward out of mostly free files.
    pub moved_pages: usize,
}

/// What a recovery from the object store would read and rebuild.
struct Survey {
    /// The lowest file whose header it reads.
    horizon: u64,
    /// The files the directories it rebuilds name.
    named: BTreeSet<u64>,
}

/// Empties the set of files being carried forward, however the compaction
/// ends.
struct Clear<'a>(&'a RefCell<BTreeSet<u64>>);

impl Drop for Clear<'_> {
    fn drop(&mut self) {
        self.0.borrow_mut().clear();
    }
}

impl PageStore {
    /// Compact the object store: delete every file nothing needs, carry the
    /// live pages of the mostly free files forward, and delete those files
    /// too (see the module docs). Nothing when another upload holds the
    /// turn, or when the object store holds no cut yet.
    ///
    /// # Errors
    /// [`StorageError::NotObjectBacked`] for a disk-only store;
    /// [`StorageError::Fenced`] once another writer's file was met; an
    /// object-store fault. What it deleted stays deleted; what it did not
    /// is left for the next one.
    pub async fn compact_remote(&self) -> StorageResult<RemoteCompacted> {
        let object = self.object()?;
        if object.uploading.replace(true) {
            return Ok(RemoteCompacted::default());
        }
        let _one = Release(&object.uploading);
        if object.fenced.get() {
            return Err(StorageError::Fenced);
        }
        let mut done = RemoteCompacted::default();
        let Some(horizon) = self.delete_unneeded(&mut done).await? else {
            return Ok(done);
        };
        let victims = self.victims_below(horizon);
        if victims.is_empty() {
            return Ok(done);
        }
        *object.moving.borrow_mut() = victims;
        let clear = Clear(&object.moving);
        while self.moving_left() {
            let up = self.upload_turn().await?;
            done.moved_pages += up.moved;
            if up.files == 0 {
                break; // nothing could go up: leave the rest for next time
            }
        }
        drop(clear);
        if self.has_disk() {
            // A durable batch: the disk's roots stop naming the old files.
            self.drain()?;
        }
        self.delete_unneeded(&mut done).await?;
        Ok(done)
    }

    /// nothing names, and advance the chain's oldest live file. The horizon,
    /// or `None` when the object store holds no cut to recover.
    async fn delete_unneeded(
        &self,
        done: &mut RemoteCompacted,
    ) -> StorageResult<Option<u64>> {
        let object = self.object()?;
        // The turn is held: the remote directories do not change meanwhile.
        let mut live = self.remote_live();
        for file in object.rooted.borrow().iter().flatten() {
            live.entry(*file).or_insert(0);
        }
        let registered = self.registered();
        let position = self.position;
        let Some(survey) =
            survey(&object.remote, &registered, position).await?
        else {
            return Ok(None);
        };
        if let Some(link) = &object.link {
            // The files are the storage shard's: it deletes what none of
            // its data shards needs (RFC 0073 §8).
            let needed = live
                .keys()
                .chain(&survey.named)
                .copied()
                .filter(|&f| f < survey.horizon)
                .collect();
            let report = Report {
                horizon: survey.horizon,
                needed,
                live: self.remote_live(),
                percent: self.victim_free_percent,
            };
            let answer = link.compact(position as usize, report).await?;
            done.deleted_files += answer.deleted;
            object.chain.borrow_mut().start = answer.start;
            *object.victims.borrow_mut() = answer
                .victims
                .into_iter()
                .filter(|f| live.contains_key(f))
                .collect();
            return Ok(Some(survey.horizon));
        }
        let start = object.chain.borrow().start;
        let doomed: Vec<u64> = (start..survey.horizon)
            .filter(|f| !live.contains_key(f) && !survey.named.contains(f))
            .filter(|f| !object.deleted.borrow().contains(f))
            .collect();
        let remote = &object.remote;
        stream::iter(doomed)
            .map(|file| async move {
                remote.bucket().delete(&remote.file_key(file)).await?;
                StorageResult::Ok(file)
            })
            .buffer_unordered(IN_FLIGHT)
            .try_for_each(|file| {
                object.deleted.borrow_mut().insert(file);
                done.deleted_files += 1;
                async { Ok(()) }
            })
            .await?;
        // The oldest file still standing: none is older than the lowest one
        // anything names, and every file below the horizon but those is gone.
        let oldest = [live.keys().next(), survey.named.first()]
            .into_iter()
            .flatten()
            .copied()
            .fold(survey.horizon, u64::min)
            .max(start);
        object.chain.borrow_mut().start = oldest;
        object.deleted.borrow_mut().retain(|&f| f >= oldest);
        Ok(Some(survey.horizon))
    }

    /// The files below `horizon` holding live pages, but fewer than
    /// [`StoreOptions::victim_free_percent`](crate::StoreOptions) leaves of
    /// a file's 8 MiB.
    fn victims_below(&self, horizon: u64) -> BTreeSet<u64> {
        if let Some(object) = &self.object
            && object.link.is_some()
        {
            // The storage shard weighed them across its data shards.
            return std::mem::take(&mut *object.victims.borrow_mut());
        }
        let free = u64::from(self.victim_free_percent.min(100));
        let most = FILE_BLOCKS * (100 - free) / 100;
        self.remote_live()
            .into_iter()
            .filter(|&(file, live)| file < horizon && live < most)
            .map(|(file, _)| file)
            .collect()
    }

    /// Whether a page or a dictionary still lies in a file being emptied.
    fn moving_left(&self) -> bool {
        let Some(object) = &self.object else {
            return false;
        };
        let moving = object.moving.borrow();
        let left = |desc: BlockDescriptor| {
            desc.is_allocated() && moving.contains(&file_of(desc.start()))
        };
        self.types.iter().any(|slot| {
            left(slot.remote_dict().get())
                || slot
                    .remote_directory()
                    .borrow()
                    .iter()
                    .any(|dir| dir.slots().iter().any(|&d| left(d)))
        })
    }

    /// The remote buckets of slot `idx`, and whether its dictionary, an
    /// upload carries forward out of the files being emptied — up to
    /// `budget` blocks, which it spends.
    pub(crate) fn moves_of(
        &self,
        idx: usize,
        budget: &mut u64,
    ) -> (BTreeSet<usize>, bool) {
        let mut moved = BTreeSet::new();
        let Some(object) = &self.object else {
            return (moved, false);
        };
        let moving = object.moving.borrow();
        if moving.is_empty() {
            return (moved, false);
        }
        let slot = &self.types[idx];
        let dict = slot.remote_dict().get();
        let move_dict =
            dict.is_allocated() && moving.contains(&file_of(dict.start()));
        if let Some(dir) = slot.remote_directory().borrow().as_ref() {
            for (bucket, desc) in dir.slots().iter().enumerate() {
                if *budget == 0 {
                    break;
                }
                if desc.is_allocated()
                    && moving.contains(&file_of(desc.start()))
                {
                    moved.insert(bucket);
                    *budget = budget.saturating_sub(desc.count());
                }
            }
        }
        (moved, move_dict)
    }
}

/// What a recovery from `remote` would read and rebuild, found the way it
/// finds them; `None` when there is no cut to recover.
async fn survey(
    remote: &Remote,
    registered: &[u64],
    position: u32,
) -> StorageResult<Option<Survey>> {
    let Some(newest) = newest_file(remote).await? else {
        return Ok(None);
    };
    let mut headers = Headers::new(remote);
    let Some((_, top)) = newest_cut(&mut headers, newest, position).await?
    else {
        return Ok(None);
    };
    let walked = walk(&mut headers, &top, registered, Lane::Types).await?;
    let mut named = BTreeSet::new();
    for w in walked {
        for desc in w.slots.into_iter().chain(w.dict) {
            if desc.is_allocated() {
                named.insert(file_of(desc.start()));
            }
        }
    }
    Ok(Some(Survey {
        horizon: headers.lowest_file(),
        named,
    }))
}

#[cfg(test)]
mod tests {
    use wavedb_platform::carrier::block_on;
    use wavedb_platform::s3::When;
    use wavedb_platform::s3::test_server::TestServer;

    use super::survey;
    use crate::block::BlockDescriptor;
    use crate::header::{Header, NO_FILE, TypeEntry};
    use crate::recover_headers::file_bytes;
    use crate::remote::{FILE_BLOCKS, Remote};
    use crate::upload_file::{FIRST_PAGE, SECTION};

    const SH: u64 = 7;

    /// Where file `file`'s header starts.
    const fn at(file: u64) -> u64 {
        file * FILE_BLOCKS + SECTION
    }

    /// A one-block page at the head of file `file`.
    fn page(file: u64) -> u64 {
        BlockDescriptor::new(file * FILE_BLOCKS + FIRST_PAGE, 1).raw()
    }

    /// PUT file `file`, a whole batch of its own, naming `changed` for a
    /// two-bucket type whose previous entries are in file `last`'s header.
    fn put(
        remote: &Remote,
        file: u64,
        last: Option<u64>,
        changed: Vec<(u32, u64)>,
    ) {
        let header = Header {
            at: at(file),
            prev: file.checked_sub(1).map_or(NO_FILE, at),
            horizon: 0,
            shard: 0,
            roots: NO_FILE,
            end_of_batch: true,
            types: vec![TypeEntry {
                struct_hash: SH,
                buckets: 2,
                last: last.map_or(NO_FILE, at),
                dict: None,
                changed,
            }],
            remote: Vec::new(),
            chain: None,
        };
        block_on(remote.bucket().put(
            &remote.file_key(file),
            &file_bytes(&header, at(file)),
            When::Always,
        ))
        .unwrap();
    }

    /// A recovery reads the newest whole batch, the header ending the batch
    /// before, and back along the type's chain until every bucket is known:
    /// the lowest of those is the horizon, and a header restating every
    /// bucket raises it to the batch before it.
    #[test]
    fn the_horizon_is_the_lowest_header_a_recovery_reads() {
        let dir = tempfile::tempdir().unwrap();
        let server = TestServer::start(dir.path());
        let remote = Remote::new(server.bucket(), "t/");
        put(&remote, 0, None, vec![(1, page(0))]);
        put(&remote, 1, Some(0), vec![(0, page(1))]);
        put(&remote, 2, Some(1), vec![(0, page(2))]);
        let s = block_on(survey(&remote, &[SH], 0)).unwrap().unwrap();
        assert_eq!(s.horizon, 0, "bucket 1 is only known from file 0");
        assert_eq!(s.named.iter().copied().collect::<Vec<_>>(), [0, 2]);

        put(&remote, 3, Some(2), vec![(0, page(2)), (1, page(0))]);
        let s = block_on(survey(&remote, &[SH], 0)).unwrap().unwrap();
        assert_eq!(
            s.horizon, 2,
            "the batch and the one ending the batch before"
        );
        assert_eq!(s.named.iter().copied().collect::<Vec<_>>(), [0, 2]);
    }
}
