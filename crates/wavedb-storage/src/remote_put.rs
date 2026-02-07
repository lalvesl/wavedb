//! A storage shard's files going down ([RFC 0073] §8) — split from
//! [`crate::remote_shard`], whose thread runs it: oldest first, one at a
//! time, each once every section reserved in it is written, its roots block
//! naming every data shard's newest cut whose whole upload is in.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::time::Duration;

use wavedb_platform::s3::{Put, When};

use crate::block::BLOCK_SIZE;
use crate::error::{StorageError, StorageResult};
use crate::header::NO_FILE;
use crate::remote::FILE_BLOCKS;
use crate::remote_shard::{File, RemoteShard};
use crate::roots::Roots;

/// How long a reserved section may wait for its bytes.
const STALLED: Duration = Duration::from_secs(30);

impl RemoteShard {
    /// Send down every file ready to go, oldest first: the closed ones, and
    /// the open one too when `now`.
    pub(crate) fn send_down(&mut self, now: bool) {
        while let Some(file) = self.files.front_mut() {
            if !file.ready() && file.since.elapsed() > STALLED {
                // An upload reserved and never wrote — its task gone: what
                // it left is a hole, and the files after it go down.
                for section in file.sections.values_mut() {
                    section.get_or_insert_with(Vec::new);
                }
            }
            if !file.ready() || !(file.closed || now) {
                return;
            }
            if file.sections.is_empty() {
                self.files.pop_front();
                continue;
            }
            let Some(mut file) = self.files.pop_front() else {
                return;
            };
            file.closed = true;
            self.put(&file);
        }
    }

    /// Assemble `file` — its roots block, then every section at its offset
    /// — PUT it, and answer the uploads it completes.
    fn put(&mut self, file: &File) {
        if self.fenced {
            // Another writer's file was met: nothing more goes down.
            self.settle(file.number, &Err(StorageError::Fenced));
            return;
        }
        let mut by_shard = self.by_shard.clone();
        for w in &self.waiting {
            if let Some(cut) = w.cut
                && crate::remote::file_of(cut) == file.number
                && w.failed.is_none()
                && let Some(root) = by_shard.get_mut(w.position)
            {
                *root = cut;
            }
        }
        let at = file.number * FILE_BLOCKS;
        let roots =
            Roots::new(at, self.newest.unwrap_or(NO_FILE), by_shard.clone())
                .with_chain(self.start, self.end)
                .encode();
        let put = roots.and_then(|roots| {
            let mut bytes = roots;
            for (&base, section) in &file.sections {
                let offset = ((base - at) as usize) * BLOCK_SIZE;
                bytes.resize(offset, 0);
                bytes.extend_from_slice(section.as_deref().unwrap_or(&[]));
            }
            let key = self.remote.file_key(file.number);
            let bucket = self.remote.bucket();
            futures::executor::block_on(bucket.put(&key, &bytes, When::Absent))
                .map_err(StorageError::from)
        });
        let outcome = match put {
            Ok(Put::Stored { .. }) => {
                self.newest = Some(at);
                self.by_shard = by_shard;
                Ok(())
            }
            Ok(Put::Refused) => {
                self.fenced = true;
                Err(StorageError::Fenced)
            }
            Err(e) => Err(e),
        };
        self.settle(file.number, &outcome);
    }

    /// File `file` went down — or failed to: answer the uploads it was the
    /// last of, and fail the others in it.
    fn settle(&mut self, file: u64, outcome: &StorageResult<()>) {
        let mut answered = Vec::new();
        for (i, w) in self.waiting.iter_mut().enumerate() {
            if !w.files.remove(&file) {
                continue;
            }
            if let Err(e) = outcome
                && w.failed.is_none()
            {
                w.failed = Some(clone_error(e));
            }
            if w.files.is_empty() {
                answered.push(i);
            }
        }
        for i in answered.into_iter().rev() {
            let w = self.waiting.swap_remove(i);
            let _ = w.reply.send(w.failed.map_or(Ok(()), Err));
        }
    }
}

/// A storage error to answer more than one upload with.
fn clone_error(e: &StorageError) -> StorageError {
    match e {
        StorageError::Fenced => StorageError::Fenced,
        other => StorageError::Io(std::io::Error::other(other.to_string())),
    }
}
