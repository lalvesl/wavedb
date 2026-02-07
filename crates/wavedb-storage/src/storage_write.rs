//! A storage shard's write ([RFC 0073] §4) — split from
//! [`crate::storage_shard`], whose thread runs it: what gathered goes down
//! by address with a roots block after it, the `fsync` goes to the helper,
//! and its answer answers every seal the write held.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::fs::File;
use std::sync::mpsc;

use crate::barrier::Answer;
use crate::block::Run;
use crate::error::{StorageError, StorageResult};
use crate::header::NO_FILE;
use crate::roots::{ROOTS_BLOCKS, Roots};
use crate::storage_shard::{Msg, Seal, StorageShard};

/// The storage write in flight: what its `fsync` answers.
pub struct Flight {
    pub(crate) id: u64,
    pub(crate) seals: Vec<Seal>,
    /// The deferred frees it covers.
    pub(crate) mark: u64,
    /// The roots block it supersedes.
    pub(crate) prev: Option<u64>,
}

impl StorageShard {
    /// Write what gathered and a roots block, and ask for the `fsync` —
    /// unless a write is in flight: then once it is durable.
    pub(crate) fn flush(&mut self) {
        if self.flight.is_some() || !self.owes() {
            return;
        }
        self.urgent = false;
        let seals = std::mem::take(&mut self.seals);
        for seal in &seals {
            if let Some(root) = self.by_shard.get_mut(seal.position) {
                *root = seal.root;
            }
            for &run in &seal.frees {
                self.alloc.defer(run);
            }
        }
        let mut writes = std::mem::take(&mut self.gathered);
        self.gathered_bytes = 0;
        let written = self.write_down(&mut writes);
        self.flights += 1;
        let flight = Flight {
            id: self.flights,
            seals,
            mark: self.alloc.mark(),
            prev: None,
        };
        if let Ok((at, files)) =
            written.and_then(|at| Ok((at, self.file.take_dirty()?)))
        {
            let prev = self.newest.replace(at);
            self.flight = Some(Flight { prev, ..flight });
            if self.syncs.send((self.flights, files)).is_err() {
                self.synced(self.flights, false);
            }
        } else {
            self.flight = Some(flight);
            self.synced(self.flights, false);
        }
    }

    /// The gathered windows, by address, then the roots block — every run
    /// of them that lies end to end in one file as one write: where the
    /// roots block went.
    fn write_down(
        &mut self,
        writes: &mut Vec<(Run, Vec<u8>)>,
    ) -> StorageResult<u64> {
        let at = self.alloc.alloc(ROOTS_BLOCKS);
        let prev = self.newest.unwrap_or(NO_FILE);
        let roots = Roots::new(at.start, prev, self.by_shard.clone());
        writes.push((at, roots.encode()?));
        writes.sort_by_key(|(run, _)| run.start);
        let mut segment: Option<(u64, Run, Vec<u8>)> = None;
        for (run, mut bytes) in writes.drain(..) {
            let file = self
                .alloc
                .file_of(run.start)
                .ok_or(StorageError::Corrupt("a window outside every file"))?;
            // A window's bytes run to its end: the next one starts there.
            bytes.resize(run.byte_len() as usize, 0);
            match &mut segment {
                Some((f, seg, buf)) if *f == file && seg.end() == run.start => {
                    seg.count += run.count;
                    buf.extend_from_slice(&bytes);
                }
                _ => {
                    if let Some((f, seg, buf)) = segment.take() {
                        self.file.write_run(f, seg, &buf)?;
                    }
                    segment = Some((file, run, bytes));
                }
            }
        }
        if let Some((f, seg, buf)) = segment {
            self.file.write_run(f, seg, &buf)?;
        }
        Ok(at.start)
    }

    /// The `fsync` of `flight` answered: every seal of it is too.
    pub(crate) fn synced(&mut self, flight: u64, ok: bool) {
        let Some(done) = self.flight.take_if(|f| f.id == flight) else {
            return;
        };
        self.failed |= !ok;
        let ok = ok && !self.failed;
        if ok {
            self.alloc.release_to(done.mark);
            if let Some(prev) = done.prev {
                self.alloc.free(Run::new(prev, ROOTS_BLOCKS));
            }
            self.reclaim();
        }
        for seal in done.seals {
            let answer: Answer = if ok {
                Ok(())
            } else {
                Err(std::io::ErrorKind::Other)
            };
            let _ = seal.report.send((seal.batch, answer));
            for waiter in seal.waiters {
                let _ = waiter.send(if ok {
                    Ok(())
                } else {
                    Err(StorageError::BarrierFailed)
                });
            }
        }
    }
}

/// The `fsync` helper: sync each storage write's files, and say so.
pub fn sync_files(
    jobs: &mpsc::Receiver<(u64, Vec<File>)>,
    report: &mpsc::Sender<Msg>,
) {
    while let Ok((flight, files)) = jobs.recv() {
        let ok = files.iter().all(|f| f.sync_all().is_ok());
        // Closed before the answer: a descriptor holds its file's lock.
        drop(files);
        if report.send(Msg::Synced { flight, ok }).is_err() {
            return;
        }
    }
}
