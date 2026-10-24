//! The journal's barrier off the writer's thread ([RFC 0067] §4).
//!
//! A batch is durable once its journal frame is `fsync`ed, and an `fsync` is
//! a blocking call that takes as long as the device says. Taken inline, it
//! stops the thread that took it — on a node, a worker, and with it every
//! other owner that worker serves. So a store can hand its barriers to a
//! [`Barrier`]: a helper thread that does nothing but `fsync`. The batch is
//! written and committed as before, and its `apply` then **waits** for the
//! helper instead of blocking: the worker runs other owners' operations
//! meanwhile, and the batch's owner stays locked until it is durable, so
//! nobody reads it before it is.
//!
//! The helper takes whatever requests have queued since its last round and
//! syncs each file **once** for all of them — `fsync` flushes the file, not
//! one write — so a burst on one journal costs one barrier, with no window
//! and no timer: group commit falls out of the queue.
//!
//! ## Counting barriers across threads
//!
//! Retirement (RFC 0047) asks "was this file flushed *since* this frame was
//! appended?". With barriers on two threads, a flush that started before the
//! append and finished after it must not count. So a journal file counts the
//! barriers that **started** and those that **completed**: a frame appended
//! when `started = n` is durable once `completed > n` — more barriers have
//! finished than had begun before it, so one of them began after it.
//!
//! ## A failed barrier
//!
//! A batch whose barrier fails is already committed to the caches, and an
//! `fsync` that failed leaves the file's state unknown. Its `apply` reports
//! the fault, and the file is marked failed: every later append to it is
//! refused ([`StorageError::BarrierFailed`]) until the store is reopened and
//! recovers from what is actually on disk.
//!
//! [RFC 0067]: ../../../rfcs/0067-thread-per-core-shards-WIP.md

use std::fs::File;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use futures::channel::oneshot;

use crate::error::{StorageError, StorageResult};

/// A journal file and the barriers taken on it, shared by the store that
/// writes it and the helper that syncs it.
#[derive(Debug)]
pub struct Synced {
    file: File,
    started: AtomicU64,
    completed: AtomicU64,
    failed: AtomicBool,
}

impl Synced {
    pub(crate) const fn new(file: File) -> Self {
        Self {
            file,
            started: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicBool::new(false),
        }
    }

    /// The file itself, for writes and reads.
    pub(crate) const fn file(&self) -> &File {
        &self.file
    }

    /// Take one barrier on this file, on the calling thread.
    ///
    /// # Errors
    /// [`StorageError::Io`] if the sync fails; the file is then failed.
    pub(crate) fn sync(&self) -> StorageResult<()> {
        self.started.fetch_add(1, Ordering::AcqRel);
        match self.file.sync_all() {
            Ok(()) => {
                self.completed.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            Err(e) => {
                self.failed.store(true, Ordering::Release);
                Err(e.into())
            }
        }
    }

    /// Barriers begun on this file — what a frame's durability is measured
    /// against, read right after appending it.
    pub(crate) fn started(&self) -> u64 {
        self.started.load(Ordering::Acquire)
    }

    /// Barriers completed on this file.
    pub(crate) fn completed(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    /// Refuse to append to a file whose barrier failed.
    ///
    /// # Errors
    /// [`StorageError::BarrierFailed`].
    pub(crate) fn check(&self) -> StorageResult<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::BarrierFailed);
        }
        Ok(())
    }
}

/// One barrier asked of the helper, and who waits for it — nobody, for a
/// windowed store's periodic flush.
struct Request {
    file: Arc<Synced>,
    done: Option<oneshot::Sender<StorageResult<()>>>,
}

/// A thread's `fsync` helper. Cheap to clone; the helper thread ends once
/// every clone is gone.
#[derive(Debug, Clone)]
pub struct Barrier {
    requests: async_channel::Sender<Request>,
}

impl Barrier {
    /// Start a helper thread named `name`.
    ///
    /// # Errors
    /// The OS refusing to start the thread.
    pub fn spawn(name: &str) -> std::io::Result<Self> {
        let (requests, queue) = async_channel::unbounded::<Request>();
        std::thread::Builder::new()
            .name(name.into())
            .spawn(move || serve(&queue))?;
        Ok(Self { requests })
    }

    /// Ask for a barrier on `file` covering everything written to it so far,
    /// and wait for it.
    pub(crate) fn sync(&self, file: &Arc<Synced>) -> Durable {
        let (done, wait) = oneshot::channel();
        self.send(file, Some(done));
        Durable(wait)
    }

    /// Ask for a barrier on `file` and do not wait — a windowed store's
    /// periodic flush, which no acknowledgement depends on.
    pub(crate) fn flush(&self, file: &Arc<Synced>) {
        self.send(file, None);
    }

    fn send(
        &self,
        file: &Arc<Synced>,
        done: Option<oneshot::Sender<StorageResult<()>>>,
    ) {
        // Unbounded, so this never waits; closed only if the helper died, in
        // which case the dropped sender answers the waiter with a fault.
        let _ = self.requests.try_send(Request {
            file: Arc::clone(file),
            done,
        });
    }
}

/// The helper: take every queued request, sync each file once, answer.
fn serve(queue: &async_channel::Receiver<Request>) {
    while let Ok(first) = queue.recv_blocking() {
        let mut round = vec![first];
        while let Ok(more) = queue.try_recv() {
            round.push(more);
        }
        // Files in this round and how their one barrier went, in order.
        let mut synced: Vec<(*const Synced, Result<(), std::io::ErrorKind>)> =
            Vec::new();
        for request in round {
            let key = Arc::as_ptr(&request.file);
            let earlier = synced.iter().find(|(k, _)| *k == key);
            let outcome = if let Some((_, outcome)) = earlier {
                *outcome
            } else {
                let outcome = request.file.sync().map_err(kind);
                synced.push((key, outcome));
                outcome
            };
            if let Some(done) = request.done {
                let _ = done.send(outcome.map_err(|k| {
                    StorageError::Io(std::io::Error::new(k, "journal barrier"))
                }));
            }
        }
    }
}

/// The kind of an I/O fault, which is all a shared outcome can carry.
fn kind(e: StorageError) -> std::io::ErrorKind {
    if let StorageError::Io(io) = e {
        io.kind()
    } else {
        std::io::ErrorKind::Other
    }
}

/// A batch's wait for its barrier.
#[derive(Debug)]
pub struct Durable(oneshot::Receiver<StorageResult<()>>);

impl Future for Durable {
    type Output = StorageResult<()>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|answer| {
            answer.unwrap_or_else(|_| {
                Err(StorageError::Io(std::io::Error::other(
                    "the barrier helper is gone",
                )))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::sync::Arc;

    use super::{Barrier, Synced};

    fn file() -> (tempfile::TempDir, Arc<Synced>) {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::File::create(dir.path().join("j.log")).unwrap();
        (dir, Arc::new(Synced::new(file)))
    }

    #[test]
    fn a_barrier_is_counted_when_it_starts_and_when_it_completes() {
        let (_dir, f) = file();
        assert_eq!((f.started(), f.completed()), (0, 0));
        f.sync().unwrap();
        assert_eq!((f.started(), f.completed()), (1, 1));
    }

    #[test]
    fn the_helper_syncs_and_answers() {
        let (_dir, f) = file();
        (&f.file).write_all(b"frame").unwrap();
        let barrier = Barrier::spawn("barrier-test").unwrap();
        futures::executor::block_on(barrier.sync(&f)).unwrap();
        assert!(f.completed() >= 1);
    }

    /// Requests queued together share one barrier per file: sixteen waits
    /// queued before the helper looks cost one `fsync`, and all are answered.
    #[test]
    fn a_round_syncs_each_file_once() {
        let (_dir, f) = file();
        let (requests, queue) = async_channel::unbounded();
        let barrier = Barrier { requests };
        let waits: Vec<_> = (0..16).map(|_| barrier.sync(&f)).collect();
        drop(barrier);
        super::serve(&queue); // one round, then the queue is closed
        for wait in waits {
            futures::executor::block_on(wait).unwrap();
        }
        assert_eq!((f.started(), f.completed()), (1, 1));
    }

    #[test]
    fn a_failed_file_refuses_appends() {
        let (_dir, f) = file();
        assert!(f.check().is_ok());
        f.failed.store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(f.check(), Err(crate::StorageError::BarrierFailed)));
    }
}
