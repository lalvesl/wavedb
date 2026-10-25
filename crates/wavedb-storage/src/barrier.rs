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
//! ## Nothing shared
//!
//! The helper and the worker share no memory. A request carries its own
//! duplicate of the journal's file descriptor (an `fsync` on any descriptor
//! of a file flushes that file) and a channel to answer on; the journal's
//! counters and its failed mark belong to the worker alone, as plain fields,
//! and change only when the worker asks for a barrier or reads an answer. So
//! there is no lock and no atomic on either side of the seam — only the two
//! channels the messages travel through.
//!
//! ## Counting barriers
//!
//! Retirement (RFC 0047) asks "was this file flushed *since* this frame was
//! appended?". A flush that started before the append and finished after it
//! must not count. So a journal counts the barriers **started** — asked of
//! the helper, or begun inline — and those **completed** — answered, or done
//! inline: a frame appended when `started = n` is durable once
//! `completed > n`. More barriers have finished than had begun before it, so
//! one of them began after it — and a barrier asked of the helper begins no
//! earlier than it was asked, since the helper syncs only what it has
//! already dequeued.
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
use std::io::ErrorKind;
use std::os::unix::fs::MetadataExt;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};

use futures::channel::oneshot;

use crate::error::{StorageError, StorageResult};

/// How one barrier went, as the helper reports it.
type Answer = Result<(), ErrorKind>;

/// A journal file and the barriers taken on it — owned by the worker that
/// writes it (see the module docs).
#[derive(Debug)]
pub struct Synced {
    file: File,
    /// The file's identity, so a round syncs it once whatever descriptor
    /// each request carries.
    key: (u64, u64),
    started: u64,
    completed: u64,
    failed: bool,
    /// Where the helper answers, and the end each request carries.
    answers: mpsc::Receiver<Answer>,
    answer_to: mpsc::Sender<Answer>,
}

impl Synced {
    /// Track `file`'s barriers.
    ///
    /// # Errors
    /// [`StorageError::Io`] if the file's identity cannot be read.
    pub(crate) fn new(file: File) -> StorageResult<Self> {
        let meta = file.metadata()?;
        let (answer_to, answers) = mpsc::channel();
        Ok(Self {
            file,
            key: (meta.dev(), meta.ino()),
            started: 0,
            completed: 0,
            failed: false,
            answers,
            answer_to,
        })
    }

    /// The file itself, for writes and reads.
    pub(crate) const fn file(&self) -> &File {
        &self.file
    }

    /// Take one barrier on this file, on the calling thread.
    ///
    /// # Errors
    /// [`StorageError::Io`] if the sync fails; the file is then failed.
    pub(crate) fn sync(&mut self) -> StorageResult<()> {
        self.started += 1;
        match self.file.sync_all() {
            Ok(()) => {
                self.completed += 1;
                Ok(())
            }
            Err(e) => {
                self.failed = true;
                Err(e.into())
            }
        }
    }

    /// Barriers begun on this file — what a frame's durability is measured
    /// against, read right after appending it.
    pub(crate) fn started(&mut self) -> u64 {
        self.read_answers();
        self.started
    }

    /// Barriers completed on this file.
    pub(crate) fn completed(&mut self) -> u64 {
        self.read_answers();
        self.completed
    }

    /// Refuse to append to a file whose barrier failed.
    ///
    /// # Errors
    /// [`StorageError::BarrierFailed`].
    pub(crate) fn check(&mut self) -> StorageResult<()> {
        self.read_answers();
        if self.failed {
            return Err(StorageError::BarrierFailed);
        }
        Ok(())
    }

    /// Count what the helper has answered since the last look.
    fn read_answers(&mut self) {
        while let Ok(answer) = self.answers.try_recv() {
            match answer {
                Ok(()) => self.completed += 1,
                Err(_) => self.failed = true,
            }
        }
    }

    /// A request for one barrier on this file, counted as started: it is
    /// asked after everything written so far, and the helper syncs it no
    /// earlier.
    fn request(
        &mut self,
        done: Option<oneshot::Sender<StorageResult<()>>>,
    ) -> StorageResult<Request> {
        let file = match self.file.try_clone() {
            Ok(file) => file,
            Err(e) => {
                self.failed = true;
                return Err(e.into());
            }
        };
        self.started += 1;
        Ok(Request {
            key: self.key,
            file,
            answer: self.answer_to.clone(),
            done,
        })
    }
}

/// One barrier asked of the helper: its own descriptor of the file, where to
/// count the outcome, and who waits for it — nobody, for a windowed store's
/// periodic flush.
struct Request {
    key: (u64, u64),
    file: File,
    answer: mpsc::Sender<Answer>,
    done: Option<oneshot::Sender<StorageResult<()>>>,
}

/// A thread's `fsync` helper. Cheap to clone; the helper thread ends once
/// every clone is gone.
#[derive(Debug, Clone)]
pub struct Barrier {
    requests: mpsc::Sender<Request>,
}

impl Barrier {
    /// Start a helper thread named `name`.
    ///
    /// # Errors
    /// The OS refusing to start the thread.
    pub fn spawn(name: &str) -> std::io::Result<Self> {
        let (requests, queue) = mpsc::channel::<Request>();
        std::thread::Builder::new()
            .name(name.into())
            .spawn(move || serve(&queue))?;
        Ok(Self { requests })
    }

    /// Ask for a barrier on `file` covering everything written to it so far,
    /// and wait for it.
    pub(crate) fn sync(&self, file: &mut Synced) -> Durable {
        let (done, wait) = oneshot::channel();
        match file.request(Some(done)) {
            Ok(request) => self.send(request),
            // The descriptor could not be duplicated; the request never
            // existed, so its waiter is answered here.
            Err(e) => {
                let (done, wait) = oneshot::channel();
                let _ = done.send(Err(e));
                return Durable(wait);
            }
        }
        Durable(wait)
    }

    /// Ask for a barrier on `file` and do not wait — a windowed store's
    /// periodic flush, which no acknowledgement depends on. `false` when it
    /// could not even be asked; the next window asks again.
    pub(crate) fn flush(&self, file: &mut Synced) -> bool {
        file.request(None).map(|request| self.send(request)).is_ok()
    }

    fn send(&self, request: Request) {
        // Unbounded, so this never waits; closed only if the helper died, in
        // which case the dropped request answers its waiter with a fault.
        let _ = self.requests.send(request);
    }
}

/// The helper: take every queued request, sync each file once, answer.
fn serve(queue: &mpsc::Receiver<Request>) {
    while let Ok(first) = queue.recv() {
        let mut round = vec![first];
        round.extend(queue.try_iter());
        // Files in this round and how their one barrier went, in order.
        let mut synced: Vec<((u64, u64), Answer)> = Vec::new();
        for request in round {
            let earlier = synced.iter().find(|(key, _)| *key == request.key);
            let outcome = if let Some((_, outcome)) = earlier {
                *outcome
            } else {
                let outcome = request.file.sync_all().map_err(|e| e.kind());
                synced.push((request.key, outcome));
                outcome
            };
            // A journal gone since it asked has nobody left to count for.
            let _ = request.answer.send(outcome);
            if let Some(done) = request.done {
                let _ = done.send(outcome.map_err(|k| {
                    StorageError::Io(std::io::Error::new(k, "journal barrier"))
                }));
            }
        }
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
    use std::sync::mpsc;

    use super::{Barrier, Synced};

    fn file() -> (tempfile::TempDir, Synced) {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::File::create(dir.path().join("j.log")).unwrap();
        (dir, Synced::new(file).unwrap())
    }

    #[test]
    fn a_barrier_is_counted_when_it_starts_and_when_it_completes() {
        let (_dir, mut f) = file();
        assert_eq!((f.started(), f.completed()), (0, 0));
        f.sync().unwrap();
        assert_eq!((f.started(), f.completed()), (1, 1));
    }

    #[test]
    fn the_helper_syncs_and_answers() {
        let (_dir, mut f) = file();
        (&f.file).write_all(b"frame").unwrap();
        let barrier = Barrier::spawn("barrier-test").unwrap();
        let wait = barrier.sync(&mut f);
        assert_eq!(f.started(), 1, "counted as started when asked");
        futures::executor::block_on(wait).unwrap();
        assert_eq!(f.completed(), 1, "counted as completed when answered");
    }

    /// Requests queued together share one barrier per file: sixteen waits
    /// queued before the helper looks are served by one round, and every one
    /// is answered and counted.
    #[test]
    fn a_round_answers_every_request() {
        let (_dir, mut f) = file();
        let (requests, queue) = mpsc::channel();
        let barrier = Barrier { requests };
        let waits: Vec<_> = (0..16).map(|_| barrier.sync(&mut f)).collect();
        drop(barrier);
        super::serve(&queue); // one round, then the queue is closed
        for wait in waits {
            futures::executor::block_on(wait).unwrap();
        }
        assert_eq!((f.started(), f.completed()), (16, 16));
    }

    #[test]
    fn a_failed_answer_fails_the_file() {
        let (_dir, mut f) = file();
        assert!(f.check().is_ok());
        f.answer_to.send(Err(std::io::ErrorKind::Other)).unwrap();
        assert!(matches!(f.check(), Err(crate::StorageError::BarrierFailed)));
    }
}
