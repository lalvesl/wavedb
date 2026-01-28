//! The settle's barrier off the writer's thread ([RFC 0072] §2, the helper
//! of [RFC 0067] §4).
//!
//! A batch is durable once the data files its settle wrote are `fsync`ed,
//! and an `fsync` is a blocking call that takes as long as the device says.
//! Taken inline, it stops the thread that took it — on a node, a worker, and
//! with it every other owner that worker serves. So a store can hand its
//! barriers to a [`Barrier`]: a helper thread that does nothing but `fsync`.
//! The settle asks for one when it ends a batch and goes on; every durable
//! write the batch carries **waits** for the answer, its task parked — the
//! worker's executor runs other tasks meanwhile — and its owner locked, so
//! nobody reads it before it is durable.
//!
//! The helper takes whatever requests have queued since its last round and
//! syncs each file **once** for all of them — `fsync` flushes the file, not
//! one write — and answers every waiter each request carries.
//!
//! ## Nothing shared
//!
//! The helper and the worker share no memory. A request carries its own
//! duplicates of the data files' descriptors (an `fsync` on any descriptor
//! of a file flushes that file), its waiters, and a channel to report on;
//! the store's count of answered batches and its failed mark are the
//! worker's alone, as plain fields, read only when the worker looks. So
//! there is no lock and no atomic on either side of the seam — only the
//! channels the messages travel through.
//!
//! ## A failed barrier
//!
//! An `fsync` that failed leaves the files' state unknown. Every waiter of
//! that batch gets the fault, and the store is marked failed: every later
//! write is refused ([`StorageError::BarrierFailed`]) until it is reopened
//! and reconstructed from what is actually on disk.
//!
//! [RFC 0067]: ../../../rfcs/0067-thread-per-core-shards-WIP.md
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md

use std::fs::File;
use std::future::Future;
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};

use futures::channel::oneshot;

use crate::error::{StorageError, StorageResult};

/// How one barrier went, as the helper reports it.
type Answer = Result<(), ErrorKind>;

/// A waiter for a batch to be durable — the sending half of a [`Durable`].
pub type Waiter = oneshot::Sender<StorageResult<()>>;

/// The batches a store asked barriers for, and how they went — owned by the
/// worker that writes the store (see the module docs).
#[derive(Debug)]
pub struct Syncs {
    /// The newest batch a barrier was asked for, or taken inline.
    asked: u64,
    /// The newest batch known durable.
    answered: u64,
    failed: bool,
    /// Where the helper reports, and the end each request carries.
    reports: mpsc::Receiver<(u64, Answer)>,
    report_to: mpsc::Sender<(u64, Answer)>,
}

impl Default for Syncs {
    fn default() -> Self {
        let (report_to, reports) = mpsc::channel();
        Self {
            asked: 0,
            answered: 0,
            failed: false,
            reports,
            report_to,
        }
    }
}

impl Syncs {
    /// Read what the helper reported since the last look.
    fn read_reports(&mut self) {
        while let Ok((batch, answer)) = self.reports.try_recv() {
            match answer {
                Ok(()) => self.answered = self.answered.max(batch),
                Err(_) => self.failed = true,
            }
        }
    }

    /// The newest batch known durable.
    pub(crate) fn answered(&mut self) -> u64 {
        self.read_reports();
        self.answered
    }

    /// Whether every batch asked for is durable.
    pub(crate) fn settled(&mut self) -> bool {
        self.answered() >= self.asked
    }

    /// Refuse to go on once a barrier failed.
    ///
    /// # Errors
    /// [`StorageError::BarrierFailed`].
    pub(crate) fn check(&mut self) -> StorageResult<()> {
        self.read_reports();
        if self.failed {
            return Err(StorageError::BarrierFailed);
        }
        Ok(())
    }

    /// Mark the store failed: a write's state on disk is unknown.
    pub(crate) const fn fail(&mut self) {
        self.failed = true;
    }

    /// Block until every batch asked for is answered — or a barrier failed.
    pub(crate) fn wait(&mut self) {
        self.read_reports();
        while self.answered < self.asked && !self.failed {
            match self.reports.recv() {
                Ok((batch, Ok(()))) => {
                    self.answered = self.answered.max(batch);
                }
                Ok((_, Err(_))) | Err(_) => self.failed = true,
            }
        }
    }

    /// Batch `batch` was synced on the calling thread, or failed to be.
    pub(crate) fn done_inline(&mut self, batch: u64, ok: bool) {
        self.asked = self.asked.max(batch);
        if ok {
            self.answered = self.answered.max(batch);
        } else {
            self.failed = true;
        }
    }

    /// The request for batch `batch`'s barrier over `files`, answering
    /// `waiters` — counted as asked.
    fn request(
        &mut self,
        batch: u64,
        files: Vec<File>,
        waiters: Vec<Waiter>,
    ) -> Request {
        self.asked = self.asked.max(batch);
        Request {
            batch,
            files,
            report: self.report_to.clone(),
            waiters,
        }
    }
}

/// One barrier asked of the helper: its own descriptors of the files, where
/// to report, and who waits for it.
struct Request {
    batch: u64,
    files: Vec<File>,
    report: mpsc::Sender<(u64, Answer)>,
    waiters: Vec<Waiter>,
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

    /// Ask for batch `batch`'s barrier over `files` and go on; `waiters`
    /// are answered when it is taken.
    pub(crate) fn sync(
        &self,
        syncs: &mut Syncs,
        batch: u64,
        files: Vec<File>,
        waiters: Vec<Waiter>,
    ) {
        let request = syncs.request(batch, files, waiters);
        // Unbounded, so this never waits; closed only if the helper died, in
        // which case the dropped request answers its waiters with a fault
        // ([`Durable`]) and its batch is never reported durable.
        let _ = self.requests.send(request);
    }
}

/// The identity of a file, so a round syncs it once whatever descriptor
/// each request carries.
fn key(file: &File) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    file.metadata().ok().map(|m| (m.dev(), m.ino()))
}

/// The helper: take every queued request, sync each file once, answer.
fn serve(queue: &mpsc::Receiver<Request>) {
    while let Ok(first) = queue.recv() {
        let mut round = vec![first];
        round.extend(queue.try_iter());
        // Files in this round and how their one barrier went.
        let mut synced: Vec<((u64, u64), Answer)> = Vec::new();
        for request in round {
            let mut outcome: Answer = Ok(());
            for file in &request.files {
                let id = key(file);
                let earlier = id.and_then(|id| {
                    synced.iter().find(|(k, _)| *k == id).map(|(_, a)| *a)
                });
                let this = earlier.unwrap_or_else(|| {
                    let done = file.sync_all().map_err(|e| e.kind());
                    if let Some(id) = id {
                        synced.push((id, done));
                    }
                    done
                });
                outcome = outcome.and(this);
            }
            // A store gone since it asked has nobody left to count for.
            let _ = request.report.send((request.batch, outcome));
            for waiter in request.waiters {
                let _ = waiter.send(outcome.map_err(|k| {
                    StorageError::Io(std::io::Error::new(k, "settle barrier"))
                }));
            }
        }
    }
}

/// A durable write's wait for its batch's barrier.
#[derive(Debug)]
pub struct Durable(pub(crate) oneshot::Receiver<StorageResult<()>>);

impl Durable {
    /// A wait and the waiter that ends it.
    pub(crate) fn new() -> (Waiter, Self) {
        let (waiter, wait) = oneshot::channel();
        (waiter, Self(wait))
    }
}

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

    use super::{Barrier, Durable, Syncs};

    fn file() -> (tempfile::TempDir, std::fs::File) {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::File::create(dir.path().join("d")).unwrap();
        (dir, file)
    }

    #[test]
    fn the_helper_syncs_answers_and_reports() {
        let (_dir, mut f) = file();
        f.write_all(b"page").unwrap();
        let barrier = Barrier::spawn("barrier-test").unwrap();
        let mut syncs = Syncs::default();
        let (waiter, wait) = Durable::new();
        let files = vec![f.try_clone().unwrap()];
        barrier.sync(&mut syncs, 1, files, vec![waiter]);
        futures::executor::block_on(wait).unwrap();
        // The report is sent before the waiters are answered.
        assert_eq!(syncs.answered(), 1);
    }

    /// Requests queued together share one barrier per file, and every
    /// waiter of every request is answered.
    #[test]
    fn a_round_answers_every_waiter() {
        let (_dir, f) = file();
        let (requests, queue) = mpsc::channel();
        let barrier = Barrier { requests };
        let mut syncs = Syncs::default();
        let mut waits = Vec::new();
        for batch in 1..=4 {
            let (waiters, ws): (Vec<_>, Vec<_>) =
                (0..4).map(|_| Durable::new()).unzip();
            let files = vec![f.try_clone().unwrap()];
            barrier.sync(&mut syncs, batch, files, waiters);
            waits.extend(ws);
        }
        drop(barrier);
        super::serve(&queue); // one round, then the queue is closed
        for wait in waits {
            futures::executor::block_on(wait).unwrap();
        }
        assert_eq!(syncs.answered(), 4);
        assert!(syncs.settled());
    }

    #[test]
    fn a_failed_report_fails_the_store() {
        let mut syncs = Syncs::default();
        assert!(syncs.check().is_ok());
        syncs
            .report_to
            .send((1, Err(std::io::ErrorKind::Other)))
            .unwrap();
        assert!(matches!(
            syncs.check(),
            Err(crate::StorageError::BarrierFailed)
        ));
    }
}
