//! A storage shard's chain of files in the object store ([RFC 0073] §8):
//! one thread per storage shard assembles its data shards' uploads into
//! files and PUTs them — one PUT for every data shard that uploaded
//! meanwhile, where each used to make its own.
//!
//! - **`Reserve`** places an upload's sections — a header and its pages
//!   each, laid out by the data shard ([`crate::upload_file`]) — in the open
//!   file, one after the other, while each fits inside the file's 8 MiB of
//!   address space; one that does not opens the next file, at its first
//!   block past the roots block. A section larger than a file has one of
//!   its own, its last page running past (RFC 0070 §3). Answered at once
//!   with the bases and where the chain stands.
//! - **`Write`** hands the built sections over, with the header ending a
//!   batch if the upload is a cut.
//! - **The tick** (1 s), an upload someone waits on, or a full file sends
//!   the files down, oldest first, one at a time, each once every section
//!   reserved in it is written: the roots block at its head names every data
//!   shard's newest cut — a cut only once every file of its upload is in —
//!   and the PUT is create-only, so a file already there is another
//!   writer's: the storage shard is fenced.
//! - **An upload is answered** once every file holding its sections is in;
//!   a PUT that failed fails every upload in it, whose numbers are never
//!   written again.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use futures::channel::oneshot;

use crate::error::{StorageError, StorageResult};
use crate::header::NO_FILE;
use crate::remote::{FILE_BLOCKS, Remote};
use crate::remote_shard_compact::{CompactAnswer, Report};
use crate::roots::ROOTS_BLOCKS;

/// How long uploads gather before their files go down.
const TICK: Duration = Duration::from_secs(1);

/// What a data shard tells its storage shard's remote thread.
#[derive(Debug)]
pub enum RemoteMsg {
    /// Room for an upload's sections, `sizes` blocks each, in order.
    Reserve {
        sizes: Vec<u64>,
        reply: oneshot::Sender<RemoteReserved>,
    },
    /// An upload's sections, built at their bases, in order — the last
    /// one's header ending a batch when `cut`.
    Write {
        position: usize,
        sections: Vec<(u64, Vec<u8>)>,
        cut: bool,
        urgent: bool,
        reply: oneshot::Sender<StorageResult<()>>,
    },
    /// Reserved sections that will never be written.
    Abandon { bases: Vec<u64> },
    /// A data shard compacts: what it still needs below its horizon
    /// (`crate::remote_shard_compact`).
    Compact {
        position: usize,
        report: Report,
        reply: oneshot::Sender<CompactAnswer>,
    },
    /// A data shard closed its link; told once the thread is done, if it
    /// was the last.
    Closed(mpsc::Sender<()>),
}

/// A reservation, answered.
#[derive(Debug, Clone)]
pub struct RemoteReserved {
    /// Where each section starts.
    pub bases: Vec<u64>,
    /// The newest roots block written, which the sections' headers name.
    pub roots: u64,
    /// Where the chain of files stands.
    pub start: u64,
    pub end: u64,
}

/// A file being filled, or waiting to go down.
pub struct File {
    pub(crate) number: u64,
    /// The next block a section starts at.
    pub(crate) cursor: u64,
    /// Section base → its bytes once written (`None`: reserved only).
    pub(crate) sections: BTreeMap<u64, Option<Vec<u8>>>,
    /// No section is placed in it any more.
    pub(crate) closed: bool,
    /// When its newest section was reserved.
    pub(crate) since: Instant,
}

impl File {
    const fn first(number: u64) -> u64 {
        number * FILE_BLOCKS + ROOTS_BLOCKS
    }

    pub(crate) fn ready(&self) -> bool {
        self.sections.values().all(Option::is_some)
    }
}

/// An upload waiting for its files.
pub struct Waiting {
    pub(crate) position: usize,
    pub(crate) files: BTreeSet<u64>,
    /// The header ending its batch, if it is a cut.
    pub(crate) cut: Option<u64>,
    pub(crate) failed: Option<StorageError>,
    pub(crate) reply: oneshot::Sender<StorageResult<()>>,
}

/// A storage shard's remote thread (see the module docs).
pub struct RemoteShard {
    pub(crate) remote: Remote,
    pub(crate) end: u64,
    pub(crate) start: u64,
    /// Per data shard, by position: its newest cut in the object store.
    pub(crate) by_shard: Vec<u64>,
    /// The newest roots block PUT.
    pub(crate) newest: Option<u64>,
    pub(crate) files: VecDeque<File>,
    pub(crate) waiting: Vec<Waiting>,
    pub(crate) urgent: bool,
    pub(crate) fenced: bool,
    /// Per data shard, the latest compaction report.
    pub(crate) reports: Vec<Option<Report>>,
    /// Files deleted above `start`: not deleted again.
    pub(crate) deleted: BTreeSet<u64>,
}

impl RemoteShard {
    /// The chain under `remote`, standing at `start..end`, whose data shards'
    /// cuts are `by_shard` and whose newest roots block is `newest`.
    #[must_use]
    pub const fn new(
        remote: Remote,
        (start, end): (u64, u64),
        by_shard: Vec<u64>,
        newest: Option<u64>,
    ) -> Self {
        Self {
            remote,
            end,
            start,
            reports: Vec::new(),
            deleted: BTreeSet::new(),
            by_shard,
            newest,
            files: VecDeque::new(),
            waiting: Vec::new(),
            urgent: false,
            fenced: false,
        }
    }

    /// Start the thread, named `name`; `links` data shards upload through
    /// it.
    ///
    /// # Errors
    /// The OS refusing to start a thread.
    pub fn spawn(
        self,
        name: &str,
        links: usize,
    ) -> std::io::Result<mpsc::Sender<RemoteMsg>> {
        let (inbox, queue) = mpsc::channel::<RemoteMsg>();
        std::thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                if let Some(last) = self.run(&queue, links) {
                    let _ = last.send(());
                }
            })?;
        Ok(inbox)
    }

    fn run(
        mut self,
        inbox: &mpsc::Receiver<RemoteMsg>,
        mut links: usize,
    ) -> Option<mpsc::Sender<()>> {
        let mut tick = Instant::now() + TICK;
        let mut last = None;
        while links > 0 || !self.waiting.is_empty() {
            let wait = tick.saturating_duration_since(Instant::now());
            let due = match inbox.recv_timeout(wait) {
                Ok(RemoteMsg::Closed(goodbye)) => {
                    links = links.saturating_sub(1);
                    if links == 0 {
                        last = Some(goodbye);
                    } else {
                        let _ = goodbye.send(());
                    }
                    links == 0
                }
                Ok(msg) => {
                    self.take(msg);
                    false
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    tick = Instant::now() + TICK;
                    true
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return last,
            };
            let now = due || std::mem::take(&mut self.urgent);
            self.send_down(now);
        }
        last
    }

    fn take(&mut self, msg: RemoteMsg) {
        match msg {
            RemoteMsg::Reserve { sizes, reply } => {
                let bases = sizes.iter().map(|&s| self.place(s)).collect();
                let _ = reply.send(RemoteReserved {
                    bases,
                    roots: self.newest.unwrap_or(NO_FILE),
                    start: self.start,
                    end: self.end,
                });
            }
            RemoteMsg::Write {
                position,
                sections,
                cut,
                urgent,
                reply,
            } => self.write(position, sections, cut, urgent, reply),
            RemoteMsg::Abandon { bases } => {
                for base in bases {
                    self.fill(base, Vec::new());
                }
            }
            RemoteMsg::Compact {
                position,
                report,
                reply,
            } => {
                let _ = reply.send(self.compact(position, report));
            }
            RemoteMsg::Closed(goodbye) => {
                let _ = goodbye.send(());
            }
        }
    }

    /// Place a section `blocks` long: in the open file if it fits inside
    /// its address space, or alone at the head of the next.
    fn place(&mut self, blocks: u64) -> u64 {
        let open = self.files.back().filter(|f| !f.closed).map(|f| {
            let fits = f.cursor + blocks <= (f.number + 1) * FILE_BLOCKS;
            (fits || f.cursor == File::first(f.number), f.number)
        });
        if !matches!(open, Some((true, _))) {
            if let Some(file) = self.files.back_mut() {
                file.closed = true;
            }
            let number = self.end.div_ceil(FILE_BLOCKS);
            self.files.push_back(File {
                number,
                cursor: File::first(number),
                sections: BTreeMap::new(),
                closed: false,
                since: Instant::now(),
            });
        }
        let Some(file) = self.files.back_mut() else {
            return NO_FILE; // unreachable: one was just opened
        };
        file.since = Instant::now();
        let base = file.cursor;
        file.cursor += blocks;
        file.sections.insert(base, None);
        self.end = self.end.max(file.cursor);
        base
    }

    /// The sections of an upload, written.
    fn write(
        &mut self,
        position: usize,
        sections: Vec<(u64, Vec<u8>)>,
        cut: bool,
        urgent: bool,
        reply: oneshot::Sender<StorageResult<()>>,
    ) {
        if self.fenced {
            let _ = reply.send(Err(StorageError::Fenced));
            return;
        }
        let files: BTreeSet<u64> = sections
            .iter()
            .map(|(base, _)| crate::remote::file_of(*base))
            .collect();
        if files
            .iter()
            .any(|&n| self.files.iter().all(|f| f.number != n))
        {
            // Too late: the file went down with a hole where it was.
            let _ = reply.send(Err(StorageError::Corrupt(
                "an upload outlived its reservation",
            )));
            return;
        }
        let cut = sections.last().filter(|_| cut).map(|(base, _)| *base);
        for (base, bytes) in sections {
            self.fill(base, bytes);
        }
        self.urgent |= urgent;
        self.waiting.push(Waiting {
            position,
            files,
            cut,
            failed: None,
            reply,
        });
    }

    fn fill(&mut self, base: u64, bytes: Vec<u8>) {
        let file = crate::remote::file_of(base);
        if let Some(f) = self.files.iter_mut().find(|f| f.number == file) {
            f.sections.insert(base, Some(bytes));
        }
    }
}
