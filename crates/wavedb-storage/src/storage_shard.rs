//! A storage shard's thread ([RFC 0073] §4): the one owner of its data
//! files and their allocator, which every data shard of the storage shard
//! writes through.
//!
//! A plain OS thread, no executor, its inbox a channel every data shard
//! sends on without waiting:
//!
//! - **`Reserve`** is answered at once — allocation is arithmetic in RAM —
//!   with the run, the file it lies in, and the newest roots block, which
//!   the round's header names.
//! - **`Write`** hands over a built window; **`Seal`** says the header
//!   ending a data shard's batch was in its last one, with the runs the
//!   batch stopped naming and who waits for it. Both gather in RAM.
//! - **The tick** (200 ms) writes what gathered — the windows by address,
//!   then a **roots block** naming every data shard's root — and hands the
//!   `fsync` to its helper, whose answer comes back on the same inbox: the
//!   thread is never blocked on the device while there are reservations to
//!   answer. A seal someone waits on goes at once; so do 8 MiB gathered.
//! - **One storage write in flight**: the next is written once the one
//!   before is durable, so only the newest can be cut by a crash, and its
//!   roots block lands after every write before it is durable.
//! - **Durable**: every seal of the write is answered — its batch reported
//!   to its data shard, its waiters woken — its frees become holes, and
//!   the roots block before it is dead.
//!
//! Nothing shared: messages carry what they need, and the answers go back on
//! channels. The thread ends once every data shard closed its link and what
//! they wrote is durable.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::collections::BTreeSet;
use std::fs::File;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use futures::channel::oneshot;

use crate::barrier::{Answer, Waiter};
use crate::block::{BlockAllocator, Run};
use crate::block_file::BlockFile;
use crate::compact::Space;
use crate::header::NO_FILE;
use crate::storage_write::{Flight, sync_files};

/// How often the gathered writes go down.
const TICK: Duration = Duration::from_millis(200);

/// Bytes gathered that make a write go before its tick.
const GATHER_BYTES: u64 = 8 << 20;

/// What a data shard, or the `fsync` helper, tells a storage thread.
#[derive(Debug)]
pub enum Msg {
    /// A window `blocks` long, reserved where the storage shard writes next.
    Reserve {
        blocks: u64,
        position: usize,
        reply: oneshot::Sender<Reserved>,
    },
    /// A reserved window, built.
    Write { window: Run, bytes: Vec<u8> },
    /// A reserved window that will never be written.
    Abandon { window: Run },
    /// A data shard's batch ended in its last window.
    Seal(Seal),
    /// The `fsync` of storage write `flight` answered.
    Synced { flight: u64, ok: bool },
    /// A data shard closed its link; told so once nothing of the storage
    /// shard holds its files any more, if it was the last.
    Closed(mpsc::Sender<()>),
    /// A data shard compacts (RFC 0073 §7): which files to empty — the
    /// running evacuation's, or a new one of every file at least
    /// `percent` free.
    Victims {
        percent: u8,
        position: usize,
        reply: oneshot::Sender<Vec<(u64, u64)>>,
    },
    /// A data shard carried its runs out of the evacuation's files, and a
    /// batch over the moves is durable.
    Evacuated { position: usize },
}

/// A reservation, answered.
#[derive(Debug, Clone)]
pub struct Reserved {
    pub run: Run,
    /// The data file it lies in, by first block.
    pub file: u64,
    /// The newest roots block written, which the round's header names.
    pub roots: u64,
    /// How the storage shard's blocks are used.
    pub space: Space,
    /// Data files deleted since this data shard's last reservation, whose
    /// handles it closes.
    pub gone: Vec<u64>,
}

/// A data shard's batch, ended in its last window.
#[derive(Debug)]
pub struct Seal {
    pub position: usize,
    /// The header that ends the batch: the data shard's root from now.
    pub root: u64,
    pub batch: u64,
    /// The runs the batch stopped naming.
    pub frees: Vec<Run>,
    pub waiters: Vec<Waiter>,
    /// Where the data shard hears that `batch` is durable.
    pub report: mpsc::Sender<(u64, Answer)>,
    /// Somebody waits: write it now.
    pub urgent: bool,
}

/// The files an evacuation empties, by `(first block, end)`, and the data
/// shards done with them (`crate::storage_compact`).
pub type Evacuation = (Vec<(u64, u64)>, BTreeSet<usize>);

/// A storage thread's state (see the module docs).
pub struct StorageShard {
    pub(crate) file: BlockFile,
    pub(crate) alloc: BlockAllocator,
    /// Per data shard, by position: its root.
    pub(crate) by_shard: Vec<u64>,
    /// The newest roots block written.
    pub(crate) newest: Option<u64>,
    pub(crate) gathered: Vec<(Run, Vec<u8>)>,
    pub(crate) gathered_bytes: u64,
    pub(crate) seals: Vec<Seal>,
    pub(crate) urgent: bool,
    pub(crate) flight: Option<Flight>,
    pub(crate) flights: u64,
    pub(crate) syncs: mpsc::Sender<(u64, Vec<File>)>,
    pub(crate) failed: bool,
    /// The files being emptied, and the data shards done with them.
    pub(crate) evacuation: Option<Evacuation>,
    /// Every data file deleted, in order, and per data shard how many of
    /// them it was told of.
    pub(crate) deleted: Vec<u64>,
    pub(crate) told: Vec<usize>,
}

impl StorageShard {
    /// A storage shard over `file` and its rebuilt `alloc`, whose data
    /// shards' roots are `by_shard` in the roots block `newest`.
    #[must_use]
    pub fn new(
        file: BlockFile,
        alloc: BlockAllocator,
        by_shard: Vec<u64>,
        newest: Option<u64>,
    ) -> (Self, mpsc::Receiver<(u64, Vec<File>)>) {
        let (syncs, jobs) = mpsc::channel();
        let told = vec![0; by_shard.len()];
        let shard = Self {
            file,
            alloc,
            by_shard,
            newest,
            gathered: Vec::new(),
            gathered_bytes: 0,
            seals: Vec::new(),
            urgent: false,
            flight: None,
            flights: 0,
            syncs,
            failed: false,
            evacuation: None,
            deleted: Vec::new(),
            told,
        };
        (shard, jobs)
    }

    /// Start the thread, and its `fsync` helper, named after `name`;
    /// `links` data shards write through it.
    ///
    /// # Errors
    /// The OS refusing to start a thread.
    pub fn spawn(
        self,
        jobs: mpsc::Receiver<(u64, Vec<File>)>,
        name: &str,
        links: usize,
    ) -> std::io::Result<mpsc::Sender<Msg>> {
        let (inbox, queue) = mpsc::channel::<Msg>();
        let report = inbox.clone();
        std::thread::Builder::new()
            .name(format!("{name}-fsync"))
            .spawn(move || sync_files(&jobs, &report))?;
        std::thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                // The last data shard's goodbye waits for the files' locks
                // to go with the thread's state.
                if let Some(last) = self.run(&queue, links) {
                    let _ = last.send(());
                }
            })?;
        Ok(inbox)
    }

    /// The thread's loop: every message, and the tick — until every data
    /// shard closed its link and what they wrote is durable. The last one's
    /// goodbye comes back, to be answered once the state is dropped.
    fn run(
        mut self,
        inbox: &mpsc::Receiver<Msg>,
        mut links: usize,
    ) -> Option<mpsc::Sender<()>> {
        let mut tick = Instant::now() + TICK;
        let mut last = None;
        while links > 0 || self.flight.is_some() || self.owes() {
            let wait = tick.saturating_duration_since(Instant::now());
            match inbox.recv_timeout(wait) {
                Ok(Msg::Closed(goodbye)) => {
                    links = links.saturating_sub(1);
                    if links == 0 {
                        last = Some(goodbye);
                    } else {
                        let _ = goodbye.send(());
                    }
                }
                Ok(msg) => self.take(msg),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    tick = Instant::now() + TICK;
                    self.flush();
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return last,
            }
            if self.urgent
                || self.gathered_bytes >= GATHER_BYTES
                || (links == 0 && self.owes())
            {
                self.flush();
            }
        }
        last
    }

    /// Whether anything waits to be written.
    pub(crate) const fn owes(&self) -> bool {
        !self.gathered.is_empty() || !self.seals.is_empty()
    }

    fn take(&mut self, msg: Msg) {
        match msg {
            Msg::Reserve {
                blocks,
                position,
                reply,
            } => {
                let run = self.alloc.alloc(blocks);
                let _ = reply.send(Reserved {
                    run,
                    file: self.alloc.file_of(run.start).unwrap_or(run.start),
                    roots: self.newest.unwrap_or(NO_FILE),
                    space: self.space(),
                    gone: self.gone_for(position),
                });
            }
            Msg::Write { window, bytes } => {
                self.gathered_bytes += bytes.len() as u64;
                self.gathered.push((window, bytes));
            }
            Msg::Abandon { window } => self.alloc.free(window),
            Msg::Seal(seal) => {
                self.urgent |= seal.urgent;
                self.seals.push(seal);
            }
            Msg::Synced { flight, ok } => self.synced(flight, ok),
            Msg::Victims {
                percent,
                position,
                reply,
            } => {
                let _ = reply.send(self.victims(percent, position));
            }
            Msg::Evacuated { position } => self.evacuated(position),
            Msg::Closed(goodbye) => {
                let _ = goodbye.send(());
            }
        }
    }
}
