//! A data shard's side of its storage shard ([RFC 0073] §2–§3): where its
//! rounds' windows go.
//!
//! - **Its own files** ([`Storage::Own`]): the store is its storage shard's
//!   one data shard — the client cache, a test, a database of one storage
//!   shard per data shard. It allocates, writes and takes its barriers
//!   itself, as before the split.
//! - **A storage thread's** ([`Storage::Shared`]): a round reserves its
//!   window by message and awaits the answer — parked, not blocking — then
//!   hands the built window over and publishes it at once, its bytes held
//!   readable until they land (`crate::reader`). The runs it frees wait for
//!   the batch's seal, which carries them with the batch's waiters; the
//!   storage thread answers once the storage write holding the batch's end
//!   is durable (`crate::storage_shard`).
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::cell::{Cell, RefCell};
use std::sync::mpsc;

use futures::channel::oneshot;

use crate::barrier::Waiter;
use crate::block::{BlockAllocator, Run};
use crate::compact::Space;
use crate::error::{StorageError, StorageResult};
use crate::page_store::PageStore;
use crate::storage_shard::{Msg, Seal};

/// Where a data shard's windows go (see the module docs).
#[derive(Debug)]
pub enum Storage {
    /// Its own data files, and their allocator.
    Own(RefCell<BlockAllocator>),
    /// A storage thread shared with the storage shard's other data shards.
    Shared(Link),
}

/// A data shard's link to its storage thread.
#[derive(Debug)]
pub struct Link {
    inbox: mpsc::Sender<Msg>,
    /// Data shards writing through the storage shard.
    per: u64,
    /// The runs freed since the last seal, which carries them.
    frees: RefCell<Vec<Run>>,
    /// The next seal is waited on by a caller with nothing else to run.
    urgent: Cell<bool>,
    /// The storage shard's space, as the last answer told it.
    space: Cell<Space>,
}

impl Link {
    /// A link onto the storage thread whose inbox is `inbox`, which `per`
    /// data shards write through.
    #[must_use]
    pub fn new(inbox: mpsc::Sender<Msg>, per: usize) -> Self {
        Self {
            inbox,
            per: (per as u64).max(1),
            frees: RefCell::new(Vec::new()),
            urgent: Cell::new(false),
            space: Cell::new(Space::default()),
        }
    }

    fn send(&self, msg: Msg) -> StorageResult<()> {
        self.inbox.send(msg).map_err(|_| gone())
    }
}

/// The storage thread said goodbye — or crashed.
fn gone() -> StorageError {
    StorageError::Io(std::io::Error::other("the storage thread is gone"))
}

/// Closing the link is a data shard's goodbye: the storage thread ends once
/// every one of them said it — and the last one's waits for it to have
/// ended, so the files are free to open again when the drop returns.
impl Drop for Link {
    fn drop(&mut self) {
        let (goodbye, answered) = mpsc::channel();
        if self.inbox.send(Msg::Closed(goodbye)).is_ok() {
            let _ = answered.recv();
        }
    }
}

impl PageStore {
    /// The allocator of a store writing its own files.
    pub(crate) const fn own(&self) -> Option<&RefCell<BlockAllocator>> {
        match &self.storage {
            Storage::Own(alloc) => Some(alloc),
            Storage::Shared(_) => None,
        }
    }

    /// The window `blocks` long a round writes, reserved where its storage
    /// shard writes next.
    ///
    /// # Errors
    /// A storage thread gone.
    pub(crate) async fn reserve(&self, blocks: u64) -> StorageResult<Run> {
        let link = match &self.storage {
            Storage::Own(alloc) => return Ok(alloc.borrow_mut().alloc(blocks)),
            Storage::Shared(link) => link,
        };
        let (reply, answer) = oneshot::channel();
        let position = self.position as usize;
        link.send(Msg::Reserve {
            blocks,
            position,
            reply,
        })?;
        let reserved = answer.await.map_err(|_| gone())?;
        self.file.note_file(reserved.file);
        for file in reserved.gone {
            self.file.forget_file(file);
        }
        link.space.set(reserved.space);
        self.chain.borrow_mut().roots.newest = Some(reserved.roots);
        Ok(reserved.run)
    }

    /// This data shard's share of its storage shard's space: its own
    /// files', or a storage thread's divided among its data shards, as the
    /// last answer told it.
    pub(crate) const fn share_of_space(&self) -> Option<Space> {
        let Storage::Shared(link) = &self.storage else {
            return None;
        };
        let space = link.space.get();
        Some(Space {
            file_blocks: space.file_blocks / link.per,
            free_blocks: space.free_blocks / link.per,
            deferred_blocks: space.deferred_blocks / link.per,
        })
    }

    /// The files this data shard is to empty (RFC 0073 §7).
    ///
    /// # Errors
    /// A storage thread gone.
    pub(crate) async fn shared_victims(
        &self,
        percent: u8,
    ) -> StorageResult<Vec<(u64, u64)>> {
        let Storage::Shared(link) = &self.storage else {
            return Ok(Vec::new());
        };
        let (reply, answer) = oneshot::channel();
        let position = self.position as usize;
        link.send(Msg::Victims {
            percent,
            position,
            reply,
        })?;
        answer.await.map_err(|_| gone())
    }

    /// This data shard carried its runs out of the evacuation's files.
    ///
    /// # Errors
    /// A storage thread gone.
    pub(crate) fn evacuated(&self) -> StorageResult<()> {
        let Storage::Shared(link) = &self.storage else {
            return Ok(());
        };
        let position = self.position as usize;
        link.send(Msg::Evacuated { position })
    }

    /// Write a round's built window where it was reserved: on its own
    /// files at once; through the storage thread at its next write, the
    /// bytes readable here until they land.
    ///
    /// # Errors
    /// The write's fault; a storage thread gone.
    pub(crate) fn write_window(
        &self,
        window: Run,
        buf: Vec<u8>,
    ) -> StorageResult<()> {
        let link = match &self.storage {
            Storage::Own(alloc) => {
                let file = alloc.borrow().file_of(window.start).ok_or(
                    StorageError::Corrupt("a window outside every file"),
                )?;
                return self.file.write_run(file, window, &buf);
            }
            Storage::Shared(link) => link,
        };
        let batch = self.next_batch();
        self.file.hold(window, batch, buf.clone());
        link.send(Msg::Write { window, bytes: buf })
    }

    /// A reserved window that will never be written: a storage thread
    /// makes it a hole (a store's own files are failed by then).
    pub(crate) fn abandon(&self, window: Run) {
        if let Storage::Shared(link) = &self.storage {
            let _ = link.send(Msg::Abandon { window });
        }
    }

    /// A run no directory names any more: on disk, freed once the batch
    /// that stopped naming it is durable (`crate::alloc`) — through a
    /// storage thread, by the batch's seal; in RAM, at once.
    pub(crate) fn retire_run(&self, run: Run) {
        match &self.storage {
            Storage::Own(alloc) => {
                let mut alloc = alloc.borrow_mut();
                if self.has_disk() {
                    alloc.defer(run);
                } else {
                    alloc.free(run);
                }
            }
            Storage::Shared(link) => link.frees.borrow_mut().push(run),
        }
    }

    /// Hand batch `batch`, ended at header `root`, to the storage thread:
    /// answered — `waiters` and the store's barrier count — once the
    /// storage write holding its end is durable.
    ///
    /// # Errors
    /// A storage thread gone; a store writing its own files, which takes
    /// its barriers itself.
    pub(crate) fn seal(
        &self,
        batch: u64,
        root: u64,
        waiters: Vec<Waiter>,
    ) -> StorageResult<()> {
        let Storage::Shared(link) = &self.storage else {
            return Err(StorageError::Corrupt("a seal of a store's own files"));
        };
        let report = self.cycle.borrow_mut().syncs.ask(batch);
        let seal = Seal {
            position: self.position as usize,
            root,
            batch,
            frees: std::mem::take(&mut *link.frees.borrow_mut()),
            urgent: link.urgent.replace(false) || !waiters.is_empty(),
            waiters,
            report,
        };
        link.send(Msg::Seal(seal))
    }

    /// The next seal is waited on by a caller with nothing else to run
    /// (`drain`): the storage thread writes it at once.
    pub(crate) fn hurry(&self) {
        if let Storage::Shared(link) = &self.storage {
            link.urgent.set(true);
        }
    }
}
