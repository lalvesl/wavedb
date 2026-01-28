//! The native cache on a carrier of its own ([RFC 0072] §11).
//!
//! The engine behind the cache settles on a tick, as a node's does: a write
//! commits to its record cache and becomes durable at the next 200 ms
//! settle cycle. A tick needs a task, and the application's thread is not
//! ours to run one on — so the [`PageStore`] lives on a carrier thread (one
//! Embassy executor, RFC 0066) whose one task serves the store and ticks
//! it, and the `Db` reaches it by channel, as it reaches the connection
//! manager.
//!
//! What crosses is the [`Store`] seam itself — a read, a batch — so every
//! typed operation (mirrors, adoption, the warm walks) stays where it was,
//! over this proxy. A durable batch is applied durably there: the store has
//! no barrier helper, so the carrier settles it inline, `fsync` included,
//! and the application's thread only waits for the answer.
//!
//! Dropping the proxy closes the channel; the task settles what the store
//! holds, drops it, and the drop returns only then, so a reopen of the same
//! directory finds it free.
//!
//! [RFC 0072]: https://github.com/wavedb/wavedb/blob/main/rfcs/0072-the-settle-is-the-durability-point.md

use std::time::{Duration, Instant};

use async_channel::{Receiver, Sender};
use embassy_futures::select::{Either, select};
use futures::channel::oneshot;
use wavedb_core::{Error, Id, Result, Store, Write};
use wavedb_platform::carrier::{self, Here};
use wavedb_platform::time::sleep;
use wavedb_storage::{PageStore, StorageRegistry};

/// How often the cache settles what it committed — a node's tick.
pub const SETTLE_TICK: Duration = Duration::from_millis(200);

/// How many caches one process may hold open at once: the carrier task's
/// static pool (Embassy's tasks come from pools, never the heap).
const MAX_CACHES: usize = 32;

/// One request to the cache's carrier.
enum Ask {
    Get {
        struct_hash: Option<u64>,
        id: Id,
        answer: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    Apply {
        batch: Vec<Write>,
        durable: bool,
        answer: oneshot::Sender<Result<()>>,
    },
}

/// The native cache store: a [`Store`] whose engine runs on its own
/// carrier thread.
#[derive(Debug)]
pub struct CacheStore {
    asks: Sender<Ask>,
    /// Hung up by the carrier's task once the store is settled and dropped.
    closed: std::sync::mpsc::Receiver<()>,
}

impl CacheStore {
    /// Open the engine rooted at `dir` — registering exactly the client
    /// registry's declared types — and start its carrier.
    ///
    /// # Errors
    /// The engine's open faults, converted at the documented seam
    /// (`StorageError` → `core::Error::Backend`); a carrier that cannot
    /// start, or [`MAX_CACHES`] already open.
    pub(crate) fn open(
        registry: &impl StorageRegistry,
        dir: &std::path::Path,
    ) -> Result<Self> {
        let store = PageStore::open(dir, &registry.storage_entries())?;
        let (asks, inbox) = async_channel::unbounded();
        let (done, closed) = std::sync::mpsc::channel();
        let (started, admitted) = std::sync::mpsc::channel();
        carrier::spawn("wavedb-cache", move |here| {
            let task = serve(store, inbox, done, here.clone());
            let _ = started.send(here.spawn(task).is_ok());
        })
        .map_err(|e| Error::Backend(format!("cache carrier: {e}")))?;
        if admitted.recv() != Ok(true) {
            return Err(Error::Backend(format!(
                "cache carrier: {MAX_CACHES} caches already open"
            )));
        }
        Ok(Self { asks, closed })
    }

    /// Send `ask` and wait for its answer.
    async fn ask<T>(
        &self,
        ask: impl FnOnce(oneshot::Sender<Result<T>>) -> Ask,
    ) -> Result<T> {
        let (answer, answered) = oneshot::channel();
        self.asks.send(ask(answer)).await.map_err(|_| gone())?;
        answered.await.map_err(|_| gone())?
    }
}

impl Drop for CacheStore {
    fn drop(&mut self) {
        self.asks.close();
        // A plain channel, not an executor: this may run inside one.
        let _ = self.closed.recv();
    }
}

impl Store for CacheStore {
    async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
        self.ask(|answer| Ask::Get {
            struct_hash: None,
            id,
            answer,
        })
        .await
    }

    async fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> Result<Option<Vec<u8>>> {
        self.ask(|answer| Ask::Get {
            struct_hash: Some(struct_hash),
            id,
            answer,
        })
        .await
    }

    async fn apply(&self, batch: &[Write]) -> Result<()> {
        self.ask(|answer| Ask::Apply {
            batch: batch.to_vec(),
            durable: false,
            answer,
        })
        .await
    }

    async fn apply_durable(&self, batch: &[Write]) -> Result<()> {
        self.ask(|answer| Ask::Apply {
            batch: batch.to_vec(),
            durable: true,
            answer,
        })
        .await
    }
}

/// The cache's carrier gone — it only ends once the proxy is dropped, so
/// this is a carrier that died.
fn gone() -> Error {
    Error::Backend(String::from("cache carrier stopped"))
}

/// The cache's one task: serve every ask in turn, and settle on the tick.
#[wavedb_platform::carrier::rt::task(
    pool_size = MAX_CACHES,
    embassy_executor = wavedb_platform::carrier::rt
)]
async fn serve(
    store: PageStore,
    inbox: Receiver<Ask>,
    done: std::sync::mpsc::Sender<()>,
    here: Here,
) {
    let mut due = Instant::now() + SETTLE_TICK;
    loop {
        let wait = due.saturating_duration_since(Instant::now());
        match select(inbox.recv(), sleep(wait)).await {
            Either::First(Ok(ask)) => answer(&store, ask).await,
            Either::First(Err(_)) => break,
            Either::Second(()) => {
                tick(&store);
                due = Instant::now() + SETTLE_TICK;
            }
        }
    }
    drop(store); // a store dropped settles what it holds
    drop(done);
    drop(here);
}

/// Serve one ask. A disk-only store's futures resolve at once, so the asks
/// are served whole, one after another.
async fn answer(store: &PageStore, ask: Ask) {
    match ask {
        Ask::Get {
            struct_hash,
            id,
            answer,
        } => {
            let got = match struct_hash {
                Some(hash) => store.get_of(hash, id).await,
                None => store.get(id).await,
            };
            let _ = answer.send(got);
        }
        Ask::Apply {
            batch,
            durable,
            answer,
        } => {
            let applied = if durable {
                Store::apply_durable(store, &batch).await
            } else {
                store.apply(&batch).await
            };
            let _ = answer.send(applied);
        }
    }
}

/// The tick: settle what was committed — nothing at all when nothing was —
/// and let the record cache down to its budget. A failed settle fails the
/// store, which the next write reports.
fn tick(store: &PageStore) {
    let _ = store.drain();
    store.evict_to_budget();
}
