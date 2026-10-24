//! `wavedb-quick-node` — the serving + storage node.
//!
//! **Server and database are the same binary.** A node links a schema
//! crate's `expose_server!` output (`REGISTRY`) — which carries both halves
//! it needs: the dispatch surface ([`Exposure`]) and the storage surface
//! ([`StorageRegistry`], the [`StructStorage`] slots to open the engine with)
//! — binds an HTTP POST socket, and serves records out of its sharded
//! database (`MANIFEST` + `shards/xx`, one `PageStore` each — RFC 0067).
//!
//! ```no_run
//! # async fn run<E>(registry: E) -> wavedb_quick_node::Result<()>
//! # where E: wavedb_quick_node::NodeTasks + wavedb_storage::StorageRegistry {
//! wavedb_quick_node::Server::new(registry)
//!     .data_dir("./data")
//!     .serve("0.0.0.0:7700")
//!     .await
//! # }
//! ```
//!
//! **Single node for now.** Durability is the journal (a write is durable
//! once journaled). The ring / gossip / replication / failover machinery the
//! README describes is the target design, deferred.
//!
//! [`Exposure`]: wavedb_core::expose::Exposure
//! [`StorageRegistry`]: wavedb_storage::StorageRegistry
//! [`StructStorage`]: wavedb_storage::StructStorage

// The node runs each thread on one Embassy executor (see `serve`, RFC 0066),
// so the `Store`-generic engine futures are deliberately non-`Send` — an
// internal node seam, not a public `Send`-bounded API. Same stance
// `wavedb-core` and `wavedb-storage` take with their engine seams.
#![allow(clippy::future_not_send)]

#[doc(hidden)]
#[path = "rt.rs"]
pub mod __rt;
pub mod dispatch;
pub mod edge;
pub mod error;
pub mod feed;
mod serve;
mod serve_ws;
pub mod shard;
mod subscribe;
pub mod upkeep;
mod ws_read;

use std::future::{Future, pending};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use embassy_futures::select::{Either, select};
use wavedb_platform::io::Listener;
use wavedb_storage::{StorageRegistry, StoreOptions};

pub use error::{Result, ServerError};

/// Implemented for a schema's `ServerRegistry` by `expose_server!` — the
/// node's tasks, made concrete for that registry (RFC 0066).
pub use __rt::NodeTasks;
pub use upkeep::Maintenance;

/// A node, configured but not yet bound.
///
/// `E` is a schema crate's `expose_server!` registry: it is the
/// [`Exposure`](wavedb_core::expose::Exposure) dispatch surface, the
/// [`StorageRegistry`] that names the engine's per-type slots, and — through
/// [`NodeTasks`] — the node's tasks made concrete for it.
#[derive(Debug, Clone)]
pub struct Server<E> {
    registry: E,
    data_dir: PathBuf,
    maintenance: Maintenance,
    secret: Option<[u8; 32]>,
    store: StoreOptions,
    /// Workers to serve on — see [`Server::threads`].
    threads: usize,
}

/// A node that has opened its engine and bound its socket — ready to
/// [`run`](Bound::run). Splitting bind from run lets a caller read the
/// [`local_addr`](Bound::local_addr) first (tests bind port 0).
pub struct Bound<E> {
    registry: E,
    listener: Listener,
    /// The database's data shards, in shard order — opened here, and handed
    /// to the workers that will own them when serving starts (RFC 0067
    /// step 4). Until then [`route`](Bound::route) may seed through them.
    stores: Vec<wavedb_storage::PageStore>,
    /// Workers to serve on — see [`Server::threads`].
    threads: usize,
    maintenance: Maintenance,
    secret: [u8; 32],
}

impl<E> Server<E>
where
    E: NodeTasks + StorageRegistry,
{
    /// Configure a node around a schema registry. Data goes to `./data`
    /// until [`data_dir`](Self::data_dir) says otherwise.
    #[must_use]
    pub fn new(registry: E) -> Self {
        Self {
            registry,
            data_dir: PathBuf::from("data"),
            maintenance: Maintenance::default(),
            secret: None,
            store: StoreOptions::default(),
            threads: 1,
        }
    }

    /// The token-signing secret (HMAC key). Without one, a fresh random
    /// secret is drawn at [`bind`](Self::bind) — fine for a single node
    /// (tokens simply die on restart); set it explicitly to survive
    /// restarts or share across processes.
    #[must_use]
    pub const fn secret(mut self, secret: [u8; 32]) -> Self {
        self.secret = Some(secret);
        self
    }

    /// Set the directory holding `data.bin` + `journal.log`.
    #[must_use]
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = dir.into();
        self
    }

    /// Group-commit writes: allow an acknowledged batch to sit unsynced for
    /// up to `window`, so a burst costs one `fsync` instead of one each
    /// (RFC 0061).
    ///
    /// **Default zero — one barrier per batch**, which is the promise the
    /// engine is built around. Setting this weakens what `Ok` means for
    /// every write the node serves: a crash then loses a *suffix* of
    /// acknowledged writes (never a corrupt store). Set it when the
    /// application can say how much recent work it is willing to lose, and
    /// not otherwise.
    #[must_use]
    pub const fn relax_window(mut self, window: Duration) -> Self {
        self.store.relax_window = window;
        self
    }

    /// Checkpoint (persist the pages' metadata and truncate the journal)
    /// once the journal exceeds `bytes`. Default 64 MiB.
    #[must_use]
    pub const fn checkpoint_after_bytes(mut self, bytes: u64) -> Self {
        self.maintenance.checkpoint_after_bytes = bytes;
        self
    }

    /// Evict settled cache entries down to `bytes` (reads then serve from
    /// the pages). Default 1 GiB.
    #[must_use]
    pub const fn cache_budget_bytes(mut self, bytes: usize) -> Self {
        self.maintenance.cache_budget_bytes = bytes;
        self
    }

    /// How many worker threads serve the database's shards (RFC 0067).
    /// Default 1.
    ///
    /// The database has a fixed number of data shards — 64, recorded in its
    /// `MANIFEST` at creation — and each worker serves a contiguous range of
    /// them, so the thread count can change between runs without touching a
    /// byte. **Raising this only buys throughput where the owners differ**:
    /// two operations on one owner serialise on the brake wherever they land
    /// (`CONCURRENCY_BRAKE.md`), and the brake's key is the owner — one
    /// collection, or one Unique type under one tenant. Each worker costs a
    /// thread, and owns the stores of its shards outright.
    ///
    /// Clamped to `1..=` the shard count: no worker is not a degraded node, it
    /// is a node that serves nothing.
    #[must_use]
    pub const fn threads(mut self, count: usize) -> Self {
        self.threads = count;
        self
    }

    /// Open the engine and bind the listener, without yet accepting.
    ///
    /// # Errors
    /// [`ServerError::Storage`] if the engine can't open (busy, corruption),
    /// [`ServerError::Io`] if the socket can't bind.
    // Nothing awaits here since the socket moved to `async-io`, whose bind is
    // synchronous. It stays `async` because bringing a network up is not
    // synchronous everywhere — on `embassy-net` it waits for the link — and
    // callers should not change signature twice (RFC 0066).
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn bind(self, addr: &str) -> Result<Bound<E>> {
        let stores = wavedb_storage::open_shards(
            &self.data_dir,
            &self.registry.storage_entries(),
            self.store,
            wavedb_core::placement::DEFAULT_SHARDS,
        )?;
        let listener = Listener::bind(addr)?;
        // Publish for the token-minting helpers (`wavedb::auth`) — one node
        // per process, like the engine's storage slots. The secret is
        // whatever the *first* open installed: an in-process reopen keeps
        // verifying the tokens it already issued.
        wavedb_net::auth::set_node_secret(
            self.secret.unwrap_or_else(random_secret),
        );
        let secret = *wavedb_net::auth::node_secret()
            .unwrap_or_else(|| unreachable!("just installed"));
        Ok(Bound {
            registry: self.registry,
            listener,
            stores,
            threads: self.threads,
            maintenance: self.maintenance,
            secret,
        })
    }

    /// Open, bind, and serve until the listener faults — the one-call path.
    ///
    /// # Errors
    /// As [`bind`](Self::bind), plus a fatal accept fault while serving.
    pub async fn serve(self, addr: &str) -> Result<()> {
        self.bind(addr).await?.run().await
    }
}

impl<E> Bound<E>
where
    E: NodeTasks,
{
    /// The address the listener actually bound (resolves an `:0` request).
    ///
    /// # Errors
    /// [`ServerError::Io`] if the socket address can't be read.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// A route onto the opened database for node-side seeding (e.g. creating
    /// a collection `Pivot` before serving, or admin tooling): wrap it in a
    /// [`RouteHandle`](wavedb_core::RouteHandle) and each typed operation
    /// runs on its owner's data shard. Ordinary requests never touch this;
    /// they route through [`run`](Self::run). Seeding writes bypass the
    /// notify wrapper, so they raise no live events — subscriptions predate
    /// serving anyway.
    #[must_use]
    pub fn route(&self) -> shard::Seeder<'_, E> {
        shard::Seeder::new(self.registry, &self.stores)
    }

    /// Accept and serve connections until the listener faults (runs forever
    /// under normal operation). A background maintenance task settles
    /// queued writes into pages, checkpoints past the journal threshold,
    /// and holds the caches to budget.
    ///
    /// # Errors
    /// [`ServerError::Net`] on a fatal accept fault.
    pub async fn run(self) -> Result<()> {
        self.run_with_shutdown(pending()).await
    }

    /// Accept and serve connections until `shutdown` resolves, then settle
    /// and checkpoint (a clean restart replays nothing) and return —
    /// dropping the engine, which releases the process-wide store claim.
    ///
    /// # Errors
    /// [`ServerError::Net`] on a fatal accept fault; [`ServerError::Storage`]
    /// if the final checkpoint fails.
    pub async fn run_with_shutdown(
        self,
        shutdown: impl Future<Output = ()>,
    ) -> Result<()> {
        // Committed mutations come back from the workers as plain data; the
        // subscription table lives on the accept thread, which fans them out.
        let (mutations, incoming) = feed::feed(feed::MUTATION_DEPTH);
        // The workers take the stores (RFC 0067 step 4). Started here rather
        // than on the accept thread because this is who waits for them to
        // close the stores, below.
        let (router, closing) = shard::Router::start(
            self.registry,
            self.stores,
            &mutations,
            self.secret,
            self.threads,
            self.maintenance,
        )
        .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;
        // Ours goes; the workers hold the live clones, so the publisher ends
        // when the last of them does rather than never.
        drop(mutations);
        let (stop, stopped) = async_channel::bounded::<()>(1);
        let (outcome, served) = futures::channel::oneshot::channel();
        let ctx = serve::Accept {
            listener: self.listener,
            router,
            incoming,
            secret: self.secret,
            stop: stopped,
            outcome,
        };
        // The node runs on its own carrier thread (RFC 0066); this future only
        // waits for it, so the caller may await it under any executor.
        let finished =
            wavedb_platform::carrier::spawn("wavedb-accept", |here| {
                // Refused (`MAX_NODES` running): `outcome` drops, and the caller
                // hears `served` cancelled below.
                let _ = E::spawn_accept(ctx, here);
            })
            .map_err(|e| {
                ServerError::Io(std::io::Error::other(e.to_string()))
            })?;
        let mut served = std::pin::pin!(served);
        let result = match select(std::pin::pin!(shutdown), &mut served).await {
            Either::First(()) => {
                stop.close();
                served.await
            }
            Either::Second(result) => result,
        };
        stop.close();
        // Every connection, the WS readers and the background task have
        // returned once the carrier has, and the router went with them — so
        // each worker finishes its last job and closes its stores.
        finished.await;
        let served = result.unwrap_or_else(|_| Err(node_refused()));
        // Clean shutdown: everything settled and committed, each checkpoint's
        // frame forced durable — a restart replays nothing — and every store
        // dropped before its worker answers, so a restart can reopen at once.
        let closed = shard::closed_all(closing).await;
        served?;
        closed.map_err(ServerError::Storage)
    }
}

/// The accept loop never ran: `MAX_NODES` nodes are already running in this
/// process.
fn node_refused() -> ServerError {
    ServerError::Io(std::io::Error::other("no free node slot"))
}

/// A fresh random 32-byte secret from platform entropy
/// (`wavedb_platform::rand`, infallible natively — and a node is always
/// native). Default for a node given no explicit secret.
fn random_secret() -> [u8; 32] {
    wavedb_platform::rand::secret32()
        .unwrap_or_else(|_| unreachable!("native entropy is infallible"))
}
