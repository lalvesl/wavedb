//! The `Db` handle — the client's connection to a node.
//!
//! `Db` binds an identity (`user` + `tenant`) to a transport and turns typed
//! calls into command frames. The tenant is bound **once, here** — it is
//! never restated on a read (the partition key is structural).
//!
//! [`connect`](Db::connect) is transport-only; the [`open`](Db::open) family
//! (M6) attaches a local [`CacheStore`] as a **node-first write-through
//! cache**: every op goes to the node, successful results mirror into the
//! local store, and reads are served from it only when the transport fails
//! *and* it can actually answer. See [`crate::client_cache`] for the exact
//! semantics.

use core::time::Duration;
use std::rc::Rc;
use std::sync::Arc;

use wavedb_core::expose::{Command, Reply};
use wavedb_core::{LocalHandle, U48};
use wavedb_net::NetClient;

use crate::cache::CacheStore;
use crate::error::{Error, Result};

/// A handle to a WaveDB node, bound to one `user`/`tenant` identity.
///
/// Cheap to clone or re-scope ([`as_tenant`](Self::as_tenant)); it holds no
/// live connection (each call dials fresh over HTTP POST).
#[derive(Debug, Clone)]
pub struct Db {
    client: NetClient,
    user: U48,
    tenant: U48,
    /// The signed access token each request carries (M8); absent = the
    /// unauthenticated tier (only `#[server(public)]` functions answer).
    access_token: Option<Vec<u8>>,
    /// The local write-through cache (`Db::open`); `None` = pure transport
    /// handle (`Db::connect`). Shared by clones — one engine per client, used
    /// from one thread, as the engine is (an `Rc`, like the browser's store).
    cache: Option<Rc<CacheStore>>,
    /// How watches transport their events (WebSocket push by default;
    /// [`watch_via_polling`](Self::watch_via_polling) switches to POSTs).
    watch_mode: wavedb_net::WatchMode,
    /// Writes that refused transport, awaiting node-first FIFO replay (W8).
    /// Shared across clones and [`as_tenant`](Self::as_tenant) re-scopes so any
    /// handle drains what any queued.
    offline: Arc<crate::offline_queue::OfflineQueue>,
}

impl Db {
    /// Connect to the node at `addr` (`host:port`) as `user` under `tenant`.
    ///
    /// For a B2C app `tenant == user`. There is no handshake over HTTP POST
    /// (the identity rides in every request), so this only builds the handle;
    /// it is `async` to match the WebSocket path that will handshake (M7).
    ///
    /// # Errors
    /// None today; the signature is fallible for the future transports.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn connect(
        addr: impl Into<String>,
        user: U48,
        tenant: U48,
    ) -> Result<Self> {
        Ok(Self {
            client: NetClient::new(addr),
            user,
            tenant,
            access_token: None,
            cache: None,
            watch_mode: wavedb_net::WatchMode::WebSocket,
            offline: Arc::new(crate::offline_queue::OfflineQueue::default()),
        })
    }

    /// [`connect`](Self::connect) plus a local write-through cache at the
    /// **default location** for `app`: natively the journal + data files
    /// engine under the platform cache base (`<base>/<app>` — `app` is the
    /// leaf directly under the base, created automatically; see
    /// [`crate::cache::default_dir`]); in the browser the IndexedDB database
    /// named exactly `app`.
    ///
    /// `registry` is the schema crate's `CLIENT_REGISTRY`: the declared
    /// client surface names exactly the types the native engine registers
    /// slots for. The wasm store is a flat map and ignores it — the parameter
    /// exists so application code spells the call identically on both
    /// targets.
    ///
    /// # Errors
    /// A cache open/replay fault ([`Error::Core`]); natively also a missing
    /// platform cache base, or [`EngineBusy`] when the app's cache directory
    /// is already open (by another `Db::open`, here or in another process).
    ///
    /// [`EngineBusy`]: wavedb_storage::StorageError::EngineBusy
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn open<R: wavedb_storage::StorageRegistry>(
        registry: R,
        addr: impl Into<String>,
        user: U48,
        tenant: U48,
        app: &str,
    ) -> Result<Self> {
        let dir = crate::cache::default_dir(app)?;
        Self::open_at(registry, addr, user, tenant, dir).await
    }

    /// [`open`](Self::open) with the native cache rooted at an explicit
    /// `dir` instead of the platform default (tests, portable installs).
    ///
    /// # Errors
    /// As [`open`](Self::open).
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn open_at<R: wavedb_storage::StorageRegistry>(
        registry: R,
        addr: impl Into<String>,
        user: U48,
        tenant: U48,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<Self> {
        let store = crate::cache::open_native(&registry, dir.as_ref())?;
        let mut db = Self::connect(addr, user, tenant).await?;
        db.cache = Some(Rc::new(store));
        Ok(db)
    }

    /// [`connect`](Self::connect) plus the IndexedDB write-through cache in
    /// the database named exactly `app` (no forced prefix — the developer
    /// chooses the whole name, matching the native `<base>/<app>` leaf). The
    /// `registry` parameter is unused here (the browser store is a flat
    /// `Id → bytes` map, no per-type slots) — it exists so application code
    /// spells the call identically on both targets.
    ///
    /// # Errors
    /// [`Error::Core`] (`Backend`) when IndexedDB is unavailable or refuses
    /// to open.
    #[cfg(target_arch = "wasm32")]
    pub async fn open<R>(
        registry: R,
        addr: impl Into<String>,
        user: U48,
        tenant: U48,
        app: &str,
    ) -> Result<Self> {
        let _ = registry;
        let store = CacheStore::open(app).await.map_err(Error::Core)?;
        let mut db = Self::connect(addr, user, tenant).await?;
        db.cache = Some(Rc::new(store));
        Ok(db)
    }

    /// The typed surface of the local cache store, when this handle was
    /// opened with one — direct reads/writes that never touch the node
    /// (inspection, tests, offline tooling). `None` for a
    /// [`connect`](Self::connect) handle.
    #[must_use]
    pub fn local(&self) -> Option<LocalHandle<'_, CacheStore>> {
        self.cache
            .as_deref()
            .map(|store| LocalHandle::new(store, self.tenant))
    }

    /// The same handle authenticating with `token` (the access half of a
    /// login's pair). Every subsequent request carries it; the node derives
    /// `user`/`tenant` from the verified claims.
    #[must_use]
    pub fn with_access_token(mut self, token: Vec<u8>) -> Self {
        self.access_token = Some(token);
        self
    }

    /// Deliver this handle's watches over "anything new?" POST polls instead
    /// of a pushed WebSocket — for clients whose path to the node cannot hold
    /// one open. `every` is the base interval: a busy watch polls at it, a
    /// quiet one backs off beyond it (W7 idle backoff). Larger intervals cost
    /// latency, not correctness — the node is **stateless**, so each poll
    /// navigates the disk past the client's cursor and nothing committed
    /// during an outage is missed.
    #[must_use]
    pub const fn watch_via_polling(mut self, every: Duration) -> Self {
        self.watch_mode = wavedb_net::WatchMode::HttpPoll(every);
        self
    }

    /// The identity claim each request ships (also presented once as the
    /// WebSocket `Hello` by [`crate::watch`]).
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn auth(&self) -> wavedb_net::Auth {
        self.access_token.clone().map_or(
            wavedb_net::Auth::Anonymous {
                tenant: self.tenant,
            },
            wavedb_net::Auth::Token,
        )
    }

    /// The node address this handle is bound to.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn addr(&self) -> &str {
        self.client.addr()
    }

    /// Whether this handle carries an access token — the node refuses
    /// anonymous subscriptions, so a watch requires one.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) const fn has_token(&self) -> bool {
        self.access_token.is_some()
    }

    /// The transport this handle's watches ride.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) const fn watch_mode(&self) -> wavedb_net::WatchMode {
        self.watch_mode
    }

    /// A handle to the **same node** scoped to a different `tenant` — the
    /// server-side cross-tenant seam (e.g. a `#[server]` `register` writing
    /// into a freshly minted tenant's space). Keeps the same `user`.
    #[must_use]
    pub fn as_tenant(&self, tenant: U48) -> Self {
        Self {
            client: self.client.clone(),
            user: self.user,
            tenant,
            access_token: self.access_token.clone(),
            cache: self.cache.clone(),
            watch_mode: self.watch_mode,
            offline: self.offline.clone(),
        }
    }

    /// The tenant this handle is bound to.
    #[must_use]
    pub const fn tenant(&self) -> U48 {
        self.tenant
    }

    /// The user this handle authenticates as.
    #[must_use]
    pub const fn user(&self) -> U48 {
        self.user
    }

    /// Send one command to the node and return its reply, flattening a
    /// node-side refusal into [`Error::Node`].
    ///
    /// # Errors
    /// [`Error::Transport`] on a socket/framing fault, [`Error::Node`] on a
    /// node-side refusal.
    // `Db` is re-exported, so `pub(crate)` is meaningful (not redundant) — it
    // keeps this internal seam out of the public API while the typed surfaces
    // in sibling modules reach it.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) async fn command(
        &self,
        struct_hash: u64,
        command: Command,
        payload: Vec<u8>,
    ) -> Result<Reply> {
        self.client
            .call(self.auth(), struct_hash, command, payload)
            .await?
            .map_err(Error::Node)
    }

    /// The offline write queue this handle shares (W8) — the
    /// [`offline_queue`](crate::offline_queue) module hangs `enqueue`/`drain`
    /// off it.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) fn offline_queue(&self) -> &crate::offline_queue::OfflineQueue {
        &self.offline
    }

    /// Send a walk-shaped command and stream its item frames back as the
    /// node writes them; each item is one record's wire bytes.
    ///
    /// # Errors
    /// [`Error::Transport`] establishing the exchange; later faults (and a
    /// node-side refusal, which arrives as the stream's final word) ride
    /// the stream's items.
    #[allow(clippy::redundant_pub_crate)]
    pub(crate) async fn command_stream(
        &self,
        struct_hash: u64,
        command: Command,
        payload: Vec<u8>,
    ) -> Result<impl futures::Stream<Item = Result<Vec<u8>>> + use<>> {
        use futures::TryStreamExt;
        let items = self
            .client
            .call_stream(self.auth(), struct_hash, command, payload)
            .await?;
        // Keep the failure classes apart: a node refusal riding the stream
        // is `Error::Node`, not a transport fault.
        Ok(items.map_err(|e| match e {
            wavedb_net::Error::Node(n) => Error::Node(n),
            other => Error::Transport(other),
        }))
    }

    /// Call a `#[server]` function by its hash, decoding the wire-encoded
    /// return. The generated client stub is a thin wrapper over this. A
    /// function ignores the frame `command` (its hash *is* the operation), so
    /// a filler is sent.
    ///
    /// # Errors
    /// [`Error::Transport`] / [`Error::Node`] on a failed call, or a decode
    /// fault on the return; [`Error::UnexpectedReply`] if the node did not
    /// answer with a function return.
    pub async fn call_fn<R: wavedb_core::WaveWire>(
        &self,
        struct_hash: u64,
        payload: Vec<u8>,
    ) -> Result<R> {
        let reply = self.command(struct_hash, Command::Get, payload).await?;
        crate::reply::returned(reply)
    }

    /// Call a **stream-returning** `#[server]` function by its hash,
    /// decoding each item frame as the node writes it. The generated client
    /// stub for an `impl Stream`-returning fn is a thin wrapper over this.
    pub fn call_fn_stream<R: wavedb_core::WaveWire + 'static>(
        &self,
        struct_hash: u64,
        payload: Vec<u8>,
    ) -> impl futures::Stream<Item = Result<R>> {
        crate::client_handle::streamed(self, struct_hash, Command::Get, payload)
    }
}
