//! The accept loop and per-connection handling (native, HTTP POST).
//!
//! One connection = one request/response exchange (the tunnel sends
//! `connection: close`). The node runs on its own **accept carrier** — a
//! thread with one Embassy executor (RFC 0066) — and each connection is a
//! task from a static pool there, so no `Send` bound leaks onto the
//! `Store`-generic engine futures (deliberately non-`Send` — an internal node
//! seam, not a public API). A full pool refuses the connection.
//!
//! ## Stopping
//!
//! Embassy cannot abort a task, so every task on the carrier watches one
//! `halt` channel and returns when the accept loop closes it. The carrier's
//! thread returns once the last of them has (`wavedb_platform::carrier`).
//!
//! ## What this thread does, and what it hands off
//!
//! A POST is **routed**: decoded here, executed on the shard that owns it
//! ([`Router`](crate::shard::Router)), written back here. This thread runs no
//! gate and touches no engine on that path.
//!
//! A WebSocket session is not routed yet, and it still executes its `Call`s
//! here — over a **cacheless** store, which is what makes two executors over
//! one engine sound, and under the same [`OwnerLocks`](crate::shard::OwnerLocks)
//! table, which is what keeps the brake a brake across both. Routing it too
//! turns on a question this seam does not get to answer alone: a session binds
//! its identity once at `Hello`, and routing re-verifies the token per
//! command, so a long-lived watch would start refusing the moment its token
//! expired. That is arguably more correct and definitely a behaviour change.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use async_channel::Receiver;
use embassy_futures::select::{Either, select};
use embedded_io_async::Write;
use futures::channel::oneshot;
use wavedb_core::Store;
use wavedb_core::expose::{Exposure, Reply};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_net::frame::{Request, Response, StreamFrame};
use wavedb_net::http::{self, Incoming};
use wavedb_platform::carrier::Here;
use wavedb_platform::io::{IoError, Listener, Stream};

use crate::__rt::NodeTasks;
use crate::shard::{OwnerLocks, Router, ShardStore, Shards};
use crate::subscribe::{NotifyStore, SubTable, Subscriptions};
use crate::upkeep::{self, Maintenance};
use crate::{ServerError, dispatch, feed, serve_ws};

/// The store the accept carrier serves WebSocket sessions through.
type Serving = NotifyStore<ShardStore, Subscriptions>;

/// What the accept loop is started with. Built on the caller's thread and
/// moved onto the accept carrier, so every field is `Send`; the node's
/// non-`Send` state is built over there, by [`accept`].
pub struct Accept<E> {
    pub(crate) registry: E,
    pub(crate) listener: Listener,
    pub(crate) shards: Shards,
    pub(crate) maintenance: Maintenance,
    pub(crate) secret: [u8; 32],
    /// Closed by the caller to stop serving.
    pub(crate) stop: Receiver<()>,
    /// How serving ended: `Ok` on a stop, the fault otherwise.
    pub(crate) outcome: oneshot::Sender<crate::Result<()>>,
}

/// One accepted connection, as its task receives it.
pub struct Connection<E> {
    registry: E,
    sock: Stream,
    node: Node<Serving>,
    halt: Receiver<()>,
}

/// The accept loop: build the node on this thread, then accept until stopped
/// or until the listener faults.
pub async fn accept<E: NodeTasks>(ctx: Accept<E>, here: Here) {
    let Accept {
        registry,
        listener,
        shards,
        maintenance,
        secret,
        stop,
        outcome,
    } = ctx;
    let result = serve(
        registry,
        &listener,
        &shards,
        maintenance,
        secret,
        &stop,
        &here,
    )
    .await;
    let _ = outcome.send(result);
}

async fn serve<E: NodeTasks>(
    registry: E,
    listener: &Listener,
    shards: &Shards,
    maintenance: Maintenance,
    secret: [u8; 32],
    stop: &Receiver<()>,
    here: &Here,
) -> crate::Result<()> {
    // One table for the node: the brake that keeps two interleaved
    // collection ops from losing an index update now that the store
    // underneath genuinely suspends (`CONCURRENCY_BRAKE.md`). Shared with
    // every shard worker, which is what makes it exclude anything at all
    // once operations run on more than this thread.
    let locks = Arc::new(OwnerLocks::new());
    let subs = Rc::new(RefCell::new(SubTable::default()));
    // Committed mutations come back from the shards as plain data; the
    // subscription table lives here and is fanned out by `feed::publish`.
    let (mutations, incoming) = feed::feed(feed::MUTATION_DEPTH);
    let router = Router::start(
        registry,
        &shards.handle(),
        &locks,
        &mutations,
        secret,
        shards.count(),
    )
    .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;
    // Ours goes; the workers hold the live clones, so the publisher ends when
    // the last of them does rather than never.
    drop(mutations);

    // Closed when serving stops: every task on this carrier watches it.
    let (halt, halted) = async_channel::bounded::<()>(1);
    upkeep::spawn_background(
        shards.handle(),
        maintenance,
        incoming,
        Rc::clone(&subs),
        halted.clone(),
        here,
    )?;
    // The accept thread's own view: cacheless (see `Shards::store`) and
    // wrapped so a WebSocket `Call` executed here still reaches the
    // watchers, exactly as a routed one does from its shard.
    let node = Node {
        router,
        store: Rc::new(NotifyStore::new(shards.store(), Rc::clone(&subs))),
        secret,
        locks,
        subs,
    };
    let result = loop {
        match select(listener.accept(), stop.recv()).await {
            Either::First(Ok((sock, _))) => {
                let conn = Connection {
                    registry,
                    sock,
                    node: node.clone(),
                    halt: halted.clone(),
                };
                // A full pool refuses: the connection drops, closing its
                // socket — an explicit refusal, never a queue.
                let _ = E::spawn_connection(conn, here.clone());
            }
            Either::First(Err(e)) => break Err(e.into()),
            // A message or a closed channel: either way, stop.
            Either::Second(_) => break Ok(()),
        }
    };
    halt.close();
    result
}

/// One connection's task: serve it, or stop with the node.
pub async fn connection<E: NodeTasks>(conn: Connection<E>, here: Here) {
    let Connection {
        registry,
        sock,
        node,
        halt,
    } = conn;
    // A per-connection fault is dropped: it never takes the node down. (No
    // tracing dep yet — silent.)
    let _ =
        select(serve_connection(sock, &registry, &node, &here), halt.recv())
            .await;
}

/// Everything a connection needs beyond its socket and the registry.
///
/// One struct rather than five parameters, because they travel together
/// everywhere and are cloned together per connection — each field is a
/// refcount or a key, so a clone is a handful of increments.
pub struct Node<S> {
    /// Where a POST goes: the shard that owns it.
    pub router: Router,
    /// The accept thread's own cacheless view, for the WebSocket sessions
    /// that still execute here. See the module note.
    pub store: Rc<S>,
    pub secret: [u8; 32],
    /// Per-owner serialisation — see `CONCURRENCY_BRAKE.md`. One table for
    /// the whole node, `Arc` because the shard workers hold it too.
    pub locks: Arc<OwnerLocks>,
    pub subs: Rc<RefCell<SubTable>>,
}

// Manual: a derive would demand `S: Clone`, and the store is shared by `Rc`
// rather than cloned.
impl<S> Clone for Node<S> {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
            store: Rc::clone(&self.store),
            secret: self.secret,
            locks: Arc::clone(&self.locks),
            subs: Rc::clone(&self.subs),
        }
    }
}

/// Read one request and answer it: a POST body dispatches + writes the
/// framed response; a WebSocket upgrade switches protocols and hands the
/// socket to the [`serve_ws`] session loop.
async fn serve_connection<E, S>(
    sock: Stream,
    registry: &E,
    node: &Node<S>,
    here: &Here,
) -> wavedb_net::Result<()>
where
    E: Exposure,
    S: Store,
{
    let (mut reader, mut writer) = sock.split();
    match http::read_request(&mut reader).await? {
        None => Ok(()), // peer closed without sending — clean.
        Some(Incoming::Post(body)) => {
            match from_wire::<Request>(&body) {
                // Handed off, not handled: the gates and the engine run on
                // the owning shard, and this task only waits for bytes to
                // write back.
                Ok(request) => {
                    let answer = node.router.dispatch(request).await;
                    write_response(&mut writer, answer).await
                }
                // The envelope is malformed — a transport-level client
                // error, not a WaveDB refusal (no struct_hash to refuse).
                Err(_) => http::write_bad_request(&mut writer).await,
            }
        }
        Some(Incoming::Upgrade { key }) => {
            let accept = wavedb_platform::ws::codec::accept_key(&key);
            http::write_switching_head(&mut writer, &accept).await?;
            serve_ws::serve(reader, writer, registry, node, here).await
        }
    }
}

/// Write one response as its frame sequence: a `Values` reply (a walk)
/// unpacks into one `Item` frame per record — flushed as written, so the
/// client streams them — then an `End`; everything else is a bare `End`.
///
/// (The walk itself is still buffered inside `execute` for now; when the
/// engine goes streaming only this seam's producer changes — the wire and
/// the clients already speak frames.)
async fn write_response<W>(
    w: &mut W,
    answer: dispatch::Answer,
) -> wavedb_net::Result<()>
where
    W: Write<Error = IoError>,
{
    let dispatch::Answer { response, sync } = answer;
    http::write_ok_head(w).await?;
    // The W7 piggyback delta **leads** the response, so the manager peels it
    // off the front without parsing the command's own item frames; a request
    // that declared no topics gets none.
    if let Some(delta) = sync {
        http::write_frame(w, &to_wire(&StreamFrame::Sync(delta))).await?;
    }
    let end = match response {
        Response::Ok(Reply::Values(entries)) => {
            for entry in entries {
                let item = to_wire(&StreamFrame::Item(entry));
                http::write_frame(w, &item).await?;
            }
            Response::Ok(Reply::Done)
        }
        other => other,
    };
    http::write_frame(w, &to_wire(&StreamFrame::End(end))).await
}
