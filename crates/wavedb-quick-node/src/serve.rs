//! The accept loop and per-connection handling (native, HTTP POST).
//!
//! One connection = one request/response exchange (the tunnel sends
//! `connection: close`). Connections are handled concurrently on the current
//! thread via a [`LocalSet`] + [`spawn_local`], so no `Send` bound leaks onto
//! the `Store`-generic engine futures (deliberately non-`Send` — an internal
//! node seam, not a public API).
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
//!
//! [`spawn_local`]: tokio::task::spawn_local

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use embedded_io_async::Write;
use tokio::task::{LocalSet, spawn_local};
use wavedb_core::Store;
use wavedb_core::expose::{Exposure, Reply};
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_net::frame::{Request, Response, StreamFrame};
use wavedb_net::http::{self, Incoming};
use wavedb_platform::io::{IoError, Listener, Stream};

use crate::shard::{OwnerLocks, Router};
use crate::subscribe::SubTable;
use crate::{dispatch, serve_ws};

/// Serve `store` under `registry` on an already-bound `listener` until either
/// the `shutdown` future resolves or an accept fault. Each connection is
/// handled on its own local task (current-thread, no `Send` bound);
/// `maintenance` runs alongside on the same [`LocalSet`] (cancelled when
/// serving stops).
///
/// # Errors
/// A fatal accept fault (the listener socket broke).
pub async fn run<E, S, F, M>(
    listener: Listener,
    registry: E,
    node: Node<S>,
    maintenance: M,
    shutdown: F,
) -> wavedb_net::Result<()>
where
    E: Exposure + Copy + 'static,
    S: Store + 'static,
    F: Future<Output = ()>,
    M: Future<Output = ()> + 'static,
{
    let local = LocalSet::new();
    local
        .run_until(async move {
            let upkeep = spawn_local(maintenance);
            tokio::pin!(shutdown);
            loop {
                let sock = tokio::select! {
                    accepted = listener.accept() => accepted?.0,
                    () = &mut shutdown => {
                        // Stop maintaining before the caller's final
                        // drain + checkpoint takes over.
                        upkeep.abort();
                        return Ok(());
                    }
                };
                let node = node.clone();
                spawn_local(async move {
                    // A per-connection fault is dropped: it never takes the
                    // node down. (No tracing dep yet — silent.)
                    let _ = serve_connection(sock, &registry, &node).await;
                });
            }
        })
        .await
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
            serve_ws::serve(
                reader,
                writer,
                registry,
                &*node.store,
                &node.locks,
                &node.secret,
                &node.subs,
            )
            .await
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
