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
//! Everything is **routed**: a POST is decoded here, executed on the shard
//! that owns it ([`Router`](crate::shard::Router)), and written back here; a
//! WebSocket session's `Call`s go the same way, carrying the caller the
//! session bound at `Hello`. This thread holds sockets and the subscription
//! table, and touches no engine: it has no store at all.

use std::cell::RefCell;
use std::rc::Rc;

use async_channel::Receiver;
use embassy_futures::select::{Either, select};
use embedded_io_async::Write;
use futures::channel::oneshot;
use wavedb_core::expose::Reply;
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_net::frame::{Request, Response, StreamFrame};
use wavedb_net::http::{self, Incoming};
use wavedb_platform::carrier::Here;
use wavedb_platform::io::{IoError, Listener, Stream};

use crate::__rt::NodeTasks;
use crate::shard::{Router, Shards};
use crate::subscribe::SubTable;
use crate::upkeep::{self, Maintenance};
use crate::{ServerError, dispatch, feed, serve_ws};

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
    sock: Stream,
    node: Node<E>,
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
    let subs = Rc::new(RefCell::new(SubTable::default()));
    // Committed mutations come back from the shards as plain data; the
    // subscription table lives here and is fanned out by `feed::publish`.
    let (mutations, incoming) = feed::feed(feed::MUTATION_DEPTH);
    let router = Router::start(
        registry,
        &shards.handle(),
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
    let node = Node {
        router,
        secret,
        subs,
    };
    let result = loop {
        match select(listener.accept(), stop.recv()).await {
            Either::First(Ok((sock, _))) => {
                let conn = Connection {
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

/// One connection's task: serve it, or stop with the node. Made concrete per
/// registry by [`node_tasks!`](crate::node_tasks): routing asks the registry
/// how each hash is addressed.
pub async fn connection<E: NodeTasks>(conn: Connection<E>, here: Here) {
    let Connection { sock, node, halt } = conn;
    // A per-connection fault is dropped: it never takes the node down. (No
    // tracing dep yet — silent.)
    let _ = select(serve_connection(sock, &node, &here), halt.recv()).await;
}

/// Everything a connection needs beyond its socket.
///
/// Each field is a refcount or a key, so a clone per connection is a handful
/// of increments.
#[derive(Clone)]
pub struct Node<E> {
    /// Where every command goes: the shard that owns it.
    pub router: Router<E>,
    /// Gate 1's key, for a WebSocket session's `Hello`.
    pub secret: [u8; 32],
    pub subs: Rc<RefCell<SubTable>>,
}

/// Read one request and answer it: a POST body dispatches + writes the
/// framed response; a WebSocket upgrade switches protocols and hands the
/// socket to the [`serve_ws`] session loop.
async fn serve_connection<E: NodeTasks>(
    sock: Stream,
    node: &Node<E>,
    here: &Here,
) -> wavedb_net::Result<()> {
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
            serve_ws::serve(reader, writer, node, here).await
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
