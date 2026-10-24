//! Ingress: each worker accepts and serves its own connections ([RFC 0067]
//! §6, step 5).
//!
//! The node binds one listener per worker on one address
//! ([`Listener::bind_many`]); on Linux the kernel spreads connections across
//! them. A worker's accept loop runs beside its jobs, on the same carrier,
//! and each connection it accepts is a task there, from a static pool. A
//! full pool refuses the connection: its socket closes, never queued.
//!
//! One connection = one request/response exchange (the tunnel sends
//! `connection: close`), or a WebSocket session. Either way the connection
//! **executes nothing itself**: every operation goes to its owner's worker
//! as a [`Job`](crate::shard::Job) through the [`Router`] — this worker's
//! own queue when it owns the operation, another worker's otherwise. Local
//! and remote are the same code, and a connection's task stays the size of
//! the I/O it does, not of the engine call it waits on.
//!
//! ## Stopping
//!
//! Embassy cannot abort a task, so every connection watches one `halt`
//! channel and returns when the accept loop closes it. The accept loop ends
//! when the node's `stop` closes, or when its listener faults — reported
//! to the node, which then stops every worker.
//!
//! [RFC 0067]: ../../../rfcs/0067-thread-per-core-shards-WIP.md
//! [`Listener::bind_many`]: wavedb_platform::io::Listener::bind_many

use async_channel::{Receiver, Sender};
use embassy_futures::select::{Either, select};
use embedded_io_async::Write;
use wavedb_core::expose::Reply;
use wavedb_core::wire::{from_wire, to_wire};
use wavedb_net::frame::{Request, Response, StreamFrame};
use wavedb_net::http::{self, Incoming};
use wavedb_platform::carrier::Here;
use wavedb_platform::io::{IoError, Listener, Stream};

use crate::__rt::NodeTasks;
use crate::shard::Router;
use crate::{ServerError, dispatch, serve_ws};

/// What one worker's accept loop is started with. Built on the caller's
/// thread and moved onto the worker's carrier, so every field is `Send`.
pub struct Accept<E> {
    pub(crate) listener: Listener,
    /// Onto every worker, this one included.
    pub(crate) router: Router<E>,
    /// Closed by the node to stop serving.
    pub(crate) stop: Receiver<()>,
    /// Where a listener fault goes: it stops the node.
    pub(crate) fault: Sender<ServerError>,
}

/// The node's half of ingress, handed to the workers when they start: one
/// listener each (fewer where the platform cannot spread connections), the
/// stop signal, and the fault channel.
pub struct Ingress {
    pub(crate) listeners: Vec<Listener>,
    pub(crate) stop: Receiver<()>,
    pub(crate) fault: Sender<ServerError>,
}

impl Ingress {
    /// The accept loop for the next worker, while listeners remain.
    pub(crate) fn next<E: Clone>(
        &mut self,
        router: &Router<E>,
    ) -> Option<Accept<E>> {
        let listener = self.listeners.pop()?;
        Some(Accept {
            listener,
            router: router.clone(),
            stop: self.stop.clone(),
            fault: self.fault.clone(),
        })
    }
}

/// One accepted connection, as its task receives it.
pub struct Connection<E> {
    sock: Stream,
    router: Router<E>,
    halt: Receiver<()>,
}

/// A worker's accept loop: accept until stopped or until the listener
/// faults, then stop this worker's connections.
pub async fn accept<E: NodeTasks>(ctx: Accept<E>, here: Here) {
    let Accept {
        listener,
        router,
        stop,
        fault,
    } = ctx;
    // Closed when serving stops: every connection on this carrier watches it.
    let (halt, halted) = async_channel::bounded::<()>(1);
    loop {
        match select(listener.accept(), stop.recv()).await {
            Either::First(Ok((sock, _))) => {
                let conn = Connection {
                    sock,
                    router: router.clone(),
                    halt: halted.clone(),
                };
                // A full pool refuses: the connection drops, closing its
                // socket — an explicit refusal, never a queue.
                let _ = E::spawn_connection(conn, here.clone());
            }
            Either::First(Err(e)) => {
                let _ = fault.try_send(e.into());
                break;
            }
            // A message or a closed channel: either way, stop.
            Either::Second(_) => break,
        }
    }
    halt.close();
    drop(here); // held to the end: the carrier waits for this loop
}

/// One connection's task: serve it, or stop with the worker. Made concrete
/// per registry by [`node_tasks!`](crate::node_tasks): routing asks the
/// registry how each hash is addressed.
pub async fn connection<E: NodeTasks>(conn: Connection<E>, here: Here) {
    let Connection { sock, router, halt } = conn;
    // A per-connection fault is dropped: it never takes the node down. (No
    // tracing dep yet — silent.)
    let _ = select(serve_connection(sock, &router, &here), halt.recv()).await;
}

/// Read one request and answer it: a POST body dispatches + writes the
/// framed response; a WebSocket upgrade switches protocols and hands the
/// socket to the [`serve_ws`] session loop.
async fn serve_connection<E: NodeTasks>(
    sock: Stream,
    router: &Router<E>,
    here: &Here,
) -> wavedb_net::Result<()> {
    let (mut reader, mut writer) = sock.split();
    match http::read_request(&mut reader).await? {
        None => Ok(()), // peer closed without sending — clean.
        Some(Incoming::Post(body)) => {
            match from_wire::<Request>(&body) {
                // Handed off, not handled: the gates and the engine run on
                // the owning worker, and this task only waits for bytes to
                // write back.
                Ok(request) => {
                    let answer = router.dispatch(request).await;
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
            serve_ws::serve(reader, writer, router, here).await
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

/// What an accept loop carries has to cross onto its worker.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<Accept<()>>();
    assert_send::<Ingress>();
};
