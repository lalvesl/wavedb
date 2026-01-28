//! The client-side transport: build a [`Request`](crate::frame::Request), POST it, read the framed
//! response.
//!
//! Compiles for native and wasm32 alike. Every exchange routes through the
//! process's [`manager`](crate::manager) task — the one place connections
//! are made — which does the actual POST (`wavedb_platform::http`: a fresh
//! `TcpStream` vs a browser `fetch`) and pumps the response frames back;
//! everything above that seam is byte-for-byte the same protocol.
//!
//! One exchange = one fresh connection. Each call re-sends the identity —
//! plain HTTP has no connection to bind identity to (the WebSocket watch
//! path binds it once; see [`manager`](crate::manager)). A response is a
//! sequence of [`StreamFrame`]s: a scalar command answers with a bare
//! [`End`](StreamFrame::End); a walk streams `Item`s as the node produces
//! them, so [`call_stream`](NetClient::call_stream) yields records without
//! waiting for the whole collection.

use futures::Stream;
use wavedb_core::expose::{Command, Reply};
use wavedb_wire::from_wire;

use crate::error::{Error, Result};
use crate::frame::{Auth, CommandFrame, NodeError, Response, StreamFrame};
use crate::manager::PostFrames;

/// A thin client bound to one node address. Cheap to clone/rebuild — it holds
/// no connection (each call dials fresh).
#[derive(Debug, Clone)]
pub struct NetClient {
    addr: String,
}

/// The outcome of a transported command: either the node's [`Reply`], or the
/// structured [`NodeError`] it shipped inside a 200. A transport-level
/// failure (socket, framing) is the outer [`Error`] instead.
pub type Executed = core::result::Result<Reply, NodeError>;

impl NetClient {
    /// Bind to a node at `host:port` (e.g. `"127.0.0.1:7700"`).
    #[must_use]
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    /// The node address this client is bound to — what a sibling transport
    /// (the WebSocket [`WsSession`](crate::WsSession)) dials.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// POST the request through the manager and return the response's
    /// frames. Resolves once the head is in — an establishment fault is an
    /// `Err` here, a mid-stream fault rides the frames.
    async fn exchange(
        &self,
        auth: Auth,
        frame: CommandFrame,
    ) -> Result<PostFrames> {
        // The manager assembles the `Request` (it owns the poll cursors it
        // piggybacks onto the frame — W7), so the client hands it the parts.
        crate::manager::post(&self.addr, auth, frame).await
    }

    /// Send one scalar command under `auth` and await the node's answer.
    ///
    /// # Errors
    /// A transport [`Error`] (socket, HTTP framing, an undecodable frame, or
    /// an `Item` where a scalar answer was due). A node-side refusal is
    /// **not** an error — it is the `Err` arm of the returned [`Executed`].
    pub async fn call(
        &self,
        auth: Auth,
        struct_hash: u64,
        command: Command,
        payload: Vec<u8>,
    ) -> Result<Executed> {
        let frame = CommandFrame::new(struct_hash, command, payload);
        self.call_frame(auth, frame).await
    }

    /// [`call`](Self::call) with the frame built by the caller — a durable
    /// write's ([`CommandFrame::durable`]).
    ///
    /// # Errors
    /// As [`call`](Self::call).
    pub async fn call_frame(
        &self,
        auth: Auth,
        frame: CommandFrame,
    ) -> Result<Executed> {
        let mut frames = self.exchange(auth, frame).await?;
        loop {
            let bytes = frames
                .next_frame()
                .await?
                .ok_or(Error::Http("response ended before its End frame"))?;
            match from_wire::<StreamFrame>(&bytes)? {
                StreamFrame::End(response) => {
                    return Ok(response.into_result());
                }
                // A piggyback delta (W7) is not consumed on this scalar path
                // yet — skip it and read on to the End.
                StreamFrame::Sync(_) => {}
                StreamFrame::Item(_) => {
                    return Err(Error::Http("item frame on a scalar command"));
                }
            }
        }
    }

    /// [`call`](Self::call), flattening a node refusal into the transport
    /// [`Error`] so a caller that treats every failure alike gets one type.
    ///
    /// # Errors
    /// [`Error::Node`] on a node-side refusal, else a transport fault.
    pub async fn call_ok(
        &self,
        auth: Auth,
        struct_hash: u64,
        command: Command,
        payload: Vec<u8>,
    ) -> Result<Reply> {
        self.call(auth, struct_hash, command, payload)
            .await?
            .map_err(Error::Node)
    }

    /// Send a walk-shaped command and stream the item frames back as the
    /// node writes them. Each item is one record's wire bytes; the stream
    /// ends at the node's `End` frame — a node-side fault mid-walk surfaces
    /// as a trailing [`Error::Node`] item after the records already shipped.
    ///
    /// # Errors
    /// A transport fault establishing the exchange; later faults ride the
    /// stream.
    pub async fn call_stream(
        &self,
        auth: Auth,
        struct_hash: u64,
        command: Command,
        payload: Vec<u8>,
    ) -> Result<impl Stream<Item = Result<Vec<u8>>> + use<>> {
        let frame = CommandFrame::new(struct_hash, command, payload);
        let frames = self.exchange(auth, frame).await?;
        Ok(futures::stream::unfold(Some(frames), |state| async move {
            let mut frames = state?;
            loop {
                let item = match frames.next_frame().await {
                    Ok(Some(bytes)) => match from_wire::<StreamFrame>(&bytes) {
                        Ok(StreamFrame::Item(item)) => {
                            return Some((Ok(item), Some(frames)));
                        }
                        // A piggyback delta (W7) is not consumed on this path
                        // yet — skip it and read on.
                        Ok(StreamFrame::Sync(_)) => continue,
                        Ok(StreamFrame::End(Response::Ok(_))) => return None,
                        Ok(StreamFrame::End(Response::Err(e))) => {
                            Err(Error::Node(e))
                        }
                        Err(e) => Err(e.into()),
                    },
                    Ok(None) => {
                        Err(Error::Http("response ended before its End frame"))
                    }
                    Err(e) => Err(e),
                };
                // Terminal: yield the fault once, then end the stream.
                return Some((item, None));
            }
        }))
    }
}
