//! One stateless "anything new?" sync exchange — shared by the HTTP-poll
//! actor and the WebSocket reconnect catch-up.
//!
//! Builds a [`SyncRequest`] from a topic→cursor map (`None` = register at the
//! tail), POSTs it over the reserved sync hash, and decodes the scalar
//! [`SyncReply`]. The node holds no per-session state; the cursors live with
//! the caller (see [`crate::sync`]).

use std::collections::HashMap;

use wavedb_core::expose::{Command, Reply};
use wavedb_wire::{from_wire, to_wire};

use crate::error::{Error, Result};
use crate::frame::{Auth, CommandFrame, Request, Response, StreamFrame};
use crate::frames::FrameReader;
use crate::sync::{SYNC_STRUCT_HASH, SyncReply, SyncRequest, TopicCursor};
use crate::ws::Topic;

/// POST one sync request declaring every watched topic with its cursor, and
/// decode the node's scalar [`SyncReply`].
///
/// # Errors
/// A transport fault, a node refusal ([`Error::Node`] — authoritative), or an
/// off-protocol reply.
pub(super) async fn sync_once(
    addr: &str,
    auth: &Auth,
    cursors: &HashMap<Topic, Option<u64>>,
) -> Result<SyncReply> {
    let request = Request {
        auth: auth.clone(),
        frame: CommandFrame {
            durable: false,
            struct_hash: SYNC_STRUCT_HASH,
            // Like a function call, sync ignores the frame command.
            command: Command::Get,
            payload: to_wire(&SyncRequest {
                topics: cursors
                    .iter()
                    .map(|(topic, since)| TopicCursor {
                        topic: *topic,
                        since: *since,
                    })
                    .collect(),
            }),
        },
        // The dedicated sync exchange carries its topics in the payload, not
        // as a command piggyback — never a `Sync` frame on this path.
        sync: Vec::new(),
    };
    let stream = wavedb_platform::http::post(addr, &to_wire(&request)).await?;
    let mut frames = FrameReader::new(stream);
    let bytes = frames
        .next_frame()
        .await?
        .ok_or(Error::Http("response ended before its End frame"))?;
    match from_wire::<StreamFrame>(&bytes)? {
        StreamFrame::End(Response::Ok(Reply::Returned(reply))) => {
            Ok(from_wire::<SyncReply>(&reply)?)
        }
        StreamFrame::End(Response::Ok(_)) => {
            Err(Error::Http("sync answered with a non-return reply"))
        }
        StreamFrame::End(Response::Err(refusal)) => Err(Error::Node(refusal)),
        StreamFrame::Item(_) => {
            Err(Error::Http("item frame on a scalar command"))
        }
        // A piggyback delta only rides a command reply, never the dedicated
        // sync exchange — its presence here is off-protocol.
        StreamFrame::Sync(_) => {
            Err(Error::Http("sync frame on the sync exchange"))
        }
    }
}
