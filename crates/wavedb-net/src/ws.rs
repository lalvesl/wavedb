//! The WebSocket envelopes — what rides each binary message (both
//! targets; the server session loop lives in `wavedb-quick-node`).
//!
//! The connection replaces the per-request identity: the token is
//! presented **once**, in [`ClientMsg::Hello`], and every later message
//! executes as that verified caller — the WebSocket is bound to an
//! identity the way an HTTP POST never can be. After the hello:
//!
//! - [`ClientMsg::Call`] runs one [`CommandFrame`] through the same gates
//!   and dispatch as a POST body, answered by [`ServerMsg::Item`]s and one
//!   [`ServerMsg::End`] (the framed-stream shape, one message per frame).
//!   Calls are FIFO per connection — the documented client queue model.
//! - [`ClientMsg::Subscribe`] declares interest in a [`Topic`] — a Unique
//!   anchor or one collection `Pivot`, exactly what a screen shows. Every
//!   accepted mutation under the caller's tenant that matches a
//!   subscription arrives as a [`ServerMsg::Event`] carrying the record's
//!   new state. The tenant never rides a topic: it is the connection's
//!   bound identity, so a client cannot watch another tenant's data.

use wavedb_core::{Id, LocalId};
use wavedb_wire::WaveWire;

use crate::frame::{Auth, CommandFrame, Response};

/// What a subscription names: one type's Unique anchor (`pivot: None`) or
/// one collection (`pivot: Some` — the collection's `Pivot` `LocalId`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, WaveWire)]
pub struct Topic {
    /// The `#[wavedb]` struct the events are about.
    pub struct_hash: u64,
    /// `None` = the Unique anchor; `Some` = one collection's `Pivot`.
    pub pivot: Option<LocalId>,
}

/// What happened to the record an event names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, WaveWire)]
pub enum EventKind {
    /// Inserted, updated, or Unique-saved — `body` is the new state.
    Saved,
    /// Moved to the dead tree — `body` is empty. Carries the removal
    /// instant (the node's dead-log key), so a catch-up cursor advances
    /// past the removal instead of re-receiving it forever.
    Removed(u64),
}

/// One mutation, pushed to every subscriber of its topic.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct RecordEvent {
    /// The subscription this event answers.
    pub topic: Topic,
    /// The record's authoritative identity (node-minted for an insert).
    pub id: Id,
    /// Saved or removed.
    pub kind: EventKind,
    /// The new live version's [`Metadata`] (`None` on a removal) — riding
    /// the event lets a mirror adopt the node's authoritative chain data
    /// verbatim instead of minting its own.
    ///
    /// [`Metadata`]: wavedb_core::Metadata
    pub meta: Option<wavedb_core::Metadata>,
    /// The record's new body wire bytes (empty for a removal).
    pub body: Vec<u8>,
}

impl RecordEvent {
    /// The node instant this event advances a topic's reconnect cursor to —
    /// the removal instant for a removal, the live version's `CreatedAt`
    /// instant (carried in `meta`) for a save. `None` if a save arrived
    /// without metadata (a plain, non-catch-up frame).
    #[must_use]
    pub fn instant(&self) -> Option<u64> {
        match self.kind {
            EventKind::Removed(at) => Some(at),
            EventKind::Saved => match self.meta.as_ref()?.succession {
                wavedb_core::Succession::CreatedAt(at) => Some(at),
                wavedb_core::Succession::Next(_) => None,
            },
        }
    }
}

/// A client→server message. The first message on a connection must be
/// [`Hello`](Self::Hello); everything else is refused until the identity
/// is bound.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum ClientMsg {
    /// Present the identity claim — once, binding the connection.
    Hello(Auth),
    /// Run one command as the bound caller (same frame as a POST body).
    Call(CommandFrame),
    /// Start watching a topic under the bound tenant.
    Subscribe(Topic),
    /// Stop watching a topic.
    Unsubscribe(Topic),
}

/// A server→client message.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum ServerMsg {
    /// The hello's identity verified; the connection is bound.
    HelloOk,
    /// One walk item of the in-flight call (calls are FIFO, so items
    /// always belong to the oldest unanswered call).
    Item(Vec<u8>),
    /// The in-flight call's final word.
    End(Response),
    /// A subscribed topic's mutation.
    Event(RecordEvent),
    /// The ack of a [`Subscribe`](ClientMsg::Subscribe) /
    /// [`Unsubscribe`](ClientMsg::Unsubscribe): messages are handled FIFO,
    /// so once this arrives the subscription table mutation is live and a
    /// watch established before a mutation cannot miss it. The `u64` is the
    /// topic's **current tail instant** — a fresh subscribe seeds its
    /// reconnect cursor from it (ignored on an unsubscribe ack).
    TopicOk(Topic, u64),
}

#[cfg(test)]
mod tests {
    use wavedb_core::expose::Command;
    use wavedb_core::{Id, LocalId, U48};
    use wavedb_wire::{from_wire, to_wire};

    use super::{ClientMsg, EventKind, RecordEvent, ServerMsg, Topic};
    use crate::frame::{Auth, CommandFrame, Response};

    #[test]
    fn client_messages_roundtrip_on_the_wire() {
        let topic = Topic {
            struct_hash: 0xFEED,
            pivot: Some(LocalId::new(7, true, 3)),
        };
        for msg in [
            ClientMsg::Hello(Auth::Anonymous {
                tenant: U48::from(9u32),
            }),
            ClientMsg::Call(CommandFrame {
                durable: false,
                struct_hash: 0xABCD,
                command: Command::Save,
                payload: vec![1, 2, 3],
            }),
            ClientMsg::Subscribe(topic),
            ClientMsg::Unsubscribe(topic),
        ] {
            assert_eq!(from_wire::<ClientMsg>(&to_wire(&msg)).unwrap(), msg);
        }
    }

    #[test]
    fn server_messages_roundtrip_on_the_wire() {
        for msg in [
            ServerMsg::HelloOk,
            ServerMsg::Item(vec![4, 5]),
            ServerMsg::End(Response::Ok(wavedb_core::expose::Reply::Done)),
            ServerMsg::Event(RecordEvent {
                topic: Topic {
                    struct_hash: 1,
                    pivot: None,
                },
                id: Id::new(7, U48::from(2u32), false, 1),
                kind: EventKind::Saved,
                meta: Some(wavedb_core::Metadata {
                    succession: wavedb_core::Succession::CreatedAt(42),
                    user: U48::from(2u32),
                    ..wavedb_core::Metadata::default()
                }),
                body: vec![9],
            }),
            ServerMsg::TopicOk(
                Topic {
                    struct_hash: 5,
                    pivot: Some(LocalId::new(2, false, 1)),
                },
                0,
            ),
        ] {
            assert_eq!(from_wire::<ServerMsg>(&to_wire(&msg)).unwrap(), msg);
        }
    }

    #[test]
    fn truncated_event_is_a_wire_error() {
        let bytes = to_wire(&ServerMsg::Event(RecordEvent {
            topic: Topic {
                struct_hash: 3,
                pivot: Some(LocalId::new(1, false, 0)),
            },
            id: Id::new(1, U48::from(1u32), false, 0),
            kind: EventKind::Removed(41),
            meta: None,
            body: Vec::new(),
        }));
        assert!(from_wire::<ServerMsg>(&bytes[..bytes.len() - 1]).is_err());
    }
}
