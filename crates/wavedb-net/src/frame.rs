//! The self-contained request/response envelopes.
//!
//! **One uniform frame** carries every operation: a record command today, a
//! `#[server]` function call in M4 — functions and structs share the
//! `STRUCT_HASH` space, so nothing at the frame level tells them apart. The
//! transport is a dumb tunnel: identity, the command, and errors all ride
//! *inside* these wire values, never in HTTP headers or status codes.

use wavedb_core::expose::{Command, Reply};
use wavedb_core::{Error as CoreError, U48};
use wavedb_wire::WaveWire;

use crate::sync::TopicCursor;

/// One operation to run on the node.
///
/// Which item (`struct_hash`), which op (`command` — ignored by a
/// server-function arm, whose hash *is* the operation), and the operation's
/// payload bytes (a record body, an `Id`, or an args tuple).
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct CommandFrame {
    /// A `#[wavedb]` struct or (M4) a `#[server]` fn — one hash space.
    pub struct_hash: u64,
    /// The struct op; a function arm ignores it.
    pub command: Command,
    /// The op's wire payload, decoded by the matched arm.
    pub payload: Vec<u8>,
    /// A write's `_durable` twin (RFC 0072 §12): the node answers once the
    /// batch is durable on the owner's shard. One bit rather than four more
    /// commands; a read and a function ignore it.
    pub durable: bool,
}

impl CommandFrame {
    /// A frame for `command` on `struct_hash`, answered once committed.
    #[must_use]
    pub const fn new(
        struct_hash: u64,
        command: Command,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            struct_hash,
            command,
            payload,
            durable: false,
        }
    }

    /// The same frame, answered only once its write is durable.
    #[must_use]
    pub const fn durable(mut self) -> Self {
        self.durable = true;
        self
    }
}

/// The request's identity claim — verified node-side (gate 1) into the
/// caller the engine executes as. `Hash` because the connection manager
/// keys its per-identity connections on `(addr, Auth)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, WaveWire)]
pub enum Auth {
    /// No token: the unauthenticated tier (`user = U48::MAX`). The claimed
    /// tenant only scopes `#[server(public)]` functions (login/register);
    /// every other operation refuses.
    Anonymous {
        /// The tenant public functions execute under.
        tenant: U48,
    },
    /// A signed access token ([`crate::auth`]); `user`/`tenant` come from
    /// the verified claims — a claimed tenant cannot override them.
    Token(Vec<u8>),
}

/// A complete request — everything the node needs, in the POST body.
/// Identity rides in [`Auth`], never in an HTTP header.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct Request {
    /// The identity claim gate 1 verifies.
    pub auth: Auth,
    /// The operation.
    pub frame: CommandFrame,
    /// Poll subscriptions to piggyback a sync delta onto the reply (W7):
    /// each declared topic with the caller's cursor. Empty = no piggyback,
    /// the node answers the command alone. Stays stateless — the cursors ride
    /// here, the node holds nothing.
    pub sync: Vec<TopicCursor>,
}

/// What kind of node-side failure a [`NodeError`] reports — the typed half
/// of the core error, flattened for the wire (evidence rides in `message`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, WaveWire)]
pub enum NodeErrorKind {
    /// Unlisted type, excluded op, or wrong-shape command — uniform refusal.
    UnknownStructHash,
    /// The payload did not decode as the declared type.
    Wire,
    /// A collection handle's `Pivot` record was missing.
    PivotMissing,
    /// An index pointed at a record the store no longer holds.
    RecordMissing,

    /// A value read as a chain segment or sparse-index node had a foreign
    /// lane tag.
    LaneBadTag,
    /// A chain segment or sparse-index pointer resolved to nothing.
    ChainNodeMissing,
    /// A secondary-index lookup out of the declared range.
    SecondaryIndexOutOfRange,
    /// A value did not fit in 48 bits.
    U48Overflow,
    /// A storage-backend fault (disk I/O, corruption).
    Backend,
    /// Identity refused: missing/expired/forged token, or an operation the
    /// caller's tier may not perform. One uniform kind — which check failed
    /// is deliberately not distinguishable on the wire.
    Unauthorized,
    /// A concurrent save of the same anchor won the race — the op wrote
    /// nothing; the caller re-reads and re-plans.
    Conflict,
    /// The version chain at a live anchor is corrupt node-side.
    ChainCorrupt,
    /// A save addressed an id that is not the value's own content-derived
    /// (`#[wavedb::key(...)]`) anchor — renaming is remove + insert.
    KeyMismatch,
    /// A list read named an ordering the type does not declare.
    ListOutOfRange,
    /// A fuzzy read named an undeclared index.
    FuzzyOutOfRange,
    /// A struct command's verified token carried `user != tenant` — the
    /// engine's model is one user per tenant. Distinct from
    /// [`Unauthorized`](Self::Unauthorized) on purpose: the caller *is*
    /// authenticated, and the fix is the token's shape rather than a login.
    IdentityMismatch,
    /// A `#[server]` body reached past the owners it holds (RFC 0067 §5) —
    /// a node-side programming fault, reported to the caller as one.
    LockOrder,
}

/// A structured node-side rejection, riding **inside** the 200 response —
/// HTTP status stays transport-only.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire, thiserror::Error)]
#[error("node error ({kind:?}) on {struct_hash:#018x}: {message}")]
pub struct NodeError {
    /// The failure class.
    pub kind: NodeErrorKind,
    /// The frame's `struct_hash` the failure is about.
    pub struct_hash: u64,
    /// Human-readable evidence (the core error's display).
    pub message: String,
}

impl NodeError {
    /// Flatten a core error into its wire shape, stamped with the frame's
    /// `struct_hash`.
    #[must_use]
    pub fn from_core(struct_hash: u64, err: &CoreError) -> Self {
        let kind = match err {
            CoreError::Wire(_) => NodeErrorKind::Wire,
            CoreError::U48Overflow(_) => NodeErrorKind::U48Overflow,
            CoreError::UnknownStructHash(_) => NodeErrorKind::UnknownStructHash,

            CoreError::LaneBadTag(_) => NodeErrorKind::LaneBadTag,
            CoreError::ChainNodeMissing(_) => NodeErrorKind::ChainNodeMissing,
            CoreError::PivotMissing(_) => NodeErrorKind::PivotMissing,
            CoreError::RecordMissing(_) => NodeErrorKind::RecordMissing,
            CoreError::SecondaryIndexOutOfRange(_) => {
                NodeErrorKind::SecondaryIndexOutOfRange
            }
            CoreError::Unauthorized(_) => NodeErrorKind::Unauthorized,
            CoreError::Backend(_) => NodeErrorKind::Backend,
            CoreError::Conflict(_) => NodeErrorKind::Conflict,
            CoreError::ChainCorrupt(_) => NodeErrorKind::ChainCorrupt,
            CoreError::KeyMismatch(_) => NodeErrorKind::KeyMismatch,
            CoreError::ListOutOfRange(_) => NodeErrorKind::ListOutOfRange,
            CoreError::FuzzyOutOfRange(_) => NodeErrorKind::FuzzyOutOfRange,
            CoreError::IdentityMismatch(..) => NodeErrorKind::IdentityMismatch,
            CoreError::LockOrder(_) => NodeErrorKind::LockOrder,
        };
        Self {
            kind,
            struct_hash,
            message: err.to_string(),
        }
    }
}

/// The node's answer: the executed command's [`Reply`], or a structured
/// refusal. Always shipped in a 200 — an HTTP-level failure means the
/// *transport* broke, not the operation.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum Response {
    /// The command executed; its reply.
    Ok(Reply),
    /// The node refused or failed the command.
    Err(NodeError),
}

impl Response {
    /// Convert into a plain `Result`.
    ///
    /// # Errors
    /// The [`NodeError`] the node shipped, when the response is `Err`.
    pub fn into_result(self) -> core::result::Result<Reply, NodeError> {
        match self {
            Self::Ok(reply) => Ok(reply),
            Self::Err(e) => Err(e),
        }
    }
}

/// One frame of a response body.
///
/// Every response is a **sequence** of these (`[len u32 LE][wire]` each): an
/// optional **leading** [`Sync`](Self::Sync) piggyback delta (W7), then zero
/// or more [`Item`](Self::Item)s — a walk's records, written as the node
/// produces them — then exactly one [`End`](Self::End) carrying the exchange's
/// final word. A scalar command is the degenerate case: no items, just the
/// `End`. A fault mid-walk ends the stream early with `End(Err(..))` after the
/// items already shipped.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum StreamFrame {
    /// One walk item's wire bytes (a record body, or a `(Metadata, body)`
    /// pair for a history walk).
    Item(Vec<u8>),
    /// A piggybacked sync delta (W7): the wire-encoded [`SyncReply`] for the
    /// request's declared poll topics. When present it **leads** the response
    /// (before any item), so the connection manager peels it off the front
    /// without parsing the command's own frames. Absent unless the request
    /// carried a `sync` declaration.
    ///
    /// [`SyncReply`]: crate::sync::SyncReply
    Sync(Vec<u8>),
    /// The final word; nothing follows on the connection.
    End(Response),
}

#[cfg(test)]
mod tests {
    use wavedb_core::expose::{Command, Reply};
    use wavedb_core::{Error as CoreError, U48};
    use wavedb_wire::{from_wire, to_wire};

    use super::{
        Auth, CommandFrame, NodeError, NodeErrorKind, Request, Response,
        StreamFrame,
    };

    fn request() -> Request {
        Request {
            auth: Auth::Anonymous {
                tenant: U48::from(42u32),
            },
            frame: CommandFrame {
                durable: false,
                struct_hash: 0xABCD,
                command: Command::Save,
                payload: vec![1, 2, 3],
            },
            sync: Vec::new(),
        }
    }

    #[test]
    fn request_roundtrips_on_the_wire() {
        let r = request();
        assert_eq!(from_wire::<Request>(&to_wire(&r)).unwrap(), r);
    }

    #[test]
    fn request_with_a_piggyback_declaration_roundtrips() {
        use crate::sync::TopicCursor;
        use crate::ws::Topic;

        let mut r = request();
        r.sync = vec![
            TopicCursor {
                topic: Topic {
                    struct_hash: 0x1234,
                    pivot: None,
                },
                since: Some(7),
            },
            TopicCursor {
                topic: Topic {
                    struct_hash: 0x1234,
                    pivot: None,
                },
                since: None,
            },
        ];
        assert_eq!(from_wire::<Request>(&to_wire(&r)).unwrap(), r);
    }

    #[test]
    fn sync_frame_roundtrips_on_the_wire() {
        let f = StreamFrame::Sync(vec![9, 8, 7]);
        assert_eq!(from_wire::<StreamFrame>(&to_wire(&f)).unwrap(), f);
    }

    #[test]
    fn response_roundtrips_both_arms() {
        let ok = Response::Ok(Reply::Value(Some(vec![9])));
        assert_eq!(from_wire::<Response>(&to_wire(&ok)).unwrap(), ok);

        let err = Response::Err(NodeError {
            kind: NodeErrorKind::UnknownStructHash,
            struct_hash: 0xFEED,
            message: "unknown struct hash".into(),
        });
        assert_eq!(from_wire::<Response>(&to_wire(&err)).unwrap(), err);
    }

    #[test]
    fn truncated_request_is_a_wire_error() {
        let bytes = to_wire(&request());
        assert!(from_wire::<Request>(&bytes[..bytes.len() - 2]).is_err());
    }

    #[test]
    fn node_error_flattens_core_variants() {
        let e = NodeError::from_core(7, &CoreError::UnknownStructHash(0xBEEF));
        assert_eq!(e.kind, NodeErrorKind::UnknownStructHash);
        assert_eq!(e.struct_hash, 7);
        assert!(e.message.contains("unknown struct hash"));

        let e = NodeError::from_core(7, &CoreError::Backend("disk".into()));
        assert_eq!(e.kind, NodeErrorKind::Backend);
        assert!(e.message.contains("disk"));
    }

    #[test]
    fn response_into_result_maps_arms() {
        assert_eq!(
            Response::Ok(Reply::Done).into_result().unwrap(),
            Reply::Done
        );
        let err = NodeError {
            kind: NodeErrorKind::Backend,
            struct_hash: 1,
            message: "x".into(),
        };
        assert_eq!(Response::Err(err.clone()).into_result().unwrap_err(), err);
    }
}
