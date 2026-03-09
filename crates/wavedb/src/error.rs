//! The client-facing error type.
//!
//! Wraps the layers a typed call crosses — the core engine error, the
//! transport error, and a node-side refusal — plus the small set of
//! application-level errors a `#[server]` body raises (`not_found`,
//! `already_exists`, `unauthorized`).

use thiserror::Error;

/// A failure from a typed client call.
#[derive(Debug, Error)]
pub enum Error {
    /// A core engine / wire fault (e.g. a body that failed to decode).
    #[error(transparent)]
    Core(#[from] wavedb_core::Error),
    /// A transport fault (socket, HTTP framing).
    #[error(transparent)]
    Transport(#[from] wavedb_net::Error),
    /// The node refused or failed the command (a structured rejection that
    /// rode back inside a 200).
    #[error(transparent)]
    Node(#[from] wavedb_net::NodeError),
    /// The node answered with a reply shape the call did not expect (e.g. an
    /// `Inserted` where a `Value` was due) — a protocol mismatch.
    #[error("unexpected reply from node")]
    UnexpectedReply,
    /// The requested record / entity does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// A create conflicted with an existing record / entity.
    #[error("already exists: {0}")]
    AlreadyExists(String),
    /// The caller is not authorized for the operation.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
}

impl Error {
    /// A "not found" error carrying `what`.
    pub fn not_found(what: impl Into<String>) -> Self {
        Self::NotFound(what.into())
    }

    /// An "already exists" error carrying `what`.
    pub fn already_exists(what: impl Into<String>) -> Self {
        Self::AlreadyExists(what.into())
    }

    /// An "unauthorized" error carrying `why`.
    pub fn unauthorized(why: impl Into<String>) -> Self {
        Self::Unauthorized(why.into())
    }
}

/// Flatten the client error back into the core error a `#[server]`
/// dispatch step must return — the typed variants keep their identity
/// (`Unauthorized` stays refusable as such at the node); the transport
/// wrappers, which a node-side body only hits through nested calls,
/// flatten to `Backend` evidence.
impl From<Error> for wavedb_core::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Core(c) => c,
            Error::Unauthorized(why) => Self::Unauthorized(why),
            other => Self::Backend(other.to_string()),
        }
    }
}

/// A view or decode fault (`wire.view()?`, `wire.try_into()?`) through the
/// core seam: it is [`wavedb_core::Error::Wire`].
impl From<wavedb_core::wire::Error> for Error {
    fn from(e: wavedb_core::wire::Error) -> Self {
        Self::Core(e.into())
    }
}

/// Shorthand for a `Result` carrying the client [`Error`](enum@Error).
pub type Result<T> = core::result::Result<T, Error>;
