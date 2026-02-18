//! The workspace error type.

use thiserror::Error;

use crate::id::Id;
use crate::local_id::LocalId;
use crate::u48::U48;

/// Errors raised by `wavedb-core`. Wire (de)serialization faults arrive through
/// the [`Wire`](Error::Wire) variant (from the standalone `wavedb-wire` crate);
/// the rest are core/engine concerns.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    /// A wire (de)serialization fault — a buffer/size mismatch or an intrinsic
    /// per-type check (see [`wavedb_wire::Error`]).
    #[error(transparent)]
    Wire(#[from] wavedb_wire::Error),
    /// A value handed to `U48::new` did not fit in 48 bits.
    #[error("value {0} exceeds 48 bits")]
    U48Overflow(u64),
    /// A wire envelope carried a `STRUCT_HASH` not declared in this build's
    /// registry (a record written under a schema this binary doesn't know).
    #[error("unknown struct hash {0:#018x}")]
    UnknownStructHash(u64),

    /// A value read as a chain segment or sparse-index node did not start
    /// with its lane's reserved hash — the pointer resolved to some other
    /// kind of value.
    #[error("foreign lane tag {0:#018x}")]
    LaneBadTag(u64),
    /// A chain segment or sparse-index pointer resolved to nothing in the
    /// backing [`Store`](crate::Store) — a dangling neighbour, child or root
    /// pointer. The chain's own writes are one atomic batch, so this means the
    /// structure disagrees with the store, never a half-applied mutation.
    #[error("chain node {0:?} missing")]
    ChainNodeMissing(LocalId),
    /// A collection handle's `Pivot` record was not in the [`Store`] — a stale
    /// or foreign `PivotId`.
    ///
    /// [`Store`]: crate::Store
    #[error("pivot record {0:?} missing")]
    PivotMissing(LocalId),
    /// An index pointed at a record the [`Store`] no longer holds — index out
    /// of sync with the record space.
    ///
    /// [`Store`]: crate::Store
    #[error("record {0:?} missing")]
    RecordMissing(Id),
    /// A batch's [`Write::Expect`] guard found the record changed between
    /// the plan's read and the commit — a concurrent save of the same
    /// anchor. Nothing was written; the caller re-plans against the new
    /// live version.
    ///
    /// [`Write::Expect`]: crate::Write::Expect
    #[error("write conflict at {0:?}")]
    Conflict(Id),
    /// The record at a live anchor carried an archive's `Succession::Next`
    /// — the version chain at this id is corrupt.
    #[error("version chain corrupt at {0:?}")]
    ChainCorrupt(Id),
    /// A save addressed an id that is not the value's own content-derived
    /// anchor (`#[wavedb::key(...)]` types): the key fields ARE the
    /// identity, so "renaming" is an explicit `remove` + `insert` of the
    /// new key — never a silent write that would duplicate the record or
    /// orphan its indexes.
    #[error("value's natural key does not derive the addressed id {0:?}")]
    KeyMismatch(Id),
    /// A secondary-index lookup named an index this collection's `Pivot` does
    /// not declare (out of `0..NUM_SECONDARIES`).
    #[error("secondary index {0} out of range")]
    SecondaryIndexOutOfRange(usize),
    /// A fuzzy read named an index this collection's `Pivot` does not
    /// declare (out of `0..NUM_FUZZY`).
    #[error("fuzzy index {0} out of range")]
    FuzzyOutOfRange(usize),
    /// A list read named an ordering this collection's `Pivot` does not declare
    /// (out of `0..NUM_LISTS`).
    #[error("declared list {0} out of range")]
    ListOutOfRange(usize),
    /// The caller's identity tier may not perform this operation (M8 gate:
    /// an unauthenticated caller on a login-required item). The message is
    /// evidence for the log; the wire flattens it to one uniform kind.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// A **client** struct command arrived with `user != tenant`.
    ///
    /// The engine's identity model is one user per tenant: a tenant is the
    /// isolation boundary *and* the principal, so a verified token whose two
    /// halves disagree describes an authorization question the engine has no
    /// answer for — `Metadata.permission` is carried but never consulted
    /// (`crate::permission`), so admitting the write would silently grant
    /// blanket access inside the tenant rather than the narrower one the
    /// mismatch implies.
    ///
    /// Refusing here is the honest answer until intra-tenant permissions
    /// exist. It binds the **client** path only: server-side code runs as
    /// whatever identity it chooses (`ServerDb::as_identity`), because the
    /// node is the authority, not a principal being checked.
    #[error(
        "identity mismatch: user {0:?} under tenant {1:?} (one user per tenant)"
    )]
    IdentityMismatch(U48, U48),
    /// A `#[server]` body reached past the owners it holds (RFC 0067 §5): a
    /// second `lock` while one is held, an operation on an owner the held
    /// lock does not cover, or a lock asked of a handle that cannot take one.
    ///
    /// Taking every owner a body needs in **one** `lock` is what keeps two
    /// bodies from each holding an owner and waiting for the other's, so a
    /// body that reaches further is refused rather than allowed to wait.
    #[error("lock order: {0}")]
    LockOrder(&'static str),
    /// A failure inside a [`Store`](crate::Store) backend — disk I/O, on-disk
    /// corruption, or similar. Core stays I/O-free, so the concrete cause is
    /// flattened to a message at the trait boundary.
    #[error("storage backend: {0}")]
    Backend(String),
}

/// Shorthand for a `Result` carrying the workspace [`Error`](enum@Error).
pub type Result<T> = core::result::Result<T, Error>;
