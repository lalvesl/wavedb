//! The storage-engine error type.
//!
//! Distinct from [`wavedb_core::Error`]: this layer does real file I/O, so it adds
//! I/O and on-disk-corruption variants. Wire/codec faults from core flow through
//! [`StorageError::Core`].

use thiserror::Error;

/// Errors raised by `wavedb-storage`.
#[derive(Debug, Error)]
pub enum StorageError {
    /// An underlying filesystem error.
    #[error("storage io: {0}")]
    Io(#[from] std::io::Error),
    /// The `MANIFEST` did not start with the expected magic — not a
    /// WaveDB data file (or a corrupt one).
    #[error("not a wavedb data file (bad magic)")]
    BadMagic,
    /// The database was written by an incompatible on-disk format version.
    #[error("unsupported format version {0}")]
    BadVersion(u32),
    /// A read/write addressed blocks past the current end of the file.
    #[error("data file too short: need {need} bytes, file has {have}")]
    OutOfBounds {
        /// Bytes the operation required.
        need: u64,
        /// Bytes the file actually holds.
        have: u64,
    },
    /// A buffer handed to a positioned write exceeded its target run.
    #[error("write of {got} bytes exceeds run capacity {cap}")]
    RunOverflow {
        /// Bytes the caller tried to write.
        got: u64,
        /// Byte capacity of the target run.
        cap: u64,
    },
    /// An on-disk structure failed an integrity check on read (crc, bounds, tag).
    #[error("corrupt {0}")]
    Corrupt(&'static str),
    /// A record larger than the record cache can hold
    /// (`record_log::MAX_RECORD`), refused before anything is committed.
    #[error("a record of {0} bytes is too large to commit")]
    RecordTooLarge(usize),
    /// A write named a `STRUCT_HASH` with no registered [`StructStorage`] slot
    /// — the type was not listed at [`PageStore::open`]. Registration is an
    /// explicit allowlist (like exposure): unlisted types fail loudly here.
    ///
    /// [`StructStorage`]: crate::struct_storage::StructStorage
    /// [`PageStore::open`]: crate::page_store::PageStore::open
    #[error("no StructStorage registered for STRUCT_HASH {0:#018x}")]
    UnregisteredStructHash(u64),
    /// The data directory is already open — by another
    /// [`PageStore`](crate::page_store::PageStore) in this process or by
    /// another process. Its files are locked while a store has it open.
    #[error("the data directory is already open")]
    EngineBusy,
    /// A settle's write or barrier failed: the files' durable state is
    /// unknown, so the store refuses further writes until it is reopened and
    /// reconstructed from what is on disk (`crate::barrier`).
    #[error("a settle's write or barrier failed; reopen the store")]
    BarrierFailed,
    /// The object store failed a request (RFC 0070) — a socket fault, or an
    /// answer the client does not take for success.
    #[error("object store: {0}")]
    Remote(#[from] wavedb_platform::Error),
    /// The database was created with its pages in an object store and opened
    /// without one, or the other way round. The mode is fixed at creation
    /// (RFC 0070), recorded in the `MANIFEST`.
    #[error("the database is {} but was opened {}", if *object_backed { "object-backed" } else { "disk-only" }, if *object_backed { "without an object store" } else { "with one" })]
    ModeMismatch {
        /// What the `MANIFEST` records.
        object_backed: bool,
    },
    /// An object-store operation on a store that has none.
    #[error("this store has no object store")]
    NotObjectBacked,
    /// A file this store was writing already existed: another writer is
    /// using the same prefix (RFC 0070 §11). Every file is created only if
    /// absent, so the two never overwrite each other; the store that meets
    /// the other's file stops taking writes — what it holds that is not
    /// uploaded cannot go up any more — until it is reopened.
    #[error("another writer is using this object store prefix")]
    Fenced,
    /// A conversion found no database to convert ([`convert`](crate::convert)).
    #[error("there is no database to convert")]
    NoDatabase,
    /// A conversion to an object store found another database already under
    /// its prefix ([`convert`](crate::convert)): never written over.
    #[error("the object store prefix already holds a database")]
    ObjectStoreInUse,
    /// A core/codec fault surfaced inside the engine.
    #[error(transparent)]
    Core(#[from] wavedb_core::Error),
}

/// A held lock on a data file means another store has the directory open
/// ([`StorageError::EngineBusy`]); any other failure to lock is I/O.
impl From<std::fs::TryLockError> for StorageError {
    fn from(e: std::fs::TryLockError) -> Self {
        match e {
            std::fs::TryLockError::WouldBlock => Self::EngineBusy,
            std::fs::TryLockError::Error(io) => Self::Io(io),
        }
    }
}

/// Shorthand for a `Result` carrying [`StorageError`].
pub type StorageResult<T> = core::result::Result<T, StorageError>;

/// Flatten an engine error into [`wavedb_core::Error::Backend`] at the [`Store`]
/// boundary — core declares the trait but stays I/O-free, so it can't name a
/// concrete disk fault. A core error that merely passed *through* the engine
/// (a [`Write::Expect`] conflict, a codec fault) keeps its own type: callers
/// branch on those (a conflict means "re-plan", never "disk broke").
///
/// [`Store`]: wavedb_core::Store
/// [`Write::Expect`]: wavedb_core::Write::Expect
impl From<StorageError> for wavedb_core::Error {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::Core(inner) => inner,
            other => Self::Backend(other.to_string()),
        }
    }
}
