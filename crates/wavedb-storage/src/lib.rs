//! `wavedb-storage` — the per-node engine: block manager, per-`STRUCT_HASH`
//! page directory (linear hashing), page format, dictionaries, journal pipeline.
//!
//! Built bottom-up. The [`block`] layer (descriptor + allocator), the
//! [`directory`] addressing math, and the [`block_file`] I/O seam are in place;
//! the page format, dictionaries, and journal pipeline follow. See
//! `crates/wavedb-storage/README.md` for the target design.

// Byte-precise packing/hashing code casts deliberately between integer widths.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::cast_sign_loss
)]
// The engine is used from one thread and its futures borrow it, so they are
// not `Send` (see `thread_safety` below).
#![allow(clippy::future_not_send)]

pub mod alloc;
mod apply;
pub mod barrier;
pub mod block;
pub mod block_file;
mod checkpoint;
mod commit;
mod commit_steps;
mod defrag;
pub mod dictionary;
pub mod directory;
mod directory_pages;
mod edit;
pub mod error;
pub mod journal;
mod journal_frame;
mod meta_log;
pub mod page;
pub mod page_cache;
pub mod page_store;
mod plan;
mod read_through;
mod record_cache;
mod record_index;
mod record_log;
mod retire;
mod settle;
pub mod shard_set;
pub mod struct_storage;

pub use barrier::{Barrier, Durable};
pub use block::{BLOCK_SIZE, BlockAllocator, BlockDescriptor, Run};
pub use block_file::{BlockFile, IoCounts, RESERVED_BLOCKS};
pub use commit_steps::Checkpointing;
pub use defrag::DefragReport;
pub use dictionary::{DictState, Dictionary};
pub use directory::{Directory, bucket_index, hash_of};
pub use error::{StorageError, StorageResult};
pub use journal::{CommitFrame, Journal, JournalFrame};
pub use page::SlotPage;
pub use page_store::{DEFAULT_RECORD_CACHE_BYTES, PageStore, StoreOptions};
pub use settle::Progress;
pub use shard_set::{MANIFEST, open_shards, open_shards_with};
pub use struct_storage::{BPTREE_NODE_STORAGE, StorageRegistry, StructStorage};

/// The engine's threading contract, **asserted rather than described**.
///
/// A store is used from **one thread**. Its state lives in plain cells
/// (`RefCell`, `Cell`) — no lock, no atomic — because nothing reaches it from
/// two threads at once: a node moves each store onto the worker that owns it
/// (RFC 0067), a benchmark consumer builds its driver on its own thread, and
/// the one other thread the engine runs, the barrier helper, shares nothing
/// with it — each request carries its own file descriptor and a channel to
/// answer on (`crate::barrier`).
///
/// So `PageStore` is `Send` — it must move to its owner's thread once, at
/// startup, and the parallel open builds stores on scoped threads — and is
/// deliberately **not** `Sync`, which is what makes the cells sound without
/// a lock. Its futures borrow it, so they are not `Send` either, the stance
/// `#![allow(clippy::future_not_send)]` above records.
///
/// A non-`Send` field would take the first property away silently, so it is
/// a compile error instead. It costs nothing at runtime — a `const` closure
/// is type-checked and never called.
mod thread_safety {
    use super::PageStore;

    const _: fn() = || {
        const fn assert_send<T: Send>() {}
        assert_send::<PageStore>();
    };
}
