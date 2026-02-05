//! `wavedb-storage` — the per-node engine: block manager, per-`STRUCT_HASH`
//! page directory (linear hashing), page format, dictionaries, and the settle
//! pipeline whose batches are the durability point (RFC 0072). See
//! `crates/wavedb-storage/README.md` for the design.

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
mod backing;
pub mod barrier;
pub mod block;
pub mod block_file;
mod checkpoint;
mod compact;
mod convert;
mod cycle;
mod data_file;
mod defrag;
pub mod dictionary;
pub mod directory;
mod directory_pages;
mod disk_chain;
mod disk_header;
pub mod error;
mod evict;
mod file_space;
mod header;
mod link;
pub mod manifest;
mod object_mode;
pub mod page;
pub mod page_cache;
pub mod page_store;
mod plan;
mod read_through;
mod reader;
mod record_cache;
mod record_index;
mod record_log;
mod record_segment;
mod recover;
mod recover_headers;
pub mod remote;
mod remote_compact;
mod reopen;
mod restate;
mod roots;
mod roots_find;
mod settle;
pub mod shard_set;
mod storage_compact;
mod storage_open;
mod storage_shard;
mod storage_write;
mod store_options;
pub mod struct_storage;
mod upload;
mod upload_file;
mod upload_plan;
mod upload_publish;
mod wait;
mod walk;
mod window;

pub use alloc::{Cut, DEFAULT_FILE_BYTES, FileUse};
pub use barrier::{Barrier, Durable};
pub use block::{BLOCK_SIZE, BlockAllocator, BlockDescriptor, Run};
pub use block_file::{BlockFile, IoCounts};
pub use compact::{Compacted, Compacting, Space};
pub use convert::{Backend, convert};
pub use dictionary::{DictState, Dictionary};
pub use directory::{Directory, bucket_index, hash_of};
pub use error::{StorageError, StorageResult};
pub use manifest::{MANIFEST, Manifest};
pub use page::SlotPage;
pub use page_store::{DEFAULT_RECORD_CACHE_BYTES, PageStore, StoreOptions};
pub use remote::Remote;
pub use remote_compact::{MOVE_BLOCKS, RemoteCompacted};
pub use settle::Progress;
pub use shard_set::{open_object_shards, open_shards, open_shards_with};
pub use struct_storage::{BPTREE_NODE_STORAGE, StorageRegistry, StructStorage};
pub use upload::Uploaded;

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
