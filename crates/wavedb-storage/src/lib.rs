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
// The lint fires on the crate's `async fn`s in their *generic* setting; the
// concrete engine's futures are `Send`, and `thread_safety` below asserts it.
#![allow(clippy::future_not_send)]

pub mod alloc;
mod apply;
pub mod block;
pub mod block_file;
mod checkpoint;
mod commit;
mod defrag;
pub mod dictionary;
pub mod directory;
mod directory_pages;
mod edit;
pub mod error;
pub mod journal;
mod meta_log;
pub mod page;
pub mod page_cache;
pub mod page_store;
mod plan;
mod read_through;
mod retire;
mod settle;
pub mod struct_storage;

pub use block::{BLOCK_SIZE, BlockAllocator, BlockDescriptor, Run};
pub use block_file::{BlockFile, IoCounts, RESERVED_BLOCKS};
pub use defrag::DefragReport;
pub use dictionary::{DictState, Dictionary};
pub use directory::{Directory, bucket_index, hash_of};
pub use error::{StorageError, StorageResult};
pub use journal::{CommitFrame, Journal, JournalFrame};
pub use page::SlotPage;
pub use page_store::{DEFAULT_RECORD_CACHE_BYTES, PageStore, StoreOptions};
pub use settle::Progress;
pub use struct_storage::{
    BPTREE_NODE_STORAGE, StorageRegistry, StructDictionary, StructDirectory,
    StructMemCache, StructStorage,
};

/// The engine's thread-safety, **asserted rather than described**.
///
/// `PageStore` is `Send + Sync` and its futures are `Send` today, and nothing
/// arranges that: it holds `parking_lot` locks and `&'static StructStorage`
/// slots, and it never awaits, so both properties fall out of how the code
/// happens to be written. A single `Rc`/`RefCell` field, or one non-`Send`
/// value held across a future await point, would silently take them away.
///
/// That matters because two designs rest on them —
/// [RFC 0064](../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md) builds
/// multi-thread ownership on top, and
/// [RFC 0063](../../../rfcs/0063-engine-yield-map-and-interruptible-engine-PLANNED.md)'s
/// yield map is only meaningful if they hold. 0063 also records what happens to
/// a load-bearing property nobody asserts: its I1 and I2 became invariants by
/// accident, discovered years later by reading for something else. A comment
/// would have the same fate, so these are compile errors instead.
///
/// Both cost nothing at runtime — a `const` closure is type-checked and never
/// called.
mod thread_safety {
    use super::PageStore;
    use wavedb_core::store::Store;

    const _: fn() = || {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PageStore>();
    };

    // The type being `Send + Sync` does not imply its futures are: an `async
    // fn` body may hold something non-`Send` across an await. That is the
    // regression this second assertion catches and the first cannot.
    const _: fn(&PageStore) = |store| {
        fn assert_send<T: Send>(_: T) {}
        let id = wavedb_core::Id::from_raw(0);
        assert_send(async move { store.apply(&[]).await });
        assert_send(async move { store.get(id).await });
        assert_send(async move { store.get_of(0, id).await });
    };
}
