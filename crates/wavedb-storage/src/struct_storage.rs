//! [`StructStorage`] — one struct's storage **declaration**, and `TypeSlot`
//! — that struct's storage **state** inside one open store.
//!
//! `#[wavedb]` emits one `static StructStorage` per declared type (native
//! targets only), holding only what is fixed at compile time: the
//! `STRUCT_HASH` and the compression policy. The [`PageStore`] receives the
//! statics as an explicit registry at open — declared, not discovered,
//! exactly like the exposure lists — and builds a `TypeSlot` for each: that
//! type's page directory and dictionary, each in a cell of its own, owned by that
//! store and nothing else. The type's cached records live in its log, in the
//! store's record cache (`crate::record_cache`, RFC 0068), at the same slot
//! index.
//!
//! So the route from a `STRUCT_HASH` to its state is still resolved once, at
//! open, from a compile-time list; two types never share a cell;
//! and several stores can be open in one process, each over its own
//! directory (RFC 0067 — one store per shard).
//!
//! [`PageStore`]: crate::page_store::PageStore

use std::cell::RefCell;
use wavedb_core::index::BPTREE_NODE_STRUCT_HASH;

use crate::dictionary::DictState;
use crate::directory::Directory;

/// One type's page-directory slot. `None` until the store settles the type's
/// first page (the [`Directory`] needs the per-database seed).
pub(crate) type StructDirectory = RefCell<Option<Directory>>;

/// One type's compression slot: its zstd policy + dictionary + persisted run.
/// Per-type by nature — a page holds one type, so it compresses against that
/// type's own dictionary and nothing else.
pub(crate) type StructDictionary = RefCell<DictState>;

/// One struct's storage declaration: its `STRUCT_HASH` and page compression.
///
/// `const`-constructible so the `#[wavedb]` macro can emit it as a `static` on
/// the declared type. It holds no state: an open store keeps that in a slot of
/// its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructStorage {
    struct_hash: u64,
    compress: bool,
}

impl StructStorage {
    /// A slot for `struct_hash`, pages compressed (the record/Pivot default).
    #[must_use]
    pub const fn new(struct_hash: u64) -> Self {
        Self {
            struct_hash,
            compress: true,
        }
    }

    /// A slot whose pages never run through zstd — for hot, constantly
    /// rewritten page kinds where the CPU spend doesn't pay (`BpTree` nodes),
    /// or a `#[wavedb(compress = false)]` type.
    #[must_use]
    pub const fn without_compression(struct_hash: u64) -> Self {
        Self {
            struct_hash,
            compress: false,
        }
    }

    /// The `STRUCT_HASH` this slot stores.
    #[must_use]
    pub const fn struct_hash(&self) -> u64 {
        self.struct_hash
    }

    /// Whether this type's pages compress — fixed by the declaration; only the
    /// dictionary contents change at runtime.
    #[must_use]
    pub const fn compress(&self) -> bool {
        self.compress
    }
}

/// One type's state inside one open store — its directory and compression
/// dictionary.
#[derive(Debug)]
pub(crate) struct TypeSlot {
    struct_hash: u64,
    dir: StructDirectory,
    dict: StructDictionary,
}

impl TypeSlot {
    /// Empty state for the type `declared`.
    pub(crate) const fn new(declared: &StructStorage) -> Self {
        Self {
            struct_hash: declared.struct_hash,
            dir: RefCell::new(None),
            dict: RefCell::new(DictState::new(declared.compress)),
        }
    }

    /// The `STRUCT_HASH` this slot stores.
    pub(crate) const fn struct_hash(&self) -> u64 {
        self.struct_hash
    }

    /// This type's page-directory slot — its own cell. `None` until the store settles the first value.
    pub(crate) const fn directory(&self) -> &StructDirectory {
        &self.dir
    }

    /// This type's compression slot — its own cell: the zstd policy, the raw-content dictionary, and the block run
    /// the dictionary is persisted in.
    pub(crate) const fn dictionary(&self) -> &StructDictionary {
        &self.dict
    }
}

/// The storage half of a node registry — the [`StructStorage`] declarations
/// a node must register at [`PageStore::open`].
///
/// `expose_server!` emits this impl for its zero-sized `ServerRegistry`
/// (native targets only), flattening every listed type's
/// `storage_entries()`. So a node's `.registry(REGISTRY)` alone carries both
/// halves: the dispatch surface ([`Exposure`]) *and* the storage surface —
/// declared once, not discovered.
///
/// The reserved [`BPTREE_NODE_STORAGE`] slot is **not** returned here;
/// [`PageStore::open`] adds it automatically.
///
/// [`Exposure`]: wavedb_core::expose::Exposure
/// [`PageStore::open`]: crate::page_store::PageStore::open
pub trait StorageRegistry {
    /// The slots for every declared type (record + any generated Pivot),
    /// flattened into one registry list for [`PageStore::open`].
    ///
    /// [`PageStore::open`]: crate::page_store::PageStore::open
    fn storage_entries(&self) -> Vec<&'static StructStorage>;
}

/// The reserved slot every `BpTree` node value settles into.
///
/// Compression off: node pages are rewritten on every index mutation, so zstd
/// there is CPU for nothing. [`PageStore::open`] registers it automatically —
/// callers list only their own types.
///
/// [`PageStore::open`]: crate::page_store::PageStore::open
pub static BPTREE_NODE_STORAGE: StructStorage =
    StructStorage::without_compression(BPTREE_NODE_STRUCT_HASH);

#[cfg(test)]
mod tests {
    use super::{BPTREE_NODE_STORAGE, StructStorage, TypeSlot};
    use crate::directory::Directory;
    use wavedb_core::index::BPTREE_NODE_STRUCT_HASH;

    static DECLARED: StructStorage = StructStorage::new(0xABCD);

    /// Two stores' slots for one declared type share nothing: state belongs
    /// to the slot, not to the static.
    #[test]
    fn two_slots_of_one_type_are_independent() {
        let (a, b) = (TypeSlot::new(&DECLARED), TypeSlot::new(&DECLARED));
        assert_eq!(a.struct_hash(), 0xABCD);
        assert!(a.dictionary().borrow_mut().enabled());

        *a.directory().borrow_mut() = Some(Directory::new([1, 2, 3, 4]));
        assert!(a.directory().borrow_mut().is_some());
        assert!(
            b.directory().borrow_mut().is_none(),
            "the other slot is untouched"
        );
    }

    #[test]
    fn node_slot_is_reserved_and_uncompressed() {
        assert_eq!(BPTREE_NODE_STORAGE.struct_hash(), BPTREE_NODE_STRUCT_HASH);
        assert!(!BPTREE_NODE_STORAGE.compress());
        assert!(
            !TypeSlot::new(&BPTREE_NODE_STORAGE)
                .dictionary()
                .borrow_mut()
                .enabled()
        );
    }
}
