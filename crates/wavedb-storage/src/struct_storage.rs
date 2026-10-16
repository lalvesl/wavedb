//! [`StructStorage`] — one struct's storage **declaration**, and `TypeSlot`
//! — that struct's storage **state** inside one open store.
//!
//! `#[wavedb]` emits one `static StructStorage` per declared type (native
//! targets only), holding only what is fixed at compile time: the
//! `STRUCT_HASH` and the compression policy. The [`PageStore`] receives the
//! statics as an explicit registry at open — declared, not discovered,
//! exactly like the exposure lists — and builds a `TypeSlot` for each: that
//! type's cache, page directory and dictionary, with a lock per part, owned by
//! that store and nothing else.
//!
//! So the route from a `STRUCT_HASH` to its state is still resolved once, at
//! open, from a compile-time list; two types never contend on a shared lock;
//! and several stores can be open in one process, each over its own
//! directory (RFC 0067 — one store per shard).
//!
//! [`PageStore`]: crate::page_store::PageStore

use std::collections::{BTreeMap, BTreeSet};

use parking_lot::{Mutex, RwLock};
use wavedb_core::Id;
use wavedb_core::index::BPTREE_NODE_STRUCT_HASH;

use crate::dictionary::DictState;
use crate::directory::Directory;

/// One type's in-memory record cache: `Id.raw → wire bytes`, ordered by `Id`.
pub(crate) type StructMemCache = RwLock<BTreeMap<u128, Vec<u8>>>;

/// One type's page-directory slot. `None` until the store settles the type's
/// first page (the [`Directory`] needs the per-database seed).
pub(crate) type StructDirectory = Mutex<Option<Directory>>;

/// One type's compression slot: its zstd policy + dictionary + persisted run.
/// Per-type by nature — a page holds one type, so it compresses against that
/// type's own dictionary and nothing else.
pub(crate) type StructDictionary = Mutex<DictState>;

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

/// One type's state inside one open store — its own cache, directory,
/// compression dictionary and removal tombstones, with a lock per part.
#[derive(Debug)]
pub(crate) struct TypeSlot {
    struct_hash: u64,
    cache: StructMemCache,
    dir: StructDirectory,
    dict: StructDictionary,
    /// Ids removed from the cache whose page write hasn't settled yet — the
    /// read path must not fall through to the (stale) page for these. Marked
    /// at commit, cleared when the settle rewrites the page.
    dead: RwLock<BTreeSet<u128>>,
}

impl TypeSlot {
    /// Empty state for the type `declared`.
    pub(crate) const fn new(declared: &StructStorage) -> Self {
        Self {
            struct_hash: declared.struct_hash,
            cache: RwLock::new(BTreeMap::new()),
            dir: Mutex::new(None),
            dict: Mutex::new(DictState::new(declared.compress)),
            dead: RwLock::new(BTreeSet::new()),
        }
    }

    /// The `STRUCT_HASH` this slot stores.
    pub(crate) const fn struct_hash(&self) -> u64 {
        self.struct_hash
    }

    /// This type's in-memory cache — its own `RwLock`, shared with no other
    /// type, so reads of different types never contend.
    pub(crate) const fn mem_cache(&self) -> &StructMemCache {
        &self.cache
    }

    /// This type's page-directory slot — its own `Mutex`, shared with no other
    /// type. `None` until the store settles the first value.
    pub(crate) const fn directory(&self) -> &StructDirectory {
        &self.dir
    }

    /// This type's compression slot — its own `Mutex`, shared with no other
    /// type: the zstd policy, the raw-content dictionary, and the block run
    /// the dictionary is persisted in.
    pub(crate) const fn dictionary(&self) -> &StructDictionary {
        &self.dict
    }

    /// The cached bytes at `id`, if present (a read lock on this type only).
    pub(crate) fn get(&self, id: Id) -> Option<Vec<u8>> {
        self.cache.read().get(&id.raw()).cloned()
    }

    /// Number of records currently cached for this type.
    pub(crate) fn cached_len(&self) -> usize {
        self.cache.read().len()
    }

    /// Total bytes of cached record values for this type.
    pub(crate) fn cached_bytes(&self) -> usize {
        self.cache.read().values().map(Vec::len).sum()
    }

    /// Evict cached entries until at least `target` bytes are freed (or the
    /// cache empties), returning the bytes actually freed. Only called on
    /// **settled** entries (the store's eviction path guarantees it) — an
    /// evicted read falls through to the type's pages.
    // The write guard spans the whole eviction loop by design.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn evict_up_to(&self, target: usize) -> usize {
        let mut cache = self.cache.write();
        let mut freed = 0usize;
        while freed < target {
            let Some((_, bytes)) = cache.pop_first() else {
                break;
            };
            freed += bytes.len();
        }
        freed
    }

    /// Mark `id` removed-but-unsettled: its page still holds the old bytes,
    /// so the read fallback must answer `None` until the settle lands.
    pub(crate) fn mark_removed(&self, id: Id) {
        self.dead.write().insert(id.raw());
    }

    /// Clear `id`'s removal tombstone — its page now agrees (settled), or a
    /// newer `Put` superseded the remove.
    pub(crate) fn clear_removed(&self, id: Id) {
        self.dead.write().remove(&id.raw());
    }

    /// Whether `id` is removed but its page write hasn't settled yet.
    pub(crate) fn is_removed(&self, id: Id) -> bool {
        self.dead.read().contains(&id.raw())
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
    use wavedb_core::index::BPTREE_NODE_STRUCT_HASH;
    use wavedb_core::{Id, U48};

    static DECLARED: StructStorage = StructStorage::new(0xABCD);

    fn id(key: u64) -> Id {
        Id::new(key, U48::from(1u32), false, 0)
    }

    /// Two stores' slots for one declared type share nothing: state belongs
    /// to the slot, not to the static.
    #[test]
    fn two_slots_of_one_type_are_independent() {
        let (a, b) = (TypeSlot::new(&DECLARED), TypeSlot::new(&DECLARED));
        assert_eq!(a.struct_hash(), 0xABCD);
        assert!(a.dictionary().lock().enabled());

        a.mem_cache().write().insert(id(1).raw(), vec![7, 8]);
        assert_eq!(a.get(id(1)), Some(vec![7, 8]));
        assert_eq!(a.cached_len(), 1);
        assert_eq!(b.get(id(1)), None, "the other store's slot is untouched");
        assert!(b.directory().lock().is_none());
    }

    #[test]
    fn node_slot_is_reserved_and_uncompressed() {
        assert_eq!(BPTREE_NODE_STORAGE.struct_hash(), BPTREE_NODE_STRUCT_HASH);
        assert!(!BPTREE_NODE_STORAGE.compress());
        assert!(
            !TypeSlot::new(&BPTREE_NODE_STORAGE)
                .dictionary()
                .lock()
                .enabled()
        );
    }
}
