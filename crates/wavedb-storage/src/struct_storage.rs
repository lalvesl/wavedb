//! [`StructStorage`] — one struct's storage **declaration**, and `TypeSlot`
//! — that struct's storage **state** inside one open store.
//!
//! `#[wavedb]` emits one `static StructStorage` per declared type (native
//! targets only), holding only what is fixed at compile time: the
//! `STRUCT_HASH` and the compression policy. The [`PageStore`] receives the
//! statics as an explicit registry at open — declared, not discovered,
//! exactly like the exposure lists — and builds a `TypeSlot` for each: that
//! type's page directories, each in a cell of its own, owned by that store and
//! nothing else. The type's cached records live in its log, in the
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

use crate::ordered::Journal;
use crate::pages::{Pages, disk_pages, remote_pages};

/// One type's page index (RFC 0078). `None` until the store settles the
/// type's first page.
pub(crate) type StructDirectory = RefCell<Option<Pages>>;

/// One struct's storage declaration: its `STRUCT_HASH` and page compression.
///
/// `const`-constructible so the `#[wavedb]` macro can emit it as a `static` on
/// the declared type. It holds no state: an open store keeps that in a slot of
/// its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructStorage {
    struct_hash: u64,
    compress: bool,
    resident: bool,
    counted: bool,
}

impl StructStorage {
    /// A slot for `struct_hash`, pages compressed (the record/Pivot default).
    #[must_use]
    pub const fn new(struct_hash: u64) -> Self {
        Self {
            struct_hash,
            compress: true,
            resident: false,
            counted: false,
        }
    }

    /// A slot whose pages never run through zstd — for hot, constantly
    /// rewritten page kinds where the CPU spend doesn't pay, or a
    /// `#[wavedb(compress = false)]` type.
    #[must_use]
    pub const fn without_compression(struct_hash: u64) -> Self {
        Self {
            struct_hash,
            compress: false,
            resident: false,
            counted: false,
        }
    }

    /// The same declaration, its pages **resident**: an object-backed store
    /// never evicts them (RFC 0070 §10). For what navigation reads — the
    /// recency, removal, secondary and fuzzy chains, Pivots —
    /// so a walk never waits on the object store, only the record bytes it
    /// leads to do. Behaviour, not bytes: it does not fold into the hash.
    #[must_use]
    pub const fn resident(self) -> Self {
        Self {
            resident: true,
            ..self
        }
    }

    /// The same declaration, each page's entries **counted** in its index
    /// (RFC 0078 §7): a declared list's lane, whose length and page skips
    /// are then sums in RAM rather than reads.
    #[must_use]
    pub const fn counted(self) -> Self {
        Self {
            counted: true,
            ..self
        }
    }

    /// The `STRUCT_HASH` this slot stores.
    #[must_use]
    pub const fn struct_hash(&self) -> u64 {
        self.struct_hash
    }

    /// Whether this type's pages stay resident ([`resident`](Self::resident)).
    #[must_use]
    pub const fn is_resident(&self) -> bool {
        self.resident
    }

    /// Whether this type's pages compress — fixed by the declaration.
    #[must_use]
    pub const fn compress(&self) -> bool {
        self.compress
    }
}

/// One type's state inside one open store — its directories.
#[derive(Debug)]
pub(crate) struct TypeSlot {
    struct_hash: u64,
    /// Never evicted ([`StructStorage::resident`]).
    resident: bool,
    /// Its pages' entries counted ([`StructStorage::counted`]).
    counted: bool,
    /// Its pages compressed ([`StructStorage::compress`]).
    compress: bool,
    dir: StructDirectory,
    /// The object store's index for this type (RFC 0070 §2): 128 KiB
    /// pages, ordered by the same keys. `None` until the first upload.
    remote: StructDirectory,
    /// What of the object store's index the disk's headers have not named
    /// yet: its own journal is the object store's headers' (RFC 0078 §6).
    remote_journal: RefCell<Journal>,
}

impl TypeSlot {
    /// Empty state for the type `declared`.
    pub(crate) const fn new(declared: &StructStorage) -> Self {
        Self {
            struct_hash: declared.struct_hash,
            resident: declared.resident,
            counted: declared.counted,
            compress: declared.compress,
            dir: RefCell::new(None),
            remote: RefCell::new(None),
            remote_journal: RefCell::new(Journal::new()),
        }
    }

    /// The `STRUCT_HASH` this slot stores.
    pub(crate) const fn struct_hash(&self) -> u64 {
        self.struct_hash
    }

    /// Whether this type's pages are never evicted.
    pub(crate) const fn is_resident(&self) -> bool {
        self.resident
    }

    /// An empty disk index for this type.
    pub(crate) const fn new_pages(&self) -> Pages {
        disk_pages(self.counted)
    }

    /// An empty object-store index for this type.
    pub(crate) const fn new_remote_pages(&self) -> Pages {
        remote_pages(self.counted)
    }

    /// This type's page-directory slot — its own cell. `None` until the store settles the first value.
    pub(crate) const fn directory(&self) -> &StructDirectory {
        &self.dir
    }

    /// This type's directory in the object store — its own cell.
    pub(crate) const fn remote_directory(&self) -> &StructDirectory {
        &self.remote
    }

    /// What of the object store's index the disk's headers still owe.
    pub(crate) const fn remote_journal(&self) -> &RefCell<Journal> {
        &self.remote_journal
    }

    /// Whether this type's pages compress (zstd, each page alone: RFC 0081
    /// §9).
    pub(crate) const fn compresses(&self) -> bool {
        self.compress
    }

    /// How the write path stores this type's pages.
    pub(crate) const fn compression(&self) -> crate::page::Compression {
        crate::page::Compression::of(self.compress)
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
/// [`Exposure`]: wavedb_core::expose::Exposure
/// [`PageStore::open`]: crate::page_store::PageStore::open
pub trait StorageRegistry {
    /// The slots for every declared type (record + any generated Pivot),
    /// flattened into one registry list for [`PageStore::open`].
    ///
    /// [`PageStore::open`]: crate::page_store::PageStore::open
    fn storage_entries(&self) -> Vec<&'static StructStorage>;
}

/// One slot per registered type, sorted by `STRUCT_HASH`. Generated Pivot
/// types of identical shape may share a hash; they also share pages today,
/// so one slot serves the hash — dedup.
pub(crate) fn slots_of(types: &[&'static StructStorage]) -> Vec<TypeSlot> {
    let mut declared: Vec<&'static StructStorage> = types.to_vec();
    declared.sort_by_key(|s| s.struct_hash());
    declared.dedup_by_key(|s| s.struct_hash());
    declared.into_iter().map(TypeSlot::new).collect()
}

#[cfg(test)]
mod tests {
    use super::{StructStorage, TypeSlot};
    use crate::pages::disk_pages;

    static DECLARED: StructStorage = StructStorage::new(0xABCD);

    /// Two stores' slots for one declared type share nothing: state belongs
    /// to the slot, not to the static.
    #[test]
    fn two_slots_of_one_type_are_independent() {
        let (a, b) = (TypeSlot::new(&DECLARED), TypeSlot::new(&DECLARED));
        assert_eq!(a.struct_hash(), 0xABCD);
        assert!(a.compresses());

        *a.directory().borrow_mut() = Some(disk_pages(false));
        assert!(a.directory().borrow_mut().is_some());
        assert!(
            b.directory().borrow_mut().is_none(),
            "the other slot is untouched"
        );
    }
}
