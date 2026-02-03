//! [`StoreOptions`] — what a [`PageStore`](crate::PageStore) is opened with:
//! the cache budgets, the data files' size, and the object store its pages
//! live in, if any. Split from [`crate::page_store`].

/// How much RAM a store may spend answering reads, and where its pages live —
/// chosen once at [`open_with`](crate::PageStore::open_with) (RFCs 0044,
/// 0065 §6, 0070, 0071).
///
/// Opened, never inferred: a cache budget that drifts is a memory ceiling
/// nobody can hold a process to. Durability is not among them: it is chosen
/// per write (`apply` or [`apply_durable`](crate::PageStore::apply_durable),
/// RFC 0072).
///
/// **The cache budgets never reach stored bytes**, so they fold into no
/// `STRUCT_HASH`: they change what is held in RAM, never what is written.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    /// Bytes of **decoded pages** the block file may hold (RFC 0044) — by
    /// run, on disk and in the object store, decompressed once when read.
    ///
    /// Zero disables the cache. Read once at open: a budget that changed
    /// under a running store would make "how much RAM does this hold" a
    /// question with no answer.
    pub page_cache_bytes: usize,

    /// Bytes of **decoded records** the caches should be evicted down to —
    /// the budget [`evict_to_budget`](crate::PageStore::evict_to_budget)
    /// targets.
    ///
    /// **The store never applies it on its own.** Eviction stays where it
    /// already is, in the caller's hands (the node's upkeep loop, a
    /// benchmark's phase boundary), because a `PageStore` that dropped cache
    /// entries on a timer of its own would be doing work no caller asked for
    /// inside a window someone is timing. What this field changes is only
    /// *where the number lives*: opened with the store, instead of a constant
    /// remembered separately at every call site.
    pub record_cache_bytes: usize,

    /// Where the pages live besides the disk (RFC 0070): `Some` makes the
    /// disk a cache of this object store. Fixed when the database is
    /// created — the `MANIFEST` records it, and opening with the other
    /// answer is [`StorageError::ModeMismatch`](crate::StorageError::ModeMismatch).
    /// The endpoint and the credentials are not stored anywhere: they come
    /// with every open.
    /// [`PageStore::open_object`](crate::PageStore::open_object) opens the
    /// same object store with no disk at all (object mode).
    pub remote: Option<crate::remote::Remote>,
    /// Bytes a data file grows to before the next one opens (RFC 0071),
    /// [`DEFAULT_FILE_BYTES`](crate::DEFAULT_FILE_BYTES) by default. A run
    /// is never split, so the run that reaches it lands whole. It decides
    /// only when a file closes — the directory listing says where every file
    /// is — so it may change between runs, and files of different sizes
    /// coexist. Fewer, larger files hold fewer descriptors open; smaller ones
    /// give space back in smaller steps.
    pub file_bytes: u64,
    /// How much of a data file must be free — holes, or runs held back for
    /// a recovery root — before compaction empties it (RFC 0071 §7), 50 by
    /// default. A file mostly alive is not rewritten for little.
    pub victim_free_percent: u8,
    /// Storage shards a **new** database is created with (RFC 0073 §1),
    /// dividing its data shards: data shard `p` writes through storage shard
    /// `p / (P / S)`. `0`, the default, is one per data shard. Fixed at
    /// creation — the `MANIFEST` records it, and a later open's answer is
    /// ignored, as its data shard count is.
    pub storage_shards: usize,
}

/// Bytes of decoded records [`StoreOptions::record_cache_bytes`] defaults to.
///
/// The number callers were already passing by hand, so the default changes
/// nothing; it is advisory in the strict sense, since nothing evicts until a
/// caller asks.
pub const DEFAULT_RECORD_CACHE_BYTES: usize = 96 << 20;

impl Default for StoreOptions {
    /// A 64 MiB page cache, and a record-cache budget nothing applies
    /// unless asked.
    fn default() -> Self {
        Self {
            page_cache_bytes: crate::page_cache::DEFAULT_BUDGET_BYTES,
            record_cache_bytes: DEFAULT_RECORD_CACHE_BYTES,
            remote: None,
            file_bytes: crate::alloc::DEFAULT_FILE_BYTES,
            victim_free_percent: 50,
            storage_shards: 0,
        }
    }
}
