//! Where a [`Server`]'s database lives and how it keeps it: the data
//! directory, the durability window, the object store, and the checkpoint
//! and cache numbers. Split from the crate root, which keeps the rest of
//! the builder and the serving.

use std::path::PathBuf;
use std::time::Duration;

use wavedb_storage::StorageRegistry;

use crate::{NodeTasks, Server};

impl<E> Server<E>
where
    E: NodeTasks + StorageRegistry,
{
    /// Set the directory holding the database: its `MANIFEST` and one
    /// directory per shard under `shards/`, each with its data files and
    /// journal.
    #[must_use]
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = dir.into();
        self
    }

    /// Group-commit writes: allow an acknowledged batch to sit unsynced for
    /// up to `window`, so a burst costs one `fsync` instead of one each
    /// (RFC 0061).
    ///
    /// **Default zero — one barrier per batch**, which is the promise the
    /// engine is built around. Setting this weakens what `Ok` means for
    /// every write the node serves: a crash then loses a *suffix* of
    /// acknowledged writes (never a corrupt store). Set it when the
    /// application can say how much recent work it is willing to lose, and
    /// not otherwise.
    #[must_use]
    pub const fn relax_window(mut self, window: Duration) -> Self {
        self.store.relax_window = window;
        self
    }

    /// Keep the pages in an object store, the disk caching them (RFC 0070):
    /// each shard keeps its objects under `shards/<xx>/` below `remote`'s
    /// prefix, and uploads what it settled once it holds back
    /// [`upload_after_bytes`](crate::Maintenance::upload_after_bytes) or
    /// after [`upload_interval`](Self::upload_interval). Disk-only or not is
    /// fixed when the database is created, and opening it the other way is
    /// refused; [`object_mode`](Self::object_mode) opens the same files.
    #[must_use]
    pub fn object_store(mut self, remote: wavedb_storage::Remote) -> Self {
        self.store.remote = Some(remote);
        self.diskless = false;
        self
    }

    /// Keep the database in an object store **alone**, with no disk at all
    /// (RFC 0070 §9): pages not uploaded yet and the ones read lately live
    /// in RAM, up to
    /// [`object_ram_bytes`](crate::Maintenance::object_ram_bytes), and
    /// there is no journal — a write is durable once uploaded, which happens
    /// past [`upload_after_bytes`](crate::Maintenance::upload_after_bytes) or
    /// after [`upload_interval`](Self::upload_interval), whichever comes
    /// first.
    /// A crash loses at most that interval of acknowledged writes; a
    /// shutdown uploads everything first and loses none.
    ///
    /// No lock guards the prefix: run one node per prefix. A second one is
    /// fenced the moment it meets the first's files, not silently merged.
    /// A database that was disk + object opens this way after a clean
    /// shutdown, and the other way round.
    #[must_use]
    pub fn object_mode(mut self, remote: wavedb_storage::Remote) -> Self {
        self.store.remote = Some(remote);
        self.diskless = true;
        self
    }

    /// The longest a write waits for its upload while its shard holds less
    /// than a file's worth — in object mode, the durability window. Each
    /// shard with a write waiting costs a PUT per interval. Default 10 s;
    /// `None` uploads on bytes alone.
    #[must_use]
    pub const fn upload_interval(mut self, interval: Option<Duration>) -> Self {
        self.maintenance.upload_interval = interval;
        self
    }

    /// Bytes a shard's data file grows to before the next one opens
    /// (RFC 0071). Default 10 GiB. It decides only when a file closes, so it
    /// may change between runs: fewer, larger files hold fewer descriptors
    /// open, smaller ones give space back in smaller steps.
    #[must_use]
    pub const fn data_file_bytes(mut self, bytes: u64) -> Self {
        self.store.file_bytes = bytes;
        self
    }

    /// How much of a data file must be free before compaction empties it
    /// (RFC 0071 §7), in percent. Default 50: a file mostly alive is not
    /// rewritten for little.
    #[must_use]
    pub const fn victim_free_percent(mut self, percent: u8) -> Self {
        self.store.victim_free_percent = percent;
        self
    }

    /// Checkpoint (persist the pages' metadata and truncate the journal)
    /// once the journal exceeds `bytes`. Default 64 MiB.
    #[must_use]
    pub const fn checkpoint_after_bytes(mut self, bytes: u64) -> Self {
        self.maintenance.checkpoint_after_bytes = bytes;
        self
    }

    /// Evict settled cache entries down to `bytes` (reads then serve from
    /// the pages). Default 1 GiB.
    #[must_use]
    pub const fn cache_budget_bytes(mut self, bytes: usize) -> Self {
        self.maintenance.cache_budget_bytes = bytes;
        self
    }
}
