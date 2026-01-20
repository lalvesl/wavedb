//! The journal's frames: what one append writes ([`crate::journal`] has the
//! file they are written to, and why there are two kinds).

use wavedb_core::Write;
use wavedb_core::wire::WaveWire;

/// One journal frame — see the module docs for the two kinds.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum JournalFrame {
    /// One atomic batch of writes.
    Batch(Vec<Write>),
    /// The retirement record of an older journal.
    Commit(CommitFrame),
}

/// "Journal `journal_ts` is **done**" — its writes are settled into
/// the data files — plus where to find the addressing state that makes those
/// pages findable.
///
/// The frame is a **pointer**, not the state: the descriptors themselves ride
/// the settle windows that changed them
/// ([RFC 0046](../../../rfcs/0046-directory-deltas-in-the-window.md)), and each
/// chunk names the one before it, so this needs to name only the newest
/// ([RFC 0048](../../../rfcs/0048-chained-addressing-log.md)).
///
/// **Fixed size.** It was a list of every chunk since the last snapshot, which
/// meant appending the whole list again at every checkpoint to name state the
/// previous frame already named — quadratic in the compaction interval, and so
/// a ceiling on that interval. Now it is 16 bytes whether the log holds one
/// chunk or a thousand.
///
/// Appended **after** the page window is synced: physical order is the
/// contract, and a frame is only meaningful once the blocks it addresses
/// are durable.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct CommitFrame {
    /// Timestamp of the journal this commit retires — the DONE marker.
    pub journal_ts: u64,
    /// Raw descriptor of the newest edit chunk; the walk goes back from here
    /// to the snapshot. `0` = no addressing log yet (a first-generation
    /// database whose directories are still empty).
    pub head: u64,
}
