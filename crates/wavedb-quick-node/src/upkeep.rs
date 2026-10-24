//! The node's maintenance **policy**.
//!
//! Plain numbers: when a worker opens a checkpoint, how far it evicts, when
//! it defragments. Each worker applies them to the stores it owns, on its
//! own thread, between its jobs (`shard::background`, RFC 0067 step 4).

/// The background maintenance policy: how the node settles, checkpoints,
/// and bounds its caches while serving.
#[derive(Debug, Clone, Copy)]
pub struct Maintenance {
    /// Journal bytes that trigger a checkpoint (journal truncates to zero).
    pub checkpoint_after_bytes: u64,
    /// Cache bytes the settle task evicts down to (settled entries only).
    pub cache_budget_bytes: usize,
    /// Defragment once the largest free extent falls below this many blocks —
    /// the point where a checkpoint's window stops fitting a hole and starts
    /// growing the tail (RFC 0042).
    pub defrag_below_blocks: u64,
    /// Blocks one defragmentation pass may copy. The cleaner's whole cost is
    /// this, so it is a plain IO budget per tick.
    pub defrag_budget_blocks: u64,
}

impl Default for Maintenance {
    fn default() -> Self {
        Self {
            checkpoint_after_bytes: 64 * 1024 * 1024, // 64 MiB of journal
            cache_budget_bytes: 1024 * 1024 * 1024,   // 1 GiB — generous
            defrag_below_blocks: 256, // 1 MiB of contiguous room
            defrag_budget_blocks: 256, // ≤ 1 MiB copied per tick
        }
    }
}
