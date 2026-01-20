//! [`MetaLog`] — the addressing chain's bookkeeping, and when to compact it.
//!
//! The chunks themselves are [`crate::edit`]'s business: this tracks which of
//! them are live, hands the `Commit` frame its [`head`](MetaLog::head), and
//! decides when a round should write a full snapshot instead of another delta.
//!
//! Splitting it out is the file-length rule, but the seam is real — one module
//! is a wire format plus a fold, the other is a retention policy.

use crate::block::{BlockDescriptor, Run};
use crate::edit::MAX_EDIT_CHUNKS;

/// Edit-chunk blocks that must accumulate before rewriting the snapshot is
/// worth it. Without a floor a small database would snapshot every round.
const COMPACT_FLOOR_BLOCKS: u64 = 16; // 64 KiB

/// The log of addressing records currently on disk: the last full snapshot and
/// every delta chunk written since it. A `Commit` frame is this, verbatim.
#[derive(Debug, Default)]
pub struct MetaLog {
    snapshot: BlockDescriptor,
    edits: Vec<BlockDescriptor>,
    edit_blocks: u64,
}

impl MetaLog {
    /// The log a recovered chain describes, oldest chunk first (`walk`'s
    /// order). The first is treated as the snapshot: it is one whenever a
    /// compaction has happened, and on a database young enough that none has,
    /// the only consequence is a smaller ratio base — which the
    /// [`COMPACT_FLOOR_BLOCKS`] floor covers.
    pub fn restored(chain: Vec<BlockDescriptor>) -> Self {
        let mut chunks = chain.into_iter();
        let snapshot = chunks.next().unwrap_or(BlockDescriptor::EMPTY);
        let edits: Vec<BlockDescriptor> = chunks.collect();
        let edit_blocks = edits.iter().map(|d| d.count()).sum();
        Self {
            snapshot,
            edits,
            edit_blocks,
        }
    }

    /// Whether the next round should write a full snapshot instead of a delta:
    /// once the deltas outweigh the state they patch, rewriting it costs less
    /// than carrying them.
    pub fn wants_snapshot(&self) -> bool {
        self.edits.len() >= MAX_EDIT_CHUNKS
            || self.edit_blocks
                >= self.snapshot.count().max(COMPACT_FLOOR_BLOCKS)
    }

    /// Record the round's chunk, returning the runs it superseded (a snapshot
    /// supersedes the previous snapshot and every chunk after it).
    pub fn record(&mut self, chunk: BlockDescriptor, full: bool) -> Vec<Run> {
        if !full {
            self.edit_blocks += chunk.count();
            self.edits.push(chunk);
            return Vec::new();
        }
        let mut stale: Vec<Run> =
            self.edits.drain(..).map(BlockDescriptor::run).collect();
        if self.snapshot.is_allocated() {
            stale.push(self.snapshot.run());
        }
        self.edit_blocks = 0;
        self.snapshot = chunk;
        stale
    }

    /// Whether any delta chunk follows the snapshot.
    pub const fn has_deltas(&self) -> bool {
        !self.edits.is_empty()
    }

    /// Every run the log occupies — part of a checkpoint's protected set.
    pub fn runs(&self) -> Vec<Run> {
        self.edits
            .iter()
            .chain(std::iter::once(&self.snapshot))
            .filter(|d| d.is_allocated())
            .map(|d| d.run())
            .collect()
    }

    /// The newest chunk's raw descriptor — what a `Commit` frame carries, and
    /// what the next round writes as its `prev`.
    ///
    /// The whole log is *this*, plus the chain in the data files: a frame is 8
    /// bytes whether the log holds one chunk or a thousand ([RFC 0048]).
    ///
    /// [RFC 0048]: ../../../rfcs/0048-chained-addressing-log.md
    pub fn head(&self) -> u64 {
        self.edits.last().unwrap_or(&self.snapshot).raw()
    }
}

#[cfg(test)]
mod tests {
    use super::MetaLog;
    use crate::block::BlockDescriptor;

    #[test]
    fn compaction_triggers_on_the_floor_then_on_the_ratio() {
        let mut log = MetaLog::default();
        assert!(!log.wants_snapshot(), "an empty log needs no snapshot");
        // Below the floor a small database keeps appending deltas.
        for start in 0..15 {
            log.record(BlockDescriptor::new(start, 1), false);
        }
        assert!(!log.wants_snapshot());
        log.record(BlockDescriptor::new(100, 1), false);
        assert!(log.wants_snapshot(), "16 blocks of deltas hits the floor");

        // Snapshotting frees everything before it and resets the count.
        let stale = log.record(BlockDescriptor::new(200, 64), true);
        assert_eq!(stale.len(), 16, "every delta chunk is superseded");
        assert!(!log.wants_snapshot());
        assert_eq!(log.runs().len(), 1, "only the snapshot is live");

        // Now the ratio rules: 64 blocks of snapshot want 64 of deltas.
        for start in 0..63 {
            log.record(BlockDescriptor::new(1000 + start, 1), false);
        }
        assert!(!log.wants_snapshot());
        log.record(BlockDescriptor::new(2000, 1), false);
        assert!(log.wants_snapshot());
    }

    /// RFC 0048: the frame names the newest chunk and nothing else, so it does
    /// not grow as the log does — and a log restored from the walked chain
    /// resumes with the same accounting.
    #[test]
    fn the_frame_is_the_head_however_long_the_log() {
        let mut log = MetaLog::default();
        log.record(BlockDescriptor::new(10, 2), true);
        assert_eq!(log.head(), BlockDescriptor::new(10, 2).raw());

        let mut chain = vec![BlockDescriptor::new(10, 2)];
        for start in 0..40 {
            let desc = BlockDescriptor::new(20 + start, 1);
            log.record(desc, false);
            chain.push(desc);
            assert_eq!(log.head(), desc.raw(), "the head is always the newest");
        }

        let restored = MetaLog::restored(chain);
        assert_eq!(restored.head(), log.head());
        assert_eq!(restored.runs().len(), log.runs().len());
        assert_eq!(restored.wants_snapshot(), log.wants_snapshot());
    }

    /// A fresh database writes deltas before it has ever snapshotted, so the
    /// chain's oldest chunk is a delta over an empty directory. Restoring must
    /// keep every one of them reachable.
    #[test]
    fn a_chain_that_never_snapshotted_restores_whole() {
        let mut log = MetaLog::default();
        let chain: Vec<BlockDescriptor> = (0..3)
            .map(|start| BlockDescriptor::new(100 + start, 1))
            .collect();
        for desc in &chain {
            log.record(*desc, false);
        }
        let restored = MetaLog::restored(chain);
        assert_eq!(restored.head(), log.head());
        assert_eq!(restored.runs().len(), 3, "no chunk may be dropped");
    }
}
