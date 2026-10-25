//! The sparse index over a chain ([RFC 0052]) — [`SparseTree`]: the handle,
//! and the reads a chain and its pager make through it.
//!
//! [`sparse`](super::sparse) is the node: how one level descends and how it
//! encodes. This is the tree over those nodes, read side — a lookup **by key**
//! and a lookup **by global offset**, each one descent.
//! [`sparse_write`](super::sparse_write) is the other half: keeping one entry
//! per chain segment, and every count above it, in step with the chain.
//!
//! [RFC 0052]: https://github.com/wavedb/wavedb/blob/main/rfcs/0052-segment-size-as-the-pagination-unit.md

use crate::error::{Error, Result};
use crate::local_id::LocalId;
use crate::store::{Store, Write};
use crate::u48::U48;

use super::node_key::SecKey;
use super::segment::{Lane, mint_lane_id};
use super::sparse::{Slot, SparseNode, Step};

/// Max entries in a sparse-index node before it splits.
///
/// One constant for both kinds, because a [`Slot`] and a [`Branch`](super::sparse::Branch) are the same
/// shape — least key, pointer, count — unlike `BpTree`, whose leaf and internal
/// bodies differ enough to want separate capacities. Sized so a node of
/// instant-keyed entries stays inside the storage engine's 32 KiB bucket target.
pub const DEFAULT_SPARSE_CAP: usize = 700;

/// The sparse index over one chain: segment separators carrying element counts,
/// so a lookup **by key** and a lookup **by global offset** are each one descent.
///
/// Holds only its root pointer, its tenant, its lane and its split capacity;
/// nodes live in the [`Store`] under `LocalId::to_id(tenant)`.
#[derive(Debug, Clone, Copy)]
pub struct SparseTree {
    pub(super) root: LocalId,
    pub(super) tenant: U48,
    pub(super) lane_hash: u64,
    pub(super) cap: usize,
}

impl SparseTree {
    /// Open the index rooted at `root` for `tenant`, over `struct_hash`'s index
    /// lane.
    #[must_use]
    pub fn at(root: LocalId, tenant: U48, struct_hash: u64) -> Self {
        Self {
            root,
            tenant,
            lane_hash: Lane::Index.hash(struct_hash),
            cap: DEFAULT_SPARSE_CAP,
        }
    }

    /// Override the node capacity (small caps make deep trees cheap to build in
    /// tests; production uses [`DEFAULT_SPARSE_CAP`]).
    #[must_use]
    pub const fn with_cap(mut self, cap: usize) -> Self {
        self.cap = cap;
        self
    }

    /// Plan a fresh, empty index: the handle plus the [`Write`] persisting its
    /// empty root leaf. The caller commits the write inside its own batch and
    /// persists the root in its `Pivot` — once, since the root never moves.
    #[must_use]
    pub fn plan_create(tenant: U48, struct_hash: u64) -> (Self, Write) {
        let lane_hash = Lane::Index.hash(struct_hash);
        let tree = Self {
            root: mint_lane_id(lane_hash),
            tenant,
            lane_hash,
            cap: DEFAULT_SPARSE_CAP,
        };
        (tree, tree.put(tree.root, &SparseNode::Leaf(Vec::new())))
    }

    /// The root pointer — permanent for the index's whole life.
    #[must_use]
    pub const fn root(&self) -> LocalId {
        self.root
    }

    /// The segment covering `key`, or `None` when `key` sorts below every
    /// segment (the chain's head is what covers it then).
    ///
    /// # Errors
    /// Propagates a [`Store`] failure, [`Error::ChainNodeMissing`] on a dangling
    /// pointer, or [`Error::LaneBadTag`] when a pointer resolves to a foreign
    /// value.
    pub async fn find<S: Store>(
        &self,
        store: &S,
        key: &SecKey,
    ) -> Result<Option<Slot>> {
        let mut id = self.root;
        loop {
            match self.load(store, id).await?.step_to_key(key) {
                None => return Ok(None),
                Some(Step::Segment { slot, .. }) => return Ok(Some(slot)),
                Some(Step::Child { node, .. }) => id = node,
            }
        }
    }

    /// The segment holding global `offset`, and how far into it that element
    /// sits — the pager's "jump to page k" as one descent rather than a walk.
    ///
    /// # Errors
    /// As [`find`](Self::find).
    pub async fn find_offset<S: Store>(
        &self,
        store: &S,
        offset: u64,
    ) -> Result<Option<(Slot, u64)>> {
        let (mut id, mut at) = (self.root, offset);
        loop {
            match self.load(store, id).await?.step_to_offset(at) {
                None => return Ok(None),
                Some(Step::Segment {
                    slot,
                    offset: within,
                }) => {
                    return Ok(Some((slot, within)));
                }
                Some(Step::Child {
                    node,
                    offset: within,
                }) => {
                    id = node;
                    at = within;
                }
            }
        }
    }

    /// Total elements across every segment — the pager's "of M", exact for an
    /// unfiltered listing, one read cold.
    ///
    /// # Errors
    /// As [`find`](Self::find).
    pub async fn total<S: Store>(&self, store: &S) -> Result<u64> {
        Ok(self.load(store, self.root).await?.total())
    }

    /// The [`Write`] persisting `node` under `id`.
    pub(super) fn put(&self, id: LocalId, node: &SparseNode) -> Write {
        Write::Put(id.to_id(self.tenant), node.to_bytes(self.lane_hash))
    }

    /// Read one node, checking its lane tag.
    pub(super) async fn load<S: Store>(
        &self,
        store: &S,
        id: LocalId,
    ) -> Result<SparseNode> {
        let bytes = store
            .get_of(self.lane_hash, id.to_id(self.tenant))
            .await?
            .ok_or(Error::ChainNodeMissing(id))?;
        SparseNode::from_bytes(self.lane_hash, &bytes)
    }

    /// Every node reachable from the root, root included — the reachable set a
    /// leak check compares against the store's size, and the depth a descent
    /// costs.
    #[cfg(test)]
    pub(super) async fn reachable<S: Store>(
        &self,
        store: &S,
    ) -> (usize, usize) {
        let mut depth = 0;
        let mut count = 0;
        let mut level = vec![self.root];
        while !level.is_empty() {
            depth += 1;
            count += level.len();
            let mut next = Vec::new();
            for id in level {
                if let Ok(SparseNode::Internal(entries)) =
                    self.load(store, id).await
                {
                    next.extend(entries.iter().map(|b| b.node));
                }
            }
            level = next;
        }
        (count, depth)
    }
}
