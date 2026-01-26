//! What the disk's headers keep track of between windows ([RFC 0072] §3–§8):
//! the changes no header names yet, and where the chain of headers stands.
//! Split from [`crate::disk_header`], which writes the headers.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point-WIP.md

use std::collections::{BTreeMap, BTreeSet};

use crate::header::Rotation;

/// Changes no header names yet, per slot index.
#[derive(Debug, Default)]
pub struct Unrecorded {
    pub(crate) slots: BTreeMap<usize, Touched>,
}

/// One slot's unrecorded changes.
#[derive(Debug, Default)]
pub struct Touched {
    /// Disk buckets: a page placed, emptied, evicted, or its flag cleared.
    pub(crate) disk: BTreeSet<usize>,
    /// Remote buckets an upload placed or emptied.
    pub(crate) remote: BTreeSet<usize>,
    /// The dictionary moved.
    pub(crate) dict: bool,
    /// The object store's copy of the dictionary moved.
    pub(crate) remote_dict: bool,
}

impl Touched {
    pub(crate) fn is_empty(&self) -> bool {
        self.disk.is_empty()
            && self.remote.is_empty()
            && !self.dict
            && !self.remote_dict
    }
}

/// Where the disk's chain of headers stands.
#[derive(Debug, Default)]
pub struct DiskChain {
    /// The newest header written.
    pub(crate) tail: Option<u64>,
    /// The header ending the batch before the newest one ended.
    pub(crate) last_end: Option<u64>,
    /// Per type, the disk directories' rotation.
    pub(crate) types: BTreeMap<u64, Rotation>,
    /// Per type, the object store's directories' rotation.
    pub(crate) remote: BTreeMap<u64, Rotation>,
    /// The lowest header the open read: the horizon stays there until the
    /// rotations pass it.
    pub(crate) floor: u64,
    /// The headers a reconstruction may still read.
    pub(crate) live: BTreeSet<u64>,
    /// A full restatement is under way (`record_all`).
    pub(crate) restating: bool,
    /// The header it began in, once one is written.
    pub(crate) restate_from: Option<u64>,
    /// The header the last full restatement began in: every bucket is named
    /// at or after it.
    pub(crate) restated: Option<u64>,
}

impl DiskChain {
    /// Whether headers below the last full restatement are still live —
    /// one more batch has to end for them to go.
    pub(crate) fn outlived(&self) -> bool {
        self.restated
            .is_some_and(|from| self.live.range(..from).next().is_some())
    }
}
