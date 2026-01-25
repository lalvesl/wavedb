//! Folding the addressing log back into directories ([RFC 0048]) — split
//! from [`crate::edit`], which writes the chunks this reads.
//!
//! [RFC 0048]: ../../../rfcs/0048-chained-addressing-log.md

use std::collections::BTreeMap;

use crate::edit::{EditChunk, SlotEdit};
use crate::error::{StorageError, StorageResult};

/// The addressing state rebuilt by replaying a snapshot and the chunks after
/// it, in order.
#[derive(Debug, Default)]
pub struct Replay {
    parts: Replayed,
}

/// What a [`Replay`] rebuilt, per `STRUCT_HASH`.
#[derive(Debug, Default)]
pub struct Replayed {
    /// The disk directories' raw descriptors.
    pub slots: BTreeMap<u64, Vec<u64>>,
    /// The dictionaries' runs.
    pub dicts: BTreeMap<u64, u64>,
    /// The object store's directories' raw descriptors (RFC 0070).
    pub remote: BTreeMap<u64, Vec<u64>>,
    /// The object store's chain, as the newest chunk naming it left it.
    pub chain: Option<crate::header::ChainRecord>,
    /// The remote descriptors of the types' dictionary images.
    pub remote_dicts: BTreeMap<u64, u64>,
}

impl Replay {
    /// Fold one chunk in.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] if a chunk names a bucket outside the
    /// directory size it declares.
    pub fn apply(&mut self, chunk: &EditChunk) -> StorageResult<()> {
        for edit in &chunk.slots {
            fold(&mut self.parts.slots, edit)?;
            if let Some(dict) = edit.dict {
                self.parts.dicts.insert(edit.struct_hash, dict);
            }
        }
        for edit in &chunk.remote {
            fold(&mut self.parts.remote, edit)?;
            if let Some(dict) = edit.dict {
                self.parts.remote_dicts.insert(edit.struct_hash, dict);
            }
        }
        if let Some(chain) = &chunk.chain {
            self.parts.chain = Some(chain.clone());
        }
        Ok(())
    }

    /// Everything rebuilt.
    pub fn into_parts(self) -> Replayed {
        self.parts
    }
}

/// Apply one type's edit to its directory in `dirs`.
fn fold(
    dirs: &mut BTreeMap<u64, Vec<u64>>,
    edit: &SlotEdit,
) -> StorageResult<()> {
    let addresses = dirs.entry(edit.struct_hash).or_default();
    if (edit.buckets as usize) > addresses.len() {
        addresses.resize(edit.buckets as usize, 0);
    }
    for &(bucket, raw) in &edit.changed {
        let cell = addresses
            .get_mut(bucket as usize)
            .ok_or(StorageError::Corrupt("edit chunk bucket out of range"))?;
        *cell = raw;
    }
    Ok(())
}
