//! The head of every uploaded file ([RFC 0070] §5–§6) and the chain the
//! headers form — what a recovery from the object store alone walks.
//!
//! - **Every header names every type** the object store holds: its bucket
//!   count, its dictionary, and `last` — the newest earlier file whose
//!   header has entries for it. The newest header therefore says what a
//!   complete addressing looks like, and from where each type's entries are
//!   to be gathered, newest first.
//! - **Spare room restates old entries** (`refresh`): the type mentioned
//!   longest ago gets its entries rewritten from a rotating cursor, so every
//!   type keeps appearing near the head of the chain and a recovery reads a
//!   bounded number of headers — no snapshot object, no extra PUT.
//! - **The chain is checked**: `prev` names the file before this one and the
//!   hash of its header, and `group` the upload this file belongs to. A
//!   recovery accepts an upload only whole, with every link intact, so the
//!   files a crashed upload left above the recorded end are never taken for
//!   the newest.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

use std::collections::BTreeMap;

use wavedb_core::wire::{
    CRC_PREFIX_LEN, WaveWire, from_wire_checked, to_wire_checked,
};

use crate::error::{StorageError, StorageResult};

/// "No such file" in a link.
pub const NO_FILE: u64 = u64::MAX;

/// The head of an uploaded file.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct FileHeader {
    /// This file's own number — a copy under another key is not it.
    pub file: u64,
    /// The first file of the upload this file belongs to.
    pub group: u64,
    /// How many files that upload wrote.
    pub group_files: u32,
    /// The file before this one in the chain ([`NO_FILE`] for the first).
    pub prev: u64,
    /// The hash of that file's header ([`header_hash`]).
    pub prev_hash: u64,
    /// The oldest file not deleted when this one was written. It may lag
    /// behind the truth, never run ahead of it (§4).
    pub start: u64,
    /// The remote end once the upload is in.
    pub end: u64,
    /// The per-database hash seed every record is routed with.
    pub seed: [u64; 4],
    /// Every type the object store holds.
    pub types: Vec<TypeEntry>,
}

/// One type in a header.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct TypeEntry {
    pub struct_hash: u64,
    /// The remote directory's bucket count.
    pub buckets: u32,
    /// The newest earlier file with entries for this type ([`NO_FILE`]).
    pub last: u64,
    /// The remote descriptor of the type's dictionary image, if any.
    pub dict: Option<u64>,
    /// `(bucket, remote descriptor)`: pages placed in this file, and old
    /// entries restated.
    pub changed: Vec<(u32, u64)>,
}

/// Bytes a type's entry takes with no `changed` items.
pub const ENTRY_BYTES: usize = TypeEntry::STACK_SIZE + 8;

/// Bytes one `changed` item takes.
pub const ITEM_BYTES: usize = <(u32, u64)>::STACK_SIZE;

impl FileHeader {
    /// Its encoded length (`[len u32][crc][wire]`), without encoding it.
    pub fn encoded_len(&self) -> usize {
        4 + CRC_PREFIX_LEN + Self::STACK_SIZE + self.heap_size()
    }

    /// `[len u32 LE][crc32][wire]` — the envelope an edit chunk uses.
    pub fn encode(&self) -> Vec<u8> {
        let payload = to_wire_checked(self);
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Read a header from the head of a file, returning it with its hash.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] when the bytes are not a header.
    pub fn decode(bytes: &[u8]) -> StorageResult<(Self, u64)> {
        let len = bytes
            .get(..4)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_le_bytes)
            .ok_or(StorageError::Corrupt("file header length"))?
            as usize;
        let envelope = bytes
            .get(..4 + len)
            .ok_or(StorageError::Corrupt("file header cut short"))?;
        let header = from_wire_checked(&envelope[4..])
            .map_err(|_| StorageError::Corrupt("file header"))?;
        Ok((header, header_hash(envelope)))
    }
}

/// The hash a `prev_hash` names: SeaHash over the header's envelope.
pub const fn header_hash(envelope: &[u8]) -> u64 {
    seahash::hash(envelope)
}

/// Where an object-backed store's chain of files stands — in RAM, and
/// recorded in the disk's addressing log ([`ChainRecord`]).
#[derive(Debug, Default, Clone)]
pub struct Chain {
    /// The next remote block an upload writes at.
    pub end: u64,
    /// The oldest live file.
    pub start: u64,
    /// The newest file and its header's hash, once anything was uploaded.
    pub tail: Option<(u64, u64)>,
    /// Per type: the newest file with entries for it.
    pub last: BTreeMap<u64, u64>,
    /// Per type: where its next restated entries begin. RAM only — after a
    /// reopen the rotation simply starts over.
    pub cursor: BTreeMap<u64, usize>,
}

/// A [`Chain`] as an addressing chunk records it.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct ChainRecord {
    pub end: u64,
    pub start: u64,
    pub tail_file: u64,
    pub tail_hash: u64,
    pub last: Vec<(u64, u64)>,
}

impl Chain {
    pub fn record(&self) -> ChainRecord {
        let (tail_file, tail_hash) = self.tail.unwrap_or((NO_FILE, 0));
        ChainRecord {
            end: self.end,
            start: self.start,
            tail_file,
            tail_hash,
            last: self.last.iter().map(|(&h, &f)| (h, f)).collect(),
        }
    }

    pub fn from_record(record: &ChainRecord) -> Self {
        Self {
            end: record.end,
            start: record.start,
            tail: (record.tail_file != NO_FILE)
                .then_some((record.tail_file, record.tail_hash)),
            last: record.last.iter().copied().collect(),
            cursor: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ENTRY_BYTES, FileHeader, ITEM_BYTES, NO_FILE, TypeEntry};

    fn header() -> FileHeader {
        FileHeader {
            file: 7,
            group: 6,
            group_files: 2,
            prev: 6,
            prev_hash: 99,
            start: 0,
            end: 7 * 2048 + 20,
            seed: [1, 2, 3, 4],
            types: vec![TypeEntry {
                struct_hash: 5,
                buckets: 3,
                last: NO_FILE,
                dict: Some(42),
                changed: vec![(0, 1), (2, 3)],
            }],
        }
    }

    #[test]
    fn a_header_roundtrips_and_its_length_is_known_ahead() {
        let h = header();
        let bytes = h.encode();
        assert_eq!(bytes.len(), h.encoded_len());
        let mut padded = bytes.clone();
        padded.resize(32 * 1024, 0);
        let (back, hash) = FileHeader::decode(&padded).unwrap();
        assert_eq!(back, h);
        assert_eq!(hash, super::header_hash(&bytes));
    }

    /// The constants the packer budgets with match the encoding.
    #[test]
    fn entries_and_items_cost_what_the_budget_says() {
        let mut h = header();
        h.types.clear();
        let bare = h.encoded_len();
        h.types.push(TypeEntry {
            struct_hash: 1,
            buckets: 1,
            last: NO_FILE,
            dict: Some(0),
            changed: Vec::new(),
        });
        assert_eq!(h.encoded_len(), bare + ENTRY_BYTES);
        h.types[0].changed.push((1, 1));
        assert_eq!(h.encoded_len(), bare + ENTRY_BYTES + ITEM_BYTES);
    }

    #[test]
    fn bytes_that_are_not_a_header_are_refused() {
        assert!(FileHeader::decode(&[]).is_err());
        assert!(FileHeader::decode(&[9, 0, 0, 0, 1, 2]).is_err());
        let mut bytes = header().encode();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        assert!(FileHeader::decode(&bytes).is_err(), "a flipped bit");
    }
}
