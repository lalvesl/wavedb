//! The header that closes every batch ([RFC 0072] §3) — today the head of
//! every uploaded file ([RFC 0070] §5–§6) — and the chain the headers form,
//! which a reconstruction walks.
//!
//! - **A header names the types its batch changed**, each with its bucket
//!   count, its dictionary, the addresses that changed, and `last` — the
//!   header before it with entries for that type. Following `last`, a type's
//!   addresses are gathered newest first.
//! - **Spare room restates old addresses** (`crate::upload_file`): the type
//!   restated longest ago gets its addresses rewritten from a rotating
//!   cursor, so every type — one never written included — keeps appearing
//!   near the head of the chain and a reconstruction reads a bounded number
//!   of headers.
//! - **`prev` names the header before**, whatever types it holds: the way
//!   back to a type the newer headers do not name, and the check that a
//!   batch spread over several headers is whole — every header back to the
//!   previous `end_of_batch` present.
//!
//! No hash links the headers: nothing is written twice — a file number is
//! never reused, every PUT is create-only — so a header's crc and its own
//! `at` are the whole check.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md

use std::collections::BTreeMap;

use wavedb_core::wire::{
    CRC_PREFIX_LEN, WaveWire, from_wire_checked, to_wire_checked,
};

use crate::error::{StorageError, StorageResult};

/// "No such header" in a link.
pub const NO_FILE: u64 = u64::MAX;

/// The header closing a batch, or a part of one.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct Header {
    /// Where this header is — the file's own number: a copy of it under
    /// another key is not it.
    pub at: u64,
    /// The header before this one ([`NO_FILE`] for the first).
    pub prev: u64,
    /// Whether this header ends a batch: with the headers before it, it
    /// holds every write up to a point and none in part. A reconstruction
    /// roots only in one.
    pub end_of_batch: bool,
    /// The oldest file not deleted when this one was written. It may lag
    /// behind the truth, never run ahead of it (RFC 0070 §4).
    pub start: u64,
    /// The remote end once this batch is in.
    pub end: u64,
    /// The types this header names: changed, then restated.
    pub types: Vec<TypeEntry>,
    /// On disk, in disk + object mode: the object store's directories, the
    /// same way — each entry's `last` the disk header before with an entry
    /// for that type's remote directory. Empty in an object store's file.
    pub remote: Vec<TypeEntry>,
    /// On disk, in disk + object mode: where the object store's chain of
    /// files stands. `None` in an object store's file.
    pub chain: Option<ChainRecord>,
}

/// One type in a header.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct TypeEntry {
    pub struct_hash: u64,
    /// The directory's bucket count.
    pub buckets: u32,
    /// The header before this one with entries for this type
    /// ([`NO_FILE`]).
    pub last: u64,
    /// The type's dictionary image, if any.
    pub dict: Option<u64>,
    /// `(bucket, address)`: what the batch changed, and what is restated.
    pub changed: Vec<(u32, u64)>,
}

/// Bytes a type's entry takes with no `changed` items.
pub const ENTRY_BYTES: usize = TypeEntry::STACK_SIZE + 8;

/// Bytes one `changed` item takes.
pub const ITEM_BYTES: usize = <(u32, u64)>::STACK_SIZE;

/// A header's encoded length with no type in it.
pub const BARE_BYTES: usize = 4 + CRC_PREFIX_LEN + Header::STACK_SIZE;

impl Header {
    /// Its encoded length (`[len u32][crc][wire]`), without encoding it.
    pub fn encoded_len(&self) -> usize {
        BARE_BYTES + self.heap_size()
    }

    /// `[len u32 LE][crc32][wire]` — the envelope a page uses.
    pub fn encode(&self) -> Vec<u8> {
        let payload = to_wire_checked(self);
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Read a header from the bytes it heads.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] when the bytes are not a header.
    pub fn decode(bytes: &[u8]) -> StorageResult<Self> {
        let len = bytes
            .get(..4)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_le_bytes)
            .ok_or(StorageError::Corrupt("header length"))?
            as usize;
        let envelope = bytes
            .get(4..4 + len)
            .ok_or(StorageError::Corrupt("header cut short"))?;
        from_wire_checked(envelope).map_err(|_| StorageError::Corrupt("header"))
    }
}

/// Where a type stands in the chain of headers. RAM only but `last`, which
/// the newest headers record; after a reopen the rotation starts over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rotation {
    /// The newest header with entries for the type.
    pub last: u64,
    /// Where its next restated addresses begin.
    pub shift: usize,
    /// The header in which spare room last restated it — what the spare
    /// room is handed out by, the oldest first. `None`: never.
    pub shifted: Option<u64>,
    /// The header the running rotation began in.
    pub began: Option<u64>,
    /// The header the last whole rotation began in: every bucket of the type
    /// is named at or after it, so a reconstruction reads nothing older for
    /// it (`crate::restate::horizon`).
    pub covered: Option<u64>,
}

impl Default for Rotation {
    fn default() -> Self {
        Self {
            last: NO_FILE,
            shift: 0,
            shifted: None,
            began: None,
            covered: None,
        }
    }
}

/// Where an object-backed store's chain of files stands — in RAM, and
/// recorded in the disk's headers ([`ChainRecord`]).
#[derive(Debug, Default, Clone)]
pub struct Chain {
    /// The next remote block an upload writes at.
    pub end: u64,
    /// The oldest live file.
    pub start: u64,
    /// The newest file, once anything was uploaded.
    pub tail: Option<u64>,
    /// Per type: where it stands in the chain.
    pub types: BTreeMap<u64, Rotation>,
}

/// A [`Chain`] as an addressing chunk records it.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct ChainRecord {
    pub end: u64,
    pub start: u64,
    pub tail: u64,
    pub last: Vec<(u64, u64)>,
}

impl Chain {
    /// The newest header with entries for `struct_hash`.
    pub fn last(&self, struct_hash: u64) -> u64 {
        self.types.get(&struct_hash).map_or(NO_FILE, |r| r.last)
    }

    pub fn record(&self) -> ChainRecord {
        ChainRecord {
            end: self.end,
            start: self.start,
            tail: self.tail.unwrap_or(NO_FILE),
            last: self.types.iter().map(|(&h, r)| (h, r.last)).collect(),
        }
    }

    pub fn from_record(record: &ChainRecord) -> Self {
        Self {
            end: record.end,
            start: record.start,
            tail: (record.tail != NO_FILE).then_some(record.tail),
            types: record
                .last
                .iter()
                .map(|&(h, last)| {
                    let rotation = Rotation {
                        last,
                        ..Rotation::default()
                    };
                    (h, rotation)
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BARE_BYTES, ENTRY_BYTES, Header, ITEM_BYTES, NO_FILE, TypeEntry,
    };

    fn header() -> Header {
        Header {
            at: 7,
            prev: 6,
            end_of_batch: true,
            start: 0,
            end: 7 * 2048 + 20,
            types: vec![TypeEntry {
                struct_hash: 5,
                buckets: 3,
                last: NO_FILE,
                dict: Some(42),
                changed: vec![(0, 1), (2, 3)],
            }],
            remote: Vec::new(),
            chain: None,
        }
    }

    #[test]
    fn a_header_roundtrips_and_its_length_is_known_ahead() {
        let h = header();
        let bytes = h.encode();
        assert_eq!(bytes.len(), h.encoded_len());
        let mut padded = bytes;
        padded.resize(32 * 1024, 0);
        assert_eq!(Header::decode(&padded).unwrap(), h);
    }

    /// The constants the packer budgets with match the encoding.
    #[test]
    fn entries_and_items_cost_what_the_budget_says() {
        let mut h = header();
        h.types.clear();
        assert_eq!(h.encoded_len(), BARE_BYTES);
        assert_eq!(h.encode().len(), BARE_BYTES);
        h.types.push(TypeEntry {
            struct_hash: 1,
            buckets: 1,
            last: NO_FILE,
            dict: Some(0),
            changed: Vec::new(),
        });
        assert_eq!(h.encoded_len(), BARE_BYTES + ENTRY_BYTES);
        h.types[0].changed.push((1, 1));
        assert_eq!(h.encoded_len(), BARE_BYTES + ENTRY_BYTES + ITEM_BYTES);
    }

    #[test]
    fn bytes_that_are_not_a_header_are_refused() {
        assert!(Header::decode(&[]).is_err());
        assert!(Header::decode(&[9, 0, 0, 0, 1, 2]).is_err());
        let mut bytes = header().encode();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        assert!(Header::decode(&bytes).is_err(), "a flipped bit");
    }
}
