//! [`SlotPage`] — the on-disk page format for a homogeneous run of records.
//!
//! A page holds records of **exactly one `STRUCT_HASH`** (Unique anchors, NonUnique
//! records, Pivots, `BpTree` nodes — the node encoding lives in `wavedb_core`'s
//! index layer and is stored here as ordinary bytes).
//!
//! ## Layout — checked wire outside, optionally dictionary-zstd inside
//!
//! ```text
//! [ payload_len (u32 LE) ][ to_wire_checked(PageEnvelope) ]  … zero-padded to the run
//!                          └── [ crc32 ][ struct_hash · PagePayload ] ──┘
//! PagePayload::Raw ( to_wire(PageBody) )                               — stored plain
//! PagePayload::Zstd { dict_len, raw_len, zstd(to_wire(PageBody)) }     — dictionary-compressed
//! ```
//!
//! The envelopes are plain `WaveWire` structs/enums — no page-private byte
//! format. The order is deliberate: the crc covers the **stored** bytes, so
//! corruption is caught by the codec before zstd ever runs; `struct_hash` and
//! the payload kind sit outside the compressed body so a page is
//! self-describing without decompression. The only framing outside the codec
//! is the leading length — a page reads back from a run of whole zero-padded
//! blocks, and the checked decoder needs the exact payload slice.
//!
//! **Not every page compresses.** The caller picks per write
//! ([`Directory`](crate::directory::Directory) carries the per-type policy —
//! e.g. hot, constantly-rewritten `BpTree` node pages skip zstd), and even
//! with compression on, a body zstd cannot shrink is stored `Raw` — a page
//! never grows for having been "compressed". `dict_len` pins a `Zstd` page to
//! the [`Dictionary`] state (an append-only buffer, so a state is a prefix
//! length) it was compressed against; see [`crate::dictionary`].
//!
//! In memory a page is a `BTreeMap<u128, Vec<u8>>` keyed by the record `Id`'s raw
//! `u128`, so iteration is in `Id` order (which, `KEY`-first, is type/time order)
//! and serialisation is deterministic.

use std::collections::BTreeMap;

use wavedb_core::Id;
use wavedb_core::wire::{
    WaveWire, from_wire, from_wire_checked, to_wire, to_wire_checked,
};

use crate::dictionary::Dictionary;
use crate::error::{StorageError, StorageResult};

/// Per-page prefix: `payload_len (u32)`.
const LEN_PREFIX: usize = 4;

/// zstd compression level for page bodies (the crate's default; CPU is free
/// here — no join processing competes for it).
const LEVEL: i32 = 3;

/// The stored envelope: everything needed to open the page, uncompressed.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
struct PageEnvelope {
    /// The `STRUCT_HASH` every record in this page shares — readable without
    /// the dictionary.
    struct_hash: u64,
    /// The body, stored plain or dictionary-compressed.
    payload: PagePayload,
}

/// How the page body is stored.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
enum PagePayload {
    /// `to_wire(PageBody)`, plain — compression off for this page's type, or
    /// zstd could not shrink the body.
    Raw(Vec<u8>),
    /// `to_wire(PageBody)` compressed against `dictionary[..dict_len]`.
    Zstd {
        /// The [`Dictionary`] state (prefix length) the body binds.
        dict_len: u32,
        /// Decompressed body length (the decompressor's exact capacity).
        raw_len: u32,
        /// The zstd frame.
        bytes: Vec<u8>,
    },
}

/// The compressed body — the records themselves.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
struct PageBody {
    /// `(Id.raw(), record wire bytes)` in ascending `Id` order.
    records: Vec<(u128, Vec<u8>)>,
}

/// A homogeneous in-memory page of records, all of one `STRUCT_HASH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotPage {
    struct_hash: u64,
    /// `Id.raw()` → wire-encoded record bytes, ordered by `Id`.
    records: BTreeMap<u128, Vec<u8>>,
}

impl SlotPage {
    /// A new, empty page for `struct_hash`.
    #[must_use]
    pub const fn new(struct_hash: u64) -> Self {
        Self {
            struct_hash,
            records: BTreeMap::new(),
        }
    }

    /// The `STRUCT_HASH` every record in this page shares.
    #[must_use]
    pub const fn struct_hash(&self) -> u64 {
        self.struct_hash
    }

    /// Number of records held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// `true` if the page holds no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// What the page takes in RAM, decoded — what the page cache charges
    /// it: the record bytes, and per record its id, its `Vec` and its share
    /// of the map's nodes.
    #[must_use]
    pub fn footprint(&self) -> usize {
        /// The id, the `Vec` and a share of a B-tree node, about.
        const PER_RECORD: usize = 64;
        size_of::<Self>()
            + self
                .records
                .values()
                .map(|bytes| bytes.capacity() + PER_RECORD)
                .sum::<usize>()
    }

    /// Insert or overwrite the record stored at `id`.
    pub fn upsert(&mut self, id: Id, bytes: Vec<u8>) {
        self.records.insert(id.raw(), bytes);
    }

    /// The record bytes stored at `id`, if present.
    #[must_use]
    pub fn get(&self, id: Id) -> Option<&[u8]> {
        self.records.get(&id.raw()).map(Vec::as_slice)
    }

    /// Remove the record at `id`, returning its bytes if it was present.
    pub fn remove(&mut self, id: Id) -> Option<Vec<u8>> {
        self.records.remove(&id.raw())
    }

    /// Iterate `(Id, bytes)` in `Id` order.
    pub fn iter(&self) -> impl Iterator<Item = (Id, &[u8])> {
        self.records
            .iter()
            .map(|(&raw, bytes)| (Id::from_raw(raw), bytes.as_slice()))
    }

    /// Consume the page into its `(Id, bytes)` entries, in `Id` order.
    pub fn into_entries(self) -> impl Iterator<Item = (Id, Vec<u8>)> {
        self.records
            .into_iter()
            .map(|(raw, bytes)| (Id::from_raw(raw), bytes))
    }

    /// Serialise, framed `[len][checked envelope]`. With `compress` the body
    /// is zstd'd against `dict`'s **latest** state — but stored `Raw` anyway
    /// when zstd cannot shrink it (a page never grows for having been
    /// "compressed"). Without `compress` (a hot page type opting out), zstd
    /// never runs.
    ///
    /// # Errors
    /// [`StorageError::Io`] if zstd fails (allocation-class faults only).
    pub fn to_bytes(
        &self,
        dict: &Dictionary,
        compress: bool,
    ) -> StorageResult<Vec<u8>> {
        let body = PageBody {
            records: self
                .records
                .iter()
                .map(|(&raw, bytes)| (raw, bytes.clone()))
                .collect(),
        };
        let raw = to_wire(&body);

        let payload = if compress {
            let zstd =
                zstd::bulk::Compressor::with_dictionary(LEVEL, dict.latest())?
                    .compress(&raw)?;
            if zstd.len() < raw.len() {
                PagePayload::Zstd {
                    dict_len: dict.len() as u32,
                    raw_len: raw.len() as u32,
                    bytes: zstd,
                }
            } else {
                PagePayload::Raw(raw)
            }
        } else {
            PagePayload::Raw(raw)
        };

        let payload = to_wire_checked(&PageEnvelope {
            struct_hash: self.struct_hash,
            payload,
        });
        let mut out = Vec::with_capacity(LEN_PREFIX + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Whether `buf` holds a whole page: its length prefix in range and its
    /// envelope's crc right — what a reconstruction checks of the batch a
    /// crash may have cut (RFC 0072 §7), without decompressing it.
    #[must_use]
    pub fn intact(buf: &[u8]) -> bool {
        let Some(len) = buf
            .get(..LEN_PREFIX)
            .and_then(|s| s.try_into().ok())
            .map(u32::from_le_bytes)
        else {
            return false;
        };
        buf.get(LEN_PREFIX..LEN_PREFIX + len as usize)
            .is_some_and(|env| from_wire_checked::<PageEnvelope>(env).is_ok())
    }

    /// Parse a page from bytes (typically a whole zero-padded run): length
    /// prefix, crc + envelope decode through the checked codec, then — for a
    /// `Zstd` payload — decompress against the dictionary
    /// state the page was written under.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] on a truncated buffer, an out-of-range
    /// length, a crc mismatch, an undecodable envelope/body, or a `dict_len`
    /// stamping a state `dict` never reached.
    pub fn from_bytes(buf: &[u8], dict: &Dictionary) -> StorageResult<Self> {
        let len_bytes: [u8; LEN_PREFIX] = buf
            .get(..LEN_PREFIX)
            .and_then(|s| s.try_into().ok())
            .ok_or(StorageError::Corrupt("page length truncated"))?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        let end = LEN_PREFIX
            .checked_add(len)
            .filter(|&end| end <= buf.len())
            .ok_or(StorageError::Corrupt("page length out of range"))?;

        let envelope: PageEnvelope =
            from_wire_checked(&buf[LEN_PREFIX..end])
                .map_err(|_| StorageError::Corrupt("page envelope"))?;
        let raw = match envelope.payload {
            PagePayload::Raw(raw) => raw,
            PagePayload::Zstd {
                dict_len,
                raw_len,
                bytes,
            } => {
                let state = dict.state(dict_len as usize).ok_or(
                    StorageError::Corrupt("page dictionary state unknown"),
                )?;
                zstd::bulk::Decompressor::with_dictionary(state)
                    .and_then(|mut d| d.decompress(&bytes, raw_len as usize))
                    .map_err(|_| StorageError::Corrupt("page decompress"))?
            }
        };
        let body: PageBody =
            from_wire(&raw).map_err(|_| StorageError::Corrupt("page body"))?;

        Ok(Self {
            struct_hash: envelope.struct_hash,
            records: body.records.into_iter().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SlotPage;
    use crate::block::BLOCK_SIZE;
    use crate::dictionary::Dictionary;
    use crate::error::StorageError;
    use wavedb_core::{Id, U48};

    fn id(key: u64) -> Id {
        Id::new(key, U48::from(1u32), false, key as u16)
    }

    #[test]
    fn upsert_get_remove() {
        let mut p = SlotPage::new(0xABCD);
        assert!(p.is_empty());
        p.upsert(id(1), vec![1, 2, 3]);
        p.upsert(id(2), vec![4, 5]);
        assert_eq!(p.len(), 2);
        assert_eq!(p.get(id(1)), Some(&[1, 2, 3][..]));
        // Overwrite.
        p.upsert(id(1), vec![9]);
        assert_eq!(p.get(id(1)), Some(&[9][..]));
        assert_eq!(p.remove(id(2)), Some(vec![4, 5]));
        assert_eq!(p.get(id(2)), None);
    }

    #[test]
    fn roundtrip_with_empty_and_warm_dictionary() {
        let mut p = SlotPage::new(0x1122_3344_5566_7788);
        p.upsert(id(10), vec![0xAA; 5]);
        p.upsert(id(20), b"hello world".to_vec());
        p.upsert(id(30), vec![]); // empty record is legal

        // Cold: no dictionary content yet.
        let cold = Dictionary::new();
        let back =
            SlotPage::from_bytes(&p.to_bytes(&cold, true).unwrap(), &cold)
                .unwrap();
        assert_eq!(back, p);
        assert_eq!(back.struct_hash(), 0x1122_3344_5566_7788);

        // Warm: sampled content participates in the roundtrip.
        let mut warm = Dictionary::new();
        warm.sample(b"hello world");
        warm.sample(&[0xAA; 5]);
        let back =
            SlotPage::from_bytes(&p.to_bytes(&warm, true).unwrap(), &warm)
                .unwrap();
        assert_eq!(back, p);
    }

    // The versioning rule: a page stamped at dictionary state N must stay
    // readable after the dictionary grows — a state is a stable prefix.
    #[test]
    fn old_page_survives_dictionary_growth() {
        let mut p = SlotPage::new(9);
        p.upsert(id(1), b"record one".to_vec());

        let mut dict = Dictionary::new();
        dict.sample(b"record one");
        let bytes = p.to_bytes(&dict, true).unwrap(); // stamped at len("record one")

        dict.sample(b"record two arriving later");
        dict.sample(&[0x55; 1000]);
        let back = SlotPage::from_bytes(&bytes, &dict).unwrap();
        assert_eq!(back, p, "grown dictionary broke an old page");
    }

    // A stamp beyond anything this dictionary ever held (foreign file, or a
    // replay that diverged) is a typed corruption, not a zstd panic. The page
    // must be compressible, or the Raw fallback would skip the stamp entirely.
    #[test]
    fn unknown_dictionary_state_detected() {
        let mut p = SlotPage::new(9);
        for i in 0..20u64 {
            p.upsert(id(i + 1), vec![0xEE; 200]);
        }
        let mut big = Dictionary::new();
        big.sample(&[7u8; 500]);
        let bytes = p.to_bytes(&big, true).unwrap(); // dict_len = 500

        let small = Dictionary::new(); // never reached state 500
        assert!(matches!(
            SlotPage::from_bytes(&bytes, &small),
            Err(StorageError::Corrupt("page dictionary state unknown"))
        ));
    }

    #[test]
    fn iteration_is_in_id_order() {
        let mut p = SlotPage::new(1);
        p.upsert(id(30), vec![3]);
        p.upsert(id(10), vec![1]);
        p.upsert(id(20), vec![2]);
        let keys: Vec<u64> = p.iter().map(|(i, _)| i.key()).collect();
        assert_eq!(keys, vec![10, 20, 30]);
    }

    #[test]
    fn empty_page_roundtrips() {
        let d = Dictionary::new();
        let p = SlotPage::new(7);
        let back =
            SlotPage::from_bytes(&p.to_bytes(&d, true).unwrap(), &d).unwrap();
        assert!(back.is_empty());
        assert_eq!(back.struct_hash(), 7);
    }

    #[test]
    fn crc_mismatch_detected() {
        let d = Dictionary::new();
        let mut p = SlotPage::new(1);
        p.upsert(id(1), vec![1, 2, 3]);
        let mut bytes = p.to_bytes(&d, true).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF; // corrupt the envelope
        assert!(matches!(
            SlotPage::from_bytes(&bytes, &d),
            Err(StorageError::Corrupt(_))
        ));
    }

    #[test]
    fn truncated_buffer_detected() {
        let d = Dictionary::new();
        // Shorter than the length prefix itself.
        assert!(matches!(
            SlotPage::from_bytes(&[0u8; 2], &d),
            Err(StorageError::Corrupt(_))
        ));
        // A length that reaches past the buffer.
        let mut p = SlotPage::new(1);
        p.upsert(id(1), vec![1, 2, 3]);
        let bytes = p.to_bytes(&d, true).unwrap();
        assert!(matches!(
            SlotPage::from_bytes(&bytes[..bytes.len() - 1], &d),
            Err(StorageError::Corrupt("page length out of range"))
        ));
    }

    #[test]
    fn reads_from_padded_run() {
        // A page on disk occupies whole blocks; the tail past the payload is
        // zero padding. from_bytes must ignore it and still verify the crc.
        let d = Dictionary::new();
        let mut p = SlotPage::new(5);
        p.upsert(id(1), vec![1, 2, 3]);
        p.upsert(id(2), b"abc".to_vec());
        let mut bytes = p.to_bytes(&d, true).unwrap();
        bytes.resize(BLOCK_SIZE * 2, 0);
        let back = SlotPage::from_bytes(&bytes, &d).unwrap();
        assert_eq!(back, p);
    }

    // Repetitive records must actually shrink on disk once the dictionary has
    // seen their shape — the point of the whole mechanism.
    #[test]
    fn similar_records_compress_well() {
        let record = |i: u64| {
            format!("{{\"kind\":\"invoice\",\"cents\":{i},\"paid\":false}}")
                .into_bytes()
        };
        let mut dict = Dictionary::new();
        dict.sample(&record(0));

        let mut p = SlotPage::new(2);
        let mut raw_total = 0usize;
        for i in 0..200u64 {
            let r = record(i);
            raw_total += r.len();
            p.upsert(id(i + 1), r);
        }
        let stored = p.to_bytes(&dict, true).unwrap();
        assert!(
            stored.len() < raw_total / 2,
            "expected >2x compression on repetitive records: {} vs {raw_total}",
            stored.len()
        );
    }
}
