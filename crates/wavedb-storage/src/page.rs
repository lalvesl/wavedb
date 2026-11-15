//! [`SlotPage`] — the on-disk page format for a homogeneous run of records.
//!
//! A page holds records of **exactly one lane** — one `STRUCT_HASH`'s anchors
//! (Unique records, NonUnique records, Pivots) or one of its chains' entries
//! ([RFC 0078] §7) — each under its **key**: an anchor's is its `Id`'s 16
//! bytes, a chain entry's `[pivot][declaration][key]`. Their encoding lives
//! in `wavedb_core` and is stored here as ordinary bytes.
//!
//! ## Stored — checked wire outside, optionally zstd inside
//!
//! The layout is in `page/codec.rs`: a checked envelope (`struct_hash`, then the
//! body plain or zstd'd, with no dictionary — RFC 0081 §9), the body itself
//! three columns — keys, ends, the records back to back. The envelopes are
//! plain `WaveWire` structs/enums — no page-private byte format. The order
//! is deliberate: the crc covers the **stored** bytes, so corruption is
//! caught by the codec before zstd ever runs; `struct_hash` and the payload
//! kind sit outside the compressed body so a page is self-describing without
//! decompression. The only framing outside the codec is the leading length
//! — a page reads back from a run of whole zero-padded blocks, and the
//! checked decoder needs the exact payload slice.
//!
//! **Not every page compresses.** The caller picks per write (the type's
//! `StructStorage::compress` — a `#[wavedb(compress = false)]` type skips
//! zstd), and even with compression on, a body zstd cannot shrink is
//! stored `Raw` — a page never grows for having been "compressed".
//!
//! ## In RAM — the body as read, and the changes on it (RFC 0076)
//!
//! A page read from disk is **its decompressed body**, one buffer kept as it
//! is: a record is a range of it, found by a binary search over the key
//! column, so opening a page allocates once whatever it holds. Writes land
//! in a map of **changes** beside it — a record put, or one of the body's
//! removed — which a read consults first and an encode
//! ([`to_bytes`](SlotPage::to_bytes)) seals into a new body, which the page
//! then keeps. Iteration is in key order, so serialisation is
//! deterministic.
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::collections::BTreeMap;
use std::collections::btree_map;
use std::iter::Peekable;

use crate::error::StorageResult;

mod body;
mod codec;

use body::{Columns, Cuts};

/// Per-page prefix: `payload_len (u32)`.
const LEN_PREFIX: usize = 4;

/// How a page's body is stored: raw, or zstd'd alone at a level for the
/// path that writes it (RFC 0081 §9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Never compressed: a type opted out (`#[wavedb(compress = false)]`).
    Raw,
    /// The write path's: zstd's default level, cheap beside the write.
    Fast,
    /// The cleanup's — a compaction's merges and moves, in idle time —
    /// where CPU is cheap and a page written is read for long: zstd's
    /// highest level short of its ultra ones, whose windows a page of at
    /// most a few hundred KiB cannot use.
    Thorough,
}

impl Compression {
    /// A type's write path: [`Fast`](Self::Fast) if its pages compress.
    #[must_use]
    pub const fn of(compresses: bool) -> Self {
        if compresses { Self::Fast } else { Self::Raw }
    }

    /// The cleanup's for the same type: [`Thorough`](Self::Thorough) where
    /// it compresses at all.
    #[must_use]
    pub const fn thorough(self) -> Self {
        match self {
            Self::Raw => Self::Raw,
            Self::Fast | Self::Thorough => Self::Thorough,
        }
    }

    /// The zstd level, `None` for raw.
    const fn level(self) -> Option<i32> {
        match self {
            Self::Raw => None,
            Self::Fast => Some(3),
            Self::Thorough => Some(19),
        }
    }
}

/// A homogeneous in-memory page of records, all of one lane.
#[derive(Clone)]
pub struct SlotPage {
    struct_hash: u64,
    /// The encoded body the page was read as (or last sealed into); empty
    /// for a page never encoded. Its records are ranges of it.
    body: Box<[u8]>,
    /// Where the body's columns sit in it.
    cuts: Cuts,
    /// The writes since: `Some` a record put, `None` one of the body's
    /// removed. Ordered by key.
    changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Records held, the changes counted in.
    len: usize,
}

impl SlotPage {
    /// A new, empty page for `struct_hash`.
    #[must_use]
    pub fn new(struct_hash: u64) -> Self {
        Self {
            struct_hash,
            body: Box::default(),
            cuts: Cuts::default(),
            changes: BTreeMap::new(),
            len: 0,
        }
    }

    /// A page over a checked `body` of `len` records.
    const fn opened(
        struct_hash: u64,
        body: Box<[u8]>,
        cuts: Cuts,
        len: usize,
    ) -> Self {
        Self {
            struct_hash,
            body,
            cuts,
            changes: BTreeMap::new(),
            len,
        }
    }

    /// The `STRUCT_HASH` every record in this page shares.
    #[must_use]
    pub const fn struct_hash(&self) -> u64 {
        self.struct_hash
    }

    /// Number of records held.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// `true` if the page holds no records.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// What the page takes in RAM — what the page cache charges it: its
    /// body, and per change its bytes, its `Vec` and its share of the map's
    /// nodes.
    #[must_use]
    pub fn footprint(&self) -> usize {
        /// The two `Vec`s and a share of a B-tree node, about.
        const PER_CHANGE: usize = 72;
        size_of::<Self>()
            + self.body.len()
            + self
                .changes
                .iter()
                .map(|(k, v)| {
                    k.capacity()
                        + v.as_ref().map_or(0, Vec::capacity)
                        + PER_CHANGE
                })
                .sum::<usize>()
    }

    /// Insert or overwrite the record stored at `key`.
    pub fn upsert(&mut self, key: &[u8], bytes: Vec<u8>) {
        if self.get(key).is_none() {
            self.len += 1;
        }
        self.changes.insert(key.to_vec(), Some(bytes));
    }

    /// The record bytes stored at `key`, if present.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        if let Some(change) = self.changes.get(key) {
            return change.as_deref();
        }
        let cols = self.columns()?;
        cols.record(cols.find(key).ok()?)
    }

    /// Remove the record at `key`; whether it was there.
    pub fn remove(&mut self, key: &[u8]) -> bool {
        if self.get(key).is_none() {
            return false;
        }
        self.len -= 1;
        if self.columns().is_some_and(|c| c.find(key).is_ok()) {
            self.changes.insert(key.to_vec(), None);
        } else {
            self.changes.remove(key);
        }
        true
    }

    /// Iterate `(key, bytes)` in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        Entries {
            body: Body {
                cols: self.columns(),
                at: 0,
            },
            changes: self.changes.iter().peekable(),
        }
    }

    /// The body's columns; `None` while the page has none.
    fn columns(&self) -> Option<Columns<'_>> {
        Columns::at(&self.body, self.cuts)
    }

    /// Fold the changes into a new body, which the page keeps.
    fn seal(&mut self) -> StorageResult<()> {
        if self.changes.is_empty() && !self.body.is_empty() {
            return Ok(());
        }
        let bytes = self.body.len()
            + self.changes.values().flatten().map(Vec::len).sum::<usize>();
        let body = body::body_of(self.iter(), self.len, bytes)?;
        self.cuts = body::cuts_of(&body)?;
        self.body = body.into_boxed_slice();
        self.changes.clear();
        Ok(())
    }
}

impl PartialEq for SlotPage {
    /// The same records under the same type, however each page holds them.
    fn eq(&self, other: &Self) -> bool {
        self.struct_hash == other.struct_hash
            && self.len == other.len
            && self.iter().eq(other.iter())
    }
}

impl Eq for SlotPage {}

impl core::fmt::Debug for SlotPage {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SlotPage")
            .field("struct_hash", &self.struct_hash)
            .field("len", &self.len)
            .field("body", &self.body.len())
            .field("cuts", &self.cuts)
            .field("changes", &self.changes.len())
            .finish()
    }
}

/// The body's records from `at` on; none without a body.
struct Body<'p> {
    cols: Option<Columns<'p>>,
    at: usize,
}

impl<'p> Body<'p> {
    /// The next record's key, the record left where it is.
    fn peek_key(&self) -> Option<&'p [u8]> {
        self.cols.as_ref()?.key(self.at)
    }
}

impl<'p> Iterator for Body<'p> {
    type Item = (&'p [u8], &'p [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        let cols = self.cols.as_ref()?;
        let key = cols.key(self.at)?;
        let record = cols.record(self.at)?;
        self.at += 1;
        Some((key, record))
    }
}

/// The body's records and the changes, merged in key order: a change wins
/// over the body's record at its key, and a removal drops it.
///
/// The body is peeked by key alone: a `Peekable` over it carried each record
/// ahead of its turn, which cost the merge five times the walk.
struct Entries<'p> {
    body: Body<'p>,
    changes: Peekable<btree_map::Iter<'p, Vec<u8>, Option<Vec<u8>>>>,
}

impl<'p> Iterator for Entries<'p> {
    type Item = (&'p [u8], &'p [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // No change left (a page as read has none): the body alone.
            let change = self.changes.peek().map(|&(key, _)| key.as_slice());
            let body = change.and_then(|_| self.body.peek_key());
            match change {
                Some(c) if body.is_none_or(|b| c <= b) => {
                    if body == Some(c) {
                        self.body.at += 1;
                    }
                    // A change at `c`: its record, or a removal to skip.
                    if let Some((key, Some(record))) = self.changes.next() {
                        return Some((key.as_slice(), record.as_slice()));
                    }
                }
                _ => return self.body.next(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Compression, SlotPage};
    use crate::block::BLOCK_SIZE;
    use crate::error::StorageError;
    use wavedb_core::{Id, U48};

    fn id(key: u64) -> Vec<u8> {
        Id::new(key, U48::from(1u32), false, key as u16)
            .raw()
            .to_be_bytes()
            .to_vec()
    }

    /// The `KEY` of an `Id`'s key bytes.
    fn key_of(bytes: &[u8]) -> u64 {
        u64::from_be_bytes(bytes[..8].try_into().unwrap())
    }

    #[test]
    fn upsert_get_remove() {
        let mut p = SlotPage::new(0xABCD);
        assert!(p.is_empty());
        p.upsert(&id(1), vec![1, 2, 3]);
        p.upsert(&id(2), vec![4, 5]);
        assert_eq!(p.len(), 2);
        assert_eq!(p.get(&id(1)), Some(&[1, 2, 3][..]));
        // Overwrite.
        p.upsert(&id(1), vec![9]);
        assert_eq!(p.get(&id(1)), Some(&[9][..]));
        assert!(p.remove(&id(2)));
        assert!(!p.remove(&id(2)), "a second removal finds nothing");
        assert_eq!(p.get(&id(2)), None);
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn roundtrip_compressed_and_plain() {
        let mut p = SlotPage::new(0x1122_3344_5566_7788);
        p.upsert(&id(10), vec![0xAA; 5]);
        p.upsert(&id(20), b"hello world".to_vec());
        p.upsert(&id(30), vec![]); // empty record is legal
        for compress in [true, false] {
            let back =
                SlotPage::from_bytes(&p.to_bytes(compress).unwrap()).unwrap();
            assert_eq!(back, p);
            assert_eq!(back.struct_hash(), 0x1122_3344_5566_7788);
        }
    }

    /// A page zstd shrinks is stored compressed, without a dictionary, and
    /// reads back.
    #[test]
    fn a_compressible_page_is_stored_smaller() {
        let mut p = SlotPage::new(9);
        for i in 0..20u64 {
            p.upsert(&id(i + 1), vec![0xEE; 200]);
        }
        let zstd = p.to_bytes(true).unwrap();
        let raw = p.to_bytes(false).unwrap();
        assert!(
            zstd.len() * 4 < raw.len(),
            "{} vs {}",
            zstd.len(),
            raw.len()
        );
        assert_eq!(SlotPage::from_bytes(&zstd).unwrap(), p);
    }

    #[test]
    fn iteration_is_in_key_order() {
        let mut p = SlotPage::new(1);
        p.upsert(&id(30), vec![3]);
        p.upsert(&id(10), vec![1]);
        p.upsert(&id(20), vec![2]);
        let keys: Vec<u64> = p.iter().map(|(k, _)| key_of(k)).collect();
        assert_eq!(keys, vec![10, 20, 30]);
    }

    #[test]
    fn empty_page_roundtrips() {
        let mut p = SlotPage::new(7);
        let back = SlotPage::from_bytes(&p.to_bytes(true).unwrap()).unwrap();
        assert!(back.is_empty());
        assert_eq!(back.struct_hash(), 7);
    }

    #[test]
    fn crc_mismatch_detected() {
        let mut p = SlotPage::new(1);
        p.upsert(&id(1), vec![1, 2, 3]);
        let mut bytes = p.to_bytes(true).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF; // corrupt the envelope
        assert!(matches!(
            SlotPage::from_bytes(&bytes),
            Err(StorageError::Corrupt(_))
        ));
    }

    #[test]
    fn truncated_buffer_detected() {
        // Shorter than the length prefix itself.
        assert!(matches!(
            SlotPage::from_bytes(&[0u8; 2]),
            Err(StorageError::Corrupt(_))
        ));
        // A length that reaches past the buffer.
        let mut p = SlotPage::new(1);
        p.upsert(&id(1), vec![1, 2, 3]);
        let bytes = p.to_bytes(true).unwrap();
        assert!(matches!(
            SlotPage::from_bytes(&bytes[..bytes.len() - 1]),
            Err(StorageError::Corrupt("page length out of range"))
        ));
    }

    #[test]
    fn reads_from_padded_run() {
        // A page on disk occupies whole blocks; the tail past the payload is
        // zero padding. from_bytes must ignore it and still verify the crc.
        let mut p = SlotPage::new(5);
        p.upsert(&id(1), vec![1, 2, 3]);
        p.upsert(&id(2), b"abc".to_vec());
        let mut bytes = p.to_bytes(true).unwrap();
        bytes.resize(BLOCK_SIZE * 2, 0);
        let back = SlotPage::from_bytes(&bytes).unwrap();
        assert_eq!(back, p);
    }

    /// A page read back is its body: the changes on top of it win, a removal
    /// hides a body record, and iteration merges both in id order.
    #[test]
    fn changes_on_a_read_page_win_over_its_body() {
        let mut p = SlotPage::new(3);
        for k in [10, 20, 30, 40] {
            p.upsert(&id(k), vec![k as u8; 3]);
        }
        let mut back =
            SlotPage::from_bytes(&p.to_bytes(true).unwrap()).unwrap();
        back.upsert(&id(20), b"new".to_vec()); // over a body record
        back.upsert(&id(25), b"mid".to_vec()); // between two
        back.upsert(&id(50), b"end".to_vec()); // past the last
        assert!(back.remove(&id(30))); // a body record
        assert!(back.remove(&id(50))); // a change
        assert!(!back.remove(&id(99)));
        assert_eq!(back.len(), 4);
        assert_eq!(back.get(&id(10)), Some(&[10u8; 3][..]));
        assert_eq!(back.get(&id(20)), Some(&b"new"[..]));
        assert_eq!(back.get(&id(30)), None);
        assert_eq!(back.get(&id(50)), None);
        let seen: Vec<(u64, Vec<u8>)> =
            back.iter().map(|(k, b)| (key_of(k), b.to_vec())).collect();
        assert_eq!(
            seen,
            [
                (10, vec![10; 3]),
                (20, b"new".to_vec()),
                (25, b"mid".to_vec()),
                (40, vec![40; 3]),
            ]
        );
        // And it encodes as what it holds.
        let again =
            SlotPage::from_bytes(&back.to_bytes(true).unwrap()).unwrap();
        assert_eq!(again, back);
    }

    /// Encoding seals the changes into the body the page keeps: nothing
    /// per record is left, and the page held is the page written.
    #[test]
    fn an_encoded_page_keeps_one_buffer() {
        let mut p = SlotPage::new(4);
        for k in 1..=500 {
            p.upsert(&id(k), vec![7; 40]);
        }
        let bytes = p.to_bytes(false).unwrap();
        // The body alone: four column lengths and the key width, then per
        // record its key, its end and its 40 bytes.
        let body = 5 * 4 + 500 * (16 + 4 + 40);
        assert_eq!(p.footprint(), size_of::<SlotPage>() + body);
        assert_eq!(
            p.to_bytes(false).unwrap(),
            bytes,
            "sealing twice changes nothing"
        );
        let read = SlotPage::from_bytes(&bytes).unwrap();
        assert_eq!(read.footprint(), p.footprint());
        assert_eq!(read, p);
    }

    // Repetitive records must actually shrink on disk, with no dictionary:
    // a page holds enough of them for zstd to find their shape.
    #[test]
    fn similar_records_compress_well() {
        let record = |i: u64| {
            format!("{{\"kind\":\"invoice\",\"cents\":{i},\"paid\":false}}")
                .into_bytes()
        };
        let mut p = SlotPage::new(2);
        let mut raw_total = 0usize;
        for i in 0..200u64 {
            let r = record(i);
            raw_total += r.len();
            p.upsert(&id(i + 1), r);
        }
        let stored = p.to_bytes(true).unwrap();
        assert!(
            stored.len() < raw_total / 2,
            "expected >2x compression on repetitive records: {} vs {raw_total}",
            stored.len()
        );
    }

    /// The cleanup's level stores the same page in no more bytes than the
    /// write path's, and it reads back the same: the level is the writer's
    /// choice alone.
    #[test]
    fn the_cleanup_s_level_reads_back_the_same_page() {
        let mut p = SlotPage::new(2);
        for i in 0..300u64 {
            let r = format!("{{\"order\":{i},\"sku\":\"A-{}\"}}", i % 17);
            p.upsert(&id(i + 1), r.into_bytes());
        }
        let fast = p.encode(Compression::Fast).unwrap();
        let thorough = p.encode(Compression::Thorough).unwrap();
        assert!(
            thorough.len() <= fast.len(),
            "{} > {}",
            thorough.len(),
            fast.len()
        );
        assert_eq!(SlotPage::from_bytes(&thorough).unwrap(), p);
        assert_eq!(SlotPage::from_bytes(&fast).unwrap(), p);
        assert_eq!(Compression::Raw.thorough(), Compression::Raw);
        assert_eq!(Compression::of(true).thorough(), Compression::Thorough);
    }
}
