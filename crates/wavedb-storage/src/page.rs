//! [`SlotPage`] — the on-disk page format for a homogeneous run of records.
//!
//! A page holds records of **exactly one `STRUCT_HASH`** (Unique anchors, NonUnique
//! records, Pivots, chain segments and index nodes — their encoding lives in `wavedb_core`'s
//! index layer and is stored here as ordinary bytes).
//!
//! ## Stored — checked wire outside, optionally dictionary-zstd inside
//!
//! The layout is in `page/codec.rs`: a checked envelope (`struct_hash`, then the
//! body plain or zstd'd against the type's dictionary), the body itself
//! three columns — ids, ends, the records back to back. The envelopes are
//! plain `WaveWire` structs/enums — no page-private byte format. The order
//! is deliberate: the crc covers the **stored** bytes, so corruption is
//! caught by the codec before zstd ever runs; `struct_hash` and the payload
//! kind sit outside the compressed body so a page is self-describing without
//! decompression. The only framing outside the codec is the leading length
//! — a page reads back from a run of whole zero-padded blocks, and the
//! checked decoder needs the exact payload slice.
//!
//! **Not every page compresses.** The caller picks per write
//! ([`Directory`](crate::directory::Directory) carries the per-type policy —
//! e.g. a `#[wavedb(compress = false)]` type skips zstd), and even
//! with compression on, a body zstd cannot shrink is stored `Raw` — a page
//! never grows for having been "compressed". `dict_len` pins a `Zstd` page to
//! the [`Dictionary`](crate::dictionary::Dictionary) state (an append-only
//! buffer, so a state is a prefix length) it was compressed against; see
//! [`crate::dictionary`].
//!
//! ## In RAM — the body as read, and the changes on it (RFC 0076)
//!
//! A page read from disk is **its decompressed body**, one buffer kept as it
//! is: a record is a range of it, found by a binary search over the id
//! column, so opening a page allocates once whatever it holds. Writes land
//! in a map of **changes** beside it — a record put, or one of the body's
//! removed — which a read consults first and an encode
//! ([`to_bytes`](SlotPage::to_bytes)) seals into a new body, which the page
//! then keeps. Iteration is in `Id` order (which, `KEY`-first, is type/time
//! order), so serialisation is deterministic.

use std::collections::BTreeMap;
use std::collections::btree_map;
use std::iter::Peekable;

use wavedb_core::Id;

use crate::error::StorageResult;

mod codec;

use codec::{Columns, Cuts};

/// Per-page prefix: `payload_len (u32)`.
const LEN_PREFIX: usize = 4;

/// zstd compression level for page bodies (the crate's default; CPU is free
/// here — no join processing competes for it).
const LEVEL: i32 = 3;

/// A homogeneous in-memory page of records, all of one `STRUCT_HASH`.
#[derive(Clone)]
pub struct SlotPage {
    struct_hash: u64,
    /// The encoded body the page was read as (or last sealed into); empty
    /// for a page never encoded. Its records are ranges of it.
    body: Box<[u8]>,
    /// Where the body's columns sit in it.
    cuts: Cuts,
    /// The writes since: `Some` a record put, `None` one of the body's
    /// removed. Ordered by `Id.raw()`.
    changes: BTreeMap<u128, Option<Vec<u8>>>,
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
        /// The id, the `Vec` and a share of a B-tree node, about.
        const PER_CHANGE: usize = 64;
        size_of::<Self>()
            + self.body.len()
            + self
                .changes
                .values()
                .map(|v| v.as_ref().map_or(0, Vec::capacity) + PER_CHANGE)
                .sum::<usize>()
    }

    /// Insert or overwrite the record stored at `id`.
    pub fn upsert(&mut self, id: Id, bytes: Vec<u8>) {
        if self.get(id).is_none() {
            self.len += 1;
        }
        self.changes.insert(id.raw(), Some(bytes));
    }

    /// The record bytes stored at `id`, if present.
    #[must_use]
    pub fn get(&self, id: Id) -> Option<&[u8]> {
        let raw = id.raw();
        if let Some(change) = self.changes.get(&raw) {
            return change.as_deref();
        }
        let cols = self.columns()?;
        cols.record(cols.find(raw).ok()?)
    }

    /// Remove the record at `id`; whether it was there.
    pub fn remove(&mut self, id: Id) -> bool {
        if self.get(id).is_none() {
            return false;
        }
        self.len -= 1;
        let raw = id.raw();
        if self.columns().is_some_and(|c| c.find(raw).is_ok()) {
            self.changes.insert(raw, None);
        } else {
            self.changes.remove(&raw);
        }
        true
    }

    /// Iterate `(Id, bytes)` in `Id` order.
    pub fn iter(&self) -> impl Iterator<Item = (Id, &[u8])> {
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
        let body = codec::body_of(
            self.iter().map(|(id, b)| (id.raw(), b)),
            self.len,
            bytes,
        )?;
        self.cuts = codec::cuts_of(&body)?;
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

impl Body<'_> {
    /// The next record's id, the record left where it is.
    fn peek_id(&self) -> Option<u128> {
        self.cols.as_ref()?.id(self.at)
    }
}

impl<'p> Iterator for Body<'p> {
    type Item = (u128, &'p [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        let cols = self.cols.as_ref()?;
        let id = cols.id(self.at)?;
        let record = cols.record(self.at)?;
        self.at += 1;
        Some((id, record))
    }
}

/// The body's records and the changes, merged in id order: a change wins
/// over the body's record at its id, and a removal drops it.
///
/// The body is peeked by id alone: a `Peekable` over it carried each record
/// ahead of its turn, which cost the merge five times the walk.
struct Entries<'p> {
    body: Body<'p>,
    changes: Peekable<btree_map::Iter<'p, u128, Option<Vec<u8>>>>,
}

impl<'p> Iterator for Entries<'p> {
    type Item = (Id, &'p [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // No change left (a page as read has none): the body alone.
            let change = self.changes.peek().map(|&(&raw, _)| raw);
            let body = change.and_then(|_| self.body.peek_id());
            match change {
                Some(c) if body.is_none_or(|b| c <= b) => {
                    if body == Some(c) {
                        self.body.at += 1;
                    }
                    // A change at `c`: its record, or a removal to skip.
                    if let Some((_, Some(record))) = self.changes.next() {
                        return Some((Id::from_raw(c), record.as_slice()));
                    }
                }
                _ => {
                    return self
                        .body
                        .next()
                        .map(|(raw, record)| (Id::from_raw(raw), record));
                }
            }
        }
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
        assert!(p.remove(id(2)));
        assert!(!p.remove(id(2)), "a second removal finds nothing");
        assert_eq!(p.get(id(2)), None);
        assert_eq!(p.len(), 1);
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
        let mut p = SlotPage::new(7);
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

    /// A page read back is its body: the changes on top of it win, a removal
    /// hides a body record, and iteration merges both in id order.
    #[test]
    fn changes_on_a_read_page_win_over_its_body() {
        let d = Dictionary::new();
        let mut p = SlotPage::new(3);
        for k in [10, 20, 30, 40] {
            p.upsert(id(k), vec![k as u8; 3]);
        }
        let mut back =
            SlotPage::from_bytes(&p.to_bytes(&d, true).unwrap(), &d).unwrap();
        back.upsert(id(20), b"new".to_vec()); // over a body record
        back.upsert(id(25), b"mid".to_vec()); // between two
        back.upsert(id(50), b"end".to_vec()); // past the last
        assert!(back.remove(id(30))); // a body record
        assert!(back.remove(id(50))); // a change
        assert!(!back.remove(id(99)));
        assert_eq!(back.len(), 4);
        assert_eq!(back.get(id(10)), Some(&[10u8; 3][..]));
        assert_eq!(back.get(id(20)), Some(&b"new"[..]));
        assert_eq!(back.get(id(30)), None);
        assert_eq!(back.get(id(50)), None);
        let seen: Vec<(u64, Vec<u8>)> =
            back.iter().map(|(i, b)| (i.key(), b.to_vec())).collect();
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
        let again = SlotPage::from_bytes(&back.to_bytes(&d, true).unwrap(), &d)
            .unwrap();
        assert_eq!(again, back);
    }

    /// Encoding seals the changes into the body the page keeps: nothing
    /// per record is left, and the page held is the page written.
    #[test]
    fn an_encoded_page_keeps_one_buffer() {
        let d = Dictionary::new();
        let mut p = SlotPage::new(4);
        for k in 1..=500 {
            p.upsert(id(k), vec![7; 40]);
        }
        let bytes = p.to_bytes(&d, false).unwrap();
        // The body alone: three column lengths, then per record its id, its
        // end and its 40 bytes.
        let body = 3 * 4 + 500 * (16 + 4 + 40);
        assert_eq!(p.footprint(), size_of::<SlotPage>() + body);
        assert_eq!(
            p.to_bytes(&d, false).unwrap(),
            bytes,
            "sealing twice changes nothing"
        );
        let read = SlotPage::from_bytes(&bytes, &d).unwrap();
        assert_eq!(read.footprint(), p.footprint());
        assert_eq!(read, p);
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
